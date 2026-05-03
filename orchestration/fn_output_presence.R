#!/usr/bin/env Rscript
# pipeline-smart-cache: Smart Detection Helper
#
# Satisfies spec: "Smart Detection Helper SHALL Be A Standalone Reusable Function"
# in pipeline-smart-cache capability.
#
# Contract:
#   - Parse db_paths.yaml to resolve canonical DB paths
#   - Check file.exists() for each declared layer
#   - Return a list(missing, present) — caller decides what to do
#
# No Makefile-specific calls. No system2("make", ...). Pure I/O + YAML.

# ---------- #512: canonical resolver eager-source at top level ----------
# Captures own file path at source() time (sys.frame()$ofile available
# during script load), then resolves sibling 04_utils/ via shared/
# symlink chain. Only sources if helper not already in scope (avoids
# double-source when autoinit-loaded). Fail-fast on missing — drift
# is a deployment bug, not a runtime fallback.
local({
  if (exists("resolve_db_path_entry", mode = "function",
             envir = .GlobalEnv, inherits = FALSE)) {
    return(invisible(NULL))
  }
  self_path <- tryCatch(
    normalizePath(sys.frame(1)$ofile, mustWork = FALSE),
    error = function(e) ""
  )
  if (!nzchar(self_path) || !file.exists(self_path)) {
    # Sourced via Rscript --file= or similar — fall back to commandArgs
    args <- commandArgs(trailingOnly = FALSE)
    file_arg <- args[grep("^--file=", args)]
    if (length(file_arg) > 0L) {
      self_path <- normalizePath(sub("^--file=", "", file_arg[[1]]),
                                 mustWork = FALSE)
    }
  }
  if (!nzchar(self_path) || !file.exists(self_path)) return(invisible(NULL))
  self_dir <- dirname(self_path)
  helper_path <- file.path(self_dir, "..", "..", "global_scripts",
                           "04_utils", "fn_resolve_db_path_entry.R")
  if (file.exists(helper_path)) {
    source(helper_path, local = FALSE)
  }
})

# ---------- Pure function: check DB layer presence ----------
#
# #512: removed inline `.resolve_db_entry()` — now sources canonical
# `resolve_db_path_entry()` from `04_utils/fn_resolve_db_path_entry.R`
# (single source of truth, prevents drift with `fn_load_db_paths.R`).
# Caller resolution: function body sources canonical helper if not in scope
# (orchestration runs in UPDATE_MODE where autoinit-loaded shared/04_utils/
# is in scope; standalone CLI invocation falls through to the explicit
# source() below).

#' Check which DB layers are present on disk for a given project.
#'
#' @param project_root  Absolute path to the company project root (contains
#'                      `scripts/global_scripts/30_global_data/parameters/scd_type1/db_paths.yaml`).
#' @param yaml_path     Optional override path to `db_paths.yaml`.
#' @return list(
#'   missing          = named character vector of layer -> absolute path
#'                      for REQUIRED missing files (blocks pipeline),
#'   present          = named character vector of layer -> absolute path
#'                      for files that exist on disk,
#'   optional_missing = named character vector of layer -> absolute path
#'                      for `required: false` entries that are missing
#'                      (does NOT trigger nuclear rebuild; #455 fix)
#' )
#' @export
check_db_layers_presence <- function(project_root, yaml_path = NULL) {
  if (is.null(yaml_path)) {
    yaml_path <- file.path(
      project_root, "scripts", "global_scripts", "30_global_data",
      "parameters", "scd_type1", "db_paths.yaml"
    )
  }
  # #455: harden line vs vector yaml_path (parallel to the bug we're fixing
  # below — same defensive pattern)
  if (length(yaml_path) != 1L || !file.exists(yaml_path)) {
    stop("db_paths.yaml not found: ", paste(yaml_path, collapse = ", "),
         call. = FALSE)
  }

  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' required. Install with install.packages('yaml').",
         call. = FALSE)
  }

  # #512: source canonical resolver from 04_utils/ if not already in scope
  # (autoinit sweep typically loads it; fallback paths handle (a) standalone
  # CLI invocation from Makefile, (b) test fixtures that mock project_root
  # via tempdir).
  if (!exists("resolve_db_path_entry", mode = "function", inherits = TRUE)) {
    # Self-relative path: orchestration/ is sibling of global_scripts/04_utils/
    # via shared/ symlink chain. sys.frames walk locates this file's location.
    self_dir <- NULL
    for (i in rev(seq_len(sys.nframe()))) {
      ofile <- tryCatch(sys.frame(i)$ofile, error = function(e) NULL)
      if (!is.null(ofile) && nzchar(ofile)) {
        self_dir <- dirname(normalizePath(ofile))
        break
      }
    }
    helper_candidates <- c(
      # Self-relative (works for autoinit-bypass and test contexts):
      # shared/update_scripts/orchestration/ → ../../global_scripts/04_utils/
      if (!is.null(self_dir)) {
        file.path(self_dir, "..", "..", "global_scripts", "04_utils",
                  "fn_resolve_db_path_entry.R")
      },
      # project_root hints
      file.path(project_root, "scripts", "global_scripts", "04_utils",
                "fn_resolve_db_path_entry.R"),
      file.path(project_root, "shared", "global_scripts", "04_utils",
                "fn_resolve_db_path_entry.R")
    )
    helper_path <- NULL
    for (cand in helper_candidates) {
      if (!is.null(cand) && file.exists(cand)) {
        helper_path <- cand
        break
      }
    }
    if (is.null(helper_path)) {
      stop("Canonical helper resolve_db_path_entry() not found. Tried: ",
           paste(Filter(Negate(is.null), helper_candidates), collapse = ", "),
           call. = FALSE)
    }
    source(helper_path, local = FALSE)
  }

  config <- yaml::read_yaml(yaml_path)

  missing_v          <- character()
  present_v          <- character()
  optional_missing_v <- character()

  # #511 C4: detect cross-section name collision (same key in databases
  # and domain). Caused dual-bucket LEAK in original code (same name
  # written to both present_v and missing_v). yaml authoring bug —
  # fail-fast rather than silently dedupe. Currently no production
  # db_paths.yaml has cross-section duplicates (verified via grep
  # 2026-05-03 #511); fail-fast prevents future regression.
  seen_names <- character()

  # #455: walk databases + domain sections, resolve each entry through the
  # canonical helper. Sections are kept separate for error-message context.
  walk_section <- function(section_list, section_name) {
    if (is.null(section_list)) return(invisible(NULL))
    for (name in names(section_list)) {
      # #511 C4: cross-section collision check
      if (name %in% seen_names) {
        stop(sprintf(
          paste0("db_paths.yaml authoring error: name '%s' appears in ",
                 "both 'databases' and 'domain' sections. Each name must ",
                 "be unique across sections (else dual-bucket leak in ",
                 "presence check)."),
          name
        ), call. = FALSE)
      }
      seen_names[[length(seen_names) + 1L]] <<- name

      resolved <- resolve_db_path_entry(section_list[[name]], name, section_name)
      abs <- file.path(project_root, resolved$path)
      if (file.exists(abs)) {
        present_v[[name]] <<- abs
      } else if (resolved$required) {
        # #513: helper guarantees logical(1) non-NA via stop()-on-malformed
        # branch. Direct access — was isTRUE() wrapper (dead defensive code).
        missing_v[[name]] <<- abs
      } else {
        optional_missing_v[[name]] <<- abs
      }
    }
  }
  walk_section(config$databases, "databases")
  walk_section(config$domain,    "domain")

  list(missing = missing_v, present = present_v,
       optional_missing = optional_missing_v)
}

`%||%` <- function(a, b) if (is.null(a)) b else a

# ---------- Orchestration: smart detection + interactive prompt ----------

#' Smart cache check with interactive gating; intended to be the Makefile
#' entry point. Honors:
#'   - FORCE=1 env var (nuclear rebuild, skip prompt)
#'   - stdin is not a TTY (non-interactive auto-proceed)
#'
#' @param project_root         Absolute project root path.
#' @param store_path           Path to _targets store directory.
#' @param target_script        Path to _targets.R.
#' @return invisible(list(mode, missing_layers))
#' @export
check_and_run <- function(project_root, store_path, target_script) {
  force_mode <- nzchar(Sys.getenv("FORCE", ""))

  if (force_mode) {
    message("⚠ FORCE=1 detected: nuclear rebuild (tar_destroy + full tar_make)")
    run_nuclear(store_path, target_script)
    return(invisible(list(mode = "nuclear", missing_layers = character())))
  }

  result <- check_db_layers_presence(project_root)
  missing_layers <- names(result$missing)
  optional_missing <- names(result$optional_missing)

  if (length(missing_layers) == 0) {
    # #513: distinguish "fully present" from "required-only present + N
    # optional missing" — both go selective rebuild but the user-facing
    # message was misleadingly identical.
    if (length(optional_missing) > 0L) {
      message(sprintf(
        "✓ All required DB layers present (%d optional missing: %s); running selective tar_make()",
        length(optional_missing),
        paste(optional_missing, collapse = ", ")
      ))
    } else {
      message("✓ All DB layers present; running selective tar_make()")
    }
    run_selective(target_script, store_path)
    return(invisible(list(mode = "selective", missing_layers = character())))
  }

  # Missing layers detected — warn and gate on interactivity
  message("⚠ Detected missing DB layer(s):")
  for (layer in missing_layers) {
    message(sprintf("  - %s: %s", layer, result$missing[[layer]]))
  }
  message("")
  message(sprintf("This will trigger nuclear rebuild (tar_destroy + full %s-layer pipeline).",
                  length(missing_layers) + length(result$present)))

  tty_stdin <- isatty(stdin())
  if (!tty_stdin) {
    message("  non-interactive mode: auto-proceed")
    run_nuclear(store_path, target_script)
    return(invisible(list(mode = "nuclear-auto", missing_layers = missing_layers)))
  }

  cat("\nPress ENTER to continue, Ctrl-C to abort: ")
  reply <- readLines(con = stdin(), n = 1)
  # Any input (including empty ENTER) → proceed
  run_nuclear(store_path, target_script)
  invisible(list(mode = "nuclear-confirmed", missing_layers = missing_layers))
}

# ---------- Internal runners (thin wrappers; testable) ----------

run_selective <- function(target_script, store_path) {
  targets::tar_make(script = target_script, store = store_path)
}

run_nuclear <- function(store_path, target_script) {
  if (dir.exists(store_path)) {
    targets::tar_destroy(destroy = "all", ask = FALSE,
                         script = target_script, store = store_path)
    message("  _targets store destroyed")
  }
  targets::tar_make(script = target_script, store = store_path)
}

# ---------- CLI entry (called by Makefile) ----------
# Usage: Rscript fn_output_presence.R <project_root> <store_path> <target_script>

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 3) {
    stop(
      "Usage: Rscript fn_output_presence.R <project_root> <store_path> <target_script>",
      call. = FALSE
    )
  }
  check_and_run(
    project_root  = args[[1]],
    store_path    = args[[2]],
    target_script = args[[3]]
  )
}
