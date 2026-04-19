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

# ---------- Pure function: check DB layer presence ----------

#' Check which DB layers are present on disk for a given project.
#'
#' @param project_root  Absolute path to the company project root (contains
#'                      `scripts/global_scripts/30_global_data/parameters/scd_type1/db_paths.yaml`).
#' @param yaml_path     Optional override path to `db_paths.yaml`.
#' @return list(
#'   missing = named character vector of layer -> absolute path for missing files,
#'   present = named character vector of layer -> absolute path for present files
#' )
#' @export
check_db_layers_presence <- function(project_root, yaml_path = NULL) {
  if (is.null(yaml_path)) {
    yaml_path <- file.path(
      project_root, "scripts", "global_scripts", "30_global_data",
      "parameters", "scd_type1", "db_paths.yaml"
    )
  }
  if (!file.exists(yaml_path)) {
    stop("db_paths.yaml not found: ", yaml_path, call. = FALSE)
  }

  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' required. Install with install.packages('yaml').",
         call. = FALSE)
  }

  config <- yaml::read_yaml(yaml_path)

  # Check both `databases` (6-layer ETL) and `domain` (SCD-typed domain DBs)
  # since autoinit() loads both in UPDATE_MODE/GLOBAL_MODE.
  all_paths <- c(
    config$databases %||% list(),
    config$domain    %||% list()
  )
  if (length(all_paths) == 0) {
    return(list(missing = character(), present = character()))
  }

  missing_v <- character()
  present_v <- character()
  for (name in names(all_paths)) {
    rel <- all_paths[[name]]
    abs <- file.path(project_root, rel)
    if (file.exists(abs)) {
      present_v[[name]] <- abs
    } else {
      missing_v[[name]] <- abs
    }
  }
  list(missing = missing_v, present = present_v)
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

  if (length(missing_layers) == 0) {
    message("✓ All DB layers present; running selective tar_make()")
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
