library(targets)
library(yaml)

# =============================================================================
# MAMBA Target Pipeline (Config-Driven)
# Implements MP142 (Configuration-Driven Pipeline) and MP108 (ETL phase order)
# Per SO_P016: Config at Company scope (project root), scripts at Universal scope
#
# Directory structure:
#   {project_root}/                        <- Company scope
#   ├── _targets_config.yaml               <- Config file (generated from merge)
#   └── scripts/update_scripts/            <- Universal scope (this dir)
#       └── _targets.R                     <- This file
# =============================================================================

pipeline_dir <- "."

# Resolve project root: prefer MAMBA_PROJECT_ROOT env var (set by Makefile)
# to avoid symlink + '..' path resolution issues. Fall back to relative path
# for backward compatibility when invoked outside Makefile.
project_root_env <- Sys.getenv("MAMBA_PROJECT_ROOT", "")
if (nzchar(project_root_env) && dir.exists(project_root_env)) {
  project_root <- project_root_env
} else {
  project_root <- normalizePath(file.path(pipeline_dir, "..", ".."), mustWork = FALSE)
}
config_path <- file.path(project_root, "_targets_config.yaml")

# Environment filters (optional)
target_platform <- Sys.getenv("MAMBA_PLATFORM", "all")
target_script   <- Sys.getenv("MAMBA_TARGET", "")
run_layer       <- Sys.getenv("MAMBA_LAYER", "both")    # etl | drv | both

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
script_to_target_name <- function(script_path) {
  base <- tools::file_path_sans_ext(basename(script_path))
  make.names(gsub("[^A-Za-z0-9_]", "_", base))
}

build_r_command <- function(full_path) {
  # Use the already-resolved project_root (from env var or fallback)
  # instead of re-deriving from relative path
  rprofile_path <- normalizePath(file.path(project_root, ".Rprofile"), winslash = "/", mustWork = FALSE)
  sc_rprofile_path <- normalizePath(
    file.path(project_root, "scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"),
    winslash = "/",
    mustWork = FALSE
  )
  if (file.exists(rprofile_path)) {
    init_source <- sprintf("source(%s)", shQuote(rprofile_path))
  } else if (file.exists(sc_rprofile_path)) {
    init_source <- sprintf("source(%s)", shQuote(sc_rprofile_path))
  } else {
    stop("Cannot find .Rprofile or sc_Rprofile.R for autoinit()")
  }
  full_path_norm <- normalizePath(full_path, winslash = "/", mustWork = FALSE)
  # Force UPDATE_MODE for ETL/DRV orchestration.
  # In `R --vanilla -e`, script-path detection falls back to APP_MODE.
  expr <- sprintf("setwd(%s); OPERATION_MODE <- 'UPDATE_MODE'; %s; autoinit(); source(%s)",
                  shQuote(project_root),
                  init_source,
                  shQuote(full_path_norm))
  c("--vanilla", "-e", shQuote(expr))
}

resolve_script_path <- function(layer_dir, platform, script_path) {
  if (grepl("[/\\\\]", script_path)) {
    file.path(layer_dir, script_path)
  } else {
    file.path(layer_dir, platform, script_path)
  }
}

run_etl_script <- function(script_path, platform) {
  full_path <- resolve_script_path(file.path(pipeline_dir, "ETL"), platform, script_path)
  r_bin <- Sys.which("R")
  if (r_bin == "") stop("R not found on PATH")
  message(sprintf("[ETL:%s] %s", platform, script_path))
  status <- system2(r_bin, build_r_command(full_path))
  if (status != 0) stop(sprintf("ETL failed (%s)", script_path))
  list(success = TRUE, script = script_path, platform = platform, layer = "etl")
}

run_drv_script <- function(script_path, platform) {
  full_path <- resolve_script_path(file.path(pipeline_dir, "DRV"), platform, script_path)
  r_bin <- Sys.which("R")
  if (r_bin == "") stop("R not found on PATH")
  message(sprintf("[DRV:%s] %s", platform, script_path))
  status <- system2(r_bin, build_r_command(full_path))
  if (status != 0) stop(sprintf("DRV failed (%s)", script_path))
  list(success = TRUE, script = script_path, platform = platform, layer = "drv")
}

create_command <- function(deps, call_str) {
  if (length(deps) == 0) {
    parse(text = call_str)[[1]]
  } else {
    dep_block <- paste(deps, collapse = "; ")
    parse(text = sprintf("{%s; %s}", dep_block, call_str))[[1]]
  }
}

# -----------------------------------------------------------------------------
# Build script definitions from config
# -----------------------------------------------------------------------------
build_definitions <- function(config, platforms) {
  defs <- list()

  add_def <- function(name, type, platform, script, deps) {
    defs[[name]] <<- list(
      name = name,
      type = type,
      platform = platform,
      script = script,
      deps = deps
    )
  }

  for (platform in platforms) {
    pc <- config$platforms[[platform]]
    if (is.null(pc)) next

    # ETL scripts
    if (!is.null(pc$etl)) {
      for (datatype in names(pc$etl)) {
        dt_conf <- pc$etl[[datatype]]
        if (is.null(dt_conf$scripts)) next
        for (script_def in dt_conf$scripts) {
          script_name <- script_def$script
          target_name <- script_to_target_name(script_name)
          deps <- character()
          if (!is.null(script_def$depends)) {
            deps <- vapply(script_def$depends, script_to_target_name, character(1))
          }
          add_def(target_name, "etl", platform, script_name, deps)
        }
      }
    }

    # DRV scripts
    if (!is.null(pc$drv)) {
      for (group_name in names(pc$drv)) {
        group_conf <- pc$drv[[group_name]]
        if (is.null(group_conf$scripts)) next
        for (script_def in group_conf$scripts) {
          script_name <- script_def$script
          target_name <- script_to_target_name(script_name)
          deps <- character()
          if (!is.null(script_def$depends_etl)) {
            deps <- c(deps, vapply(script_def$depends_etl, script_to_target_name, character(1)))
          }
          if (!is.null(script_def$depends_drv)) {
            deps <- c(deps, vapply(script_def$depends_drv, script_to_target_name, character(1)))
          }
          add_def(target_name, "drv", platform, script_name, unique(deps))
        }
      }
    }
  }

  defs
}

# -----------------------------------------------------------------------------
# Dependency closure
# -----------------------------------------------------------------------------
collect_allowed <- function(defs, start_set) {
  allowed <- character()
  queue <- start_set
  while (length(queue) > 0) {
    current <- queue[[1]]
    queue <- queue[-1]
    if (current %in% allowed) next
    allowed <- c(allowed, current)
    deps <- defs[[current]]$deps
    deps <- deps[deps %in% names(defs)]
    queue <- c(queue, deps)
  }
  allowed
}

# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
build_targets <- function() {
  config <- yaml::read_yaml(config_path)

  if (target_platform == "all") {
    platforms <- names(config$platforms)
  } else {
    requested <- strsplit(target_platform, ",")[[1]]
    platforms <- unique(c(requested, "all"))
    platforms <- platforms[platforms %in% names(config$platforms)]
  }
  defs <- build_definitions(config, platforms)

  if (length(defs) == 0) {
    stop("No targets defined for the selected platform(s).")
  }

  if (nzchar(target_script)) {
    start <- script_to_target_name(target_script)
    if (start %in% names(defs)) {
      # Exact match: single target
      start_set <- start
    } else {
      # Prefix match: e.g. "amz_D01" matches all "amz_D01_*" targets
      prefix_pattern <- paste0("^", start, "_")
      prefix_matches <- grep(prefix_pattern, names(defs), value = TRUE)
      if (length(prefix_matches) > 0) {
        message(sprintf("[Pipeline] TARGET=%s expanded to %d targets: %s",
                        target_script, length(prefix_matches),
                        paste(prefix_matches, collapse = ", ")))
        start_set <- prefix_matches
      } else {
        stop(sprintf("Requested target '%s' not found (exact or prefix) in configuration", target_script))
      }
    }
  } else {
    if (run_layer == "etl") {
      start_set <- names(defs)[vapply(defs, function(x) x$type == "etl", logical(1))]
    } else if (run_layer == "drv") {
      start_set <- names(defs)[vapply(defs, function(x) x$type == "drv", logical(1))]
    } else {
      start_set <- names(defs)
    }
  }

  allowed <- collect_allowed(defs, start_set)
  defs <- defs[allowed]

  targets <- list()
  for (def in defs) {
    deps <- def$deps
    run_call <- if (def$type == "etl") {
      sprintf("run_etl_script(%s, %s)", shQuote(def$script), shQuote(def$platform))
    } else {
      sprintf("run_drv_script(%s, %s)", shQuote(def$script), shQuote(def$platform))
    }
    command <- create_command(deps, run_call)
    target <- tar_target_raw(name = def$name, command = command)
    targets <- c(targets, list(target))
  }

  targets
}

# -----------------------------------------------------------------------------
# Target list
# -----------------------------------------------------------------------------
tar_option_set(packages = character())
build_targets()
