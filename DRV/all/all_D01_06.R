#!/usr/bin/env Rscript
#####
# DERIVATION: D01 Master Execution (All Platforms)
# VERSION: 2.0
# PLATFORM: all
# GROUP: D01
# SEQUENCE: 06
# PURPOSE: Orchestrate D01_00 through D01_08 across platforms
# CONSUMES: transformed_data.df_{platform}_sales___standardized
# PRODUCES: app_data.df_profile_by_customer, app_data.df_dna_by_customer, app_data.df_segments_by_customer, app_data.df_rsv_classified
# PRINCIPLE: MP064, DM_R044, DM_R022, DM_R048
#####
#all_D01_06

#' @title D01 Master Execution (All Platforms)
#' @description Orchestrate D01_00 through D01_08 across platforms
#' @input_tables transformed_data.df_{platform}_sales___standardized
#' @output_tables app_data.df_profile_by_customer, app_data.df_dna_by_customer, app_data.df_segments_by_customer, app_data.df_rsv_classified
#' @business_rules Orchestrate D01_00 through D01_08 across platforms.
#' @platform all
#' @author MAMBA Development Team
#' @date 2025-12-30


# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

autoinit()

resolve_default_platforms <- function() {
  if (!exists("get_platform_config", mode = "function", inherits = TRUE)) {
    config_fn <- file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R")
    if (file.exists(config_fn)) {
      source(config_fn)
    }
  }

  platforms <- NULL
  if (exists("get_platform_config", mode = "function", inherits = TRUE)) {
    platforms <- tryCatch(get_platform_config(), error = function(e) NULL)
  }

  if (!is.list(platforms) || length(platforms) == 0) {
    return(c("cbz", "eby"))
  }

  platform_ids <- names(platforms)
  if (is.null(platform_ids) || !any(nzchar(platform_ids))) {
    platform_ids <- vapply(
      platforms,
      function(entry) {
        if (is.list(entry) && !is.null(entry$platform_id)) {
          return(as.character(entry$platform_id))
        }
        ""
      },
      character(1)
    )
    platform_ids <- platform_ids[nzchar(platform_ids)]
    if (length(platform_ids) == 0) {
      return(c("cbz", "eby"))
    }
    names(platforms) <- platform_ids
  }

  is_active <- function(entry) {
    if (!is.list(entry)) return(TRUE)
    status <- entry$status
    if (!is.null(status) && tolower(as.character(status)) != "active") return(FALSE)
    enabled <- entry$enabled
    if (!is.null(enabled) && !isTRUE(enabled)) return(FALSE)
    TRUE
  }

  active_platforms <- platform_ids[vapply(platforms[platform_ids], is_active, logical(1))]
  if (length(active_platforms) == 0) active_platforms <- platform_ids
  active_platforms
}

DEFAULT_PLATFORMS <- resolve_default_platforms()

drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
drv_script_name <- "all_D01_06"
start_time <- Sys.time()

error_occurred <- FALSE
test_passed <- FALSE

parse_platforms <- function(args, default_platforms) {
  platforms_arg <- NULL
  for (idx in seq_along(args)) {
    if (args[idx] %in% c("--platforms", "--platform") && idx < length(args)) {
      platforms_arg <- args[idx + 1]
      break
    }
    if (grepl("^--platforms=", args[idx])) {
      platforms_arg <- sub("^--platforms=", "", args[idx])
      break
    }
  }
  if (is.null(platforms_arg)) {
    env_platforms <- Sys.getenv("D01_PLATFORMS", "")
    if (nzchar(env_platforms)) {
      platforms_arg <- env_platforms
    }
  }
  if (is.null(platforms_arg) || platforms_arg == "" || platforms_arg == "all") {
    return(default_platforms)
  }
  platforms <- strsplit(platforms_arg, ",", fixed = TRUE)[[1]]
  platforms <- trimws(platforms)
  platforms <- platforms[nzchar(platforms)]
  if (length(platforms) == 0) {
    return(default_platforms)
  }
  platforms
}

platforms <- parse_platforms(commandArgs(trailingOnly = TRUE), DEFAULT_PLATFORMS)

rscript_path <- Sys.which("Rscript")
if (!nzchar(rscript_path)) {
  stop("Rscript not found in PATH")
}

script_map <- list(
  d01_00 = function(platform) file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform, sprintf("%s_D01_00.R", platform)),
  d01_01 = function(platform) file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform, sprintf("%s_D01_01.R", platform)),
  d01_02 = function(platform) file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform, sprintf("%s_D01_02.R", platform)),
  d01_03 = function(platform) file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform, sprintf("%s_D01_03.R", platform)),
  d01_04 = function(platform) file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform, sprintf("%s_D01_04.R", platform)),
  d01_05 = function(platform) file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform, sprintf("%s_D01_05.R", platform)),
  d01_07 = function(platform) file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform, sprintf("%s_D01_07_product_line_coverage_audit.R", platform))
)

run_script <- function(script_path, step_name, platform_id) {
  if (!file.exists(script_path)) {
    stop(sprintf("Missing %s script for %s: %s", step_name, platform_id, script_path))
  }
  message(sprintf("[%s] Running %s...", platform_id, step_name))
  status <- system2(rscript_path, args = c(script_path))
  if (!is.null(status) && status != 0) {
    stop(sprintf("[%s] %s failed with status %d", platform_id, step_name, status))
  }
}

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  message("D01_06: Starting master execution")
  message("Platforms: ", paste(platforms, collapse = ", "))

  for (platform_id in platforms) {
    run_script(script_map$d01_00(platform_id), "D01_00", platform_id)
    run_script(script_map$d01_01(platform_id), "D01_01", platform_id)
    run_script(script_map$d01_02(platform_id), "D01_02", platform_id)
    run_script(script_map$d01_03(platform_id), "D01_03", platform_id)
    run_script(script_map$d01_04(platform_id), "D01_04", platform_id)
    run_script(script_map$d01_05(platform_id), "D01_05", platform_id)

    audit_script <- script_map$d01_07(platform_id)
    if (file.exists(audit_script)) {
      run_script(audit_script, "D01_07", platform_id)
    } else {
      message(sprintf("[%s] D01_07 script not found, skipping coverage audit", platform_id))
    }
  }

  # D01_08: RSV Pre-classification (cross-platform, runs once after all platforms)
  # Forward platform list so D01_08 processes the same platforms as this master script
  Sys.setenv(D01_PLATFORMS = paste(platforms, collapse = ","))
  d01_08_script <- file.path(APP_DIR, "scripts", "update_scripts", "DRV", "all", "all_D01_08.R")
  if (file.exists(d01_08_script)) {
    run_script(d01_08_script, "D01_08", "all")
  } else {
    stop("[all] D01_08 script not found: ", d01_08_script,
         ". RSV pre-classification is a required pipeline step.")
  }

}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred) {
  tryCatch({
    test_passed <- TRUE
    message("TEST: Execution completed without script errors")
  }, error = function(e) {
    test_passed <<- FALSE
    message(sprintf("TEST: ERROR - %s", e$message))
  })
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
message("SUMMARY: ", ifelse(!error_occurred && test_passed, "SUCCESS", "FAILED"))
message(sprintf("SUMMARY: Platforms: %s", paste(platforms, collapse = ", ")))
message(sprintf("SUMMARY: Execution time (secs): %.2f", execution_time))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

if (error_occurred || !test_passed) {
  autodeinit()
  quit(save = "no", status = 1)
}
autodeinit()
# NO STATEMENTS AFTER THIS LINE
