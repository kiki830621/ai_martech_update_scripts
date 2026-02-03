#!/usr/bin/env Rscript
#####
# DERIVATION: D01_06 Master Execution (AMZ)
# VERSION: 2.0
# PLATFORM: amz
# GROUP: D01
# SEQUENCE: 06
# PURPOSE: Orchestrate D01_00 through D01_05 for amz platform
# PRINCIPLE: MP064, DM_R044
#####
#amz_D01_06

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

OPERATION_MODE <- "UPDATE_MODE"
autoinit()

error_occurred <- FALSE
test_passed <- FALSE
start_time <- Sys.time()
platform_id <- "amz"

rscript_path <- Sys.which("Rscript")
if (!nzchar(rscript_path)) stop("Rscript not found in PATH")

drv_dir <- file.path(APP_DIR, "scripts", "update_scripts", "DRV", platform_id)

run_script <- function(script_name) {
  script_path <- file.path(drv_dir, script_name)
  if (!file.exists(script_path)) {
    stop(sprintf("Missing script: %s", script_path))
  }
  message(sprintf("[%s] Running %s...", platform_id, script_name))
  status <- system2(rscript_path, args = c(script_path))
  if (!is.null(status) && status != 0) {
    stop(sprintf("[%s] %s failed with status %d", platform_id, script_name, status))
  }
}

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  message("D01_06: Starting master execution for amz")
  run_script(sprintf("%s_D01_00.R", platform_id))
  run_script(sprintf("%s_D01_01.R", platform_id))
  run_script(sprintf("%s_D01_02.R", platform_id))
  run_script(sprintf("%s_D01_03.R", platform_id))
  run_script(sprintf("%s_D01_04.R", platform_id))
  run_script(sprintf("%s_D01_05.R", platform_id))
}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred) {
  test_passed <- TRUE
  message("TEST: All D01 steps completed without errors")
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
message("SUMMARY: ", ifelse(!error_occurred && test_passed, "SUCCESS", "FAILED"))
message(sprintf("SUMMARY: Platform: %s", platform_id))
message(sprintf("SUMMARY: Execution time (secs): %.2f", execution_time))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

autodeinit()
# NO STATEMENTS AFTER THIS LINE
