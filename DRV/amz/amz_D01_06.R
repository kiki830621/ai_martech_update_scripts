#!/usr/bin/env Rscript
#####
# DERIVATION: D01 Master Execution (AMZ)
# VERSION: 2.0
# PLATFORM: amz
# GROUP: D01
# SEQUENCE: 06
# PURPOSE: Orchestrate D01_03 through D01_05 for AMZ
# CONSUMES: transformed_data.df_amz_sales___standardized
# PRODUCES: app_data.df_profile_by_customer, app_data.df_dna_by_customer, app_data.df_segments_by_customer
# PRINCIPLE: MP064, DM_R044, DM_R022, DM_R048
#####
#amz_D01_06

#' @title D01 Master Execution (AMZ)
#' @description Orchestrate D01_03 through D01_05 for AMZ
#' @input_tables transformed_data.df_amz_sales___standardized
#' @output_tables app_data.df_profile_by_customer, app_data.df_dna_by_customer, app_data.df_segments_by_customer
#' @business_rules Orchestrate D01_03 through D01_05 for AMZ.
#' @platform amz
#' @author MAMBA Development Team
#' @date 2025-12-30


# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

autoinit()

error_occurred <- FALSE
test_passed <- FALSE
start_time <- Sys.time()
platform_id <- "amz"

rscript_path <- Sys.which("Rscript")
if (!nzchar(rscript_path)) {
  stop("Rscript not found in PATH")
}

all_script_path <- file.path(APP_DIR, "scripts", "update_scripts", "DRV", "all", "all_D01_06.R")

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  if (!file.exists(all_script_path)) {
    stop(sprintf("Missing master script: %s", all_script_path))
  }
  message(sprintf("MAIN: Running D01 master flow for %s...", platform_id))
  status <- system2(rscript_path, args = c(all_script_path, sprintf("--platforms=%s", platform_id)))
  if (!is.null(status) && status != 0) {
    stop(sprintf("Master execution failed with status %d", status))
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
    message("TEST: Master execution completed")
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
message(sprintf("SUMMARY: Platform: %s", platform_id))
message(sprintf("SUMMARY: Execution time (secs): %.2f", execution_time))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

autodeinit()
# NO STATEMENTS AFTER THIS LINE
