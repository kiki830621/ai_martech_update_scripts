#!/usr/bin/env Rscript
#####
#P09_D04_06
# DERIVATION: Cross-Platform Feature Preparation (Wrapper)
# VERSION: 1.1
# PLATFORM: all
# GROUP: D04
# SEQUENCE: 06
# PURPOSE: Wrapper entry point for feature preparation (delegates to all_D04_09.R)
# CONSUMES: (via all_D04_09.R)
# PRODUCES: processed_data.df_precision_features
# PRINCIPLE: DM_R044, MP064
#####

#' @title ALL D04_06 Wrapper - Feature Preparation
#' @description Wrapper entry point that executes all_D04_09.R.
#' @requires base
#' @input_tables transformed_data.duckdb (ETL 2TR stage)
#' @output_tables processed_data.df_precision_features
#' @business_rules Wrapper only; delegates execution to all_D04_09.R.
#' @platform all
#' @author MAMBA Development Team
#' @date 2025-12-30

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

error_occurred <- FALSE
test_passed <- FALSE
start_time <- Sys.time()

# ==============================================================================
# PART 2: MAIN
# ==============================================================================
tryCatch({
  message("Running all_D04_09.R via wrapper (D04_06)...")
  exit_status <- system2("Rscript", "scripts/update_scripts/DRV/all/all_D04_09.R")
  if (!is.null(exit_status) && exit_status != 0) {
    stop(sprintf("all_D04_09.R exited with status %s", exit_status))
  }
  test_passed <- TRUE
}, error = function(e) {
  message("ERROR in MAIN: ", e$message)
  error_occurred <<- TRUE
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================
if (!error_occurred) {
  message("Wrapper completed without errors.")
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================
message("DERIVATION SUMMARY")
message(sprintf("Script: %s", "all_D04_06.R"))
message(sprintf("Status: %s", ifelse(test_passed && !error_occurred, "SUCCESS", "FAILED")))
message(sprintf("Execution Time: %.2f seconds", as.numeric(difftime(Sys.time(), start_time, units = "secs"))))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================
autodeinit()
# End of file
