#!/usr/bin/env Rscript
#####
#P09_D04_04
# DERIVATION: Cross-Platform Time Series Expansion (Wrapper)
# VERSION: 1.1
# PLATFORM: all
# GROUP: D04
# SEQUENCE: 04
# PURPOSE: Wrapper entry point for time series expansion (delegates to all_D04_07.R)
# CONSUMES: (via all_D04_07.R)
# PRODUCES: processed_data.df_precision_time_series
# PRINCIPLE: DM_R044, MP064
#####

#' @title ALL D04_04 Wrapper - Time Series Expansion
#' @description Wrapper entry point that executes all_D04_07.R.
#' @requires base
#' @input_tables transformed_data.duckdb (from ETL 2TR stage)
#' @output_tables processed_data.df_precision_time_series
#' @business_rules Wrapper only; delegates execution to all_D04_07.R.
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
  message("Running all_D04_07.R via wrapper (D04_04)...")
  exit_status <- system2("Rscript", "scripts/update_scripts/DRV/all/all_D04_07.R")
  if (!is.null(exit_status) && exit_status != 0) {
    stop(sprintf("all_D04_07.R exited with status %s", exit_status))
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
message(sprintf("Script: %s", "all_D04_04.R"))
message(sprintf("Status: %s", ifelse(test_passed && !error_occurred, "SUCCESS", "FAILED")))
message(sprintf("Execution Time: %.2f seconds", as.numeric(difftime(Sys.time(), start_time, units = "secs"))))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================
autodeinit()
# End of file
