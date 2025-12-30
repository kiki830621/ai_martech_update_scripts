#!/usr/bin/env Rscript
#####
#P07_D04_06
# DERIVATION: CBZ App Data Publishing (Wrapper)
# VERSION: 1.1
# PLATFORM: cbz
# GROUP: D04
# SEQUENCE: 06
# PURPOSE: Wrapper entry point for app data publishing (delegates to cbz_D04_02.R)
# CONSUMES: (via cbz_D04_02.R)
# PRODUCES: app_data.df_cbz_poisson_analysis_{product_line}, app_data.df_cbz_poisson_analysis_all
# PRINCIPLE: DM_R044, MP064
#####

#' @title CBZ D04_06 Wrapper - App Data Publishing
#' @description Wrapper entry point that executes cbz_D04_02.R.
#' @requires base
#' @input_tables df_cbz_sales_complete_time_series_{product_line} (app_data.duckdb)
#' @output_tables app_data.df_cbz_poisson_analysis_{product_line}, app_data.df_cbz_poisson_analysis_all
#' @business_rules Wrapper only; delegates execution to cbz_D04_02.R.
#' @platform cbz
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
  message("Running cbz_D04_02.R via wrapper (D04_06)...")
  exit_status <- system2("Rscript", "scripts/update_scripts/DRV/cbz/cbz_D04_02.R")
  if (!is.null(exit_status) && exit_status != 0) {
    stop(sprintf("cbz_D04_02.R exited with status %s", exit_status))
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
message(sprintf("Script: %s", "cbz_D04_06.R"))
message(sprintf("Status: %s", ifelse(test_passed && !error_occurred, "SUCCESS", "FAILED")))
message(sprintf("Execution Time: %.2f seconds", as.numeric(difftime(Sys.time(), start_time, units = "secs"))))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================
autodeinit()
# End of file
