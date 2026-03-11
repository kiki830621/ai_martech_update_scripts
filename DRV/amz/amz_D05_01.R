#!/usr/bin/env Rscript
#####
# DERIVATION: D05_01 Macro Monthly Summary (AMZ)
# VERSION: 1.0
# PLATFORM: amz
# GROUP: D05
# SEQUENCE: 01
# PURPOSE: Aggregate standardized sales into monthly macro trends
# CORE_FUNCTION: global_scripts/16_derivations/fn_D05_01_core.R
# CONSUMES: transformed_data.df_amz_sales___standardized
# PRODUCES: df_macro_monthly_summary (app_data)
# DEPENDS_ON_ETL: amz_ETL_sales_2TS
# PRINCIPLE: MP064, DM_R044, MP145
#####
#amz_D05_01

#' @title D05_01 Macro Monthly Summary (AMZ)
#' @description Aggregate standardized sales into monthly macro trends:
#'   revenue, order count, active/new customers, MoM/YoY growth rates.
#' @input_tables transformed_data.df_amz_sales___standardized
#' @output_tables app_data.df_macro_monthly_summary
#' @platform amz
#' @date 2026-03-07


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

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D05_01_core.R")
if (!file.exists(core_path)) {
  stop("Missing CORE_FUNCTION: global_scripts/16_derivations/fn_D05_01_core.R")
}
source(core_path)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

result <- NULL
tryCatch({
  result <- run_D05_01(platform_id = platform_id)
  test_passed <- isTRUE(result$success)
}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred && !test_passed) {
  message("TEST: Core function reported failure")
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

# Exit with non-zero status on failure so pipeline orchestration detects errors (#341)
if (error_occurred || !test_passed) {
  message("DEINIT: Exiting with status 1 due to errors")
  autodeinit()
  quit(status = 1, save = "no")
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE
