#!/usr/bin/env Rscript
#####
# DERIVATION: D11_01 Product Master UNION (AMZ)
# VERSION: 1.0
# PLATFORM: amz
# GROUP: D11
# SEQUENCE: 01
# PURPOSE: UNION 10 product_attributes raw tables → app_data.df_product_master
# CORE_FUNCTION: global_scripts/16_derivations/fn_D11_01_core.R
# CONSUMES: raw_data.df_amz_product_attributes_*___raw (10 PLs from bridge orchestrator)
# PRODUCES: app_data.df_product_master
# DEPENDS_ON_ETL: amz_ETL_product_attributes_0IM (#569)
# DEPENDS_ON_DRV: (none)
# PRINCIPLE: MP064, MP102 v1.3, MP161, MP163, DM_R023 v1.2, DEV_R038
#####
# amz_D11_01
#
# Closes the bridge → DRV gap from #573:
# - Bridge orchestrator (#569) writes per-PL canonical raw tables
# - This DRV unifies them into single app_data.df_product_master VIEW
# - Downstream apps consume single canonical table instead of 10 per-PL tables

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

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D11_01_core.R")
if (!file.exists(core_path)) {
  stop("Missing CORE_FUNCTION: global_scripts/16_derivations/fn_D11_01_core.R")
}
source(core_path)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

result <- NULL
tryCatch({
  result <- run_D11_01(platform_id = platform_id)
  test_passed <- isTRUE(result$test_passed)
}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred) {
  if (test_passed) {
    message(sprintf("TEST: PASSED — %d rows from %d product lines written to %s",
                    result$n_rows, result$n_product_lines, result$target_table))
  } else {
    message("TEST: Core function reported failure (test_passed=FALSE)")
  }
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
message("SUMMARY: ", ifelse(!error_occurred && test_passed, "SUCCESS", "FAILED"))
message(sprintf("SUMMARY: Platform: %s", platform_id))
if (!is.null(result)) {
  message(sprintf("SUMMARY: Rows: %d | Product lines: %d",
                  result$n_rows %||% 0, result$n_product_lines %||% 0))
}
message(sprintf("SUMMARY: Execution time (secs): %.2f", execution_time))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

autodeinit()
# NO STATEMENTS AFTER THIS LINE
