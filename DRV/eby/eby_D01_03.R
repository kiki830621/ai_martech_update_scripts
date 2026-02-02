#!/usr/bin/env Rscript
#####
# DERIVATION: D01_03 Customer DNA Analysis (EBY)
# VERSION: 2.3
# PLATFORM: eby
# GROUP: D01
# SEQUENCE: 03
# PURPOSE: Execute DNA analysis via core function
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_03_core.R
# CONSUMES: processed_data.df_eby_customer_rfm, processed_data.df_eby_sales_by_customer_by_date
# PRODUCES: cleansed_data.df_dna_by_customer___cleansed
# DEPENDS_ON_ETL: eby_ETL_sales_2TR
# DEPENDS_ON_DRV: eby_D01_02
# PRINCIPLE: MP064, MP145, DEV_R037, DEV_R038, DM_R022, DM_R044, DM_R048
#####
#eby_D01_03

#' @title D01_03 Customer DNA Analysis (EBY)
#' @description Execute DNA analysis via core function
#' @input_tables processed_data.df_eby_customer_rfm, processed_data.df_eby_sales_by_customer_by_date
#' @output_tables cleansed_data.df_dna_by_customer___cleansed
#' @business_rules Execute DNA analysis via core function.
#' @platform eby
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
platform_id <- "eby"

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D01_03_core.R")
if (!file.exists(core_path)) {
  stop("Missing CORE_FUNCTION: global_scripts/16_derivations/fn_D01_03_core.R")
}
source(core_path)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

result <- NULL
tryCatch({
  result <- run_D01_03(platform_id = platform_id)
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

autodeinit()
# NO STATEMENTS AFTER THIS LINE
