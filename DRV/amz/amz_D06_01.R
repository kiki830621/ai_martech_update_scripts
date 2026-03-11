#!/usr/bin/env Rscript
#####
# DERIVATION: D06_01 Product Recommendation (AMZ)
# VERSION: 1.0
# PLATFORM: amz
# GROUP: D06
# SEQUENCE: 01
# PURPOSE: Generate personalized product recommendations using RF + conditional probability
# CORE_FUNCTION: global_scripts/16_derivations/fn_D06_01_core.R
# CONSUMES: transformed_data.df_amz_sales___standardized
# PRODUCES: app_data.df_product_recommendations
# DEPENDS_ON_ETL: amz_ETL_sales_2TS
# DEPENDS_ON_DRV: (none)
# PRINCIPLE: MP064, MP029, DEV_R052, DM_R044
#####
#amz_D06_01

#' @title D06_01 Product Recommendation (AMZ)
#' @description Generate personalized product recommendations using Random Forest
#'   for first recommendation and conditional probability transition matrix for
#'   subsequent recommendations.
#' @input_tables transformed_data.df_amz_sales___standardized
#' @output_tables app_data.df_product_recommendations
#' @business_rules RF predicts 1st product; conditional probability chain for 2nd-5th products.
#' @platform amz
#' @author D_RACING Development Team
#' @date 2026-03-11


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

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D06_01_core.R")
if (!file.exists(core_path)) {
  stop("Missing CORE_FUNCTION: global_scripts/16_derivations/fn_D06_01_core.R")
}
source(core_path)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

result <- NULL
tryCatch({
  result <- run_D06_01(platform_id = platform_id)
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
