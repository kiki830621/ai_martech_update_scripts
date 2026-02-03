#!/usr/bin/env Rscript
#####
# DERIVATION: D01_04 Customer Profile (AMZ)
# VERSION: 2.0
# PLATFORM: amz
# GROUP: D01
# SEQUENCE: 04
# PURPOSE: Customer Profile
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_04_core.R
# CONSUMES: transformed_data.df_amz_sales___standardized
# PRODUCES: cleansed_data.df_profile_by_customer___cleansed
# DEPENDS_ON_DRV: amz_D01_03
# PRINCIPLE: MP064, MP145, DEV_R037, DEV_R038, DM_R022, DM_R044
#####
#amz_D01_04

#' @title D01_04 Customer Profile (AMZ)
#' @description Customer Profile
#' @input_tables transformed_data.df_amz_sales___standardized
#' @output_tables cleansed_data.df_profile_by_customer___cleansed
#' @business_rules Customer Profile.
#' @platform amz
#' @author QEF_DESIGN Development Team
#' @date 2026-02-02


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

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D01_04_core.R")
if (!file.exists(core_path)) {
  stop("Missing CORE_FUNCTION: global_scripts/16_derivations/fn_D01_04_core.R")
}
source(core_path)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

result <- NULL
tryCatch({
  result <- run_D01_04(platform_id = platform_id)
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
