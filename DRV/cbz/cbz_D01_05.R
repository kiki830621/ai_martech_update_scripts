#!/usr/bin/env Rscript
#####
# DERIVATION: D01_05 Final Validation (CBZ)
# VERSION: 3.0
# PLATFORM: cbz
# GROUP: D01
# SEQUENCE: 05
# PURPOSE: Validate D01 outputs via core function
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_05_core.R
# CONSUMES: app_data.df_dna_by_customer,
#           app_data.df_profile_by_customer,
#           app_data.df_segments_by_customer
# PRODUCES: validation_result
# DEPENDS_ON_DRV: cbz_D01_04
# PRINCIPLE: MP064, MP144, DEV_R037, DEV_R038, DM_R022, DM_R044, DM_R048
#####
#cbz_D01_05

#' @title D01_05 Final Validation (CBZ)
#' @description Validate D01 outputs via core function
#' @input_tables app_data.df_dna_by_customer, app_data.df_profile_by_customer, app_data.df_segments_by_customer
#' @output_tables validation_result
#' @business_rules Validate app-facing D01 contracts.
#' @platform cbz
#' @author MAMBA Development Team
#' @date 2026-02-08


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
platform_id <- "cbz"

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D01_05_core.R")
if (!file.exists(core_path)) {
  stop("Missing CORE_FUNCTION: global_scripts/16_derivations/fn_D01_05_core.R")
}
source(core_path)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

result <- NULL
tryCatch({
  result <- run_D01_05(platform_id = platform_id)
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

if (error_occurred || !test_passed) {
  autodeinit()
  quit(save = "no", status = 1)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE
