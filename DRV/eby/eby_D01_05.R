#!/usr/bin/env Rscript
#####
# DERIVATION: D01_05 Application Views (EBY)
# VERSION: 2.1
# PLATFORM: eby
# GROUP: D01
# SEQUENCE: 05
# PURPOSE: Normalize cleansed outputs via core function
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_05_core.R
# CONSUMES: cleansed_data.df_customer_dna___cleansed,
#           cleansed_data.df_customer_profile___cleansed
# PRODUCES: app_data.df_customer_dna,
#           app_data.df_customer_profile,
#           app_data.df_customer_segments,
#           app_data.v_customer_dna_analytics,
#           app_data.v_customer_segments,
#           app_data.v_segment_statistics
# DEPENDS_ON: D00_app_data_init
# PRINCIPLE: MP064, MP144, DEV_R037, DEV_R038, DM_R022, DM_R044, DM_R048
#####
#eby_D01_05

#' @title D01_05 Application Views (EBY)
#' @description Normalize cleansed outputs via core function
#' @input_tables cleansed_data.df_customer_dna___cleansed,
#' @output_tables app_data.df_customer_dna,
#' @business_rules Normalize cleansed outputs via core function.
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

autodeinit()
# NO STATEMENTS AFTER THIS LINE
