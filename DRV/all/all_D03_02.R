#!/usr/bin/env Rscript
#####
# DERIVATION: D03_02 Market-Entry Analysis
# VERSION: 1.0
# PLATFORM: all
# GROUP: D03
# SEQUENCE: 02
# PURPOSE: Per product line, build competitor benchmarking + competitor-coefficient
#          model (for product lines a company has not launched / no own sales).
# CORE_FUNCTION: global_scripts/16_derivations/fn_D03_02_core.R
# CONSUMES: app_data.df_position
# PRODUCES: app_data.df_market_entry_analysis
# DEPENDS_ON_DRV: D03_01 (sibling positioning; df_position produced upstream)
# PRINCIPLE: MP064, MP029, MP140, MP153, DM_R044, DM_R023
#####
#all_D03_02

#' @title Market-Entry Analysis
#' @description Per product_line_id, competitor attribute benchmarking (C, always)
#'   + competitor-coefficient model coefficients (A, when estimable). The
#'   pre-launch attractiveness percentile is computed at UI time (needs a planned
#'   product profile), so this DRV stores model + benchmarking + estimability only.
#' @input_tables df_position (app_data)
#' @output_tables df_market_entry_analysis (app_data)
#' @business_rules Competitor rows only (df_position is the competitor universe);
#'   withhold (A) rather than over-fit small / homogeneous markets (MP029).
#' @platform all
#' @author MAMBA Development Team
#' @date 2026-06-05

# ---- PART 1: INITIALIZE ----
if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
autoinit()

error_occurred <- FALSE
test_passed    <- FALSE
start_time     <- Sys.time()

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D03_02_core.R")
model_path <- file.path(GLOBAL_DIR, "04_utils", "fn_estimate_market_entry.R")
if (!file.exists(core_path)) stop(sprintf("Missing CORE_FUNCTION: %s", core_path))
if (file.exists(model_path)) source(model_path)   # estimate_market_entry()
source(core_path)                                  # build_market_entry_analysis(), run_D03_02()

# ---- PART 2: MAIN ----
result <- NULL
tryCatch({
  message("[all_D03_02] Building market-entry analysis from df_position...")
  result <- run_D03_02(db_path_list)
  # run_D03_02 returns the built data.frame (or NULL if df_position absent).
  # Success = table written with >= 0 rows (an empty result is a legitimate
  # no-competitor-data state, not a failure — the table still materializes).
  test_passed <- !is.null(result)
}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ---- PART 3: TEST ----
if (!error_occurred && !test_passed) {
  message("TEST: run_D03_02 returned NULL (df_position not found) — no table written")
}

# ---- PART 4: SUMMARIZE ----
execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
if (!is.null(result)) {
  message(sprintf("SUMMARY: df_market_entry_analysis rows: %d", nrow(result)))
}
message(sprintf("SUMMARY: %s", ifelse(!error_occurred && test_passed, "SUCCESS", "FAILED")))
message(sprintf("SUMMARY: Execution time (secs): %.2f", execution_time))

# ---- PART 5: DEINITIALIZE ----
if (error_occurred) {
  autodeinit()
  quit(save = "no", status = 1)
}
autodeinit()
# NO STATEMENTS AFTER THIS LINE
