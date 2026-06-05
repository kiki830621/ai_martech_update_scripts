#!/usr/bin/env Rscript
#####
# DERIVATION: D05_03 Rolling NES Snapshot (cross-platform loop)
# VERSION: 1.0
# PLATFORM: all
# GROUP: D05
# SEQUENCE: 03
# PURPOSE: Re-run analysis_dna() at 4 as-of cutoffs (now + lag-1m/1q/1y) to
#          populate df_dna_snapshots_by_period in app_data (per-customer x
#          period NES snapshots → true S->E0 awakening rate + NES transition)
# CORE_FUNCTION: global_scripts/16_derivations/fn_D05_03_core.R
# CONSUMES: processed_data.df_{platform}_sales_by_customer_by_date
# PRODUCES: app_data.df_dna_snapshots_by_period
# DEPENDS_ON_DRV: D01_01
# PRINCIPLE: MP064, MP140, MP142, MP167, DM_R069
#####
#all_D05_03

#' @title D05_03 Rolling NES Snapshot (cross-platform loop)
#' @description Builds per-customer x period NES snapshots by re-running the
#'   unchanged analysis_dna() at 4 as-of cutoffs. Inputs re-aggregated from the
#'   date<=cutoff slice (Guard 1); empty periods skipped (Guard 2); params
#'   refreshed per run (Guard 3). run_D05_03 loops platforms + 'all' rollup
#'   internally, so this wrapper resolves the active platform list and calls it
#'   once.
#' @input_tables df_{platform}_sales_by_customer_by_date (processed_data)
#' @output_tables df_dna_snapshots_by_period (app_data)
#' @platform all
#' @date 2026-06-05

# ---- PART 1: INITIALIZE ----
if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
autoinit()

error_occurred <- FALSE
test_passed    <- FALSE
start_time     <- Sys.time()

# Resolve platforms from config (MP142: Configuration-Driven Pipeline)
config_fn <- file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R")
if (file.exists(config_fn)) source(config_fn)

platforms <- if (exists("get_platform_config", mode = "function")) {
  tryCatch({
    pc <- get_platform_config()
    active <- names(pc)[vapply(pc, function(e) {
      is.list(e) &&
        (is.null(e$status) || tolower(e$status) == "active") &&
        (is.null(e$enabled) || isTRUE(e$enabled))
    }, logical(1))]
    setdiff(active, "all")  # 'all' rollup is computed inside run_D05_03
  }, error = function(e) "amz")
} else {
  "amz"
}
if (length(platforms) == 0) platforms <- "amz"

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D05_03_core.R")
if (!file.exists(core_path)) {
  stop(sprintf("Missing CORE_FUNCTION: %s", core_path))
}
source(core_path)

# ---- PART 2: MAIN ----
result <- NULL
tryCatch({
  message(sprintf("[D05_03] Running rolling NES snapshot for platforms: %s",
                  paste(platforms, collapse = ", ")))
  result      <- run_D05_03(platforms = platforms)
  test_passed <- isTRUE(result)
}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ---- PART 3: TEST ----
if (!error_occurred && !test_passed) {
  message("TEST: run_D05_03 reported no snapshots produced")
}

# ---- PART 4: SUMMARIZE ----
execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
message(sprintf("SUMMARY: %s", ifelse(!error_occurred && test_passed, "SUCCESS", "FAILED")))
message(sprintf("SUMMARY: Platforms: %s", paste(platforms, collapse = ", ")))
message(sprintf("SUMMARY: Execution time (secs): %.2f", execution_time))

# ---- PART 5: DEINITIALIZE ----
if (exists("autodeinit", mode = "function")) autodeinit()
