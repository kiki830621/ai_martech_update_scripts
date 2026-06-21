#!/usr/bin/env Rscript
#####
# DERIVATION: D01_09 Cross-Category Purchase Rate (company-level scalar)
# VERSION: 1.0
# PLATFORM: all
# GROUP: D01
# SEQUENCE: 09
# PURPOSE: Loop over active non-aggregate platforms and call run_D01_09() to
#          populate df_cross_category_rate in app_data (company-level scalar:
#          share of customers buying >= 2 real categories + 1/2/3+ distribution).
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_09_core.R
# CONSUMES: processed_data.df_{platform}_sales_by_customer_by_date
# PRODUCES: app_data.df_cross_category_rate
# DEPENDS_ON_DRV: all_D01_08  (auto-assigned by scanner generic dep logic; runs
#                 after the per-platform DNA tables are materialized)
# PRINCIPLE: MP064, MP140, MP142, MP167, MP169
#####
#all_D01_09
#
# Serial note (#1354 #815): serial 09, not 06. all_D01_06 is the D01 MASTER
# orchestrator (loops platforms running D01_00..08) and amz_D01_06 is a
# per-platform runner — both occupy 06. The fn_D01_06_core.R gap was a false
# "free serial" signal (06 is the orchestrator slot, no matching core). 09 is the
# lowest serial free in all three D01 namespaces (fn core / all runner / amz runner).
# Mirrors all_D05_01.R (cross-platform loop). Registered via config-scan glob of
# DRV/all/all_D01_*.R — run `make config-full` after adding this file.

# ---- PART 1: INITIALIZE ----
if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
autoinit()

error_occurred <- FALSE
test_passed    <- FALSE
start_time     <- Sys.time()

# Resolve platform from config (MP142: Configuration-Driven Pipeline)
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
    # Exclude aggregate pseudo-platforms (e.g. "all") — D01_09 reads individual
    # platform by-customer-by-date sources, not roll-up entries.
    setdiff(active, "all")
  }, error = function(e) "amz")
} else {
  "amz"
}
if (length(platforms) == 0) platforms <- "amz"

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D01_09_core.R")
if (!file.exists(core_path)) {
  stop(sprintf("Missing CORE_FUNCTION: %s", core_path))
}
source(core_path)

# ---- PART 2: MAIN ----
result <- NULL
tryCatch({
  # Loop over platforms — fn_D01_09_core uses write_cross_category_table()
  # to preserve other platforms' rows on each write.
  for (platform_id in platforms) {
    message(sprintf("[%s] Running D01_09 cross-category purchase rate...", platform_id))
    result      <- run_D01_09(platform_id = platform_id)
    test_passed <- isTRUE(result$success)
    if (!test_passed) {
      fail_msg <- if (!is.null(result$message)) result$message else "no message returned"
      message(sprintf("[%s] D01_09 failed — %s", platform_id, fail_msg))
      break
    }
    if (!is.null(result$data)) {
      r <- result$data
      message(sprintf(
        "[%s] cross_category_rate_pct=%s (total=%s, multi=%s; 1/2/3+ = %s/%s/%s)",
        platform_id,
        ifelse(is.na(r$cross_category_rate_pct), "-", as.character(r$cross_category_rate_pct)),
        r$n_customers_total, r$n_customers_multi,
        r$n_pl_1, r$n_pl_2, r$n_pl_3plus
      ))
    }
  }
}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ---- PART 3: TEST ----
if (!error_occurred && !test_passed) {
  message("TEST: Core function reported failure")
}

# ---- PART 4: SUMMARIZE ----
execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
message(sprintf("SUMMARY: %s", ifelse(!error_occurred && test_passed, "SUCCESS", "FAILED")))
message(sprintf("SUMMARY: Platforms: %s", paste(platforms, collapse = ", ")))
message(sprintf("SUMMARY: Execution time (secs): %.2f", execution_time))

# ---- PART 5: DEINITIALIZE ----
if (error_occurred || !test_passed) {
  autodeinit()
  quit(save = "no", status = 1)
}
autodeinit()
# NO STATEMENTS AFTER THIS LINE
