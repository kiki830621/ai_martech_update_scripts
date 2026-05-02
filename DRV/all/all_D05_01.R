#!/usr/bin/env Rscript
#####
# DERIVATION: D05_01 Macro Monthly Summary (cross-platform loop)
# VERSION: 1.0
# PLATFORM: all
# GROUP: D05
# SEQUENCE: 01
# PURPOSE: Loop over active non-aggregate platforms and call run_D05_01()
#          to populate df_macro_monthly_summary in app_data
# CORE_FUNCTION: global_scripts/16_derivations/fn_D05_01_core.R
# CONSUMES: transformed_data.df_{platform}_sales___standardized
# PRODUCES: app_data.df_macro_monthly_summary
# DEPENDS_ON_ETL: {platform}_ETL_sales_2TS
# PRINCIPLE: MP064, MP140, MP142, DM_R044
#####
#all_D05_01

#' @title D05_01 Macro Monthly Summary (cross-platform loop)
#' @description Cross-platform loop for D05_01 monthly aggregation. Each
#'   active platform is processed sequentially. The core function uses
#'   write_platform_table_d05() to preserve other platforms' rows when
#'   writing df_macro_monthly_summary.
#' @input_tables df_{platform}_sales___standardized (transformed_data)
#' @output_tables df_macro_monthly_summary (app_data)
#' @platform all
#' @date 2026-04-13

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
    # Exclude aggregate pseudo-platforms (e.g. "all") — D05_01 processes
    # individual platform data sources, not roll-up entries
    setdiff(active, "all")
  }, error = function(e) "amz")
} else {
  "amz"
}
if (length(platforms) == 0) platforms <- "amz"

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D05_01_core.R")
if (!file.exists(core_path)) {
  stop(sprintf("Missing CORE_FUNCTION: %s", core_path))
}
source(core_path)

# Issue #416: category aggregate + excess_growth finalize (post-loop step)
finalize_path <- file.path(GLOBAL_DIR, "16_derivations",
                           "fn_D05_01_finalize_category.R")
if (!file.exists(finalize_path)) {
  stop(sprintf("Missing FINALIZE_FUNCTION: %s", finalize_path))
}
source(finalize_path)

# ---- PART 2: MAIN ----
result <- NULL
tryCatch({
  # Loop over platforms — fn_D05_01_core uses write_platform_table_d05()
  # to preserve other platforms' rows on each write (#374)
  for (platform_id in platforms) {
    message(sprintf("[%s] Running D05_01 macro monthly aggregation...", platform_id))
    result      <- run_D05_01(platform_id = platform_id)
    test_passed <- isTRUE(result$success)
    if (!test_passed) {
      fail_msg <- if (!is.null(result$message)) result$message else "no message returned"
      message(sprintf("[%s] D05_01 failed — %s", platform_id, fail_msg))
      break
    }
  }

  # Issue #416: after all per-platform brand rows are written, compute
  # category aggregate (cross-platform SUM per product_line) + excess_growth.
  if (test_passed) {
    message("[finalize] Computing category aggregate + excess_growth (#416)...")
    app_con <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
    on.exit(if (DBI::dbIsValid(app_con)) DBI::dbDisconnect(app_con, shutdown = FALSE),
            add = TRUE)
    fin_result <- finalize_D05_01_category(brand_df = NULL, app_data = app_con)
    test_passed <- isTRUE(fin_result$success)
    if (test_passed) {
      message(sprintf("[finalize] Wrote %d rows with category + excess columns",
                      fin_result$rows_written))
    } else {
      message("[finalize] FAILED — category + excess_growth columns missing")
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
