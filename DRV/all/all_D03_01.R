#!/usr/bin/env Rscript
#####
# DERIVATION: D03_01 Geographic Sales Aggregation
# VERSION: 1.0
# PLATFORM: all
# GROUP: D03
# SEQUENCE: 01
# PURPOSE: Aggregate sales by country for world market map visualization
# CORE_FUNCTION: global_scripts/16_derivations/fn_D03_01_core.R
# CONSUMES: transformed_data.df_{platform}_sales___standardized
# PRODUCES: app_data.df_geo_sales_by_country, app_data.df_customer_country_map, app_data.df_geo_sales_by_state
# DEPENDS_ON_ETL: {platform}_ETL_sales_2TS
# PRINCIPLE: MP064, MP029, MP140, DM_R044
#####
#all_D03_01

#' @title Geographic Sales Aggregation
#' @description Aggregates sales data by country and product line for world map visualization
#' @input_tables df_{platform}_sales___standardized (transformed_data)
#' @output_tables df_geo_sales_by_country, df_customer_country_map, df_geo_sales_by_state (app_data)
#' @business_rules Shipped orders only, group by country + product_line, plus 'all' roll-up
#' @platform all
#' @author MAMBA Development Team
#' @date 2026-03-05

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
    # Exclude aggregate pseudo-platforms (e.g. "all") — D03_01 processes
    # individual platform data sources, not roll-up entries
    setdiff(active, "all")
  }, error = function(e) "amz")
} else {
  "amz"
}
if (length(platforms) == 0) platforms <- "amz"

core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D03_01_core.R")
if (!file.exists(core_path)) {
  stop(sprintf("Missing CORE_FUNCTION: %s", core_path))
}
source(core_path)

# ---- PART 2: MAIN ----
result <- NULL
tryCatch({
  # NOTE: run_D03_01() writes to df_geo_sales_by_country with overwrite=TRUE.

  # With multiple platforms, only the last platform's data survives.
  # This is acceptable for single-platform companies (e.g., D_RACING/amz).
  if (length(platforms) > 1) {
    stop(sprintf(
      "D03_01: %d platforms detected (%s). Core function writes to shared tables with overwrite=TRUE — only the last platform's data would survive. Multi-platform support requires platform-scoped table names. Aborting.",
      length(platforms), paste(platforms, collapse = ", ")
    ))
  }
  for (platform_id in platforms) {
    message(sprintf("[%s] Running D03_01 geographic aggregation...", platform_id))
    result      <- run_D03_01(platform_id = platform_id)
    test_passed <- isTRUE(result$success)
    if (!test_passed) {
      message(sprintf("[%s] D03_01 failed", platform_id))
      break
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
