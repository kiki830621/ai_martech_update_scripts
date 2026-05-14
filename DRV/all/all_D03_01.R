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
#           (country + state tables include platform_id='all' cross-platform rollup rows, #417 MP055)
# DEPENDS_ON_ETL: {platform}_ETL_sales_2TS
# PRINCIPLE: MP064, MP029, MP140, MP055, DM_R044
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

# Resolve per-platform column_spec from app_config (Refs #671).
# Loads `app_config.yaml > derivations > D03_01 > column_spec > <platform>` mapping.
# When missing, falls back to `default_amz_column_spec()` (legacy backward-compat for amz).
# Returns named list: platform_id → column_spec (list with order_id/revenue/country/state/quantity).
resolve_d03_column_spec_map <- function() {
  project_root <- if (exists("MAMBA_PROJECT_ROOT", inherits = TRUE) &&
                       nzchar(Sys.getenv("MAMBA_PROJECT_ROOT", unset = ""))) {
    Sys.getenv("MAMBA_PROJECT_ROOT")
  } else if (exists("MAMBA_PROJECT_ROOT", inherits = TRUE)) {
    get("MAMBA_PROJECT_ROOT", inherits = TRUE)
  } else {
    getwd()
  }
  app_cfg_path <- file.path(project_root, "app_config.yaml")
  if (!requireNamespace("yaml", quietly = TRUE) || !file.exists(app_cfg_path)) {
    return(list())  # no override; callers fall back to default_amz_column_spec()
  }
  cfg <- tryCatch(yaml::read_yaml(app_cfg_path), error = function(e) NULL)
  if (is.null(cfg)) return(list())
  override <- cfg$derivations$D03_01$column_spec
  if (is.null(override) || !is.list(override)) return(list())
  override
}

column_spec_map <- resolve_d03_column_spec_map()

# ---- PART 2: MAIN ----
result <- NULL
tryCatch({
  # NOTE (#374): Multi-platform now safe — fn_D03_01_core.R uses
  # write_platform_table_d03() which preserves rows for other platforms when
  # called sequentially. Previous abort guard removed (#371 blocker 9 → #374).
  # Per-platform tryCatch (Refs #671): a single platform failing graceful-skip
  # (missing column_spec, missing source table) must NOT abort sibling platforms.
  # test_passed = TRUE iff AT LEAST ONE platform produced output successfully.
  any_platform_succeeded <- FALSE
  for (platform_id in platforms) {
    spec <- column_spec_map[[platform_id]]
    if (is.null(spec)) {
      message(sprintf("[%s] No app_config derivations.D03_01.column_spec — using default_amz_column_spec() (Refs #671 backward-compat)", platform_id))
      spec <- default_amz_column_spec()
    }
    message(sprintf("[%s] Running D03_01 geographic aggregation...", platform_id))
    result <- tryCatch(
      run_D03_01(platform_id = platform_id, config = NULL, column_spec = spec),
      error = function(e) {
        message(sprintf("[%s] D03_01 error (continuing with other platforms): %s",
                        platform_id, e$message))
        list(success = FALSE, platform_id = platform_id,
             rows_processed = 0L, outputs = character(0))
      }
    )
    if (isTRUE(result$success)) {
      any_platform_succeeded <- TRUE
    } else {
      message(sprintf("[%s] D03_01 did not succeed (success=%s); continuing",
                      platform_id, isTRUE(result$success)))
    }
  }
  test_passed <- any_platform_succeeded

  # #417: after per-platform rows are in place, produce the cross-platform
  # platform_id='all' rollup so UI components that default to platform=all
  # (e.g. VitalSigns > 全球戰情室 worldMap) find data instead of rendering
  # blank. Per MP055 Special Treatment of 'ALL' Category.
  if (test_passed) {
    db_util_path <- file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R")
    if (file.exists(db_util_path)) source(db_util_path)
    agg_opened_here <- FALSE
    if (!exists("app_data") || !inherits(app_data, "DBIConnection") || !DBI::dbIsValid(app_data)) {
      app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
      agg_opened_here <- TRUE
    }
    message("[all_D03_01] Aggregating platform_id='all' rollup rows (#417)...")
    agg_ok <- aggregate_D03_01_all_platforms(app_data)
    if (!isTRUE(agg_ok)) {
      message("[all_D03_01] WARNING: platform='all' rollup returned FALSE; continuing")
    }
    if (agg_opened_here && DBI::dbIsValid(app_data)) {
      DBI::dbDisconnect(app_data, shutdown = FALSE)
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
