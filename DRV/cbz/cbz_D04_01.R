#!/usr/bin/env Rscript
#####
#P07_D04_01
# DERIVATION: CBZ Poisson Time Label Enrichment
# VERSION: 1.0
# PLATFORM: cbz
# GROUP: D04
# SEQUENCE: 01
# PURPOSE: Add hierarchical time labels to Poisson analysis outputs
# CONSUMES: app_data.df_cbz_poisson_analysis_all, raw_data.df_cbz_orders___raw
# PRODUCES: app_data.df_cbz_poisson_analysis_all, app_data.df_cbz_poisson_analysis_all_backup_YYYYMMDD
# PRINCIPLE: DM_R044, MP064, R120
#####

#' @title CBZ Poisson Time Label Enrichment
#' @description Add year/month/weekday labels to Poisson analysis outputs for UI display.
#' @requires DBI, duckdb, dplyr, lubridate
#' @input_tables app_data.df_cbz_poisson_analysis_all, raw_data.df_cbz_orders___raw
#' @output_tables app_data.df_cbz_poisson_analysis_all, app_data.df_cbz_poisson_analysis_all_backup_YYYYMMDD
#' @business_rules Derive year/month/weekday labels from raw orders; overwrite app_data with backup.
#' @platform cbz
#' @author MAMBA Development Team
#' @date 2025-12-30

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
tbl2_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R")
)
tbl2_path <- tbl2_candidates[file.exists(tbl2_candidates)][1]
if (is.na(tbl2_path)) {
  stop("fn_tbl2.R not found in expected paths")
}
source(tbl2_path)
message("=== cbz_DER_poisson_time_labels.R ===")
message("Starting Poisson time label enrichment")

# Initialize environment
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()
if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before connections.")
}
library(dplyr)
library(lubridate)
error_occurred <- FALSE
test_passed <- FALSE
start_time <- Sys.time()

# Connect to app database (UI output lives in app_data)
con_app <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
message("Connected to app_data database")

# Connect to raw data for date context
con_raw <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
message("Connected to raw_data database for date context")

# ==============================================================================
# PART 2: MAIN
# ==============================================================================
message("\n--- MAIN: Processing time labels ---")

tryCatch({
  # Step 1: Get date range from raw orders to understand time context
  message("Step 1: Extracting date ranges from raw orders...")

  orders_raw <- tbl2(con_raw, "df_cbz_orders___raw") %>%
    mutate(created_at_ts = as.POSIXct(created_at)) %>%
    filter(!is.na(created_at_ts))

  date_context <- orders_raw %>%
    summarise(
      min_date = min(created_at_ts, na.rm = TRUE),
      max_date = max(created_at_ts, na.rm = TRUE),
      total_orders = n()
    ) %>%
    collect()

  # Extract year context (use max date year as the analysis year)
  analysis_year <- lubridate::year(date_context$max_date)
  min_year <- lubridate::year(date_context$min_date)
  if (is.na(analysis_year)) {
    warning("No valid created_at values; using current year for labels.")
    analysis_year <- lubridate::year(Sys.Date())
    min_year <- analysis_year
  }
  analysis_year_value <- analysis_year

  message(sprintf("  Date range: %s to %s",
                 date_context$min_date, date_context$max_date))
  message(sprintf("  Analysis year: %d", analysis_year))

  # Step 2: Get monthly order distribution for accurate month labels
  message("Step 2: Calculating monthly distribution...")

  monthly_distribution <- orders_raw %>%
    mutate(
      order_year = year(created_at_ts),
      order_month = month(created_at_ts)
    ) %>%
    group_by(order_year, order_month) %>%
    summarise(
      month_orders = n(),
      .groups = "drop"
    ) %>%
    collect() %>%
    arrange(order_year, order_month)

  # Step 3: Load existing Poisson analysis data
  message("Step 3: Loading existing Poisson analysis data...")

  poisson_data <- tbl2(con_app, "df_cbz_poisson_analysis_all") %>%
    collect()

  message(sprintf("  Loaded %d rows of Poisson analysis", nrow(poisson_data)))

  # Step 4: Enrich with hierarchical time labels
  message("Step 4: Enriching with hierarchical time labels...")

  poisson_enriched <- poisson_data %>%
    mutate(
      # Extract time components from predictor names
      analysis_year = case_when(
        predictor == "year" ~ .env$analysis_year_value,
        grepl("^month_", predictor) ~ .env$analysis_year_value,
        TRUE ~ NA_integer_
      ),

      analysis_month = as.integer(stringr::str_match(
        predictor,
        "^month_(\\d+)$"
      )[, 2]),

      # For day predictor (day of month effect)
      analysis_day = case_when(
        predictor == "day" ~ NA_integer_,  # Generic day effect, not specific date
        TRUE ~ NA_integer_
      ),

      # ISO week for weekly patterns
      analysis_week = NA_integer_,

      # Quarter calculation
      analysis_quarter = case_when(
        !is.na(analysis_month) ~ ceiling(analysis_month / 3),
        TRUE ~ NA_integer_
      ),

      # Generate hierarchical labels with actual dates
      year_label = case_when(
        predictor == "year" ~ paste0(analysis_year, "年"),
        TRUE ~ NA_character_
      ),

      month_label = case_when(
        grepl("^month_", predictor) ~ paste0(
          analysis_year, "年",
          analysis_month, "月"
        ),
        TRUE ~ NA_character_
      ),

      # Day label (for generic day of month effect)
      day_label = case_when(
        predictor == "day" ~ "每月日期效應",
        TRUE ~ NA_character_
      ),

      # Week labels for weekdays
      week_label = case_when(
        predictor == "monday" ~ "週一",
        predictor == "tuesday" ~ "週二",
        predictor == "wednesday" ~ "週三",
        predictor == "thursday" ~ "週四",
        predictor == "friday" ~ "週五",
        predictor == "saturday" ~ "週六",
        predictor == "sunday" ~ "週日",
        TRUE ~ NA_character_
      ),

      quarter_label = case_when(
        !is.na(analysis_quarter) ~ paste0(analysis_year, "年Q", analysis_quarter),
        TRUE ~ NA_character_
      ),

      # Define time hierarchy
      time_hierarchy = case_when(
        predictor == "year" ~ "year",
        grepl("^month_", predictor) ~ "month",
        predictor == "day" ~ "day",
        predictor %in% c("monday", "tuesday", "wednesday", "thursday",
                        "friday", "saturday", "sunday") ~ "weekday",
        TRUE ~ "other"
      ),

      time_granularity = case_when(
        predictor == "year" ~ "yearly",
        grepl("^month_", predictor) ~ "monthly",
        predictor == "day" ~ "daily",
        predictor %in% c("monday", "tuesday", "wednesday", "thursday",
                        "friday", "saturday", "sunday") ~ "weekly",
        TRUE ~ NA_character_
      ),

      month_start_str = if_else(
        !is.na(analysis_month),
        sprintf("%d-%02d-01", .env$analysis_year_value, analysis_month),
        NA_character_
      ),
      month_start_date = as.Date(month_start_str, format = "%Y-%m-%d"),
      month_end_date = if_else(
        !is.na(month_start_date),
        as.Date(lubridate::ceiling_date(month_start_date, "month") - lubridate::days(1)),
        as.Date(NA)
      ),

      # Calculate date ranges for each period
      date_start = case_when(
        predictor == "year" ~ as.Date(
          sprintf("%d-01-01", .env$analysis_year_value),
          format = "%Y-%m-%d"
        ),
        grepl("^month_", predictor) ~ month_start_date,
        TRUE ~ NA_Date_
      ),

      date_end = case_when(
        predictor == "year" ~ as.Date(
          sprintf("%d-12-31", .env$analysis_year_value),
          format = "%Y-%m-%d"
        ),
        grepl("^month_", predictor) ~ month_end_date,
        TRUE ~ NA_Date_
      ),

      # Calculate period duration
      period_days = case_when(
        !is.na(date_start) & !is.na(date_end) ~ as.integer(date_end - date_start + 1),
        TRUE ~ NA_integer_
      ),

      # Add data coverage indicator (simplified for now)
      is_complete_period = TRUE,
      data_coverage_pct = 100.0
    ) %>%
    select(-month_start_str, -month_start_date, -month_end_date)

  # Add display priority for sorting
  poisson_enriched <- poisson_enriched %>%
    mutate(
      display_order = case_when(
        time_hierarchy == "year" ~ 1000,
        time_hierarchy == "quarter" ~ 2000 + coalesce(analysis_quarter, 0),
        time_hierarchy == "month" ~ 3000 + coalesce(analysis_month, 0),
        time_hierarchy == "weekday" ~ 4000 + case_when(
          predictor == "monday" ~ 1,
          predictor == "tuesday" ~ 2,
          predictor == "wednesday" ~ 3,
          predictor == "thursday" ~ 4,
          predictor == "friday" ~ 5,
          predictor == "saturday" ~ 6,
          predictor == "sunday" ~ 7,
          TRUE ~ 8
        ),
        time_hierarchy == "day" ~ 5000,
        TRUE ~ 9999
      )
    )

  message(sprintf("  Enriched %d rows with time labels", nrow(poisson_enriched)))

  # Step 5: Write enriched data back to database
  message("Step 5: Writing enriched data back to database...")

  # Create backup of original table
  backup_table_name <- paste0("df_cbz_poisson_analysis_all_backup_",
                              format(Sys.Date(), "%Y%m%d"))

  if (DBI::dbExistsTable(con_app, backup_table_name)) {
    DBI::dbRemoveTable(con_app, backup_table_name)
  }

  # Create backup
  DBI::dbExecute(con_app, sprintf(
    "CREATE TABLE %s AS SELECT * FROM df_cbz_poisson_analysis_all",
    backup_table_name
  ))
  message(sprintf("  Created backup table: %s", backup_table_name))

  # Write enriched data
  DBI::dbWriteTable(con_app, "df_cbz_poisson_analysis_all",
                    poisson_enriched, overwrite = TRUE)
  message("  Successfully updated df_cbz_poisson_analysis_all")

  # Step 6: Create summary statistics
  message("Step 6: Creating summary statistics...")

  summary_stats <- poisson_enriched %>%
    group_by(time_hierarchy) %>%
    summarise(
      count = n(),
      with_labels = sum(!is.na(coalesce(year_label, month_label,
                                        day_label, week_label))),
      avg_irr = mean(incidence_rate_ratio, na.rm = TRUE),
      .groups = "drop"
    )

  print(summary_stats)

}, error = function(e) {
  message(sprintf("ERROR in MAIN: %s", e$message))
  error_occurred <<- TRUE
  stop(e)
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================
message("\n--- TEST: Validating enriched data ---")

tryCatch({
  test_passed <- TRUE
  # Test 1: Check if enrichment was successful
  test_data <- tbl2(con_app, "df_cbz_poisson_analysis_all") %>%
    select(predictor, time_hierarchy, year_label, month_label,
           analysis_year, analysis_month) %>%
    collect()

  # Test month labels
  month_rows <- test_data %>% filter(grepl("^month_", predictor))
  if (nrow(month_rows) > 0) {
    month_labels_present <- sum(!is.na(month_rows$month_label))
    message(sprintf("Test 1: Month labels - %d/%d populated",
                   month_labels_present, nrow(month_rows)))
    if (month_labels_present == 0) {
      warning("No month labels were created!")
      test_passed <- FALSE
    }
  }

  # Test year labels
  year_rows <- test_data %>% filter(predictor == "year")
  if (nrow(year_rows) > 0) {
    year_labels_present <- sum(!is.na(year_rows$year_label))
    message(sprintf("Test 2: Year labels - %d/%d populated",
                   year_labels_present, nrow(year_rows)))
  }

  # Test hierarchy classification
  hierarchy_counts <- test_data %>%
    count(time_hierarchy) %>%
    arrange(time_hierarchy)

  message("Test 3: Hierarchy classification:")
  print(hierarchy_counts)

  # Test date ranges
  date_test <- tbl2(con_app, "df_cbz_poisson_analysis_all") %>%
    filter(!is.na(date_start)) %>%
    select(predictor, date_start, date_end, period_days) %>%
    head(5) %>%
    collect()

  if (nrow(date_test) > 0) {
    message("Test 4: Date ranges sample:")
    print(date_test)
  }

  if (test_passed) {
    message("\n✅ All tests PASSED")
  } else {
    message("\n⚠️ Some tests FAILED - review warnings above")
  }

}, error = function(e) {
  message(sprintf("ERROR in TEST: %s", e$message))
  test_passed <- FALSE
})

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================
message("\n--- SUMMARY: Execution report ---")
message(sprintf("Status: %s", ifelse(test_passed && !error_occurred, "SUCCESS ✅", "FAILED ❌")))
message(sprintf("Execution Time: %.2f seconds", as.numeric(difftime(Sys.time(), start_time, units = "secs"))))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================
message("\n--- DEINITIALIZE: Cleaning up ---")

# Close database connections
DBI::dbDisconnect(con_app)
DBI::dbDisconnect(con_raw)
message("Database connections closed")

status_label <- ifelse(test_passed && !error_occurred, "SUCCESS ✅", "FAILED ❌")
message("\n=== cbz_DER_poisson_time_labels.R completed ===")
message(sprintf("Status: %s", status_label))

# Clean up environment (must be last)
autodeinit()
# End of file
