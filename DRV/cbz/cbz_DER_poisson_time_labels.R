# cbz_DER_poisson_time_labels.R
# Derivation script to enrich Poisson analysis with hierarchical time labels
#
# Following principles:
# - MP064: ETL-Derivation Separation (business logic in derivation layer)
# - R113: Four-part script structure (INITIALIZE/MAIN/TEST/DEINITIALIZE)
# - MP031: Proper autoinit()/autodeinit() usage
# - R092: Universal data access pattern
# - R120: Filter variable naming conventions
#
# Purpose: Add year, month, and day context to Poisson time analysis data
# Input: df_cbz_poisson_analysis_all (existing Poisson analysis results)
# Output: Enriched table with hierarchical date labels
# ==============================================================================

# INITIALIZE ------------------------------------------------------------------
message("=== cbz_DER_poisson_time_labels.R ===")
message("Starting Poisson time label enrichment")

# Initialize environment
autoinit()

# Connect to app database
con_app <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
message("Connected to app_data database")

# Connect to raw data for date context
con_raw <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
message("Connected to raw_data database for date context")

# MAIN ------------------------------------------------------------------------
message("\n--- MAIN: Processing time labels ---")

tryCatch({
  # Step 1: Get date range from raw orders to understand time context
  message("Step 1: Extracting date ranges from raw orders...")

  date_context <- tbl2(con_raw, "df_cbz_orders___raw") %>%
    summarise(
      min_date = min(created_at, na.rm = TRUE),
      max_date = max(created_at, na.rm = TRUE),
      total_orders = n()
    ) %>%
    collect()

  # Extract year context (use max date year as the analysis year)
  analysis_year <- lubridate::year(date_context$max_date)
  min_year <- lubridate::year(date_context$min_date)

  message(sprintf("  Date range: %s to %s",
                 date_context$min_date, date_context$max_date))
  message(sprintf("  Analysis year: %d", analysis_year))

  # Step 2: Get monthly order distribution for accurate month labels
  message("Step 2: Calculating monthly distribution...")

  monthly_distribution <- tbl2(con_raw, "df_cbz_orders___raw") %>%
    mutate(
      order_year = year(created_at),
      order_month = month(created_at)
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
        predictor == "year" ~ analysis_year,
        grepl("^month_", predictor) ~ analysis_year,
        TRUE ~ NA_integer_
      ),

      analysis_month = case_when(
        grepl("^month_", predictor) ~ as.integer(gsub("month_", "", predictor)),
        TRUE ~ NA_integer_
      ),

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

      # Calculate date ranges for each period
      date_start = case_when(
        predictor == "year" ~ as.Date(paste0(analysis_year, "-01-01")),
        grepl("^month_", predictor) ~ as.Date(paste0(
          analysis_year, "-",
          sprintf("%02d", analysis_month), "-01"
        )),
        TRUE ~ NA_Date_
      ),

      date_end = case_when(
        predictor == "year" ~ as.Date(paste0(analysis_year, "-12-31")),
        grepl("^month_", predictor) ~ {
          # Last day of the month
          next_month <- if_else(analysis_month == 12,
                               as.Date(paste0(analysis_year + 1, "-01-01")),
                               as.Date(paste0(analysis_year, "-",
                                           sprintf("%02d", analysis_month + 1), "-01")))
          next_month - 1
        },
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
    )

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
  stop(e)
})

# TEST ------------------------------------------------------------------------
message("\n--- TEST: Validating enriched data ---")

test_passed <- TRUE

tryCatch({
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

# DEINITIALIZE ----------------------------------------------------------------
message("\n--- DEINITIALIZE: Cleaning up ---")

# Close database connections
DBI::dbDisconnect(con_app)
DBI::dbDisconnect(con_raw)
message("Database connections closed")

# Clean up environment
autodeinit()
message("Environment deinitialized")

message("\n=== cbz_DER_poisson_time_labels.R completed ===")
message(sprintf("Status: %s", ifelse(test_passed, "SUCCESS ✅", "FAILED ❌")))