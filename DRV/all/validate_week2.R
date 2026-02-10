#####
# CONSUMES: df_precision_features, df_precision_time_series, df_scripts
# PRODUCES: none
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: none
#####

#!/usr/bin/env Rscript
# ==============================================================================
# Week 2 Validation Script: DRV Layer Implementation
# ==============================================================================
#
# Purpose: Validate Week 2 DRV deliverables for Precision Marketing
# Validates:
#   - Utility functions (fn_complete_time_series, fn_aggregate_features)
#   - DRV scripts (all_D04_09.R, all_D04_07.R)
#   - processed_data.duckdb with DRV tables
#   - R117 compliance (time series transparency markers)
#   - MP109 compliance (DRV 2TR stage only)
#
# Week 2 Implementation: MAMBA Precision Marketing Redesign
# Date: 2025-11-13
# ==============================================================================

sql_read_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R")
)
sql_read_path <- sql_read_candidates[file.exists(sql_read_candidates)][1]
if (is.na(sql_read_path)) {
  stop("fn_sql_read.R not found in expected paths")
}
source(sql_read_path)
library(duckdb)
library(dplyr)
library(tibble)

# Initialize paths
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

db_path <- if (exists("db_path_list", inherits = TRUE) &&
  !is.null(db_path_list$processed_data)) {
  db_path_list$processed_data
} else {
  file.path("data", "processed_data.duckdb")
}

# ==============================================================================
# Configuration
# ==============================================================================

# Expected files
EXPECTED_FILES <- list(
  utilities = c(
    "scripts/global_scripts/04_utils/fn_complete_time_series.R",
    "scripts/global_scripts/04_utils/fn_aggregate_features.R"
  ),
  df_scripts = c(
    "scripts/update_scripts/DRV/all/all_D04_09.R",
    "scripts/update_scripts/DRV/all/all_D04_07.R",
    "scripts/update_scripts/DRV/all/validate_week2.R",
    "scripts/update_scripts/DRV/all/README.md"
  ),
  databases = c(
    db_path
  )
)

# Expected DRV tables
EXPECTED_DRV_TABLES <- c(
  "df_precision_features",
  "df_precision_time_series"
)

# R117 required columns for time series
R117_REQUIRED_COLUMNS <- c(
  "data_source",         # 'REAL' or 'FILLED'
  "filling_method",      # Fill method used
  "filling_timestamp"    # When filling occurred
)

# ==============================================================================
# Validation Functions
# ==============================================================================

#' Validation Results Tracker
validation_results <- list(
  passed = 0,
  failed = 0,
  warnings = 0,
  tests = list()
)

#' Record Test Result
record_test <- function(test_name, status, message) {
  result <- list(
    name = test_name,
    status = status,  # "PASS", "FAIL", "WARN"
    message = message,
    timestamp = Sys.time()
  )

  validation_results$tests <<- c(validation_results$tests, list(result))

  if (status == "PASS") {
    validation_results$passed <<- validation_results$passed + 1
    cat(sprintf("PASS: %s\n", test_name))
  } else if (status == "FAIL") {
    validation_results$failed <<- validation_results$failed + 1
    cat(sprintf("FAIL: %s\n  -> %s\n", test_name, message))
  } else if (status == "WARN") {
    validation_results$warnings <<- validation_results$warnings + 1
    cat(sprintf("WARN: %s\n  -> %s\n", test_name, message))
  }
}

# ==============================================================================
# TEST SUITE 1: File Existence
# ==============================================================================

test_file_existence <- function() {
  cat("\n====================================================================\n")
  cat("TEST SUITE 1: File Existence\n")
  cat("====================================================================\n\n")

  # Test utility functions
  cat("1.1 Utility Functions\n")
  cat("--------------------------------------------------------------------\n")
  for (file in EXPECTED_FILES$utilities) {
    if (file.exists(file)) {
      size <- file.info(file)$size
      record_test(
        sprintf("Utility exists: %s", basename(file)),
        "PASS",
        sprintf("File size: %d bytes", size)
      )
    } else {
      record_test(
        sprintf("Utility exists: %s", basename(file)),
        "FAIL",
        sprintf("File not found: %s", file)
      )
    }
  }

  # Test DRV scripts
  cat("\n1.2 DRV Scripts\n")
  cat("--------------------------------------------------------------------\n")
  for (file in EXPECTED_FILES$df_scripts) {
    if (file.exists(file)) {
      size <- file.info(file)$size
      record_test(
        sprintf("DRV script exists: %s", basename(file)),
        "PASS",
        sprintf("File size: %d bytes", size)
      )
    } else {
      record_test(
        sprintf("DRV script exists: %s", basename(file)),
        "FAIL",
        sprintf("File not found: %s", file)
      )
    }
  }

  # Test databases
  cat("\n1.3 Databases\n")
  cat("--------------------------------------------------------------------\n")
  for (file in EXPECTED_FILES$databases) {
    if (file.exists(file)) {
      size_mb <- file.info(file)$size / (1024 * 1024)
      record_test(
        sprintf("Database exists: %s", basename(file)),
        "PASS",
        sprintf("File size: %.2f MB", size_mb)
      )
    } else {
      record_test(
        sprintf("Database exists: %s", basename(file)),
        "FAIL",
        sprintf("File not found: %s", file)
      )
    }
  }
}

# ==============================================================================
# TEST SUITE 2: DRV Table Existence and Schema
# ==============================================================================

test_drv_tables <- function() {
  cat("\n====================================================================\n")
  cat("TEST SUITE 2: DRV Table Existence and Schema\n")
  cat("====================================================================\n\n")

  if (!file.exists(db_path)) {
    record_test(
      "DRV tables validation",
      "FAIL",
      sprintf("Database not found: %s", db_path)
    )
    return()
  }

  # Connect to database
  con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
  available_tables <- dbListTables(con)

  cat("2.1 Table Existence\n")
  cat("--------------------------------------------------------------------\n")

  for (table in EXPECTED_DRV_TABLES) {
    if (table %in% available_tables) {
      row_count <- sql_read(con, sprintf("SELECT COUNT(*) as n FROM %s", table))$n
      col_count <- length(dbListFields(con, table))

      record_test(
        sprintf("DRV table exists: %s", table),
        "PASS",
        sprintf("%d rows, %d columns", row_count, col_count)
      )
    } else {
      record_test(
        sprintf("DRV table exists: %s", table),
        "FAIL",
        sprintf("Table not found in %s", basename(db_path))
      )
    }
  }

  dbDisconnect(con, shutdown = TRUE)
}

# ==============================================================================
# TEST SUITE 3: R117 Compliance (Time Series Transparency)
# ==============================================================================

test_r117_compliance <- function() {
  cat("\n====================================================================\n")
  cat("TEST SUITE 3: R117 Time Series Transparency Compliance\n")
  cat("====================================================================\n\n")

  if (!file.exists(db_path)) {
    record_test(
      "R117 compliance check",
      "FAIL",
      "processed_data.duckdb not found"
    )
    return()
  }

  con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
  available_tables <- dbListTables(con)

  if (!"df_precision_time_series" %in% available_tables) {
    record_test(
      "R117 compliance: time series table",
      "FAIL",
      "df_precision_time_series table not found"
    )
    dbDisconnect(con, shutdown = TRUE)
    return()
  }

  # Check R117 required columns
  cat("3.1 R117 Required Columns\n")
  cat("--------------------------------------------------------------------\n")

  time_series_cols <- dbListFields(con, "df_precision_time_series")

  for (col in R117_REQUIRED_COLUMNS) {
    if (col %in% time_series_cols) {
      record_test(
        sprintf("R117 column exists: %s", col),
        "PASS",
        "Time series transparency marker present"
      )
    } else {
      record_test(
        sprintf("R117 column exists: %s", col),
        "FAIL",
        sprintf("Required R117 column missing: %s", col)
      )
    }
  }

  # Check data_source values
  cat("\n3.2 R117 data_source Values\n")
  cat("--------------------------------------------------------------------\n")

  if ("data_source" %in% time_series_cols) {
    query <- "SELECT DISTINCT data_source FROM df_precision_time_series WHERE data_source IS NOT NULL"
    data_sources <- sql_read(con, query)$data_source

    valid_sources <- c("REAL", "FILLED", "PLACEHOLDER")
    invalid_sources <- setdiff(data_sources, valid_sources)

    if (length(invalid_sources) == 0) {
      record_test(
        "R117 data_source values",
        "PASS",
        sprintf("All values valid: %s", paste(data_sources, collapse = ", "))
      )
    } else {
      record_test(
        "R117 data_source values",
        "FAIL",
        sprintf("Invalid values found: %s", paste(invalid_sources, collapse = ", "))
      )
    }
  }

  dbDisconnect(con, shutdown = TRUE)
}

# ==============================================================================
# TEST SUITE 4: Feature Aggregation Quality
# ==============================================================================

test_feature_aggregation <- function() {
  cat("\n====================================================================\n")
  cat("TEST SUITE 4: Feature Aggregation Quality\n")
  cat("====================================================================\n\n")

  if (!file.exists(db_path)) {
    record_test(
      "Feature aggregation check",
      "FAIL",
      "processed_data.duckdb not found"
    )
    return()
  }

  con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
  available_tables <- dbListTables(con)

  if (!"df_precision_features" %in% available_tables) {
    record_test(
      "Feature aggregation: table exists",
      "FAIL",
      "df_precision_features table not found"
    )
    dbDisconnect(con, shutdown = TRUE)
    return()
  }

  # Read features table
  features <- tbl2(con, "df_precision_features") %>% collect()

  cat("4.1 Data Completeness\n")
  cat("--------------------------------------------------------------------\n")

  # Check row count
  if (nrow(features) > 0) {
    record_test(
      "Feature table has data",
      "PASS",
      sprintf("%d aggregated groups", nrow(features))
    )
  } else {
    record_test(
      "Feature table has data",
      "WARN",
      "Table exists but has 0 rows (ETL may not have run yet)"
    )
  }

  # Check for product_line column
  if ("product_line" %in% names(features)) {
    unique_pls <- unique(features$product_line)
    record_test(
      "Product lines present",
      "PASS",
      sprintf("%d product lines: %s", length(unique_pls), paste(unique_pls, collapse = ", "))
    )
  } else {
    record_test(
      "Product lines present",
      "FAIL",
      "product_line column not found"
    )
  }

  # Check for aggregation metadata (MP102)
  cat("\n4.2 MP102 Metadata Compliance\n")
  cat("--------------------------------------------------------------------\n")

  mp102_cols <- c("aggregation_level", "aggregation_timestamp", "aggregation_method")
  for (col in mp102_cols) {
    if (col %in% names(features)) {
      record_test(
        sprintf("MP102 metadata: %s", col),
        "PASS",
        "Metadata column present"
      )
    } else {
      record_test(
        sprintf("MP102 metadata: %s", col),
        "WARN",
        sprintf("Metadata column missing: %s", col)
      )
    }
  }

  # Check for Inf/NaN values
  cat("\n4.3 Data Quality Checks\n")
  cat("--------------------------------------------------------------------\n")

  numeric_cols <- names(features)[sapply(features, is.numeric)]
  has_inf <- any(sapply(features[numeric_cols], function(x) any(is.infinite(x), na.rm = TRUE)))
  has_nan <- any(sapply(features[numeric_cols], function(x) any(is.nan(x))))

  if (!has_inf) {
    record_test(
      "No Inf values in numeric columns",
      "PASS",
      "All numeric values finite"
    )
  } else {
    record_test(
      "No Inf values in numeric columns",
      "FAIL",
      "Infinite values detected in aggregated features"
    )
  }

  if (!has_nan) {
    record_test(
      "No NaN values in numeric columns",
      "PASS",
      "All numeric values valid"
    )
  } else {
    record_test(
      "No NaN values in numeric columns",
      "WARN",
      "NaN values detected (may be expected for some statistics)"
    )
  }

  dbDisconnect(con, shutdown = TRUE)
}

# ==============================================================================
# TEST SUITE 5: MP109 Compliance (DRV Layer)
# ==============================================================================

test_mp109_compliance <- function() {
  cat("\n====================================================================\n")
  cat("TEST SUITE 5: MP109 DRV Layer Compliance\n")
  cat("====================================================================\n\n")

  cat("5.1 DRV Stage Verification\n")
  cat("--------------------------------------------------------------------\n")

  # Check that DRV scripts don't have 0IM or 1ST stages
  df_scripts <- c(
    "scripts/update_scripts/DRV/all/all_D04_09.R",
    "scripts/update_scripts/DRV/all/all_D04_07.R"
  )

  for (script in df_scripts) {
    if (file.exists(script)) {
      content <- readLines(script, warn = FALSE)

      # Check for prohibited stages in DRV
      has_0im <- any(grepl("0IM|_0IM|import.*stage", content, ignore.case = TRUE))
      has_1st <- any(grepl("\\b1ST\\b|_1ST|standardization.*stage", content, ignore.case = TRUE))

      if (!has_0im && !has_1st) {
        record_test(
          sprintf("MP109: %s has no 0IM/1ST stages", basename(script)),
          "PASS",
          "DRV correctly implements only 2TR (derivation)"
        )
      } else {
        stages <- c()
        if (has_0im) stages <- c(stages, "0IM")
        if (has_1st) stages <- c(stages, "1ST")

        record_test(
          sprintf("MP109: %s has no 0IM/1ST stages", basename(script)),
          "WARN",
          sprintf("Script mentions: %s (should only have 2TR)", paste(stages, collapse = ", "))
        )
      }
    }
  }
}

# ==============================================================================
# Generate Summary Report
# ==============================================================================

generate_summary_report <- function() {
  cat("\n\n")
  cat("====================================================================\n")
  cat("WEEK 2 VALIDATION SUMMARY REPORT\n")
  cat("====================================================================\n")
  cat(sprintf("Validation Date: %s\n", Sys.time()))
  cat(sprintf("Total Tests:     %d\n", length(validation_results$tests)))
  cat(sprintf("Passed:          %d\n", validation_results$passed))
  cat(sprintf("Failed:          %d\n", validation_results$failed))
  cat(sprintf("Warnings:        %d\n", validation_results$warnings))
  cat("--------------------------------------------------------------------\n")

  pass_rate <- validation_results$passed / length(validation_results$tests) * 100

  if (validation_results$failed == 0) {
    cat(sprintf("\nVALIDATION PASSED (%.1f%% pass rate)\n", pass_rate))
    cat("\nWeek 2 DRV implementation is READY FOR USE\n")
    return_code <- 0
  } else {
    cat(sprintf("\nVALIDATION FAILED (%.1f%% pass rate)\n", pass_rate))
    cat(sprintf("\n%d critical issues must be fixed before deployment\n", validation_results$failed))
    return_code <- 1
  }

  if (validation_results$warnings > 0) {
    cat(sprintf("\n%d warnings detected - review recommended\n", validation_results$warnings))
  }

  cat("====================================================================\n\n")

  return(return_code)
}

# ==============================================================================
# Main Execution
# ==============================================================================

main <- function() {
  cat("====================================================================\n")
  cat("Week 2 Validation: DRV Layer Implementation\n")
  cat("====================================================================\n")
  cat("Validating Precision Marketing DRV deliverables...\n")
  cat("====================================================================\n")

  # Run all test suites
  test_file_existence()
  test_drv_tables()
  test_r117_compliance()
  test_feature_aggregation()
  test_mp109_compliance()

  # Generate summary report
  return_code <- generate_summary_report()

  return(return_code)
}

# Execute validation
if (!interactive()) {
  return_code <- main()
  quit(status = return_code)
} else {
  main()
}

# 5. AUTODEINIT
autodeinit()
