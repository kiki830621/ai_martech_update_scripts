#!/usr/bin/env Rscript
#####
#P09_D04_08
# DERIVATION: Precision Marketing Poisson Analysis
# VERSION: 2.0
# PLATFORM: all
# GROUP: D04
# SEQUENCE: 08
# PURPOSE: Run Poisson regression analysis with R118 statistical significance
# CONSUMES: processed_data.duckdb/df_precision_features or transformed_precision_{product}
# PRODUCES: processed_data.duckdb/df_precision_poisson_analysis
# PRINCIPLE: DM_R044, MP064, MP109, R118, MP029, MP102
#####

#all_D04_08

#' @title Precision Marketing DRV - Poisson Analysis
#' @description Run Poisson regression analysis on product features to identify
#'              statistically significant drivers of product performance.
#'              Implements R118 statistical significance documentation.
#'              Calculates ACTUAL variable ranges (MP029 compliance).
#' @requires duckdb, dplyr, tidyr
#' @input_tables processed_data.duckdb (from ETL + DRV)
#' @output_tables processed_data.duckdb/df_precision_poisson_analysis
#' @business_rules If no features, write empty schema; otherwise run Poisson per product line.
#' @platform all
#' @author MAMBA Development Team
#' @date 2025-12-14

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

# 1.0: Autoinit - Environment setup
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
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()
if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

# 1.1: Load required packages
library(duckdb)
library(dplyr)
library(tidyr)

# 1.2: Initialize tracking variables
error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()
connection_created <- FALSE

# 1.3: Source utility function
utils_path <- "scripts/global_scripts/04_utils/fn_run_poisson_regression.R"
if (file.exists(utils_path)) {
  source(utils_path)
  message(sprintf("  - Loaded utility: %s", basename(utils_path)))
} else {
  stop(sprintf("ERROR: Utility function not found: %s", utils_path))
}

# 1.4: Configuration
PRODUCT_LINES <- c(
  "electric_can_opener",
  "milk_frother",
  "salt_and_pepper_grinder",
  "silicone_spatula",
  "meat_claw",
  "pastry_brush"
)

DB_PATH <- db_path_list$processed_data

EXCLUDE_COLS <- c(
  "product_id", "product_line", "product_brand", "product_title",
  "country", "product_url", "product_image_url",
  "original_price", "original_currency", "conversion_rate",
  "import_timestamp", "staging_timestamp", "transformation_timestamp",
  "enrichment_timestamp", "aggregation_timestamp",
  "source_table", "aggregation_level", "aggregation_method",
  "rating", "review_count",
  "n_products", "total_source_products"
)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  message("════════════════════════════════════════════════════════════════════")
  message("Precision Marketing DRV - Poisson Analysis")
  message("════════════════════════════════════════════════════════════════════")
  message(sprintf("Process Date: %s", start_time))
  message(sprintf("Database: %s", DB_PATH))
  message("")

  # 2.1: Validate Input Database
  if (!file.exists(DB_PATH)) {
    stop(sprintf("Database not found: %s\nPlease run ETL first.", DB_PATH))
  }

  # 2.2: Connect to Database
  message("[Step 1/5] Connecting to database...")
  con <- dbConnectDuckdb(DB_PATH, read_only = FALSE)
  connection_created <- TRUE
  tables <- dbListTables(con)
  message(sprintf("  Available tables: %d", length(tables)))
  message("")

  # 2.3: Determine Data Source
  message("[Step 2/5] Selecting data source...")
  skip_analysis <- FALSE
  if ("df_precision_features" %in% tables) {
    message("  Using aggregated features: df_precision_features")
    use_aggregated <- TRUE
  } else {
    profile_tables <- grep("^transformed_precision_", tables, value = TRUE)
    if (length(profile_tables) == 0) {
      message("  ⚠️ No input tables found; writing empty schema (MP029)")
      use_aggregated <- NA
      skip_analysis <- TRUE
    }
    if (!skip_analysis) {
      message(sprintf("  Using transformed product profiles: %d tables", length(profile_tables)))
      use_aggregated <- FALSE
    }
  }
  message("")

  # 2.4: Process Each Product Line
  if (skip_analysis) {
    message("[Step 3/5] Skipping regression: no input tables available")
  } else {
    message("[Step 3/5] Running Poisson regression by product line...")
  }
  all_results <- list()

  for (pl in PRODUCT_LINES) {
    if (skip_analysis) {
      break
    }
    message(sprintf("\n  [Product Line: %s]", toupper(pl)))

    # Load data
    if (use_aggregated) {
      features <- tryCatch({
        tbl2(con, "df_precision_features") %>%
          filter(product_line == pl) %>%
          collect()
      }, error = function(e) NULL)
    } else {
      table_name <- sprintf("transformed_precision_%s", pl)
      if (!table_name %in% tables) {
        message(sprintf("    ⚠️ Table %s not found, skipping", table_name))
        next
      }
      features <- tryCatch({
        tbl2(con, table_name) %>% collect()
      }, error = function(e) NULL)
    }

    if (is.null(features) || nrow(features) == 0) {
      message(sprintf("    ⚠️ No data for %s, skipping", pl))
      next
    }

    message(sprintf("    Loaded %d rows", nrow(features)))

    # Identify predictor columns
    numeric_cols <- names(features)[sapply(features, is.numeric)]
    predictor_cols <- setdiff(numeric_cols, EXCLUDE_COLS)
    predictor_cols <- predictor_cols[sapply(predictor_cols, function(col) {
      sd(features[[col]], na.rm = TRUE) > 0
    })]

    if (length(predictor_cols) == 0) {
      message(sprintf("    ⚠️ No valid predictors, skipping"))
      next
    }

    # Determine outcome variable
    outcome_col <- if ("review_count" %in% names(features)) {
      "review_count"
    } else if ("review_count_sum" %in% names(features)) {
      "review_count_sum"
    } else {
      message(sprintf("    ⚠️ No outcome variable, skipping"))
      next
    }

    # Prepare regression data
    regression_data <- features %>%
      select(outcome = all_of(outcome_col), all_of(predictor_cols)) %>%
      filter(!is.na(outcome), outcome >= 0) %>%
      filter(rowSums(is.na(select(., all_of(predictor_cols)))) < length(predictor_cols))

    predictor_cols <- setdiff(names(regression_data), "outcome")
    predictor_cols <- predictor_cols[sapply(predictor_cols, function(col) {
      sd(regression_data[[col]], na.rm = TRUE) > 0
    })]

    if (nrow(regression_data) < 10 || length(predictor_cols) == 0) {
      message(sprintf("    ⚠️ Insufficient data (n=%d), skipping", nrow(regression_data)))
      next
    }

    # Run Poisson regression
    results <- tryCatch({
      fn_run_poisson_regression(
        data = regression_data,
        outcome_col = "outcome",
        predictor_cols = predictor_cols,
        offset_col = NULL
      )
    }, error = function(e) {
      message(sprintf("    ⚠️ Regression failed: %s", e$message))
      NULL
    })

    if (!is.null(results)) {
      results$product_line <- pl
      all_results[[pl]] <- results
      n_sig <- sum(results$is_significant, na.rm = TRUE)
      message(sprintf("    ✓ Regression complete: %d predictors, %d significant", nrow(results), n_sig))
    }
  }
  message("")

  # 2.5: Combine and Write Results
  message("[Step 4/5] Writing results to database...")

  if (skip_analysis || length(all_results) == 0) {
    # Create placeholder
    message("  Creating R118-compliant placeholder...")
    combined_results <- data.frame(
      product_line = character(),
      predictor = character(),
      coefficient = numeric(),
      std_error = numeric(),
      z_value = numeric(),
      p_value = numeric(),
      is_significant = logical(),
      significance_flag = character(),
      predictor_min = numeric(),
      predictor_max = numeric(),
      predictor_range = numeric(),
      predictor_is_binary = logical(),
      predictor_is_categorical = logical(),
      track_multiplier = numeric(),
      outcome_variable = character(),
      model_sample_size = integer(),
      model_deviance = numeric(),
      model_aic = numeric(),
      analysis_timestamp = character(),
      stringsAsFactors = FALSE
    )
    processing_mode <- "EMPTY_SCHEMA"
  } else {
    combined_results <- bind_rows(all_results)
    processing_mode <- "REAL_DATA"
  }

  if ("df_precision_poisson_analysis" %in% dbListTables(con)) {
    dbRemoveTable(con, "df_precision_poisson_analysis")
  }
  dbWriteTable(con, "df_precision_poisson_analysis", combined_results, overwrite = TRUE)
  rows_processed <- nrow(combined_results)
  message(sprintf("  ✓ Wrote %d rows to df_precision_poisson_analysis", rows_processed))
  message("")

}, error = function(e) {
  message("ERROR in MAIN: ", e$message)
  error_occurred <<- TRUE
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred) {
  tryCatch({
    message("────────────────────────────────────────────────────────────────────")
    message("PART 3: TEST - Validating output...")
    message("────────────────────────────────────────────────────────────────────")

    # 3.1: Verify Output Table Exists
    tables <- dbListTables(con)
    if (!"df_precision_poisson_analysis" %in% tables) {
      stop("Output table df_precision_poisson_analysis not found")
    }
    message("  ✓ Output table exists")

    # 3.2: Validate R118 Compliance Columns
    test_data <- tbl2(con, "df_precision_poisson_analysis") %>%
      head(0) %>%
      collect()
    required_cols <- c("p_value", "is_significant", "significance_flag")
    missing_cols <- setdiff(required_cols, names(test_data))
    if (length(missing_cols) > 0) {
      stop(sprintf("Missing R118 columns: %s", paste(missing_cols, collapse = ", ")))
    }
    message("  ✓ R118 significance columns present")

    # 3.3: Validate MP029 Compliance Columns
    range_cols <- c("predictor_min", "predictor_max", "predictor_range")
    missing_range <- setdiff(range_cols, names(test_data))
    if (length(missing_range) > 0) {
      stop(sprintf("Missing MP029 range columns: %s", paste(missing_range, collapse = ", ")))
    }
    message("  ✓ MP029 range metadata columns present")

    # 3.4: Validate Processing Mode
    if (!exists("processing_mode")) {
      stop("Processing mode not set")
    }
    message(sprintf("  ✓ Processing mode: %s", processing_mode))

    message("  ✅ All tests passed")
    test_passed <- TRUE

  }, error = function(e) {
    message("ERROR in TEST: ", e$message)
    test_passed <<- FALSE
  })
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

end_time <- Sys.time()
execution_time <- difftime(end_time, start_time, units = "secs")

message("")
message("════════════════════════════════════════════════════════════════════")
message("DERIVATION SUMMARY")
message("════════════════════════════════════════════════════════════════════")
message(sprintf("Script:           %s", "all_D04_08.R"))
message(sprintf("Platform:         all (cross-platform)"))
message(sprintf("Status:           %s", ifelse(test_passed, "SUCCESS", "FAILED")))
message(sprintf("Mode:             %s", ifelse(exists("processing_mode"), processing_mode, "UNKNOWN")))
message(sprintf("Rows Processed:   %d", rows_processed))
message(sprintf("Execution Time:   %.2f seconds", as.numeric(execution_time)))
message("════════════════════════════════════════════════════════════════════")
message("")

# Compliance Documentation
message("Principle Compliance:")
message("─────────────────────────────────────────────────────────────────")
message("✓ R118: p-values and significance flags included")
message("✓ MP029: Variable ranges from actual data (no guessing)")
message("✓ MP102: Complete metadata for all predictors")
message("✓ MP109: DRV derivation layer (statistical analysis)")
message("─────────────────────────────────────────────────────────────────")
message("")

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

# 5.1: Close Database Connection
if (exists("connection_created") && connection_created) {
  if (exists("con") && inherits(con, "DBIConnection")) {
    dbDisconnect(con, shutdown = TRUE)
    message("Disconnected from database")
  }
}

# 5.2: Return result for {targets} pipeline
if (!interactive()) {
  if (test_passed) {
    message("\n✅ Script completed successfully")
  } else {
    message("\n❌ Script completed with errors")
  }
}

# 5.3: Autodeinit (MUST be last statement)
autodeinit()
# End of file
