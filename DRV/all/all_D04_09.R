#!/usr/bin/env Rscript
#####
# DERIVATION: Precision Marketing Feature Preparation
# VERSION: 2.0
# PLATFORM: all
# GROUP: D04
# SEQUENCE: 09
# PURPOSE: Aggregate product features across product lines for Poisson analysis
# CONSUMES: transformed_data.duckdb (ETL 2TR stage)
# PRODUCES: processed_data.duckdb/df_precision_features
# PRINCIPLE: DM_R044, MP064, MP109, MP029, MP102, DM_R039, DM_R023
#####
#all_D04_09
# ==============================================================================
# Precision Marketing DRV - Feature Preparation
# ==============================================================================
#
# Purpose: Aggregate product features across all product lines for market analysis
# Stage: DRV 2TR (Derivation - cross-product feature aggregation)
# Input: transformed_data.duckdb (from ETL 2TR stage)
# Output: processed_data.duckdb/df_precision_features
#
# Principle Compliance:
# - MP109: DRV Derivation Layer (2TR stage only - no 0IM/1ST)
# - MP029: No Fake Data (only aggregating real ETL outputs)
# - MP102: Completeness (add aggregation metadata)
# - MP064: ETL-Derivation Separation (DRV reads from ETL outputs, no modification)
#
# DRV Tasks:
# 1. Union all product lines into single feature table
# 2. Aggregate features by product_line and country
# 3. Calculate summary statistics (mean, median, sd, min, max)
# 4. Calculate feature prevalence for binary/dummy variables
# 5. Add aggregation metadata
#
# Week 2 Implementation: MAMBA Precision Marketing Redesign
# Date: 2025-11-13
# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

# 1.0: Autoinit - Environment setup
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

# 1.1: Load required packages
library(duckdb)
library(dplyr)
library(tidyr)
library(tibble)

# 1.2: Initialize tracking variables
error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()

# 1.3: Validate configuration
if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

# 1.4: Source utility function
utils_path <- file.path(GLOBAL_DIR, "04_utils", "fn_aggregate_features.R")

if (file.exists(utils_path)) {
  source(utils_path)
  message(sprintf("✓ Loaded utility: %s", basename(utils_path)))
} else {
  stop(sprintf("ERROR: Utility function not found: %s", utils_path))
}

# ==============================================================================
# Configuration
# ==============================================================================

# Product lines to process
# Product lines (use English IDs matching ETL output tables)
PRODUCT_LINES <- c(
  "electric_can_opener",
  "milk_frother",
  "salt_and_pepper_grinder",
  "silicone_spatula",
  "meat_claw",
  "pastry_brush"
)

# Database paths
DB_TRANSFORMED <- db_path_list$transformed_data
DB_PROCESSED <- db_path_list$processed_data

# Features to aggregate (continuous metrics)
CONTINUOUS_FEATURES <- c(
  "price_usd",
  "rating",
  "review_count",
  "quality_score"
)

# Aggregation functions
AGG_FUNCTIONS <- c("mean", "median", "sd", "min", "max")

# 1.5: Connect to databases
if (!file.exists(DB_TRANSFORMED)) {
  stop(sprintf("ERROR: Input database not found: %s\nRun ETL first.", DB_TRANSFORMED))
}

con_transformed <- dbConnectDuckdb(DB_TRANSFORMED, read_only = TRUE)
con_processed <- dbConnectDuckdb(DB_PROCESSED, read_only = FALSE)
connection_created_transformed <- TRUE
connection_created_processed <- TRUE

# ==============================================================================
# Main DRV Feature Preparation Function
# ==============================================================================

precision_drv_feature_preparation <- function(con_transformed, con_processed) {
  message("════════════════════════════════════════════════════════════════════")
  message("Precision Marketing DRV - Feature Preparation")
  message("════════════════════════════════════════════════════════════════════")
  message(sprintf("Process Date: %s", Sys.time()))
  message(sprintf("Input Database: %s", DB_TRANSFORMED))
  message(sprintf("Output Database: %s", DB_PROCESSED))
  message(sprintf("Product Lines: %s", paste(PRODUCT_LINES, collapse = ", ")))
  message("")

  # === VALIDATION ===

  if (!inherits(con_transformed, "DBIConnection") || !DBI::dbIsValid(con_transformed)) {
    stop("Invalid transformed_data connection")
  }
  if (!inherits(con_processed, "DBIConnection") || !DBI::dbIsValid(con_processed)) {
    stop("Invalid processed_data connection")
  }

  message("[Step 1/6] Database connections validated")
  message("")

  # === UNION ALL PRODUCT LINES ===

  message("[Step 3/6] Unioning product lines...")

  all_products <- NULL
  union_stats <- data.frame(
    product_line = character(),
    row_count = integer(),
    stringsAsFactors = FALSE
  )

  for (pl in PRODUCT_LINES) {
    table_name <- sprintf("transformed_precision_%s", pl)

    # Check if table exists
    if (!dbExistsTable(con_transformed, table_name)) {
      warning(sprintf("  ⚠️  Table %s not found, skipping", table_name))
      next
    }

    # Read product line data
    pl_data <- tbl2(con_transformed, table_name) %>% collect()

    # Add product_line identifier
    pl_data$product_line <- pl

    # Union with accumulated data
    if (is.null(all_products)) {
      all_products <- pl_data
    } else {
      # Find common columns for union
      common_cols <- intersect(names(all_products), names(pl_data))

      # === TYPE COERCION FIX (TD-001) ===
      # Issue: Chinese column names standardized to blank strings create
      # numbered placeholders (_2, _3, _4) with inconsistent types.
      # Solution: Force type consistency before union
      for (col in common_cols) {
        type1 <- class(all_products[[col]])[1]
        type2 <- class(pl_data[[col]])[1]

        if (type1 != type2) {
          message(sprintf("  ⚠️  Type mismatch for '%s': %s vs %s, coercing to character",
                         col, type1, type2))

          # Coerce both to character (safest common type)
          all_products[[col]] <- as.character(all_products[[col]])
          pl_data[[col]] <- as.character(pl_data[[col]])
        }
      }
      # === END TYPE COERCION FIX ===

      all_products <- bind_rows(
        all_products %>% select(all_of(common_cols)),
        pl_data %>% select(all_of(common_cols))
      )
    }

    # Track statistics
    union_stats <- union_stats %>%
      add_row(product_line = pl, row_count = nrow(pl_data))

    message(sprintf("  ✓ Added %s: %d products", pl, nrow(pl_data)))
  }

  if (is.null(all_products) || nrow(all_products) == 0) {
    stop("ERROR: No product data found in transformed_data.duckdb")
  }

  total_products <- nrow(all_products)
  message(sprintf("\n  ✓ Total products unioned: %d", total_products))
  message("")

  # === IDENTIFY FEATURE COLUMNS ===

  message("[Step 4/6] Identifying feature columns...")

  # Get numeric columns
  numeric_cols <- names(all_products)[sapply(all_products, is.numeric)]

  # Exclude metadata and identifier columns
  exclude_cols <- c(
    "price_usd", "original_price", "conversion_rate",  # Will aggregate separately
    "rating", "review_count", "quality_score",          # Will aggregate separately
    "import_timestamp", "transformation_timestamp"       # Metadata
  )

  # Binary feature columns (dummy variables)
  binary_features <- setdiff(numeric_cols, exclude_cols)
  binary_features <- binary_features[sapply(binary_features, function(col) {
    unique_vals <- unique(all_products[[col]][!is.na(all_products[[col]])])
    all(unique_vals %in% c(0, 1))
  })]

  message(sprintf("  - Continuous features: %d", length(CONTINUOUS_FEATURES)))
  message(sprintf("  - Binary features: %d", length(binary_features)))
  message("")

  # === AGGREGATE BY PRODUCT_LINE + COUNTRY ===

  message("[Step 5/6] Aggregating features...")

  # Check if country column exists
  if (!"country" %in% names(all_products)) {
    warning("  ⚠️  'country' column not found. Aggregating by product_line only.")
    group_cols <- c("product_line")
  } else {
    group_cols <- c("product_line", "country")
  }

  # Filter continuous features that exist in data
  features_to_aggregate <- intersect(CONTINUOUS_FEATURES, names(all_products))

  if (length(features_to_aggregate) == 0) {
    stop("ERROR: No continuous features found in data for aggregation")
  }

  # Aggregate continuous features
  aggregated_features <- fn_aggregate_features(
    data = all_products,
    group_cols = group_cols,
    feature_cols = features_to_aggregate,
    agg_functions = AGG_FUNCTIONS,
    include_prevalence = FALSE,  # Will handle separately
    add_metadata = FALSE          # Will add manually
  )

  message(sprintf("  ✓ Aggregated %d groups", nrow(aggregated_features)))
  message("")

  # === CALCULATE FEATURE PREVALENCE ===

  if (length(binary_features) > 0) {
    message("[Step 5.5/6] Calculating feature prevalence...")

    # Filter binary features that exist in data
    binary_features_exist <- intersect(binary_features, names(all_products))

    if (length(binary_features_exist) > 0) {
      prevalence_data <- all_products %>%
        group_by(across(all_of(group_cols))) %>%
        summarise(
          across(
            all_of(binary_features_exist),
            ~mean(., na.rm = TRUE),
            .names = "{.col}_prevalence"
          ),
          .groups = "drop"
        )

      # Join with aggregated features
      aggregated_features <- aggregated_features %>%
        left_join(prevalence_data, by = group_cols)

      message(sprintf("  ✓ Prevalence calculated for %d binary features", length(binary_features_exist)))
    }
    message("")
  }

  # === ADD METADATA (MP102 Compliance) ===

  message("[Step 6/6] Adding metadata...")

  aggregated_features$aggregation_level <- paste(group_cols, collapse = "_")
  aggregated_features$aggregation_timestamp <- Sys.time()
  aggregated_features$aggregation_method <- paste(AGG_FUNCTIONS, collapse = ",")
  aggregated_features$source_table <- "transformed_data.duckdb (all product lines)"
  aggregated_features$total_source_products <- total_products

  message("  ✓ Metadata columns added")
  message("")

  # === WRITE TO PROCESSED DATABASE ===

  message("[Output] Writing to processed_data.duckdb...")

  dbWriteTable(
    conn = con_processed,
    name = "df_precision_features",
    value = aggregated_features,
    overwrite = TRUE
  )

  message(sprintf("  ✓ Wrote %d rows to df_precision_features", nrow(aggregated_features)))
  message("")

  # === SUMMARY STATISTICS ===

  message("════════════════════════════════════════════════════════════════════")
  message("✅ DRV Feature Preparation Complete")
  message("════════════════════════════════════════════════════════════════════")
  message(sprintf("Input Products:          %d", total_products))
  message(sprintf("Product Lines:           %d (%s)",
                  length(PRODUCT_LINES),
                  paste(PRODUCT_LINES, collapse = ", ")))
  message(sprintf("Aggregated Groups:       %d", nrow(aggregated_features)))
  message(sprintf("Grouping Dimensions:     %s", paste(group_cols, collapse = " + ")))
  message(sprintf("Features Aggregated:     %d", length(features_to_aggregate)))
  message(sprintf("Binary Features:         %d", length(binary_features)))
  message(sprintf("Output Columns:          %d", ncol(aggregated_features)))
  message("")
  message(sprintf("Output Table:            %s/df_precision_features",
                  basename(DB_PROCESSED)))
  message("════════════════════════════════════════════════════════════════════")
  message("")

  # === SHOW SAMPLE OUTPUT ===

  message("Sample Output (first 5 rows):")
  print(head(aggregated_features, 5))
  message("")

  return(list(
    success = TRUE,
    rows_written = nrow(aggregated_features),
    features_aggregated = length(features_to_aggregate),
    product_lines = PRODUCT_LINES,
    output_table = "df_precision_features"
  ))
}

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  result <- precision_drv_feature_preparation(con_transformed, con_processed)
  if (!is.null(result) && isTRUE(result$success)) {
    rows_processed <- result$rows_written
  }
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

    tables <- dbListTables(con_processed)
    if (!"df_precision_features" %in% tables) {
      stop("Output table df_precision_features not found")
    }
    message("  ✓ Output table exists")

    row_count <- tbl2(con_processed, "df_precision_features") %>%
      summarise(n = dplyr::n()) %>%
      collect() %>%
      dplyr::pull(n)

    if (row_count < 1) {
      stop("Output table df_precision_features is empty")
    }

    message(sprintf("  ✓ Rows written: %d", row_count))
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
message(sprintf("Script:           %s", "all_D04_09.R"))
message(sprintf("Platform:         all (cross-platform)"))
message(sprintf("Status:           %s", ifelse(test_passed, "SUCCESS", "FAILED")))
message(sprintf("Rows Processed:   %d", rows_processed))
message(sprintf("Execution Time:   %.2f seconds", as.numeric(execution_time)))
message("════════════════════════════════════════════════════════════════════")
message("")

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

if (exists("connection_created_transformed") && connection_created_transformed) {
  if (exists("con_transformed") && inherits(con_transformed, "DBIConnection")) {
    dbDisconnect(con_transformed, shutdown = TRUE)
    message("Disconnected from transformed_data database")
  }
}

if (exists("connection_created_processed") && connection_created_processed) {
  if (exists("con_processed") && inherits(con_processed, "DBIConnection")) {
    dbDisconnect(con_processed, shutdown = TRUE)
    message("Disconnected from processed_data database")
  }
}

# 5.2: Autodeinit (MUST be last statement)
autodeinit()
