#!/usr/bin/env Rscript
# ==============================================================================
# Precision Marketing ETL - Stage 1ST (Standardization)
# ==============================================================================
#
# Purpose: Standardize product profile data for precision marketing
# Stage: 1ST (Standardization - data types, currency, naming)
# Input: raw_data.duckdb (from 0IM stage)
# Output: staged_data.duckdb
#
# Principle Compliance:
# - MP108: Base ETL Pipeline Separation (1ST = Standardization only)
# - R116: Currency Standardization (convert all prices to USD)
# - MP102: Completeness (preserve original values + metadata)
# - R078: Column Naming for Operations (standardize attribute names)
# - MP029: No Fake Data Principle (use real exchange rates)
#
# Standardization Tasks:
# 1. Currency standardization (R116) → USD conversion with audit trail
# 2. Variable name standardization (MP102) → snake_case convention
# 3. Data type standardization → proper R data types
# 4. Dimension extraction → country from currency code
#
# Week 1 Implementation: MAMBA Precision Marketing Redesign
# Date: 2025-11-12
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
library(duckdb)
library(dplyr)
library(tibble)

# Source utility functions
source(file.path("scripts", "global_scripts", "04_utils", "fn_convert_currency_to_usd.R"))
source(file.path("scripts", "global_scripts", "04_utils", "fn_standardize_attribute_names.R"))

# ==============================================================================
# Configuration
# ==============================================================================

# Product lines to process
# Product lines (use English IDs that match 0IM output table names)
PRODUCT_LINES <- c(
  "electric_can_opener",
  "milk_frother",
  "salt_and_pepper_grinder",
  "silicone_spatula",
  "meat_claw",
  "pastry_brush"
)

# Database paths
INPUT_DB <- file.path("data", "raw_data.duckdb")
OUTPUT_DB <- file.path("data", "staged_data.duckdb")

# Exchange rate configuration
RATE_SOURCE <- "FIXED"  # Use fixed rates for Week 1
RATE_DATE <- Sys.Date()

# ==============================================================================
# Helper Functions
# ==============================================================================

#' Extract Country from Currency Code
#'
#' Maps ISO 4217 currency codes to ISO 3166-1 alpha-3 country codes
#'
#' @param currency_code ISO 4217 currency code (e.g., "USD", "EUR", "TWD")
#' @return ISO 3166-1 alpha-3 country code (e.g., "USA", "DEU", "TWN")
#'
extract_country_from_currency <- function(currency_code) {

  # Currency to country mapping (ISO 4217 → ISO 3166-1)
  currency_to_country <- list(
    USD = "USA",    # United States
    EUR = "DEU",    # Germany (Eurozone representative)
    GBP = "GBR",    # United Kingdom
    TWD = "TWN",    # Taiwan
    AUD = "AUS",    # Australia
    CAD = "CAN",    # Canada
    JPY = "JPN",    # Japan
    CNY = "CHN",    # China
    KRW = "KOR",    # South Korea
    SGD = "SGP",    # Singapore
    HKD = "HKG",    # Hong Kong
    INR = "IND",    # India
    MXN = "MEX",    # Mexico
    BRL = "BRA",    # Brazil
    ZAR = "ZAF"     # South Africa
  )

  # Map currency codes to countries
  countries <- sapply(currency_code, function(curr) {
    if (is.na(curr)) {
      return("UNK")  # Unknown
    }

    country <- currency_to_country[[curr]]
    if (is.null(country)) {
      return("UNK")  # Unknown currency
    }

    return(country)
  })

  return(unname(countries))
}

#' Standardize Data Types
#'
#' Converts columns to appropriate R data types
#'
#' @param data Data frame to standardize
#' @return Data frame with standardized data types
#'
standardize_data_types <- function(data) {

  # Numeric columns
  numeric_patterns <- c("price", "rating", "review_count", "quantity", "amount", "rate")
  for (pattern in numeric_patterns) {
    matching_cols <- grep(pattern, names(data), value = TRUE)
    for (col in matching_cols) {
      if (!is.numeric(data[[col]])) {
        tryCatch({
          data[[col]] <- as.numeric(data[[col]])
        }, warning = function(w) {
          message(sprintf("  WARNING: Could not convert %s to numeric", col))
        })
      }
    }
  }

  # Date columns
  date_patterns <- c("date", "_dt", "timestamp")
  for (pattern in date_patterns) {
    matching_cols <- grep(pattern, names(data), value = TRUE, ignore.case = TRUE)
    for (col in matching_cols) {
      if (!inherits(data[[col]], "Date") && !inherits(data[[col]], "POSIXct")) {
        tryCatch({
          data[[col]] <- as.Date(data[[col]])
        }, error = function(e) {
          # Try POSIXct if Date fails
          tryCatch({
            data[[col]] <- as.POSIXct(data[[col]])
          }, error = function(e2) {
            message(sprintf("  WARNING: Could not convert %s to date/datetime", col))
          })
        })
      }
    }
  }

  # Character columns (ensure proper encoding)
  char_cols <- sapply(data, is.character)
  if (any(char_cols)) {
    for (col in names(data)[char_cols]) {
      # Only apply enc2utf8 to non-NA values
      tryCatch({
        non_na_idx <- !is.na(data[[col]])
        if(any(non_na_idx)) {
          data[[col]][non_na_idx] <- enc2utf8(data[[col]][non_na_idx])
        }
      }, error = function(e) {
        message(sprintf("  WARNING: Could not convert %s to UTF-8: %s", col, e$message))
      })
    }
  }

  return(data)
}

# ==============================================================================
# Main ETL 1ST Function
# ==============================================================================

precision_etl_1st <- function() {
  message("====================================================================")
  message("Precision Marketing ETL - Stage 1ST (Standardization)")
  message("====================================================================")
  message(sprintf("Process Date: %s", Sys.time()))
  message(sprintf("Input Database: %s", INPUT_DB))
  message(sprintf("Output Database: %s", OUTPUT_DB))
  message(sprintf("Product Lines: %s", paste(PRODUCT_LINES, collapse = ", ")))
  message("")

  # Check input database exists
  if (!file.exists(INPUT_DB)) {
    stop(sprintf("ERROR: Input database not found: %s\nRun precision_ETL_product_profiles_0IM.R first.", INPUT_DB))
  }

  # Connect to databases
  con_raw <- dbConnect(duckdb::duckdb(), INPUT_DB, read_only = TRUE)
  con_staged <- dbConnect(duckdb::duckdb(), OUTPUT_DB)

  message("✓ Database connections established")
  message("")

  # Process each product line
  staging_results <- list()

  for (pl in PRODUCT_LINES) {
    message("====================================================================")
    message(sprintf("Processing product line: %s", pl))
    message("====================================================================")

    tryCatch({
      # Read raw data
      raw_table <- sprintf("raw_precision_%s", pl)
      message(sprintf("  → Reading from: %s", raw_table))

      raw_data <- tbl2(con_raw, raw_table) %>% collect()
      message(sprintf("  ✓ Retrieved %d rows", nrow(raw_data)))

      # === STANDARDIZATION TASKS ===

      # Task 1: Variable Name Standardization (MP102)
      message("  → Standardizing attribute names...")
      staged_data <- fn_standardize_attribute_names_domain(
        raw_data,
        domain = "product",
        preserve_original = TRUE
      )

      # Task 2: Data Type Standardization
      message("  → Standardizing data types...")
      staged_data <- standardize_data_types(staged_data)

      # Task 3: Currency Standardization (R116) ⭐ CRITICAL
      message("  → Applying R116: Currency standardization...")

      # Check if price and currency columns exist
      has_price <- "price" %in% names(staged_data)
      has_currency <- "currency" %in% names(staged_data)

      if (has_price && has_currency) {
        staged_data <- fn_convert_currency_to_usd(
          data = staged_data,
          price_col = "price",
          currency_col = "currency",
          rate_source = RATE_SOURCE,
          rate_date = RATE_DATE
        )

        # Validate currency conversion
        validate_currency_conversion(staged_data)

      } else {
        warning(sprintf("  R116 WARNING: %s does not have price/currency columns. Skipping currency conversion.", pl))
      }

      # Task 4: Extract Country Dimension from Currency
      if ("original_currency" %in% names(staged_data)) {
        message("  → Extracting country dimension...")
        staged_data$country <- extract_country_from_currency(staged_data$original_currency)
      }

      # Task 5: Add Staging Metadata
      staged_data$staging_timestamp <- Sys.time()
      staged_data$staging_version <- "1.0"

      # Validate staged data
      message("  → Validating staged data...")
      validate_attribute_names(staged_data)

      # Write to staged database
      staged_table <- sprintf("staged_precision_%s", pl)
      message(sprintf("  → Writing to: %s", staged_table))

      dbWriteTable(con_staged, staged_table, staged_data, overwrite = TRUE)

      message(sprintf("  ✓ Successfully staged %d rows to %s", nrow(staged_data), staged_table))

      # Store result
      staging_results[[pl]] <- list(
        status = "success",
        rows = nrow(staged_data),
        columns = ncol(staged_data),
        has_currency = has_price && has_currency,
        table_name = staged_table
      )

    }, error = function(e) {
      message(sprintf("  ✗ ERROR processing %s: %s", pl, e$message))

      staging_results[[pl]] <<- list(
        status = "error",
        error_message = e$message
      )
    })

    message("")
  }

  # Disconnect from databases
  dbDisconnect(con_raw, shutdown = FALSE)
  dbDisconnect(con_staged, shutdown = TRUE)
  message("✓ Database connections closed")
  message("")

  # Print summary
  message("====================================================================")
  message("ETL 1ST Standardization Summary")
  message("====================================================================")

  success_count <- sum(sapply(staging_results, function(x) x$status == "success"))
  error_count <- sum(sapply(staging_results, function(x) x$status == "error"))

  message(sprintf("Total product lines: %d", length(PRODUCT_LINES)))
  message(sprintf("Successfully staged: %d", success_count))
  message(sprintf("Failed staging: %d", error_count))
  message("")

  if (success_count > 0) {
    message("Successful staging:")
    for (pl in names(staging_results)) {
      result <- staging_results[[pl]]
      if (result$status == "success") {
        currency_status <- if (result$has_currency) "✓ R116 applied" else "○ No currency"
        message(sprintf("  ✓ %s: %d rows, %d cols → %s (%s)",
                       pl, result$rows, result$columns, result$table_name, currency_status))
      }
    }
    message("")
  }

  if (error_count > 0) {
    message("Failed staging:")
    for (pl in names(staging_results)) {
      result <- staging_results[[pl]]
      if (result$status == "error") {
        message(sprintf("  ✗ %s: %s", pl, result$error_message))
      }
    }
    message("")
  }

  message("====================================================================")
  message("ETL 1ST Complete")
  message("====================================================================")
  message(sprintf("Output database: %s", OUTPUT_DB))
  message(sprintf("Principle compliance: R116 (Currency), MP102 (Standardization), MP108 (Stage Separation)"))
  message(sprintf("Next step: Run precision_ETL_product_profiles_2TR.R"))
  message("")

  return(invisible(staging_results))
}

# ==============================================================================
# Execute if run as script
# ==============================================================================

if (!interactive()) {
  result <- precision_etl_1st()

  # Exit with appropriate status code
  error_count <- sum(sapply(result, function(x) x$status == "error"))
  if (error_count > 0) {
    quit(status = 1)
  } else {
    quit(status = 0)
  }
}
