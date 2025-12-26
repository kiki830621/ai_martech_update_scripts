#!/usr/bin/env Rscript
# ==============================================================================
# Precision Marketing ETL - Stage 0IM (Import)
# ==============================================================================
#
# Purpose: Import product profile data from Google Sheets for precision marketing
# Stage: 0IM (Import - No transformations)
# Data Source: Google Sheets (public sheet)
# Output: raw_data.duckdb
#
# Principle Compliance:
# - MP108: Base ETL Pipeline Separation (0IM = Import only, no transformations)
# - MP029: No Fake Data Principle (import real data exactly as-is)
# - MP102: Completeness (add metadata: import_timestamp, product_line_id, data_source)
# - R116: Currency Standardization (deferred to 1ST stage, import raw currency)
#
# Product Lines: alf, irf, pre, rek, tur, wak
#
# Week 1 Implementation: MAMBA Precision Marketing Redesign
# Date: 2025-11-12
# ==============================================================================

library(googlesheets4)
library(duckdb)
library(dplyr)
library(tibble)

# ==============================================================================
# Configuration
# ==============================================================================

# Google Sheets configuration
SHEET_ID <- "1aKyyOMpIJtDtpqe7Iz0AfSU0W9aAdpSdPDD1zgnqO30"

# Product line mapping: Chinese sheet names → English IDs
# Based on product_line sheet in Google Sheets
PRODUCT_LINE_MAPPING <- list(
  "開罐器" = "electric_can_opener",
  "奶泡器" = "milk_frother",
  "研磨罐" = "salt_and_pepper_grinder",
  "鍋鏟" = "silicone_spatula",
  "撕肉爪" = "meat_claw",
  "油刷" = "pastry_brush"
)

# Sheet names to import (Chinese names as they appear in Google Sheets)
PRODUCT_LINES <- names(PRODUCT_LINE_MAPPING)

# Output database path (relative to project root)
OUTPUT_DB <- file.path("data", "raw_data.duckdb")

# ==============================================================================
# Main ETL 0IM Function
# ==============================================================================

precision_etl_0im <- function() {
  message("====================================================================")
  message("Precision Marketing ETL - Stage 0IM (Import)")
  message("====================================================================")
  message(sprintf("Import Date: %s", Sys.time()))
  message(sprintf("Data Source: Google Sheets (ID: %s)", SHEET_ID))
  message(sprintf("Product Lines: %s", paste(PRODUCT_LINES, collapse = ", ")))
  message("")

  # Authenticate with Google Sheets (use deauth for public sheets)
  gs4_deauth()
  message("✓ Google Sheets authentication complete (public mode)")

  # Connect to raw_data.duckdb
  con_raw <- dbConnect(duckdb::duckdb(), OUTPUT_DB)
  message(sprintf("✓ Connected to database: %s", OUTPUT_DB))
  message("")

  # Import each product line
  import_results <- list()

  for (pl_chinese in PRODUCT_LINES) {
    pl_english <- PRODUCT_LINE_MAPPING[[pl_chinese]]

    message(sprintf("===================================================================="))
    message(sprintf("Importing product line: %s (%s)", pl_chinese, pl_english))
    message(sprintf("===================================================================="))

    tryCatch({
      # Read from Google Sheets (using Chinese sheet name)
      message(sprintf("  → Reading sheet: %s", pl_chinese))
      raw_data <- read_sheet(SHEET_ID, sheet = pl_chinese)

      message(sprintf("  ✓ Retrieved %d rows, %d columns", nrow(raw_data), ncol(raw_data)))

      # Convert list columns to character to avoid DuckDB registration errors
      # (Google Sheets sometimes returns multi-value cells as lists)
      message("  → Converting list columns to character...")
      list_cols <- sapply(raw_data, is.list)
      if(any(list_cols)) {
        list_col_names <- names(raw_data)[list_cols]
        message(sprintf("    Found %d list columns: %s",
                       sum(list_cols),
                       paste(list_col_names, collapse = ", ")))

        for(col_name in list_col_names) {
          raw_data[[col_name]] <- sapply(raw_data[[col_name]], function(x) {
            if(is.null(x) || length(x) == 0) return(NA_character_)
            paste(as.character(x), collapse = "; ")
          })
        }
      }

      # Add metadata columns (MP102: Completeness)
      message("  → Adding metadata columns...")
      raw_data <- raw_data %>%
        mutate(
          import_timestamp = Sys.time(),
          product_line_id = pl_english,           # Use English ID
          product_line_name_chinese = pl_chinese,  # Keep Chinese name for reference
          data_source = "google_sheets"
        )

      # Write to raw table (MP108: No transformations in 0IM stage)
      # Use English ID for table name
      table_name <- sprintf("raw_precision_%s", pl_english)
      message(sprintf("  → Writing to table: %s", table_name))

      dbWriteTable(con_raw, table_name, raw_data, overwrite = TRUE)

      message(sprintf("  ✓ Successfully imported %d rows to %s", nrow(raw_data), table_name))

      # Store result
      import_results[[pl_english]] <- list(
        status = "success",
        rows = nrow(raw_data),
        columns = ncol(raw_data),
        table_name = table_name,
        chinese_name = pl_chinese
      )

    }, error = function(e) {
      message(sprintf("  ✗ ERROR importing %s (%s): %s", pl_chinese, pl_english, e$message))

      import_results[[pl_english]] <<- list(
        status = "error",
        error_message = e$message,
        chinese_name = pl_chinese
      )
    })

    message("")
  }

  # Disconnect from database
  dbDisconnect(con_raw, shutdown = TRUE)
  message("✓ Database connection closed")
  message("")

  # Print summary
  message("====================================================================")
  message("ETL 0IM Import Summary")
  message("====================================================================")

  success_count <- sum(sapply(import_results, function(x) x$status == "success"))
  error_count <- sum(sapply(import_results, function(x) x$status == "error"))

  message(sprintf("Total product lines: %d", length(PRODUCT_LINES)))
  message(sprintf("Successfully imported: %d", success_count))
  message(sprintf("Failed imports: %d", error_count))
  message("")

  if (success_count > 0) {
    message("Successful imports:")
    for (pl in names(import_results)) {
      result <- import_results[[pl]]
      if (result$status == "success") {
        message(sprintf("  ✓ %s: %d rows → %s",
                       pl, result$rows, result$table_name))
      }
    }
    message("")
  }

  if (error_count > 0) {
    message("Failed imports:")
    for (pl in names(import_results)) {
      result <- import_results[[pl]]
      if (result$status == "error") {
        message(sprintf("  ✗ %s: %s", pl, result$error_message))
      }
    }
    message("")
  }

  message("====================================================================")
  message("ETL 0IM Complete")
  message("====================================================================")
  message(sprintf("Output database: %s", OUTPUT_DB))
  message(sprintf("Next step: Run precision_ETL_product_profiles_1ST.R"))
  message("")

  return(invisible(import_results))
}

# ==============================================================================
# Execute if run as script
# ==============================================================================

if (!interactive()) {
  result <- precision_etl_0im()

  # Exit with appropriate status code
  error_count <- sum(sapply(result, function(x) x$status == "error"))
  if (error_count > 0) {
    quit(status = 1)
  } else {
    quit(status = 0)
  }
}
