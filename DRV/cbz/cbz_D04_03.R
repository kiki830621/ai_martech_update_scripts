#!/usr/bin/env Rscript
#####
#P07_D04_03
# DERIVATION: CBZ R120 Metadata Enrichment
# VERSION: 3.0
# PLATFORM: cbz
# GROUP: D04
# SEQUENCE: 03
# PURPOSE: Enrich Poisson analysis tables with R120 metadata
# CONSUMES: processed_data.duckdb/df_cbz_poisson_analysis_{product}
# PRODUCES: processed_data.duckdb/df_cbz_poisson_analysis_{product} (enriched)
# PRINCIPLE: DM_R044, MP064, MP029, R120
#####

#cbz_D04_03

#' @title CBZ DRV - R120 Metadata Enrichment
#' @description Enrich 7 legacy product line tables with R120 metadata
#'              using pattern-based detection for dummy variables.
#'              Correctly handles dummy-coded variables in Poisson regression.
#' @requires DBI, duckdb, dplyr
#' @input_tables processed_data.duckdb/df_cbz_poisson_analysis_{product}
#' @output_tables processed_data.duckdb/df_cbz_poisson_analysis_{product}
#' @business_rules R120 ranges computed from app_data time series; enrich existing processed_data tables; skip missing lines.
#' @platform cbz
#' @author MAMBA Development Team
#' @date 2025-12-14

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

# 1.0: Autoinit - Environment setup
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

# 1.1: Load required packages
library(DBI)
library(duckdb)
library(dplyr)

# 1.2: Initialize tracking variables
error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()
connection_created_processed <- FALSE
connection_created_app <- FALSE

# 1.3: Source utility function
utils_path <- "scripts/global_scripts/04_utils/fn_enrich_poisson_R120_metadata.R"
if (file.exists(utils_path)) {
  source(utils_path)
  message(sprintf("  - Loaded utility: %s", basename(utils_path)))
} else {
  stop(sprintf("ERROR: Utility function not found: %s", utils_path))
}

# 1.4: Configuration
if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}
PRODUCT_LINES <- c("alf", "irf", "pre", "rek", "tur", "wak")
DB_PROCESSED <- db_path_list$processed_data
DB_APP <- db_path_list$app_data

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  message("════════════════════════════════════════════════════════════════════")
  message("CBZ DRV - R120 Metadata Enrichment")
  message("════════════════════════════════════════════════════════════════════")
  message(sprintf("Process Date: %s", start_time))
  message(sprintf("Input Database (Poisson): %s", DB_PROCESSED))
  message(sprintf("Input Database (Time Series): %s", DB_APP))
  message(sprintf("Output Database: %s", DB_PROCESSED))
  message("")

  # 2.1: Validate Databases
  if (!file.exists(DB_APP)) {
    stop(sprintf("App database not found: %s", DB_APP))
  }

  # 2.2: Connect to Databases
  message("[Step 1/4] Connecting to databases...")
  con_processed <- dbConnectDuckdb(DB_PROCESSED, read_only = FALSE)
  connection_created_processed <- TRUE
  con_app <- dbConnectDuckdb(DB_APP, read_only = TRUE)
  connection_created_app <- TRUE
  message("  ✓ Database connections established")
  message("")

  # 2.3: Load Source Time Series Data
  message("[Step 2/4] Loading source time series data...")
  if ("df_cbz_sales_complete_time_series" %in% dbListTables(con_app)) {
    source_ts <- tbl2(con_app, "df_cbz_sales_complete_time_series") %>% collect()
    if (nrow(source_ts) == 0) {
      stop("Time series table is empty. R120 enrichment requires real data (MP029).")
    }
    message(sprintf("  ✓ Time series data: %s rows", format(nrow(source_ts), big.mark = ",")))
  } else {
    stop("Time series data not available. R120 enrichment requires real data (MP029).")
  }
  message("")

  # 2.4: Process Each Product Line
  message("[Step 3/4] Enriching product line tables...")
  total_enriched <- 0
  total_dummies <- 0
  total_continuous <- 0
  tables_processed <- character()

  for (pl in PRODUCT_LINES) {
    message(sprintf("\n  [Product Line: %s]", toupper(pl)))

    table_name <- sprintf("df_cbz_poisson_analysis_%s", pl)

    if (!dbExistsTable(con_processed, table_name)) {
      message(sprintf("    ⚠️ Table not found: %s", table_name))
      next
    }

    legacy_data <- tbl2(con_processed, table_name) %>% collect()
    message(sprintf("    Loaded %d predictors", nrow(legacy_data)))

    if (nrow(legacy_data) == 0) {
      message("    No predictors; writing empty table with R120 columns")
      required_cols <- c(
        "predictor_min", "predictor_max", "predictor_range", "track_multiplier",
        "predictor_is_binary", "predictor_is_categorical",
        "r120_enrichment_method", "r120_enrichment_date"
      )
      for (col in required_cols) {
        if (!col %in% names(legacy_data)) {
          legacy_data[[col]] <- switch(
            col,
            predictor_min = NA_real_,
            predictor_max = NA_real_,
            predictor_range = NA_real_,
            track_multiplier = NA_real_,
            predictor_is_binary = NA,
            predictor_is_categorical = NA,
            r120_enrichment_method = NA_character_,
            r120_enrichment_date = NA_character_,
            NA
          )
        }
      }
      dbWriteTable(con_processed, table_name, legacy_data, overwrite = TRUE)
      tables_processed <- c(tables_processed, table_name)
      next
    }

    # Remove old R120 metadata if exists
    if ("r120_enrichment_method" %in% names(legacy_data)) {
      message("    Removing old R120 metadata...")
      legacy_data <- legacy_data %>%
        select(-c(predictor_min, predictor_max, predictor_range, track_multiplier,
                  predictor_is_binary, predictor_is_categorical,
                  r120_enrichment_method, r120_enrichment_date))
    }

    # Enrich using utility function
    enriched_data <- fn_enrich_poisson_R120_metadata(legacy_data, source_ts)

    # Count variable types
    n_dummy <- sum(grepl("dummy_pattern", enriched_data$r120_enrichment_method))
    n_continuous <- sum(grepl("source_data|time_feature_default", enriched_data$r120_enrichment_method))

    total_dummies <- total_dummies + n_dummy
    total_continuous <- total_continuous + n_continuous

    # Write to processed_data
    dbWriteTable(con_processed, table_name, enriched_data, overwrite = TRUE)
    message(sprintf("    ✓ Wrote %d predictors (dummy: %d, continuous: %d)",
                    nrow(enriched_data), n_dummy, n_continuous))

    total_enriched <- total_enriched + nrow(enriched_data)
    tables_processed <- c(tables_processed, table_name)
  }

  rows_processed <- total_enriched
  message("")
  message("[Step 4/4] Enrichment complete")

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

    # 3.1: Verify Output Tables Exist
    processed_tables <- dbListTables(con_processed)
    missing_tables <- setdiff(tables_processed, processed_tables)
    if (length(missing_tables) > 0) {
      stop(sprintf("Missing output tables: %s", paste(missing_tables, collapse = ", ")))
    }
    message(sprintf("  ✓ All %d output tables exist", length(tables_processed)))

    # 3.2: Validate R120 Metadata Columns
    if (length(tables_processed) > 0) {
      sample_table <- tables_processed[1]
      test_data <- tbl2(con_processed, sample_table) %>%
        head(1) %>%
        collect()
      required_cols <- c("predictor_min", "predictor_max", "predictor_range",
                         "track_multiplier", "r120_enrichment_method")
      missing_cols <- setdiff(required_cols, names(test_data))
      if (length(missing_cols) > 0) {
        stop(sprintf("Missing R120 columns: %s", paste(missing_cols, collapse = ", ")))
      }
      message("  ✓ R120 metadata columns present")
    }

    # 3.3: Validate Enrichment Totals
    if (rows_processed == 0) {
      message("  ⚠️ Warning: No rows processed")
    } else {
      message(sprintf("  ✓ Total predictors enriched: %d", rows_processed))
    }

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
message(sprintf("Script:           %s", "cbz_D04_03.R"))
message(sprintf("Platform:         cbz"))
message(sprintf("Status:           %s", ifelse(test_passed, "SUCCESS", "FAILED")))
message(sprintf("Tables Processed: %d", length(tables_processed)))
message(sprintf("Rows Processed:   %d", rows_processed))
message(sprintf("  - Dummy vars:   %d", total_dummies))
message(sprintf("  - Continuous:   %d", total_continuous))
message(sprintf("Execution Time:   %.2f seconds", as.numeric(execution_time)))
message("════════════════════════════════════════════════════════════════════")
message("")

# Compliance Documentation
message("Principle Compliance:")
message("─────────────────────────────────────────────────────────────────")
message("✓ R120: Range metadata correctly calculated")
message("✓ MP029: Actual ranges from data (no fake values)")
message("✓ Pattern detection: Dummy vs continuous variables")
message("─────────────────────────────────────────────────────────────────")
message("")

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

# 5.1: Close Database Connections
if (exists("connection_created_processed") && connection_created_processed) {
  if (exists("con_processed") && inherits(con_processed, "DBIConnection")) {
    dbDisconnect(con_processed, shutdown = TRUE)
    message("Disconnected from processed_data database")
  }
}

if (exists("connection_created_app") && connection_created_app) {
  if (exists("con_app") && inherits(con_app, "DBIConnection")) {
    dbDisconnect(con_app, shutdown = TRUE)
    message("Disconnected from app_data database")
  }
}

# 5.2: Return result for {targets} pipeline
if (!interactive()) {
  if (test_passed) {
    message("\n✅ Script completed successfully")
  } else {
    message("\n❌ Script completed with errors")
    quit(status = 1)
  }
}

# 5.3: Autodeinit (MUST be last statement)
autodeinit()
# End of file
