#' Upload app_data DuckDB tables to Supabase PostgreSQL
#'
#' @description
#' This script synchronizes data from local DuckDB (app_data.duckdb) to Supabase.
#' It creates tables in Supabase and uploads all data, handling type conversions.
#'
#' Usage:
#'   Rscript scripts/update_scripts/utilities/upload_app_data_to_supabase.R
#'
#' Required environment variables:
#'   - SUPABASE_DB_HOST
#'   - SUPABASE_DB_PASSWORD
#'
#' Following Principles:
#'   - MP029: No Fake Data (uses real production data only)
#'   - DM_R023: Universal DBI Approach
#'   - MP064: ETL-Derivation Separation
#'
#' @author MAMBA Team
#' @date 2026-01-25

# ==============================================================================
# INITIALIZE
# ==============================================================================

cat("=== DuckDB to Supabase Data Upload ===\n\n")
cat("Start time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Load required packages
required_packages <- c("DBI", "duckdb", "RPostgres", "dplyr", "jsonlite")

for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat("Installing package:", pkg, "\n")
    install.packages(pkg)
  }
  suppressPackageStartupMessages(library(pkg, character.only = TRUE))
}

# Source Supabase connection function
source("scripts/global_scripts/02_db_utils/supabase/fn_dbConnectSupabase.R")

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# DuckDB source path
DUCKDB_PATH <- "data/app_data/app_data.duckdb"

# Tables to exclude from upload (backups, empty tables, etc.)
EXCLUDE_PATTERNS <- c(
  "_backup_",        # Backup tables
  "^df_precision_"   # Empty precision tables
)

# Tables that need special handling (large tables)
LARGE_TABLE_THRESHOLD <- 50000  # rows - upload in batches

# Batch size for large tables
BATCH_SIZE <- 10000

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

#' Check if table should be excluded
should_exclude <- function(table_name) {
  for (pattern in EXCLUDE_PATTERNS) {
    if (grepl(pattern, table_name)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

#' Convert DuckDB types to PostgreSQL compatible types
#' Some DuckDB types need conversion for PostgreSQL compatibility
prepare_for_postgres <- function(df) {
  # Convert list columns to JSON strings
  for (col in names(df)) {
    if (is.list(df[[col]])) {
      df[[col]] <- sapply(df[[col]], function(x) {
        if (is.null(x) || length(x) == 0) {
          NA_character_
        } else {
          jsonlite::toJSON(x, auto_unbox = TRUE)
        }
      })
    }
    # Handle POSIXlt (convert to POSIXct)
    if (inherits(df[[col]], "POSIXlt")) {
      df[[col]] <- as.POSIXct(df[[col]])
    }
    # Handle difftime (convert to numeric seconds)
    # DuckDB's difftime can have out-of-range values for PostgreSQL
    if (inherits(df[[col]], "difftime")) {
      df[[col]] <- as.numeric(df[[col]], units = "secs")
    }
    # Handle integer64 (convert to numeric to avoid %d format issues)
    if (inherits(df[[col]], "integer64")) {
      df[[col]] <- as.numeric(df[[col]])
    }
  }
  return(df)
}

#' Upload table to Supabase
upload_table <- function(con_duck, con_supa, table_name, verbose = TRUE) {
  tryCatch({
    # Get row count
    row_count <- dbGetQuery(con_duck, sprintf(
      'SELECT COUNT(*) as n FROM "%s"', table_name
    ))$n

    if (row_count == 0) {
      if (verbose) cat("  Skipping (empty table)\n")
      return(list(success = TRUE, rows = 0, message = "Empty table skipped"))
    }

    if (verbose) cat(sprintf("  Rows: %s\n", format(row_count, scientific = FALSE)))

    # Drop existing table in Supabase if exists
    tryCatch({
      dbExecute(con_supa, sprintf('DROP TABLE IF EXISTS "%s" CASCADE', table_name))
    }, error = function(e) {
      # Ignore drop errors
    })

    # For large tables, upload in batches
    if (row_count > LARGE_TABLE_THRESHOLD) {
      if (verbose) cat(sprintf("  Large table - uploading in batches of %d\n", BATCH_SIZE))

      # Get data in batches
      offset <- 0
      first_batch <- TRUE

      while (offset < row_count) {
        query <- sprintf(
          'SELECT * FROM "%s" LIMIT %d OFFSET %d',
          table_name, BATCH_SIZE, offset
        )
        batch_data <- dbGetQuery(con_duck, query)

        if (nrow(batch_data) == 0) break

        # Prepare data for PostgreSQL
        batch_data <- prepare_for_postgres(batch_data)

        if (first_batch) {
          # First batch - create table
          dbWriteTable(con_supa, table_name, batch_data,
                       overwrite = TRUE, row.names = FALSE)
          first_batch <- FALSE
        } else {
          # Subsequent batches - append
          dbWriteTable(con_supa, table_name, batch_data,
                       append = TRUE, row.names = FALSE)
        }

        offset <- offset + BATCH_SIZE
        if (verbose) {
          progress <- min(offset, row_count)
          cat(sprintf("    Uploaded %d/%d rows (%.1f%%)\n",
                      progress, row_count, 100 * progress / row_count))
        }
      }

    } else {
      # Small table - upload at once
      df <- dbGetQuery(con_duck, sprintf('SELECT * FROM "%s"', table_name))
      df <- prepare_for_postgres(df)
      dbWriteTable(con_supa, table_name, df, overwrite = TRUE, row.names = FALSE)
    }

    # Verify upload
    uploaded_count <- dbGetQuery(con_supa, sprintf(
      'SELECT COUNT(*) as n FROM "%s"', table_name
    ))$n

    if (uploaded_count == row_count) {
      if (verbose) cat(sprintf("  ✓ Verified: %s rows uploaded\n", format(uploaded_count, scientific = FALSE)))
      return(list(success = TRUE, rows = uploaded_count, message = "Success"))
    } else {
      warning_msg <- sprintf("Row count mismatch: expected %s, got %s",
                             format(row_count, scientific = FALSE),
                             format(uploaded_count, scientific = FALSE))
      if (verbose) cat(sprintf("  ⚠ Warning: %s\n", warning_msg))
      return(list(success = TRUE, rows = uploaded_count, message = warning_msg))
    }

  }, error = function(e) {
    if (verbose) cat(sprintf("  ✗ Error: %s\n", e$message))
    return(list(success = FALSE, rows = 0, message = e$message))
  })
}

# ==============================================================================
# MAIN
# ==============================================================================

main <- function() {
  # Connect to DuckDB
  cat("Connecting to DuckDB:", DUCKDB_PATH, "\n")
  if (!file.exists(DUCKDB_PATH)) {
    stop("DuckDB file not found: ", DUCKDB_PATH)
  }
  con_duck <- dbConnect(duckdb::duckdb(), DUCKDB_PATH, read_only = TRUE)
  cat("✓ Connected to DuckDB\n\n")

  # Connect to Supabase
  cat("Connecting to Supabase...\n")
  con_supa <- dbConnectSupabase(verbose = TRUE)
  cat("\n")

  # Get list of tables
  tables <- dbListTables(con_duck)
  cat(sprintf("Found %d tables in DuckDB\n\n", length(tables)))

  # Filter tables
  tables_to_upload <- tables[!sapply(tables, should_exclude)]
  cat(sprintf("Tables to upload (after filtering): %d\n", length(tables_to_upload)))

  excluded <- setdiff(tables, tables_to_upload)
  if (length(excluded) > 0) {
    cat("Excluded tables:\n")
    for (t in excluded) {
      cat(sprintf("  - %s\n", t))
    }
  }
  cat("\n")

  # Upload each table
  results <- list()
  total_rows <- 0

  for (i in seq_along(tables_to_upload)) {
    table_name <- tables_to_upload[i]
    cat(sprintf("[%d/%d] Uploading: %s\n", i, length(tables_to_upload), table_name))

    result <- upload_table(con_duck, con_supa, table_name)
    results[[table_name]] <- result

    if (result$success) {
      total_rows <- total_rows + result$rows
    }
    cat("\n")
  }

  # Summary
  cat("=== Upload Summary ===\n\n")

  success_count <- sum(sapply(results, function(x) x$success))
  fail_count <- length(results) - success_count

  cat(sprintf("Total tables: %d\n", length(results)))
  cat(sprintf("Successful: %d\n", success_count))
  cat(sprintf("Failed: %d\n", fail_count))
  cat(sprintf("Total rows uploaded: %s\n", format(total_rows, big.mark = ",")))

  if (fail_count > 0) {
    cat("\nFailed tables:\n")
    for (name in names(results)) {
      if (!results[[name]]$success) {
        cat(sprintf("  - %s: %s\n", name, results[[name]]$message))
      }
    }
  }

  # Disconnect
  dbDisconnect(con_duck)
  dbDisconnect(con_supa)

  cat("\n✓ Upload complete\n")
  cat("End time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

  return(invisible(results))
}

# ==============================================================================
# RUN
# ==============================================================================

if (!interactive()) {
  results <- main()
}
