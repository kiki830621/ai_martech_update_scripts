#!/usr/bin/env Rscript

#' ---
#' title: "Rename DRV Tables to DF Prefix"
#' description: "Migration script to rename all drv_ prefixed tables to df_ prefix"
#' author: "principle-product-manager"
#' date: "2025-11-13"
#' implements: "R119 - Universal df_ Prefix for All Datasets"
#' ---

#' ## Purpose
#'
#' This script renames all tables with `df_` prefix to `df_` prefix in:
#' 1. processed_data.duckdb (if exists)
#' 2. app_data/app_data.duckdb
#'
#' ## Rationale
#'
#' Per R119, ALL datasets must use `df_` prefix regardless of source layer.
#' The legacy `drv_` prefix incorrectly identifies SOURCE (derivation layer)
#' rather than TYPE (dataset/data frame).
#'
#' ## Safety
#'
#' - Verifies row counts before and after rename
#' - Reports any discrepancies
#' - Generates detailed migration report

library(DBI)
library(duckdb)

# Ensure tbl2 is available (DM_R023)
if (!exists("tbl2")) {
  source(file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"))
}

# Utility: Repeat string operator
`%R%` <- function(x, n) paste(rep(x, n), collapse = "")

cat("\n")
cat("=" %R% 60, "\n", sep = "")
cat("DRV to DF Table Renaming Migration\n")
cat("Implements: R119 - Universal df_ Prefix for All Datasets\n")
cat("Date:", as.character(Sys.time()), "\n")
cat("=" %R% 60, "\n\n", sep = "")

#' Rename df_ tables in a database
#'
#' @param db_path Path to DuckDB database
#' @param db_name Human-readable name for reporting
#' @return List with migration details
rename_drv_tables <- function(db_path, db_name) {
  cat("\n")
  cat("-" %R% 60, "\n", sep = "")
  cat("Database:", db_name, "\n")
  cat("Path:", db_path, "\n")
  cat("-" %R% 60, "\n", sep = "")

  # Check if database exists
  if (!file.exists(db_path)) {
    cat("⚠️  Database not found, skipping\n")
    return(list(
      db_name = db_name,
      db_path = db_path,
      status = "skipped",
      reason = "file_not_found",
      tables_renamed = 0
    ))
  }

  # Connect to database
  con <- dbConnect(duckdb::duckdb(), db_path)

  # Get all tables
  all_tables <- dbListTables(con)
  cat("Total tables:", length(all_tables), "\n")

  # Find legacy drv_ prefixed tables
  drv_tables <- grep("^drv_", all_tables, value = TRUE)
  cat("Tables with drv_ prefix:", length(drv_tables), "\n")

  if (length(drv_tables) == 0) {
    cat("✓ No drv_ tables found - database already compliant\n")
    dbDisconnect(con, shutdown = TRUE)
    return(list(
      db_name = db_name,
      db_path = db_path,
      status = "already_compliant",
      tables_renamed = 0,
      all_tables = all_tables
    ))
  }

  cat("\nTables to rename:\n")
  print(drv_tables)
  cat("\n")

  # Rename each table
  rename_results <- list()

  for (old_name in drv_tables) {
    # New name: replace drv_ with df_
    new_name <- sub("^drv_", "df_", old_name)

    tryCatch({
      # Get row count before rename
      count_before <- tbl2(con, old_name) |>
        dplyr::summarise(n = dplyr::n()) |>
        dplyr::pull(n)

      # Rename table
      dbExecute(
        con,
        sprintf("ALTER TABLE %s RENAME TO %s", old_name, new_name)
      )

      # Verify row count after rename
      count_after <- tbl2(con, new_name) |>
        dplyr::summarise(n = dplyr::n()) |>
        dplyr::pull(n)

      # Check for data loss
      data_loss <- (count_before != count_after)

      if (data_loss) {
        status_icon <- "❌"
        status_msg <- sprintf(
          "ROW COUNT MISMATCH: before=%d, after=%d",
          count_before, count_after
        )
      } else {
        status_icon <- "✓"
        status_msg <- "OK"
      }

      cat(sprintf(
        "%s Renamed: %s -> %s (%d rows) %s\n",
        status_icon, old_name, new_name, count_after, status_msg
      ))

      rename_results[[old_name]] <- list(
        old_name = old_name,
        new_name = new_name,
        count_before = count_before,
        count_after = count_after,
        data_loss = data_loss,
        status = ifelse(data_loss, "warning", "success")
      )

    }, error = function(e) {
      cat(sprintf("❌ ERROR renaming %s: %s\n", old_name, e$message))
      rename_results[[old_name]] <- list(
        old_name = old_name,
        new_name = new_name,
        error = e$message,
        status = "error"
      )
    })
  }

  # Get final table list
  final_tables <- dbListTables(con)

  dbDisconnect(con, shutdown = TRUE)

  cat("\n✓ Migration complete for", db_name, "\n")
  cat("  Tables renamed:", length(rename_results), "\n")

  return(list(
    db_name = db_name,
    db_path = db_path,
    status = "completed",
    tables_renamed = length(rename_results),
    rename_details = rename_results,
    initial_tables = all_tables,
    final_tables = final_tables
  ))
}

# Execute migrations
migrations <- list()

# Migration 1: processed_data.duckdb
migrations$processed_data <- rename_drv_tables(
  db_path = "data/processed_data/processed_data.duckdb",
  db_name = "processed_data"
)

# Migration 2: app_data.duckdb
migrations$app_data <- rename_drv_tables(
  db_path = "data/app_data/app_data.duckdb",
  db_name = "app_data"
)

# Summary Report
cat("\n")
cat("=" %R% 60, "\n", sep = "")
cat("MIGRATION SUMMARY\n")
cat("=" %R% 60, "\n", sep = "")

total_renamed <- 0
total_errors <- 0
total_warnings <- 0

for (db in names(migrations)) {
  result <- migrations[[db]]
  cat("\n", result$db_name, ":\n", sep = "")
  cat("  Status:", result$status, "\n")
  cat("  Tables renamed:", result$tables_renamed, "\n")

  total_renamed <- total_renamed + result$tables_renamed

  # Count errors and warnings
  if (!is.null(result$rename_details)) {
    errors <- sum(sapply(result$rename_details, function(x) x$status == "error"))
    warnings <- sum(sapply(result$rename_details, function(x) x$status == "warning"))

    if (errors > 0) {
      cat("  ❌ Errors:", errors, "\n")
      total_errors <- total_errors + errors
    }
    if (warnings > 0) {
      cat("  ⚠️  Warnings:", warnings, "\n")
      total_warnings <- total_warnings + warnings
    }
  }
}

cat("\n")
cat("Total tables renamed:", total_renamed, "\n")
cat("Total errors:", total_errors, "\n")
cat("Total warnings:", total_warnings, "\n")

if (total_errors == 0 && total_warnings == 0) {
  cat("\n✅ Migration completed successfully with no issues\n")
} else if (total_errors > 0) {
  cat("\n❌ Migration completed with ERRORS - manual review required\n")
} else {
  cat("\n⚠️  Migration completed with WARNINGS - please review\n")
}

# Save migration report
report_path <- sprintf(
  "scripts/global_scripts/00_principles/CHANGELOG/2025-11-13_drv_to_df_migration_report.txt"
)

cat("\nSaving migration report to:", report_path, "\n")

report_content <- capture.output({
  cat("=" %R% 60, "\n", sep = "")
  cat("DRV to DF Table Renaming Migration Report\n")
  cat("Date:", as.character(Sys.time()), "\n")
  cat("Implements: R119 - Universal df_ Prefix for All Datasets\n")
  cat("=" %R% 60, "\n\n", sep = "")

  for (db in names(migrations)) {
    result <- migrations[[db]]
    cat("\n")
    cat(result$db_name, "\n")
    cat("-" %R% 40, "\n", sep = "")
    cat("Path:", result$db_path, "\n")
    cat("Status:", result$status, "\n")
    cat("Tables renamed:", result$tables_renamed, "\n\n")

    if (!is.null(result$rename_details)) {
      cat("Rename Details:\n")
      for (item in result$rename_details) {
        cat(sprintf(
          "  %s -> %s: %s\n",
          item$old_name,
          item$new_name,
          item$status
        ))
        if (!is.null(item$count_before)) {
          cat(sprintf(
            "    Rows: before=%d, after=%d\n",
            item$count_before,
            item$count_after
          ))
        }
        if (!is.null(item$error)) {
          cat(sprintf("    Error: %s\n", item$error))
        }
      }
    }

    if (!is.null(result$final_tables)) {
      cat("\nFinal table list:\n")
      print(result$final_tables)
    }
  }

  cat("\n")
  cat("=" %R% 60, "\n", sep = "")
  cat("Summary\n")
  cat("=" %R% 60, "\n", sep = "")
  cat("Total tables renamed:", total_renamed, "\n")
  cat("Total errors:", total_errors, "\n")
  cat("Total warnings:", total_warnings, "\n")

  if (total_errors == 0 && total_warnings == 0) {
    cat("\n✅ Migration successful\n")
  } else {
    cat("\n⚠️  Review required\n")
  }
})

writeLines(report_content, report_path)

cat("\n✓ Report saved\n")

# Return migration results (for programmatic use)
invisible(migrations)
