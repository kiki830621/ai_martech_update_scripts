#!/usr/bin/env Rscript
# =============================================================================
# SCRIPT: consolidate_to_app_data.R
# PURPOSE: Consolidate all DRV outputs to appdata.duckdb for application consumption
# COMPLIANCE: MP110 Application Data Consolidation
# AUTHOR: MAMBA Framework
# CREATED: 2025-11-13
# =============================================================================

# MP110: APPLICATION DATA CONSOLIDATION
# This script implements Tier 4 (APP) of the five-tier architecture:
# 0IM → 1ST → 2TR → DRV → APP
#
# Purpose: Move all DRV outputs from analytical layer (processed_data.duckdb)
#          to application layer (appdata.duckdb) for production consumption

library(DBI)
library(duckdb)
library(data.table)

if (!exists("tbl2")) {
  source(file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"))
}

cat("\n")
cat("================================================================================\n")
cat("MP110: APPLICATION DATA CONSOLIDATION\n")
cat("================================================================================\n")
cat("Purpose: Consolidate DRV outputs to appdata.duckdb\n")
cat("Tier: 4 (APP) - Application consumption layer\n")
cat("Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("================================================================================\n\n")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Source databases (where DRV outputs are)
processed_paths <- c(
  "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/MAMBA/data/processed_data.duckdb",
  "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/MAMBA/data/local_data/processed_data.duckdb"
)

# Target database (where applications read from)
app_data_path <- "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/MAMBA/data/app_data/app_data.duckdb"

# Ensure target directory exists
app_data_dir <- dirname(app_data_path)
if (!dir.exists(app_data_dir)) {
  cat("Creating app_data directory:", app_data_dir, "\n")
  dir.create(app_data_dir, showWarnings = FALSE, recursive = TRUE)
}

# =============================================================================
# CONSOLIDATION PROCESS
# =============================================================================

# Connect to target database
cat("\n[1/4] Connecting to target database\n")
cat("  Path:", app_data_path, "\n")
con_app <- dbConnect(duckdb::duckdb(), app_data_path)

# Track consolidation statistics
total_tables_copied <- 0
total_rows_copied <- 0
consolidation_log <- list()

# Process each source database
cat("\n[2/4] Processing source databases\n")

for (i in seq_along(processed_paths)) {
  source_path <- processed_paths[i]

  cat("\n  Source", i, "of", length(processed_paths), "\n")
  cat("    Path:", source_path, "\n")

  # Check if source exists
  if (!file.exists(source_path)) {
    cat("    Status: NOT FOUND (skipping)\n")
    next
  }

  # Connect to source (read-only)
  con_src <- dbConnect(duckdb::duckdb(), source_path, read_only = TRUE)

  # Get all tables
  all_tables <- dbListTables(con_src)

  # R119: DRV tables now use df_ prefix like all datasets
  # However, some legacy tables may still have drv_ prefix
  # Filter for DRV outputs by pattern (both df_ and drv_ prefixes)
  # DRV tables match pattern: {df|drv}_{domain}_{entity} where entity includes:
  # product_features, time_series, poisson_analysis, etc.
  drv_pattern <- "^(df|drv)_(cbz|eby|precision)_(product_features|time_series|poisson_analysis)"
  drv_tables <- grep(drv_pattern, all_tables, value = TRUE)

  cat("    Total tables:", length(all_tables), "\n")
  cat("    DRV tables:", length(drv_tables), "\n")

  if (length(drv_tables) == 0) {
    cat("    Status: No DRV tables found (skipping)\n")
    dbDisconnect(con_src, shutdown = TRUE)
    next
  }

  # Copy each DRV table
  cat("\n    Copying DRV tables:\n")

  for (tbl in drv_tables) {
    cat("      -", tbl, "... ")

    tryCatch({
      # Read from source
      data <- dbReadTable(con_src, tbl)
      row_count <- nrow(data)
      size_kb <- as.numeric(object.size(data)) / 1024

      # Convert drv_ prefix to df_ prefix for app_data layer
      target_tbl <- sub("^drv_", "df_", tbl)

      # Write to app_data (overwrite if exists)
      dbWriteTable(con_app, target_tbl, data, overwrite = TRUE)

      if (tbl != target_tbl) {
        cat("OK (", row_count, " rows) [renamed: ", tbl, " → ", target_tbl, "]\n", sep = "")
      } else {
        cat("OK (", row_count, " rows)\n", sep = "")
      }

      # Track statistics
      total_tables_copied <- total_tables_copied + 1
      total_rows_copied <- total_rows_copied + row_count

      # Log details
      consolidation_log[[target_tbl]] <- list(
        source = basename(source_path),
        source_table = tbl,
        target_table = target_tbl,
        rows = row_count,
        size_kb = size_kb,
        columns = ncol(data),
        timestamp = Sys.time()
      )

    }, error = function(e) {
      cat("FAILED\n")
      cat("        Error:", conditionMessage(e), "\n")
    })
  }

  # Disconnect from source
  dbDisconnect(con_src, shutdown = TRUE)
  cat("    Status: Complete\n")
}

# =============================================================================
# VERIFICATION
# =============================================================================

cat("\n[3/4] Verifying appdata.duckdb\n")

# Get all tables in app_data
final_tables <- dbListTables(con_app)
cat("  Total tables in appdata.duckdb:", length(final_tables), "\n\n")

# Show summary of each table
cat("  Table Summary:\n")
cat("  ", strrep("-", 70), "\n", sep = "")
cat("  ", sprintf("%-40s %15s %10s", "Table Name", "Row Count", "Size (KB)"), "\n", sep = "")
cat("  ", strrep("-", 70), "\n", sep = "")

total_size_kb <- 0
for (tbl in sort(final_tables)) {
  # Get row count
  row_count <- tbl2(con_app, tbl) |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::pull(n)

  # Approximate size from copied data (if available)
  size_result <- NA_real_
  if (!is.null(consolidation_log[[tbl]]$size_kb)) {
    size_result <- consolidation_log[[tbl]]$size_kb
  }

  if (is.na(size_result)) {
    size_display <- "N/A"
  } else {
    total_size_kb <- total_size_kb + size_result
    size_display <- sprintf("%.1f", size_result)
  }

  cat("  ", sprintf("%-40s %15s %10s", tbl, format(row_count, big.mark = ","), size_display), "\n", sep = "")
}

cat("  ", strrep("-", 70), "\n", sep = "")
cat("  ", sprintf("%-40s %15s %10s", "TOTAL", format(total_rows_copied, big.mark = ","), sprintf("%.1f", total_size_kb)), "\n", sep = "")
cat("  ", strrep("-", 70), "\n", sep = "")

# =============================================================================
# CLEANUP
# =============================================================================

cat("\n[4/4] Cleanup\n")
dbDisconnect(con_app, shutdown = TRUE)
cat("  Disconnected from appdata.duckdb\n")

# =============================================================================
# FINAL REPORT
# =============================================================================

cat("\n")
cat("================================================================================\n")
cat("CONSOLIDATION COMPLETE\n")
cat("================================================================================\n")
cat("\n")
cat("Summary:\n")
cat("  Tables copied:", total_tables_copied, "\n")
cat("  Total rows:", format(total_rows_copied, big.mark = ","), "\n")
cat("  Target database:", app_data_path, "\n")
cat("\n")
cat("Status: SUCCESS\n")
cat("\n")
cat("Next Steps:\n")
cat("  1. Update applications to read from app_data/appdata.duckdb\n")
cat("  2. Verify application functionality\n")
cat("  3. Remove any direct references to processed_data.duckdb in apps\n")
cat("\n")
cat("Compliance:\n")
cat("  MP110: Application Data Consolidation - IMPLEMENTED\n")
cat("  Tier 4 (APP): Application consumption layer - ACTIVE\n")
cat("\n")
cat("================================================================================\n")
cat("\n")
