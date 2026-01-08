#!/usr/bin/env Rscript
# ============================================================================
# Export app_data.duckdb to Parquet files for Posit Connect deployment
#
# Purpose:
#   - DuckDB files (109MB) exceed Git LFS limitations for Posit Connect
#   - Parquet files (~20MB total) can be tracked directly in git
#   - App uses dual-mode: DuckDB locally, Parquet on Posit Connect
#
# Usage:
#   Rscript scripts/update_scripts/utilities/export_app_data_to_parquet.R
#
# Following Principles:
#   - DM_R056: Posit Connect Deployment Assets (app_data as deployment-ready)
#   - MP064: ETL-Derivation Separation (this is a utility, not ETL)
# ============================================================================

# ===== INITIALIZE =====
library(DBI)
library(duckdb)

# Paths
duckdb_path <- "data/app_data/app_data.duckdb"
parquet_dir <- "data/app_data/parquet"

# ===== VALIDATION =====
if (!file.exists(duckdb_path)) {
  stop("❌ DuckDB file not found: ", duckdb_path)
}

cat("📦 Exporting app_data.duckdb to Parquet format\n")
cat("   Source: ", duckdb_path, "\n")
cat("   Target: ", parquet_dir, "/\n\n")

# ===== MAIN =====
# Create parquet directory
dir.create(parquet_dir, showWarnings = FALSE, recursive = TRUE)

# Connect to DuckDB
con <- dbConnect(duckdb::duckdb(), duckdb_path, read_only = TRUE)

# Get all tables
tables <- dbListTables(con)
cat("Found", length(tables), "tables to export:\n")

# Export each table
total_size <- 0
for (t in tables) {
  path <- file.path(parquet_dir, paste0(t, ".parquet"))

  # Get row count
  row_count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", t))$n

  # Export using ZSTD compression for better compression ratio
  query <- sprintf("COPY %s TO '%s' (FORMAT PARQUET, COMPRESSION 'zstd')", t, path)
  dbExecute(con, query)

  # Get file size
  file_size <- file.info(path)$size
  total_size <- total_size + file_size

  cat(sprintf("  ✅ %s: %s rows → %.2f MB\n",
              t, format(row_count, big.mark = ","), file_size / 1024^2))
}

# ===== SUMMARY =====
cat("\n============================\n")
cat("📊 Export Summary\n")
cat("============================\n")
cat(sprintf("Tables exported: %d\n", length(tables)))
cat(sprintf("Total Parquet size: %.2f MB\n", total_size / 1024^2))

duckdb_size <- file.info(duckdb_path)$size
cat(sprintf("Original DuckDB size: %.2f MB\n", duckdb_size / 1024^2))
cat(sprintf("Compression ratio: %.1f%%\n", (1 - total_size / duckdb_size) * 100))

cat("\n✅ Export complete!\n")
cat("\nNext steps:\n")
cat("  1. git add data/app_data/parquet/\n")
cat("  2. git commit -m '[DEPLOY] Export app_data to Parquet for Posit Connect'\n")
cat("  3. git push\n")

# ===== CLEANUP =====
dbDisconnect(con)
