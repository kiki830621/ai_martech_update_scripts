#!/usr/bin/env Rscript
#' Generate Time Series Filling Statistics Metadata
#' 
#' Documents time series completion and R117 transparency markers
#' Shows REAL vs FILLED data distribution per MP029 (No Fake Data)
#' 
#' @output metadata/time_series_filling_stats.csv

library(dplyr)
library(DBI)
library(duckdb)
library(tibble)

message("=======================================================")
message("GENERATING TIME SERIES FILLING STATISTICS")
message("=======================================================\n")

# ============================================================
# Initialize Paths
# ============================================================

source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

db_path <- if (exists("db_path_list", inherits = TRUE) &&
  !is.null(db_path_list$processed_data)) {
  db_path_list$processed_data
} else {
  file.path("data", "processed_data.duckdb")
}

metadata_dir <- "metadata"
if (!dir.exists(metadata_dir)) {
  dir.create(metadata_dir, recursive = TRUE)
}

# ============================================================
# Connect to Database
# ============================================================

if (!file.exists(db_path)) {
  message(sprintf("WARN: Database not found: %s", db_path))
  message("   Creating empty metadata file...")

  empty_metadata <- tibble(
    product_line = character(),
    country = character(),
    date_range_start = as.Date(character()),
    date_range_end = as.Date(character()),
    total_periods = integer(),
    real_periods = integer(),
    filled_periods = integer(),
    fill_rate = numeric(),
    filling_method = character(),
    timestamp = as.POSIXct(character())
  )

  write.csv(empty_metadata, file.path(metadata_dir, "time_series_filling_stats.csv"),
    row.names = FALSE)
  stop("Database not found. Empty metadata created.")
}

con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)

# ============================================================
# Check if Time Series Table Exists
# ============================================================

if (!dbExistsTable(con, "df_precision_time_series")) {
  message("WARN: df_precision_time_series table not found")
  message("   Creating empty metadata file...")

  empty_metadata <- tibble(
    product_line = character(),
    country = character(),
    date_range_start = as.Date(character()),
    date_range_end = as.Date(character()),
    total_periods = integer(),
    real_periods = integer(),
    filled_periods = integer(),
    fill_rate = numeric(),
    filling_method = character(),
    timestamp = as.POSIXct(character())
  )
  
  write.csv(empty_metadata, file.path(metadata_dir, "time_series_filling_stats.csv"),
    row.names = FALSE)
  dbDisconnect(con, shutdown = TRUE)
  stop("Time series table not found. Empty metadata created.")
}

# ============================================================
# Calculate Filling Statistics
# ============================================================

message("Calculating filling statistics by product line and country...")

table_columns <- dbListFields(con, "df_precision_time_series")
has_filling_method <- "filling_method" %in% table_columns

filling_method_sql <- if (has_filling_method) {
  paste0(
    "CASE ",
    "WHEN COUNT(DISTINCT filling_method) = 1 THEN MAX(filling_method) ",
    "WHEN COUNT(DISTINCT filling_method) = 0 THEN NULL ",
    "ELSE 'mixed' END AS filling_method"
  )
} else {
  "'unknown' AS filling_method"
}

query <- sprintf(
  "SELECT
     product_line,
     country,
     MIN(date) as date_range_start,
     MAX(date) as date_range_end,
     COUNT(*) as total_periods,
     SUM(CASE WHEN data_source = 'REAL' THEN 1 ELSE 0 END) as real_periods,
     SUM(CASE WHEN data_source = 'FILLED' THEN 1 ELSE 0 END) as filled_periods,
     CAST(SUM(CASE WHEN data_source = 'FILLED' THEN 1 ELSE 0 END) AS FLOAT) /
       CAST(COUNT(*) AS FLOAT) as fill_rate,
     %s
   FROM df_precision_time_series
   GROUP BY product_line, country
   ORDER BY product_line, country",
  filling_method_sql
)

filling_stats <- dbGetQuery(con, query)

# ============================================================
# Add Metadata
# ============================================================

message("Adding metadata...")

if (nrow(filling_stats) == 0) {
  write.csv(filling_stats, file.path(metadata_dir, "time_series_filling_stats.csv"),
    row.names = FALSE)
  dbDisconnect(con, shutdown = TRUE)
  message("No time series rows found. Empty metadata saved.")
  quit(status = 0)
}

filling_stats <- filling_stats %>%
  mutate(
    timestamp = Sys.time()
  )

# ============================================================
# Calculate Summary Statistics
# ============================================================

message("\nSummary Statistics:")

overall_stats <- filling_stats %>%
  summarize(
    total_combinations = n(),
    avg_fill_rate = mean(fill_rate, na.rm = TRUE),
    max_fill_rate = max(fill_rate, na.rm = TRUE),
    min_fill_rate = min(fill_rate, na.rm = TRUE),
    n_no_filling = sum(fill_rate == 0),
    n_high_filling = sum(fill_rate > 0.5)
  )

message(sprintf("  Total product-country combinations: %d", overall_stats$total_combinations))
message(sprintf("  Average fill rate: %.1f%%", overall_stats$avg_fill_rate * 100))
message(sprintf("  Fill rate range: %.1f%% to %.1f%%", 
               overall_stats$min_fill_rate * 100,
               overall_stats$max_fill_rate * 100))
message(sprintf("  Combinations with no filling: %d", overall_stats$n_no_filling))
message(sprintf("  Combinations with >50%% filling: %d", overall_stats$n_high_filling))

# R117 Compliance Check
if (overall_stats$avg_fill_rate > 0.80) {
  message("\nWARN: Average fill rate >80% - possible R117 violation")
  message("   Consider using more real data or shorter time windows")
}

# ============================================================
# Product Line Breakdown
# ============================================================

message("\nBy Product Line:")

product_stats <- filling_stats %>%
  group_by(product_line) %>%
  summarize(
    n_countries = n(),
    avg_fill_rate = mean(fill_rate, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(avg_fill_rate))

for (i in 1:nrow(product_stats)) {
  message(sprintf("  %s: %.1f%% fill rate (%d countries)",
                 product_stats$product_line[i],
                 product_stats$avg_fill_rate[i] * 100,
                 product_stats$n_countries[i]))
}

# ============================================================
# Save Metadata
# ============================================================

output_file <- file.path(metadata_dir, "time_series_filling_stats.csv")
write.csv(filling_stats, output_file, row.names = FALSE)

message(sprintf("\nPASS: Metadata saved to: %s", output_file))
message(sprintf("   Records: %d", nrow(filling_stats)))

# ============================================================
# Generate Warning Report for High Fill Rates
# ============================================================

high_fill <- filling_stats %>%
  filter(fill_rate > 0.80) %>%
  arrange(desc(fill_rate))

if (nrow(high_fill) > 0) {
  warning_file <- file.path(metadata_dir, "time_series_high_fill_warning.csv")
  write.csv(high_fill, warning_file, row.names = FALSE)
  
  message(sprintf("\nWARN: High fill rate warning file created: %s", warning_file))
  message(sprintf("   %d product-country combinations exceed 80%% fill threshold", nrow(high_fill)))
  
  message("\n   Top 5 highest fill rates:")
  for (i in 1:min(5, nrow(high_fill))) {
    message(sprintf("     %s - %s: %.1f%%",
                   high_fill$product_line[i],
                   high_fill$country[i],
                   high_fill$fill_rate[i] * 100))
  }
}

# ============================================================
# Cleanup
# ============================================================

dbDisconnect(con, shutdown = TRUE)

message("\n=======================================================")
message("TIME SERIES METADATA GENERATION COMPLETE")
message("=======================================================")
