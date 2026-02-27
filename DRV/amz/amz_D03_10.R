#####
# CONSUMES: df_amz_competitor_sales
# PRODUCES: df_amz_competitor_sales
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: amz_D03_09
#####


#' @title amz_D03_10
#' @description Derivation task
#' @business_rules See script comments for business logic.
#' @platform amz
#' @author MAMBA Development Team
#' @date 2025-12-30
#' @logical_step_id D03_10
#' @logical_step_status implemented

# amz_D03_10.R - Import Competitor Sales Data for Amazon
# D03_10: Import competitor sales data for positioning analysis
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - MP81: Explicit Parameter Specification

# Initialize environment
sql_read_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R")
)
sql_read_path <- sql_read_candidates[file.exists(sql_read_candidates)][1]
if (is.na(sql_read_path)) {
  stop("fn_sql_read.R not found in expected paths")
}
source(sql_read_path)
needgoogledrive <- TRUE
autoinit()

# Connect to databases with appropriate access
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

# Define source directory for competitor sales data
competitor_sales_dir <- file.path(RAW_DATA_DIR, "competitor_sales")

# Log beginning of process
message("Starting D03_10 (Import Competitor Sales Data) for Amazon product lines")
message("Reading data from: ", competitor_sales_dir)
if (!dir.exists(competitor_sales_dir)) {
  stop("VALIDATE FAILED: competitor_sales directory does not exist: ", competitor_sales_dir)
}

# Import competitor sales data
import_result <- core_import_df_amz_competitor_sales(
  main_folder = competitor_sales_dir,
  db_connection = raw_data
)
imported_count <- if (
  is.list(import_result) && !is.null(import_result$total_rows_imported)
) {
  import_result$total_rows_imported
} else {
  import_result
}
if (is.null(imported_count) || is.na(imported_count) || imported_count == 0L) {
  stop("VALIDATE FAILED: D03_10 competitor sales import returned no rows")
}
if (is.list(import_result)) {
  if (length(import_result$skipped_folders_invalid_reference) > 0L) {
    message(
      "D03_10: invalid/unmatched product-line folders (skipped): ",
      paste(import_result$skipped_folders_invalid_reference, collapse = ", ")
    )
  }
  if (length(import_result$skipped_folders_no_supported_files) > 0L) {
    message(
      "D03_10: no supported files in these folders (skipped): ",
      paste(import_result$skipped_folders_no_supported_files, collapse = ", ")
    )
  }
  if (length(import_result$skipped_folders_no_rows) > 0L) {
    message(
      "D03_10: supported files found but no imported rows in these folders: ",
      paste(import_result$skipped_folders_no_rows, collapse = ", ")
    )
  }
}

# Verify imported data
message("\nVerifying imported data:")
if (!("df_amz_competitor_sales" %in% dbListTables(raw_data))) {
  stop("VALIDATE FAILED: table df_amz_competitor_sales is missing after import")
}
sales_count <- sql_read(
  raw_data,
  "SELECT COUNT(*) AS count FROM df_amz_competitor_sales"
)[1, 1]
if (is.na(sales_count) || sales_count == 0L) {
  stop("VALIDATE FAILED: df_amz_competitor_sales is empty after D03_10 import")
}

asin_count <- sql_read(
  raw_data,
  "SELECT COUNT(DISTINCT asin) AS count FROM df_amz_competitor_sales"
)[1, 1]

product_line_count <- sql_read(
  raw_data,
  "SELECT COUNT(DISTINCT product_line_id) AS count FROM df_amz_competitor_sales"
)[1, 1]

message("- Total sales records: ", sales_count)
message("- Unique ASINs: ", asin_count)
message("- Product lines: ", product_line_count)

# Show sample of imported data
message("\nSample of imported data:")
sample_data <- sql_read(
  raw_data,
  "SELECT asin, date, product_line_id, sales FROM df_amz_competitor_sales LIMIT 5"
)
print(sample_data)

# Clean up and disconnect
autodeinit()

# Log completion
message("\nAmazon competitor sales data import completed successfully for D03_10 step")
