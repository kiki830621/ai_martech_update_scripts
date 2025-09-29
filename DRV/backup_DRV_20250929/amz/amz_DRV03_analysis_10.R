# amz_D03_10.R - Import Competitor Sales Data for Amazon
# D03_10: Imports sales data for competitor products
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - MP81: Explicit Parameter Specification

# Initialize environment
needgoogledrive <- TRUE
autoinit()

# Connect to databases with appropriate access
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

# Define source directory for competitor sales data
competitor_sales_dir <- file.path(RAW_DATA_DIR, "competitor_sales")

# Log beginning of process
message("Starting D03_10 (Import Competitor Sales Data) for Amazon product lines")
message("Reading data from: ", competitor_sales_dir)

# Import competitor sales data
import_df_amz_competitor_sales(
  main_folder = competitor_sales_dir,
  db_connection = raw_data
)

# Verify imported data
message("\nVerifying imported data:")
sales_count <- DBI::dbGetQuery(
  raw_data,
  "SELECT COUNT(*) AS count FROM df_amz_competitor_sales"
)[1, 1]

asin_count <- DBI::dbGetQuery(
  raw_data,
  "SELECT COUNT(DISTINCT asin) AS count FROM df_amz_competitor_sales"
)[1, 1]

product_line_count <- DBI::dbGetQuery(
  raw_data,
  "SELECT COUNT(DISTINCT product_line_id) AS count FROM df_amz_competitor_sales"
)[1, 1]

message("- Total sales records: ", sales_count)
message("- Unique ASINs: ", asin_count)
message("- Product lines: ", product_line_count)

# Show sample of imported data
message("\nSample of imported data:")
sample_data <- DBI::dbGetQuery(
  raw_data,
  "SELECT asin, date, product_line_id, sales FROM df_amz_competitor_sales LIMIT 5"
)
print(sample_data)

# Clean up and disconnect
autodeinit()

# Log completion
message("\nAmazon competitor sales data import completed successfully for D03_10 step")