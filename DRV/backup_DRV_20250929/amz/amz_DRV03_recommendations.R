# amz_D03_04.R - Query Comment Property Ratings by ASIN for Amazon
# D03_04: Creates property ratings by ASIN for positioning analysis
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - R49: Apply Over Loops
# - MP81: Explicit Parameter Specification

# Initialize environment
needgoogledrive <- TRUE
autoinit()

# Source required utility functions
source(file.path("../../../../global_scripts", "04_utils", "fn_process_comment_property_ratings_by_asin.R"))

# Define utility functions for this script
safe_mean <- function(x) {
  m <- mean(x, na.rm = TRUE)
  if (is.nan(m)) NA_real_ else m
}

replace_nan <- function(df) {
  df[] <- lapply(df, function(col) if (is.numeric(col)) na_if(col, NaN) else col)
  df
}

# Connect to databases with appropriate access
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)
app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
comment_property_rating <- dbConnectDuckdb(db_path_list$comment_property_rating, read_only = FALSE)
comment_property_rating_results <- dbConnectDuckdb(db_path_list$comment_property_rating_results, read_only = FALSE)

# Log beginning of process
message("Starting D03_04 (Query Comment Property Ratings by ASIN) for Amazon product lines")

# Configuration parameters
impute_missing <- TRUE  # Whether to impute missing values
m <- 5                 # Number of imputations for mice
maxit <- 20            # Maximum iterations for mice
seed <- 500            # Random seed for reproducibility

# Process each product line
success_count <- 0
for (product_line_id_i in vec_product_line_id_noall) {
  message("\n==============================")
  
  # Process the product line
  success <- process_comment_property_ratings_by_asin(
    processed_data = processed_data,
    product_line_id = product_line_id_i,
    impute_missing = impute_missing,
    m = m,
    maxit = maxit,
    seed = seed
  )
  
  # Track successful processing
  if (success) {
    success_count <- success_count + 1
  }
}

# Summary report
message("\n==============================")
message("Processing complete:")
message("- Total product lines processed: ", length(vec_product_line_id_noall))
message("- Successfully processed: ", success_count)
message("- Failed: ", length(vec_product_line_id_noall) - success_count)

# Output verification: Check created tables
message("\nVerifying created tables:")
for (product_line_id_i in vec_product_line_id_noall) {
  table_name <- paste0("df_comment_property_ratingonly_by_asin_", product_line_id_i)
  
  if (DBI::dbExistsTable(processed_data, table_name)) {
    # Count rows
    row_count <- DBI::dbGetQuery(
      processed_data,
      paste0("SELECT COUNT(*) FROM ", table_name)
    )[1,1]
    
    # Count columns
    col_count <- length(DBI::dbListFields(processed_data, table_name))
    
    # Check for specific columns
    asin_exists <- "asin" %in% DBI::dbListFields(processed_data, table_name)
    
    message("- Table ", table_name, ": ", 
            row_count, " rows, ", 
            col_count, " columns",
            if (!asin_exists) " (WARNING: missing 'asin' column)" else "")
  } else {
    message("- Table not found: ", table_name)
  }
}

# Clean up and disconnect
autodeinit()

# Log completion
message("\nAmazon comment property rating aggregation by ASIN completed for D03_04 step")