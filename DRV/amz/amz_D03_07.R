#####
# CONSUMES: df_comment_property_ratingonly_by_asin_
# PRODUCES: df_comment_property_ratingonly_by_asin_*
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: amz_D03_06
#####


#' @title amz_D03_07
#' @description Derivation task
#' @business_rules See script comments for business logic.
#' @platform amz
#' @author MAMBA Development Team
#' @date 2025-12-30
#' @logical_step_id D03_04
#' @logical_step_status reassigned
#' @legacy_step_id D03_07

# amz_D03_07.R - Query Comment Property Ratings by ASIN for Amazon
# D03_07: Creates property ratings by ASIN for positioning analysis
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - R49: Apply Over Loops
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

# Source required utility functions with robust path resolution
process_comment_property_ratings_by_asin_path <- c(
  file.path("scripts", "global_scripts", "04_utils", "fn_process_comment_property_ratings_by_asin.R"),
  file.path("..", "global_scripts", "04_utils", "fn_process_comment_property_ratings_by_asin.R"),
  file.path("..", "..", "global_scripts", "04_utils", "fn_process_comment_property_ratings_by_asin.R"),
  file.path("..", "..", "..", "global_scripts", "04_utils", "fn_process_comment_property_ratings_by_asin.R"),
  file.path("..", "..", "..", "..", "global_scripts", "04_utils", "fn_process_comment_property_ratings_by_asin.R")
)
process_comment_property_ratings_by_asin_path <- process_comment_property_ratings_by_asin_path[file.exists(process_comment_property_ratings_by_asin_path)][1]
if (is.na(process_comment_property_ratings_by_asin_path)) {
  stop("fn_process_comment_property_ratings_by_asin.R not found in expected paths")
}
source(process_comment_property_ratings_by_asin_path)

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
failed_lines <- character()
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
  } else {
    failed_lines <- c(failed_lines, product_line_id_i)
  }
}

# Summary report
message("\n==============================")
message("Processing complete:")
message("- Total product lines processed: ", length(vec_product_line_id_noall))
message("- Successfully processed: ", success_count)
message("- Failed: ", length(vec_product_line_id_noall) - success_count)

# Output verification: Check created tables
missing_tables <- character()
message("\nVerifying created tables:")
for (product_line_id_i in vec_product_line_id_noall) {
  table_name <- paste0("df_comment_property_ratingonly_by_asin_", product_line_id_i)
  
  if (DBI::dbExistsTable(processed_data, table_name)) {
    # Count rows
    row_count <- sql_read(
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
    missing_tables <- c(missing_tables, table_name)
  }
}

# Enforce strict completion policy: missing outputs should fail
if (length(failed_lines) > 0) {
  stop(
    "D03_04 failed: missing source tables or processing errors for product lines: ",
    paste(failed_lines, collapse = ", ")
  )
}

if (length(missing_tables) > 0) {
  stop(
    "D03_04 failed: expected output table missing for product lines: ",
    paste(
      sub("^df_comment_property_ratingonly_by_asin_", "", missing_tables),
      collapse = ", "
    )
  )
}

if (success_count == 0L) {
  stop("D03_04 failed: no product line was successfully processed.")
}

# Clean up and disconnect
autodeinit()

# Log completion
message("\nAmazon comment property rating aggregation by ASIN completed for D03_04 step")
