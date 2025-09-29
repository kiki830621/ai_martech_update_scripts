# amz_D03_01.R - Comment Property Rating Analysis
# D03_01: Comment Property Rating Analysis (repositioned from legacy D03_06)
#
# This script creates property rating tables and performs wide-to-long transformation
# for comment property analysis. It reads from transformed_data and stores the long 
# format data in the comment_property_rating database.
#
# Data Flow:
# 1. Mount transformed_data database using dbAttachDuckdb
# 2. Read df_amz_review___transformed and df_all_comment_property___transformed
# 3. Perform wide-to-long transformation
# 4. Store results in comment_property_rating database
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - R49: Apply Over Loops
# - MP81: Explicit Parameter Specification
# - MP999: Simplified DuckDB Attach

# Initialize environment
needgoogledrive <- TRUE
autoinit()

# Connect to comment_property_rating database (main connection)
comment_property_rating <- dbConnectDuckdb(db_path_list$comment_property_rating, read_only = FALSE)

# Mount transformed_data database for reading
dbAttachDuckdb(
  con = comment_property_rating,
  path = db_path_list$transformed_data,
  alias = "transformed_data",
  read_only = TRUE
)

# Verify mounted database access
tryCatch({
  # Test if we can access the mounted tables
  test_query1 <- "SELECT COUNT(*) FROM transformed_data.df_amz_review___transformed"
  test_result1 <- dbGetQuery(comment_property_rating, test_query1)
  
  test_query2 <- "SELECT COUNT(*) FROM transformed_data.df_all_comment_property___transformed"
  test_result2 <- dbGetQuery(comment_property_rating, test_query2)
  
  message("Successfully mounted transformed_data database")
  message("Found ", test_result1[[1]], " reviews and ", test_result2[[1]], " properties")
}, error = function(e) {
  stop("Cannot access mounted tables from transformed_data: ", e$message)
})

# Execute the comment property ratings analysis with wide-to-long transformation
comment_sample_size <- 30  # Sample size for comment analysis
type_filter <- c("屬性")   # Filter for property types

# Process each product line
for (product_line_id_i in vec_product_line_id_noall) {
  
  # Log processing status
  message("Processing product line: ", product_line_id_i)
  
  # Read ALL review data from mounted transformed_data using tbl2
  # Note: ETL06 has standardized field names: review_title, review_body
  # Include ALL products (both competitor and non-competitor) for raw data
  df_review___all <- tbl2(comment_property_rating, "transformed_data.df_amz_review___transformed") %>% 
    filter(product_line_id == product_line_id_i) %>% 
    collect()
  
  message("Loaded ", nrow(df_review___all), " reviews from transformed_data (all products)")
  
  # Read property definitions from mounted transformed_data using tbl2
  df_comment_property___filtered <- tbl2(comment_property_rating, "transformed_data.df_all_comment_property___transformed") %>%
    filter(product_line_id == product_line_id_i) %>% 
    filter(type %in% !!type_filter) %>%
    collect()
  
  message("Loaded ", nrow(df_comment_property___filtered), " properties from transformed_data")
  
  # Use property names as column names (already processed)
  new_columns <- df_comment_property___filtered$property_name
  
  # Log the property columns being created
  message("Creating property columns: ", paste(new_columns, collapse = ", "))
  
  # Select core fields for SCD Type 2 append
  # Keep only essential fields to avoid append issues
  df_review___selected <- df_review___all %>%
    select(
      # Core identification fields
      platform_id,
      product_line_id,
      product_id,
      reviewer_id,
      # Review content fields (standardized names)
      review_date,
      review_title,
      review_body,
      rating,
      # Additional useful fields
      verified,
      helpful,
      url,
      style,
      # Competition flags
      included_competiter
    )
  
  # Create review data with new property columns (initialized to NA)
  df_review___new_columns <- df_review___selected %>% 
    arrange(product_id, review_date) %>%       # Sort by product ID and date
    group_by(product_id) %>%                   # Group by product ID
    mutate(
      # Add empty columns for each property
      !!!setNames(rep(list(NA_character_), length(new_columns)), new_columns)
    ) %>%
    ungroup()
  
  # Create raw table with property columns
  table_name_raw <- paste0("df_comment_property_rating_", product_line_id_i, "___raw")
  dbWriteTable(comment_property_rating, table_name_raw, df_review___new_columns, overwrite = TRUE)
  message("Created raw table: ", table_name_raw)
  
  # Create sampled data for analysis (using configurable comment_sample_size 
  # per product_id) - Filter to competitor products only at sampling stage
  df_review___sampled <- df_review___new_columns %>%
    filter(included_competiter == TRUE) %>%  # Filter competitors at sampling stage
    group_by(product_id) %>%
    slice_tail(n = comment_sample_size) %>%
    ungroup()
  
  # Log sample information
  unique_products <- unique(df_review___sampled$product_id)
  all_products_count <- length(unique(df_review___new_columns$product_id))
  competitor_products_count <- length(unique(df_review___new_columns$product_id[df_review___new_columns$included_competiter == TRUE]))
  
  message("Raw data: ", nrow(df_review___new_columns), " reviews from ", all_products_count, " products (all)")
  message("Sampled: ", nrow(df_review___sampled), " competitor reviews from ", 
          length(unique_products), " competitor products")
  message("Competitor ratio: ", competitor_products_count, "/", all_products_count, " products")
  message("Sample products: ", paste(head(unique_products, 5), collapse = ", "))
  
  # Calculate estimated processing time
  n_operations <- nrow(df_review___sampled) * length(new_columns)
  interval <- 10 / 20  # Seconds per iteration
  total_seconds <- n_operations * interval
  total_minutes <- total_seconds / 60
  
  time_display <- if (total_seconds < 60) {
    sprintf("%.0f seconds", total_seconds)
  } else {
    sprintf("%.1f minutes (%.0f seconds)", total_minutes, total_seconds)
  }
  
  message(sprintf(
    "Estimated processing time: %d operations × %.1f seconds = %s",
    n_operations, interval, time_display
  ))
  
  # Create sampled table
  table_name_sampled <- paste0("df_comment_property_rating_", 
                               product_line_id_i, "___sampled")
  dbWriteTable(comment_property_rating, table_name_sampled, 
               df_review___sampled, overwrite = TRUE)
  message("Created sampled table: ", table_name_sampled)
  
  # Create long-format data with property definitions for each review
  df_review___long <- df_review___sampled %>%
    tidyr::pivot_longer(
      cols      = tidyselect::all_of(new_columns),      # Columns to expand
      names_to  = "property_name",                      # New column: property name
      values_to = "result"                              # New column: result value
    ) %>%
    # Join with property definitions but select only essential fields
    left_join(
      df_comment_property___filtered %>%
        select(
          property_name,
          property_id,
          property_name_english,
          type,
          definition,
          product_line_id
        )
    )
  
  # Create long-format table for analysis
  table_name_long <- paste0("df_comment_property_rating_", 
                            product_line_id_i, "___sampled_long")
  dbWriteTable(comment_property_rating, table_name_long, 
               df_review___long, overwrite = TRUE)
  message("Created long-format table: ", table_name_long)
  
  # Log completion for this product line
  message("Completed processing for product line: ", product_line_id_i)
  message("Generated tables: ", 
          paste(c(table_name_raw, table_name_sampled, table_name_long), 
                collapse = ", "))
}

# Clean up and disconnect
autodeinit()

# Log completion
message("Amazon comment property rating analysis completed successfully for D03_01")
message("Wide-to-long transformation completed")
message("Long format data stored in comment_property_rating database")
message("Data source: transformed_data (mounted via dbAttachDuckdb)")