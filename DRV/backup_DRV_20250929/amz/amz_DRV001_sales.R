#' @file amz_D01_05.R
#' @requires DBI
#' @requires dplyr
#' @requires tidyr
#' @principle R007 Update Script Naming Convention
#' @principle R113 Update Script Structure
#' @principle R120 Filter Variable Naming Convention
#' @principle MP031 Initialization First
#' @principle MP033 Deinitialization Final
#' @principle MP047 Functional Programming
#' @platform AMZ (Amazon)
#' @author Original Author
#' @modified_by Claude
#' @date 2025-05-19
#' @title Create Dimensional Views
#' @description Creates customer-date aggregations and customer-level dimensional views for each product line filter
#'              using functional transformations

# 1. INITIALIZE
autoinit()

# Connect to required databases
connection_created_processed <- FALSE

if (!exists("processed_data") || !inherits(processed_data, "DBIConnection")) {
  processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)
  connection_created_processed <- TRUE
  message("Connected to processed_data database")
}

# Initialize error tracking
error_occurred <- FALSE
test_passed <- FALSE

# 2. MAIN
tryCatch({
  # Log script start
  message("Starting amz_D01_05: Creating dimensional views for each product line using functional transformations")

  # Check if the source table exists
  if (!dbExistsTable(processed_data, "df_amazon_sales_standardized")) {
    stop("Source table processed_data.df_amazon_sales_standardized does not exist")
  }
  
  # Check if transformation functions exist
  if (!exists("transform_sales_to_sales_by_customer_by_date") || !exists("transform_sales_by_customer_by_date_to_sales_by_customer")) {
    message("Loading transformation functions")
    source(file.path("../../../../global_scripts", "04_utils", "fn_sales_transformations.R"))
  }
  
  message("Using product lines: ", paste(vec_product_line_id, collapse = ", "))
  
  # Import standardized data once
  df_amazon_sales_standardized <- tbl(processed_data, "df_amazon_sales_standardized") %>% collect()
  message(sprintf("Loaded %d rows from processed_data.df_amazon_sales_standardized", 
                 nrow(df_amazon_sales_standardized)))
  
  # Initialize empty data frames to collect results
  combined_by_customer_by_date <- data.frame()
  combined_by_customer <- data.frame()
  
  # Process each product line filter separately
  for (current_product_line_id in vec_product_line_id) {
    message(sprintf("Processing product line filter: %s", current_product_line_id))
    
    # Filter data for this product line (if not "all")
    if (current_product_line_id == "all") {
      df_current_product_line <- df_amazon_sales_standardized
      message("Using all data for 'all' product line")
    } else {
      df_current_product_line <- df_amazon_sales_standardized %>%
        filter(product_line_id == current_product_line_id)
      message(sprintf("Filtered to %d rows for product line: %s", 
                     nrow(df_current_product_line), current_product_line_id))
    }
    
    # Skip if no data for this product line
    if (nrow(df_current_product_line) == 0) {
      message("No data available for product line: ", current_product_line_id, ". Skipping.")
      next
    }
    
    # Apply transformation functions to create dimensional views
    message("Applying transformation functions for product line: ", current_product_line_id)
    
    # Transform to customer-by-date view
    df_product_line_by_customer_by_date <- transform_sales_to_sales_by_customer.by_date(df_current_product_line) %>% 
      mutate(product_line_id_filter = current_product_line_id)
    
    # Transform to customer-level view
    df_product_line_by_customer <- transform_sales_by_customer.by_date_to_sales_by_customer(df_product_line_by_customer_by_date) %>% 
      mutate(product_line_id_filter = current_product_line_id)
    
    # Append results to combined dataframes
    combined_by_customer_by_date <- bind_rows(combined_by_customer_by_date, df_product_line_by_customer_by_date)
    combined_by_customer <- bind_rows(combined_by_customer, df_product_line_by_customer)
    
    message("Completed processing for product line: ", current_product_line_id)
  }
  
  # Save combined customer-by-date aggregation
  message("Saving combined customer-by-date aggregation with all product lines")
  message(sprintf("Total of %d rows across all product lines", nrow(combined_by_customer_by_date)))
  dbWriteTable(
    processed_data,
    "df_amazon_sales_by_customer_by_date",
    combined_by_customer_by_date,
    append = FALSE,
    overwrite = TRUE
  )
  
  # Save combined customer aggregation
  message("Saving combined customer-level aggregation with all product lines")
  message(sprintf("Total of %d rows across all product lines", nrow(combined_by_customer)))
  dbWriteTable(
    processed_data,
    "df_amazon_sales_by_customer",
    combined_by_customer,
    append = FALSE,
    overwrite = TRUE
  )
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Verify tables exist
    tables_to_check <- c("df_amazon_sales_by_customer", "df_amazon_sales_by_customer_by_date")
    all_tables_valid <- TRUE
    
    for (table_name in tables_to_check) {
      if (!dbExistsTable(processed_data, table_name)) {
        message("Verification failed: Table ", table_name, " does not exist")
        all_tables_valid <- FALSE
        next
      }
      
      # Check row count
      row_count <- tbl(processed_data, table_name) %>% count() %>% pull()
      
      if (row_count > 0) {
        message("Verification successful: ", row_count, " records found in ", table_name)
        
        # Check product line filter distribution
        product_line_counts <- tbl(processed_data, table_name) %>%
          group_by(product_line_id_filter) %>%
          summarize(count = n()) %>%
          arrange(desc(count)) %>%
          collect()
        
        message("Distribution by product line filter for ", table_name, ":")
        print(product_line_counts)
        
        # Verify all requested product lines are present
        if (exists("vec_product_line_id")) {
          processed_product_lines <- product_line_counts %>% pull(product_line_id_filter)
          missing_product_lines <- setdiff(vec_product_line_id, processed_product_lines)
          
          if (length(missing_product_lines) > 0) {
            message("Warning: The following product lines were not found in the results: ", 
                   paste(missing_product_lines, collapse = ", "))
          } else {
            message("All specified product lines were successfully processed")
          }
        }
        
        # Display a sample of the data for each product line
        if (exists("vec_product_line_id")) {
          for (pl in vec_product_line_id) {
            message(paste0("--- Product Line Filter: ", pl, " ---"))
            sample_data <- tbl(processed_data, table_name) %>%
              filter(product_line_id_filter == pl) %>%
              head(2) %>%
              collect()
            if (nrow(sample_data) > 0) {
              print(sample_data)
            } else {
              message("No data available for this product line filter")
            }
          }
        }
      } else {
        message("Verification failed: Table ", table_name, " exists but contains no records")
        all_tables_valid <- FALSE
      }
    }
    
    test_passed <- all_tables_valid
  }, error = function(e) {
    message("Error in TEST section: ", e$message)
    test_passed <- FALSE
  })
} else {
  message("Skipping tests due to error in MAIN section")
  test_passed <- FALSE
}

# 4. DEINITIALIZE
tryCatch({
  # Set final status before deinitialization
  if (test_passed) {
    message("Script executed successfully with all tests passed")
    final_status <- TRUE
  } else {
    message("Script execution incomplete or tests failed")
    final_status <- FALSE
  }
  
}, error = function(e) {
  message("Error in DEINITIALIZE section: ", e$message)
  final_status <- FALSE
}, finally = {
  # This will always execute
  message("Script execution completed at ", Sys.time())
})

# Return final status
if (exists("final_status")) {
  final_status
} else {
  FALSE
}

autodeinit()
