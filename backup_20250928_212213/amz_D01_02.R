#' @file amz_D01_02.R
#' @requires DBI
#' @requires dplyr
#' @requires stringr
#' @principle R007 Update Script Naming Convention
#' @principle R113 Update Script Structure
#' @principle MP031 Initialization First
#' @principle MP033 Deinitialization Final
#' @platform AMZ (Amazon)
#' @author Original Author
#' @modified_by Claude
#' @date 2025-05-18
#' @title Preprocess Cleansed Amazon Sales Data
#' @description Standardizes cleansed Amazon sales data by enforcing data type consistency and
#'              applying business rules, then stores in the processed database

# 1. INITIALIZE
autoinit()

# Connect to required databases
connection_created_cleansed <- FALSE
connection_created_processed <- FALSE

if (!exists("cleansed_data") || !inherits(cleansed_data, "DBIConnection")) {
  cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = TRUE)
  connection_created_cleansed <- TRUE
  message("Connected to cleansed_data database (read-only)")
}

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
  message("Starting amz_D01_02: Standardizing cleansed_data.df_amazon_sales to processed_data.df_amazon_sales")

  # Check if the source table exists
  if (!dbExistsTable(cleansed_data, "df_amazon_sales")) {
    stop("Source table cleansed_data.df_amazon_sales does not exist")
  }
  
  # Import cleansed data
  cleansed_amazon_sales <- tbl(cleansed_data, "df_amazon_sales") %>% collect()
  message(sprintf("Loaded %d rows from cleansed_data.df_amazon_sales", nrow(cleansed_amazon_sales)))
  
  # Standardization operations
  standardized_data <- cleansed_amazon_sales %>%
    # Enforce data type consistency
    mutate(
      # Ensure time is in proper datetime format
      time = as.POSIXct(time),
      # Convert amount/price fields to numeric values
      product_price = as.numeric(product_price)) %>%
    # Handle missing values with appropriate imputation
    mutate(
      product_price = ifelse(is.na(product_price) | product_price < 0, 0, product_price)
    ) %>%
    # Remove invalid records
    filter(!is.na(time))
  
  # Write the standardized data to the target table
  dbWriteTable(processed_data, "df_amazon_sales", standardized_data, overwrite = TRUE)
  message(sprintf("Successfully wrote %d rows to processed_data.df_amazon_sales", nrow(standardized_data)))
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Verify table exists in processed_data
    if (!dbExistsTable(processed_data, "df_amazon_sales")) {
      message("Verification failed: Table df_amazon_sales does not exist in processed_data")
      test_passed <- FALSE
    } else {
      # Count records
      processed_row_count <- tbl(processed_data, "df_amazon_sales") %>% count() %>% pull()
      cleansed_row_count <- tbl(cleansed_data, "df_amazon_sales") %>% count() %>% pull()
      
      if (processed_row_count > 0) {
        message("Verification successful: ", processed_row_count, " records found in processed_data.df_amazon_sales")
        
        # Check for valid row count relationship
        if (processed_row_count <= cleansed_row_count) {
          message("Validation passed: Processed data has ", processed_row_count, 
                 " rows, which is less than or equal to cleansed data's ", cleansed_row_count, " rows")
        } else {
          message("Validation warning: Processed data has more rows than cleansed data, which is unexpected")
        }
        
        # Display a sample of the data for verification
        sample_data <- tbl(processed_data, "df_amazon_sales") %>% head(5) %>% collect()
        print(sample_data)
        
        test_passed <- TRUE
      } else {
        message("Verification failed: Table exists but contains no records")
        test_passed <- FALSE
      }
    }
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
