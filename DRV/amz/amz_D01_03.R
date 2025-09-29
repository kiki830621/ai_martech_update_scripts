#' @file amz_D01_03.R
#' @requires DBI
#' @requires dplyr
#' @requires tidyr
#' @principle R007 Update Script Naming Convention
#' @principle R113 Update Script Structure
#' @principle R120 Filter Variable Naming Convention
#' @principle MP031 Initialization First
#' @principle MP033 Deinitialization Final
#' @platform AMZ (Amazon)
#' @author Original Author
#' @modified_by Claude
#' @date 2025-05-19
#' @title Standardize Amazon Sales Data
#' @description Standardizes processed Amazon sales data by ensuring consistent field naming
#'              and required data types for DNA analysis

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
  message("Starting amz_D01_03: Standardizing field names in processed_data.df_amazon_sales")

  # Check if the source table exists
  if (!dbExistsTable(processed_data, "df_amazon_sales")) {
    stop("Source table processed_data.df_amazon_sales does not exist")
  }
  
  # Import processed data
  df_amazon_sales <- tbl(processed_data, "df_amazon_sales") %>% collect()
  message(sprintf("Loaded %d rows from processed_data.df_amazon_sales", nrow(df_amazon_sales)))
  
  df_product_profile_dictionary <- tbl(processed_data, "df_product_profile_dictionary") %>% collect()
  
  # Standardize field names required for DNA analysis
  df_amazon_sales_standardized <- df_amazon_sales %>% 
    rename(
      lineproduct_price = product_price,
      payment_time = time
    ) %>% 
    mutate(
      # Generate customer_id from available data
      customer_id = as.integer(as.factor(ship_postal_code))
    ) %>% 
    drop_na(customer_id) %>%
    # Add platform identifier
    mutate(
      platform_id = "amz"
    ) %>% left_join(
      df_product_profile_dictionary, 
      by = "asin"
    ) 
  
  # Save standardized data
  message("Saving standardized data to processed_data.df_amazon_sales_standardized")
  dbWriteTable(
    processed_data,
    "df_amazon_sales_standardized",
    df_amazon_sales_standardized,
    append = FALSE,
    overwrite = TRUE
  )
  
  # Note: Customer profile creation is moved to D01_04.R as per D01 derivation flow
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Verify standardized table exists
    if (!dbExistsTable(processed_data, "df_amazon_sales_standardized")) {
      message("Verification failed: Table df_amazon_sales_standardized does not exist")
      test_passed <- FALSE
    } else {
      # Check for standardized field names
      standardized_data <- tbl(processed_data, "df_amazon_sales_standardized") %>%
        head(5) %>%
        collect()
      
      required_columns <- c("customer_id", "lineproduct_price", "payment_time", "platform_id", "product_line_id")
      missing_columns <- setdiff(required_columns, colnames(standardized_data))
      
      if (length(missing_columns) == 0) {
        message("Verification successful: All required standardized columns found")
        
        # Display a sample of the standardized data
        print(standardized_data %>% select(customer_id, lineproduct_price, payment_time, platform_id, product_line_id))
        
        # Check if product_line_id was properly joined
        product_line_distribution <- tbl(processed_data, "df_amazon_sales_standardized") %>%
          group_by(product_line_id) %>%
          summarize(count = n()) %>%
          arrange(desc(count)) %>%
          collect()
        
        message("Product line distribution:")
        print(product_line_distribution)
        
        test_passed <- TRUE
      } else {
        message("Verification failed: Missing required columns: ", paste(missing_columns, collapse=", "))
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
