#' @file amz_D01_04.R
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
#' @title Create Customer Profile
#' @description Creates customer profiles from standardized Amazon sales data for application use

# 1. INITIALIZE
autoinit()

# Connect to required databases
connection_created_processed <- FALSE
connection_created_app <- FALSE

if (!exists("processed_data") || !inherits(processed_data, "DBIConnection")) {
  processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = TRUE)
  connection_created_processed <- TRUE
  message("Connected to processed_data database (read-only)")
}

if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
  app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
  connection_created_app <- TRUE
  message("Connected to app_data database")
}

# Initialize error tracking
error_occurred <- FALSE
test_passed <- FALSE

# 2. MAIN
tryCatch({
  # Log script start
  message("Starting amz_D01_04: Creating customer profiles")

  # Check if the source table exists
  if (!dbExistsTable(processed_data, "df_amazon_sales_standardized")) {
    stop("Source table processed_data.df_amazon_sales_standardized does not exist")
  }
  
  # Import standardized data
  df_amazon_sales_standardized <- tbl(processed_data, "df_amazon_sales_standardized") %>% collect()
  message(sprintf("Loaded %d rows from processed_data.df_amazon_sales_standardized", 
                 nrow(df_amazon_sales_standardized)))
  
  # Create customer profile (independent of product line)
  message("Creating customer profile")
  df_customer_profile <- df_amazon_sales_standardized %>%
    mutate(
      buyer_name = as.character(customer_id),
      email = ship_postal_code
    ) %>% 
    select(customer_id, buyer_name, email, platform_id) %>%   
    distinct(customer_id, platform_id, .keep_all=TRUE) %>% 
    arrange(customer_id)
  
  message(sprintf("Created %d unique customer profiles", nrow(df_customer_profile)))
  
  # Save customer profile
  message("Saving customer profile to app_data.df_customer_profile")
  dbWriteTable(
    app_data,
    "df_customer_profile",
    df_customer_profile,
    append = TRUE
  )
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Check for customer profile records
    if (!dbExistsTable(app_data, "df_customer_profile")) {
      message("Verification failed: Table df_customer_profile does not exist")
      test_passed <- FALSE
    } else {
      customer_count <- tbl(app_data, "df_customer_profile") %>%
        filter(platform_id == "amz") %>%
        count() %>%
        pull()
      
      if (customer_count > 0) {
        message("Verification successful: ", customer_count, " Amazon customer profiles created")
        
        # Display a sample of the customer profile data
        customer_sample <- tbl(app_data, "df_customer_profile") %>%
          filter(platform_id == "amz") %>%
          head(5) %>%
          collect()
        
        message("Sample customer profiles:")
        print(customer_sample)
        
        # Check for duplicates
        duplicate_check <- tbl(app_data, "df_customer_profile") %>%
          filter(platform_id == "amz") %>%
          group_by(customer_id, platform_id) %>%
          summarize(count = n(), .groups = "drop") %>%
          filter(count > 1) %>%
          collect()
        
        if (nrow(duplicate_check) > 0) {
          message("Warning: Found ", nrow(duplicate_check), " duplicate customer profiles")
          print(duplicate_check)
        } else {
          message("No duplicate customer profiles found")
        }
        
        test_passed <- TRUE
      } else {
        message("Verification failed: No Amazon customer profiles found")
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