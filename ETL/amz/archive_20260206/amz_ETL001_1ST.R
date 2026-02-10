#' @file amz_D01_01.R
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
#' @title Cleanse Raw Amazon Sales Data
#' @description Transforms raw Amazon sales data by standardizing formats, cleaning text fields,
#'              and removing invalid entries, then stores in the cleansed database

# 1. INITIALIZE
autoinit()

# Connect to required databases if not already connected
connection_created_raw <- FALSE
connection_created_cleansed <- FALSE

if (!exists("raw_data") || !inherits(raw_data, "DBIConnection")) {
  raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
  connection_created_raw <- TRUE
  message("Connected to raw_data database (read-only)")
}

if (!exists("cleansed_data") || !inherits(cleansed_data, "DBIConnection")) {
  cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = FALSE)
  connection_created_cleansed <- TRUE
  message("Connected to cleansed_data database")
}

# Initialize error tracking
error_occurred <- FALSE
test_passed <- FALSE
df_amazon_sales___cleansed <- NULL

# 2. MAIN
tryCatch({
  # Log script start
  message("Starting amz_D01_01: Cleansing raw_data.df_amazon_sales to cleansed_data.df_amazon_sales")

  # Check if the source table exists
  if (!dbExistsTable(raw_data, "df_amazon_sales")) {
    stop("Source table raw_data.df_amazon_sales does not exist")
  }
  
  # Import raw data
  raw_amazon_sales <- tbl(raw_data, "df_amazon_sales") %>% collect()
  message(sprintf("Loaded %d rows from raw_data.df_amazon_sales", nrow(raw_amazon_sales)))
  
  # Cleansing operations
  df_amazon_sales___cleansed <- raw_amazon_sales %>%
    # Remove invalid entries
    filter(!is.na(amazon_order_id), !is.na(purchase_date)) %>%
    # Correct formatting issues
    mutate(
      # Transform time fields to proper time format
      time = as.POSIXct(purchase_date),
      # Standardize text fields
      product_name = stringr::str_trim(product_name),
      asin = stringr::str_trim(asin),
      # Add platform identifier
      platform_id = "amz"
    ) %>%
    # Remove special characters from text fields
    mutate(across(where(is.character), ~gsub("[^[:alnum:][:space:]]", "", .))) %>%
    # Handle NA strings
    mutate(across(where(is.character), ~na_if(., NA_character_))) %>%
    # Remove duplicates
    distinct()
  
  # Create the target table if it doesn't exist
  if (!dbExistsTable(cleansed_data, "df_amazon_sales")) {
    message("Creating cleansed_data.df_amazon_sales table")
    dbCreateTable(cleansed_data, "df_amazon_sales", df_amazon_sales___cleansed)
  }
  
  # Write the cleansed data to the target table
  dbWriteTable(cleansed_data, "df_amazon_sales", df_amazon_sales___cleansed, overwrite = TRUE)
  message(sprintf("Successfully wrote %d rows to cleansed_data.df_amazon_sales", nrow(df_amazon_sales___cleansed)))
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Verify table exists in cleansed_data
    table_exists_query <- "SELECT name FROM sqlite_master WHERE type='table' AND name='df_amazon_sales'"
    table_exists <- nrow(dbGetQuery(cleansed_data, table_exists_query)) > 0
    
    if (table_exists) {
      # Count records in cleansed table
      cleansed_row_count <- tbl(cleansed_data, "df_amazon_sales") %>% count() %>% pull()
      
      if (cleansed_row_count > 0) {
        message("Verification successful: ", cleansed_row_count, " records found in cleansed_data.df_amazon_sales")
        
        # Verify that cleansed data has same or fewer rows than raw data (due to filtering)
        if (exists("raw_amazon_sales") && is.data.frame(raw_amazon_sales)) {
          raw_count <- nrow(raw_amazon_sales)
          if (cleansed_row_count <= raw_count) {
            message("Validation passed: Cleansed data has ", cleansed_row_count, " rows, which is less than or equal to raw data's ", raw_count, " rows")
          } else {
            message("Validation warning: Cleansed data has more rows than raw data, which is unexpected")
          }
        }
        
        # Check for required columns in the cleansed data
        required_columns <- c("amazon_order_id", "purchase_date", "time", "platform_id")
        cleansed_columns <- colnames(tbl(cleansed_data, "df_amazon_sales") %>% head(1) %>% collect())
        missing_columns <- setdiff(required_columns, cleansed_columns)
        
        if (length(missing_columns) == 0) {
          message("Validation passed: All required columns exist in cleansed data")
          test_passed <- TRUE
        } else {
          message("Validation failed: Missing required columns: ", paste(missing_columns, collapse = ", "))
          test_passed <- FALSE
        }
      } else {
        message("Verification failed: Table exists but contains no records")
        test_passed <- FALSE
      }
    } else {
      message("Verification failed: Table df_amazon_sales does not exist in cleansed_data")
      test_passed <- FALSE
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
  # Clean up temporary objects
  if (exists("raw_amazon_sales")) {
    rm(raw_amazon_sales)
  }
  if (exists("df_amazon_sales___cleansed")) {
    rm(df_amazon_sales___cleansed)
  }
  
  # Set final status before deinitialization
  if (exists("test_passed") && test_passed) {
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