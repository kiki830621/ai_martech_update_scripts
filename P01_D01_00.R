#' @file D01_00_P01.R
#' @requires DBI
#' @requires dplyr
#' @requires readr
#' @principle R113 Update Script Structure
#' @principle MP031 Initialization First
#' @principle MP033 Deinitialization Final
#' @author Original Author
#' @modified_by Claude
#' @date 2025-04-15
#' @title Import External Raw Amazon Sales Data
#' @description Imports raw Amazon sales data from external files into the raw_data database

# 1. INITIALIZE
# Source initialization script for update mode
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))

# Connect to required databases if not already connected
connection_created_raw <- FALSE
connection_created_cleansed <- FALSE

if (!exists("raw_data") || !inherits(raw_data, "DBIConnection")) {
  raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
  connection_created_raw <- TRUE
  message("Connected to raw_data database")
}

if (!exists("cleansed_data") || !inherits(cleansed_data, "DBIConnection")) {
  cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = FALSE)
  connection_created_cleansed <- TRUE
  message("Connected to cleansed_data database")
}

# Initialize error tracking
error_occurred <- FALSE

# 2. MAIN
tryCatch({
  # Log script start
  message("Starting D01_00: Importing external_raw_data.amazon_sales to raw_data.df_amazon_sales")

  # Create or replace the amazon_sales_dta table structure
  # First look for any available files to use as structure reference
  amazon_files <- list.files(file.path(raw_data_folder, "amazon_sales"), 
                            pattern = "\\.(xlsx|csv)$", 
                            recursive = TRUE, 
                            full.names = TRUE)

  if (length(amazon_files) > 0) {
    # Use the first file as a reference for structure
    example_file <- amazon_files[1]
    message("Using ", example_file, " as structure reference")
    create_or_replace_df_amazon_sales(raw_data, example_location = example_file)
  } else {
    # Fallback to creating the structure without a reference file
    message("No example files found. Creating table with default structure.")
    create_or_replace_df_amazon_sales(raw_data)
  }

  raw_data_folder <- "../rawdata_WISER"
  # Define the folder path containing Amazon sales data
  main_folder <- file.path(raw_data_folder, "amazon_sales")

  # Import Amazon sales data from Excel files in the folder
  message("Importing data from ", main_folder)
  import_result <- import_df_amazon_sales(main_folder, raw_data)

  if (is.null(import_result)) {
    stop("Failed to import Amazon sales data")
  }
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Verify import success by checking if table exists and has data
    table_exists_query <- "SELECT name FROM sqlite_master WHERE type='table' AND name='df_amazon_sales'"
    table_exists <- nrow(dbGetQuery(raw_data, table_exists_query)) > 0
    
    if (table_exists) {
      # Count records
      row_count <- tbl(raw_data, "df_amazon_sales") %>% count() %>% pull()
      
      if (row_count > 0) {
        message("Verification successful: ", row_count, " records imported into raw_data.df_amazon_sales")
        
        # Display a sample of the data for verification
        sample_data <- tbl(raw_data, "df_amazon_sales") %>% head(5) %>% collect()
        print(sample_data)
        
        test_passed <- TRUE
      } else {
        message("Verification failed: Table exists but contains no records")
        test_passed <- FALSE
      }
    } else {
      message("Verification failed: Table df_amazon_sales does not exist")
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
  # Set final status before deinitialization
  if (exists("test_passed") && test_passed) {
    message("Script executed successfully with all tests passed")
    final_status <- TRUE
  } else {
    message("Script execution incomplete or tests failed")
    final_status <- FALSE
  }
  
  # Use standard deinitialization script
  source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))
  
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

