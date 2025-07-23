# amz_ETL04_0IM.R - Amazon 競爭對手資料匯入
# ETL04 階段 0 匯入：從 Google Sheets 匯入競爭對手 ID
# 遵循 R113：四部分更新腳本結構

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

# Initialize environment using autoinit system
# Set required dependencies before initialization
needgoogledrive <- TRUE

# Extend Google API timeout to reduce timeout failures
options(gargle_timeout = 60)

# Initialize using unified autoinit system
autoinit()

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: Amazon competitor products import (ETL04 0IM) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL04 Import Phase - Amazon competitor products...")

  # Retry mechanism for transient timeout errors ---------------------------------
  max_attempts <- 3
  attempt <- 1
  repeat {
    message(glue::glue("MAIN: Attempt {attempt}/{max_attempts} to import competitor products"))
    import_error <- NULL
    tryCatch({
      # Import competitor products for all product lines
      # This is the raw import phase - preserve original data structure
      competitor_products <- import_competitor_products(
        db_connection = raw_data,
        google_sheet_id = app_configs$googlesheet$product_profile,
        sheet_name = "競爭者",
        product_line_df = df_product_line,
        platform = "amz",  # Specify platform for table naming
        skip_validation = TRUE  # Skip complex validation in import phase
      )
    }, error = function(e) {
      import_error <<- e
    })

    if (is.null(import_error)) {
      script_success <- TRUE
      message("MAIN: ETL04 Import Phase completed successfully")
      break
    } else if (attempt < max_attempts && grepl("Timeout", import_error$message, ignore.case = TRUE)) {
      message("MAIN WARNING: Timeout encountered, retrying in 5 seconds...")
      Sys.sleep(5)
      attempt <- attempt + 1
    } else {
      stop(import_error)
    }
  }

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message("MAIN ERROR: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================

if (script_success) {
  tryCatch({
    message("TEST: Verifying ETL04 Import Phase results...")

    # Check if competitor products table exists and has data
    table_name <- "df_amz_competitor_product_id"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
      product_count <- dbGetQuery(raw_data, query)$count

      if (product_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", product_count,
                " competitor products imported to raw_data")
        
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", table_name, " LIMIT 3")
        sample_data <- dbGetQuery(raw_data, structure_query)
        message("TEST: Sample raw data structure:")
        print(sample_data)
        
        # Check for required columns
        required_cols <- c("product_line_id", "asin", "brand")
        actual_cols <- names(sample_data)
        missing_cols <- setdiff(required_cols, actual_cols)
        
        if (length(missing_cols) > 0) {
          message("TEST WARNING: Missing expected columns: ", paste(missing_cols, collapse = ", "))
        } else {
          message("TEST: All required columns present")
        }
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no competitor products found in table")
      }
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - table ", table_name, " not found")
    }

  }, error = function(e) {
    test_passed <<- FALSE
    message("TEST ERROR: ", e$message)
  })
} else {
  message("TEST: Skipped due to main script failure")
}

# ==============================================================================
# 4. DEINITIALIZE
# ==============================================================================

# Determine final status before tearing down
if (script_success && test_passed) {
  message("DEINITIALIZE: ETL04 Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL04 Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL04 Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL04 Import Phase (amz_ETL04_0IM.R) completed")