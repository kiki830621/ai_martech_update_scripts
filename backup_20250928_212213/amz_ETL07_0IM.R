# amz_ETL07_0IM.R - Amazon 競爭對手銷售資料匯入
# ETL07 階段 0 匯入：從 CSV 檔案匯入競爭對手銷售資料
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

# Initialize using unified autoinit system
autoinit()

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

# Define source directory for competitor sales data
competitor_sales_dir <- file.path(RAW_DATA_DIR %||% "data", "competitor_sales")

message("INITIALIZE: Amazon competitor sales import (ETL07 0IM) script initialized")
message("INITIALIZE: Data source directory: ", competitor_sales_dir)

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL07 Import Phase - Amazon competitor sales...")

  # Check if source directory exists
  if (!dir.exists(competitor_sales_dir)) {
    warning("Competitor sales directory does not exist: ", competitor_sales_dir)
    message("MAIN: Creating placeholder directory structure...")
    dir.create(competitor_sales_dir, recursive = TRUE, showWarnings = FALSE)
    
    # Create placeholder to show expected structure
    placeholder_path <- file.path(competitor_sales_dir, "README.txt")
    writeLines(c(
      "This directory should contain competitor sales CSV files",
      "organized in subdirectories by product line.",
      "",
      "Expected structure:",
      "competitor_sales/",
      "├── product_line_1/",
      "│   ├── sales_data_1.csv",
      "│   └── sales_data_2.csv",
      "└── product_line_2/",
      "    └── sales_data_3.csv"
    ), placeholder_path)
    
    message("MAIN: Created placeholder directory with README")
  }
  
  # Import competitor sales data using existing function
  import_df_amz_competitor_sales(
    main_folder = competitor_sales_dir,
    db_connection = raw_data
  )
  
  script_success <- TRUE
  message("MAIN: ETL07 Import Phase completed successfully")

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
    message("TEST: Verifying ETL07 Import Phase results...")

    # Check if competitor sales table exists
    table_name <- "df_amz_competitor_sales"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
      sales_count <- dbGetQuery(raw_data, query)$count

      test_passed <- TRUE
      message("TEST: Verification successful - ", sales_count,
              " competitor sales records imported")
      
      if (sales_count > 0) {
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", table_name, " LIMIT 3")
        sample_data <- dbGetQuery(raw_data, structure_query)
        message("TEST: Sample raw data structure:")
        print(sample_data)
        
        # Check for required columns
        required_cols <- c("asin", "date", "product_line_id", "sales")
        actual_cols <- names(sample_data)
        missing_cols <- setdiff(required_cols, actual_cols)
        
        if (length(missing_cols) > 0) {
          message("TEST WARNING: Missing expected columns: ", paste(missing_cols, collapse = ", "))
        } else {
          message("TEST: All required columns present")
        }
        
        # Check data statistics
        asin_count <- dbGetQuery(raw_data, paste0("SELECT COUNT(DISTINCT asin) as count FROM ", table_name))$count
        product_line_count <- dbGetQuery(raw_data, paste0("SELECT COUNT(DISTINCT product_line_id) as count FROM ", table_name))$count
        
        message("TEST: Unique ASINs: ", asin_count)
        message("TEST: Product lines: ", product_line_count)
        
        # Check date range
        date_range <- dbGetQuery(raw_data, paste0("SELECT MIN(date) as min_date, MAX(date) as max_date FROM ", table_name))
        message("TEST: Date range: ", date_range$min_date, " to ", date_range$max_date)
        
      } else {
        message("TEST: Table exists but is empty (no CSV files found)")
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
  message("DEINITIALIZE: ETL07 Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL07 Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL07 Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL07 Import Phase (amz_ETL07_0IM.R) completed")