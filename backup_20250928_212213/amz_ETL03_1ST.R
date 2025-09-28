# amz_ETL03_1ST.R - Amazon 商品屬性資料暫存
# ETL03 階段 1 暫存：暫存和驗證所有匯入的商品資料
# 遵循 R113：四部分更新腳本結構

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

# Initialize using unified autoinit system
autoinit()

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)

message("INITIALIZE: Amazon product profiles staging script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting Amazon product profiles staging...")

  # Use the dedicated staging function
  staging_results <- stage_product_profiles(
    raw_db_connection = raw_data,
    staged_db_connection = staged_data,
    table_pattern = "^df_product_profile_",
    overwrite_existing = TRUE
  )
  
  # Extract results for verification
  total_staged_rows <- staging_results$total_rows_staged
  staged_tables_count <- length(staging_results$tables_processed)

  script_success <- TRUE
  message("MAIN: Amazon product profiles staging completed successfully")
  message("MAIN: Processed ", staged_tables_count, " tables")
  message("MAIN: Total staged rows: ", total_staged_rows)

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
    message("TEST: Verifying product profiles staging...")

    # Use staging results for verification
    if (length(staging_results$tables_processed) == 0) {
      test_passed <- FALSE
      message("TEST: Verification failed - no staged tables were processed")
    } else if (staging_results$total_rows_staged == 0) {
      test_passed <- FALSE
      message("TEST: Verification failed - no rows were staged")
    } else {
      # Verify tables actually exist in database
      staged_tables <- DBI::dbListTables(staged_data)
      existing_staged_tables <- intersect(staging_results$tables_processed, 
                                         staged_tables)
      
      if (length(existing_staged_tables) == length(staging_results$tables_processed)) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", staging_results$total_rows_staged,
                " total staged product profiles across ", 
                length(staging_results$tables_processed), " tables")
        
        # Verify staging metadata on first table
        sample_table <- staging_results$tables_processed[1]
        sample_query <- paste0("SELECT etl_staging_timestamp, etl_validation_status, etl_phase FROM ", 
                              sample_table, " LIMIT 1")
        sample_data <- DBI::dbGetQuery(staged_data, sample_query)
        message("TEST: Sample staging metadata - Phase: ", sample_data$etl_phase, 
                ", Status: ", sample_data$etl_validation_status)
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - some staged tables missing from database")
      }
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

# Determine final status before tearing down -------------------------------------------------
if (script_success && test_passed) {
  message("DEINITIALIZE: Script completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: Script completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: Script failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)
DBI::dbDisconnect(staged_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: Amazon product profiles staging script completed")