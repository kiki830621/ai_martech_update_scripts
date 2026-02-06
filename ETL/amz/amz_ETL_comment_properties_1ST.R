# amz_ETL05_1ST.R - Amazon 評論屬性資料階段處理
# ETL05 階段 1 階段處理：資料驗證、類型最佳化、重複檢查
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

message("INITIALIZE: Amazon comment properties staging (ETL05 1ST) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL05 Staging Phase - Amazon comment properties...")

  # Load raw data from previous phase
  source_table <- "df_all_comment_property"
  
  # Check if source table exists
  if (!source_table %in% dbListTables(raw_data)) {
    stop("Source table ", source_table, " not found in raw_data. Please run ETL05_0IM first.")
  }
  
  # Load raw comment properties data
  raw_comment_properties <- dbGetQuery(raw_data, paste("SELECT * FROM", source_table))
  message("MAIN: Loaded ", nrow(raw_comment_properties), " raw comment properties")
  
  # Stage comment properties with validation and optimization
  staged_comment_properties <- stage_comment_properties(
    raw_data = raw_comment_properties,
    perform_validation = TRUE,
    optimize_types = TRUE,
    check_duplicates = TRUE,
    encoding_target = "UTF-8"
  )
  
  # Write to staged database
  target_table <- "df_all_comment_property___staged"
  dbWriteTable(staged_data, target_table, staged_comment_properties, overwrite = TRUE)
  
  script_success <- TRUE
  message("MAIN: ETL05 Staging Phase completed successfully")
  message("MAIN: Staged ", nrow(staged_comment_properties), " comment properties")

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
    message("TEST: Verifying ETL05 Staging Phase results...")

    # Check if staged table exists and has data
    target_table <- "df_all_comment_property___staged"
    
    if (target_table %in% dbListTables(staged_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", target_table)
      staged_count <- dbGetQuery(staged_data, query)$count

      if (staged_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", staged_count,
                " comment properties staged")
        
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", target_table, " LIMIT 3")
        sample_data <- dbGetQuery(staged_data, structure_query)
        message("TEST: Sample staged data structure:")
        print(sample_data)
        
        # Check for ETL metadata columns
        etl_cols <- c("etl_phase", "etl_staging_timestamp", "etl_data_quality_score")
        actual_cols <- names(sample_data)
        missing_etl_cols <- setdiff(etl_cols, actual_cols)
        
        if (length(missing_etl_cols) > 0) {
          message("TEST WARNING: Missing ETL metadata columns: ", paste(missing_etl_cols, collapse = ", "))
        } else {
          message("TEST: All ETL metadata columns present")
        }
        
        # Check data quality scores
        quality_query <- paste0("SELECT AVG(etl_data_quality_score) as avg_quality FROM ", target_table)
        avg_quality <- dbGetQuery(staged_data, quality_query)$avg_quality
        message("TEST: Average data quality score: ", round(avg_quality, 2), "%")
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no staged comment properties found")
      }
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - staged table ", target_table, " not found")
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
  message("DEINITIALIZE: ETL05 Staging Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL05 Staging Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL05 Staging Phase failed during execution")
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

message("DEINITIALIZE: ETL05 Staging Phase (amz_ETL05_1ST.R) completed")