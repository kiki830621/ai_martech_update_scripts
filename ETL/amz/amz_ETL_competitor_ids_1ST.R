# amz_ETL04_1ST.R - Amazon 競爭對手資料暫存
# ETL04 階段 1 暫存：資料驗證、類型優化、重複檢查
# 遵循 R113：四部分更新腳本結構

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

# Initialize environment using autoinit system
autoinit()

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)

message("INITIALIZE: Amazon competitor products staging (ETL04 1ST) script initialized")

# Source ETL utility function
source(file.path(GLOBAL_DIR, "05_etl_utils", "all", "stage", "fn_stage_competitor_products.R"))

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL04 Staging Phase - Amazon competitor products...")

  # Check if source table exists
  source_table <- "df_amz_competitor_product_id"
  
  if (!source_table %in% dbListTables(raw_data)) {
    stop("Source table ", source_table, " not found in raw_data. Please run amz_ETL04_0IM.R first.")
  }

  # Load raw data
  message("MAIN: Loading raw competitor products data...")
  raw_competitor_products <- dbGetQuery(raw_data, paste("SELECT * FROM", source_table))
  
  message("MAIN: Raw data loaded - ", nrow(raw_competitor_products), " rows")

  # Stage competitor products data
  staged_competitor_products <- stage_competitor_products(
    raw_data = raw_competitor_products,
    platform = "amz",
    perform_validation = TRUE,
    optimize_types = TRUE,
    check_duplicates = TRUE,
    encoding_target = "UTF-8"
  )

  # Write staged data to database
  target_table <- "df_amz_competitor_product_id___staged"
  dbWriteTable(staged_data, target_table, staged_competitor_products, overwrite = TRUE)
  
  message("MAIN: Staged data written to ", target_table, " - ", nrow(staged_competitor_products), " rows")
  
  script_success <- TRUE
  message("MAIN: ETL04 Staging Phase completed successfully")

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
    message("TEST: Verifying ETL04 Staging Phase results...")

    # Check if staged table exists and has data
    target_table <- "df_amz_competitor_product_id___staged"
    
    if (target_table %in% dbListTables(staged_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", target_table)
      staged_count <- dbGetQuery(staged_data, query)$count
      
      # Get original count for comparison
      original_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_amz_competitor_product_id")$count

      if (staged_count > 0) {
        message("TEST: Verification successful - ", staged_count, " rows in staged data")
        message("TEST: Data flow: raw (", original_count, ") -> staged (", staged_count, ")")
        
        # Check for data quality improvements
        sample_query <- paste0("SELECT * FROM ", target_table, " LIMIT 3")
        sample_data <- dbGetQuery(staged_data, sample_query)
        message("TEST: Sample staged data:")
        print(sample_data)
        
        # Check for required columns and validation fields
        required_cols <- c("product_line_id", "asin", "brand")
        validation_cols <- c("etl_staging_timestamp", "etl_validation_status", "etl_data_quality_score")
        actual_cols <- names(sample_data)
        
        missing_required <- setdiff(required_cols, actual_cols)
        missing_validation <- setdiff(validation_cols, actual_cols)
        
        if (length(missing_required) > 0) {
          message("TEST WARNING: Missing required columns: ", paste(missing_required, collapse = ", "))
        } else {
          message("TEST: All required columns present")
        }
        
        if (length(missing_validation) > 0) {
          message("TEST INFO: Missing validation columns: ", paste(missing_validation, collapse = ", "))
        } else {
          message("TEST: All validation columns added")
        }
        
        # Check for duplicates
        dup_query <- paste0("SELECT asin, COUNT(*) as count FROM ", target_table, " GROUP BY asin HAVING COUNT(*) > 1")
        duplicates <- dbGetQuery(staged_data, dup_query)
        
        if (nrow(duplicates) > 0) {
          message("TEST WARNING: ", nrow(duplicates), " duplicate ASINs found in staged data")
        } else {
          message("TEST: No duplicates found - validation successful")
        }
        
        test_passed <- TRUE
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no data found in staged table")
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
  message("DEINITIALIZE: ETL04 Staging Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL04 Staging Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL04 Staging Phase failed during execution")
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

message("DEINITIALIZE: ETL04 Staging Phase (amz_ETL04_1ST.R) completed")