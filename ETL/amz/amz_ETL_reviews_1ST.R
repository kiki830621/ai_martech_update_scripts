# amz_ETL06_1ST.R - Amazon Reviews 資料清理階段處理
# ETL06 階段 1 階段處理：清理和標準化 Amazon reviews 資料
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

message("INITIALIZE: Amazon reviews staging (ETL06 1ST) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL06 Staging Phase - Amazon reviews...")

  # Load raw data from previous phase
  source_table <- "df_amz_review"
  
  # Check if source table exists
  if (!source_table %in% dbListTables(raw_data)) {
    stop("Source table ", source_table, " not found in raw_data. Please run ETL06_0IM first.")
  }
  
  # Load raw reviews data
  message("MAIN: Reading data from ", source_table)
  df_amazon_reviews <- dbGetQuery(raw_data, paste("SELECT * FROM", source_table))
  initial_rows <- nrow(df_amazon_reviews)
  message("MAIN: Processing ", initial_rows, " raw Amazon reviews")
  
  # Apply cleansing logic based on D03_04 fn_cleanse_amazon_reviews
  # 1. Fix date format using parse_date_time3
  # 2. Standardize column names to ETL schema
  # 3. Filter for valid ASIN format (10 alphanumeric characters)
  
  # Apply cleansing transformations using the ETL cleansing function
  message("STAGE: Starting Amazon reviews cleansing...")
  message("STAGE: Processing ", initial_rows, " raw reviews")
  
  # Step 1: Fix date formats
  message("STAGE: Fixing date formats...")
  staged_reviews <- df_amazon_reviews %>%
    dplyr::mutate(
      # Fix date format - extract actual date from "Reviewed in the United States on [date]"
      review_date = dplyr::case_when(
        stringr::str_detect(date, "Reviewed in .* on (.+)") ~ 
          stringr::str_extract(date, "(?<=on ).*"),
        TRUE ~ date
      ),
      # Parse the extracted date
      review_date = parse_date_time3(review_date)
    )
  message("STAGE: Date format fixing completed")
  
  # Step 2: Clean data (keep original column names)
  message("STAGE: Cleaning data...")
  staged_reviews <- staged_reviews %>%
    # Ensure rating is integer but keep original column names
    dplyr::mutate(
      rating = as.integer(rating)
    ) %>%
    dplyr::select(-date)      # Remove original date column
  message("STAGE: Data cleaning completed")
  
  # Step 3: Filter for valid ASINs (still using original `variation` column)
  message("STAGE: Filtering for valid ASINs...")
  staged_reviews <- staged_reviews %>%
    dplyr::filter(
      !is.na(variation) & stringr::str_detect(variation, "^[A-Z0-9]{10}$")
    )
  message("STAGE: ASIN filtering completed")
  
  # Step 4: Normalize text encoding to UTF-8 (using original column names)
  message("STAGE: Normalizing text encoding to UTF-8...")
  staged_reviews <- staged_reviews %>%
    convert_all_columns_to_utf8()
  message("STAGE: Encoding normalization completed")
  
  # Step 5: Perform data validation (using original column names)
  message("STAGE: Performing data validation...")
  staged_reviews <- staged_reviews %>%
    # Basic data quality scoring
    dplyr::mutate(
      is_valid_rating = rating >= 1 & rating <= 5,
      has_review_text = !is.na(body) & nchar(body) > 0,
      has_valid_date = !is.na(review_date)
    )
  
  # Calculate basic quality score
  quality_score <- round(100 * mean(
    staged_reviews$is_valid_rating & 
    staged_reviews$has_review_text & 
    staged_reviews$has_valid_date
  ))
  message("STAGE: Data quality score: ", quality_score, "%")
  message("STAGE: Data validation completed")
  
  # Step 6: Remove duplicates
  message("STAGE: Removing duplicate records...")
  pre_dedup_rows <- nrow(staged_reviews)
  staged_reviews <- staged_reviews %>%
    dplyr::distinct()
  post_dedup_rows <- nrow(staged_reviews)
  duplicate_removed <- pre_dedup_rows - post_dedup_rows
  
  if (duplicate_removed > 0) {
    message("STAGE: Removed ", duplicate_removed, " duplicate records")
  } else {
    message("STAGE: No duplicate records found")
  }
  message("STAGE: Deduplication completed")
  
  # Count results
  final_rows <- nrow(staged_reviews)
  removed_rows <- initial_rows - final_rows
  message("STAGE: Reviews cleansing completed - ", final_rows, " records processed")
  
  # Add ETL staging metadata
  message("STAGE: Added staging metadata")
  staged_reviews <- staged_reviews %>%
    dplyr::mutate(
      etl_phase = "staged",
      etl_staging_timestamp = Sys.time(),
      etl_records_processed = initial_rows,
      etl_records_output = final_rows,
      etl_processing_order = row_number(),
      # Use the calculated quality score for all records
      etl_data_quality_score = quality_score,
      # Add date components for analysis (only if review_date is valid)
      review_year = ifelse(!is.na(review_date), lubridate::year(review_date), NA),
      review_month = ifelse(!is.na(review_date), lubridate::month(review_date), NA),
      review_text_length = nchar(as.character(body))
    ) %>%
    # Remove temporary validation columns
    dplyr::select(-is_valid_rating, -has_review_text, -has_valid_date)
  
  # Write to staged database
  if (final_rows > 0) {
    target_table <- "df_amz_review___staged"
    dbWriteTable(staged_data, target_table, staged_reviews, overwrite = TRUE)
    message("MAIN: Successfully wrote ", final_rows, " cleansed reviews to ", target_table)
    
    # Generate and display summary statistics
    if ("rating" %in% names(staged_reviews)) {
      review_stats <- staged_reviews %>%
        dplyr::group_by(rating) %>%
        dplyr::summarize(count = n(), .groups = "drop") %>%
        dplyr::arrange(dplyr::desc(count))
      
      message("MAIN: Review statistics by rating:")
      print(review_stats)
    }
    
    script_success <- TRUE
    message("MAIN: ETL06 Staging Phase completed successfully")
  } else {
    warning("MAIN: No valid records to write to the database")
    script_success <- FALSE
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
    message("TEST: Verifying ETL06 Staging Phase results...")

    # Check if staged table exists and has data
    target_table <- "df_amz_review___staged"
    
    if (target_table %in% dbListTables(staged_data)) {
      # Check row count
      staged_count <- dbGetQuery(staged_data, paste0("SELECT COUNT(*) as count FROM ", target_table))$count

      if (staged_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", staged_count, " reviews staged")
        
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
        if ("etl_data_quality_score" %in% actual_cols) {
          quality_stats <- dbGetQuery(staged_data, paste0(
            "SELECT ",
            "MIN(etl_data_quality_score) as min_quality, ",
            "AVG(etl_data_quality_score) as avg_quality, ",
            "MAX(etl_data_quality_score) as max_quality ",
            "FROM ", target_table
          ))
          message("TEST: Data quality scores - Min: ", quality_stats$min_quality, 
                  ", Avg: ", round(quality_stats$avg_quality, 2), 
                  ", Max: ", quality_stats$max_quality)
        }
        
        # Check ASIN validation
        asin_check <- dbGetQuery(staged_data, paste0(
          "SELECT COUNT(*) as valid_asins FROM ", target_table,
          " WHERE variation IS NOT NULL AND LENGTH(variation) = 10"
        ))
        message("TEST: Valid ASINs: ", asin_check$valid_asins, " (should equal total count)")
        
        # Check date validation
        if ("review_year" %in% actual_cols) {
          year_range <- dbGetQuery(staged_data, paste0(
            "SELECT MIN(review_year) as min_year, MAX(review_year) as max_year FROM ", target_table
          ))
          message("TEST: Review year range: ", year_range$min_year, " to ", year_range$max_year)
        }
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no staged reviews found")
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
  message("DEINITIALIZE: ETL06 Staging Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL06 Staging Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL06 Staging Phase failed during execution")
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

message("DEINITIALIZE: ETL06 Staging Phase (amz_ETL06_1ST.R) completed")