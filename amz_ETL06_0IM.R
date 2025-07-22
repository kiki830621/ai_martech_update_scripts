# amz_ETL06_0IM.R - Amazon 商品評論資料匯入
# ETL06 階段 0 匯入：從本地檔案匯入商品評論資料
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
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: Amazon reviews import (ETL06 0IM) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL06 Import Phase - Amazon reviews...")
  
  # Import Amazon reviews from CSV/Excel files
  reviews_dir <- file.path(RAW_DATA_DIR, "amazon_reviews")
  message("MAIN: Importing reviews from: ", reviews_dir)
  
  # Check if directory exists
  if (!dir.exists(reviews_dir)) {
    # Create directory and show structure
    message("MAIN: Reviews directory does not exist, creating placeholder...")
    dir.create(reviews_dir, recursive = TRUE, showWarnings = FALSE)
    
    # Create README to show expected structure
    readme_path <- file.path(reviews_dir, "README.txt")
    writeLines(c(
      "Place Amazon review CSV or Excel files in this directory.",
      "Files will be imported recursively from all subdirectories.",
      "",
      "Expected structure:",
      "amazon_reviews/",
      "├── 001_product_line/",
      "│   ├── reviews_batch1.csv",
      "│   └── reviews_batch2.xlsx",
      "└── 002_product_line/",
      "    └── reviews_data.csv",
      "",
      "Files should contain columns like:",
      "- date (Amazon date format: 'Reviewed in the United States on [date]')",
      "- author (reviewer name)",
      "- body (review content)", 
      "- rating",
      "- variation (ASIN)",
      "- path (file source path)"
    ), readme_path)
    
    # Create table structure using generate_create_table_query for proper schema
    message("MAIN: Creating reviews table structure with proper schema")
    
    create_sql <- generate_create_table_query(
      con = raw_data,
      target_table = "df_amz_review",
      or_replace = TRUE,
      column_defs = list(
        list(name = "date", type = "VARCHAR", not_null = TRUE),
        list(name = "author", type = "VARCHAR"),
        list(name = "verified", type = "VARCHAR"),
        list(name = "helpful", type = "VARCHAR"),
        list(name = "title", type = "VARCHAR"),
        list(name = "body", type = "TEXT"),
        list(name = "rating", type = "NUMERIC"),
        list(name = "images", type = "TEXT"),
        list(name = "videos", type = "TEXT"),
        list(name = "url", type = "VARCHAR"),
        list(name = "variation", type = "VARCHAR", not_null = TRUE),
        list(name = "style", type = "VARCHAR"),
        list(name = "path", type = "VARCHAR", not_null = TRUE)
      )
    )
    
    dbExecute(raw_data, create_sql)
    message("MAIN: Created reviews table structure with proper schema")
    
  } else {
    # Check if there are any files to import
    files <- list.files(reviews_dir, pattern = "\\.(csv|xlsx?)$", 
                       recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
    
    if (length(files) == 0) {
      message("MAIN: No CSV or Excel files found in ", reviews_dir)
      
      # Check if table already exists with data
      if ("df_amz_review" %in% dbListTables(raw_data)) {
        existing_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_amz_review")$count
        if (existing_count > 0) {
          message("MAIN: Using existing reviews data with ", existing_count, " records")
        } else {
          message("MAIN: Reviews table exists but is empty")
        }
      } else {
        # Create table structure using generate_create_table_query
        message("MAIN: No files found, creating empty table structure")
        
        create_sql <- generate_create_table_query(
          con = raw_data,
          target_table = "df_amz_review",
          or_replace = TRUE,
          column_defs = list(
            list(name = "date", type = "VARCHAR", not_null = TRUE),
            list(name = "author", type = "VARCHAR"),
            list(name = "verified", type = "VARCHAR"),
            list(name = "helpful", type = "VARCHAR"),
            list(name = "title", type = "VARCHAR"),
            list(name = "body", type = "TEXT"),
            list(name = "rating", type = "INTEGER"),
            list(name = "images", type = "TEXT"),
            list(name = "videos", type = "TEXT"),
            list(name = "url", type = "VARCHAR"),
            list(name = "variation", type = "VARCHAR", not_null = TRUE),
            list(name = "style", type = "VARCHAR"),
            list(name = "path", type = "VARCHAR", not_null = TRUE)
          )
        )
        
        dbExecute(raw_data, create_sql)
        message("MAIN: Created empty reviews table structure")
      }
      
    } else {
      # Import all CSV and Excel files from the directory
      message("MAIN: Found ", length(files), " files to import")
      df_amz_review <- import_csvxlsx(reviews_dir)
      
      # First create table structure with proper schema
      message("MAIN: Creating table structure with proper schema")
      create_sql <- generate_create_table_query(
        con = raw_data,
        target_table = "df_amz_review",
        or_replace = TRUE,
        column_defs = list(
          list(name = "date", type = "VARCHAR", not_null = TRUE),
          list(name = "author", type = "VARCHAR"),
          list(name = "verified", type = "VARCHAR"),
          list(name = "helpful", type = "VARCHAR"),
          list(name = "title", type = "VARCHAR"),
          list(name = "body", type = "TEXT"),
          list(name = "rating", type = "INTEGER"),
          list(name = "images", type = "TEXT"),
          list(name = "videos", type = "TEXT"),
          list(name = "url", type = "VARCHAR"),
          list(name = "variation", type = "VARCHAR", not_null = TRUE),
          list(name = "style", type = "VARCHAR"),
          list(name = "path", type = "VARCHAR", not_null = TRUE)
        )
      )
      
      dbExecute(raw_data, create_sql)
      
      # Append data to the properly structured table
      message("MAIN: Appending ", nrow(df_amz_review), " reviews to raw_data")
      dbWriteTable(raw_data, "df_amz_review", df_amz_review, append = TRUE)
    }
  }
  
  script_success <- TRUE
  message("MAIN: ETL06 Import Phase completed successfully")

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
    message("TEST: Verifying ETL06 Import Phase results...")

    # Check if reviews table exists
    table_name <- "df_amz_review"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      review_count <- dbGetQuery(raw_data, paste0("SELECT COUNT(*) as count FROM ", table_name))$count

      test_passed <- TRUE
      message("TEST: Verification successful - ", review_count, " reviews in raw_data")
      
      # Show table structure and sample data
      if (review_count > 0) {
        # Check columns
        columns <- dbListFields(raw_data, table_name)
        message("TEST: Review table columns: ", paste(columns, collapse = ", "))
        
        # Show sample data
        sample_query <- paste0("SELECT * FROM ", table_name, " LIMIT 5")
        sample_data <- dbGetQuery(raw_data, sample_query)
        message("TEST: Sample reviews:")
        print(head(sample_data, 3))
        
        # Data quality checks
        if ("asin" %in% columns) {
          unique_asins <- dbGetQuery(raw_data, paste0("SELECT COUNT(DISTINCT asin) as count FROM ", table_name))$count
          message("TEST: Unique ASINs: ", unique_asins)
        }
        
        if ("rating" %in% columns) {
          rating_stats <- dbGetQuery(raw_data, paste0("SELECT MIN(rating) as min_rating, MAX(rating) as max_rating, AVG(rating) as avg_rating FROM ", table_name))
          message("TEST: Rating range: ", rating_stats$min_rating, " to ", rating_stats$max_rating, 
                  " (avg: ", round(rating_stats$avg_rating, 2), ")")
        }
        
        if ("path" %in% columns) {
          source_files <- dbGetQuery(raw_data, paste0("SELECT COUNT(DISTINCT path) as count FROM ", table_name))$count
          message("TEST: Data imported from ", source_files, " source files")
        }
        
      } else {
        message("TEST: Table exists but is empty (no data files found)")
      }
      
      # Check for expected columns (original Amazon format)
      expected_cols <- c("date", "author", "body", "rating", "variation", "path")
      actual_cols <- dbListFields(raw_data, table_name)
      missing_cols <- setdiff(expected_cols, actual_cols)
      extra_cols <- setdiff(actual_cols, expected_cols)
      
      if (length(missing_cols) > 0) {
        message("TEST WARNING: Missing expected columns: ", paste(missing_cols, collapse = ", "))
      }
      if (length(extra_cols) > 0) {
        message("TEST INFO: Additional columns found: ", paste(extra_cols, collapse = ", "))
      }
      if (length(missing_cols) == 0 && length(extra_cols) == 0) {
        message("TEST: All expected columns present with no extras")
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
  message("DEINITIALIZE: ETL06 Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL06 Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL06 Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL06 Import Phase (amz_ETL06_0IM.R) completed")