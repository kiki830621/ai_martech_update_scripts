# amz_ETL_competitor_sales_0IM.R - Amazon Competitor Sales Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL competitor_sales Phase 0IM: Import from local Excel/CSV files
# Output: raw_data.duckdb → df_amz_competitor_sales

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
sql_read_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R")
)
sql_read_path <- sql_read_candidates[file.exists(sql_read_candidates)][1]
if (is.na(sql_read_path)) {
  stop("fn_sql_read.R not found in expected paths")
}
source(sql_read_path)
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

# Initialize environment using autoinit system
# Set required dependencies before initialization
needgoogledrive <- TRUE

# Initialize using unified autoinit system
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$competitor_sales
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

source_type <- tolower(as.character(etl_profile$source_type %||% ""))
if (!source_type %in% c("excel", "csv")) {
  stop(sprintf("VALIDATE FAILED: competitor_sales requires source_type='excel' or 'csv', got '%s'", source_type))
}

rawdata_root <- RAW_DATA_DIR %||% file.path(APP_DIR, "data", "local_data", "rawdata_QEF_DESIGN")
rawdata_pattern <- as.character(etl_profile$rawdata_pattern %||% "")
if (!nzchar(rawdata_pattern)) {
  stop("VALIDATE FAILED: competitor_sales profile missing rawdata_pattern")
}
rawdata_rel_dir <- sub("/\\*.*$", "", rawdata_pattern)
rawdata_rel_dir <- sub("/$", "", rawdata_rel_dir)
competitor_sales_dir <- file.path(rawdata_root, rawdata_rel_dir)

message("INITIALIZE: Amazon competitor sales import (ETL competitor_sales 0IM) script initialized")
message("INITIALIZE: Data source directory: ", competitor_sales_dir)

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL competitor_sales Import Phase - Amazon competitor sales...")

  matched_files <- Sys.glob(file.path(rawdata_root, rawdata_pattern))
  if (length(matched_files) == 0) {
    stop(sprintf("VALIDATE FAILED: No files match pattern '%s'", rawdata_pattern))
  }
  message(sprintf("VALIDATE: Found %d files matching declared pattern", length(matched_files)))

  # Check if source directory exists
  if (!dir.exists(competitor_sales_dir)) {
    stop("VALIDATE FAILED: competitor_sales directory does not exist: ", competitor_sales_dir)
  }
  
  # Import competitor sales data using existing function
  import_result <- core_import_df_amz_competitor_sales(
    main_folder = competitor_sales_dir,
    db_connection = raw_data
  )
  imported_count <- if (
    is.list(import_result) && !is.null(import_result$total_rows_imported)
  ) {
    import_result$total_rows_imported
  } else {
    import_result
  }
  if (is.null(imported_count) || is.na(imported_count) || imported_count == 0L) {
    stop("VALIDATE FAILED: ETL competitor sales import produced zero rows")
  }

  if (is.list(import_result)) {
    if (length(import_result$skipped_folders_invalid_reference) > 0L) {
      message(
        "ETL competitor_sales: invalid/unmatched product-line folders (skipped): ",
        paste(import_result$skipped_folders_invalid_reference, collapse = ", ")
      )
    }
    if (length(import_result$skipped_folders_no_supported_files) > 0L) {
      message(
        "ETL competitor_sales: no supported files (skipped folders) = ",
        paste(import_result$skipped_folders_no_supported_files, collapse = ", ")
      )
    }
    if (length(import_result$skipped_folders_no_rows) > 0L) {
      message(
        "ETL competitor_sales: supported files but no imported rows (reviewed folders) = ",
        paste(import_result$skipped_folders_no_rows, collapse = ", ")
      )
    }
  }
  
  script_success <- TRUE
  message("MAIN: ETL competitor_sales Import Phase completed successfully")

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
    message("TEST: Verifying ETL competitor_sales Import Phase results...")

    # Check if competitor sales table exists
    table_name <- "df_amz_competitor_sales"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
      sales_count <- sql_read(raw_data, query)$count

      test_passed <- TRUE
      message("TEST: Verification successful - ", sales_count,
              " competitor sales records imported")
      
      if (sales_count > 0) {
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", table_name, " LIMIT 3")
        sample_data <- sql_read(raw_data, structure_query)
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
        asin_count <- sql_read(raw_data, paste0("SELECT COUNT(DISTINCT asin) as count FROM ", table_name))$count
        product_line_count <- sql_read(raw_data, paste0("SELECT COUNT(DISTINCT product_line_id) as count FROM ", table_name))$count
        
        message("TEST: Unique ASINs: ", asin_count)
        message("TEST: Product lines: ", product_line_count)
        
        # Check date range
        date_range <- sql_read(raw_data, paste0("SELECT MIN(date) as min_date, MAX(date) as max_date FROM ", table_name))
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
  message("DEINITIALIZE: ETL competitor_sales Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL competitor_sales Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL competitor_sales Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL competitor_sales Import Phase (amz_ETL_competitor_sales_0IM.R) completed")
