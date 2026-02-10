# amz_ETL04_2TR.R - Amazon 競爭對手資料轉換
# ETL04 階段 2 轉換：標準化格式、商業邏輯處理
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
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)

message("INITIALIZE: Amazon competitor products transformation (ETL04 2TR) script initialized")

# Source ETL utility function
source(file.path(GLOBAL_DIR, "05_etl_utils", "all", "transform", "fn_transform_competitor_products.R"))

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL04 Transform Phase - Amazon competitor products...")

  # Check if source table exists
  source_table <- "df_amz_competitor_product_id___staged"
  
  if (!source_table %in% dbListTables(staged_data)) {
    stop("Source table ", source_table, " not found in staged_data. Please run amz_ETL04_1ST.R first.")
  }

  # Load staged data
  message("MAIN: Loading staged competitor products data...")
  staged_competitor_products <- dbGetQuery(staged_data, paste("SELECT * FROM", source_table))
  
  message("MAIN: Staged data loaded - ", nrow(staged_competitor_products), " rows")

  # Transform competitor products data
  transformed_competitor_products <- transform_competitor_products(
    staged_data = staged_competitor_products,
    platform = "amz",
    standardize_fields = TRUE,
    apply_business_rules = TRUE,
    generate_lookup_keys = TRUE,
    encoding_target = "UTF-8"
  )

  # Write transformed data to database
  target_table <- "df_amz_competitor_product_id___transformed"
  dbWriteTable(transformed_data, target_table, transformed_competitor_products, overwrite = TRUE)
  
  message("MAIN: Transformed data written to ", target_table, " - ", nrow(transformed_competitor_products), " rows")
  
  script_success <- TRUE
  message("MAIN: ETL04 Transform Phase completed successfully")

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
    message("TEST: Verifying ETL04 Transform Phase results...")

    # Check if transformed table exists and has data
    target_table <- "df_amz_competitor_product_id___transformed"
    
    if (target_table %in% dbListTables(transformed_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", target_table)
      transformed_count <- dbGetQuery(transformed_data, query)$count
      
      # Get staged count for comparison
      staged_count <- dbGetQuery(staged_data, "SELECT COUNT(*) as count FROM df_amz_competitor_product_id___staged")$count

      if (transformed_count > 0) {
        message("TEST: Verification successful - ", transformed_count, " rows in transformed data")
        message("TEST: Data flow: staged (", staged_count, ") -> transformed (", transformed_count, ")")
        
        # Check for standardized fields
        sample_query <- paste0("SELECT * FROM ", target_table, " LIMIT 3")
        sample_data <- dbGetQuery(transformed_data, sample_query)
        message("TEST: Sample transformed data:")
        print(sample_data)
        
        # Check for required standardized columns (product_line_id and product_id are critical)
        required_standard_cols <- c("product_line_id", "product_id", "brand", "competitor_rank")
        transform_cols <- c("etl_transform_timestamp", "etl_phase", "schema_version")
        actual_cols <- names(sample_data)
        
        # Specifically verify product_line_id and product_id mapping
        if ("product_line_id" %in% actual_cols && "product_id" %in% actual_cols) {
          message("TEST: ✅ Key fields present - product_line_id and product_id (from asin)")
        } else {
          message("TEST: ❌ Missing critical fields - product_line_id or product_id")
        }
        
        missing_standard <- setdiff(required_standard_cols, actual_cols)
        missing_transform <- setdiff(transform_cols, actual_cols)
        
        if (length(missing_standard) > 0) {
          message("TEST WARNING: Missing standardized columns: ", paste(missing_standard, collapse = ", "))
        } else {
          message("TEST: All required standardized columns present")
        }
        
        if (length(missing_transform) > 0) {
          message("TEST INFO: Missing transform columns: ", paste(missing_transform, collapse = ", "))
        } else {
          message("TEST: All transform metadata columns added")
        }
        
        # Check for business rules application
        if ("competitor_rank" %in% actual_cols) {
          rank_query <- paste0("SELECT MIN(competitor_rank) as min_rank, MAX(competitor_rank) as max_rank FROM ", target_table)
          rank_stats <- dbGetQuery(transformed_data, rank_query)
          message("TEST: Competitor rank range: ", rank_stats$min_rank, " - ", rank_stats$max_rank)
        }
        
        # Check for lookup keys
        if ("lookup_key" %in% actual_cols) {
          lookup_query <- paste0("SELECT COUNT(DISTINCT lookup_key) as unique_keys FROM ", target_table)
          lookup_stats <- dbGetQuery(transformed_data, lookup_query)
          message("TEST: Unique lookup keys generated: ", lookup_stats$unique_keys)
        }
        
        # Check schema version
        if ("schema_version" %in% actual_cols) {
          version_query <- paste0("SELECT DISTINCT schema_version FROM ", target_table)
          schema_version <- dbGetQuery(transformed_data, version_query)$schema_version[1]
          message("TEST: Schema version: ", schema_version)
        }
        
        test_passed <- TRUE
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no data found in transformed table")
      }
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - transformed table ", target_table, " not found")
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
  message("DEINITIALIZE: ETL04 Transform Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL04 Transform Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL04 Transform Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(staged_data)
DBI::dbDisconnect(transformed_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL04 Transform Phase (amz_ETL04_2TR.R) completed")