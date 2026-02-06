# amz_ETL05_2TR.R - Amazon 評論屬性資料轉換
# ETL05 階段 2 轉換：商業規則應用、lookup key 生成
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
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)

message("INITIALIZE: Amazon comment properties transform (ETL05 2TR) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL05 Transform Phase - Amazon comment properties...")

  # Load staged data from previous phase
  source_table <- "df_all_comment_property___staged"
  
  # Check if source table exists
  if (!source_table %in% dbListTables(staged_data)) {
    stop("Source table ", source_table, " not found in staged_data. Please run ETL05_1ST first.")
  }
  
  # Load staged comment properties data
  staged_comment_properties <- dbGetQuery(staged_data, paste("SELECT * FROM", source_table))
  message("MAIN: Loaded ", nrow(staged_comment_properties), " staged comment properties")
  
  # Transform comment properties with business rules
  transformed_comment_properties <- transform_comment_properties(
    staged_data = staged_comment_properties,
    standardize_fields = TRUE,
    apply_business_rules = TRUE,
    generate_lookup_keys = TRUE,
    encoding_target = "UTF-8"
  )
  
  # Write to transformed database
  target_table <- "df_all_comment_property___transformed"
  dbWriteTable(transformed_data, target_table, transformed_comment_properties, overwrite = TRUE)
  
  script_success <- TRUE
  message("MAIN: ETL05 Transform Phase completed successfully")
  message("MAIN: Transformed ", nrow(transformed_comment_properties), " comment properties")

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
    message("TEST: Verifying ETL05 Transform Phase results...")

    # Check if transformed table exists and has data
    target_table <- "df_all_comment_property___transformed"
    
    if (target_table %in% dbListTables(transformed_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", target_table)
      transformed_count <- dbGetQuery(transformed_data, query)$count

      if (transformed_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", transformed_count,
                " comment properties transformed")
        
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", target_table, " LIMIT 3")
        sample_data <- dbGetQuery(transformed_data, structure_query)
        message("TEST: Sample transformed data structure:")
        print(sample_data)
        
        # Check for lookup keys
        lookup_keys <- c("property_lookup_key", "product_property_key", "temporal_key")
        actual_cols <- names(sample_data)
        missing_keys <- setdiff(lookup_keys, actual_cols)
        
        if (length(missing_keys) > 0) {
          message("TEST WARNING: Missing lookup keys: ", paste(missing_keys, collapse = ", "))
        } else {
          message("TEST: All lookup keys present")
        }
        
        # Check final quality score
        quality_query <- paste0("SELECT AVG(final_quality_score) as avg_quality FROM ", target_table)
        avg_quality <- dbGetQuery(transformed_data, quality_query)$avg_quality
        message("TEST: Average final quality score: ", round(avg_quality, 2), "%")
        
        # Check property distribution by type
        type_query <- paste0("SELECT type, COUNT(*) as count FROM ", target_table, " GROUP BY type")
        type_dist <- dbGetQuery(transformed_data, type_query)
        message("TEST: Property type distribution:")
        print(type_dist)
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no transformed comment properties found")
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
  message("DEINITIALIZE: ETL05 Transform Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL05 Transform Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL05 Transform Phase failed during execution")
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

message("DEINITIALIZE: ETL05 Transform Phase (amz_ETL05_2TR.R) completed")