# amz_ETL03_2TR.R - Amazon 商品屬性資料轉換
# ETL03 階段 2 轉換：將暫存資料轉換為業務分析格式
# 遵循 R113：四部分更新腳本結構

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

# Initialize using unified autoinit system
tryCatch({
  autoinit()
  message("INITIALIZE: autoinit() completed successfully")
}, error = function(e) {
  stop("INITIALIZE: autoinit() failed. Please run this script from the application root directory. Error: ", e$message)
})

# Verify essential dependencies are loaded
required_functions <- c("transform_product_profiles", "dbConnectDuckdb")
missing_functions <- c()

for (func_name in required_functions) {
  if (!exists(func_name)) {
    missing_functions <- c(missing_functions, func_name)
  }
}

if (length(missing_functions) > 0) {
  stop("INITIALIZE: Missing required functions: ", paste(missing_functions, collapse = ", "), 
       ". Please ensure proper initialization from application root directory.")
}

# Verify essential variables are available
required_vars <- c("db_path_list", "app_configs")
missing_vars <- c()

for (var_name in required_vars) {
  if (!exists(var_name)) {
    missing_vars <- c(missing_vars, var_name)
  }
}

if (length(missing_vars) > 0) {
  stop("INITIALIZE: Missing required variables: ", paste(missing_vars, collapse = ", "), 
       ". Please ensure proper initialization.")
}

# Verify database paths are available
required_db_paths <- c("staged_data", "transformed_data")
missing_db_paths <- setdiff(required_db_paths, names(db_path_list))

if (length(missing_db_paths) > 0) {
  stop("INITIALIZE: Missing database paths: ", paste(missing_db_paths, collapse = ", "))
}

# Establish database connections using dbConnectDuckdb
tryCatch({
  staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
  message("INITIALIZE: Connected to staged_data: ", db_path_list$staged_data)
}, error = function(e) {
  stop("INITIALIZE: Failed to connect to staged_data database: ", e$message)
})

tryCatch({
  transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)
  message("INITIALIZE: Connected to transformed_data: ", db_path_list$transformed_data)
}, error = function(e) {
  stop("INITIALIZE: Failed to connect to transformed_data database: ", e$message)
})

# Verify column aliases configuration is available
if (exists("app_configs") && "list_colname_aliases" %in% names(app_configs)) {
  alias_config <- app_configs$list_colname_aliases
  if ("standard_names" %in% names(alias_config)) {
    standard_fields <- names(alias_config$standard_names)
    message("INITIALIZE: Column aliases loaded for: ", paste(standard_fields, collapse = ", "))
  } else {
    warning("INITIALIZE: No standard_names found in alias configuration")
  }
} else {
  warning("INITIALIZE: Column aliases configuration not available - will use fallback logic")
}

message("INITIALIZE: Amazon product profiles transform script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting Amazon product profiles transformation...")
  
  # Verify that staged database has expected tables
  staged_tables <- DBI::dbListTables(staged_data)
  staged_product_tables <- staged_tables[grepl("^df_product_profile_.*___staged$", staged_tables)]
  
  if (length(staged_product_tables) == 0) {
    stop("MAIN: No staged product profile tables found. Please run staging scripts first.")
  }
  
  message("MAIN: Found ", length(staged_product_tables), " staged tables to transform: ", 
          paste(staged_product_tables, collapse = ", "))

  # Use the dedicated transform function
  transform_results <- transform_product_profiles(
    staged_db_connection = staged_data,
    transformed_db_connection = transformed_data,
    table_pattern = "^df_product_profile_.*___staged$",
    overwrite_existing = TRUE
  )
  
  # Extract results for verification
  total_transformed_rows <- transform_results$total_rows_transformed
  transformed_tables_count <- length(transform_results$tables_processed)

  script_success <- TRUE
  message("MAIN: Amazon product profiles transformation completed successfully")
  message("MAIN: Processed ", transformed_tables_count, " tables")
  message("MAIN: Total transformed rows: ", total_transformed_rows)

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message("MAIN ERROR: ", e$message)
  if (!is.null(e$call)) {
    message("MAIN ERROR Call: ", deparse(e$call))
  }
})

# ==============================================================================
# 3. TEST
# ==============================================================================

if (script_success) {
  tryCatch({
    message("TEST: Verifying product profiles transformation...")

    # Use transform results for verification
    if (length(transform_results$tables_processed) == 0) {
      test_passed <- FALSE
      message("TEST: Verification failed - no transformed tables were processed")
    } else if (transform_results$total_rows_transformed == 0) {
      test_passed <- FALSE
      message("TEST: Verification failed - no rows were transformed")
    } else {
      # Verify tables actually exist in database
      transformed_tables <- DBI::dbListTables(transformed_data)
      existing_transformed_tables <- intersect(transform_results$tables_processed, 
                                             transformed_tables)
      
      if (length(existing_transformed_tables) == length(transform_results$tables_processed)) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", transform_results$total_rows_transformed,
                " total transformed product profiles across ", 
                length(transform_results$tables_processed), " tables")
        
        # Verify column aliases configuration is available
        if (exists("app_configs") && "list_colname_aliases" %in% names(app_configs)) {
          alias_config <- app_configs$list_colname_aliases
          if ("standard_names" %in% names(alias_config)) {
            standard_fields <- names(alias_config$standard_names)
            message("TEST: Column aliases available for: ", paste(standard_fields, collapse = ", "))
          } else {
            message("TEST: Warning - no standard_names found in alias configuration")
          }
        } else {
          message("TEST: Warning - column aliases configuration not available")
        }
        
        # Verify transformation metadata on first table
        sample_table <- transform_results$tables_processed[1]
        sample_query <- paste0("SELECT etl_transform_timestamp, etl_phase, schema_version FROM ", 
                              sample_table, " LIMIT 1")
        sample_data <- DBI::dbGetQuery(transformed_data, sample_query)
        message("TEST: Sample transform metadata - Phase: ", sample_data$etl_phase, 
                ", Schema: ", sample_data$schema_version)
                
        # Verify fixed column ordering and Chinese attributes
        columns_query <- paste0("SELECT * FROM ", sample_table, " LIMIT 1")
        sample_columns <- DBI::dbGetQuery(transformed_data, columns_query)
        expected_start_cols <- c("product_brand", "product_id", "product_title", 
                               "product_line_id", "price", "rating", "num_rating")
        actual_start_cols <- names(sample_columns)[1:7]
        
        if (identical(actual_start_cols, expected_start_cols)) {
          message("TEST: Column ordering verification successful")
        } else {
          test_passed <- FALSE
          message("TEST: Column ordering verification failed")
          message("TEST: Expected: ", paste(expected_start_cols, collapse = ", "))
          message("TEST: Actual: ", paste(actual_start_cols, collapse = ", "))
        }
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - some transformed tables missing from database")
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

# Determine final status before tearing down
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
tryCatch({
  if (exists("staged_data") && inherits(staged_data, "DBIConnection")) {
    DBI::dbDisconnect(staged_data)
    message("DEINITIALIZE: Disconnected from staged_data")
  }
}, error = function(e) {
  warning("DEINITIALIZE: Error disconnecting staged_data: ", e$message)
})

tryCatch({
  if (exists("transformed_data") && inherits(transformed_data, "DBIConnection")) {
    DBI::dbDisconnect(transformed_data)
    message("DEINITIALIZE: Disconnected from transformed_data")
  }
}, error = function(e) {
  warning("DEINITIALIZE: Error disconnecting transformed_data: ", e$message)
})

# Clean up resources using autodeinit system
tryCatch({
  if (exists("autodeinit")) {
    autodeinit()
    message("DEINITIALIZE: autodeinit() completed")
  }
}, error = function(e) {
  warning("DEINITIALIZE: Error in autodeinit: ", e$message)
})

message("DEINITIALIZE: Amazon product profiles transform script completed")