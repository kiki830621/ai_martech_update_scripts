# amz_ETL03_0IM.R - Amazon 商品屬性資料匯入
# ETL03 階段 0 匯入：從 Google Sheets 匯入商品屬性資料
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

message("INITIALIZE: Amazon product profiles import script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting Amazon product profiles import...")

  # Import product profiles for all active product lines
  import_result <- import_product_profiles(
    db_connection = raw_data,
    product_line_df = df_product_line,
    google_sheet_id = "16-k48xxFzSZm2p8j9SZf4V041fldcYnR8ectjsjuxZQ",
    sheet_name_prefix = "product_profile"
  )
  
  # Check if import actually succeeded
  message("MAIN: Import function returned: ", length(import_result), " product line results")
  
  # List all tables in raw_data database
  all_tables <- dbListTables(raw_data)
  message("MAIN: All tables in raw_data: ", paste(all_tables, collapse = ", "))
  
  # Check specific product profile tables
  product_profile_tables <- all_tables[grepl("^df_product_profile_", all_tables)]
  message("MAIN: product profile tables found: ", paste(product_profile_tables, collapse = ", "))

  script_success <- TRUE
  message("MAIN: Amazon product profiles import completed successfully")

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
    message("TEST: Verifying product profiles import...")

    # Check if individual product profile tables exist and have data
    # Get active product lines (excluding 'all')
    active_product_lines <- df_product_line %>%
      filter(included == TRUE, product_line_id != "all") %>%
      pull(product_line_id)
    
    total_product_count <- 0
    tables_found <- 0
    
    for (product_line_id in active_product_lines) {
      table_name <- paste0("df_product_profile_", product_line_id)
      if (dbExistsTable(raw_data, table_name)) {
        tables_found <- tables_found + 1
        query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
        product_count <- dbGetQuery(raw_data, query)$count
        total_product_count <- total_product_count + product_count
        message("TEST: Found ", product_count, " products in ", table_name)
      }
    }
    
    if (tables_found > 0 && total_product_count > 0) {
      test_passed <- TRUE
      message("TEST: Verification successful - ", total_product_count,
              " total product profiles imported across ", tables_found, " product lines")
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - no product profile tables found or empty")
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

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: Amazon product profiles import script completed")