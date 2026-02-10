# ==============================================================================
# MAMBA-Specific eBay Order Details ETL - Staging Phase (1ST)
# Following DM_R037: Company-Specific ETL Naming Rule
# Following MP104: ETL Data Flow Separation Principle
# Following MP064: ETL-Derivation Separation Principle
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby) - Custom SQL Server Implementation
# Data Type: Order Details (BAYORE table - order line items)
# Phase: 1ST (Staging)
# 
# This script stages BAYORE data from raw to staged layer
# Handles encoding issues and standardizes column names
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP031: Initialization First
# Following DM_R039: Database Connection Pattern Rule

message(strrep("=", 80))
message("INITIALIZE: Starting MAMBA eBay Order Details Staging")
message("INITIALIZE: Script: eby_ETL_order_details_1ST___MAMBA.R")
message("INITIALIZE: Company-specific implementation for MAMBA")
message("INITIALIZE: Data type: Order Details (BAYORE staging)")
message(strrep("=", 80))

# ------------------------------------------------------------------------------
# 1.1: Basic Initialization
# ------------------------------------------------------------------------------

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_order_details_1ST___MAMBA"
script_version <- "2.0.0"  # New separated architecture

# Following MP101: Global Environment Access Pattern
# Following MP103: Auto-deinit Behavior
if (!exists("autoinit", mode = "function")) {
  source(file.path("..", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

# The autoinit() function automatically detects the script location
autoinit()

# Load required libraries
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)
library(stringr)

# Source required functions (Following DM_R039)
source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R")

# Following MP106: Console Transparency
message("INITIALIZE: [OK] Global initialization complete")
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message("INITIALIZE: Following MP064 ETL-Derivation Separation")
message("INITIALIZE: Following MP104 ETL Data Flow Separation")

# ------------------------------------------------------------------------------
# 1.2: Database Connections (Following DM_R039)
# ------------------------------------------------------------------------------
message("INITIALIZE: Establishing database connections...")

# Connect to both raw and staged databases
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)

message("INITIALIZE: ✅ Database connections established")
message(sprintf("INITIALIZE: Source: %s", db_path_list$raw_data))
message(sprintf("INITIALIZE: Target: %s", db_path_list$staged_data))

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

message("MAIN: Starting MAMBA eBay ORDER DETAILS staging process")
main_start_time <- Sys.time()

# Following MP099: Real-time Progress Reporting
progress_counter <- 0
report_progress <- function(msg) {
  progress_counter <<- progress_counter + 1
  elapsed <- round(difftime(Sys.time(), main_start_time, units = "secs"), 2)
  message(sprintf("[%03d @ %.2fs] %s", progress_counter, elapsed, msg))
}

tryCatch({
  # ------------------------------------------------------------------------------
  # 2.1: Read Raw Data
  # ------------------------------------------------------------------------------
  report_progress("Reading raw BAYORE data...")
  
  raw_details <- dbReadTable(raw_data, "df_eby_order_details___raw___MAMBA")
  n_raw <- nrow(raw_details)
  report_progress(sprintf("Loaded %d raw order details", n_raw))
  
  # ------------------------------------------------------------------------------
  # 2.2: Column Standardization (Following MP064 - 1ST phase responsibilities)
  # ------------------------------------------------------------------------------
  report_progress("Standardizing column names...")
  
  # Check which columns actually exist in the raw data
  existing_columns <- names(raw_details)
  message(sprintf("MAIN: Found %d columns in raw data", length(existing_columns)))
  message(sprintf("MAIN: Columns: %s", paste(existing_columns, collapse = ", ")))
  
  # Define the full column mapping
  # Column mapping based on official codebook.csv from eBay SQL Server
  column_mapping <- list(
    # Order linking fields
    ORE001 = "order_id",          # 單號 (Order Number) - Links to BAYORD.ORD001
    ORE002 = "line_item_number",  # 流水號 (Serial Number)
    
    # Product identification
    ORE003 = "ebay_item_code",    # EBAY商品代號 (eBay Item Code)
    ORE004 = "product_name",      # 品名 (Product Name)
    ORE005 = "erp_product_no",    # ERP品號 (ERP Product Number)
    ORE006 = "application_data",  # ApplicationData (likely SKU/custom label)
    
    # Product details
    ORE007 = "condition",         # 新舊程度 (Product Condition - New/Used/etc)
    
    # Quantities and pricing
    ORE008 = "quantity",          # 數量 (Quantity)
    ORE009 = "unit_price",        # 單價 (Unit Price)
    
    # Additional fields
    ORE010 = "listing_country",   # 上架國別 (Listing Country - int code)
    ORE011 = "email",             # Email
    ORE012 = "static_alias",      # StaticAlias
    
    # Critical JOIN key
    ORE013 = "batch_key",         # Unnamed field - matches with BAYORD.ORD022
    ORE014 = "reserved_field"     # Unnamed field in codebook
    
    # Note: Only ORE001-ORE014 exist in current data due to UTF-8 issues
    # Extended columns ORE015-ORE030 would include:
    # - Additional product variations
    # - Seller details
    # - Fulfillment information
    # - eBay-specific identifiers
    # - Cost breakdowns
  )
  
  # Filter mapping to only include columns that exist
  columns_to_rename <- column_mapping[names(column_mapping) %in% existing_columns]
  
  if (length(columns_to_rename) == 0) {
    stop("MAIN: No recognized BAYORE columns found in raw data")
  }
  
  message(sprintf("MAIN: Renaming %d columns", length(columns_to_rename)))
  
  # Check for missing critical columns
  critical_columns <- c("ORE001", "ORE002", "ORE004", "ORE013")
  missing_critical <- setdiff(critical_columns, names(columns_to_rename))
  if (length(missing_critical) > 0) {
    warning(sprintf("MAIN: ⚠️ Missing critical columns: %s", paste(missing_critical, collapse = ", ")))
  }
  
  # Perform the rename with only existing columns
  # Note: rename() expects new_name = old_name format
  # So we reverse the mapping: setNames(old_names, new_names)
  rename_args <- setNames(names(columns_to_rename), unlist(columns_to_rename))
  staged_details <- raw_details %>%
    rename(!!!rename_args)
  
  # ------------------------------------------------------------------------------
  # 2.3: Data Type Conversions and Cleaning
  # ------------------------------------------------------------------------------
  report_progress("Converting data types and cleaning...")
  
  # Get the column names that exist in staged_details
  staged_columns <- names(staged_details)
  
  # Apply conversions only for columns that exist
  staged_details <- staged_details %>%
    mutate(
      # Numeric conversions based on codebook data types
      across(any_of(c("quantity")), ~as.integer(.)),  # 數量
      across(any_of(c("unit_price")), ~as.numeric(.)), # 單價
      across(any_of(c("listing_country")), ~as.integer(.)), # 上架國別 (int code)
      
      # Add staging metadata
      staged_timestamp = Sys.time(),
      staging_version = script_version
    )
  
  # Handle encoding for batch_key if it exists
  if ("batch_key" %in% staged_columns) {
    staged_details <- staged_details %>%
      mutate(batch_key = iconv(batch_key, from = "latin1", to = "UTF-8", sub = ""))
  }
  
  # Clean text fields that exist
  text_fields_to_clean <- intersect(
    c("product_name", "erp_product_no", "application_data", "condition", "email", "static_alias"),
    staged_columns
  )
  
  if (length(text_fields_to_clean) > 0) {
    staged_details <- staged_details %>%
      mutate(across(all_of(text_fields_to_clean), str_trim))
  }
  
  # Standardize condition field if it exists
  if ("condition" %in% staged_columns) {
    staged_details <- staged_details %>%
      mutate(condition = str_trim(condition))
  }
  
  # Calculate derived fields only if source columns exist
  if (all(c("quantity", "line_total", "unit_price") %in% staged_columns)) {
    staged_details <- staged_details %>%
      mutate(actual_unit_price = if_else(quantity > 0, line_total / quantity, unit_price))
  } else if (all(c("quantity", "line_total") %in% staged_columns)) {
    # If unit_price doesn't exist, still calculate actual_unit_price
    staged_details <- staged_details %>%
      mutate(actual_unit_price = if_else(quantity > 0, line_total / quantity, NA_real_))
  }
  
  message(sprintf("MAIN: Processed %d columns", ncol(staged_details)))
  
  # ------------------------------------------------------------------------------
  # 2.4: Data Quality Checks
  # ------------------------------------------------------------------------------
  report_progress("Performing data quality checks...")
  
  # Get the column names that exist in staged_details
  staged_columns <- names(staged_details)
  
  # Check for duplicates if key columns exist
  if (all(c("order_id", "line_item_number") %in% staged_columns)) {
    n_duplicates <- staged_details %>%
      group_by(order_id, line_item_number) %>%
      filter(n() > 1) %>%
      nrow()
    
    if (n_duplicates > 0) {
      warning(sprintf("MAIN: Found %d duplicate line items", n_duplicates))
      # Remove duplicates, keeping the latest
      if ("transaction_date" %in% staged_columns) {
        staged_details <- staged_details %>%
          group_by(order_id, line_item_number) %>%
          arrange(desc(transaction_date)) %>%
          slice(1) %>%
          ungroup()
      } else {
        # If no transaction_date, just keep first occurrence
        staged_details <- staged_details %>%
          group_by(order_id, line_item_number) %>%
          slice(1) %>%
          ungroup()
      }
    }
  }
  
  # Check for missing critical fields (only for columns that exist)
  if ("order_id" %in% staged_columns) {
    missing_order_ids <- sum(is.na(staged_details$order_id))
    message(sprintf("MAIN: Missing order_ids: %d", missing_order_ids))
  }
  
  if ("line_item_number" %in% staged_columns) {
    missing_line_items <- sum(is.na(staged_details$line_item_number))
    message(sprintf("MAIN: Missing line_item_numbers: %d", missing_line_items))
  }
  
  if ("batch_key" %in% staged_columns) {
    missing_batch <- sum(is.na(staged_details$batch_key))
    message(sprintf("MAIN: Missing batch_keys: %d (critical for JOIN)", missing_batch))
  } else {
    message("MAIN: ⚠️ batch_key column not present - JOINs with orders may fail")
  }
  
  if ("product_sku" %in% staged_columns) {
    missing_sku <- sum(is.na(staged_details$product_sku))
    message(sprintf("MAIN: Missing product_skus: %d", missing_sku))
  }
  
  # Check for data anomalies (only for columns that exist)
  if ("quantity" %in% staged_columns) {
    negative_quantities <- sum(staged_details$quantity < 0, na.rm = TRUE)
    if (negative_quantities > 0) {
      warning(sprintf("MAIN: Found %d negative quantities", negative_quantities))
    }
  }
  
  if ("unit_price" %in% staged_columns) {
    negative_prices <- sum(staged_details$unit_price < 0, na.rm = TRUE)
    if (negative_prices > 0) {
      warning(sprintf("MAIN: Found %d negative prices", negative_prices))
    }
  }
  
  # Report on available vs expected columns
  expected_columns <- c("order_id", "line_item_number", "batch_key", "product_sku", 
                        "quantity", "unit_price", "line_total", "transaction_date")
  available_expected <- intersect(expected_columns, staged_columns)
  missing_expected <- setdiff(expected_columns, staged_columns)
  
  message(sprintf("MAIN: Available expected columns: %d/%d", 
                  length(available_expected), length(expected_columns)))
  if (length(missing_expected) > 0) {
    message(sprintf("MAIN: Missing expected columns: %s", 
                    paste(missing_expected, collapse = ", ")))
  }
  
  # ------------------------------------------------------------------------------
  # 2.5: Store Staged Data
  # ------------------------------------------------------------------------------
  report_progress("Storing staged BAYORE data...")
  
  # Store in staged_data database with MAMBA-specific naming
  table_name <- "df_eby_order_details___staged___MAMBA"
  
  if (dbExistsTable(staged_data, table_name)) {
    dbRemoveTable(staged_data, table_name)
    message(sprintf("MAIN: Dropped existing table: %s", table_name))
  }
  
  dbWriteTable(staged_data, table_name, staged_details)
  n_staged <- nrow(staged_details)
  message(sprintf("MAIN: ✅ Stored %d staged order details in %s", n_staged, table_name))
  
  # Display sample for verification - safely select columns that exist
  message("MAIN: Sample of staged BAYORE data:")
  sample_data <- head(staged_details, 3)
  
  # Only show columns that actually exist
  display_cols <- intersect(
    c("order_id", "line_item_number", "batch_key", "product_name", "quantity", "unit_price"),
    names(sample_data)
  )
  
  if (length(display_cols) > 0) {
    print(sample_data[, display_cols])
  } else {
    # If none of the expected columns exist, show first 5 columns
    print(sample_data[, 1:min(5, ncol(sample_data))])
  }
  
  main_elapsed <- round(difftime(Sys.time(), main_start_time, units = "secs"), 2)
  message(sprintf("MAIN: ✅ Order details staging completed in %.2f seconds", main_elapsed))
  
}, error = function(e) {
  message(sprintf("MAIN: ❌ Error during order details staging: %s", e$message))
  stop(e)
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

message("TEST: Starting validation tests...")
test_start_time <- Sys.time()

tryCatch({
  # Test 1: Verify table exists
  if (!dbExistsTable(staged_data, "df_eby_order_details___staged___MAMBA")) {
    stop("TEST: Table df_eby_order_details___staged___MAMBA does not exist")
  }
  message("TEST: ✅ Table exists")
  
  # Test 2: Verify data staged
  row_count <- dbGetQuery(staged_data, "SELECT COUNT(*) as n FROM df_eby_order_details___staged___MAMBA")$n
  if (row_count == 0) {
    stop("TEST: No data in df_eby_order_details___staged___MAMBA")
  }
  message(sprintf("TEST: ✅ Data staged (%d rows)", row_count))
  
  # Test 3: Verify standardized columns exist
  columns <- dbListFields(staged_data, "df_eby_order_details___staged___MAMBA")
  
  # Check for essential columns (ones we know should exist from the mapping)
  essential_cols <- c("order_id", "line_item_number", "batch_key")
  missing_essential <- setdiff(essential_cols, columns)
  
  if (length(missing_essential) > 0) {
    warning(sprintf("TEST: ⚠️ Missing essential columns: %s", paste(missing_essential, collapse = ", ")))
  } else {
    message("TEST: ✅ Essential standardized columns present")
  }
  
  # Report on all standardized columns found
  expected_cols <- c("order_id", "line_item_number", "batch_key", "product_name", 
                     "quantity", "unit_price", "ebay_item_code", "erp_product_no")
  found_expected <- intersect(expected_cols, columns)
  message(sprintf("TEST: Found %d/%d expected columns", length(found_expected), length(expected_cols)))
  
  # Test 4: Verify no raw column names remain
  raw_cols <- grep("^ORE", columns, value = TRUE)
  if (length(raw_cols) > 0) {
    warning(sprintf("TEST: ⚠️ Raw columns still present: %s", paste(raw_cols, collapse = ", ")))
  } else {
    message("TEST: ✅ All columns standardized (no ORE* columns)")
  }
  
  # Test 5: Verify JOIN keys are ready
  join_key_test <- dbGetQuery(staged_data, 
    "SELECT COUNT(*) as n FROM df_eby_order_details___staged___MAMBA 
     WHERE order_id IS NOT NULL AND batch_key IS NOT NULL")$n
  message(sprintf("TEST: Records ready for JOIN: %d", join_key_test))
  
  test_elapsed <- round(difftime(Sys.time(), test_start_time, units = "secs"), 2)
  message(sprintf("TEST: ✅ All tests passed in %.2f seconds", test_elapsed))
  
}, error = function(e) {
  message(sprintf("TEST: ❌ Test failed: %s", e$message))
  stop(e)
})

# ==============================================================================
# PART 4: DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: Starting cleanup...")

# Close database connections
if (exists("raw_data") && !is.null(raw_data)) {
  dbDisconnect(raw_data)
  message("DEINITIALIZE: Disconnected from raw_data")
}

if (exists("staged_data") && !is.null(staged_data)) {
  dbDisconnect(staged_data)
  message("DEINITIALIZE: Disconnected from staged_data")
}

# Final timing
total_elapsed <- round(difftime(Sys.time(), script_start_time, units = "secs"), 2)
message(sprintf("DEINITIALIZE: Total execution time: %.2f seconds", total_elapsed))

# ==============================================================================
# PART 5: AUTODEINIT
# ==============================================================================
# Following MP103: autodeinit() must be the absolute last statement

message("AUTODEINIT: Executing final cleanup...")
autodeinit()
