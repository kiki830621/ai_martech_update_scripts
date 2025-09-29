# ==============================================================================
# MAMBA-Specific eBay Sales ETL - Transform Phase ONLY (2TR)
# Following MP109: Derived Data Pipeline Principle
# Following DM_R037: Company-Specific ETL Naming Rule
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby)
# Data Type: Sales (DERIVED from orders + order_details)
# Phase: 2TR ONLY (Transform) - This is a DERIVED ETL per MP109
# 
# IMPORTANT: Sales is a DERIVED entity created by JOINing orders and order_details
# Per MP109, derived ETLs don't need 0IM or 1ST phases
# This script reads from staged orders and order_details tables
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP031: Initialization First
# Following MP109: Derived Data Pipeline Principle

message(strrep("=", 80))
message("INITIALIZE: Starting MAMBA eBay Sales Transform (eby_ETL_sales_2TR___MAMBA.R)")
message("INITIALIZE: DERIVED ETL - No 0IM or 1ST phases needed (MP109)")
message("INITIALIZE: Creates sales by JOINing orders + order_details")
message(strrep("=", 80))

# ------------------------------------------------------------------------------
# 1.1: Basic Initialization
# ------------------------------------------------------------------------------

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_sales_2TR___MAMBA"
script_version <- "2.0.0"  # Version 2: Refactored as DERIVED ETL

# Following MP101: Global Environment Access Pattern
# Following MP103: Auto-deinit Behavior
if (!exists("autoinit", mode = "function")) {
  source(file.path("..", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

# Initialize the environment
autoinit()

# Load required libraries
library(DBI)
library(duckdb)
library(dplyr)
library(data.table)

# Source required functions
source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R")

# Following MP106: Console Transparency
message("INITIALIZE: [OK] Global initialization complete")
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message("INITIALIZE: Following MP109 Derived Data Pipeline pattern")
message("INITIALIZE: Following MP064 ETL-Derivation Separation")
message("INITIALIZE: Following DM_R040 Structural JOIN pattern")

# ------------------------------------------------------------------------------
# 1.2: Database Connections (Following DM_R039)
# ------------------------------------------------------------------------------
message("INITIALIZE: Establishing database connections...")

# Connect to staged and transformed databases
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)

message("INITIALIZE: ✅ Database connections established")
message(sprintf("INITIALIZE: Reading from: %s", db_path_list$staged_data))
message(sprintf("INITIALIZE: Writing to: %s", db_path_list$transformed_data))

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

message("MAIN: Starting DERIVED sales transformation (orders + order_details JOIN)")
main_start_time <- Sys.time()

tryCatch({
  # ------------------------------------------------------------------------------
  # 2.1: Load Staged Data from BASE ETLs
  # ------------------------------------------------------------------------------
  message("MAIN: Loading staged data from BASE ETLs...")
  
  # Check if required tables exist
  staged_tables <- dbListTables(staged_data)
  
  orders_table <- "df_eby_orders___staged___MAMBA"
  details_table <- "df_eby_order_details___staged___MAMBA"
  
  if (!orders_table %in% staged_tables) {
    stop(sprintf("Required table %s not found. Run eby_ETL_orders_1ST first.", orders_table))
  }
  
  if (!details_table %in% staged_tables) {
    stop(sprintf("Required table %s not found. Run eby_ETL_order_details_1ST first.", details_table))
  }
  
  # Load orders data
  message(sprintf("MAIN: Loading %s...", orders_table))
  orders_staged <- dbGetQuery(staged_data, sprintf("SELECT * FROM %s", orders_table))
  n_orders <- nrow(orders_staged)
  message(sprintf("MAIN: Loaded %d orders", n_orders))
  
  # Load order_details data
  message(sprintf("MAIN: Loading %s...", details_table))
  details_staged <- dbGetQuery(staged_data, sprintf("SELECT * FROM %s", details_table))
  n_details <- nrow(details_staged)
  message(sprintf("MAIN: Loaded %d order detail lines", n_details))
  
  # ------------------------------------------------------------------------------
  # 2.2: Perform Structural JOIN (Following DM_R040)
  # ------------------------------------------------------------------------------
  message("MAIN: Performing structural JOIN (MP064: Allowed in 2TR for derived entities)")
  
  # Convert to data.table for efficient processing
  dt_orders <- as.data.table(orders_staged)
  dt_details <- as.data.table(details_staged)
  
  # =============================================================================
  # CRITICAL DATABASE DESIGN DOCUMENTATION - MAMBA eBay Composite Keys
  # =============================================================================
  # The MAMBA eBay database uses an UNUSUAL composite key design that violates
  # normal database design principles. This must be handled carefully:
  #
  # BAYORD (Orders) Table:
  #   Primary Key: (ORD001, ORD009)
  #   - ORD001: Order Number (NOT unique by itself!)
  #   - ORD009: Seller eBay Email (required for uniqueness)
  #   
  # BAYORE (Order Details) Table:
  #   Primary Key: (ORE001, ORE002, ORE013)
  #   - ORE001: Order Number (matches ORD001)
  #   - ORE002: Line Item Number
  #   - ORE013: Contains COPY of seller email (matches ORD009)
  #
  # Foreign Key Relationship:
  #   BAYORE(ORE001, ORE013) → BAYORD(ORD001, ORD009)
  #   This means: (order_id, seller_email_copy) → (order_id, seller_email)
  #
  # WHY THIS IS UNUSUAL:
  # 1. Order numbers are NOT unique across sellers
  # 2. Same order number can exist for different sellers
  # 3. Seller email is duplicated in detail records (denormalization)
  # 4. JOIN requires matching BOTH order_id AND seller email
  #
  # COMMON MISTAKES TO AVOID:
  # - DON'T assume order_id alone is unique
  # - DON'T use ORD022 (batch_key in orders) for JOIN - it's NOT part of PK
  # - DON'T perform single-key JOIN on order_id only
  # - DON'T assume ORE013 is a batch number - it's actually seller email
  # =============================================================================
  
  # Identify join keys based on MAMBA database foreign key constraint
  # BAYORD columns after 1ST standardization:
  #   order_id = ORD001 (part of composite PK)
  #   seller_ebay_email = ORD009 (part of composite PK)
  # BAYORE columns after 1ST standardization:
  #   order_id = ORE001 (part of composite PK, FK)
  #   batch_key = ORE013 (part of composite PK, FK - contains seller email copy)
  
  # Check for required columns from 1ST phase
  order_keys <- c("order_id", "seller_ebay_email")  # From ORD001, ORD009
  detail_keys <- c("order_id", "batch_key")  # From ORE001, ORE013 (batch_key contains seller email)
  
  # Verify columns exist
  missing_order_cols <- setdiff(order_keys, names(dt_orders))
  if (length(missing_order_cols) > 0) {
    stop(sprintf("MAIN: ❌ Missing required order columns: %s", 
                 paste(missing_order_cols, collapse = ", ")))
  }
  
  missing_detail_cols <- setdiff(detail_keys, names(dt_details))
  if (length(missing_detail_cols) > 0) {
    stop(sprintf("MAIN: ❌ Missing required detail columns: %s", 
                 paste(missing_detail_cols, collapse = ", ")))
  }
  
  # ------------------------------------------------------------------------------
  # CRITICAL: Validate data integrity before JOIN
  # ------------------------------------------------------------------------------
  message("MAIN: Validating data integrity before JOIN...")
  
  # Check for duplicate composite keys in orders (should be none)
  dup_orders <- dt_orders[, .N, by = .(order_id, seller_ebay_email)][N > 1]
  if (nrow(dup_orders) > 0) {
    warning(sprintf("MAIN: ⚠️ Found %d duplicate composite keys in orders table!", nrow(dup_orders)))
    message("MAIN: Sample of duplicate order keys:")
    print(head(dup_orders, 5))
  } else {
    message("MAIN: ✅ No duplicate composite keys in orders")
  }
  
  # Check for NULL values in key columns
  null_order_keys <- dt_orders[is.na(order_id) | is.na(seller_ebay_email), .N]
  null_detail_keys <- dt_details[is.na(order_id) | is.na(batch_key), .N]
  
  if (null_order_keys > 0) {
    warning(sprintf("MAIN: ⚠️ Found %d orders with NULL key values", null_order_keys))
  }
  if (null_detail_keys > 0) {
    warning(sprintf("MAIN: ⚠️ Found %d details with NULL key values", null_detail_keys))
  }
  
  # Check if batch_key in details matches any seller_ebay_email in orders
  unique_seller_emails <- unique(dt_orders$seller_ebay_email)
  unique_batch_keys <- unique(dt_details$batch_key)
  matching_keys <- intersect(unique_seller_emails, unique_batch_keys)
  
  message(sprintf("MAIN: Unique seller emails in orders: %d", length(unique_seller_emails)))
  message(sprintf("MAIN: Unique batch keys in details: %d", length(unique_batch_keys)))
  message(sprintf("MAIN: Matching email/batch keys: %d", length(matching_keys)))
  
  if (length(matching_keys) == 0) {
    warning("MAIN: ⚠️ NO matching keys between seller_ebay_email and batch_key!")
    warning("MAIN: This suggests a data issue - ORE013 should contain seller email copies")
    
    # Show samples for debugging
    message("MAIN: Sample seller_ebay_email values:")
    print(head(unique_seller_emails, 5))
    message("MAIN: Sample batch_key values:")
    print(head(unique_batch_keys, 5))
  }
  
  # ------------------------------------------------------------------------------
  # Perform the COMPOSITE KEY JOIN
  # ------------------------------------------------------------------------------
  message("MAIN: Performing composite key JOIN...")
  message("MAIN: JOIN condition: order_id AND (seller_ebay_email = batch_key)")
  message(sprintf("MAIN: Orders table: %d rows", nrow(dt_orders)))
  message(sprintf("MAIN: Details table: %d rows", nrow(dt_details)))
  
  # CRITICAL: The JOIN must match the database foreign key relationship
  # BAYORE(ORE001, ORE013) → BAYORD(ORD001, ORD009)
  # This means: details(order_id, batch_key) → orders(order_id, seller_ebay_email)
  dt_sales <- dt_orders[dt_details, 
                        on = .(order_id = order_id,
                               seller_ebay_email = batch_key),  # ORD009 = ORE013
                        nomatch = 0]  # Inner join
  
  n_sales <- nrow(dt_sales)
  message(sprintf("MAIN: Created %d sales transaction records", n_sales))
  
  # Validate JOIN results
  if (n_sales == 0) {
    warning("MAIN: ❌ JOIN produced NO results! Check composite key matching.")
    stop("No sales records created - composite key mismatch")
  } else if (n_sales < nrow(dt_details) * 0.5) {
    warning(sprintf("MAIN: ⚠️ JOIN matched only %.1f%% of detail records", 
                    (n_sales / nrow(dt_details)) * 100))
    message("MAIN: This may indicate a composite key issue")
  } else {
    message(sprintf("MAIN: ✅ JOIN matched %.1f%% of detail records", 
                    (n_sales / nrow(dt_details)) * 100))
  }
  
  # ------------------------------------------------------------------------------
  # 2.3: Transform to Final Schema
  # ------------------------------------------------------------------------------
  message("MAIN: Transforming to final sales schema...")
  
  # Calculate derived fields
  dt_sales[, `:=`(
    # Create unique transaction ID
    transaction_id = paste0(order_number, "_", serial_number),
    
    # Calculate line-level totals
    line_total = quantity * unit_price,
    
    # Add transformation metadata
    transformation_timestamp = Sys.time(),
    transformation_version = script_version,
    etl_pipeline = "DERIVED_SALES"
  )]
  
  # Add time dimensions if order_date exists
  if ("order_date" %in% names(dt_sales)) {
    dt_sales[, `:=`(
      order_year = year(as.Date(order_date)),
      order_month = month(as.Date(order_date)),
      order_day = day(as.Date(order_date)),
      order_weekday = weekdays(as.Date(order_date))
    )]
  }
  
  # Select and order final columns
  # Keep all columns but ensure key columns are first
  setcolorder(dt_sales, c("transaction_id", "order_number", "serial_number"))
  
  # ------------------------------------------------------------------------------
  # 2.4: Store Transformed Data
  # ------------------------------------------------------------------------------
  message("MAIN: Storing transformed sales data...")
  
  # Following MP102: ETL Output Standardization
  # Following DM_R037: Company-specific naming
  table_name <- "df_eby_sales___transformed___MAMBA"
  
  if (dbExistsTable(transformed_data, table_name)) {
    dbRemoveTable(transformed_data, table_name)
    message(sprintf("MAIN: Dropped existing table: %s", table_name))
  }
  
  # Convert back to data.frame for database write
  df_sales <- as.data.frame(dt_sales)
  
  dbWriteTable(transformed_data, table_name, df_sales)
  message(sprintf("MAIN: ✅ Stored %d sales records in %s", n_sales, table_name))
  
  # Display sample for verification
  message("MAIN: Sample of transformed sales data:")
  sample_data <- head(df_sales, 3)
  if (ncol(sample_data) > 10) {
    # Show key columns only if too many columns
    key_cols <- c("transaction_id", "order_number", "product_name", 
                  "quantity", "unit_price", "line_total")
    key_cols <- intersect(key_cols, names(sample_data))
    print(sample_data[, key_cols])
  } else {
    print(sample_data)
  }
  
  # Calculate summary statistics
  if ("line_total" %in% names(df_sales)) {
    total_revenue <- sum(df_sales$line_total, na.rm = TRUE)
    message(sprintf("MAIN: Total revenue: £%.2f", total_revenue))
  }
  
  unique_orders <- length(unique(df_sales$order_number))
  message(sprintf("MAIN: Unique orders: %d", unique_orders))
  
  main_elapsed <- round(difftime(Sys.time(), main_start_time, units = "secs"), 2)
  message(sprintf("MAIN: ✅ Sales transformation completed in %.2f seconds", main_elapsed))
  
}, error = function(e) {
  message(sprintf("MAIN: ❌ Error during sales transformation: %s", e$message))
  stop(e)
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

message("TEST: Starting validation tests...")
test_start_time <- Sys.time()

tryCatch({
  # Test 1: Verify table exists
  if (!dbExistsTable(transformed_data, "df_eby_sales___transformed___MAMBA")) {
    stop("TEST: Table df_eby_sales___transformed___MAMBA does not exist")
  }
  message("TEST: ✅ Table exists")
  
  # Test 2: Verify data was created
  row_count <- dbGetQuery(transformed_data, 
    "SELECT COUNT(*) as n FROM df_eby_sales___transformed___MAMBA")$n
  if (row_count == 0) {
    stop("TEST: No data in df_eby_sales___transformed___MAMBA")
  }
  message(sprintf("TEST: ✅ Data created (%d rows)", row_count))
  
  # Test 3: Verify key columns exist
  columns <- dbListFields(transformed_data, "df_eby_sales___transformed___MAMBA")
  required_cols <- c("transaction_id", "order_number", "transformation_timestamp")
  missing_cols <- setdiff(required_cols, columns)
  
  if (length(missing_cols) > 0) {
    stop(sprintf("TEST: Missing required columns: %s", 
                 paste(missing_cols, collapse = ", ")))
  }
  message("TEST: ✅ All required columns present")
  
  # Test 4: Verify this is a DERIVED ETL (MP109 compliance)
  etl_type <- dbGetQuery(transformed_data, 
    "SELECT DISTINCT etl_pipeline FROM df_eby_sales___transformed___MAMBA LIMIT 1")
  if (etl_type$etl_pipeline == "DERIVED_SALES") {
    message("TEST: ✅ Correctly marked as DERIVED ETL (MP109 compliant)")
  }
  
  # Test 5: Verify no duplicate transactions
  dup_check <- dbGetQuery(transformed_data, "
    SELECT transaction_id, COUNT(*) as cnt 
    FROM df_eby_sales___transformed___MAMBA 
    GROUP BY transaction_id 
    HAVING COUNT(*) > 1
  ")
  if (nrow(dup_check) > 0) {
    warning(sprintf("TEST: ⚠️ Found %d duplicate transaction IDs", nrow(dup_check)))
  } else {
    message("TEST: ✅ No duplicate transactions")
  }
  
  # Test 6: Verify composite key integrity in result
  # Check that all sales records have both key components
  key_integrity <- dbGetQuery(transformed_data, "
    SELECT COUNT(*) as total,
           SUM(CASE WHEN order_number IS NOT NULL THEN 1 ELSE 0 END) as has_order,
           SUM(CASE WHEN seller_ebay_email IS NOT NULL THEN 1 ELSE 0 END) as has_seller
    FROM df_eby_sales___transformed___MAMBA
  ")
  
  if (key_integrity$has_order == key_integrity$total && 
      key_integrity$has_seller == key_integrity$total) {
    message("TEST: ✅ All sales records have complete composite keys")
  } else {
    warning(sprintf("TEST: ⚠️ Missing keys - Order: %d/%d, Seller: %d/%d",
                    key_integrity$has_order, key_integrity$total,
                    key_integrity$has_seller, key_integrity$total))
  }
  
  # Test 7: Sample validation - show JOIN results
  message("TEST: Sample of joined sales data (first 3 records):")
  sample_check <- dbGetQuery(transformed_data, "
    SELECT transaction_id, order_number, seller_ebay_email, 
           product_name, quantity, unit_price
    FROM df_eby_sales___transformed___MAMBA
    LIMIT 3
  ")
  print(sample_check)
  
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
if (exists("staged_data") && !is.null(staged_data)) {
  dbDisconnect(staged_data)
  message("DEINITIALIZE: Disconnected from staged_data")
}

if (exists("transformed_data") && !is.null(transformed_data)) {
  dbDisconnect(transformed_data)
  message("DEINITIALIZE: Disconnected from transformed_data")
}

# Final timing
total_elapsed <- round(difftime(Sys.time(), script_start_time, units = "secs"), 2)
message(sprintf("DEINITIALIZE: Total execution time: %.2f seconds", total_elapsed))

# ==============================================================================
# PART 5: AUTODEINIT
# ==============================================================================
# Following MP103: autodeinit() must be the absolute last statement

message("AUTODEINIT: Executing final cleanup...")
message(strrep("=", 80))
message(sprintf("DERIVED ETL COMPLETE: %s", script_name))
message("This is a DERIVED ETL following MP109 - no 0IM or 1ST phases needed")
message(strrep("=", 80))

autodeinit()