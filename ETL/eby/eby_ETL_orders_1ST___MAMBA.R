# ==============================================================================
# MAMBA-Specific eBay Orders ETL - Staging Phase (1ST)
# Following DM_R037: Company-Specific ETL Naming Rule
# Following MP104: ETL Data Flow Separation Principle
# Following MP064: ETL-Derivation Separation Principle
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby) - Custom SQL Server Implementation
# Data Type: Orders (BAYORD table - order headers)
# Phase: 1ST (Staging)
# 
# This script stages BAYORD data from raw to staged layer
# Handles encoding issues and standardizes column names
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP031: Initialization First
# Following DM_R039: Database Connection Pattern Rule

message(strrep("=", 80))
message("INITIALIZE: Starting MAMBA eBay Orders Staging (eby_ETL_orders_1ST___MAMBA.R)")
message("INITIALIZE: Company-specific implementation for MAMBA")
message("INITIALIZE: Data type: Orders (BAYORD staging)")
message(strrep("=", 80))

# ------------------------------------------------------------------------------
# 1.1: Basic Initialization
# ------------------------------------------------------------------------------

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_orders_1ST___MAMBA"
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

message("MAIN: Starting MAMBA eBay ORDERS staging process")
main_start_time <- Sys.time()

tryCatch({
  # ------------------------------------------------------------------------------
  # 2.1: Read Raw Data
  # ------------------------------------------------------------------------------
  message("MAIN: Reading raw BAYORD data...")
  
  raw_orders <- dbReadTable(raw_data, "df_eby_orders___raw___MAMBA")
  n_raw <- nrow(raw_orders)
  message(sprintf("MAIN: Loaded %d raw orders", n_raw))
  
  # ------------------------------------------------------------------------------
  # 2.2: Column Standardization (Following MP064 - 1ST phase responsibilities)
  # ------------------------------------------------------------------------------
  message("MAIN: Standardizing column names...")
  
  # Map cryptic BAYORD columns to business-friendly names
  # Based on official codebook.csv definitions from eBay SQL Server
  staged_orders <- raw_orders %>%
    rename(
      # Order identification
      order_id = ORD001,            # 單號 (Order Number) - Primary Key
      other_order_number = ORD002,  # 其它單號 (Other Order Number)
      order_date = ORD003,          # 訂單日期 (Order Date)
      payment_date = ORD004,        # 付款日期 (Payment Date)
      
      # Payment information
      payment_total = ORD005,       # 付款總額 (Payment Total Amount)
      payment_method = ORD006,      # 付款方式 (Payment Method - int code)
      payment_currency = ORD007,    # 付款幣別 (Payment Currency)
      
      # Seller information
      seller_ebay_account = ORD008, # 賣家EBAY帳號 (Seller eBay Account)
      seller_ebay_email = ORD009,   # 賣家EBAY郵件 (Seller eBay Email)
      
      # Shipping address fields
      recipient_name = ORD010,      # 收件人 (Recipient)
      street_address_1 = ORD011,    # Street1
      street_address_2 = ORD012,    # Street2
      city_name = ORD013,           # CityName
      state_or_province = ORD014,   # StateOrProvince
      postal_code = ORD015,         # PostalCode
      country_name = ORD016,        # CountryName - for geographic analysis
      
      # Additional contact and order info
      address_source = ORD017,      # 地址來源 (Address Source - int code)
      phone_area_code = ORD018,     # 電話區碼 (Phone Area Code)
      phone_number = ORD019,        # 電話 (Phone Number)
      buyer_ebay_id = ORD020,       # 買家EBAY (Buyer eBay ID)
      shipping_fee = ORD021,        # 運費 (Shipping Fee)
      batch_key = ORD022,           # 捉取註記 (Capture Note - for JOIN with ORE013)
      import_order_flag = ORD023,   # 匯入訂單 (Import Order Flag)
      reserved_1 = ORD024,          # Reserved field
      reserved_2 = ORD025,          # Reserved field
      reserved_3 = ORD026,          # Reserved field
      reserved_4 = ORD027,          # Reserved field
      reserved_5 = ORD028           # Reserved field
    )
  
  # ------------------------------------------------------------------------------
  # 2.3: Data Type Conversions and Cleaning
  # ------------------------------------------------------------------------------
  message("MAIN: Converting data types and cleaning...")
  
  staged_orders <- staged_orders %>%
    mutate(
      # Date conversions
      order_date = as.POSIXct(order_date, tz = "UTC"),
      payment_date = as.POSIXct(payment_date, tz = "UTC"),
      
      # Numeric conversions
      payment_total = as.numeric(payment_total),
      payment_method = as.integer(payment_method),  # int code
      shipping_fee = as.numeric(shipping_fee),
      address_source = as.integer(address_source),  # int code
      
      # Handle encoding issues for batch_key (critical for JOIN)
      # This addresses the VARBINARY casting issue from SQL Server
      batch_key = iconv(batch_key, from = "latin1", to = "UTF-8", sub = ""),
      
      # Clean text fields
      recipient_name = str_trim(recipient_name),
      street_address_1 = str_trim(street_address_1),
      street_address_2 = str_trim(street_address_2),
      city_name = str_trim(city_name),
      state_or_province = str_trim(state_or_province),
      postal_code = str_trim(postal_code),
      country_name = str_trim(country_name),
      
      # Standardize status codes
      order_status = toupper(str_trim(order_status)),
      
      # Add staging metadata
      staged_timestamp = Sys.time(),
      staging_version = script_version
    )
  
  # ------------------------------------------------------------------------------
  # 2.4: Data Quality Checks
  # ------------------------------------------------------------------------------
  message("MAIN: Performing data quality checks...")
  
  # Check for duplicates
  n_duplicates <- staged_orders %>%
    group_by(order_id, batch_key) %>%
    filter(n() > 1) %>%
    nrow()
  
  if (n_duplicates > 0) {
    warning(sprintf("MAIN: Found %d duplicate orders", n_duplicates))
    # Remove duplicates, keeping the latest
    staged_orders <- staged_orders %>%
      group_by(order_id, batch_key) %>%
      arrange(desc(order_date)) %>%
      slice(1) %>%
      ungroup()
  }
  
  # Check for missing critical fields
  missing_ids <- sum(is.na(staged_orders$order_id))
  missing_dates <- sum(is.na(staged_orders$order_date))
  missing_batch <- sum(is.na(staged_orders$batch_key))
  
  message(sprintf("MAIN: Missing order_ids: %d", missing_ids))
  message(sprintf("MAIN: Missing order_dates: %d", missing_dates))
  message(sprintf("MAIN: Missing batch_keys: %d (critical for JOIN)", missing_batch))
  
  # ------------------------------------------------------------------------------
  # 2.5: Store Staged Data
  # ------------------------------------------------------------------------------
  message("MAIN: Storing staged BAYORD data...")
  
  # Store in staged_data database with MAMBA-specific naming
  table_name <- "df_eby_orders___staged___MAMBA"
  
  if (dbExistsTable(staged_data, table_name)) {
    dbRemoveTable(staged_data, table_name)
    message(sprintf("MAIN: Dropped existing table: %s", table_name))
  }
  
  dbWriteTable(staged_data, table_name, staged_orders)
  n_staged <- nrow(staged_orders)
  message(sprintf("MAIN: ✅ Stored %d staged orders in %s", n_staged, table_name))
  
  # Display sample for verification
  message("MAIN: Sample of staged BAYORD data:")
  sample_data <- head(staged_orders, 3)
  print(sample_data[, c("order_id", "order_date", "batch_key", "country_name", "gross_amount")])
  
  main_elapsed <- round(difftime(Sys.time(), main_start_time, units = "secs"), 2)
  message(sprintf("MAIN: ✅ Orders staging completed in %.2f seconds", main_elapsed))
  
}, error = function(e) {
  message(sprintf("MAIN: ❌ Error during orders staging: %s", e$message))
  stop(e)
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

message("TEST: Starting validation tests...")
test_start_time <- Sys.time()

tryCatch({
  # Test 1: Verify table exists
  if (!dbExistsTable(staged_data, "df_eby_orders___staged___MAMBA")) {
    stop("TEST: Table df_eby_orders___staged___MAMBA does not exist")
  }
  message("TEST: ✅ Table exists")
  
  # Test 2: Verify data staged
  row_count <- dbGetQuery(staged_data, "SELECT COUNT(*) as n FROM df_eby_orders___staged___MAMBA")$n
  if (row_count == 0) {
    stop("TEST: No data in df_eby_orders___staged___MAMBA")
  }
  message(sprintf("TEST: ✅ Data staged (%d rows)", row_count))
  
  # Test 3: Verify standardized columns exist
  columns <- dbListFields(staged_data, "df_eby_orders___staged___MAMBA")
  required_cols <- c("order_id", "order_date", "batch_key", "country_name", "gross_amount")
  missing_cols <- setdiff(required_cols, columns)
  
  if (length(missing_cols) > 0) {
    stop(sprintf("TEST: Missing required columns: %s", paste(missing_cols, collapse = ", ")))
  }
  message("TEST: ✅ All required standardized columns present")
  
  # Test 4: Verify no raw column names remain
  raw_cols <- grep("^ORD", columns, value = TRUE)
  if (length(raw_cols) > 0) {
    warning(sprintf("TEST: ⚠️ Raw columns still present: %s", paste(raw_cols, collapse = ", ")))
  } else {
    message("TEST: ✅ All columns standardized (no ORD* columns)")
  }
  
  # Test 5: Verify batch_key is ready for JOIN
  batch_key_nulls <- dbGetQuery(staged_data, 
    "SELECT COUNT(*) as n FROM df_eby_orders___staged___MAMBA WHERE batch_key IS NULL")$n
  message(sprintf("TEST: Batch key nulls: %d (needed for JOIN)", batch_key_nulls))
  
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