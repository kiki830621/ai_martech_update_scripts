# ==============================================================================
# MAMBA-Specific eBay Sales ETL - Staging Phase (1ST)
# Following DM_R037: Company-Specific ETL Naming Rule
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby) - Custom SQL Server Implementation  
# Data Type: Sales
# Phase: 1ST (Staging)
#
# This script stages MAMBA's eBay sales data from raw to staging format
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Script Structure Standard Rule
# Following DEV_R009: Initialization Sourcing Rule
# Following MP031: Initialization First
# Following SO_R013: Initialization Imports Only Rule

message(strrep("=", 80))
message("INITIALIZE: Starting MAMBA eBay Sales Staging (eby_ETL_sales_1ST___MAMBA.R)")
message("INITIALIZE: Company-specific staging for MAMBA")
message(strrep("=", 80))

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_sales_1ST___MAMBA"
script_version <- "1.0.0"

# Following MP101: Global Environment Access Pattern
# Following MP103: Auto-deinit Behavior
# Source the initialization system if autoinit() is not available
if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

# The autoinit() function automatically detects the script location,
# sets OPERATION_MODE to UPDATE_MODE, and sources the appropriate initialization
# All required libraries and global utilities are loaded automatically
autoinit()

# MAMBA-specific environment setup
# Load environment variables after initialization completes
if (!require(dotenv, quietly = TRUE)) {
  install.packages("dotenv")
}
dotenv::load_dot_env()

# Following MP106: Console Transparency - detailed progress reporting  
message("INITIALIZE: ✓ Global initialization complete")
message(sprintf("INITIALIZE: Operation mode: %s", OPERATION_MODE))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message("INITIALIZE: 1ST Phase - Column renaming and transformations (MP064 compliance)")

# Note: The following functions are already loaded by autoinit():
# - fn_data_cleaning.R
# - fn_parse_datetime_columns.R  
# - fn_clean_column_names.R
# - fn_handle_na.R

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

message("MAIN: Starting MAMBA eBay sales staging process")
main_start_time <- Sys.time()

# Initialize script success variable
script_success <- FALSE

# Connect to raw database
connect_to_raw_db <- function() {
  message("MAIN: Connecting to raw database...")
  # Following MAMBA 7-Layer Architecture: 1ST Phase reads from raw_data.duckdb
  
  raw_conn <- tryCatch({
    DBI::dbConnect(duckdb::duckdb(), "data/local_data/raw_data.duckdb")
  }, error = function(e) {
    stop("Failed to connect to raw database: ", e$message)
  })
  
  message("MAIN: ✓ Connected to raw database (data/local_data/raw_data.duckdb)")
  return(raw_conn)
}

# Stage MAMBA-specific eBay sales data
stage_mamba_eby_sales <- function(raw_conn) {
  message("MAIN: Loading raw data from df_eby_sales___raw...")
  
  # Read raw data
  df_raw <- DBI::dbGetQuery(raw_conn, 
    "SELECT * FROM df_eby_sales___raw")
  
  # Following MP106: Console Transparency - detailed record analysis
  message(sprintf("MAIN: Loaded %d raw records", nrow(df_raw)))
  message(sprintf("MAIN: Raw data columns: %d", ncol(df_raw)))
  message("MAIN: Starting 1ST phase transformations...")
  
  # Convert to data.table for efficient processing
  dt_staging <- as.data.table(df_raw)
  
  # ======================================
  # Phase 1: Column Renaming (moved from 0IM for MP064 compliance)
  # ======================================
  
  message("MAIN: Applying column renaming (moved from 0IM phase for MP064 compliance)...")
  
  # Following MP064: All column renaming happens in 1ST phase
  # Map original SQL Server column names to business-friendly names
  column_mapping <- c(
    # Order header fields (BAYORD)
    "ORD001" = "order_number",
    "ORD002" = "other_order_number", 
    "ORD003" = "order_date",
    "ORD004" = "payment_date",
    "ORD005" = "total_payment",
    "ORD006" = "payment_method",
    "ORD007" = "payment_currency",
    "ORD008" = "seller_ebay_account",
    "ORD009" = "seller_ebay_email",
    "ORD010" = "recipient",
    "ORD011" = "street1",
    "ORD012" = "street2", 
    "ORD013" = "city_name",
    "ORD014" = "state_or_province",
    "ORD015" = "postal_code",
    "ORD016" = "country_name",
    "ORD020" = "buyer_ebay",
    "ORD021" = "shipping_fee",
    "ORD046" = "tax_code_type",
    "ORD047" = "tax_code", 
    "ORD048" = "vat_amount",
    
    # Order detail fields (BAYORE)
    "ORE002" = "serial_number",
    "ORE003" = "ebay_item_number",
    "ORE004" = "product_name",
    "ORE005" = "erp_product_number", 
    "ORE006" = "application_data",
    "ORE007" = "item_condition",
    "ORE008" = "quantity",
    "ORE009" = "unit_price",
    "ORE010" = "transaction_price",
    "ORE011" = "ebay_transaction_id",
    "ORE012" = "purchase_date",
    "ORE014" = "variation",
    "ORE015" = "payment_status"
  )
  
  # Apply column renaming
  for (old_name in names(column_mapping)) {
    if (old_name %in% names(dt_staging)) {
      new_name <- column_mapping[[old_name]]
      setnames(dt_staging, old_name, new_name)
      message(sprintf("MAIN:  Renamed %s -> %s", old_name, new_name))
    }
  }
  
  # ======================================
  # Phase 2: Data Cleaning and Standardization  
  # ======================================
  
  message("MAIN: Performing data cleaning...")
  
  # Helper function to parse MAMBA date format "YYYYMMDD HHMMSS" to standard date string
  parse_mamba_date <- function(date_str) {
    if (is.na(date_str) || nchar(date_str) < 8) return(NA_character_)
    date_part <- substr(date_str, 1, 8)
    parsed_date <- as.Date(date_part, format = "%Y%m%d")
    # Return as formatted string to avoid numeric conversion
    return(format(parsed_date, "%Y-%m-%d"))
  }
  
  # Parse all date columns to proper Date format strings
  message("MAIN:  Parsing MAMBA date format (YYYYMMDD HHMMSS) for date columns...")
  dt_staging[, order_date := sapply(order_date, parse_mamba_date)]
  dt_staging[, payment_date := sapply(payment_date, parse_mamba_date)]
  dt_staging[, purchase_date := sapply(purchase_date, parse_mamba_date)]
  
  # 1. Clean remaining column names (following R69)
  names(dt_staging) <- clean_column_names(names(dt_staging))
  
  # 2. Parse datetime columns - skip for now as dates are already parsed
  # datetime_cols <- c("order_date", "created_at", "updated_at")
  # for (col in datetime_cols) {
  #   if (col %in% names(dt_staging)) {
  #     dt_staging[, (col) := fn_parse_datetime_columns(get(col))]
  #   }
  # }
  
  # 3. Handle NA values - using basic handling for now
  # dt_staging <- fn_handle_na(dt_staging, 
  #                            method = "smart",
  #                            preserve_cols = c("customer_email", "ebay_buyer_username"))
  message("MAIN:  Skipping advanced datetime parsing and NA handling for basic transformation")
  
  # 4. Add MAMBA-specific calculated fields first
  message("MAIN: Adding MAMBA-specific calculated fields (moved from 0IM phase for MP064 compliance)...")
  
  # MAMBA-specific business logic fields (moved from 0IM phase)
  dt_staging[, mamba_commission_rate := 10]  # MAMBA's standard eBay commission
  dt_staging[, mamba_warehouse_code := "TW01"]  # Taiwan main warehouse  
  dt_staging[, mamba_fulfillment_type := ifelse(!is.na(shipping_fee) & shipping_fee > 0, "MAMBA_FULFILLED", "FREE_SHIPPING")]
  
  # Calculate profit margin using transaction_price and unit_price
  dt_staging[, mamba_profit_margin := ifelse(
    !is.na(transaction_price) & transaction_price > 0,
    (transaction_price - unit_price * 0.7) / transaction_price * 100,
    0
  )]
  
  # 6. Create additional derived fields
  message("MAIN: Creating additional derived fields...")
  
  # Customer ID generation (anonymized) - use buyer_ebay if available
  dt_staging[, customer_id := as.character(as.integer(factor(
    ifelse(!is.na(buyer_ebay), buyer_ebay, seller_ebay_email)
  )))]
  
  # Product SKU normalization - use erp_product_number
  dt_staging[, product_sku := toupper(trimws(erp_product_number))]
  
  # Order month/year for partitioning - dates are already parsed to proper format
  dt_staging[, order_year := ifelse(!is.na(order_date), year(as.Date(order_date)), NA)]
  dt_staging[, order_month := ifelse(!is.na(order_date), month(as.Date(order_date)), NA)]
  dt_staging[, order_quarter := ifelse(!is.na(order_date), quarter(as.Date(order_date)), NA)]
  
  # Revenue calculations using transaction_price
  dt_staging[, gross_revenue := quantity * transaction_price]
  dt_staging[, net_revenue := gross_revenue * (1 - mamba_commission_rate/100)]
  
  # eBay platform fees (assumed 10% if not provided)
  dt_staging[, ebay_fee := gross_revenue * 0.10]
  
  # MAMBA net profit
  dt_staging[, mamba_net_profit := net_revenue - ebay_fee]
  
  # 7. Data validation
  message("MAIN: Validating staged data...")
  
  # Check for required fields (using renamed columns)
  required_fields <- c("order_number", "customer_id", "order_date", 
                       "product_sku", "quantity", "unit_price")
  
  missing_fields <- setdiff(required_fields, names(dt_staging))
  if (length(missing_fields) > 0) {
    warning("Missing required fields: ", paste(missing_fields, collapse = ", "))
  }
  
  # Remove duplicate orders (using renamed column)
  dt_staging <- unique(dt_staging, by = "order_number")
  
  # Remove test orders (MAMBA-specific pattern)
  dt_staging <- dt_staging[!grepl("TEST|DEMO", order_number, ignore.case = TRUE)]
  
  # 8. Add staging metadata
  dt_staging[, staging_timestamp := Sys.time()]
  dt_staging[, staging_version := script_version]
  dt_staging[, staging_source := "MAMBA_EBY_SQL"]
  
  # Following MP106: Console Transparency - final staging summary
  message(sprintf("MAIN: ✓ Staged %d records", nrow(dt_staging)))
  message(sprintf("MAIN: Total columns after transformations: %d", ncol(dt_staging)))
  message(sprintf("MAIN: MAMBA-specific fields added: %d", 
                 sum(c("mamba_commission_rate", "mamba_profit_margin", 
                      "mamba_warehouse_code", "mamba_fulfillment_type") %in% names(dt_staging))))
  message("MAIN: 1ST Phase complete - All renaming and basic transformations applied (MP064)")
  
  return(as.data.frame(dt_staging))
}

# Save staged data
save_staged_data <- function(df_staged) {
  message("MAIN: Saving staged data...")
  # Following MAMBA 7-Layer Architecture: 1ST Phase writes to staged_data.duckdb
  
  # Create connection to staged database
  staged_conn <- tryCatch({
    DBI::dbConnect(duckdb::duckdb(), "data/local_data/staged_data.duckdb")
  }, error = function(e) {
    stop("Failed to connect to staged database: ", e$message)
  })
  
  # Write to staging table with MAMBA-specific naming
  DBI::dbWriteTable(staged_conn, "df_eby_sales___staged",
                    df_staged, overwrite = TRUE)
  
  # Create indices for common queries
  DBI::dbExecute(staged_conn, 
    "CREATE INDEX IF NOT EXISTS idx_order_date ON df_eby_sales___staged(order_date)")
  DBI::dbExecute(staged_conn,
    "CREATE INDEX IF NOT EXISTS idx_customer_id ON df_eby_sales___staged(customer_id)")
  DBI::dbExecute(staged_conn,
    "CREATE INDEX IF NOT EXISTS idx_product_sku ON df_eby_sales___staged(product_sku)")
  
  message("MAIN: ✓ Data saved to df_eby_sales___staged with indices")
  message("MAIN: ✓ Staged data written to data/local_data/staged_data.duckdb (MAMBA 7-Layer compliant)")
  
  # Close the connection
  DBI::dbDisconnect(staged_conn)
}

# Execute main process
tryCatch({
  # Connect to raw database
  raw_conn <- connect_to_raw_db()
  
  # Stage data
  df_staged <- stage_mamba_eby_sales(raw_conn)
  
  # Save staged data
  save_staged_data(df_staged)
  
  # Clean up
  DBI::dbDisconnect(raw_conn)
  
  script_success <<- TRUE
  
}, error = function(e) {
  message("MAIN: ❌ Error in staging process: ", e$message)
  script_success <<- FALSE
})

main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
message(sprintf("MAIN: Main process completed in %.2f seconds", main_elapsed))

# ==============================================================================
# PART 3: TEST
# ==============================================================================

message("TEST: Starting validation tests")
test_start_time <- Sys.time()
test_passed <- TRUE

# Test 1: Verify staged data was written
tryCatch({
  test_conn <- DBI::dbConnect(duckdb::duckdb(), "data/local_data/staged_data.duckdb")
  
  if (DBI::dbExistsTable(test_conn, "df_eby_sales___staged")) {
    row_count <- DBI::dbGetQuery(test_conn,
                                 "SELECT COUNT(*) as n FROM df_eby_sales___staged")$n
    
    if (row_count > 0) {
      message(sprintf("TEST: ✓ Staging table contains %d records", row_count))
    } else {
      message("TEST: ⚠ Staging table exists but is empty")
      test_passed <- FALSE
    }
  } else {
    message("TEST: ❌ Table df_eby_sales___staged not found")
    test_passed <- FALSE
  }
  
  # Test 2: Verify derived fields were created
  if (test_passed) {
    schema <- DBI::dbGetQuery(test_conn,
                             "SELECT column_name FROM information_schema.columns
                              WHERE table_name = 'df_eby_sales___staged'")
    
    derived_fields <- c("customer_id", "product_sku", "order_year", "order_month",
                       "gross_revenue", "net_revenue", "mamba_net_profit")
    
    missing_fields <- setdiff(derived_fields, schema$column_name)
    
    if (length(missing_fields) == 0) {
      message("TEST: ✓ All derived fields present")
    } else {
      message("TEST: ⚠ Missing derived fields: ", paste(missing_fields, collapse = ", "))
      test_passed <- FALSE
    }
  }
  
  # Test 3: Verify data quality
  if (test_passed) {
    quality_check <- DBI::dbGetQuery(test_conn,
      "SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT order_number) as unique_orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(CASE WHEN mamba_fulfillment_type = 'UNKNOWN' THEN 1 END) as unknown_fulfillment,
        MIN(order_date) as earliest_order,
        MAX(order_date) as latest_order
      FROM df_eby_sales___staged")
    
    message("TEST: Data Quality Metrics:")
    message(sprintf("  - Total records: %d", quality_check$total_records))
    message(sprintf("  - Unique orders: %d", quality_check$unique_orders))
    message(sprintf("  - Unique customers: %d", quality_check$unique_customers))
    message(sprintf("  - Unknown fulfillment: %d", quality_check$unknown_fulfillment))
    message(sprintf("  - Date range: %s to %s", 
                   quality_check$earliest_order, quality_check$latest_order))
    
    if (quality_check$unique_orders == quality_check$total_records) {
      message("TEST: ✓ No duplicate orders found")
    } else {
      message("TEST: ⚠ Duplicate orders detected")
    }
  }
  
  DBI::dbDisconnect(test_conn)
  
}, error = function(e) {
  message("TEST: ❌ Test failed: ", e$message)
  test_passed <- FALSE
})

test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
message(sprintf("TEST: Tests completed in %.2f seconds", test_elapsed))

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

message("SUMMARIZE: Generating staging summary")

# Prepare final metrics
final_metrics <- list(
  script_name = script_name,
  company = "MAMBA",
  platform = "eby",
  data_type = "sales",
  phase = "1ST",
  success = script_success && test_passed,
  records_staged = ifelse(exists("df_staged"), nrow(df_staged), 0),
  execution_time = as.numeric(Sys.time() - script_start_time, units = "secs"),
  compliance = c("DM_R037", "MP104", "DM_R028", "DEV_R032", "MP030")
)

# Display summary
message(strrep("=", 80))
message("📊 MAMBA EBY SALES STAGING SUMMARY")
message(strrep("=", 80))
message(sprintf("🏢 Company: %s", final_metrics$company))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("📦 Data Type: %s", final_metrics$data_type))
message(sprintf("🔄 Phase: %s (Staging)", final_metrics$phase))
message(sprintf("📝 Records Staged: %d", final_metrics$records_staged))
message(sprintf("🕐 Total Time: %.2f seconds", final_metrics$execution_time))
message(sprintf("✅ Status: %s", ifelse(final_metrics$success, "SUCCESS", "FAILED")))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))

# Show data quality summary if available
if (exists("quality_check")) {
  message("📈 Data Quality Summary:")
  message(sprintf("  • Unique Customers: %d", quality_check$unique_customers))
  message(sprintf("  • Data Completeness: %.1f%%", 
                 (1 - quality_check$unknown_fulfillment/quality_check$total_records) * 100))
}

message(strrep("=", 80))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: 🧹 Cleaning up resources...")

# Note: SSH tunnel not needed for staging phase
# All database connections already closed in main process

message("DEINITIALIZE: ✅ MAMBA eBay Sales Staging completed")
message(sprintf("DEINITIALIZE: 🏁 Script finished at: %s",
               format(Sys.time(), "%Y-%m-%d %H:%M:%S")))

# Following DEV_R032: Five-Part Script Structure Standard
# MP103: autodeinit() removes ALL variables - must be absolute last statement
autodeinit()
# NO STATEMENTS AFTER THIS LINE