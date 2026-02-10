# ==============================================================================
# MAMBA-Specific eBay Orders ETL - Import Phase (0IM)
# Following DM_R037: Company-Specific ETL Naming Rule
# Following MP104: ETL Data Flow Separation Principle
# Following MP064: ETL-Derivation Separation Principle
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby) - Custom SQL Server Implementation
# Data Type: Orders (BAYORD table - order headers)
# Phase: 0IM (Import)
# 
# This script connects to MAMBA's own eBay SQL Server database
# SSH Tunnel: 220.128.138.146 -> SQL Server: 125.227.84.85:1433
# Imports ONLY BAYORD table (order headers) - separated from BAYORE per MP104
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP031: Initialization First
# Following DM_R039: Database Connection Pattern Rule

message(strrep("=", 80))
message("INITIALIZE: Starting MAMBA eBay Orders Import (eby_ETL_orders_0IM___MAMBA.R)")
message("INITIALIZE: Company-specific implementation for MAMBA")
message("INITIALIZE: Data type: Orders (BAYORD table only)")
message(strrep("=", 80))

# ------------------------------------------------------------------------------
# 1.1: Basic Initialization
# ------------------------------------------------------------------------------

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_orders_0IM___MAMBA"
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
library(odbc)

# Source required functions (Following DM_R039)
source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R")

# Following MP106: Console Transparency
message("INITIALIZE: [OK] Global initialization complete")
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message("INITIALIZE: Following MP064 ETL-Derivation Separation")
message("INITIALIZE: Following MP104 ETL Data Flow Separation")

# Verify MAMBA-specific environment variables
required_vars <- c(
  "EBY_SSH_HOST", "EBY_SSH_USER", "EBY_SSH_PASSWORD",
  "EBY_SQL_HOST", "EBY_SQL_PORT", "EBY_SQL_USER", 
  "EBY_SQL_PASSWORD", "EBY_SQL_DATABASE"
)

missing_vars <- setdiff(required_vars, names(Sys.getenv())[Sys.getenv() != ""])
if (length(missing_vars) > 0) {
  stop("Missing required environment variables: ", paste(missing_vars, collapse = ", "))
}

message("INITIALIZE: [OK] Environment variables loaded")
message("INITIALIZE: 0IM Phase - Raw BAYORD import only (MP064 compliance)")

# ------------------------------------------------------------------------------
# 1.2: Database Connections (Following DM_R039)
# ------------------------------------------------------------------------------
message("INITIALIZE: Establishing database connections...")

# Connect to raw_data database
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: ✅ Database connections established")
message(sprintf("INITIALIZE: Connected to: %s", db_path_list$raw_data))

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

message("MAIN: Starting MAMBA eBay ORDERS import process")
main_start_time <- Sys.time()

tryCatch({
  # ------------------------------------------------------------------------------
  # 2.1: Setup SSH Tunnel and Connect to SQL Server
  # ------------------------------------------------------------------------------
  message("MAIN: Connecting to MAMBA SQL Server...")
  
  # Source the auto-tunnel function
  source("scripts/global_scripts/02_db_utils/fn_ensure_mamba_tunnel.R")
  
  # Connect with automatic tunnel establishment
  sql_conn <- fn_connect_mamba_sql(auto_tunnel = TRUE)
  
  message("MAIN: Connected to SQL Server successfully")
  
  # ------------------------------------------------------------------------------
  # 2.3: Import BAYORD Table Only (Following MP104 - Data Type Separation)
  # ------------------------------------------------------------------------------
  message("MAIN: Querying BAYORD (orders) table...")
  message("MAIN: No JOIN operations in 0IM phase (MP064 compliance)")
  
  # Query ONLY BAYORD table - limited columns to avoid encoding issues
  # Following MP100: UTF-8 Encoding Standard
  query <- "
    SELECT 
      ORD001, ORD002, ORD003, ORD004, ORD005,
      ORD006, ORD007, ORD008, ORD009, ORD010,
      ORD011, ORD012, ORD013, ORD014, ORD015,
      ORD016, ORD020, ORD021, ORD046, ORD047, 
      ORD048
    FROM BAYORD
    WHERE ORD003 >= '2024-01-01'
    ORDER BY ORD003, ORD001
  "
  
  message("MAIN: Executing BAYORD query (limited columns for UTF-8 safety)...")
  
  # Try with encoding handling
  tryCatch({
    bayord_data <- dbGetQuery(sql_conn, query)
    
    # Convert character columns to UTF-8
    char_cols <- sapply(bayord_data, is.character)
    for(col in names(bayord_data)[char_cols]) {
      bayord_data[[col]] <- iconv(bayord_data[[col]], from = "LATIN1", to = "UTF-8", sub = "")
    }
    
    message("MAIN: Successfully retrieved data with UTF-8 conversion")
  }, error = function(e) {
    message("MAIN: UTF-8 error encountered, trying minimal column set...")
    
    # Fallback to minimal columns
    query_minimal <- "
      SELECT 
        ORD001, ORD003, ORD005, ORD009, ORD016
      FROM BAYORD
      WHERE ORD003 >= '2024-01-01'
      ORDER BY ORD003, ORD001
    "
    bayord_data <- dbGetQuery(sql_conn, query_minimal)
    message("MAIN: Using minimal column set due to encoding issues")
  })
  
  n_orders <- nrow(bayord_data)
  message(sprintf("MAIN: Retrieved %d orders from BAYORD", n_orders))
  
  # Disconnect from SQL Server (but keep tunnel for other scripts)
  dbDisconnect(sql_conn)
  message("MAIN: Disconnected from SQL Server")
  message("MAIN: SSH tunnel kept alive for subsequent ETL scripts")
  
  # ------------------------------------------------------------------------------
  # 2.4: Store Raw Data (Following MP064 - Preserve Raw Structure)
  # ------------------------------------------------------------------------------
  message("MAIN: Storing raw BAYORD data...")
  
  # Store in raw_data database with MAMBA-specific naming
  # Following DM_R037: Company-specific suffix
  table_name <- "df_eby_orders___raw___MAMBA"
  
  if (dbExistsTable(raw_data, table_name)) {
    dbRemoveTable(raw_data, table_name)
    message(sprintf("MAIN: Dropped existing table: %s", table_name))
  }
  
  dbWriteTable(raw_data, table_name, bayord_data)
  message(sprintf("MAIN: ✅ Stored %d orders in %s", n_orders, table_name))
  
  # Display sample for verification
  message("MAIN: Sample of imported BAYORD data:")
  sample_data <- head(bayord_data, 3)
  print(sample_data[, c("ORD001", "ORD003", "ORD010", "ORD016")])  # ID, Date, Recipient, Country
  
  main_elapsed <- round(difftime(Sys.time(), main_start_time, units = "secs"), 2)
  message(sprintf("MAIN: ✅ Orders import completed in %.2f seconds", main_elapsed))
  
}, error = function(e) {
  message(sprintf("MAIN: ❌ Error during orders import: %s", e$message))
  
  # Ensure cleanup happens even on error
  if (exists("sql_conn") && !is.null(sql_conn)) {
    try(dbDisconnect(sql_conn), silent = TRUE)
  }
  # Keep tunnel alive for other scripts
  
  stop(e)
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

message("TEST: Starting validation tests...")
test_start_time <- Sys.time()

tryCatch({
  # Test 1: Verify table exists
  if (!dbExistsTable(raw_data, "df_eby_orders___raw___MAMBA")) {
    stop("TEST: Table df_eby_orders___raw___MAMBA does not exist")
  }
  message("TEST: ✅ Table exists")
  
  # Test 2: Verify data imported
  row_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as n FROM df_eby_orders___raw___MAMBA")$n
  if (row_count == 0) {
    stop("TEST: No data in df_eby_orders___raw___MAMBA")
  }
  message(sprintf("TEST: ✅ Data imported (%d rows)", row_count))
  
  # Test 3: Verify key columns exist
  columns <- dbListFields(raw_data, "df_eby_orders___raw___MAMBA")
  required_cols <- c("ORD001", "ORD003", "ORD009", "ORD010", "ORD016")
  missing_cols <- setdiff(required_cols, columns)
  
  if (length(missing_cols) > 0) {
    stop(sprintf("TEST: Missing required columns: %s", paste(missing_cols, collapse = ", ")))
  }
  message("TEST: ✅ All required columns present")
  
  # Test 4: Verify no BAYORE data mixed in (MP104 compliance)
  if (any(grepl("^ORE", columns))) {
    stop("TEST: ❌ BAYORE columns found - violates MP104 data separation")
  }
  message("TEST: ✅ No BAYORE data mixed - MP104 compliant")
  
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

# Final timing
total_elapsed <- round(difftime(Sys.time(), script_start_time, units = "secs"), 2)
message(sprintf("DEINITIALIZE: Total execution time: %.2f seconds", total_elapsed))

# ==============================================================================
# PART 5: AUTODEINIT
# ==============================================================================
# Following MP103: autodeinit() must be the absolute last statement

message("AUTODEINIT: Executing final cleanup...")
autodeinit()