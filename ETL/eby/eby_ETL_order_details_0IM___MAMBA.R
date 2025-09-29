# ==============================================================================
# MAMBA-Specific eBay Order Details ETL - Import Phase (0IM)
# Following DM_R037: Company-Specific ETL Naming Rule
# Following MP104: ETL Data Flow Separation Principle
# Following MP064: ETL-Derivation Separation Principle
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby) - Custom SQL Server Implementation
# Data Type: Order Details (BAYORE table - order line items)
# Phase: 0IM (Import)
# 
# This script connects to MAMBA's own eBay SQL Server database
# SSH Tunnel: 220.128.138.146 -> SQL Server: 125.227.84.85:1433
# Imports ONLY BAYORE table (order details) - separated from BAYORD per MP104
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP031: Initialization First
# Following DM_R039: Database Connection Pattern Rule

message(strrep("=", 80))
message("INITIALIZE: Starting MAMBA eBay Order Details Import")
message("INITIALIZE: Script: eby_ETL_order_details_0IM___MAMBA.R")
message("INITIALIZE: Company-specific implementation for MAMBA")
message("INITIALIZE: Data type: Order Details (BAYORE table only)")
message(strrep("=", 80))

# ------------------------------------------------------------------------------
# 1.1: Basic Initialization
# ------------------------------------------------------------------------------

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_order_details_0IM___MAMBA"
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
message("INITIALIZE: Following MP099 Real-time Progress Reporting")

# Verify MAMBA-specific environment variables with detailed reporting
message("INITIALIZE: Checking environment variables...")
required_vars <- c(
  "EBY_SSH_HOST", "EBY_SSH_USER", "EBY_SSH_PASSWORD",
  "EBY_SQL_HOST", "EBY_SQL_PORT", "EBY_SQL_USER", 
  "EBY_SQL_PASSWORD", "EBY_SQL_DATABASE"
)

# MP099: Real-time progress for each variable check
vars_status <- list()
for (var in required_vars) {
  value <- Sys.getenv(var)
  if (nzchar(value)) {
    # Show variable is set but don't reveal the actual value for security
    if (grepl("PASSWORD", var)) {
      message(sprintf("INITIALIZE:   ✓ %s = [REDACTED]", var))
    } else if (var == "EBY_SSH_USER" || var == "EBY_SQL_USER") {
      message(sprintf("INITIALIZE:   ✓ %s = %s", var, value))
    } else {
      # For hosts and ports, show the value for debugging
      message(sprintf("INITIALIZE:   ✓ %s = %s", var, value))
    }
    vars_status[[var]] <- TRUE
  } else {
    message(sprintf("INITIALIZE:   ✗ %s = [NOT SET]", var))
    vars_status[[var]] <- FALSE
  }
}

missing_vars <- names(vars_status)[!unlist(vars_status)]
if (length(missing_vars) > 0) {
  stop("Missing required environment variables: ", paste(missing_vars, collapse = ", "), 
       "\nPlease check your .env file in the project root.")
}

message("INITIALIZE: ✅ All environment variables loaded successfully")
message("INITIALIZE: 0IM Phase - Raw BAYORE import only (MP064 compliance)")

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

message("MAIN: Starting MAMBA eBay ORDER DETAILS import process")
main_start_time <- Sys.time()

tryCatch({
  # ------------------------------------------------------------------------------
  # 2.1: Setup SSH Tunnel and Connect to SQL Server
  # ------------------------------------------------------------------------------
  message("MAIN: Connecting to MAMBA SQL Server...")
  message(sprintf("MAIN: SSH Target: %s@%s", 
                  Sys.getenv("EBY_SSH_USER"), 
                  Sys.getenv("EBY_SSH_HOST")))
  message(sprintf("MAIN: SQL Target: %s:%s/%s", 
                  Sys.getenv("EBY_SQL_HOST"), 
                  Sys.getenv("EBY_SQL_PORT"),
                  Sys.getenv("EBY_SQL_DATABASE")))
  
  # Source the auto-tunnel function
  source("scripts/global_scripts/02_db_utils/fn_ensure_mamba_tunnel.R")
  
  # Connect with automatic tunnel establishment (MP099: with progress)
  message("MAIN: Establishing SSH tunnel...")
  sql_conn <- fn_connect_mamba_sql(auto_tunnel = TRUE)
  
  message("MAIN: ✅ Connected to SQL Server successfully")
  
  # ------------------------------------------------------------------------------
  # 2.3: Import BAYORE Table Only (Following MP104 - Data Type Separation)
  # ------------------------------------------------------------------------------
  message("MAIN: Querying BAYORE (order details) table...")
  message("MAIN: No JOIN operations in 0IM phase (MP064 compliance)")
  
  # Query ONLY BAYORE table - no JOIN with BAYORD
  # Note: We'll handle the JOIN key relationship in derivation layer
  # MP100: UTF-8 encoding issues - select only essential columns first
  query <- "
    SELECT 
      ORE001, ORE002, ORE003, ORE004, ORE005,
      ORE006, ORE007, ORE008, ORE009, ORE010,
      ORE011, ORE012, ORE013, ORE014
    FROM BAYORE
    WHERE ORE004 >= '2024-01-01'
    ORDER BY ORE004, ORE001, ORE002
  "
  
  message("MAIN: Executing BAYORE query...")
  
  # Execute query with encoding handling
  tryCatch({
    bayore_data <- dbGetQuery(sql_conn, query)
  }, error = function(e) {
    message("MAIN: UTF-8 encoding issue detected, trying with basic columns only...")
    # Fallback to minimal columns if encoding issues
    query_basic <- "
      SELECT 
        ORE001, ORE002, ORE003, ORE004,
        ORE006, ORE013
      FROM BAYORE
      WHERE ORE004 >= '2024-01-01'
      ORDER BY ORE004, ORE001, ORE002
    "
    bayore_data <- dbGetQuery(sql_conn, query_basic)
  })
  
  # Clean any potential encoding issues
  for (col in names(bayore_data)) {
    if (is.character(bayore_data[[col]])) {
      # Convert to UTF-8 and remove invalid characters
      bayore_data[[col]] <- iconv(bayore_data[[col]], from = "LATIN1", to = "UTF-8", sub = "")
    }
  }
  
  n_details <- nrow(bayore_data)
  message(sprintf("MAIN: Retrieved %d order details from BAYORE", n_details))
  
  # Disconnect from SQL Server (but keep tunnel for other scripts)
  dbDisconnect(sql_conn)
  message("MAIN: Disconnected from SQL Server")
  message("MAIN: SSH tunnel kept alive for subsequent ETL scripts")
  
  # ------------------------------------------------------------------------------
  # 2.4: Store Raw Data (Following MP064 - Preserve Raw Structure)
  # ------------------------------------------------------------------------------
  message("MAIN: Storing raw BAYORE data...")
  
  # Store in raw_data database with MAMBA-specific naming
  # Following DM_R037: Company-specific suffix
  table_name <- "df_eby_order_details___raw___MAMBA"
  
  if (dbExistsTable(raw_data, table_name)) {
    dbRemoveTable(raw_data, table_name)
    message(sprintf("MAIN: Dropped existing table: %s", table_name))
  }
  
  dbWriteTable(raw_data, table_name, bayore_data)
  message(sprintf("MAIN: ✅ Stored %d order details in %s", n_details, table_name))
  
  # Display sample for verification
  message("MAIN: Sample of imported BAYORE data:")
  sample_data <- head(bayore_data, 3)
  # Show available columns
  available_cols <- intersect(c("ORE001", "ORE002", "ORE006", "ORE013"), names(sample_data))
  if (length(available_cols) > 0) {
    print(sample_data[, available_cols])
  } else {
    print(head(sample_data, 3))
  }  
  
  main_elapsed <- round(difftime(Sys.time(), main_start_time, units = "secs"), 2)
  message(sprintf("MAIN: ✅ Order details import completed in %.2f seconds", main_elapsed))
  
}, error = function(e) {
  message(sprintf("MAIN: ❌ Error during order details import: %s", e$message))
  
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
  if (!dbExistsTable(raw_data, "df_eby_order_details___raw___MAMBA")) {
    stop("TEST: Table df_eby_order_details___raw___MAMBA does not exist")
  }
  message("TEST: ✅ Table exists")
  
  # Test 2: Verify data imported
  row_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as n FROM df_eby_order_details___raw___MAMBA")$n
  if (row_count == 0) {
    stop("TEST: No data in df_eby_order_details___raw___MAMBA")
  }
  message(sprintf("TEST: ✅ Data imported (%d rows)", row_count))
  
  # Test 3: Verify key columns exist
  columns <- dbListFields(raw_data, "df_eby_order_details___raw___MAMBA")
  # Minimum required columns for order details
  required_cols <- c("ORE001", "ORE002", "ORE004")  # Order ID, Line Item, Date
  missing_cols <- setdiff(required_cols, columns)
  
  if (length(missing_cols) > 0) {
    stop(sprintf("TEST: Missing required columns: %s", paste(missing_cols, collapse = ", ")))
  }
  message("TEST: ✅ All required columns present")
  
  # Test 4: Verify no BAYORD data mixed in (MP104 compliance)
  if (any(grepl("^ORD", columns))) {
    stop("TEST: ❌ BAYORD columns found - violates MP104 data separation")
  }
  message("TEST: ✅ No BAYORD data mixed - MP104 compliant")
  
  # Test 5: Verify JOIN keys are present for future derivation
  join_keys_present <- all(c("ORE001", "ORE013") %in% columns)
  if (!join_keys_present) {
    warning("TEST: ⚠️ JOIN keys (ORE001, ORE013) may be missing for future derivation")
  } else {
    message("TEST: ✅ JOIN keys present for future derivation")
  }
  
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
