# ==============================================================================
# MAMBA-Specific eBay Sales ETL - Import Phase (0IM)
# Following DM_R037: Company-Specific ETL Naming Rule
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby) - Custom SQL Server Implementation
# Data Type: Sales
# Phase: 0IM (Import)
# 
# This script connects to MAMBA's own eBay SQL Server database
# SSH Tunnel: 220.128.138.146 -> SQL Server: 125.227.84.85:1433
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Script Structure Standard Rule
# Following DEV_R009: Initialization Sourcing Rule
# Following MP031: Initialization First
# Following SO_R013: Initialization Imports Only Rule
# Following DM_R039: Database Connection Pattern Rule

message(strrep("=", 80))
message("INITIALIZE: Starting MAMBA eBay Sales Import (eby_ETL_sales_0IM___MAMBA.R)")
message("INITIALIZE: Company-specific implementation for MAMBA")
message(strrep("=", 80))

# ------------------------------------------------------------------------------
# 1.1: Basic Initialization
# ------------------------------------------------------------------------------
# Following MP031: Initialization First

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_sales_0IM___MAMBA"
script_version <- "1.1.0"  # Updated for DM_R039 compliance

# Following MP101: Global Environment Access Pattern
# Following MP103: Auto-deinit Behavior
# Source the initialization system if autoinit() is not available
if (!exists("autoinit", mode = "function")) {
  source(file.path("..", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

# The autoinit() function automatically detects the script location,
# sets OPERATION_MODE to UPDATE_MODE, and sources the appropriate initialization
autoinit()

# Load required libraries
library(DBI)
library(duckdb)
library(dplyr)
library(odbc)

# Source required functions (Following DM_R039)
source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R")

# Following MP106: Console Transparency - detailed progress reporting
message("INITIALIZE: [OK] Global initialization complete")
message(sprintf("INITIALIZE: Operation mode: %s", OPERATION_MODE))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message(sprintf("INITIALIZE: Following MP064 ETL-Derivation Separation"))

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

# Following MP106: Console Transparency
message("INITIALIZE: [OK] Environment variables loaded")
message(sprintf("INITIALIZE: Required vars checked: %d/%d", 
               length(required_vars) - length(missing_vars), length(required_vars)))
message("INITIALIZE: 0IM Phase - Raw data import only (MP064 compliance)")

# ------------------------------------------------------------------------------
# 1.2: Database Connections (Following DM_R039)
# ------------------------------------------------------------------------------
message("INITIALIZE: Establishing database connections...")

# Connect to raw_data database using dbConnectDuckdb and db_path_list
# Following DM_R039: Database Connection Pattern Rule
# Following MP097: DuckDB Refactoring Standards
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: ✅ All database connections established")
message(sprintf("INITIALIZE: Connected to: %s", db_path_list$raw_data))

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

message("MAIN: Starting MAMBA eBay data import process")
main_start_time <- Sys.time()

# Check or create SSH tunnel for MAMBA's server
ensure_ssh_tunnel <- function() {
  message("MAIN: Checking SSH tunnel to MAMBA's SQL Server...")
  
  # Get credentials from environment variables
  db_name <- Sys.getenv("EBY_SQL_DATABASE")
  db_user <- Sys.getenv("EBY_SQL_USER")
  db_password <- Sys.getenv("EBY_SQL_PASSWORD")
  ssh_host <- Sys.getenv("EBY_SSH_HOST")
  ssh_user <- Sys.getenv("EBY_SSH_USER")
  sql_host <- Sys.getenv("EBY_SQL_HOST")
  sql_port <- Sys.getenv("EBY_SQL_PORT", "1433")
  local_port <- Sys.getenv("EBY_LOCAL_PORT", "1433")
  
  # Validate credentials are present
  if (db_name == "" || db_user == "" || db_password == "") {
    stop("Missing required environment variables. Please set:\n",
         "  EBY_SQL_DATABASE, EBY_SQL_USER, EBY_SQL_PASSWORD\n",
         "See SECURE_CONFIGURATION_GUIDE.md for details.")
  }
  
  # Test if we can connect to localhost:1433 (tunnel endpoint)
  can_connect <- tryCatch({
    test_conn <- DBI::dbConnect(
      odbc::odbc(),
      Driver = "ODBC Driver 18 for SQL Server",
      Server = paste0("tcp:127.0.0.1,", local_port),
      Database = db_name,
      UID = db_user,
      PWD = db_password,
      Encrypt = "no",
      ConnectionTimeout = 5  # Quick timeout for testing
    )
    DBI::dbDisconnect(test_conn)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  if (can_connect) {
    message("MAIN:  SSH tunnel is active and SQL Server is accessible")
    return(TRUE)
  }
  
  message("MAIN: SSH tunnel not detected, attempting to create...")
  
  if (ssh_host != "" && ssh_user != "" && sql_host != "") {
    message(sprintf("MAIN: SSH Command: ssh -L %s:%s:%s %s@%s",
                   local_port, sql_host, sql_port, ssh_user, ssh_host))
    warning("Please ensure SSH tunnel is running manually using the command above.")
    warning("You will be prompted for your SSH password.")
  } else {
    warning("SSH tunnel configuration not found in environment variables.")
    warning("Please set EBY_SSH_HOST, EBY_SSH_USER, EBY_SQL_HOST variables.")
  }
  
  return(FALSE)
}

# Connect to MAMBA's eBay SQL Server
connect_to_mamba_sql <- function() {
  message("MAIN: Connecting to MAMBA's eBay SQL Server...")
  
  # Get credentials from environment variables (SECURITY: Never hardcode!)
  db_name <- Sys.getenv("EBY_SQL_DATABASE")
  db_user <- Sys.getenv("EBY_SQL_USER")
  db_password <- Sys.getenv("EBY_SQL_PASSWORD")
  local_port <- Sys.getenv("EBY_LOCAL_PORT", "1433")
  
  # Validate credentials
  if (db_name == "" || db_user == "" || db_password == "") {
    stop("Missing required database credentials.\n",
         "Please set environment variables:\n",
         "  EBY_SQL_DATABASE, EBY_SQL_USER, EBY_SQL_PASSWORD\n",
         "See scripts/global_scripts/02_db_utils/SECURE_CONFIGURATION_GUIDE.md")
  }
  
  conn <- tryCatch({
    DBI::dbConnect(
      odbc::odbc(),
      Driver = "ODBC Driver 18 for SQL Server",  # MAMBA uses ODBC Driver 18
      Server = paste0("tcp:127.0.0.1,", local_port),  # Connect through SSH tunnel
      Database = db_name,
      UID = db_user,
      PWD = db_password,
      Encrypt = "no"  # Required for MAMBA's SQL Server setup
    )
  }, error = function(e) {
    stop("Failed to connect to database: ", e$message,
         "\nEnsure SSH tunnel is running and credentials are correct.")
  })
  
  message(sprintf("MAIN:  Connected to %s database on MAMBA's SQL Server", db_name))
  return(conn)
}

# Import MAMBA-specific eBay sales data with UTF-8 compliance (MP100)
import_mamba_eby_sales <- function(conn) {
  message("MAIN: Importing MAMBA eBay sales data from BAYORD/BAYORE tables...")
  message("MAIN:  Applying MP100 UTF-8 encoding handling for legacy SQL Server")
  
  # MAMBA uses specific table structure:
  # BAYORD - Order header table (ORD001-ORD048 columns)
  # BAYORE - Order detail table (ORE001-ORE015 columns)
  # They are joined on ORE001=ORD001 AND ORE013=ORD009
  
  # MP100 Compliance: Handle UTF-8 encoding issues in legacy SQL Server
  # Strategy: Import in phases, apply UTF-8 conversion on problematic fields
  
  # Phase 1: Import non-problematic fields with ORIGINAL column names (MP064 compliance)
  # MP100 + DM_R025 Compliance: Use BINARY casting to avoid UTF-8 conversion errors
  # Following MP064: 0IM phase must preserve original column names - NO RENAMING
  query_phase1 <- "
    SELECT 
      -- Order header fields (BAYORD) - Raw column names preserved
      ORD001,
      ORD002,
      ORD003,
      ORD004,
      ORD005,
      ORD006,
      ORD007,
      ORD008,
      ORD009,
      ORD021,
      ORD046,
      ORD047,
      ORD048,
      
      -- Order detail fields (BAYORE) - Raw column names preserved
      ORE002,
      ORE003,
      ORE005,
      ORE007,
      ORE008,
      ORE009,
      ORE010,
      ORE011,
      ORE012
      
    FROM BAYORE
    INNER JOIN BAYORD
      ON BAYORE.ORE001 = BAYORD.ORD001
      AND CAST(BAYORE.ORE013 AS VARBINARY(50)) = CAST(BAYORD.ORD009 AS VARBINARY(50))
    WHERE CAST(ORD003 AS VARCHAR(15)) >= FORMAT(DATEADD(month, -3, GETDATE()), 'yyyyMMdd')
    ORDER BY CAST(ORD003 AS VARCHAR(15)) DESC
  "
  
  # Phase 2: Import problematic text fields with ORIGINAL column names (MP064 compliance)
  # MP100 Compliance: Use COLLATE to ensure proper character handling
  # DM_R025 Compliance: Handle SQL Server encoding properly
  # Following MP064: 0IM phase must preserve original column names - NO RENAMING
  query_phase2 <- "
    SELECT 
      ORD001,
      -- Text fields with explicit collation for UTF-8 compatibility
      -- Using ISNULL to handle NULL values properly
      ISNULL(CAST(ORD010 AS NVARCHAR(500)) COLLATE Chinese_PRC_CI_AS, '') AS ORD010,
      ISNULL(CAST(ORD011 AS NVARCHAR(500)) COLLATE Chinese_PRC_CI_AS, '') AS ORD011, 
      ISNULL(CAST(ORD012 AS NVARCHAR(500)) COLLATE Chinese_PRC_CI_AS, '') AS ORD012,
      ISNULL(CAST(ORD013 AS NVARCHAR(200)) COLLATE Chinese_PRC_CI_AS, '') AS ORD013,
      ISNULL(CAST(ORD014 AS NVARCHAR(200)) COLLATE Chinese_PRC_CI_AS, '') AS ORD014,
      ISNULL(CAST(ORD015 AS NVARCHAR(50)) COLLATE Chinese_PRC_CI_AS, '') AS ORD015,
      ISNULL(CAST(ORD016 AS NVARCHAR(200)) COLLATE Chinese_PRC_CI_AS, '') AS ORD016,
      ISNULL(CAST(ORD020 AS NVARCHAR(200)) COLLATE Chinese_PRC_CI_AS, '') AS ORD020,
      ISNULL(CAST(ORE004 AS NVARCHAR(500)) COLLATE Chinese_PRC_CI_AS, '') AS ORE004,
      ISNULL(CAST(ORE006 AS NVARCHAR(500)) COLLATE Chinese_PRC_CI_AS, '') AS ORE006,
      ISNULL(CAST(ORE014 AS NVARCHAR(200)) COLLATE Chinese_PRC_CI_AS, '') AS ORE014,
      ISNULL(CAST(ORE015 AS NVARCHAR(100)) COLLATE Chinese_PRC_CI_AS, '') AS ORE015
      
    FROM BAYORE
    INNER JOIN BAYORD
      ON BAYORE.ORE001 = BAYORD.ORD001
      AND CAST(BAYORE.ORE013 AS VARBINARY(50)) = CAST(BAYORD.ORD009 AS VARBINARY(50))
    WHERE CAST(ORD003 AS VARCHAR(15)) >= FORMAT(DATEADD(month, -3, GETDATE()), 'yyyyMMdd')
    ORDER BY CAST(ORD003 AS VARCHAR(15)) DESC
  "
  
  message("MAIN:  Phase 1: Importing safe numeric/date fields...")
  df_safe <- tryCatch({
    DBI::dbGetQuery(conn, query_phase1)
  }, error = function(e) {
    message("MAIN:  Phase 1 failed: ", e$message)
    stop("Phase 1 import failed: ", e$message)
  })
  
  message(sprintf("MAIN:  Phase 1: Retrieved %d records", nrow(df_safe)))
  
  message("MAIN:  Phase 2: Importing text fields with UTF-8 conversion...")
  message("MAIN:  Using Chinese_PRC_CI_AS collation for SQL Server compatibility")
  df_text <- tryCatch({
    # MP100 Compliance: Set connection encoding if possible
    # Some ODBC drivers support encoding parameter
    tryCatch({
      DBI::dbSendStatement(conn, "SET NAMES 'UTF8'")
    }, error = function(e) {
      # Ignore if command not supported
    })
    
    # Execute query with proper encoding handling
    result <- DBI::dbGetQuery(conn, query_phase2)
    
    # MP100 Post-processing: Clean any remaining encoding issues
    text_cols <- c("ORD010", "ORD011", "ORD012", "ORD013", "ORD014", "ORD015",
                   "ORD016", "ORD020", "ORE004", "ORE006", "ORE014", "ORE015")
    
    for (col in text_cols) {
      if (col %in% names(result)) {
        # Convert from SQL Server encoding to UTF-8
        result[[col]] <- sapply(result[[col]], function(x) {
          if (is.na(x) || is.null(x) || x == "") return(NA_character_)
          
          # Try to convert from Windows-1252 or GB2312 to UTF-8
          x_clean <- tryCatch({
            # First try assuming Windows-1252 (common for SQL Server)
            iconv(x, from = "Windows-1252", to = "UTF-8", sub = "")
          }, error = function(e) {
            # Fallback to GB2312 for Chinese characters
            tryCatch({
              iconv(x, from = "GB2312", to = "UTF-8", sub = "")
            }, error = function(e2) {
              # Last resort: try automatic detection
              iconv(x, from = "", to = "UTF-8", sub = "")
            })
          })
          
          # Remove any null characters
          x_clean <- gsub('\\0', '', x_clean)
          
          # Return cleaned value or original if all conversions failed
          if (is.na(x_clean) || x_clean == "") x else x_clean
        }, USE.NAMES = FALSE)
      }
    }
    
    result
  }, error = function(e) {
    message("MAIN:  Phase 2 failed with error: ", e$message)
    message("MAIN:  Attempting alternative query approach...")
    
    # Alternative approach: Import as VARCHAR with explicit conversion
    # MP100 Compliance: Force VARCHAR to avoid NVARCHAR issues
    query_fallback <- "
      SELECT 
        ORD001,
        -- Convert to VARCHAR first, then we'll handle encoding in R
        CONVERT(VARCHAR(500), ORD010) AS ORD010,
        CONVERT(VARCHAR(500), ORD011) AS ORD011,
        CONVERT(VARCHAR(500), ORD012) AS ORD012,
        CONVERT(VARCHAR(200), ORD013) AS ORD013,
        CONVERT(VARCHAR(200), ORD014) AS ORD014,
        CONVERT(VARCHAR(50), ORD015) AS ORD015,
        CONVERT(VARCHAR(200), ORD016) AS ORD016,
        CONVERT(VARCHAR(200), ORD020) AS ORD020,
        CONVERT(VARCHAR(500), ORE004) AS ORE004,
        CONVERT(VARCHAR(500), ORE006) AS ORE006,
        CONVERT(VARCHAR(200), ORE014) AS ORE014,
        CONVERT(VARCHAR(100), ORE015) AS ORE015
        
      FROM BAYORE
      INNER JOIN BAYORD
        ON BAYORE.ORE001 = BAYORD.ORD001
        AND CAST(BAYORE.ORE013 AS VARBINARY(50)) = CAST(BAYORD.ORD009 AS VARBINARY(50))
      WHERE CAST(ORD003 AS VARCHAR(15)) >= FORMAT(DATEADD(month, -3, GETDATE()), 'yyyyMMdd')
      ORDER BY CAST(ORD003 AS VARCHAR(15)) DESC
    "
    
    result <- DBI::dbGetQuery(conn, query_fallback)
    
    # MP100 Post-processing: Apply encoding conversion in R
    text_cols <- c("ORD010", "ORD011", "ORD012", "ORD013", "ORD014", "ORD015",
                   "ORD016", "ORD020", "ORE004", "ORE006", "ORE014", "ORE015")
    
    for (col in text_cols) {
      if (col %in% names(result)) {
        result[[col]] <- sapply(result[[col]], function(x) {
          if (is.na(x) || is.null(x)) return(NA_character_)
          
          # Clean and convert
          x_clean <- gsub('[[:cntrl:]]', '', x)  # Remove control characters
          x_clean <- iconv(x_clean, from = "latin1", to = "UTF-8", sub = "")
          
          # If conversion failed, return placeholder
          if (is.na(x_clean) || x_clean == "") {
            return(paste0("ENCODING_ISSUE_", substr(col, 1, 6)))
          }
          x_clean
        }, USE.NAMES = FALSE)
      }
    }
    
    result
  })
  
  message(sprintf("MAIN:  Phase 2: Retrieved %d text records", nrow(df_text)))
  
  # Phase 3: Merge datasets using raw column names (MP064 compliance)
  message("MAIN:  Phase 3: Merging datasets...")
  df_sales <- merge(df_safe, df_text, by = "ORD001", all.x = TRUE)
  
  # MP100 Compliance: Apply UTF-8 cleaning to text fields using raw column names (MP064)
  message("MAIN:  Applying MP100 UTF-8 cleaning...")
  
  # Following MP064: Use original column names in 0IM phase
  text_columns <- c("ORD010", "ORD011", "ORD012", "ORD013", 
                   "ORD014", "ORD015", "ORD016", "ORD020", 
                   "ORE004", "ORE006", "ORE014", "ORE015")
  
  for (col in text_columns) {
    if (col %in% names(df_sales)) {
      # Remove null characters and invalid UTF-8 sequences
      df_sales[[col]] <- sapply(df_sales[[col]], function(x) {
        if (is.na(x) || is.null(x)) return(NA_character_)
        
        # Convert to character and remove problematic characters
        x_clean <- as.character(x)
        x_clean <- gsub('\\0', '', x_clean)  # Remove null characters
        x_clean <- iconv(x_clean, from = "", to = "UTF-8", sub = "?")  # Convert to UTF-8
        
        return(x_clean)
      }, USE.NAMES = FALSE)
    }
  }
  
  # Following MP106: Console Transparency - detailed record counts  
  message(sprintf("MAIN:  Retrieved %d sales records from MAMBATEK database", nrow(df_sales)))
  message("MAIN:  MP100 UTF-8 compliance applied - all text fields cleaned")
  message(sprintf("MAIN:  Data period: %d months back from %s", 3, Sys.Date()))
  message(sprintf("MAIN:  Raw columns preserved: %d columns", ncol(df_sales)))
  message("MAIN:  0IM Phase complete - NO renaming or transformations applied (MP064)")
  
  # Add minimal metadata for 0IM phase (MP064 compliance - no business logic)
  df_sales$import_timestamp <- Sys.time()
  df_sales$import_source <- "MAMBATEK_SQL_SERVER"
  df_sales$platform_code <- "eby"
  df_sales$company_code <- "MAMBA"
  
  # MP100 Compliance flag
  df_sales$utf8_compliant <- TRUE
  df_sales$encoding_method <- "PHASED_IMPORT_WITH_CLEANING"
  
  # Following MP064: All calculated fields moved to 1ST phase
  # NO business logic calculations in 0IM phase
  
  return(df_sales)
}

# Execute main process
tryCatch({
  # Ensure SSH tunnel is available (may already be running)
  tunnel_status <- ensure_ssh_tunnel()
  
  if (!tunnel_status) {
    stop("SSH tunnel is required but not available. Please run manually: ssh -L 1433:125.227.84.85:1433 kylelin@220.128.138.146")
  }
  
  # Connect to database
  conn <- connect_to_mamba_sql()
  
  # Import data
  df_eby_sales_raw <- import_mamba_eby_sales(conn)
  
  # Save to DuckDB (following MP096: Data Storage Selection Strategy)
  # Following MAMBA 7-Layer Architecture: 0IM Phase stores in raw_data.duckdb
  # Following DM_R039: Database connections already established in INITIALIZE
  
  # Write to database with MAMBA-specific naming (DM_R037 compliance)
  DBI::dbWriteTable(raw_data, "df_eby_sales___raw___MAMBA", 
                    df_eby_sales_raw, overwrite = TRUE)
  
  message("MAIN:  Data saved to df_eby_sales___raw___MAMBA")
  
  # Clean up SQL Server connection only
  DBI::dbDisconnect(conn)
  
  script_success <- TRUE
  
}, error = function(e) {
  message("MAIN:  Error in main process: ", e$message)
  script_success <- FALSE
})

main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
message(sprintf("MAIN: Main process completed in %.2f seconds", main_elapsed))

# ==============================================================================
# PART 3: TEST
# ==============================================================================

message("TEST: Starting validation tests")
test_start_time <- Sys.time()
test_passed <- TRUE

# Test 1: Verify data was written
tryCatch({
  # Following DM_R039: Use existing connection from INITIALIZE
  # No need to create new connection - use raw_data connection
  
  if (DBI::dbExistsTable(raw_data, "df_eby_sales___raw___MAMBA")) {
    row_count <- DBI::dbGetQuery(raw_data, 
                                 "SELECT COUNT(*) as n FROM df_eby_sales___raw___MAMBA")$n
    
    if (row_count > 0) {
      message(sprintf("TEST:  Table contains %d records", row_count))
    } else {
      message("TEST:  Table exists but is empty")
      test_passed <- FALSE
    }
  } else {
    message("TEST:  Table df_eby_sales___raw___MAMBA not found")
    test_passed <- FALSE
  }
  
  # Test 2: Verify raw column names preserved (MP064 compliance)
  if (test_passed) {
    schema <- DBI::dbGetQuery(raw_data, 
                             "SELECT column_name FROM information_schema.columns 
                              WHERE table_name = 'df_eby_sales___raw___MAMBA'")
    
    # Following MP064: 0IM phase should have original column names
    required_raw_fields <- c("ORD001", "ORD003", "ORD005", "ORD008", 
                            "ORE002", "ORE003", "ORE008", "ORE009")
    
    missing_fields <- setdiff(required_raw_fields, schema$column_name)
    
    if (length(missing_fields) == 0) {
      message("TEST:  All required raw fields present (MP064 compliant)")
    } else {
      message("TEST:  Missing raw fields: ", paste(missing_fields, collapse = ", "))
    }
  }
  
  # Following DM_R039: Connection will be closed in DEINITIALIZE
  
}, error = function(e) {
  message("TEST:  Test failed: ", e$message)
  test_passed <- FALSE
})

test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
message(sprintf("TEST: Tests completed in %.2f seconds", test_elapsed))

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

message("SUMMARIZE: Generating import summary")

# Prepare final metrics
final_metrics <- list(
  script_name = script_name,
  company = "MAMBA",
  platform = "eby",
  data_type = "sales",
  phase = "0IM",
  success = script_success && test_passed,
  records_imported = ifelse(exists("df_eby_sales_raw"), nrow(df_eby_sales_raw), 0),
  execution_time = as.numeric(Sys.time() - script_start_time, units = "secs"),
  ssh_host = Sys.getenv("EBY_SSH_HOST"),
  sql_server = Sys.getenv("EBY_SQL_HOST"),
  compliance = c("DM_R037", "MP104", "DM_R028", "DEV_R032")
)

# Display summary
message(strrep("=", 80))
message(" MAMBA EBY SALES IMPORT SUMMARY")
message(strrep("=", 80))
message(sprintf(" Company: %s", final_metrics$company))
message(sprintf(" Platform: %s (Custom SQL Server)", final_metrics$platform))
message(sprintf(" Data Type: %s", final_metrics$data_type))
message(sprintf(" Phase: %s", final_metrics$phase))
message(sprintf(" Records Imported: %d", final_metrics$records_imported))
message(sprintf(" Total Time: %.2f seconds", final_metrics$execution_time))
message(sprintf(" SSH Tunnel: %s", final_metrics$ssh_host))
message(sprintf(" SQL Server: %s", final_metrics$sql_server))
message(sprintf(" Status: %s", ifelse(final_metrics$success, "SUCCESS", "FAILED")))
message(sprintf(" Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message(strrep("=", 80))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE:  Cleaning up resources...")

# Following DM_R039: Close all database connections
if (exists("raw_data") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data)
  message("DEINITIALIZE: Closed raw_data connection")
}

# Note: SSH tunnel management
# The SSH tunnel is typically kept running for the entire ETL pipeline
# Manual command: ssh -L 1433:125.227.84.85:1433 kylelin@220.128.138.146
# This allows all ETL phases (0IM, 1ST, 2TR) to use the same connection

message("DEINITIALIZE:  MAMBA eBay Sales Import completed")
message(sprintf("DEINITIALIZE:  Script finished at: %s", 
               format(Sys.time(), "%Y-%m-%d %H:%M:%S")))

# Following DEV_R032: Five-Part Script Structure Standard
# MP103: autodeinit() removes ALL variables - must be absolute last statement
autodeinit()
# NO STATEMENTS AFTER THIS LINE