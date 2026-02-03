# amz_ETL_sales_0IM___QEF_DESIGN.R - Import Amazon Sales Data from Excel
# ==============================================================================
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following DM_R037: Company-Specific Script Suffix
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Sales Phase 0IM (Import): Read raw xlsx files into raw_data.duckdb
# Input: rawdata_QEF_DESIGN/amazon_sales/ (24 monthly xlsx files)
# Output: raw_data.duckdb → df_amazon_sales
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "amz_ETL_sales_0IM___QEF_DESIGN"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: Starting Amazon Sales Import (0IM Phase) - QEF_DESIGN")
message(sprintf("INITIALIZE: Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message(strrep("=", 80))

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
OPERATION_MODE <- "UPDATE_MODE"
autoinit()

message("INITIALIZE: Loading required libraries...")
library(DBI)
library(duckdb)
library(readxl)

message("INITIALIZE: Loading import function...")
source(file.path(GLOBAL_DIR, "05_etl_utils", "amz", "import_amazon_sales.R"))
source(file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R"))

message("INITIALIZE: Connecting to raw_data database...")
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
message(sprintf("INITIALIZE: Using: %s", db_path_list$raw_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: Initialization completed (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: Starting Amazon Sales Import...")
main_start_time <- Sys.time()

tryCatch({
  # Define raw data path (DM_R037: QEF_DESIGN-specific)
  rawdata_path <- file.path(APP_DIR, "data", "local_data",
                            "rawdata_QEF_DESIGN", "amazon_sales")

  if (!dir.exists(rawdata_path)) {
    stop(sprintf("Raw data directory not found: %s", rawdata_path))
  }

  # Count available files
  xlsx_files <- list.files(rawdata_path, pattern = "\\.xlsx$",
                           recursive = TRUE, full.names = TRUE)
  message(sprintf("MAIN: Found %d xlsx files in %s", length(xlsx_files), rawdata_path))

  # Import using shared function (overwrite for clean import)
  message("MAIN: Importing Amazon sales data...")
  import_df_amazon_sales(rawdata_path, raw_data, overwrite = TRUE, verbose = TRUE)

  # Verify import
  if (dbExistsTable(raw_data, "df_amazon_sales")) {
    row_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as n FROM df_amazon_sales")$n
    message(sprintf("MAIN: Successfully imported %d rows into df_amazon_sales", row_count))
    script_success <- TRUE
  } else {
    stop("Table df_amazon_sales was not created after import")
  }

  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: Import completed (%.2fs)", main_elapsed))

}, error = function(e) {
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR after %.2fs: %s", main_elapsed, e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: Starting import verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    # Test 1: Table exists
    if (!dbExistsTable(raw_data, "df_amazon_sales")) {
      stop("Table df_amazon_sales does not exist")
    }
    message("TEST: Table exists")

    # Test 2: Has data
    row_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as n FROM df_amazon_sales")$n
    if (row_count == 0) {
      stop("Table df_amazon_sales is empty")
    }
    message(sprintf("TEST: %d rows imported", row_count))

    # Test 3: Required columns exist
    columns <- dbListFields(raw_data, "df_amazon_sales")
    required <- c("sku", "purchase_date")
    missing <- setdiff(required, columns)
    if (length(missing) > 0) {
      stop(sprintf("Missing required columns: %s", paste(missing, collapse = ", ")))
    }
    message("TEST: Required columns present (sku, purchase_date)")

    # Test 4: Sample data
    message("TEST: Sample data:")
    sample <- dbGetQuery(raw_data, "SELECT sku, purchase_date, item_price FROM df_amazon_sales LIMIT 3")
    print(sample)

    test_passed <- TRUE
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: Verification passed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_passed <<- FALSE
    message(sprintf("TEST: Verification failed: %s", e$message))
  })
} else {
  message("TEST: Skipped due to main script failure")
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================

message(strrep("=", 80))
message("SUMMARIZE: AMAZON SALES IMPORT (0IM)")
message(strrep("=", 80))
message(sprintf("Platform: amz"))
message(sprintf("Phase: 0IM (Import)"))
message(sprintf("Company: QEF_DESIGN"))
message(sprintf("Total time: %.2fs", as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
message(sprintf("Compliance: MP064, DM_R028, DM_R037, DEV_R032"))
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: Cleaning up...")
if (exists("raw_data") && inherits(raw_data, "DBIConnection") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
