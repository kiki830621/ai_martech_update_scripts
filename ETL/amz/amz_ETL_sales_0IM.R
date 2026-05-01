# amz_ETL_sales_0IM.R - Import Amazon Sales Data
# ==============================================================================
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following DM_R037 v3.0: Config-Driven Import (source_type/version in app_config.yaml)
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Sales Phase 0IM (Import): Read raw data into raw_data.duckdb
# Input: RAW_DATA_DIR/amazon_sales/ (monthly xlsx files)
# Output: raw_data.duckdb → df_amazon_sales
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

sql_read_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R")
)
sql_read_path <- sql_read_candidates[file.exists(sql_read_candidates)][1]
if (is.na(sql_read_path)) {
  stop("fn_sql_read.R not found in expected paths")
}
source(sql_read_path)
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
import_result <- NULL
script_start_time <- Sys.time()
script_name <- "amz_ETL_sales_0IM"
script_version <- "1.1.0"

message(strrep("=", 80))
message("INITIALIZE: Starting Amazon Sales Import (0IM Phase)")
message(sprintf("INITIALIZE: Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message(strrep("=", 80))

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
OPERATION_MODE <- "UPDATE_MODE"
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$sales
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))

message("INITIALIZE: Loading required libraries...")
library(DBI)
library(duckdb)
library(readxl)

message("INITIALIZE: Loading import function...")
# #445: fix_excel_serial_dates() must be sourced BEFORE import_amazon_sales
# because the import function calls it via defensive exists() check.
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "fn_fix_excel_serial_dates.R"))
# #475: validate_amz_sales_row_integrity() — same defensive exists() pattern
# (drops column-shifted rows + writes df_amazon_sales_coverage_audit per
# SO_R038 v1.1 rule 5 / MP163).
source(file.path(GLOBAL_DIR, "05_etl_utils", "amz", "fn_validate_amz_sales_row_integrity.R"))
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
  # Resolve config-driven source profile (DM_R037 v3.0)
  source_type <- tolower(as.character(etl_profile$source_type %||% ""))
  if (!source_type %in% c("excel", "csv")) {
    stop(sprintf("VALIDATE FAILED: Unsupported source_type for sales: %s", source_type))
  }

  rawdata_root <- RAW_DATA_DIR %||% file.path(APP_DIR, "data", "local_data", "rawdata_QEF_DESIGN")
  rawdata_pattern <- as.character(etl_profile$rawdata_pattern %||% "")
  if (!nzchar(rawdata_pattern)) {
    stop("VALIDATE FAILED: sales profile missing rawdata_pattern")
  }

  matched_files <- Sys.glob(file.path(rawdata_root, rawdata_pattern))
  if (length(matched_files) == 0) {
    stop(sprintf("VALIDATE FAILED: No files match pattern '%s'", rawdata_pattern))
  }
  message(sprintf("VALIDATE: Found %d files matching declared pattern", length(matched_files)))

  # Pattern root (e.g., amazon_sales/*/*.xlsx -> amazon_sales)
  rawdata_rel_dir <- sub("/\\*.*$", "", rawdata_pattern)
  rawdata_rel_dir <- sub("/$", "", rawdata_rel_dir)
  rawdata_path <- file.path(rawdata_root, rawdata_rel_dir)

  if (!dir.exists(rawdata_path)) {
    stop(sprintf("Raw data directory not found: %s", rawdata_path))
  }

  # Count available files
  xlsx_files <- list.files(rawdata_path, pattern = "\\.xlsx$",
                           recursive = TRUE, full.names = TRUE)
  message(sprintf("MAIN: Found %d xlsx files in %s", length(xlsx_files), rawdata_path))

  # Import using shared function (overwrite for clean import)
  message("MAIN: Importing Amazon sales data...")
  audit_dir <- file.path(
    APP_DIR, "output", "etl_validation", "amz", "sales_0IM"
  )
  dir.create(audit_dir, recursive = TRUE, showWarnings = FALSE)
  audit_csv <- file.path(
    audit_dir,
    sprintf("sales_0IM_audit_%s.csv", format(Sys.time(), "%Y%m%d_%H%M%S"))
  )

  import_result <- import_df_amazon_sales(
    folder_path = rawdata_path,
    connection = raw_data,
    overwrite = TRUE,
    verbose = TRUE,
    fail_on_any_error = TRUE,
    return_report = TRUE,
    write_audit_csv = audit_csv,
    col_types = "text"
  )
  message("MAIN: 0IM audit report: ", audit_csv)

  if (!isTRUE(import_result$summary$success)) {
    stop(sprintf(
      paste(
        "0IM completeness gate failed:",
        "files_failed=%d, per_file_reconciliation_ok=%s, key_reconciliation_ok=%s, transaction_result=%s"
      ),
      import_result$summary$files_failed,
      import_result$summary$per_file_reconciliation_ok,
      import_result$summary$key_reconciliation_ok,
      import_result$summary$transaction_result
    ))
  }

  # Verify import
  if (dbExistsTable(raw_data, "df_amazon_sales")) {
    row_count <- sql_read(raw_data, "SELECT COUNT(*) as n FROM df_amazon_sales")$n
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
    row_count <- sql_read(raw_data, "SELECT COUNT(*) as n FROM df_amazon_sales")$n
    if (row_count == 0) {
      stop("Table df_amazon_sales is empty")
    }
    message(sprintf("TEST: %d rows imported", row_count))

    # Test 2.5: Source file completeness gate
    if (is.null(import_result) || is.null(import_result$summary)) {
      stop("Import report not found; cannot verify 0IM completeness")
    }
    if (import_result$summary$files_failed > 0) {
      stop(sprintf("Import completeness failed: %d files failed", import_result$summary$files_failed))
    }
    if (!isTRUE(import_result$summary$per_file_reconciliation_ok)) {
      stop("Per-file row reconciliation failed")
    }
    if (identical(import_result$summary$key_reconciliation_ok, FALSE)) {
      stop("ASIN key reconciliation failed")
    }
    if (!identical(import_result$summary$transaction_result, "COMMIT")) {
      stop(sprintf(
        "Transaction boundary failed: expected COMMIT, got %s",
        import_result$summary$transaction_result
      ))
    }
    if (row_count != import_result$summary$rows_expected_from_imported_files) {
      stop(sprintf(
        "Row reconciliation failed: table=%d expected=%d",
        row_count,
        import_result$summary$rows_expected_from_imported_files
      ))
    }
    message(sprintf(
      paste(
        "TEST: 0IM completeness passed",
        "(files_total=%d imported=%d empty=%d failed=%d)"
      ),
      import_result$summary$files_total,
      import_result$summary$files_imported,
      import_result$summary$files_empty,
      import_result$summary$files_failed
    ))

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
    sample <- sql_read(raw_data, "SELECT sku, purchase_date, item_price FROM df_amazon_sales LIMIT 3")
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
message(sprintf("Source: %s_%s", etl_profile$source_type, etl_profile$version))
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
