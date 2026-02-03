# amz_ETL_sales_1ST.R - Amazon Sales Data Staging
# ==============================================================================
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Sales Phase 1ST (Staging): Clean and stage raw Amazon sales data
# Input: raw_data.duckdb (df_amazon_sales)
# Output: staged_data.duckdb (df_amz_sales___staged)
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "amz_ETL_sales_1ST"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: Starting Amazon Sales Staging (1ST Phase)")
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
library(data.table)

source(file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R"))

message("INITIALIZE: Connecting to databases...")
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)
message(sprintf("INITIALIZE: Read from: %s", db_path_list$raw_data))
message(sprintf("INITIALIZE: Write to: %s", db_path_list$staged_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: Initialization completed (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: Starting Amazon Sales Staging...")
main_start_time <- Sys.time()

tryCatch({
  input_table <- "df_amazon_sales"
  output_table <- "df_amz_sales___staged"

  # Check source table
  if (!dbExistsTable(raw_data, input_table)) {
    stop(sprintf("Required table %s not found. Run amz_ETL_sales_0IM first.", input_table))
  }

  # Load raw data
  message(sprintf("MAIN: Step 1/4 - Loading %s...", input_table))
  load_start <- Sys.time()
  df_raw <- dbGetQuery(raw_data, sprintf("SELECT * FROM %s", input_table))
  n_raw <- nrow(df_raw)
  message(sprintf("MAIN: Loaded %d records (%.2fs)",
                  n_raw, as.numeric(Sys.time() - load_start, units = "secs")))

  if (n_raw == 0) {
    stop("No raw data found - cannot proceed with staging")
  }

  dt <- as.data.table(df_raw)

  # Step 2: Remove rows with all-NA critical fields
  message("MAIN: Step 2/4 - Removing invalid records...")
  clean_start <- Sys.time()

  # Remove rows where sku is NA
  n_before <- nrow(dt)
  dt <- dt[!is.na(sku) & nzchar(trimws(sku))]
  n_removed_sku <- n_before - nrow(dt)
  if (n_removed_sku > 0) {
    message(sprintf("    Removed %d rows with missing/empty SKU", n_removed_sku))
  }

  # Remove rows where purchase_date is NA
  n_before <- nrow(dt)
  dt <- dt[!is.na(purchase_date)]
  n_removed_date <- n_before - nrow(dt)
  if (n_removed_date > 0) {
    message(sprintf("    Removed %d rows with missing purchase_date", n_removed_date))
  }

  message(sprintf("MAIN: Cleaning done (%.2fs), %d records remain",
                  as.numeric(Sys.time() - clean_start, units = "secs"), nrow(dt)))

  # Step 3: Trim string columns and deduplicate
  message("MAIN: Step 3/4 - Trimming strings and deduplicating...")
  trim_start <- Sys.time()

  # Trim all character columns
  char_cols <- names(dt)[vapply(dt, is.character, logical(1))]
  for (col in char_cols) {
    set(dt, j = col, value = trimws(dt[[col]]))
  }

  # Deduplicate based on amazon_order_id + sku (if amazon_order_id exists)
  n_before <- nrow(dt)
  if ("amazon_order_id" %in% names(dt)) {
    dt <- unique(dt, by = c("amazon_order_id", "sku"))
  } else {
    dt <- unique(dt)
  }
  n_deduped <- n_before - nrow(dt)
  if (n_deduped > 0) {
    message(sprintf("    Removed %d duplicate records", n_deduped))
  }

  message(sprintf("MAIN: Trim/dedup done (%.2fs)",
                  as.numeric(Sys.time() - trim_start, units = "secs")))

  # Step 4: Add platform_id and write to staged_data
  message("MAIN: Step 4/4 - Writing staged data...")
  write_start <- Sys.time()

  dt[, platform_id := "amz"]

  # Drop existing table if present
  if (dbExistsTable(staged_data, output_table)) {
    dbRemoveTable(staged_data, output_table)
    message(sprintf("MAIN: Dropped existing table: %s", output_table))
  }

  dbWriteTable(staged_data, output_table, as.data.frame(dt), overwrite = TRUE)

  # Verify write
  actual_count <- dbGetQuery(staged_data,
    sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
  message(sprintf("MAIN: Stored %d records in %s (%.2fs)",
                  actual_count, output_table,
                  as.numeric(Sys.time() - write_start, units = "secs")))

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: Staging completed (%.2fs). %d → %d records",
                  main_elapsed, n_raw, actual_count))

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: Starting staging verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_amz_sales___staged"

    # Test 1: Table exists
    if (!dbExistsTable(staged_data, output_table)) {
      stop("Table does not exist")
    }
    message("TEST: Table exists")

    # Test 2: Has data
    row_count <- dbGetQuery(staged_data,
      sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
    if (row_count == 0) stop("Table is empty")
    message(sprintf("TEST: %d rows", row_count))

    # Test 3: platform_id column exists and is 'amz'
    platform_check <- dbGetQuery(staged_data, sprintf(
      "SELECT DISTINCT platform_id FROM %s", output_table))
    if (!"amz" %in% platform_check$platform_id) {
      stop("platform_id 'amz' not found")
    }
    message("TEST: platform_id = 'amz' confirmed")

    # Test 4: No NULL SKUs
    null_sku <- dbGetQuery(staged_data, sprintf(
      "SELECT COUNT(*) as n FROM %s WHERE sku IS NULL OR TRIM(sku) = ''", output_table))$n
    if (null_sku > 0) warning(sprintf("Found %d NULL/empty SKUs", null_sku))
    else message("TEST: No NULL/empty SKUs")

    test_passed <- TRUE
    message(sprintf("TEST: Verification passed (%.2fs)",
                    as.numeric(Sys.time() - test_start_time, units = "secs")))

  }, error = function(e) {
    test_passed <<- FALSE
    message(sprintf("TEST: Failed: %s", e$message))
  })
} else {
  message("TEST: Skipped due to main failure")
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================

message(strrep("=", 80))
message("SUMMARIZE: AMAZON SALES STAGING (1ST)")
message(strrep("=", 80))
message(sprintf("Platform: amz | Phase: 1ST"))
message(sprintf("Total time: %.2fs", as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
message(sprintf("Compliance: MP064, DM_R028, DEV_R032"))
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: Cleaning up...")
if (exists("raw_data") && inherits(raw_data, "DBIConnection") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data)
}
if (exists("staged_data") && inherits(staged_data, "DBIConnection") && DBI::dbIsValid(staged_data)) {
  DBI::dbDisconnect(staged_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
