# amz_ETL_sales_2TS.R - Amazon Sales Data Standardization
# ==============================================================================
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Sales Phase 2TS (Standardization): Create unified schema for derivations
# Input: transformed_data.duckdb (df_amz_sales___transformed)
# Output: transformed_data.duckdb (df_amz_sales___standardized)
#
# Purpose: Bridge ETL output to Derivation input by standardizing column names
#   - purchase_date → payment_time (D01 expects payment_time)
#   - item_price → lineproduct_price (D01 expects lineproduct_price)
#   - ship_postal_code → customer_id (Amazon has no buyer_email in flat file)
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "amz_ETL_sales_2TS"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: Starting Amazon ETL Sales Standardization (2TS Phase)")
message(sprintf("INITIALIZE: Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message("INITIALIZE: Purpose: Column standardization for D01 derivations")
message("INITIALIZE: Mapping: purchase_date->payment_time, item_price->lineproduct_price")
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
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)
message(sprintf("INITIALIZE: Using: %s", db_path_list$transformed_data))

# Also connect to raw_data for KEYS product mapping
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
message(sprintf("INITIALIZE: KEYS from: %s", db_path_list$raw_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: Initialization completed (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: Starting ETL Sales Standardization...")
main_start_time <- Sys.time()

tryCatch({
  # --------------------------------------------------------------------------
  # 2.1: Load Transformed Data
  # --------------------------------------------------------------------------
  message("MAIN: Step 1/4 - Loading transformed data...")
  load_start <- Sys.time()

  input_table <- "df_amz_sales___transformed"
  output_table <- "df_amz_sales___standardized"

  if (!dbExistsTable(transformed_data, input_table)) {
    stop(sprintf("Required table %s not found. Run amz_ETL_sales_2TR first.", input_table))
  }

  sales_transformed <- dbGetQuery(transformed_data, sprintf("SELECT * FROM %s", input_table))
  n_records <- nrow(sales_transformed)
  message(sprintf("MAIN: Loaded %d records (%.2fs)",
                  n_records, as.numeric(Sys.time() - load_start, units = "secs")))

  if (n_records == 0) {
    stop("No transformed data found - cannot proceed with standardization")
  }

  dt_sales <- as.data.table(sales_transformed)

  # --------------------------------------------------------------------------
  # 2.2: Standardize Column Names for D01 Derivations
  # --------------------------------------------------------------------------
  message("MAIN: Step 2/4 - Standardizing column names...")
  transform_start <- Sys.time()

  # Map purchase_date → payment_time
  if ("purchase_date" %in% names(dt_sales)) {
    dt_sales[, payment_time := purchase_date]
    message("    Mapped: purchase_date -> payment_time")
  } else {
    stop("No purchase_date column found")
  }

  # Map item_price → lineproduct_price
  if ("item_price" %in% names(dt_sales)) {
    dt_sales[, lineproduct_price := item_price]
    message("    Mapped: item_price -> lineproduct_price")
  } else {
    stop("No item_price column found")
  }

  # Ensure platform_id exists
  if (!"platform_id" %in% names(dt_sales)) {
    dt_sales[, platform_id := "amz"]
    message("    Created: platform_id = 'amz'")
  }

  # --------------------------------------------------------------------------
  # 2.3: Generate customer_id from ship_postal_code
  # --------------------------------------------------------------------------
  message("MAIN: Step 3/4 - Generating customer_id...")
  cid_start <- Sys.time()

  # QEF Amazon flat file has no buyer_email; use ship_postal_code as proxy
  if ("ship_postal_code" %in% names(dt_sales)) {
    # Clean postal code: trim, uppercase, remove non-alphanumeric
    dt_sales[, ship_postal_code_clean := toupper(trimws(as.character(ship_postal_code)))]
    dt_sales[, ship_postal_code_clean := gsub("[^A-Z0-9]", "", ship_postal_code_clean)]

    # Generate integer customer_id from postal code factor
    dt_sales[nzchar(ship_postal_code_clean) & !is.na(ship_postal_code_clean),
             customer_id := as.integer(as.factor(ship_postal_code_clean))]

    n_with_id <- sum(!is.na(dt_sales$customer_id))
    n_unique <- length(unique(dt_sales$customer_id[!is.na(dt_sales$customer_id)]))
    message(sprintf("    Generated customer_id from ship_postal_code: %d records, %d unique customers",
                    n_with_id, n_unique))

    # Remove records without customer_id
    n_before <- nrow(dt_sales)
    dt_sales <- dt_sales[!is.na(customer_id)]
    n_removed <- n_before - nrow(dt_sales)
    if (n_removed > 0) {
      message(sprintf("    Removed %d records without valid postal code", n_removed))
    }

    # Clean up temp column
    dt_sales[, ship_postal_code_clean := NULL]
  } else {
    stop("No ship_postal_code column found - cannot generate customer_id")
  }

  message(sprintf("MAIN: Customer ID generation done (%.2fs)",
                  as.numeric(Sys.time() - cid_start, units = "secs")))

  # --------------------------------------------------------------------------
  # 2.4: Join product_line_id from KEYS and write
  # --------------------------------------------------------------------------
  message("MAIN: Step 4/4 - Joining product keys and writing...")
  write_start <- Sys.time()

  # Try to join product_line_id from KEYS if available
  if (dbExistsTable(raw_data, "df_amz_product_keys")) {
    keys_dt <- as.data.table(dbGetQuery(raw_data, "SELECT * FROM df_amz_product_keys"))
    if ("product_line_id" %in% names(keys_dt) && "sku" %in% names(keys_dt)) {
      key_cols <- intersect(c("sku", "product_line_id", "product_name", "cost", "profit"), names(keys_dt))
      keys_subset <- unique(keys_dt[, ..key_cols])
      n_before_join <- nrow(dt_sales)
      dt_sales <- merge(dt_sales, keys_subset, by = "sku", all.x = TRUE)
      n_matched <- sum(!is.na(dt_sales$product_line_id))
      message(sprintf("    Joined product keys: %d/%d matched product_line_id",
                      n_matched, nrow(dt_sales)))
    }
  } else {
    message("    Note: df_amz_product_keys not found - skipping product key join")
    message("    Run amz_ETL_keys_0IM___QEF_DESIGN.R first for product mapping")
  }

  # Verify D01 required columns
  required_cols <- c("customer_id", "payment_time", "lineproduct_price", "platform_id")
  missing_cols <- setdiff(required_cols, names(dt_sales))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns for D01: %s", paste(missing_cols, collapse = ", ")))
  }
  message(sprintf("MAIN: All D01 required columns present: %s", paste(required_cols, collapse = ", ")))

  # Add standardization metadata
  dt_sales[, `:=`(
    standardization_timestamp = Sys.time(),
    standardization_version = script_version
  )]

  # Drop existing output table
  if (dbExistsTable(transformed_data, output_table)) {
    dbRemoveTable(transformed_data, output_table)
    message(sprintf("MAIN: Dropped existing table: %s", output_table))
  }

  dbWriteTable(transformed_data, output_table, as.data.frame(dt_sales), overwrite = TRUE)

  actual_count <- dbGetQuery(transformed_data,
    sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
  message(sprintf("MAIN: Stored %d records in %s (%.2fs)",
                  actual_count, output_table,
                  as.numeric(Sys.time() - write_start, units = "secs")))

  # Show sample
  message("MAIN: Sample of standardized data (D01 required columns):")
  sample_data <- head(as.data.frame(dt_sales), 3)
  print(sample_data[, required_cols])

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: Standardization completed (%.2fs)", main_elapsed))

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: Starting standardization verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_amz_sales___standardized"

    # Test 1: Table exists
    if (!dbExistsTable(transformed_data, output_table)) stop("Table does not exist")
    message("TEST: Table exists")

    # Test 2: Has data
    row_count <- dbGetQuery(transformed_data,
      sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
    if (row_count == 0) stop("Table is empty")
    message(sprintf("TEST: %d rows", row_count))

    # Test 3: D01 required columns present
    columns <- dbListFields(transformed_data, output_table)
    required_cols <- c("customer_id", "payment_time", "lineproduct_price", "platform_id")
    missing_cols <- setdiff(required_cols, columns)
    if (length(missing_cols) > 0) {
      stop(sprintf("Missing D01 columns: %s", paste(missing_cols, collapse = ", ")))
    }
    message("TEST: All D01 required columns present")

    # Test 4: No NULL payment_time
    null_time <- dbGetQuery(transformed_data, sprintf(
      "SELECT COUNT(*) as n FROM %s WHERE payment_time IS NULL", output_table))$n
    if (null_time > 0) warning(sprintf("Found %d NULL payment_time values", null_time))
    else message("TEST: No NULL payment_time values")

    # Test 5: No NULL lineproduct_price
    null_price <- dbGetQuery(transformed_data, sprintf(
      "SELECT COUNT(*) as n FROM %s WHERE lineproduct_price IS NULL", output_table))$n
    if (null_price > 0) warning(sprintf("Found %d NULL lineproduct_price values", null_price))
    else message("TEST: No NULL lineproduct_price values")

    # Test 6: Sample verification
    message("TEST: Sample verification:")
    sample_check <- dbGetQuery(transformed_data, sprintf(
      "SELECT customer_id, payment_time, lineproduct_price, platform_id FROM %s LIMIT 3",
      output_table))
    print(sample_check)

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
message("SUMMARIZE: AMAZON SALES STANDARDIZATION (2TS)")
message(strrep("=", 80))
message(sprintf("Platform: amz | Phase: 2TS"))
message(sprintf("Purpose: Column standardization for D01 derivations"))
message(sprintf("Mapping: purchase_date->payment_time, item_price->lineproduct_price"))
message(sprintf("Customer ID: Generated from ship_postal_code"))
message(sprintf("Total time: %.2fs", as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
message(sprintf("Compliance: MP064, DM_R028, DEV_R032, MP103"))
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: Cleaning up...")
if (exists("raw_data") && inherits(raw_data, "DBIConnection") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data)
}
if (exists("transformed_data") && inherits(transformed_data, "DBIConnection") && DBI::dbIsValid(transformed_data)) {
  DBI::dbDisconnect(transformed_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
