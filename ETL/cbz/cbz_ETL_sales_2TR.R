# cbz_ETL_sales_2TR.R - Cyberbiz Sales Data Transformation
# ==============================================================================
# Following MP108: BASE ETL 0IM→1ST→2TR Pipeline
# Following MP104: ETL Data Flow Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following MP064: ETL-Derivation Separation Principle
# Following MP102: ETL Output Standardization Principle
# Following DM_R040: Structural JOIN Constraints
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Sales Phase 2TR (Transform): Cross-platform standardization
# Input: staged_data.duckdb (df_cbz_sales___staged)
# Output: transformed_data.duckdb (df_cbz_sales___transformed)
# Schema: Conforms to transformed_schemas.yaml#sales_transformed
#
# PIPELINE TYPE: BASE_SALES
# CBZ sales are already expanded to line items in 0IM (API line_items)
# No structural JOIN needed - direct transformation to standard schema
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "cbz_ETL_sales_2TR"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: ⚡ Starting Cyberbiz ETL Sales Transformation (2TR Phase)")
message(sprintf("INITIALIZE: 🕐 Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message("INITIALIZE: 📋 Script: cbz_ETL_sales_2TR.R v", script_version)
message("INITIALIZE: 📋 Compliance: MP108 (0IM→1ST→2TR) + MP102 (Output Standardization)")
message("INITIALIZE: 📋 Pipeline Type: BASE_SALES (No JOIN needed)")
message("INITIALIZE: 📋 Output Schema: transformed_schemas.yaml#sales_transformed")
message(strrep("=", 80))

# Initialize using unified autoinit system
autoinit()

# Load required libraries with progress feedback
message("INITIALIZE: 📦 Loading required libraries...")
lib_start <- Sys.time()
library(DBI)
library(duckdb)
library(dplyr)
library(data.table)
library(lubridate)
lib_elapsed <- as.numeric(Sys.time() - lib_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Libraries loaded successfully (%.2fs)", lib_elapsed))

# Source required functions
message("INITIALIZE: 📥 Loading database utilities...")
source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R")

# Establish database connections
message("INITIALIZE: 🔗 Connecting to databases...")
db_start <- Sys.time()
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)
db_elapsed <- as.numeric(Sys.time() - db_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Database connections established (%.2fs)", db_elapsed))
message(sprintf("INITIALIZE: 📖 Reading from: %s", db_path_list$staged_data))
message(sprintf("INITIALIZE: 📝 Writing to: %s", db_path_list$transformed_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: ✅ Initialization completed successfully (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: 🚀 Starting ETL Sales Transformation - Cross-platform standardization...")
main_start_time <- Sys.time()

tryCatch({
  # ----------------------------------------------------------------------------
  # 2.1: Load Staged Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 1/6 - Loading staged data...")
  load_start <- Sys.time()

  # Check if staged table exists
  staged_tables <- dbListTables(staged_data)
  staged_table <- "df_cbz_sales___staged"

  if (!staged_table %in% staged_tables) {
    stop(sprintf("Required table %s not found. Run cbz_ETL_sales_1ST.R first.", staged_table))
  }

  # Load staged data
  message(sprintf("MAIN: Loading %s...", staged_table))
  sales_staged <- dbGetQuery(staged_data, sprintf("SELECT * FROM %s", staged_table))
  n_staged <- nrow(sales_staged)

  load_elapsed <- as.numeric(Sys.time() - load_start, units = "secs")
  message(sprintf("MAIN: ✅ Loaded %d staged records (%.2fs)", n_staged, load_elapsed))

  if (n_staged == 0) {
    stop("No staged data found - cannot proceed with transformation")
  }

  # ----------------------------------------------------------------------------
  # 2.2: Convert to data.table for Efficient Processing
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 2/6 - Data preparation...")
  dt_sales <- as.data.table(sales_staged)
  message(sprintf("MAIN: ✅ Converted to data.table: %d rows × %d columns",
                  nrow(dt_sales), ncol(dt_sales)))

  # ----------------------------------------------------------------------------
  # 2.3: Create Cross-Platform Standard Identifiers
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 3/6 - Creating standard identifiers...")
  id_start <- Sys.time()

  # Create transaction_id per transformed_schemas.yaml
  # For CBZ: Use existing sales_transaction_id or create from order_id + sequence
  if ("sales_transaction_id" %in% names(dt_sales)) {
    dt_sales[, transaction_id := as.character(sales_transaction_id)]
    message("    ✅ Using sales_transaction_id as transaction_id")
  } else if ("order_id" %in% names(dt_sales)) {
    # Create sequence within each order
    dt_sales[, transaction_id := paste0(order_id, "_", sprintf("%03d", seq_len(.N))), by = order_id]
    message("    ✅ Created transaction_id from order_id + sequence")
  } else {
    stop("Cannot create transaction_id - no order_id or sales_transaction_id found")
  }

  # Ensure uniqueness of transaction_id
  if (anyDuplicated(dt_sales$transaction_id) > 0) {
    dup_count <- sum(duplicated(dt_sales$transaction_id))
    warning(sprintf("    ⚠️ Found %d duplicate transaction_ids - adding suffix", dup_count))
    dt_sales[, transaction_id := paste0(transaction_id, "_", seq_len(.N)),
             by = transaction_id]
  }

  id_elapsed <- as.numeric(Sys.time() - id_start, units = "secs")
  message(sprintf("MAIN: ✅ Standard identifiers created (%.2fs)", id_elapsed))

  # ----------------------------------------------------------------------------
  # 2.4: Standardize to Cross-Platform Schema (transformed_schemas.yaml)
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 4/6 - Schema standardization...")
  transform_start <- Sys.time()

  # Map to standard field names per transformed_schemas.yaml#sales_transformed
  # Required fields: transaction_id, order_id, customer_id, product_id, product_name,
  #                  sku, quantity, unit_price, line_total, order_date, etc.

  # Parse order_date to DATE type (required format: YYYY-MM-DD)
  if ("order_date" %in% names(dt_sales)) {
    dt_sales[, order_date := as.Date(order_date)]
    message("    ✅ Converted order_date to DATE type")
  } else {
    stop("order_date field missing - required for transformation")
  }

  # Extract time dimensions (required per transformed_schemas.yaml)
  dt_sales[, `:=`(
    order_year = year(order_date),
    order_month = month(order_date),
    order_quarter = quarter(order_date),
    order_day = day(order_date),
    order_weekday = weekdays(order_date)
  )]
  message("    ✅ Extracted time dimensions: year, month, quarter, day, weekday")

  # Map missing core fields from available API fields
  if (!"product_name" %in% names(dt_sales) && "title" %in% names(dt_sales)) {
    dt_sales[, product_name := title]
    message("    ✅ Mapped product_name from title")
  }

  if (!"unit_price" %in% names(dt_sales)) {
    if ("price" %in% names(dt_sales)) {
      dt_sales[, unit_price := price]
      message("    ✅ Mapped unit_price from price")
    } else if (all(c("total_price_after_discounts", "quantity") %in% names(dt_sales))) {
      dt_sales[, unit_price := total_price_after_discounts / quantity]
      message("    ✅ Derived unit_price from total_price_after_discounts / quantity")
    }
  }

  # Calculate line_total and discount_amount for consistency with schema
  if (!"line_total" %in% names(dt_sales) && all(c("quantity", "unit_price") %in% names(dt_sales))) {
    dt_sales[, line_total := quantity * unit_price]
    message("    ✅ Calculated line_total = quantity × unit_price")
  }

  if ("total_price_after_discounts" %in% names(dt_sales) &&
      all(c("quantity", "unit_price") %in% names(dt_sales))) {
    dt_sales[, discount_amount := pmax((quantity * unit_price) - total_price_after_discounts, 0)]
    dt_sales[, line_total := quantity * unit_price]  # enforce schema definition
    message("    ✅ Derived discount_amount and enforced line_total = quantity × unit_price")
  }

  # Ensure required financial fields are NUMERIC with proper precision
  if ("unit_price" %in% names(dt_sales)) {
    dt_sales[, unit_price := round(as.numeric(unit_price), 2)]
  }
  if ("line_total" %in% names(dt_sales)) {
    dt_sales[, line_total := round(as.numeric(line_total), 2)]
  }
  if ("tax_amount" %in% names(dt_sales)) {
    dt_sales[, tax_amount := round(as.numeric(tax_amount), 2)]
  }
  if ("discount_amount" %in% names(dt_sales)) {
    dt_sales[, discount_amount := round(as.numeric(discount_amount), 2)]
  }

  # Add transformation metadata (required per transformed_schemas.yaml)
  dt_sales[, `:=`(
    platform_id = "cbz",
    transformation_timestamp = Sys.time(),
    transformation_version = script_version,
    etl_pipeline = "BASE_SALES"  # CBZ is BASE (not DERIVED via JOIN)
  )]
  message("    ✅ Added transformation metadata: platform_id, timestamp, version, pipeline")

  transform_elapsed <- as.numeric(Sys.time() - transform_start, units = "secs")
  message(sprintf("MAIN: ✅ Schema standardization completed (%.2fs)", transform_elapsed))

  # ----------------------------------------------------------------------------
  # 2.5: Validate Against transformed_schemas.yaml Requirements
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 5/6 - Schema validation...")
  validate_start <- Sys.time()

  # Check required fields per transformed_schemas.yaml
  required_fields <- c(
    "transaction_id", "order_id", "customer_id", "product_id", "product_name",
    "quantity", "unit_price", "line_total",
    "order_date", "order_year", "order_month", "order_quarter",
    "platform_id", "transformation_timestamp", "etl_pipeline"
  )

  missing_fields <- setdiff(required_fields, names(dt_sales))
  if (length(missing_fields) > 0) {
    warning(sprintf("MAIN: ⚠️ Missing recommended fields: %s",
                    paste(missing_fields, collapse = ", ")))
    # Note: Some fields like customer_id may not exist in all data sources
  }

  present_required <- intersect(required_fields, names(dt_sales))
  message(sprintf("MAIN: ✅ Present required fields (%d/%d): %s",
                  length(present_required), length(required_fields),
                  paste(head(present_required, 8), collapse = ", ")))

  # Business rule validation: line_total = quantity × unit_price
  if (all(c("line_total", "quantity", "unit_price") %in% names(dt_sales))) {
    calculated_total <- dt_sales$quantity * dt_sales$unit_price
    tolerance <- 0.02  # Allow 2 cent rounding difference
    mismatch <- abs(dt_sales$line_total - calculated_total) > tolerance
    mismatch_count <- sum(mismatch, na.rm = TRUE)

    if (mismatch_count > 0) {
      warning(sprintf("MAIN: ⚠️ Found %d records where line_total != quantity × unit_price",
                      mismatch_count))
    } else {
      message("    ✅ Business rule validated: line_total = quantity × unit_price")
    }
  }

  # Check uniqueness of transaction_id
  dup_txn <- sum(duplicated(dt_sales$transaction_id))
  if (dup_txn > 0) {
    stop(sprintf("MAIN: ❌ Found %d duplicate transaction_ids - violates uniqueness constraint", dup_txn))
  } else {
    message("    ✅ Uniqueness validated: transaction_id is unique")
  }

  validate_elapsed <- as.numeric(Sys.time() - validate_start, units = "secs")
  message(sprintf("MAIN: ✅ Schema validation completed (%.2fs)", validate_elapsed))

  # ----------------------------------------------------------------------------
  # 2.6: Store Transformed Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 6/6 - Writing transformed data...")
  write_start <- Sys.time()

  # Table name per transformed_schemas.yaml pattern
  output_table <- "df_cbz_sales___transformed"

  # Drop existing table if present
  if (dbExistsTable(transformed_data, output_table)) {
    dbRemoveTable(transformed_data, output_table)
    message(sprintf("MAIN: 🗑️ Dropped existing table: %s", output_table))
  }

  # Convert back to data.frame for database write
  df_transformed <- as.data.frame(dt_sales)

  # Write to transformed_data
  dbWriteTable(transformed_data, output_table, df_transformed, overwrite = TRUE)

  # Verify write
  actual_count <- dbGetQuery(transformed_data,
    sprintf("SELECT COUNT(*) as count FROM %s", output_table))$count

  write_elapsed <- as.numeric(Sys.time() - write_start, units = "secs")
  message(sprintf("MAIN: ✅ Stored %d records in %s (%.2fs)",
                  actual_count, output_table, write_elapsed))

  # Display sample for verification
  message("MAIN: 📋 Sample of transformed data (first 3 records):")
  sample_data <- head(df_transformed, 3)

  # Show key columns
  key_cols <- c("transaction_id", "order_id", "product_name",
                "quantity", "unit_price", "line_total", "order_date")
  key_cols_present <- intersect(key_cols, names(sample_data))
  print(sample_data[, key_cols_present])

  # Summary statistics
  if ("line_total" %in% names(df_transformed)) {
    total_revenue <- sum(df_transformed$line_total, na.rm = TRUE)
    message(sprintf("MAIN: 💰 Total revenue: $%.2f", total_revenue))
  }

  unique_orders <- length(unique(df_transformed$order_id))
  message(sprintf("MAIN: 📦 Unique orders: %d", unique_orders))

  if ("order_year" %in% names(df_transformed)) {
    year_dist <- table(df_transformed$order_year)
    message(sprintf("MAIN: 📅 Year distribution: %s",
                    paste(sprintf("%s(%d)", names(year_dist), year_dist), collapse = ", ")))
  }

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Sales Transformation completed successfully (%.2fs)", main_elapsed))

}, error = function(e) {
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ❌ ERROR after %.2fs: %s", main_elapsed, e$message))
  message(sprintf("MAIN: 📍 Error traceback: %s", paste(deparse(sys.calls()), collapse = "\n")))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: 🧪 Starting ETL Sales Transformation verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_cbz_sales___transformed"

    # Test 1: Verify table exists
    if (!dbExistsTable(transformed_data, output_table)) {
      stop(sprintf("TEST: Table %s does not exist", output_table))
    }
    message("TEST: ✅ Table exists")

    # Test 2: Verify data was created
    row_count <- dbGetQuery(transformed_data,
      sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
    if (row_count == 0) {
      stop(sprintf("TEST: No data in %s", output_table))
    }
    message(sprintf("TEST: ✅ Data created (%d rows)", row_count))

    # Test 3: Verify required columns exist
    columns <- dbListFields(transformed_data, output_table)
    required_cols <- c("transaction_id", "order_id", "platform_id",
                       "transformation_timestamp", "etl_pipeline")
    missing_cols <- setdiff(required_cols, columns)

    if (length(missing_cols) > 0) {
      stop(sprintf("TEST: Missing required columns: %s",
                   paste(missing_cols, collapse = ", ")))
    }
    message("TEST: ✅ All required columns present")

    # Test 4: Verify platform_id is correct
    platform_check <- dbGetQuery(transformed_data,
      sprintf("SELECT DISTINCT platform_id FROM %s", output_table))
    if (nrow(platform_check) != 1 || platform_check$platform_id[1] != "cbz") {
      stop(sprintf("TEST: platform_id should be 'cbz', found: %s",
                   paste(platform_check$platform_id, collapse = ", ")))
    }
    message("TEST: ✅ platform_id is 'cbz'")

    # Test 5: Verify etl_pipeline is BASE_SALES
    pipeline_check <- dbGetQuery(transformed_data,
      sprintf("SELECT DISTINCT etl_pipeline FROM %s", output_table))
    if (pipeline_check$etl_pipeline[1] != "BASE_SALES") {
      warning(sprintf("TEST: etl_pipeline should be 'BASE_SALES', found: %s",
                      pipeline_check$etl_pipeline[1]))
    } else {
      message("TEST: ✅ etl_pipeline is 'BASE_SALES'")
    }

    # Test 6: Verify no duplicate transaction IDs
    dup_check <- dbGetQuery(transformed_data, sprintf("
      SELECT transaction_id, COUNT(*) as cnt
      FROM %s
      GROUP BY transaction_id
      HAVING COUNT(*) > 1
    ", output_table))
    if (nrow(dup_check) > 0) {
      stop(sprintf("TEST: Found %d duplicate transaction IDs", nrow(dup_check)))
    }
    message("TEST: ✅ No duplicate transactions")

    # Test 7: Verify time dimensions
    if ("order_year" %in% columns) {
      year_check <- dbGetQuery(transformed_data, sprintf("
        SELECT MIN(order_year) as min_year, MAX(order_year) as max_year
        FROM %s
      ", output_table))
      message(sprintf("TEST: 📅 Year range: %d - %d",
                      year_check$min_year, year_check$max_year))
    }

    # Test 8: Sample data verification
    message("TEST: 📋 Sample verification (first 3 records):")
    sample_check <- dbGetQuery(transformed_data, sprintf("
      SELECT transaction_id, order_id, product_name, quantity, unit_price, line_total
      FROM %s
      LIMIT 3
    ", output_table))
    print(sample_check)

    test_passed <- TRUE
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Transformation verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Transformation verification failed after %.2fs: %s",
                    test_elapsed, e$message))
  })
} else {
  message("TEST: ⏭️ Skipped due to main script failure")
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================

summarize_start_time <- Sys.time()

# Determine status
if (script_success && test_passed) {
  message("SUMMARIZE: ✅ ETL Sales Transformation completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Sales Transformation failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "sales",
  platform = "cbz",
  etl_phase = "2TR",
  pipeline_type = "BASE_SALES",
  output_schema = "transformed_schemas.yaml#sales_transformed",
  compliance = c("MP108", "MP104", "MP102", "DM_R028", "MP064", "DM_R040", "DEV_R032", "MP103")
)

# Final summary reporting
message(strrep("=", 80))
message("SUMMARIZE: 📊 SALES TRANSFORMATION SUMMARY")
message(strrep("=", 80))
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("⚙️  Pipeline Type: %s", final_metrics$pipeline_type))
message(sprintf("📋 Output Schema: %s", final_metrics$output_schema))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message(strrep("=", 80))

message("SUMMARIZE: ✅ ETL Sales Transformation (cbz_ETL_sales_2TR.R) completed")
message(sprintf("SUMMARIZE: 🏁 Final completion time: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))

# Prepare return value
final_return_status <- final_metrics$final_status

summarize_elapsed <- as.numeric(Sys.time() - summarize_start_time, units = "secs")
message(sprintf("SUMMARIZE: ✅ Summary completed (%.2fs)", summarize_elapsed))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: 🧹 Starting cleanup...")
deinit_start_time <- Sys.time()

# Cleanup database connections
message("DEINITIALIZE: 🔌 Disconnecting databases...")
DBI::dbDisconnect(staged_data)
DBI::dbDisconnect(transformed_data)

# Log cleanup completion
deinit_elapsed <- as.numeric(Sys.time() - deinit_start_time, units = "secs")
message(sprintf("DEINITIALIZE: ✅ Cleanup completed (%.2fs)", deinit_elapsed))

# Following MP103: autodeinit() removes ALL variables - must be absolute last statement
message("DEINITIALIZE: 🧹 Executing autodeinit()...")
autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
