# cbz_ETL_orders_2TR.R - Cyberbiz Order Data Transformation
# ==============================================================================
# Following MP108: BASE ETL 0IM→1ST→2TR Pipeline
# Following MP104: ETL Data Flow Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following MP064: ETL-Derivation Separation Principle
# Following MP102: ETL Output Standardization Principle
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Orders Phase 2TR (Transform): Cross-platform standardization
# Input: staged_data.duckdb (df_cbz_orders___staged)
# Output: transformed_data.duckdb (df_cbz_orders___transformed)
# Schema: Conforms to transformed_schemas.yaml#orders_transformed
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "cbz_ETL_orders_2TR"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: ⚡ Starting Cyberbiz ETL Order Transformation (2TR Phase)")
message(sprintf("INITIALIZE: 🕐 Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message("INITIALIZE: 📋 Script: cbz_ETL_orders_2TR.R v", script_version)
message("INITIALIZE: 📋 Compliance: MP108 (0IM→1ST→2TR) + MP102 (Output Standardization)")
message("INITIALIZE: 📋 Output Schema: transformed_schemas.yaml#orders_transformed")
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

message("MAIN: 🚀 Starting ETL Order Transformation - Cross-platform standardization...")
main_start_time <- Sys.time()

tryCatch({
  # ----------------------------------------------------------------------------
  # 2.1: Load Staged Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 1/5 - Loading staged data...")
  load_start <- Sys.time()

  # Check if staged table exists
  staged_tables <- dbListTables(staged_data)
  staged_table <- "df_cbz_orders___staged"

  if (!staged_table %in% staged_tables) {
    stop(sprintf("Required table %s not found. Run cbz_ETL_orders_1ST.R first.", staged_table))
  }

  # Load staged data
  message(sprintf("MAIN: Loading %s...", staged_table))
  orders_staged <- dbGetQuery(staged_data, sprintf("SELECT * FROM %s", staged_table))
  n_staged <- nrow(orders_staged)

  load_elapsed <- as.numeric(Sys.time() - load_start, units = "secs")
  message(sprintf("MAIN: ✅ Loaded %d staged records (%.2fs)", n_staged, load_elapsed))

  if (n_staged == 0) {
    stop("No staged data found - cannot proceed with transformation")
  }

  # ----------------------------------------------------------------------------
  # 2.2: Convert to data.table for Efficient Processing
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 2/5 - Data preparation...")
  dt_orders <- as.data.table(orders_staged)
  message(sprintf("MAIN: ✅ Converted to data.table: %d rows × %d columns",
                  nrow(dt_orders), ncol(dt_orders)))

  # Column mapping: CBZ field name differences
  if ("created_at" %in% names(dt_orders) && !"order_date" %in% names(dt_orders)) {
    dt_orders[, order_date := created_at]
    message("    ✓ Mapped 'created_at' → 'order_date'")
  }

  # Handle nested order_status field
  if (!"order_status" %in% names(dt_orders)) {
    # Try common nested field patterns
    if ("status" %in% names(dt_orders)) {
      dt_orders[, order_status := status]
      message("    ✓ Mapped 'status' → 'order_status'")
    } else if (any(grepl("status", names(dt_orders), ignore.case = TRUE))) {
      status_col <- grep("status", names(dt_orders), ignore.case = TRUE, value = TRUE)[1]
      dt_orders[, order_status := get(status_col)]
      message(sprintf("    ✓ Mapped '%s' → 'order_status'", status_col))
    } else {
      dt_orders[, order_status := "unknown"]
      warning("    ⚠ No status field found, using 'unknown'")
    }
  }

  # Handle nested order_total field
  if (!"order_total" %in% names(dt_orders)) {
    # Try common total field patterns
    if ("total_price" %in% names(dt_orders)) {
      dt_orders[, order_total := total_price]
      message("    ✓ Mapped 'total_price' → 'order_total'")
    } else if ("total" %in% names(dt_orders)) {
      dt_orders[, order_total := total]
      message("    ✓ Mapped 'total' → 'order_total'")
    } else if (any(grepl("total|amount", names(dt_orders), ignore.case = TRUE))) {
      total_col <- grep("total|amount", names(dt_orders), ignore.case = TRUE, value = TRUE)[1]
      dt_orders[, order_total := as.numeric(get(total_col))]
      message(sprintf("    ✓ Mapped '%s' → 'order_total'", total_col))
    } else {
      dt_orders[, order_total := 0]
      warning("    ⚠ No total field found, using 0")
    }
  }

  # ----------------------------------------------------------------------------
  # 2.3: Standardize to Cross-Platform Schema (transformed_schemas.yaml)
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 3/5 - Schema standardization...")
  transform_start <- Sys.time()

  # Parse order_date to DATE type (required per transformed_schemas.yaml)
  if ("order_date" %in% names(dt_orders)) {
    dt_orders[, order_date := as.Date(order_date)]
    message("    ✅ Converted order_date to DATE type")
  }

  # Standardize order_status to standard values per transformed_schemas.yaml
  # Valid values: ["pending", "processing", "shipped", "delivered", "cancelled", "refunded"]
  if ("order_status" %in% names(dt_orders)) {
    # Create mapping for CBZ order statuses
    status_mapping <- c(
      "待處理" = "pending",
      "處理中" = "processing",
      "已出貨" = "shipped",
      "已送達" = "delivered",
      "已完成" = "delivered",
      "已取消" = "cancelled",
      "已退款" = "refunded",
      "退貨" = "refunded"
    )

    # Apply mapping if possible
    dt_orders[, order_status_original := order_status]
    dt_orders[, order_status := {
      mapped <- status_mapping[as.character(order_status)]
      ifelse(is.na(mapped), tolower(trimws(order_status)), mapped)
    }]
    message("    ✅ Standardized order_status to cross-platform values")
  }

  # Standardize payment_method per transformed_schemas.yaml
  # Valid values: ["credit_card", "debit_card", "paypal", "bank_transfer", "cash_on_delivery", "other"]
  if ("payment_method" %in% names(dt_orders)) {
    payment_mapping <- c(
      "信用卡" = "credit_card",
      "轉帳" = "bank_transfer",
      "貨到付款" = "cash_on_delivery",
      "PayPal" = "paypal"
    )

    dt_orders[, payment_method_original := payment_method]
    dt_orders[, payment_method := {
      mapped <- payment_mapping[as.character(payment_method)]
      ifelse(is.na(mapped), "other", mapped)
    }]
    message("    ✅ Standardized payment_method to cross-platform values")
  }

  # Rename order_total if exists (for schema consistency)
  if ("total_amount" %in% names(dt_orders) && !"order_total" %in% names(dt_orders)) {
    dt_orders[, order_total := total_amount]
    message("    ✅ Renamed total_amount to order_total for schema consistency")
  }

  # Add transformation metadata (required per transformed_schemas.yaml)
  dt_orders[, `:=`(
    platform_id = "cbz",
    transformation_timestamp = Sys.time(),
    transformation_version = script_version
  )]
  message("    ✅ Added transformation metadata: platform_id, timestamp, version")

  transform_elapsed <- as.numeric(Sys.time() - transform_start, units = "secs")
  message(sprintf("MAIN: ✅ Schema standardization completed (%.2fs)", transform_elapsed))

  # ----------------------------------------------------------------------------
  # 2.4: Validate Against transformed_schemas.yaml Requirements
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 4/5 - Schema validation...")
  validate_start <- Sys.time()

  # Check required fields per transformed_schemas.yaml#orders_transformed
  required_fields <- c(
    "order_id", "customer_id", "order_date", "order_status", "order_total",
    "platform_id", "transformation_timestamp"
  )

  missing_fields <- setdiff(required_fields, names(dt_orders))
  if (length(missing_fields) > 0) {
    warning(sprintf("MAIN: ⚠️ Missing recommended fields: %s",
                    paste(missing_fields, collapse = ", ")))
  }

  present_required <- intersect(required_fields, names(dt_orders))
  message(sprintf("MAIN: ✅ Present required fields (%d/%d): %s",
                  length(present_required), length(required_fields),
                  paste(head(present_required, 8), collapse = ", ")))

  # Check uniqueness of order_id (required per schema)
  dup_orders <- sum(duplicated(dt_orders$order_id))
  if (dup_orders > 0) {
    warning(sprintf("MAIN: ⚠️ Found %d duplicate order_ids", dup_orders))
    dt_orders <- dt_orders[!duplicated(order_id)]
    message(sprintf("MAIN: Removed duplicates, %d unique orders remaining", nrow(dt_orders)))
  } else {
    message("    ✅ Uniqueness validated: order_id is unique")
  }

  validate_elapsed <- as.numeric(Sys.time() - validate_start, units = "secs")
  message(sprintf("MAIN: ✅ Schema validation completed (%.2fs)", validate_elapsed))

  # ----------------------------------------------------------------------------
  # 2.5: Store Transformed Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 5/5 - Writing transformed data...")
  write_start <- Sys.time()

  # Table name per transformed_schemas.yaml pattern
  output_table <- "df_cbz_orders___transformed"

  # Drop existing table if present
  if (dbExistsTable(transformed_data, output_table)) {
    dbRemoveTable(transformed_data, output_table)
    message(sprintf("MAIN: 🗑️ Dropped existing table: %s", output_table))
  }

  # Convert back to data.frame for database write
  df_transformed <- as.data.frame(dt_orders)

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
  key_cols <- c("order_id", "customer_id", "order_date", "order_status",
                "order_total", "platform_id")
  key_cols_present <- intersect(key_cols, names(sample_data))
  print(sample_data[, key_cols_present])

  # Summary statistics
  if ("order_total" %in% names(df_transformed)) {
    total_revenue <- sum(df_transformed$order_total, na.rm = TRUE)
    message(sprintf("MAIN: 💰 Total order value: $%.2f", total_revenue))
  }

  if ("order_status" %in% names(df_transformed)) {
    status_dist <- table(df_transformed$order_status)
    message(sprintf("MAIN: 📊 Order status distribution: %s",
                    paste(sprintf("%s(%d)", names(status_dist), status_dist), collapse = ", ")))
  }

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Order Transformation completed successfully (%.2fs)", main_elapsed))

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

message("TEST: 🧪 Starting ETL Order Transformation verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_cbz_orders___transformed"

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
    required_cols <- c("order_id", "platform_id", "transformation_timestamp")
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

    # Test 5: Verify no duplicate order IDs
    dup_check <- dbGetQuery(transformed_data, sprintf("
      SELECT order_id, COUNT(*) as cnt
      FROM %s
      GROUP BY order_id
      HAVING COUNT(*) > 1
    ", output_table))
    if (nrow(dup_check) > 0) {
      stop(sprintf("TEST: Found %d duplicate order IDs", nrow(dup_check)))
    }
    message("TEST: ✅ No duplicate orders")

    # Test 6: Verify order_status standardization
    if ("order_status" %in% columns) {
      valid_statuses <- c("pending", "processing", "shipped", "delivered", "cancelled", "refunded")
      status_check <- dbGetQuery(transformed_data, sprintf("
        SELECT DISTINCT order_status
        FROM %s
        WHERE order_status IS NOT NULL
      ", output_table))

      invalid_statuses <- setdiff(status_check$order_status, valid_statuses)
      if (length(invalid_statuses) > 0) {
        warning(sprintf("TEST: ⚠️ Found non-standard statuses: %s",
                        paste(invalid_statuses, collapse = ", ")))
      } else {
        message("TEST: ✅ All order statuses are standardized")
      }
    }

    # Test 7: Sample data verification
    message("TEST: 📋 Sample verification (first 3 records):")
    sample_check <- dbGetQuery(transformed_data, sprintf("
      SELECT order_id, customer_id, order_date, order_status, order_total
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
  message("SUMMARIZE: ✅ ETL Order Transformation completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Order Transformation failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "orders",
  platform = "cbz",
  etl_phase = "2TR",
  output_schema = "transformed_schemas.yaml#orders_transformed",
  compliance = c("MP108", "MP104", "MP102", "DM_R028", "MP064", "DEV_R032", "MP103")
)

# Final summary reporting
message(strrep("=", 80))
message("SUMMARIZE: 📊 ORDER TRANSFORMATION SUMMARY")
message(strrep("=", 80))
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("📋 Output Schema: %s", final_metrics$output_schema))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message(strrep("=", 80))

message("SUMMARIZE: ✅ ETL Order Transformation (cbz_ETL_orders_2TR.R) completed")
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
