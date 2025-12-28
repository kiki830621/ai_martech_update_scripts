# cbz_ETL_sales_2TS.R - Cyberbiz Sales Data Standardization
# ==============================================================================
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Sales Phase 2TS (Standardization): Create unified schema for derivations
# Input: transformed_data.duckdb (df_cbz_sales___transformed)
# Output: transformed_data.duckdb (df_cbz_sales___standardized)
#
# Purpose: Bridge ETL output to Derivation input by standardizing column names
#   - order_date → payment_time (D01 expects payment_time)
#   - line_total → lineproduct_price (D01 expects lineproduct_price)
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "cbz_ETL_sales_2TS"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: ⚡ Starting Cyberbiz ETL Sales Standardization (2TS Phase)")
message(sprintf("INITIALIZE: 🕐 Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message("INITIALIZE: 📋 Script: cbz_ETL_sales_2TS.R v", script_version)
message("INITIALIZE: 📋 Purpose: Column standardization for D01 derivations")
message("INITIALIZE: 📋 Mapping: order_date→payment_time, line_total→lineproduct_price")
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
lib_elapsed <- as.numeric(Sys.time() - lib_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Libraries loaded successfully (%.2fs)", lib_elapsed))

# Source required functions
message("INITIALIZE: 📥 Loading database utilities...")
source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R")

# Establish database connections
message("INITIALIZE: 🔗 Connecting to databases...")
db_start <- Sys.time()
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)
db_elapsed <- as.numeric(Sys.time() - db_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Database connection established (%.2fs)", db_elapsed))
message(sprintf("INITIALIZE: 📖📝 Using: %s", db_path_list$transformed_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: ✅ Initialization completed successfully (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: 🚀 Starting ETL Sales Standardization...")
main_start_time <- Sys.time()

tryCatch({
  # ----------------------------------------------------------------------------
  # 2.1: Load Transformed Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 1/3 - Loading transformed data...")
  load_start <- Sys.time()

  input_table <- "df_cbz_sales___transformed"
  output_table <- "df_cbz_sales___standardized"

  # Check if source table exists
  if (!dbExistsTable(transformed_data, input_table)) {
    stop(sprintf("Required table %s not found. Run cbz_ETL_sales_2TR.R first.", input_table))
  }

  # Load transformed data
  message(sprintf("MAIN: Loading %s...", input_table))
  sales_transformed <- dbGetQuery(transformed_data, sprintf("SELECT * FROM %s", input_table))
  n_records <- nrow(sales_transformed)

  load_elapsed <- as.numeric(Sys.time() - load_start, units = "secs")
  message(sprintf("MAIN: ✅ Loaded %d records (%.2fs)", n_records, load_elapsed))

  if (n_records == 0) {
    stop("No transformed data found - cannot proceed with standardization")
  }

  # ----------------------------------------------------------------------------
  # 2.2: Standardize Column Names for D01 Derivations
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 2/3 - Standardizing column names...")
  transform_start <- Sys.time()

  dt_sales <- as.data.table(sales_transformed)

  # Column mapping for D01 compatibility
  # D01_03 expects: customer_id, payment_time, lineproduct_price, platform_code, platform_id

  # Map order_date → payment_time
  if ("order_date" %in% names(dt_sales)) {
    dt_sales[, payment_time := order_date]
    message("    ✅ Mapped: order_date → payment_time")
  } else if ("order_created_at" %in% names(dt_sales)) {
    dt_sales[, payment_time := order_created_at]
    message("    ✅ Mapped: order_created_at → payment_time")
  } else {
    stop("No date column found (order_date or order_created_at)")
  }

  # Map line_total → lineproduct_price
  if ("line_total" %in% names(dt_sales)) {
    dt_sales[, lineproduct_price := line_total]
    message("    ✅ Mapped: line_total → lineproduct_price")
  } else {
    stop("No line_total column found")
  }

  # Ensure platform_id exists (D01 expects it)
  if (!"platform_id" %in% names(dt_sales)) {
    if ("platform_code" %in% names(dt_sales)) {
      dt_sales[, platform_id := platform_code]
      message("    ✅ Created: platform_id from platform_code")
    } else {
      dt_sales[, platform_id := "cbz"]
      message("    ✅ Created: platform_id = 'cbz'")
    }
  }

  # Verify required columns for D01
  required_cols <- c("customer_id", "payment_time", "lineproduct_price", "platform_code", "platform_id")
  missing_cols <- setdiff(required_cols, names(dt_sales))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns for D01: %s", paste(missing_cols, collapse = ", ")))
  }
  message(sprintf("MAIN: ✅ All D01 required columns present: %s", paste(required_cols, collapse = ", ")))

  # Add standardization metadata
  dt_sales[, `:=`(
    standardization_timestamp = Sys.time(),
    standardization_version = script_version
  )]
  message("    ✅ Added standardization metadata")

  transform_elapsed <- as.numeric(Sys.time() - transform_start, units = "secs")
  message(sprintf("MAIN: ✅ Column standardization completed (%.2fs)", transform_elapsed))

  # ----------------------------------------------------------------------------
  # 2.3: Store Standardized Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 3/3 - Writing standardized data...")
  write_start <- Sys.time()

  # Drop existing table or view if present
  if (dbExistsTable(transformed_data, output_table)) {
    # Try to drop as view first (in case it's a view from debugging)
    tryCatch({
      dbExecute(transformed_data, sprintf("DROP VIEW IF EXISTS %s", output_table))
      message(sprintf("MAIN: 🗑️ Dropped existing view: %s", output_table))
    }, error = function(e) {
      # If not a view, drop as table
      dbRemoveTable(transformed_data, output_table)
      message(sprintf("MAIN: 🗑️ Dropped existing table: %s", output_table))
    })
  }

  # Convert back to data.frame for database write
  df_standardized <- as.data.frame(dt_sales)

  # Write to transformed_data
  dbWriteTable(transformed_data, output_table, df_standardized, overwrite = TRUE)

  # Verify write
  actual_count <- dbGetQuery(transformed_data,
    sprintf("SELECT COUNT(*) as count FROM %s", output_table))$count

  write_elapsed <- as.numeric(Sys.time() - write_start, units = "secs")
  message(sprintf("MAIN: ✅ Stored %d records in %s (%.2fs)",
                  actual_count, output_table, write_elapsed))

  # Display sample for verification
  message("MAIN: 📋 Sample of standardized data (D01 required columns):")
  sample_data <- head(df_standardized, 3)
  print(sample_data[, required_cols])

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Sales Standardization completed successfully (%.2fs)", main_elapsed))

}, error = function(e) {
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ❌ ERROR after %.2fs: %s", main_elapsed, e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: 🧪 Starting ETL Sales Standardization verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_cbz_sales___standardized"

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

    # Test 3: Verify D01 required columns exist
    columns <- dbListFields(transformed_data, output_table)
    required_cols <- c("customer_id", "payment_time", "lineproduct_price",
                       "platform_code", "platform_id")
    missing_cols <- setdiff(required_cols, columns)

    if (length(missing_cols) > 0) {
      stop(sprintf("TEST: Missing D01 required columns: %s",
                   paste(missing_cols, collapse = ", ")))
    }
    message("TEST: ✅ All D01 required columns present")

    # Test 4: Verify payment_time has valid data
    time_check <- dbGetQuery(transformed_data, sprintf("
      SELECT COUNT(*) as null_count
      FROM %s
      WHERE payment_time IS NULL
    ", output_table))
    if (time_check$null_count > 0) {
      warning(sprintf("TEST: ⚠️ Found %d NULL payment_time values", time_check$null_count))
    } else {
      message("TEST: ✅ No NULL payment_time values")
    }

    # Test 5: Verify lineproduct_price has valid data
    price_check <- dbGetQuery(transformed_data, sprintf("
      SELECT COUNT(*) as null_count
      FROM %s
      WHERE lineproduct_price IS NULL
    ", output_table))
    if (price_check$null_count > 0) {
      warning(sprintf("TEST: ⚠️ Found %d NULL lineproduct_price values", price_check$null_count))
    } else {
      message("TEST: ✅ No NULL lineproduct_price values")
    }

    # Test 6: Sample data verification
    message("TEST: 📋 Sample verification (D01 required columns):")
    sample_check <- dbGetQuery(transformed_data, sprintf("
      SELECT customer_id, payment_time, lineproduct_price, platform_code, platform_id
      FROM %s
      LIMIT 3
    ", output_table))
    print(sample_check)

    test_passed <- TRUE
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Standardization verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Standardization verification failed after %.2fs: %s",
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
  message("SUMMARIZE: ✅ ETL Sales Standardization completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Sales Standardization failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "sales",
  platform = "cbz",
  etl_phase = "2TS",
  purpose = "Column standardization for D01 derivations",
  compliance = c("MP064", "DM_R028", "DEV_R032", "MP103", "MP099")
)

# Final summary reporting
message(strrep("=", 80))
message("SUMMARIZE: 📊 SALES STANDARDIZATION SUMMARY")
message(strrep("=", 80))
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("🎯 Purpose: %s", final_metrics$purpose))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message(strrep("=", 80))

message("SUMMARIZE: ✅ ETL Sales Standardization (cbz_ETL_sales_2TS.R) completed")
message(sprintf("SUMMARIZE: 🏁 Final completion time: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))

summarize_elapsed <- as.numeric(Sys.time() - summarize_start_time, units = "secs")
message(sprintf("SUMMARIZE: ✅ Summary completed (%.2fs)", summarize_elapsed))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: 🧹 Starting cleanup...")
deinit_start_time <- Sys.time()

# Cleanup database connections
message("DEINITIALIZE: 🔌 Disconnecting databases...")
DBI::dbDisconnect(transformed_data)

# Log cleanup completion
deinit_elapsed <- as.numeric(Sys.time() - deinit_start_time, units = "secs")
message(sprintf("DEINITIALIZE: ✅ Cleanup completed (%.2fs)", deinit_elapsed))

# Following MP103: autodeinit() removes ALL variables - must be absolute last statement
message("DEINITIALIZE: 🧹 Executing autodeinit()...")
autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
