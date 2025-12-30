# cbz_ETL_customers_2TR.R - Cyberbiz Customer Data Transformation
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
# ETL Customers Phase 2TR (Transform): Cross-platform standardization
# Input: staged_data.duckdb (df_cbz_customers___staged)
# Output: transformed_data.duckdb (df_cbz_customers___transformed)
# Schema: Conforms to transformed_schemas.yaml#customers_transformed
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "cbz_ETL_customers_2TR"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: ⚡ Starting Cyberbiz ETL Customer Transformation (2TR Phase)")
message(sprintf("INITIALIZE: 🕐 Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message("INITIALIZE: 📋 Script: cbz_ETL_customers_2TR.R v", script_version)
message("INITIALIZE: 📋 Compliance: MP108 (0IM→1ST→2TR) + MP102 (Output Standardization)")
message("INITIALIZE: 📋 Output Schema: transformed_schemas.yaml#customers_transformed")
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

message("MAIN: 🚀 Starting ETL Customer Transformation - Cross-platform standardization...")
main_start_time <- Sys.time()

tryCatch({
  # ----------------------------------------------------------------------------
  # 2.1: Load Staged Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 1/5 - Loading staged data...")
  load_start <- Sys.time()

  # Check if staged table exists
  staged_tables <- dbListTables(staged_data)
  staged_table <- "df_cbz_customers___staged"

  if (!staged_table %in% staged_tables) {
    stop(sprintf("Required table %s not found. Run cbz_ETL_customers_1ST.R first.", staged_table))
  }

  # Load staged data
  message(sprintf("MAIN: Loading %s...", staged_table))
  customers_staged <- dbGetQuery(staged_data, sprintf("SELECT * FROM %s", staged_table))
  n_staged <- nrow(customers_staged)

  load_elapsed <- as.numeric(Sys.time() - load_start, units = "secs")
  message(sprintf("MAIN: ✅ Loaded %d staged records (%.2fs)", n_staged, load_elapsed))

  if (n_staged == 0) {
    stop("No staged data found - cannot proceed with transformation")
  }

  # ----------------------------------------------------------------------------
  # 2.2: Convert to data.table for Efficient Processing
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 2/5 - Data preparation...")
  dt_customers <- as.data.table(customers_staged)
  message(sprintf("MAIN: ✅ Converted to data.table: %d rows × %d columns",
                  nrow(dt_customers), ncol(dt_customers)))

  # ----------------------------------------------------------------------------
  # 2.3: Standardize to Cross-Platform Schema (transformed_schemas.yaml)
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 3/5 - Schema standardization...")
  transform_start <- Sys.time()

  # Parse registration_date to DATE type (per transformed_schemas.yaml)
  if ("registration_date" %in% names(dt_customers)) {
    dt_customers[, registration_date := as.Date(registration_date)]
    message("    ✅ Converted registration_date to DATE type")
  }

  # Standardize customer_email to lowercase (per transformed_schemas.yaml transformation rule)
  if ("customer_email" %in% names(dt_customers)) {
    dt_customers[!is.na(customer_email), customer_email := tolower(trimws(customer_email))]
    message("    ✅ Standardized customer_email to lowercase")
  }

  # Add transformation metadata (required per transformed_schemas.yaml)
  dt_customers[, `:=`(
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

  # Check required fields per transformed_schemas.yaml#customers_transformed
  required_fields <- c(
    "customer_id", "platform_id", "transformation_timestamp"
  )

  missing_fields <- setdiff(required_fields, names(dt_customers))
  if (length(missing_fields) > 0) {
    stop(sprintf("MAIN: ❌ Missing required fields: %s",
                 paste(missing_fields, collapse = ", ")))
  }

  present_required <- intersect(required_fields, names(dt_customers))
  message(sprintf("MAIN: ✅ All required fields present: %s",
                  paste(present_required, collapse = ", ")))

  # Check uniqueness of customer_id (required per schema)
  dup_customers <- sum(duplicated(dt_customers$customer_id))
  if (dup_customers > 0) {
    warning(sprintf("MAIN: ⚠️ Found %d duplicate customer_ids", dup_customers))
    # Keep first occurrence, remove duplicates
    dt_customers <- dt_customers[!duplicated(customer_id)]
    message(sprintf("MAIN: Removed duplicates, %d unique customers remaining", nrow(dt_customers)))
  } else {
    message("    ✅ Uniqueness validated: customer_id is unique")
  }

  validate_elapsed <- as.numeric(Sys.time() - validate_start, units = "secs")
  message(sprintf("MAIN: ✅ Schema validation completed (%.2fs)", validate_elapsed))

  # ----------------------------------------------------------------------------
  # 2.5: Store Transformed Data
  # ----------------------------------------------------------------------------
  message("MAIN: 📊 Phase progress: Step 5/5 - Writing transformed data...")
  write_start <- Sys.time()

  # Table name per transformed_schemas.yaml pattern
  output_table <- "df_cbz_customers___transformed"

  # Drop existing table if present
  if (dbExistsTable(transformed_data, output_table)) {
    dbRemoveTable(transformed_data, output_table)
    message(sprintf("MAIN: 🗑️ Dropped existing table: %s", output_table))
  }

  # Convert back to data.frame for database write
  df_transformed <- as.data.frame(dt_customers)

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
  key_cols <- c("customer_id", "customer_name", "customer_email",
                "registration_date", "platform_id")
  key_cols_present <- intersect(key_cols, names(sample_data))
  print(sample_data[, key_cols_present])

  # Summary statistics
  unique_emails <- length(unique(df_transformed$customer_email[!is.na(df_transformed$customer_email)]))
  message(sprintf("MAIN: 📧 Unique customer emails: %d", unique_emails))

  if ("registration_year" %in% names(df_transformed)) {
    year_dist <- table(df_transformed$registration_year)
    message(sprintf("MAIN: 📅 Registration year distribution: %s",
                    paste(sprintf("%s(%d)", names(year_dist), year_dist), collapse = ", ")))
  }

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Customer Transformation completed successfully (%.2fs)", main_elapsed))

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

message("TEST: 🧪 Starting ETL Customer Transformation verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_cbz_customers___transformed"

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
    required_cols <- c("customer_id", "platform_id", "transformation_timestamp")
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

    # Test 5: Verify no duplicate customer IDs
    dup_check <- dbGetQuery(transformed_data, sprintf("
      SELECT customer_id, COUNT(*) as cnt
      FROM %s
      GROUP BY customer_id
      HAVING COUNT(*) > 1
    ", output_table))
    if (nrow(dup_check) > 0) {
      stop(sprintf("TEST: Found %d duplicate customer IDs", nrow(dup_check)))
    }
    message("TEST: ✅ No duplicate customers")

    # Test 6: Verify email standardization
    if ("customer_email" %in% columns) {
      uppercase_check <- dbGetQuery(transformed_data, sprintf("
        SELECT COUNT(*) as cnt
        FROM %s
        WHERE customer_email IS NOT NULL
        AND customer_email != LOWER(customer_email)
      ", output_table))
      if (uppercase_check$cnt > 0) {
        warning(sprintf("TEST: ⚠️ Found %d emails not in lowercase", uppercase_check$cnt))
      } else {
        message("TEST: ✅ All emails standardized to lowercase")
      }
    }

    # Test 7: Sample data verification
    message("TEST: 📋 Sample verification (first 3 records):")
    sample_check <- dbGetQuery(transformed_data, sprintf("
      SELECT customer_id, customer_name, customer_email, registration_date
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
  message("SUMMARIZE: ✅ ETL Customer Transformation completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Customer Transformation failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "customers",
  platform = "cbz",
  etl_phase = "2TR",
  output_schema = "transformed_schemas.yaml#customers_transformed",
  compliance = c("MP108", "MP104", "MP102", "DM_R028", "MP064", "DEV_R032", "MP103")
)

# Final summary reporting
message(strrep("=", 80))
message("SUMMARIZE: 📊 CUSTOMER TRANSFORMATION SUMMARY")
message(strrep("=", 80))
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("📋 Output Schema: %s", final_metrics$output_schema))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message(strrep("=", 80))

message("SUMMARIZE: ✅ ETL Customer Transformation (cbz_ETL_customers_2TR.R) completed")
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
