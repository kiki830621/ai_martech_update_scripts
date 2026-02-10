# cbz_ETL_orders_1ST.R - Cyberbiz Order Data Staging
# ==============================================================================
# Following MP108: BASE ETL 0IM→1ST→2TR Pipeline
# Following MP104: ETL Data Flow Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R037: 1ST Phase Transformation Constraints
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Orders Phase 1ST (Staging): Encoding standardization and type conversion
# NO business logic, NO JOINs - only data cleaning and standardization
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
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
script_start_time <- Sys.time()

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Order Staging (1ST Phase)")
message(sprintf("INITIALIZE: 🕐 Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message("INITIALIZE: 📋 Compliance: MP108 (0IM→1ST→2TR) + DM_R037 (1ST Constraints)")

# Initialize using unified autoinit system
autoinit()

# Load required libraries with progress feedback
message("INITIALIZE: 📦 Loading required libraries...")
lib_start <- Sys.time()
library(dplyr)
library(data.table)
library(lubridate)
lib_elapsed <- as.numeric(Sys.time() - lib_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Libraries loaded successfully (%.2fs)", lib_elapsed))

# Establish database connections
message("INITIALIZE: 🔗 Connecting to databases...")
db_start <- Sys.time()
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)
db_elapsed <- as.numeric(Sys.time() - db_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Database connections established (%.2fs)", db_elapsed))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: ✅ Initialization completed successfully (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

main_start_time <- Sys.time()
tryCatch({
  message("MAIN: 🚀 Starting ETL Order Staging - Cyberbiz order data standardization...")
  message("MAIN: 📊 Phase progress: Step 1/5 - Reading raw data...")

  # Read raw order data
  read_start <- Sys.time()
  df_raw <- dbReadTable(raw_data, "df_cbz_orders___raw")
  read_elapsed <- as.numeric(Sys.time() - read_start, units = "secs")
  message(sprintf("MAIN: ✅ Raw data loaded: %d rows × %d columns (%.2fs)",
                  nrow(df_raw), ncol(df_raw), read_elapsed))

  # Convert to data.table for efficient processing
  message("MAIN: 📊 Phase progress: Step 2/5 - Data type conversion and cleaning...")
  convert_start <- Sys.time()
  dt_staging <- as.data.table(df_raw)

  # Helper function: Clean column names
  clean_column_names <- function(col_names) {
    cleaned <- gsub("\\.", "_", col_names)
    cleaned <- tolower(cleaned)
    return(cleaned)
  }

  # Clean column names
  names(dt_staging) <- clean_column_names(names(dt_staging))

  # Helper function: Parse CBZ date formats
  parse_cbz_date <- function(date_val) {
    if (is.null(date_val) || is.na(date_val)) return(NA_character_)

    parsed <- tryCatch({
      if (inherits(date_val, "POSIXct") || inherits(date_val, "POSIXlt")) {
        as.Date(date_val)
      } else if (inherits(date_val, "Date")) {
        date_val
      } else {
        date_str <- as.character(date_val)
        if (grepl("^\\d{4}-\\d{2}-\\d{2}", date_str)) {
          as.Date(substr(date_str, 1, 10))
        } else {
          as.Date(date_str)
        }
      }
    }, error = function(e) NA)

    if (is.na(parsed)) return(NA_character_)
    return(format(parsed, "%Y-%m-%d"))
  }

  convert_elapsed <- as.numeric(Sys.time() - convert_start, units = "secs")
  message(sprintf("MAIN: ✅ Column names cleaned (%.2fs)", convert_elapsed))

  # Step 3: Standardize data types and encoding
  message("MAIN: 📊 Phase progress: Step 3/5 - Type standardization and encoding...")
  standardize_start <- Sys.time()

  # Parse dates
  if ("order_date" %in% names(dt_staging)) {
    dt_staging[, order_date := sapply(order_date, parse_cbz_date)]
    message("    ✅ Standardized order_date to YYYY-MM-DD format")
  }

  if ("created_at" %in% names(dt_staging)) {
    dt_staging[, created_at := sapply(created_at, parse_cbz_date)]
    message("    ✅ Standardized created_at to YYYY-MM-DD format")
  }

  if ("updated_at" %in% names(dt_staging)) {
    dt_staging[, updated_at := sapply(updated_at, parse_cbz_date)]
    message("    ✅ Standardized updated_at to YYYY-MM-DD format")
  }

  if ("shipped_date" %in% names(dt_staging)) {
    dt_staging[, shipped_date := sapply(shipped_date, parse_cbz_date)]
    message("    ✅ Standardized shipped_date to YYYY-MM-DD format")
  }

  # Ensure numeric types for amounts
  if ("total_amount" %in% names(dt_staging)) {
    dt_staging[, total_amount := as.numeric(total_amount)]
  }

  if ("shipping_amount" %in% names(dt_staging)) {
    dt_staging[, shipping_amount := as.numeric(shipping_amount)]
  }

  if ("tax_amount" %in% names(dt_staging)) {
    dt_staging[, tax_amount := as.numeric(tax_amount)]
  }

  if ("discount_amount" %in% names(dt_staging)) {
    dt_staging[, discount_amount := as.numeric(discount_amount)]
  }

  # Ensure character types for IDs and codes
  if ("order_id" %in% names(dt_staging)) {
    dt_staging[, order_id := as.character(order_id)]
  }

  if ("customer_id" %in% names(dt_staging)) {
    dt_staging[, customer_id := as.character(customer_id)]
  }

  if ("order_status" %in% names(dt_staging)) {
    dt_staging[, order_status := as.character(order_status)]
  }

  if ("payment_method" %in% names(dt_staging)) {
    dt_staging[, payment_method := as.character(payment_method)]
  }

  if ("shipping_method" %in% names(dt_staging)) {
    dt_staging[, shipping_method := as.character(shipping_method)]
  }

  standardize_elapsed <- as.numeric(Sys.time() - standardize_start, units = "secs")
  message(sprintf("MAIN: ✅ Type standardization completed (%.2fs)", standardize_elapsed))

  # Step 4: Create derived identification fields (allowed in 1ST per DM_R037)
  message("MAIN: 📊 Phase progress: Step 4/5 - Creating derived identification fields...")
  derive_start <- Sys.time()

  # Extract year and month from order_date for partitioning
  if ("order_date" %in% names(dt_staging)) {
    dt_staging[!is.na(order_date), `:=`(
      order_year = year(as.Date(order_date)),
      order_month = month(as.Date(order_date)),
      order_quarter = quarter(as.Date(order_date))
    )]
    message("    ✅ Created temporal partitioning fields: order_year, order_month, order_quarter")
  }

  # Add staging metadata
  dt_staging[, `:=`(
    staging_timestamp = Sys.time(),
    etl_phase = "1ST"
  )]
  message("    ✅ Added staging metadata")

  derive_elapsed <- as.numeric(Sys.time() - derive_start, units = "secs")
  message(sprintf("MAIN: ✅ Derived fields created (%.2fs)", derive_elapsed))

  # Step 5: Data validation and quality checks
  message("MAIN: 📊 Phase progress: Step 5/5 - Data validation...")
  validate_start <- Sys.time()

  # Remove rows with critical missing values
  initial_rows <- nrow(dt_staging)

  # Must have order_id
  if ("order_id" %in% names(dt_staging)) {
    dt_staging <- dt_staging[!is.na(order_id) & order_id != ""]
    removed_count <- initial_rows - nrow(dt_staging)
    if (removed_count > 0) {
      message(sprintf("    ⚠️ Removed %d rows with missing order_id", removed_count))
    }
  }

  # Must have order_date
  if ("order_date" %in% names(dt_staging)) {
    initial_rows <- nrow(dt_staging)
    dt_staging <- dt_staging[!is.na(order_date) & order_date != ""]
    removed_count <- initial_rows - nrow(dt_staging)
    if (removed_count > 0) {
      message(sprintf("    ⚠️ Removed %d rows with missing order_date", removed_count))
    }
  }

  # Flag invalid amounts (negative total_amount)
  if ("total_amount" %in% names(dt_staging)) {
    invalid_amounts <- dt_staging[!is.na(total_amount) & total_amount < 0, .N]
    if (invalid_amounts > 0) {
      message(sprintf("    ⚠️ Found %d rows with negative total_amount", invalid_amounts))
      # Set to NA rather than remove
      dt_staging[!is.na(total_amount) & total_amount < 0, total_amount := NA_real_]
    }
  }

  validate_elapsed <- as.numeric(Sys.time() - validate_start, units = "secs")
  message(sprintf("MAIN: ✅ Data validation completed: %d valid rows (%.2fs)",
                  nrow(dt_staging), validate_elapsed))

  # Write to staged database
  message("MAIN: 💾 Writing to staged_data database...")
  write_start <- Sys.time()

  df_staged <- as.data.frame(dt_staging)
  dbWriteTable(staged_data, "df_cbz_orders___staged", df_staged, overwrite = TRUE)

  # Verify write
  actual_count <- sql_read(staged_data, "SELECT COUNT(*) as count FROM df_cbz_orders___staged")$count
  write_elapsed <- as.numeric(Sys.time() - write_start, units = "secs")

  message(sprintf("MAIN: ✅ Staged data written and verified: %d records (%.2fs)",
                  actual_count, write_elapsed))

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Order Staging completed successfully (%.2fs)", main_elapsed))

}, error = function(e) {
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ❌ ERROR after %.2fs: %s", main_elapsed, e$message))
  message(sprintf("MAIN: 📍 Error traceback: %s", paste(sys.calls(), collapse = " -> ")))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    message("TEST: 🧪 Starting ETL Order Staging verification...")

    # Test staged table existence
    table_name <- "df_cbz_orders___staged"

    if (table_name %in% dbListTables(staged_data)) {
      staged_count <- sql_read(staged_data,
        paste0("SELECT COUNT(*) as count FROM ", table_name))$count

      message(sprintf("TEST: ✅ Staged table verification: %d records", staged_count))

      if (staged_count > 0) {
        # Verify column structure
        columns <- dbListFields(staged_data, table_name)
        message(sprintf("TEST: 📝 Staged table structure (%d columns): %s",
                        length(columns), paste(head(columns, 10), collapse = ", ")))

        # Verify required columns
        required_columns <- c("order_id", "staging_timestamp", "etl_phase")
        missing_columns <- setdiff(required_columns, columns)
        if (length(missing_columns) > 0) {
          message(sprintf("TEST: ⚠️ Missing required columns: %s",
                          paste(missing_columns, collapse = ", ")))
          test_passed <- FALSE
        } else {
          message("TEST: ✅ All required columns present")
          test_passed <- TRUE
        }

        # Data quality checks
        if ("order_date" %in% columns) {
          date_check <- sql_read(staged_data, paste0(
            "SELECT COUNT(*) as valid_dates FROM ", table_name,
            " WHERE order_date IS NOT NULL AND order_date != ''"
          ))
          message(sprintf("TEST: 📅 Valid order dates: %d records", date_check$valid_dates))
        }

        if ("total_amount" %in% columns) {
          amount_stats <- sql_read(staged_data, paste0(
            "SELECT MIN(total_amount) as min_amt, MAX(total_amount) as max_amt, ",
            "AVG(total_amount) as avg_amt FROM ", table_name, " WHERE total_amount IS NOT NULL"
          ))
          message(sprintf("TEST: 💰 Amount stats: min=%.2f, max=%.2f, avg=%.2f",
                          amount_stats$min_amt, amount_stats$max_amt, amount_stats$avg_amt))
        }

        if ("order_status" %in% columns) {
          status_dist <- sql_read(staged_data, paste0(
            "SELECT order_status, COUNT(*) as count FROM ", table_name,
            " WHERE order_status IS NOT NULL GROUP BY order_status ORDER BY count DESC LIMIT 5"
          ))
          if (nrow(status_dist) > 0) {
            message(sprintf("TEST: 📊 Top order statuses: %s",
                            paste(sprintf("%s(%d)", status_dist$order_status, status_dist$count),
                                  collapse = ", ")))
          }
        }

      } else {
        test_passed <- FALSE
        message("TEST: ⚠️ Staged table is empty")
      }

    } else {
      test_passed <- FALSE
      message(sprintf("TEST: ❌ Staged table '%s' not found", table_name))
    }

    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Staging verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Staging verification failed after %.2fs: %s",
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
  message("SUMMARIZE: ✅ ETL Order Staging completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Order Staging failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "orders",
  platform = "cbz",
  etl_phase = "1ST",
  compliance = c("MP108", "MP104", "DM_R028", "MP064", "DM_R037", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 ORDER STAGING SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Order Staging (cbz_ETL_orders_1ST.R) completed")
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
DBI::dbDisconnect(raw_data)
DBI::dbDisconnect(staged_data)

# Log cleanup completion
deinit_elapsed <- as.numeric(Sys.time() - deinit_start_time, units = "secs")
message(sprintf("DEINITIALIZE: ✅ Cleanup completed (%.2fs)", deinit_elapsed))

# Following MP103: autodeinit() removes ALL variables - must be absolute last statement
message("DEINITIALIZE: 🧹 Executing autodeinit()...")
autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
