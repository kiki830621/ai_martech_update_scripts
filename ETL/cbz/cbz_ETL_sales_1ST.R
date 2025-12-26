# cbz_ETL_sales_1ST.R - Cyberbiz Sales Data Staging
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
# ETL Sales Phase 1ST (Staging): Encoding standardization and type conversion
# NO business logic, NO JOINs - only data cleaning and standardization
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Sales Staging (1ST Phase)")
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
  message("MAIN: 🚀 Starting ETL Sales Staging - Cyberbiz sales data standardization...")
  message("MAIN: 📊 Phase progress: Step 1/5 - Reading raw data...")

  # Read raw sales data
  read_start <- Sys.time()
  df_raw <- dbReadTable(raw_data, "df_cbz_sales___raw")
  read_elapsed <- as.numeric(Sys.time() - read_start, units = "secs")
  message(sprintf("MAIN: ✅ Raw data loaded: %d rows × %d columns (%.2fs)",
                  nrow(df_raw), ncol(df_raw), read_elapsed))

  # Convert to data.table for efficient processing
  message("MAIN: 📊 Phase progress: Step 2/5 - Data type conversion and cleaning...")
  convert_start <- Sys.time()
  dt_staging <- as.data.table(df_raw)

  # Helper function: Clean column names (remove line_items. prefix if present)
  # Handle duplicates by prioritizing line_items.* columns (more specific)
  clean_column_names <- function(col_names) {
    cleaned <- gsub("^line_items\\.", "", col_names)
    cleaned <- gsub("\\.", "_", cleaned)
    cleaned <- tolower(cleaned)

    # Handle duplicates: if both 'created_at' and 'line_items.created_at' exist,
    # keep line_items version and rename the other to 'order_created_at'
    if (any(duplicated(cleaned))) {
      dup_names <- unique(cleaned[duplicated(cleaned)])
      message(sprintf("    ⚠️ Found %d duplicate column names after initial cleaning", length(dup_names)))

      for (dup in dup_names) {
        dup_indices <- which(cleaned == dup)
        # First occurrence (non-line_items version) gets prefix
        # Later occurrences (from line_items.*) keep the name
        original_names <- col_names[dup_indices]

        for (i in seq_along(dup_indices)) {
          idx <- dup_indices[i]
          if (!grepl("^line_items\\.", original_names[i])) {
            # This is the order-level field
            # Special case: 'id' should become 'legacy_order_id' not 'order_id' (which exists)
            if (dup == "id") {
              cleaned[idx] <- "legacy_order_id"
            } else {
              cleaned[idx] <- paste0("order_", dup)
            }
            message(sprintf("    ✅ Renamed '%s' -> '%s' (order-level field)",
                            original_names[i], cleaned[idx]))
          }
        }
      }

      # Double-check for any remaining duplicates
      if (any(duplicated(cleaned))) {
        stop("CRITICAL: Still have duplicate columns after renaming. Check column mapping logic.")
      }
    }

    return(cleaned)
  }

  # Clean column names
  names(dt_staging) <- clean_column_names(names(dt_staging))

  # Helper function: Parse CBZ date formats
  # CBZ dates might be in ISO format (YYYY-MM-DD) or timestamp format
  parse_cbz_date <- function(date_val) {
    if (is.null(date_val) || is.na(date_val)) return(NA_character_)

    # Try parsing as Date first
    parsed <- tryCatch({
      if (inherits(date_val, "POSIXct") || inherits(date_val, "POSIXlt")) {
        as.Date(date_val)
      } else if (inherits(date_val, "Date")) {
        date_val
      } else {
        # Try parsing string
        date_str <- as.character(date_val)
        # Handle ISO format YYYY-MM-DD
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

  # Parse dates (order_date, created_at, updated_at if present)
  # CBZ data uses created_at as the transaction date
  if ("created_at" %in% names(dt_staging)) {
    dt_staging[, created_at := sapply(created_at, parse_cbz_date)]
    message("    ✅ Standardized created_at to YYYY-MM-DD format")

    # Create order_date from created_at (required for 2TR transformation)
    dt_staging[, order_date := created_at]
    message("    ✅ Created order_date from created_at (transaction date)")
  }

  if ("updated_at" %in% names(dt_staging)) {
    dt_staging[, updated_at := sapply(updated_at, parse_cbz_date)]
    message("    ✅ Standardized updated_at to YYYY-MM-DD format")
  }

  # Ensure numeric types for quantities and amounts
  if ("quantity" %in% names(dt_staging)) {
    dt_staging[, quantity := as.integer(quantity)]
  }

  if ("unit_price" %in% names(dt_staging)) {
    dt_staging[, unit_price := as.numeric(unit_price)]
  }

  if ("total_amount" %in% names(dt_staging)) {
    dt_staging[, total_amount := as.numeric(total_amount)]
  }

  if ("price" %in% names(dt_staging)) {
    dt_staging[, price := as.numeric(price)]
  }

  # Ensure character types for IDs and codes
  if ("order_id" %in% names(dt_staging)) {
    dt_staging[, order_id := as.character(order_id)]
  }

  if ("sales_transaction_id" %in% names(dt_staging)) {
    dt_staging[, sales_transaction_id := as.character(sales_transaction_id)]
  }

  if ("product_id" %in% names(dt_staging)) {
    dt_staging[, product_id := as.character(product_id)]
  }

  if ("sku" %in% names(dt_staging)) {
    dt_staging[, sku := as.character(sku)]
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

  # Create unique line_item_id if not present (for data integrity)
  if (!"line_item_id" %in% names(dt_staging) && "sales_transaction_id" %in% names(dt_staging)) {
    dt_staging[, line_item_id := sales_transaction_id]
    message("    ✅ Created line_item_id from sales_transaction_id")
  }

  # Calculate line_total if components exist but total doesn't
  if ("quantity" %in% names(dt_staging) && "unit_price" %in% names(dt_staging)) {
    if (!"line_total" %in% names(dt_staging)) {
      dt_staging[, line_total := quantity * unit_price]
      message("    ✅ Calculated line_total from quantity × unit_price")
    }
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

  # Must have product information
  if ("product_id" %in% names(dt_staging)) {
    initial_rows <- nrow(dt_staging)
    dt_staging <- dt_staging[!is.na(product_id) & product_id != ""]
    removed_count <- initial_rows - nrow(dt_staging)
    if (removed_count > 0) {
      message(sprintf("    ⚠️ Removed %d rows with missing product_id", removed_count))
    }
  }

  # Flag invalid quantities (negative or zero)
  if ("quantity" %in% names(dt_staging)) {
    invalid_qty <- dt_staging[!is.na(quantity) & quantity <= 0, .N]
    if (invalid_qty > 0) {
      message(sprintf("    ⚠️ Found %d rows with invalid quantity (<=0)", invalid_qty))
      # Set to NA rather than remove
      dt_staging[!is.na(quantity) & quantity <= 0, quantity := NA_integer_]
    }
  }

  validate_elapsed <- as.numeric(Sys.time() - validate_start, units = "secs")
  message(sprintf("MAIN: ✅ Data validation completed: %d valid rows (%.2fs)",
                  nrow(dt_staging), validate_elapsed))

  # Write to staged database
  message("MAIN: 💾 Writing to staged_data database...")
  write_start <- Sys.time()

  df_staged <- as.data.frame(dt_staging)
  dbWriteTable(staged_data, "df_cbz_sales___staged", df_staged, overwrite = TRUE)

  # Verify write
  actual_count <- dbGetQuery(staged_data, "SELECT COUNT(*) as count FROM df_cbz_sales___staged")$count
  write_elapsed <- as.numeric(Sys.time() - write_start, units = "secs")

  message(sprintf("MAIN: ✅ Staged data written and verified: %d records (%.2fs)",
                  actual_count, write_elapsed))

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Sales Staging completed successfully (%.2fs)", main_elapsed))

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
    message("TEST: 🧪 Starting ETL Sales Staging verification...")

    # Test staged table existence
    table_name <- "df_cbz_sales___staged"

    if (table_name %in% dbListTables(staged_data)) {
      staged_count <- dbGetQuery(staged_data,
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
          date_check <- dbGetQuery(staged_data, paste0(
            "SELECT COUNT(*) as valid_dates FROM ", table_name,
            " WHERE order_date IS NOT NULL AND order_date != ''"
          ))
          message(sprintf("TEST: 📅 Valid order dates: %d records", date_check$valid_dates))
        }

        if ("quantity" %in% columns) {
          qty_stats <- dbGetQuery(staged_data, paste0(
            "SELECT MIN(quantity) as min_qty, MAX(quantity) as max_qty, ",
            "AVG(quantity) as avg_qty, ",
            "COUNT(CASE WHEN quantity IS NULL THEN 1 END) as null_qty ",
            "FROM ", table_name
          ))
          message(sprintf("TEST: 📊 Quantity stats: min=%s, max=%s, avg=%.2f, nulls=%d",
                          qty_stats$min_qty, qty_stats$max_qty,
                          ifelse(is.na(qty_stats$avg_qty), 0, qty_stats$avg_qty),
                          qty_stats$null_qty))
        }

        if ("order_year" %in% columns) {
          year_dist <- dbGetQuery(staged_data, paste0(
            "SELECT order_year, COUNT(*) as count FROM ", table_name,
            " WHERE order_year IS NOT NULL GROUP BY order_year ORDER BY order_year"
          ))
          if (nrow(year_dist) > 0) {
            message(sprintf("TEST: 📆 Year distribution: %s",
                            paste(sprintf("%d(%d)", year_dist$order_year, year_dist$count),
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
  message("SUMMARIZE: ✅ ETL Sales Staging completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Sales Staging failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "sales",
  platform = "cbz",
  etl_phase = "1ST",
  compliance = c("MP108", "MP104", "DM_R028", "MP064", "DM_R037", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 SALES STAGING SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Sales Staging (cbz_ETL_sales_1ST.R) completed")
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
