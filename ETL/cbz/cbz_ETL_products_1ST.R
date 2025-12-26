# cbz_ETL_products_1ST.R - Cyberbiz Product Data Staging
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
# ETL Products Phase 1ST (Staging): Encoding standardization and type conversion
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

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Product Staging (1ST Phase)")
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
  message("MAIN: 🚀 Starting ETL Product Staging - Cyberbiz product data standardization...")
  message("MAIN: 📊 Phase progress: Step 1/5 - Reading raw data...")

  # Read raw product data
  read_start <- Sys.time()
  df_raw <- dbReadTable(raw_data, "df_cbz_products___raw")
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

  convert_elapsed <- as.numeric(Sys.time() - convert_start, units = "secs")
  message(sprintf("MAIN: ✅ Column names cleaned (%.2fs)", convert_elapsed))

  # Step 3: Standardize data types and encoding
  message("MAIN: 📊 Phase progress: Step 3/5 - Type standardization and encoding...")
  standardize_start <- Sys.time()

  # Ensure character types for IDs and text fields
  if ("product_id" %in% names(dt_staging)) {
    dt_staging[, product_id := as.character(product_id)]
  }

  if ("product_name" %in% names(dt_staging)) {
    dt_staging[, product_name := as.character(product_name)]
    # Trim whitespace
    dt_staging[!is.na(product_name), product_name := trimws(product_name)]
  }

  if ("sku" %in% names(dt_staging)) {
    dt_staging[, sku := as.character(sku)]
    dt_staging[!is.na(sku), sku := trimws(sku)]
  }

  if ("category" %in% names(dt_staging)) {
    dt_staging[, category := as.character(category)]
  }

  if ("subcategory" %in% names(dt_staging)) {
    dt_staging[, subcategory := as.character(subcategory)]
  }

  if ("brand" %in% names(dt_staging)) {
    dt_staging[, brand := as.character(brand)]
  }

  # Ensure numeric types for prices
  if ("price" %in% names(dt_staging)) {
    dt_staging[, price := as.numeric(price)]
  }

  if ("cost" %in% names(dt_staging)) {
    dt_staging[, cost := as.numeric(cost)]
  }

  if ("current_price" %in% names(dt_staging)) {
    dt_staging[, current_price := as.numeric(current_price)]
  }

  # Ensure boolean for active status
  if ("is_active" %in% names(dt_staging)) {
    dt_staging[, is_active := as.logical(is_active)]
  }

  if ("in_stock" %in% names(dt_staging)) {
    dt_staging[, in_stock := as.logical(in_stock)]
  }

  # Ensure integer for quantities
  if ("stock_quantity" %in% names(dt_staging)) {
    dt_staging[, stock_quantity := as.integer(stock_quantity)]
  }

  standardize_elapsed <- as.numeric(Sys.time() - standardize_start, units = "secs")
  message(sprintf("MAIN: ✅ Type standardization completed (%.2fs)", standardize_elapsed))

  # Step 4: Create derived identification fields (allowed in 1ST per DM_R037)
  message("MAIN: 📊 Phase progress: Step 4/5 - Creating derived identification fields...")
  derive_start <- Sys.time()

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

  # Must have product_id
  if ("product_id" %in% names(dt_staging)) {
    dt_staging <- dt_staging[!is.na(product_id) & product_id != ""]
    removed_count <- initial_rows - nrow(dt_staging)
    if (removed_count > 0) {
      message(sprintf("    ⚠️ Removed %d rows with missing product_id", removed_count))
    }
  }

  # Must have product_name
  if ("product_name" %in% names(dt_staging)) {
    initial_rows <- nrow(dt_staging)
    dt_staging <- dt_staging[!is.na(product_name) & product_name != ""]
    removed_count <- initial_rows - nrow(dt_staging)
    if (removed_count > 0) {
      message(sprintf("    ⚠️ Removed %d rows with missing product_name", removed_count))
    }
  }

  # Flag invalid prices (negative values)
  if ("price" %in% names(dt_staging)) {
    invalid_prices <- dt_staging[!is.na(price) & price < 0, .N]
    if (invalid_prices > 0) {
      message(sprintf("    ⚠️ Found %d rows with negative price", invalid_prices))
      # Set to NA rather than remove
      dt_staging[!is.na(price) & price < 0, price := NA_real_]
    }
  }

  validate_elapsed <- as.numeric(Sys.time() - validate_start, units = "secs")
  message(sprintf("MAIN: ✅ Data validation completed: %d valid rows (%.2fs)",
                  nrow(dt_staging), validate_elapsed))

  # Write to staged database
  message("MAIN: 💾 Writing to staged_data database...")
  write_start <- Sys.time()

  df_staged <- as.data.frame(dt_staging)
  dbWriteTable(staged_data, "df_cbz_products___staged", df_staged, overwrite = TRUE)

  # Verify write
  actual_count <- dbGetQuery(staged_data, "SELECT COUNT(*) as count FROM df_cbz_products___staged")$count
  write_elapsed <- as.numeric(Sys.time() - write_start, units = "secs")

  message(sprintf("MAIN: ✅ Staged data written and verified: %d records (%.2fs)",
                  actual_count, write_elapsed))

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Product Staging completed successfully (%.2fs)", main_elapsed))

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
    message("TEST: 🧪 Starting ETL Product Staging verification...")

    # Test staged table existence
    table_name <- "df_cbz_products___staged"

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
        required_columns <- c("product_id", "staging_timestamp", "etl_phase")
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
        if ("price" %in% columns) {
          price_stats <- dbGetQuery(staged_data, paste0(
            "SELECT MIN(price) as min_p, MAX(price) as max_p, ",
            "AVG(price) as avg_p FROM ", table_name, " WHERE price IS NOT NULL"
          ))
          message(sprintf("TEST: 💰 Price stats: min=%.2f, max=%.2f, avg=%.2f",
                          price_stats$min_p, price_stats$max_p, price_stats$avg_p))
        }

        if ("category" %in% columns) {
          cat_dist <- dbGetQuery(staged_data, paste0(
            "SELECT category, COUNT(*) as count FROM ", table_name,
            " WHERE category IS NOT NULL GROUP BY category ORDER BY count DESC LIMIT 5"
          ))
          if (nrow(cat_dist) > 0) {
            message(sprintf("TEST: 📊 Top categories: %s",
                            paste(sprintf("%s(%d)", cat_dist$category, cat_dist$count),
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
  message("SUMMARIZE: ✅ ETL Product Staging completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Product Staging failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "products",
  platform = "cbz",
  etl_phase = "1ST",
  compliance = c("MP108", "MP104", "DM_R028", "MP064", "DM_R037", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 PRODUCT STAGING SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Product Staging (cbz_ETL_products_1ST.R) completed")
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
