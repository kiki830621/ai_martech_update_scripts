# cbz_ETL_customers_1ST.R - Cyberbiz Customer Data Staging
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
# ETL Customers Phase 1ST (Staging): Encoding standardization and type conversion
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

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Customer Staging (1ST Phase)")
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
  message("MAIN: 🚀 Starting ETL Customer Staging - Cyberbiz customer data standardization...")
  message("MAIN: 📊 Phase progress: Step 1/5 - Reading raw data...")

  # Read raw customer data
  read_start <- Sys.time()
  df_raw <- dbReadTable(raw_data, "df_cbz_customers___raw")
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

  # Map CBZ API column names to standard schema
  # CBZ API uses 'id' but we need 'customer_id' for consistency
  if ("id" %in% names(dt_staging) && !"customer_id" %in% names(dt_staging)) {
    dt_staging[, customer_id := id]
    message("    ✅ Mapped 'id' to 'customer_id'")
  }

  # Map 'name' to 'customer_name' if needed
  if ("name" %in% names(dt_staging) && !"customer_name" %in% names(dt_staging)) {
    dt_staging[, customer_name := name]
    message("    ✅ Mapped 'name' to 'customer_name'")
  }

  # Map 'email' to 'customer_email' if needed
  if ("email" %in% names(dt_staging) && !"customer_email" %in% names(dt_staging)) {
    dt_staging[, customer_email := email]
    message("    ✅ Mapped 'email' to 'customer_email'")
  }

  # Map 'mobile' to 'phone_number' if needed
  if ("mobile" %in% names(dt_staging) && !"phone_number" %in% names(dt_staging)) {
    dt_staging[, phone_number := mobile]
    message("    ✅ Mapped 'mobile' to 'phone_number'")
  }

  # Map 'birthday' to 'birth_date' if needed
  if ("birthday" %in% names(dt_staging) && !"birth_date" %in% names(dt_staging)) {
    dt_staging[, birth_date := birthday]
    message("    ✅ Mapped 'birthday' to 'birth_date'")
  }

  # No 'registration_date' in CBZ API - use 'created_at' instead
  if ("created_at" %in% names(dt_staging) && !"registration_date" %in% names(dt_staging)) {
    dt_staging[, registration_date := created_at]
    message("    ✅ Mapped 'created_at' to 'registration_date'")
  }

  convert_elapsed <- as.numeric(Sys.time() - convert_start, units = "secs")
  message(sprintf("MAIN: ✅ Column names cleaned and mapped (%.2fs)", convert_elapsed))

  # Step 3: Standardize data types and encoding
  message("MAIN: 📊 Phase progress: Step 3/5 - Type standardization and encoding...")
  standardize_start <- Sys.time()

  # Parse dates (registration_date, birth_date if present)
  if ("registration_date" %in% names(dt_staging)) {
    dt_staging[, registration_date := sapply(registration_date, parse_cbz_date)]
    message("    ✅ Standardized registration_date to YYYY-MM-DD format")
  }

  if ("birth_date" %in% names(dt_staging)) {
    dt_staging[, birth_date := sapply(birth_date, parse_cbz_date)]
    message("    ✅ Standardized birth_date to YYYY-MM-DD format")
  }

  if ("created_at" %in% names(dt_staging)) {
    dt_staging[, created_at := sapply(created_at, parse_cbz_date)]
    message("    ✅ Standardized created_at to YYYY-MM-DD format")
  }

  if ("updated_at" %in% names(dt_staging)) {
    dt_staging[, updated_at := sapply(updated_at, parse_cbz_date)]
    message("    ✅ Standardized updated_at to YYYY-MM-DD format")
  }

  # Ensure character types for IDs and text fields
  if ("customer_id" %in% names(dt_staging)) {
    dt_staging[, customer_id := as.character(customer_id)]
  }

  if ("customer_name" %in% names(dt_staging)) {
    dt_staging[, customer_name := as.character(customer_name)]
    # Trim whitespace
    dt_staging[!is.na(customer_name), customer_name := trimws(customer_name)]
  }

  if ("customer_email" %in% names(dt_staging)) {
    dt_staging[, customer_email := as.character(customer_email)]
    # Standardize to lowercase and trim
    dt_staging[!is.na(customer_email), customer_email := tolower(trimws(customer_email))]
  }

  if ("phone_number" %in% names(dt_staging)) {
    dt_staging[, phone_number := as.character(phone_number)]
    # Remove whitespace from phone numbers
    dt_staging[!is.na(phone_number), phone_number := gsub("\\s+", "", phone_number)]
  }

  if ("gender" %in% names(dt_staging)) {
    dt_staging[, gender := as.character(gender)]
  }

  if ("city" %in% names(dt_staging)) {
    dt_staging[, city := as.character(city)]
  }

  if ("postal_code" %in% names(dt_staging)) {
    dt_staging[, postal_code := as.character(postal_code)]
  }

  if ("address" %in% names(dt_staging)) {
    dt_staging[, address := as.character(address)]
  }

  standardize_elapsed <- as.numeric(Sys.time() - standardize_start, units = "secs")
  message(sprintf("MAIN: ✅ Type standardization completed (%.2fs)", standardize_elapsed))

  # Step 4: Create derived identification fields (allowed in 1ST per DM_R037)
  message("MAIN: 📊 Phase progress: Step 4/5 - Creating derived identification fields...")
  derive_start <- Sys.time()

  # Extract year and month from registration_date for analysis
  if ("registration_date" %in% names(dt_staging)) {
    dt_staging[!is.na(registration_date), `:=`(
      registration_year = year(as.Date(registration_date)),
      registration_month = month(as.Date(registration_date))
    )]
    message("    ✅ Created temporal fields: registration_year, registration_month")
  }

  # Create email domain for segmentation (allowed in 1ST as identification)
  if ("customer_email" %in% names(dt_staging)) {
    dt_staging[!is.na(customer_email) & customer_email != "",
               email_domain := sub(".*@", "", customer_email)]
    message("    ✅ Extracted email_domain for identification")
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

  # Must have customer_id
  if ("customer_id" %in% names(dt_staging)) {
    dt_staging <- dt_staging[!is.na(customer_id) & customer_id != ""]
    removed_count <- initial_rows - nrow(dt_staging)
    if (removed_count > 0) {
      message(sprintf("    ⚠️ Removed %d rows with missing customer_id", removed_count))
    }
  }

  # Flag invalid emails (basic validation)
  if ("customer_email" %in% names(dt_staging)) {
    invalid_emails <- dt_staging[
      !is.na(customer_email) &
      customer_email != "" &
      !grepl("^[^@]+@[^@]+\\.[^@]+$", customer_email),
      .N
    ]
    if (invalid_emails > 0) {
      message(sprintf("    ⚠️ Found %d rows with potentially invalid email format", invalid_emails))
      # Set invalid emails to NA rather than remove
      dt_staging[
        !is.na(customer_email) &
        customer_email != "" &
        !grepl("^[^@]+@[^@]+\\.[^@]+$", customer_email),
        customer_email := NA_character_
      ]
    }
  }

  validate_elapsed <- as.numeric(Sys.time() - validate_start, units = "secs")
  message(sprintf("MAIN: ✅ Data validation completed: %d valid rows (%.2fs)",
                  nrow(dt_staging), validate_elapsed))

  # Write to staged database
  message("MAIN: 💾 Writing to staged_data database...")
  write_start <- Sys.time()

  df_staged <- as.data.frame(dt_staging)
  dbWriteTable(staged_data, "df_cbz_customers___staged", df_staged, overwrite = TRUE)

  # Verify write
  actual_count <- dbGetQuery(staged_data, "SELECT COUNT(*) as count FROM df_cbz_customers___staged")$count
  write_elapsed <- as.numeric(Sys.time() - write_start, units = "secs")

  message(sprintf("MAIN: ✅ Staged data written and verified: %d records (%.2fs)",
                  actual_count, write_elapsed))

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Customer Staging completed successfully (%.2fs)", main_elapsed))

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
    message("TEST: 🧪 Starting ETL Customer Staging verification...")

    # Test staged table existence
    table_name <- "df_cbz_customers___staged"

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
        required_columns <- c("customer_id", "staging_timestamp", "etl_phase")
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
        if ("customer_email" %in% columns) {
          email_check <- dbGetQuery(staged_data, paste0(
            "SELECT COUNT(*) as valid_emails FROM ", table_name,
            " WHERE customer_email IS NOT NULL AND customer_email != ''"
          ))
          message(sprintf("TEST: 📧 Valid customer emails: %d records", email_check$valid_emails))
        }

        if ("registration_date" %in% columns) {
          date_check <- dbGetQuery(staged_data, paste0(
            "SELECT COUNT(*) as valid_dates FROM ", table_name,
            " WHERE registration_date IS NOT NULL AND registration_date != ''"
          ))
          message(sprintf("TEST: 📅 Valid registration dates: %d records", date_check$valid_dates))
        }

        if ("registration_year" %in% columns) {
          year_dist <- dbGetQuery(staged_data, paste0(
            "SELECT registration_year, COUNT(*) as count FROM ", table_name,
            " WHERE registration_year IS NOT NULL GROUP BY registration_year ORDER BY registration_year"
          ))
          if (nrow(year_dist) > 0) {
            message(sprintf("TEST: 📆 Registration year distribution: %s",
                            paste(sprintf("%d(%d)", year_dist$registration_year, year_dist$count),
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
  message("SUMMARIZE: ✅ ETL Customer Staging completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Customer Staging failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "customers",
  platform = "cbz",
  etl_phase = "1ST",
  compliance = c("MP108", "MP104", "DM_R028", "MP064", "DM_R037", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 CUSTOMER STAGING SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🔄 ETL Phase: %s", final_metrics$etl_phase))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Customer Staging (cbz_ETL_customers_1ST.R) completed")
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
