# amz_ETL_sales_2TR.R - Amazon Sales Data Transformation
# ==============================================================================
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
#
# ETL Sales Phase 2TR (Transform): Standardize data types and filter invalid records
# Input: staged_data.duckdb (df_amz_sales___staged)
# Output: transformed_data.duckdb (df_amz_sales___transformed)
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

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
script_name <- "amz_ETL_sales_2TR"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: Starting Amazon Sales Transformation (2TR Phase)")
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

parse_mixed_datetime <- function(values, tz = "UTC") {
  raw_chr <- trimws(as.character(values))
  out <- rep(as.POSIXct(NA, tz = tz), length(raw_chr))

  if (length(raw_chr) == 0) {
    return(out)
  }

  is_blank <- is.na(raw_chr) | raw_chr == ""
  if (all(is_blank)) {
    return(out)
  }

  numeric_vals <- suppressWarnings(as.numeric(raw_chr))
  is_excel_serial <- !is_blank & !is.na(numeric_vals) & numeric_vals > 20000 & numeric_vals < 80000
  if (any(is_excel_serial)) {
    out[is_excel_serial] <- as.POSIXct(
      (numeric_vals[is_excel_serial] - 25569) * 86400,
      origin = "1970-01-01",
      tz = tz
    )
  }

  need_parse <- !is_blank & is.na(out)
  if (any(need_parse)) {
    parse_chr <- raw_chr[need_parse]
    parse_chr <- gsub("Z$", "", parse_chr)
    parsed <- suppressWarnings(as.POSIXct(
      parse_chr,
      tz = tz,
      tryFormats = c(
        "%Y-%m-%d %H:%M:%OS",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%OS",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%OS",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%m/%d/%Y %H:%M:%OS",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%d/%m/%Y %H:%M:%OS",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y"
      )
    ))
    out[need_parse] <- parsed
  }

  out
}

message("INITIALIZE: Connecting to databases...")
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)
message(sprintf("INITIALIZE: Read from: %s", db_path_list$staged_data))
message(sprintf("INITIALIZE: Write to: %s", db_path_list$transformed_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: Initialization completed (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: Starting Amazon Sales Transformation...")
main_start_time <- Sys.time()

tryCatch({
  input_table <- "df_amz_sales___staged"
  output_table <- "df_amz_sales___transformed"

  # Check source table
  if (!dbExistsTable(staged_data, input_table)) {
    stop(sprintf("Required table %s not found. Run amz_ETL_sales_1ST first.", input_table))
  }

  # Load staged data
  message(sprintf("MAIN: Step 1/4 - Loading %s...", input_table))
  load_start <- Sys.time()
  df_staged <- sql_read(staged_data, sprintf("SELECT * FROM %s", input_table))
  n_staged <- nrow(df_staged)
  message(sprintf("MAIN: Loaded %d records (%.2fs)",
                  n_staged, as.numeric(Sys.time() - load_start, units = "secs")))

  if (n_staged == 0) {
    stop("No staged data found - cannot proceed with transformation")
  }

  dt <- as.data.table(df_staged)

  # Step 2: Standardize date columns
  message("MAIN: Step 2/4 - Standardizing date columns...")
  date_start <- Sys.time()

  if ("purchase_date" %in% names(dt)) {
    dt[, purchase_date := parse_mixed_datetime(purchase_date, tz = "UTC")]
    n_na_date <- sum(is.na(dt$purchase_date))
    if (n_na_date > 0) {
      message(sprintf("    Warning: %d records with unparseable purchase_date", n_na_date))
    }
    message("    purchase_date → POSIXct")
  }

  if ("last_updated_date" %in% names(dt)) {
    dt[, last_updated_date := parse_mixed_datetime(last_updated_date, tz = "UTC")]
    message("    last_updated_date → POSIXct")
  }

  message(sprintf("MAIN: Date standardization done (%.2fs)",
                  as.numeric(Sys.time() - date_start, units = "secs")))

  # Step 3: Standardize numeric columns
  message("MAIN: Step 3/4 - Standardizing numeric columns...")
  num_start <- Sys.time()

  price_cols <- c("item_price", "item_tax", "shipping_price", "shipping_tax",
                  "gift_wrap_price", "gift_wrap_tax",
                  "item_promotion_discount", "ship_promotion_discount")

  for (col in intersect(price_cols, names(dt))) {
    if (!is.numeric(dt[[col]])) {
      set(dt, j = col, value = as.numeric(dt[[col]]))
      message(sprintf("    %s → numeric", col))
    }
  }

  if ("quantity" %in% names(dt) && !is.integer(dt$quantity)) {
    dt[, quantity := as.integer(quantity)]
    message("    quantity → integer")
  }

  message(sprintf("MAIN: Numeric standardization done (%.2fs)",
                  as.numeric(Sys.time() - num_start, units = "secs")))

  # Step 4: Filter and write
  message("MAIN: Step 4/4 - Filtering and writing transformed data...")
  write_start <- Sys.time()

  # Filter: remove cancelled/pending orders if order_status exists
  n_before <- nrow(dt)
  if ("order_status" %in% names(dt)) {
    # Keep Shipped and other completed statuses; exclude Cancelled
    dt <- dt[!tolower(order_status) %in% c("cancelled")]
    n_filtered <- n_before - nrow(dt)
    if (n_filtered > 0) {
      message(sprintf("    Filtered %d cancelled orders", n_filtered))
    }
  }

  # Filter: remove records with NA purchase_date
  n_before <- nrow(dt)
  dt <- dt[!is.na(purchase_date)]
  n_removed <- n_before - nrow(dt)
  if (n_removed > 0) {
    message(sprintf("    Removed %d records with NA purchase_date", n_removed))
  }

  # Drop existing table if present
  if (dbExistsTable(transformed_data, output_table)) {
    dbRemoveTable(transformed_data, output_table)
  }

  dbWriteTable(transformed_data, output_table, as.data.frame(dt), overwrite = TRUE)

  actual_count <- sql_read(transformed_data,
    sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
  message(sprintf("MAIN: Stored %d records in %s (%.2fs)",
                  actual_count, output_table,
                  as.numeric(Sys.time() - write_start, units = "secs")))

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: Transformation completed (%.2fs). %d → %d records",
                  main_elapsed, n_staged, actual_count))

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: Starting transformation verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_amz_sales___transformed"

    # Test 1: Table exists
    if (!dbExistsTable(transformed_data, output_table)) stop("Table does not exist")
    message("TEST: Table exists")

    # Test 2: Has data
    row_count <- sql_read(transformed_data,
      sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
    if (row_count == 0) stop("Table is empty")
    message(sprintf("TEST: %d rows", row_count))

    # Test 3: purchase_date is not all NULL
    null_dates <- sql_read(transformed_data, sprintf(
      "SELECT COUNT(*) as n FROM %s WHERE purchase_date IS NULL", output_table))$n
    if (null_dates == row_count) stop("All purchase_date values are NULL")
    message(sprintf("TEST: %d valid purchase_dates", row_count - null_dates))

    # Test 4: Verify numeric item_price
    price_sample <- sql_read(transformed_data, sprintf(
      "SELECT item_price FROM %s WHERE item_price IS NOT NULL LIMIT 3", output_table))
    if (nrow(price_sample) > 0 && is.numeric(price_sample$item_price)) {
      message("TEST: item_price is numeric")
    }

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
message("SUMMARIZE: AMAZON SALES TRANSFORMATION (2TR)")
message(strrep("=", 80))
message(sprintf("Platform: amz | Phase: 2TR"))
message(sprintf("Total time: %.2fs", as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
message(sprintf("Compliance: MP064, DM_R028, DEV_R032"))
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: Cleaning up...")
if (exists("staged_data") && inherits(staged_data, "DBIConnection") && DBI::dbIsValid(staged_data)) {
  DBI::dbDisconnect(staged_data)
}
if (exists("transformed_data") && inherits(transformed_data, "DBIConnection") && DBI::dbIsValid(transformed_data)) {
  DBI::dbDisconnect(transformed_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
