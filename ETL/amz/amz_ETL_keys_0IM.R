# amz_ETL_keys_0IM.R - Import KEYS.xlsx Product Mapping
# ==============================================================================
# Following MP064: ETL-Derivation Separation Principle
# Following DM_R028: ETL Data Type Separation Rule
# Following DM_R037 v3.0: Config-Driven Import (source_type/version in app_config.yaml)
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
#
# ETL Keys Phase 0IM (Import): Read KEYS.xlsx product mapping into raw_data.duckdb
# Input: rawdata_QEF_DESIGN/KEYS.xlsx
#   Columns: ProductLine, 產品名稱, 網址, ASIN, SKU, 成本, 利潤
# Output: raw_data.duckdb → df_amz_product_keys
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
script_name <- "amz_ETL_keys_0IM"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: Starting KEYS.xlsx Product Mapping Import (0IM Phase)")
message(sprintf("INITIALIZE: Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message(strrep("=", 80))

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
OPERATION_MODE <- "UPDATE_MODE"
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$keys
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))

message("INITIALIZE: Loading required libraries...")
library(DBI)
library(duckdb)
library(readxl)
library(data.table)

source(file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R"))

message("INITIALIZE: Connecting to raw_data database...")
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
message(sprintf("INITIALIZE: Using: %s", db_path_list$raw_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: Initialization completed (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: Starting KEYS.xlsx import...")
main_start_time <- Sys.time()

tryCatch({
  source_type <- tolower(as.character(etl_profile$source_type %||% ""))
  if (source_type != "excel") {
    stop(sprintf("VALIDATE FAILED: keys requires source_type='excel', got '%s'", source_type))
  }

  rawdata_root <- file.path(APP_DIR, "data", "local_data", "rawdata_QEF_DESIGN")
  rawdata_rel_path <- as.character(etl_profile$rawdata_path %||% "")
  if (!nzchar(rawdata_rel_path)) {
    stop("VALIDATE FAILED: keys profile missing rawdata_path")
  }

  # Define path to KEYS.xlsx from config
  keys_path <- file.path(rawdata_root, rawdata_rel_path)

  if (!file.exists(keys_path)) {
    stop(sprintf("VALIDATE FAILED: rawdata_path not found '%s'", rawdata_rel_path))
  }
  message(sprintf("VALIDATE: Found declared rawdata_path '%s'", rawdata_rel_path))

  # Read KEYS.xlsx
  message(sprintf("MAIN: Step 1/3 - Reading %s...", keys_path))
  df_keys <- as.data.table(read_excel(keys_path))
  n_rows <- nrow(df_keys)
  message(sprintf("MAIN: Read %d rows with columns: %s",
                  n_rows, paste(names(df_keys), collapse = ", ")))

  if (n_rows == 0) {
    stop("KEYS.xlsx is empty")
  }

  # Step 2: Standardize column names to snake_case
  message("MAIN: Step 2/3 - Standardizing columns...")

  # Rename Chinese/mixed columns to English snake_case
  # Original: ProductLine, 產品名稱, 網址, ASIN, SKU, 成本, 利潤
  col_mapping <- c(
    "ProductLine" = "product_line_id",
    "\u7522\u54c1\u540d\u7a31" = "product_name",      # 產品名稱
    "\u7db2\u5740" = "url",                              # 網址
    "ASIN" = "asin",
    "SKU" = "sku",
    "\u6210\u672c" = "cost",                             # 成本
    "\u5229\u6f64" = "profit"                            # 利潤
  )

  for (old_name in names(col_mapping)) {
    if (old_name %in% names(df_keys)) {
      setnames(df_keys, old_name, col_mapping[[old_name]])
      message(sprintf("    Renamed: %s -> %s", old_name, col_mapping[[old_name]]))
    }
  }

  # Ensure numeric columns
  if ("cost" %in% names(df_keys)) {
    df_keys[, cost := as.numeric(cost)]
  }
  if ("profit" %in% names(df_keys)) {
    df_keys[, profit := as.numeric(profit)]
  }

  # Trim string columns
  char_cols <- names(df_keys)[vapply(df_keys, is.character, logical(1))]
  for (col in char_cols) {
    set(df_keys, j = col, value = trimws(df_keys[[col]]))
  }

  # Add platform_id
  df_keys[, platform_id := "amz"]

  message(sprintf("MAIN: Standardized columns: %s", paste(names(df_keys), collapse = ", ")))

  # Step 3: Write to raw_data
  message("MAIN: Step 3/3 - Writing to raw_data database...")
  output_table <- "df_amz_product_keys"

  if (dbExistsTable(raw_data, output_table)) {
    dbRemoveTable(raw_data, output_table)
    message(sprintf("MAIN: Dropped existing table: %s", output_table))
  }

  dbWriteTable(raw_data, output_table, as.data.frame(df_keys), overwrite = TRUE)

  actual_count <- sql_read(raw_data,
    sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
  message(sprintf("MAIN: Stored %d records in %s", actual_count, output_table))

  # Display the data
  message("MAIN: Product key mapping:")
  print(as.data.frame(df_keys))

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: KEYS import completed (%.2fs)", main_elapsed))

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: Starting KEYS import verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_amz_product_keys"

    # Test 1: Table exists
    if (!dbExistsTable(raw_data, output_table)) stop("Table does not exist")
    message("TEST: Table exists")

    # Test 2: Has data
    row_count <- sql_read(raw_data,
      sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
    if (row_count == 0) stop("Table is empty")
    message(sprintf("TEST: %d product keys", row_count))

    # Test 3: Required columns
    columns <- dbListFields(raw_data, output_table)
    required <- c("product_line_id", "asin", "sku")
    missing <- setdiff(required, columns)
    if (length(missing) > 0) {
      stop(sprintf("Missing columns: %s", paste(missing, collapse = ", ")))
    }
    message("TEST: Required columns present (product_line_id, asin, sku)")

    # Test 4: No duplicate SKUs
    sku_counts <- sql_read(raw_data, sprintf(
      "SELECT sku, COUNT(*) as n FROM %s GROUP BY sku HAVING COUNT(*) > 1", output_table))
    if (nrow(sku_counts) > 0) {
      warning(sprintf("Found %d duplicate SKUs", nrow(sku_counts)))
    } else {
      message("TEST: No duplicate SKUs")
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
message("SUMMARIZE: KEYS PRODUCT MAPPING IMPORT (0IM)")
message(strrep("=", 80))
message(sprintf("Platform: amz | Data Type: keys"))
message(sprintf("Source: %s_%s", etl_profile$source_type, etl_profile$version))
message(sprintf("Total time: %.2fs", as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
message(sprintf("Compliance: MP064, DM_R028, DM_R037, DEV_R032"))
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: Cleaning up...")
if (exists("raw_data") && inherits(raw_data, "DBIConnection") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
