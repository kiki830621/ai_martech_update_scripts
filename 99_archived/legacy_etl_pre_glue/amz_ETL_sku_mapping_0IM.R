# amz_ETL_sku_mapping_0IM.R - Amazon SKU-to-ASIN Mapping Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL sku_mapping Phase 0IM: Import from local Excel file
# Output: raw_data.duckdb → df_amz_sku_mapping

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

# Initialize using unified autoinit system
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$sku_mapping
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

source_type <- tolower(as.character(etl_profile$source_type %||% ""))
if (source_type != "excel") {
  stop(sprintf("VALIDATE FAILED: sku_mapping requires source_type='excel', got '%s'", source_type))
}

rawdata_root <- RAW_DATA_DIR %||% file.path(APP_DIR, "data", "local_data", "rawdata_QEF_DESIGN")
rawdata_rel_path <- as.character(etl_profile$rawdata_path %||% "")
if (!nzchar(rawdata_rel_path)) {
  stop("VALIDATE FAILED: sku_mapping profile missing rawdata_path")
}

# Define source file for SKU mapping from config
sku_mapping_file <- file.path(rawdata_root, rawdata_rel_path)
sku_mapping_dir <- dirname(sku_mapping_file)

message("INITIALIZE: Amazon SKU mapping import (ETL sku_mapping 0IM) script initialized")
message("INITIALIZE: Data source file: ", sku_mapping_file)

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL sku_mapping Import Phase - Amazon SKU-to-ASIN mapping...")

  # Validate config-declared source file exists
  if (!file.exists(sku_mapping_file)) {
    stop(sprintf("VALIDATE FAILED: rawdata_path not found '%s'", rawdata_rel_path))
  }
  message(sprintf("VALIDATE: Found declared rawdata_path '%s'", rawdata_rel_path))

  # Read the SKU mapping Excel file
  # qef-product-master-redesign task 4.2 / #462 fix:
  # The standard `SKUtoASIN number.xlsx` template has:
  #   row 1: title text ("SKU Details" merged across cells)
  #   row 2: blank
  #   row 3: real header (Name, SKU, Product Line, AMZ ASIN, 成本, 利潤)
  # Without skip=2, read_excel treated row 1 as header → columns became
  # `sku_details, 2, 3, 4, 5, 6` and downstream lookups silently broke.
  # skip=2 makes row 3 the header row.
  message("MAIN: Reading SKU mapping from: ", basename(sku_mapping_file), " (skip=2)")
  df_sku <- readxl::read_excel(sku_mapping_file, skip = 2)

  # Standardize column names to snake_case
  names(df_sku) <- tolower(gsub("[^a-zA-Z0-9]", "_", names(df_sku)))
  names(df_sku) <- gsub("_+", "_", names(df_sku))
  names(df_sku) <- gsub("^_|_$", "", names(df_sku))

  # Add source file tracking
  df_sku$source_file <- basename(sku_mapping_file)

  message("MAIN: Read ", nrow(df_sku), " rows, ", ncol(df_sku), " columns")
  message("MAIN: Columns: ", paste(names(df_sku), collapse = ", "))

  # Write to raw_data
  df_sku <- as.data.frame(df_sku)
  dbWriteTable(raw_data, "df_amz_sku_mapping", df_sku, overwrite = TRUE)
  message("MAIN: Wrote df_amz_sku_mapping to raw_data")

  script_success <- TRUE
  message("MAIN: ETL sku_mapping Import Phase completed successfully")

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message("MAIN ERROR: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================

if (script_success) {
  tryCatch({
    message("TEST: Verifying ETL sku_mapping Import Phase results...")

    table_name <- "df_amz_sku_mapping"

    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      sku_count <- sql_read(raw_data, paste0("SELECT COUNT(*) as count FROM ", table_name))$count

      if (sku_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", sku_count, " SKU mappings imported")

        # Show sample data
        sample_data <- sql_read(raw_data, paste0("SELECT * FROM ", table_name, " LIMIT 5"))
        message("TEST: Sample raw data structure:")
        print(sample_data)

        # Show columns
        columns <- dbListFields(raw_data, table_name)
        message("TEST: Columns: ", paste(columns, collapse = ", "))
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - table is empty")
      }
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - table ", table_name, " not found")
    }

  }, error = function(e) {
    test_passed <<- FALSE
    message("TEST ERROR: ", e$message)
  })
} else {
  message("TEST: Skipped due to main script failure")
}

# ==============================================================================
# 4. DEINITIALIZE
# ==============================================================================

# Determine final status before tearing down
if (script_success && test_passed) {
  message("DEINITIALIZE: ETL sku_mapping Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL sku_mapping Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL sku_mapping Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL sku_mapping Import Phase (amz_ETL_sku_mapping_0IM.R) completed")
