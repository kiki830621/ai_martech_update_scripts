# amz_ETL_keepa_0IM.R - Amazon Keepa Sales Rank Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL keepa Phase 0IM: Import from local Excel files
# Output: raw_data.duckdb → df_amz_keepa

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
etl_profile <- platform_cfg$etl_sources$keepa
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

source_type <- tolower(as.character(etl_profile$source_type %||% ""))
if (!source_type %in% c("excel", "csv")) {
  stop(sprintf("VALIDATE FAILED: keepa requires source_type='excel' or 'csv', got '%s'", source_type))
}

rawdata_root <- RAW_DATA_DIR %||% file.path(APP_DIR, "data", "local_data", "rawdata_QEF_DESIGN")
rawdata_pattern <- as.character(etl_profile$rawdata_pattern %||% "")
if (!nzchar(rawdata_pattern)) {
  stop("VALIDATE FAILED: keepa profile missing rawdata_pattern")
}
keepa_files <- Sys.glob(file.path(rawdata_root, rawdata_pattern))

message("INITIALIZE: Amazon Keepa import (ETL keepa 0IM) script initialized")
message("INITIALIZE: Data source pattern: ", rawdata_pattern)

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL keepa Import Phase - Amazon Keepa sales rank data...")

  if (length(keepa_files) == 0) {
    stop(sprintf("VALIDATE FAILED: No files match pattern '%s'", rawdata_pattern))
  }
  message(sprintf("VALIDATE: Found %d files matching declared pattern", length(keepa_files)))

  # Read all Keepa files and combine
  all_keepa <- lapply(keepa_files, function(f) {
    tryCatch({
      df <- readxl::read_excel(f)
      # Standardize column names to snake_case
      names(df) <- tolower(gsub("[^a-zA-Z0-9]", "_", names(df)))
      names(df) <- gsub("_+", "_", names(df))
      names(df) <- gsub("^_|_$", "", names(df))
      # Prevent DuckDB write failures when source has duplicate headers
      names(df) <- make.unique(names(df), sep = "_dup_")

      # Extract product_line_id from directory name (e.g., "001_hunting_safety_glasses")
      dir_name <- basename(dirname(f))
      df$product_line_id <- dir_name

      # Extract ASIN from filename (e.g., "keepa-B0BWJF9SDG-20250916.xlsx")
      file_asin <- gsub("keepa-([A-Z0-9]+)-.*", "\\1", basename(f))
      if (!("asin" %in% names(df))) {
        df$asin <- file_asin
      }

      # Add source file tracking
      df$source_file <- basename(f)

      df
    }, error = function(e) {
      message("MAIN WARNING: Failed to read ", basename(f), ": ", e$message)
      NULL
    })
  })

  # Remove NULL entries and combine
  all_keepa <- all_keepa[!sapply(all_keepa, is.null)]

  if (length(all_keepa) > 0) {
    df_amz_keepa <- data.table::rbindlist(all_keepa, fill = TRUE)
    df_amz_keepa <- as.data.frame(df_amz_keepa)

    message("MAIN: Combined ", nrow(df_amz_keepa), " rows from ", length(all_keepa), " files")

    # Write to raw_data
    dbWriteTable(raw_data, "df_amz_keepa", df_amz_keepa, overwrite = TRUE)
    message("MAIN: Wrote df_amz_keepa to raw_data")
  } else {
    stop("All Keepa files failed to read")
  }

  script_success <- TRUE
  message("MAIN: ETL keepa Import Phase completed successfully")

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
    message("TEST: Verifying ETL keepa Import Phase results...")

    table_name <- "df_amz_keepa"

    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      keepa_count <- sql_read(raw_data, paste0("SELECT COUNT(*) as count FROM ", table_name))$count

      test_passed <- TRUE
      message("TEST: Verification successful - ", keepa_count, " Keepa records imported")

      if (keepa_count > 0) {
        # Show sample data
        sample_data <- sql_read(raw_data, paste0("SELECT * FROM ", table_name, " LIMIT 3"))
        message("TEST: Sample raw data structure:")
        print(sample_data)

        # Check product line distribution
        pl_stats <- sql_read(raw_data, paste0(
          "SELECT product_line_id, COUNT(*) as count FROM ", table_name,
          " GROUP BY product_line_id ORDER BY product_line_id"
        ))
        message("TEST: Product line distribution:")
        print(pl_stats)
      } else {
        message("TEST: Table exists but is empty (no data files found)")
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
  message("DEINITIALIZE: ETL keepa Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL keepa Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL keepa Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL keepa Import Phase (amz_ETL_keepa_0IM.R) completed")
