# amz_ETL_demographic_0IM.R - Amazon Demographic Data Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL demographic Phase 0IM: Import from local CSV files
# Output: raw_data.duckdb → df_amz_demographic

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
etl_profile <- platform_cfg$etl_sources$demographic
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

source_type <- tolower(as.character(etl_profile$source_type %||% ""))
if (source_type != "csv") {
  stop(sprintf("VALIDATE FAILED: demographic requires source_type='csv', got '%s'", source_type))
}

rawdata_root <- RAW_DATA_DIR %||% file.path(APP_DIR, "data", "local_data", "rawdata_QEF_DESIGN")
rawdata_pattern <- as.character(etl_profile$rawdata_pattern %||% "")
if (!nzchar(rawdata_pattern)) {
  stop("VALIDATE FAILED: demographic profile missing rawdata_pattern")
}
demographic_files <- Sys.glob(file.path(rawdata_root, rawdata_pattern))

message("INITIALIZE: Amazon demographic import (ETL demographic 0IM) script initialized")
message("INITIALIZE: Data source pattern: ", rawdata_pattern)

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL demographic Import Phase - Amazon demographic data...")

  if (length(demographic_files) == 0) {
    stop(sprintf("VALIDATE FAILED: No files match pattern '%s'", rawdata_pattern))
  }
  message(sprintf("VALIDATE: Found %d files matching declared pattern", length(demographic_files)))

  # Read all CSV files using data.table::fread for performance
  all_demographic <- lapply(demographic_files, function(f) {
    tryCatch({
      df <- data.table::fread(f, encoding = "UTF-8")

      # Standardize column names to snake_case
      names(df) <- tolower(gsub("[^a-zA-Z0-9]", "_", names(df)))
      names(df) <- gsub("_+", "_", names(df))
      names(df) <- gsub("^_|_$", "", names(df))

      # Add source file tracking
      df$source_file <- basename(f)

      df
    }, error = function(e) {
      message("MAIN WARNING: Failed to read ", basename(f), ": ", e$message)
      NULL
    })
  })

  # Remove NULL entries and combine
  all_demographic <- all_demographic[!sapply(all_demographic, is.null)]

  if (length(all_demographic) > 0) {
    df_amz_demographic <- data.table::rbindlist(all_demographic, fill = TRUE)
    df_amz_demographic <- as.data.frame(df_amz_demographic)

    message("MAIN: Combined ", nrow(df_amz_demographic), " rows from ", length(all_demographic), " files")

    # Write to raw_data
    dbWriteTable(raw_data, "df_amz_demographic", df_amz_demographic, overwrite = TRUE)
    message("MAIN: Wrote df_amz_demographic to raw_data")
  } else {
    stop("All demographic CSV files failed to read")
  }

  script_success <- TRUE
  message("MAIN: ETL demographic Import Phase completed successfully")

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
    message("TEST: Verifying ETL demographic Import Phase results...")

    table_name <- "df_amz_demographic"

    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      demo_count <- sql_read(raw_data, paste0("SELECT COUNT(*) as count FROM ", table_name))$count

      test_passed <- TRUE
      message("TEST: Verification successful - ", demo_count, " demographic records imported")

      if (demo_count > 0) {
        # Show sample data
        sample_data <- sql_read(raw_data, paste0("SELECT * FROM ", table_name, " LIMIT 3"))
        message("TEST: Sample raw data structure:")
        print(sample_data)

        # Show columns
        columns <- dbListFields(raw_data, table_name)
        message("TEST: Columns: ", paste(columns, collapse = ", "))

        # Check source file distribution
        file_stats <- sql_read(raw_data, paste0(
          "SELECT source_file, COUNT(*) as count FROM ", table_name,
          " GROUP BY source_file ORDER BY source_file"
        ))
        message("TEST: Source file distribution:")
        print(file_stats)
      } else {
        message("TEST: Table exists but is empty")
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
  message("DEINITIALIZE: ETL demographic Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL demographic Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL demographic Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL demographic Import Phase (amz_ETL_demographic_0IM.R) completed")
