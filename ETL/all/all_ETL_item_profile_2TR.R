#' @file all_ETL_item_profile_2TR.R
#' @requires DBI
#' @requires duckdb
#' @requires dplyr
#' @principle MP064 ETL/Derivation Separation
#' @principle DM_R028 ETL Data Type Separation
#' @principle MP102 ETL Output Standardization
#' @author Claude
#' @date 2025-12-26
#' @title Item Profile ETL - Transform Phase
#' @description
#'   Transform item profile data from staged_data to transformed_data.
#'
#'   2TR Phase: Apply standardized transformations
#'   - Standardize column names
#'   - Add transform metadata
#'   - Prepare for downstream consumption

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
autoinit()

if (!exists("g_project_root") || is.null(g_project_root)) {
  g_project_root <- getwd()
  message("Derived g_project_root: ", g_project_root)
}

library(dplyr)
library(DBI)

message(strrep("=", 60))
message("Item Profile ETL - Transform Phase (2TR)")
message("Following: MP064, DM_R028, MP102")
message(strrep("=", 60))

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  # ==========================================================================
  # STEP 1: Connect to databases
  # ==========================================================================
  message("\n--- Connecting to Databases ---")

  # Source: staged_data
  if (!exists("staged_con") || is.null(staged_con)) {
    staged_db_path <- file.path(g_project_root, "data/local_data/staged_data.duckdb")
    staged_con <- DBI::dbConnect(duckdb::duckdb(), staged_db_path)
    message("Connected to staged_data: ", staged_db_path)
  }

  # Target: transformed_data
  if (!exists("transformed_con") || is.null(transformed_con)) {
    transformed_db_path <- file.path(g_project_root, "data/local_data/transformed_data.duckdb")
    transformed_con <- DBI::dbConnect(duckdb::duckdb(), transformed_db_path)
    message("Connected to transformed_data: ", transformed_db_path)
  }

  # ==========================================================================
  # STEP 2: Get item_profile tables from staged_data
  # ==========================================================================
  message("\n--- Finding Item Profile Tables ---")

  all_tables <- DBI::dbListTables(staged_con)
  profile_tables <- all_tables[grepl("^df_all_item_profile_", all_tables)]

  message("Found ", length(profile_tables), " item profile tables")

  # Track results
  success_count <- 0
  failed_count <- 0

  # ==========================================================================
  # STEP 3: Transform each table
  # ==========================================================================
  message("\n--- Transforming Tables ---")

  for (tbl_name in profile_tables) {
    message("\nProcessing: ", tbl_name)

    tryCatch({
      # Read from staged_data
      df_staged <- DBI::dbReadTable(staged_con, tbl_name)
      message("  Staged rows: ", nrow(df_staged))

      # Apply transformations
      df_transformed <- df_staged %>%
        mutate(
          # Ensure sku and ebay_item_number are clean
          sku = trimws(as.character(sku)),
          ebay_item_number = trimws(as.character(ebay_item_number)),

          # Remove "NA" strings
          sku = ifelse(sku == "NA" | sku == "", NA_character_, sku),
          ebay_item_number = ifelse(ebay_item_number == "NA" | ebay_item_number == "", NA_character_, ebay_item_number),

          # Add transform metadata
          transform_timestamp = Sys.time(),
          schema_version = "2TR_v1.0"
        )

      # Write to transformed_data (same table name)
      DBI::dbWriteTable(
        transformed_con,
        tbl_name,
        df_transformed,
        overwrite = TRUE
      )

      message("  SUCCESS: ", nrow(df_transformed), " rows transformed")
      success_count <- success_count + 1

    }, error = function(e) {
      message("  FAILED: ", e$message)
      failed_count <<- failed_count + 1
    })
  }

  # ==========================================================================
  # STEP 4: Summary
  # ==========================================================================
  message("\n", strrep("=", 60))
  message("Transform Summary")
  message(strrep("=", 60))
  message("  Successful: ", success_count)
  message("  Failed: ", failed_count)

}, error = function(e) {
  message("ERROR: Item Profile Transform failed")
  message("  Error: ", e$message)
  stop("2TR failed: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================
message("\n--- Verification ---")

tryCatch({
  all_tables <- DBI::dbListTables(transformed_con)
  profile_tables <- all_tables[grepl("^df_all_item_profile_", all_tables)]

  message("Item profile tables in transformed_data:")
  for (tbl in profile_tables) {
    row_count <- sql_read(transformed_con, sprintf("SELECT COUNT(*) as n FROM %s", tbl))$n

    # Check sku/ebay coverage
    coverage <- sql_read(transformed_con, sprintf("
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN sku IS NOT NULL THEN 1 ELSE 0 END) as has_sku,
        SUM(CASE WHEN ebay_item_number IS NOT NULL THEN 1 ELSE 0 END) as has_eby
      FROM %s
    ", tbl))

    message("  ", tbl, ": ", row_count, " rows")
    message("    SKU coverage: ", coverage$has_sku, "/", coverage$total)
    message("    eBay coverage: ", coverage$has_eby, "/", coverage$total)
  }

}, error = function(e) {
  warning("Verification failed: ", e$message)
})

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================
message("\n", strrep("=", 60))
message("Item Profile Transform Complete")
message(strrep("=", 60))
message("  Source: staged_data.duckdb/df_all_item_profile_*")
message("  Target: transformed_data.duckdb/df_all_item_profile_*")
message("  Status: Complete")

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================
autodeinit()
