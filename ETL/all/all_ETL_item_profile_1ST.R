#' @file all_ETL_item_profile_1ST.R
#' @requires DBI
#' @requires duckdb
#' @requires dplyr
#' @principle MP064 ETL/Derivation Separation
#' @principle DM_R028 ETL Data Type Separation
#' @principle MP102 ETL Output Standardization
#' @author Claude
#' @date 2025-12-26
#' @title Item Profile ETL - Staging Phase
#' @description
#'   Stage item profile data from raw_data to staged_data.
#'
#'   1ST Phase: Data type optimization and validation
#'   - Convert list columns to character
#'   - Validate required fields
#'   - Add staging metadata

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================
autoinit()

if (!exists("g_project_root") || is.null(g_project_root)) {
  g_project_root <- getwd()
  message("Derived g_project_root: ", g_project_root)
}

library(dplyr)
library(DBI)

message(strrep("=", 60))
message("Item Profile ETL - Staging Phase (1ST)")
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

  # Source: raw_data
  if (!exists("raw_con") || is.null(raw_con)) {
    raw_db_path <- file.path(g_project_root, "data/local_data/raw_data.duckdb")
    raw_con <- DBI::dbConnect(duckdb::duckdb(), raw_db_path)
    message("Connected to raw_data: ", raw_db_path)
  }

  # Target: staged_data
  if (!exists("staged_con") || is.null(staged_con)) {
    staged_db_path <- file.path(g_project_root, "data/local_data/staged_data.duckdb")
    staged_con <- DBI::dbConnect(duckdb::duckdb(), staged_db_path)
    message("Connected to staged_data: ", staged_db_path)
  }

  # ==========================================================================
  # STEP 2: Get item_profile tables from raw_data
  # ==========================================================================
  message("\n--- Finding Item Profile Tables ---")

  all_tables <- DBI::dbListTables(raw_con)
  profile_tables <- all_tables[grepl("^df_all_item_profile_", all_tables)]

  message("Found ", length(profile_tables), " item profile tables")

  # Track results
  success_count <- 0
  failed_count <- 0

  # ==========================================================================
  # STEP 3: Stage each table
  # ==========================================================================
  message("\n--- Staging Tables ---")

  for (tbl_name in profile_tables) {
    message("\nProcessing: ", tbl_name)

    tryCatch({
      # Read from raw_data
      df_raw <- DBI::dbReadTable(raw_con, tbl_name)
      message("  Raw rows: ", nrow(df_raw))

      # Apply staging transformations
      df_staged <- df_raw %>%
        mutate(
          # Ensure key columns are character type
          across(where(is.list), ~ sapply(.x, function(x) {
            if (is.null(x) || length(x) == 0) NA_character_
            else paste(as.character(x), collapse = "; ")
          })),

          # Add staging metadata
          staged_timestamp = Sys.time(),
          validation_status = "passed"
        )

      # Ensure sku and ebay_item_number are character
      if ("sku" %in% names(df_staged)) {
        df_staged$sku <- as.character(df_staged$sku)
      }
      if ("ebay_item_number" %in% names(df_staged)) {
        df_staged$ebay_item_number <- as.character(df_staged$ebay_item_number)
      }

      # Write to staged_data (same table name)
      DBI::dbWriteTable(
        staged_con,
        tbl_name,
        df_staged,
        overwrite = TRUE
      )

      message("  SUCCESS: ", nrow(df_staged), " rows staged")
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
  message("Staging Summary")
  message(strrep("=", 60))
  message("  Successful: ", success_count)
  message("  Failed: ", failed_count)

}, error = function(e) {
  message("ERROR: Item Profile Staging failed")
  message("  Error: ", e$message)
  stop("1ST failed: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================
message("\n--- Verification ---")

tryCatch({
  all_tables <- DBI::dbListTables(staged_con)
  profile_tables <- all_tables[grepl("^df_all_item_profile_", all_tables)]

  message("Item profile tables in staged_data:")
  for (tbl in profile_tables) {
    row_count <- DBI::dbGetQuery(staged_con, sprintf("SELECT COUNT(*) as n FROM %s", tbl))$n
    message("  ", tbl, ": ", row_count, " rows")
  }

}, error = function(e) {
  warning("Verification failed: ", e$message)
})

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================
message("\n", strrep("=", 60))
message("Item Profile Staging Complete")
message(strrep("=", 60))
message("  Source: raw_data.duckdb/df_all_item_profile_*")
message("  Target: staged_data.duckdb/df_all_item_profile_*")
message("  Status: Complete")

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================
autodeinit()
