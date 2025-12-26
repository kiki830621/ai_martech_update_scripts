#' @file all_ETL_item_profile_0IM.R
#' @requires DBI
#' @requires duckdb
#' @requires googlesheets4
#' @requires janitor
#' @requires dplyr
#' @principle MP064 ETL/Derivation Separation
#' @principle SO_P010 Config-Driven Customization
#' @principle DM_R028 ETL Data Type Separation
#' @author Claude
#' @date 2025-12-26
#' @title Item Profile ETL - Import from Google Sheets
#' @description
#'   ETL script to import item profiles for all product lines from Google Sheets.
#'
#'   This script replaces the legacy fn_import_item_profiles.R function with
#'   a proper ETL script following the 6-Layer architecture.
#'
#'   Data Flow:
#'   1. Read product line configuration from app_data
#'   2. For each active product line, fetch from Google Sheets
#'   3. Clean and standardize data
#'   4. Write to raw_data.duckdb as df_all_item_profile_{product_line_id}

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================
autoinit()

# Ensure g_project_root is available
if (!exists("g_project_root") || is.null(g_project_root)) {
  g_project_root <- getwd()
  message("Derived g_project_root: ", g_project_root)
}

library(dplyr)
library(DBI)

message(strrep("=", 60))
message("Item Profile ETL - Import from Google Sheets")
message("Following: MP064, SO_P010, DM_R028")
message(strrep("=", 60))

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  # ==========================================================================
  # STEP 1: Load configuration
  # ==========================================================================
  message("\n--- Loading Configuration ---")

  app_config_path <- file.path(g_project_root, "app_config.yaml")
  if (!file.exists(app_config_path)) {
    stop("app_config.yaml not found: ", app_config_path)
  }

  app_config <- yaml::read_yaml(app_config_path)

  # Get Google Sheet ID from config
  google_sheet_id <- app_config$googlesheet$product_profile
  if (is.null(google_sheet_id) || google_sheet_id == "") {
    stop("googlesheet.product_profile not found in app_config.yaml")
  }

  message("Google Sheet ID: ", google_sheet_id)

  # ==========================================================================
  # STEP 2: Connect to databases
  # ==========================================================================
  message("\n--- Connecting to Databases ---")

  # Connect to app_data to get product line info
  if (!exists("app_con") || is.null(app_con)) {
    app_db_path <- file.path(g_project_root, "data/app_data/app_data.duckdb")
    app_con <- DBI::dbConnect(duckdb::duckdb(), app_db_path)
    message("Connected to app_data: ", app_db_path)
  }

  # Connect to raw_data for output
  if (!exists("raw_con") || is.null(raw_con)) {
    raw_db_path <- file.path(g_project_root, "data/local_data/raw_data.duckdb")
    raw_con <- DBI::dbConnect(duckdb::duckdb(), raw_db_path)
    message("Connected to raw_data: ", raw_db_path)
  }

  # ==========================================================================
  # STEP 3: Get active product lines
  # ==========================================================================
  message("\n--- Loading Product Lines ---")

  # Check if df_product_line exists
  if (DBI::dbExistsTable(app_con, "df_product_line")) {
    df_product_line <- DBI::dbGetQuery(app_con, "
      SELECT product_line_id, product_line_name_chinese
      FROM df_product_line
      WHERE included = TRUE AND product_line_id != 'all'
    ")
    message("Found ", nrow(df_product_line), " active product lines")
  } else {
    # Fallback: use hardcoded product lines (MAMBA specific)
    # Updated 2025-12-26: Correct Google Sheets names discovered
    message("df_product_line not found, using hardcoded list")
    df_product_line <- data.frame(
      product_line_id = c("tur", "pre", "irn", "alu", "kit", "pip"),
      product_line_name_chinese = c("渦輪", "洩壓閥", "鐵葉(渦輪葉輪)", "鋁葉(壓縮葉輪)", "修理包", "油水管包"),
      stringsAsFactors = FALSE
    )
  }

  print(df_product_line)

  # ==========================================================================
  # STEP 4: Import from Google Sheets
  # ==========================================================================
  message("\n--- Importing from Google Sheets ---")

  # Authenticate with Google Sheets (deauth for public sheets)
  googlesheets4::gs4_deauth()

  # Track results
  success_count <- 0
  failed_count <- 0
  failed_lines <- character(0)

  for (i in seq_len(nrow(df_product_line))) {
    product_line_id <- df_product_line$product_line_id[i]
    product_line_name <- df_product_line$product_line_name_chinese[i]

    # Sheet name format: item_profile_{中文名}
    sheet_name <- paste0("item_profile_", product_line_name)
    table_name <- paste0("df_all_item_profile_", product_line_id)

    message("\n  Processing: ", product_line_name, " (", product_line_id, ")")
    message("    Sheet: ", sheet_name)
    message("    Table: ", table_name)

    tryCatch({
      # Read from Google Sheets
      df_item_profile <- googlesheets4::read_sheet(
        ss = google_sheet_id,
        sheet = sheet_name
      )

      # Clean column names
      df_item_profile <- df_item_profile %>%
        janitor::clean_names(ascii = FALSE)

      # Convert list columns to character (DM_R024)
      for (col_name in names(df_item_profile)) {
        if (is.list(df_item_profile[[col_name]])) {
          df_item_profile[[col_name]] <- sapply(df_item_profile[[col_name]], function(x) {
            if (is.null(x) || length(x) == 0) NA_character_
            else paste(as.character(x), collapse = "; ")
          })
        }
      }

      # Add metadata
      df_item_profile <- df_item_profile %>%
        mutate(
          product_line_id = product_line_id,
          product_line_name = product_line_name,
          import_timestamp = Sys.time()
        )

      # Write to raw_data
      DBI::dbWriteTable(
        raw_con,
        table_name,
        as.data.frame(df_item_profile),
        overwrite = TRUE
      )

      message("    SUCCESS: ", nrow(df_item_profile), " rows imported")
      success_count <- success_count + 1

    }, error = function(e) {
      message("    FAILED: ", e$message)
      failed_count <<- failed_count + 1
      failed_lines <<- c(failed_lines, product_line_id)
    })
  }

  # ==========================================================================
  # STEP 5: Summary
  # ==========================================================================
  message("\n", strrep("=", 60))
  message("Import Summary")
  message(strrep("=", 60))
  message("  Successful: ", success_count)
  message("  Failed: ", failed_count)
  if (length(failed_lines) > 0) {
    message("  Failed lines: ", paste(failed_lines, collapse = ", "))
  }

}, error = function(e) {
  message("ERROR: Item Profile ETL failed")
  message("  Error: ", e$message)
  stop("0IM failed: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================
message("\n--- Verification ---")

tryCatch({
  # List all item_profile tables
  all_tables <- DBI::dbListTables(raw_con)
  profile_tables <- all_tables[grepl("^df_all_item_profile_", all_tables)]

  message("Item profile tables in raw_data:")
  for (tbl in profile_tables) {
    row_count <- DBI::dbGetQuery(raw_con, sprintf("SELECT COUNT(*) as n FROM %s", tbl))$n
    col_count <- length(DBI::dbListFields(raw_con, tbl))
    message("  ", tbl, ": ", row_count, " rows, ", col_count, " columns")
  }

}, error = function(e) {
  warning("Verification failed: ", e$message)
})

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================
message("\n", strrep("=", 60))
message("Item Profile ETL Complete")
message(strrep("=", 60))
message("  Source: Google Sheets (", google_sheet_id, ")")
message("  Target: raw_data.duckdb/df_all_item_profile_*")
message("  Status: Complete")

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================
autodeinit()
