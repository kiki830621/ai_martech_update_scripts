#' @file all_ETL_product_mapping_0IM___MAMBA.R
#' @requires DBI
#' @requires duckdb
#' @requires readxl
#' @requires dplyr
#' @principle DM_R052 Cross-Platform Product Identification
#' @principle DM_R037 Company-Specific ETL Naming
#' @principle MP064 ETL/Derivation Separation
#' @author Claude
#' @date 2025-12-26
#' @title Product Mapping ETL - Multi-Source Cross-Platform ID Resolution
#' @description
#'   ETL script to import SKU-to-eBay product mapping from MULTIPLE sources:
#'
#'   Source 1: df_all_item_profile_* tables (6 product lines from Google Sheets)
#'   Source 2: SKUtoeBay number.xlsx (Excel file)
#'   Source 3: "SKU to eBay Item Number" sheet from Google Sheets (primary mapping)
#'
#'   Per DM_R052 (Cross-Platform Product Identification):
#'   - Uses surrogate key (mapping_id) as primary key
#'   - Supports M:N (many-to-many) relationships
#'   - SKU and eby_item_id allow duplicates and NULL
#'   - Integrates multiple sources with deduplication
#'
#'   Per DM_R037 (Company-Specific ETL Naming):
#'   - ___MAMBA suffix indicates this is MAMBA company-specific mapping
#'
#'   Data Flow:
#'   1. Read from transformed_data: df_all_item_profile_* (sku, ebay_item_number)
#'      (After item_profile ETL 0IM→1ST→2TR completes)
#'   2. Read from Excel: SKUtoeBay number.xlsx
#'   3. UNION ALL sources
#'   4. Deduplicate by (sku, eby_item_id)
#'   5. Generate surrogate key (mapping_id)
#'   6. Write to app_data.duckdb as metadata table

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

# Ensure g_project_root is available
if (!exists("g_project_root") || is.null(g_project_root)) {
  g_project_root <- getwd()
  message("Derived g_project_root: ", g_project_root)
}

library(readxl)
library(dplyr)
library(DBI)
library(googlesheets4)
library(yaml)
library(janitor)

message(strrep("=", 60))
message("Product Mapping ETL - Multi-Source Integration")
message("Following: DM_R052, DM_R037, MP064")
message(strrep("=", 60))

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  # ==========================================================================
  # SOURCE 1: Read from transformed_data.duckdb - df_all_item_profile_* tables
  # (After item_profile ETL 0IM→1ST→2TR completes)
  # ==========================================================================
  message("\n--- SOURCE 1: Google Sheets (transformed_data) ---")

  # Connect to transformed_data if not available
  if (!exists("transformed_con") || is.null(transformed_con)) {
    transformed_db_path <- file.path(g_project_root, "data/local_data/transformed_data.duckdb")
    if (file.exists(transformed_db_path)) {
      transformed_con <- DBI::dbConnect(duckdb::duckdb(), transformed_db_path)
      message("Connected to transformed_data: ", transformed_db_path)
    } else {
      warning("transformed_data.duckdb not found: ", transformed_db_path)
      transformed_con <- NULL
    }
  }

  # Product lines to scan
  product_lines <- c("alf", "irf", "pre", "rek", "tur", "wak")
  mapping_from_profiles <- NULL

  if (!is.null(transformed_con)) {
    mapping_list <- lapply(product_lines, function(pl) {
      tbl_name <- paste0("df_all_item_profile_", pl)

      if (DBI::dbExistsTable(transformed_con, tbl_name)) {
        # Check which columns exist
        cols <- DBI::dbListFields(transformed_con, tbl_name)
        has_sku <- "sku" %in% cols
        has_eby <- "ebay_item_number" %in% cols
        has_name <- "product_name" %in% cols

        if (has_sku || has_eby) {
          sql <- sprintf("
            SELECT DISTINCT
              %s AS sku,
              %s AS eby_item_id,
              %s AS product_name,
              '%s' AS product_line_id,
              'google_sheet' AS source
            FROM %s
            WHERE %s IS NOT NULL OR %s IS NOT NULL
          ",
            if (has_sku) "CAST(sku AS VARCHAR)" else "NULL",
            if (has_eby) "CAST(ebay_item_number AS VARCHAR)" else "NULL",
            if (has_name) "product_name" else "NULL",
            pl,
            tbl_name,
            if (has_sku) "sku" else "1=0",
            if (has_eby) "ebay_item_number" else "1=0"
          )

          result <- sql_read(transformed_con, sql)
          message("  ", tbl_name, ": ", nrow(result), " rows")
          return(result)
        } else {
          message("  ", tbl_name, ": No sku/ebay_item_number columns")
          return(NULL)
        }
      } else {
        message("  ", tbl_name, ": Table not found")
        return(NULL)
      }
    })

    # Combine all profile mappings
    mapping_from_profiles <- bind_rows(mapping_list)
    message("Total from Google Sheets: ", nrow(mapping_from_profiles), " rows")
  } else {
    message("Skipping Source 1: transformed_data not available")
    mapping_from_profiles <- data.frame(
      sku = character(),
      eby_item_id = character(),
      product_name = character(),
      product_line_id = character(),
      source = character()
    )
  }

  # ==========================================================================
  # SOURCE 2: Read from Excel file
  # ==========================================================================
  message("\n--- SOURCE 2: Excel File ---")

  excel_path <- file.path(
    g_project_root,
    "data/local_data/rawdata_MAMBA/SKUtoeBay numbers/SKUtoeBay number.xlsx"
  )

  mapping_from_excel <- NULL

  if (file.exists(excel_path)) {
    message("Reading from: ", excel_path)

    df_raw <- read_excel(
      excel_path,
      sheet = "sku details",
      skip = 5
    )

    message("Excel raw rows: ", nrow(df_raw))

    # Find column indices based on content patterns
    col_names <- names(df_raw)
    name_col <- grep("Name", col_names, value = TRUE)[1]
    sku_col <- grep("^SKU$", col_names, value = TRUE)[1]
    line_col <- grep("Product Line", col_names, value = TRUE)[1]
    eby_col <- grep("eBay", col_names, value = TRUE)[1]

    message("Column mapping:")
    message("  Name: ", name_col, " | SKU: ", sku_col)
    message("  Line: ", line_col, " | eBay: ", eby_col)

    # Transform to standardized schema
    mapping_from_excel <- df_raw %>%
      transmute(
        sku = if (!is.null(sku_col)) as.character(.data[[sku_col]]) else NA_character_,
        eby_item_id = if (!is.null(eby_col)) as.character(.data[[eby_col]]) else NA_character_,
        product_name = if (!is.null(name_col)) as.character(.data[[name_col]]) else NA_character_,
        product_line_id = if (!is.null(line_col)) as.character(.data[[line_col]]) else NA_character_,
        source = "excel"
      ) %>%
      filter(!is.na(sku) | !is.na(eby_item_id))

    message("Total from Excel: ", nrow(mapping_from_excel), " rows")
  } else {
    message("Excel file not found: ", excel_path)
    mapping_from_excel <- data.frame(
      sku = character(),
      eby_item_id = character(),
      product_name = character(),
      product_line_id = character(),
      source = character()
    )
  }

  # ==========================================================================
  # SOURCE 3: Read from Google Sheets - "SKU to eBay Item Number" sheet
  # This is the PRIMARY mapping source with 47+ entries
  # ==========================================================================
  message("\n--- SOURCE 3: Google Sheets (SKU to eBay Item Number) ---")

  mapping_from_gsheet <- NULL

  tryCatch({
    # Get Google Sheet ID from config
    app_config_path <- file.path(g_project_root, "app_config.yaml")
    if (file.exists(app_config_path)) {
      app_config <- yaml::read_yaml(app_config_path)
      google_sheet_id <- app_config$googlesheet$product_profile

      if (!is.null(google_sheet_id) && google_sheet_id != "") {
        message("Reading from Google Sheets: ", google_sheet_id)

        googlesheets4::gs4_deauth()

        df_sku_eby_gsheet <- googlesheets4::read_sheet(
          ss = google_sheet_id,
          sheet = "SKU to eBay Item Number"
        )

        message("Google Sheets raw rows: ", nrow(df_sku_eby_gsheet))
        message("  Columns: ", paste(names(df_sku_eby_gsheet), collapse = ", "))

        # Standardize columns - use janitor to clean names first
        df_sku_eby_gsheet <- janitor::clean_names(df_sku_eby_gsheet, ascii = FALSE)
        message("  Cleaned columns: ", paste(names(df_sku_eby_gsheet), collapse = ", "))

        # Find matching columns dynamically
        col_names <- names(df_sku_eby_gsheet)
        sku_col <- col_names[grepl("^sku$", col_names, ignore.case = TRUE)][1]
        eby_col <- col_names[grepl("e_?bay", col_names, ignore.case = TRUE)][1]
        name_col <- col_names[grepl("^name$", col_names, ignore.case = TRUE)][1]
        cat_col <- col_names[grepl("product.*category|category", col_names, ignore.case = TRUE)][1]

        message("  Column mapping: SKU=", sku_col, ", eBay=", eby_col, ", Name=", name_col, ", Category=", cat_col)

        mapping_from_gsheet <- df_sku_eby_gsheet %>%
          transmute(
            sku = if (!is.na(sku_col)) as.character(.data[[sku_col]]) else NA_character_,
            eby_item_id = if (!is.na(eby_col)) as.character(.data[[eby_col]]) else NA_character_,
            product_name = if (!is.na(name_col)) as.character(.data[[name_col]]) else NA_character_,
            product_line_id = if (!is.na(cat_col)) as.character(.data[[cat_col]]) else NA_character_,
            source = "google_sheet_sku_eby"
          ) %>%
          filter(!is.na(sku) | !is.na(eby_item_id))

        message("Total from Google Sheets (SKU to eBay): ", nrow(mapping_from_gsheet), " rows")
      }
    }
  }, error = function(e) {
    message("  WARNING: Failed to read Google Sheets SKU-eBay mapping: ", e$message)
    mapping_from_gsheet <<- NULL
  })

  if (is.null(mapping_from_gsheet)) {
    mapping_from_gsheet <- data.frame(
      sku = character(),
      eby_item_id = character(),
      product_name = character(),
      product_line_id = character(),
      source = character()
    )
  }

  # ==========================================================================
  # STEP 4: UNION ALL and Deduplicate
  # ==========================================================================
  message("\n--- Merging Sources ---")

  # Combine all sources (3 sources now)
  df_combined <- bind_rows(
    mapping_from_profiles,    # Source 1: item_profile tables
    mapping_from_excel,       # Source 2: Excel file
    mapping_from_gsheet       # Source 3: Google Sheets "SKU to eBay Item Number"
  )

  message("Combined (before dedup): ", nrow(df_combined), " rows")

  # Deduplicate by (sku, eby_item_id), keep first occurrence
  # Priority: google_sheet_sku_eby (dedicated mapping) > google_sheet (item_profile) > excel
  # Using desc(source) puts google_sheet_sku_eby first
  df_product_mapping <- df_combined %>%
    arrange(desc(source)) %>%  # google_sheet_sku_eby > google_sheet > excel
    group_by(sku, eby_item_id) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    # Add surrogate key (DM_R052: support M:N)
    mutate(
      mapping_id = row_number(),
      amz_asin = NA_character_,    # Reserved for Amazon
      color_variant = NA_character_ # For M:N tracking
    ) %>%
    # Reorder columns with mapping_id first
    select(mapping_id, sku, eby_item_id, amz_asin, product_name, product_line_name = product_line_id, color_variant, source)

  message("After deduplication: ", nrow(df_product_mapping), " rows")

  # Source breakdown
  source_summary <- df_product_mapping %>%
    count(source) %>%
    arrange(desc(n))
  message("\nSource breakdown:")
  for (i in seq_len(nrow(source_summary))) {
    message("  ", source_summary$source[i], ": ", source_summary$n[i], " rows")
  }

  # ==========================================================================
  # STEP 4: Write to app_data.duckdb
  # Per DM_R052: mapping table is metadata, stored in app_data
  # ==========================================================================
  message("\n--- Writing to app_data ---")

  # Get database connection
  if (!exists("app_con") || is.null(app_con)) {
    db_path <- file.path(g_project_root, "data/app_data/app_data.duckdb")
    app_con <- DBI::dbConnect(duckdb::duckdb(), db_path)
    message("Created app_data connection: ", db_path)
  }

  # Write table
  DBI::dbWriteTable(
    app_con,
    "df_product_mapping",
    df_product_mapping,
    overwrite = TRUE
  )

  message(strrep("=", 60))
  message("SUCCESS: Product mapping imported (Multi-Source)")
  message("  Table: df_product_mapping")
  message("  Rows: ", nrow(df_product_mapping))
  message("  Columns: ", paste(names(df_product_mapping), collapse = ", "))
  message(strrep("=", 60))

  # Display sample data
  if (nrow(df_product_mapping) > 0) {
    message("\nSample data:")
    print(head(df_product_mapping, 5))
  }

}, error = function(e) {
  message("ERROR: Product mapping ETL failed")
  message("  Error: ", e$message)
  stop("0IM failed: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================
# Verify table was created correctly
tryCatch({
  test_count <- sql_read(app_con, "SELECT COUNT(*) as n FROM df_product_mapping")
  message("\nVerification: df_product_mapping has ", test_count$n, " rows")

  # Verify M:N support: check for duplicate SKUs or eBay IDs
  dup_check <- sql_read(app_con, "
    SELECT
      (SELECT COUNT(*) FROM (SELECT sku FROM df_product_mapping WHERE sku IS NOT NULL GROUP BY sku HAVING COUNT(*) > 1)) as dup_skus,
      (SELECT COUNT(*) FROM (SELECT eby_item_id FROM df_product_mapping WHERE eby_item_id IS NOT NULL GROUP BY eby_item_id HAVING COUNT(*) > 1)) as dup_eby
  ")
  message("M:N Check - SKUs with multiple eBay: ", dup_check$dup_skus)
  message("M:N Check - eBay with multiple SKUs: ", dup_check$dup_eby)

}, error = function(e) {
  warning("Verification failed: ", e$message)
})

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================
message("\n", strrep("=", 60))
message("Product Mapping ETL Summary (Multi-Source)")
message(strrep("=", 60))
message("  Source 1: transformed_data.duckdb/df_all_item_profile_* (item_profile sheets)")
message("  Source 2: SKUtoeBay number.xlsx (Excel)")
message("  Source 3: Google Sheets 'SKU to eBay Item Number' (primary mapping)")
message("  Target: app_data.duckdb/df_product_mapping")
message("  Schema: DM_R052 (M:N with surrogate key)")
message("  Status: Complete")

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================
autodeinit()
