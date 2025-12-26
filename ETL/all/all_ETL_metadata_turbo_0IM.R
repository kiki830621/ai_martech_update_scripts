#' @file all_ETL_metadata_turbo_0IM.R
#' @requires DBI
#' @requires duckdb
#' @requires googlesheets4
#' @requires yaml
#' @principle MP137 No Hardcoded Project-Specific Content
#' @principle SO_P010 Config-Driven Customization
#' @principle SO_P015 No Hardcoded Lookup Tables
#' @principle DM_R004 Section 6: Three-Tier Data Architecture
#' @principle MP064 ETL/Derivation Separation
#' @author Claude
#' @date 2025-12-14
#' @modified 2025-12-14 - Phase 5: Corrected to write to MAMBA app_data (company tier)
#' @title Metadata Turbo ETL - Import from Google Sheets
#' @description
#'   Imports turbocharger product attribute metadata from Google Sheets.
#'   This data provides Chinese/English display name mappings for
#'   fn_generate_display_name.R to query.
#'
#'   Data Flow:
#'   Google Sheets (metadata_Turbo) -> {company}/data/app_data/app_data.duckdb
#'
#'   The configuration source is defined in:
#'   global_scripts/03_config/metadata_sources.yaml
#'
#'   Per DM_R004 Section 6 (Three-Tier Architecture):
#'   - df_metadata_turbo is MAMBA-specific (company tier)
#'   - It belongs in {company}/data/app_data/, NOT global_scripts/global_data/
#'   - global_data/ is for data shared by ALL companies

# 1. INITIALIZE
autoinit()

# Ensure g_project_root is available (fallback for APP_MODE)
if (!exists("g_project_root") || is.null(g_project_root)) {
  g_project_root <- getwd()
  message("Derived g_project_root: ", g_project_root)
}

# Ensure g_global_scripts_path is available (fallback for APP_MODE)
if (!exists("g_global_scripts_path") || is.null(g_global_scripts_path)) {
  g_global_scripts_path <- file.path(g_project_root, "scripts", "global_scripts")
  message("Derived g_global_scripts_path: ", g_global_scripts_path)
}

# Load configuration (SO_P010: Config-Driven)
config_path <- file.path(
  g_global_scripts_path,
  "03_config",
  "metadata_sources.yaml"
)

if (!file.exists(config_path)) {
  stop("Configuration file not found: ", config_path)
}

metadata_config <- yaml::read_yaml(config_path)
turbo_schema <- metadata_config$metadata_schema$turbo

if (is.null(turbo_schema)) {
  stop("metadata_schema.turbo configuration not found in metadata_sources.yaml")
}

# Also load company-specific settings from app_config.yaml (SO_P016)
app_config_path <- file.path(g_project_root, "app_config.yaml")
if (!file.exists(app_config_path)) {
  stop("app_config.yaml not found: ", app_config_path)
}

app_config <- yaml::read_yaml(app_config_path)
turbo_company <- app_config$metadata_sources$turbo

if (is.null(turbo_company) || !isTRUE(turbo_company$enabled)) {
  stop("metadata_sources.turbo not enabled in app_config.yaml")
}

# Merge schema + company settings
turbo_config <- modifyList(turbo_schema, turbo_company)

message("Loaded configuration for: ", turbo_config$description)
message("  Sheet ID: ", turbo_config$sheet_id)
message("  Sheet Name: ", turbo_config$sheet_name)

# Connect to COMPANY database (DM_R004 Section 6: Three-Tier Architecture)
# df_metadata_turbo is MAMBA-specific, belongs in company tier (app_data/)
# NOT in global_data/ which is for ALL companies

# Determine target path based on target_scope
if (turbo_config$target_scope == "company" || turbo_config$target_database == "app_data") {
  # Company tier: {project}/data/app_data/
  app_db_path <- file.path(
    g_project_root,  # MAMBA project root
    "data", "app_data",
    turbo_config$target_db_file  # "app_data.duckdb"
  )
} else if (turbo_config$target_scope == "universal" || turbo_config$target_database == "global_data") {
  # Universal tier: global_scripts/global_data/ (for ALL companies)
  app_db_path <- file.path(
    g_global_scripts_path,
    "global_data",
    turbo_config$target_db_file
  )
} else {
  stop("Unknown target_scope: ", turbo_config$target_scope)
}

if (!file.exists(app_db_path)) {
  stop("Target database not found: ", app_db_path)
}

app_con <- DBI::dbConnect(duckdb::duckdb(), app_db_path)
connection_created <- TRUE
message("Connected to ", turbo_config$target_scope, " database: ", app_db_path)

# Initialize tracking
error_occurred <- FALSE
test_passed <- FALSE

# 2. MAIN
tryCatch({
  message("Fetching metadata from Google Sheets...")

  # Read from Google Sheets (SO_P015: External source, not hardcoded)
  df_metadata_turbo <- googlesheets4::read_sheet(
    ss = turbo_config$sheet_id,
    sheet = turbo_config$sheet_name
  )

  message("  Rows fetched: ", nrow(df_metadata_turbo))
  message("  Original columns: ", paste(names(df_metadata_turbo), collapse = ", "))

  # Transform columns to match expected schema
  # Google Sheet: 屬性, attribute, 定義, 水準, 屬性水準-內容0-6
  # Expected: predictor_pattern, display_name_zh, display_name_en, display_category

  df_metadata_turbo <- df_metadata_turbo %>%
    dplyr::transmute(
      # Convert "Seller Name" → "seller_name" for predictor matching
      predictor_pattern = tolower(gsub(" ", "_", attribute)),
      # Chinese display name from 屬性 column
      display_name_zh = `屬性`,
      # English display name from attribute column
      display_name_en = attribute,
      # Default category (can be enhanced later)
      display_category = "product_attribute"
    ) %>%
    # Remove rows with NA in critical columns
    dplyr::filter(!is.na(predictor_pattern), predictor_pattern != "")

  # Convert to data.frame to avoid tibble/list-column issues with DuckDB
  df_metadata_turbo <- as.data.frame(df_metadata_turbo)

  message("  Transformed columns: ", paste(names(df_metadata_turbo), collapse = ", "))
  message("  Final rows: ", nrow(df_metadata_turbo))
  message("  Sample predictor_pattern: ",
          paste(head(df_metadata_turbo$predictor_pattern, 5), collapse = ", "))

  # Write to target database (DM_R004 Section 6: Three-Tier Architecture)
  # MP137: All mappings in DB, not code
  target_table <- turbo_config$target_table

  DBI::dbWriteTable(
    conn = app_con,
    name = target_table,
    value = as.data.frame(df_metadata_turbo),
    overwrite = TRUE
  )

  message("Written to ", turbo_config$target_scope, " database table: ", target_table)

  # Verify write
  row_count <- DBI::dbGetQuery(
    app_con,
    sprintf("SELECT COUNT(*) as n FROM %s", target_table)
  )$n

  if (row_count == nrow(df_metadata_turbo)) {
    message("Verification passed: ", row_count, " rows in database")
    test_passed <- TRUE
  } else {
    warning("Row count mismatch: expected ", nrow(df_metadata_turbo),
            ", got ", row_count)
    error_occurred <- TRUE
  }

}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <<- TRUE
})

# 3. TEST
if (!error_occurred && test_passed) {
  tryCatch({
    # Sample query test
    sample <- DBI::dbGetQuery(
      app_con,
      sprintf("SELECT * FROM %s LIMIT 3", turbo_config$target_table)
    )
    message("Sample data:")
    print(sample)
  }, error = function(e) {
    message("Test query failed: ", e$message)
  })
}

# 4. DEINITIALIZE
tryCatch({
  if (exists("connection_created") && connection_created &&
      exists("app_con") && inherits(app_con, "DBIConnection")) {
    DBI::dbDisconnect(app_con)
    message("Database connection closed")
  }

  if (test_passed && !error_occurred) {
    message("ETL completed successfully")
    final_status <- TRUE
  } else {
    message("ETL completed with issues")
    final_status <- FALSE
  }

}, error = function(e) {
  message("Error in DEINITIALIZE: ", e$message)
  final_status <- FALSE
}, finally = {
  message("Script completed at ", Sys.time())
})

if (exists("final_status")) {
  final_status
} else {
  FALSE
}

autodeinit()
