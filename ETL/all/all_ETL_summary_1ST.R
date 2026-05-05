#' @file all_ETL_summary_1ST.R
#' @principle MP163 Gate 1 — Always-Runnable Pipeline
#' @principle MP137 No Hardcoded Project-Specific Content
#' @principle SO_P010 Config-Driven Customization
#' @principle DM_R028 ETL Phase Separation
#' @description
#'   Imports SKU→ASIN mapping (product_property_dictionary) from a
#'   company-specific Google Sheet. MAMBA/kitchenMAMA opt-in via
#'   app_config.yaml > metadata_sources.sku_to_asin.enabled.
#'
#'   Per MP163 Gate 1 (Always-Runnable Pipeline): non-MAMBA companies
#'   (QEF_DESIGN/D_RACING/WISER/etc.) graceful-skip without blocking the
#'   pipeline. Auth failures (non-interactive runs without cached token)
#'   also graceful-skip. See #576.
#'
#'   Historical bug fixed: prior version stored read result in a local
#'   variable that autodeinit() destroyed — table never reached
#'   raw_data.duckdb. This refactor persists when read succeeds.

# 1. INITIALIZE
autoinit()

# Ensure g_project_root / g_global_scripts_path are set (APP_MODE fallback)
if (!exists("g_project_root") || is.null(g_project_root)) {
  g_project_root <- getwd()
  message("Derived g_project_root: ", g_project_root)
}
if (!exists("g_global_scripts_path") || is.null(g_global_scripts_path)) {
  g_global_scripts_path <- file.path(g_project_root, "scripts", "global_scripts")
  message("Derived g_global_scripts_path: ", g_global_scripts_path)
}

# Read config gate (SO_P010 + MP137)
app_config_path <- file.path(g_project_root, "app_config.yaml")
if (!file.exists(app_config_path)) {
  message("all_ETL_summary_1ST: app_config.yaml not found — skipping (MP163 Gate 1).")
  q("no", status = 0, runLast = FALSE)
}

app_config <- yaml::read_yaml(app_config_path)
sku_to_asin_cfg <- app_config$metadata_sources$sku_to_asin

if (is.null(sku_to_asin_cfg) || !isTRUE(sku_to_asin_cfg$enabled)) {
  # MP163 Gate 1: non-MAMBA companies skip silently.
  # MAMBA/kitchenMAMA enable via:
  #   metadata_sources:
  #     sku_to_asin:
  #       enabled: true
  #       sheet_id: "..."
  #       sheet_name: "SKUtoASIN"
  message("all_ETL_summary_1ST: metadata_sources.sku_to_asin not enabled — skipping (MAMBA-only ETL, MP163 Gate 1).")
  q("no", status = 0, runLast = FALSE)
}

if (is.null(sku_to_asin_cfg$sheet_id) || !nzchar(sku_to_asin_cfg$sheet_id)) {
  message("all_ETL_summary_1ST: sku_to_asin.sheet_id missing — skipping (MP163 Gate 1).")
  q("no", status = 0, runLast = FALSE)
}

sheet_id <- sku_to_asin_cfg$sheet_id
sheet_name <- if (!is.null(sku_to_asin_cfg$sheet_name)) sku_to_asin_cfg$sheet_name else "SKUtoASIN"

message("all_ETL_summary_1ST: enabled — sheet_id=", sheet_id, ", sheet_name=", sheet_name)

# 2. MAIN
error_occurred <- FALSE

product_property_dictionary <- tryCatch({
  googlesheet_con <- googlesheets4::as_sheets_id(sheet_id)
  googlesheets4::read_sheet(googlesheet_con, sheet = sheet_name)
}, error = function(e) {
  # MP163 Gate 1: auth or read failure must not abort the pipeline.
  # Common cause: gs4_auth() can't get credentials in non-interactive R session
  # (no cached token, no GOOGLE_APPLICATION_CREDENTIALS / service account JSON).
  # Fix: cache token interactively once OR set up service account.
  message("all_ETL_summary_1ST: Google Sheets read failed — skipping (MP163 Gate 1).")
  message("  Error: ", e$message)
  message("  Common fix: run interactively once with `googlesheets4::gs4_auth()` to cache token,")
  message("              OR configure service account via gs4_auth(path = '/path/to/sa.json').")
  NULL
})

if (is.null(product_property_dictionary)) {
  q("no", status = 0, runLast = FALSE)
}

message("  Rows fetched: ", nrow(product_property_dictionary))

# Persist to raw_data.duckdb (was missing in legacy version — variable was
# created and immediately destroyed by autodeinit()).
if (!exists("raw_data") || !inherits(raw_data, "DBIConnection")) {
  raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
  connection_created_raw <- TRUE
}

tryCatch({
  DBI::dbWriteTable(
    conn = raw_data,
    name = "product_property_dictionary",
    value = as.data.frame(product_property_dictionary),
    overwrite = TRUE
  )
  message("Written to raw_data.duckdb table: product_property_dictionary")
}, error = function(e) {
  message("all_ETL_summary_1ST: failed to persist product_property_dictionary: ", e$message)
  error_occurred <<- TRUE
})

# 3. TEST
if (!error_occurred) {
  row_count <- DBI::dbGetQuery(
    raw_data,
    "SELECT COUNT(*) AS n FROM product_property_dictionary"
  )$n
  message("Verification: ", row_count, " rows in raw_data.product_property_dictionary")
}

# 4. DEINITIALIZE
if (exists("connection_created_raw") && isTRUE(connection_created_raw) &&
    exists("raw_data") && inherits(raw_data, "DBIConnection")) {
  DBI::dbDisconnect(raw_data, shutdown = TRUE)
}

autodeinit()
