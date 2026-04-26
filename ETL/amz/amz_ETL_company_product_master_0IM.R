# amz_ETL_company_product_master_0IM.R - Import canonical company product master
# ==============================================================================
# Following MP064: ETL-Derivation Separation
# Following DM_R028: ETL Data Type Separation
# Following DM_R064: Column-name access over positional access
# Following IC_P002: Cross-company verification (resolver tolerates per-company gaps)
# Implements qef-product-master-redesign spectra change tasks 2.3.
#
# ETL Phase 0IM (Import):
#   - Multi-source resolve (Gsheet `company_product_master` tab + xlsx)
#   - Hybrid conflict arbitration (D4)
#   - Output: raw_data.duckdb → df_amz_company_product_master
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
script_name <- "amz_ETL_company_product_master_0IM"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: Starting Company Product Master Import (0IM Phase)")
message(sprintf("INITIALIZE: Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message(strrep("=", 80))

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

needgoogledrive <- TRUE
OPERATION_MODE <- "UPDATE_MODE"
autoinit()

source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "amz", "fn_arbitrate_product_master_conflicts.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "amz", "fn_resolve_company_product_master.R"))
# MP155 Status Surface (qef-gsheet-three-surface-redesign):
source(file.path(GLOBAL_DIR, "05_etl_utils", "amz", "fn_detect_anomalies.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "amz", "fn_write_status_gsheet.R"))

library(DBI)
library(duckdb)

raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
message(sprintf("INITIALIZE: Using: %s", db_path_list$raw_data))

# ==============================================================================
# 2. MAIN
# ==============================================================================

main_start_time <- Sys.time()

tryCatch({
  message("MAIN: Loading app_config + rawdata_dir...")

  # Locate app_config.yaml (lives at company project root, not under shared/)
  app_config_path <- if (exists("CONFIG_PATH", inherits = TRUE)) {
    get("CONFIG_PATH", inherits = TRUE)
  } else {
    "app_config.yaml"
  }
  app_config <- yaml::read_yaml(app_config_path)

  rawdata_dir <- RAW_DATA_DIR %||% file.path(
    APP_DIR, "data", "local_data", paste0("rawdata_", COMPANY_CODE)
  )

  message(sprintf("MAIN: rawdata_dir=%s", rawdata_dir))
  message("MAIN: Resolving company product master from 4 sources...")

  df_master <- resolve_company_product_master(app_config, rawdata_dir)

  n_rows <- nrow(df_master)
  message(sprintf("MAIN: Resolved %d company product master rows", n_rows))

  if (n_rows > 0) {
    # Source provenance summary (D4 audit log)
    src_summary <- table(df_master$source_origin)
    for (src in names(src_summary)) {
      message(sprintf("MAIN: source_origin=%s rows=%d", src, src_summary[[src]]))
    }
  }

  output_table <- "df_amz_company_product_master"
  if (dbExistsTable(raw_data, output_table)) {
    dbRemoveTable(raw_data, output_table)
  }
  dbWriteTable(raw_data, output_table, as.data.frame(df_master), overwrite = TRUE)
  message(sprintf("MAIN: Wrote %d rows to %s", n_rows, output_table))

  # ----- MP155 Status Surface writeback -----
  # When app_config.yaml has platforms.amz.etl_sources.status_gsheet section,
  # publish anomalies / missing_master / drift to the Status Gsheet.
  # Silently skipped when the section is absent or sheet_id="TBD". Any API
  # failure surfaces as warning() and does NOT abort the ETL.
  status_cfg <- app_config$platforms$amz$etl_sources$status_gsheet
  if (!is.null(status_cfg)) {
    message("MAIN: Publishing MP155 Status Surface ...")
    anom_out <- detect_anomalies(df_master)
    write_status_gsheet(
      status_gsheet_config = status_cfg,
      anomalies = anom_out$anomalies,
      missing_master = anom_out$missing_master,
      drift = anom_out$drift
    )
  }

  script_success <- TRUE
  message(sprintf("MAIN: completed (%.2fs)",
                  as.numeric(Sys.time() - main_start_time, units = "secs")))

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

if (script_success) {
  tryCatch({
    output_table <- "df_amz_company_product_master"
    if (!dbExistsTable(raw_data, output_table)) stop("Table does not exist")

    cols <- dbListFields(raw_data, output_table)
    required <- c("sku", "marketplace", "amz_asin", "product_line_id",
                  "brand", "source_origin")
    missing <- setdiff(required, cols)
    if (length(missing) > 0) {
      stop(sprintf("Missing columns: %s", paste(missing, collapse = ", ")))
    }

    message("TEST: Required columns present")
    test_passed <- TRUE

  }, error = function(e) {
    test_passed <<- FALSE
    message(sprintf("TEST: Failed: %s", e$message))
  })
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================

message(strrep("=", 80))
message("SUMMARIZE: COMPANY PRODUCT MASTER IMPORT (0IM)")
message(strrep("=", 80))
message(sprintf("Total time: %.2fs",
                as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s",
                if (script_success && test_passed) "SUCCESS" else "FAILED"))
message(sprintf("Compliance: MP064, DM_R028, DM_R064, IC_P002, qef-product-master-redesign"))
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

if (exists("raw_data") && inherits(raw_data, "DBIConnection") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
