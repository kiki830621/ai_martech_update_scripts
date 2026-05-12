# amz_ETL_sales_2TR.R - Amazon Sales Data Transformation
# ==============================================================================
# Following MP064 v2.2: ETL-Derivation Separation (post-glue canonical baseline)
# Following DM_R028 v2.3: ETL Data Type Separation Rule
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
#
# Post-glue era: schema mapping (legacy column names → canonical) happens at
# bridge layer (sales.bridge.yaml). 2TR scope (post-trim per legacy-etl-
# deprecation-playbook Step 4): post-canonical transformation only:
#   - Date string parse (canonical order_date is VARCHAR Excel-serial → POSIXct)
#   - ASIN-as-sku product_id backfill (when product_id sentinel UNKNOWN_PRODUCT
#     AND amz_seller_sku is ASIN-shaped, restore product_id from sku per #472)
#   - Defense NA filter on order_date (bridge pre_filter handles most)
#   - Coverage audit table append for ASIN-fallback rows (MP163 §3)
#
# NO type coercion (bridge sets canonical types: INTEGER quantity, NUMERIC
# unit_price/total_amount, VARCHAR identifiers).
# NO Cancelled/Pending filter (bridge pre_filter handles).
# NO schema mapping (bridge handles).
#
# Input:  staged_data.duckdb (df_amz_sales___staged) — canonical names from 1ST
# Output: transformed_data.duckdb (df_amz_sales___transformed)
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
script_name <- "amz_ETL_sales_2TR"
script_version <- "2.0.0"  # post-glue era: post-canonical transformation only

message(strrep("=", 80))
message("INITIALIZE: Starting Amazon Sales Transformation (2TR Phase, post-glue era)")
message(sprintf("INITIALIZE: Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message(strrep("=", 80))

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
OPERATION_MODE <- "UPDATE_MODE"
autoinit()

message("INITIALIZE: Loading required libraries...")
library(DBI)
library(duckdb)
library(data.table)

source(file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R"))

# Excel-serial-or-ISO datetime parser (canonical order_date is VARCHAR from bridge
# because schema coercion `format(., '%Y-%m-%d') from POSIXct` is unsupported op
# in deterministic interpreter — order_date passes through as source string,
# typically Excel serial number text. Per #631 / fix-glue-layer-infra-blockers.)
parse_mixed_datetime <- function(values, tz = "UTC") {
  raw_chr <- trimws(as.character(values))
  out <- rep(as.POSIXct(NA, tz = tz), length(raw_chr))
  if (length(raw_chr) == 0) return(out)

  is_blank <- is.na(raw_chr) | raw_chr == ""
  if (all(is_blank)) return(out)

  numeric_vals <- suppressWarnings(as.numeric(raw_chr))
  is_excel_serial <- !is_blank & !is.na(numeric_vals) & numeric_vals > 20000 & numeric_vals < 80000
  if (any(is_excel_serial)) {
    out[is_excel_serial] <- as.POSIXct(
      (numeric_vals[is_excel_serial] - 25569) * 86400,
      origin = "1970-01-01", tz = tz
    )
  }

  need_parse <- !is_blank & is.na(out)
  if (any(need_parse)) {
    parse_chr <- gsub("Z$", "", raw_chr[need_parse])
    parsed <- suppressWarnings(as.POSIXct(
      parse_chr, tz = tz,
      tryFormats = c(
        "%Y-%m-%d %H:%M:%OS", "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%OS", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%OS", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
        "%m/%d/%Y %H:%M:%OS", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y",
        "%d/%m/%Y %H:%M:%OS", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"
      )
    ))
    out[need_parse] <- parsed
  }
  out
}

message("INITIALIZE: Connecting to databases...")
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)
message(sprintf("INITIALIZE: Read from: %s", db_path_list$staged_data))
message(sprintf("INITIALIZE: Write to: %s", db_path_list$transformed_data))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: Initialization completed (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: Starting Amazon Sales Transformation (post-canonical)...")
main_start_time <- Sys.time()

tryCatch({
  input_table <- "df_amz_sales___staged"
  output_table <- "df_amz_sales___transformed"

  if (!dbExistsTable(staged_data, input_table)) {
    stop(sprintf("Required table %s not found. Run amz_ETL_sales_1ST first.", input_table))
  }

  # Load staged data (canonical names from 1ST)
  message(sprintf("MAIN: Step 1/3 - Loading %s...", input_table))
  load_start <- Sys.time()
  df_staged <- sql_read(staged_data, sprintf("SELECT * FROM %s", input_table))
  n_staged <- nrow(df_staged)
  message(sprintf("MAIN: Loaded %d records (%.2fs)",
                  n_staged, as.numeric(Sys.time() - load_start, units = "secs")))

  if (n_staged == 0) {
    stop("No staged data found - cannot proceed with transformation")
  }

  dt <- as.data.table(df_staged)

  # Step 2: Parse canonical order_date string → POSIXct
  # Canonical order_date is VARCHAR (Excel serial text from bridge) per #631
  # unsupported coercion fallthrough. Parse to POSIXct for downstream DRV.
  message("MAIN: Step 2/3 - Parsing canonical order_date...")
  date_start <- Sys.time()

  if ("order_date" %in% names(dt)) {
    dt[, order_date := parse_mixed_datetime(order_date, tz = "UTC")]
    n_na_date <- sum(is.na(dt$order_date))
    if (n_na_date > 0) {
      message(sprintf("    Warning: %d records with unparseable order_date", n_na_date))
    }
    message("    order_date → POSIXct")
  }

  message(sprintf("MAIN: Date parse done (%.2fs)",
                  as.numeric(Sys.time() - date_start, units = "secs")))

  # Step 3: ASIN-as-sku backfill + final filter + write
  # Per #472: when product_id is sentinel "UNKNOWN_PRODUCT" AND amz_seller_sku
  # is ASIN-shaped (^[A-Z0-9]{10}$), use amz_seller_sku as product_id.
  # Non-destructive (MP154-compliant): never overwrites valid product_id.
  message("MAIN: Step 3/3 - ASIN backfill, defense filter, and write...")
  bf_start <- Sys.time()

  asin_fallback_audit_rows <- data.frame(
    source_file = character(0), source_row_id = character(0),
    reason = character(0), detected_via = character(0),
    stringsAsFactors = FALSE
  )

  if (all(c("product_id", "amz_seller_sku") %in% names(dt))) {
    is_sentinel <- dt$product_id == "UNKNOWN_PRODUCT" |
                   is.na(dt$product_id) |
                   nchar(trimws(as.character(dt$product_id))) == 0
    sku_chr <- as.character(dt$amz_seller_sku)
    is_asin_shape <- !is.na(sku_chr) & grepl("^[A-Z0-9]{10}$", sku_chr)
    backfill_idx <- is_sentinel & is_asin_shape

    n_backfilled <- sum(backfill_idx)
    if (n_backfilled > 0L) {
      # Capture audit rows BEFORE backfill (MP163 §3 surface-to-attention)
      asin_fallback_audit_rows <- data.frame(
        source_file = "df_amz_sales___staged",
        source_row_id = as.character(dt$amz_amazon_order_id[backfill_idx]),
        reason = "asin_as_sku_fallback",
        detected_via = "amz_ETL_sales_2TR v2.0 post-glue backfill",
        stringsAsFactors = FALSE
      )
      # Apply backfill
      dt[backfill_idx, product_id := amz_seller_sku]
      message(sprintf("    Backfilled %d product_id from amz_seller_sku (ASIN-shape sku fallback per #472)",
                      n_backfilled))
    }
  }

  # Defense filter: drop rows with NA order_date (bridge pre_filter already
  # excludes Cancelled/Pending + quantity=0; this is final guard.)
  n_before <- nrow(dt)
  dt <- dt[!is.na(order_date)]
  n_removed <- n_before - nrow(dt)
  if (n_removed > 0) {
    message(sprintf("    Removed %d records with NA order_date (parser fail)", n_removed))
  }

  # Drop existing table if present
  if (dbExistsTable(transformed_data, output_table)) {
    dbRemoveTable(transformed_data, output_table)
  }

  dbWriteTable(transformed_data, output_table, as.data.frame(dt), overwrite = TRUE)

  actual_count <- sql_read(transformed_data,
    sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
  message(sprintf("MAIN: Stored %d records in %s (%.2fs)",
                  actual_count, output_table,
                  as.numeric(Sys.time() - bf_start, units = "secs")))

  # Append asin-as-sku-fallback audit rows (MP163 §3)
  if (nrow(asin_fallback_audit_rows) > 0L) {
    audit_table <- "df_amazon_sales_coverage_audit"
    tryCatch({
      if (dbExistsTable(transformed_data, audit_table)) {
        dbWriteTable(transformed_data, audit_table, asin_fallback_audit_rows,
                     append = TRUE, row.names = FALSE)
      } else {
        dbWriteTable(transformed_data, audit_table, asin_fallback_audit_rows,
                     overwrite = TRUE, row.names = FALSE)
      }
      message(sprintf("MAIN: Appended %d row(s) to %s (asin_as_sku_fallback)",
                      nrow(asin_fallback_audit_rows), audit_table))
    }, error = function(e) {
      warning(sprintf("Failed to write coverage audit: %s", e$message),
              call. = FALSE)
    })
  }

  script_success <- TRUE
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: Transformation completed (%.2fs). %d → %d records",
                  main_elapsed, n_staged, actual_count))

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: Starting transformation verification...")
test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    output_table <- "df_amz_sales___transformed"

    if (!dbExistsTable(transformed_data, output_table)) stop("Table does not exist")
    message("TEST: Table exists")

    row_count <- sql_read(transformed_data,
      sprintf("SELECT COUNT(*) as n FROM %s", output_table))$n
    if (row_count == 0) stop("Table is empty")
    message(sprintf("TEST: %d rows", row_count))

    # order_date is POSIXct after Step 2 parse
    null_dates <- sql_read(transformed_data, sprintf(
      "SELECT COUNT(*) as n FROM %s WHERE order_date IS NULL", output_table))$n
    if (null_dates == row_count) stop("All order_date values are NULL after parse")
    message(sprintf("TEST: %d valid order_dates", row_count - null_dates))

    test_passed <- TRUE
    message(sprintf("TEST: Verification passed (%.2fs)",
                    as.numeric(Sys.time() - test_start_time, units = "secs")))

  }, error = function(e) {
    test_passed <<- FALSE
    message(sprintf("TEST: Failed: %s", e$message))
  })
} else {
  message("TEST: Skipped due to main failure")
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================

message(strrep("=", 80))
message("SUMMARIZE: AMAZON SALES TRANSFORMATION (2TR, post-glue era)")
message(strrep("=", 80))
message(sprintf("Platform: amz | Phase: 2TR"))
message(sprintf("Total time: %.2fs", as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
message(sprintf("Compliance: MP064 v2.2, DM_R028 v2.3, DEV_R032"))
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

message("DEINITIALIZE: Cleaning up...")
if (exists("staged_data") && inherits(staged_data, "DBIConnection") && DBI::dbIsValid(staged_data)) {
  DBI::dbDisconnect(staged_data)
}
if (exists("transformed_data") && inherits(transformed_data, "DBIConnection") && DBI::dbIsValid(transformed_data)) {
  DBI::dbDisconnect(transformed_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
