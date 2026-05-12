# amz_ETL_company_product_master_1ST.R - Stage company product master
# Implements qef-product-master-redesign spectra change task 2.4.
#
# 1ST Phase (Staging): trim, type coercion, marketplace default backfill.
# Source: raw_data.df_amz_company_product_master
# Output: staged_data.df_amz_company_product_master___staged

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
if (is.na(sql_read_path)) stop("fn_sql_read.R not found")
source(sql_read_path)

script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

autoinit()

raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)

message("INITIALIZE: amz_ETL_company_product_master_1ST started")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  source_table <- "df_amz_company_product_master"
  output_table <- "df_amz_company_product_master___staged"

  if (!source_table %in% dbListTables(raw_data)) {
    stop(sprintf("Source table %s not found; run 0IM first", source_table))
  }

  df <- DBI::dbReadTable(raw_data, source_table)
  message(sprintf("MAIN: Read %d rows from %s", nrow(df), source_table))

  # --- Trim character fields ----------------------------------------------
  char_cols <- c("sku", "marketplace", "amz_asin", "product_line_id",
                 "brand", "product_name", "status", "launch_date",
                 "source_origin")
  for (col in intersect(char_cols, names(df))) {
    df[[col]] <- trimws(as.character(df[[col]]))
    df[[col]][!nzchar(df[[col]])] <- NA_character_
  }

  # --- Marketplace default backfill (D2) ----------------------------------
  if ("marketplace" %in% names(df)) {
    na_count <- sum(is.na(df$marketplace))
    if (na_count > 0) {
      df$marketplace[is.na(df$marketplace)] <- "amz_us"
      message(sprintf("MAIN: Backfilled %d NA marketplace -> amz_us", na_count))
    }
  }

  # --- Numeric coercion ---------------------------------------------------
  for (col in c("cost", "profit")) {
    if (col %in% names(df)) {
      df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
    }
  }

  # --- Date parse ---------------------------------------------------------
  if ("launch_date" %in% names(df)) {
    df$launch_date <- suppressWarnings(as.Date(df$launch_date))
  }

  if (dbExistsTable(staged_data, output_table)) {
    dbRemoveTable(staged_data, output_table)
  }
  dbWriteTable(staged_data, output_table, df, overwrite = TRUE)
  message(sprintf("MAIN: Wrote %d rows to %s", nrow(df), output_table))

  script_success <- TRUE
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
    output_table <- "df_amz_company_product_master___staged"
    cols <- dbListFields(staged_data, output_table)
    if (!"marketplace" %in% cols || !"sku" %in% cols) {
      stop("Required columns missing")
    }
    test_passed <- TRUE
    message("TEST: schema OK")
  }, error = function(e) {
    test_passed <<- FALSE
    message(sprintf("TEST: %s", e$message))
  })
}

# ==============================================================================
# 4-5. DEINITIALIZE
# ==============================================================================

DBI::dbDisconnect(raw_data)
DBI::dbDisconnect(staged_data)

message(sprintf("STATUS: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))

autodeinit()
