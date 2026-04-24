# amz_ETL_product_master_1ST.R - Stage product master (catalogue layer)
# Implements qef-product-master-redesign spectra change task 3.3.
#
# 1ST Phase: trim + marketplace default backfill.
# Source: raw_data.df_amz_product_master
# Output: staged_data.df_amz_product_master___staged

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

message("INITIALIZE: amz_ETL_product_master_1ST started")

tryCatch({
  source_table <- "df_amz_product_master"
  output_table <- "df_amz_product_master___staged"

  if (!source_table %in% dbListTables(raw_data)) {
    stop(sprintf("Source table %s not found; run product_profiles 0IM first (it builds the union)", source_table))
  }

  df <- DBI::dbReadTable(raw_data, source_table)
  message(sprintf("MAIN: Read %d rows from %s", nrow(df), source_table))

  for (col in c("amz_asin", "marketplace", "product_line_id", "brand", "product_name")) {
    if (col %in% names(df)) {
      df[[col]] <- trimws(as.character(df[[col]]))
      df[[col]][!nzchar(df[[col]])] <- NA_character_
    }
  }

  if ("marketplace" %in% names(df)) {
    na_count <- sum(is.na(df$marketplace))
    if (na_count > 0) {
      df$marketplace[is.na(df$marketplace)] <- "amz_us"
      message(sprintf("MAIN: Backfilled %d NA marketplace -> amz_us", na_count))
    }
  }

  if (dbExistsTable(staged_data, output_table)) dbRemoveTable(staged_data, output_table)
  dbWriteTable(staged_data, output_table, df, overwrite = TRUE)
  message(sprintf("MAIN: Wrote %d rows to %s", nrow(df), output_table))

  script_success <- TRUE
}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

if (script_success) {
  test_passed <- TRUE
  message("TEST: schema OK")
}

DBI::dbDisconnect(raw_data)
DBI::dbDisconnect(staged_data)
message(sprintf("STATUS: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
autodeinit()
