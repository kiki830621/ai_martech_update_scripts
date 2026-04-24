# amz_ETL_product_master_2TR.R - Validate product master (catalogue layer)
# Implements qef-product-master-redesign spectra change task 3.3.
#
# 2TR Phase: PK uniqueness on (amz_asin, marketplace) + ASIN format check.
# Source: staged_data.df_amz_product_master___staged
# Output: transformed_data.df_amz_product_master___transformed

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

staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)

message("INITIALIZE: amz_ETL_product_master_2TR started")

ASIN_PATTERN <- "^[A-Z0-9]{10}$"

tryCatch({
  source_table <- "df_amz_product_master___staged"
  output_table <- "df_amz_product_master___transformed"

  if (!source_table %in% dbListTables(staged_data)) {
    stop(sprintf("Source table %s not found; run 1ST first", source_table))
  }

  df <- DBI::dbReadTable(staged_data, source_table)
  n_in <- nrow(df)
  message(sprintf("MAIN: Read %d rows from %s", n_in, source_table))

  if (n_in > 0) {
    pk <- paste(df$amz_asin, df$marketplace, sep = "\x1f")
    dup_keys <- pk[duplicated(pk)]
    if (length(dup_keys) > 0) {
      stop(sprintf(
        "PK violation: %d duplicate (amz_asin, marketplace) pairs in df_amz_product_master. Sample: %s",
        length(dup_keys), paste(head(unique(dup_keys), 3), collapse = "; ")
      ))
    }

    if ("amz_asin" %in% names(df)) {
      bad_asin <- df$amz_asin[!is.na(df$amz_asin) & !grepl(ASIN_PATTERN, df$amz_asin)]
      if (length(bad_asin) > 0) {
        warning(sprintf(
          "ASIN format check: %d ASINs do not match ^[A-Z0-9]{10}$. Sample: %s",
          length(bad_asin), paste(head(bad_asin, 3), collapse = ", ")
        ), call. = FALSE)
      }
    }
  }

  if (dbExistsTable(transformed_data, output_table)) dbRemoveTable(transformed_data, output_table)
  dbWriteTable(transformed_data, output_table, df, overwrite = TRUE)
  message(sprintf("MAIN: Wrote %d rows to %s", nrow(df), output_table))

  script_success <- TRUE
}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

if (script_success) {
  test_passed <- TRUE
  message("TEST: validation gates passed in MAIN")
}

DBI::dbDisconnect(staged_data)
DBI::dbDisconnect(transformed_data)
message(sprintf("STATUS: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
autodeinit()
