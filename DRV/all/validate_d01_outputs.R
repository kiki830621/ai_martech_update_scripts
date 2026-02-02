#!/usr/bin/env Rscript
# validate_d01_outputs.R
#
# PURPOSE:
#   Post-run validation for D01 outputs with detailed table/row count checks.
#
# USAGE:
#   Rscript scripts/update_scripts/DRV/all/validate_d01_outputs.R --platforms=cbz,eby
#   Rscript scripts/update_scripts/DRV/all/validate_d01_outputs.R --platforms=all
#
# PRINCIPLE:
#   MP106 (Console Output Transparency), DM_R044 (Derivation Implementation Standard)

suppressPackageStartupMessages({
  library(DBI)
})

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
autoinit()

if (!exists("dbConnectDuckdb", mode = "function")) {
  source(file.path("scripts", "global_scripts", "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R"))
}

resolve_default_platforms <- function() {
  if (!exists("get_platform_config", mode = "function", inherits = TRUE)) {
    config_fn <- file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R")
    if (file.exists(config_fn)) {
      source(config_fn)
    }
  }

  platforms <- NULL
  if (exists("get_platform_config", mode = "function", inherits = TRUE)) {
    platforms <- tryCatch(get_platform_config(), error = function(e) NULL)
  }

  if (!is.list(platforms) || length(platforms) == 0) {
    return(c("cbz", "eby"))
  }

  platform_ids <- names(platforms)
  if (is.null(platform_ids) || !any(nzchar(platform_ids))) {
    platform_ids <- vapply(
      platforms,
      function(entry) {
        if (is.list(entry) && !is.null(entry$platform_id)) {
          return(as.character(entry$platform_id))
        }
        ""
      },
      character(1)
    )
    platform_ids <- platform_ids[nzchar(platform_ids)]
    if (length(platform_ids) == 0) {
      return(c("cbz", "eby"))
    }
    names(platforms) <- platform_ids
  }

  is_active <- function(entry) {
    if (!is.list(entry)) return(TRUE)
    status <- entry$status
    if (!is.null(status) && tolower(as.character(status)) != "active") return(FALSE)
    enabled <- entry$enabled
    if (!is.null(enabled) && !isTRUE(enabled)) return(FALSE)
    TRUE
  }

  active_platforms <- platform_ids[vapply(platforms[platform_ids], is_active, logical(1))]
  if (length(active_platforms) == 0) active_platforms <- platform_ids
  active_platforms
}

parse_platforms <- function(args, default_platforms) {
  platforms_arg <- NULL
  for (idx in seq_along(args)) {
    if (args[idx] %in% c("--platforms", "--platform") && idx < length(args)) {
      platforms_arg <- args[idx + 1]
      break
    }
    if (grepl("^--platforms=", args[idx])) {
      platforms_arg <- sub("^--platforms=", "", args[idx])
      break
    }
  }
  if (is.null(platforms_arg)) {
    env_platforms <- Sys.getenv("D01_PLATFORMS", "")
    if (nzchar(env_platforms)) {
      platforms_arg <- env_platforms
    }
  }
  if (is.null(platforms_arg) || platforms_arg == "" || platforms_arg == "all") {
    return(default_platforms)
  }
  platforms <- strsplit(platforms_arg, ",", fixed = TRUE)[[1]]
  platforms <- trimws(platforms)
  platforms <- platforms[nzchar(platforms)]
  if (length(platforms) == 0) {
    return(default_platforms)
  }
  platforms
}

platforms <- parse_platforms(commandArgs(trailingOnly = TRUE), resolve_default_platforms())
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

results <- list()

format_value <- function(value) {
  if (is.null(value)) return("NA")
  if (length(value) == 0) return("NA")
  if (is.na(value)) return("NA")
  as.character(value)
}

record_result <- function(check, platform, status, detail, expected = NA_character_, actual = NA_character_) {
  entry <- data.frame(
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    platform = ifelse(is.null(platform), "all", platform),
    check = check,
    status = status,
    detail = detail,
    expected = format_value(expected),
    actual = format_value(actual),
    stringsAsFactors = FALSE
  )
  results[[length(results) + 1]] <<- entry

  header <- sprintf("%s [%s] %s", status, entry$platform, check)
  message(header)
  message("  ", detail)
  if (!is.na(expected) || !is.na(actual)) {
    message(sprintf("  expected: %s | actual: %s", format_value(expected), format_value(actual)))
  }
}

connect_db <- function(label, path, read_only = TRUE) {
  if (is.null(path) || !file.exists(path)) {
    record_result(
      check = sprintf("Database availability (%s)", label),
      platform = NULL,
      status = "FAIL",
      detail = sprintf("Database file not found: %s", format_value(path))
    )
    return(NULL)
  }
  con <- dbConnectDuckdb(path, read_only = read_only)
  record_result(
    check = sprintf("Database availability (%s)", label),
    platform = NULL,
    status = "PASS",
    detail = sprintf("Connected to %s", path)
  )
  con
}

get_row_count <- function(con, table_name, where_clause = NULL, params = list()) {
  if (is.null(con) || !DBI::dbIsValid(con)) return(NA_integer_)
  if (!DBI::dbExistsTable(con, table_name)) return(NA_integer_)
  table_id <- DBI::dbQuoteIdentifier(con, table_name)
  query <- if (is.null(where_clause)) {
    sprintf("SELECT COUNT(*) AS n FROM %s", table_id)
  } else {
    sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s", table_id, where_clause)
  }
  as.integer(DBI::dbGetQuery(con, query, params = params)$n)
}

get_distinct_count <- function(con, table_name, column, where_clause = NULL, params = list()) {
  if (is.null(con) || !DBI::dbIsValid(con)) return(NA_integer_)
  if (!DBI::dbExistsTable(con, table_name)) return(NA_integer_)
  table_id <- DBI::dbQuoteIdentifier(con, table_name)
  query <- if (is.null(where_clause)) {
    sprintf("SELECT COUNT(DISTINCT %s) AS n FROM %s", column, table_id)
  } else {
    sprintf("SELECT COUNT(DISTINCT %s) AS n FROM %s WHERE %s", column, table_id, where_clause)
  }
  as.integer(DBI::dbGetQuery(con, query, params = params)$n)
}

check_table_exists <- function(con, table_name, db_label, platform) {
  if (is.null(con)) {
    record_result(
      check = sprintf("Table exists (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Database connection not available"
    )
    return(FALSE)
  }
  exists <- DBI::dbExistsTable(con, table_name)
  if (exists) {
    record_result(
      check = sprintf("Table exists (%s.%s)", db_label, table_name),
      platform = platform,
      status = "PASS",
      detail = "Table found"
    )
  } else {
    record_result(
      check = sprintf("Table exists (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Table not found"
    )
  }
  exists
}

check_required_columns <- function(con, table_name, db_label, platform, required_cols) {
  if (is.null(con) || !DBI::dbExistsTable(con, table_name)) {
    record_result(
      check = sprintf("Required columns (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Table missing; cannot validate columns"
    )
    return(FALSE)
  }
  cols <- DBI::dbListFields(con, table_name)
  missing_cols <- setdiff(required_cols, cols)
  if (length(missing_cols) > 0) {
    record_result(
      check = sprintf("Required columns (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = sprintf("Missing columns: %s", paste(missing_cols, collapse = ", "))
    )
    return(FALSE)
  }
  record_result(
    check = sprintf("Required columns (%s.%s)", db_label, table_name),
    platform = platform,
    status = "PASS",
    detail = sprintf("All required columns present (%d)", length(required_cols))
  )
  TRUE
}

check_row_count <- function(con, table_name, db_label, platform, where_clause = NULL, params = list(), min_rows = 1) {
  if (is.null(con) || !DBI::dbExistsTable(con, table_name)) {
    record_result(
      check = sprintf("Row count (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Table missing; cannot count rows"
    )
    return(NA_integer_)
  }
  row_count <- get_row_count(con, table_name, where_clause, params)
  if (is.na(row_count)) {
    record_result(
      check = sprintf("Row count (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Unable to compute row count"
    )
    return(NA_integer_)
  }
  if (row_count < min_rows) {
    record_result(
      check = sprintf("Row count (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Row count below minimum",
      expected = sprintf(">= %d", min_rows),
      actual = row_count
    )
  } else {
    record_result(
      check = sprintf("Row count (%s.%s)", db_label, table_name),
      platform = platform,
      status = "PASS",
      detail = "Row count meets minimum",
      expected = sprintf(">= %d", min_rows),
      actual = row_count
    )
  }
  row_count
}

check_platform_id_consistency <- function(con, table_name, db_label, platform) {
  if (is.null(con) || !DBI::dbExistsTable(con, table_name)) {
    record_result(
      check = sprintf("Platform id consistency (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Table missing; cannot validate platform_id"
    )
    return(FALSE)
  }
  cols <- DBI::dbListFields(con, table_name)
  if (!"platform_id" %in% cols) {
    record_result(
      check = sprintf("Platform id consistency (%s.%s)", db_label, table_name),
      platform = platform,
      status = "WARN",
      detail = "platform_id column not found"
    )
    return(FALSE)
  }
  table_id <- DBI::dbQuoteIdentifier(con, table_name)
  query <- sprintf(
    "SELECT COUNT(*) AS n FROM %s WHERE platform_id IS NOT NULL AND platform_id <> ?",
    table_id
  )
  mismatch_count <- as.integer(DBI::dbGetQuery(con, query, params = list(platform))$n)
  if (mismatch_count > 0) {
    record_result(
      check = sprintf("Platform id consistency (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = sprintf("%d rows with mismatched platform_id", mismatch_count),
      expected = platform,
      actual = sprintf("mismatched rows: %d", mismatch_count)
    )
    return(FALSE)
  }
  record_result(
    check = sprintf("Platform id consistency (%s.%s)", db_label, table_name),
    platform = platform,
    status = "PASS",
    detail = "All rows match platform_id",
    expected = platform,
    actual = platform
  )
  TRUE
}

check_missing_platform_id <- function(con, table_name, db_label, platform) {
  if (is.null(con) || !DBI::dbExistsTable(con, table_name)) {
    record_result(
      check = sprintf("Missing platform_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Table missing; cannot validate platform_id"
    )
    return(NA_integer_)
  }
  cols <- DBI::dbListFields(con, table_name)
  if (!"platform_id" %in% cols) {
    record_result(
      check = sprintf("Missing platform_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "WARN",
      detail = "platform_id column not found"
    )
    return(NA_integer_)
  }
  table_id <- DBI::dbQuoteIdentifier(con, table_name)
  query <- sprintf("SELECT COUNT(*) AS n FROM %s WHERE platform_id IS NULL OR platform_id = ''", table_id)
  null_count <- as.integer(DBI::dbGetQuery(con, query)$n)
  if (null_count > 0) {
    record_result(
      check = sprintf("Missing platform_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "WARN",
      detail = sprintf("%d rows with NULL/empty platform_id", null_count),
      expected = "0",
      actual = null_count
    )
  } else {
    record_result(
      check = sprintf("Missing platform_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "PASS",
      detail = "No NULL/empty platform_id values",
      expected = "0",
      actual = null_count
    )
  }
  null_count
}

check_duplicate_customers <- function(con, table_name, db_label, platform) {
  if (is.null(con) || !DBI::dbExistsTable(con, table_name)) {
    record_result(
      check = sprintf("Duplicate customers (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Table missing; cannot validate customer_id uniqueness"
    )
    return(NA_integer_)
  }
  cols <- DBI::dbListFields(con, table_name)
  if (!"customer_id" %in% cols) {
    record_result(
      check = sprintf("Duplicate customers (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "customer_id column not found"
    )
    return(NA_integer_)
  }
  table_id <- DBI::dbQuoteIdentifier(con, table_name)
  query <- sprintf("SELECT COUNT(*) - COUNT(DISTINCT customer_id) AS n FROM %s", table_id)
  dup_count <- as.integer(DBI::dbGetQuery(con, query)$n)
  if (dup_count > 0) {
    record_result(
      check = sprintf("Duplicate customers (%s.%s)", db_label, table_name),
      platform = platform,
      status = "WARN",
      detail = sprintf("%d duplicate customer_id rows detected", dup_count),
      expected = "0",
      actual = dup_count
    )
  } else {
    record_result(
      check = sprintf("Duplicate customers (%s.%s)", db_label, table_name),
      platform = platform,
      status = "PASS",
      detail = "No duplicate customer_id rows",
      expected = "0",
      actual = dup_count
    )
  }
  dup_count
}

check_missing_customer_id <- function(con, table_name, db_label, platform) {
  if (is.null(con) || !DBI::dbExistsTable(con, table_name)) {
    record_result(
      check = sprintf("Missing customer_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "Table missing; cannot validate customer_id"
    )
    return(NA_integer_)
  }
  cols <- DBI::dbListFields(con, table_name)
  if (!"customer_id" %in% cols) {
    record_result(
      check = sprintf("Missing customer_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "FAIL",
      detail = "customer_id column not found"
    )
    return(NA_integer_)
  }
  table_id <- DBI::dbQuoteIdentifier(con, table_name)
  query <- sprintf("SELECT COUNT(*) AS n FROM %s WHERE customer_id IS NULL", table_id)
  null_count <- as.integer(DBI::dbGetQuery(con, query)$n)
  if (null_count > 0) {
    record_result(
      check = sprintf("Missing customer_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "WARN",
      detail = sprintf("%d rows with NULL customer_id", null_count),
      expected = "0",
      actual = null_count
    )
  } else {
    record_result(
      check = sprintf("Missing customer_id (%s.%s)", db_label, table_name),
      platform = platform,
      status = "PASS",
      detail = "No NULL customer_id values",
      expected = "0",
      actual = null_count
    )
  }
  null_count
}

connect_or_default <- function(path_value, fallback) {
  if (!is.null(path_value) && nzchar(path_value)) {
    return(path_value)
  }
  fallback
}

transformed_path <- connect_or_default(db_path_list$transformed_data, file.path("data", "transformed_data.duckdb"))
processed_path <- connect_or_default(db_path_list$processed_data, file.path("data", "processed_data.duckdb"))
cleansed_path <- connect_or_default(db_path_list$cleansed_data, file.path("data", "cleansed_data.duckdb"))
app_path <- connect_or_default(db_path_list$app_data, file.path("data", "app_data", "app_data.duckdb"))

con_transformed <- connect_db("transformed_data", transformed_path, read_only = TRUE)
con_processed <- connect_db("processed_data", processed_path, read_only = TRUE)
con_cleansed <- connect_db("cleansed_data", cleansed_path, read_only = TRUE)
con_app <- connect_db("app_data", app_path, read_only = TRUE)

platform_summaries <- list()

for (platform_id in platforms) {
  message("")
  message("=== D01 Validation for platform: ", platform_id, " ===")

  counts <- list(
    sales_rows = NA_integer_,
    sales_customers = NA_integer_,
    by_date_rows = NA_integer_,
    by_customer_rows = NA_integer_,
    by_customer_customers = NA_integer_,
    rfm_rows = NA_integer_,
    dna_rows = NA_integer_,
    profile_rows = NA_integer_,
    app_profile_rows = NA_integer_,
    app_dna_rows = NA_integer_,
    app_segments_rows = NA_integer_
  )

  # D01_00 input table (transformed_data)
  sales_table <- sprintf("df_%s_sales___standardized", platform_id)
  if (check_table_exists(con_transformed, sales_table, "transformed_data", platform_id)) {
    check_required_columns(
      con_transformed,
      sales_table,
      "transformed_data",
      platform_id,
      c("customer_id", "payment_time", "lineproduct_price")
    )
    counts$sales_rows <- check_row_count(con_transformed, sales_table, "transformed_data", platform_id, min_rows = 1)
    counts$sales_customers <- get_distinct_count(con_transformed, sales_table, "customer_id")
    record_result(
      check = sprintf("Distinct customers (transformed_data.%s)", sales_table),
      platform = platform_id,
      status = ifelse(is.na(counts$sales_customers) || counts$sales_customers == 0, "FAIL", "PASS"),
      detail = "Distinct customer_id count",
      expected = ">= 1",
      actual = counts$sales_customers
    )
    check_platform_id_consistency(con_transformed, sales_table, "transformed_data", platform_id)
    check_missing_customer_id(con_transformed, sales_table, "transformed_data", platform_id)
  }

  # D01_01 outputs (processed_data)
  by_date_table <- sprintf("df_%s_sales_by_customer_by_date", platform_id)
  by_customer_table <- sprintf("df_%s_sales_by_customer", platform_id)

  if (check_table_exists(con_processed, by_date_table, "processed_data", platform_id)) {
    check_required_columns(
      con_processed,
      by_date_table,
      "processed_data",
      platform_id,
      c("customer_id", "sum_spent_by_date", "count_transactions_by_date", "min_time_by_date", "product_line_id_filter")
    )
    counts$by_date_rows <- check_row_count(con_processed, by_date_table, "processed_data", platform_id, min_rows = 1)
    check_platform_id_consistency(con_processed, by_date_table, "processed_data", platform_id)
    check_missing_customer_id(con_processed, by_date_table, "processed_data", platform_id)
  }

  if (check_table_exists(con_processed, by_customer_table, "processed_data", platform_id)) {
    check_required_columns(
      con_processed,
      by_customer_table,
      "processed_data",
      platform_id,
      c("customer_id", "sum_sales_by_customer", "sum_transactions_by_customer", "ipt", "ni", "product_line_id_filter")
    )
    counts$by_customer_rows <- check_row_count(con_processed, by_customer_table, "processed_data", platform_id, min_rows = 1)
    counts$by_customer_customers <- get_distinct_count(con_processed, by_customer_table, "customer_id")
    record_result(
      check = sprintf("Distinct customers (processed_data.%s)", by_customer_table),
      platform = platform_id,
      status = ifelse(is.na(counts$by_customer_customers) || counts$by_customer_customers == 0, "FAIL", "PASS"),
      detail = "Distinct customer_id count",
      expected = ">= 1",
      actual = counts$by_customer_customers
    )
    check_platform_id_consistency(con_processed, by_customer_table, "processed_data", platform_id)
    check_missing_customer_id(con_processed, by_customer_table, "processed_data", platform_id)
    check_duplicate_customers(con_processed, by_customer_table, "processed_data", platform_id)
  }

  # D01_02 output (processed_data)
  rfm_table <- sprintf("df_%s_customer_rfm", platform_id)
  if (check_table_exists(con_processed, rfm_table, "processed_data", platform_id)) {
    check_required_columns(
      con_processed,
      rfm_table,
      "processed_data",
      platform_id,
      c("customer_id", "platform_id", "r_value", "f_value", "m_value", "ipt", "customer_tenure_days", "product_line_id_filter")
    )
    counts$rfm_rows <- check_row_count(con_processed, rfm_table, "processed_data", platform_id, min_rows = 1)
    check_platform_id_consistency(con_processed, rfm_table, "processed_data", platform_id)
    check_missing_customer_id(con_processed, rfm_table, "processed_data", platform_id)
    check_duplicate_customers(con_processed, rfm_table, "processed_data", platform_id)
  }

  # D01_03 output (cleansed_data)
  dna_table <- "df_dna_by_customer___cleansed"
  if (check_table_exists(con_cleansed, dna_table, "cleansed_data", platform_id)) {
    check_required_columns(
      con_cleansed,
      dna_table,
      "cleansed_data",
      platform_id,
      c("customer_id", "nes_status", "platform_id", "product_line_id_filter",
        "dna_m_score", "dna_f_score", "dna_r_score", "cai", "dna_segment")
    )
    counts$dna_rows <- check_row_count(
      con_cleansed,
      dna_table,
      "cleansed_data",
      platform_id,
      where_clause = "platform_id = ?",
      params = list(platform_id),
      min_rows = 1
    )
    check_missing_platform_id(con_cleansed, dna_table, "cleansed_data", platform_id)
    check_missing_customer_id(con_cleansed, dna_table, "cleansed_data", platform_id)
  }

  # D01_04 output (cleansed_data)
  profile_table <- "df_profile_by_customer___cleansed"
  if (check_table_exists(con_cleansed, profile_table, "cleansed_data", platform_id)) {
    check_required_columns(
      con_cleansed,
      profile_table,
      "cleansed_data",
      platform_id,
      c("customer_id", "platform_id", "buyer_name", "email")
    )
    counts$profile_rows <- check_row_count(
      con_cleansed,
      profile_table,
      "cleansed_data",
      platform_id,
      where_clause = "platform_id = ?",
      params = list(platform_id),
      min_rows = 1
    )
    check_missing_platform_id(con_cleansed, profile_table, "cleansed_data", platform_id)
    check_missing_customer_id(con_cleansed, profile_table, "cleansed_data", platform_id)
  }

  # D01_05 outputs (app_data)
  app_profile_table <- "df_profile_by_customer"
  app_dna_table <- "df_dna_by_customer"
  app_segments_table <- "df_segments_by_customer"

  if (check_table_exists(con_app, app_profile_table, "app_data", platform_id)) {
    check_required_columns(
      con_app,
      app_profile_table,
      "app_data",
      platform_id,
      c("customer_id", "platform_id", "buyer_name", "email")
    )
    counts$app_profile_rows <- check_row_count(
      con_app,
      app_profile_table,
      "app_data",
      platform_id,
      where_clause = "platform_id = ?",
      params = list(platform_id),
      min_rows = 1
    )
    check_missing_platform_id(con_app, app_profile_table, "app_data", platform_id)
    check_missing_customer_id(con_app, app_profile_table, "app_data", platform_id)
  }

  if (check_table_exists(con_app, app_dna_table, "app_data", platform_id)) {
    check_required_columns(
      con_app,
      app_dna_table,
      "app_data",
      platform_id,
      c("customer_id", "platform_id", "product_line_id_filter",
        "nes_status", "dna_segment", "cai",
        "dna_m_score", "dna_f_score", "dna_r_score")
    )
    counts$app_dna_rows <- check_row_count(
      con_app,
      app_dna_table,
      "app_data",
      platform_id,
      where_clause = "platform_id = ?",
      params = list(platform_id),
      min_rows = 1
    )
    check_missing_platform_id(con_app, app_dna_table, "app_data", platform_id)
    check_missing_customer_id(con_app, app_dna_table, "app_data", platform_id)
  }

  if (check_table_exists(con_app, app_segments_table, "app_data", platform_id)) {
    check_required_columns(
      con_app,
      app_segments_table,
      "app_data",
      platform_id,
      c("customer_id", "platform_id", "product_line_id_filter",
        "nes_status", "dna_segment", "cai", "value_tier")
    )
    counts$app_segments_rows <- check_row_count(
      con_app,
      app_segments_table,
      "app_data",
      platform_id,
      where_clause = "platform_id = ?",
      params = list(platform_id),
      min_rows = 1
    )
    check_missing_platform_id(con_app, app_segments_table, "app_data", platform_id)
    check_missing_customer_id(con_app, app_segments_table, "app_data", platform_id)
  }

  # Views (app_data)
  view_names <- c("v_customer_dna_analytics", "v_customer_segments", "v_segment_statistics")
  for (view_name in view_names) {
    check_table_exists(con_app, view_name, "app_data", platform_id)
  }

  # Cross-table consistency checks
  if (!is.na(counts$by_customer_rows) && !is.na(counts$rfm_rows)) {
    status <- ifelse(counts$by_customer_rows == counts$rfm_rows, "PASS", "WARN")
    record_result(
      check = "Row count match (sales_by_customer vs customer_rfm)",
      platform = platform_id,
      status = status,
      detail = "Expected equal row counts",
      expected = counts$by_customer_rows,
      actual = counts$rfm_rows
    )
  }

  if (!is.na(counts$rfm_rows) && !is.na(counts$dna_rows)) {
    status <- ifelse(counts$rfm_rows == counts$dna_rows, "PASS", "WARN")
    record_result(
      check = "Row count match (customer_rfm vs customer_dna)",
      platform = platform_id,
      status = status,
      detail = "Expected equal row counts",
      expected = counts$rfm_rows,
      actual = counts$dna_rows
    )
  }

  if (!is.na(counts$profile_rows) && !is.na(counts$by_customer_rows)) {
    status <- ifelse(counts$profile_rows == counts$by_customer_rows, "PASS", "WARN")
    record_result(
      check = "Row count match (profile vs sales_by_customer)",
      platform = platform_id,
      status = status,
      detail = "Expected equal row counts",
      expected = counts$by_customer_rows,
      actual = counts$profile_rows
    )
  }

  if (!is.na(counts$app_profile_rows) && !is.na(counts$profile_rows)) {
    status <- ifelse(counts$app_profile_rows == counts$profile_rows, "PASS", "WARN")
    record_result(
      check = "Row count match (app_profile vs cleansed_profile)",
      platform = platform_id,
      status = status,
      detail = "Expected equal row counts",
      expected = counts$profile_rows,
      actual = counts$app_profile_rows
    )
  }

  if (!is.na(counts$app_dna_rows) && !is.na(counts$dna_rows)) {
    status <- ifelse(counts$app_dna_rows == counts$dna_rows, "PASS", "WARN")
    record_result(
      check = "Row count match (app_dna vs cleansed_dna)",
      platform = platform_id,
      status = status,
      detail = "Expected equal row counts",
      expected = counts$dna_rows,
      actual = counts$app_dna_rows
    )
  }

  if (!is.na(counts$app_segments_rows) && !is.na(counts$app_dna_rows)) {
    status <- ifelse(counts$app_segments_rows == counts$app_dna_rows, "PASS", "WARN")
    record_result(
      check = "Row count match (app_segments vs app_dna)",
      platform = platform_id,
      status = status,
      detail = "Expected equal row counts",
      expected = counts$app_dna_rows,
      actual = counts$app_segments_rows
    )
  }

  if (!is.na(counts$by_date_rows) && !is.na(counts$by_customer_rows)) {
    status <- ifelse(counts$by_date_rows >= counts$by_customer_rows, "PASS", "WARN")
    record_result(
      check = "Row count sanity (by_date >= by_customer)",
      platform = platform_id,
      status = status,
      detail = "by_date rows should be >= by_customer rows",
      expected = sprintf(">= %d", counts$by_customer_rows),
      actual = counts$by_date_rows
    )
  }

  platform_summaries[[platform_id]] <- counts
}

results_df <- do.call(rbind, results)

message("")
message("=== D01 Validation Summary ===")
if (!is.null(results_df) && nrow(results_df) > 0) {
  pass_count <- sum(results_df$status == "PASS")
  warn_count <- sum(results_df$status == "WARN")
  fail_count <- sum(results_df$status == "FAIL")
  message(sprintf("Total checks: %d", nrow(results_df)))
  message(sprintf("PASS: %d", pass_count))
  message(sprintf("WARN: %d", warn_count))
  message(sprintf("FAIL: %d", fail_count))
} else {
  pass_count <- 0
  warn_count <- 0
  fail_count <- 0
  message("No validation results recorded")
}

message("")
message("Row count snapshot by platform:")
for (platform_id in names(platform_summaries)) {
  summary <- platform_summaries[[platform_id]]
  message(sprintf(
    "  %s | sales=%s by_date=%s by_customer=%s rfm=%s dna=%s profile=%s app_profile=%s app_dna=%s app_segments=%s",
    platform_id,
    format_value(summary$sales_rows),
    format_value(summary$by_date_rows),
    format_value(summary$by_customer_rows),
    format_value(summary$rfm_rows),
    format_value(summary$dna_rows),
    format_value(summary$profile_rows),
    format_value(summary$app_profile_rows),
    format_value(summary$app_dna_rows),
    format_value(summary$app_segments_rows)
  ))
}

validation_dir <- file.path("validation")
if (!dir.exists(validation_dir)) {
  dir.create(validation_dir, recursive = TRUE)
}
output_file <- file.path(validation_dir, sprintf("d01_validation_%s.csv", timestamp))
if (!is.null(results_df) && nrow(results_df) > 0) {
  write.csv(results_df, output_file, row.names = FALSE)
  message("")
  message(sprintf("Validation report written: %s", output_file))
}

if (fail_count > 0) {
  message("VALIDATION RESULT: FAIL")
  quit(status = 1)
} else if (warn_count > 0) {
  message("VALIDATION RESULT: WARN")
} else {
  message("VALIDATION RESULT: PASS")
}

if (!is.null(con_transformed) && DBI::dbIsValid(con_transformed)) DBI::dbDisconnect(con_transformed, shutdown = FALSE)
if (!is.null(con_processed) && DBI::dbIsValid(con_processed)) DBI::dbDisconnect(con_processed, shutdown = FALSE)
if (!is.null(con_cleansed) && DBI::dbIsValid(con_cleansed)) DBI::dbDisconnect(con_cleansed, shutdown = FALSE)
if (!is.null(con_app) && DBI::dbIsValid(con_app)) DBI::dbDisconnect(con_app, shutdown = FALSE)
