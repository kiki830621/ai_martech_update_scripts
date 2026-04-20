#!/usr/bin/env Rscript
#####
# DERIVATION: D01_07 Product-Line Coverage Audit (AMZ)
# VERSION: 1.0
# PLATFORM: amz
# GROUP: D01
# SEQUENCE: 07
# PURPOSE: Emit deterministic product-line coverage report after ETL+D01
# CONSUMES: transformed_data.df_product_profile_*___transformed,
#           transformed_data.df_amz_sales___standardized,
#           app_data.df_dna_by_customer
# PRODUCES: output/etl_validation/amz/product_line_coverage/*.csv + realtime log summary
# PRINCIPLE: MP106, MP149, DM_R027
#####
# amz_D01_07_product_line_coverage_audit

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}

autoinit()

script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()
script_name <- "amz_D01_07_product_line_coverage_audit"
script_version <- "1.0.0"

message(strrep("=", 80))
message("INITIALIZE: Starting AMZ Product-Line Coverage Audit (D01_07)")
message(sprintf("INITIALIZE: Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message(sprintf("INITIALIZE: Script: %s v%s", script_name, script_version))
message(strrep("=", 80))

library(DBI)
library(duckdb)
library(data.table)

source(file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R"))

transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = TRUE)

retention_days <- suppressWarnings(as.integer(Sys.getenv("COVERAGE_RETENTION_DAYS", "30")))
if (is.na(retention_days) || retention_days < 1) {
  retention_days <- 30L
}
fail_on_gap <- tolower(Sys.getenv("COVERAGE_FAIL_ON_GAP", "false")) %in% c("1", "true", "yes")

output_dir <- file.path(APP_DIR, "output", "etl_validation", "amz", "product_line_coverage")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# DM_R054 v2.1: df_product_line is loaded from meta_data.duckdb by UPDATE_MODE
# init via fn_load_product_lines(). The old `product_line_path` file.exists
# check on df_product_line.csv was stale (value was never read); removed to
# stay compliant with §6 (runtime MAY NOT read CSV seeds). The bootstrap
# check is the autoinit fail-fast precheck on meta_data.duckdb itself.

# coerce_included removed — replaced by get_active_product_lines() (#363)

safe_quote <- function(x) {
  gsub("'", "''", x, fixed = TRUE)
}

detect_competitor_source <- function() {
  candidates <- list(
    list(con = transformed_data, table = "df_amz_competitor_product_id___transformed"),
    list(con = raw_data, table = "df_amz_competitor_product_id")
  )

  for (cand in candidates) {
    if (!DBI::dbExistsTable(cand$con, cand$table)) next
    cols <- DBI::dbListFields(cand$con, cand$table)
    id_col <- c("asin", "product_id", "sku")
    id_col <- id_col[id_col %in% cols][1]
    if (is.na(id_col)) next

    if (!"product_line_id" %in% cols) next

    return(list(
      con = cand$con,
      table = cand$table,
      id_col = id_col
    ))
  }

  NULL
}

get_profile_distinct_asin <- function(product_line_id) {
  tbl <- sprintf("df_product_profile_%s___transformed", product_line_id)
  if (!DBI::dbExistsTable(transformed_data, tbl)) {
    return(list(n = 0L, missing_source = TRUE))
  }

  cols <- DBI::dbListFields(transformed_data, tbl)
  asin_col <- c("asin", "product_id", "sku")
  asin_col <- asin_col[asin_col %in% cols][1]
  if (is.na(asin_col)) {
    return(list(n = 0L, missing_source = TRUE))
  }

  q <- sprintf(
    paste(
      "SELECT COUNT(DISTINCT upper(trim(CAST(%s AS VARCHAR)))) AS n",
      "FROM %s",
      "WHERE %s IS NOT NULL",
      "AND length(trim(CAST(%s AS VARCHAR))) > 0"
    ),
    asin_col, tbl, asin_col, asin_col
  )
  n <- DBI::dbGetQuery(transformed_data, q)$n
  list(n = as.integer(n), missing_source = FALSE)
}

get_competitor_distinct_asin <- function(product_line_id, source_info) {
  if (is.null(source_info)) {
    return(0L)
  }

  pl <- safe_quote(product_line_id)
  q <- sprintf(
    paste(
      "SELECT COUNT(DISTINCT upper(trim(CAST(%s AS VARCHAR)))) AS n",
      "FROM %s",
      "WHERE lower(trim(CAST(product_line_id AS VARCHAR))) = '%s'",
      "AND %s IS NOT NULL",
      "AND length(trim(CAST(%s AS VARCHAR))) > 0"
    ),
    source_info$id_col,
    source_info$table,
    tolower(pl),
    source_info$id_col,
    source_info$id_col
  )
  as.integer(DBI::dbGetQuery(source_info$con, q)$n)
}

get_sales_rows <- function(product_line_id) {
  if (!DBI::dbExistsTable(transformed_data, "df_amz_sales___standardized")) {
    return(0L)
  }
  pl <- safe_quote(product_line_id)
  q <- sprintf(
    paste(
      "SELECT COUNT(*) AS n",
      "FROM df_amz_sales___standardized",
      "WHERE lower(trim(CAST(product_line_id AS VARCHAR))) = '%s'"
    ),
    tolower(pl)
  )
  as.integer(DBI::dbGetQuery(transformed_data, q)$n)
}

get_dna_rows <- function(product_line_id) {
  if (!DBI::dbExistsTable(app_data, "df_dna_by_customer")) {
    return(0L)
  }
  pl <- safe_quote(product_line_id)
  q <- sprintf(
    paste(
      "SELECT COUNT(*) AS n",
      "FROM df_dna_by_customer",
      "WHERE lower(trim(CAST(product_line_id_filter AS VARCHAR))) = '%s'"
    ),
    tolower(pl)
  )
  as.integer(DBI::dbGetQuery(app_data, q)$n)
}

determine_gap_type <- function(profile_distinct_asin, sales_rows, dna_rows, missing_profile_source) {
  if (missing_profile_source || profile_distinct_asin <= 0L) {
    return("A_missing_profile_source")
  }
  if (sales_rows <= 0L || dna_rows <= 0L) {
    return("B_profile_present_but_no_sales_coverage")
  }
  "OK"
}

# ==============================================================================
# 2. MAIN
# ==============================================================================

message("MAIN: Building deterministic product-line coverage report...")
main_start_time <- Sys.time()

tryCatch({
  product_lines <- as.data.table(get_active_product_lines())
  product_lines[, product_line_id := tolower(trimws(as.character(product_line_id)))]

  if (nrow(product_lines) == 0) {
    stop("No active product lines found")
  }

  competitor_source <- detect_competitor_source()
  if (is.null(competitor_source)) {
    message("MAIN WARNING: No competitor source table found. competitor_distinct_asin will be 0.")
  } else {
    message(sprintf(
      "MAIN: Competitor source resolved: %s.%s (id_col=%s)",
      if (identical(competitor_source$con, transformed_data)) "transformed_data" else "raw_data",
      competitor_source$table,
      competitor_source$id_col
    ))
  }

  report_rows <- vector("list", nrow(product_lines))
  non_ok <- 0L

  for (i in seq_len(nrow(product_lines))) {
    pl <- product_lines$product_line_id[i]
    profile_info <- get_profile_distinct_asin(pl)
    competitor_n <- get_competitor_distinct_asin(pl, competitor_source)
    sales_n <- get_sales_rows(pl)
    dna_n <- get_dna_rows(pl)
    gap_type <- determine_gap_type(
      profile_distinct_asin = profile_info$n,
      sales_rows = sales_n,
      dna_rows = dna_n,
      missing_profile_source = profile_info$missing_source
    )
    if (gap_type != "OK") non_ok <- non_ok + 1L

    report_rows[[i]] <- data.frame(
      product_line_id = pl,
      profile_distinct_asin = as.integer(profile_info$n),
      competitor_distinct_asin = as.integer(competitor_n),
      sales_rows = as.integer(sales_n),
      dna_rows = as.integer(dna_n),
      gap_type = gap_type,
      stringsAsFactors = FALSE
    )
  }

  coverage_report <- rbindlist(report_rows, fill = TRUE)
  setorder(coverage_report, product_line_id)

  message("MAIN: Coverage summary by product line")
  for (i in seq_len(nrow(coverage_report))) {
    row <- coverage_report[i]
    level <- if (row$gap_type == "OK") "INFO" else "WARN"
    message(sprintf(
      "%s: %s | profile_asin=%d competitor_asin=%d sales_rows=%d dna_rows=%d gap_type=%s",
      level,
      row$product_line_id,
      row$profile_distinct_asin,
      row$competitor_distinct_asin,
      row$sales_rows,
      row$dna_rows,
      row$gap_type
    ))
  }

  summary_by_gap <- coverage_report[, .N, by = gap_type]
  message("MAIN: Gap-type summary")
  print(summary_by_gap)

  ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
  report_path <- file.path(output_dir, sprintf("product_line_coverage_report_%s.csv", ts))
  latest_path <- file.path(output_dir, "product_line_coverage_report_latest.csv")
  fwrite(coverage_report, report_path)
  fwrite(coverage_report, latest_path)
  message(sprintf("MAIN: Coverage report saved: %s", report_path))
  message(sprintf("MAIN: Coverage report latest: %s", latest_path))

  old_files <- list.files(output_dir, pattern = "^product_line_coverage_report_\\d{8}_\\d{6}\\.csv$", full.names = TRUE)
  if (length(old_files) > 0) {
    file_info <- file.info(old_files)
    cutoff_time <- Sys.time() - as.difftime(retention_days, units = "days")
    to_delete <- rownames(file_info)[!is.na(file_info$mtime) & file_info$mtime < cutoff_time]
    if (length(to_delete) > 0) {
      unlink(to_delete, force = TRUE)
      message(sprintf("MAIN: Retention cleanup removed %d old report file(s) (> %d days)", length(to_delete), retention_days))
    }
  }

  if (non_ok > 0) {
    warning(sprintf(
      "Coverage guardrail detected %d non-OK product line(s). See report: %s",
      non_ok, report_path
    ))
    if (fail_on_gap) {
      stop("COVERAGE_FAIL_ON_GAP is enabled and non-OK product lines exist")
    }
  } else {
    message("MAIN: All active product lines are OK")
  }

  script_success <- TRUE

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ERROR: %s", e$message))
})

main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
message(sprintf("MAIN: Completed in %.2fs", main_elapsed))

# ==============================================================================
# 3. TEST
# ==============================================================================

message("TEST: Verifying coverage report output...")
if (script_success) {
  latest_path <- file.path(output_dir, "product_line_coverage_report_latest.csv")
  if (!file.exists(latest_path)) {
    message("TEST: FAILED - latest coverage report not found")
    test_passed <- FALSE
  } else {
    test_df <- fread(latest_path)
    required_cols <- c(
      "product_line_id", "profile_distinct_asin", "competitor_distinct_asin",
      "sales_rows", "dna_rows", "gap_type"
    )
    missing_cols <- setdiff(required_cols, names(test_df))
    if (length(missing_cols) > 0) {
      message(sprintf("TEST: FAILED - missing columns: %s", paste(missing_cols, collapse = ", ")))
      test_passed <- FALSE
    } else {
      message(sprintf("TEST: PASS - report generated with %d rows", nrow(test_df)))
      test_passed <- TRUE
    }
  }
} else {
  message("TEST: Skipped due to main failure")
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================

message(strrep("=", 80))
message("SUMMARIZE: AMZ PRODUCT-LINE COVERAGE AUDIT (D01_07)")
message(strrep("=", 80))
message(sprintf("Platform: amz | Phase: D01_07"))
message(sprintf("Total time: %.2fs", as.numeric(Sys.time() - script_start_time, units = "secs")))
message(sprintf("Status: %s", if (script_success && test_passed) "SUCCESS" else "FAILED"))
message("Compliance: MP106, MP149, DM_R027")
message(strrep("=", 80))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================

if (exists("transformed_data") && inherits(transformed_data, "DBIConnection") && DBI::dbIsValid(transformed_data)) {
  DBI::dbDisconnect(transformed_data)
}
if (exists("raw_data") && inherits(raw_data, "DBIConnection") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data)
}
if (exists("app_data") && inherits(app_data, "DBIConnection") && DBI::dbIsValid(app_data)) {
  DBI::dbDisconnect(app_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE
