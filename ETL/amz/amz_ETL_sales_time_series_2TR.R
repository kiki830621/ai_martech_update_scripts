#!/usr/bin/env Rscript
# ==============================================================================
# AMZ Sales Time Series ETL - Derived 2TR
# ==============================================================================
# PURPOSE: Build complete sales time series for Poisson analysis (R117).
# PLATFORM: amz
# PHASE: 2TR (derived ETL)
# CONSUMES: transformed_data.df_amz_sales___transformed,
#           transformed_data.df_product_profile_{product_line}___transformed (per active PL),
#           transformed_data.df_amz_review___transformed,
#           raw_data.df_amz_competitor_product_id,
#           meta_data.df_product_line (via global df_product_line, DM_R054 v2.1)
# PRODUCES: app_data.df_amz_sales_complete_time_series_{product_line},
#           app_data.df_amz_sales_complete_time_series
# PRINCIPLE: MP064, MP109, R117, DM_R053, MP029
# REFS: #674 (extends #661) — product_profile + review enrichment so D04_02 Poisson
#       can detect comment_attribute / product_attribute predictor_types, not only
#       time_feature.
# ==============================================================================
#
# Notes on amz vs cbz differences (#360):
# - amz uses `asin` as primary product identifier (cbz uses sku + ebay_item_number)
# - amz lacks per-buyer identity; customer_id is derived from ship_postal_code (#359)
# - asin → product_line_id mapping source: df_amz_competitor_product_id (358 rows)
# - Poisson input aggregates by (date, product_line_id) rather than per-customer
#   because Amazon buyer identity is unreliable
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
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

library(DBI)
library(duckdb)
library(dplyr)
library(tidyr)
library(tibble)
library(readr)

source("scripts/global_scripts/04_utils/fn_complete_time_series.R")

error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()
product_lines <- character()

if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

DB_RAW <- db_path_list$raw_data
DB_TRANSFORMED <- db_path_list$transformed_data
DB_APP <- db_path_list$app_data

INPUT_SALES_TABLE <- "df_amz_sales___transformed"
MAPPING_TABLE <- "df_amz_competitor_product_id"
SCRIPT_VERSION <- "v1.0_ETL_TS_AMZ"
TIME_UNIT <- "day"
FILL_METHOD <- "zero"

# DM_R054 v2.1: df_product_line is sourced from meta_data.duckdb, loaded into
# the global env by UPDATE_MODE init via fn_load_product_lines(). get_active_product_lines()
# (Step 2/6 below) consumes the in-memory global — no CSV read happens here.
# The old `PRODUCT_LINE_PATH` constant + file.exists() gate were stale dead
# code (never used to read); removed to comply with §6 (no CSV seed at runtime).

# Empty schema (used for write-empty fallback per MP029)
empty_time_series_schema <- tibble(
  amz_asin = character(),
  time = as.Date(character()),
  year = numeric(),
  day = integer(),
  month_1 = numeric(), month_2 = numeric(), month_3 = numeric(),
  month_4 = numeric(), month_5 = numeric(), month_6 = numeric(),
  month_7 = numeric(), month_8 = numeric(), month_9 = numeric(),
  month_10 = numeric(), month_11 = numeric(), month_12 = numeric(),
  monday = numeric(), tuesday = numeric(), wednesday = numeric(),
  thursday = numeric(), friday = numeric(), saturday = numeric(),
  sunday = numeric(),
  product_line_id.x = character(),
  sales = integer(),
  sales_platform = numeric(),
  product_line_name = character(),
  product_line_id.y = character(),
  data_source = character(),
  filling_method = character(),
  filling_timestamp = as.POSIXct(character()),
  source_table = character(),
  processing_version = character(),
  enrichment_version = character()
)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  message("====================================================================")
  message("AMZ ETL Time Series Completion (R117)")
  message("====================================================================")
  message(sprintf("Process Date: %s", start_time))

  if (!file.exists(DB_TRANSFORMED)) stop(sprintf("DB missing: %s", DB_TRANSFORMED))

  message("[Step 1/6] Connecting to databases...")
  con_raw <- dbConnectDuckdb(DB_RAW, read_only = TRUE)
  con_transformed <- dbConnectDuckdb(DB_TRANSFORMED, read_only = TRUE)
  con_app <- dbConnectDuckdb(DB_APP, read_only = FALSE)

  if (!dbExistsTable(con_transformed, INPUT_SALES_TABLE)) {
    stop(sprintf("Input sales table not found: %s", INPUT_SALES_TABLE))
  }

  message("[Step 2/6] Loading product line dictionary...")
  product_line_lookup <- get_active_product_lines()

  product_lines <- product_line_lookup$product_line_id
  if (length(product_lines) == 0) stop("No active product lines")
  message(sprintf("  OK Active product lines: %s", paste(product_lines, collapse = ", ")))

  message("[Step 3/6] Building asin -> product_line mapping...")
  if (!dbExistsTable(con_raw, MAPPING_TABLE)) {
    stop(sprintf("Missing asin mapping table: %s", MAPPING_TABLE))
  }
  # Read full mapping (with source_header if present) before dedup,
  # so we can detect ASINs claimed by multiple PLs (per #818 root cause:
  # df_amz_competitor_product_id had 16 hsg-sfg duplicate entries that
  # silently lost to distinct() ordering, masking sfg as "code bug").
  # Preserve raw row order via .orig_row so the audit can report the SAME
  # winner that production distinct(asin, .keep_all=TRUE) keeps
  # (verify #818 Logic#1 fix — alphabetical arrange would diverge once raw
  # row order differs from alphabetical, e.g. (psg, rpl) conflicts).
  asin_mapping_full <- tbl2(con_raw, MAPPING_TABLE) %>%
    select(any_of(c("asin", "product_line_id", "source_header"))) %>%
    collect() %>%
    mutate(
      asin = as.character(asin),
      product_line_id = as.character(product_line_id)
    )
  # Enforce uniform 6-col schema for audit (verify #818 Logic#4 fix —
  # source_header NA when upstream mapping table doesn't have it).
  if (!"source_header" %in% names(asin_mapping_full)) {
    asin_mapping_full$source_header <- NA_character_
  }
  asin_mapping_full <- asin_mapping_full %>%
    filter(!is.na(asin) & asin != "") %>%
    mutate(.orig_row = dplyr::row_number())

  # Defensive: detect duplicate ASINs across product_line_id BEFORE distinct().
  # MP154 (side-effect defense — no silent mapping failure) +
  # MP163 partial — Gate 1 (always-runnable) + Gate 2 (visible-gap sentinel)
  # + Gate 4 (asymptotic completeness) satisfied; **Gate 3 (Surface-to-Attention)
  # deferred** — currently no Shiny / GSheet / digest reader of the audit table.
  # Track full alignment via #839 (Shiny reader / GSheet pipeline / email digest
  # wiring); until that lands, do NOT upgrade the doc-string back to plain MP163.
  #
  # NA product_line_id is pre-filtered (verify #818 Logic#2 fix) so (hsg, NA)
  # does not false-flag as 2-PL dup. ASIN rows with NA product_line_id are
  # a separate data-quality concern out of scope for this audit.
  mapping_dup_counts <- asin_mapping_full %>%
    filter(!is.na(product_line_id)) %>%
    group_by(asin) %>%
    summarise(n_pls = dplyr::n_distinct(product_line_id), .groups = "drop") %>%
    filter(n_pls > 1)

  if (nrow(mapping_dup_counts) > 0) {
    # Audit winner aligned with production distinct() semantics:
    # raw row order first (NOT alphabetical). Use .orig_row preserved above.
    mapping_dup_audit <- asin_mapping_full %>%
      filter(asin %in% mapping_dup_counts$asin) %>%
      arrange(asin, .orig_row) %>%
      group_by(asin) %>%
      mutate(
        distinct_winner = .orig_row == dplyr::first(.orig_row),
        conflict_pls = paste(sort(unique(product_line_id)), collapse = ", "),
        detected_at = Sys.time()
      ) %>%
      ungroup() %>%
      select(-.orig_row)

    dbWriteTable(con_app, "df_amz_mapping_duplicates_audit",
                 as.data.frame(mapping_dup_audit), overwrite = TRUE)

    # Avoid truncation on realistic multi-co × multi-PL warnings (default
    # warning.length = 1000 truncates ~3k+ char strings — verify #818 Logic#3 fix).
    # NOTE: on.exit() at top-level inside a tryCatch() has no enclosing function
    # frame and never fires (verify #818 round 2 Logic NEW#2 / Codex on.exit caveat).
    # Production `Rscript` exits naturally so warning.length=8170L doesn't leak;
    # `source()` callers will leak the option (acceptable — script is Rscript-first).
    options(warning.length = 8170L)
    warning(sprintf(
      "[MP154 + MP163 partial] %d ASIN(s) in '%s' map to multiple product_line_id; %d rows silently dropped by distinct() (raw row order winner). See df_amz_mapping_duplicates_audit in app_data for full conflict list. PLs affected: %s",
      nrow(mapping_dup_counts),
      MAPPING_TABLE,
      nrow(asin_mapping_full) - dplyr::n_distinct(asin_mapping_full$asin),
      paste(sort(unique(mapping_dup_audit$product_line_id)), collapse = ", ")
    ))
  } else {
    # Write empty audit table so downstream can rely on the table existing
    # (MP163 Gate 1 — pipeline always runnable). Schema matches the
    # populated path (6 cols including source_header) per #818 Logic#4 fix.
    empty_audit <- data.frame(
      asin = character(), product_line_id = character(),
      source_header = character(), distinct_winner = logical(),
      conflict_pls = character(),
      detected_at = as.POSIXct(character(), tz = "UTC")
    )
    dbWriteTable(con_app, "df_amz_mapping_duplicates_audit",
                 empty_audit, overwrite = TRUE)
  }

  # Apply dedup AFTER audit captured the full picture
  asin_mapping_raw <- asin_mapping_full %>%
    select(asin, product_line_id) %>%
    distinct(asin, .keep_all = TRUE)

  # Build a simple name lookup (may not have both zh/en for every row)
  pl_names <- product_line_lookup %>%
    mutate(
      pl_zh = as.character(product_line_name_chinese),
      pl_en = as.character(product_line_name_english),
      product_line_name = ifelse(!is.na(pl_zh) & nzchar(pl_zh), pl_zh, pl_en)
    ) %>%
    select(product_line_id, product_line_name)

  asin_mapping <- asin_mapping_raw %>%
    left_join(pl_names, by = "product_line_id") %>%
    select(asin, product_line_id, product_line_name)

  message(sprintf("  OK Mapped ASINs: %d", nrow(asin_mapping)))

  message("[Step 4/6] Loading and mapping sales data...")
  # Post-glue era: canonical schema in df_amz_sales___transformed
  # (was legacy purchase_date/asin/item_price; now order_date/product_id/total_amount).
  sales_raw <- tbl2(con_transformed, INPUT_SALES_TABLE) %>%
    select(any_of(c("order_date", "product_id", "quantity",
                    "total_amount", "customer_id"))) %>%
    collect()

  if (!all(c("order_date", "product_id", "quantity", "total_amount") %in% names(sales_raw))) {
    stop("Missing required canonical columns in transformed sales (order_date, product_id, quantity, total_amount)")
  }

  sales_raw <- sales_raw %>%
    mutate(
      order_date = as.Date(order_date),  # canonical order_date is POSIXct from 2TR; as.Date truncates time
      amz_asin = as.character(product_id),  # canonical product_id is the asin-mapped column
      quantity = as.numeric(quantity),
      line_total = as.numeric(total_amount),  # bridge yaml already derived total_amount = unit_price * quantity
      lineproduct_price = line_total
    ) %>%
    filter(!is.na(order_date), !is.na(amz_asin))

  sales_mapped <- sales_raw %>%
    left_join(asin_mapping, by = c("amz_asin" = "asin"))  # asin_mapping table still uses 'asin' column (reference table, not canonical sales)

  mapped_rows <- sum(!is.na(sales_mapped$product_line_id))
  unmapped_rows <- sum(is.na(sales_mapped$product_line_id))
  message(sprintf("  OK Sales rows: %d  (mapped=%d, unmapped=%d)",
                  nrow(sales_mapped), mapped_rows, unmapped_rows))

  if (mapped_rows == 0) {
    message("[Step 5/6] No mapped sales rows; writing empty tables per MP029")
    dbWriteTable(con_app, "df_amz_sales_complete_time_series",
                 empty_time_series_schema, overwrite = TRUE)
    for (pl in product_lines) {
      out_tbl <- sprintf("df_amz_sales_complete_time_series_%s", pl)
      dbWriteTable(con_app, out_tbl, empty_time_series_schema, overwrite = TRUE)
    }
  } else {
    message("[Step 5/6] Aggregating and completing time series...")

    # Aggregate by (date, asin, product_line) — sum quantity and revenue
    sales_agg <- sales_mapped %>%
      filter(!is.na(product_line_id)) %>%
      group_by(order_date, amz_asin, product_line_id, product_line_name) %>%
      summarise(
        sales = sum(quantity, na.rm = TRUE),
        sales_platform = sum(lineproduct_price, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      rename(time = order_date)

    ts_result <- fn_complete_time_series(
      data = sales_agg,
      date_col = "time",
      group_cols = c("amz_asin", "product_line_id", "product_line_name"),
      value_cols = c("sales", "sales_platform"),
      fill_method = FILL_METHOD,
      mark_filled = TRUE,
      time_unit = TIME_UNIT
    )

    completed_all <- ts_result$data %>%
      mutate(
        sales = as.integer(round(sales)),
        sales_platform = as.numeric(sales_platform),
        product_line_id.x = product_line_id,
        product_line_id.y = product_line_id,
        source_table = INPUT_SALES_TABLE,
        processing_version = SCRIPT_VERSION,
        enrichment_version = SCRIPT_VERSION,
        filling_method = FILL_METHOD,
        filling_timestamp = Sys.time()
      ) %>%
      select(-product_line_id) %>%
      mutate(
        year = as.numeric(format(time, "%Y")),
        day = as.integer(format(time, "%d")),
        month = as.integer(format(time, "%m")),
        weekday = as.integer(format(time, "%u"))
      )

    for (i in 1:12) {
      completed_all[[paste0("month_", i)]] <- as.numeric(completed_all$month == i)
    }
    completed_all$monday    <- as.numeric(completed_all$weekday == 1)
    completed_all$tuesday   <- as.numeric(completed_all$weekday == 2)
    completed_all$wednesday <- as.numeric(completed_all$weekday == 3)
    completed_all$thursday  <- as.numeric(completed_all$weekday == 4)
    completed_all$friday    <- as.numeric(completed_all$weekday == 5)
    completed_all$saturday  <- as.numeric(completed_all$weekday == 6)
    completed_all$sunday    <- as.numeric(completed_all$weekday == 7)

    completed_all <- completed_all %>%
      select(-month, -weekday)

    # ==========================================================================
    # Step 5b/6: Enrich with product_profile + review aggregates (#674)
    #
    # Loads df_product_profile_{pl}___transformed (per active PL, UNION) and
    # aggregates df_amz_review___transformed per asin, then LEFT JOIN both onto
    # completed_all. This unlocks `comment_attribute` and `product_attribute`
    # predictor_types in D04_02 Poisson analysis (was 100% time_feature only).
    #
    # Missing source tables are skipped with a WARN so this ETL stays runnable
    # in environments where reviews / profiles are not yet ingested (MP163
    # progressive completeness — pipeline always runs, gaps surface visibly).
    # ==========================================================================
    message("[Step 5b/6] Enriching with product_profile + review aggregates...")

    rows_before_enrich <- nrow(completed_all)

    # ---- Load product profile per active product line and UNION ----
    product_profile_all <- NULL
    for (pl in product_lines) {
      profile_tbl <- sprintf("df_product_profile_%s___transformed", pl)
      if (dbExistsTable(con_transformed, profile_tbl)) {
        pl_profile <- tbl2(con_transformed, profile_tbl) %>%
          collect()
        if (nrow(pl_profile) > 0) {
          pl_profile$source_product_line_id <- pl
          if (is.null(product_profile_all)) {
            product_profile_all <- pl_profile
          } else {
            product_profile_all <- dplyr::bind_rows(product_profile_all, pl_profile)
          }
        }
      } else {
        message(sprintf("  WARN: product_profile table missing: %s", profile_tbl))
      }
    }

    if (!is.null(product_profile_all) && nrow(product_profile_all) > 0) {
      product_profile_all <- product_profile_all %>%
        mutate(product_id = as.character(product_id)) %>%
        filter(!is.na(product_id), product_id != "") %>%
        distinct(product_id, .keep_all = TRUE)  # one row per asin

      # Drop columns that would collide with completed_all (product_line_id is
      # already present as .x/.y; we keep source_product_line_id for trace)
      product_profile_join <- product_profile_all %>%
        select(-any_of(c("product_line_id")))

      completed_all <- completed_all %>%
        left_join(product_profile_join, by = c("amz_asin" = "product_id"))

      message(sprintf("  OK product_profile enrichment: %d distinct asins, %d columns added",
                      nrow(product_profile_all),
                      ncol(product_profile_join) - 1))  # -1 for the JOIN key
    } else {
      message("  WARN: No product_profile rows available; skipping profile enrichment")
    }

    # ---- Aggregate df_amz_review___transformed per asin ----
    if (dbExistsTable(con_transformed, "df_amz_review___transformed")) {
      review_agg <- tbl2(con_transformed, "df_amz_review___transformed") %>%
        select(any_of(c("product_id", "helpful"))) %>%
        collect() %>%
        mutate(
          product_id = as.character(product_id),
          helpful = suppressWarnings(as.numeric(helpful))
        ) %>%
        filter(!is.na(product_id), product_id != "") %>%
        group_by(product_id) %>%
        summarise(
          review_count = dplyr::n(),
          helpful_avg = mean(helpful, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(helpful_avg = ifelse(is.nan(helpful_avg), NA_real_, helpful_avg))

      completed_all <- completed_all %>%
        left_join(review_agg, by = c("amz_asin" = "product_id"))

      message(sprintf("  OK review enrichment: %d asins with review aggregates", nrow(review_agg)))
    } else {
      message("  WARN: df_amz_review___transformed missing; skipping review enrichment")
    }

    # ---- LEFT JOIN invariant: row count MUST be preserved ----
    rows_after_enrich <- nrow(completed_all)
    if (rows_after_enrich != rows_before_enrich) {
      stop(sprintf(
        "Enrichment broke LEFT JOIN invariant: before=%d, after=%d (likely duplicate keys in profile/review)",
        rows_before_enrich, rows_after_enrich
      ))
    }
    message(sprintf("  OK enrichment preserved row count: %d", rows_after_enrich))

    ordered_cols <- c(
      "amz_asin", "time", "year", "day",
      paste0("month_", 1:12),
      "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
      "product_line_id.x", "sales", "sales_platform",
      "product_line_name", "product_line_id.y",
      "data_source", "filling_method", "filling_timestamp",
      "source_table", "processing_version", "enrichment_version"
    )

    completed_all <- completed_all %>%
      select(any_of(ordered_cols), everything())

    dbWriteTable(con_app, "df_amz_sales_complete_time_series",
                 completed_all, overwrite = TRUE)

    for (pl in product_lines) {
      out_tbl <- sprintf("df_amz_sales_complete_time_series_%s", pl)
      pl_data <- completed_all %>%
        filter(product_line_id.x == pl)
      dbWriteTable(con_app, out_tbl, pl_data, overwrite = TRUE)
      rows_processed <- rows_processed + nrow(pl_data)
    }
    message(sprintf("  OK Time series rows: %d", rows_processed))
  }

  message("[Step 6/6] Completed")

}, error = function(e) {
  message("ERROR in MAIN: ", e$message)
  error_occurred <<- TRUE
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred) {
  tryCatch({
    required_cols <- c("time", "sales", "product_line_id.x")
    if (!dbExistsTable(con_app, "df_amz_sales_complete_time_series")) {
      stop("Missing: df_amz_sales_complete_time_series")
    }
    cols <- dbListFields(con_app, "df_amz_sales_complete_time_series")
    missing_cols <- setdiff(required_cols, cols)
    if (length(missing_cols) > 0) {
      stop(sprintf("Missing columns: %s", paste(missing_cols, collapse = ", ")))
    }

    # #818 defensive instrumentation: verify mapping duplicates audit table
    # exists (always — even when empty — per MP163 progressive completeness)
    if (!dbExistsTable(con_app, "df_amz_mapping_duplicates_audit")) {
      stop("Missing: df_amz_mapping_duplicates_audit (#818 defensive audit table)")
    }
    audit_cols <- dbListFields(con_app, "df_amz_mapping_duplicates_audit")
    required_audit_cols <- c("asin", "product_line_id", "distinct_winner", "conflict_pls")
    missing_audit_cols <- setdiff(required_audit_cols, audit_cols)
    if (length(missing_audit_cols) > 0) {
      stop(sprintf("Audit table missing columns: %s",
                   paste(missing_audit_cols, collapse = ", ")))
    }

    message("  OK Output tables validated")
    test_passed <- TRUE
  }, error = function(e) {
    message("ERROR in TEST: ", e$message)
    test_passed <<- FALSE
  })
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

end_time <- Sys.time()
message("====================================================================")
message("ETL SUMMARY")
message("====================================================================")
message(sprintf("Script:           amz_ETL_sales_time_series_2TR.R"))
message(sprintf("Status:           %s", ifelse(test_passed, "SUCCESS", "FAILED")))
message(sprintf("Rows Processed:   %d", rows_processed))
message(sprintf("Execution Time:   %.2f seconds",
                as.numeric(difftime(end_time, start_time, units = "secs"))))
message("====================================================================")

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

if (exists("con_raw") && inherits(con_raw, "DBIConnection")) {
  dbDisconnect(con_raw, shutdown = TRUE)
}
if (exists("con_transformed") && inherits(con_transformed, "DBIConnection")) {
  dbDisconnect(con_transformed, shutdown = TRUE)
}
if (exists("con_app") && inherits(con_app, "DBIConnection")) {
  dbDisconnect(con_app, shutdown = TRUE)
}

autodeinit()

# verify #818 Logic#5 / Codex#2 fix — exit nonzero on PART 3 TEST failure
# so CI / cron / make pipeline catches schema regressions instead of
# silently returning exit 0 with "Status: FAILED" only in log.
if (exists("test_passed") && isFALSE(test_passed)) {
  quit(status = 1, save = "no")
}
# End of file
