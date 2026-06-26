# amz_ETL_sales_0IM.R - Bridge-driven 0IM for amz main sales (#1418)
#####
# CONSUMES: data/local_data/rawdata_<COMPANY>/amazon_sales/{YYYY-M}/{YYYY-MM}.xlsx (flat xlsx)
# PRODUCES: raw_data.df_amz_sales___raw
# DEPENDS_ON_ETL: none
# Bridges consumed: bridges/<COMPANY>/amz/sales.bridge.v2.yaml (source_type: xlsx)
# Following: MP064 (ETL-Derivation separation), MP102 v1.3, MP156-158, MP163, DM_R028, DM_R061
#####
# Background (#1418):
#   The glue cutover #627 (commit 1a6b45c) archived the legacy amz_ETL_sales_0IM.R
#   (df_amazon_sales importer) and pointed to "run sales.bridge.yaml" as the
#   replacement, but never created a bridge-era runner. Result: df_amz_sales___raw
#   could not be rebuilt via `make run` (amz_ETL_sales_1ST.R merely CONSUMES it and
#   stop()s if absent). This restores the runner in bridge-era form.
#
#   sales.bridge.v2.yaml is source_type: xlsx (FLAT) — unlike folder_tree bridges
#   (e.g. competitor_sales), a flat source requires the CALLER to read the source
#   and pass `prerawdata`. The monthly All-Orders xlsx are single-tab (tab name =
#   month, not per-PL), read with col_types="text" to MATCH the bridge schema
#   fingerprint (Excel date cells then arrive as serial strings — see #1383). The
#   bridge's order_date `to_iso_date` coercion (#1383, commit 0f71b41) normalizes
#   the Excel serial + ISO mix to a uniform ISO date.
#
#   df_amz_sales___raw is order-level (one row per order line); product_line_id is
#   NOT a raw column — it is derived downstream. So this runner needs no PL/tab logic.
#
#   Company-aware (IC_P002 / #1219 pattern): resolves the current company from cwd
#   and runs ITS sales bridge. Companies without a sales bridge are a graceful no-op
#   (MP163) — no break to their `make run`. Write is idempotent (bridge import is
#   DELETE-by-import_source + INSERT; raw is rebuildable, DM_R061).

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

autoinit()

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
})

source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_glue_bridge.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_apply_mapping.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_validate_against_schema.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_hash_prerawdata_schema.R"))

raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

bridges_root <- file.path(GLOBAL_DIR, "01_db", "raw_schema", "_authoring", "bridges")
prerawdata_root <- getwd()

# Company-aware bridge resolution (IC_P002 / #1219 cross-co)
current_company <- basename(normalizePath(getwd()))
# Runtime prefers the v2 schema-driven bridge (#989); v1 is the legacy fallback.
company_bridge_v2 <- file.path(bridges_root, current_company, "amz", "sales.bridge.v2.yaml")
company_bridge_v1 <- file.path(bridges_root, current_company, "amz", "sales.bridge.yaml")
has_sales_bridge <- file.exists(company_bridge_v2) || file.exists(company_bridge_v1)

# ==============================================================================
# 2. MAIN — read flat xlsx (col_types=text) + invoke fn_glue_bridge
# ==============================================================================

if (!has_sales_bridge) {
  # Graceful no-op (MP163): company has no sales bridge.
  message(sprintf(
    "amz_ETL_sales_0IM: skip — no sales bridge for %s. No-op (MP163).",
    current_company
  ))
  DBI::dbDisconnect(raw_data, shutdown = TRUE)
  autodeinit()
} else {
  message(sprintf("amz_ETL_sales_0IM: starting flat-xlsx bridge-driven 0IM for %s (#1418)", current_company))

  main_run <- tryCatch({
    # Glob the monthly All-Orders xlsx. The bridge source_uri is
    # data/local_data/rawdata_<COMPANY>/amazon_sales/{YYYY-M}/{YYYY-MM}.xlsx;
    # one single-tab file per month.
    xlsx_glob <- file.path("data", "local_data",
                           paste0("rawdata_", current_company),
                           "amazon_sales", "*", "*.xlsx")
    xlsx_files <- sort(Sys.glob(xlsx_glob))
    if (length(xlsx_files) == 0L) {
      stop(sprintf("no monthly All-Orders xlsx matched '%s'", xlsx_glob))
    }
    message(sprintf("  reading %d monthly All-Orders xlsx (col_types='text')", length(xlsx_files)))

    # col_types='text' matches the bridge schema fingerprint (all-character read);
    # Excel date cells arrive as serial strings, normalized by the bridge's
    # order_date to_iso_date coercion (#1383).
    reads <- lapply(xlsx_files, function(f) readxl::read_excel(f, col_types = "text"))

    # Align each file to the canonical All-Orders schema before binding. The
    # Amazon All-Orders export has a FIXED column ORDER; a stray data-entry cell
    # can overwrite a single header label in one month's file (verified: 2024-06
    # had its position-1 `amazon-order-id` header overwritten with an ASIN
    # `B0D6BHH2FM`, while the column data were still the order IDs). A naive
    # bind_rows column-union would then break the bridge schema fingerprint (a
    # sorted names-only sha256 over the canonical 41-col schema) AND mis-shape the
    # raw. Recover by position (DM_R064 parse-time positional exception — position
    # IS the semantic here, headers are labels), but ONLY for minor corruption:
    #   - canonical = the consensus (modal) column-name vector across all files
    #   - a file with the canonical column COUNT and <= 2 mismatched header names
    #     gets the canonical names assigned by position (recovers corrupted labels)
    #   - a file whose shape diverges materially is DROPPED with a surfaced warning
    #     (MP163) rather than silently mis-mapped.
    name_vecs <- lapply(reads, names)
    key  <- vapply(name_vecs, function(n) paste(n, collapse = "\x1f"), character(1))
    ukey <- unique(key)
    canonical_names <- name_vecs[[ which(key == ukey[which.max(tabulate(match(key, ukey)))])[1] ]]
    ncanon <- length(canonical_names)

    aligned <- vector("list", length(reads))
    for (i in seq_along(reads)) {
      nm <- name_vecs[[i]]
      n_mismatch <- if (length(nm) == ncanon) sum(nm != canonical_names) else NA_integer_
      if (!is.na(n_mismatch) && n_mismatch <= 2L) {
        if (n_mismatch > 0L) {
          message(sprintf("  [recover] %s: %d corrupted header(s) restored by position -> %s",
                          basename(xlsx_files[i]), n_mismatch,
                          paste(canonical_names[nm != canonical_names], collapse = ", ")))
          names(reads[[i]]) <- canonical_names
        }
        aligned[[i]] <- reads[[i]]
      } else {
        warning(sprintf(
          "amz_ETL_sales_0IM: %s schema diverges from canonical (ncol=%d vs %d) — DROPPED, not positionally recoverable (MP163)",
          basename(xlsx_files[i]), length(nm), ncanon), call. = FALSE)
      }
    }
    aligned <- aligned[!vapply(aligned, is.null, logical(1))]
    prerawdata <- dplyr::bind_rows(aligned)
    message(sprintf("  read %d rows x %d cols across %d/%d files",
                    nrow(prerawdata), ncol(prerawdata), length(aligned), length(xlsx_files)))

    res <- fn_glue_bridge(
      company = current_company,
      platform = "amz",
      source = "sales",
      prerawdata = prerawdata,        # flat source: caller supplies the data
      target_con = raw_data,
      bridges_root = bridges_root,
      prerawdata_root = prerawdata_root
    )

    if (!("df_amz_sales___raw" %in% dbListTables(raw_data))) {
      stop("VALIDATE FAILED: df_amz_sales___raw missing after bridge import")
    }
    n_rows <- sql_read(raw_data, "SELECT COUNT(*) AS n FROM df_amz_sales___raw")$n[1]
    if (is.null(n_rows) || is.na(n_rows) || n_rows == 0L) {
      stop("VALIDATE FAILED: df_amz_sales___raw empty after bridge import")
    }
    message(sprintf("  [OK] sales bridge: %d rows in df_amz_sales___raw", n_rows))

    # #1383 regression guard: order_date must be uniform ISO yyyy-mm-dd (no Excel
    # serial '45xxx' leak). DuckDB SIMILAR TO is a full-string regex match.
    bad_dates <- sql_read(raw_data,
      "SELECT COUNT(*) AS n FROM df_amz_sales___raw
         WHERE order_date IS NOT NULL
           AND order_date NOT SIMILAR TO '[0-9]{4}-[0-9]{2}-[0-9]{2}'")$n[1]
    if (!is.na(bad_dates) && bad_dates > 0L) {
      warning(sprintf(
        "amz_ETL_sales_0IM: %d order_date value(s) are not ISO yyyy-mm-dd — possible #1383 regression (to_iso_date coercion not applied?)",
        bad_dates), call. = FALSE)
    } else {
      message("  [OK] order_date all ISO yyyy-mm-dd (#1383 to_iso_date applied)")
    }

    TRUE
  }, error = function(e) {
    message("  [ERROR] amz_ETL_sales_0IM: ", conditionMessage(e))
    FALSE
  })

  # ============================================================================
  # 3. DEINITIALIZE
  # ============================================================================
  DBI::dbDisconnect(raw_data, shutdown = TRUE)
  if (!isTRUE(main_run)) {
    stop("amz_ETL_sales_0IM failed — see [ERROR] above")
  }
  message("amz_ETL_sales_0IM: done")
  autodeinit()
}
