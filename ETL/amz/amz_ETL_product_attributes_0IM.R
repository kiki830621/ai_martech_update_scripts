# amz_ETL_product_attributes_0IM.R - Bridge-driven 0IM for product attributes (#569)
#####
# CONSUMES: data/database_to_csv/raw_data/df_product_profile_{pl}.csv (12 PLs)
# PRODUCES: raw_data.df_amz_product_attributes_{pl}___raw (one per PL)
# DEPENDS_ON_ETL: amz_ETL_product_profiles_2TR (legacy ETL produces df_product_profile_*
#                 in raw_data; all_S02_00 exports to CSV; this script reads CSVs and
#                 invokes fn_glue_bridge to write canonical bridge-driven raw tables)
# Bridges consumed: bridges/QEF_DESIGN/amz/product_attributes_{pl}.bridge.yaml
#                 + bridges/QEF_DESIGN/amz/company_product_extension.bridge.yaml
# Following: MP064, MP102 v1.3, MP156, MP157, MP158, MP163, DM_R028
#####
# Background:
#   12 bridges authored 2026-04-29 (#460 Phase 9 task 9.6) but never consumed
#   at runtime — no R script in the make pipeline invoked fn_glue_bridge().
#   This script closes that gap. With #564's fn_glue_bridge.R fix to honor
#   optional_fields and #543/#569 ship-readiness flow, this is the missing
#   piece for the canonical ASIN → product_line mapping (which feeds D11
#   product_master VIEW + sentinel asymptotic shrinkage per MP163 Gate 4).
#
#   Bridges with REQUIRES_HUMAN_REVIEW placeholder will be REJECTED by
#   fn_glue_bridge's reviewer guard (MP102 v1.3 ship-readiness gate).
#   Per QEF state on 2026-05-05: only product_attributes_blb is ship-ready;
#   the other 11 bridges (10 product_attributes + company_product_extension)
#   need /glue-bridge ensemble review + Step 6e to ship. Until then this
#   orchestrator will skip them with INFO message — graceful no-op (per MP163
#   progressive completeness).

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Locate sql_read helper for autoinit fallback
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

# Initialize via autoinit (loads db_path_list, GLOBAL_DIR, etc.)
autoinit()

# Source fn_glue_bridge runtime
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_glue_bridge.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_apply_mapping.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_validate_against_schema.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_hash_prerawdata_schema.R"))

raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

# Bridge yaml root
bridges_root <- file.path(GLOBAL_DIR, "01_db", "raw_schema",
                          "_authoring", "bridges")

# CSV source root (produced by all_S02_00 from prior ETL)
csv_source_root <- "data/database_to_csv/raw_data"

# ==============================================================================
# 2. MAIN — invoke fn_glue_bridge for each bridge
# ==============================================================================

message("amz_ETL_product_attributes_0IM: starting bridge-driven 0IM")

# Bridges to process (per #569 scope: 11 product_attributes + 1 company_product_extension)
bridges_spec <- list(
  product_attributes_blb = "df_product_profile_blb.csv",
  product_attributes_bys = "df_product_profile_bys.csv",
  product_attributes_cas = "df_product_profile_cas.csv",
  product_attributes_hsg = "df_product_profile_hsg.csv",
  product_attributes_its = "df_product_profile_its.csv",
  product_attributes_psg = "df_product_profile_psg.csv",
  product_attributes_rpl = "df_product_profile_rpl.csv",
  product_attributes_sfg = "df_product_profile_sfg.csv",
  product_attributes_sfo = "df_product_profile_sfo.csv",
  product_attributes_sgf = "df_product_profile_sgf.csv",
  product_attributes_sgo = "df_product_profile_sgo.csv",
  product_attributes_sss = "df_product_profile_sss.csv",
  company_product_extension = "df_company_product_extension.csv"
)

results <- list()
n_processed <- 0L
n_skipped <- 0L
n_errored <- 0L

main_run <- tryCatch({
  for (bridge_source in names(bridges_spec)) {
    csv_filename <- bridges_spec[[bridge_source]]
    csv_path <- file.path(csv_source_root, csv_filename)

    if (!file.exists(csv_path)) {
      message(sprintf("  [SKIP] %s: source CSV not found at %s",
                      bridge_source, csv_path))
      n_skipped <- n_skipped + 1L
      results[[bridge_source]] <- list(status = "skip_no_source",
                                        csv_path = csv_path)
      next
    }

    # Load CSV as data.frame
    prerawdata <- tryCatch(
      readr::read_csv(csv_path, show_col_types = FALSE),
      error = function(e) {
        message(sprintf("  [SKIP] %s: CSV read error: %s",
                        bridge_source, e$message))
        NULL
      }
    )
    if (is.null(prerawdata)) {
      n_skipped <- n_skipped + 1L
      results[[bridge_source]] <- list(status = "skip_csv_error")
      next
    }

    prerawdata <- as.data.frame(prerawdata)

    # Invoke fn_glue_bridge — reviewer guard inside will reject placeholder
    # reviewed_by bridges per MP102 v1.3
    res <- tryCatch({
      fn_glue_bridge(
        company = "QEF_DESIGN",
        platform = "amz",
        source = bridge_source,
        prerawdata = prerawdata,
        target_con = raw_data,
        bridges_root = bridges_root
      )
    }, error = function(e) {
      msg <- e$message
      if (grepl("REQUIRES_HUMAN_REVIEW|placeholder|reviewed_by", msg, ignore.case = TRUE)) {
        message(sprintf("  [SKIP] %s: bridge not ship-ready (reviewed_by placeholder) - %s",
                        bridge_source, substr(msg, 1, 80)))
        list(status = "skip_not_ship_ready", error = msg)
      } else if (grepl("Verdict|review.md|sibling", msg, ignore.case = TRUE)) {
        message(sprintf("  [SKIP] %s: missing review.md or Verdict line - %s",
                        bridge_source, substr(msg, 1, 80)))
        list(status = "skip_no_verdict", error = msg)
      } else if (grepl("[Ss]chema drift|[Ff]ingerprint", msg)) {
        # Per /glue-bridge Gotcha #1: schema drift is recoverable via refingerprint.
        # Don't treat as terminal error — log and continue to other bridges.
        # Resolution: re-run /glue-bridge skill on the affected bridge to update
        # the schema_fingerprint, OR investigate why the source changed.
        message(sprintf("  [SKIP] %s: schema drift (refingerprint needed) - %s",
                        bridge_source, substr(msg, 1, 120)))
        list(status = "skip_drift", error = msg)
      } else {
        message(sprintf("  [ERROR] %s: %s", bridge_source,
                        substr(msg, 1, 200)))
        list(status = "error", error = msg)
      }
    })

    if (is.null(res$status) || !is.character(res$status)) {
      # Successful run
      n_processed <- n_processed + 1L
      results[[bridge_source]] <- c(list(status = "ok"), res)
      message(sprintf("  [OK] %s: %d input → %d output rows, %d errors",
                      bridge_source,
                      res$n_input_rows %||% NA,
                      res$n_output_rows %||% NA,
                      res$n_errors %||% NA))
    } else if (res$status == "error") {
      n_errored <- n_errored + 1L
      results[[bridge_source]] <- res
    } else {
      n_skipped <- n_skipped + 1L
      results[[bridge_source]] <- res
    }
  }

  TRUE
}, error = function(e) {
  main_error <<- e$message
  message("amz_ETL_product_attributes_0IM: MAIN ERROR — ", e$message)
  FALSE
})

# ==============================================================================
# 3. TEST — verify at least 1 bridge produced a non-empty raw table
# ==============================================================================

if (main_run) {
  test_run <- tryCatch({
    pl_codes <- gsub("^product_attributes_", "", grep("^product_attributes_",
                                                      names(results), value = TRUE))
    n_tables_present <- 0L
    for (pl in pl_codes) {
      table_name <- sprintf("df_amz_product_attributes_%s___raw", pl)
      if (DBI::dbExistsTable(raw_data, table_name)) {
        n <- DBI::dbGetQuery(raw_data,
          sprintf("SELECT COUNT(*) FROM %s", table_name))[[1]]
        if (n > 0) n_tables_present <- n_tables_present + 1L
      }
    }
    message(sprintf("amz_ETL_product_attributes_0IM: TEST - %d non-empty raw tables present",
                    n_tables_present))
    n_tables_present > 0L  # at least 1 must succeed for test to pass
  }, error = function(e) {
    message("amz_ETL_product_attributes_0IM: TEST ERROR — ", e$message)
    FALSE
  })
  test_passed <- test_run
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================

message("amz_ETL_product_attributes_0IM: SUMMARY")
message(sprintf("  Bridges processed (ok):       %d", n_processed))
message(sprintf("  Bridges skipped (not ready):  %d", n_skipped))
message(sprintf("  Bridges errored:              %d", n_errored))
message(sprintf("  Test passed (>=1 raw table):  %s", test_passed))

script_success <- main_run && (n_errored == 0L)

# ==============================================================================
# 5. DEINIT
# ==============================================================================

# Close raw_data connection AFTER all phases (TEST included) complete
if (exists("raw_data") && DBI::dbIsValid(raw_data)) {
  DBI::dbDisconnect(raw_data, shutdown = TRUE)
}

if (!script_success) {
  if (!is.null(main_error)) {
    stop(sprintf("amz_ETL_product_attributes_0IM failed: %s", main_error))
  }
  if (n_errored > 0L) {
    stop(sprintf("amz_ETL_product_attributes_0IM: %d bridge(s) errored — see log",
                 n_errored))
  }
}

# AUTOINIT cleanup
if (exists("autodeinit", mode = "function")) autodeinit()
