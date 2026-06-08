# amz_ETL_competitor_sales_0IM.R - Bridge-driven 0IM for competitor sales (#1219)
#####
# CONSUMES: data/local_data/rawdata_<COMPANY>/competitor_sales/{product_line_dir}/{asin} (folder_tree)
# PRODUCES: raw_data.df_amz_competitor_sales___raw
# DEPENDS_ON_ETL: none
# Bridges consumed: bridges/<COMPANY>/amz/competitor_sales.bridge.yaml (folder_tree)
# Following: MP064 (ETL-Derivation separation), MP102 v1.3, MP156-158, MP163, DM_R028, DM_R070
#####
# Background (#1219):
#   competitor_sales.bridge.yaml (folder_tree, value_map folder name -> canonical
#   product_line_id) was orphaned — never wired. raw_data.df_amz_competitor_sales was
#   instead produced by legacy core_import_df_amz_competitor_sales() called from a DRV
#   script (amz_D03_10.R) using positional-index folder->PL mapping → whole-line
#   off-by-one PL mislabeling AND rebuilt raw in DRV (MP064 violation). This ETL-layer
#   0IM invokes fn_glue_bridge with the bridge's correct value_map.
#
#   Company-aware (#1219 cross-co / IC_P002): resolves the current company from cwd and
#   runs ITS competitor_sales bridge. Companies without a competitor_sales bridge yet
#   (#1200 Phase 4.x) are a graceful no-op (MP163) — no break to their make run.
#   folder_tree bridge: prerawdata = NULL (fn_glue_bridge reads the folder tree itself).
#   Write is idempotent DELETE-by-import_source + INSERT (legal re-ingest, DM_R061).

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

source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_glue_bridge.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_apply_mapping.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_validate_against_schema.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_hash_prerawdata_schema.R"))

raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

bridges_root <- file.path(GLOBAL_DIR, "01_db", "raw_schema", "_authoring", "bridges")
prerawdata_root <- getwd()

# Company-aware bridge resolution (#1219 cross-co)
current_company <- basename(normalizePath(getwd()))
company_bridge <- file.path(bridges_root, current_company, "amz", "competitor_sales.bridge.yaml")

# ==============================================================================
# 2. MAIN — invoke fn_glue_bridge (folder_tree, prerawdata = NULL)
# ==============================================================================

if (!file.exists(company_bridge)) {
  # Graceful no-op (MP163): company has no competitor_sales bridge yet (#1200 Phase 4.x).
  message(sprintf("amz_ETL_competitor_sales_0IM: skip — no competitor_sales bridge for %s (%s); #1200 Phase 4.x. No-op (MP163).",
                  current_company, company_bridge))
  DBI::dbDisconnect(raw_data, shutdown = TRUE)
  autodeinit()
} else {
  message(sprintf("amz_ETL_competitor_sales_0IM: starting folder_tree bridge-driven 0IM for %s (#1219)", current_company))

  main_run <- tryCatch({
    res <- fn_glue_bridge(
      company = current_company,
      platform = "amz",
      source = "competitor_sales",
      prerawdata = NULL,              # folder_tree reads its own source
      target_con = raw_data,
      bridges_root = bridges_root,
      prerawdata_root = prerawdata_root
    )

    if (!("df_amz_competitor_sales___raw" %in% dbListTables(raw_data))) {
      stop("VALIDATE FAILED: df_amz_competitor_sales___raw missing after bridge import")
    }
    n_rows <- sql_read(raw_data,
      "SELECT COUNT(*) AS n FROM df_amz_competitor_sales___raw")$n[1]
    if (is.null(n_rows) || is.na(n_rows) || n_rows == 0L) {
      stop("VALIDATE FAILED: df_amz_competitor_sales___raw empty after bridge import")
    }
    message(sprintf("  [OK] competitor_sales bridge: %d rows in df_amz_competitor_sales___raw", n_rows))

    per_pl <- sql_read(raw_data,
      "SELECT product_line_id, COUNT(DISTINCT amz_asin) AS n_asin
         FROM df_amz_competitor_sales___raw GROUP BY product_line_id")
    message(sprintf("  competitor_sales spans %d product lines", nrow(per_pl)))

    TRUE
  }, error = function(e) {
    message("  [ERROR] amz_ETL_competitor_sales_0IM: ", conditionMessage(e))
    FALSE
  })

  # ==============================================================================
  # 3. DEINITIALIZE
  # ==============================================================================
  DBI::dbDisconnect(raw_data, shutdown = TRUE)
  if (!isTRUE(main_run)) {
    stop("amz_ETL_competitor_sales_0IM failed — see [ERROR] above")
  }
  message("amz_ETL_competitor_sales_0IM: done")
  autodeinit()
}
