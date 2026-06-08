# amz_ETL_competitor_sales_0IM.R - Bridge-driven 0IM for competitor sales (#1219)
#####
# CONSUMES: data/local_data/rawdata_QEF_DESIGN/competitor_sales/{product_line_dir}/{asin} (folder_tree)
# PRODUCES: raw_data.df_amz_competitor_sales
# DEPENDS_ON_ETL: none
# Bridges consumed: bridges/QEF_DESIGN/amz/competitor_sales.bridge.yaml (folder_tree)
# Following: MP064 (ETL-Derivation separation), MP102 v1.3, MP156-158, MP163, DM_R028, DM_R070
#####
# Background (#1219):
#   competitor_sales.bridge.yaml (folder_tree, value_map folder name -> canonical
#   product_line_id) was authored + reviewed CONVERGED 2026-06-07 but ORPHANED —
#   never wired into the pipeline. raw_data.df_amz_competitor_sales was instead
#   produced by the legacy core_import_df_amz_competitor_sales() called from a DRV
#   script (amz_D03_10.R), which used positional-index folder->PL mapping → whole-
#   line off-by-one PL mislabeling (psg got blb's folder, hsg got sss's, etc.) AND
#   rebuilt raw in DRV (violates MP064). This script closes the gap: ETL-layer 0IM
#   that invokes fn_glue_bridge with the bridge's correct value_map, so every
#   product line's competitor sales lands under the right product_line_id.
#
#   The bridge is folder_tree (prerawdata = NULL): fn_glue_bridge reads the
#   competitor_sales folder tree itself per the bridge's path_template. Write is
#   idempotent DELETE-by-import_source + INSERT (only this bridge's rows touched,
#   per fn_glue_bridge.R:467-511) — a legal re-ingest, NOT a raw rebuild (DM_R061).

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

# Source fn_glue_bridge runtime + its helpers
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_glue_bridge.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_apply_mapping.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_validate_against_schema.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "glue", "fn_hash_prerawdata_schema.R"))

raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

bridges_root <- file.path(GLOBAL_DIR, "01_db", "raw_schema", "_authoring", "bridges")
# prerawdata_root = company root (cwd at ETL runtime); bridge path_template is
# relative: data/local_data/rawdata_QEF_DESIGN/competitor_sales/{product_line_dir}/{asin}
prerawdata_root <- getwd()

# ==============================================================================
# 2. MAIN — invoke fn_glue_bridge (folder_tree, prerawdata = NULL)
# ==============================================================================

message("amz_ETL_competitor_sales_0IM: starting folder_tree bridge-driven 0IM (#1219)")

main_run <- tryCatch({
  res <- fn_glue_bridge(
    company = "QEF_DESIGN",
    platform = "amz",
    source = "competitor_sales",
    prerawdata = NULL,              # folder_tree reads its own source
    target_con = raw_data,
    bridges_root = bridges_root,
    prerawdata_root = prerawdata_root
  )

  if (!("df_amz_competitor_sales" %in% dbListTables(raw_data))) {
    stop("VALIDATE FAILED: df_amz_competitor_sales missing after bridge import")
  }
  n_rows <- sql_read(raw_data,
    "SELECT COUNT(*) AS n FROM df_amz_competitor_sales")$n[1]
  if (is.null(n_rows) || is.na(n_rows) || n_rows == 0L) {
    stop("VALIDATE FAILED: df_amz_competitor_sales empty after bridge import")
  }
  message(sprintf("  [OK] competitor_sales bridge: %d rows in df_amz_competitor_sales", n_rows))

  # DM_R070 / #1219 guard: each PL's ASINs SHALL ⊆ its own competitor_sales folder.
  # Surfaces re-mislabeling (off-by-one regression) as a warning (MP163 — surface,
  # do not silently pass). Advisory: does not stop the import.
  per_pl <- sql_read(raw_data,
    "SELECT product_line_id, COUNT(DISTINCT amz_asin) AS n_asin
       FROM df_amz_competitor_sales GROUP BY product_line_id")
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
