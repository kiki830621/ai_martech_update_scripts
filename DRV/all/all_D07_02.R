#####
# CONSUMES: df_position, df_all_comment_property
# PRODUCES: df_market_segmentation, df_market_segmentation_summary
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: amz_D03_11
#####
#
# all_D07_02.R — D07_02 pipeline DRV: deterministic market-segmentation snapshot.
# Spectra change: market-segmentation-shared-snapshot (Task 2.1; Decisions 1-7).
#
# Targets-wired execution wrapper for run_D07_02() (fn_D07_02_core.R). Materializes
# the ONE deterministic canonical segmentation snapshot — df_market_segmentation
# (product grain) + df_market_segmentation_summary (segment grain) — that EVERY
# BrandEdge segmentation surface reads (positionMS upper chart, the
# csa_market_segments narrative, the 6-axis profiling radar, the Ansoff feed), so
# they always agree (Decision 1).
#
# Decision 2 lockstep: the snapshot is produced as the FIRST step of the same
# frozen D07 precompute, BEFORE the csa_market_segments narrative (all_D07_01.R),
# which now READS this snapshot (Task 2.2). all_D07_01.R declares DEPENDS_ON_DRV:
# all_D07_02 so the pipeline runs the snapshot first.
#
# IMPORTANT (ordering): the targets DAG dependency is assembled by the pipeline
# config (config-scan + app_config pipeline section), NOT by the header markers
# above. config-scan's generic within-group rule makes a higher-sequence script
# depend on the lower one (i.e. all_D07_02 -> all_D07_01), which is the OPPOSITE
# of the lockstep order needed here. Task 6.1 (cross-company config) MUST set the
# config so all_D07_01 depends on all_D07_02 (and all_D07_02 does NOT depend on
# all_D07_01). See the implementation note for this change.
#
# LLM-free + deterministic (perform_csa_analysis hclust primary + 245-attribute
# naming + chi-square 6-axis profile); cheap to co-locate per refresh.
#
# Platform-agnostic (DM_R066): processes ALL of the company's active platforms
# derived from df_position. Runs in pipeline (UPDATE_MODE) context.
#
# @principle MP167, MP140, MP163, MP064, DM_R023, DM_R066

# --- init (mirror all_D07_01.R) ---------------------------------------------
sql_read_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R")
)
sql_read_path <- sql_read_candidates[file.exists(sql_read_candidates)][1]
if (!is.na(sql_read_path)) source(sql_read_path)
needgoogledrive <- FALSE
autoinit()

suppressPackageStartupMessages({ library(DBI); library(duckdb); library(dplyr) })

# Locate global_scripts files robustly regardless of the runner's CWD.
gs <- function(...) {
  for (base in c(file.path("scripts", "global_scripts"),
                 file.path("..", "global_scripts"),
                 file.path("..", "..", "global_scripts"),
                 file.path("..", "..", "..", "global_scripts"))) {
    p <- file.path(base, ...)
    if (file.exists(p)) return(p)
  }
  stop("cannot locate global_scripts file: ", file.path(...))
}

# 16_derivations is NOT auto-sourced by autoinit (sweep policy #517) — source the
# snapshot producer + its pure helpers explicitly.
source(gs("16_derivations", "fn_market_segmentation_snapshot_helpers.R"))
source(gs("16_derivations", "fn_D07_02_core.R"))
if (!exists("generate_create_table_query", mode = "function"))
  source(gs("01_db", "generate_create_table_query", "fn_generate_create_table_query.R"))
if (!exists("create_df_market_segmentation_tables", mode = "function"))
  source(gs("01_db", "fn_create_df_market_segmentation_tables.R"))

# Deterministic clustering / naming / profiling engines live in BrandEdge
# component files. The #517 sweep narrowing may skip them under UPDATE_MODE, so
# source defensively (top-level = definitions only; the !exists guard makes it a
# no-op once the sweep already loaded them).
if (!exists("perform_csa_analysis", mode = "function") || !exists("analyze_clusters", mode = "function")) {
  source(gs("10_rshinyapp_components", "position", "positionMSPlotly", "positionMSPlotly.R"))
}
if (!exists("compute_cluster_top_var", mode = "function") || !exists("compute_profile_deviation_table", mode = "function")) {
  source(gs("10_rshinyapp_components", "position", "marketSegmentationKmeans", "marketSegmentationKmeans.R"))
}
if (!exists("make_names", mode = "function")) source(gs("05_etl_utils", "common", "string", "fn_make_names.R"))

# analyze_clusters uses translate() for the #1346 "Others / Mixed" bucket label;
# translate is assigned to .GlobalEnv at APP startup but never in UPDATE_MODE
# precompute. Initialize it here so the engines resolve it (mirrors all_D07_01.R;
# identity fallback if unavailable).
if (!exists("translate", mode = "function")) {
  if (!exists("initialize_ui_translation", mode = "function")) source(gs("04_utils", "fn_initialize_ui_translation.R"))
  if (!exists("initialize_translation_system", mode = "function")) source(gs("04_utils", "fn_translation.R"))
  translate <- initialize_ui_translation()
}

stopifnot(exists("run_D07_02", mode = "function"),
          exists("build_theme_axis_map", mode = "function"),
          exists("compute_market_segmentation_snapshot", mode = "function"),
          exists("perform_csa_analysis", mode = "function"))

# --- connect + load upstream (collect once) ---------------------------------
# NB: no top-level on.exit() (see all_D07_01.R note — under the targets runner it
# fires on a transient eval frame and kills the connection). Disconnect explicitly
# at the end (shutdown = FALSE; autodeinit does the full teardown).
app_con <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)

df_position <- if (DBI::dbExistsTable(app_con, "df_position")) {
  tbl2(app_con, "df_position") |> dplyr::collect()
} else NULL

# df_all_comment_property tags each theme (property_name) with its 類型 (type);
# build_theme_axis_map groups the themes into the 6 profiling axes (Decision 6).
# Normalize property_name with make_names() so the keys match the df_position
# theme columns (which fn_process_position_table make_names()'d) — mirrors
# marketSegmentationKmeans.R's axis-map build.
comment_property <- if (DBI::dbExistsTable(app_con, "df_all_comment_property")) {
  tbl2(app_con, "df_all_comment_property") |> dplyr::collect()
} else NULL
if (!is.null(comment_property) && "property_name" %in% names(comment_property) &&
    exists("make_names", mode = "function")) {
  comment_property$property_name <- make_names(trimws(as.character(comment_property$property_name)))
}
theme_axis_map <- build_theme_axis_map(comment_property)
if (length(theme_axis_map) == 0L) {
  message("[D07_02] WARNING: theme->axis map empty (df_all_comment_property missing/unmapped) — ",
          "6-axis profile will be empty (Others/Mixed sentinel downstream).")
}

# Own brand for the SKU-based within-segment vendor share (Decision 4; #1234).
own_brand <- tryCatch(
  if (exists("resolve_company_brand", mode = "function")) resolve_company_brand() else NULL,
  error = function(e) NULL)

# --- active platforms (single source: df_position) --------------------------
platforms <- if (!is.null(df_position) && "platform_id" %in% names(df_position)) {
  p <- unique(stats::na.omit(df_position$platform_id)); p[nzchar(p)]
} else character(0)
if (length(platforms) == 0L) {
  message("[D07_02] df_position absent/empty — no segmentation snapshot to materialize (MP163).")
}
message(sprintf("[D07_02] active platforms: %s", paste(platforms, collapse = ", ")))

tot_prod <- 0L; tot_summ <- 0L
for (platform in platforms) {
  res <- run_D07_02(
    platform_id    = platform,
    position_data  = df_position,
    app_data_con   = app_con,
    theme_axis_map = theme_axis_map,
    own_brand      = own_brand
  )
  tot_prod <- tot_prod + res[["df_market_segmentation"]]
  tot_summ <- tot_summ + res[["df_market_segmentation_summary"]]
}

message(sprintf("[D07_02] DONE: df_market_segmentation=%d rows, df_market_segmentation_summary=%d rows across %d platform(s)",
                tot_prod, tot_summ, length(platforms)))

# Explicit cleanup (shutdown = FALSE so the shared duckdb instance stays alive for
# autodeinit's full teardown; mirrors all_D07_01.R). No top-level on.exit (see above).
tryCatch(if (DBI::dbIsValid(app_con)) DBI::dbDisconnect(app_con, shutdown = FALSE),
         error = function(e) NULL)

autodeinit()
