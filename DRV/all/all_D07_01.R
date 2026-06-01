#####
# CONSUMES: df_dna_by_customer, df_position, df_macro_monthly_summary, df_geo_sales_by_country
# PRODUCES: df_ai_insight
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: all_D01_08, amz_D03_11, all_D05_01
#####
#
# all_D07_01.R — D07_01 pipeline DRV: AI-insight precompute (cross-platform).
# Spectra change: mp167-d07-incremental-recompute (Task 4.2; implements design D5+D6).
#
# Targets-wired execution wrapper for run_D07_01() (fn_D07_01_core.R). Iterates the
# d07_prompt_registry (12 eligible prompts), resolves each prompt's product_line
# scope + assembles its heterogeneous extra-data sources, and calls run_D07_01 per
# (platform, prompt). run_D07_01 is INCREMENTAL: it recomputes a cell only when its
# input/config fingerprint changed (or D07_FORCE_RECOMPUTE=1), so this wrapper is
# safe to run on EVERY `make run` — unchanged cells cost only deterministic
# input-building + hashing, no LLM call (design D5).
#
# Data-source parity (CRITICAL): each extra-data source below mirrors the live
# component's loader (same tbl2 filter / same summarise) so the precomputed insight
# is byte-identical to what the live fallback would produce from build_ai_insight_inputs.
#
# Platform-agnostic name (DM_R066): processes ALL of the company's active platforms
# (derived from the data), so it lives in DRV/all/ not a platform dir.
#
# @principle MP167, MP163, MP064, DM_R023, DM_R066, DEV_R053

# --- init (mirror amz_D03_11.R) ---------------------------------------------
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

# 16_derivations is NOT auto-sourced by autoinit (sweep policy #517) — source the
# D07 core + registry explicitly. Fingerprint / create / build live in auto-sourced
# dirs but source defensively in case the sweep skipped them.
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
source(gs("16_derivations", "fn_D07_01_core.R"))
source(gs("16_derivations", "fn_d07_prompt_registry.R"))
if (!exists("fn_ai_insight_fingerprint", mode = "function")) source(gs("08_ai", "fn_ai_insight_fingerprint.R"))
if (!exists("create_df_ai_insight_table", mode = "function")) source(gs("01_db", "fn_create_df_ai_insight_table.R"))
if (!exists("build_ai_insight_inputs", mode = "function")) source(gs("08_ai", "fn_build_ai_insight_inputs.R"))

stopifnot(exists("run_D07_01", mode = "function"),
          exists("d07_prompt_registry"),
          exists("build_ansoff_segmentation_profile", mode = "function"))

api_key <- Sys.getenv("OPENAI_API_KEY")
if (!nzchar(api_key)) {
  message("[D07_01] WARNING: OPENAI_API_KEY not set — every cell needing recompute will fail+skip (MP163). Set it (e.g. load company .env) for real precompute.")
}

# --- connect + load upstream (collect once) ---------------------------------
app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
on.exit(tryCatch(DBI::dbDisconnect(app_data, shutdown = TRUE), error = function(e) NULL), add = TRUE)

if (!DBI::dbExistsTable(app_data, "df_ai_insight")) create_df_ai_insight_table(app_data, or_replace = FALSE)

dna_full <- tbl2(app_data, "df_dna_by_customer") |> dplyr::collect()
df_position <- if (DBI::dbExistsTable(app_data, "df_position")) {
  tbl2(app_data, "df_position") |> dplyr::collect()
} else NULL

# MP163 sentinel product lines are gap markers, not real lines to precompute.
SENTINEL_PL <- c("unclassified", "UNKNOWN")

# --- extra-data source builders (mirror live components for parity) ---------
# Each returns the frame the corresponding build_ai_insight_inputs branch reads.
build_source <- function(name, platform, pl) {
  switch(name,
    # macroTrends.R / growthValidation.R: df_macro_monthly_summary scoped
    "macro" = ,
    "growth" = {
      if (!DBI::dbExistsTable(app_data, "df_macro_monthly_summary")) return(NULL)
      tbl2(app_data, "df_macro_monthly_summary") |>
        dplyr::filter(platform_id == !!platform, product_line_id_filter == !!pl) |>
        dplyr::collect()
    },
    # worldMap.R: df_geo_sales_by_country scoped (no country filter, #348)
    "geo" = {
      if (!DBI::dbExistsTable(app_data, "df_geo_sales_by_country")) return(NULL)
      tbl2(app_data, "df_geo_sales_by_country") |>
        dplyr::filter(platform_id == !!platform, product_line_id_filter == !!pl) |>
        dplyr::collect()
    },
    # macroTrends.R dna_snapshot reactive: 1-row summary of the scoped dna
    "dna_snapshot" = {
      sub <- dna_full[dna_full$platform_id == platform &
                        dna_full$product_line_id_filter == pl, , drop = FALSE]
      if (nrow(sub) == 0) return(NULL)
      tc <- nrow(sub)
      data.frame(
        total_customers = tc,
        avg_clv      = mean(sub$clv, na.rm = TRUE),
        avg_p_alive  = mean(sub$p_alive, na.rm = TRUE),
        e0_pct       = if (tc > 0) sum(sub$nes_status == "E0", na.rm = TRUE) * 100 / tc else NA_real_,
        sleep_pct    = if (tc > 0) sum(sub$nes_status %in% c("S1", "S2", "S3"), na.rm = TRUE) * 100 / tc else NA_real_,
        stringsAsFactors = FALSE
      )
    },
    # positionKFE.R: df_position scoped to the product line
    "position" = {
      if (is.null(df_position)) return(NULL)
      df_position[df_position$product_line_id == pl, , drop = FALSE]
    },
    # marketGrowthTrack.R: serialized k-means segmentation profile (whole df, filters internally)
    "profile" = {
      if (is.null(df_position)) return(NULL)
      tryCatch(build_ansoff_segmentation_profile(df_position, pl), error = function(e) NULL)
    },
    NULL
  )
}

# --- active platforms (single source: the data) -----------------------------
platforms <- unique(stats::na.omit(dna_full$platform_id))
if (!is.null(df_position) && "platform_id" %in% names(df_position)) {
  platforms <- union(platforms, unique(stats::na.omit(df_position$platform_id)))
}
platforms <- platforms[nzchar(platforms)]
message(sprintf("[D07_01] active platforms: %s", paste(platforms, collapse = ", ")))

LOCALES <- c("zh_tw", "en")
tot_written <- 0L; tot_skipped <- 0L; tot_unchanged <- 0L

for (platform in platforms) {
  dna_plat <- dna_full[dna_full$platform_id == platform, , drop = FALSE]

  for (entry in d07_prompt_registry) {
    # Resolve this prompt's product_line scope from its declared pl_source.
    pls <- if (identical(entry$pl_source, "position")) {
      if (is.null(df_position)) character(0) else unique(df_position$product_line_id)
    } else {
      unique(dna_plat$product_line_id_filter)
    }
    pls <- setdiff(pls, SENTINEL_PL)
    pls <- pls[!is.na(pls) & nzchar(pls)]
    if (length(pls) == 0) next

    # Build a prompt-specific extra_data_provider supplying exactly this prompt's
    # extra_sources (NULL for dna-only customer_analysis prompts).
    es <- entry$extra_sources
    edp <- if (length(es) > 0) {
      force(es); force(platform)
      function(pl) stats::setNames(lapply(es, function(s) build_source(s, platform, pl)), es)
    } else NULL

    res <- run_D07_01(
      platform_id   = platform,
      dna_data      = dna_plat,
      app_data_con  = app_data,
      prompt_keys   = entry$prompt_key,
      product_lines = pls,
      locales       = LOCALES,
      api_key       = api_key,
      extra_data_provider = edp
    )
    tot_written   <- tot_written   + res$written
    tot_skipped   <- tot_skipped   + res$skipped
    tot_unchanged <- tot_unchanged + (if (is.null(res$unchanged)) 0L else res$unchanged)
  }
}

message(sprintf("[D07_01] DONE: written=%d unchanged=%d skipped=%d across %d platform(s)",
                tot_written, tot_unchanged, tot_skipped, length(platforms)))

autodeinit()
