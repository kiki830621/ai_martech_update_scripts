# ARCHIVED 2026-04-19 — DM_R054 v2.1 runtime migration (issue #424)
#
# This code used to live at `all_ETL_summary_1ST.R` lines 24-30 and wrote
# `df_platform` into `app_data.duckdb`. Per DM_R054 v2 (2026-04-19),
# `df_platform` is metadata and MUST live EXCLUSIVELY in `meta_data.duckdb`.
# Its canonical producer is now `shared/update_scripts/ETL/all/all_ETL_meta_init_0IM.R`
# (pre-autoinit bootstrap ETL).
#
# This file is RETAINED as a historical reference only. DO NOT source it.
# DO NOT add it back to `_targets` or `Makefile`. If you are reading this
# because an old workflow referenced `df_platform` in `app_data.duckdb`,
# migrate the reader to `meta_data.duckdb` (spec §6 + §7 in DM_R054 v2.1).

# Original content preserved verbatim (was line 24-30 of all_ETL_summary_1ST.R):
#
# app_data <- dbConnectDuckdb(db_path_list$app_data)
#
# df_platform___selected<-
#   df_platform %>%
#   filter(platform_id %in% c("all",app_configs$platform))
#
# df_platform___selected
#
# dbWriteTable(app_data, "df_platform", df_platform___selected, overwrite = TRUE)
