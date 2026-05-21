#####
# all_D06_01.R - D06 Market Segmentation K-means orchestrator
#
# CONSUMES: df_qef_attribute_metadata, df_qef_customer_attributes
# PRODUCES: df_qef_market_segmentation_clusters, df_qef_market_segmentation_profiles
# DEPENDS_ON_ETL: all_ETL_attribute_metadata_0IM
# DEPENDS_ON_DRV: (none)
#
# Spec: qef-market-segmentation-redesign change, Phase A
# Tasks: A.2 (#6-#11) + A.3 (#12-#15)
# Issue: #804
#####

# ─── INITIALIZE ──────────────────────────────────────────────────────────
source(file.path("shared", "global_scripts", "00_principles", "sc_initialization_update_mode.R"))

# Load core function
source(file.path("shared", "global_scripts", "16_derivations",
                 "fn_D06_01_market_segmentation_kmeans_core.R"))

# ─── MAIN ────────────────────────────────────────────────────────────────
message("[D06_01] Starting market segmentation for QEF_DESIGN")

# Connect to processed_data layer
con <- DBI::dbConnect(duckdb::duckdb(), db_path_list$processed_data)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

# Step 1: K-means clustering
result <- D06_01_market_segmentation_kmeans_core(con, verbose = TRUE)

# Write clusters table
message(sprintf("[D06_01] Writing df_qef_market_segmentation_clusters (%d rows)",
                 nrow(result$clusters)))
DBI::dbWriteTable(con, "df_qef_market_segmentation_clusters",
                  result$clusters, overwrite = TRUE)

# Step 2: Chi-square profile derivation
message("[D06_01] Computing chi-square profile tests")
profiles <- compute_profile_chisq(con, result$clusters, result$metadata)

message(sprintf("[D06_01] Writing df_qef_market_segmentation_profiles (%d rows)",
                 nrow(profiles)))
DBI::dbWriteTable(con, "df_qef_market_segmentation_profiles",
                  profiles, overwrite = TRUE)

# Step 3: Summary
n_significant <- sum(profiles$is_significant, na.rm = TRUE)
message(sprintf("[D06_01] ✓ %d significant cluster × profile_variable pairs (p < 0.05)",
                 n_significant))
message(sprintf("[D06_01] chosen_k = %s", result$chosen_k_log))

# ─── DEINITIALIZE ────────────────────────────────────────────────────────
autodeinit()
