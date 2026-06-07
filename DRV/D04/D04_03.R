#!/usr/bin/env Rscript
#####
# D04_03
# DERIVATION: Platform-Agnostic Market Attribute Coefficient Model
# VERSION: 1.0 (#1200, design D8-D13)
# GROUP: D04
# SEQUENCE: 03
# PURPOSE: Per-attribute market (own∪competitor) Poisson IRR coefficients from
#          df_position, so sparse-own product lines become estimable (#1200).
#          Platform identity resolved at runtime per DM_R066.
# CONSUMES: df_position (app_data), df_{platform}_sales_complete_time_series_{pl} (app_data),
#           df_all_comment_property (raw_data — property scale config)
# PRODUCES: df_{platform}_market_attribute_coefficients (app_data)
# DEPENDS_ON_DRV: D03_11 (df_position producer)
# PRINCIPLE: DM_R066, MP064, MP163, MP167, DEV_R050, DEV_R058; design D8-D13 (#1200)
#####

# D04_03 — Market Attribute Coefficient Model
#
# Design (openspec/changes/precision-marketing-market-model, locked via /spectra-discuss):
#  - D8  source = df_position own∪competitor cross-section.
#  - D9  this new table + repointed UI; D04_02 (time) untouched.
#  - D10 encoding = use df_position values directly (2尺度 proportion / 5尺度 rating);
#        scale (df_all_comment_property) drives the interpretation label only; only
#        PL-mapped attributes are modeled.
#  - D11 quasipoisson + min_obs=8 withhold; IRR displayed via exp(coef×range) (#1158).
#  - D13 honest-sparse accepted; the UI presents ONLY significant (p<0.05) drivers.

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

tbl2_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R")
)
tbl2_path <- tbl2_candidates[file.exists(tbl2_candidates)][1]
if (is.na(tbl2_path)) stop("fn_tbl2.R not found in expected paths")
source(tbl2_path)
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

library(DBI); library(duckdb); library(dplyr)

# Market-model helpers (#1200)
source("scripts/global_scripts/16_derivations/fn_assemble_market_model_data.R")
source("scripts/global_scripts/04_utils/fn_fit_univariate_poisson.R")
source("scripts/global_scripts/16_derivations/fn_fit_market_attribute_coefficients.R")
source("scripts/global_scripts/16_derivations/fn_build_market_attribute_table.R")
source("scripts/global_scripts/04_utils/fn_resolve_drv_platform.R")

platform <- resolve_drv_platform()

if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

error_occurred <- FALSE; test_passed <- FALSE; rows_processed <- 0
start_time <- Sys.time()
DRV_VERSION <- "v1.0_market_attr_DMR066"
output_table_name <- sprintf("df_%s_market_attribute_coefficients", platform)

cat("\n════════════════════════════════════════════════════════════════════\n")
cat(sprintf("%s Market Attribute Coefficient DRV (D04_03, #1200, DM_R066)\n", toupper(platform)))
cat("════════════════════════════════════════════════════════════════════\n")
cat("Platform (runtime-resolved):", platform, "\n")
cat("Output:", output_table_name, "(app_data)\n\n")

con_app <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
con_raw <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)

PRODUCT_LINES <- get_active_product_lines()$product_line_id
if (length(PRODUCT_LINES) == 0) {
  stop("No active product lines; verify meta_data df_product_line and app_config.yaml.")
}
cat("Product lines:", paste(toupper(PRODUCT_LINES), collapse = ", "), "\n\n")

# D10: property → scale config (source of truth = df_all_comment_property in raw_data).
# Used for (a) which attributes are mapped to each PL, (b) the interpretation label.
if (!dbExistsTable(con_raw, "df_all_comment_property")) {
  stop("df_all_comment_property not found in raw_data — required for scale-driven encoding (D10).")
}
prop_cfg <- tbl2(con_raw, "df_all_comment_property") %>%
  select(product_line_id, property_name, scale) %>% collect()
prop_cfg$scale[is.na(prop_cfg$scale) | prop_cfg$scale == ""] <- "5尺度"  # code fallback (D10)
prop_cfg <- prop_cfg[!duplicated(paste(prop_cfg$product_line_id, prop_cfg$property_name)), ]

# Output schema (superset of fn_build_market_attribute_table + scale label + run meta).
empty_market_schema <- tibble(
  product_line_id = character(), platform = character(), predictor = character(),
  predictor_type = character(), scale = character(), attr_kind = character(),
  estimation_status = character(), drop_reason = character(),
  coefficient = numeric(), incidence_rate_ratio = numeric(), irr_display = numeric(),
  std_error = numeric(), z_value = numeric(), p_value = numeric(),
  conf_low = numeric(), conf_high = numeric(),
  irr_conf_low = numeric(), irr_conf_high = numeric(),
  predictor_min = numeric(), predictor_max = numeric(), predictor_range = numeric(),
  predictor_is_binary = logical(), track_multiplier = numeric(),
  convergence = character(),
  sample_size = integer(), n_own = integer(), n_competitor = integer(),
  competitor_included = logical(),
  # display columns — schema-compatible with poissonFeatureAnalysis/CommentAnalysis
  # so the UI repoint (D9) is a table-name swap. Chinese predictor names are already
  # human-readable, so display_name = predictor; category by attr_kind.
  display_name = character(), display_name_en = character(),
  display_name_zh = character(), display_category = character(),
  analysis_date = as.Date(character()), analysis_version = character(),
  computed_at = as.POSIXct(character()), data_version = as.Date(character())
)

market_sentinel <- function(pl, reason) {
  # MP163: skipped PL still appears in the _all table with a visible sentinel.
  s <- empty_market_schema[1, ]
  s[1, ] <- NA
  s$product_line_id <- pl; s$platform <- platform
  s$predictor <- "__no_market_data__"; s$predictor_type <- "structural"
  s$scale <- NA_character_; s$attr_kind <- NA_character_
  s$estimation_status <- "not_estimable"; s$drop_reason <- reason
  s$predictor_is_binary <- NA; s$competitor_included <- FALSE
  s$n_own <- NA_integer_; s$n_competitor <- NA_integer_; s$sample_size <- NA_integer_
  s$analysis_date <- Sys.Date(); s$analysis_version <- DRV_VERSION
  s$computed_at <- start_time
  s
}

ts_key <- switch(platform, "amz" = "amz_asin", "cbz" = "cbz_item_id",
                 "eby" = "eby_item_id", "shp" = "shp_item_id", NULL)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

all_results <- list()
tryCatch({

for (pl in PRODUCT_LINES) {
  cat(sprintf("── %s ──\n", toupper(pl)))

  if (!dbExistsTable(con_app, "df_position")) {
    stop("df_position not found in app_data — D03_11 must run before D04_03.")
  }
  # ---- #1200 rework (鄭澈 2026-06-07): COMBINE attribute sources ----
  # df_product_profile_{pl} (raw_data) = full product set + GoogleSheet image-mining
  # attrs (圖片探勘); df_position (app_data) = customer-review-topic attrs. Joined by
  # ASIN → ~194 attrs (fixes "only 66"). Univariate model retained (#1200); only the
  # attribute breadth changes. KitchenMAMA multilevel method is for OTHER issues.
  prof_tbl <- sprintf("df_product_profile_%s", pl)
  if (!dbExistsTable(con_raw, prof_tbl)) {
    cat("  ⚠️  no df_product_profile; sentinel\n\n")
    all_results[[pl]] <- market_sentinel(pl, "no_product_profile"); next
  }
  prof <- tbl2(con_raw, prof_tbl) %>% collect()
  if (!"ASIN" %in% names(prof)) {
    all_results[[pl]] <- market_sentinel(pl, "profile_no_ASIN"); next
  }
  names(prof)[names(prof) == "ASIN"] <- "product_id"
  prof <- prof[!is.na(prof$product_id) & nzchar(as.character(prof$product_id)), , drop = FALSE]

  # review-topic attrs from df_position (PL-mapped), keyed by ASIN, LEFT JOIN
  pos <- tbl2(con_app, "df_position") %>% filter(product_line_id == !!pl) %>% collect()
  mapped_rev <- if ("product_id" %in% names(pos))
    intersect(prop_cfg$property_name[prop_cfg$product_line_id == pl], names(pos)) else character(0)
  combined <- prof
  if (length(mapped_rev) > 0L) {
    pos_rev <- pos[!duplicated(pos$product_id), c("product_id", mapped_rev), drop = FALSE]
    combined <- merge(prof, pos_rev, by = "product_id", all.x = TRUE)
  }

  # attribute columns: profile attrs ∪ review attrs, numeric-coercible, excl id/meta
  NON_ATTR <- c("product_id", "SKU", "sku", "brand", "品牌", "rating", "sales",
                "product_line_id", "is_competitor", "amz_asin")
  cand <- setdiff(names(combined), NON_ATTR)
  cand <- cand[!grepl("^etl_|^\\.", cand)]
  is_num <- vapply(cand, function(c) any(!is.na(suppressWarnings(as.numeric(combined[[c]])))), logical(1))
  attr_cols <- cand[is_num]
  for (a in attr_cols) combined[[a]] <- suppressWarnings(as.numeric(combined[[a]]))
  if (length(attr_cols) == 0L) {
    all_results[[pl]] <- market_sentinel(pl, "no_numeric_attrs"); next
  }

  # competitor sales: prefer df_position.sales (ASIN-matched to the positioning set,
  # the proven key), fall back to raw competitor_sales agg for products only in the
  # profile. Own rows stay NA here → backfilled from own time series below.
  combined$sales <- NA_real_
  if ("sales" %in% names(pos) && "product_id" %in% names(pos)) {
    pos_sales <- setNames(suppressWarnings(as.numeric(pos$sales)),
                          as.character(pos$product_id))
    combined$sales <- unname(pos_sales[as.character(combined$product_id)])
  }
  if (dbExistsTable(con_raw, "df_amz_competitor_sales")) {
    cs <- tbl2(con_raw, "df_amz_competitor_sales") %>%
      filter(product_line_id == !!pl) %>%
      group_by(amz_asin) %>% summarise(s = sum(sales, na.rm = TRUE), .groups = "drop") %>% collect()
    csv <- setNames(as.numeric(cs$s), as.character(cs$amz_asin))
    fill <- is.na(combined$sales)
    combined$sales[fill] <- unname(csv[as.character(combined$product_id[fill])])
  }

  # ownership / own-sales backfill: own ASIN = product_id in own time series
  own_lookup <- setNames(numeric(0), character(0))
  ts_tbl <- sprintf("df_%s_sales_complete_time_series_%s", platform, pl)
  if (!is.null(ts_key) && dbExistsTable(con_app, ts_tbl)) {
    agg <- tbl2(con_app, ts_tbl) %>%
      group_by(.data[[ts_key]]) %>% summarise(s = sum(sales, na.rm = TRUE), .groups = "drop") %>% collect()
    own_lookup <- setNames(as.numeric(agg$s), as.character(agg[[ts_key]]))
  }

  out <- tryCatch(
    fn_build_market_attribute_table(combined, own_lookup, attr_cols,
                                    platform = platform, product_line = pl,
                                    id_col = "product_id"),
    error = function(e) { cat("  ❌ build failed:", conditionMessage(e), "\n"); NULL })
  if (is.null(out) || nrow(out) == 0L) {
    all_results[[pl]] <- market_sentinel(pl, "build_failed_or_empty"); next
  }

  # D10/D11: attach scale/kind label + #1158 displayed IRR + run metadata.
  # Three attr origins → attr_kind + predictor_type (drives the InsightForge split):
  #   review attr scale=2尺度 (人/場/情感 persona mentions) → mention_proportion → comment_attribute (poissonComment)
  #   review attr scale=5尺度 (屬性/缺點 quality ratings)   → rating            → product_attribute (poissonFeature)
  #   product-profile attr (image-mining 圖片探勘 + listing features, not in prop_cfg) → product_feature → product_attribute
  scale_map <- setNames(prop_cfg$scale[prop_cfg$product_line_id == pl],
                        prop_cfg$property_name[prop_cfg$product_line_id == pl])
  is_review <- out$predictor %in% names(scale_map)
  out$scale <- unname(scale_map[out$predictor])               # NA for product-profile attrs
  out$attr_kind <- ifelse(!is_review, "product_feature",
                   ifelse(out$scale == "2尺度", "mention_proportion", "rating"))
  out$predictor_type <- ifelse(out$attr_kind == "mention_proportion",
                               "comment_attribute", "product_attribute")
  out$irr_display <- exp(out$coefficient * out$predictor_range)  # #1158
  # display columns (UI schema compatibility). Chinese predictor = display name.
  out$display_name <- out$predictor
  out$display_name_en <- out$predictor
  out$display_name_zh <- out$predictor
  out$display_category <- ifelse(out$attr_kind == "mention_proportion", "市場提及屬性",
                          ifelse(out$attr_kind == "product_feature", "產品圖文屬性", "市場評分屬性"))
  out$analysis_date <- Sys.Date(); out$analysis_version <- DRV_VERSION
  out$computed_at <- start_time; out$data_version <- Sys.Date()

  out <- out[, names(empty_market_schema), drop = FALSE]
  n_sig <- sum(out$estimation_status == "estimated" & !is.na(out$p_value) & out$p_value < 0.05)
  cat(sprintf("  ✓ %d attrs | est=%d | significant(p<0.05)=%d | own=%s comp=%s\n\n",
              nrow(out), sum(out$estimation_status == "estimated"), n_sig,
              out$n_own[1], out$n_competitor[1]))
  all_results[[pl]] <- out
}

cat("════════════════════════════════════════════════════════════════════\n")
if (length(all_results) > 0) {
  merged <- bind_rows(all_results)
  dbWriteTable(con_app, output_table_name, merged, overwrite = TRUE)
  rows_processed <- nrow(merged)
  cat(sprintf("✅ Wrote %s: %d rows across %d PL(s)\n", output_table_name,
              rows_processed, length(all_results)))
} else {
  dbWriteTable(con_app, output_table_name, empty_market_schema, overwrite = TRUE)
  cat(sprintf("⚠️  No results; wrote empty schema to %s\n", output_table_name))
}

}, error = function(e) { message("ERROR in MAIN: ", e$message); error_occurred <<- TRUE })

# ==============================================================================
# PART 3: TEST
# ==============================================================================
if (!error_occurred) {
  if (dbExistsTable(con_app, output_table_name)) {
    n <- tbl2(con_app, output_table_name) %>% summarise(n = dplyr::n()) %>% collect() %>% pull(n)
    cat(sprintf("\n[TEST] %s exists with %d rows\n", output_table_name, n))
    test_passed <- n > 0
  } else {
    cat(sprintf("\n[TEST] ❌ %s not found\n", output_table_name)); test_passed <- FALSE
  }
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================
cat("\n════════════════════════════════════════════════════════════════════\n")
cat(sprintf("D04_03 SUMMARY — platform=%s status=%s rows=%d time=%.1fs\n",
            platform, ifelse(test_passed, "SUCCESS", "FAILED"), rows_processed,
            as.numeric(difftime(Sys.time(), start_time, units = "secs"))))
cat("════════════════════════════════════════════════════════════════════\n")

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================
if (exists("con_raw") && inherits(con_raw, "DBIConnection")) dbDisconnect(con_raw, shutdown = TRUE)
if (exists("con_app") && inherits(con_app, "DBIConnection")) dbDisconnect(con_app, shutdown = TRUE)
autodeinit()
# End of file
