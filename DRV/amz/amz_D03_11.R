#####
# CONSUMES: df_comment_property_ratingonly_*, df_amz_competitor_sales, df_all_comment_property
# PRODUCES: position tables in app_data, df_amz_comment_property_coverage_audit
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: amz_D03_10
#####


#' @title amz_D03_11
#' @description Derivation task
#' @business_rules See script comments for business logic.
#' @platform amz
#' @author MAMBA Development Team
#' @date 2025-12-30
#' @logical_step_id D03_11
#' @logical_step_status implemented

# amz_D03_11.R - Create Position Table for Amazon
# D03_11: Combines all processed data into the final position table
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - R49: Apply Over Loops
# - MP81: Explicit Parameter Specification

# Initialize environment
needgoogledrive <- TRUE
autoinit()

# Connect to databases with appropriate access
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = TRUE)
app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)

# Log beginning of process
message("Starting D03_11 (Create Position Table) for Amazon product lines")

# Phase 1: Process each product line data
message("\n== PHASE 1: Processing individual product lines ==")

success_count <- 0
failed_lines <- character()
for (product_line_id_i in vec_product_line_id_noall) {
  success <- process_position_table(
    product_line_id = product_line_id_i,
    raw_data = raw_data,
    processed_data = processed_data,
    app_data = app_data,
    paste_ = paste_
  )
  
  if (success) {
    success_count <- success_count + 1
  } else {
    failed_lines <- c(failed_lines, product_line_id_i)
  }
}

if (length(failed_lines) > 0) {
  stop(
    "D03_05 failed: per-product-line processing failed for: ",
    paste(failed_lines, collapse = ", ")
  )
}

if (success_count == 0L) {
  stop("D03_05 failed: no product line was successfully processed.")
}

# Phase 2: Merge all product line data
merge_success <- merge_position_tables(
  product_line_ids = vec_product_line_id_noall,
  app_data = app_data,
  paste_ = paste_
)

# Phase 3: Finalize the position table
if (merge_success) {
  finalize_position_args <- list(app_data = app_data)
  finalize_fn_formals <- names(formals(finalize_position_table))
  if ("coalesce_suffix_cols" %in% finalize_fn_formals) {
    finalize_position_args$coalesce_suffix_cols <- coalesce_suffix_cols
  }
  finalize_success <- do.call(finalize_position_table, finalize_position_args)
  
  # Verify the final position table
  verify_position_table(app_data)
  
  # Clean up temporary tables
  cleanup_temp_position_tables(app_data)
}

if (!isTRUE(merge_success)) {
  stop("D03_05 failed: merge_position_tables() returned FALSE.")
}

if (!isTRUE(exists("finalize_success") && finalize_success)) {
  stop("D03_05 failed: finalize_position_table() returned FALSE.")
}

if (!DBI::dbExistsTable(app_data, "df_position")) {
  stop("D03_05 failed: final table app_data.df_position was not created.")
}

# Phase 4 (#1277, DM_R071/MP163): comment-property coverage audit.
# 後算重建每主題 fate（displayed / display_type_filtered / no_review_mention /
# insufficient_coverage / dropped_unexplained / type_excluded），寫
# df_amz_comment_property_coverage_audit 供 positionTable「未顯示主題」列表。
# Verify round-1 wiring（6-AI ensemble）：傳真實 score_types + display_types
# (#1208) + 實際 df_position 欄（observation 非 prediction）+ production
# name normalizer（make_names + 品牌→品牌價值）。
# Graceful：任一 PL 輸入缺漏 warning 跳過（不偽造 fate）；零 rows 時寫空 schema
# 表（不留 stale 舊資料）。
message("\n== PHASE 4: Comment-property coverage audit (#1277) ==")
for (dep in c(
  file.path("scripts", "global_scripts", "04_utils", "fn_comment_property_coverage_audit.R"),
  file.path("scripts", "global_scripts", "04_utils", "fn_get_comment_property_score_types.R"),
  file.path("scripts", "global_scripts", "04_utils", "fn_get_position_display_types.R"),
  file.path("scripts", "global_scripts", "05_etl_utils", "common", "string", "fn_make_names.R"))) {
  if (file.exists(dep)) try(source(dep), silent = TRUE)
}
audit_empty_schema <- data.frame(
  product_line_id = character(0), property_name = character(0),
  type = character(0), scale = numeric(0), fate = character(0),
  coverage_pct = numeric(0), n_asins = integer(0), stringsAsFactors = FALSE)
if (exists("fn_build_comment_property_coverage_audit", mode = "function")) {
  # Production column-name normalizer (mirrors fn_process_comment_property_
  # ratings_by_asin Step 2): make_names + 品牌→品牌價值 rename.
  audit_normalizer <- function(x) {
    y <- if (exists("make_names", mode = "function")) make_names(x) else x
    y[y == "品牌"] <- "品牌價值"
    y
  }
  audit_score_types <- if (exists("fn_get_comment_property_score_types", mode = "function")) {
    fn_get_comment_property_score_types()
  } else NULL  # no gate is safer than a wrong gate
  audit_display_types <- if (exists("fn_get_position_display_types", mode = "function")) {
    fn_get_position_display_types()
  } else c("屬性", "場", "場景")  # #1208 documented fallback
  read_tbl <- function(con, tbl_name) {
    if (exists("tbl2", mode = "function")) tbl2(con, tbl_name) else dplyr::tbl(con, tbl_name)
  }
  audit_all <- list()
  for (pl in vec_product_line_id_noall) {
    themes_df <- tryCatch(
      read_tbl(raw_data, "df_all_comment_property") %>%
        dplyr::filter(product_line_id == !!pl) %>%
        dplyr::collect() %>%
        dplyr::select(dplyr::any_of(c("property_name", "type", "scale"))),
      error = function(e) NULL)
    if (is.null(themes_df) || nrow(themes_df) == 0) {
      warning("#1277 coverage audit: no themes for PL '", pl, "' — skipped")
      next
    }
    pivot_table <- paste0("df_comment_property_ratingonly_by_asin_", pl)
    if (!DBI::dbExistsTable(processed_data, pivot_table)) {
      # Engineering absence (rating pipeline never ran for this PL) — skip
      # rather than fabricate no_review_mention for every theme (MP163: the
      # gap must be honest, not mislabeled as a business fact).
      warning("#1277 coverage audit: pivot table missing for PL '", pl, "' — skipped")
      next
    }
    pivot_df <- tryCatch(
      read_tbl(processed_data, pivot_table) %>% dplyr::collect(),
      error = function(e) NULL)
    if (is.null(pivot_df)) {
      warning("#1277 coverage audit: pivot read failed for PL '", pl, "' — skipped")
      next
    }
    # Actual df_position columns carrying data for this PL (observation input)
    pos_cols <- tryCatch({
      pos_pl <- read_tbl(app_data, "df_position") %>%
        dplyr::filter(product_line_id == !!pl) %>% dplyr::collect()
      if (nrow(pos_pl) == 0) character(0)
      else names(pos_pl)[vapply(pos_pl, function(x) any(!is.na(x)), logical(1))]
    }, error = function(e) NULL)
    audit_all[[pl]] <- fn_build_comment_property_coverage_audit(
      themes_df = themes_df, pivot_df = pivot_df,
      product_line_id = pl, score_types = audit_score_types,
      coverage_threshold = 0.3, position_cols = pos_cols,
      display_types = audit_display_types, name_normalizer = audit_normalizer)
  }
  audit_df <- if (length(audit_all) > 0) do.call(rbind, audit_all) else audit_empty_schema
  ok_write <- tryCatch({
    DBI::dbWriteTable(app_data, "df_amz_comment_property_coverage_audit",
                      audit_df, overwrite = TRUE)
    TRUE
  }, error = function(e) {
    warning("#1277 coverage audit: write failed — ", conditionMessage(e)); FALSE
  })
  if (ok_write) {
    message("- Coverage audit: ", nrow(audit_df), " theme rows across ",
            length(audit_all), " product lines (",
            sum(audit_df$fate != "displayed"), " not displayed)")
  }
} else {
  warning("#1277 coverage audit skipped: fn_build_comment_property_coverage_audit not available")
}

# Report overall results
message("\n== Overall Results ==")
message("- Product lines processed: ", success_count, " of ", length(vec_product_line_id_noall))
message("- Merge operation: ", if (merge_success) "Successful" else "Failed")
message("- Finalization: ", if (exists("finalize_success") && finalize_success) "Successful" else "Not completed")

# Clean up and disconnect
autodeinit()

# Log completion
message("\nAmazon position table creation completed successfully for D03_11 step")
