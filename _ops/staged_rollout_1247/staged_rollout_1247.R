#!/usr/bin/env Rscript
#####
# staged_rollout_1247.R  —  #1247 staged-rollout helper (capture + diff)
#
# Captures a snapshot of the market-model output tables that D04_03 writes to
# app_data, and diffs a BEFORE (current main code) vs AFTER (#1247 one-hot encoder)
# snapshot into a markdown report for 林郁翔 sign-off.
#
# This is OPS tooling for the #1247 staged rollout. It does NOT run the pipeline
# and does NOT touch live data — it only reads app_data and writes RDS/markdown.
# Run `make run TARGET=amz_D04_03` yourself between the two captures (see RUNBOOK.md).
#
# Tables read (both written to app_data by DRV/D04/D04_03.R):
#   df_{platform}_market_attribute_coefficients   (predictor-level fit results)
#   df_{platform}_market_attribute_coverage_audit (per-attr fate, DM_R071 / #1210)
#
# Reads use tbl2 + dplyr (DM_R023). Real data only (MP029).
#
# Usage (run from the COMPANY project root, e.g. QEF_DESIGN/):
#   Rscript <path>/staged_rollout_1247.R capture before --out /tmp/rollout_1247
#   # ... apply #1247 code, tar_invalidate, make run TARGET=amz_D04_03 ...
#   Rscript <path>/staged_rollout_1247.R capture after  --out /tmp/rollout_1247
#   Rscript <path>/staged_rollout_1247.R diff \
#       /tmp/rollout_1247/<company>_amz_before.rds \
#       /tmp/rollout_1247/<company>_amz_after.rds  --out /tmp/rollout_1247/diff.md
#
# Flags:
#   --db <path>        app_data.duckdb path (default: data/app_data/app_data.duckdb)
#   --platform <code>  platform code (default: amz)
#   --out <dir|file>   capture: output dir; diff: output .md path
#   --company <name>   override company label (default: basename of getwd())
#####

suppressWarnings(suppressMessages({
  library(DBI); library(dplyr)
}))

# ---- arg parsing (positional subcommand + --flags) -------------------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("usage: staged_rollout_1247.R <capture|diff> [args] [--flags]")
subcmd <- args[1]
rest   <- args[-1]

get_flag <- function(name, default = NULL) {
  i <- match(paste0("--", name), rest)
  if (is.na(i) || i == length(rest)) return(default)
  rest[i + 1]
}
positional <- rest[!grepl("^--", rest)]
# drop flag VALUES from positionals (a value immediately follows a --flag token)
flag_idx <- which(grepl("^--", rest))
val_idx  <- flag_idx + 1
positional <- rest[setdiff(seq_along(rest), c(flag_idx, val_idx))]

platform   <- get_flag("platform", "amz")
db_path    <- get_flag("db", file.path("data", "app_data", "app_data.duckdb"))
company    <- get_flag("company", basename(normalizePath(getwd(), mustWork = FALSE)))

# ---- source canonical DB helpers (DM_R023) --------------------------------
src_first <- function(cands) {
  for (p in cands) if (file.exists(p)) { source(p); return(invisible(TRUE)) }
  stop("could not source any of: ", paste(cands, collapse = ", "))
}
src_first(c("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R",
            "scripts/global_scripts/02_db_utils/fn_dbConnectDuckdb.R"))
src_first(c("scripts/global_scripts/02_db_utils/tbl2/fn_tbl2.R",
            "scripts/global_scripts/02_db_utils/fn_tbl2.R"))

coef_table  <- sprintf("df_%s_market_attribute_coefficients", platform)
audit_table <- sprintf("df_%s_market_attribute_coverage_audit", platform)

read_tbl_safe <- function(con, tbl) {
  if (!DBI::dbExistsTable(con, tbl)) return(NULL)
  tbl2(con, tbl) %>% collect()
}
# keep only columns that actually exist (schema-defensive)
pick <- function(df, cols) df[, intersect(cols, names(df)), drop = FALSE]

# ===========================================================================
# capture
# ===========================================================================
if (subcmd == "capture") {
  label <- positional[1]
  if (is.na(label) || !label %in% c("before", "after"))
    stop("capture needs a label: before | after")
  outdir <- get_flag("out", file.path(tempdir(), "rollout_1247"))
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  if (!file.exists(db_path))
    stop("app_data.duckdb not found at: ", db_path,
         "\nRun `make run TARGET=", platform, "_D04_03` first, or pass --db <path>.")

  con <- dbConnectDuckdb(db_path, read_only = TRUE)
  on.exit(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

  coef  <- read_tbl_safe(con, coef_table)
  audit <- read_tbl_safe(con, audit_table)

  if (is.null(coef))
    cat(sprintf("WARN: %s not present — D04_03 may not have run for %s\n", coef_table, platform))
  if (is.null(audit))
    cat(sprintf("WARN: %s not present (DM_R071 audit)\n", audit_table))

  coef  <- if (!is.null(coef))  pick(coef,  c("product_line_id","predictor","predictor_type",
              "scale","attr_kind","coefficient","predictor_range","irr_display",
              "p_value","estimation_status","display_category")) else NULL
  audit <- if (!is.null(audit)) pick(audit, c("product_line_id","source_table","attr_name",
              "entered_model","coverage_fate","drop_reason")) else NULL

  snap <- list(
    coef = coef, audit = audit,
    meta = list(label = label, company = company, platform = platform,
                db_path = normalizePath(db_path),
                n_coef = if (is.null(coef)) 0L else nrow(coef),
                n_audit = if (is.null(audit)) 0L else nrow(audit),
                captured_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
  )
  out_rds <- file.path(outdir, sprintf("%s_%s_%s.rds", company, platform, label))
  saveRDS(snap, out_rds)
  cat(sprintf("[capture %s] %s: %d coef rows, %d audit rows -> %s\n",
              label, company, snap$meta$n_coef, snap$meta$n_audit, out_rds))
  quit(status = 0)
}

# ===========================================================================
# diff
# ===========================================================================
if (subcmd == "diff") {
  bpath <- positional[1]; apath <- positional[2]
  if (is.na(bpath) || is.na(apath)) stop("diff needs <before.rds> <after.rds>")
  out_md <- get_flag("out", file.path(dirname(bpath), "diff_1247.md"))
  B <- readRDS(bpath); A <- readRDS(apath)

  # significance: estimated (converged) AND p < 0.05
  is_sig <- function(df) {
    if (is.null(df) || !nrow(df)) return(logical(0))
    est <- if ("estimation_status" %in% names(df)) df$estimation_status == "estimated" else TRUE
    p   <- if ("p_value" %in% names(df)) (!is.na(df$p_value) & df$p_value < 0.05) else FALSE
    est & p
  }
  keyof <- function(df) {
    if (is.null(df) || !nrow(df)) return(character(0))
    if ("product_line_id" %in% names(df)) paste(df$product_line_id, df$predictor, sep = "||")
    else df$predictor
  }
  fate_counts <- function(df) {
    if (is.null(df) || !nrow(df) || !"coverage_fate" %in% names(df))
      return(setNames(integer(0), character(0)))
    table(df$coverage_fate)
  }

  Bc <- B$coef; Ac <- A$coef
  Bk <- keyof(Bc); Ak <- keyof(Ac)
  new_keys  <- setdiff(Ak, Bk)              # categoricals now entering
  drop_keys <- setdiff(Bk, Ak)              # should be ~empty (R4: nothing pre-filtered)
  shared    <- intersect(Bk, Ak)

  n_b <- if (is.null(Bc)) 0L else nrow(Bc)
  n_a <- if (is.null(Ac)) 0L else nrow(Ac)
  sig_b <- sum(is_sig(Bc)); sig_a <- sum(is_sig(Ac))

  # coefficient drift on shared predictors (existing numeric attrs should be ~stable)
  drift_rows <- ""
  if (length(shared) && "coefficient" %in% names(Bc) && "coefficient" %in% names(Ac)) {
    bdf <- data.frame(key = Bk, coef_b = Bc$coefficient, stringsAsFactors = FALSE)
    adf <- data.frame(key = Ak, coef_a = Ac$coefficient, stringsAsFactors = FALSE)
    m <- merge(bdf, adf, by = "key")
    m$absd <- abs(m$coef_a - m$coef_b)
    m <- m[order(-m$absd), ]
    big <- m[!is.na(m$absd) & m$absd > 1e-6, ]
    drift_rows <- if (nrow(big) == 0) "_(none — all shared predictors bit-stable)_\n"
      else paste0(sprintf("- `%s`: %.4f -> %.4f (|Δ|=%.4f)\n",
                          head(big$key, 30), head(big$coef_b, 30),
                          head(big$coef_a, 30), head(big$absd, 30)), collapse = "")
  }

  # fate transitions (audit)
  fb <- fate_counts(B$audit); fa <- fate_counts(A$audit)
  all_fates <- sort(union(names(fb), names(fa)))
  fate_tbl <- paste0(sprintf("| %s | %d | %d | %+d |\n", all_fates,
                             ifelse(all_fates %in% names(fb), fb[all_fates], 0L),
                             ifelse(all_fates %in% names(fa), fa[all_fates], 0L),
                             (ifelse(all_fates %in% names(fa), fa[all_fates], 0L) -
                              ifelse(all_fates %in% names(fb), fb[all_fates], 0L))),
                     collapse = "")

  # new entrants detail (the categoricals)
  ne <- ""
  if (length(new_keys) && !is.null(Ac)) {
    sub <- Ac[Ak %in% new_keys, , drop = FALSE]
    sub_sig <- is_sig(sub)
    ord <- order(!sub_sig, sub$p_value)
    sub <- sub[ord, ]; sub_sig <- sub_sig[ord]
    show <- head(seq_len(nrow(sub)), 60)
    ne <- paste0(sprintf("| %s | %s | %s | %s | %s |\n",
            if ("product_line_id" %in% names(sub)) sub$product_line_id[show] else "-",
            sub$predictor[show],
            if ("predictor_type" %in% names(sub)) sub$predictor_type[show] else "-",
            ifelse(is.na(sub$p_value[show]), "NA", sprintf("%.4f", sub$p_value[show])),
            ifelse(sub_sig[show], "**yes**", "no")), collapse = "")
  }

  # significance flips on shared predictors
  flips <- 0L
  if (length(shared)) {
    sb <- setNames(is_sig(Bc), Bk); sa <- setNames(is_sig(Ac), Ak)
    flips <- sum(sb[shared] != sa[shared], na.rm = TRUE)
  }

  md <- c(
    sprintf("# #1247 市場模型 staged-rollout diff — %s (%s)", A$meta$company, platform),
    "",
    sprintf("BEFORE (current main) captured %s · AFTER (#1247 one-hot) captured %s",
            B$meta$captured_at, A$meta$captured_at),
    "",
    "## 一句話",
    sprintf("加入類別屬性 one-hot 後，進入市場模型的 predictor 從 **%d → %d**（新增 **%d** 個，多為類別 dummy），顯著（p<0.05）從 **%d → %d**。被排除而非進場的 predictor 應 ≈ 0（R4 後篩非預篩）：實測 **%d** 個。",
            n_b, n_a, length(new_keys), sig_b, sig_a, length(drop_keys)),
    "",
    "## Coverage fate 轉移（audit / DM_R071）",
    "| coverage_fate | before | after | Δ |",
    "|---|---|---|---|",
    fate_tbl,
    "> 期望：`non_numeric` 大幅下降（類別欄改走 one-hot → estimated/not_estimable/not_significant）；`source_table_not_consumed` 不應增加。",
    "",
    sprintf("## 新進場 predictor（after \\ before，共 %d，列前 60）", length(new_keys)),
    "| product_line | predictor | type | p_value | 顯著 |",
    "|---|---|---|---|---|",
    if (nzchar(ne)) ne else "_(none)_\n",
    "",
    sprintf("## 退場 predictor（before \\ after，共 %d）— R4 要求 ≈ 0", length(drop_keys)),
    if (length(drop_keys)) paste0(paste0("- `", head(drop_keys, 40), "`\n"), collapse = "")
      else "_(none — 沒有任何原本進場的 predictor 被新流程踢掉，符合 R4)_\n",
    "",
    "## 既有 predictor 係數漂移（shared，應 ≈ 0）",
    drift_rows,
    "",
    sprintf("## 顯著性翻轉（shared predictor 由顯著↔不顯著）：%d", flips),
    "",
    "## R4 驗收 checklist（人工確認）",
    "- [ ] 每個連續屬性仍進場（退場清單為空）",
    "- [ ] 每個類別屬性 → ≥1 個 dummy 進場（或誠實 not_estimable，非預先剔除）",
    "- [ ] comment 屬性（除 缺點）有進場（comment_attribute fate 非全 source_table_not_consumed）",
    "- [ ] 缺點 不在 predictor 清單（含 `缺點/場` 之類複合 type）",
    "- [ ] predictor 數 == audit entered-dummy 數（modulo #1248 的 .x/.y 已知低估）",
    "",
    "## 簽核",
    "- [ ] 林郁翔 sign-off（diff 合理、無異常係數）→ 之後才 Supabase upload + deploy",
    ""
  )
  writeLines(md, out_md)
  cat(sprintf("[diff] %d->%d predictors, %d new, %d dropped, %d sig-flips -> %s\n",
              n_b, n_a, length(new_keys), length(drop_keys), flips, out_md))
  quit(status = 0)
}

stop("unknown subcommand: ", subcmd, " (use capture | diff)")
