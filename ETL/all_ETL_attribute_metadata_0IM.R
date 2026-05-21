#####
# all_ETL_attribute_metadata_0IM.R
# ETL Phase 0IM: Import QEF attribute metadata from Google Sheet → raw_data layer
#
# CONSUMES: Google Sheet 1hSIvb3lV0OILGu31FL41OL4_vSf3iVAx80pckz5hVy0 K-column
# PRODUCES: df_qef_attribute_metadata (in raw_data.duckdb)
# DEPENDS_ON_ETL: (none — this is the source ETL for attribute metadata)
#
# Spec: qef-market-segmentation-redesign change, Requirement "GSheet attribute metadata sync"
# Tasks: A.1 (#1-#5)
# Issue: #804 (K-means 4 步驟新分析)
#
# Sync cadence: weekly (Sunday 03:00 via launchd) + manual override via `make sync-attributes`
# Per discussion.md A7 / spec scenario "weekly cron sync"
#####

# ─── INITIALIZE ──────────────────────────────────────────────────────────
source(file.path("shared", "global_scripts", "00_principles", "sc_initialization_update_mode.R"))

# Required packages (per DEV_R057 — must be in canonical core list)
required_pkgs <- c("googlesheets4", "DBI", "duckdb", "dplyr")
missing <- required_pkgs[!sapply(required_pkgs, requireNamespace, quietly = TRUE)]
if (length(missing) > 0) {
  stop("Missing required packages: ", paste(missing, collapse = ", "),
       "\nInstall with: install.packages(c('", paste(missing, collapse = "', '"), "'))")
}

# ─── CONFIG ──────────────────────────────────────────────────────────────
GSHEET_ID <- "1hSIvb3lV0OILGu31FL41OL4_vSf3iVAx80pckz5hVy0"
GSHEET_GID <- "1217363324"
TARGET_TABLE <- "df_qef_attribute_metadata"

# ─── MAIN ────────────────────────────────────────────────────────────────
sync_qef_attribute_metadata <- function(gsheet_id = GSHEET_ID,
                                         gsheet_gid = GSHEET_GID,
                                         verbose = TRUE) {

  if (verbose) message(sprintf("[ETL attribute_metadata] Starting sync from GSheet %s (gid=%s)",
                                gsheet_id, gsheet_gid))

  # ── 1. Fetch from Google Sheet ──
  googlesheets4::gs4_deauth()  # Public sheet — no auth needed; if private, replace with gs4_auth()

  url <- sprintf("https://docs.google.com/spreadsheets/d/%s/edit#gid=%s", gsheet_id, gsheet_gid)
  raw_df <- tryCatch({
    googlesheets4::read_sheet(url, sheet = NULL)  # NULL = first sheet by default; adjust if specific sheet name needed
  }, error = function(e) {
    stop("Failed to read GSheet ", gsheet_id, ": ", conditionMessage(e),
         "\nIf sheet is private, run googlesheets4::gs4_auth() first or set GOOGLE_APPLICATION_CREDENTIALS")
  })

  if (verbose) message(sprintf("[ETL attribute_metadata] Fetched %d rows × %d cols",
                                nrow(raw_df), ncol(raw_df)))

  # ── 2. Parse K column structure (per spec Requirement) ──
  # Expected K-column structure (per discussion.md A5):
  #   variable_id        - unique identifier (e.g. "scene_outdoor_sports")
  #   variable_name_zh   - Traditional Chinese display name
  #   variable_name_en   - English display name
  #   category           - one of: 情感 / 人 / 場 / 文化 / 美學 / 身份認同 / (other)
  #   scale_type         - "5_point" (5 尺度) or "2_point" (2 尺度)
  #   direction          - "positive" / "negative" (Q5 deferred pending GSheet schema confirm)
  #
  # If column names differ from above, this function needs alignment with actual GSheet schema.
  # Implementer notes (#804 A5): GSheet schema confirmation is one of 3 open deps before spectra-apply
  # — actual column mapping may need adjustment after老師 reviews this code.

  required_cols <- c("variable_id", "variable_name_zh", "category", "scale_type")
  missing_cols <- setdiff(required_cols, colnames(raw_df))
  if (length(missing_cols) > 0) {
    warning("GSheet missing expected columns: ", paste(missing_cols, collapse = ", "),
            "\nAvailable columns: ", paste(colnames(raw_df), collapse = ", "),
            "\nProceeding with what's available;downstream D06_01 may fail until schema aligns.")
  }

  # Coerce types defensively
  attr_df <- as.data.frame(raw_df, stringsAsFactors = FALSE)

  # Drop NA rows on variable_id (essential identifier)
  if ("variable_id" %in% colnames(attr_df)) {
    n_before <- nrow(attr_df)
    attr_df <- attr_df[!is.na(attr_df$variable_id) & nchar(trimws(attr_df$variable_id)) > 0, ]
    if (n_before > nrow(attr_df)) {
      message(sprintf("[ETL attribute_metadata] Dropped %d rows with NA/empty variable_id",
                       n_before - nrow(attr_df)))
    }
  }

  # Add ETL metadata
  attr_df$_etl_source <- "gsheet"
  attr_df$_etl_synced_at <- as.character(Sys.time())

  # ── 3. Write to raw_data.duckdb ──
  raw_data_path <- db_path_list$raw_data
  if (is.null(raw_data_path) || !file.exists(raw_data_path)) {
    stop("raw_data.duckdb not at canonical path. Expected: ", raw_data_path,
         "\nRun autoinit() in UPDATE_MODE to initialize.")
  }

  con <- DBI::dbConnect(duckdb::duckdb(), raw_data_path)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  # OVERWRITE strategy (per spec scenario "weekly cron sync" — replace not append)
  DBI::dbWriteTable(con, TARGET_TABLE, attr_df, overwrite = TRUE)

  if (verbose) message(sprintf("[ETL attribute_metadata] Wrote %d rows to %s in %s",
                                nrow(attr_df), TARGET_TABLE, raw_data_path))

  invisible(attr_df)
}

# ─── EXECUTE (when run via Rscript) ──────────────────────────────────────
if (!interactive() && sys.nframe() == 0) {
  tryCatch({
    sync_qef_attribute_metadata()
    message("[ETL attribute_metadata] ✓ Sync complete")
  }, error = function(e) {
    message("[ETL attribute_metadata] ✗ Sync FAILED: ", conditionMessage(e))
    quit(status = 1)
  })
}

# ─── DEINITIALIZE ────────────────────────────────────────────────────────
# autodeinit() called by sc_initialization_update_mode at script end
