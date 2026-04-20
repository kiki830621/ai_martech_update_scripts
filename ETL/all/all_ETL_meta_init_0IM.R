#####
# all_ETL_meta_init_0IM.R
# ETL: Metadata Initialization (canonical producer for meta_data.duckdb)
#
# CONSUMES:
#   - app_config.yaml (platforms: section for df_platform)
#   - data/app_data/parameters/scd_type1/df_product_line.csv (seed for df_product_line)
#   - shared/global_scripts/30_global_data/parameters/scd_type1/db_paths.yaml (for meta_data path)
# PRODUCES:
#   - meta_data.df_platform        (one row per active platform)
#   - meta_data.df_product_line    (from CSV seed)
# DEPENDS_ON_ETL: (none — meta_init is foundational / bootstrap)
# DEPENDS_ON_DRV: (none)
#
# Principle: DM_R054 v2 (metadata MUST live in meta_data.duckdb, NOT app_data)
# Principle: DM_R061 (meta_data is non-rebuildable; 7th layer, parallel to raw_data)
# Principle: DM_R062 (canonical path from db_paths.yaml, no fallback)
# Principle: MP029 (real data from config / CSV, no fake placeholder)
# Principle: MP140 (PRODUCES annotation above)
#
# Bootstrap note:
#   This is a "pre-autoinit" ETL. It DOES NOT call autoinit() because
#   autoinit()'s fail-fast precheck (autoinit-failfast-policy spec) would
#   stop on missing meta_data.duckdb — creating a chicken-and-egg bootstrap
#   problem. Instead, this script does minimal init: parse db_paths.yaml +
#   app_config.yaml directly, resolve paths, write to meta_data.duckdb.
#
# Idempotency: Running twice with same inputs produces identical meta_data.duckdb
# Preservation: ___COMPANY-suffix diagnostic tables (e.g. df_eby_review___transformed___MAMBA,
#               triple underscore per DM_R037 v3.0) are NOT touched
# Motivation: #422 + spectra change `metadata-to-meta-data-duckdb`
#####

# ============================================================================
# PART 1: INITIALIZE (minimal, pre-autoinit)
# ============================================================================

# Resolve project root: prefer env var (Makefile sets MAMBA_PROJECT_ROOT),
# fall back to getwd() for manual invocation. No autoinit().
project_root <- Sys.getenv("MAMBA_PROJECT_ROOT", "")
if (!nzchar(project_root) || !dir.exists(project_root)) {
  project_root <- getwd()
}
if (!file.exists(file.path(project_root, "app_config.yaml"))) {
  stop(
    "Could not resolve project root. Looked at: ", project_root, "\n",
    "Expected app_config.yaml at that path. ",
    "Set MAMBA_PROJECT_ROOT or run from project root.",
    call. = FALSE
  )
}

db_paths_yaml <- file.path(
  project_root, "scripts", "global_scripts",
  "30_global_data", "parameters", "scd_type1", "db_paths.yaml"
)
if (!file.exists(db_paths_yaml)) {
  stop(
    "db_paths.yaml not found: ", db_paths_yaml, "\n",
    "Ensure shared/global_scripts symlink is correctly set up.",
    call. = FALSE
  )
}

if (!requireNamespace("yaml", quietly = TRUE)) {
  stop("Package 'yaml' required. Install with install.packages('yaml').",
       call. = FALSE)
}

# Local helper (autoinit-provided %||% is not available in pre-autoinit ETL)
`%||%` <- function(a, b) if (is.null(a)) b else a

error_occurred <- FALSE
start_time     <- Sys.time()

# ============================================================================
# PART 2: MAIN
# ============================================================================

tryCatch({
  message("══════════════════════════════════════════════════════════════════")
  message("ETL: Metadata Initialization (all_ETL_meta_init_0IM)")
  message("Producer for meta_data.duckdb (DM_R054 v2)")
  message(sprintf("Project root: %s", project_root))
  message("══════════════════════════════════════════════════════════════════")

  # -----------------------------------------------------------------------
  # 2.1 Resolve canonical meta_data path (DM_R062)
  # -----------------------------------------------------------------------
  db_config <- yaml::read_yaml(db_paths_yaml)
  rel_meta  <- db_config$databases$meta_data %||% NULL
  if (is.null(rel_meta)) {
    stop(
      "db_paths.yaml does not declare `databases.meta_data`. ",
      "Cannot produce without canonical path.",
      call. = FALSE
    )
  }
  meta_path <- file.path(project_root, rel_meta)
  message(sprintf("[meta_init] canonical meta_data path: %s", meta_path))

  # Ensure parent dir exists (this ETL legitimately creates meta_data.duckdb)
  dir.create(dirname(meta_path), showWarnings = FALSE, recursive = TRUE)

  # -----------------------------------------------------------------------
  # 2.2 Build df_platform from app_config.yaml `platforms:` section
  #     (design.md Decision 2: df_platform source = app_config.yaml reconstruct)
  # -----------------------------------------------------------------------
  app_config_path <- file.path(project_root, "app_config.yaml")
  app_config      <- yaml::read_yaml(app_config_path)

  # Support two app_config schemas:
  #   New (MAMBA/QEF_DESIGN/D_RACING, DM_R037 v3.0): `platforms:` dict
  #     with {platform_id: {status: active, ...}, ...}
  #   Legacy (kitchenMAMA/WISER):                    `platform:` list
  #     with [platform_id, ...] (all treated as active)
  platforms_cfg <- app_config$platforms
  platform_list <- app_config$platform

  active_platform_ids <- character()

  if (!is.null(platforms_cfg) && length(platforms_cfg) > 0) {
    # New schema: dict with status field
    for (pid in names(platforms_cfg)) {
      status <- platforms_cfg[[pid]]$status
      if (!is.null(status) && identical(status, "active")) {
        active_platform_ids <- c(active_platform_ids, pid)
      }
    }
  } else if (!is.null(platform_list) && length(platform_list) > 0) {
    # Legacy schema: list (all entries treated as active)
    active_platform_ids <- as.character(unlist(platform_list))
    message(sprintf(
      "[meta_init] legacy `platform:` schema detected (%d entries); ",
      length(active_platform_ids)
    ),
    "treating all as active. Consider migrating to `platforms:` dict with status fields.")
  } else {
    stop(
      "app_config.yaml has neither `platforms:` dict nor `platform:` list. ",
      "df_platform cannot be reconstructed.",
      call. = FALSE
    )
  }

  if (length(active_platform_ids) == 0) {
    stop(
      "No active platform found in app_config.yaml. ",
      "df_platform would be empty — refusing to write.",
      call. = FALSE
    )
  }

  df_platform <- data.frame(
    platform_id = active_platform_ids,
    status      = "active",
    stringsAsFactors = FALSE
  )
  message(sprintf(
    "[meta_init] df_platform constructed: %d active platform(s) — %s",
    nrow(df_platform), paste(active_platform_ids, collapse = ", ")
  ))

  # -----------------------------------------------------------------------
  # 2.3 Load df_product_line from CSV seed
  #     (design.md Decision 3: DuckDB canonical, CSV as seed only)
  # -----------------------------------------------------------------------
  product_line_csv <- file.path(
    project_root, "data", "app_data",
    "parameters", "scd_type1", "df_product_line.csv"
  )
  if (!file.exists(product_line_csv)) {
    stop(
      "df_product_line.csv seed not found: ", product_line_csv, "\n",
      "Action: place a valid df_product_line.csv at that path. ",
      "The ETL refuses to create an empty or partial df_product_line table ",
      "(see metadata-storage-policy spec: Producer ETL runs without seed).",
      call. = FALSE
    )
  }
  df_product_line <- utils::read.csv(
    product_line_csv, stringsAsFactors = FALSE, check.names = FALSE
  )
  # #427 F22: with check.names=FALSE, column names can carry trailing/leading
  # whitespace (e.g., " product_line_id" or "product_line_id " from CSV edited
  # in Excel). trim them so required_cols match-up is robust.
  orig_cols <- names(df_product_line)
  trimmed_cols <- trimws(orig_cols)
  whitespace_mismatches <- orig_cols[orig_cols != trimmed_cols]
  if (length(whitespace_mismatches) > 0) {
    warning(sprintf(
      "[meta_init] df_product_line.csv column names had leading/trailing whitespace (normalized): %s",
      paste(sprintf("'%s' -> '%s'",
                    whitespace_mismatches,
                    trimmed_cols[orig_cols != trimmed_cols]),
            collapse = "; ")
    ), call. = FALSE)
    names(df_product_line) <- trimmed_cols
  }
  required_cols <- c("product_line_id",
                     "product_line_name_english",
                     "product_line_name_chinese")
  missing_cols <- setdiff(required_cols, names(df_product_line))
  if (length(missing_cols) > 0) {
    stop(
      "df_product_line.csv missing required columns: ",
      paste(missing_cols, collapse = ", "), "\n",
      "Found columns: ", paste(names(df_product_line), collapse = ", "),
      call. = FALSE
    )
  }
  # Non-blocking info for unexpected extra columns — surfaces silent schema
  # drift (e.g., a new expected column added but not in required_cols yet).
  known_cols <- c(required_cols, "comment_property_sheet_tab")
  extra_cols <- setdiff(names(df_product_line), known_cols)
  if (length(extra_cols) > 0) {
    message(sprintf(
      "[meta_init] df_product_line.csv has extra columns (passed through): %s",
      paste(extra_cols, collapse = ", ")
    ))
  }
  message(sprintf(
    "[meta_init] df_product_line loaded from CSV seed: %d row(s)",
    nrow(df_product_line)
  ))

  # -----------------------------------------------------------------------
  # 2.4 Write to meta_data.duckdb (idempotent: overwrite owned tables,
  #     preserve ___COMPANY-suffix diagnostic tables — triple underscore per DM_R037 v3.0)
  # -----------------------------------------------------------------------
  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE)) {
    stop("Packages DBI and duckdb required.", call. = FALSE)
  }

  con_meta <- DBI::dbConnect(duckdb::duckdb(), meta_path, read_only = FALSE)
  on.exit(
    try(DBI::dbDisconnect(con_meta, shutdown = TRUE), silent = TRUE),
    add = TRUE
  )

  # List pre-existing tables for preservation audit trail
  pre_tables <- DBI::dbListTables(con_meta)
  preserved_tables <- grep("___[A-Z_]+$", pre_tables, value = TRUE)
  if (length(preserved_tables) > 0) {
    message(sprintf(
      "[meta_init] preserving %d company-suffix diagnostic table(s): %s",
      length(preserved_tables),
      paste(preserved_tables, collapse = ", ")
    ))
  }

  # Overwrite owned tables (producer contract)
  DBI::dbWriteTable(con_meta, "df_platform",     df_platform,     overwrite = TRUE)
  DBI::dbWriteTable(con_meta, "df_product_line", df_product_line, overwrite = TRUE)
  message("[meta_init] wrote df_platform + df_product_line to meta_data.duckdb")

  # Smoke check: verify row counts
  check_platform     <- DBI::dbGetQuery(con_meta,
    "SELECT COUNT(*) AS n FROM df_platform")$n
  check_product_line <- DBI::dbGetQuery(con_meta,
    "SELECT COUNT(*) AS n FROM df_product_line")$n
  if (check_platform != nrow(df_platform) ||
      check_product_line != nrow(df_product_line)) {
    stop("Post-write verification failed: row counts do not match source.",
         call. = FALSE)
  }

  DBI::dbDisconnect(con_meta, shutdown = TRUE)

  execution_time <- as.numeric(Sys.time() - start_time, units = "secs")
  message(sprintf("[meta_init] DONE in %.2fs", execution_time))

}, error = function(e) {
  message("[meta_init] ERROR: ", conditionMessage(e))
  error_occurred <<- TRUE
})

# ============================================================================
# PART 3: SUMMARIZE
# ============================================================================

message("══════════════════════════════════════════════════════════════════")
message("DERIVATION SUMMARY")
message(sprintf("Script:          %s", "all_ETL_meta_init_0IM.R"))
message(sprintf("Status:          %s", if (error_occurred) "FAILED" else "SUCCESS"))
message(sprintf(
  "Target:          %s",
  if (exists("meta_path")) meta_path else "(unresolved)"
))
message(sprintf("Execution Time:  %.2f seconds",
                as.numeric(Sys.time() - start_time, units = "secs")))
message("══════════════════════════════════════════════════════════════════")

if (error_occurred) {
  quit(status = 1, save = "no")
}
