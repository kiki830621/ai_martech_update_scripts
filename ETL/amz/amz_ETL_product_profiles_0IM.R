# amz_ETL_product_profiles_0IM.R - Amazon Product Profiles Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL product_profiles Phase 0IM: Import from Google Sheets
# Output: raw_data.duckdb → df_product_profile_{product_line_id}

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
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
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
source_profile_asin <- list()

# Initialize environment using autoinit system
# Set required dependencies before initialization
needgoogledrive <- TRUE

# Initialize using unified autoinit system
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$product_profiles
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))
if (tolower(as.character(etl_profile$source_type %||% "")) != "gsheets") {
  stop(sprintf("VALIDATE FAILED: product_profiles requires source_type='gsheets', got '%s'",
               etl_profile$source_type %||% ""))
}
if (!nzchar(as.character(etl_profile$sheet_id %||% ""))) {
  stop("VALIDATE FAILED: product_profiles profile missing sheet_id")
}

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: Amazon product profiles import initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting Amazon product profiles import...")

  # DM_R037 v3.0: read connection params from etl_sources config
  src_cfg <- platform_cfg$etl_sources$product_profiles
  gs_id <- googlesheets4::as_sheets_id(src_cfg$sheet_id)

  # Product line tab matching rules for AMZ coding sheet.
  product_line_zh_anchor <- c(
    hsg = "安全眼鏡", sfg = "安全眼鏡", sfo = "安全眼鏡", sss = "安全眼鏡",
    bys = "太陽眼鏡", cas = "太陽眼鏡", sgf = "太陽眼鏡", sgo = "太陽眼鏡",
    psg = "摩托車護目鏡", blb = "抗藍光眼鏡", its = "嬰幼兒童眼鏡",
    rpl = "備片", wwp = "濕紙巾", htl = "手工具", gcl = "眼鏡盒"
  )
  product_line_keywords <- list(
    hsg = c("hunting", "safety", "glasses"),
    sfg = c("safety", "glasses"),
    sfo = c("safety", "glasses", "fit", "over"),
    sss = c("safety", "glasses", "side", "shields"),
    bys = c("baseball", "youth"),
    cas = c("cycling", "adult"),
    sgf = c("sunglasses", "fishing"),
    sgo = c("sunglasses", "fit", "over"),
    psg = c("powersports", "goggles"),
    blb = c("blue", "light", "blocking", "glasses"),
    its = c("infant", "toddler", "sunglasses"),
    rpl = c("replacement", "lens"),
    wwp = character(0),
    htl = character(0),
    gcl = character(0)
  )

  resolve_best_tab <- function(product_line_id, candidates) {
    anchor <- product_line_zh_anchor[[product_line_id]] %||% ""
    candidate_pool <- candidates
    if (nzchar(anchor)) {
      candidate_pool <- candidate_pool[grepl(anchor, candidate_pool, fixed = TRUE)]
    }
    if (length(candidate_pool) == 0) {
      return(NA_character_)
    }

    keys <- product_line_keywords[[product_line_id]]
    if (is.null(keys)) keys <- character(0)
    if (length(keys) == 0) {
      return(candidate_pool[which.min(nchar(candidate_pool))])
    }

    score_df <- do.call(
      rbind,
      lapply(candidate_pool, function(tab_name) {
        tab_lower <- tolower(tab_name)
        matched <- sum(vapply(keys, function(k) grepl(k, tab_lower, fixed = TRUE), logical(1)))
        extra <- max(0, length(strsplit(gsub("[^a-z0-9]+", " ", tab_lower), "\\s+")[[1]]) - matched)
        data.frame(tab_name = tab_name, matched = matched, extra = extra, nchar = nchar(tab_name))
      })
    )
    score_df <- score_df[order(-score_df$matched, score_df$extra, score_df$nchar), ]
    score_df$tab_name[1]
  }

  tab_names <- googlesheets4::sheet_properties(gs_id)$name
  meta_tabs <- c(
    "產品類別彙整表", "進度表", "產品對照表", "SKU to ASIN",
    "Holiday", "Product Line", "Promotion Codebook", "Promotion", "中英文名稱對照表"
  )
  profile_tabs <- tab_names[
    !tab_names %in% meta_tabs &
      !grepl("水準表", tab_names) &
      !grepl("_原$", tab_names)
  ]

  active_product_lines <- df_product_line %>%
    dplyr::filter(included == TRUE, product_line_id != "all")

  import_result <- list()
  for (i in seq_len(nrow(active_product_lines))) {
    product_line_id <- active_product_lines$product_line_id[i]
    resolved_tab <- resolve_best_tab(product_line_id, profile_tabs)
    if (is.na(resolved_tab)) {
      warning("MAIN: No profile tab resolved for product_line_id=", product_line_id, " - skipping")
      next
    }

    message("MAIN: Importing ", product_line_id, " from tab '", resolved_tab, "'")
    df_product_profile <- googlesheets4::read_sheet(gs_id, sheet = resolved_tab, .name_repair = "minimal")

    if (nrow(df_product_profile) == 0) {
      warning("MAIN: Tab '", resolved_tab, "' is empty - skipping")
      next
    }

    # Remove duplicated header names while keeping first occurrence.
    dup_logical <- duplicated(names(df_product_profile))
    if (any(dup_logical)) {
      df_product_profile <- df_product_profile[, !dup_logical, drop = FALSE]
    }

    # DuckDB cannot write list-typed columns directly.
    list_cols <- vapply(df_product_profile, is.list, logical(1))
    if (any(list_cols)) {
      df_product_profile[list_cols] <- lapply(df_product_profile[list_cols], function(x) {
        vapply(x, toString, character(1))
      })
    }

    if (exists("convert_all_columns_to_utf8", mode = "function")) {
      df_product_profile <- tryCatch(
        convert_all_columns_to_utf8(df_product_profile),
        error = function(e) {
          warning("MAIN: UTF-8 conversion skipped for tab '", resolved_tab, "': ", e$message)
          df_product_profile
        }
      )
    }
    if (exists("remove_illegal_utf8", mode = "function")) {
      df_product_profile <- tryCatch(
        remove_illegal_utf8(df_product_profile),
        error = function(e) {
          warning("MAIN: Illegal UTF-8 cleanup skipped for tab '", resolved_tab, "': ", e$message)
          df_product_profile
        }
      )
    }

    # DM_R027 v1.1: capture source ASIN set for 0IM reconciliation
    asin_col_idx <- which(tolower(names(df_product_profile)) == "asin")[1]
    if (!is.na(asin_col_idx)) {
      asin_vals <- trimws(as.character(df_product_profile[[asin_col_idx]]))
      asin_vals <- unique(asin_vals[!is.na(asin_vals) & nzchar(asin_vals)])
      source_profile_asin[[product_line_id]] <- asin_vals
    } else {
      warning("MAIN: ASIN column not found for product_line_id=", product_line_id,
              " (tab=", resolved_tab, "); reconciliation will skip this table")
    }

    df_product_profile <- df_product_profile %>%
      dplyr::mutate(
        etl_import_timestamp = Sys.time(),
        etl_import_source = resolved_tab,
        etl_product_line_id = product_line_id,
        etl_phase = "import"
      )

    target_table <- paste0("df_product_profile_", product_line_id)
    DBI::dbWriteTable(raw_data, target_table, df_product_profile, overwrite = TRUE)
    import_result[[product_line_id]] <- list(
      table = target_table,
      tab = resolved_tab,
      rows = nrow(df_product_profile)
    )
    message("MAIN: Imported ", nrow(df_product_profile), " rows into ", target_table)
  }

  missing_product_lines <- setdiff(active_product_lines$product_line_id, names(import_result))
  if (length(missing_product_lines) > 0) {
    message("MAIN WARNING: No imported profile data for product_line_id(s): ",
            paste(missing_product_lines, collapse = ", "))
  }

  if (length(import_result) == 0) {
    stop("No product profile tabs were imported")
  }

  message("MAIN: Imported ", length(import_result), " product profile tables")

  # List all tables in raw_data database
  all_tables <- dbListTables(raw_data)
  message("MAIN: All tables in raw_data: ", paste(all_tables, collapse = ", "))
  
  # Check specific product profile tables
  product_profile_tables <- all_tables[grepl("^df_product_profile_", all_tables)]
  message("MAIN: product profile tables found: ", paste(product_profile_tables, collapse = ", "))

  script_success <- TRUE
  message("MAIN: Amazon product profiles import completed successfully")

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message("MAIN ERROR: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================

if (script_success) {
  tryCatch({
    message("TEST: Verifying product profiles import...")

    # Check if individual product profile tables exist and have data
    # Get active product lines (excluding 'all')
    active_product_lines <- df_product_line %>%
      filter(included == TRUE, product_line_id != "all") %>%
      pull(product_line_id)
    
    total_product_count <- 0
    tables_found <- 0
    
    for (product_line_id in active_product_lines) {
      table_name <- paste0("df_product_profile_", product_line_id)
      if (dbExistsTable(raw_data, table_name)) {
        tables_found <- tables_found + 1
        query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
        product_count <- sql_read(raw_data, query)$count
        total_product_count <- total_product_count + product_count
        message("TEST: Found ", product_count, " products in ", table_name)
      }
    }
    
    if (tables_found > 0 && total_product_count > 0) {
      test_passed <- TRUE
      message("TEST: Verification successful - ", total_product_count,
              " total product profiles imported across ", tables_found, " product lines")
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - no product profile tables found or empty")
    }

    # DM_R027 v1.1: 0IM source-to-local ASIN reconciliation gate
    if (test_passed && length(source_profile_asin) > 0) {
      recon_failed <- FALSE
      for (product_line_id in names(source_profile_asin)) {
        table_name <- paste0("df_product_profile_", product_line_id)
        if (!dbExistsTable(raw_data, table_name)) {
          message("TEST RECON FAIL: table not found for product_line_id=", product_line_id)
          recon_failed <- TRUE
          next
        }

        local_query <- sprintf(
          "SELECT DISTINCT CAST(ASIN AS VARCHAR) AS asin
           FROM %s
           WHERE ASIN IS NOT NULL AND length(trim(CAST(ASIN AS VARCHAR))) > 0",
          table_name
        )
        local_asin <- DBI::dbGetQuery(raw_data, local_query)$asin
        local_asin <- unique(trimws(as.character(local_asin)))
        local_asin <- local_asin[!is.na(local_asin) & nzchar(local_asin)]

        source_asin <- unique(trimws(as.character(source_profile_asin[[product_line_id]])))
        source_asin <- source_asin[!is.na(source_asin) & nzchar(source_asin)]

        missing_in_local <- setdiff(source_asin, local_asin)
        extra_in_local <- setdiff(local_asin, source_asin)

        if (length(missing_in_local) > 0 || length(extra_in_local) > 0) {
          recon_failed <- TRUE
          message(
            "TEST RECON FAIL: ", product_line_id,
            " source_n=", length(source_asin),
            " local_n=", length(local_asin),
            " missing=", length(missing_in_local),
            " extra=", length(extra_in_local)
          )
          if (length(missing_in_local) > 0) {
            message("  Missing sample: ", paste(head(missing_in_local, 10), collapse = ", "))
          }
          if (length(extra_in_local) > 0) {
            message("  Extra sample: ", paste(head(extra_in_local, 10), collapse = ", "))
          }
        } else {
          message("TEST RECON OK: ", product_line_id,
                  " ASIN sets match (", length(source_asin), ")")
        }
      }

      if (recon_failed) {
        test_passed <- FALSE
        message("TEST: DM_R027 0IM ASIN reconciliation failed")
      } else {
        message("TEST: DM_R027 0IM ASIN reconciliation passed")
      }
    }

  }, error = function(e) {
    test_passed <<- FALSE
    message("TEST ERROR: ", e$message)
  })
} else {
  message("TEST: Skipped due to main script failure")
}

# ==============================================================================
# 4. DEINITIALIZE
# ==============================================================================

# Determine final status before tearing down -------------------------------------------------
if (script_success && test_passed) {
  message("DEINITIALIZE: Script completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: Script completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: Script failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: Amazon product profiles import script completed")
