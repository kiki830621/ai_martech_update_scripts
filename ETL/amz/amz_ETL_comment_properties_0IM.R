# amz_ETL_comment_properties_0IM.R - Amazon Comment Properties Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL comment_properties Phase 0IM: Import from Google Sheets
# Output: raw_data.duckdb → df_all_comment_property

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

# Initialize environment using autoinit system
# Set required dependencies before initialization
needgoogledrive <- TRUE

# Extend Google API timeout to reduce timeout failures
options(gargle_timeout = 60)

# Initialize using unified autoinit system
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$comment_properties
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))
if (tolower(as.character(etl_profile$source_type %||% "")) != "gsheets") {
  stop(sprintf("VALIDATE FAILED: comment_properties requires source_type='gsheets', got '%s'",
               etl_profile$source_type %||% ""))
}
if (!nzchar(as.character(etl_profile$sheet_id %||% ""))) {
  stop("VALIDATE FAILED: comment_properties profile missing sheet_id")
}

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: Amazon comment properties import (ETL05 0IM) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL comment_properties Import Phase - Amazon comment properties...")

  src_cfg <- platform_cfg$etl_sources$comment_properties
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

  pick_col <- function(df, candidates) {
    existing <- candidates[candidates %in% names(df)]
    if (length(existing) == 0) {
      return(rep(NA_character_, nrow(df)))
    }
    as.character(df[[existing[1]]])
  }

  tab_names <- googlesheets4::sheet_properties(gs_id)$name
  comment_tabs <- tab_names[grepl("水準表", tab_names)]

  active_product_lines <- df_product_line %>%
    dplyr::filter(included == TRUE, product_line_id != "all")

  result_list <- list()
  for (i in seq_len(nrow(active_product_lines))) {
    product_line_id <- active_product_lines$product_line_id[i]
    resolved_tab <- resolve_best_tab(product_line_id, comment_tabs)
    if (is.na(resolved_tab)) {
      message("MAIN: No comment-properties tab found for ", product_line_id, " - skipping")
      next
    }

    message("MAIN: Reading comment properties for ", product_line_id, " from tab '", resolved_tab, "'")
    tab_df <- googlesheets4::read_sheet(gs_id, sheet = resolved_tab, .name_repair = "minimal")
    if (nrow(tab_df) == 0) {
      warning("MAIN: Tab '", resolved_tab, "' is empty - skipping")
      next
    }

    tab_df <- janitor::clean_names(tab_df, ascii = FALSE)
    content_cols <- names(tab_df)[grepl("^屬性水準_內容[0-9]+$", names(tab_df))]
    if (length(content_cols) > 0) {
      ord <- order(as.integer(sub("^屬性水準_內容", "", content_cols)))
      content_cols <- content_cols[ord]
    }

    content_or_na <- function(idx) {
      if (length(content_cols) < idx) return(rep(NA_character_, nrow(tab_df)))
      as.character(tab_df[[content_cols[idx]]])
    }

    parsed_df <- data.frame(
      product_line_id = product_line_id,
      property_id = suppressWarnings(as.integer(pick_col(tab_df, c("number", "property_id", "編號")))),
      property_name = pick_col(tab_df, c("屬性", "property_name")),
      property_name_english = pick_col(tab_df, c("attribute", "property_name_english")),
      frequency = suppressWarnings(as.integer(pick_col(tab_df, c("frequency", "頻率")))),
      proportion = suppressWarnings(as.numeric(pick_col(tab_df, c("proportion", "比例")))),
      definition = pick_col(tab_df, c("定義", "definition")),
      review_1 = content_or_na(1),
      translation_1 = content_or_na(2),
      review_2 = content_or_na(3),
      translation_2 = content_or_na(4),
      review_3 = content_or_na(5),
      translation_3 = content_or_na(6),
      type = pick_col(tab_df, c("type", "水準", "類型")),
      note = pick_col(tab_df, c("note", "備註")),
      stringsAsFactors = FALSE
    )

    parsed_df <- parsed_df %>%
      dplyr::filter(!is.na(property_id), !is.na(property_name), trimws(property_name) != "") %>%
      dplyr::mutate(
        property_name_english = dplyr::if_else(
          is.na(property_name_english) | trimws(property_name_english) == "",
          property_name,
          property_name_english
        ),
        etl_import_source = resolved_tab,
        etl_import_timestamp = Sys.time(),
        etl_phase = "import"
      )

    if (nrow(parsed_df) == 0) {
      warning("MAIN: Parsed 0 rows from tab '", resolved_tab, "'")
      next
    }

    result_list[[product_line_id]] <- parsed_df
    message("MAIN: Parsed ", nrow(parsed_df), " properties for ", product_line_id)
  }

  missing_product_lines <- setdiff(active_product_lines$product_line_id, names(result_list))
  if (length(missing_product_lines) > 0) {
    message("MAIN WARNING: No comment-property source data for product_line_id(s): ",
            paste(missing_product_lines, collapse = ", "))
  }

  if (length(result_list) == 0) {
    stop("No comment property rows parsed from any sheet tab")
  }

  comment_properties <- dplyr::bind_rows(result_list) %>%
    dplyr::distinct(product_line_id, property_id, .keep_all = TRUE)

  DBI::dbWriteTable(raw_data, "df_all_comment_property", comment_properties, overwrite = TRUE)
  message("MAIN: Wrote ", nrow(comment_properties), " rows into df_all_comment_property")

  script_success <- TRUE
  message("MAIN: ETL comment_properties Import Phase completed successfully")

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
    message("TEST: Verifying ETL comment_properties Import Phase results...")

    # Check if comment properties table exists and has data
    table_name <- "df_all_comment_property"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
      property_count <- sql_read(raw_data, query)$count

      if (property_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", property_count,
                " comment properties imported to raw_data")
        
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", table_name, " LIMIT 3")
        sample_data <- sql_read(raw_data, structure_query)
        message("TEST: Sample raw data structure:")
        print(sample_data)
        
        # Check for required columns
        required_cols <- c("product_line_id", "property_id", "property_name")
        actual_cols <- names(sample_data)
        missing_cols <- setdiff(required_cols, actual_cols)
        
        if (length(missing_cols) > 0) {
          message("TEST WARNING: Missing expected columns: ", paste(missing_cols, collapse = ", "))
        } else {
          message("TEST: All required columns present")
        }
        
        # Check product line distribution
        product_line_query <- paste0("SELECT product_line_id, COUNT(*) as count FROM ", table_name, " GROUP BY product_line_id")
        product_line_stats <- sql_read(raw_data, product_line_query)
        message("TEST: Product line distribution:")
        print(product_line_stats)
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no comment properties found in table")
      }
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - table ", table_name, " not found")
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

# Determine final status before tearing down
if (script_success && test_passed) {
  message("DEINITIALIZE: ETL comment_properties Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL comment_properties Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL comment_properties Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL comment_properties Import Phase (amz_ETL_comment_properties_0IM.R) completed")
