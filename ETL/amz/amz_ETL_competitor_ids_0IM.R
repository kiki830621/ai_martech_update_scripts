# amz_ETL_competitor_ids_0IM.R - Amazon Competitor IDs Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL competitor_ids Phase 0IM: Import from Google Sheets
# Output: raw_data.duckdb → df_amz_competitor_product_id

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
expected_competitor_asin <- data.frame(
  product_line_id = character(0),
  asin = character(0),
  stringsAsFactors = FALSE
)
source_missing_profile_asin <- data.frame(
  product_line_id = character(0),
  asin = character(0),
  stringsAsFactors = FALSE
)

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
etl_profile <- platform_cfg$etl_sources$competitor_ids
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))
if (tolower(as.character(etl_profile$source_type %||% "")) != "gsheets") {
  stop(sprintf("VALIDATE FAILED: competitor_ids requires source_type='gsheets', got '%s'",
               etl_profile$source_type %||% ""))
}
if (!nzchar(as.character(etl_profile$sheet_id %||% ""))) {
  stop("VALIDATE FAILED: competitor_ids profile missing sheet_id")
}
if (!nzchar(as.character(etl_profile$sheet_name %||% ""))) {
  stop("VALIDATE FAILED: competitor_ids profile missing sheet_name")
}

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: Amazon competitor products import (ETL04 0IM) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL competitor_ids Import Phase - Amazon competitor products...")

  src_cfg <- platform_cfg$etl_sources$competitor_ids
  gs_id <- googlesheets4::as_sheets_id(src_cfg$sheet_id)

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

  resolve_best_header <- function(product_line_id, candidates) {
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
      lapply(candidate_pool, function(header_name) {
        header_lower <- tolower(header_name)
        matched <- sum(vapply(keys, function(k) grepl(k, header_lower, fixed = TRUE), logical(1)))
        extra <- max(0, length(strsplit(gsub("[^a-z0-9]+", " ", header_lower), "\\s+")[[1]]) - matched)
        data.frame(header_name = header_name, matched = matched, extra = extra, nchar = nchar(header_name))
      })
    )
    score_df <- score_df[order(-score_df$matched, score_df$extra, score_df$nchar), ]
    score_df$header_name[1]
  }

  competitor_raw <- googlesheets4::read_sheet(gs_id, sheet = src_cfg$sheet_name, col_types = "c")
  raw_headers <- names(competitor_raw)
  product_headers <- raw_headers[!grepl("^(\\.\\.\\.|產品編碼|銷售數據|評論數據|品牌)", raw_headers)]

  active_product_lines <- df_product_line %>%
    dplyr::filter(included == TRUE, product_line_id != "all")

  result_list <- list()
  for (i in seq_len(nrow(active_product_lines))) {
    product_line_id <- active_product_lines$product_line_id[i]
    resolved_header <- resolve_best_header(product_line_id, product_headers)
    if (is.na(resolved_header)) {
      message("MAIN: No competitor header resolved for ", product_line_id, " - skipping")
      next
    }

    col_idx <- match(resolved_header, raw_headers)
    if (is.na(col_idx)) next
    asin_idx <- col_idx
    sales_idx <- col_idx + 1
    review_idx <- col_idx + 2
    brand_idx <- col_idx + 4

    asin_vec <- if (asin_idx <= ncol(competitor_raw)) as.character(competitor_raw[[asin_idx]]) else rep(NA_character_, nrow(competitor_raw))
    sales_vec <- if (sales_idx <= ncol(competitor_raw)) as.character(competitor_raw[[sales_idx]]) else rep(NA_character_, nrow(competitor_raw))
    review_vec <- if (review_idx <= ncol(competitor_raw)) as.character(competitor_raw[[review_idx]]) else rep(NA_character_, nrow(competitor_raw))
    brand_vec <- if (brand_idx <= ncol(competitor_raw)) as.character(competitor_raw[[brand_idx]]) else rep(NA_character_, nrow(competitor_raw))

    block_df <- data.frame(
      product_line_id = product_line_id,
      asin = asin_vec,
      sales_data = sales_vec,
      review_data = review_vec,
      brand = brand_vec,
      source_header = resolved_header,
      stringsAsFactors = FALSE
    ) %>%
      dplyr::mutate(
        asin = trimws(asin),
        brand = dplyr::if_else(is.na(brand) | trimws(brand) == "", "UNKNOWN", trimws(brand)),
        sales_data = trimws(sales_data),
        review_data = trimws(review_data)
      ) %>%
      dplyr::filter(!is.na(asin), asin != "", grepl("^[A-Za-z0-9]{8,}$", asin))

    if (nrow(block_df) == 0) {
      next
    }
    result_list[[product_line_id]] <- block_df
    message("MAIN: Parsed ", nrow(block_df), " competitor rows for ", product_line_id)
  }

  if (length(result_list) == 0) {
    stop("No competitor products were parsed from sheet")
  }

  competitor_products <- dplyr::bind_rows(result_list) %>%
    dplyr::distinct(product_line_id, asin, .keep_all = TRUE)

  # Ensure QEF profile ASINs are represented in competitor list for downstream joins.
  # Missing entries are backfilled with a warning (source coverage issue, not import failure).
  profile_asin_rows <- list()
  for (i in seq_len(nrow(active_product_lines))) {
    product_line_id <- active_product_lines$product_line_id[i]
    profile_table <- paste0("df_product_profile_", product_line_id)
    if (!dbExistsTable(raw_data, profile_table)) {
      message("MAIN WARNING: profile table missing for ", product_line_id,
              " (", profile_table, "); skip QEF ASIN coverage check for this line")
      next
    }

    profile_query <- sprintf(
      "SELECT DISTINCT CAST(ASIN AS VARCHAR) AS asin
       FROM %s
       WHERE ASIN IS NOT NULL
         AND length(trim(CAST(ASIN AS VARCHAR))) > 0",
      profile_table
    )
    profile_asin <- DBI::dbGetQuery(raw_data, profile_query)$asin
    profile_asin <- unique(trimws(as.character(profile_asin)))
    profile_asin <- profile_asin[!is.na(profile_asin) & profile_asin != ""]
    if (length(profile_asin) == 0) next

    profile_asin_rows[[product_line_id]] <- data.frame(
      product_line_id = product_line_id,
      asin = profile_asin,
      stringsAsFactors = FALSE
    )
  }

  if (length(profile_asin_rows) > 0) {
    profile_asin_df <- dplyr::bind_rows(profile_asin_rows) %>%
      dplyr::distinct(product_line_id, asin)
    source_competitor_asin <- competitor_products %>%
      dplyr::transmute(
        product_line_id = trimws(as.character(product_line_id)),
        asin = trimws(as.character(asin))
      ) %>%
      dplyr::filter(
        !is.na(product_line_id), product_line_id != "",
        !is.na(asin), asin != ""
      ) %>%
      dplyr::distinct(product_line_id, asin)

    source_missing_profile_asin <- dplyr::anti_join(
      profile_asin_df,
      source_competitor_asin,
      by = c("product_line_id", "asin")
    )

    if (nrow(source_missing_profile_asin) > 0) {
      qef_brand_label <- "QEF_SELF"
      if (exists("brand_name", inherits = TRUE)) {
        brand_name_candidate <- trimws(as.character(get("brand_name", inherits = TRUE)))
        if (!is.na(brand_name_candidate) && nzchar(brand_name_candidate)) {
          qef_brand_label <- brand_name_candidate
        }
      }
      if (identical(qef_brand_label, "QEF_SELF") && requireNamespace("yaml", quietly = TRUE)) {
        app_config_path <- if (exists("CONFIG_PATH", inherits = TRUE)) {
          get("CONFIG_PATH", inherits = TRUE)
        } else {
          "app_config.yaml"
        }
        if (file.exists(app_config_path)) {
          app_cfg <- tryCatch(yaml::read_yaml(app_config_path), error = function(e) NULL)
          app_cfg_brand <- trimws(as.character(app_cfg$brand_name %||% ""))
          if (!is.na(app_cfg_brand) && nzchar(app_cfg_brand)) {
            qef_brand_label <- app_cfg_brand
          }
        }
      }

      message(
        "MAIN WARNING: Found ", nrow(source_missing_profile_asin),
        " profile ASIN not present in competitor source; auto-backfilling into df_amz_competitor_product_id"
      )
      sample_missing <- head(
        paste0(source_missing_profile_asin$product_line_id, ":", source_missing_profile_asin$asin),
        10
      )
      message("MAIN WARNING: Missing sample: ", paste(sample_missing, collapse = ", "))

      backfill_rows <- source_missing_profile_asin %>%
        dplyr::mutate(
          sales_data = NA_character_,
          review_data = NA_character_,
          brand = qef_brand_label,
          source_header = "AUTO_PROFILE_ASIN_BACKFILL"
        )

      competitor_products <- dplyr::bind_rows(competitor_products, backfill_rows) %>%
        dplyr::distinct(product_line_id, asin, .keep_all = TRUE)
      message("MAIN: Backfilled ", nrow(backfill_rows), " QEF profile ASIN rows")
    }
  } else {
    message("MAIN WARNING: No profile ASIN data available; skip QEF ASIN coverage check")
  }

  # DM_R027 v1.1: capture source ASIN keys for 0IM reconciliation
  expected_competitor_asin <- competitor_products %>%
    dplyr::transmute(
      product_line_id = trimws(as.character(product_line_id)),
      asin = trimws(as.character(asin))
    ) %>%
    dplyr::filter(
      !is.na(product_line_id), product_line_id != "",
      !is.na(asin), asin != ""
    ) %>%
    dplyr::distinct(product_line_id, asin)

  DBI::dbWriteTable(raw_data, "df_amz_competitor_product_id", competitor_products, overwrite = TRUE)
  message("MAIN: Wrote ", nrow(competitor_products), " rows into df_amz_competitor_product_id")

  script_success <- TRUE
  message("MAIN: ETL competitor_ids Import Phase completed successfully")

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
    message("TEST: Verifying ETL competitor_ids Import Phase results...")

    # Check if competitor products table exists and has data
    table_name <- "df_amz_competitor_product_id"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
      product_count <- sql_read(raw_data, query)$count

      if (product_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", product_count,
                " competitor products imported to raw_data")
        
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", table_name, " LIMIT 3")
        sample_data <- sql_read(raw_data, structure_query)
        message("TEST: Sample raw data structure:")
        print(sample_data)
        
        # Check for required columns
        required_cols <- c("product_line_id", "asin", "brand")
        actual_cols <- names(sample_data)
        missing_cols <- setdiff(required_cols, actual_cols)
        
        if (length(missing_cols) > 0) {
          message("TEST WARNING: Missing expected columns: ", paste(missing_cols, collapse = ", "))
        } else {
          message("TEST: All required columns present")
        }

        if (nrow(source_missing_profile_asin) > 0) {
          message(
            "TEST WARNING: ", nrow(source_missing_profile_asin),
            " profile ASIN were absent in competitor source and auto-backfilled"
          )
          warning_sample <- head(
            paste0(source_missing_profile_asin$product_line_id, ":", source_missing_profile_asin$asin),
            10
          )
          message("TEST WARNING: Backfill sample: ", paste(warning_sample, collapse = ", "))
        }

        # DM_R027 v1.1: 0IM source-to-local ASIN reconciliation gate
        if (test_passed && nrow(expected_competitor_asin) > 0) {
          local_asin_query <- paste(
            "SELECT DISTINCT",
            "CAST(product_line_id AS VARCHAR) AS product_line_id,",
            "CAST(asin AS VARCHAR) AS asin",
            "FROM df_amz_competitor_product_id",
            "WHERE product_line_id IS NOT NULL",
            "AND length(trim(CAST(product_line_id AS VARCHAR))) > 0",
            "AND asin IS NOT NULL",
            "AND length(trim(CAST(asin AS VARCHAR))) > 0"
          )
          local_competitor_asin <- DBI::dbGetQuery(raw_data, local_asin_query) %>%
            dplyr::mutate(
              product_line_id = trimws(as.character(product_line_id)),
              asin = trimws(as.character(asin))
            ) %>%
            dplyr::filter(
              !is.na(product_line_id), product_line_id != "",
              !is.na(asin), asin != ""
            ) %>%
            dplyr::distinct(product_line_id, asin)

          missing_in_local <- dplyr::anti_join(
            expected_competitor_asin,
            local_competitor_asin,
            by = c("product_line_id", "asin")
          )
          extra_in_local <- dplyr::anti_join(
            local_competitor_asin,
            expected_competitor_asin,
            by = c("product_line_id", "asin")
          )

          if (nrow(missing_in_local) > 0 || nrow(extra_in_local) > 0) {
            test_passed <- FALSE
            message(
              "TEST RECON FAIL: competitor_ids source_n=", nrow(expected_competitor_asin),
              " local_n=", nrow(local_competitor_asin),
              " missing=", nrow(missing_in_local),
              " extra=", nrow(extra_in_local)
            )
            if (nrow(missing_in_local) > 0) {
              missing_sample <- head(
                paste0(missing_in_local$product_line_id, ":", missing_in_local$asin),
                10
              )
              message("  Missing sample: ", paste(missing_sample, collapse = ", "))
            }
            if (nrow(extra_in_local) > 0) {
              extra_sample <- head(
                paste0(extra_in_local$product_line_id, ":", extra_in_local$asin),
                10
              )
              message("  Extra sample: ", paste(extra_sample, collapse = ", "))
            }
            message("TEST: DM_R027 0IM ASIN reconciliation failed")
          } else {
            message("TEST: DM_R027 0IM ASIN reconciliation passed")
          }
        }
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no competitor products found in table")
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
  message("DEINITIALIZE: ETL competitor_ids Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL competitor_ids Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL competitor_ids Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL competitor_ids Import Phase (amz_ETL_competitor_ids_0IM.R) completed")
