#' @file S01_01.R
#' @sequence S01 product and Product Line Profiles
#' @step 01 Import and Map product Profiles
#' @rule R118 Lowercase Natural Key Rule
#' @rule R119 Memory-Resident Parameters Rule
#' @description Import product data and map to product lines

#' Import and Map product Profiles
#'
#' This function imports product dictionary data from external sources, maps products
#' to product lines, and filters by active platforms. It implements the requirements
#' of S01_01.
#'
#' @param conn DBI connection. Database connection to use.
#' @param data_dir Character. Directory containing external data.
#' @param product_source Character. Source file or pattern for product data.
#' @return Invisibly returns the product profile data frame.
#'

autoinit()

# ARCHIVED 2026-04-19 — df_platform writer block removed per DM_R054 v2.1
# (issue #424). `df_platform` is metadata and lives EXCLUSIVELY in
# meta_data.duckdb, produced by all_ETL_meta_init_0IM.R. See
#   ./archive_DM_R054_v2_20260419/archived_df_platform_writer.R
# for the historical block preserved verbatim. DO NOT re-add a writer here.

########product_property_dictionary_KM
googlesheet_con <-as_sheets_id("1aKyyOMpIJtDtpqe7Iz0AfSU0W9aAdpSdPDD1zgnqO30")
product_property_dictionary  <-read_sheet(googlesheet_con, sheet = "SKUtoASIN")
  





autodeinit()
