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

app_data <- dbConnectDuckdb(db_path_list$app_data)

df_platform___selected<- 
  df_platform %>% 
  filter(platform_id %in% c("all",app_configs$platform))

df_platform___selected

dbWriteTable(app_data, "df_platform", df_platform___selected, overwrite = TRUE)

########product_property_dictionary_KM
googlesheet_con <-as_sheets_id("1aKyyOMpIJtDtpqe7Iz0AfSU0W9aAdpSdPDD1zgnqO30")
product_property_dictionary  <-read_sheet(googlesheet_con, sheet = "SKUtoASIN")
  





autodeinit()
