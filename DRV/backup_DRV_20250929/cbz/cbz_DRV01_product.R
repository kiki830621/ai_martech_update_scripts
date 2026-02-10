#####
# CONSUMES: df_amazon_sales, df_amazon_sales___1, df_amazon_sales_by_customer, df_amazon_sales_by_customer_by_date
# PRODUCES: df_amazon_sales_by_customer, df_amazon_sales_by_customer_by_date
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: none
#####

#####
#P07_D01_04
tbl2_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R")
)
tbl2_path <- tbl2_candidates[file.exists(tbl2_candidates)][1]
if (is.na(tbl2_path)) {
  stop("fn_tbl2.R not found in expected paths")
}
source(tbl2_path)
OPERATION_MODE <- "UPDATE_MODE"
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))
processed_data <- dbConnectDuckdb(db_path_list$processed_data)
app_data <- dbConnectDuckdb(db_path_list$app_data)


df_amazon_sales___1 <- tbl2(processed_data, "df_amazon_sales___1") %>% collect()

df_amazon_sales.by_customer.by_date <- transform_sales_to_sales_by_customer.by_date(df_amazon_sales___1)
df_amazon_sales.by_customer<- transform_sales_by_customer.by_date_to_sales_by_customer(df_amazon_sales.by_customer.by_date)

dbWriteTable(
  processed_data,
  "df_amazon_sales_by_customer_by_date",
  df_amazon_sales.by_customer.by_date,
  append = FALSE,
  row.names = TRUE,
  overwrite = TRUEs
)

dbWriteTable(
  processed_data,
  "df_amazon_sales_by_customer",
  df_amazon_sales.by_customer,
  append = FALSE,
  row.names = TRUE,
  overwrite = TRUE
)

source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))

