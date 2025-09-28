#####
#P07_D01_04
OPERATION_MODE <- "UPDATE_MODE"
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))
processed_data <- dbConnectDuckdb(db_path_list$processed_data)
app_data <- dbConnectDuckdb(db_path_list$app_data)


df_amazon_sales___1 <- tbl(processed_data, "df_amazon_sales___1") %>% collect()

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

