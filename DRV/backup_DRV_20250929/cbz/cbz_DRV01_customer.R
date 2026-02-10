#####
# CONSUMES: df_amazon_sales, df_amazon_sales___1, df_customer_profile
# PRODUCES: df_amazon_sales___1, df_customer_profile
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: none
#####

#####
#P07_D01_03
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

df_amazon_sales___1<- tbl2(processed_data, "df_amazon_sales") %>% 
  collect() %>% 
  rename(lineproduct_price = product_price,
         payment_time = time) %>% 
  mutate(customer_id = as.integer(as.factor(ship_postal_code))) %>% 
  drop_na(customer_id)
  

dbWriteTable(
  processed_data,
  "df_amazon_sales___1",
  df_amazon_sales___1,
  append = FALSE,
  row.names = TRUE,
  overwrite = TRUE
)

df.amazon.customer_profile <- df_amazon_sales___1 %>%
  mutate(buyer_name = customer_id,email = ship_postal_code) %>% 
  select(customer_id, buyer_name, email) %>%   
  distinct(customer_id,buyer_name,.keep_all=T)%>% 
  arrange(customer_id) %>% 
  mutate(platform_id = 2L)

dbWriteTable(
  app_data,
  "df_customer_profile",
  df.amazon.customer_profile,
  append=T
)

tbl2(app_data,"df_customer_profile") %>% filter(platform_id==2)

source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))
