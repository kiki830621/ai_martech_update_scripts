#####
#P07_D01_03
OPERATION_MODE <- "UPDATE_MODE"
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))
processed_data <- dbConnectDuckdb(db_path_list$processed_data)
app_data <- dbConnectDuckdb(db_path_list$app_data)

df_amazon_sales___1<- tbl(processed_data, "df_amazon_sales") %>% 
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

tbl(app_data,"df_customer_profile") %>% filter(platform_id==2)

source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))
