autoinit()

comment_property_rating <- dbConnectDuckdb(db_path_list$comment_property_rating, read_only = FALSE)


comment_property_rating_results <- dbConnectDuckdb(db_path_list$comment_property_rating_results, read_only = FALSE)


query1 <- generate_create_table_query(
  con = comment_property_rating,
  target_table = "df_comment_property_rating_jew___raw",
  source_table = NULL,
  column_defs = list(
    list(name= "product_line_id", type = "VARCHAR", not_null = TRUE),
    list(name = "product_line_name_english", type = "VARCHAR"),
    list(name = "date", type = "DATE", not_null = TRUE),
    list(name = "asin", type = "VARCHAR", not_null = TRUE),
    list(name = "author", type = "VARCHAR", not_null = TRUE),
    list(name = "title", type = "VARCHAR", not_null = TRUE),
    list(name = "body", type = "VARCHAR", not_null = TRUE),
    list(name = "response", type = "VARCHAR", not_null = TRUE),
    list(name = "included", type = "BOOLEAN"),
    list(name = "included_competiter", type = "BOOLEAN")
    ),
  primary_key = c("product_line_id", "date","asin","author","title","body")
)
    
    
print_query(query1, "SIMPLE TABLE WITH SINGLE PRIMARY KEY")

# Execute query to create the table
dbExecute(comment_property_rating_results, query1)



autodeinit()
