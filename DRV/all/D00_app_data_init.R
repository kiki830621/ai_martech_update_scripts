# ==============================================================================
# D00: Create/Refresh App Data Tables (Customer DNA)
# Follows DM_R044 (derivation structure), MP058 (schema-first), MP064 (separation)
# ==============================================================================

# PART 1: INITIALIZE ----------------------------------------------------------
if (!exists("autoinit", mode = "function")) {
  source(file.path("scripts", "global_scripts", "22_initializations", "sc_Rprofile.R"))
}
OPERATION_MODE <- "UPDATE_MODE"
autoinit()

source(file.path(GLOBAL_DIR, "02_db_utils", "duckdb", "fn_dbConnectDuckdb.R"))

if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
  app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
  connection_created_app <- TRUE
} else {
  connection_created_app <- FALSE
}

# Source table creators (one function per table)
source_if_missing <- function(path) {
  if (file.exists(path)) source(path)
}
source_if_missing(file.path("scripts", "global_scripts", "14_sql_utils", "fn_sanitize_identifier.R"))
source_if_missing(file.path("scripts", "global_scripts", "14_sql_utils", "fn_quote_identifier.R"))
source_if_missing(file.path("scripts", "global_scripts", "01_db", "fn_create_df_profile_by_customer_table.R"))
source_if_missing(file.path("scripts", "global_scripts", "01_db", "fn_create_df_dna_by_customer_table.R"))
source_if_missing(file.path("scripts", "global_scripts", "01_db", "fn_create_df_segments_by_customer_table.R"))

tables_to_create <- c(
  "df_profile_by_customer",
  "df_dna_by_customer",
  "df_segments_by_customer"
)

success <- TRUE
created <- list()

# PART 2: MAIN ----------------------------------------------------------------
tryCatch({
  create_df_profile_by_customer_table(app_data, or_replace = TRUE, verbose = TRUE)
  created <- c(created, "df_profile_by_customer")

  create_df_dna_by_customer_table(app_data, or_replace = TRUE, verbose = TRUE)
  created <- c(created, "df_dna_by_customer")

  create_df_segments_by_customer_table(app_data, or_replace = TRUE, verbose = TRUE)
  created <- c(created, "df_segments_by_customer")

}, error = function(e) {
  success <<- FALSE
  message("MAIN: ERROR creating tables - ", e$message)
})

# PART 3: TEST ----------------------------------------------------------------
if (success) {
  tryCatch({
    for (tbl_name in tables_to_create) {
      if (!dbExistsTable(app_data, tbl_name)) {
        stop(sprintf("Table %s not found after creation", tbl_name))
      }
    }
    message("TEST: All tables created successfully")
  }, error = function(e) {
    success <<- FALSE
    message("TEST: ERROR - ", e$message)
  })
}

# PART 4: SUMMARIZE -----------------------------------------------------------
message(strrep("=", 80))
message(" D00 APP DATA TABLES")
message(strrep("=", 80))
message(sprintf(" Tables created: %s", paste(created, collapse = ", ")))
message(sprintf(" Status: %s", ifelse(success, "SUCCESS", "FAILED")))
message(strrep("=", 80))

# PART 5: DEINITIALIZE --------------------------------------------------------
if (connection_created_app && exists("app_data") && DBI::dbIsValid(app_data)) {
  DBI::dbDisconnect(app_data)
}

autodeinit()
# NO STATEMENTS AFTER THIS LINE
