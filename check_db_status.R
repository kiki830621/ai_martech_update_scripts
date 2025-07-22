# Check database status
message("Initializing environment...")
VERBOSE_INITIALIZATION <- TRUE

# Source initialization script
source(file.path("../../../../global_scripts", "00_principles", "000g_initialization_update_mode.R"))

# Try to connect to databases
tryCatch({
  message("Attempting to connect to raw_data...")
  raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
  message("Raw data tables:")
  print(dbListTables(raw_data))
}, error = function(e) {
  message("Error connecting to raw_data: ", e$message)
})

tryCatch({
  message("Attempting to connect to cleansed_data...")
  cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = TRUE)
  message("Cleansed data tables:")
  print(dbListTables(cleansed_data))
}, error = function(e) {
  message("Error connecting to cleansed_data: ", e$message)
})

tryCatch({
  message("Attempting to connect to processed_data...")
  processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = TRUE)
  message("Processed data tables:")
  print(dbListTables(processed_data))
}, error = function(e) {
  message("Error connecting to processed_data: ", e$message)
})

# Clean up
dbDisconnect_all()
message("Done.")