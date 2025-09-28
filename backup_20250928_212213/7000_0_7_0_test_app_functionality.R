# Test App Functionality
# This script follows R08_update_script_naming.md convention:
# 7000_0_7_0_test_app_functionality.R
# - 70: Testing bundle group
# - 00: First script in the bundle
# - 0: Main script (not a sub-script)
# - 7_0: Connected to module 7.0 (Testing module)
# - test_app_functionality: Describes the purpose

# Initialize the environment
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))

# Source the testing module
script_paths <- list(
  verify_config = file.path("modules", "M70_testing", "M70_fn_verify_config.R"),
  test_app = file.path("modules", "M70_testing", "M70_fn_test_app.R")
)

# Check if the necessary files exist
for (path_name in names(script_paths)) {
  path <- script_paths[[path_name]]
  if (!file.exists(path)) {
    stop(paste("Required file not found:", path))
  }
  source(path)
}

# Configuration path
config_path <- "app_config.yaml"

# Set verbose output
VERBOSE <- TRUE

# Function to log messages
log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(paste0("[", level, " ", timestamp, "] ", msg, "\n"))
}

# Record test execution
log_file <- file.path("update_scripts", "logs", paste0("app_test_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log"))
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

# Verify configuration
log_msg("Starting configuration verification...")
verify_results <- M70_verify_config(config_path, verbose = VERBOSE)

# Write verification results to log
sink(log_file, append = TRUE)
cat("CONFIGURATION VERIFICATION RESULTS:\n")
cat("Success:", verify_results$success, "\n")
if (length(verify_results$errors) > 0) {
  cat("Errors:\n")
  for (error_name in names(verify_results$errors)) {
    cat("- ", error_name, ": ", verify_results$errors[[error_name]], "\n", sep = "")
  }
}
if (length(verify_results$warnings) > 0) {
  cat("Warnings:\n")
  for (warning_name in names(verify_results$warnings)) {
    cat("- ", warning_name, ": ", verify_results$warnings[[warning_name]], "\n", sep = "")
  }
}
cat("\n")
sink()

# If verification succeeded, run the app test
if (verify_results$success) {
  log_msg("Configuration is valid. Starting app test...")
  test_results <- M70_test_app(config_path, test_mode = "basic", timeout = 30)
  
  # Write test results to log
  sink(log_file, append = TRUE)
  cat("APP TEST RESULTS:\n")
  cat("Success:", test_results$success, "\n")
  if (length(test_results$errors) > 0) {
    cat("Errors:\n")
    for (error_name in names(test_results$errors)) {
      cat("- ", error_name, ": ", test_results$errors[[error_name]], "\n", sep = "")
    }
  }
  if (length(test_results$warnings) > 0) {
    cat("Warnings:\n")
    for (warning_name in names(test_results$warnings)) {
      cat("- ", warning_name, ": ", test_results$warnings[[warning_name]], "\n", sep = "")
    }
  }
  cat("\n")
  sink()
  
  if (test_results$success) {
    log_msg("App test completed successfully!", "SUCCESS")
    log_msg(paste("Test report saved to:", test_results$info$report_path))
  } else {
    log_msg("App test failed!", "ERROR")
    if (length(test_results$errors) > 0) {
      for (error_name in names(test_results$errors)) {
        log_msg(paste0(error_name, ": ", test_results$errors[[error_name]]), "ERROR")
      }
    }
    log_msg(paste("Test report saved to:", test_results$info$report_path))
  }
} else {
  log_msg("Configuration verification failed. Not running app test.", "ERROR")
  log_msg(paste("Verification report saved to:", verify_results$info$report_path))
}

log_msg("Test execution complete.")

# Clean up
source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))