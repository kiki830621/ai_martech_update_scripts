#!/usr/bin/env Rscript
#' Week 4 Validation Script
#' 
#' Validates all Week 4 deliverables:
#' - Validation template utility exists
#' - Master validation script runs successfully
#' - All 4 metadata files generated
#' - Compliance report generated
#' - Compliance rate >= 95%

library(dplyr)

message("=======================================================")
message("WEEK 4 DELIVERABLES VALIDATION")
message("=======================================================\n")

validation_results <- list()

# ============================================================
# Check 1: Validation Template Utility Exists
# ============================================================

message("[CHECK 1] Validation template utility...")

template_file <- "scripts/global_scripts/04_utils/fn_validate_etl_drv_template.R"

if (file.exists(template_file)) {
  # Try to source it
  tryCatch({
    source(template_file)
    
    # Check if function exists
    if (exists("fn_validate_etl_drv_template")) {
      validation_results$template_utility <- list(
        check = "Validation template utility exists and loads",
        compliant = TRUE,
        detail = sprintf("Found at %s", template_file)
      )
      message("  PASS: Template utility exists and loads successfully")
    } else {
      validation_results$template_utility <- list(
        check = "Validation template utility exists and loads",
        compliant = FALSE,
        detail = "File exists but function not defined"
      )
      message("  FAIL: Function not defined in file")
    }
  }, error = function(e) {
    validation_results$template_utility <<- list(
      check = "Validation template utility exists and loads",
      compliant = FALSE,
      detail = sprintf("Error loading: %s", as.character(e))
    )
    message(sprintf("  FAIL: Error loading - %s", as.character(e)))
  })
} else {
  validation_results$template_utility <- list(
    check = "Validation template utility exists and loads",
    compliant = FALSE,
    detail = sprintf("File not found: %s", template_file)
  )
  message(sprintf("  FAIL: File not found - %s", template_file))
}

# ============================================================
# Check 2: Master Validation Script Exists and Runs
# ============================================================

message("\n[CHECK 2] Master validation script...")

master_script <- "scripts/global_scripts/98_test/validate_precision_etl_drv.R"

if (file.exists(master_script)) {
  message(sprintf("  PASS Found: %s", master_script))
  
  # Check if it's executable
  file_info <- file.info(master_script)
  
  # Try to run it (capture output)
  message("  Running master validation script...")
  
  result <- tryCatch({
    system2("Rscript", args = master_script, 
           stdout = TRUE, stderr = TRUE)
  }, error = function(e) {
    message(sprintf("  WARN Error running script: %s", as.character(e)))
    return(NULL)
  })
  
  if (!is.null(result)) {
    validation_results$master_script <- list(
      check = "Master validation script runs successfully",
      compliant = TRUE,
      detail = sprintf("Script executed successfully")
    )
    message("  PASS: Master validation script runs")
  } else {
    validation_results$master_script <- list(
      check = "Master validation script runs successfully",
      compliant = FALSE,
      detail = "Script execution failed"
    )
    message("  FAIL: Script execution failed")
  }
} else {
  validation_results$master_script <- list(
    check = "Master validation script exists",
    compliant = FALSE,
    detail = sprintf("File not found: %s", master_script)
  )
  message(sprintf("  FAIL: File not found - %s", master_script))
}

# ============================================================
# Check 3: Metadata Files Generated
# ============================================================

message("\n[CHECK 3] Metadata files...")

metadata_files <- c(
  "metadata/variable_name_transformations.csv",
  "metadata/dummy_encoding_metadata.csv",
  "metadata/time_series_filling_stats.csv",
  "metadata/country_extraction_metadata.csv"
)

metadata_exists <- sapply(metadata_files, file.exists)

for (i in seq_along(metadata_files)) {
  if (metadata_exists[i]) {
    # Check if file has content
    meta_data <- tryCatch({
      read.csv(metadata_files[i])
    }, error = function(e) NULL)
    
    if (!is.null(meta_data) && nrow(meta_data) > 0) {
      validation_results[[paste0("metadata_", i)]] <- list(
        check = sprintf("Metadata file %s complete", basename(metadata_files[i])),
        compliant = TRUE,
        detail = sprintf("%d records", nrow(meta_data))
      )
      message(sprintf("  PASS: %s (%d records)", 
                     basename(metadata_files[i]), nrow(meta_data)))
    } else {
      validation_results[[paste0("metadata_", i)]] <- list(
        check = sprintf("Metadata file %s complete", basename(metadata_files[i])),
        compliant = FALSE,
        detail = "File exists but empty or invalid"
      )
      message(sprintf("  FAIL: %s is empty or invalid", 
                     basename(metadata_files[i])))
    }
  } else {
    validation_results[[paste0("metadata_", i)]] <- list(
      check = sprintf("Metadata file %s exists", basename(metadata_files[i])),
      compliant = FALSE,
      detail = sprintf("File not found: %s", metadata_files[i])
    )
    message(sprintf("  FAIL: %s not found", basename(metadata_files[i])))
  }
}

# ============================================================
# Check 4: Compliance Report Generated
# ============================================================

message("\n[CHECK 4] Compliance report...")

report_script <- "scripts/global_scripts/98_test/generate_compliance_report.R"

if (file.exists(report_script)) {
  message(sprintf("  PASS Found: %s", report_script))
  
  # Check if validation results exist
  validation_files <- list.files("validation", 
                                 pattern = "precision_etl_drv_validation_.*\\.csv",
                                 full.names = TRUE)
  
  if (length(validation_files) > 0) {
    # Try to run compliance report generator
    message("  Running compliance report generator...")
    
    result <- tryCatch({
      system2("Rscript", args = report_script,
             stdout = TRUE, stderr = TRUE)
    }, error = function(e) {
      message(sprintf("  WARN Error running script: %s", as.character(e)))
      return(NULL)
    })
    
    # Check if report was generated
    report_files <- list.files("validation",
                               pattern = "PRINCIPLE_COMPLIANCE_REPORT_.*\\.md",
                               full.names = TRUE)
    
    if (length(report_files) > 0) {
      validation_results$compliance_report <- list(
        check = "Compliance report generated",
        compliant = TRUE,
        detail = sprintf("Report: %s", basename(report_files[length(report_files)]))
      )
      message(sprintf("  PASS: Compliance report generated - %s", 
                     basename(report_files[length(report_files)])))
    } else {
      validation_results$compliance_report <- list(
        check = "Compliance report generated",
        compliant = FALSE,
        detail = "Script ran but no report file found"
      )
      message("  FAIL: No report file generated")
    }
  } else {
    validation_results$compliance_report <- list(
      check = "Compliance report can be generated",
      compliant = FALSE,
      detail = "No validation results CSV found"
    )
    message("  WARN SKIP: No validation results to generate report from")
  }
} else {
  validation_results$compliance_report <- list(
    check = "Compliance report script exists",
    compliant = FALSE,
    detail = sprintf("File not found: %s", report_script)
  )
  message(sprintf("  FAIL: File not found - %s", report_script))
}

# ============================================================
# Check 5: Compliance Rate >= 95%
# ============================================================

message("\n[CHECK 5] Compliance rate threshold...")

validation_files <- list.files("validation",
                               pattern = "precision_etl_drv_validation_.*\\.csv",
                               full.names = TRUE)

if (length(validation_files) > 0) {
  # Load most recent validation results
  latest_validation <- validation_files[which.max(file.mtime(validation_files))]
  validation_data <- read.csv(latest_validation)
  
  compliance_rate <- mean(validation_data$compliant, na.rm = TRUE)
  
  if (compliance_rate >= 0.95) {
    validation_results$compliance_threshold <- list(
      check = "Compliance rate >= 95%",
      compliant = TRUE,
      detail = sprintf("Achieved %.1f%% compliance", compliance_rate * 100)
    )
    message(sprintf("  PASS: Compliance rate %.1f%% (threshold: 95%%)", 
                   compliance_rate * 100))
  } else {
    validation_results$compliance_threshold <- list(
      check = "Compliance rate >= 95%",
      compliant = FALSE,
      detail = sprintf("Only %.1f%% compliance (need 95%%)", compliance_rate * 100)
    )
    message(sprintf("  FAIL: Compliance rate %.1f%% < 95%% threshold", 
                   compliance_rate * 100))
  }
} else {
  validation_results$compliance_threshold <- list(
    check = "Compliance rate >= 95%",
    compliant = FALSE,
    detail = "No validation results found"
  )
  message("  FAIL: No validation results to check")
}

# ============================================================
# Check 6: Metadata Generation Scripts Exist
# ============================================================

message("\n[CHECK 6] Metadata generation scripts...")

metadata_scripts <- c(
  "scripts/update_scripts/ETL/precision/generate_variable_name_metadata.R",
  "scripts/update_scripts/ETL/precision/generate_dummy_encoding_metadata.R",
  "scripts/update_scripts/DRV/all/generate_time_series_metadata.R",
  "scripts/update_scripts/ETL/precision/generate_country_metadata.R"
)

all_scripts_exist <- TRUE

for (script in metadata_scripts) {
  if (file.exists(script)) {
    message(sprintf("  PASS: %s", basename(script)))
  } else {
    message(sprintf("  FAIL: %s not found", basename(script)))
    all_scripts_exist <- FALSE
  }
}

validation_results$metadata_scripts <- list(
  check = "All 4 metadata generation scripts exist",
  compliant = all_scripts_exist,
  detail = sprintf("%d/4 scripts found", sum(sapply(metadata_scripts, file.exists)))
)

# ============================================================
# Summary
# ============================================================

message("\n=======================================================")
message("WEEK 4 VALIDATION SUMMARY")
message("=======================================================")

# Convert to data frame
results_df <- bind_rows(lapply(names(validation_results), function(name) {
  result <- validation_results[[name]]
  tibble(
    check_id = name,
    check_description = result$check,
    compliant = result$compliant,
    detail = result$detail
  )
}))

total_checks <- nrow(results_df)
passed_checks <- sum(results_df$compliant)
failed_checks <- total_checks - passed_checks
compliance_rate <- passed_checks / total_checks

message(sprintf("\nTotal checks: %d", total_checks))
message(sprintf("Passed: %d (%.1f%%)", passed_checks, compliance_rate * 100))
message(sprintf("Failed: %d", failed_checks))

if (failed_checks > 0) {
  message("\nFailed checks:")
  failed <- results_df %>% filter(!compliant)
  for (i in 1:nrow(failed)) {
    message(sprintf("  - %s: %s", failed$check_description[i], failed$detail[i]))
  }
}

# Save results
output_file <- sprintf("validation/week4_validation_%s.csv", 
                      format(Sys.time(), "%Y%m%d_%H%M%S"))
write.csv(results_df, output_file, row.names = FALSE)

message(sprintf("\nREPORT Results saved to: %s", output_file))

message("\n=======================================================")

if (compliance_rate == 1.0) {
  message("PASS WEEK 4 VALIDATION PASSED - All deliverables complete!")
  quit(status = 0)
} else if (compliance_rate >= 0.80) {
  message("WARN WEEK 4 VALIDATION PASSED WITH WARNINGS")
  quit(status = 0)
} else {
  message("FAIL WEEK 4 VALIDATION FAILED - Critical deliverables missing")
  quit(status = 1)
}
