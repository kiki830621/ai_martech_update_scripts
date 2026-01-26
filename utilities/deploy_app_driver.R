#' Deploy Driver: Supabase Upload + Deployment Prep
#'
#' @description
#' Orchestrates the deployment flow in a clear, repeatable order:
#' 1) Upload DuckDB app_data to Supabase
#' 2) Update manifest.json via sc_deployment_config.R (non-interactive)
#' 3) Print git commands for commit/push
#'
#' Usage:
#'   Rscript scripts/update_scripts/utilities/deploy_app_driver.R
#'   Rscript scripts/update_scripts/utilities/deploy_app_driver.R --skip-upload
#'   Rscript scripts/update_scripts/utilities/deploy_app_driver.R --skip-manifest
#'
#' @author MAMBA Team
#' @date 2026-01-25

cat("=== Deployment Driver (Supabase -> Deploy) ===\n\n")
cat("Start time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

args <- commandArgs(trailingOnly = TRUE)
skip_upload <- "--skip-upload" %in% args
skip_manifest <- "--skip-manifest" %in% args

require_env <- function(name) {
  value <- Sys.getenv(name, "")
  if (value == "") {
    stop("Missing required environment variable: ", name)
  }
  invisible(value)
}

run_step <- function(label, fn) {
  cat("\n--- ", label, " ---\n", sep = "")
  fn()
}

# Step 1: Upload app_data to Supabase
if (!skip_upload) {
  run_step("Step 1: Upload app_data to Supabase", function() {
    require_env("SUPABASE_DB_HOST")
    require_env("SUPABASE_DB_PASSWORD")

    duckdb_path <- "data/app_data/app_data.duckdb"
    if (!file.exists(duckdb_path)) {
      stop("DuckDB file not found: ", duckdb_path)
    }

    exit_code <- system2(
      "Rscript",
      c("scripts/update_scripts/utilities/upload_app_data_to_supabase.R"),
      stdout = "",
      stderr = ""
    )

    if (!is.numeric(exit_code) || exit_code != 0) {
      stop("Supabase upload failed (exit code: ", exit_code, ")")
    }

    cat("✓ Supabase upload completed\n")
  })
} else {
  cat("Skipping upload step (--skip-upload)\n")
}

# Step 2: Update manifest and deployment checks
if (!skip_manifest) {
  run_step("Step 2: Update manifest via sc_deployment_config.R", function() {
    source("scripts/global_scripts/23_deployment/sc_deployment_config.R")
    ok <- deploy_with_config(interactive = FALSE)
    if (isFALSE(ok)) {
      stop("Deployment config check failed")
    }
    cat("✓ Deployment prep completed\n")
  })
} else {
  cat("Skipping manifest update (--skip-manifest)\n")
}

# Step 3: Git instructions (manual by default)
run_step("Step 3: Git commit/push (manual)", function() {
  cat("Next steps (run manually):\n")
  cat("  git status -sb\n")
  cat("  git add -A\n")
  cat("  git commit -m \"[DEPLOY] Supabase sync + manifest update\"\n")
  cat("  git push\n")
})

cat("\nEnd time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("✓ Driver complete\n")
