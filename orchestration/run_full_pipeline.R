#!/usr/bin/env Rscript
#' @file run_full_pipeline.R
#' @title WISER Full ETL/DRV Pipeline Execution
#' @description Executes complete ETL and DRV pipeline in correct order
#' @principle MP064 ETL-Derivation Separation
#' @author Claude
#' @date 2025-01-28

message("Starting WISER Full Pipeline")
start_time <- Sys.time()

# Phase 1: Import (0IM)
message("\n[Phase 1] Running Import scripts...")
import_files <- list.files("../ETL", pattern = "_0IM\\.R$", recursive = TRUE, full.names = TRUE)
for (file in import_files) {
  message(sprintf("  Executing: %s", file))
  source(file)
}

# Phase 2: Stage (1ST)
message("\n[Phase 2] Running Stage scripts...")
stage_files <- list.files("../ETL", pattern = "_1ST\\.R$", recursive = TRUE, full.names = TRUE)
for (file in stage_files) {
  message(sprintf("  Executing: %s", file))
  source(file)
}

# Phase 3: Transform (2TR)
message("\n[Phase 3] Running Transform scripts...")
transform_files <- list.files("../ETL", pattern = "_2TR\\.R$", recursive = TRUE, full.names = TRUE)
for (file in transform_files) {
  message(sprintf("  Executing: %s", file))
  source(file)
}

# Phase 4: Derivations
message("\n[Phase 4] Running Derivation scripts...")
drv_files <- list.files("../DRV", pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
for (file in drv_files) {
  message(sprintf("  Executing: %s", file))
  source(file)
}

end_time <- Sys.time()
duration <- difftime(end_time, start_time, units = "mins")
message(sprintf("\nPipeline completed in %.2f minutes", as.numeric(duration)))

