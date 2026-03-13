#####
# DERIVATION: D01_08 Pre-compute RSV Classification
# GROUP: D01 (Customer DNA Analysis)
# SEQUENCE: 08
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_08_core.R
# CONSUMES: app_data.df_dna_by_customer
# PRODUCES: app_data.df_rsv_classified
# DEPENDS_ON_DRV: D01_05 (df_dna_by_customer must exist)
# PRINCIPLE: UX_P002, MP064, DEV_R038
#####

# ===========================================================================
# D01_08: Pre-compute RSV Classification
# ===========================================================================
# This derivation moves classify_rsv_and_strategy() from app runtime to
# DRV time, eliminating 10-15s render delay for rsvMatrix, marketingDecision,
# and customerExport components.
#
# PERFORMANCE IMPACT:
# - Before: 3 components each query 100k+ rows + classify in R (10-15s)
# - After: 3 components read pre-computed table (~50ms)
# ===========================================================================

# PART 1: INITIALIZE
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

# Source the core function
source("scripts/global_scripts/16_derivations/fn_D01_08_core.R")

# Source the RSV classification function (used by run_D01_08)
source("scripts/global_scripts/10_rshinyapp_components/tagpilot/fn_rsv_classification.R")

# PART 2: MAIN

# Reuse resolve_default_platforms / parse_platforms from D01_07 pattern
resolve_default_platforms <- function() {
  if (!exists("get_platform_config", mode = "function", inherits = TRUE)) {
    config_fn <- file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R")
    if (file.exists(config_fn)) {
      source(config_fn)
    }
  }

  platforms <- NULL
  if (exists("get_platform_config", mode = "function", inherits = TRUE)) {
    platforms <- tryCatch(get_platform_config(), error = function(e) NULL)
  }

  if (!is.list(platforms) || length(platforms) == 0) {
    return(c("cbz", "eby"))
  }

  platform_ids <- names(platforms)
  if (is.null(platform_ids) || !any(nzchar(platform_ids))) {
    platform_ids <- vapply(
      platforms,
      function(entry) {
        if (is.list(entry) && !is.null(entry$platform_id)) {
          return(as.character(entry$platform_id))
        }
        ""
      },
      character(1)
    )
    platform_ids <- platform_ids[nzchar(platform_ids)]
    if (length(platform_ids) == 0) {
      return(c("cbz", "eby"))
    }
    names(platforms) <- platform_ids
  }

  is_active <- function(entry) {
    if (!is.list(entry)) return(TRUE)
    status <- entry$status
    if (!is.null(status) && tolower(as.character(status)) != "active") return(FALSE)
    enabled <- entry$enabled
    if (!is.null(enabled) && !isTRUE(enabled)) return(FALSE)
    TRUE
  }

  active_platforms <- platform_ids[vapply(platforms[platform_ids], is_active, logical(1))]
  if (length(active_platforms) == 0) active_platforms <- platform_ids
  active_platforms
}

parse_platforms <- function(args, default_platforms) {
  platforms_arg <- NULL
  for (idx in seq_along(args)) {
    if (args[idx] %in% c("--platforms", "--platform") && idx < length(args)) {
      platforms_arg <- args[idx + 1]
      break
    }
    if (grepl("^--platforms=", args[idx])) {
      platforms_arg <- sub("^--platforms=", "", args[idx])
      break
    }
  }
  if (is.null(platforms_arg)) {
    env_platforms <- Sys.getenv("D01_PLATFORMS", "")
    if (nzchar(env_platforms)) {
      platforms_arg <- env_platforms
    }
  }
  if (is.null(platforms_arg) || platforms_arg == "" || platforms_arg == "all") {
    return(default_platforms)
  }
  platforms <- strsplit(platforms_arg, ",", fixed = TRUE)[[1]]
  platforms <- trimws(platforms)
  platforms <- platforms[nzchar(platforms)]
  if (length(platforms) == 0) {
    return(default_platforms)
  }
  platforms
}

config <- tryCatch(yaml::read_yaml("app_config.yaml"), error = function(e) NULL)
enabled_platforms <- parse_platforms(commandArgs(trailingOnly = TRUE), resolve_default_platforms())

message("════════════════════════════════════════════════════════════════════")
message(sprintf("D01_08 Wrapper: Starting for platforms: %s",
                paste(enabled_platforms, collapse = ", ")))
message("════════════════════════════════════════════════════════════════════")

# Execute the core function for all platforms
results <- run_D01_08_all_platforms(
  enabled_platforms = enabled_platforms,
  config = config
)

# PART 3: SUMMARIZE
success_count <- sum(vapply(results, function(x) isTRUE(x$success), logical(1)))
total_count <- length(results)

message("")
message("════════════════════════════════════════════════════════════════════")
message(sprintf("D01_08 Wrapper: Completed %d/%d platforms successfully",
                success_count, total_count))
message("════════════════════════════════════════════════════════════════════")

# Exit with non-zero status if any platform failed (pipeline orchestration depends on this)
if (success_count < total_count) {
  message(sprintf("D01_08 Wrapper: %d platform(s) failed — exiting with status 1",
                  total_count - success_count))
  autodeinit()
  quit(save = "no", status = 1)
}

# Return results for pipeline tracking
invisible(results)

# 5. AUTODEINIT
autodeinit()
