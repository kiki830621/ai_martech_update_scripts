# Makefile
# MAMBA Pipeline Orchestration CLI
# Per MP140: Pipeline Orchestration Principle
# Per MP141: Scheduled Execution Pattern
# Per MP142: Configuration-Driven Pipeline
#
# Usage:
#   make run                      # Run full pipeline (all platforms)
#   make run PLATFORM=cbz         # Run specific platform only
#   make run TARGET=cbz_D04_02    # Run specific target with dependencies
#   make run LAYER=etl            # Run ETL layer only
#   make status                   # Check pipeline status
#   make vis                      # Visualize dependency graph
#   make clean                    # Destroy cache and rebuild
#   make schedule                 # Set up scheduled execution (macOS)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Default values
PLATFORM ?= all
TARGET ?=
LAYER ?= both

# DM_R069 Deploy Data Sync Discipline: `make run` conditionally chains Supabase
# upload (mtime drift threshold). Override with `make run-no-upload` (sets =0).
UPLOAD_CHAIN ?= 1

# R command
R := Rscript

# Directories (Per SO_P016: Config at project root, scripts in subrepo)
# NOTE: PIPELINE_DIR uses $(CURDIR) (physical path, resolves symlinks) for
# local file operations. PROJECT_ROOT uses $$PWD (logical path, preserves
# symlinks) so that ../../ navigates to the company project directory, not
# the superproject. See #364 for details.
PIPELINE_DIR ?= $(CURDIR)
PROJECT_ROOT ?= $(if $(MAMBA_PROJECT_ROOT),$(MAMBA_PROJECT_ROOT),$(shell cd "$$PWD/../.." && pwd))
GLOBAL_SCRIPTS ?= $(shell cd "$(PIPELINE_DIR)/../global_scripts" && pwd)
CONFIG_PATH ?= $(PROJECT_ROOT)/_targets_config.yaml
BASE_TEMPLATE ?= $(GLOBAL_SCRIPTS)/21_rshinyapp_templates/config/_targets_config.base.yaml
APP_CONFIG ?= $(PROJECT_ROOT)/app_config.yaml
LOGS_DIR ?= $(PIPELINE_DIR)/logs
STORE_DIR ?= $(PROJECT_ROOT)/_targets
TARGET_SCRIPT ?= $(PIPELINE_DIR)/_targets.R

# Timestamp for logs
TIMESTAMP := $(shell date +%Y%m%d_%H%M%S)

# =============================================================================
# MAIN TARGETS
# =============================================================================

.PHONY: help run run-dry status vis clean config-merge config-scan config-full config-validate schedule unschedule schedule-status logs e2e e2e-filter e2e-login e2e-vitalsigns upload run-no-upload _maybe-upload _stale-warn verify-partial-month

help:
	@echo "MAMBA Pipeline Orchestration"
	@echo "============================"
	@echo ""
	@echo "Run Commands:"
	@echo "  make run                     Run full pipeline (all platforms)"
	@echo "  make run PLATFORM=cbz        Run specific platform only"
	@echo "  make run TARGET=cbz_D04_02   Run specific target with dependencies"
	@echo "  make run LAYER=etl           Run ETL layer only (etl|drv|both)"
	@echo "  make run-dry                 Show what would run (no execution)"
	@echo ""
	@echo "Status Commands:"
	@echo "  make status                  Check pipeline status"
	@echo "  make vis                     Visualize dependency graph (opens browser)"
	@echo "  make logs                    Show recent log files"
	@echo ""
	@echo "Configuration Commands:"
	@echo "  make config-merge            Merge base template + app_config overrides"
	@echo "  make config-scan             Scan ETL/DRV and update script lists"
	@echo "  make config-full             Full config: merge + scan + validate"
	@echo "  make config-validate         Validate _targets_config.yaml"
	@echo ""
	@echo "Maintenance Commands:"
	@echo "  make clean                   Destroy cache and rebuild"
	@echo "  make clean-logs              Remove old log files (>30 days)"
	@echo ""
	@echo "Scheduling Commands (macOS):"
	@echo "  make schedule                Set up weekly auto-execution"
	@echo "  make unschedule              Disable auto-execution"
	@echo "  make schedule-status         Check scheduling status"
	@echo ""
	@echo "E2E Test Commands:"
	@echo "  make e2e                     Run full E2E suite"
	@echo "  make e2e-filter FILTER=login Run matching E2E tests"
	@echo "  make e2e-login               Run login E2E tests"
	@echo "  make e2e-vitalsigns          Run VitalSigns E2E tests"
	@echo ""
	@echo "Examples:"
	@echo "  make run PLATFORM=cbz TARGET=cbz_D04_02"
	@echo "  make run LAYER=etl PLATFORM=amz"
	@echo "  make e2e-login"
	@echo "  make e2e-vitalsigns"

# =============================================================================
# RUN COMMANDS
# =============================================================================

run:
	@echo "═══════════════════════════════════════════════════════════════════"
	@echo "MAMBA Pipeline Execution"
	@echo "Platform: $(PLATFORM) | Layer: $(LAYER) | Target: $(TARGET) | FORCE: $(FORCE)"
	@echo "Started: $$(date)"
	@echo "═══════════════════════════════════════════════════════════════════"
	@mkdir -p $(LOGS_DIR)
	MAMBA_PROJECT_ROOT=$(PROJECT_ROOT) \
	MAMBA_PLATFORM=$(PLATFORM) \
	MAMBA_LAYER=$(LAYER) \
	MAMBA_TARGET=$(TARGET) \
	FORCE=$(FORCE) \
	$(R) $(PIPELINE_DIR)/orchestration/fn_output_presence.R \
		"$(PROJECT_ROOT)" "$(STORE_DIR)" "$(TARGET_SCRIPT)" 2>&1 \
		| tee $(LOGS_DIR)/run_$(TIMESTAMP).log
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════════"
	@echo "Completed: $$(date)"
	@echo "Log: $(LOGS_DIR)/run_$(TIMESTAMP).log"
	@echo "═══════════════════════════════════════════════════════════════════"
	@if [ "$(UPLOAD_CHAIN)" = "1" ]; then $(MAKE) --no-print-directory _maybe-upload; else $(MAKE) --no-print-directory _stale-warn; fi

run-dry:
	@echo "Dry run - showing what would execute:"
	@echo "Platform: $(PLATFORM) | Layer: $(LAYER) | Target: $(TARGET)"
	@echo ""
	MAMBA_PROJECT_ROOT=$(PROJECT_ROOT) \
	MAMBA_PLATFORM=$(PLATFORM) \
	MAMBA_LAYER=$(LAYER) \
	MAMBA_TARGET=$(TARGET) \
	$(R) -e "print(utils::head(targets::tar_manifest(script = '$(TARGET_SCRIPT)', callr_function = NULL), 50))"

# Platform shortcuts
run-cbz:
	$(MAKE) run PLATFORM=cbz

run-amz:
	$(MAKE) run PLATFORM=amz

run-eby:
	$(MAKE) run PLATFORM=eby

run-all-platforms:
	$(MAKE) run PLATFORM=all

# Layer shortcuts
run-etl:
	$(MAKE) run LAYER=etl

run-drv:
	$(MAKE) run LAYER=drv

# =============================================================================
# SUPABASE UPLOAD (DM_R069 Deploy Data Sync Discipline)
# =============================================================================

# Upload local app_data DuckDB to Supabase (production backend per DM_R063).
upload:
	@echo "═══════════════════════════════════════════════════════════════════"
	@echo "Supabase Upload (DM_R069)"
	@echo "═══════════════════════════════════════════════════════════════════"
	@cd "$(PROJECT_ROOT)" && $(R) "$(GLOBAL_SCRIPTS)/23_deployment/03_deploy/upload_app_data_to_supabase.R"

# Internal: conditionally invoke upload based on mtime drift threshold.
# should_upload() exit convention: status 10 = upload needed, 0 = up-to-date.
_maybe-upload:
	@cd "$(PROJECT_ROOT)" && $(R) -e 'source("$(GLOBAL_SCRIPTS)/23_deployment/03_deploy/fn_should_upload.R"); quit(status = if (isTRUE(should_upload(project_root = "."))) 10L else 0L)' ; \
	rc=$$? ; \
	if [ $$rc -eq 10 ]; then \
		echo "→ Supabase upload needed (DuckDB mtime > last upload)" ; \
		$(MAKE) --no-print-directory upload ; \
	elif [ $$rc -eq 0 ]; then \
		echo "✓ Supabase already up-to-date (skipping upload)" ; \
	else \
		echo "⚠ should_upload check errored (rc=$$rc); skipping auto-upload — run 'make upload' manually if needed" ; \
	fi

# Internal: warn (to stderr) if stale but do NOT upload (used by run-no-upload).
_stale-warn:
	@cd "$(PROJECT_ROOT)" && $(R) -e 'source("$(GLOBAL_SCRIPTS)/23_deployment/03_deploy/fn_should_upload.R"); if (isTRUE(should_upload(project_root = "."))) message("\342\232\240 Supabase stale (DuckDB updated since last upload). Run \47make upload\47 before deploying.")' 1>&2 || true

# Run the pipeline WITHOUT chaining Supabase upload (dev iteration escape hatch).
# Still warns to stderr if Supabase is stale (never fully silent, per DM_R069).
run-no-upload:
	@$(MAKE) --no-print-directory run UPLOAD_CHAIN=0

# MP165 v1.3 (#668) DRV-Layer Step 2 propagation gate.
# Runs `drv_output_shape_gate.R` against the per-company contract yaml
# WITHOUT running upstream derivations — fast re-verify after manual fix.
# Calendar default: warn-mode pre-2026-06-13, strict-mode after.
# Override via `make verify-drv DRV_GATE_MODE=strict` (or warn).
verify-drv:
	@COMPANY=$$(basename $(PROJECT_ROOT)); \
	YAML_NAME=$$(echo $$COMPANY | tr '[:upper:]' '[:lower:]'); \
	GATE=$(PIPELINE_DIR)/../global_scripts/23_deployment/drv_output_shape_gate.R; \
	YAML=$(PIPELINE_DIR)/../global_scripts/98_test/e2e/contracts/$${YAML_NAME}_drv.yaml; \
	DB=$(PROJECT_ROOT)/data/app_data/app_data.duckdb; \
	MODE_FLAG=$${DRV_GATE_MODE:+--$${DRV_GATE_MODE}-mode}; \
	echo "═══ MP165 v1.3 DRV Output Shape Gate ($$COMPANY) ═══"; \
	Rscript $$GATE --company $$COMPANY --contracts-path $$YAML --db-path $$DB $$MODE_FLAG

# Partial-month reader lint (#910). Static scan -- no DB, no app.
# Enforces that every df_macro_monthly_summary reader filters is_partial_month
# (defensive infra for the #811 regression class). On-demand / CI only; NOT
# chained into `make run` (source lint, orthogonal to the data pipeline).
verify-partial-month:
	@echo "=== Partial-month reader lint (#910) ==="; \
	Rscript $(PIPELINE_DIR)/../global_scripts/23_deployment/check_partial_month_filter.R

# =============================================================================
# STATUS COMMANDS
# =============================================================================

status:
	@echo "Pipeline Status"
	@echo "==============="
	@$(R) -e "targets::tar_progress(store = '$(STORE_DIR)')" 2>/dev/null || echo "No pipeline state found. Run 'make run' first."

vis:
	@echo "Opening dependency visualization..."
	@$(R) -e "targets::tar_visnetwork(script = '$(TARGET_SCRIPT)', store = '$(STORE_DIR)')" 2>/dev/null || echo "No pipeline state found. Run 'make run' first."

logs:
	@echo "Recent log files:"
	@ls -lt $(LOGS_DIR)/*.log 2>/dev/null | head -10 || echo "No logs found."

# =============================================================================
# CONFIGURATION COMMANDS
# Per SO_P016: Configuration Scope Hierarchy
# - Base template: global_scripts/21_rshinyapp_templates/config/_targets_config.base.yaml (Universal)
# - Company override: app_config.yaml pipeline: section (Company)
# - Generated output: {project_root}/_targets_config.yaml (Company)
# =============================================================================

config-merge:
	@echo "Merging pipeline configuration..."
	@echo "  Base template: $(BASE_TEMPLATE)"
	@echo "  Company config: $(APP_CONFIG)"
	@echo "  Output: $(CONFIG_PATH)"
	@$(R) -e "\
	source('$(GLOBAL_SCRIPTS)/04_utils/fn_merge_pipeline_config.R'); \
	merge_pipeline_config( \
	  base_path = '$(BASE_TEMPLATE)', \
	  app_config_path = '$(APP_CONFIG)', \
	  output_path = '$(CONFIG_PATH)', \
	  scan_scripts = FALSE \
	)"

config-scan:
	@echo "Scanning ETL/DRV directories and updating script lists..."
	@$(R) -e "\
	source('$(GLOBAL_SCRIPTS)/04_utils/fn_merge_pipeline_config.R'); \
	config <- yaml::read_yaml('$(CONFIG_PATH)'); \
	app_config <- yaml::read_yaml('$(APP_CONFIG)'); \
	config <- scan_and_update_scripts( \
	  config, \
	  etl_dir = '$(PIPELINE_DIR)/ETL', \
	  drv_dir = '$(PIPELINE_DIR)/DRV', \
	  verbose = TRUE, \
	  current_company = app_config\$$brand_name \
	); \
	yaml::write_yaml(config, '$(CONFIG_PATH)'); \
	cat('✓ Script lists updated\n')"

config-full:
	@echo "Full configuration: merge + scan + validate"
	$(MAKE) config-merge
	$(MAKE) config-scan
	$(MAKE) config-validate

config-validate:
	@echo "Validating $(CONFIG_PATH)..."
	@$(R) -e "\
	library(yaml); \
	config <- yaml::read_yaml('$(CONFIG_PATH)'); \
	cat('✓ YAML syntax valid\n'); \
	cat('✓ Platforms:', paste(names(config\$$platforms), collapse=', '), '\n'); \
	cat('✓ Phase order:', paste(config\$$phase_order, collapse=' → '), '\n'); \
	"

# =============================================================================
# MAINTENANCE COMMANDS
# =============================================================================

clean:
	@echo "Destroying pipeline cache..."
	@$(R) -e "targets::tar_destroy(store = '$(STORE_DIR)')" 2>/dev/null || true
	@rm -rf $(STORE_DIR)
	@echo "Cache destroyed. Next 'make run' will rebuild from scratch."

clean-logs:
	@echo "Removing log files older than 30 days..."
	@find $(LOGS_DIR) -name "*.log" -mtime +30 -delete 2>/dev/null || true
	@echo "Done."

# =============================================================================
# SCHEDULING COMMANDS (macOS launchd)
# =============================================================================

PLIST_NAME := com.mamba.pipeline
PLIST_PATH := ~/Library/LaunchAgents/$(PLIST_NAME).plist

schedule:
	@echo "Setting up scheduled execution..."
	@mkdir -p ~/Library/LaunchAgents
	@echo '<?xml version="1.0" encoding="UTF-8"?>' > $(PLIST_PATH)
	@echo '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">' >> $(PLIST_PATH)
	@echo '<plist version="1.0">' >> $(PLIST_PATH)
	@echo '<dict>' >> $(PLIST_PATH)
	@echo '    <key>Label</key>' >> $(PLIST_PATH)
	@echo '    <string>$(PLIST_NAME)</string>' >> $(PLIST_PATH)
	@echo '    <key>ProgramArguments</key>' >> $(PLIST_PATH)
	@echo '    <array>' >> $(PLIST_PATH)
	@echo '        <string>/usr/bin/make</string>' >> $(PLIST_PATH)
	@echo '        <string>-C</string>' >> $(PLIST_PATH)
	@echo '        <string>$(PIPELINE_DIR)</string>' >> $(PLIST_PATH)
	@echo '        <string>run</string>' >> $(PLIST_PATH)
	@echo '    </array>' >> $(PLIST_PATH)
	@echo '    <key>StartCalendarInterval</key>' >> $(PLIST_PATH)
	@echo '    <dict>' >> $(PLIST_PATH)
	@echo '        <key>Weekday</key>' >> $(PLIST_PATH)
	@echo '        <integer>0</integer>' >> $(PLIST_PATH)
	@echo '        <key>Hour</key>' >> $(PLIST_PATH)
	@echo '        <integer>2</integer>' >> $(PLIST_PATH)
	@echo '        <key>Minute</key>' >> $(PLIST_PATH)
	@echo '        <integer>0</integer>' >> $(PLIST_PATH)
	@echo '    </dict>' >> $(PLIST_PATH)
	@echo '    <key>StandardOutPath</key>' >> $(PLIST_PATH)
	@echo '    <string>$(LOGS_DIR)/scheduled_stdout.log</string>' >> $(PLIST_PATH)
	@echo '    <key>StandardErrorPath</key>' >> $(PLIST_PATH)
	@echo '    <string>$(LOGS_DIR)/scheduled_stderr.log</string>' >> $(PLIST_PATH)
	@echo '    <key>WorkingDirectory</key>' >> $(PLIST_PATH)
	@echo '    <string>$(PIPELINE_DIR)</string>' >> $(PLIST_PATH)
	@echo '</dict>' >> $(PLIST_PATH)
	@echo '</plist>' >> $(PLIST_PATH)
	@launchctl load $(PLIST_PATH)
	@echo "✓ Scheduled: Weekly on Sunday at 2:00 AM"
	@echo "  Plist: $(PLIST_PATH)"

unschedule:
	@echo "Disabling scheduled execution..."
	@launchctl unload $(PLIST_PATH) 2>/dev/null || true
	@rm -f $(PLIST_PATH)
	@echo "✓ Unscheduled"

schedule-status:
	@echo "Checking schedule status..."
	@launchctl list | grep $(PLIST_NAME) && echo "✓ Scheduled" || echo "✗ Not scheduled"

# =============================================================================
# E2E TESTING (TD_R007)
# =============================================================================

e2e:
	@if [ ! -d "$(PROJECT_ROOT)/scripts/global_scripts/98_test/e2e" ]; then \
		echo "✗ E2E test directory not found at $(PROJECT_ROOT)/scripts/global_scripts/98_test/e2e"; \
		echo "  Run from a company project root: cd D_RACING && make e2e"; \
		exit 1; \
	fi
	@echo "Running E2E tests from $(PROJECT_ROOT)..."
	@cd $(PROJECT_ROOT) && \
		NOT_CRAN=true \
		E2E_PROJECT_ROOT="$(PROJECT_ROOT)" \
		$(R) -e "testthat::test_dir('scripts/global_scripts/98_test/e2e', reporter='summary')"

e2e-filter:
	@if [ ! -d "$(PROJECT_ROOT)/scripts/global_scripts/98_test/e2e" ]; then \
		echo "✗ E2E test directory not found at $(PROJECT_ROOT)/scripts/global_scripts/98_test/e2e"; \
		echo "  Run from a company project root: cd D_RACING && make e2e"; \
		exit 1; \
	fi
	@echo "Running E2E tests matching filter='$(FILTER)'..."
	@cd $(PROJECT_ROOT) && \
		NOT_CRAN=true \
		E2E_PROJECT_ROOT="$(PROJECT_ROOT)" \
		$(R) -e "testthat::test_dir('scripts/global_scripts/98_test/e2e', filter='$(FILTER)', reporter='summary')"

e2e-login:
	@$(MAKE) e2e-filter FILTER=login

e2e-vitalsigns:
	@$(MAKE) e2e-filter FILTER=vitalsigns
