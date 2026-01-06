# MAMBA DRV Pipeline - Makefile
# ============================================================================
# Usage: make help
#
# Principle Compliance:
#   - DEV_R016: Evolution Over Replacement
#   - DM_R047: Multi-Platform Data Synchronization
# ============================================================================

# Variables
DRV_PATH := $(shell pwd)
RSCRIPT := $(shell which Rscript)
PLIST_TEMPLATE := scheduling/launchd_template.plist
PLIST_TARGET := $(HOME)/Library/LaunchAgents/com.mamba.drv-weekly.plist
PROJECT_ROOT := $(abspath $(DRV_PATH)/../../..)
D01_SCRIPT := $(PROJECT_ROOT)/scripts/update_scripts/DRV/all/all_D01_06.R
D01_VALIDATE_SCRIPT := $(PROJECT_ROOT)/scripts/update_scripts/DRV/all/validate_d01_outputs.R
D01_PLATFORMS ?= all
D04_VALIDATE_SCRIPT := $(PROJECT_ROOT)/scripts/global_scripts/00_principles/docs/en/part2_implementations/CH10_derivations/validate_derivation_format.R

.PHONY: help run status clean vis schedule unschedule schedule-status check-targets \
	d01 d01-cbz d01-eby d01-all d01-validate \
	d04 d04-cbz d04-eby d04-all d04-validate \
	d04-01-cbz d04-02-cbz d04-03-cbz d04-04-cbz d04-05-cbz d04-06-cbz \
	d04-02-eby d04-04-eby \
	d04-04-all d04-05-all d04-06-all d04-07-all d04-08-all d04-09-all \
	d01-00 d01-00-cbz d01-00-eby \
	d01-01 d01-01-cbz d01-01-eby \
	d01-02 d01-02-cbz d01-02-eby \
	d01-03 d01-03-cbz d01-03-eby

# Default: show help
help:
	@echo ""
	@echo "MAMBA DRV Pipeline Commands"
	@echo "============================"
	@echo ""
	@echo "Pipeline Commands:"
	@echo "  make run         Execute DRV pipeline (targets::tar_make)"
	@echo "  make status      Show pipeline progress"
	@echo "  make vis         Visualize pipeline DAG"
	@echo "  make clean       Destroy cache and re-run everything"
	@echo "  make d01         Run D01 master derivation (all platforms)"
	@echo "  make d01 D01_PLATFORMS=cbz,eby  Run D01 for selected platforms"
	@echo "  make d01-cbz     Run D01 for cbz only"
	@echo "  make d01-validate Validate D01 outputs (tables + row counts)"
	@echo "  make d04         Run D04 pipelines (cbz + eby + all)"
	@echo "  make d04-cbz     Run D04 for cbz (D04_04 -> D04_02 + optional D04_01/D04_03)"
	@echo "  make d04-eby     Run D04 for eby (D04_04 then D04_02)"
	@echo "  make d04-all     Run D04 precision (D04_07 + D04_09 + D04_08)"
	@echo "  make d04-validate Validate D04 scripts (DM_R044 format)"
	@echo ""
	@echo "D01_03 DNA Analysis (DEV_R038 pattern):"
	@echo "  make d01-00      Run D01_00 for all platforms"
	@echo "  make d01-00-cbz  Run D01_00 for CBZ only"
	@echo "  make d01-01      Run D01_01 for all platforms"
	@echo "  make d01-01-cbz  Run D01_01 for CBZ only"
	@echo "  make d01-02      Run D01_02 for all platforms"
	@echo "  make d01-02-cbz  Run D01_02 for CBZ only"
	@echo "  make d01-03      Run D01_03 for all platforms"
	@echo "  make d01-03-cbz  Run D01_03 for CBZ only"
	@echo "  make d01-03-eby  Run D01_03 for EBY only"
	@echo ""
	@echo "D04 Poisson Marketing (module steps):"
	@echo "  make d04-01-cbz  Run D04_01 Time Label Enrichment (CBZ)"
	@echo "  make d04-02-cbz  Run D04_02 Poisson Analysis (CBZ)"
	@echo "  make d04-03-cbz  Run D04_03 R120 Metadata (CBZ)"
	@echo "  make d04-04-cbz  Run D04_04 Time Series (CBZ)"
	@echo "  make d04-04-eby  Run D04_04 Time Series (EBY)"
	@echo "  make d04-02-eby  Run D04_02 Poisson Analysis (EBY)"
	@echo "  make d04-07-all  Run D04_07 Precision Time Series (ALL)"
	@echo "  make d04-09-all  Run D04_09 Feature Preparation (ALL)"
	@echo "  make d04-08-all  Run D04_08 Precision Poisson (ALL)"
	@echo ""
	@echo "Scheduling Commands (for the machine that will run scheduled jobs):"
	@echo "  make schedule         Set up weekly auto-execution"
	@echo "  make unschedule       Disable auto-execution"
	@echo "  make schedule-status  Check schedule status"
	@echo ""
	@echo "Setup:"
	@echo "  make check-targets    Verify {targets} package is installed"
	@echo ""
	@echo "Path Info:"
	@echo "  DRV_PATH: $(DRV_PATH)"
	@echo "  Rscript:  $(RSCRIPT)"
	@echo ""

# Check if targets package is installed
check-targets:
	@echo "Checking {targets} package..."
	@$(RSCRIPT) -e "if (!requireNamespace('targets', quietly = TRUE)) { \
		cat('Installing targets package...\n'); \
		install.packages('targets', repos='https://cloud.r-project.org') \
	} else { \
		cat('targets package OK:', as.character(packageVersion('targets')), '\n') \
	}"

# Execute targets pipeline
run: check-targets
	@echo ""
	@echo "Running DRV pipeline..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ""
	@$(RSCRIPT) -e "targets::tar_make()"
	@echo ""
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# Execute D01 master derivation (all platforms by default)
d01:
	@echo ""
	@echo "Running D01 master derivation..."
	@echo "Platforms: $(D01_PLATFORMS)"
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ""
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" "$(D01_SCRIPT)" --platforms="$(D01_PLATFORMS)"
	@echo ""
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-cbz:
	@$(MAKE) d01 D01_PLATFORMS=cbz

d01-eby:
	@$(MAKE) d01 D01_PLATFORMS=eby

d01-all:
	@$(MAKE) d01 D01_PLATFORMS=all

d01-validate:
	@echo ""
	@echo "Running D01 post-run validation..."
	@echo "Platforms: $(D01_PLATFORMS)"
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ""
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" "$(D01_VALIDATE_SCRIPT)" --platforms="$(D01_PLATFORMS)"
	@echo ""
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================================
# D01_00: Consume ETL Output (per-platform scripts)
# ============================================================================

d01-00-cbz:
	@echo ""
	@echo "Running D01_00 Consume ETL Output (CBZ)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D01_00.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-00-eby:
	@echo ""
	@echo "Running D01_00 Consume ETL Output (EBY)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/eby/eby_D01_00.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-00: d01-00-cbz d01-00-eby
	@echo ""
	@echo "All D01_00 Consume ETL Output completed!"

# ============================================================================
# D01_01: Customer Aggregation (per-platform scripts)
# ============================================================================

d01-01-cbz:
	@echo ""
	@echo "Running D01_01 Customer Aggregation (CBZ)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D01_01.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-01-eby:
	@echo ""
	@echo "Running D01_01 Customer Aggregation (EBY)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/eby/eby_D01_01.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-01: d01-01-cbz d01-01-eby
	@echo ""
	@echo "All D01_01 Customer Aggregation completed!"

# ============================================================================
# D01_02: RFM Calculation (per-platform scripts)
# ============================================================================

d01-02-cbz:
	@echo ""
	@echo "Running D01_02 RFM Calculation (CBZ)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D01_02.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-02-eby:
	@echo ""
	@echo "Running D01_02 RFM Calculation (EBY)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/eby/eby_D01_02.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-02: d01-02-cbz d01-02-eby
	@echo ""
	@echo "All D01_02 RFM Calculation completed!"

# ============================================================================
# D01_03: Customer DNA Analysis (per-platform scripts)
# ============================================================================

d01-03-cbz:
	@echo ""
	@echo "Running D01_03 Customer DNA Analysis (CBZ)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D01_03.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-03-eby:
	@echo ""
	@echo "Running D01_03 Customer DNA Analysis (EBY)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/eby/eby_D01_03.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d01-03: d01-03-cbz d01-03-eby
	@echo ""
	@echo "All D01_03 Customer DNA Analysis completed!"

# ============================================================================
# D04: Poisson Marketing (CBZ/EBY/ALL)
# ============================================================================

d04:
	@$(MAKE) d04-cbz
	@$(MAKE) d04-eby
	@$(MAKE) d04-all
	@echo ""
	@echo "All D04 pipelines completed!"

d04-cbz: d04-04-cbz d04-02-cbz d04-01-cbz d04-03-cbz
	@echo ""
	@echo "D04 CBZ pipeline completed!"

d04-eby: d04-04-eby d04-02-eby
	@echo ""
	@echo "D04 EBY pipeline completed!"

d04-all: d04-07-all d04-09-all d04-08-all
	@echo ""
	@echo "D04 ALL precision pipeline completed!"

d04-validate:
	@echo ""
	@echo "Validating D04 scripts against DM_R044..."
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" "$(D04_VALIDATE_SCRIPT)" scripts/update_scripts/DRV/cbz '^cbz_D04_[0-9]{2}\.R$$'
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" "$(D04_VALIDATE_SCRIPT)" scripts/update_scripts/DRV/eby '^eby_D04_[0-9]{2}\.R$$'
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" "$(D04_VALIDATE_SCRIPT)" scripts/update_scripts/DRV/all '^all_D04_[0-9]{2}\.R$$'
	@echo "D04 validation completed."

# ============================================================================
# D04_01: Time Label Enrichment (CBZ)
# ============================================================================

d04-01-cbz:
	@echo ""
	@echo "Running D04_01 Time Label Enrichment (CBZ)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D04_01.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================================
# D04_02: Poisson Analysis (CBZ/EBY)
# ============================================================================

d04-02-cbz:
	@echo ""
	@echo "Running D04_02 Poisson Analysis (CBZ)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D04_02.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d04-02-eby:
	@echo ""
	@echo "Running D04_02 Poisson Analysis (EBY)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/eby/eby_D04_02.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================================
# D04_03: R120 Metadata (CBZ)
# ============================================================================

d04-03-cbz:
	@echo ""
	@echo "Running D04_03 R120 Metadata Enrichment (CBZ)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D04_03.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================================
# D04_04: Time Series Expansion (EBY + ALL wrappers)
# ============================================================================

d04-04-cbz:
	@echo ""
	@echo "Running D04_04 Time Series Expansion (CBZ wrapper)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D04_04.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d04-04-eby:
	@echo ""
	@echo "Running D04_04 Time Series Expansion (EBY)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/eby/eby_D04_04.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d04-04-all:
	@echo ""
	@echo "Running D04_04 Time Series Expansion (ALL wrapper)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/all/all_D04_04.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================================
# D04_05: Poisson Wrappers (CBZ/ALL)
# ============================================================================

d04-05-cbz:
	@echo ""
	@echo "Running D04_05 Poisson Calculation (CBZ wrapper)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D04_05.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d04-05-all:
	@echo ""
	@echo "Running D04_05 Poisson Calculation (ALL wrapper)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/all/all_D04_05.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================================
# D04_06: App Data Publishing / Feature Prep (CBZ/ALL)
# ============================================================================

d04-06-cbz:
	@echo ""
	@echo "Running D04_06 App Data Publishing (CBZ wrapper)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/cbz/cbz_D04_06.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d04-06-all:
	@echo ""
	@echo "Running D04_06 Feature Preparation (ALL wrapper)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/all/all_D04_06.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================================
# D04_07/08/09: Precision Marketing (ALL)
# ============================================================================

d04-07-all:
	@echo ""
	@echo "Running D04_07 Precision Time Series (ALL)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/all/all_D04_07.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d04-08-all:
	@echo ""
	@echo "Running D04_08 Precision Poisson Analysis (ALL)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/all/all_D04_08.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

d04-09-all:
	@echo ""
	@echo "Running D04_09 Feature Preparation (ALL)..."
	@echo "Started: $$(date '+%Y-%m-%d %H:%M:%S')"
	@cd "$(PROJECT_ROOT)" && "$(RSCRIPT)" scripts/update_scripts/DRV/all/all_D04_09.R
	@echo "Completed: $$(date '+%Y-%m-%d %H:%M:%S')"

# Show pipeline status
status:
	@$(RSCRIPT) -e "targets::tar_progress()"

# Visualize pipeline
vis:
	@echo "Opening pipeline visualization..."
	@$(RSCRIPT) -e "targets::tar_visnetwork()"

# Clean and re-run
clean:
	@echo "Destroying cache and re-running..."
	@$(RSCRIPT) -e "targets::tar_destroy()"
	@$(MAKE) run

# Set up weekly schedule (macOS launchd)
schedule: $(PLIST_TEMPLATE)
	@echo ""
	@echo "Setting up weekly schedule..."
	@echo "  DRV Path: $(DRV_PATH)"
	@echo "  Rscript:  $(RSCRIPT)"
	@echo ""
	@sed -e "s|__DRV_PATH__|$(DRV_PATH)|g" \
	     -e "s|__RSCRIPT_PATH__|$(RSCRIPT)|g" \
	     "$(PLIST_TEMPLATE)" > "$(PLIST_TARGET)"
	@launchctl load "$(PLIST_TARGET)"
	@echo ""
	@echo "Schedule enabled!"
	@echo "  Time: Every Sunday at 2:00 AM"
	@echo "  Log:  /tmp/mamba-drv.log"
	@echo "  Err:  /tmp/mamba-drv-error.log"
	@echo ""
	@echo "Commands:"
	@echo "  make schedule-status  Check if running"
	@echo "  make unschedule       Disable"
	@echo ""

# Disable schedule
unschedule:
	@launchctl unload "$(PLIST_TARGET)" 2>/dev/null || true
	@rm -f "$(PLIST_TARGET)"
	@echo "Schedule disabled"

# Check schedule status
schedule-status:
	@echo ""
	@echo "Schedule Status:"
	@launchctl list 2>/dev/null | grep -E "mamba|PID|Label" || echo "  (not scheduled)"
	@echo ""
	@if [ -f "$(PLIST_TARGET)" ]; then \
		echo "Plist: $(PLIST_TARGET)"; \
	else \
		echo "Plist: (not installed)"; \
	fi
	@echo ""
