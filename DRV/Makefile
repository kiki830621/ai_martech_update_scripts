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

.PHONY: help run status clean vis schedule unschedule schedule-status check-targets

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
