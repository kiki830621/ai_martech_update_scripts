#!/bin/bash
# run_full_validation.sh
# MAMBA Precision Marketing Week 4 validation + metadata generation

set -euo pipefail

echo "=== Week 4 Validation (Metadata + Compliance) ==="
echo "Start: $(date)"
echo ""

# Metadata generation
Rscript scripts/update_scripts/ETL/precision/generate_variable_name_metadata.R
Rscript scripts/update_scripts/ETL/precision/generate_dummy_encoding_metadata.R
Rscript scripts/update_scripts/DRV/all/generate_time_series_metadata.R
Rscript scripts/update_scripts/ETL/precision/generate_country_metadata.R

# Validation + compliance report
Rscript scripts/global_scripts/98_test/validate_precision_etl_drv.R
Rscript scripts/global_scripts/98_test/generate_compliance_report.R
Rscript scripts/update_scripts/DRV/all/validate_week4.R

echo ""
echo "=== Week 4 Validation Complete ==="
echo "End: $(date)"
