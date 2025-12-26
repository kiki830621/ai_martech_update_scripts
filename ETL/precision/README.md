# Precision Marketing ETL Pipeline

## Overview

This directory contains the complete ETL pipeline for MAMBA Precision Marketing product profile data. The pipeline implements a principle-driven, three-stage architecture designed for multi-product line analysis.

**Implementation Status**: Week 1 Complete (2025-11-12)
**Product Lines**: 6 (alf, irf, pre, rek, tur, wak)
**Data Source**: Google Sheets (public)

## Architecture

### Principle Compliance

This ETL pipeline strictly follows MAMBA principles:

- **MP108**: Base ETL Pipeline Separation (0IM → 1ST → 2TR)
- **R116**: Currency Standardization in ETL 1ST Stage
- **MP102**: Completeness and Standardization
- **MP029**: No Fake Data Principle (real exchange rates)
- **MP064**: ETL-Derivation Separation
- **R078**: Column Naming for Operations

### Three-Stage Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    PRECISION MARKETING ETL                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  0IM (Import)                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ • Read from Google Sheets (6 product lines)              │  │
│  │ • Import AS-IS (no transformations per MP108)            │  │
│  │ • Add metadata: import_timestamp, product_line_id        │  │
│  │ • Output: raw_data.duckdb                                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            ↓                                    │
│  1ST (Standardization) ⭐ R116 CURRENCY CONVERSION              │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ • Standardize variable names (snake_case)                │  │
│  │ • Convert currencies to USD (R116)                       │  │
│  │ • Preserve original values + metadata                    │  │
│  │ • Standardize data types                                 │  │
│  │ • Extract country dimension                              │  │
│  │ • Output: staged_data.duckdb                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            ↓                                    │
│  2TR (Transformation)                                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ • Price segmentation (low/medium/high)                   │  │
│  │ • Rating categorization (poor/fair/good/excellent)       │  │
│  │ • Quality score calculation                              │  │
│  │ • Competitiveness indicators                             │  │
│  │ • Feature engineering                                    │  │
│  │ • Output: transformed_data.duckdb                        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## File Structure

```
precision/
├── README.md                                # This file
├── precision_ETL_product_profiles_0IM.R    # Import stage
├── precision_ETL_product_profiles_1ST.R    # Standardization stage (R116)
├── precision_ETL_product_profiles_2TR.R    # Transformation stage
└── validate_week1.R                         # Validation script
```

## Data Flow

### Input Data

**Source**: Google Sheets
**Sheet ID**: `1aKyyOMpIJtDtpqe7Iz0AfSU0W9aAdpSdPDD1zgnqO30`
**Product Lines**: alf, irf, pre, rek, tur, wak

Expected columns (minimum):
- `product_id`: Unique product identifier
- `product_brand`: Brand name
- `product_title`: Product title/name
- `price`: Price value
- `currency`: ISO 4217 currency code (USD, EUR, TWD, etc.)
- `rating`: Product rating (0-5 scale)
- `review_count`: Number of reviews

### Output Databases

1. **raw_data.duckdb** (0IM output)
   - Tables: `raw_precision_alf`, `raw_precision_irf`, etc.
   - Schema: Original columns + metadata (import_timestamp, product_line_id, data_source)

2. **staged_data.duckdb** (1ST output)
   - Tables: `staged_precision_alf`, `staged_precision_irf`, etc.
   - Schema: Standardized columns + R116 currency fields
   - Key additions:
     - `price_usd`: Standardized USD price (R116)
     - `original_price`: Original price value
     - `original_currency`: Original currency code
     - `conversion_rate`: Exchange rate used
     - `conversion_date`: Date of conversion
     - `country`: Extracted from currency code

3. **transformed_data.duckdb** (2TR output)
   - Tables: `transformed_precision_alf`, `transformed_precision_irf`, etc.
   - Schema: Staged columns + derived features
   - Key additions:
     - `price_segment`: low/medium/high
     - `rating_category`: poor/fair/good/excellent
     - `review_volume`: low/medium/high
     - `quality_score`: Composite metric (0-1)
     - `is_competitive`: Boolean flag

## Usage

### Running the Complete Pipeline

Execute all three stages sequentially:

```bash
# Stage 0IM: Import from Google Sheets
Rscript scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_0IM.R

# Stage 1ST: Standardization (R116 currency conversion)
Rscript scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_1ST.R

# Stage 2TR: Transformation (feature engineering)
Rscript scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_2TR.R

# Validate results
Rscript scripts/update_scripts/ETL/precision/validate_week1.R
```

### Running Individual Stages

Each stage can be run independently (assuming prerequisites are met):

```bash
# Import only
Rscript scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_0IM.R

# Standardization only (requires raw_data.duckdb)
Rscript scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_1ST.R

# Transformation only (requires staged_data.duckdb)
Rscript scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_2TR.R
```

### Interactive Usage

```r
# Source the scripts in R console
source("scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_0IM.R")
source("scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_1ST.R")
source("scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_2TR.R")

# Run functions
result_0im <- precision_etl_0im()
result_1st <- precision_etl_1st()
result_2tr <- precision_etl_2tr()

# Validate
source("scripts/update_scripts/ETL/precision/validate_week1.R")
validation <- validate_week1()
```

## Validation

### Validation Checks

The `validate_week1.R` script performs comprehensive validation:

**Stage 0IM Checks:**
- ✓ raw_data.duckdb exists
- ✓ All 6 raw tables exist
- ✓ Metadata columns present (import_timestamp, product_line_id, data_source)

**Stage 1ST Checks:**
- ✓ staged_data.duckdb exists
- ✓ All 6 staged tables exist
- ✓ R116 currency fields present (price_usd, original_price, etc.)
- ✓ Currency conversion rates reasonable (0.0001 < rate < 100)
- ✓ No missing values in critical fields (product_id)

**Stage 2TR Checks:**
- ✓ transformed_data.duckdb exists
- ✓ All 6 transformed tables exist
- ✓ Derived features present (price_segment, quality_score, etc.)

### Running Validation

```bash
Rscript scripts/update_scripts/ETL/precision/validate_week1.R

# Expected output:
# ✅ ALL VALIDATIONS PASSED
# Week 1 deliverables are complete and compliant
```

## Dependencies

### R Packages

```r
# Core packages
library(duckdb)      # Database management
library(dplyr)       # Data manipulation
library(tibble)      # Data frames
library(googlesheets4) # Google Sheets import
library(jsonlite)    # JSON parsing (for attributes)

# Install if needed
install.packages(c("duckdb", "dplyr", "tibble", "googlesheets4", "jsonlite"))
```

### Utility Functions

Located in `scripts/global_scripts/04_utils/`:

- `fn_convert_currency_to_usd.R`: R116-compliant currency conversion
- `fn_standardize_attribute_names.R`: Variable name standardization

## R116 Currency Standardization

### Why R116 is Critical

Currency standardization is implemented in the 1ST stage to prevent MP029 violations (No Fake Data). Mixing currencies without standardization creates synthetic comparisons:

❌ **VIOLATION**: Comparing TWD 1000 vs USD 1000 as equal
✅ **CORRECT**: Convert both to USD (TWD 1000 = USD 32, USD 1000 = USD 1000)

### Exchange Rate Source

**Week 1 Implementation**: Fixed exchange rates (RATE_SOURCE = "FIXED")

Default rates (as of 2025-11-12):
- USD: 1.0000 (base currency)
- EUR: 1.0800
- GBP: 1.2700
- TWD: 0.0320
- AUD: 0.6500
- CAD: 0.7300
- JPY: 0.0067
- CNY: 0.1380

**Future Enhancement**: Integrate ECB or FRED APIs for live exchange rates

### Audit Trail

R116 requires complete conversion metadata:

```
original_price      → Original price value (e.g., 1000)
original_currency   → ISO 4217 code (e.g., "TWD")
price_usd           → Converted USD value (e.g., 32.00)
conversion_rate     → Rate used (e.g., 0.0320)
conversion_date     → Date of conversion
conversion_source   → Source of rate ("FIXED", "ECB", "FRED")
```

## Troubleshooting

### Common Issues

**Issue 1: Google Sheets Authentication Error**
```
Error: Authentication required
```
**Solution**: The sheet must be publicly accessible. If private, run `gs4_auth()` first.

---

**Issue 2: Database Not Found**
```
ERROR: Input database not found: data/raw_data.duckdb
```
**Solution**: Run the previous ETL stage first (0IM → 1ST → 2TR sequence required).

---

**Issue 3: Missing Columns**
```
VIOLATION R116: Price column 'price' not found in data
```
**Solution**: Verify Google Sheets has expected columns. Check sheet structure.

---

**Issue 4: Unreasonable Conversion Rates**
```
R116 WARNING: 15 rows have unusual conversion rates
```
**Solution**: Check original_currency values. May have invalid currency codes.

### Debug Mode

For detailed logging, run scripts in R console:

```r
# Enable detailed messages
options(warn = 1)

# Source and run
source("scripts/update_scripts/ETL/precision/precision_ETL_product_profiles_1ST.R")
result <- precision_etl_1st()

# Inspect results
print(result)
```

## Future Enhancements (Week 2+)

### Week 2: DRV Derivation Scripts
- Cross-product line aggregations
- Market position analysis
- Competitive benchmarking

### Week 3: Time Series Integration
- Historical price tracking
- Trend analysis
- Seasonality patterns

### Week 4: Platform Integration
- Amazon API integration
- eBay API integration
- Real-time data updates

## References

### MAMBA Principles

- **MP108**: Base ETL Pipeline Separation - `scripts/global_scripts/00_principles/docs/en/part1_principles/CH09_etl_pipelines/meta_principles/MP108_base_etl_pipeline_separation.qmd`
- **R116**: Currency Standardization - `scripts/global_scripts/00_principles/docs/en/part1_principles/CH09_etl_pipelines/rules/R116_currency_standardization_etl_1st.qmd`
- **MP102**: Standardization Principle
- **MP029**: No Fake Data Principle

### Design Document

Full redesign specification: `docs/suggestion/MAMBA/20251021/MAMBA_PRECISION_MARKETING_PRINCIPLE_BASED_REDESIGN.md`

## Support

For issues or questions:
1. Check principle documentation in `scripts/global_scripts/00_principles/`
2. Review ETL schemas in `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH17_database_specifications/etl_schemas/`
3. Consult MAMBA architecture documentation

---

**Last Updated**: 2025-11-12
**Version**: 1.0 (Week 1 Complete)
**Coordinator**: principle-product-manager
