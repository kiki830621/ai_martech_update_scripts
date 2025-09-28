# WISER Update Scripts Compliance Report & Improvement Plan

## Executive Summary
Analysis Date: 2025-01-28
Total Scripts: 52 R files
ETL Compliant: 13 files (25%)
**Critical Finding**: 75% of scripts violate MAMBA naming conventions

## 🚨 Critical Principle Violations

### 1. **DM_R028 Violation**: ETL Naming Convention
**Standard**: `{platform}_ETL_{datatype}_{phase}.R`
**Violation Rate**: 75% (39 of 52 files)

### 2. **MP064 Violation**: ETL-Derivation Separation
**Issue**: Mixed business logic and data movement in single files

### 3. **MP104 Violation**: ETL Data Flow Separation
**Issue**: Unclear data pipeline phases

## 📊 Current File Analysis

### Pattern Categories Identified

#### 1. **Legacy Pattern Files** (Non-compliant)
```
P01_D01_00.R through P01_D01_06.R  ← Unclear platform/purpose
S01_00.R, S01_01.R                 ← Unknown S designation
all_D00_01.R, all_D00_02.R         ← Ambiguous "all" platform
all_S02_00.R                       ← Mixed S/D notation
amz_D00_03.R, amz_D01_00-06.R      ← Non-standard phase codes
```

#### 2. **Compliant ETL Files** (Following DM_R028)
```
amz_ETL03_0IM.R  ← ✅ Correct: {platform}_ETL{number}_{phase}
amz_ETL03_1ST.R
amz_ETL03_2TR.R
(and 10 more following this pattern)
```

## 🔧 Proposed Restructuring Plan

### Phase 1: Decode Legacy Patterns

Based on analysis, the legacy codes likely mean:
- **P01**: Platform 01 (possibly "primary" or specific platform)
- **D00/D01**: Data versions or derivation levels
- **S01/S02**: Stage or summary operations
- **all**: Cross-platform operations

### Phase 2: File Renaming Strategy

#### Current → Proposed Mappings

**P01_D01 Series → Platform-specific ETL**
```bash
P01_D01_00.R → cbz_ETL01_0IM.R  # Import phase
P01_D01_01.R → cbz_ETL01_1ST.R  # Stage phase
P01_D01_02.R → cbz_ETL01_2TR.R  # Transform phase
P01_D01_03.R → cbz_DRV01_customer.R  # Derivation (if business logic)
P01_D01_04.R → cbz_DRV01_product.R
P01_D01_05.R → cbz_DRV01_sales.R
P01_D01_06.R → cbz_DRV01_metrics.R
```

**S01 Series → Summary/Aggregation ETL**
```bash
S01_00.R → all_ETL_summary_0IM.R
S01_01.R → all_ETL_summary_1ST.R
```

**all_D00 Series → Cross-platform ETL**
```bash
all_D00_01.R → all_ETL01_0IM.R
all_D00_02.R → all_ETL01_1ST.R
all_S02_00.R → all_ETL_summary_2TR.R
```

**amz_D Series → Amazon ETL (standardize)**
```bash
amz_D00_03.R → amz_ETL01_0IM.R
amz_D01_00.R → amz_ETL02_0IM.R
amz_D01_01.R → amz_ETL02_1ST.R
amz_D01_02.R → amz_ETL02_2TR.R
amz_D01_03.R → amz_DRV02_customer.R
amz_D01_04.R → amz_DRV02_product.R
amz_D01_05.R → amz_DRV02_sales.R
amz_D01_06.R → amz_DRV02_metrics.R
```

## 📁 Recommended Directory Structure

```
update_scripts/
├── ETL/                         # Pure data movement (MP064)
│   ├── cbz/                     # Platform-specific
│   │   ├── cbz_ETL01_0IM.R     # Import
│   │   ├── cbz_ETL01_1ST.R     # Stage
│   │   └── cbz_ETL01_2TR.R     # Transform
│   ├── amz/
│   │   ├── amz_ETL01_0IM.R
│   │   ├── amz_ETL01_1ST.R
│   │   └── amz_ETL01_2TR.R
│   └── all/                     # Cross-platform
│       ├── all_ETL01_0IM.R
│       ├── all_ETL01_1ST.R
│       └── all_ETL01_2TR.R
│
├── DRV/                         # Business logic (MP064)
│   ├── cbz/
│   │   ├── cbz_DRV01_customer.R
│   │   ├── cbz_DRV01_product.R
│   │   └── cbz_DRV01_sales.R
│   └── amz/
│       ├── amz_DRV01_customer.R
│       └── amz_DRV01_product.R
│
└── orchestration/               # Workflow control
    ├── run_daily_etl.R
    └── run_full_pipeline.R
```

## ✅ Implementation Checklist

### Immediate Actions (Priority 1)
- [ ] Create ETL/ and DRV/ subdirectories
- [ ] Move compliant amz_ETL files to ETL/amz/
- [ ] Create migration script for batch renaming

### Short-term Actions (Priority 2)
- [ ] Analyze each P01/S01/D00 file to determine actual function
- [ ] Separate ETL logic from business logic (MP064)
- [ ] Rename files following DM_R028 convention

### Long-term Actions (Priority 3)
- [ ] Implement R113 four-part structure in all scripts
- [ ] Add proper initialization/deinitialization (MP031/MP033)
- [ ] Create comprehensive documentation

## 🚫 MP029 Compliance Note

**CRITICAL**: No fake/sample data will be created during migration.
All data operations must use actual production or historical data.

## 📝 Migration Script Template

```r
#!/usr/bin/env Rscript
# WISER ETL Migration Script
# Principle: DM_R028, MP064, MP104

# Create directory structure
dir.create("ETL/cbz", recursive = TRUE)
dir.create("ETL/amz", recursive = TRUE)
dir.create("ETL/all", recursive = TRUE)
dir.create("DRV/cbz", recursive = TRUE)
dir.create("DRV/amz", recursive = TRUE)

# File mapping (NO FAKE DATA - only rename operations)
file_mappings <- list(
  "P01_D01_00.R" = "ETL/cbz/cbz_ETL01_0IM.R",
  "P01_D01_01.R" = "ETL/cbz/cbz_ETL01_1ST.R",
  # ... continue mapping
)

# Execute renaming (with backup)
for (old_name in names(file_mappings)) {
  new_name <- file_mappings[[old_name]]
  if (file.exists(old_name)) {
    file.copy(old_name, paste0(old_name, ".backup"))
    file.rename(old_name, new_name)
    cat(sprintf("Migrated: %s → %s\n", old_name, new_name))
  }
}
```

## 📈 Expected Benefits

1. **Clarity**: Clear separation of ETL vs business logic
2. **Maintainability**: Standardized naming = easier navigation
3. **Compliance**: 100% adherence to MAMBA principles
4. **Scalability**: Easy to add new platforms/data types
5. **Debugging**: Clear pipeline phases aid troubleshooting

## 🎯 Success Metrics

- **Before**: 25% compliance (13/52 files)
- **After Target**: 100% compliance (52/52 files)
- **ETL-DRV Separation**: 100% (currently ~0%)
- **Directory Organization**: Hierarchical by function

## Next Steps

1. Review and approve this plan
2. Run migration script with backups
3. Update any dependent scripts/configurations
4. Document new structure in README
5. Train team on new conventions

---

*Generated by MAMBA Principle Analysis*
*Compliant with MP029: No fake data used or generated*