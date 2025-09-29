# DRV Files Renaming Plan

## Issue Identified
The DRV files have internal P-number codes that don't match the external filenames. The files should be renamed to reflect their internal numbering for consistency.

## Internal Numbering System
- **P07**: Platform 07 (CBZ - Carrefour Brazil)
- **D01**: Derivation group 01
- **03-06**: Sequential sub-numbers

## Current → Proposed Renaming

### CBZ Platform (P07)
Based on internal P-numbers found:

| Current File | Internal Code | Proposed Name | Description |
|-------------|---------------|---------------|-------------|
| cbz_DRV01_customer.R | #P07_D01_03 | cbz_D01_03.R | Customer analysis |
| cbz_DRV01_product.R | #P07_D01_04 | cbz_D01_04.R | Product analysis |
| cbz_DRV01_sales.R | #P07_D01_05 | cbz_D01_05.R | Sales analysis |
| cbz_DRV01_metrics.R | #P07_D01_05 | cbz_D01_06.R | Metrics (fix duplicate) |

### Naming Convention Options

**Option 1: Minimal change (keep DRV)**
- Pattern: `{platform}_DRV{group}_{sequence}.R`
- Example: `cbz_DRV01_03.R`

**Option 2: Follow internal pattern exactly**
- Pattern: `{platform}_D{group}_{sequence}.R`
- Example: `cbz_D01_03.R`

**Option 3: Keep descriptive names with numbers**
- Pattern: `{platform}_D{group}_{sequence}_{description}.R`
- Example: `cbz_D01_03_customer.R`

## Recommendation
Use **Option 2** (`cbz_D01_03.R`) because:
1. Matches the internal P-number structure
2. Maintains sequential ordering
3. Simplifies file navigation
4. Aligns with MAMBA principle patterns

## Action Items
1. Check all DRV files for internal P-numbers
2. Fix any duplicate numbering
3. Rename files to match internal codes
4. Update any scripts that reference these files