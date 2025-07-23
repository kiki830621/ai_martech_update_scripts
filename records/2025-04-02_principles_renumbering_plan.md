# Principles Renumbering Plan

**Date**: 2025-04-02  
**Author**: Claude  
**Subject**: Plan for renumbering the MP/P/R principle files to eliminate gaps  

## Overview

This document outlines a comprehensive plan for renumbering the Meta-Principles (MP), Principles (P), and Rules (R) files to eliminate gaps in the numerical sequence. Following the recommendations in P08_naming_principles.md, this renumbering will create a more logical, sequential organization that improves navigation and maintainability.

## Current State Analysis

### Meta-Principles (MP)

**Current numbers**: MP00, MP01, MP02, MP18, MP19, MP22, MP23, MP28, MP29

**Gaps**: MP03-MP17, MP20-MP21, MP24-MP27

### Principles (P)

**Current numbers**: P03, P04, P05, P06, P07, P08, P09, P16, P24, P25

**Gaps**: P00-P02, P10-P15, P17-P23, P26+

### Rules (R)

**Current numbers**: R01, R02, R03, R26, R27

**Gaps**: R00, R04-R25, R28+

## Renumbering Plan

### Meta-Principles (MP)

| Current Number | Document Title | New Number |
|----------------|----------------|------------|
| MP00 | Axiomatization System | MP00 |
| MP01 | Primitive Terms and Definitions | MP01 |
| MP02 | Structural Blueprint | MP02 |
| MP18 | Operating Modes | MP03 |
| MP19 | Mode Hierarchy | MP04 |
| MP22 | Instance vs. Principle | MP05 |
| MP23 | Data Source Hierarchy | MP06 |
| MP28 | Documentation Organization | MP07 |
| MP29 | Terminology Axiomatization | MP08 |

### Principles (P)

| Current Number | Document Title | New Number |
|----------------|----------------|------------|
| P03 | Project Principles | P00 |
| P04 | Script Separation | P01 |
| P05 | Data Integrity | P02 |
| P06 | Debug Principles | P03 |
| P07 | App Construction Principles | P04 |
| P08 | Naming Principles | P05 |
| P09 | Data Visualization | P06 |
| P16 | App Bottom-Up Construction | P07 |
| P24 | Deployment Patterns | P08 |
| P25 | Authentic Context Testing | P09 |

### Rules (R)

| Current Number | Document Title | New Number |
|----------------|----------------|------------|
| R01 | Directory Structure | R00 |
| R02 | File Naming Convention | R01 |
| R03 | Principle Documentation | R02 |
| R26 | Platform Neutral Code | R03 |
| R27 | App YAML Configuration | R04 |

## Implementation Strategy

The renumbering will be implemented in several phases to ensure consistency and minimize disruption:

### Phase 1: Preparation
1. Create backups of all principle files in 99_archive/00_principles_renumbering_backup_2025_04_02
2. Document all relationships between principles before renumbering
3. Create a temporary lookup table mapping old numbers to new numbers

### Phase 2: File Renaming
1. Rename each file according to the mapping above
2. For each file:
   - Update the `id` field in the YAML front matter
   - Update any self-references within the document

### Phase 3: Relationship Updates
1. For each file, update all relationship references in the YAML front matter:
   - `derives_from`
   - `influences`
   - `implements`
   - `extends`
   - `related_to`

### Phase 4: Content Updates
1. Update all cross-references within the document content
2. Verify that all links and references use the new numbering scheme

### Phase 5: README Updates
1. Update the README.md file with the new file names and numbers
2. Document the renumbering process in the Recent Updates section

## Impact Analysis

### Benefits
- Elimination of numbering gaps improves logical organization
- Sequential numbering makes it easier to locate principles
- Lower numbers for frequently used principles improves memorability
- Consistent numbering makes relationships easier to understand

### Risks
- Temporary confusion during transition period
- Potential for broken references if any are missed
- Historical references to old numbers may become invalid

## Validation Plan

After renumbering is complete, validate the changes by:
1. Verifying that all files have the correct name and content
2. Checking that all YAML front matter correctly references the new numbers
3. Ensuring that all internal document references use the new numbers
4. Validating that README.md correctly lists all principles with new numbers

## Rollback Plan

If significant issues are encountered:
1. Restore all files from the backup in 99_archive/00_principles_renumbering_backup_2025_04_02
2. Document the issues encountered in a new record

## Conclusion

This renumbering plan will significantly improve the organization and maintainability of the principles documentation. By eliminating gaps and creating a logical, sequential numbering system, we'll enhance the usability of the principles framework in line with the recommendations in P08_naming_principles.md.