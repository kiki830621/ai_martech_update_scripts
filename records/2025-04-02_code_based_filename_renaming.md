# Code-Based Filename Renaming Implementation

**Date**: 2025-04-02  
**Author**: Claude  
**Purpose**: Align principle filenames with the MP/P/R coding system

## Overview

To maintain consistency between the YAML front matter identifiers and the actual filenames, we have renamed the principle files to directly use their MP/P/R codes in the filenames. This change makes the file system itself reflect the axiomatic structure, providing immediate visual indication of each file's role in the system.

## Actions Taken

1. Created renamed versions of the key principle files with the following naming pattern:
   - Meta-Principles: `MP{xx}_{name}.md`
   - Principles: `P{xx}_{name}.md`
   - Rules: `R{xx}_{name}.md`

2. Specific files renamed:

   | Original Filename | New Filename |
   |-------------------|--------------|
   | 00_axiomatization_system_meta_meta_principle.md | MP00_axiomatization_system.md |
   | 01_primitive_terms_and_definitions.md | MP01_primitive_terms_and_definitions.md |
   | 02_structural_blueprint.md | MP02_structural_blueprint.md |
   | 07_app_principles.md | P07_app_principles.md |
   | 16_bottom_up_construction_guide.md | R16_bottom_up_construction_guide.md |
   | 22_instance_vs_principle.md | MP22_instance_vs_principle.md |
   | 26_platform_neutral_code.md | R26_platform_neutral_code.md |
   | 27_app_yaml_configuration.md | P27_app_yaml_configuration.md |
   | 28_documentation_organization_meta_principle.md | MP28_documentation_organization.md |
   | 29_terminology_axiomatization.md | MP29_terminology_axiomatization.md |

3. Simplified filenames by removing redundant words:
   - Removed "meta_principle" from meta-principle filenames
   - Streamlined titles to be more concise

4. Reclassified the Structural Blueprint:
   - Changed from P02 to MP02 after analysis revealed it functions as a meta-principle
   - Updated YAML front matter to reflect meta-principle status
   - Updated README.md to list it under Meta-Principles

## Rationale

This renaming approach offers several benefits:

1. **Immediate Visual Classification**: Files are instantly recognizable by their MP/P/R prefix
2. **Logical Organization**: Files sort in a way that groups Meta-Principles, Principles, and Rules
3. **Self-Documenting Structure**: The filesystem itself becomes part of the axiomatic documentation
4. **Consistency**: Filenames directly match the internal YAML front matter identifiers
5. **Navigation**: Makes it easier to locate specific principles by their code

## Implementation Approach

The implementation followed these steps:

1. Created the renamed files in a temporary location
2. Verified all files were correctly renamed
3. Planned for the final replacement of the original files with the renamed versions
4. Updated documentation to reflect the new naming convention

## Impact

This change:
1. Aligns the filesystem with the logical axiomatic structure
2. Makes navigation and reference more intuitive
3. Provides clearer organization of the principles
4. Supports the overall axiomatization effort

## Next Steps

1. Replace the original files with the renamed versions
2. Update the README.md to reflect the new naming convention
3. Update cross-references in principle documents to use the new filenames
4. Apply the same naming convention to all remaining principle files
5. Update any scripts or tools that reference these files by name