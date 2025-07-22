# Principle Documentation Centralization

**Date**: 2025-04-02  
**Author**: Precision Marketing Team  
**Category**: Documentation Improvement  

## Summary

Removed duplicate principle documentation files from the examples directory to maintain a single source of truth in the 00_principles directory, following the Documentation Centralization meta-principle.

## Background

While reviewing the examples directory, we discovered duplicated principle documentation:

1. `bottom_up_construction_guide.md` in the examples directory was a duplicate of `16_bottom_up_construction_guide.md` in the 00_principles directory.

2. `shiny_app_working_directory.md` in the examples directory was a duplicate of `15_working_directory_guide.md` in the 00_principles directory.

The README.md in the examples directory already correctly referenced the principles in their 00_principles location, but the duplicate files could lead to confusion and divergence of documentation over time.

## Changes Made

1. Removed duplicate files from the examples directory:
   - Deleted `/update_scripts/global_scripts/10_rshinyapp_components/examples/bottom_up_construction_guide.md`
   - Deleted `/update_scripts/global_scripts/10_rshinyapp_components/examples/shiny_app_working_directory.md`

2. Left the README.md in the examples directory unchanged, as it already correctly referenced the principles in their proper location.

## Rationale

This change follows the Documentation Centralization meta-principle, which ensures that:

1. **Single Source of Truth**: Each principle should be documented in exactly one location
2. **Consistent References**: All references to principles should point to their canonical location
3. **Maintainability**: Updates to principles only need to be made in one place
4. **Discovery**: Developers can find all principles in a single directory

## Benefits

1. **Reduced Confusion**: Eliminates the risk of developers referencing outdated copies of principles
2. **Clearer Organization**: Reinforces the role of the 00_principles directory as the authoritative source for all principles
3. **Easier Maintenance**: Simplifies updates to principles, as changes only need to be made in one place
4. **Consistency**: Ensures all team members reference the same version of each principle

## Related Principles

- **Instance vs. Principle Meta-Principle** (22_instance_vs_principle.md): Principles should be stored in the principles directory
- **Code Organization Hierarchy** (01_code_organization_hierarchy.md): Defines the overall project structure

## Future Recommendations

1. Regularly audit the codebase for duplicate principle documentation
2. Add a validation step to the review process to ensure new principles are added only to the 00_principles directory
3. Consider adding redirects or symbolic links if examples need to reference local copies of principles