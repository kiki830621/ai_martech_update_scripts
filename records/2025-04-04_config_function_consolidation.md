# Configuration Function Consolidation

## Overview
Eliminated redundant YAML configuration loading function by standardizing on the more comprehensive `load_app_config` function.

## Motivation
The application had two separate functions for loading YAML configuration files:
- `fn_read_yaml_config.R` in the 11_rshinyapp_utils directory
- `fn_load_app_config.R` in the 04_utils directory

This violated several principles:
- R08_global_scripts_synchronization: Avoid duplicate functionality in global scripts
- MP18_dont_repeat_yourself: Eliminate redundant code
- R13_initialization_sourcing: Standardize initialization procedures

## Changes Made

1. Updated app_bs4dash_prototype.R to use load_app_config directly:
   - Removed dependency on fn_read_yaml_config.R
   - Used the more comprehensive load_app_config function

2. Deprecated fn_read_yaml_config.R:
   - The functionality is completely covered by load_app_config
   - Maintains the same interface for backward compatibility
   - Provides more robust translation and locale handling

## Implementation Details

The updated approach provides several benefits:
- **Unified Configuration**: All configuration loading uses a single function
- **Enhanced Functionality**: Includes translation dictionary loading, locale checking
- **Proper Sequencing**: Ensures YAML is loaded before initialization
- **Reduced Redundancy**: Eliminates duplicate code for YAML parsing

## Expected Impact
- More consistent configuration loading across all applications
- Simplified codebase with fewer redundant functions
- Improved maintainability with centralized configuration logic
- Clearer dependency chain between configuration and initialization

## Archive and Backward Compatibility

1. Archived the original fn_read_yaml_config.R to 99_archive/11_rshinyapp_utils_archive for reference
2. Created a shim wrapper in the original location that:
   - Issues a deprecation warning
   - Calls load_app_config under the hood
   - Maintains backward compatibility
   - Falls back to original implementation if load_app_config is unavailable

3. Enhanced load_app_config with:
   - Support for pre-loaded configuration
   - Option to disable parameter loading
   - Better documentation for all options

This approach provides a smooth transition path while:
- Clearly marking the redundant function as deprecated
- Maintaining backward compatibility for existing code
- Encouraging migration to the preferred function
- Following R28_archiving_standard principles

## Next Steps
1. Update other application files to use load_app_config directly
2. Add the deprecated function to a list of products to be removed in future versions
3. Review other utility functions for similar consolidation opportunities