# Test Scripts Organization

**Date**: 2025-04-02  
**Author**: Precision Marketing Team  
**Category**: Process Improvement  

## Summary

Established a formal process for organizing test scripts by implementing a principle that test scripts should be moved to the debug folder after completion. Enhanced the debug directory structure to better organize different types of test scripts.

## Changes Made

1. Created a dedicated subdirectory for Shiny test applications:
   - Added `rshinyapp/` folder within the debug directory
   - Moved the newly created permission testing app to this location

2. Updated the debug directory README to:
   - Document the new subdirectory structure
   - List the available test applications 
   - Add the principle that test scripts should be moved to debug after completion

3. Added detailed documentation for the test application:
   - Purpose and functionality
   - Expected behavior across different operation modes
   - Instructions for running and interpreting results

## Test Application Details

The `test_db_permission_app.R` is an interactive Shiny application that validates the data access permission system implementation. It provides:

1. Operation mode switching between APP_MODE, UPDATE_MODE, and GLOBAL_MODE
2. Testing connections to databases in all three data layers:
   - App Layer: app_data
   - Processing Layer: processed_data
   - Global Layer: global_scd_type1
3. Visual verification of:
   - Connection status
   - Read/write permissions
   - Error messages for restricted operations
4. Graceful handling of initialization between mode switches

## Benefits

1. **Organized Testing**: Structured approach to debugging and testing components
2. **Clear Documentation**: Comprehensive information about test scripts purpose and behavior
3. **Best Practices**: Reinforcement of organizational principles
4. **Reusability**: Well-documented test scripts can be referenced for similar future needs
5. **Streamlined Development**: Clear separation between production code and testing utilities

## Related Principles

- **Data Source Hierarchy Principle** (23_data_source_hierarchy.md): The test app validates this principle's permission rules
- **Operating Modes Principle** (18_operating_modes.md): The test app verifies behavior across different modes

## Conclusion

The formalization of test script organization enhances the project's maintainability and provides a clearer structure for debugging tools. By establishing the principle that test scripts should move to the debug folder after completion, we ensure a clean separation between production and testing code.