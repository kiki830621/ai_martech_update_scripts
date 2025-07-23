# Parameters and Defaults Rules Implementation

## Date: 2025-04-03
## Author: Claude
## Topic: Clarifying the Distinction Between Parameters and Defaults

## Summary

This record documents the clarification and enhancement of rules regarding parameters and defaults in the application architecture. We've revised R18 (Defaults From Triple Rule) to better distinguish between defaults and parameters, and created a new R19 (YAML Parameter Configuration Rule) to establish how parameters should be specified and handled when omitted.

## Key Changes

1. **R18 Revision (Defaults From Triple Rule)**:
   - Clarified that defaults should be in triple components, not in YAML configuration
   - Added section distinguishing between defaults and parameters
   - Expanded guidance on default values usage and structure
   - Enhanced explanation of how components should handle undefined parameters

2. **R19 Creation (YAML Parameter Configuration Rule)**:
   - Created new rule focused specifically on YAML parameter configuration
   - Established that parameters should default to nothing when omitted from YAML
   - Defined the scope of what should be in YAML configuration
   - Provided guidance on parameter omission handling

## Conceptual Clarification

The revision and new rule establish a clear conceptual framework:

1. **Defaults (in Triple)**:
   - Fallback values when parameters are not specified
   - Defined in component Defaults files
   - Allow components to function without configuration
   - Provide sample data for development and testing

2. **Parameters (in YAML)**:
   - Configurable values that override defaults
   - Focus on positioning, connections, and application-specific behavior
   - May be omitted, in which case they default to nothing
   - Drive the customization of applications without modifying code

3. **Default Parameter Handling**:
   - When a parameter is omitted from YAML, it defaults to nothing (NULL)
   - Components must gracefully handle missing parameters using their default values
   - No hardcoded fallbacks should exist in app.R or server code

## Implementation Examples

### Example: Parameter vs. Default Handling

```r
# Correct implementation - YAML parameter handling with triple defaults
sidebarServer <- function(id, data_source = NULL, config = NULL) {
  moduleServer(id, function(input, output, session) {
    # Get defaults from the triple
    defaults <- sidebarDefaults()
    
    # Use parameter from config if specified, otherwise use default from triple
    position <- if (!is.null(config) && !is.null(config$position)) {
      config$position
    } else {
      defaults$position
    }
    
    # Rest of component implementation...
  })
}
```

## Benefits

The clarification of parameters and defaults provides several benefits:

1. **Clear Responsibility**: Each value has a clear "home" (parameters in YAML, defaults in triples)

2. **Component Independence**: Components can function with or without configuration

3. **Clean Application Code**: app.R remains free of implementation details and fallback logic

4. **Maintainable Configuration**: Configuration focuses solely on what varies between applications

5. **Testing Simplicity**: Components can be tested with or without configuration

6. **Flexibility**: Applications can be customized through configuration alone

## Relationship to Other Principles

These revisions support key principles:

1. **app.R Is Global (P12)**: Keeping app.R free of default values and implementation details

2. **Separation of Concerns (MP17)**: Clearly separating configuration from implementation

3. **UI-Server-Defaults Triple (R11)**: Reinforcing the importance and role of the Defaults component

## Conclusion

By clearly distinguishing between parameters (in YAML) and defaults (in component triples), we establish a more robust and maintainable application architecture. Parameters focus on what varies between applications and default to nothing when omitted, while defaults provide fallback values that allow components to function independently. This approach supports component autonomy, simplifies testing, and enables configuration-driven application customization without modifying code.