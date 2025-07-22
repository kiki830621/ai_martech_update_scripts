# Records Directory

This directory contains historical records, reports, and documentation of specific changes made to the application. These are "instances" rather than "principles" according to our Instance vs. Principle Meta-Principle.

## Purpose

The records directory serves as a centralized location for:

1. **Change Documentation**: Records of specific changes made to the application structure, configuration, or code
2. **Analysis Reports**: Results of code reviews, performance tests, or other evaluations
3. **Implementation Notes**: Specific details about how particular features were implemented
4. **Decision Records**: Documentation of specific decisions made during development

## Relationship to Principles

While the principles in `/update_scripts/global_scripts/00_principles/` define reusable guidelines and patterns, the documents in this records directory capture specific instances and historical information.

This separation follows the Instance vs. Principle Meta-Principle (22_instance_vs_principle.md), which establishes that:

1. **Principles**: Conceptual, reusable guidelines stored in the principles directory and tracked in version control
2. **Instances**: Specific implementations, reports, or artifacts stored outside the principles directory

## File Naming Convention

Files in this directory should use the following naming pattern:

```
YYYY-MM-DD_description.md
```

For example:
- `2025-04-02_app_directory_changes.md`
- `2025-04-02_null_references_report.md`

## Current Records

- `2025-04-02_app_directory_changes.md`: Documents changes to the application directory structure
- `2025-04-02_null_references_report.md`: Analysis of potential null references in the codebase
- `2025-04-02_database_permission_enhancement.md`: Documentation of database connection permission enhancements
- `2025-04-02_deployment_patterns_principle.md`: Documentation of the new deployment patterns principle
- `2025-04-02_test_scripts_organization.md`: Documentation of test scripts organization principle implementation
- `2025-04-02_authentic_context_testing.md`: Documentation of the authentic context testing principle creation
- `2025-04-02_platform_neutral_code.md`: Documentation of the platform-neutral code principle creation
- `2025-04-02_test_app.md`: Documentation of test app creation for YAML configuration testing
- `2025-04-02_principle_centralization.md`: Documentation of removing duplicate principle files
- `2025-04-02_app_yaml_configuration.md`: Documentation of the app YAML configuration principle creation
- `2025-04-02_documentation_organization.md`: Documentation of the documentation organization meta-principle creation

## Usage Guidelines

1. When making significant structural changes to the application, document them here
2. Keep records focused on specific instances rather than general principles
3. Include dates in filenames for easy chronological tracking
4. Maintain cross-references to relevant principles when applicable
5. Update this README.md when adding new types of records

This directory is stored in Dropbox but not in Git, following our file organization principles.