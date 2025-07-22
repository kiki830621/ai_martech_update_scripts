# Application Directory Structure Changes

This document records significant changes to the application directory structure, providing a historical record for developers.

## 2025-04-02: Removed redundant `app_principles.md` from root directory

### Rationale
Removed the redundant app_principles.md file from the root directory as it duplicates content already stored in the proper principles directory (`/update_scripts/global_scripts/00_principles/07_app_principles.md` and `/update_scripts/global_scripts/00_principles/17_app_construction_function.md`).

### Benefits
1. Eliminates potential confusion and inconsistencies from having principles in multiple locations
2. Follows the Instance vs. Principle Meta-Principle by centralizing principles in the designated directory
3. Simplifies maintenance by having a single source of truth for principles
4. Improves adherence to our file organization conventions

### Changes Made
```bash
rm app_principles.md
```

## 2025-04-02: Renamed `local_scripts` to `app_configs`

### Rationale
The directory was renamed to better reflect its purpose as a configuration repository rather than a collection of executable scripts. This change aligns with our declarative app construction approach where configuration is separated from implementation.

### Benefits
1. Better describes the directory's actual purpose
2. Aligns with YAML-based configuration architecture
3. Creates consistent naming with other app-related directories (app_data, app_screenshots)
4. Emphasizes the configuration-driven nature of the application

### Code Impact
- Updated default paths in utility functions:
  - `buildAppFromConfig(config_file, base_path = "app_configs")`
  - `readYamlConfig(yaml_file, base_path = "app_configs")`
- Updated documentation in app construction principles

### Changes Made
```bash
mv local_scripts app_configs
```

### Updated Documentation
- Added directory structure to app construction function principle (17_app_construction_function.md)
- Created this change log to track directory structure evolution

---

*Note: This document follows the "Instance vs. Principle" meta-principle, recording specific instances of change rather than general principles. It is stored in the update_scripts directory accordingly.*