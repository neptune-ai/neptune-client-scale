# Changelog

All notable changes to this project will be documented in this file.

<!--- ONLY INCLUDE USER-FACING CHANGES -->

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.9.0 - 2025-01-07

### Changes
* Removed support for Python 3.8 (https://github.com/neptune-ai/neptune-client-scale/pull/105)

### Added
* Added `projects.list_projects()` method to list projects accessible to the current user (https://github.com/neptune-ai/neptune-client-scale/pull/97)

### Fixed
* Fixed retry behavior on encountering a `NeptuneRetryableError` (https://github.com/neptune-ai/neptune-client-scale/pull/99)
* Fixed batching of metrics when logged with steps out of order (https://github.com/neptune-ai/neptune-client-scale/pull/91)

### Chores
* Not invoking `on_error_callback` on encountering 408 and 429 HTTP statuses (https://github.com/neptune-ai/neptune-client-scale/pull/110)

## 0.8.0 - 2024-11-26

### Added
- Added function `neptune_scale.projects.create_project()` to programmatically create Neptune projects ([#92](https://github.com/neptune-ai/neptune-client-scale/pull/92))
- Added function `neptune_scale.list_projects()` to list all projects the current user has access to ([#97](https://github.com/neptune-ai/neptune-client-scale/pull/97))

### Changed
- Removed support for Python 3.8
- Neptune will now skip non-finite metric values by default, instead of raising an error. This can be configured using
  the new `NEPTUNE_SKIP_NON_FINITE_METRICS` environment variable ([#85](https://github.com/neptune-ai/neptune-client-scale/pull/85))
- Made default error callback logs more informative ([#78](https://github.com/neptune-ai/neptune-client-scale/pull/78))
- Revamped exception descriptions ([#80](https://github.com/neptune-ai/neptune-client-scale/pull/80))
- `fields` renamed to `attributes` ([#86](https://github.com/neptune-ai/neptune-client-scale/pull/86))

### Fixed
- Fixed batching of steps ([#82](https://github.com/neptune-ai/neptune-client-scale/pull/82))

## 0.7.2 - 2024-11-07

### Added

- List, set, and tuple support for `log_configs()` ([#67](https://github.com/neptune-ai/neptune-client-scale/pull/67))
- Tuple support for tags ([#67](https://github.com/neptune-ai/neptune-client-scale/pull/67))

### Changed
- Performance improvements
- Change the logger's configuration to be more resilient ([#66](https://github.com/neptune-ai/neptune-client-scale/pull/66))
- Update docs: info about timestamp and timezones ([#69](https://github.com/neptune-ai/neptune-client-scale/pull/69))
- Strip quotes from the `NEPTUNE_PROJECT` env variable ([#51](https://github.com/neptune-ai/neptune-client-scale/pull/51))


## 0.7.1 - 2024-10-28

### Changed
- Removed `family` from run initialization parameters ([#62](https://github.com/neptune-ai/neptune-client-scale/pull/62))
- Made `timestamp` keyword-only in `log_metrics()` ([#58](https://github.com/neptune-ai/neptune-client-scale/pull/58))

## 0.6.3 - 2024-10-23

### Changed
- Changed the signature of `Run.log_metrics`:
    - `date` is now the first parameter in line with other logging methods ([#58](https://github.com/neptune-ai/neptune-client-scale/pull/58))
    - `step` and `data` are now mandatory ([#55](https://github.com/neptune-ai/neptune-client-scale/pull/55))
- Removed iterables from `log_config` value type hints ([#53](https://github.com/neptune-ai/neptune-client-scale/pull/53))

## 0.6.0 - 2024-09-09

### Added
- Dedicated exceptions for missing project or API token ([#44](https://github.com/neptune-ai/neptune-client-scale/pull/44))

### Changed
- Removed `timestamp` parameter from `add_tags()`, `remove_tags()` and `log_configs()` methods ([#37](https://github.com/neptune-ai/neptune-client-scale/pull/37))
- Performance improvements of metadata logging ([#42](https://github.com/neptune-ai/neptune-client-scale/pull/42))

## [0.5.0] - 2024-09-05

### Added
- Added docstrings to logging methods ([#40](https://github.com/neptune-ai/neptune-client-scale/pull/40))

## [0.4.0] - 2024-09-03

### Added
- Added support for integer values when logging metric values ([#33](https://github.com/neptune-ai/neptune-client-scale/pull/33))
- Added support for async lag threshold ([#22](https://github.com/neptune-ai/neptune-client-scale/pull/22))

## [0.3.0] - 2024-09-03

### Added
- Package renamed to `neptune-scale` ([#31](https://github.com/neptune-ai/neptune-client-scale/pull/31))

## [0.2.0] - 2024-09-02

### Added
- Added minimal Run classes ([#6](https://github.com/neptune-ai/neptune-client-scale/pull/6))
- Added support for `max_queue_size` and `max_queue_size_exceeded_callback` parameters in `Run` ([#7](https://github.com/neptune-ai/neptune-client-scale/pull/7))
- Added support for logging metadata ([#8](https://github.com/neptune-ai/neptune-client-scale/pull/8))
- Added support for `creation_time` ([#9](https://github.com/neptune-ai/neptune-client-scale/pull/9))
- Added support for Forking ([#9](https://github.com/neptune-ai/neptune-client-scale/pull/9))
- Added support for Experiments ([#9](https://github.com/neptune-ai/neptune-client-scale/pull/9))
- Added support for Run resume ([#9](https://github.com/neptune-ai/neptune-client-scale/pull/9))
- Added support for env variables for project and api token ([#11](https://github.com/neptune-ai/neptune-client-scale/pull/11))

## [0.1.0] - 2024-09-02

Initial package release

[unreleased]: https://github.com/neptune-ai/neptune-api/compare/0.5.0...HEAD

[0.5.0]: https://github.com/neptune-ai/neptune-api/compare/0.4.0...0.5.0

[0.4.0]: https://github.com/neptune-ai/neptune-api/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/neptune-ai/neptune-api/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/neptune-ai/neptune-api/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/neptune-ai/neptune-api/commits/0.1.0
