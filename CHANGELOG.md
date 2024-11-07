# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.2] - 2024-11-08

### Added

- List, set, and tuple support for `log_configs()` ([#67](https://github.com/neptune-ai/neptune-client-scale/pull/67))
- Tuple support for tags ([#67](https://github.com/neptune-ai/neptune-client-scale/pull/67))

### Changed

- Performance improvements
- Change the logger's configuration to be more resilient ([#66](https://github.com/neptune-ai/neptune-client-scale/pull/66))
- Update docs: info about timestamp and timezones ([#69](https://github.com/neptune-ai/neptune-client-scale/pull/69))
- Strip quotes from the `NEPTUNE_PROJECT` env variable ([#51](https://github.com/neptune-ai/neptune-client-scale/pull/51))


## [0.7.1] - 2024-10-28

### Changed
- Removed `family` from run initialization parameters ([#62](https://github.com/neptune-ai/neptune-client-scale/pull/62))
- Made `timestamp` keyword-only in `log_metrics()` ([#58](https://github.com/neptune-ai/neptune-client-scale/pull/58))

## [0.6.3] - 2024-10-23

### Changed

- Changed the signature of `Run.log_metrics`:
    - `date` is now the first parameter in line with other logging methods ([#58](https://github.com/neptune-ai/neptune-client-scale/pull/58))
    - `step` and `data` are now mandatory ([#55](https://github.com/neptune-ai/neptune-client-scale/pull/55))

- Removed iterables from `log_config` value type hints ([#53](https://github.com/neptune-ai/neptune-client-scale/pull/53))

## [0.6.0] - 2024-09-09

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
