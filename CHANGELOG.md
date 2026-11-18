# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2] - 2026-11-18

### Added

- Add bounded-concurrency asynchronous callback execution with configurable
  result ordering and exception policies.
- Add comprehensive tests for callback invocation, scheduling, lifecycle,
  cancellation, and static typing.

### Fixed

- Return the last completed result under `FIRST_COMPLETED`.
- Preserve consumer exceptions during executor shutdown.
- Close a callback execution coroutine when task creation fails.

## [0.0.1] - 2026-11-18

### Added

- Initial release.
