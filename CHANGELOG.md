# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.4] - 2026-11-18

### Added

- Export `CallbackExceptionGroup`, `CallbackFailure` and the new
  `CallbackCancelledError` from `aiopulse`.

### Changed

- **Breaking:** callback failures are always raised as a
  `CallbackExceptionGroup`, also when only one callback failed. A single
  failure was previously raised as its original exception.
- **Breaking:** remove `CallbackExecutor.failures`; the failure records are
  available from the raised group's `failures`.
- `CallbackExceptionGroup.derive()` is typed to return a
  `CallbackExceptionGroup` of the derived exception type.

### Fixed

- Record a cancellation that the executor did not request (a callback awaiting
  a cancelled future or task, or cancelling itself) as a
  `CallbackCancelledError` failure. It previously reached the consumer as
  `CancelledError`, which could end the consumer's task as cancelled and lose
  the error inside a caller's `TaskGroup`.
- Stop counting callbacks cancelled before they started as running.
- Reject a `CallbackExceptionGroup` whose exceptions do not match its failure
  records one to one, instead of ignoring an empty sequence.

## [0.0.3] - 2026-11-18

### Added

- Add configurable pending-task buffer capacity and callback-failure metadata
  on collected exception groups.

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
