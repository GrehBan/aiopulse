# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`aiopulse` is a small, dependency-light (only `typing-extensions`) Python ≥3.11
library: `@invoke` turns an async function into a factory of
`CallbackInvocation` objects, and `CallbackExecutor` runs an iterable of them
with bounded concurrency, yielding results through `async for`. `README.md`
documents the behaviour for users; update it with any behaviour change, and
add an entry under `[Unreleased]` in `CHANGELOG.md` (Keep a Changelog).

## Commands

Tooling is `uv`. The `dev` group (ruff, mypy, pyright) is installed by default;
the `test` group is not, so sync both:

```bash
uv sync --all-groups

uv run ruff check .
uv run ruff format --check .
uv run mypy src tests             # strict; includes tests/typecheck/type_contracts.py
uv run pyright                    # strict; `include` covers src only
uv run pyright tests              # tests and the static type contracts
uv run pytest                     # branch coverage, fail_under = 98
```

Single test — `addopts` always enables coverage, so a partial run fails the
98% threshold unless coverage is disabled:

```bash
uv run pytest tests/test_executor_scheduling.py::test_single_invocation_executes_once --no-cov
uv run pytest -k "fail_fast" --no-cov
```

pytest settings that matter: `asyncio_mode = "auto"` (no
`@pytest.mark.asyncio` needed), `--strict-markers`, `filterwarnings =
["error"]` (any warning, e.g. a never-awaited coroutine, fails the test) and
`timeout = 30` from pytest-timeout, so a hung test fails instead of freezing
the run.

## Code conventions enforced by config

- Every module starts with `from __future__ import annotations` (ruff isort
  `required-imports`).
- Line length 79; Google-style docstrings required in `src/` (`D` rules), not
  in `tests/`.
- mypy and pyright both run in strict mode; tests may access private
  attributes (`reportPrivateUsage = false` for `tests`).
- `TypeVar` with `default=` comes from `typing_extensions`, not `typing`.
- Tests import shared helpers relatively (`from .helpers import ...`); an
  absolute `tests.helpers` import does not resolve in pyright's `tests`
  environment.

## Architecture

Modules in `src/aiopulse/`:

- `callback.py` — data types: `CallbackInvocation` (frozen dataclass:
  callback + args + kwargs, awaited via `__call__`), `CallbackOutcome`
  (result or the `MISSING` sentinel from `_sentinel.py`, so a failed callback
  is distinguishable from one that returned `None`), `CallbackFailure`
  (input index + invocation + original exception), and the `invoke` decorator,
  typed with `ParamSpec` so the factory keeps the callback's signature.
- `policy.py` — `ExecutionPolicy` (`SEQUENTIAL` / `FIRST_COMPLETED`, result
  *ordering* only) and `ExceptionsPolicy` (`FAIL_FAST` / `COLLECT`).
- `executor.py` — `CallbackExecutor`, `CallbackExceptionGroup` and
  `CallbackCancelledError`.
- `__init__.py` — public API via `__all__`; `tests/test_public_api.py` pins
  the exact list and order, so changing exports is a contract change.

### How `CallbackExecutor` works

It is a single-use async iterator and async context manager that owns an
`asyncio.TaskGroup`, entered manually in `aenter()` (lazily on first
`__anext__` if not used via `async with`) and exited in `aclose()`. The task
that enters owns the group: a fatal `BaseException` in a callback makes the
TaskGroup cancel that task (documented in the README).

- **Two limits.** `_live` is the set of tasks whose callback has not finished;
  its size (`_running`) is bounded by `concurrency`. A task leaves it in
  `_execute`'s `finally` (so a slot frees before the task is marked done) or,
  if cancelled before starting, in `_on_done`. Do not move that removal to
  the done callback alone: it runs one loop step late and iteration then
  stops early. `_outstanding` counts scheduled tasks whose results have not
  been consumed yet and is bounded by `concurrency + buffer_size`.
  `_schedule()` pulls from the input lazily until either limit is hit; it
  only runs on consumer actions (`aenter`, each `__anext__`, each wake-up).
- **Ordering.** `_pop_ready()` holds the only policy difference: `SEQUENTIAL`
  takes the head of `_ordered` (submission order) once done; `FIRST_COMPLETED`
  takes from `_ready`, which `_on_done` fills in completion order.
- **Wake-ups.** A single `asyncio.Event` is set by `_on_done`;
  `_wait_for_completion` clears then waits. This is only race-free because
  `_fetch` checks `_pop_ready()` with no `await` before clearing — preserve
  that.
- **Locks.** `_next_lock` serializes `__anext__`; `_close_lock` serializes
  `aclose`, which is idempotent.
- **Failures.** `_execute` records an ordinary `Exception` as a
  `CallbackFailure` and returns an empty `CallbackOutcome`, which iteration
  skips. Under `FAIL_FAST` the first failure cancels all other live tasks
  (excluding the current one) and the next fetch closes the executor.
  Failures are raised only from `aclose()`, always as one
  `CallbackExceptionGroup`, even for a single failure.
- **Whose cancellation.** `_cancel_requested` is set when the executor
  cancels its tasks (`_cancel_pending`, on close and fail-fast) and when a
  callback lets a non-`Exception` `BaseException` escape (the TaskGroup then
  cancels the siblings). A `CancelledError` while it is set propagates; any
  other one (a callback awaiting a cancelled future, or cancelling itself) is
  recorded as `CallbackCancelledError` with the original as `__cause__`.
- **Consumer exceptions win.** `aclose(exc)` / `__aexit__` with an exception
  already propagating discards recorded callback failures instead of raising
  them, so the caller's exception is never replaced.
- **`CallbackExceptionGroup`** is an `ExceptionGroup[ExceptionT_co]` holding
  the original exceptions plus a parallel `failures` tuple. It overrides
  `derive()` so `split()`, `subgroup()` and `except*` produce groups whose
  `failures` still match their exceptions (matched by identity, recursively
  for nested groups).

### Tests

- `tests/helpers.py` — `collect()`, `settle()` (let the loop run a few
  steps) and `unfinished_tasks()`; prefer it over private attributes to
  check that no callback task is left behind.
- `test_executor_scheduling.py`, `test_executor_ordering.py`,
  `test_executor_failures.py`, `test_executor_lifecycle.py` — executor
  behaviour by topic. Callbacks that need a task handle record
  `asyncio.current_task()` themselves.
- `test_executor_properties.py` — Hypothesis invariants (limits, ordering,
  failure records, cleanup) over random deterministic scenarios.
- `test_callback_failures.py` — `CallbackExceptionGroup` and derivation.
- `tests/typecheck/type_contracts.py` is not collected by pytest; it is
  verified only by the type checkers.
