# aiopulse

**Lightweight asynchronous callback invocation and bounded-concurrency execution.**

`aiopulse` lets you describe asynchronous callback calls as invocation objects and execute them through a single asynchronous interface. It provides bounded task scheduling, configurable result ordering, and configurable callback exception handling.

## Features

- **Explicit invocations** — use `@invoke` to turn an asynchronous function into an invocation factory. Calling the decorated function stores its arguments without executing the callback.
- **Bounded concurrency** — control how many callback tasks the executor schedules at a time.
- **Configurable result ordering** — yield results in invocation order or as completed work is observed.
- **Configurable exception handling** — stop on the first callback failure or continue running and collect failures.
- **Async iteration** — consume successful callback results with `async for`.
- **Managed task lifecycle** — use the executor as an asynchronous context manager to ensure pending work is shut down when the consumer exits.

## Requirements

- Python 3.11 or newer.

`aiopulse` uses `asyncio.TaskGroup`, `ExceptionGroup`, and `StrEnum`, which are available in Python 3.11+.

## Installation

Clone the repository and install its dependencies with [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/GrehBan/aiopulse.git
cd aiopulse
uv sync
```

## Quick start

Decorate an asynchronous function with `@invoke`, create invocation objects by calling the decorated function, and pass those invocations to `CallbackExecutor`.

```python
import asyncio

from aiopulse import CallbackExecutor, invoke


@invoke
async def fetch_item(item_id: int) -> str:
    await asyncio.sleep(1)
    return f"item-{item_id}"


async def main() -> None:
    invocations = (fetch_item(item_id) for item_id in range(5))

    async with CallbackExecutor(invocations, concurrency=3) as executor:
        async for result in executor:
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

The executor schedules up to three callback invocations at a time. Results are yielded in the original invocation order by default, even though the callbacks may finish in a different order.

## Creating callback invocations

`@invoke` accepts an asynchronous callback and returns a callable with the callback's parameter signature. Calling that callable creates a `CallbackInvocation` containing:

- the original callback;
- the positional arguments supplied to it;
- the keyword arguments supplied to it.

The callback is **not executed** when the decorated function is called.

```python
from aiopulse import CallbackExecutor, invoke


@invoke
async def send_message(user_id: int, message: str) -> None:
    print(f"Sending to {user_id}: {message}")


first = send_message(10, "Hello")
second = send_message(user_id=20, message="Welcome")

# `first` and `second` describe calls. Their callbacks have not run yet.
```

Pass the resulting invocation objects to `CallbackExecutor`:

```python
async with CallbackExecutor([first, second]) as executor:
    async for result in executor:
        ...
```

The input can be any iterable of invocation objects, including a list, tuple, or generator. The executor consumes the iterable as it schedules work, so a generator can be used to avoid building the entire invocation collection in advance.

Callbacks must be asynchronous callables that return awaitables. `aiopulse` does not move synchronous work to a thread or process pool.

## Executing callbacks

`CallbackExecutor` is both an asynchronous iterator and an asynchronous context manager.

### Async iteration

Use `async for` to receive successful callback results:

```python
async for result in executor:
    await handle_result(result)
```

The executor starts lazily when iteration begins, unless it has already been entered through `async with` or `aenter()`.

### Context management

Prefer `async with` when the loop may finish early, for example because of `break` or an exception in the consumer:

```python
async with CallbackExecutor(invocations, concurrency=4) as executor:
    async for result in executor:
        if should_stop(result):
            break
        await handle_result(result)
```

Leaving the context closes the executor, cancels pending callback tasks, and waits for the underlying task group to finish. This ensures that callbacks do not remain running after the consumer has left the block.

If the iterator is consumed until exhaustion without an explicit context manager, the executor closes itself when there are no pending callbacks left. For explicit shutdown, call:

```python
await executor.aclose()
```

An executor instance represents one execution and cannot be restarted after it has been closed.

## Concurrency

The `concurrency` argument sets the maximum number of callback tasks that may be pending in the executor at once. It defaults to `1` and must be greater than or equal to `1`.

```python
async with CallbackExecutor(invocations, concurrency=10) as executor:
    async for result in executor:
        ...
```

The executor initially schedules up to the configured limit and schedules more invocations as results are retrieved. In ordered mode, a completed result that is waiting behind an earlier invocation still occupies a pending slot until it is yielded.

With `concurrency=1`, callbacks execute one at a time. With a value greater than `1`, multiple asynchronous callbacks can be in progress concurrently.

## Result ordering

Choose an `ExecutionPolicy` with the `execution_policy` argument.

### `ExecutionPolicy.SEQUENTIAL` (default)

Results are yielded in the same order as the input invocations.

This policy controls **result ordering**, not whether callbacks may run concurrently. When `concurrency` is greater than `1`, later callbacks can start and finish while an earlier callback is still running, but their results are held until earlier results have been yielded.

```python
async with CallbackExecutor(
    invocations,
    concurrency=5,
    execution_policy=ExecutionPolicy.SEQUENTIAL,
) as executor:
    async for result in executor:
        print(result)
```

### `ExecutionPolicy.FIRST_COMPLETED`

Results are yielded as soon as the executor observes that their callbacks have completed, regardless of their original input position.

```python
async with CallbackExecutor(
    invocations,
    concurrency=5,
    execution_policy=ExecutionPolicy.FIRST_COMPLETED,
) as executor:
    async for result in executor:
        print(result)
```

If multiple pending tasks are already complete when the executor selects the next result, it selects the first completed task in its pending order (which follows the invocation order for tasks that are still pending).

## Exception handling

Choose an `ExceptionsPolicy` with the `exceptions_policy` argument.

### `ExceptionsPolicy.FAIL_FAST` (default)

When a callback raises an `Exception`:

1. The exception is recorded.
2. The executor cancels the other pending callback tasks.
3. No further invocations are scheduled.
4. The executor waits for the task group to shut down and then propagates the recorded callback exception or exceptions.

This policy stops further work after the first observed callback failure. Because callbacks run concurrently, another callback may also fail before shutdown completes; those failures are recorded as well.

### `ExceptionsPolicy.COLLECT`

When a callback raises an `Exception`, the executor records the failure but does not cancel other callbacks because of it. It continues scheduling and executing the remaining invocations.

A failed invocation does not produce a result, so the iterator yields only successful callback results. After all invocations have been processed, the recorded failures are propagated during shutdown.

### How failures are raised

In both policies, the propagation format depends on the number of recorded callback exceptions:

- One exception is raised directly as its original exception type.
- Two or more exceptions are raised in an `ExceptionGroup`.

For example:

```python
from aiopulse import CallbackExecutor, ExceptionsPolicy


try:
    async with CallbackExecutor(
        invocations,
        concurrency=4,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    ) as executor:
        async for result in executor:
            print(result)
except ExceptionGroup as group:
    for error in group.exceptions:
        print(f"Callback failed: {error!r}")
except Exception as error:
    # A single callback exception is propagated directly.
    print(f"Callback failed: {error!r}")
```

Because `ExceptionGroup` is a Python 3.11 feature, it is available under the minimum supported Python version.

## API reference

### `invoke(callback)`

Decorator that converts an asynchronous callback into an invocation factory.

- Preserves the callback's parameter signature for callers and type checkers.
- Calling the decorated function creates an invocation object.
- Does not execute the callback.

### `CallbackExecutor(invocations, concurrency=1, execution_policy=ExecutionPolicy.SEQUENTIAL, exceptions_policy=ExceptionsPolicy.FAIL_FAST)`

Executes an iterable of callback invocations and yields successful results.

| Argument | Description |
| --- | --- |
| `invocations` | Iterable of callback invocation objects to execute. |
| `concurrency` | Maximum number of pending callback tasks. Must be at least `1`. |
| `execution_policy` | Controls the order in which successful results are yielded. |
| `exceptions_policy` | Controls how callback exceptions affect execution and when they are propagated. |

`CallbackExecutor` implements the asynchronous iterator protocol (`__aiter__` and `__anext__`) and asynchronous context manager protocol (`__aenter__` and `__aexit__`). It also exposes `aenter()` and `aclose()` for explicit lifecycle management.

### `ExecutionPolicy`

- `ExecutionPolicy.SEQUENTIAL` — yield results in invocation order.
- `ExecutionPolicy.FIRST_COMPLETED` — yield results as completed work is observed.

### `ExceptionsPolicy`

- `ExceptionsPolicy.FAIL_FAST` — cancel pending work after the first observed callback failure.
- `ExceptionsPolicy.COLLECT` — continue executing callbacks and propagate recorded failures when execution shuts down.

## Notes and limitations

- Only asynchronous callbacks that return awaitables are supported.
- The concurrency limit applies to callback tasks pending in the executor; it is not a thread or process pool.
- Callback failures raised as ordinary `Exception` instances are handled by the configured exception policy. Cancellation and other `BaseException` subclasses are handled by asyncio and `TaskGroup`, not collected as ordinary callback failures.
- Callback failures are raised by the executor, not yielded as result objects.
- Under `COLLECT`, successful results may already have been yielded before the collected callback failures are raised at shutdown.
- Under `FAIL_FAST`, concurrent callbacks may produce more than one recorded exception before shutdown completes.
- The executor is a single-use asynchronous iterator. Create a new `CallbackExecutor` for a new execution.