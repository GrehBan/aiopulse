# aiopulse

**Lightweight asynchronous callback invocation and bounded-concurrency execution.**

`aiopulse` lets you describe asynchronous callback calls as invocation objects and execute them through a single asynchronous interface. It provides bounded task scheduling, configurable result ordering, and configurable callback exception handling.

## Features

- **Explicit invocations** — use `@invoke` to turn an asynchronous function into an invocation factory. Calling the decorated function stores its arguments without executing the callback.
- **Bounded concurrency** — limit how many callbacks execute at the same time and how many unconsumed results may wait.
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

The executor runs up to three callbacks at a time. Results are yielded in the original invocation order by default, even though the callbacks may finish in a different order.

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
```

`first` and `second` describe calls; their callbacks have not run yet.

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

Leaving the context closes the executor, cancels unfinished callback tasks, and waits for the underlying task group to finish. Each callback task is cancelled at most once, so a callback can await its cleanup in a `finally` block without a second cancellation interrupting it. This ensures that callbacks do not remain running after the consumer has left the block. When no exception is propagating from the block, leaving it also raises the recorded callback failures (see [Exception handling](#exception-handling)).

If the iterator is consumed until exhaustion without an explicit context manager, the executor closes itself when there are no pending callbacks left. For explicit shutdown, call:

```python
await executor.aclose()
```

An executor instance represents one execution and cannot be restarted after it has been closed.

## Concurrency

The `concurrency` argument sets the maximum number of callbacks executing at
the same time. It defaults to `1` and must be greater than or equal to `1`.
`buffer_size` sets additional capacity for pending callbacks, including
completed results that have not yet been consumed. It defaults to `0` and
cannot be negative.

```python
async with CallbackExecutor(
    invocations,
    concurrency=10,
    buffer_size=5,
) as executor:
    async for result in executor:
        ...
```

The executor initially schedules up to the configured concurrency limit.
It schedules more invocations only while it is being iterated: when a result
is consumed, and when a callback finishes while the consumer waits for the
next result. While the consumer is busy between results, finished callbacks
are not replaced.

At most `concurrency + buffer_size` tasks can be pending. A finished callback
occupies a pending slot until iteration reaches it: its result is yielded, or
its failed outcome is skipped. Under `SEQUENTIAL` this includes results
waiting behind an earlier invocation. Buffer capacity does not increase the
number of callbacks executing simultaneously.

With `concurrency=1`, callbacks execute one at a time. With a value greater
than `1`, multiple asynchronous callbacks can be in progress concurrently.

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

If several tasks have already completed when the executor selects the next result, they are yielded in the order in which the executor observed their completion.

## Exception handling

Choose an `ExceptionsPolicy` with the `exceptions_policy` argument.

### `ExceptionsPolicy.FAIL_FAST` (default)

When a callback fails (it raises an `Exception` or ends with a cancellation the executor did not request):

1. The failure is recorded.
2. The executor cancels the other unfinished callback tasks.
3. No further invocations are scheduled.
4. On the next request for a result, or when the context is left, the executor closes: it waits for the cancelled callbacks to finish and then raises the recorded failures in a `CallbackExceptionGroup`. Results that were ready but not yet yielded are not returned.

This policy stops further work after the first observed callback failure. Because callbacks run concurrently, another callback may also fail before shutdown completes; those failures are recorded as well. The callbacks that the executor cancels are not failures.

### `ExceptionsPolicy.COLLECT`

When a callback fails, the executor records the failure but does not cancel other callbacks because of it. It continues scheduling and executing the remaining invocations.

A failed invocation does not produce a result, so the iterator yields only successful callback results. The recorded failures are raised when the executor closes: after the last result, or when the context is left earlier.

### How failures are raised

In both policies, recorded failures are raised together in one
`CallbackExceptionGroup`, even when only one callback failed. It is an
`ExceptionGroup` subtype that holds the original exceptions, in the order the
executor observed them, so `except*` works as usual. Its `failures` attribute
holds one `CallbackFailure` record per exception, in the same order: the
zero-based invocation index, the original invocation and the exception.
Subgroups created by `split()`, `subgroup()` and `except*` keep the records
of their exceptions.

A callback that ends with a cancellation the executor did not request, for
example by awaiting a cancelled future or task or by cancelling itself, is
recorded as a `CallbackCancelledError` failure, with the original
`asyncio.CancelledError` as its `__cause__`. It is therefore never mistaken
for a cancellation of the code consuming the results. Callbacks that the
executor cancels itself, on close or under `FAIL_FAST`, are not failures.

An exception raised inside the `async with` block, including the `async for`
body, takes precedence: it propagates unchanged and the recorded callback
failures are discarded.

For example:

```python
from aiopulse import CallbackExceptionGroup, CallbackExecutor, ExceptionsPolicy


try:
    async with CallbackExecutor(
        invocations,
        concurrency=4,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    ) as executor:
        async for result in executor:
            print(result)
except CallbackExceptionGroup as group:
    for failure in group.failures:
        print(f"Invocation {failure.index} failed: {failure.exception!r}")
```

Failures can also be handled by type with `except*`:

```python
try:
    async with CallbackExecutor(invocations, concurrency=4) as executor:
        async for result in executor:
            print(result)
except* TimeoutError as group:
    print(f"{len(group.exceptions)} callback(s) timed out")
```

Because `ExceptionGroup` is a Python 3.11 feature, it is available under the minimum supported Python version.

## API reference

### `invoke(callback)`

Decorator that converts an asynchronous callback into an invocation factory.

- Preserves the callback's parameter signature for callers and type checkers.
- Calling the decorated function creates an invocation object.
- Does not execute the callback.

### `CallbackExecutor(invocations, concurrency=1, buffer_size=0, execution_policy=ExecutionPolicy.SEQUENTIAL, exceptions_policy=ExceptionsPolicy.FAIL_FAST)`

Executes an iterable of callback invocations and yields successful results.

| Argument | Description |
| --- | --- |
| `invocations` | Iterable of callback invocation objects to execute. |
| `concurrency` | Maximum number of callbacks executing simultaneously. Must be at least `1`. |
| `buffer_size` | Additional pending-task capacity, including completed but unconsumed tasks. Must be non-negative. |
| `execution_policy` | Controls the order in which successful results are yielded. |
| `exceptions_policy` | Controls how callback failures affect execution. |

`CallbackExecutor` implements the asynchronous iterator protocol (`__aiter__` and `__anext__`) and asynchronous context manager protocol (`__aenter__` and `__aexit__`). It also exposes `aenter()` and `aclose()` for explicit lifecycle management.

### `ExecutionPolicy`

- `ExecutionPolicy.SEQUENTIAL` — yield results in invocation order.
- `ExecutionPolicy.FIRST_COMPLETED` — yield results as completed work is observed.

### `ExceptionsPolicy`

- `ExceptionsPolicy.FAIL_FAST` — cancel unfinished callbacks and stop scheduling after the first recorded failure.
- `ExceptionsPolicy.COLLECT` — continue executing callbacks and raise recorded failures when execution shuts down.

### `CallbackExceptionGroup`

`ExceptionGroup` subtype raised for callback failures. Its `failures` attribute is a tuple of `CallbackFailure` records, one per member exception and in the same order. Its type parameters are the member exception type and the callback result type.

### `CallbackFailure`

Frozen record of one failed invocation: `index` (zero-based position in the input), `invocation` (the original invocation, which can be inspected or retried) and `exception` (the exception raised by the callback, or a `CallbackCancelledError`).

### `CallbackCancelledError`

`Exception` recorded when a callback ends with a cancellation that the executor did not request. The original `asyncio.CancelledError` is its `__cause__`.

## Notes and limitations

- Only asynchronous callbacks that return awaitables are supported.
- The limits apply to asyncio tasks in the running event loop; `aiopulse` is not a thread or process pool.
- The exception policy handles callback failures: ordinary `Exception` instances and cancellations that the executor did not request (`CallbackCancelledError`). `KeyboardInterrupt` and `SystemExit` are re-raised by asyncio out of the event loop. Any other `BaseException` subclass aborts the execution: the task group cancels the remaining callbacks and the exception is raised in a `BaseExceptionGroup`.
- The task that first enters the executor (the `async with` block, `aenter()`, or the first `anext()`) owns its task group. When a callback raises such a `BaseException`, the task group also cancels that task, so enter and consume an executor from the same task.
- Callback failures are raised by the executor, not yielded as result objects.
- Under `COLLECT`, successful results may already have been yielded before the collected callback failures are raised at shutdown.
- Under `FAIL_FAST`, concurrent callbacks may produce more than one recorded exception before shutdown completes.
- The executor is a single-use asynchronous iterator. Create a new `CallbackExecutor` for a new execution.