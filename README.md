# aiopulse

**A lightweight asynchronous callback invocation and execution library for Python.**

`aiopulse` provides a simple way to prepare asynchronous callback invocations and execute them concurrently with bounded concurrency.

It preserves invocation order when yielding results, regardless of completion order, and provides fail-fast execution semantics with structured task management through `asyncio.TaskGroup`.

## Features

- **Invocation Factory** — convert asynchronous callbacks into factories that create prepared invocations.
- **Individual Arguments** — each invocation carries its own positional and keyword arguments.
- **Bounded Concurrency** — control the maximum number of concurrently executing callbacks.
- **Ordered Results** — results are yielded in invocation order, regardless of completion order.
- **Fail-Fast Execution** — callback failures trigger cancellation of other in-flight invocations.
- **Structured Concurrency** — manage callback task lifetimes using `asyncio.TaskGroup`.
- **Async Iteration** — consume results naturally using `async for`.
- **Explicit Lifecycle Management** — manage execution through asynchronous context managers.
- **Flexible Typing** — preserve callback signatures and support heterogeneous callback signatures and result types.

## Installation

### From source

Clone the repository and install the project dependencies:

```bash
git clone https://github.com/GrehBan/aiopulse.git
cd aiopulse
uv sync
```

### Install directly from GitHub

Using `uv`:

```bash
uv pip install "git+https://github.com/GrehBan/aiopulse.git"
```

Using `pip`:

```bash
pip install "git+https://github.com/GrehBan/aiopulse.git"
```

To add `aiopulse` as a project dependency with `uv`:

```bash
uv add "aiopulse @ git+https://github.com/GrehBan/aiopulse.git"
```

## Concepts

`aiopulse` is built around three main concepts:

- `Callback` — an asynchronous callable.
- `CallbackInvocation` — a prepared callback invocation containing its arguments.
- `CallbackExecutor` — an execution engine that manages concurrent invocation execution.

The library separates invocation preparation from execution. Preparing an invocation does not execute the underlying callback.

### Callback

A callback is an asynchronous callable that accepts arguments and returns an awaitable result.

```python
async def greet(name: str) -> str:
    return f"Hello, {name}!"
```

Callbacks can have different signatures and return types. They do not need to share a common parameter specification.

### CallbackInvocation

`CallbackInvocation` encapsulates a callback together with the arguments required to execute it.

```python
from aiopulse.callback import CallbackInvocation

invocation = CallbackInvocation(
    callback=greet,
    args=("Alice",),
    kwargs={},
)
```

A `CallbackInvocation` represents a prepared operation. It can be executed by awaiting it:

```python
result = await invocation()
```

Alternatively, invocations can be passed to `CallbackExecutor` for concurrent execution.

### invoke

The `invoke` decorator converts an asynchronous callback into an invocation factory.

```python
from aiopulse import invoke


@invoke
async def greet(name: str) -> str:
    return f"Hello, {name}!"
```

The decorated callable preserves the original callback's parameter signature and creates a `CallbackInvocation` when called.

```python
invocation = greet("Alice")
```

This does not execute `greet`. It only prepares the invocation.

Arguments are validated by static type checkers against the original callback signature.

Each invocation stores its own arguments, allowing the same callback to be prepared with different inputs.

```python
invocations = [
    greet("Alice"),
    greet("Bob"),
    greet("Charlie"),
]
```

## CallbackExecutor

`CallbackExecutor` consumes an iterable of prepared `CallbackInvocation` objects and executes them concurrently.

```python
from aiopulse import CallbackExecutor

executor = CallbackExecutor(
    invocations,
    concurrency=3,
)
```

The `concurrency` parameter defines the maximum number of callback invocations that can execute simultaneously.

The executor is an asynchronous iterator and can be used with `async for`.

```python
async with executor:
    async for result in executor:
        print(result)
```

## Execution Model

### Bounded Concurrency

The executor maintains a collection of in-flight tasks and schedules invocations up to the configured concurrency limit.

When a result is consumed, the executor schedules additional invocations to fill available execution slots.

For example, with `concurrency=3`, no more than three callback invocations are scheduled simultaneously.

Setting `concurrency=1` results in sequential execution.

### Ordered Results

Callbacks can complete in a different order from their invocation order.

However, `CallbackExecutor` always yields results in the original invocation order.

Consider the following invocations:

| Invocation | Execution time |
|---|---:|
| First | 300 ms |
| Second | 100 ms |
| Third | 200 ms |

With `concurrency=3`, the completion order is:

1. Second
2. Third
3. First

But the executor yields results in this order:

1. First
2. Second
3. Third

This allows concurrent execution while preserving deterministic result consumption.

### Fail-Fast Execution

The executor follows fail-fast semantics.

When an in-flight callback raises an exception:

1. The first observed failure is recorded.
2. Other in-flight callbacks receive cancellation requests.
3. No additional invocations are scheduled.
4. The original failure is propagated to the consumer.
5. The task group is closed as part of executor cleanup.

Cancellation follows Python's cooperative cancellation model. A callback that suppresses `asyncio.CancelledError` may delay executor shutdown.

### Task Lifecycle

Callback tasks are managed using `asyncio.TaskGroup`.

The executor supports asynchronous context management:

```python
async with executor:
    async for result in executor:
        ...
```

The context manager ensures that remaining in-flight work is cancelled when execution is interrupted or the consumer exits early.

The executor also closes automatically after normal iteration exhaustion.

## Complete Example

```python
import asyncio

from aiopulse import invoke, CallbackExecutor


@invoke
async def fetch_user(user_id: int) -> str:
    await asyncio.sleep(0.3)
    return f"User: {user_id}"


@invoke
async def calculate(a: int, b: int) -> str:
    await asyncio.sleep(0.1)
    return f"Result: {a + b}"


@invoke
async def send_notification(
    recipient: str,
    message: str,
    *,
    urgent: bool = False,
) -> str:
    await asyncio.sleep(0.2)
    priority = "urgent" if urgent else "normal"
    return f"Notification to {recipient} ({priority}): {message}"


async def main() -> None:
    # Prepare individual callback invocations.
    invocations = [
        fetch_user(42),
        calculate(10, 20),
        send_notification(
            "Alice",
            "Your report is ready",
            urgent=True,
        ),
        fetch_user(100),
    ]

    # Create an executor with a concurrency limit of 2.
    executor = CallbackExecutor(
        invocations,
        concurrency=2,
    )

    # Execute callbacks and consume results in invocation order.
    async with executor:
        async for result in executor:
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

Output:

```text
User: 42
Result: 30
Notification to Alice (urgent): Your report is ready
User: 100
```

Although callbacks may complete in a different order, results are always yielded in the order in which their invocations were supplied.

## Heterogeneous Callbacks

Callbacks do not need to share the same parameter signature.

For example:

```python
@invoke
async def get_user(user_id: int) -> User:
    ...


@invoke
async def calculate(a: float, b: float) -> int:
    ...


@invoke
async def is_available(resource: str) -> bool:
    ...
```

All three can be included in the same executor.

Their arguments are checked independently, while the executor manages their execution uniformly.

The executor can also be parameterized with a common result type or a union of possible result types:

```python
Result = User | int | bool

invocations = [
    get_user(42),
    calculate(10.5, 20.5),
    is_available("worker"),
]

executor: CallbackExecutor[Result] = CallbackExecutor(
    invocations,
    concurrency=3,
)
```

This preserves useful result typing without requiring callbacks to have identical signatures.

## API Reference

### `CallbackInvocation`

| Member | Description |
|---|---|
| `callback` | The asynchronous callback to execute. |
| `args` | Positional arguments for the callback. |
| `kwargs` | Keyword arguments for the callback. |
| `__call__()` | Execute the prepared invocation asynchronously. |

### `invoke`

| Parameter | Description |
|---|---|
| `callback` | The asynchronous callback to wrap. |

Returns a callable that preserves the callback's parameter signature and creates `CallbackInvocation` objects.

### `CallbackExecutor`

| Parameter / Member | Description |
|---|---|
| `invocations` | Iterable of prepared callback invocations. |
| `concurrency` | Maximum number of concurrently executing callbacks. |
| `entered` | Indicates whether the underlying task group has been entered. |
| `closed` | Indicates whether the executor is closed. |
| `exhausted` | Indicates whether the invocation iterator is exhausted. |
| `aenter()` | Enter the executor and start scheduling invocations. |
| `aclose()` | Cancel remaining tasks and close the executor. |
| `__aiter__()` | Return the executor as an asynchronous iterator. |
| `__anext__()` | Retrieve the next result in invocation order. |

## Design Principles

`aiopulse` is built around a few core principles:

- **Separation of preparation and execution:** creating an invocation does not execute the callback.
- **Independent invocations:** every callback invocation owns its arguments and execution state.
- **Bounded concurrency:** concurrent execution is explicitly configurable.
- **Deterministic consumption:** results preserve invocation order.
- **Structured task management:** callback tasks are managed through `asyncio.TaskGroup`.
- **Minimal abstraction:** the API builds on native Python callables, asynchronous iteration, and context management.

`aiopulse` aims to provide a small and reusable foundation for applications and libraries that need controlled concurrent execution of asynchronous callables.

It does not impose a particular callback registration system, application architecture, or execution domain. Consumers are free to prepare invocations from their own registries, event systems, plugin managers, or other sources.

## LICENSE

MIT.
