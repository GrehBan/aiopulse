from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest

from aiopulse import (
    CallbackExceptionGroup,
    CallbackExecutor,
    ExceptionsPolicy,
    invoke,
)
from aiopulse.callback import CallbackInvocation

from .helpers import unfinished_tasks


async def test_aenter_is_idempotent_and_enter_precedes_iteration() -> None:
    executed = 0

    @invoke
    async def callback() -> int:
        nonlocal executed
        executed += 1
        return 1

    executor = CallbackExecutor([callback()])
    assert await executor.__aenter__() is executor
    await executor.aenter()
    assert await anext(executor) == 1
    assert executed == 1
    await executor.aclose()


async def test_aenter_after_close_raises_runtime_error() -> None:
    executor = CallbackExecutor([])
    await executor.aclose()

    with pytest.raises(RuntimeError, match="CallbackExecutor is closed"):
        await executor.aenter()


async def test_aclose_before_entry_prevents_execution() -> None:
    executed = 0

    @invoke
    async def callback() -> int:
        nonlocal executed
        executed += 1
        return 1

    executor = CallbackExecutor([callback()])
    await executor.aclose()
    await executor.aclose()

    assert executed == 0
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


async def test_aclose_cancels_pending_and_waits_for_cleanup() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    cleaned = asyncio.Event()

    @invoke
    async def callback() -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return 1
        finally:
            cancelled.set()
            await asyncio.sleep(0)
            cleaned.set()

    executor = CallbackExecutor([callback()])
    await executor.aenter()
    await started.wait()
    await asyncio.wait_for(executor.aclose(), 1)

    assert cancelled.is_set()
    assert cleaned.is_set()
    assert not unfinished_tasks()
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


async def test_concurrent_aclose_calls_are_safe() -> None:
    started = asyncio.Event()

    @invoke
    async def callback() -> int:
        started.set()
        await asyncio.Event().wait()
        return 1

    executor = CallbackExecutor([callback()])
    await executor.aenter()
    await started.wait()

    await asyncio.gather(executor.aclose(), executor.aclose())
    assert not unfinished_tasks()


async def test_explicit_aclose_propagates_recorded_failure() -> None:
    error = RuntimeError("explicit close")
    failed = asyncio.Event()

    @invoke
    async def callback() -> int:
        failed.set()
        raise error

    executor = CallbackExecutor(
        [callback()],
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )
    await executor.aenter()
    await failed.wait()

    with pytest.raises(CallbackExceptionGroup) as caught:
        await executor.aclose()
    assert caught.value.exceptions == (error,)


async def test_explicit_aclose_without_failure_is_clean() -> None:
    @invoke
    async def callback() -> int:
        return 1

    executor = CallbackExecutor([callback()])
    await executor.aenter()
    await executor.aclose()
    await executor.aclose()

    assert not unfinished_tasks()


async def test_async_context_exit_cancels_remaining_work() -> None:
    started = asyncio.Event()
    cleaned = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return index
        finally:
            cleaned.set()

    async with CallbackExecutor(
        [callback(0), callback(1)],
        concurrency=2,
    ):
        await started.wait()

    assert cleaned.is_set()
    assert not unfinished_tasks()


async def test_async_for_break_cancels_remaining_work() -> None:
    cleaned = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            return index
        try:
            await asyncio.Event().wait()
            return index
        finally:
            cleaned.set()

    async with CallbackExecutor(
        [callback(0), callback(1)],
        concurrency=2,
    ) as executor:
        async for result in executor:
            assert result == 0
            break

    assert cleaned.is_set()
    assert not unfinished_tasks()


async def test_regression_consumer_exception_is_not_wrapped() -> None:
    callback_error = RuntimeError("callback")
    consumer_error = LookupError("consumer")

    @invoke
    async def callback() -> int:
        await asyncio.sleep(0)
        raise callback_error

    executor = CallbackExecutor(
        [callback()],
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    with pytest.raises(LookupError) as caught:
        async with executor:
            await asyncio.sleep(0)
            raise consumer_error

    assert caught.value is consumer_error


async def test_consumer_exception_waits_for_callback_cleanup() -> None:
    started = asyncio.Event()
    cleaned = asyncio.Event()
    consumer_error = LookupError("consumer")

    @invoke
    async def callback() -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return 1
        finally:
            cleaned.set()

    with pytest.raises(LookupError) as caught:
        async with CallbackExecutor([callback()]):
            await started.wait()
            raise consumer_error

    assert caught.value is consumer_error
    assert cleaned.is_set()
    assert not unfinished_tasks()


async def test_consumer_cancellation_cleans_up_pending_callback() -> None:
    started = asyncio.Event()
    cleaned = asyncio.Event()

    @invoke
    async def callback() -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return 1
        finally:
            cleaned.set()

    executor = CallbackExecutor([callback()])
    consumer = asyncio.create_task(anext(executor))
    await started.wait()
    consumer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await consumer
    assert cleaned.is_set()
    assert not unfinished_tasks()
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


async def test_repeated_cancellation_allows_aclose_to_finish_cleanup() -> None:
    callback_tasks: list[asyncio.Task[Any]] = []
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    @invoke
    async def callback() -> int:
        task = asyncio.current_task()
        assert task is not None
        callback_tasks.append(task)
        started.set()
        try:
            await asyncio.Event().wait()
            return 1
        finally:
            cleanup_started.set()
            while not release_cleanup.is_set():
                try:
                    await release_cleanup.wait()
                except asyncio.CancelledError:
                    continue
            cleanup_finished.set()

    executor = CallbackExecutor([callback()])
    consumer = asyncio.create_task(anext(executor))
    await started.wait()

    try:
        consumer.cancel()
        await cleanup_started.wait()
        consumer.cancel()

        close_task = asyncio.create_task(executor.aclose())
        await asyncio.sleep(0)
        assert not close_task.done()
    finally:
        release_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await consumer
    await asyncio.wait_for(close_task, 1)
    assert cleanup_finished.is_set()
    assert callback_tasks[0].cancelled()
    assert not unfinished_tasks()


async def test_input_iterator_exception_cancels_scheduled_tasks() -> None:
    iterator_error = LookupError("iterator failed")

    @invoke
    async def callback() -> int:
        await asyncio.Event().wait()
        return 1

    def source() -> Iterator[CallbackInvocation[[], int]]:
        yield callback()
        raise iterator_error

    executor = CallbackExecutor(source(), concurrency=2)

    with pytest.raises(LookupError) as caught:
        await executor.aenter()
    assert caught.value is iterator_error
    assert not unfinished_tasks()
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


async def test_regression_task_creation_failure_closes_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creation_error = OSError("cannot create task")

    @invoke
    async def callback() -> int:
        return 1

    executor = CallbackExecutor([callback()], concurrency=2)

    def fail_create_task(*args: Any, **kwargs: Any) -> Any:
        raise creation_error

    monkeypatch.setattr(executor._task_group, "create_task", fail_create_task)

    with pytest.raises(OSError) as caught:
        await executor.aenter()
    assert caught.value is creation_error
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


async def test_regression_task_creation_failure_explicitly_closes_coroutine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creation_error = OSError("cannot create task")

    class DummyExecution:
        closed = False

        def close(self) -> None:
            self.closed = True

    @invoke
    async def callback() -> int:
        return 1

    executor = CallbackExecutor([callback()])
    execution = DummyExecution()

    def execute_stub(_index: int, _invocation: object) -> DummyExecution:
        return execution

    def fail_create_task(*args: Any, **kwargs: Any) -> Any:
        raise creation_error

    monkeypatch.setattr(executor, "_execute", execute_stub)
    monkeypatch.setattr(executor._task_group, "create_task", fail_create_task)

    with pytest.raises(OSError) as caught:
        await executor.aenter()
    assert caught.value is creation_error
    assert execution.closed


async def test_aclose_from_another_task_stops_waiting_consumer() -> None:
    started = asyncio.Event()

    @invoke
    async def callback() -> int:
        started.set()
        await asyncio.Event().wait()
        return 1

    executor = CallbackExecutor([callback()])
    consumer = asyncio.create_task(anext(executor))
    await started.wait()

    await executor.aclose()

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(consumer, 1)
    assert not unfinished_tasks()
