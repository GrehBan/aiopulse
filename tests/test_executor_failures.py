from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator

import pytest

from aiopulse import (
    CallbackCancelledError,
    CallbackExceptionGroup,
    CallbackExecutor,
    CallbackFailure,
    ExceptionsPolicy,
    ExecutionPolicy,
    invoke,
)
from aiopulse.callback import CallbackInvocation

from .helpers import collect, unfinished_tasks


async def test_fail_fast_raises_single_failure_as_group() -> None:
    error = RuntimeError("first failure")

    @invoke
    async def fail() -> int:
        raise error

    @invoke
    async def blocked() -> int:
        await asyncio.Event().wait()
        return 2

    invocations = [fail(), blocked()]
    executor = CallbackExecutor(
        invocations,
        concurrency=2,
        exceptions_policy=ExceptionsPolicy.FAIL_FAST,
    )

    with pytest.raises(CallbackExceptionGroup) as caught:
        await collect(executor)
    assert caught.value.exceptions == (error,)
    assert caught.value.failures == (
        CallbackFailure(0, invocations[0], error),
    )
    assert not unfinished_tasks()


@pytest.mark.parametrize("execution_policy", list(ExecutionPolicy))
async def test_fail_fast_cancels_pending_and_runs_cleanup(
    execution_policy: ExecutionPolicy,
) -> None:
    failing = asyncio.Event()
    cancelled = asyncio.Event()
    cleanup_finished = asyncio.Event()

    @invoke
    async def fail() -> int:
        await failing.wait()
        raise RuntimeError("fail")

    @invoke
    async def pending() -> int:
        try:
            await asyncio.Event().wait()
            return 2
        finally:
            cancelled.set()
            await asyncio.sleep(0)
            cleanup_finished.set()

    executor = CallbackExecutor(
        [pending(), fail()],
        concurrency=2,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.FAIL_FAST,
    )
    await executor.aenter()
    failing.set()

    with pytest.raises(CallbackExceptionGroup) as caught:
        await anext(executor)
    assert [str(error) for error in caught.value.exceptions] == ["fail"]
    assert cancelled.is_set()
    assert cleanup_finished.is_set()
    assert not unfinished_tasks()


async def test_fail_fast_records_failures_raised_while_cancelling() -> None:
    first = ValueError("first")
    second = LookupError("second")
    started = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            await started.wait()
            raise first
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise second from None
        return index

    invocations = [callback(0), callback(1)]
    executor = CallbackExecutor(invocations, concurrency=2)

    with pytest.raises(CallbackExceptionGroup) as caught:
        await collect(executor)
    assert caught.value.exceptions == (first, second)
    assert caught.value.failures == (
        CallbackFailure(0, invocations[0], first),
        CallbackFailure(1, invocations[1], second),
    )


@pytest.mark.parametrize("execution_policy", list(ExecutionPolicy))
async def test_fail_fast_does_not_schedule_after_failure(
    execution_policy: ExecutionPolicy,
) -> None:
    consumed: list[int] = []

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            raise RuntimeError("stop")
        return index

    def source() -> Iterator[CallbackInvocation[[int], int]]:
        for index in range(3):
            consumed.append(index)
            yield callback(index)

    executor = CallbackExecutor(
        source(),
        concurrency=1,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.FAIL_FAST,
    )

    with pytest.raises(CallbackExceptionGroup) as caught:
        await collect(executor)
    assert [str(error) for error in caught.value.exceptions] == ["stop"]
    assert consumed == [0]
    assert not unfinished_tasks()


@pytest.mark.parametrize("execution_policy", list(ExecutionPolicy))
async def test_collect_skips_failures_and_yields_successes(
    execution_policy: ExecutionPolicy,
) -> None:
    error = ValueError("bad")

    @invoke
    async def callback(index: int) -> int:
        if index == 1:
            raise error
        return index

    invocations = [callback(0), callback(1), callback(2)]
    executor = CallbackExecutor(
        invocations,
        concurrency=2,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    results: list[int] = []
    with pytest.raises(CallbackExceptionGroup) as caught:
        async for result in executor:
            results.append(result)  # noqa: PERF401

    assert caught.value.exceptions == (error,)
    assert caught.value.failures == (
        CallbackFailure(1, invocations[1], error),
    )
    assert results == [0, 2]


@pytest.mark.parametrize("execution_policy", list(ExecutionPolicy))
async def test_collect_groups_multiple_failures(
    execution_policy: ExecutionPolicy,
) -> None:
    first = ValueError("first")
    second = LookupError("second")

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            raise first
        if index == 1:
            raise second
        return index

    invocations = [callback(0), callback(1), callback(2)]
    executor = CallbackExecutor(
        invocations,
        concurrency=3,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    results: list[int] = []
    with pytest.raises(CallbackExceptionGroup) as caught:
        async for result in executor:
            results.append(result)  # noqa: PERF401

    assert results == [2]
    assert caught.value.exceptions == (first, second)
    assert caught.value.failures == (
        CallbackFailure(0, invocations[0], first),
        CallbackFailure(1, invocations[1], second),
    )


@pytest.mark.parametrize("execution_policy", list(ExecutionPolicy))
async def test_collect_all_failures_without_successful_results(
    execution_policy: ExecutionPolicy,
) -> None:
    errors = [ValueError("one"), LookupError("two"), RuntimeError("three")]

    @invoke
    async def fail(index: int) -> int:
        raise errors[index]

    executor = CallbackExecutor(
        [fail(index) for index in range(len(errors))],
        concurrency=2,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    with pytest.raises(CallbackExceptionGroup) as caught:
        await collect(executor)

    assert caught.value.exceptions == tuple(errors)


async def test_collect_empty_and_all_success_have_no_exception() -> None:
    empty = CallbackExecutor([], exceptions_policy=ExceptionsPolicy.COLLECT)
    assert await collect(empty) == []

    @invoke
    async def callback(value: int) -> int:
        return value

    success = CallbackExecutor(
        [callback(1), callback(2)],
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )
    assert await collect(success) == [1, 2]


async def test_base_exception_is_not_converted_to_callback_failure() -> None:
    class FatalCallbackError(BaseException):
        pass

    error = FatalCallbackError("fatal")

    @invoke
    async def callback() -> int:
        raise error

    executor = CallbackExecutor(
        [callback()],
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    with pytest.raises(BaseExceptionGroup) as caught:
        await collect(executor)

    assert caught.value.exceptions == (error,)
    assert not unfinished_tasks()


async def await_cancelled_future() -> None:
    future = asyncio.get_running_loop().create_future()
    future.cancel()
    await future


async def cancel_own_task() -> None:
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    await asyncio.sleep(0)


@pytest.mark.parametrize("cancel", [await_cancelled_future, cancel_own_task])
@pytest.mark.parametrize("exceptions_policy", list(ExceptionsPolicy))
@pytest.mark.parametrize("execution_policy", list(ExecutionPolicy))
async def test_regression_unrequested_cancellation_is_callback_failure(
    execution_policy: ExecutionPolicy,
    exceptions_policy: ExceptionsPolicy,
    cancel: Callable[[], Awaitable[None]],
) -> None:
    @invoke
    async def callback(index: int) -> int:
        if index == 1:
            await cancel()
        await asyncio.sleep(0)
        return index

    invocations = [callback(index) for index in range(3)]
    executor = CallbackExecutor(
        invocations,
        concurrency=2,
        execution_policy=execution_policy,
        exceptions_policy=exceptions_policy,
    )

    results: list[int] = []
    with pytest.raises(CallbackExceptionGroup) as caught:
        async for result in executor:
            results.append(result)  # noqa: PERF401

    consumer = asyncio.current_task()
    assert consumer is not None
    assert consumer.cancelling() == 0
    (failure,) = caught.value.failures
    assert failure.index == 1
    assert failure.invocation is invocations[1]
    assert isinstance(failure.exception, CallbackCancelledError)
    assert isinstance(failure.exception.__cause__, asyncio.CancelledError)
    assert caught.value.exceptions == (failure.exception,)
    if exceptions_policy is ExceptionsPolicy.COLLECT:
        assert sorted(results) == [0, 2]
    assert not unfinished_tasks()


async def test_regression_unrequested_cancellation_reaches_caller_group() -> (
    None
):
    @invoke
    async def callback() -> int:
        await await_cancelled_future()
        return 1

    async def consume() -> None:
        await collect(CallbackExecutor([callback()]))

    with pytest.raises(ExceptionGroup) as caught:
        async with asyncio.TaskGroup() as group:
            group.create_task(consume())

    (error,) = caught.value.exceptions
    assert isinstance(error, CallbackExceptionGroup)
    assert isinstance(error.exceptions[0], CallbackCancelledError)


async def test_base_exception_cancels_siblings_without_recording_them() -> (
    None
):
    class FatalCallbackError(BaseException):
        pass

    error = FatalCallbackError("fatal")
    started = asyncio.Event()
    sibling_cancelled = asyncio.Event()
    sibling_tasks: list[asyncio.Task[object]] = []

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            await started.wait()
            raise error
        task = asyncio.current_task()
        assert task is not None
        sibling_tasks.append(task)
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise
        return index

    executor = CallbackExecutor(
        [callback(0), callback(1)],
        concurrency=2,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    with pytest.raises(BaseExceptionGroup) as caught:
        await collect(executor)

    assert caught.value.exceptions == (error,)
    assert sibling_cancelled.is_set()
    assert sibling_tasks[0].cancelled()
    assert not unfinished_tasks()
