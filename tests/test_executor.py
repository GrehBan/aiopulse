from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterable
from typing import Any

import pytest

from aiopulse import (
    CallbackExecutor,
    ExceptionsPolicy,
    ExecutionPolicy,
    invoke,
)


async def collect(executor: CallbackExecutor[Any]) -> list[Any]:
    return [result async for result in executor]


@pytest.mark.asyncio
@pytest.mark.parametrize("container", ["list", "tuple", "generator"])
async def test_executor_accepts_supported_iterables(
    container: str,
) -> None:
    @invoke
    async def callback(value: int) -> int:
        return value

    invocations = [callback(1), callback(2), callback(3)]
    if container == "list":
        source: Iterable[Any] = invocations
    elif container == "tuple":
        source = tuple(invocations)
    else:
        source = (item for item in invocations)

    executor = CallbackExecutor(source)
    assert await collect(executor) == [1, 2, 3]


@pytest.mark.asyncio
async def test_executor_does_not_consume_or_execute_during_init() -> None:
    consumed = 0
    executed = 0

    @invoke
    async def callback() -> int:
        nonlocal executed
        executed += 1
        return 1

    def source() -> Iterable[Any]:
        nonlocal consumed
        for _ in range(3):
            consumed += 1
            yield callback()

    executor = CallbackExecutor(source())

    assert consumed == 0
    assert executed == 0
    assert not executor.entered
    assert not executor.closed
    assert not executor.exhausted

    assert await collect(executor) == [1, 1, 1]
    assert consumed == 3
    assert executed == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [0, -1, -100])
async def test_executor_rejects_non_positive_concurrency(
    concurrency: int,
) -> None:
    @invoke
    async def callback() -> int:
        return 1

    with pytest.raises(
        ValueError,
        match="concurrency must be greater than or equal to 1",
    ):
        CallbackExecutor([callback()], concurrency=concurrency)


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 2, 10])
async def test_executor_accepts_valid_concurrency(concurrency: int) -> None:
    executor = CallbackExecutor([], concurrency=concurrency)
    assert executor._concurrency == concurrency


@pytest.mark.asyncio
async def test_enter_schedules_no_more_than_initial_concurrency() -> None:
    gates = [asyncio.Event() for _ in range(4)]
    started = [asyncio.Event() for _ in range(4)]

    @invoke
    async def callback(index: int) -> int:
        started[index].set()
        await gates[index].wait()
        return index

    executor = CallbackExecutor(
        [callback(index) for index in range(4)],
        concurrency=2,
    )

    await executor.aenter()
    await asyncio.gather(*(started[index].wait() for index in (0, 1)))

    assert len(executor._pending) == 2
    assert not started[2].is_set()
    assert not started[3].is_set()

    for gate in gates:
        gate.set()
    assert await collect(executor) == [0, 1, 2, 3]
    await executor.aclose()


@pytest.mark.asyncio
async def test_initial_scheduling_fills_available_slots() -> None:
    started = [asyncio.Event() for _ in range(3)]
    gate = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        started[index].set()
        await gate.wait()
        return index

    executor = CallbackExecutor(
        [callback(index) for index in range(3)],
        concurrency=5,
    )
    await executor.aenter()
    await asyncio.gather(*(event.wait() for event in started))

    assert len(executor._pending) == 3
    gate.set()
    assert await collect(executor) == [0, 1, 2]


@pytest.mark.asyncio
async def test_empty_executor_has_no_tasks_and_closes() -> None:
    executor = CallbackExecutor([])

    assert await collect(executor) == []
    assert executor.closed
    assert executor.exhausted
    assert not executor._pending


@pytest.mark.asyncio
async def test_single_invocation_executes_once() -> None:
    executions = 0

    @invoke
    async def callback() -> int:
        nonlocal executions
        executions += 1
        return 7

    executor = CallbackExecutor([callback()])

    assert await collect(executor) == [7]
    assert executions == 1
    assert executor.closed


@pytest.mark.asyncio
async def test_lazy_generator_is_consumed_as_slots_are_available() -> None:
    consumed: list[int] = []
    gate = asyncio.Event()
    started = [asyncio.Event(), asyncio.Event()]

    @invoke
    async def callback(index: int) -> int:
        if index < len(started):
            started[index].set()
        await gate.wait()
        return index

    def source() -> Iterable[Any]:
        for index in range(4):
            consumed.append(index)
            yield callback(index)

    executor = CallbackExecutor(source(), concurrency=2)
    await executor.aenter()
    await asyncio.gather(*(event.wait() for event in started))

    assert consumed == [0, 1]
    gate.set()
    assert await collect(executor) == [0, 1, 2, 3]
    assert consumed == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_completed_pending_task_still_occupies_slot() -> None:
    first_started = asyncio.Event()
    first_gate = asyncio.Event()
    active = 0
    peak = 0

    @invoke
    async def callback(index: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if index == 0:
            first_started.set()
            await first_gate.wait()
        await asyncio.sleep(0)
        active -= 1
        return index

    executor = CallbackExecutor(
        [callback(0), callback(1), callback(2)],
        concurrency=2,
    )
    await executor.aenter()
    await first_started.wait()
    first_gate.set()

    await asyncio.sleep(0)
    assert len(executor._pending) == 2
    assert peak <= 2

    assert await collect(executor) == [0, 1, 2]
    assert peak <= 2


@pytest.mark.asyncio
async def test_concurrency_never_exceeds_limit() -> None:
    active = 0
    peak = 0
    gates = [asyncio.Event() for _ in range(6)]
    starts = [asyncio.Event() for _ in range(6)]

    @invoke
    async def callback(index: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        starts[index].set()
        await gates[index].wait()
        active -= 1
        return index

    executor = CallbackExecutor(
        [callback(index) for index in range(6)],
        concurrency=3,
    )
    await executor.aenter()
    await asyncio.gather(*(event.wait() for event in starts[:3]))

    assert peak == 3
    for gate in gates:
        gate.set()

    assert await collect(executor) == [0, 1, 2, 3, 4, 5]
    assert peak == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("execution_policy", "expected"),
    [
        (ExecutionPolicy.SEQUENTIAL, [0, 1, 2, 3]),
        (ExecutionPolicy.FIRST_COMPLETED, [0, 1, 2, 3]),
    ],
)
async def test_sequential_and_first_completed_policies(
    execution_policy: ExecutionPolicy,
    expected: list[int],
) -> None:
    gates = [asyncio.Event() for _ in range(4)]

    @invoke
    async def callback(index: int) -> int:
        await gates[index].wait()
        return index

    executor = CallbackExecutor(
        [callback(index) for index in range(4)],
        concurrency=4,
        execution_policy=execution_policy,
    )

    async def consume() -> list[int]:
        return [result async for result in executor]

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)
    for gate in gates:
        gate.set()

    assert await asyncio.wait_for(consumer, 1) == expected


@pytest.mark.asyncio
async def test_sequential_waits_for_earlier_result() -> None:
    first_gate = asyncio.Event()
    second_done = asyncio.Event()

    @invoke
    async def first() -> int:
        await first_gate.wait()
        return 1

    @invoke
    async def second() -> int:
        second_done.set()
        return 2

    executor = CallbackExecutor(
        [first(), second()],
        concurrency=2,
        execution_policy=ExecutionPolicy.SEQUENTIAL,
    )

    await executor.aenter()
    await second_done.wait()
    assert not executor.closed
    first_gate.set()
    assert await anext(executor) == 1
    assert await anext(executor) == 2
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


@pytest.mark.asyncio
async def test_regression_first_completed_returns_last_completed_task() -> (
    None
):
    gates = [asyncio.Event(), asyncio.Event(), asyncio.Event()]

    @invoke
    async def callback(index: int) -> int:
        await gates[index].wait()
        return index

    executor = CallbackExecutor(
        [callback(0), callback(1), callback(2)],
        concurrency=3,
        execution_policy=ExecutionPolicy.FIRST_COMPLETED,
    )
    await executor.aenter()

    gates[1].set()
    assert await asyncio.wait_for(anext(executor), 1) == 1
    assert executor._pending

    gates[2].set()
    assert await asyncio.wait_for(anext(executor), 1) == 2
    gates[0].set()
    assert await asyncio.wait_for(anext(executor), 1) == 0
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


@pytest.mark.asyncio
async def test_multiple_completed_tasks_follow_pending_collection_order() -> (
    None
):
    gate = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        await gate.wait()
        return index

    executor = CallbackExecutor(
        [callback(0), callback(1), callback(2)],
        concurrency=3,
        execution_policy=ExecutionPolicy.FIRST_COMPLETED,
    )
    await executor.aenter()
    gate.set()
    await asyncio.sleep(0)

    assert await anext(executor) == 0
    assert await anext(executor) == 1
    assert await anext(executor) == 2


@pytest.mark.asyncio
async def test_identical_results_remain_distinguishable_by_execution() -> None:
    executions: deque[int] = deque()

    @invoke
    async def callback(index: int) -> str:
        executions.append(index)
        await asyncio.sleep(0)
        return "same"

    executor = CallbackExecutor(
        [callback(0), callback(1), callback(2)],
        concurrency=3,
        execution_policy=ExecutionPolicy.FIRST_COMPLETED,
    )

    assert await collect(executor) == ["same", "same", "same"]
    assert sorted(executions) == [0, 1, 2]


@pytest.mark.asyncio
async def test_fail_fast_propagates_single_exception_directly() -> None:
    error = RuntimeError("first failure")

    @invoke
    async def fail() -> int:
        raise error

    @invoke
    async def blocked() -> int:
        await asyncio.Event().wait()
        return 2

    executor = CallbackExecutor(
        [fail(), blocked()],
        concurrency=2,
        exceptions_policy=ExceptionsPolicy.FAIL_FAST,
    )

    with pytest.raises(RuntimeError) as caught:
        await collect(executor)
    assert caught.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_policy",
    [ExecutionPolicy.SEQUENTIAL, ExecutionPolicy.FIRST_COMPLETED],
)
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

    with pytest.raises(RuntimeError, match="fail"):
        await executor.__anext__()
    assert cancelled.is_set()
    assert cleanup_finished.is_set()
    assert not executor._pending


@pytest.mark.asyncio
async def test_fail_fast_groups_multiple_concurrent_failures() -> None:
    first = ValueError("first")
    second = LookupError("second")
    executor = CallbackExecutor(
        [],
        exceptions_policy=ExceptionsPolicy.FAIL_FAST,
    )

    executor._task_fail(first)
    executor._task_fail(second)
    with pytest.raises(ExceptionGroup) as caught:
        await executor.aclose()
    assert set(caught.value.exceptions) == {first, second}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_policy",
    [ExecutionPolicy.SEQUENTIAL, ExecutionPolicy.FIRST_COMPLETED],
)
async def test_fail_fast_does_not_schedule_after_failure(
    execution_policy: ExecutionPolicy,
) -> None:
    failure_ready = asyncio.Event()
    consumed: list[int] = []

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            failure_ready.set()
            raise RuntimeError("stop")
        return index

    def source() -> Iterable[Any]:
        for index in range(3):
            consumed.append(index)
            yield callback(index)

    executor = CallbackExecutor(
        source(),
        concurrency=1,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.FAIL_FAST,
    )
    await executor.aenter()
    await failure_ready.wait()
    await asyncio.wait_for(asyncio.shield(executor._pending[0]), 1)

    with pytest.raises(RuntimeError, match="stop"):
        await anext(executor)

    assert consumed == [0]
    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_policy",
    [ExecutionPolicy.SEQUENTIAL, ExecutionPolicy.FIRST_COMPLETED],
)
async def test_collect_skips_failures_and_yields_successes(
    execution_policy: ExecutionPolicy,
) -> None:
    error = ValueError("bad")

    @invoke
    async def callback(index: int) -> int:
        if index == 1:
            raise error
        return index

    executor = CallbackExecutor(
        [callback(0), callback(1), callback(2)],
        concurrency=2,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    results: list[int] = []
    with pytest.raises(ValueError) as caught:
        async for result in executor:
            results.append(result)  # noqa: PERF401

    assert caught.value is error
    assert results == [0, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_policy",
    [ExecutionPolicy.SEQUENTIAL, ExecutionPolicy.FIRST_COMPLETED],
)
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

    executor = CallbackExecutor(
        [callback(0), callback(1), callback(2)],
        concurrency=3,
        execution_policy=execution_policy,
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )

    results: list[int] = []
    with pytest.raises(ExceptionGroup) as caught:
        async for result in executor:
            results.append(result)  # noqa: PERF401

    assert results == [2]
    assert caught.value.exceptions == (first, second)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_policy",
    [ExecutionPolicy.SEQUENTIAL, ExecutionPolicy.FIRST_COMPLETED],
)
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

    with pytest.raises(ExceptionGroup) as caught:
        await collect(executor)

    assert caught.value.exceptions == tuple(errors)


@pytest.mark.asyncio
async def test_collect_empty_and_all_success_have_no_exception() -> None:
    empty = CallbackExecutor(
        [],
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )
    assert await collect(empty) == []

    @invoke
    async def callback(value: int) -> int:
        return value

    success = CallbackExecutor(
        [callback(1), callback(2)],
        exceptions_policy=ExceptionsPolicy.COLLECT,
    )
    assert await collect(success) == [1, 2]


@pytest.mark.asyncio
async def test_iterator_protocol_aiter_and_repeated_next() -> None:
    @invoke
    async def callback() -> int:
        return 1

    executor = CallbackExecutor([callback()])
    assert executor.__aiter__() is executor
    assert await anext(executor) == 1
    with pytest.raises(StopAsyncIteration):
        await anext(executor)
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


@pytest.mark.asyncio
async def test_concurrent_anext_calls_are_serialized() -> None:
    gates = [asyncio.Event(), asyncio.Event(), asyncio.Event()]

    @invoke
    async def callback(index: int) -> int:
        await gates[index].wait()
        return index

    executor = CallbackExecutor(
        [callback(0), callback(1), callback(2)],
        concurrency=3,
    )
    consumers = [
        asyncio.create_task(anext(executor)),
        asyncio.create_task(anext(executor)),
        asyncio.create_task(anext(executor)),
    ]

    for gate in gates:
        gate.set()
    results = await asyncio.gather(*consumers)

    assert sorted(results) == [0, 1, 2]
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


@pytest.mark.asyncio
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
    assert executor.entered
    assert await anext(executor) == 1
    assert executed == 1


@pytest.mark.asyncio
async def test_aenter_after_close_raises_runtime_error() -> None:
    executor = CallbackExecutor([])
    await executor.aclose()

    with pytest.raises(RuntimeError, match="CallbackExecutor is closed"):
        await executor.aenter()


@pytest.mark.asyncio
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

    assert executor.closed
    assert executed == 0
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


@pytest.mark.asyncio
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
    assert not executor._pending


@pytest.mark.asyncio
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
    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
async def test_async_context_break_cancels_remaining_work() -> None:
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
    ) as executor:
        await started.wait()
        break_requested = True
        assert break_requested
        assert executor.entered

    assert cleaned.is_set()
    assert executor.closed


@pytest.mark.asyncio
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
            raise consumer_error

    assert caught.value is consumer_error


@pytest.mark.asyncio
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

    executor = CallbackExecutor([callback()])

    with pytest.raises(LookupError) as caught:
        async with executor:
            await started.wait()
            raise consumer_error

    assert caught.value is consumer_error
    assert cleaned.is_set()
    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
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
    assert executor.closed


@pytest.mark.asyncio
async def test_repeated_cancellation_allows_aclose_to_finish_cleanup() -> None:
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    @invoke
    async def callback() -> int:
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
    callback_task = executor._pending[0]

    consumer.cancel()
    await cleanup_started.wait()
    consumer.cancel()

    close_task = asyncio.create_task(executor.aclose())
    await asyncio.sleep(0)
    assert not close_task.done()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await consumer
    await asyncio.wait_for(close_task, 1)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(callback_task, 1)
    assert cleanup_finished.is_set()
    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
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

    with pytest.raises(RuntimeError) as caught:
        await executor.aclose()
    assert caught.value is error


@pytest.mark.asyncio
async def test_explicit_aclose_without_failure_is_clean() -> None:
    @invoke
    async def callback() -> int:
        return 1

    executor = CallbackExecutor([callback()])
    await executor.aenter()
    await executor.aclose()
    await executor.aclose()

    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
async def test_input_iterator_exception_cancels_scheduled_tasks() -> None:
    iterator_error = LookupError("iterator failed")

    @invoke
    async def callback() -> int:
        await asyncio.Event().wait()
        return 1

    def source() -> Iterable[Any]:
        yield callback()
        raise iterator_error

    executor = CallbackExecutor(source(), concurrency=2)

    with pytest.raises(LookupError) as caught:
        await executor.aenter()
    assert caught.value is iterator_error
    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
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
    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
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

    def execute_stub(_invocation: object) -> DummyExecution:
        return execution

    monkeypatch.setattr(executor, "_execute", execute_stub)

    def fail_create_task(*args: Any, **kwargs: Any) -> Any:
        raise creation_error

    monkeypatch.setattr(executor._task_group, "create_task", fail_create_task)

    with pytest.raises(OSError) as caught:
        await executor.aenter()
    assert caught.value is creation_error
    assert execution.closed


@pytest.mark.asyncio
async def test_cancelled_callback_is_not_collected_as_exception() -> None:
    @invoke
    async def callback() -> int:
        raise asyncio.CancelledError()

    executor = CallbackExecutor([], exceptions_policy=ExceptionsPolicy.COLLECT)
    invocation = callback()

    with pytest.raises(asyncio.CancelledError):
        await executor._execute(invocation)
    assert not executor._exception_propagator.has_exceptions


@pytest.mark.asyncio
async def test_base_exception_is_not_converted_to_callback_outcome() -> None:
    class FatalCallbackError(BaseException):
        pass

    error = FatalCallbackError("fatal")

    @invoke
    async def callback() -> int:
        raise error

    executor = CallbackExecutor([callback()])

    with pytest.raises(BaseExceptionGroup) as caught:
        await collect(executor)

    assert caught.value.exceptions == (error,)
    assert executor.closed


@pytest.mark.asyncio
async def test_external_callback_cancellation_triggers_executor_cleanup() -> (
    None
):
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            started.set()
        try:
            await asyncio.Event().wait()
            return index
        finally:
            cancelled.set()

    executor = CallbackExecutor([callback(0), callback(1)], concurrency=2)
    await executor.aenter()
    await started.wait()
    executor._pending[0].cancel()

    with pytest.raises(asyncio.CancelledError):
        await anext(executor)

    assert cancelled.is_set()
    assert executor.closed
    assert not executor._pending


@pytest.mark.asyncio
async def test_wait_for_completion_with_done_task_is_noop() -> None:
    @invoke
    async def callback() -> int:
        return 1

    executor = CallbackExecutor([callback()])
    await executor.aenter()
    task = executor._pending[0]
    await asyncio.wait_for(asyncio.shield(task), 1)

    await executor._wait_for_completion()
    assert task.done()
    await executor.aclose()


@pytest.mark.asyncio
async def test_ensure_can_fetch_rejects_empty_executor() -> None:
    executor = CallbackExecutor([])

    with pytest.raises(StopAsyncIteration):
        await executor._ensure_can_fetch()
    assert executor.closed


@pytest.mark.asyncio
async def test_ensure_can_fetch_rejects_closed_executor() -> None:
    executor = CallbackExecutor([])
    await executor.aclose()

    with pytest.raises(StopAsyncIteration):
        await executor._ensure_can_fetch()


@pytest.mark.asyncio
async def test_ensure_can_fetch_propagates_registered_fail_fast_error() -> (
    None
):
    error = RuntimeError("registered failure")
    executor = CallbackExecutor([])
    executor._exception_propagator.register_exception(error)

    with pytest.raises(RuntimeError) as caught:
        await executor._ensure_can_fetch()
    assert caught.value is error
    assert executor.closed


@pytest.mark.asyncio
async def test_real_async_for_break_cancels_remaining_work() -> None:
    started = asyncio.Event()
    cleaned = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            started.set()
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
        await started.wait()
        async for result in executor:
            assert result in (0, 1)
            break

    assert cleaned.is_set()
    assert executor.closed
    assert not executor._pending
