from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest

from aiopulse import CallbackExceptionGroup, CallbackExecutor, invoke
from aiopulse.callback import CallbackInvocation

from .helpers import collect, settle, unfinished_tasks


@pytest.mark.parametrize("container", ["list", "tuple", "generator"])
async def test_executor_accepts_supported_iterables(container: str) -> None:
    @invoke
    async def callback(value: int) -> int:
        return value

    invocations = [callback(1), callback(2), callback(3)]
    source: Any
    if container == "list":
        source = invocations
    elif container == "tuple":
        source = tuple(invocations)
    else:
        source = (item for item in invocations)

    assert await collect(CallbackExecutor(source)) == [1, 2, 3]


async def test_executor_does_not_consume_or_execute_during_init() -> None:
    consumed = 0
    executed = 0

    @invoke
    async def callback() -> int:
        nonlocal executed
        executed += 1
        return 1

    def source() -> Iterator[CallbackInvocation[[], int]]:
        nonlocal consumed
        for _ in range(3):
            consumed += 1
            yield callback()

    executor = CallbackExecutor(source())
    await settle()

    assert consumed == 0
    assert executed == 0

    assert await collect(executor) == [1, 1, 1]
    assert consumed == 3
    assert executed == 3


@pytest.mark.parametrize("concurrency", [0, -1, -100])
def test_executor_rejects_non_positive_concurrency(concurrency: int) -> None:
    with pytest.raises(
        ValueError,
        match="concurrency must be greater than or equal to 1",
    ):
        CallbackExecutor([], concurrency=concurrency)


@pytest.mark.parametrize("buffer_size", [-1, -2])
def test_executor_rejects_negative_buffer_size(buffer_size: int) -> None:
    with pytest.raises(
        ValueError,
        match="buffer_size must be greater than or equal to 0",
    ):
        CallbackExecutor([], buffer_size=buffer_size)


@pytest.mark.parametrize("concurrency", [1, 2, 10])
async def test_executor_runs_with_valid_concurrency(concurrency: int) -> None:
    @invoke
    async def callback(value: int) -> int:
        await asyncio.sleep(0)
        return value

    executor = CallbackExecutor(
        [callback(index) for index in range(5)],
        concurrency=concurrency,
    )
    assert await collect(executor) == [0, 1, 2, 3, 4]


async def test_enter_schedules_no_more_than_concurrency() -> None:
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
    await asyncio.gather(started[0].wait(), started[1].wait())
    await settle()

    assert not started[2].is_set()
    assert not started[3].is_set()

    for gate in gates:
        gate.set()
    assert await collect(executor) == [0, 1, 2, 3]


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
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in started)),
        1,
    )

    gate.set()
    assert await collect(executor) == [0, 1, 2]


async def test_empty_executor_finishes_without_tasks() -> None:
    executor = CallbackExecutor([])

    assert await collect(executor) == []
    assert not unfinished_tasks()
    with pytest.raises(RuntimeError, match="CallbackExecutor is closed"):
        await executor.aenter()


async def test_single_invocation_executes_once() -> None:
    executions = 0

    @invoke
    async def callback() -> int:
        nonlocal executions
        executions += 1
        return 7

    assert await collect(CallbackExecutor([callback()])) == [7]
    assert executions == 1


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

    def source() -> Iterator[CallbackInvocation[[int], int]]:
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


def _slot_scenario() -> tuple[
    list[asyncio.Event],
    asyncio.Event,
    list[CallbackInvocation[[int], int]],
]:
    """Callback 0 blocks, callback 1 finishes at once, the rest block."""
    started = [asyncio.Event() for _ in range(4)]
    gate = asyncio.Event()

    @invoke
    async def callback(index: int) -> int:
        started[index].set()
        if index != 1:
            await gate.wait()
        return index

    return started, gate, [callback(index) for index in range(4)]


async def test_unconsumed_result_occupies_slot_without_buffer() -> None:
    started, gate, invocations = _slot_scenario()
    executor = CallbackExecutor(invocations, concurrency=2)

    first = asyncio.create_task(anext(executor))
    await asyncio.wait_for(started[1].wait(), 1)
    await settle()

    assert not started[2].is_set()

    gate.set()
    assert await first == 0
    assert await collect(executor) == [1, 2, 3]


async def test_buffer_size_allows_additional_pending_work() -> None:
    started, gate, invocations = _slot_scenario()
    executor = CallbackExecutor(invocations, concurrency=2, buffer_size=1)

    first = asyncio.create_task(anext(executor))
    await asyncio.wait_for(started[2].wait(), 1)
    await settle()

    assert not started[3].is_set()

    gate.set()
    assert await first == 0
    assert await collect(executor) == [1, 2, 3]


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
        buffer_size=2,
    )
    await executor.aenter()
    await asyncio.gather(*(event.wait() for event in starts[:3]))
    await settle()

    assert peak == 3
    for gate in gates:
        gate.set()

    assert await collect(executor) == [0, 1, 2, 3, 4, 5]
    assert peak == 3


async def test_regression_running_count_skips_unstarted_cancelled() -> None:
    @invoke
    async def callback(index: int) -> int:
        if index == 0:
            raise ValueError("first")
        await asyncio.Event().wait()
        return index

    executor = CallbackExecutor(
        [callback(index) for index in range(3)],
        concurrency=3,
    )
    await executor.aenter()
    await settle()

    assert executor._running == 0
    with pytest.raises(CallbackExceptionGroup):
        await executor.aclose()
