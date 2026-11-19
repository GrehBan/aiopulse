from __future__ import annotations

import asyncio

import pytest

from aiopulse import CallbackExecutor, ExecutionPolicy, invoke

from .helpers import collect


@pytest.mark.parametrize("execution_policy", list(ExecutionPolicy))
async def test_results_follow_input_order_when_released_together(
    execution_policy: ExecutionPolicy,
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
    consumer = asyncio.create_task(collect(executor))
    await asyncio.sleep(0)
    for gate in gates:
        gate.set()

    assert await asyncio.wait_for(consumer, 1) == [0, 1, 2, 3]


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

    consumer = asyncio.create_task(anext(executor))
    await second_done.wait()
    await asyncio.sleep(0)
    assert not consumer.done()

    first_gate.set()
    assert await asyncio.wait_for(consumer, 1) == 1
    assert await anext(executor) == 2
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


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
    gates[2].set()
    assert await asyncio.wait_for(anext(executor), 1) == 2
    gates[0].set()
    assert await asyncio.wait_for(anext(executor), 1) == 0
    with pytest.raises(StopAsyncIteration):
        await anext(executor)


async def test_first_completed_yields_finished_tasks_in_completion_order() -> (
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


async def test_identical_results_remain_distinguishable_by_execution() -> None:
    executions: list[int] = []

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
    consumers = [asyncio.create_task(anext(executor)) for _ in range(3)]

    for gate in gates:
        gate.set()
    results = await asyncio.gather(*consumers)

    assert sorted(results) == [0, 1, 2]
    with pytest.raises(StopAsyncIteration):
        await anext(executor)
