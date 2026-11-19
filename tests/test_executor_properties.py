"""Property-based checks of executor invariants on random scenarios.

Callbacks only yield with ``asyncio.sleep(0)``, so every scenario is
deterministic and Hypothesis can shrink failures reliably.
"""

from __future__ import annotations

import asyncio

from hypothesis import given, settings
from hypothesis import strategies as st

from aiopulse import (
    CallbackCancelledError,
    CallbackExceptionGroup,
    CallbackExecutor,
    ExceptionsPolicy,
    ExecutionPolicy,
    invoke,
)

from .helpers import unfinished_tasks

Call = tuple[int, str]
OK, FAIL, CANCEL = "ok", "fail", "cancel"
EXPECTED_ERROR = {FAIL: ValueError, CANCEL: CallbackCancelledError}

calls_strategy = st.lists(
    st.tuples(st.integers(0, 3), st.sampled_from((OK, OK, OK, FAIL, CANCEL))),
    max_size=10,
)


def failure_indices(
    error: BaseException | None, calls: list[Call]
) -> list[int]:
    if error is None:
        return []
    assert isinstance(error, CallbackExceptionGroup)
    assert [failure.exception for failure in error.failures] == list(
        error.exceptions
    )
    for failure in error.failures:
        _, outcome = calls[failure.index]
        assert type(failure.exception) is EXPECTED_ERROR[outcome]
    return [failure.index for failure in error.failures]


async def check_scenario(
    calls: list[Call],
    concurrency: int,
    buffer_size: int,
    execution: ExecutionPolicy,
    exceptions: ExceptionsPolicy,
    stop_after: int | None,
) -> None:
    running = 0
    peak_running = 0
    peak_outstanding = 0
    executor: CallbackExecutor[int]

    @invoke
    async def callback(index: int, steps: int, outcome: str) -> int:
        nonlocal running, peak_running, peak_outstanding
        running += 1
        peak_running = max(peak_running, running)
        peak_outstanding = max(peak_outstanding, executor._outstanding)
        try:
            for _ in range(steps):
                await asyncio.sleep(0)
            if outcome == FAIL:
                raise ValueError(index)
            if outcome == CANCEL:
                future = asyncio.get_running_loop().create_future()
                future.cancel()
                await future
            return index
        finally:
            running -= 1

    executor = CallbackExecutor(
        [callback(i, steps, end) for i, (steps, end) in enumerate(calls)],
        concurrency=concurrency,
        buffer_size=buffer_size,
        execution_policy=execution,
        exceptions_policy=exceptions,
    )
    yielded: list[int] = []
    raised: Exception | None = None
    try:
        async with executor:
            async for value in executor:
                yielded.append(value)
                if len(yielded) == stop_after:
                    break
    except Exception as error:  # noqa: BLE001
        raised = error

    assert peak_running <= concurrency
    assert peak_outstanding <= concurrency + buffer_size
    assert running == 0
    assert not unfinished_tasks()

    assert_results(yielded, raised, calls, execution, exceptions, stop_after)


def assert_results(
    yielded: list[int],
    raised: Exception | None,
    calls: list[Call],
    execution: ExecutionPolicy,
    exceptions: ExceptionsPolicy,
    stop_after: int | None,
) -> None:
    successes = [i for i, (_, end) in enumerate(calls) if end == OK]
    failing = [i for i, (_, end) in enumerate(calls) if end != OK]

    assert len(set(yielded)) == len(yielded)
    assert set(yielded) <= set(successes)
    if execution is ExecutionPolicy.SEQUENTIAL:
        assert yielded == successes[: len(yielded)]

    recorded = failure_indices(raised, calls)
    assert len(set(recorded)) == len(recorded)
    assert set(recorded) <= set(failing)

    if len(yielded) == stop_after:
        return

    if exceptions is ExceptionsPolicy.COLLECT:
        assert sorted(yielded) == successes
        assert sorted(recorded) == failing
    elif failing:
        assert recorded
    else:
        assert raised is None
        assert sorted(yielded) == successes


@settings(max_examples=300, deadline=None)
@given(
    calls=calls_strategy,
    concurrency=st.integers(1, 4),
    buffer_size=st.integers(0, 3),
    execution=st.sampled_from(ExecutionPolicy),
    exceptions=st.sampled_from(ExceptionsPolicy),
    stop_after=st.none() | st.integers(1, 5),
)
def test_executor_invariants_hold_for_random_scenarios(
    calls: list[Call],
    concurrency: int,
    buffer_size: int,
    execution: ExecutionPolicy,
    exceptions: ExceptionsPolicy,
    stop_after: int | None,
) -> None:
    asyncio.run(
        check_scenario(
            calls,
            concurrency,
            buffer_size,
            execution,
            exceptions,
            stop_after,
        )
    )
