"""Asynchronous execution and management of callbacks."""

from __future__ import annotations

import asyncio
import types
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Generic, Self

from aiopulse._sentinel import (
    MISSING,
    _Sentinel,  # pyright: ignore[reportPrivateUsage]
)
from aiopulse.callback import RT, CallbackInvocation, RT_co
from aiopulse.executor.exceptions_propagator import ExceptionsPropagator
from aiopulse.executor.policy import ExceptionsPolicy, ExecutionPolicy


@dataclass(frozen=True, slots=True)
class CallbackOutcome(Generic[RT_co]):
    """Represents the outcome of a single callback execution.

    Attributes:
        result: The result returned by the successful callback, or a
            sentinel indicating missing/failed state.
    """

    result: RT_co | _Sentinel = MISSING

    def get_result(self) -> RT_co:
        """Get outcome result or raise an error if execution failed.

        Returns:
            The successful callback result.

        Raises:
            RuntimeError: When the result is not set by the executor,
                indicating execution failed without a standard exception.
        """
        if self.result is MISSING:
            raise RuntimeError(
                "Callback execution failed without an exception"
            )
        assert not isinstance(self.result, _Sentinel)
        return self.result


class CallbackExecutor(Generic[RT_co]):
    """Asynchronous executor and manager for concurrent callbacks.

    Acts as both an asynchronous iterator yielding callback results
    and an asynchronous context manager handling the underlying task
    group lifecycle. Supports configurable concurrency limits,
    execution orders, and exception propagation policies.
    """

    def __init__(
        self,
        invocations: Iterable[CallbackInvocation[..., RT_co]],
        concurrency: int = 1,
        execution_policy: ExecutionPolicy = ExecutionPolicy.SEQUENTIAL,
        exceptions_policy: ExceptionsPolicy = ExceptionsPolicy.FAIL_FAST,
    ) -> None:
        """Initialize the callback executor with invocations and policies.

        Args:
            invocations: An iterable of callback invocations to execute.
            concurrency: The maximum number of concurrent callbacks.
            execution_policy: Policy defining execution/iteration order.
            exceptions_policy: Policy defining error-handling strategy
                (e.g., fail-fast vs collect).

        Raises:
            ValueError: If concurrency is less than 1.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be greater than or equal to 1")
        self._invocations = iter(invocations)
        self._concurrency = concurrency
        self.entered = False
        self.closed = False
        self.exhausted = False
        self._pending: deque[asyncio.Task[CallbackOutcome[RT_co]]] = deque()
        self._task_group = asyncio.TaskGroup()
        self._next_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._execution_policy = execution_policy
        self._exception_propagator = ExceptionsPropagator(exceptions_policy)

    async def _ensure_can_fetch(self) -> None:
        """Ensure that the executor can fetch the next callback result.

        Raises:
            StopAsyncIteration: If the executor is closed or no pending
                callbacks remain.
            Exception: If fail-fast execution has encountered an exception.
        """
        if self.closed:
            raise StopAsyncIteration

        if (
            self._exception_propagator.is_fail_fast
            and self._exception_propagator.has_exceptions
        ):
            await self.aclose()

        if not self._pending:
            await self.aclose()
            raise StopAsyncIteration

    async def _wait_for_completion(self) -> None:
        """Wait until at least one pending task completes."""
        unfinished = {task for task in self._pending if not task.done()}

        if unfinished:
            await asyncio.wait(
                unfinished,
                return_when=asyncio.FIRST_COMPLETED,
            )

    async def _fetch(self) -> asyncio.Task[CallbackOutcome[RT_co]]:
        """Fetch the next completed task based on execution policy.

        Returns:
            The completed asyncio Task wrapping the callback outcome.

        Raises:
            StopAsyncIteration: If closed or no pending tasks remain.
        """
        await self._ensure_can_fetch()

        if self._execution_policy == ExecutionPolicy.SEQUENTIAL:
            head = self._pending[0]
            while not head.done():
                await self._wait_for_completion()
                await self._ensure_can_fetch()
            self._pending.popleft()
            return head

        while True:
            for task in self._pending:
                if task.done():
                    self._pending.remove(task)
                    return task
            await self._wait_for_completion()
            await self._ensure_can_fetch()

    async def _next(self) -> RT_co:
        if self.closed:
            raise StopAsyncIteration

        try:
            if not self.entered:
                await self.aenter()

            while True:
                head = await self._fetch()
                outcome = head.result()

                await self._schedule()
                if (
                    outcome.result is MISSING
                    and self._exception_propagator.is_collect
                ):
                    continue

                return outcome.get_result()

        except BaseException as exc:
            if not self.closed:
                await self.aclose(
                    type(exc),
                    exc,
                    exc.__traceback__,
                )
            raise

    async def _execute(
        self,
        invocation: CallbackInvocation[..., RT],
    ) -> CallbackOutcome[RT]:
        """Execute a single callback invocation safely.

        Args:
            invocation: The callback invocation wrapper to execute.

        Returns:
            A CallbackOutcome containing the result on success, or an
            empty outcome on failure.

        Raises:
            CancelledError: If the execution task was cancelled.
        """
        try:
            result = await invocation()
        except Exception as exc:  # noqa: BLE001
            self._task_fail(exc)
            return CallbackOutcome()
        return CallbackOutcome(result=result)

    def __aiter__(self) -> Self:
        """Return self as an asynchronous iterator.

        Returns:
            The executor instance itself.
        """
        return self

    async def _schedule(self) -> None:
        """Schedule pending callbacks up to the concurrency limit."""
        try:
            while len(self._pending) < self._concurrency:
                try:
                    callback = next(self._invocations)
                except StopIteration:
                    self.exhausted = True
                    return

                execution = self._execute(callback)
                try:
                    task = self._task_group.create_task(execution)
                except BaseException:
                    execution.close()
                    raise
                self._pending.append(task)
        except BaseException as exc:
            await self.aclose(
                type(exc),
                exc,
                exc.__traceback__,
            )
            raise

    async def __anext__(self) -> RT_co:
        """Return the next result from the asynchronous iterator.

        Returns:
            The result of the next callback execution.

        Raises:
            StopAsyncIteration: When iteration is complete.
        """
        async with self._next_lock:
            return await self._next()

    async def aenter(self) -> None:
        """Enter the underlying task group and start the first batch.

        Raises:
            RuntimeError: If the executor is already closed.
        """
        if self.closed:
            raise RuntimeError("CallbackExecutor is closed")
        if self.entered:
            return

        await self._task_group.__aenter__()
        self.entered = True
        await self._schedule()

    def _cancel_pending(
        self,
        exclude: asyncio.Task[Any] | None = None,
    ) -> None:
        """Cancel all pending tasks except an optionally excluded one.

        Args:
            exclude: An optional task to exclude from cancellation.
        """
        for task in self._pending:
            if task is exclude and not task.done():
                continue
            task.cancel()

    async def aclose(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        """Asynchronously close the executor, acquiring close lock.

        Args:
            exc_type: Exception type if closing due to an exception.
            exc: Exception instance if closing due to an exception.
            tb: Traceback object if closing due to an exception.
        """
        async with self._close_lock:
            return await self._aclose(exc_type, exc, tb)

    async def _aclose(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        """Cancel in-flight work, exit task group, and close executor.

        Args:
            exc_type: Exception type if closing due to an exception.
            exc: Exception instance if closing due to an exception.
            tb: Traceback object if closing due to an exception.
        """
        if self.closed:
            return
        self.closed = True

        self._cancel_pending()
        try:
            if self.entered:
                await self._task_group.__aexit__(None, None, None)
        finally:
            self._pending.clear()
        if exc is None:
            self._exception_propagator.propagate()
        self._exception_propagator.clear()

    async def __aenter__(self) -> Self:
        """Asynchronously enter the context manager.

        Returns:
            The executor instance.
        """
        await self.aenter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        """Asynchronously exit the context manager.

        Args:
            exc_type: Exception type if exiting with an exception.
            exc: Exception instance if exiting with an exception.
            tb: Traceback object if exiting with an exception.
        """
        await self.aclose(exc_type, exc, tb)

    def _task_fail(self, exception: Exception) -> None:
        """Register a callback failure and initiate fail-fast cancellation.

        The first registered exception triggers cancellation of all remaining
        pending callbacks when the FAIL_FAST policy is active. Subsequent
        exceptions are still registered and propagated during executor
        shutdown.

        Args:
            exception: The exception raised by the callback.
        """
        is_first = self._exception_propagator.register_exception(exception)

        if (
            is_first
            and self._exception_propagator.is_fail_fast
            and not self.closed
        ):
            self._cancel_pending(
                exclude=asyncio.current_task(),
            )
