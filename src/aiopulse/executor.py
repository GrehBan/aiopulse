"""Asynchronous execution and management of callbacks."""

from __future__ import annotations

import asyncio
import types
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Generic, Self

from aiopulse.callback import RT, CallbackInvocation, RT_co


@dataclass(frozen=True, slots=True)
class CallbackOutcome(Generic[RT_co]):
    """Represents the outcome of a single callback execution.

    Attributes:
        result: The result returned by the successful callback.
    """

    result: RT_co


class CallbackExecutor(Generic[RT_co]):
    """Yield results of callbacks in order, up to concurrency.

    Semantics:
        - Results are yielded in the same order as underlying callbacks.
        - Execution is *fail-fast*: if any in-flight callback raises, all
          in-flight work is cancelled and the exception is raised.
        - Iteration exhaustion (or the last yielded result) closes the
          executor automatically.
    """

    def __init__(
        self,
        invocations: Iterable[CallbackInvocation[..., RT_co]],
        concurrency: int = 1,
    ) -> None:
        """Initialize callback executor with invocations and concurrency.

        Args:
            invocations: Callbacks invocations
            concurrency: The maximum number of concurrent callbacks.

        Raises:
            ValueError: If concurrency is less than 1.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be greater than 0")

        self._invocations = iter(invocations)
        self._concurrency = concurrency
        self.entered = False
        self.closed = False
        self.exhausted = False
        self._pending: deque[
            asyncio.Task[CallbackOutcome[RT_co] | None]
        ] = deque()
        self._task_group = asyncio.TaskGroup()
        self._failure: BaseException | None = None
        self._next_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()

    @property
    def closed_or_failed(self) -> bool:
        """Return True if executor is closed or has failed.

        Returns:
            True if closed or failed, False otherwise.
        """
        return self.closed or self._failure is not None

    def __aiter__(self) -> Self:
        """Return self as an asynchronous iterator.

        Returns:
            The executor instance itself.
        """
        return self

    async def _schedule(self) -> None:
        """Schedule pending callbacks up to concurrency limit."""
        try:
            while len(self._pending) < self._concurrency:
                try:
                    callback = next(self._invocations)
                except StopIteration:
                    self.exhausted = True
                    return

                self._pending.append(
                    self._task_group.create_task(
                        self._execute(callback)
                    )
                )
        except BaseException:
            await self.aclose()
            raise

    async def __anext__(self) -> RT_co:
        """Return the next result from asynchronous iterator.

        Returns:
            The result of the next callback execution.

        Raises:
            StopAsyncIteration: When iteration is complete.
        """
        async with self._next_lock:
            return await self._next()

    async def _ensure_can_fetch(self) -> None:
        if self.closed:
            raise StopAsyncIteration

        if self._failure is not None:
            raise self._failure

        if not self._pending:
            await self.aclose()
            raise StopAsyncIteration

    async def _fetch(self) -> asyncio.Task[CallbackOutcome[RT_co] | None]:
        """Fetch the next completed task from pending queue.

        Returns:
            The completed task.

        Raises:
            StopAsyncIteration: If closed or no pending tasks remain.
        """
        await self._ensure_can_fetch()
        head = self._pending[0]
        while not head.done():
            await asyncio.wait(
                {
                    task
                    for task in self._pending
                    if not task.done()
                },
                return_when=asyncio.FIRST_COMPLETED,
            )
            await self._ensure_can_fetch()

        self._pending.popleft()
        return head

    async def _next(self) -> RT_co:
        """Fetch the next completed callback result.

        Returns:
            The result of the callback.

        Raises:
            StopAsyncIteration: When iteration finishes.
            RuntimeError: If callback execution failed without an exception.
        """
        if self.closed:
            raise StopAsyncIteration

        try:
            if not self.entered:
                await self.aenter()
            head = await self._fetch()
        except BaseException:
            await self.aclose()
            raise

        outcome = head.result()

        await self._schedule()

        if not self._pending and self.exhausted:
            await self.aclose()

        if outcome is None:
            raise RuntimeError(
                "Callback execution failed without an exception"
            )

        return outcome.result

    async def aenter(self) -> None:
        """Enter underlying task group and start first batch.

        Raises:
            RuntimeError: If executor is closed.
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
        """Cancel all pending tasks except optionally excluded.

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
        """Asynchronously close executor, acquiring close lock.

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
        """Cancel all in-flight work and close executor.

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
                await self._task_group.__aexit__(exc_type, exc, tb)
        finally:
            self._pending.clear()

    async def __aenter__(self) -> Self:
        """Asynchronously enter context manager.

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
        """Asynchronously exit context manager.

        Args:
            exc_type: Exception type if exiting with an exception.
            exc: Exception instance if exiting with an exception.
            tb: Traceback object if exiting with an exception.
        """
        await self.aclose(exc_type, exc, tb)

    def _task_fail(self, exception: BaseException) -> None:
        """Handle task failure and cancel pending tasks.

        Args:
            exception: The exception that caused the task failure.
        """
        if self.closed_or_failed:
            return

        self._failure = exception
        self._cancel_pending(
            exclude=asyncio.current_task(),
        )

    async def _execute(
        self,
        invocation: CallbackInvocation[..., RT],
    ) -> CallbackOutcome[RT] | None:
        """Execute a single callback invocation safely.

        Args:
            invocation: The callback invocation wrapper to execute.

        Returns:
            A CallbackOutcome containing the result, or None on failure.

        Raises:
            CancelledError: If the execution task was cancelled.
        """
        try:
            result = await invocation()

        except asyncio.CancelledError as exc:
            self._task_fail(exc)
            raise
        except Exception as exc:  # noqa: BLE001
            self._task_fail(exc)
            return None
        return CallbackOutcome(result=result)