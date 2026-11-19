"""Asynchronous execution and management of callbacks."""

from __future__ import annotations

import asyncio
import types
from collections import deque
from collections.abc import Iterable, Sequence
from typing import Any, Generic, Self, cast, overload

from typing_extensions import TypeVar

from aiopulse._sentinel import MISSING
from aiopulse.callback import (
    RT,
    CallbackFailure,
    CallbackInvocation,
    CallbackOutcome,
    RT_co,
)
from aiopulse.policy import ExceptionsPolicy, ExecutionPolicy

ExceptionT_co = TypeVar(
    "ExceptionT_co",
    bound=Exception,
    covariant=True,
    default=Exception,
)
_ExceptionT = TypeVar("_ExceptionT", bound=Exception)
_BaseExceptionT = TypeVar("_BaseExceptionT", bound=BaseException)


def _is_derived_exception(
    original: BaseException,
    candidate: BaseException,
) -> bool:
    if original is candidate:
        return True
    if not isinstance(original, BaseExceptionGroup) or not isinstance(
        candidate, BaseExceptionGroup
    ):
        return False
    if original.message != candidate.message:
        return False

    unmatched = list(original.exceptions)
    for candidate_exception in candidate.exceptions:
        for index, original_exception in enumerate(unmatched):
            if _is_derived_exception(
                original_exception,
                candidate_exception,
            ):
                del unmatched[index]
                break
        else:
            return False
    return bool(candidate.exceptions)


class CallbackCancelledError(Exception):
    """A callback ended with a cancellation the executor did not request.

    For example, the callback awaited a cancelled future or task, or it
    cancelled itself. The executor records this error as a callback failure
    in place of the original ``asyncio.CancelledError``, which is kept as
    ``__cause__``. Reporting it as an ordinary exception keeps it from being
    mistaken for a cancellation of the consumer.
    """


def _unrequested_cancel(
    cause: asyncio.CancelledError,
) -> CallbackCancelledError:
    """Build the failure recorded for an unrequested cancellation.

    The executor did not request the cancellation. Withdraws every
    pending cancellation request of the current task,
    which then finishes normally instead of cancelled.
    """
    task = asyncio.current_task()
    assert task is not None
    while task.cancelling():
        task.uncancel()
    error = CallbackCancelledError("callback was cancelled unexpectedly")
    error.__cause__ = cause
    return error


class CallbackExceptionGroup(
    ExceptionGroup[ExceptionT_co],
    Generic[ExceptionT_co, RT_co],
):
    """Exception group carrying structured metadata for callback failures.

    The members are the exceptions of the failure records, unchanged: the
    exceptions raised by the callbacks, or CallbackCancelledError for a
    cancellation the executor did not request. The records are available
    through ``failures``, one per member exception and in the same order.

    Preserving the original exceptions maintains native ExceptionGroup
    semantics, including exception matching and ``except*`` handling.
    Subgroups created by ``split()``, ``subgroup()`` and ``except*`` keep
    the records of their members.

    Type parameters are the member exception type and the callback result
    type.
    """

    def __new__(
        cls,
        message: str,
        failures: tuple[CallbackFailure[RT_co], ...],
        *,
        _exceptions: Sequence[ExceptionT_co] | None = None,
    ) -> Self:
        """Create a group from failure records.

        Args:
            message: Message of the group.
            failures: Failure records; their exceptions become the members.
            _exceptions: Internal. Members to use instead of the records'
                exceptions, one per record, when deriving a subgroup whose
                members are derived groups themselves.

        Raises:
            ValueError: If there are no failures, or ``_exceptions`` does not
                hold exactly one exception for each failure.
        """
        if _exceptions is None:
            exceptions = cast(
                "tuple[ExceptionT_co, ...]",
                tuple(failure.exception for failure in failures),
            )
        elif len(_exceptions) != len(failures):
            raise ValueError("one exception is required for each failure")
        else:
            exceptions = tuple(_exceptions)
        return super().__new__(cls, message, exceptions)

    def __init__(
        self,
        message: str,
        failures: tuple[CallbackFailure[RT_co], ...],
        *,
        _exceptions: Sequence[ExceptionT_co] | None = None,
    ) -> None:
        """Store the failure records; arguments are described in __new__."""
        self._failures = failures

    @property
    def failures(self) -> tuple[CallbackFailure[RT_co], ...]:
        """Failure records, one per member exception, in the same order."""
        return self._failures

    def __repr__(self) -> str:  # noqa: D105
        details = ", ".join(
            f"(index={err.index}, exc={err.exception!r})"
            for err in self.failures
        )
        return f"CallbackExceptionGroup({self.message!r}, errors=[{details}])"

    @overload
    def derive(
        self,
        excs: Sequence[_ExceptionT],
        /,
    ) -> CallbackExceptionGroup[_ExceptionT, RT_co]: ...

    @overload
    def derive(
        self,
        excs: Sequence[_BaseExceptionT],
        /,
    ) -> BaseExceptionGroup[_BaseExceptionT]: ...

    def derive(
        self,
        excs: Sequence[BaseException],
        /,
    ) -> BaseExceptionGroup[BaseException]:
        """Create a group of the same type for a subset of the members.

        Used by ``split()``, ``subgroup()`` and ``except*``. Each exception
        keeps its failure record. An exception is either a member of this
        group, matched by identity, or a group derived from a nested group
        member.

        Args:
            excs: Exceptions of the new group.

        Returns:
            A group with the given exceptions and their failure records.

        Raises:
            ValueError: If ``excs`` is empty or an exception has no unused
                matching failure record in this group.
        """
        remaining = dict(enumerate(self.failures))
        by_identity: dict[int, list[int]] = {}
        for position, failure in remaining.items():
            by_identity.setdefault(id(failure.exception), []).append(position)

        failures: list[CallbackFailure[RT_co]] = []
        derived_exceptions: list[Exception] = []

        for exception in excs:
            if not isinstance(exception, Exception):
                raise ValueError(
                    "derived exceptions must come from this group"
                )
            match: int | None = None
            if isinstance(exception, BaseExceptionGroup):
                for position, failure in remaining.items():
                    if _is_derived_exception(failure.exception, exception):
                        match = position
                        break
            else:
                for position in by_identity.get(id(exception), ()):
                    if position in remaining:
                        match = position
                        break
            if match is None:
                raise ValueError(
                    "derived exceptions must come from this group"
                )
            failures.append(remaining.pop(match))
            derived_exceptions.append(exception)

        group_type = cast(
            "type[CallbackExceptionGroup[Exception, RT_co]]",
            type(self),
        )
        return group_type(
            self.message,
            tuple(failures),
            _exceptions=tuple(derived_exceptions),
        )


class CallbackExecutor(Generic[RT_co]):
    """Asynchronous executor for managing and consuming callback invocations.

    Consumes callback invocations lazily, schedules them with bounded
    concurrency, and exposes their results through an asynchronous iterator.

    The executor maintains two independent limits:
        - Maximum number of concurrently executing callbacks.
        - Maximum number of pending tasks, including completed tasks
        whose results have not yet been consumed.

    The execution policy determines the order in which completed results
    are yielded. Sequential execution preserves invocation order, while
    first-completed execution yields completed invocations without waiting
    for earlier ones.

    The exception policy determines how callback failures are handled.
    Fail-fast stops further processing after the first recorded failure,
    while collect records failures and continues executing remaining
    invocations. Failed outcomes are omitted from iteration.

    Recorded failures are raised together as one CallbackExceptionGroup,
    even when there is only one. A cancellation that the executor did not
    request is recorded as a CallbackCancelledError failure.

    Can be used both as an asynchronous iterator and as an asynchronous
    context manager.
    """

    def __init__(
        self,
        invocations: Iterable[CallbackInvocation[..., RT_co]],
        concurrency: int = 1,
        buffer_size: int = 0,
        execution_policy: ExecutionPolicy = ExecutionPolicy.SEQUENTIAL,
        exceptions_policy: ExceptionsPolicy = ExceptionsPolicy.FAIL_FAST,
    ) -> None:
        """Initialize the executor with callback invocations and policies.

        Args:
            invocations: Iterable of callback invocations to execute.
                Invocations are consumed lazily in their original order.
            concurrency: Maximum number of callbacks executing simultaneously.
            buffer_size: Additional capacity for pending tasks beyond the
                concurrency limit. Completed but unconsumed tasks occupy this
                capacity.
            execution_policy: Policy controlling the order in which results
                are yielded.
            exceptions_policy: Policy controlling callback failure handling.

        Raises:
            ValueError: If concurrency is less than one or buffer_size is <0.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be greater than or equal to 1")
        if buffer_size < 0:
            raise ValueError("buffer_size must be greater than or equal to 0")
        self._invocations = iter(invocations)
        self._concurrency = concurrency
        self._buffer_size = buffer_size
        self._capacity = concurrency + buffer_size
        self._execution_policy = execution_policy
        self._exception_policy = exceptions_policy
        self._is_sequential = execution_policy == ExecutionPolicy.SEQUENTIAL
        self._is_fail_fast = exceptions_policy == ExceptionsPolicy.FAIL_FAST

        self._entered = False
        self._closed = False
        self._exhausted = False

        self._outstanding = 0
        self._invocation_counter = 0

        self._ordered: deque[asyncio.Task[CallbackOutcome[RT_co]]] = deque()
        self._ready: deque[asyncio.Task[CallbackOutcome[RT_co]]] = deque()
        self._live: set[asyncio.Task[CallbackOutcome[RT_co]]] = set()
        self._failures: list[CallbackFailure[RT_co]] = []
        self._cancel_requested = False

        self._task_group = asyncio.TaskGroup()
        self._wakeup = asyncio.Event()
        self._next_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()

    @property
    def _running(self) -> int:
        """Number of callbacks currently executing."""
        return len(self._live)

    async def aenter(self) -> None:
        """Enter the executor and schedule the initial batch of callbacks.

        Repeated calls have no effect after the task group has been entered.
        The calling task owns the task group.

        Raises:
            RuntimeError: If the executor has already been closed.
            BaseException: Any error raised while scheduling, for example by
                the input iterable; the executor is closed first.
        """
        if self._closed:
            raise RuntimeError("CallbackExecutor is closed")
        if self._entered:
            return

        await self._task_group.__aenter__()
        self._entered = True
        await self._schedule()

    async def aclose(
        self,
        exc: BaseException | None = None,
    ) -> None:
        """Close the executor and release its asynchronous resources.

        Cancels unfinished callbacks and waits for them to finish. The
        operation is safe to call multiple times and serializes concurrent
        close requests.

        Args:
            exc: Exception already being propagated by the caller, if any.
                When provided, recorded callback failures are discarded rather
                than propagated separately.

        Raises:
            CallbackExceptionGroup: All recorded callback failures, in the
                order they were observed, when ``exc`` is None.
            BaseExceptionGroup: A callback raised a BaseException that is
                not an Exception; raised by the task group.
        """
        async with self._close_lock:
            await self._aclose(exc)

    async def _aclose(self, exc: BaseException | None) -> None:
        """Close the executor; the caller holds ``_close_lock``."""
        if self._closed:
            return
        self._closed = True

        self._cancel_pending()
        try:
            if self._entered:
                await self._task_group.__aexit__(None, None, None)
        finally:
            self._ordered.clear()
            self._ready.clear()
            self._live.clear()
            self._outstanding = 0
        failures = tuple(self._failures)
        self._failures.clear()
        if exc is None and failures:
            raise CallbackExceptionGroup(
                "Exceptions raised while executing callbacks",
                failures,
            )

    async def __aenter__(self) -> Self:
        """Enter the executor as an asynchronous context manager.

        Returns:
            The entered executor instance.

        Raises:
            RuntimeError: If the executor has already been closed.
            BaseException: Any error raised while scheduling the initial
                callbacks; the executor is closed first.
        """
        await self.aenter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        """Close the executor when leaving the asynchronous context.

        An exception raised inside the context takes precedence: recorded
        callback failures are then discarded.

        Args:
            exc_type: Type of the exception raised inside the context, if any.
            exc: Exception raised inside the context, if any.
            tb: Traceback associated with the exception.

        Raises:
            CallbackExceptionGroup: Recorded callback failures, when no
                exception was raised inside the context.
        """
        await self.aclose(exc)

    def __aiter__(self) -> Self:
        """Return the executor itself as an asynchronous iterator."""
        return self

    async def __anext__(self) -> RT_co:
        """Return the next successful callback result.

        Enters the executor on the first call if it has not been entered.
        Serializes concurrent calls to ensure that only one result is consumed
        at a time. Closes the executor if an exception or cancellation
        interrupts the call.

        Raises:
            StopAsyncIteration: When the executor has no more results to yield.
            CallbackExceptionGroup: Recorded callback failures: under
                fail-fast once a failure is recorded, under collect once no
                pending tasks remain.
        """
        async with self._next_lock:
            return await self._next()

    async def _next(self) -> RT_co:
        """Return the next successful result, entering lazily if needed.

        Failed outcomes have no result and are skipped: under collect,
        iteration continues; under fail-fast, the next fetch raises the
        recorded failures.

        Closes the executor if an exception or cancellation interrupts
        iteration.
        """
        if self._closed:
            raise StopAsyncIteration

        try:
            if not self._entered:
                await self.aenter()

            while True:
                outcome = (await self._fetch()).result()
                await self._schedule()
                if outcome.result is not MISSING:
                    return outcome.get_result()

        except BaseException as exc:
            if not self._closed:
                await self.aclose(exc)
            raise

    async def _fetch(self) -> asyncio.Task[CallbackOutcome[RT_co]]:
        """Wait for the next finished task the execution policy allows.

        While waiting, schedules more callbacks as execution slots free up.

        Returns:
            The finished task holding the callback outcome.

        Raises:
            StopAsyncIteration: If the executor is closed or has no pending
                tasks.
            CallbackExceptionGroup: Recorded failures, as described in
                ``_ensure_can_fetch``.
        """
        await self._ensure_can_fetch()
        while (task := self._pop_ready()) is None:
            await self._wait_for_completion()
            await self._ensure_can_fetch()
            await self._schedule()
        self._outstanding -= 1
        return task

    def _pop_ready(self) -> asyncio.Task[CallbackOutcome[RT_co]] | None:
        """Remove and return the next consumable task, if any is finished.

        Requires at least one outstanding task. Sequential execution only
        takes the oldest outstanding task, so a finished later task waits
        for it. First-completed execution takes the task whose completion
        was observed first.
        """
        if self._is_sequential:
            if self._ordered[0].done():
                return self._ordered.popleft()
            return None
        return self._ready.popleft() if self._ready else None

    async def _ensure_can_fetch(self) -> None:
        """Ensure that the executor has pending work available for consumption.

        In fail-fast mode, closes the executor when callback failures have
        been recorded, which raises those failures.

        Closes the executor when no pending tasks remain.

        Raises:
            StopAsyncIteration: If the executor is closed or no pending tasks.
            CallbackExceptionGroup: Recorded failures, under fail-fast as
                soon as one is recorded, otherwise when the executor closes
                because no pending tasks remain.
        """
        if self._closed:
            raise StopAsyncIteration

        if self._is_fail_fast and self._failures:
            await self.aclose()

        if not self._outstanding:
            await self.aclose()
            raise StopAsyncIteration

    async def _wait_for_completion(self) -> None:
        """Wait until any task completes after this call.

        Must only be called when the caller has just verified that nothing
        consumable is available; there is no ``await`` between that check and
        clearing the event, so no completion can be missed.
        """
        self._wakeup.clear()
        await self._wakeup.wait()

    async def _schedule(self) -> None:
        """Schedule callback invocations while execution and buffer allow.

        New tasks are scheduled until either the concurrency limit or the
        pending task capacity is reached, or the input is exhausted.

        Closes the executor and re-raises if scheduling fails.
        """
        try:
            while (
                not self._exhausted
                and self._running < self._concurrency
                and self._outstanding < self._capacity
            ):
                try:
                    invocation = next(self._invocations)
                except StopIteration:
                    self._exhausted = True
                    return
                self._start(invocation)
        except BaseException as exc:
            await self.aclose(exc)
            raise

    def _start(self, invocation: CallbackInvocation[..., RT_co]) -> None:
        """Start one invocation as a task and track it until consumed.

        Assigns the invocation its zero-based position in the input. If the
        task cannot be created, the never-awaited coroutine is closed and
        the error propagates.
        """
        index = self._invocation_counter
        self._invocation_counter += 1
        execution = self._execute(index, invocation)
        try:
            task = self._task_group.create_task(execution)
        except BaseException:
            execution.close()
            raise
        self._outstanding += 1
        if self._is_sequential:
            self._ordered.append(task)
        self._live.add(task)
        task.add_done_callback(self._on_done)

    async def _execute(
        self,
        index: int,
        invocation: CallbackInvocation[..., RT],
    ) -> CallbackOutcome[RT]:
        """Execute a single callback invocation and capture its outcome.

        Ordinary exceptions raised by the callback are registered as failures
        and represented by an empty CallbackOutcome. So is a cancellation the
        executor did not request, as a CallbackCancelledError; a requested
        one propagates and ends the task cancelled. Any other BaseException
        marks the coming cancellations as requested and propagates to the
        task group, which then cancels every other task.

        The task leaves the executing set as soon as the callback finishes,
        whatever the result, so a slot frees before the task is marked done.
        """
        try:
            result = await invocation()
        except asyncio.CancelledError as exc:
            if self._cancel_requested:
                raise
            self._task_fail(index, invocation, _unrequested_cancel(exc))
            return CallbackOutcome()
        except Exception as exc:  # noqa: BLE001
            self._task_fail(index, invocation, exc)
            return CallbackOutcome()
        except BaseException:
            self._cancel_requested = True
            raise
        finally:
            task = asyncio.current_task()
            assert task is not None
            self._live.discard(task)
        return CallbackOutcome(result=result)

    def _on_done(self, task: asyncio.Task[CallbackOutcome[RT_co]]) -> None:
        """Wake the consumer and, unless closed, record the completion.

        Removes the task from the executing set, which matters for a task
        cancelled before its callback started, since ``_execute`` never ran
        for it. Under first-completed execution, queues the task as ready.
        """
        self._wakeup.set()
        if self._closed:
            return
        self._live.discard(task)
        if not self._is_sequential:
            self._ready.append(task)

    def _task_fail(
        self,
        index: int,
        invocation: CallbackInvocation[..., Any],
        exception: Exception,
    ) -> None:
        """Register a callback failure and apply the fail-fast policy.

        When the first failure is recorded under fail-fast policy and the
        executor is open, requests cancellation of all other unfinished
        tasks while the failing one finishes.
        """
        is_first = self._register_exception(index, invocation, exception)

        if is_first and self._is_fail_fast and not self._closed:
            self._cancel_pending(
                exclude=asyncio.current_task(),
            )

    def _register_exception(
        self,
        index: int,
        invocation: CallbackInvocation[..., Any],
        exception: Exception,
    ) -> bool:
        """Record a callback failure and indicate whether it is the first one.

        Args:
            index: Zero-based position of the failed invocation.
            invocation: Original invocation associated with the failure.
            exception: Exception raised by the callback, or the
                CallbackCancelledError recorded in place of an unrequested
                cancellation.

        Returns:
            True if this is the first recorded failure, otherwise False.
        """
        is_first = not self._failures
        self._failures.append(
            CallbackFailure(
                index=index,
                invocation=invocation,
                exception=exception,
            )
        )
        return is_first

    def _cancel_pending(
        self,
        exclude: asyncio.Task[Any] | None = None,
    ) -> None:
        """Request cancellation of all unfinished tasks.

        Marks the cancellations as requested, so they propagate instead of
        being recorded as callback failures. An optional task can be
        excluded, allowing the currently executing callback to finish.

        A task with a cancellation already pending (from fail-fast, or
        from the task group after a fatal BaseException) is not cancelled
        again: a second cancellation would interrupt the cleanup it runs
        on its way out.
        """
        self._cancel_requested = True
        for task in self._live:
            if task is not exclude and not task.cancelling():
                task.cancel()
