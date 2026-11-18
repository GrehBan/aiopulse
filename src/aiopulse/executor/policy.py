"""Execution and exception handling policies for callback execution."""

from __future__ import annotations

from enum import StrEnum
from typing import final


@final
class ExecutionPolicy(StrEnum):
    """Defines the execution and yield policy for callback results.

    Attributes:
        SEQUENTICAL: Results are returned or yielded in the strict order
            of the underlying callback invocations.
        FIRST_COMPLETED: Results are returned or yielded as soon as each
            callback finishes, regardless of its original position.
    """

    SEQUENTIAL = "SEQUENTIAL"
    FIRST_COMPLETED = "FIRST_COMPLETED"


@final
class ExceptionsPolicy(StrEnum):
    """Defines the error-handling policy for callback execution.

    Attributes:
        FAIL_FAST: Stops execution immediately on the first exception,
            encountered cancelling all in-flight tasks
            and raising the error directly.
        COLLECT: Continues execution across tasks, collecting all exceptions
            raised by callbacks and raising them together as an ExceptionGroup.
    """

    FAIL_FAST = "FAIL_FAST"
    COLLECT = "COLLECT"
