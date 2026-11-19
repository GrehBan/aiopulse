"""Execution and exception handling policies for callback execution."""

from __future__ import annotations

from enum import StrEnum
from typing import final


@final
class ExecutionPolicy(StrEnum):
    """Defines the order in which successful results are yielded.

    The policy does not change how many callbacks run concurrently.

    Attributes:
        SEQUENTIAL: Results are yielded in the order of the input
            invocations; a finished result waits for earlier ones.
        FIRST_COMPLETED: Results are yielded in the order in which the
            executor observes their callbacks finish, regardless of
            their input position.
    """

    SEQUENTIAL = "SEQUENTIAL"
    FIRST_COMPLETED = "FIRST_COMPLETED"


@final
class ExceptionsPolicy(StrEnum):
    """Defines how callback failures affect execution.

    In both policies, recorded failures are raised together as one
    CallbackExceptionGroup when the executor closes.

    Attributes:
        FAIL_FAST: On the first recorded failure, cancels the other
            unfinished callbacks, schedules no further invocations and
            raises the failures recorded until shutdown completes.
        COLLECT: Records each failure and keeps executing the remaining
            invocations; the failures are raised after the successful
            results have been yielded.
    """

    FAIL_FAST = "FAIL_FAST"
    COLLECT = "COLLECT"
