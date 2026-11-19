"""A lightweight asynchronous callback invocation and execution library.

Provides bounded-concurrency execution with configurable result ordering
and callback failure handling.
"""

from __future__ import annotations

from aiopulse.callback import CallbackFailure, invoke
from aiopulse.executor import (
    CallbackCancelledError,
    CallbackExceptionGroup,
    CallbackExecutor,
)
from aiopulse.policy import ExceptionsPolicy, ExecutionPolicy

__all__ = [
    "invoke",
    "CallbackExecutor",
    "ExceptionsPolicy",
    "ExecutionPolicy",
    "CallbackExceptionGroup",
    "CallbackFailure",
    "CallbackCancelledError",
]
