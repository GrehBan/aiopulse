"""A lightweight asynchronous callback invocation and execution library.

Provides bounded-concurrency execution with ordered results.
"""

from __future__ import annotations

from aiopulse.callback import invoke
from aiopulse.executor.executor import CallbackExecutor
from aiopulse.executor.policy import ExceptionsPolicy, ExecutionPolicy

__all__ = ["invoke", "CallbackExecutor", "ExceptionsPolicy", "ExecutionPolicy"]
