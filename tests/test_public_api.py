from __future__ import annotations

import inspect
import sys

import aiopulse
from aiopulse import (
    CallbackCancelledError,
    CallbackExceptionGroup,
    CallbackExecutor,
    CallbackFailure,
    ExceptionsPolicy,
    ExecutionPolicy,
    invoke,
)
from aiopulse import callback as callback_module
from aiopulse import executor as executor_module
from aiopulse import policy as policy_module


def test_package_exposes_documented_public_api() -> None:
    assert aiopulse.__all__ == [
        "invoke",
        "CallbackExecutor",
        "ExceptionsPolicy",
        "ExecutionPolicy",
        "CallbackExceptionGroup",
        "CallbackFailure",
        "CallbackCancelledError",
    ]


def test_public_objects_are_the_defining_module_objects() -> None:
    assert invoke is callback_module.invoke
    assert CallbackFailure is callback_module.CallbackFailure
    assert CallbackExecutor is executor_module.CallbackExecutor
    assert CallbackExceptionGroup is executor_module.CallbackExceptionGroup
    assert CallbackCancelledError is executor_module.CallbackCancelledError
    assert ExceptionsPolicy is policy_module.ExceptionsPolicy
    assert ExecutionPolicy is policy_module.ExecutionPolicy


def test_callback_errors_are_ordinary_exceptions() -> None:
    assert issubclass(CallbackExceptionGroup, ExceptionGroup)
    assert issubclass(CallbackCancelledError, Exception)
    assert not issubclass(CallbackCancelledError, BaseExceptionGroup)


def test_supported_python_version_is_not_below_minimum() -> None:
    assert sys.version_info >= (3, 11)


def test_execution_policy_values() -> None:
    assert ExecutionPolicy.SEQUENTIAL.value == "SEQUENTIAL"
    assert ExecutionPolicy.FIRST_COMPLETED.value == "FIRST_COMPLETED"
    assert inspect.isclass(ExecutionPolicy)


def test_exception_policy_values() -> None:
    assert ExceptionsPolicy.FAIL_FAST.value == "FAIL_FAST"
    assert ExceptionsPolicy.COLLECT.value == "COLLECT"
    assert inspect.isclass(ExceptionsPolicy)


def test_executor_defaults_match_documented_policies() -> None:
    @invoke
    async def callback() -> int:
        return 1

    executor = CallbackExecutor([callback()])

    assert executor._concurrency == 1
    assert executor._buffer_size == 0
    assert executor._execution_policy is ExecutionPolicy.SEQUENTIAL
    assert executor._exception_policy is ExceptionsPolicy.FAIL_FAST
