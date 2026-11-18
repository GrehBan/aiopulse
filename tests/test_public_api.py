from __future__ import annotations

import inspect
import sys

import aiopulse
from aiopulse import (
    CallbackExecutor,
    ExceptionsPolicy,
    ExecutionPolicy,
    invoke,
)


def test_package_exposes_documented_public_api() -> None:
    assert aiopulse.__all__ == [
        "invoke",
        "CallbackExecutor",
        "ExceptionsPolicy",
        "ExecutionPolicy",
    ]


def test_public_objects_are_importable_and_public() -> None:
    assert aiopulse.invoke is invoke
    assert aiopulse.CallbackExecutor is CallbackExecutor
    assert aiopulse.ExceptionsPolicy is ExceptionsPolicy
    assert aiopulse.ExecutionPolicy is ExecutionPolicy
    assert set(aiopulse.__all__) == {
        "invoke",
        "CallbackExecutor",
        "ExceptionsPolicy",
        "ExecutionPolicy",
    }


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
    assert executor._execution_policy is ExecutionPolicy.SEQUENTIAL
    assert executor._exception_propagator.is_fail_fast
