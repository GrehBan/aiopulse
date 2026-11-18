from __future__ import annotations

import pytest

from aiopulse.executor.exceptions_propagator import ExceptionsPropagator
from aiopulse.executor.policy import ExceptionsPolicy


def test_default_policy_and_state() -> None:
    propagator = ExceptionsPropagator()

    assert propagator.is_fail_fast
    assert not propagator.is_collect
    assert not propagator.has_exceptions
    propagator.propagate()


@pytest.mark.parametrize(
    ("policy", "is_fail_fast", "is_collect"),
    [
        (ExceptionsPolicy.FAIL_FAST, True, False),
        (ExceptionsPolicy.COLLECT, False, True),
    ],
)
def test_policy_identification(
    policy: ExceptionsPolicy,
    is_fail_fast: bool,
    is_collect: bool,
) -> None:
    propagator = ExceptionsPropagator(policy)

    assert propagator.is_fail_fast is is_fail_fast
    assert propagator.is_collect is is_collect


def test_register_exception_returns_first_and_retains_order() -> None:
    propagator = ExceptionsPropagator(ExceptionsPolicy.COLLECT)
    first = ValueError("first")
    second = LookupError("second")

    assert propagator.register_exception(first)
    assert propagator.has_exceptions
    assert not propagator.register_exception(second)
    assert propagator._exceptions == __import__(
        "collections",
        fromlist=["deque"],
    ).deque([first, second])


@pytest.mark.parametrize(
    "policy",
    [ExceptionsPolicy.FAIL_FAST, ExceptionsPolicy.COLLECT],
)
def test_single_exception_is_raised_as_original_instance(
    policy: ExceptionsPolicy,
) -> None:
    propagator = ExceptionsPropagator(policy)
    error = RuntimeError("single")
    propagator.register_exception(error)

    with pytest.raises(RuntimeError) as caught:
        propagator.propagate()

    assert caught.value is error
    assert not propagator.has_exceptions
    propagator.propagate()


@pytest.mark.parametrize(
    "policy",
    [ExceptionsPolicy.FAIL_FAST, ExceptionsPolicy.COLLECT],
)
def test_multiple_exceptions_are_an_ordered_exception_group(
    policy: ExceptionsPolicy,
) -> None:
    propagator = ExceptionsPropagator(policy)
    first = ValueError("first")
    second = LookupError("second")
    third = RuntimeError("third")

    propagator.register_exception(first)
    propagator.register_exception(second)
    propagator.register_exception(third)

    with pytest.raises(ExceptionGroup) as caught:
        propagator.propagate()

    assert (
        caught.value.message == "Exceptions raised while executing callbacks"
    )
    assert caught.value.exceptions == (first, second, third)
    assert not propagator.has_exceptions

    propagator.propagate()


def test_clear_removes_registered_exceptions() -> None:
    propagator = ExceptionsPropagator()
    propagator.register_exception(ValueError("one"))
    propagator.register_exception(LookupError("two"))

    propagator.clear()

    assert not propagator.has_exceptions
    propagator.propagate()
