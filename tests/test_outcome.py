from __future__ import annotations

import dataclasses

import pytest

from aiopulse._sentinel import MISSING
from aiopulse.executor.executor import CallbackOutcome


def test_successful_outcome_returns_original_result() -> None:
    value = {"items": [1, 2, 3]}

    outcome = CallbackOutcome(result=value)

    assert outcome.result is value
    assert outcome.get_result() is value


def test_none_is_a_valid_outcome_result() -> None:
    outcome = CallbackOutcome(result=None)

    assert outcome.result is None
    assert outcome.get_result() is None


def test_missing_outcome_get_result_raises_runtime_error() -> None:
    outcome = CallbackOutcome()

    assert outcome.result is MISSING
    with pytest.raises(
        RuntimeError,
        match="Callback execution failed without an exception",
    ):
        outcome.get_result()


@pytest.mark.parametrize("value", [False, 0, "", []])
def test_falsy_successful_results_are_not_missing(value: object) -> None:
    outcome = CallbackOutcome(result=value)

    assert outcome.result is value
    assert outcome.get_result() is value


def test_callback_outcome_is_frozen_and_slotted() -> None:
    outcome = CallbackOutcome(result=42)

    assert dataclasses.is_dataclass(outcome)
    assert hasattr(outcome, "__slots__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        outcome.result = 43  # type: ignore[misc]
