from __future__ import annotations

import dataclasses

import pytest

from aiopulse.callback import CallbackFailure, invoke
from aiopulse.executor import CallbackExceptionGroup


def test_callback_failure_keeps_invocation_and_exception_context() -> None:
    error = ValueError("failed")

    @invoke
    async def callback() -> int:
        return 1

    invocation = callback()
    failure = CallbackFailure(
        index=3,
        invocation=invocation,
        exception=error,
    )

    assert failure.index == 3
    assert failure.invocation is invocation
    assert failure.exception is error
    assert dataclasses.is_dataclass(failure)
    assert hasattr(failure, "__slots__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        failure.index = 4  # type: ignore[misc]


def test_callback_exception_group_preserves_metadata() -> None:
    first_error = ValueError("first")
    second_error = LookupError("second")

    @invoke
    async def callback(value: int) -> int:
        return value

    first_invocation = callback(1)
    second_invocation = callback(2)
    failures = (
        CallbackFailure(1, first_invocation, first_error),
        CallbackFailure(2, second_invocation, second_error),
    )

    group = CallbackExceptionGroup(
        "callback failures",
        failures,
    )

    assert group.message == "callback failures"
    assert group.exceptions == (first_error, second_error)
    assert group.failures == failures
    assert repr(group) == (
        "CallbackExceptionGroup('callback failures', "
        "errors=[(index=1, exc=ValueError('first')), "
        "(index=2, exc=LookupError('second'))])"
    )


def test_derive_preserves_metadata_for_selected_exceptions() -> None:
    first_error = ValueError("first")
    second_error = LookupError("second")

    @invoke
    async def callback(value: int) -> int:
        return value

    first = CallbackFailure(1, callback(1), first_error)
    second = CallbackFailure(2, callback(2), second_error)
    group = CallbackExceptionGroup("callback failures", (first, second))

    derived = group.derive((second_error,))
    matched, rest = group.split(ValueError)

    assert derived.message == group.message
    assert derived.exceptions == (second_error,)
    assert derived.failures == (second,)
    assert isinstance(matched, CallbackExceptionGroup)
    assert isinstance(rest, CallbackExceptionGroup)
    assert matched.failures == (first,)
    assert rest.failures == (second,)


def test_derive_preserves_metadata_for_nested_exception_groups() -> None:
    nested_error = ExceptionGroup(
        "nested",
        [ValueError("matched"), LookupError("remaining")],
    )

    @invoke
    async def callback() -> int:
        return 1

    failure = CallbackFailure(0, callback(), nested_error)
    group = CallbackExceptionGroup("callback failures", (failure,))

    matched, rest = group.split(ValueError)

    assert isinstance(matched, CallbackExceptionGroup)
    assert isinstance(rest, CallbackExceptionGroup)
    assert matched.failures == (failure,)
    assert rest.failures == (failure,)
    matched_nested = matched.exceptions[0]
    rest_nested = rest.exceptions[0]
    assert isinstance(matched_nested, ExceptionGroup)
    assert isinstance(rest_nested, ExceptionGroup)
    assert matched_nested.exceptions == (nested_error.exceptions[0],)
    assert rest_nested.exceptions == (nested_error.exceptions[1],)


def test_except_star_keeps_callback_failure_metadata() -> None:
    value_error = ValueError("value")
    lookup_error = LookupError("lookup")

    @invoke
    async def callback(index: int) -> int:
        return index

    failures = (
        CallbackFailure(0, callback(0), value_error),
        CallbackFailure(1, callback(1), lookup_error),
    )
    group = CallbackExceptionGroup("callback failures", failures)
    caught: list[CallbackExceptionGroup[Exception, int]] = []

    try:
        raise group
    except* ValueError as matched:
        assert isinstance(matched, CallbackExceptionGroup)
        caught.append(matched)
    except* LookupError as matched:
        assert isinstance(matched, CallbackExceptionGroup)
        caught.append(matched)

    assert [item.exceptions[0] for item in caught] == [
        value_error,
        lookup_error,
    ]
    assert [item.failures for item in caught] == [
        (failures[0],),
        (failures[1],),
    ]


def test_derive_rejects_exceptions_without_matching_failure_records() -> None:
    error = ValueError("original")

    @invoke
    async def callback() -> int:
        return 1

    group = CallbackExceptionGroup(
        "callback failures",
        (CallbackFailure(0, callback(), error),),
    )

    with pytest.raises(
        ValueError,
        match="derived exceptions must come from this group",
    ):
        group.derive((LookupError("unrelated"),))

    with pytest.raises(
        ValueError,
        match="derived exceptions must come from this group",
    ):
        group.derive((KeyboardInterrupt(),))

    nested = ExceptionGroup("nested", [ValueError("child")])
    nested_group = CallbackExceptionGroup(
        "callback failures",
        (CallbackFailure(0, callback(), nested),),
    )
    unrelated_nested = ExceptionGroup("nested", [TypeError("child")])
    renamed_nested = ExceptionGroup("renamed", list(nested.exceptions))

    for candidate in (unrelated_nested, renamed_nested):
        with pytest.raises(
            ValueError,
            match="derived exceptions must come from this group",
        ):
            nested_group.derive((candidate,))


def test_derive_preserves_duplicate_exception_occurrences() -> None:
    error = ValueError("repeated")

    @invoke
    async def callback(index: int) -> int:
        return index

    failures = (
        CallbackFailure(0, callback(0), error),
        CallbackFailure(1, callback(1), error),
    )
    group = CallbackExceptionGroup("callback failures", failures)

    derived = group.derive((error, error))
    matched, rest = group.split(ValueError)

    assert derived.exceptions == (error, error)
    assert derived.failures == failures
    assert isinstance(matched, CallbackExceptionGroup)
    assert matched.failures == failures
    assert rest is None
    with pytest.raises(ValueError):
        group.derive(())


def test_rejects_exceptions_that_do_not_match_failure_records() -> None:
    error = ValueError("failed")

    @invoke
    async def callback() -> int:
        return 1

    failures = (CallbackFailure(0, callback(), error),)

    for exceptions in ((), (error, error)):
        with pytest.raises(
            ValueError,
            match="one exception is required for each failure",
        ):
            CallbackExceptionGroup(
                "callback failures",
                failures,
                _exceptions=exceptions,
            )
