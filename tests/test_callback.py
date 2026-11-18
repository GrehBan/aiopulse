from __future__ import annotations

import dataclasses
import inspect
from typing import Any

import pytest

from aiopulse.callback import CallbackInvocation, invoke


@pytest.mark.asyncio
async def test_callback_invocation_stores_and_forwards_arguments() -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def callback(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
        calls.append((args, kwargs))
        return (*args, kwargs)

    invocation = CallbackInvocation(
        callback=callback,
        args=(1, "two"),
        kwargs={"flag": True},
    )

    result = await invocation()

    assert invocation.callback is callback
    assert invocation.args == (1, "two")
    assert invocation.kwargs == {"flag": True}
    assert calls == [((1, "two"), {"flag": True})]
    assert result == (1, "two", {"flag": True})


@pytest.mark.asyncio
async def test_callback_invocation_supports_none_and_repeated_calls() -> None:
    count = 0

    async def callback() -> None:
        nonlocal count
        count += 1

    invocation = CallbackInvocation(callback, (), {})

    assert await invocation() is None
    assert await invocation() is None
    assert count == 2


@pytest.mark.asyncio
async def test_callback_invocation_propagates_exception_identity() -> None:
    error = LookupError("missing")

    async def callback() -> int:
        raise error

    invocation = CallbackInvocation(callback, (), {})

    with pytest.raises(LookupError) as caught:
        await invocation()
    assert caught.value is error


def test_callback_invocation_is_frozen_and_slotted() -> None:
    async def callback() -> int:
        return 1

    invocation = CallbackInvocation(callback, (), {})

    assert dataclasses.is_dataclass(invocation)
    assert hasattr(invocation, "__slots__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        invocation.args = ()  # type: ignore[misc]


def test_invoke_creates_lazy_factory_and_preserves_metadata() -> None:
    async def original(value: int, *, scale: int = 2) -> str:
        """Original callback documentation."""
        return str(value * scale)

    factory = invoke(original)
    invocation = factory(3, scale=4)

    assert isinstance(invocation, CallbackInvocation)
    assert invocation.callback is original
    assert invocation.args == (3,)
    assert invocation.kwargs == {"scale": 4}
    assert factory.__name__ == original.__name__
    assert factory.__qualname__ == original.__qualname__
    assert factory.__doc__ == original.__doc__
    assert str(inspect.signature(factory)) == (
        "(value: 'int', *, scale: 'int' = 2) -> 'str'"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "kwargs", "expected"),
    [
        ((2, 3), {}, 6),
        ((), {"left": 2, "right": 3}, 6),
        ((2,), {"right": 3}, 6),
    ],
)
async def test_invoke_accepts_positional_keyword_and_mixed_arguments(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    expected: int,
) -> None:
    @invoke
    async def multiply(left: int, right: int) -> int:
        return left * right

    invocation = multiply(*args, **kwargs)
    assert await invocation() == expected


@pytest.mark.asyncio
async def test_invoke_supports_no_parameters_and_result_kinds() -> None:
    @invoke
    async def no_parameters() -> None:
        return None

    @invoke
    async def primitive() -> int:
        return 0

    @invoke
    async def complex_value() -> dict[str, list[int]]:
        return {"values": [1, 2]}

    assert await no_parameters()() is None
    assert await primitive()() == 0
    assert await complex_value()() == {"values": [1, 2]}
