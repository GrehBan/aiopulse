"""Encapsulation and execution of asynchronous callbacks with arguments."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, ParamSpec, TypeAlias

from typing_extensions import TypeVar

from aiopulse._sentinel import (
    MISSING,
    _Sentinel,  # pyright: ignore[reportPrivateUsage]
)

RT = TypeVar("RT", default=object)
RT_co = TypeVar("RT_co", covariant=True, default=object)
P = ParamSpec("P")
Callback: TypeAlias = Callable[P, Awaitable[RT]]


@dataclass(frozen=True, slots=True)
class CallbackOutcome(Generic[RT_co]):
    """Represents the outcome of a single callback execution.

    Attributes:
        result: The result returned by the successful callback, or a
            sentinel indicating missing/failed state.
    """

    result: RT_co | _Sentinel = MISSING

    def get_result(self) -> RT_co:
        """Get outcome result or raise an error if execution failed.

        Returns:
            The successful callback result.

        Raises:
            RuntimeError: If the outcome has no result because the
                callback failed; the executor records that failure
                separately.
        """
        if self.result is MISSING:
            raise RuntimeError(
                "Callback execution failed without an exception"
            )
        assert not isinstance(self.result, _Sentinel)
        return self.result


@dataclass(frozen=True, slots=True)
class CallbackFailure(Generic[RT_co]):
    """Represent a failed callback invocation with its execution context.

    Attributes:
        index: Zero-based position of the invocation in the input iterable.
        invocation: Original invocation handle, allowing the failed callback
            to be inspected or retried.
        exception: Original exception raised by the callback, or a
            CallbackCancelledError when the callback ended with a
            cancellation that the executor did not request.
    """

    index: int
    invocation: CallbackInvocation[..., RT_co]
    exception: Exception


@dataclass(frozen=True, slots=True)
class CallbackInvocation(Generic[P, RT_co]):
    """Encapsulates a callback and its arguments for invocation.

    Attributes:
        callback: The asynchronous callback to invoke.
        args: Positional arguments for the callback.
        kwargs: Keyword arguments for the callback.
    """

    callback: Callback[P, RT_co]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    async def __call__(self) -> RT_co:
        """Execute the callback with stored arguments and keywords.

        Returns:
            The result returned by the callback execution.
        """
        return await self.callback(*self.args, **self.kwargs)


def invoke(
    callback: Callback[P, RT],
) -> Callable[P, CallbackInvocation[P, RT]]:
    """Convert an asynchronous callback into an invocation factory.

    The returned callable preserves the callback's parameter signature
    and creates a CallbackInvocation without executing the callback.

    Args:
        callback: The asynchronous callback to wrap.

    Returns:
        A callable that accepts the callback's arguments and returns
        a CallbackInvocation containing the callback and supplied arguments.
    """

    @functools.wraps(callback)
    def wrapper(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> CallbackInvocation[P, RT]:
        return CallbackInvocation(
            callback=callback,
            args=args,
            kwargs=kwargs,
        )

    return wrapper
