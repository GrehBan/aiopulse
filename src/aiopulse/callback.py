"""Encapsulation and execution of asynchronous callbacks with arguments."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, ParamSpec, TypeAlias

from typing_extensions import TypeVar

RT = TypeVar("RT", default=object)
RT_co = TypeVar("RT_co", covariant=True, default=object)
P = ParamSpec("P")
Callback: TypeAlias = Callable[P, Awaitable[RT]]


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
