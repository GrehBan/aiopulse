from __future__ import annotations

from collections.abc import Iterable
from typing import assert_type

from aiopulse import CallbackExecutor, invoke
from aiopulse.callback import CallbackFailure, CallbackInvocation
from aiopulse.executor import CallbackExceptionGroup


class Animal:
    pass


class Dog(Animal):
    pass


@invoke
async def identify(value: int, *, prefix: str = "item") -> str:
    return f"{prefix}-{value}"


@invoke
async def make_dog() -> Dog:
    return Dog()


def accept_animal_executor(_executor: CallbackExecutor[Animal]) -> None:
    pass


async def check_invocation_types() -> None:
    invocation = identify(1, prefix="id")
    assert_type(await invocation(), str)


async def check_executor_types() -> None:
    invocations = [identify(1), identify(2)]
    executor = CallbackExecutor(invocations)
    assert_type(executor, CallbackExecutor[str])

    results: list[str] = [result async for result in executor]
    assert_type(results, list[str])

    dogs: Iterable[CallbackInvocation[..., Dog]] = [make_dog()]
    dog_executor = CallbackExecutor(dogs)
    accept_animal_executor(dog_executor)
    assert_type(await anext(dog_executor), Dog)


def check_exception_group_types() -> None:
    failure = CallbackFailure(0, identify(1), LookupError("missing"))
    group = CallbackExceptionGroup("failures", (failure,))
    assert_type(group, CallbackExceptionGroup[Exception, str])
    assert_type(group.failures, tuple[CallbackFailure[str], ...])

    subgroup = group.derive([LookupError("missing")])
    assert_type(subgroup, CallbackExceptionGroup[LookupError, str])
