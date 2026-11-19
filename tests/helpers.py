"""Shared helpers for executor tests."""

from __future__ import annotations

import asyncio
from typing import Any, TypeVar

from aiopulse import CallbackExecutor

T = TypeVar("T")


async def collect(executor: CallbackExecutor[T]) -> list[T]:
    """Consume the executor to exhaustion and return its results."""
    return [result async for result in executor]


def unfinished_tasks() -> set[asyncio.Task[Any]]:
    """Return tasks of the running loop that are not finished yet.

    The calling task is excluded, so an empty set means that no callback
    task is left behind.
    """
    return asyncio.all_tasks() - {asyncio.current_task()}


async def settle(steps: int = 5) -> None:
    """Let ready tasks and done callbacks run for a few loop iterations."""
    for _ in range(steps):
        await asyncio.sleep(0)
