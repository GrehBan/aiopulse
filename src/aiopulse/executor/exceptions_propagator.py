"""Exception propagation."""

from __future__ import annotations

from collections import deque

from aiopulse.executor.policy import ExceptionsPolicy


class ExceptionsPropagator:
    """Manages and propagates exceptions according to policy rules.

    Supports fail-fast immediate propagation and error collection via
    ExceptionGroup based on the configured ExceptionsPolicy.
    """

    def __init__(
        self,
        policy: ExceptionsPolicy = ExceptionsPolicy.FAIL_FAST,
    ) -> None:
        """Initialize the propagator with an error-handling policy.

        Args:
            policy: The exception policy (FAIL_FAST or COLLECT).
        """
        self._exceptions: deque[Exception] = deque()
        self._policy = policy

    @property
    def is_fail_fast(self) -> bool:
        """Return True if exceptions policy is ``ExceptionsPolicy.FAIL_FAST``.

        Returns:
            True if exceptions policy is ``ExceptionsPolicy.FAIL_FAST``
        """
        return self._policy == ExceptionsPolicy.FAIL_FAST

    @property
    def is_collect(self) -> bool:
        """Return True if exceptions policy is ``ExceptionsPolicy.COLLECT``.

        Returns:
            True if exceptions policy is ``ExceptionsPolicy.COLLECT``
        """
        return self._policy == ExceptionsPolicy.COLLECT

    @property
    def has_exceptions(self) -> bool:
        """Return True if any exceptions have been registered.

        Returns:
            True if exceptions queue is not empty, False otherwise.
        """
        return bool(self._exceptions)

    def register_exception(self, exception: Exception) -> bool:
        """Register a caught exception for propagation.

        Args:
            exception: The exception instance to record.

        Returns:
            True if registered exception first in order
        """
        is_first = not self._exceptions
        self._exceptions.append(exception)
        return is_first

    def clear(self) -> None:
        """Clear all recorded exceptions."""
        self._exceptions.clear()

    def propagate(self) -> None:
        """Propagate recorded exceptions.

        Raises:
            ExceptionGroup: Group of collected exceptions.
        """
        if not self._exceptions:
            return

        exceptions = tuple(self._exceptions)
        self._exceptions.clear()

        if len(exceptions) == 1:
            raise exceptions[0]

        raise ExceptionGroup(
            "Exceptions raised while executing callbacks",
            exceptions,
        )
