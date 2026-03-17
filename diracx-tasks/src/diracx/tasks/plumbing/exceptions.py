from __future__ import annotations

__all__ = [
    "UnableToAcquireLock",
    "StopRetryingException",
    "TooManyRetries",
    "UnretryableError",
    "TaskRetryRequested",
    "SendTaskError",
    "ResultIsMissingError",
    "ResultIsReadyError",
    "ResultGetError",
    "TaskResultTimeoutError",
]


class UnableToAcquireLock(Exception):
    """Lock acquisition failed."""


class StopRetryingException(Exception):
    """Base exception to indicate that retries should stop."""


class TooManyRetries(StopRetryingException):
    """Task has exceeded its retry limit."""


class UnretryableError(StopRetryingException):
    """Task should not be retried."""


class TaskRetryRequested(Exception):
    """Raised by tasks that want to explicitly request a retry."""


class SendTaskError(Exception):
    """Raised when a task cannot be sent to the broker."""


class ResultIsMissingError(Exception):
    """Raised when trying to get a result that doesn't exist."""


class ResultIsReadyError(Exception):
    """Raised when we can't check if result is ready."""


class ResultGetError(Exception):
    """Raised when we can't get result from backend."""


class TaskResultTimeoutError(Exception):
    """Raised when waiting for result times out."""

    def __init__(self, timeout: float):
        self.timeout = timeout
        super().__init__(f"Task did not complete within {timeout} seconds")
