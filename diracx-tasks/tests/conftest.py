"""Shared fixtures for diracx-tasks integration tests."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.broker.base import AsyncBroker
from diracx.tasks.plumbing.broker.models import BrokerMessage
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import wrap_task
from diracx.tasks.plumbing.lock_registry import TASK, LockedObjectType
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff, NoRetry

# ---------------------------------------------------------------------------
# Test task definitions
# ---------------------------------------------------------------------------


class SuccessTask(BaseTask):
    """A task that always succeeds."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    @property
    def execution_locks(self) -> list[BaseLock]:
        return []

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


@dataclasses.dataclass
class FailOnceTask(BaseTask):
    """A task that fails on the first attempt, succeeds on retries."""

    call_count: int = 0
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = ExponentialBackoff(base_delay_seconds=1, max_retries=3)

    @property
    def execution_locks(self) -> list[BaseLock]:
        return []

    async def execute(self, **kwargs: Any) -> str:
        # Use a class-level counter to track calls across instances
        FailOnceTask._call_count = getattr(FailOnceTask, "_call_count", 0) + 1
        if FailOnceTask._call_count <= 1:
            raise RuntimeError("Simulated failure")
        return "recovered"


class DLQTask(BaseTask):
    """A task that always fails and is DLQ-eligible."""

    priority = Priority.NORMAL
    size = Size.MEDIUM
    retry_policy = NoRetry()
    dlq_eligible = True

    @property
    def execution_locks(self) -> list[BaseLock]:
        return []

    async def execute(self, **kwargs: Any) -> str:
        raise RuntimeError("Always fails")


class LockedTask(BaseTask):
    """A task with a mutex lock."""

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(LockedObjectType(TASK), "LockedTask")]

    async def execute(self, **kwargs: Any) -> str:
        return "locked_ok"


# ---------------------------------------------------------------------------
# In-memory broker for testing
# ---------------------------------------------------------------------------


class InMemoryBroker(AsyncBroker):
    """A simple in-memory broker for unit/integration tests.

    Stores messages in a list instead of Redis streams.
    """

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[BrokerMessage] = []
        self.kicked: list[BrokerMessage] = []
        # Create a mock connection pool so Worker._get_redis doesn't fail
        self.connection_pool = None  # type: ignore[assignment]

    async def kick(self, message: BrokerMessage) -> None:
        self.kicked.append(message)

    async def listen(self):  # type: ignore[override]
        # Not used in unit tests
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def in_memory_broker():
    return InMemoryBroker()


@pytest.fixture
def task_class_registry():
    return {
        "test:SuccessTask": SuccessTask,
        "test:FailOnceTask": FailOnceTask,
        "test:DLQTask": DLQTask,
        "test:LockedTask": LockedTask,
    }


@pytest.fixture
def wrapped_registry(task_class_registry):
    return {name: wrap_task(cls) for name, cls in task_class_registry.items()}
