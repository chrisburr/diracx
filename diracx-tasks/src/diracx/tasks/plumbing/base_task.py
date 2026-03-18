from __future__ import annotations

__all__ = ["BaseTask", "PeriodicBaseTask", "PeriodicVoAwareBaseTask"]

import dataclasses
from abc import ABC, abstractmethod
from contextvars import ContextVar
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from .enums import Priority, Size
from .lock_registry import TASK, LockedObjectType
from .locks import BaseLock, MutexLock
from .retry_policies import NoRetry, RetryPolicyBase
from .schedules import TaskScheduleBase

if TYPE_CHECKING:
    from .broker.models import AsyncDecoratedTask, AsyncTask


class BaseTask(ABC):
    """Base class for all DiracX tasks."""

    priority: ClassVar[Priority] = Priority.NORMAL
    size: ClassVar[Size] = Size.MEDIUM
    retry_policy: ClassVar[RetryPolicyBase] = NoRetry()
    dlq_eligible: ClassVar[bool] = False

    _broker_registry: ClassVar[ContextVar[dict[type, AsyncDecoratedTask] | None]] = (
        ContextVar("_broker_registry", default=None)
    )

    @classmethod
    def bind_broker(
        cls, broker_task_mapping: dict[type[BaseTask], AsyncDecoratedTask]
    ) -> None:
        """Bind task classes to their broker decorated tasks.

        Uses context variables to allow multiple concurrent broker bindings
        in different async contexts.
        """
        if cls._broker_registry.get() is not None:
            raise RuntimeError("Tasks are already bound to a broker in this context")
        cls._broker_registry.set(broker_task_mapping)

    @property
    @abstractmethod
    def execution_locks(self) -> list[BaseLock]:
        """Return a list of locks required by this task."""
        ...

    @abstractmethod
    async def execute(self, **kwargs: Any) -> Any:
        """Run the task logic."""
        ...

    def serialize(self) -> tuple[Any, ...]:
        """Serialize the task to a tuple of arguments for reconstruction."""
        if dataclasses.is_dataclass(self):
            return dataclasses.astuple(self)
        return ()

    async def schedule(self, *, at_time: datetime | None = None) -> AsyncTask:
        """Schedule the task for execution via the broker.

        When ``at_time`` is provided, the task is added to the delayed
        ZSET and will be promoted to a stream when the time arrives.
        """
        registry = self._broker_registry.get()
        if registry is None:
            raise RuntimeError("Task is not bound to a broker")
        broker_task = registry[self.__class__]
        if at_time is not None:
            return await broker_task.kicker().kiq_delayed(at_time, *self.serialize())
        return await broker_task.kiq(*self.serialize())


class PeriodicBaseTask(BaseTask):
    """Base class for periodic tasks.

    Periodic tasks are scheduled by the scheduler process at intervals
    determined by their ``default_schedule``.
    """

    default_schedule: ClassVar[TaskScheduleBase]
    _enabled: ClassVar[bool] = True

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [
            MutexLock(LockedObjectType(TASK), self.__class__.__name__),
        ]


class PeriodicVoAwareBaseTask(PeriodicBaseTask):
    """Base class for periodic tasks that run per-VO.

    The scheduler creates one instance per VO, each with its own
    schedule and lock key that includes the VO name.
    """

    vo: str

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [
            MutexLock(LockedObjectType(TASK), self.__class__.__name__, self.vo),
        ]
