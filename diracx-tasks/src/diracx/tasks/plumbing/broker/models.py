from __future__ import annotations

__all__ = [
    "TaskMessage",
    "BrokerMessage",
    "TaskResult",
    "AckableMessage",
    "AsyncKicker",
    "AsyncDecoratedTask",
    "AsyncTask",
]

import asyncio
import logging
import traceback
from dataclasses import asdict, is_dataclass
from datetime import datetime
from time import time
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Generic, TypeVar

import msgpack
from pydantic import BaseModel, ConfigDict, Field

from ..exceptions import (
    ResultGetError,
    ResultIsReadyError,
    SendTaskError,
    TaskResultTimeoutError,
)

if TYPE_CHECKING:
    from .base import AsyncBroker
    from .result_backend import AsyncResultBackend

logger = logging.getLogger(__name__)

_ReturnType = TypeVar("_ReturnType")


class TaskMessage(BaseModel):
    """Internal message format used by the task system."""

    task_id: str
    task_name: str
    labels: dict[str, Any]
    task_args: list[Any]
    task_kwargs: dict[str, Any]


class BrokerMessage(BaseModel):
    """Wire-protocol message wrapping a TaskMessage."""

    task_id: str
    task_name: str
    message: bytes  # serialized TaskMessage (msgpack)
    labels: dict[str, Any]

    @staticmethod
    def dumpb(value: TaskMessage) -> bytes:
        return msgpack.packb(value.model_dump(), datetime=True)

    @staticmethod
    def loadb(data: bytes) -> TaskMessage:
        return TaskMessage.model_validate(msgpack.unpackb(data, timestamp=3))

    @classmethod
    def from_task_message(cls, task_message: TaskMessage) -> BrokerMessage:
        return cls(
            task_id=task_message.task_id,
            task_name=task_message.task_name,
            message=cls.dumpb(task_message),
            labels=task_message.labels,
        )

    def to_task_message(self) -> TaskMessage:
        return self.loadb(self.message)


class TaskResult(BaseModel, Generic[_ReturnType]):
    """Result of task execution."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    is_err: bool
    return_value: _ReturnType
    execution_time: float
    error: dict[str, str] | None = None
    labels: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        execution_time: float,
        labels: dict[str, Any] | None = None,
    ) -> TaskResult[None]:
        return TaskResult[None](
            is_err=True,
            return_value=None,
            execution_time=execution_time,
            error={
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
            },
            labels=labels or {},
        )

    @classmethod
    def from_value(
        cls,
        value: _ReturnType,
        execution_time: float,
        labels: dict[str, Any] | None = None,
    ) -> TaskResult[_ReturnType]:
        return cls(
            is_err=False,
            return_value=value,
            execution_time=execution_time,
            error=None,
            labels=labels or {},
        )

    def raise_for_error(self) -> TaskResult[_ReturnType]:
        if self.is_err and self.error:
            exc = Exception(f"{self.error['type']}: {self.error['message']}")
            raise exc
        return self


class AckableMessage(BaseModel):
    """Message that can be acknowledged after processing."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: bytes
    ack: Callable[[], Awaitable[None]]


class AsyncKicker(Generic[_ReturnType]):
    """Builds and sends task messages to the broker."""

    def __init__(
        self,
        task_name: str,
        broker: AsyncBroker,
        labels: dict[str, Any],
    ) -> None:
        self.task_name = task_name
        self.broker = broker
        self.labels = labels.copy()
        self.custom_task_id: str | None = None

    def with_labels(self, **labels: str | float) -> AsyncKicker[_ReturnType]:
        self.labels.update(labels)
        return self

    def with_task_id(self, task_id: str | None) -> AsyncKicker[_ReturnType]:
        self.custom_task_id = task_id
        return self

    async def kiq(self, *args: Any, **kwargs: Any) -> AsyncTask[_ReturnType]:
        """Send the task to the broker for execution."""
        task_message = self._prepare_message(*args, **kwargs)
        broker_message = BrokerMessage.from_task_message(task_message)

        try:
            await self.broker.kick(broker_message)
        except Exception as exc:
            raise SendTaskError(
                f"Failed to send task {self.task_name} to broker"
            ) from exc

        return AsyncTask[_ReturnType](
            task_id=task_message.task_id,
            result_backend=self.broker.result_backend,
        )

    async def kiq_delayed(
        self, run_at: datetime, *args: Any, **kwargs: Any
    ) -> AsyncTask[_ReturnType]:
        """Schedule the task for future execution via the delayed ZSET.

        Instead of XADDing to a stream immediately, ZADDs to the delayed
        sorted set. The scheduler's delayed poll loop promotes the task
        to the appropriate stream when ``run_at`` arrives.
        """
        from ..scheduler.scheduler import TaskScheduler

        task_message = self._prepare_message(*args, **kwargs)
        broker_message = BrokerMessage.from_task_message(task_message)

        from redis.asyncio import Redis

        try:
            redis = Redis(connection_pool=self.broker.connection_pool)
            async with redis:
                await TaskScheduler.schedule_delayed(redis, broker_message, run_at)
        except Exception as exc:
            raise SendTaskError(
                f"Failed to schedule delayed task {self.task_name}"
            ) from exc

        return AsyncTask[_ReturnType](
            task_id=task_message.task_id,
            result_backend=self.broker.result_backend,
        )

    @classmethod
    def _prepare_arg(cls, arg: Any) -> Any:
        if isinstance(arg, BaseModel):
            arg = arg.model_dump()
        if is_dataclass(arg) and not isinstance(arg, type):
            arg = asdict(arg)
        return arg

    def _prepare_message(self, *args: Any, **kwargs: Any) -> TaskMessage:
        formatted_args = [self._prepare_arg(arg) for arg in args]
        formatted_kwargs = {k: self._prepare_arg(v) for k, v in kwargs.items()}

        task_id = self.custom_task_id
        if task_id is None:
            task_id = self.broker.id_generator()

        return TaskMessage(
            task_id=task_id,
            task_name=self.task_name,
            labels=self.labels.copy(),
            task_args=formatted_args,
            task_kwargs=formatted_kwargs,
        )


class AsyncDecoratedTask(Generic[_ReturnType]):
    """Wrapper for task functions providing kiq() for broker dispatch."""

    def __init__(
        self,
        broker: AsyncBroker,
        task_name: str,
        original_func: Callable[..., _ReturnType],
        labels: dict[str, Any],
    ) -> None:
        self.broker = broker
        self.task_name = task_name
        self.original_func = original_func
        self.labels = labels

    def __call__(self, *args: Any, **kwargs: Any) -> _ReturnType:
        return self.original_func(*args, **kwargs)

    async def kiq(self, *args: Any, **kwargs: Any) -> AsyncTask[_ReturnType]:
        return await self.kicker().kiq(*args, **kwargs)

    def kicker(self) -> AsyncKicker[_ReturnType]:
        return AsyncKicker(
            task_name=self.task_name,
            broker=self.broker,
            labels=self.labels.copy(),
        )

    def __repr__(self) -> str:
        return f"AsyncDecoratedTask({self.task_name})"


class AsyncTask(Generic[_ReturnType]):
    """Handle for tracking a submitted task's result."""

    def __init__(
        self,
        task_id: str,
        result_backend: AsyncResultBackend[_ReturnType] | None,
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend

    async def is_ready(self) -> bool:
        if self.result_backend is None:
            raise ResultIsReadyError("No result backend configured")
        try:
            return await self.result_backend.is_result_ready(self.task_id)
        except Exception as exc:
            raise ResultIsReadyError(
                f"Failed to check if task {self.task_id} is ready"
            ) from exc

    async def get_result(self) -> TaskResult[_ReturnType]:
        if self.result_backend is None:
            raise ResultGetError("No result backend configured")
        try:
            return await self.result_backend.get_result(self.task_id)
        except Exception as exc:
            raise ResultGetError(
                f"Failed to get result for task {self.task_id}"
            ) from exc

    async def wait_result(
        self,
        check_interval: float = 0.2,
        timeout: float = -1.0,
    ) -> TaskResult[_ReturnType]:
        if self.result_backend is None:
            raise ResultGetError("No result backend configured")
        start_time = time()
        while True:
            try:
                return await self.result_backend.get_result(self.task_id)
            except ResultGetError:
                pass  # Result not ready yet
            if 0 < timeout < time() - start_time:
                raise TaskResultTimeoutError(timeout=timeout)
            await asyncio.sleep(check_interval)
