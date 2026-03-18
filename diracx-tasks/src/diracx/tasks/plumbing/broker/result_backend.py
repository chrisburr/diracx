from __future__ import annotations

__all__ = ["AsyncResultBackend", "RedisResultBackend"]

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import msgpack
from redis.asyncio import BlockingConnectionPool, Redis

from ..exceptions import ResultIsMissingError
from ._types import _BlockingConnectionPool
from .models import TaskResult

_ReturnType = TypeVar("_ReturnType")


class AsyncResultBackend(ABC, Generic[_ReturnType]):
    """Abstract base class for result backends."""

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    @abstractmethod
    async def set_result(
        self, task_id: str, result: TaskResult[_ReturnType]
    ) -> None: ...

    @abstractmethod
    async def is_result_ready(self, task_id: str) -> bool: ...

    @abstractmethod
    async def get_result(self, task_id: str) -> TaskResult[_ReturnType]: ...


class RedisResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend storing results in Redis with msgpack serialization."""

    def __init__(
        self,
        redis_url: str,
        prefix: str = "diracx:results",
        result_ttl_seconds: int | None = None,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: object,
    ) -> None:
        self.redis_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
            url=redis_url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,  # type: ignore[arg-type]
        )
        self.prefix = prefix
        self.result_ttl_seconds = result_ttl_seconds

    def _task_key(self, task_id: str) -> str:
        return f"{self.prefix}:{task_id}"

    async def shutdown(self) -> None:
        await self.redis_pool.disconnect()
        await super().shutdown()

    async def set_result(self, task_id: str, result: TaskResult[_ReturnType]) -> None:
        async with Redis(connection_pool=self.redis_pool) as redis:
            serialized = msgpack.packb(result.model_dump(), datetime=True)
            if self.result_ttl_seconds:
                await redis.setex(
                    name=self._task_key(task_id),
                    time=self.result_ttl_seconds,
                    value=serialized,
                )
            else:
                await redis.set(
                    name=self._task_key(task_id),
                    value=serialized,
                )

    async def is_result_ready(self, task_id: str) -> bool:
        async with Redis(connection_pool=self.redis_pool) as redis:
            return bool(await redis.exists(self._task_key(task_id)))

    async def get_result(self, task_id: str) -> TaskResult[_ReturnType]:
        async with Redis(connection_pool=self.redis_pool) as redis:
            result_bytes = await redis.getdel(name=self._task_key(task_id))

        if result_bytes is None:
            raise ResultIsMissingError(
                f"Result for task {task_id} is missing or has expired"
            )

        result_dict = msgpack.unpackb(result_bytes, timestamp=3)
        return TaskResult[_ReturnType].model_validate(result_dict)
