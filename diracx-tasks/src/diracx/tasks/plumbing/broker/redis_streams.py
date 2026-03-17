from __future__ import annotations

__all__ = ["RedisStreamBroker"]

import logging
import uuid
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Optional

import msgpack
from redis.asyncio import BlockingConnectionPool, Connection, Redis, ResponseError

from ..enums import Priority, Size
from .base import AsyncBroker
from .models import AckableMessage, BrokerMessage
from .result_backend import AsyncResultBackend

if TYPE_CHECKING:
    from typing import TypeAlias, TypeVar

    _T = TypeVar("_T")
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool[Connection]  # type: ignore[type-arg]
else:
    from typing import TypeAlias

    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool

logger = logging.getLogger(__name__)

# The nine streams: one per (priority, size) pair
ALL_STREAM_NAMES = [f"diracx:tasks:{p}:{s}" for p in Priority for s in Size]


def stream_name_for(priority: Priority | str, size: Size | str) -> str:
    """Return the Redis stream name for a given priority+size."""
    return f"diracx:tasks:{priority}:{size}"


class RedisStreamBroker(AsyncBroker):
    """Redis broker using streams with 9 priority x size queues.

    Workers of a given size listen to 3 priority streams with strict
    ordering: REALTIME first, then NORMAL, then BACKGROUND.
    """

    def __init__(
        self,
        url: str,
        *,
        worker_size: Size = Size.MEDIUM,
        consumer_group_name: str = "diracx:tasks:workers",
        consumer_name: Optional[str] = None,
        mkstream: bool = True,
        xread_block: int = 2000,
        xread_count: int = 10,
        maxlen: Optional[int] = None,
        approximate: bool = True,
        idle_timeout: int = 600000,
        unacknowledged_batch_size: int = 100,
        max_connection_pool_size: int | None = None,
        result_backend: Optional[AsyncResultBackend[Any]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.connection_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
            url=url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        self.worker_size = worker_size
        self.consumer_group_name = consumer_group_name
        self.consumer_name = consumer_name or str(uuid.uuid4())
        self.mkstream = mkstream
        self.block = xread_block
        self.count = xread_count
        self.maxlen = maxlen
        self.approximate = approximate
        self.idle_timeout = idle_timeout
        self.unacknowledged_batch_size = unacknowledged_batch_size

    @property
    def _listen_streams(self) -> list[str]:
        """Streams this worker listens to, in strict priority order."""
        return [
            stream_name_for(p, self.worker_size)
            for p in (Priority.REALTIME, Priority.NORMAL, Priority.BACKGROUND)
        ]

    async def _declare_consumer_groups(self) -> None:
        """Ensure consumer groups exist for all streams."""
        async with Redis(connection_pool=self.connection_pool) as redis:
            for sname in ALL_STREAM_NAMES:
                try:
                    await redis.xgroup_create(
                        sname,
                        self.consumer_group_name,
                        id="$",
                        mkstream=self.mkstream,
                    )
                except ResponseError:
                    pass  # Group already exists

    async def startup(self) -> None:
        await super().startup()
        await self._declare_consumer_groups()

    async def shutdown(self) -> None:
        await super().shutdown()
        await self.connection_pool.disconnect()

    async def kick(self, message: BrokerMessage) -> None:
        """Send a message to the appropriate priority x size stream."""
        priority = message.labels.get("priority", Priority.NORMAL)
        size = message.labels.get("size", Size.MEDIUM)
        target_stream = stream_name_for(priority, size)

        async with Redis(connection_pool=self.connection_pool) as redis:
            serialized = msgpack.packb(message.model_dump(), datetime=True)
            await redis.xadd(
                target_stream,
                {b"data": serialized},
                maxlen=self.maxlen,
                approximate=self.approximate,
            )

    def _ack_generator(
        self, msg_id: str | bytes, queue_name: str | bytes
    ) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            async with Redis(connection_pool=self.connection_pool) as redis:
                await redis.xack(
                    queue_name,
                    self.consumer_group_name,
                    msg_id,
                )

        return _ack

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Yield messages from streams in strict priority order.

        Drains higher-priority streams before moving to lower ones.
        """
        async with Redis(connection_pool=self.connection_pool) as redis:
            streams = {s: ">" for s in self._listen_streams}

            while True:
                # Read from streams in priority order
                fetched = await redis.xreadgroup(
                    self.consumer_group_name,
                    self.consumer_name,
                    streams,
                    block=self.block,
                    noack=False,
                    count=self.count,
                )

                for stream, msg_list in fetched:
                    for msg_id, msg in msg_list:
                        yield AckableMessage(
                            data=msg[b"data"],
                            ack=self._ack_generator(msg_id=msg_id, queue_name=stream),
                        )

                # Reclaim unacknowledged messages
                for sname in self._listen_streams:
                    lock = redis.lock(
                        f"autoclaim:{self.consumer_group_name}:{sname}",
                    )
                    if await lock.locked():
                        continue

                    async with lock:
                        pending = await redis.xautoclaim(
                            name=sname,
                            groupname=self.consumer_group_name,
                            consumername=self.consumer_name,
                            min_idle_time=self.idle_timeout,
                            count=self.unacknowledged_batch_size,
                        )

                        if pending[1]:
                            logger.debug(
                                "Reclaimed %d unacked messages from %s",
                                len(pending[1]),
                                sname,
                            )

                        for msg_id, msg in pending[1]:
                            yield AckableMessage(
                                data=msg[b"data"],
                                ack=self._ack_generator(
                                    msg_id=msg_id, queue_name=sname
                                ),
                            )
