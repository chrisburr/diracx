from __future__ import annotations

__all__ = ["spawn_with_callback", "fire_callback"]

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any

import msgpack
from redis.asyncio import Redis

from .base_task import BaseTask

if TYPE_CHECKING:
    from .broker.base import AsyncBroker

logger = logging.getLogger(__name__)


def _group_key(group_id: str) -> str:
    return f"diracx:groups:{group_id}"


async def spawn_with_callback(
    children: list[BaseTask],
    callback: BaseTask,
    *,
    redis: Redis,
    ttl_seconds: int = 86400,
) -> str:
    """Schedule child tasks and a callback that fires when all children complete.

    Redis data structures:
      - ``diracx:groups:{group_id}:callback`` — serialized callback task
      - ``diracx:groups:{group_id}:remaining`` — atomic counter
      - ``diracx:groups:{group_id}:results:{child_task_id}`` — per-child results

    All keys get a TTL for automatic cleanup.

    Returns:
        The group_id for this callback group.

    """
    group_id = uuid.uuid4().hex

    # Store callback task
    callback_data = msgpack.packb(
        {
            "task_class": f"{callback.__class__.__module__}:{callback.__class__.__qualname__}",
            "args": list(callback.serialize()),
        },
        datetime=True,
    )
    pipe = redis.pipeline()
    pipe.set(f"{_group_key(group_id)}:callback", callback_data, ex=ttl_seconds)
    pipe.set(f"{_group_key(group_id)}:remaining", len(children), ex=ttl_seconds)
    await pipe.execute()

    # Schedule each child with the group_id label so the worker can
    # detect them as belonging to this callback group on completion.
    await asyncio.gather(
        *(child.schedule(labels={"group_id": group_id}) for child in children)
    )

    logger.info(
        "Spawned %d children with callback (group_id=%s)", len(children), group_id
    )
    return group_id


async def on_child_complete(
    redis: Redis,
    group_id: str,
    child_task_id: str,
    result: Any,
    *,
    ttl_seconds: int = 86400,
) -> bool:
    """Record a child's completion. Returns True if the callback should fire.

    Called by the worker after a child task (one with ``group_id`` in labels)
    completes.
    """
    # Store child result
    result_data = msgpack.packb(result, datetime=True)
    await redis.set(
        f"{_group_key(group_id)}:results:{child_task_id}",
        result_data,
        ex=ttl_seconds,
    )

    # Atomically decrement remaining counter
    remaining = await redis.decr(f"{_group_key(group_id)}:remaining")
    return remaining <= 0


async def fire_callback(
    redis: Redis,
    group_id: str,
    broker: AsyncBroker,
) -> None:
    """Deserialize and schedule the callback task for a completed group.

    Called by the worker when ``on_child_complete`` returns True.
    Reads the callback data from Redis, builds a ``BrokerMessage``,
    and kicks it to the broker for execution.
    """
    from .broker.models import AsyncKicker

    callback_data = await redis.get(f"{_group_key(group_id)}:callback")
    if callback_data is None:
        logger.warning("No callback data found for group %s", group_id)
        return

    payload = msgpack.unpackb(callback_data, timestamp=3)
    task_class_path: str = payload["task_class"]
    task_args: list[Any] = payload["args"]

    # Use the task class path as the task name for the kicker
    kicker: AsyncKicker[Any] = AsyncKicker(
        task_name=task_class_path,
        broker=broker,
        labels={"callback_group_id": group_id},
    )
    await kicker.kiq(*task_args)

    logger.info("Fired callback for group %s (task: %s)", group_id, task_class_path)
