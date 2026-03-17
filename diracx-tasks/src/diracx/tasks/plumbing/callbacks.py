from __future__ import annotations

__all__ = ["spawn_with_callback"]

import logging
import uuid
from typing import Any

import msgpack
from redis.asyncio import Redis

from .base_task import BaseTask

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

    # Schedule each child with the group_id in labels
    for child in children:
        await child.schedule()

    logger.info(
        "Spawned %d children with callback (group_id=%s)", len(children), group_id
    )
    return group_id


async def on_child_complete(
    redis: Redis,
    group_id: str,
    child_task_id: str,
    result: Any,
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
        ex=86400,
    )

    # Atomically decrement remaining counter
    remaining = await redis.decr(f"{_group_key(group_id)}:remaining")
    return remaining <= 0
