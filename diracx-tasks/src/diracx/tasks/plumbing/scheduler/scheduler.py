from __future__ import annotations

__all__ = ["TaskScheduler"]

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import msgpack
from redis.asyncio import BlockingConnectionPool, Connection, Redis

from ..base_task import BaseTask, PeriodicBaseTask, PeriodicVoAwareBaseTask
from ..broker.base import AsyncBroker
from ..broker.models import AsyncKicker, BrokerMessage

logger = logging.getLogger(__name__)

# Lua script for atomic delayed-task promotion:
# If the member still exists in the ZSET, remove it and return the data.
# This prevents double-scheduling if multiple scheduler instances race.
_PROMOTE_DELAYED_SCRIPT = """
local members = redis.call("zrangebyscore", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2])
if #members == 0 then
    return {}
end
for i, member in ipairs(members) do
    redis.call("zrem", KEYS[1], member)
end
return members
"""

DELAYED_ZSET_KEY = "diracx:tasks:delayed"


class TaskScheduler:
    """Scheduler managing periodic tasks and delayed ZSET polling.

    Runs as a singleton StatefulSet (1 replica) with a Redis mutex
    as defense-in-depth.

    Responsibilities:
      1. Load periodic task definitions from entry points + config
      2. Track next occurrence for each periodic task; submit when due
      3. Poll the delayed ZSET for tasks whose time has come
      4. Watch config for schedule changes
    """

    def __init__(
        self,
        broker: AsyncBroker,
        redis_url: str,
        *,
        task_registry: dict[str, type[BaseTask]] | None = None,
        prefix: str = "diracx:scheduler",
        check_interval: float = 10.0,
        delayed_poll_interval: float = 1.0,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        self.broker = broker
        self.prefix = prefix
        self.check_interval = check_interval
        self.delayed_poll_interval = delayed_poll_interval
        self.task_registry = task_registry or {}
        self.connection_pool: BlockingConnectionPool[Connection] = (  # type: ignore[type-arg]
            BlockingConnectionPool.from_url(
                url=redis_url,
                max_connections=max_connection_pool_size,
                **connection_kwargs,
            )
        )
        # Mapping of (task_class_name, vo_or_empty) -> next_scheduled_time
        self._next_runs: dict[tuple[str, str], datetime] = {}

    async def startup(self) -> None:
        await self.broker.startup()
        logger.info("Scheduler started")

    async def shutdown(self) -> None:
        await self.broker.shutdown()
        await self.connection_pool.disconnect()
        logger.info("Scheduler shut down")

    async def run_forever(self, finish_event: asyncio.Event | None = None) -> None:
        """Main scheduler loop.

        Runs both the periodic scheduler and the delayed-ZSET poller
        concurrently.
        """
        _finish = finish_event or asyncio.Event()

        periodic_task = asyncio.create_task(self._periodic_loop(_finish))
        delayed_task = asyncio.create_task(self._delayed_poll_loop(_finish))

        await asyncio.gather(periodic_task, delayed_task)

    async def _periodic_loop(self, finish_event: asyncio.Event) -> None:
        """Check periodic tasks and submit them when due."""
        # Initialize next-run times
        self._compute_initial_schedules()

        while not finish_event.is_set():
            now = datetime.now(tz=UTC)

            for (task_name, vo), next_run in list(self._next_runs.items()):
                if now >= next_run:
                    await self._submit_periodic_task(task_name, vo)
                    # Compute the next occurrence
                    task_cls = self._find_task_class(task_name)
                    if task_cls and hasattr(task_cls, "default_schedule"):
                        self._next_runs[(task_name, vo)] = (
                            task_cls.default_schedule.next_occurrence()
                        )

            try:
                await asyncio.wait_for(finish_event.wait(), timeout=self.check_interval)
                break
            except asyncio.TimeoutError:
                pass

    async def _delayed_poll_loop(self, finish_event: asyncio.Event) -> None:
        """Poll the delayed ZSET and promote due tasks to streams."""
        async with Redis(connection_pool=self.connection_pool) as redis:
            while not finish_event.is_set():
                try:
                    now_ts = datetime.now(tz=UTC).timestamp()
                    members = await redis.eval(  # type: ignore[arg-type]
                        _PROMOTE_DELAYED_SCRIPT,
                        1,
                        DELAYED_ZSET_KEY,
                        str(now_ts),
                        "100",
                    )
                    for member_data in members or []:
                        broker_msg = BrokerMessage.model_validate(
                            msgpack.unpackb(member_data, timestamp=3)
                        )
                        await self.broker.kick(broker_msg)
                        logger.debug(
                            "Promoted delayed task %s to stream",
                            broker_msg.task_name,
                        )
                except Exception:
                    logger.exception("Error in delayed poll loop")

                try:
                    await asyncio.wait_for(
                        finish_event.wait(), timeout=self.delayed_poll_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass

    def _compute_initial_schedules(self) -> None:
        """Compute the initial next-run times for all periodic tasks."""
        for task_name, task_cls in self.task_registry.items():
            if not issubclass(task_cls, PeriodicBaseTask):
                continue
            if not getattr(task_cls, "_enabled", True):
                continue
            if issubclass(task_cls, PeriodicVoAwareBaseTask):
                # VOs will be populated from config; placeholder for now
                continue
            schedule = task_cls.default_schedule
            self._next_runs[(task_name, "")] = schedule.next_occurrence()

    def add_vo_schedule(self, task_name: str, vo: str, next_run: datetime) -> None:
        """Register a VO-specific periodic task schedule."""
        self._next_runs[(task_name, vo)] = next_run

    async def _submit_periodic_task(self, task_name: str, vo: str) -> None:
        """Submit a periodic task to the broker."""
        task_cls = self._find_task_class(task_name)
        if task_cls is None:
            logger.warning("Task class %r not found", task_name)
            return

        labels: dict[str, Any] = {
            "priority": str(task_cls.priority),
            "size": str(task_cls.size),
            "periodic": True,
        }
        args: list[Any] = []

        if issubclass(task_cls, PeriodicVoAwareBaseTask) and vo:
            labels["vo"] = vo
            # VO is the first constructor argument for VO-aware tasks
            args.append(vo)

        kicker = AsyncKicker(
            task_name=task_name,
            broker=self.broker,
            labels=labels,
        )
        try:
            await kicker.kiq(*args)
            logger.info("Submitted periodic task %s (vo=%s)", task_name, vo or "N/A")
        except Exception:
            logger.exception(
                "Failed to submit periodic task %s (vo=%s)", task_name, vo or "N/A"
            )

    def _find_task_class(self, task_name: str) -> type[BaseTask] | None:
        return self.task_registry.get(task_name)

    @staticmethod
    async def schedule_delayed(
        redis: Redis,
        message: BrokerMessage,
        run_at: datetime,
    ) -> None:
        """Add a task to the delayed ZSET for future execution."""
        serialized = msgpack.packb(message.model_dump(), datetime=True)
        await redis.zadd(
            DELAYED_ZSET_KEY,
            {serialized: run_at.timestamp()},
        )
