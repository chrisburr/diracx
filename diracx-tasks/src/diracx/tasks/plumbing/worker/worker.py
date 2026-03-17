from __future__ import annotations

__all__ = ["Worker"]

import asyncio
import logging
from time import time
from typing import Any, Callable

import msgpack

from ..broker.base import AsyncBroker
from ..broker.models import AckableMessage, BrokerMessage, TaskMessage, TaskResult
from .di_resolver import solve_task_dependencies

logger = logging.getLogger(__name__)

# Sentinel value to signal queue completion
QUEUE_DONE = b"-1"


class Worker:
    """Worker that listens for tasks from broker and executes them.

    Architecture:
      - prefetcher: async loop reading from broker, puts messages in queue
      - runner: async loop executing tasks with concurrency control
    """

    def __init__(
        self,
        broker: AsyncBroker,
        task_registry: dict[str, Callable[..., Any]],
        max_concurrent_tasks: int = 10,
        max_prefetch: int = 0,
        ack_when: str = "saved",
    ) -> None:
        self.broker = broker
        self.task_registry = task_registry
        self.ack_when = ack_when

        self.sem: asyncio.Semaphore | None = None
        if max_concurrent_tasks > 0:
            self.sem = asyncio.Semaphore(max_concurrent_tasks)

        self.sem_prefetch = asyncio.Semaphore(
            max_prefetch if max_prefetch > 0 else 999999
        )

    async def listen(self, finish_event: asyncio.Event) -> None:
        """Start the prefetcher and runner tasks."""
        await self.broker.startup()

        logger.info("Worker started listening for tasks")

        queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()

        prefetcher_task = asyncio.create_task(self.prefetcher(queue, finish_event))
        runner_task = asyncio.create_task(self.runner(queue))

        await asyncio.gather(prefetcher_task, runner_task)

        logger.info("Worker shutting down")

    async def prefetcher(
        self,
        queue: asyncio.Queue[bytes | AckableMessage],
        finish_event: asyncio.Event,
    ) -> None:
        """Prefetch messages from broker and put them in queue."""
        iterator = self.broker.listen()
        current_message_task = asyncio.create_task(iterator.__anext__())  # type: ignore[arg-type]

        while True:
            if finish_event.is_set():
                break

            try:
                await self.sem_prefetch.acquire()

                done, _ = await asyncio.wait({current_message_task}, timeout=0.3)

                if not done:
                    self.sem_prefetch.release()
                    continue

                message = current_message_task.result()
                current_message_task = asyncio.create_task(iterator.__anext__())  # type: ignore[arg-type]

                await queue.put(message)

            except (asyncio.CancelledError, StopAsyncIteration):
                break

        logger.info("Prefetcher stopping")
        current_message_task.cancel()
        await queue.put(QUEUE_DONE)  # type: ignore[arg-type]
        self.sem_prefetch.release()

    async def runner(
        self,
        queue: asyncio.Queue[bytes | AckableMessage],
    ) -> None:
        """Run tasks from the queue."""
        tasks: set[asyncio.Task[Any]] = set()

        def task_done_callback(task: asyncio.Task[Any]) -> None:
            tasks.discard(task)
            if self.sem is not None:
                self.sem.release()

        while True:
            try:
                if self.sem is not None:
                    await self.sem.acquire()

                self.sem_prefetch.release()

                message = await queue.get()

                if message is QUEUE_DONE:
                    if tasks:
                        logger.info("Waiting for %d running tasks...", len(tasks))
                        await asyncio.wait(tasks)
                    break

                task = asyncio.create_task(self.callback(message))
                tasks.add(task)
                task.add_done_callback(task_done_callback)

            except asyncio.CancelledError:
                break

        logger.info("Runner stopped")

    async def callback(self, message: bytes | AckableMessage) -> None:
        """Process a single message from the broker."""
        message_data = message.data if isinstance(message, AckableMessage) else message

        try:
            broker_msg_dict = msgpack.unpackb(message_data, timestamp=3)
            broker_msg = BrokerMessage.model_validate(broker_msg_dict)
            task_message = broker_msg.to_task_message()
        except Exception:
            logger.warning("Cannot parse message, skipping", exc_info=True)
            return

        task_func = self.task_registry.get(task_message.task_name)
        if task_func is None:
            logger.warning("Task %r not found in registry", task_message.task_name)
            return

        logger.info(
            "Executing task %s (ID: %s)", task_message.task_name, task_message.task_id
        )

        if self.ack_when == "received" and isinstance(message, AckableMessage):
            await message.ack()

        result = await self.run_task(task_func, task_message)

        if self.ack_when == "executed" and isinstance(message, AckableMessage):
            await message.ack()

        try:
            if self.broker.result_backend:
                await self.broker.result_backend.set_result(
                    task_message.task_id, result
                )
        except Exception:
            logger.exception("Failed to save result")

        if self.ack_when == "saved" and isinstance(message, AckableMessage):
            await message.ack()

    async def run_task(
        self,
        task_func: Callable[..., Any],
        task_message: TaskMessage,
    ) -> TaskResult[Any]:
        """Execute a task function with dependency resolution."""
        start_time = time()
        returned = None
        found_exception: BaseException | None = None

        dep_kwargs: dict[str, Any] = {}
        async_exit_stack = None

        try:
            if hasattr(task_func, "_dependant"):
                dep_kwargs, async_exit_stack = await solve_task_dependencies(
                    call=task_func,
                    dependency_overrides=self.broker.dependency_overrides,
                    dependency_context=self.broker.custom_dependency_context,
                )

            all_kwargs = {**dep_kwargs, **task_message.kwargs}

            if asyncio.iscoroutinefunction(task_func):
                returned = await task_func(*task_message.args, **all_kwargs)
            else:
                returned = task_func(*task_message.args, **all_kwargs)

            if async_exit_stack:
                await async_exit_stack.aclose()

        except BaseException as exc:
            found_exception = exc
            if async_exit_stack:
                try:
                    await async_exit_stack.aclose()
                except Exception:
                    pass
            logger.error(
                "Exception in task %s: %s",
                task_message.task_name,
                exc,
                exc_info=True,
            )

        execution_time = time() - start_time

        if found_exception is not None:
            return TaskResult.from_exception(
                exc=found_exception,
                execution_time=execution_time,
                labels=task_message.labels,
            )
        return TaskResult.from_value(
            value=returned,
            execution_time=execution_time,
            labels=task_message.labels,
        )
