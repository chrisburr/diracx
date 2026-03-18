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
    """Execute tasks consumed from a broker.

    The worker uses a two-loop architecture:

    **prefetcher** — reads messages from the broker (Redis streams) and
    places them into an internal ``asyncio.Queue``.  When ``max_prefetch``
    is set, a semaphore limits how far ahead the prefetcher can read,
    providing backpressure so we don't buffer unbounded messages in memory.

    **runner** — pulls messages from the queue, resolves FastAPI-style
    dependencies via ``solve_task_dependencies``, executes the task
    function, persists the result to the result backend, and finally
    acknowledges the message.  ``max_concurrent_tasks`` controls how
    many tasks execute in parallel (also semaphore-gated).

    Flow::

        Broker  ──▸  prefetcher  ──▸  queue  ──▸  runner  ──▸  task_func
                     (sem_prefetch)               (sem)

    Shutdown is cooperative: setting ``finish_event`` causes the
    prefetcher to drain, send ``QUEUE_DONE`` to the runner, and the
    runner waits for in-flight tasks before exiting.
    """

    def __init__(
        self,
        broker: AsyncBroker,
        task_registry: dict[str, Callable[..., Any]],
        max_concurrent_tasks: int = 10,
        max_prefetch: int = 0,
    ) -> None:
        self.broker = broker
        self.task_registry = task_registry

        self.sem: asyncio.Semaphore | None = None
        if max_concurrent_tasks > 0:
            self.sem = asyncio.Semaphore(max_concurrent_tasks)

        self.sem_prefetch: asyncio.Semaphore | None = None
        if max_prefetch > 0:
            self.sem_prefetch = asyncio.Semaphore(max_prefetch)

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
        """Prefetch messages from broker into the internal queue.

        This is a backpressure-controlled pump between the broker
        (Redis streams) and the runner.  We wrap broker.__anext__()
        in a task so we can poll it with a timeout — otherwise we'd
        block forever and never notice finish_event.
        """
        iterator = self.broker.listen()
        # Kick off the first read from the broker as a background task
        current_message_task = asyncio.create_task(iterator.__anext__())  # type: ignore[arg-type]

        while True:
            if finish_event.is_set():
                break

            try:
                # Block until the runner has capacity for another message.
                # The runner releases this semaphore when it picks a message
                # off the queue, so this is how we apply backpressure — if
                # the runner is saturated we stop pulling from Redis.
                if self.sem_prefetch is not None:
                    await self.sem_prefetch.acquire()

                # Poll the in-flight Redis read with a short timeout so we
                # can loop back and re-check finish_event if nothing arrived.
                done, _ = await asyncio.wait({current_message_task}, timeout=0.3)

                if not done:
                    # No message yet — give back the slot and try again
                    if self.sem_prefetch is not None:
                        self.sem_prefetch.release()
                    continue

                # A message arrived — grab it and immediately start the
                # next read so Redis I/O overlaps with queue insertion.
                message = current_message_task.result()
                current_message_task = asyncio.create_task(iterator.__anext__())  # type: ignore[arg-type]

                await queue.put(message)

            except (asyncio.CancelledError, StopAsyncIteration):
                break

        # Shutting down: cancel outstanding read, tell the runner we're
        # done, and release the semaphore so the runner isn't stuck.
        logger.info("Prefetcher stopping")
        current_message_task.cancel()
        await queue.put(QUEUE_DONE)  # type: ignore[arg-type]
        if self.sem_prefetch is not None:
            self.sem_prefetch.release()

    async def runner(
        self,
        queue: asyncio.Queue[bytes | AckableMessage],
    ) -> None:
        """Pull messages from the queue and execute them concurrently.

        Each message is dispatched to ``process_message`` in its own
        ``asyncio.Task``.  The concurrency semaphore (``self.sem``)
        limits how many tasks run at once; the prefetch semaphore
        (``self.sem_prefetch``) is released here to signal the
        prefetcher that another slot is available.
        """
        tasks: set[asyncio.Task[Any]] = set()

        def task_done_callback(task: asyncio.Task[Any]) -> None:
            tasks.discard(task)
            if self.sem is not None:
                self.sem.release()

        while True:
            try:
                if self.sem is not None:
                    await self.sem.acquire()

                if self.sem_prefetch is not None:
                    self.sem_prefetch.release()

                message = await queue.get()

                if message is QUEUE_DONE:
                    if tasks:
                        logger.info("Waiting for %d running tasks...", len(tasks))
                        await asyncio.wait(tasks)
                    break

                task = asyncio.create_task(self.process_message(message))
                tasks.add(task)
                task.add_done_callback(task_done_callback)

            except asyncio.CancelledError:
                break

        logger.info("Runner stopped")

    async def process_message(self, message: bytes | AckableMessage) -> None:
        """Deserialize, look up, execute, and ack a single broker message."""
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

        result = await self.run_task(task_func, task_message)

        try:
            if self.broker.result_backend:
                await self.broker.result_backend.set_result(
                    task_message.task_id, result
                )
        except Exception:
            logger.exception("Failed to save result")

        if isinstance(message, AckableMessage):
            await message.ack()

    async def run_task(
        self,
        task_func: Callable[..., Any],
        task_message: TaskMessage,
    ) -> TaskResult[Any]:
        """Execute a task function with dependency resolution.

        Resolves FastAPI-style dependencies (declared via ``Depends``)
        through ``solve_task_dependencies``, merges them with the
        kwargs from the message, and calls the task.  Returns a
        ``TaskResult`` wrapping either the return value or the
        exception.
        """
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

            all_kwargs = {**dep_kwargs, **task_message.task_kwargs}

            returned = await task_func(*task_message.task_args, **all_kwargs)

            if async_exit_stack:
                await async_exit_stack.aclose()

        except BaseException as exc:
            found_exception = exc
            if async_exit_stack:
                try:
                    await async_exit_stack.aclose()
                except Exception:
                    logger.debug("Error closing exit stack", exc_info=True)
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
