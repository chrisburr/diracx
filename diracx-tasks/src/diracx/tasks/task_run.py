"""CLI entry point for interactive task execution.

Usage:
    diracx-task-run call <entry_point> [--args JSON] [--kwargs JSON] [--debugger {none,before,exception}]
    diracx-task-run worker [--max-concurrent-tasks N] [--redis-url URL]
    diracx-task-run scheduler [--redis-url URL]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import traceback
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from .plumbing._redis_types import LockCoordinator

DEFAULT_REDIS_URL = "redis://localhost"
REDIS_URL_ENV_VAR = "DIRACX_TASKS_REDIS_URL"


class DebugOptions(StrEnum):
    NONE = "none"
    BEFORE = "before"
    ON_ERROR = "exception"


def _get_redis_url(args: argparse.Namespace) -> str:
    """Resolve Redis URL from CLI arg, env var, or default."""
    if hasattr(args, "redis_url") and args.redis_url:
        return args.redis_url
    return os.environ.get(REDIS_URL_ENV_VAR, DEFAULT_REDIS_URL)


def main() -> None:
    """Parse arguments and dispatch to the appropriate subcommand."""
    parser = argparse.ArgumentParser(description="DiracX tasks CLI", allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)

    # call subcommand
    call_parser = subparsers.add_parser("call", help="Execute a task interactively")
    call_parser.add_argument("entry_point", help="Task entry point name")
    call_parser.add_argument(
        "--args",
        default=[],
        type=json.loads,
        help="JSON list of positional arguments (default: '[]')",
    )
    call_parser.add_argument(
        "--kwargs",
        default={},
        type=json.loads,
        help="JSON dict of keyword arguments (default: '{}')",
    )
    call_parser.add_argument(
        "--debugger",
        type=DebugOptions,
        default=DebugOptions.NONE,
        help="Drop into debugger: 'before' or 'exception'",
    )
    call_parser.set_defaults(
        func=lambda args: asyncio.run(
            call_task(
                args.entry_point,
                args=args.args,
                kwargs=args.kwargs,
                debugger=args.debugger,
            )
        )
    )

    # worker subcommand
    worker_parser = subparsers.add_parser("worker", help="Start a task worker")
    worker_parser.add_argument(
        "--max-concurrent-tasks",
        type=int,
        default=10,
        help="Maximum number of tasks to run concurrently (default: 10)",
    )
    worker_parser.add_argument(
        "--redis-url",
        type=str,
        default=None,
        help=f"Redis URL (default: ${REDIS_URL_ENV_VAR} or {DEFAULT_REDIS_URL})",
    )
    worker_parser.set_defaults(
        func=lambda args: asyncio.run(
            start_worker(
                redis_url=_get_redis_url(args),
                max_concurrent_tasks=args.max_concurrent_tasks,
            )
        )
    )

    # scheduler subcommand
    scheduler_parser = subparsers.add_parser(
        "scheduler", help="Start the task scheduler"
    )
    scheduler_parser.add_argument(
        "--redis-url",
        type=str,
        default=None,
        help=f"Redis URL (default: ${REDIS_URL_ENV_VAR} or {DEFAULT_REDIS_URL})",
    )
    scheduler_parser.set_defaults(
        func=lambda args: asyncio.run(start_scheduler(redis_url=_get_redis_url(args)))
    )

    parsed = parser.parse_args()
    parsed.func(parsed)


async def start_worker(
    redis_url: str,
    max_concurrent_tasks: int = 10,
) -> None:
    """Start a worker to execute tasks from the broker."""
    from .plumbing.broker import RedisStreamBroker
    from .plumbing.factory import (
        BaseTask,
        create_task_bindings,
        load_task_registry,
    )
    from .plumbing.worker import Worker

    broker = RedisStreamBroker(url=redis_url)
    task_classes = load_task_registry()
    task_bindings, wrapped_registry = create_task_bindings(broker, task_classes)
    BaseTask.bind_broker(task_bindings)

    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_classes,
        max_concurrent_tasks=max_concurrent_tasks,
    )

    finish_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, finish_event.set)
    await worker.listen(finish_event)


async def start_scheduler(redis_url: str) -> None:
    """Start the task scheduler process."""
    from .plumbing.broker import RedisStreamBroker
    from .plumbing.factory import load_task_registry
    from .plumbing.scheduler import TaskScheduler

    broker = RedisStreamBroker(url=redis_url)
    task_classes = load_task_registry()

    scheduler = TaskScheduler(
        broker=broker,
        redis_url=redis_url,
        task_registry=task_classes,
    )

    await scheduler.startup()
    finish_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, finish_event.set)
    try:
        await scheduler.run_forever(finish_event)
    finally:
        await scheduler.shutdown()


async def call_task(
    entry_point: str,
    args: Iterable[Any],
    kwargs: dict[str, Any],
    debugger: DebugOptions,
) -> None:
    """Execute a task interactively (no broker).

    Uses ``task_wrapper`` with ``_interactive=True`` so that structural
    locks (Mutex, RW) are acquired when Redis is available, while
    limiters (rate/concurrency) are skipped.
    """
    from .plumbing.factory import load_task_registry, task_wrapper

    registry = load_task_registry()

    task_cls = registry.get(entry_point)
    if task_cls is None:
        print(f"Task {entry_point!r} not found. Available: {sorted(registry)}")
        sys.exit(1)

    # Try to connect to Redis for lock acquisition
    redis: LockCoordinator | None = None
    redis_url = os.environ.get(REDIS_URL_ENV_VAR)
    if redis_url:
        from redis.asyncio import Redis

        redis = Redis.from_url(redis_url)

    if debugger == DebugOptions.BEFORE:
        breakpoint()  # noqa: T100
    try:
        result = await task_wrapper(
            task_cls, *args, _redis=redis, _interactive=True, **kwargs
        )
        print(f"Result: {result}")
    except Exception:
        if debugger != DebugOptions.ON_ERROR:
            raise
        import pdb

        traceback_info = sys.exc_info()
        traceback.print_exception(*traceback_info)
        pdb.post_mortem(traceback_info[2])
    finally:
        if redis is not None:
            await redis.aclose()
