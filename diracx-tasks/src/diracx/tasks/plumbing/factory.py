from __future__ import annotations

__all__ = ["task_wrapper", "wrap_task", "load_task_registry"]

import logging
from functools import partial
from importlib.metadata import entry_points
from inspect import Parameter, signature
from typing import Any, Callable

from fastapi.dependencies.utils import get_dependant

from diracx.core.extensions import select_from_extension

from .base_task import BaseTask
from .broker.models import AsyncDecoratedTask

logger = logging.getLogger(__name__)


async def task_wrapper(cls: type[BaseTask], *args: Any, **kwargs: Any) -> Any:
    """Instantiate a task, acquire locks, and execute it.

    ``args`` are the task's constructor arguments (from serialization).
    ``kwargs`` are resolved DI dependencies for ``execute()``.
    """
    task = cls(*args)
    held_locks: list[Any] = []
    # Pop redis once before the loop — otherwise only the first lock gets it
    redis = kwargs.pop("_redis", None)
    try:
        for lock in task.execution_locks:
            acquired = await lock.acquire(redis) if redis else True
            if acquired:
                held_locks.append((lock, redis))
            else:
                # Reschedule with backoff
                from .exceptions import UnableToAcquireLockError

                raise UnableToAcquireLockError(
                    f"Could not acquire lock {lock.redis_key}"
                )

        result = await task.execute(**kwargs)
        return result
    finally:
        for lock, redis in held_locks:
            if redis:
                await lock.release(redis)


def wrap_task(cls: type[BaseTask]) -> Callable[..., Any]:
    """Create a wrapped callable from a BaseTask class.

    The wrapper has a modified signature that includes:
    - Positional args for task construction
    - Keyword-only params from ``execute()`` (for DI resolution)

    Also attaches ``_dependant`` for FastAPI dependency resolution.
    """
    execute_sig = signature(cls.execute, eval_str=True)
    execute_params = list(execute_sig.parameters.values())
    if execute_params and execute_params[0].name == "self":
        execute_params = execute_params[1:]
    # Filter out VAR_KEYWORD (**kwargs) and VAR_POSITIONAL (*args) as these
    # are handled by the wrapper's own positional args/kwargs
    execute_params = [
        p
        for p in execute_params
        if p.kind not in (Parameter.VAR_KEYWORD, Parameter.VAR_POSITIONAL)
    ]

    parameters = [
        Parameter("args", Parameter.POSITIONAL_ONLY, default=()),
        Parameter("kwargs", Parameter.POSITIONAL_ONLY, default={}),
        *(p.replace(kind=Parameter.KEYWORD_ONLY) for p in execute_params),
    ]

    wrapped = partial(task_wrapper, cls)
    wrapped.__name__ = cls.__name__  # type: ignore[attr-defined]
    wrapped.__qualname__ = cls.__qualname__  # type: ignore[attr-defined]
    wrapped.__signature__ = execute_sig.replace(  # type: ignore[attr-defined]
        parameters=parameters,
        return_annotation=execute_sig.return_annotation,
    )

    wrapped._dependant = get_dependant(path="/", call=wrapped)  # type: ignore[attr-defined]

    return wrapped


def load_task_registry(
    groups: list[str] | None = None,
) -> dict[str, type[BaseTask]]:
    """Load task classes from entry points.

    Entry points are in groups like ``diracx.tasks.transformation``,
    ``diracx.tasks.jobs``, etc.

    Returns a dict of ``"group:ClassName" -> TaskClass``.
    """
    registry: dict[str, type[BaseTask]] = {}

    if groups is None:
        # Discover all diracx.tasks.* groups
        all_eps = entry_points()
        groups = [
            g
            for g in (all_eps.groups if hasattr(all_eps, "groups") else [])
            if g.startswith("diracx.tasks.")
        ]

    for group in groups:
        key = group.rsplit(".", 1)[-1]  # e.g. "transformation"
        for ep in select_from_extension(group=group):
            task_cls: type[BaseTask] = ep.load()
            task_name = f"{key}:{task_cls.__name__}"
            if task_name not in registry:  # Extension priority: first wins
                registry[task_name] = task_cls
                logger.debug("Loaded task: %s", task_name)

    return registry


def create_broker_task_mapping(
    broker: Any,
    task_registry: dict[str, type[BaseTask]],
) -> tuple[dict[type[BaseTask], AsyncDecoratedTask], dict[str, Callable[..., Any]]]:
    """Create broker task mapping and wrapped function registry.

    Returns:
        Tuple of (broker_task_mapping, wrapped_registry)

    """
    broker_task_mapping: dict[type[BaseTask], AsyncDecoratedTask] = {}
    wrapped_registry: dict[str, Callable[..., Any]] = {}

    for task_name, task_cls in task_registry.items():
        wrapped_func = wrap_task(task_cls)

        decorated_task = AsyncDecoratedTask(
            broker=broker,
            task_name=task_name,
            original_func=wrapped_func,
            labels={},
        )

        broker_task_mapping[task_cls] = decorated_task
        wrapped_registry[task_name] = wrapped_func

    return broker_task_mapping, wrapped_registry
