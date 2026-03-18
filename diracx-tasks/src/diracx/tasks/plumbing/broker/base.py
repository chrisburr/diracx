from __future__ import annotations

__all__ = ["AsyncBroker"]

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Callable
from uuid import uuid4

from ._types import _BlockingConnectionPool
from .models import AckableMessage, BrokerMessage
from .result_backend import AsyncResultBackend


def _default_id_generator() -> str:
    return uuid4().hex


class AsyncBroker(ABC):
    """Abstract base class for task brokers."""

    connection_pool: _BlockingConnectionPool

    def __init__(
        self,
        result_backend: AsyncResultBackend[Any] | None = None,
        task_id_generator: Callable[[], str] | None = None,
    ) -> None:
        self.result_backend = result_backend
        self.id_generator = task_id_generator or _default_id_generator
        self.is_worker_process = False
        self.is_scheduler_process = False
        self.custom_dependency_context: dict[type, Any] = {}
        self.dependency_overrides: dict[Callable, Callable] = {}

    async def startup(self) -> None:
        if self.result_backend:
            await self.result_backend.startup()

    async def shutdown(self) -> None:
        if self.result_backend:
            await self.result_backend.shutdown()

    @abstractmethod
    async def kick(self, message: BrokerMessage) -> None:
        """Send a message to the broker for worker consumption."""

    @abstractmethod
    def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Yield incoming messages from the broker."""
