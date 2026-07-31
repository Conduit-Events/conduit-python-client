"""Abstract transport interface for the Conduit protocol.

Concrete transports (RabbitMQ, etc.) implement these against a specific
broker. `publish`/`subscribe` accept transport-specific options as keyword
arguments rather than a fixed signature, since valid options differ per
transport (e.g. routing keys and dead-lettering only make sense for
RabbitMQ).
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any

MessageHandler = Callable[[dict[str, Any], Any], Awaitable[None]]


class Transport(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def publish(self, message: dict[str, Any], **options: Any) -> None: ...

    @abstractmethod
    async def subscribe(
        self, pattern: str, handler: MessageHandler, **options: Any
    ) -> Any: ...
