"""Pure configuration shaping for the RabbitMQ transport - no I/O.

Mirrors conduit-node-client's rabbitmq-transport-config.js: separating the
defaulting/merging logic from the transport class itself keeps it
unit-testable without a broker.

`QueueOptions`/`DeadLetterOptions` are the *unresolved* inputs a caller may
pass in (any field left as `None` means "inherit from the base config").
`QueueConfig`/`DeadLetterConfig`/`RabbitMqTransportConfig` are the
*resolved* output shapes, with every field filled in.
"""

from dataclasses import dataclass, field
from typing import Any

DEFAULT_NAMESPACE = "default"
DEFAULT_CONNECTION_NAME = "main"
DEFAULT_EXCHANGE_TYPE = "topic"
DEFAULT_PREFETCH = 10


@dataclass(frozen=True)
class DeadLetterOptions:
    enabled: bool | None = None
    exchange: str | None = None
    exchange_type: str | None = None
    queue: str | None = None
    routing_key: str | None = None


@dataclass(frozen=True)
class QueueOptions:
    name: str | None = None
    durable: bool | None = None
    exclusive: bool | None = None
    auto_delete: bool | None = None
    arguments: dict[str, Any] | None = None
    dead_letter: bool | DeadLetterOptions | None = None


@dataclass(frozen=True)
class DeadLetterConfig:
    enabled: bool
    exchange: str | None = None
    exchange_type: str | None = None
    queue: str | None = None
    routing_key: str | None = None


@dataclass(frozen=True)
class QueueDefaults:
    name: str
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    arguments: dict[str, Any] = field(default_factory=dict)
    dead_letter: bool | DeadLetterOptions = True


@dataclass(frozen=True)
class QueueConfig:
    name: str
    durable: bool
    exclusive: bool
    auto_delete: bool
    arguments: dict[str, Any]
    dead_letter: DeadLetterConfig


@dataclass(frozen=True)
class RabbitMqTransportConfig:
    namespace: str
    service: str
    connection_name: str
    url: str | None
    exchange: str
    exchange_type: str
    prefetch: int
    queue: QueueDefaults


def configure_rabbitmq_transport(
    *,
    service: str,
    namespace: str | None = None,
    connection_name: str | None = None,
    url: str | None = None,
    exchange: str | None = None,
    exchange_type: str | None = None,
    prefetch: int | None = None,
    queue: str | QueueOptions | None = None,
) -> RabbitMqTransportConfig:
    if not service:
        raise ValueError("RabbitMQ transport requires service")

    resolved_namespace = _coalesce(namespace, DEFAULT_NAMESPACE)
    resolved_exchange = _coalesce(exchange, f"conduit.{resolved_namespace}.events")

    queue_options = _normalize_queue_input(queue)
    queue_name = _coalesce(queue_options.name, f"{resolved_namespace}.{service}")

    return RabbitMqTransportConfig(
        namespace=resolved_namespace,
        service=service,
        connection_name=_coalesce(connection_name, DEFAULT_CONNECTION_NAME),
        url=url,
        exchange=resolved_exchange,
        exchange_type=_coalesce(exchange_type, DEFAULT_EXCHANGE_TYPE),
        prefetch=_coalesce(prefetch, DEFAULT_PREFETCH),
        queue=QueueDefaults(
            name=queue_name,
            durable=_coalesce(queue_options.durable, True),
            exclusive=_coalesce(queue_options.exclusive, False),
            auto_delete=_coalesce(queue_options.auto_delete, False),
            arguments=_coalesce(queue_options.arguments, {}),
            dead_letter=queue_options.dead_letter
            if queue_options.dead_letter is not None
            else True,
        ),
    )


def configure_rabbitmq_queue(
    base_config: RabbitMqTransportConfig,
    queue: str | QueueOptions | None = None,
) -> QueueConfig:
    queue_options = _normalize_queue_input(queue)

    name = _coalesce(queue_options.name, base_config.queue.name)
    if not name:
        raise ValueError(
            "RabbitMQ queue name is required. Provide service, a base "
            "queue name, or a subscribe-time queue name."
        )

    dead_letter_input = (
        queue_options.dead_letter
        if queue_options.dead_letter is not None
        else base_config.queue.dead_letter
    )
    dead_letter = _configure_dead_letter_queue(
        queue_name=name,
        exchange=base_config.exchange,
        dead_letter=dead_letter_input,
    )

    arguments = {
        **base_config.queue.arguments,
        **_coalesce(queue_options.arguments, {}),
    }
    if dead_letter.enabled:
        arguments["x-dead-letter-exchange"] = dead_letter.exchange
        arguments["x-dead-letter-routing-key"] = dead_letter.routing_key

    return QueueConfig(
        name=name,
        durable=_coalesce(queue_options.durable, base_config.queue.durable),
        exclusive=_coalesce(queue_options.exclusive, base_config.queue.exclusive),
        auto_delete=_coalesce(queue_options.auto_delete, base_config.queue.auto_delete),
        arguments=arguments,
        dead_letter=dead_letter,
    )


def _configure_dead_letter_queue(
    *,
    queue_name: str,
    exchange: str,
    dead_letter: bool | DeadLetterOptions | None = True,
) -> DeadLetterConfig:
    if dead_letter is False:
        return DeadLetterConfig(enabled=False)

    if dead_letter is True or dead_letter is None:
        return DeadLetterConfig(
            enabled=True,
            exchange=f"{exchange}.dlx",
            exchange_type="direct",
            queue=f"{queue_name}.dlq",
            routing_key=f"{queue_name}.dead",
        )

    if dead_letter.enabled is False:
        return DeadLetterConfig(enabled=False)

    return DeadLetterConfig(
        enabled=True,
        exchange=_coalesce(dead_letter.exchange, f"{exchange}.dlx"),
        exchange_type=_coalesce(dead_letter.exchange_type, "direct"),
        queue=_coalesce(dead_letter.queue, f"{queue_name}.dlq"),
        routing_key=_coalesce(dead_letter.routing_key, f"{queue_name}.dead"),
    )


def _normalize_queue_input(queue: str | QueueOptions | None) -> QueueOptions:
    if isinstance(queue, str):
        return QueueOptions(name=queue)
    return queue if queue is not None else QueueOptions()


def _coalesce[T, D](value: T | None, fallback: D) -> T | D:
    """Mirrors JS `??`: keeps falsy-but-present values (0, False, ""), only
    substitutes the fallback for `None`."""
    return value if value is not None else fallback
