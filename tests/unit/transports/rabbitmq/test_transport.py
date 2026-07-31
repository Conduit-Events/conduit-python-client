"""Unit tests for RabbitMqTransport's pure logic (pattern matching,
queue declaration-compatibility checks) - no broker required.

See tests/integration/transports/rabbitmq/ for pub/sub/ack/DLQ behavior,
which does need a real broker.
"""

import pytest

from conduit_python_client.transports.rabbitmq import DeadLetterConfig, QueueConfig
from conduit_python_client.transports.rabbitmq.transport import RabbitMqTransport


def make_queue_config(**overrides: object) -> QueueConfig:
    defaults: dict[str, object] = {
        "name": "ns.service",
        "durable": True,
        "exclusive": False,
        "auto_delete": False,
        "arguments": {},
        "dead_letter": DeadLetterConfig(enabled=False),
    }
    defaults.update(overrides)
    return QueueConfig(**defaults)  # type: ignore[arg-type]


class TestValidatePattern:
    def test_accepts_exact_pattern(self):
        transport = RabbitMqTransport(service="orders")
        transport._validate_pattern("user.created")

    def test_accepts_hash_wildcard(self):
        transport = RabbitMqTransport(service="orders")
        transport._validate_pattern("#")

    def test_rejects_empty_pattern(self):
        transport = RabbitMqTransport(service="orders")
        with pytest.raises(ValueError, match="pattern"):
            transport._validate_pattern("")

    @pytest.mark.parametrize("pattern", ["user.*", "*.created", "user.#.created"])
    def test_rejects_partial_wildcards(self, pattern):
        transport = RabbitMqTransport(service="orders")
        with pytest.raises(ValueError, match="exact event types"):
            transport._validate_pattern(pattern)


class TestMatchesPattern:
    def test_hash_matches_any_routing_key(self):
        transport = RabbitMqTransport(service="orders")
        assert transport._matches_pattern("#", "user.created") is True
        assert transport._matches_pattern("#", "anything.at.all") is True

    def test_exact_pattern_matches_only_itself(self):
        transport = RabbitMqTransport(service="orders")
        assert transport._matches_pattern("user.created", "user.created") is True
        assert transport._matches_pattern("user.created", "user.deleted") is False


class TestAssertQueueDeclarationIsCompatible:
    def test_first_declaration_is_always_accepted(self):
        transport = RabbitMqTransport(service="orders")
        transport._assert_queue_declaration_is_compatible(make_queue_config())

    def test_repeated_identical_declaration_is_accepted(self):
        transport = RabbitMqTransport(service="orders")
        transport._assert_queue_declaration_is_compatible(make_queue_config())
        transport._assert_queue_declaration_is_compatible(make_queue_config())

    def test_conflicting_declaration_is_rejected(self):
        transport = RabbitMqTransport(service="orders")
        transport._assert_queue_declaration_is_compatible(
            make_queue_config(durable=True)
        )

        with pytest.raises(ValueError, match="already been declared"):
            transport._assert_queue_declaration_is_compatible(
                make_queue_config(durable=False)
            )

    def test_different_queue_names_do_not_conflict(self):
        transport = RabbitMqTransport(service="orders")
        transport._assert_queue_declaration_is_compatible(make_queue_config(name="a"))
        transport._assert_queue_declaration_is_compatible(
            make_queue_config(name="b", durable=False)
        )
