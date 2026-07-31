import pytest

from conduit_python_client.transports.rabbitmq import (
    DeadLetterOptions,
    QueueOptions,
    configure_rabbitmq_queue,
    configure_rabbitmq_transport,
)


class TestConfigureRabbitMqTransport:
    def test_requires_service(self):
        with pytest.raises(ValueError, match="service"):
            configure_rabbitmq_transport(service="")

    def test_defaults(self):
        config = configure_rabbitmq_transport(service="orders")

        assert config.namespace == "default"
        assert config.service == "orders"
        assert config.connection_name == "main"
        assert config.exchange == "conduit.default.events"
        assert config.exchange_type == "topic"
        assert config.prefetch == 10
        assert config.queue.name == "default.orders"
        assert config.queue.durable is True
        assert config.queue.exclusive is False
        assert config.queue.auto_delete is False
        assert config.queue.arguments == {}
        assert config.queue.dead_letter is True

    def test_namespace_changes_derived_exchange_and_queue_name(self):
        config = configure_rabbitmq_transport(namespace="billing", service="orders")

        assert config.exchange == "conduit.billing.events"
        assert config.queue.name == "billing.orders"

    def test_explicit_overrides_win_over_derived_defaults(self):
        config = configure_rabbitmq_transport(
            namespace="billing",
            service="orders",
            connection_name="secondary",
            url="amqp://example.test",
            exchange="custom.exchange",
            exchange_type="direct",
            prefetch=0,
            queue="custom.queue",
        )

        assert config.connection_name == "secondary"
        assert config.url == "amqp://example.test"
        assert config.exchange == "custom.exchange"
        assert config.exchange_type == "direct"
        assert config.prefetch == 0
        assert config.queue.name == "custom.queue"

    def test_queue_options_object_sets_defaults(self):
        config = configure_rabbitmq_transport(
            service="orders",
            queue=QueueOptions(
                durable=False,
                exclusive=True,
                auto_delete=True,
                arguments={"x-custom": 1},
                dead_letter=False,
            ),
        )

        assert config.queue.durable is False
        assert config.queue.exclusive is True
        assert config.queue.auto_delete is True
        assert config.queue.arguments == {"x-custom": 1}
        assert config.queue.dead_letter is False


class TestConfigureRabbitMqQueue:
    def test_inherits_name_from_base_config(self):
        base = configure_rabbitmq_transport(namespace="billing", service="orders")

        queue = configure_rabbitmq_queue(base)

        assert queue.name == "billing.orders"

    def test_string_queue_overrides_name_only(self):
        base = configure_rabbitmq_transport(namespace="billing", service="orders")

        queue = configure_rabbitmq_queue(base, "custom.queue")

        assert queue.name == "custom.queue"
        assert queue.durable == base.queue.durable
        assert queue.exclusive == base.queue.exclusive
        assert queue.auto_delete == base.queue.auto_delete

    def test_raises_when_no_name_can_be_resolved(self):
        base = configure_rabbitmq_transport(
            service="orders", queue=QueueOptions(name="")
        )

        with pytest.raises(ValueError, match="queue name is required"):
            configure_rabbitmq_queue(base)

    def test_per_call_overrides_win_over_base_defaults(self):
        base = configure_rabbitmq_transport(
            service="orders",
            queue=QueueOptions(durable=True, exclusive=False, auto_delete=False),
        )

        queue = configure_rabbitmq_queue(
            base,
            QueueOptions(durable=False, exclusive=True, auto_delete=True),
        )

        assert queue.durable is False
        assert queue.exclusive is True
        assert queue.auto_delete is True

    def test_arguments_merge_base_and_override_with_override_winning(self):
        base = configure_rabbitmq_transport(
            service="orders",
            queue=QueueOptions(arguments={"x-shared": "base", "x-base-only": 1}),
        )

        queue = configure_rabbitmq_queue(
            base, QueueOptions(arguments={"x-shared": "override"})
        )

        assert queue.arguments["x-shared"] == "override"
        assert queue.arguments["x-base-only"] == 1

    def test_dead_letter_defaults_to_derived_exchange_and_routing_key(self):
        base = configure_rabbitmq_transport(namespace="billing", service="orders")

        queue = configure_rabbitmq_queue(base)

        assert queue.dead_letter.enabled is True
        assert queue.dead_letter.exchange == "conduit.billing.events.dlx"
        assert queue.dead_letter.exchange_type == "direct"
        assert queue.dead_letter.queue == f"{queue.name}.dlq"
        assert queue.dead_letter.routing_key == f"{queue.name}.dead"
        assert queue.arguments["x-dead-letter-exchange"] == queue.dead_letter.exchange
        assert (
            queue.arguments["x-dead-letter-routing-key"]
            == queue.dead_letter.routing_key
        )

    def test_dead_letter_false_disables_it_and_omits_arguments(self):
        base = configure_rabbitmq_transport(service="orders")

        queue = configure_rabbitmq_queue(base, QueueOptions(dead_letter=False))

        assert queue.dead_letter.enabled is False
        assert "x-dead-letter-exchange" not in queue.arguments
        assert "x-dead-letter-routing-key" not in queue.arguments

    def test_dead_letter_options_object_overrides_individual_fields(self):
        base = configure_rabbitmq_transport(service="orders")

        queue = configure_rabbitmq_queue(
            base,
            QueueOptions(
                dead_letter=DeadLetterOptions(
                    exchange="custom.dlx", routing_key="custom.dead"
                )
            ),
        )

        assert queue.dead_letter.enabled is True
        assert queue.dead_letter.exchange == "custom.dlx"
        assert queue.dead_letter.routing_key == "custom.dead"
        # Fields left unset on the override still fall back to the derived
        # defaults, not to the base config's dead-letter defaults.
        assert queue.dead_letter.exchange_type == "direct"
        assert queue.dead_letter.queue == f"{queue.name}.dlq"

    def test_dead_letter_options_object_with_enabled_false_disables_it(self):
        base = configure_rabbitmq_transport(service="orders")

        queue = configure_rabbitmq_queue(
            base, QueueOptions(dead_letter=DeadLetterOptions(enabled=False))
        )

        assert queue.dead_letter.enabled is False

    def test_per_call_dead_letter_overrides_base_default(self):
        base = configure_rabbitmq_transport(
            service="orders", queue=QueueOptions(dead_letter=False)
        )

        queue = configure_rabbitmq_queue(base, QueueOptions(dead_letter=True))

        assert queue.dead_letter.enabled is True

    def test_repeated_calls_do_not_mutate_base_config_arguments(self):
        base = configure_rabbitmq_transport(
            service="orders", queue=QueueOptions(arguments={"x-base": 1})
        )

        configure_rabbitmq_queue(base, QueueOptions(arguments={"x-once": 1}))
        queue_again = configure_rabbitmq_queue(base)

        assert "x-once" not in queue_again.arguments
        assert queue_again.arguments["x-base"] == 1
