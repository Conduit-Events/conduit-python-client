import pytest

from conduit_python_client.transports import Transport


class FullTransport(Transport):
    def __init__(self):
        self.connected = False
        self.published = []
        self.subscriptions = []

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def publish(self, message, **options):
        self.published.append((message, options))

    async def subscribe(self, pattern, handler, **options):
        subscription = {"pattern": pattern, "handler": handler, "options": options}
        self.subscriptions.append(subscription)
        return subscription


class MissingConnect(Transport):
    async def disconnect(self):
        pass

    async def publish(self, message, **options):
        pass

    async def subscribe(self, pattern, handler, **options):
        pass


class MissingDisconnect(Transport):
    async def connect(self):
        pass

    async def publish(self, message, **options):
        pass

    async def subscribe(self, pattern, handler, **options):
        pass


class MissingPublish(Transport):
    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def subscribe(self, pattern, handler, **options):
        pass


class MissingSubscribe(Transport):
    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def publish(self, message, **options):
        pass


class TestTransportIsAbstract:
    def test_rejects_direct_instantiation(self):
        with pytest.raises(TypeError, match="abstract"):
            Transport()

    @pytest.mark.parametrize(
        "incomplete_transport",
        [MissingConnect, MissingDisconnect, MissingPublish, MissingSubscribe],
    )
    def test_rejects_subclass_missing_a_method(self, incomplete_transport):
        with pytest.raises(TypeError, match="abstract"):
            incomplete_transport()


class TestTransportSubclass:
    async def test_connect_and_disconnect_toggle_state(self):
        transport = FullTransport()

        await transport.connect()
        assert transport.connected is True

        await transport.disconnect()
        assert transport.connected is False

    async def test_publish_receives_message_and_options(self):
        transport = FullTransport()

        await transport.publish({"meta": {"type": "user.created"}}, routing_key="x")

        assert transport.published == [
            ({"meta": {"type": "user.created"}}, {"routing_key": "x"})
        ]

    async def test_subscribe_receives_pattern_handler_and_options(self):
        transport = FullTransport()

        async def handler(message, ctx):
            pass

        result = await transport.subscribe("user.created", handler, queue="q")

        assert result["pattern"] == "user.created"
        assert result["handler"] is handler
        assert result["options"] == {"queue": "q"}
