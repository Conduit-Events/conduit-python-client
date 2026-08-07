# Conduit Python Client

> **Status: work in progress.** Envelope construction/validation and a RabbitMQ transport are implemented; there's no `Client` yet tying them together.

The Conduit Python Client will be the second implementation of the Conduit message protocol, alongside [`conduit-node-client`](https://github.com/Conduit-Events/conduit-node-client). Its purpose is to prove the protocol is genuinely language-neutral rather than a Node.js convention, by implementing it independently in Python and demonstrating real interoperability with the Node client over RabbitMQ.

It starts minimal — enough to construct and validate envelopes and exchange messages with `conduit-node-client` over RabbitMQ — and is expected to grow toward parity with the Node client over time, rather than being replaced by a separate "full" client later.

## Protocol

The message-envelope schema, RabbitMQ transport conventions, and cross-client conformance fixtures live in [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) — the canonical, language-neutral spec this client implements. `conduit-node-client` depends on it directly; this client will too.

## Status

- **Envelope** (`conduit_python_client.envelope`) — `Meta`/`Message` models and `EnvelopeFactory` for constructing and validating events/commands. Done.
- **RabbitMQ transport** (`conduit_python_client.transports.rabbitmq`) — publish/subscribe over RabbitMQ via aio-pika, with dead-lettering and pattern-based dispatch. Done.
- **Client** — not started yet. This is where child-message derivation (deriving `correlationId`/`causationId` for a message in response to a parent) will live, since it needs the parent message in scope, unlike the factory.

Packaging is settled (`uv`, PEP 621), and the library is async throughout (`asyncio`/aio-pika) rather than sync.

## Envelope

`conduit_python_client.envelope` provides `Meta`/`Message` (pydantic models validated against the shared [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) schema) and `EnvelopeFactory`, which fills in `id`, `timestamp`, `correlationId` (defaults to the message's own `id` for a new causal chain), and `source` so callers only need to supply `type` and `data`.

## RabbitMQ transport

The RabbitMQ transport (`conduit_python_client.transports.rabbitmq`) is built on [aio-pika](https://docs.aio-pika.dev/). Each `RabbitMqTransport` instance opens and owns a single connection — there's no connection-sharing/pooling layer, unlike `conduit-node-client`'s `RabbitMqConnectionRegistry`.

That registry exists in the Node client mainly because amqplib doesn't ship any connection-sharing primitive of its own, so sharing one TCP connection across multiple transports (via a common `connectionName`) had to be hand-rolled: reference-counted acquire/release bookkeeping. It's a resource optimization, not a correctness requirement — without it, each transport just opens its own connection, which is what this client does today.

This is left out deliberately for now rather than ported: there's no `Client` yet, so it isn't known whether the Python usage pattern will actually involve multiple transports per process wanting to share a connection, and building that plumbing ahead of an actual need would be designing for a hypothetical. If it's needed later, aio-pika already ships a tested pooling primitive (`aio_pika.pool.Pool`), so the lift would be much smaller than what Node had to build from scratch.

## License

This project is licensed under the [MIT License](./LICENSE).
