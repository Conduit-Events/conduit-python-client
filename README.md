# Conduit Python Client

> **Status: work in progress — implementation hasn't started yet.** This repository currently contains only project setup.

The Conduit Python Client will be the second implementation of the Conduit message protocol, alongside [`conduit-node-client`](https://github.com/Conduit-Events/conduit-node-client). Its purpose is to prove the protocol is genuinely language-neutral rather than a Node.js convention, by implementing it independently in Python and demonstrating real interoperability with the Node client over RabbitMQ.

It starts minimal — enough to construct and validate envelopes and exchange messages with `conduit-node-client` over RabbitMQ — and is expected to grow toward parity with the Node client over time, rather than being replaced by a separate "full" client later.

## Protocol

The message-envelope schema, RabbitMQ transport conventions, and cross-client conformance fixtures live in [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) — the canonical, language-neutral spec this client implements. `conduit-node-client` depends on it directly; this client will too.

## Status

Nothing is implemented yet. Packaging, sync vs. async, which RabbitMQ library to use, and other foundational decisions are still open.

## RabbitMQ transport

The RabbitMQ transport (`conduit_python_client.transports.rabbitmq`) is built on [aio-pika](https://docs.aio-pika.dev/). Each `RabbitMqTransport` instance opens and owns a single connection — there's no connection-sharing/pooling layer, unlike `conduit-node-client`'s `RabbitMqConnectionRegistry`.

That registry exists in the Node client mainly because amqplib doesn't ship any connection-sharing primitive of its own, so sharing one TCP connection across multiple transports (via a common `connectionName`) had to be hand-rolled: reference-counted acquire/release bookkeeping. It's a resource optimization, not a correctness requirement — without it, each transport just opens its own connection, which is what this client does today.

This is left out deliberately for now rather than ported: there's no `Client` yet, so it isn't known whether the Python usage pattern will actually involve multiple transports per process wanting to share a connection, and building that plumbing ahead of an actual need would be designing for a hypothetical. If it's needed later, aio-pika already ships a tested pooling primitive (`aio_pika.pool.Pool`), so the lift would be much smaller than what Node had to build from scratch.

## License

This project is licensed under the [MIT License](./LICENSE).
