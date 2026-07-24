# Conduit Python Client

> **Status: work in progress — implementation hasn't started yet.** This repository currently contains only project setup.

The Conduit Python Client will be the second implementation of the Conduit message protocol, alongside [`conduit-node-client`](https://github.com/Conduit-Events/conduit-node-client). Its purpose is to prove the protocol is genuinely language-neutral rather than a Node.js convention, by implementing it independently in Python and demonstrating real interoperability with the Node client over RabbitMQ.

It starts minimal — enough to construct and validate envelopes and exchange messages with `conduit-node-client` over RabbitMQ — and is expected to grow toward parity with the Node client over time, rather than being replaced by a separate "full" client later.

## Protocol

The message-envelope schema, RabbitMQ transport conventions, and cross-client conformance fixtures live in [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) — the canonical, language-neutral spec this client implements. `conduit-node-client` depends on it directly; this client will too.

## Status

Nothing is implemented yet. Packaging, sync vs. async, which RabbitMQ library to use, and other foundational decisions are still open.

## License

This project is licensed under the [MIT License](./LICENSE).
