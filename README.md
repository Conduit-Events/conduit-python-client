# Conduit Python Client

> **Status: work in progress.** Envelope construction/validation, a RabbitMQ transport, schema validation, and a `Client` tying them together are all implemented.

The Conduit Python Client will be the second implementation of the Conduit message protocol, alongside [`conduit-node-client`](https://github.com/Conduit-Events/conduit-node-client). Its purpose is to prove the protocol is genuinely language-neutral rather than a Node.js convention, by implementing it independently in Python and demonstrating real interoperability with the Node client over RabbitMQ.

It starts minimal — enough to construct and validate envelopes and exchange messages with `conduit-node-client` over RabbitMQ — and is expected to grow toward parity with the Node client over time, rather than being replaced by a separate "full" client later.

## Protocol

The message-envelope schema, RabbitMQ transport conventions, and cross-client conformance fixtures live in [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) — the canonical, language-neutral spec this client implements. `conduit-node-client` depends on it directly; this client will too.

## Status

- **Envelope** (`conduit_python_client.envelope`) — `Meta`/`Message` models and `EnvelopeFactory` for constructing and validating events/commands. Done.
- **RabbitMQ transport** (`conduit_python_client.transports.rabbitmq`) — publish/subscribe over RabbitMQ via aio-pika, with dead-lettering and pattern-based dispatch. Done.
- **Schema validation** (`conduit_python_client.schema`) — validates every message against the shared conduit-protocol JSON schema, plus optional per-type payload schemas. Done.
- **Client** (`conduit_python_client.Client`) — ties the above together behind `emit`/`command`/`on`/`subscribe`, with a start/stop lifecycle and child-message derivation (`correlationId`/`causationId` inherited from the parent a handler is responding to). Done.

Packaging is settled (`uv`, PEP 621), and the library is async throughout (`asyncio`/aio-pika) rather than sync.

## Client

`conduit_python_client.Client` is the main entry point: it wires together an `EnvelopeFactory`, a `SchemaValidator`, and a transport (a `RabbitMqTransport` by default) behind `emit()`/`command()`/`on()`/`subscribe()`.

```python
client = Client(namespace="studio", service="user-service")

client.on("user.created", handle_user_created)
await client.start()

await client.emit("user.created", {"userId": "u1"}, stream_id="u1")

await client.stop()
```

Subscriptions registered via `on()` before `start()` are activated once the transport connects; registering after `start()` activates immediately. Every handler receives a `context` alongside the message with `context.client`, `context.transport` (the raw transport-specific delivery context, e.g. routing key), and `context.emit`/`context.command` for publishing follow-up messages — these automatically inherit the parent's `streamId`/`correlationId` and set `causationId` to the parent's `id`, so a causal chain can be traced end-to-end without every handler wiring that up by hand.

This mirrors `conduit-node-client`'s `Client` closely, with two deliberate Python-side adjustments:
- Public options (`stream_id`, `correlation_id`, ...) are snake_case, matching the rest of this codebase, rather than Node's camelCase.
- `stop()` doesn't forward options to the transport. Node's `Transport.disconnect(options)` structurally accepts an options bag that nothing actually consumes yet; Python's `Transport.disconnect()` takes none. Adding that ahead of a real need would be the same kind of speculative extensibility already deferred for connection pooling below.

## Schema validation

`conduit_python_client.schema.SchemaValidator` validates outgoing and incoming messages against the shared [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) JSON schema (built on [`jsonschema`](https://python-jsonschema.readthedocs.io/)), mirroring `conduit-node-client`'s Ajv-based `SchemaValidator`. Callers can also register a JSON schema per event/command `type` to validate `data`, or alias one type's schema to another's.

## Envelope

`conduit_python_client.envelope` provides `Meta`/`Message` (pydantic models validated against the shared [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) schema) and `EnvelopeFactory`, which fills in `id`, `timestamp`, `correlationId` (defaults to the message's own `id` for a new causal chain), and `source` so callers only need to supply `type` and `data`.

## RabbitMQ transport

The RabbitMQ transport (`conduit_python_client.transports.rabbitmq`) is built on [aio-pika](https://docs.aio-pika.dev/). Each `RabbitMqTransport` instance opens and owns a single connection — there's no connection-sharing/pooling layer, unlike `conduit-node-client`'s `RabbitMqConnectionRegistry`.

That registry exists in the Node client mainly because amqplib doesn't ship any connection-sharing primitive of its own, so sharing one TCP connection across multiple transports (via a common `connectionName`) had to be hand-rolled: reference-counted acquire/release bookkeeping. It's a resource optimization, not a correctness requirement — without it, each transport just opens its own connection, which is what this client does today.

This is left out deliberately for now rather than ported: `Client` creates one transport per instance, and nothing in this client's usage so far needs multiple transports in one process to share a connection. Building that plumbing ahead of an actual need would be designing for a hypothetical. If it's needed later, aio-pika already ships a tested pooling primitive (`aio_pika.pool.Pool`), so the lift would be much smaller than what Node had to build from scratch.

## License

This project is licensed under the [MIT License](./LICENSE).
