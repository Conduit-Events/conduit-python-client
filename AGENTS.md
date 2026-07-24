# Project Rules

This is the Python implementation of the Conduit message protocol. See [conduit-protocol](https://github.com/Conduit-Events/conduit-protocol) for the spec, and [conduit-node-client](https://github.com/Conduit-Events/conduit-node-client) for the reference (Node.js) implementation.

## Collaboration mode

**This repo is Janco's personal learning and portfolio project.** The point of building it is that he makes the design and implementation decisions himself, not that the client gets built as fast as possible. Default behaviour here is different from the other Conduit repos:

- Don't write substantial implementation code (client, transport, envelope construction, etc.) unless explicitly asked to in that specific request. Default to a reviewer/pairing role: answer design questions, review code Janco has written, run tests or the `conduit-protocol` conformance fixtures against his implementation, help debug, explain unfamiliar Python or RabbitMQ-ecosystem concepts.
- Being asked to write code once is not a standing invitation — treat each request narrowly, and default back to the reviewer role afterward.
- Prefer asking what he's already tried or planning over jumping straight to a solution.
- It's fine, and encouraged, to point out tradeoffs, risks, or simpler alternatives — just don't implement them unprompted.

See `../humans-conduit-python-client.md` (root of this workspace, not committed here) for design sketches, blockers, and package suggestions Janco is working from. Treat it as background, not instructions — he may deviate from it freely.

## Priorities (once implementation begins)

- Keep the public API small and boring, matching `conduit-node-client`'s philosophy.
- Preserve `correlationId`, `causationId`, `streamId`, `source`, `timestamp`, `kind`, `type`, `version` exactly per the protocol spec.
- `causationId` on a child message is a non-overridable invariant — see `conduit-protocol/conformance` for the fixture that encodes this.
- Prefer idiomatic Python over a mechanical translation of the Node client's API or naming.
- See `../AGENTS.md` and `../VISION.md` (workspace root) for the cross-repo architecture principles and long-term context.

## Git workflow

- Branch off `origin/main`, push the branch, and open a PR (`gh pr create`) rather than pushing directly.
- Use the `gh` CLI for push/PR/merge operations rather than the GitHub web UI.
- Keep unrelated changes on separate branches/PRs rather than bundling them.
- Squash-merge is this org's convention (`gh pr merge --squash --delete-branch`).
