# xchannel-net

A network of **node managers** that replicate [xchannel](https://github.com/dkumsh/xchannel) logs across
machines. Each node runs one manager providing a **discovery service** and a **channel
creation service**; a channel's records are replicated from its single owner node to
read-only replicas on subscribing nodes, where local clients read them with plain
xchannel.

Networking. The data model is **single-writer log replication** — see
[`DESIGN.md`](DESIGN.md) for the full architecture and the decisions behind it.

## Status

Early scaffold. The architecture and protocol shapes are pinned in `DESIGN.md`; the
engine bodies are stubbed (`unimplemented!`) pending the next implementation round.

## Workspace

| Crate | Role |
|---|---|
| `xchannel-net-core` | Transport-agnostic identity, wire frames, transport trait, replication engines. |
| `xchannel-net` | Node-manager daemon: CRDT registry, decentralized discovery, TCP replication. |
| `xchannel-net-client` | Thin client library for talking to the local node manager. |
