# xchannel-net

A network of **node managers** that replicate [xchannel](https://github.com/dkumsh/xchannel) logs across
machines. Each node runs one manager providing a **discovery service** and a **channel
creation service**; a channel's records are replicated from its single owner node to
read-only replicas on subscribing nodes, where local clients read them with plain
xchannel.

The management model is a **decentralized mesh of node managers**: flat global channel
names, register-and-discover through a gossiped last-writer-wins registry, and
heartbeat-based membership to locate a channel's owner. The data model is **single-writer
log replication**. See [`DESIGN.md`](DESIGN.md) for the full architecture, the decisions
behind it, and the prior art that informed it.

## Status

Working v1: an external client process talks to its local `xchanneld` daemon to create or
subscribe to channels; the daemon discovers channels across the mesh, locates owners, and
replicates single-writer logs byte-faithfully between nodes. See `DESIGN.md` for the
architecture and the remaining refinements.

## Workspace

| Crate | Role |
|---|---|
| `xchannel-net-core` | Transport-agnostic identity, wire frames, transport trait, replication engines. |
| `xchannel-net` | Node-manager daemon (binary `xchanneld`): CRDT registry, decentralized discovery, TCP replication. |
| `xchannel-net-client` | Thin client library for talking to the local node manager. |
