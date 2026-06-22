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

**Experimental, pre-1.0** (`version = 0.0.1`); the wire protocol and on-disk layout may
change without notice.

**Platform: Unix only.** The client plane is a permission-gated Unix domain socket and the
data directory relies on Unix mode bits (`0700`/`0600`), so the daemon does not build on
Windows.

Working v1 data plane: an external client process talks to its local `xchanneld` daemon to
create or subscribe to channels; the daemon discovers channels across the mesh, locates
owners, and replicates single-writer logs between nodes, producing **record-identical**
replicas in steady-state operation. This path is covered by unit and cross-process
integration tests.

Several behaviors described in `DESIGN.md` are **designed but not yet implemented** — most
notably restart-time reconstruction (no data-dir re-registration on startup), registry
tombstones / deregistration, collision rejection notices, stream multiplexing, and **any
authentication or encryption** (all planes are unauthenticated plaintext — run only on
trusted networks; defaults bind `127.0.0.1`). See **§0 "Implementation status"** in
`DESIGN.md` for the authoritative implemented / partial / not-yet map.

## Workspace

| Crate | Role |
|---|---|
| `xchannel-net-core` | Transport-agnostic identity, wire frames, transport trait, replication engines. |
| `xchannel-net` | Node-manager daemon (binary `xchanneld`): CRDT registry, decentralized discovery, TCP replication. |
| `xchannel-net-client` | Thin client library for talking to the local node manager. |
