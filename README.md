# xchannel-net

A network of **node managers** that replicate [xchannel](https://github.com/dkumsh/xchannel) logs across
machines. Each node runs one manager providing a **discovery service** and a **channel
creation service**; a channel's records are replicated from its single owner node to
read-only replicas on subscribing nodes, where local clients read them with plain
xchannel.

The management model is a **decentralized mesh of node managers**: flat global channel
names, register-and-discover through a gossiped last-writer-wins registry, and
heartbeat-based membership to locate a channel's owner. The data model is **single-writer
log replication**. See [`doc/DESIGN.md`](doc/DESIGN.md) for the full architecture, the
decisions behind it, and the prior art that informed it.

A client never talks to a remote node. It talks to its own local manager and then reads or
writes a purely local memory-mapped log — the master it owns, or a synced replica. The
manager is a forwarder and an awareness service, never a custodian of data: it is not in
its own writer's path, and killing it loses nothing.

## Status

**Experimental, pre-1.0** (`0.2.0`); the wire protocol and on-disk layout change between
releases, without migrators. 0.2.0 does not read a 0.1.0 data directory.

**Platform: Unix only.** The client plane is a permission-gated Unix domain socket and the
data directory relies on Unix mode bits (`0700`/`0600`), so the daemon does not build on
Windows.

**Security: trusted networks only.** All network planes are unauthenticated plaintext —
anything that can reach them can register names, pull any channel's history, and inject
registry gossip. Defaults bind `127.0.0.1`, and binding elsewhere warns at startup. See
[`SECURITY.md`](SECURITY.md); authentication and encryption are future work.

What works, covered by unit, two-node and cross-process integration tests: an external
client process drives its local `xchanneld` to create or subscribe to channels; the daemon
discovers channels across the mesh, locates owners, and replicates single-writer logs
between nodes, producing **record-identical** replicas. Subscriptions are self-healing and
resume from the replica's own head. **Topics** merge many producers into one totally-ordered
log without breaking single-writer discipline ([`doc/TOPICS.md`](doc/TOPICS.md)). A
restarted daemon **reconstructs from disk** rather than from any saved metadata
([`doc/RESTART.md`](doc/RESTART.md)), and channels are discoverable by prefix
([`doc/DISCOVERY.md`](doc/DISCOVERY.md)).

Known gaps, deliberate or otherwise: no authentication of any kind; owner death **freezes**
a channel rather than failing over (a locked design decision); stream multiplexing is
unbuilt (`StreamId` is hardcoded to `0`); and there is no consumer-group or log-compaction
equivalent. §0 of [`doc/DESIGN.md`](doc/DESIGN.md) is the authoritative
implemented / partial / not-yet map.

## Crash safety

**Killing a node is not dangerous.** `kill -9`, a panic, a power cut, a reboot mid-write — none of
them corrupt anything, and none of them lose a record that was committed. That is a property of the
architecture rather than a recovery procedure, and it is worth stating plainly because it is unusual:

- **The manager is never in the writer's path.** A producer commits into its own memory-mapped log;
  the daemon only tails it to forward. Kill the daemon and the producer keeps writing at full speed
  and loses nothing — there is no buffer of yours in its memory, because it never takes custody of
  your data ([`doc/DESIGN.md`](doc/DESIGN.md) §5).
- **Nothing important is only in RAM.** Committed records are durable in the mmap. A kill part-way
  through a commit leaves a slot the reopen path resolves.
- **Positions are recomputed, not saved.** A topic's per-producer merge cursors are derived from the
  topic's own log; a subscriber resumes from its replica's own head. There is no cursor file to be
  stale, torn, or out of step with the data it describes.
- **Restart rebuilds rather than restores.** A daemon coming back scans its data directory, asks its
  peers, and lets clients reconnect. The only durable state it owns is its identity and its config,
  so there is no metadata store to corrupt.

The test suite asserts this rather than assuming it: cross-process tests `SIGKILL` a running daemon
mid-stream and require every producer to resume contiguously, with no duplicated and no missing
records.

`SIGTERM` and `SIGINT` are handled too, but only as a courtesy: a departing node tells its peers so
they stop treating its channels as reachable at once instead of waiting out the ten-second liveness
timeout. It exists for promptness, not for safety — there is nothing it has to unwind.

What a hard kill *does* cost is time. Peers take up to ten seconds to notice, and a subscriber
re-sends whatever was in flight. Prefer `SIGTERM`; reach for `SIGKILL` without anxiety.

## Workspace

| Crate | Role |
|---|---|
| [`xchannel-net-core`](xchannel-net-core) | Transport-agnostic engine: identity, wire frames, transport trait, replication, and the topic multiplexer. |
| [`xchannel-net`](xchannel-net) | Node-manager daemon (binary `xchanneld`): CRDT registry, decentralized discovery, TCP replication, duty cycle. |
| [`xchannel-net-client`](xchannel-net-client) | Thin client library for talking to the local node manager. |

## Development

```sh
just check   # cargo check + fmt --check + clippy --all-targets
cargo test --workspace
```

Design documents live in [`doc/`](doc); [`CHANGELOG.md`](CHANGELOG.md) records what changed
and, more usefully, why.
