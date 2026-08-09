# xchannel-net

The node manager for [xchannel-net](https://github.com/dkumsh/xchannel-net), as a library. It owns the
decentralized registry, discovers channels across the mesh, and replicates single-writer
[xchannel](https://github.com/dkumsh/xchannel) logs between nodes.

## Using it

```toml
[dependencies]
xchannel-net = "0.3"
```

The daemon that runs this is [`xchanneld`](https://crates.io/crates/xchanneld) — a separate crate, so
that its argument parser is not a dependency of yours. This crate's only dependency is `xchannel`.

Embedding `Node` directly is supported and is what the in-process API exists for: `host_channel` takes
a closure, which a cross-process client cannot. Most applications want
[`xchannel-net-client`](https://crates.io/crates/xchannel-net-client) instead, which talks to a local
daemon over a Unix socket.

## How it behaves

**It is never a custodian.** A writer commits into its own memory-mapped log; the daemon
only tails it to forward. Kill the daemon and the writer keeps writing at full speed and
loses nothing; on restart it resumes from where it left off.

**It keeps no metadata of its own.** On restart it reconstructs from what is on disk, from
its peers, and from clients reconnecting — the only durable node-owned state is a node id
and its configuration. A channel's own log says which channel it is, so a data directory
that has been moved or edited is refused rather than silently served under the wrong name.

**Owner death freezes a channel; it does not fail over.** Reclaiming a dead owner's *name*
is possible but operator-invoked, and guarded — an automatic reclaim would be failover by
another name, and across a partition it could retire a channel whose owner is alive and
still writing.

**Gaps are reported, never spliced.** A subscriber that falls behind the origin's retention
is told so and rebuilds; one holding a replica of a reclaimed name is told it diverged.
Neither is quietly stitched onto a different history.

The data plane is a single **duty cycle**: replication sources, replication sinks and topic
multiplexers are poll-items in one loop, so a connection costs a poll-item rather than a
thread.

## Crash safety

`kill -9`, a panic, a power cut or a reboot mid-write corrupt nothing and lose no committed record.
The daemon is never in a writer's path, so killing it cannot cost a producer anything; committed
records are durable in their mmap; merge cursors and resume positions are *recomputed* from the logs
rather than saved, so there is no metadata that can be stale or torn; and a restart reconstructs from
the data directory, its peers, and reconnecting clients. Cross-process tests `SIGKILL` a running
daemon and assert every producer resumes contiguously.

`SIGTERM`/`SIGINT` are handled, but as a courtesy rather than for safety — a departing node tells
its peers so they stop treating its channels as reachable immediately instead of after the
ten-second liveness timeout, and removes its client socket. Prefer it; use `SIGKILL` without
anxiety.

## Security

All network planes are **unauthenticated plaintext**. Anything that can reach them can
register names, pull any channel's history, and inject registry gossip. Defaults bind
loopback and binding elsewhere warns at startup. The client plane is a `0600` Unix socket
under a `0700` data directory, so local access is governed by filesystem permissions. Run on
trusted networks only — see
[`SECURITY.md`](https://github.com/dkumsh/xchannel-net/blob/main/SECURITY.md).

## Documentation

[`DESIGN.md`](https://github.com/dkumsh/xchannel-net/blob/main/doc/DESIGN.md) (architecture; §0 is the
authoritative status map), [`TOPICS.md`](https://github.com/dkumsh/xchannel-net/blob/main/doc/TOPICS.md)
(multi-producer fan-in), [`RESTART.md`](https://github.com/dkumsh/xchannel-net/blob/main/doc/RESTART.md)
(reconstruction), [`DISCOVERY.md`](https://github.com/dkumsh/xchannel-net/blob/main/doc/DISCOVERY.md).
All ship in this crate under `doc/`.

Experimental and pre-1.0: the wire protocol and on-disk layout change between releases,
without migrators. Unix only.
