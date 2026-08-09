# xchannel-net

The node-manager daemon for [xchannel-net](https://github.com/dkumsh/xchannel-net) — binary
`xchanneld`, plus the library it is assembled from. One runs per machine. It owns the
decentralized registry, discovers channels across the mesh, and replicates single-writer
[xchannel](https://github.com/dkumsh/xchannel) logs between nodes.

Applications do not link this. They use
[`xchannel-net-client`](https://crates.io/crates/xchannel-net-client) to talk to their local daemon,
then read and write purely local memory-mapped logs.

## Running it

```sh
cargo install xchannel-net    # installs `xchanneld`
xchanneld
```

Configuration is environment only:

| Variable | Default | |
|---|---|---|
| `XCHANNELD_NODE_ID` | generated on first start | Stable identity, 64 random bits kept in `<data_dir>/.node_id`. Set it only for deployments that need deterministic ids; a *configured* id is never discarded automatically, so resolving a duplicate is then yours to do. |
| `XCHANNELD_NODE_NAME` | this host's name | Human-readable label, gossiped for display in logs, errors and listings. Cosmetic — never a key, never a tie-break, so a duplicate is confusing rather than incorrect. |
| `XCHANNELD_DATA_DIR` | `$HOME/.xchannel-net` | Channel files, replicas, the node's identity, and the client socket. One daemon per directory, enforced by a lock. Must be a **local** filesystem: channels are memory-mapped, and mmap coherence over NFS or SMB is not something this relies on. A network home directory therefore needs this set explicitly. |
| `XCHANNELD_STREAM_ADDR` | `127.0.0.1:7000` | Data plane — the address to **bind**. |
| `XCHANNELD_CONTROL_ADDR` | `127.0.0.1:7001` | Registry gossip and heartbeats — the address to **bind**. |
| `XCHANNELD_ADVERTISE_STREAM_ADDR` | the bound address | What to tell peers, when it must differ from what was bound. |
| `XCHANNELD_ADVERTISE_CONTROL_ADDR` | the bound address | Same, for the control plane. Required in practice when binding a wildcard: peers gossip whatever is advertised, and `0.0.0.0` is not something any of them can dial. **Must be this instance's own address** — it is what duplicate-`NodeId` detection compares, so two nodes advertising one value (a shared env file, or a Service address used in place of a pod address) are as indistinguishable as two advertising a wildcard, and a cloned image will never stand down. A wildcard *advertised* value warns at startup; a shared routable one **cannot** be detected at all, by construction, because identical advertised addresses are indistinguishable from a link to oneself. |
| `XCHANNELD_SEEDS` | — | Comma-separated peer control addresses to form the mesh. |
| `XCHANNELD_CLIENT_PATH` | `<data_dir>/client.sock` | Local client plane. Must match what clients look for, so change both or neither. |
| `XCHANNELD_RECLAIM_AFTER_MS` | `300000` | How long an owner must be unreachable before an operator may reclaim its name. |
| `XCHANNELD_PROMOTED_TOPICS` | — | Topics given a merge thread of their own instead of the shared duty cycle. |
| `XCHANNELD_MUX_MAX_PARK_US` | `5000` | Cap on how long an idle duty cycle parks; `0` never parks. |

One daemon per data directory, enforced by an exclusive lock — a second exits immediately. To run
more than one node on a host, give each its own `XCHANNELD_DATA_DIR`; the default is deliberately a
single per-user directory, because one daemon per user is the case that should need no configuration.

A bind address is **advertised as configured** unless an advertise address is given, so a container
binding `0.0.0.0` should set `XCHANNELD_ADVERTISE_CONTROL_ADDR` (and the stream counterpart) to
something routable. Without it, peers gossip `0.0.0.0` onwards, none of them can dial back, and two
nodes that share a `NodeId` cannot be told apart — so a cloned image never stands down. The daemon warns
at startup when it binds a wildcard and has nothing else to advertise.

Exit statuses: `0` on a clean shutdown, `1` when another daemon holds the data directory, and `3` when
the daemon stopped **to be restarted** — it found another node using its generated id, owned nothing,
and discarded the id so that a supervisor's restart picks a fresh one. A supervisor that restarts on
non-zero handles that case by itself; one that does not will need a manual start.

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
