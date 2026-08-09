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
| `XCHANNELD_NODE_ID` | `1` | Stable identity. Must be unique in the mesh. |
| `XCHANNELD_DATA_DIR` | `/tmp/xchanneld` | Channel files, replicas, and the client socket. |
| `XCHANNELD_STREAM_ADDR` | `127.0.0.1:7000` | Data plane. |
| `XCHANNELD_CONTROL_ADDR` | `127.0.0.1:7001` | Registry gossip and heartbeats. |
| `XCHANNELD_SEEDS` | — | Comma-separated peer control addresses to form the mesh. |
| `XCHANNELD_CLIENT_PATH` | `<data_dir>/client.sock` | Local client plane. |
| `XCHANNELD_RECLAIM_AFTER_MS` | `300000` | How long an owner must be unreachable before an operator may reclaim its name. |
| `XCHANNELD_PROMOTED_TOPICS` | — | Topics given a merge thread of their own instead of the shared duty cycle. |
| `XCHANNELD_MUX_MAX_PARK_US` | `5000` | Cap on how long an idle duty cycle parks; `0` never parks. |

One daemon per data directory, enforced by an exclusive lock — a second exits immediately.

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
