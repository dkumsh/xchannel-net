# xchannel-net-core

Transport-agnostic engine for [xchannel-net](https://github.com/dkumsh/xchannel-net) — the wire types,
channel identity, replication engines, and topic multiplexer that the node manager is built
from. No sockets, no daemon, no policy: everything here is driven by a caller.

Most users want the daemon ([`xchannel-net`](https://crates.io/crates/xchannel-net)) or the client
([`xchannel-net-client`](https://crates.io/crates/xchannel-net-client)) instead. This crate is for
embedding the engines directly — hosting a topic multiplexer outside `xchanneld`, or putting the
replication protocol on a substrate other than TCP.

## What's in it

| Module | |
|---|---|
| `identity` | `ChannelIdentity` and the CRDT merge that resolves a name collision the same way on every node. |
| `wire`, `codec` | Control- and stream-plane messages, and a hand-rolled little-endian codec (zero dependencies). |
| `transport` | The `Transport` trait, blocking TCP and Unix implementations, and `FramedConn` — non-blocking resumable framing for a poll loop. |
| `replication` | `ReplicationSource` (tail a log → frames) and `ReplicationSink` (frames → a record-identical replica). |
| `stream` | The subscribe handshake, plus the poll-item forms a duty cycle drives. |
| `mux` | The topic multiplexer: merges N single-writer member channels into one totally-ordered topic log, stamping provenance on every record. |
| `membership`, `dissemination` | Heartbeat liveness, and the trait that makes the gossip strategy swappable. |

## Things worth knowing before you build on it

**A `RecordIndex` is absolute**, counted from channel genesis, not a position within a file
and not a count of records held. A replica whose history has been truncated by retention
still speaks in absolute indices, which is what makes resume work after a prune.

**Replicas are record-identical, not byte-identical.** Only user records cross the wire —
plus one advisory bit marking a segment boundary, so a replica's file layout tracks its
origin's and `keep_files` means the same thing at both ends.

**Nothing here persists a cursor.** A subscriber recovers its resume position from its own
replica and re-asserts it on every reconnect; neither side keeps per-subscriber state.

**A topic's order is arrival order at one merge loop** — not causal, not wall-clock, and
not reproducible across replays. Per-member order is preserved and gaps are attributed;
anything stronger must be derived from the per-record provenance.

**Security: none.** These engines do no authentication, authorization or encryption, and
`Transport` carries plaintext. See the workspace
[`SECURITY.md`](https://github.com/dkumsh/xchannel-net/blob/main/SECURITY.md).

## Documentation

[`DESIGN.md`](https://github.com/dkumsh/xchannel-net/blob/main/doc/DESIGN.md) is the architecture and
the reasoning; [`TOPICS.md`](https://github.com/dkumsh/xchannel-net/blob/main/doc/TOPICS.md) covers the
multiplexer in full. Both ship in this crate under `doc/`.

Experimental and pre-1.0: the wire protocol and on-disk layout change between releases,
without migrators.
