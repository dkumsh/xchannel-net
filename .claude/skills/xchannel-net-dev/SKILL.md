---
name: xchannel-net-dev
description: Development guide and living context for the xchannel-net project — a network of node managers that replicate xchannel logs across machines. Load this when working anywhere under the xchannel-net repo (designing, implementing, reviewing, or extending node managers, the registry, dissemination, replication, transport, or the client API). Captures locked design decisions, crate layout, conventions, current status, and next steps.
---

# xchannel-net — Development Guide

A network of **node managers** (one per node, `node ~= machine`) that turn local
[xchannel](https://github.com/dkumsh/xchannel) logs into network-visible, replicated channels. Provides
a **discovery service** and a **channel creation service**; the data plane replicates a
channel's records from its single owner node to read-only replicas on subscribing nodes.

**`DESIGN.md` at the repo root is the source of truth.** This skill is the fast-loading
orientation layer; when they disagree, DESIGN.md wins — and update both.

## Mental model (read this first)

  Networking lineage is only the *management layer* (Nodes, name-based discovery,
  register/find). Data semantics come from xchannel (persistent ordered replayable log).
  Nearest prior art: Aeron + Aeron Archive replication.
- **xchannel is single-writer.** ⇒ each logical channel has exactly one authoritative
  `Writer` on one owner node; every other node holds a **read-only replica**. There is
  **no consensus on the data path** — it is single-source fan-out. Preserve this invariant
  end-to-end; it is what keeps the system simple and fast.
- A **client never talks to a remote node directly** — only to its local manager, then
  reads/writes a purely local xchannel (the master it owns, or a synced replica).
- Records are self-describing (`msg_type: u16`, `length: u32`, `user_meta_u64`, payload).
  Replication = tail a `Reader` → ship each `User` record → `commit` into a `Writer` on
  the far side. Replicas are **record-identical, not byte-identical**.
- **Only `User` records cross the network.** `Roll`/`Skip` markers are local file
  artifacts of the source; the receiving `Writer` makes its own rolling decisions.
- **No-custody principle (DESIGN.md §5).** A node is a *forwarder + awareness service*,
  **never responsible for data** — unlike Kafka/NATS where `send()` transfers custody.
  The manager is **not in its own master's data path** (writer → mmap directly; manager
  only reads to forward). Manager death pauses remote forwarding but loses nothing; on
  restart it resumes from the last `RecordIndex`. **Restart = reconstruct, never restore
  from node-owned metadata**: rebuild from (data-dir scan + peer anti-entropy + clients
  reconnecting). The only durable node-owned state is **stable `NodeId` + config**.
  Ownership attaches to "this node holds the files," so a writer-less channel is
  *frozen but fully serveable*. **No node persists replication cursors** (DESIGN.md
  §5.2.1): the subscriber recovers its resume index and re-asserts it on (re)subscribe —
  neither source nor sink keeps a per-subscriber cursor.
- **`RecordIndex` is ABSOLUTE / genesis-relative**, source-authoritative (DESIGN.md §4).
  Resume index = `base + n` (records held), NOT a plain count — counting is wrong for a
  retention-truncated replica (`base > 0`). **No sidecar**: this is intrinsic in xchannel
  **v2** — each `ChannelHeader` carries `base_record_index` (immutable, file's first
  absolute index) + a per-file `message_count` of *user* records. Origin reads head via
  `Writer::next_record_index()`; replica via `Reader::base_record_index()` + records
  applied. Create a replica with `WriterBuilder::base_record_index(start)` so its headers
  are absolute. (xchannel ≤ v1's `message_count` was per-segment and counted skips — that
  was fixed by the v2 format change in the xchannel repo.)

## Locked design decisions (do not silently revisit)

| Area | Decision | Consequence |
|---|---|---|
| Owner death | Channel **freezes**, no failover | Same as a local writer stopping; *writer liveness* is an app concern, not ours. |
| Discovery | **Decentralized CRDT registry** | Last-writer-wins map keyed by `(registered_at_nanos, NodeId)`. No SPOF, no central name server. |
| Dissemination (v1) | **Eager broadcast + join-time anti-entropy + heartbeats** | NOT epidemic gossip / SWIM. Right for the expected **≤100 LAN nodes**. |
| Namespace | **Flat global names**, first-registrant-wins | Identity = the name; collisions resolved by the CRDT merge, loser gets `RegisterRejected`. |
| Initial pull | **Always full (retained) history** | Subscribing node materializes the whole channel; any local reader (Live/LateJoin) is instantly serviceable. |
| Redundancy / HA | **Post-v1, but keep 2 hooks** (DESIGN.md §9) | Absolute `RecordIndex` intrinsic in xchannel v2 `base_record_index` (done); name → *set* of endpoints (don't hard-bind one address). Same-machine redundancy = zero-downtime upgrades only, not machine HA. |
| Liveness | **Two separate concepts** | *Writer liveness* = app concern. *Membership liveness* (manager reachable) = ours, via heartbeats. Never conflate "no new messages" with "node down". |

Two liveness concepts and "retained history" honesty (`Gap`/`SubscribeAck.start` when
retention truncates) are subtle — keep them intact. Network positions use a logical
`RecordIndex` (counts `User` records); **never put xchannel byte offsets on the wire**.

## Crate layout

```
xchannel-net/                 (workspace root; crates live at root, NOT under crates/)
├── xchannel-net-core/        transport-agnostic core
│   ├── identity.rs           ChannelIdentity + resolve_collision (the CRDT merge key)
│   ├── wire.rs               ControlMsg (control plane) / StreamMsg (stream plane) /
│   │                         RecordFrame. Stream plane is multiplexed by StreamId;
│   │                         Subscribe/SubscribeAck handshake encodes the resume cursor
│   │                         (DESIGN.md §6.1).
│   ├── codec.rs              Hand-rolled LE codec (zero deps): encode/decode_control,
│   │                         encode/decode_stream (+ *_into for buffer reuse). Transport
│   │                         owns frame length-delimiting; 1-byte tag + u32-prefixed
│   │                         bytes/strings; Record is flat fixed header + payload.
│   ├── transport.rs          Transport + Listener traits; TcpTransport (+ try_clone) /
│   │                         TcpListener (std-only, u32-LE length-delimited, MAX_FRAME_LEN)
│   ├── membership.rs         Membership: NodeId→addr + heartbeat liveness (separate map;
│   │                         ChannelIdentity stays address-free, DESIGN §9)
│   ├── dissemination.rs      Dissemination trait — the swappable broadcast/gossip seam
│   ├── replication.rs        ReplicationSource (tail→frames) / ReplicationSink
│   │                         (frames→replica) — implemented over xchannel 4.0.0; absolute
│   │                         RecordIndex via base_record_index + next_record_index()
│   └── stream.rs             Stream-plane protocol over a Transport (generic): origin
│   │                         accept_subscription→StreamServer; subscriber subscribe→
│   │                         StreamClient. Drives the engines; tested over loopback TCP.
├── xchannel-net/             the node-manager daemon — lib + bin `xchanneld`
│   ├── node.rs               Node: host_channel (origin under data_dir) + serve_stream
│   │                         (accept loop, per-conn StreamServer thread, resolver over
│   │                         hosted map). main.rs wires it into the `xchanneld` binary.
│   ├── registry.rs           Registry: CRDT merge over ChannelIdentity (+ tests)
│   └── broadcast.rs          BroadcastDissemination (concrete/TCP): per-peer reader
│   │                         threads → inbox + Membership; announce/emit_heartbeat/pump/
│   │                         addr_of/live_members. Implements core::dissemination trait.
└── xchannel-net-client/      thin client lib. create_channel takes a
                              FnOnce(WriterBuilder)->WriterBuilder closure: manager owns
                              placement (path under data_dir), caller owns shape (all
                              builder opts, no drift). See DESIGN §7.
```

**Convergence vs dissemination are separate concerns.** The registry merge is a fixed
CRDT; how deltas travel sits behind `Dissemination`. v1 = `BroadcastDissemination`;
future-at-scale = a `foca`-backed SWIM impl behind the same trait, registry untouched.

## Dependency policy

- Keep the project **synchronous and lean** — it mirrors xchannel's low-latency,
  control-over-the-hot-path ethos. Avoid pulling an async runtime (tokio) for v1.
- The **data plane must never ride a gossip/P2P mesh** — direct point-to-point only.
- Future SWIM (only if node count outgrows ~100): **`foca` 1.0.0** is the fit
  (runtime/transport-agnostic, `no_std+alloc`, no forced tokio). `chitchat` 0.11.0 is
  prior art but hard-depends on tokio. `libp2p`/gossipsub rejected (wrong scale/shape).
- `xchannel` is the substrate (path dep `../xchannel` during co-dev; pin a release once
  the v2 change ships). Key facts (Live/LateJoin, reserve/commit, file rolling, retention
  via `keep_files`, byte-offset resume) are mapped in DESIGN.md §1.
- **Verified: reopen-for-append** (`Writer::open_or_create` → `open_file`): a restarted
  writer reopens the latest segment without truncation, resumes at the persisted
  `write_position`, with bounded crash recovery (INV5). Load-bearing for §5; no special
  support needed.
- **Landed in xchannel (format_version 2)** for this project: `ChannelHeader` grew to 128
  bytes with `base_record_index` (intrinsic absolute index — killed the sidecar);
  `message_count` is now a per-file *user*-record count; new `Writer::next_record_index()`,
  `Reader::base_record_index()`, `WriterBuilder::base_record_index()`. Greenfield: refuses
  v0/v1 files, no migrator (the `migrate` module/example were removed). See the xchannel
  repo's `FORMAT.md` §3 + `CHANGELOG`.

## Conventions

- Rust **edition 2024**, toolchain 1.95. `cargo build --workspace` / `cargo test --workspace`.
- Commits: **single-line messages only — no body, no prose, no trailers** (incl. no
  `Co-Authored-By`). **No conventional-commit type prefix** — `feat`/`fix`/`docs` are
  redundant; use plain `scope: summary` (e.g. `core: …`, `client: …`, `docs: …` only when
  the scope genuinely is docs).
  Never run `git config`. Commit/push only when asked. (Same rules apply to the `xchannel`
  repo going forward; its existing commits are left as-is.)
- Scaffold stubs use `unimplemented!("<exact intended behavior>")` so the contract is
  pinned without pretending to work.

## Current status (update this section as work lands)

_As of 2026-06-21:_
- On `main`: scaffold + doc reconciliation + dep repoint. Dep is now
  **`xchannel = { version = "4.0.0" }`** (published on crates.io; resolves from registry).
- Scaffold builds clean; `registry` collision tests pass (2).
- Implemented in `core`: **codec**, **TCP transport**, **replication engines**, and the
  **stream-plane protocol** (`core::stream`) — a channel replicates origin→replica over
  loopback TCP end-to-end. 14 core tests, clippy clean.
- **`xchanneld` serving half is live**: `Node::host_channel` + `Node::serve_stream`
  (threaded accept/dispatch) replicate a hosted channel to a subscriber over TCP; the
  `xchanneld` binary runs it (env-configured). Tested.
- **Dissemination done**: `BroadcastDissemination` (TCP) propagates registry deltas/syncs
  and learns peer addresses via heartbeats into `Membership`; verified over loopback TCP.
- Still missing: the daemon's **control listener** wiring (accept peer links → add_peer;
  periodic heartbeat; merge `pump()` output into the shared `Registry`); the
  **subscriber-side `Node`** routing (registry → `addr_of(owner)` → `stream::subscribe` →
  replica → expose to clients); **client RPC** (`xchannel-net-client` bodies).
- **xchannel 4.0.0 is published** (format_version 2, intrinsic absolute `RecordIndex`).
- Next: wire the **control plane into `Node`** (peer links + heartbeat loop + registry
  merge), then **subscriber-side subscribe** using `addr_of` to locate owners.

## Next steps (rough order; depends-on noted)

1. ~~**Wire serialization**~~ — **done** (`core::codec`, hand-rolled LE, zero deps).
2. ~~**TCP `Transport` + `Listener`**~~ — **done** (`core::transport::Tcp*`, std-only).
3. **`BroadcastDissemination`** bodies (announce / pump / live_members) + heartbeats.
4. ~~**`ReplicationSource` / `ReplicationSink`**~~ — **done** (`core::replication`).
5. Node-manager loop: **serving half done** (`Node::serve_stream`, `xchanneld` bin).
   Remaining: subscriber-side routing (registry → owner addr → pull replica), control
   plane (peer gossip + client RPC), `xchannel-net-client` bodies.
6. ~~**Membership / address resolution**~~ — **done** (`core::membership`, fed by
   `BroadcastDissemination` heartbeats; `addr_of(owner)` resolves where to connect).
7. **Wire control plane into `Node`**: control listener (accept → `add_peer`), heartbeat
   loop, drain `pump()` into the shared `Registry`. Then **subscriber-side `Node`**:
   resolve owner via registry+`addr_of` → `stream::subscribe` → run `StreamClient` thread
   → expose replica path. Then **client RPC** for external clients.

## Open questions (see DESIGN.md §8)

Serialization codec; peer discovery (seed list vs config); backpressure/retention
coupling and `Gap` handling; security/auth of connections; multiple replicas of one
channel on a node + stream dedup; **registry tombstones** (deregister as deleted-flag +
timestamp in the merge); **registry liveness vs membership** ("known, owner unreachable"
vs "known and live").

---
**Maintenance note for the assistant:** keep this skill current as the project evolves —
when a decision changes, a stub becomes real, a crate is added, or a next-step completes,
update the relevant section (especially *Current status* and *Next steps*) in the same
change. Keep DESIGN.md and this skill consistent.
