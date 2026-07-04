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

- This is **single-writer log replication / pub-sub**, NOT remote rendezvous. The networked
  part is only the *management layer* (Nodes, name-based discovery, register/find); data
  semantics come from xchannel (persistent ordered replayable log). Nearest prior art:
  Aeron + Aeron Archive replication.
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
│   ├── transport.rs          Transport + Listener traits; shared u32-LE framing; TcpTransport
│   │                         /TcpListener (network planes) + UnixTransport/UnixListener
│   │                         (local client plane); std-only, MAX_FRAME_LEN cap
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
│   ├── node.rs               Node: host_channel (register+announce), serve_stream,
│   │                         control plane (serve_control/connect_control_peer/
│   │                         run_maintenance over BroadcastDissemination+Registry), and
│   │                         subscribe (resolve via registry+membership → replica thread →
│   │                         Subscription). main.rs runs it all as `xchanneld`.
│   ├── registry.rs           Registry: CRDT merge over ChannelIdentity (+ tests)
│   └── broadcast.rs          BroadcastDissemination (concrete/TCP): per-peer reader
│   │                         threads → inbox + Membership; announce/emit_heartbeat/pump/
│   │                         addr_of/live_members. Implements core::dissemination trait.
└── xchannel-net-client/      external client↔daemon RPC over a Unix socket.
                              Client::connect(path) / connect_or_spawn() (auto-starts
                              xchanneld at DEFAULT_CLIENT_PATH; single-instance via socket
                              bind contention + stale-socket reclaim). create_channel
                              (→ Writer) / subscribe (→ Reader) / subscribe_path. Cross-
                              process ⇒ serializable ChannelOptions, NOT a closure (a
                              closure can't cross the wire; the in-process Node::host_channel
                              keeps its closure). Daemon owns placement, returns a path.
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
- `xchannel` is the substrate — the published crates.io release `xchannel = "4.0.0"` (the
  v2 format change has shipped). Key facts (Live/LateJoin, reserve/commit, file rolling,
  retention via `keep_files`, byte-offset resume) are mapped in DESIGN.md §1.
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

_As of 2026-06-22:_
- Dep is published **`xchannel = "4.0.0"`**. `.justfile` present in every commit; every
  commit passes `just check` (cargo check + fmt --check + clippy --all-targets).
- **v1 complete and hardened.** External client process → `Client` RPC → local `xchanneld`
  → gossip discovery + membership → cross-node replication. Hardening done:
  - **Self-healing subscriptions**: `Node::run_subscription` resumes from the replica head,
    reconnects on drop (backoff), and is stoppable (`Subscription::stop`/`unsubscribe`,
    socket shutdown to interrupt blocked reads). Idempotent `subscribe` RPC.
  - **Control-plane reconnection**: maintenance re-dials dropped seeds (tracked outbound
    peers, deduped, bounded `connect_timeout`).
  - **Replicas live under `data_dir/.replicas/<name>`**, distinct from origins
    (`data_dir/<name>`) — no collision when a node subscribes to a channel it also hosts.
  - **Cross-process test** spawns the real `xchanneld` and replicates via `Client` across
    processes (reads the replica — only possible cross-process). `Client::subscribe`
    retries the replica open (async creation race).
- ~28 tests across unit + two-node + client-RPC + cross-process; clippy clean; release builds.

### Topics (multi-producer fan-in) — design implemented, with documented deviations (`doc/TOPICS.md`)

The TOPICS.md design (§0–§8) is built on the `topics` branch, ~56 tests green (`just check`
clean), with the deviations noted at the end of this section (chiefly the §4.1 execution model).
**Phase 0 (the §1 prerequisites):**
- **`RegisterRejected`**: `claim_name` reserves the name before file creation and fails
  `AlreadyExists` on a lost collision (`de6f558`).
- **True `SubscribeAck.head`**: via `xchannel::Reader::head_record_index()` — dep bumped to
  **`xchannel = "4.1.0"`** (published; the head method is `2873410` in the xchannel repo)
  (`2c69805`).
- **Liveness-gated resolution**: `resolve` → `HostUnreachable` (owner not live) vs `TimedOut`
  (unknown) (`0398f44`).
- **Tombstones**: registry merge carries `(epoch, deleted)`; deregister, anti-resurrection,
  reclaim at `epoch+1`; permutation-convergence test (`add9994`).
- **Incarnation (5th prereq) — DECISION: `incarnation == the tombstone epoch`** (my best
  judgment while the user was away; revisitable). `member_id = (name, epoch)`. Respawn =
  deregister→reclaim (bumps epoch); crash = liveness→member-reaper tombstones→reclaim. No
  standalone code — reuses the epoch; the crash bridge (reaper) is Phase 2 (§6.1). If we later
  prefer a separate incarnation field (TOPICS §3.2 as literally written), revisit here.

**Phase 1 (local-only topics) — landed.** End-to-end: a client `create_topic`s, `publish_to_topic`
creates member channels, the daemon's mux merges them into the topic channel, and a consumer
`subscribe`s to the topic like any channel. Pieces:
- **Record format** (`core/mux.rs`): provenance **option (b)** — 18-byte prefix
  `{member_ref, member_index, orig_user_meta}` + slot-table control records; reserved control
  `msg_type` range (`d525b2a`).
- **Mux engine** (`core/mux.rs`): merges member `ReplicationSource`s into one topic `Writer` in
  arrival order with provenance, `max_batch_per_member` fairness, and **cursor recovery by
  scanning the topic tail** (no sidecar) — permutation-free but restart-safe (`6227435`).
- **Node API** (`create_topic`/`publish_to_topic`/`poll_muxes`/`run_mux`), members are ordinary
  registered channels; epoch = incarnation (`5ccf397`).
- **Client RPC** (`CreateTopic`/`PublishToTopic`) + `main.rs` mux loop + **cross-process
  end-to-end test** (`this commit`).

**Phase 2 core (remote members) — landed.** A member on any node feeds a topic hosted on
another: `member_of` rides `ChannelIdentity` (`3492da1`), and the topic owner's maintenance
loop (`attach_pending_members`) discovers members and attaches them — local ones by origin
file, remote ones by subscribing (a replica the mux reads, concurrent R+W across daemon
threads, which xchannel supports) (`4f2643e`). `publish_to_topic` no longer requires the topic
to be local. Proven by a two-node test (member on B → topic on A). `add_member` is idempotent
so publish-time and discovery-time attach don't collide.

**Phase 2 lifecycle (§6) + observability (§8) — landed.** The full design (§0–§8) is now
implemented:
- **`TopicGap`** (`4521b4b`): on resume, if a member aged records out of retention below the
  mux cursor, an attributed `TopicGap{member_ref, from, resumed_at}` is committed and the merge
  resumes at `earliest` — never a silent splice (§6.2).
- **Clean leave** (`9f425f4`): `Mux::remove_member` drains to head → `MemberClosed{final_index}`;
  the node detaches members whose registry entry is tombstoned/gone (drain + stop subscription).
- **Topic retirement** (`0f2b556`): `Node::deregister_topic` drains all members, writes a
  terminal marker, tombstones the topic channel, stops member subscriptions (§4.1).
- **`TopicOptions` + reaper** (`6cbd868`): `create_topic(&TopicOptions)` carries geometry +
  `max_batch_per_member` + `member_reap_after`; the reaper tombstones a member whose owner has
  been unreachable past the (opt-in, default-never) threshold so its incarnation can be
  reclaimed — `Registry::reap` is the deliberate not-owner-only exception.
- **Observability** (`2f1c0be`): `Node::topic_status` → per-member `{merged, head, lag, state:
  Quiet|Active|Unreachable}`, topic head, gaps emitted, slot-table version (§8).

**Recovery data-loss fix** (`a915d4d`, from a council review): `recover_cursors` originally keyed
cursors on the bare `member_ref` (max over the whole log) with the *latest* slot table — but
`member_ref` is a per-session counter reset each `Mux::open`, so two incarnations reusing a ref
across reopens conflated → a member could resume past its own head and **silently skip committed
records** (no `TopicGap`). Fixed by resolving each record's ref through the slot table **active at
that scan position** and keying the cursor on the resulting `(name, epoch)`. Also (`dbb7961`):
`next_ref` u16-exhaustion now errors instead of wrapping; the reaper dead-timer is keyed on
`(name, epoch)`; `add_member` emits a `TopicGap` for a fresh member with a pruned genesis and for
a resume that overshoots the source head (never `skip_to` past head). Failing repro landed first,
then the fix.

**Restart = reconstruct** (`doc/RESTART.md`): a restarted daemon **re-hosts its topics from disk**
(`Node::reconstruct_from_disk` at startup) with no persisted marker — a topic is content-sniffed
via its slot table (`mux::topic_config`), which now also **carries the topic's geometry**
(`region_size`/`mtu`) so the writer can reopen; members are re-attached from that slot table
(local origin / remote replica). Slot tables are **re-emitted every `SLOT_TABLE_REFRESH` records**
so a recent one is always retained (roll/prune safe; also honors §6.3's late-consumer-decode
promise). Proven by a cross-process daemon-restart test. Chose option (a) content-sniff over (b) an
xchannel header flag — keeps xchannel topic-agnostic, no cross-repo release.

**Honest remainders / known deviations:**
- **§4.1 execution model:** the mux runs on its **own thread** (`run_mux`, poll+sleep), not as
  poll-items in the daemon's shared forwarding loop as §4.1 specifies (same engine/invariants,
  different scheduling); the promotion path is therefore unwired.
- Recovery is correct but scans from genesis — not the bounded §5.2 scan (needs reverse reads).
- Reconstruct covers **topics + non-topic origins** (the latter recovering geometry via xchannel
  4.2.0 `Reader::region_size()`/`mtu()`). Not re-hosted: **empty** topics (no slot table). A
  reconstructed member's `member_of` reconciles via peers; remote members refresh when peers return.
  Dep is now `xchannel = "4.2.0"` (dev `[patch.crates-io]` → sibling repo until published).
- Mux poll holds the `muxes` lock during IO (§4.3 promotion path is the eventual remedy).
- Reserved control `msg_type` range is fixed (not per-`TopicOptions` as §4.2 muses).
- §9 open questions remain open **by design**: hierarchical topics (gated on registry cycle
  detection), per-topic promotion trigger (default: operator-configured), standalone-mux
  discovery (filesystem-watch candidate), cross-topic txns (explicitly out of scope).
- Client RPC surfaces `create_topic`/`publish_to_topic`; `deregister_topic`/`topic_status` are
  Node APIs (thin client-RPC wrappers are a trivial future add).

## Security

Trust model + threats + reporting are in `SECURITY.md` (TL;DR: unauthenticated plaintext,
**trusted-network only**; stream/control default to loopback, non-loopback bind warns; the
client plane is a permission-gated local Unix socket). **Tier-0 hardening is done**:
channel-name allowlist (no traversal/`.replicas` collision), absolute daemon-spawn path (no
`PATH` injection), lock-poison recovery (`util::MutexExt::lock_safe`), `MAX_CONNECTIONS` cap
on stream+client planes, `0700` data dir, 64 MiB frame cap, and the client plane on a
`0600` Unix socket under the data dir (no loopback port; `bind` arbitrates single-instance
startup + reclaims stale sockets). The daemon also takes an exclusive `flock` on
`<data_dir>/.lock` at startup, so two daemons can't share a data dir (the second exits fast;
OS-released on exit, no stale lockfile).
**Tier-1 (required before any untrusted exposure) is future**: mTLS/Noise on the network
planes, signed `ChannelIdentity` (don't trust `registered_at_nanos`/`owner`), authz.

## Next steps (post-v1 polish, optional)

1. **Auto-spawn hardening** — `connect_or_spawn` resolves an absolute path (no `PATH`), but
   doesn't `setsid`/daemonize; its exact wrapper isn't automated-tested (the cross-process
   test spawns the daemon directly).
2. **Client `Deregister` RPC** — tombstones + `Node::deregister` exist (registry merge carries
   `(epoch, deleted)`, reclaim at `epoch+1`), but there is no `ClientRequest::Deregister` to
   invoke it from a client, and an incoming tombstone doesn't proactively stop a live local
   subscription (it fails to re-resolve on next reconnect).
3. **Membership pruning** — `Membership::forget_stale` exists but nothing calls it; the
   maintenance loop could prune dead peers.
4. **Observability / graceful shutdown** — daemon loops swallow errors (`let _ =`); no
   logging or clean shutdown.

## Open questions (see DESIGN.md §8)

Serialization codec; peer discovery (seed list vs config); backpressure/retention
coupling and `Gap` handling; security/auth of connections; multiple replicas of one
channel on a node + stream dedup. (Resolved: **registry tombstones** now use an
`(epoch, deleted)` generation in the merge; **registry liveness vs membership** — `resolve`
now gates on owner liveness, `HostUnreachable` vs `TimedOut`.)

---
**Maintenance note for the assistant:** keep this skill current as the project evolves —
when a decision changes, a stub becomes real, a crate is added, or a next-step completes,
update the relevant section (especially *Current status* and *Next steps*) in the same
change. Keep DESIGN.md and this skill consistent.
