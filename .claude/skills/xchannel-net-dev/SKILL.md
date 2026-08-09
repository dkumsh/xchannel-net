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
- **Only `User` records cross the network** — plus one **advisory** geometry hint.
  `Skip` markers are local region artifacts. `Roll` markers don't travel either, but *roll
  boundaries* do: the source flags the record following a roll (`RecordFrame::starts_segment`,
  detected via xchannel 4.3.0 `Reader::file_sequence()`) and the sink rolls before applying it,
  so replicas are segment-aligned, not just record-identical. Without this, `keep_files` (which
  prunes by file count) bounds the origin's disk but not the replica's — an origin that rolls
  only explicitly (`roll_file`, no `file_roll_size`) leaves replicas as one unbounded file.
  Advisory ⇒ a sink may ignore it; the mux does, since a member's boundaries mean nothing to a
  merged topic. Amends DESIGN §4's "geometry is purely local".
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
| Owner death | Channel **freezes**, no failover | Same as a local writer stopping; *writer liveness* is an app concern, not ours. Reclaiming a dead owner's *name* is possible but **operator-invoked** (`force_deregister`, guarded on not-live + `reclaim_after`) — never automatic, since an automatic reaper is failover and would let a partitioned minority retire a live channel, its `epoch+1` reclaim then winning the merge. |
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
│   │                         (frames→replica) — implemented over xchannel 5.1.0; absolute
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
- `xchannel` is the substrate — the published crates.io release `xchannel = "5.1.0"`
  (`format_version = 3`). Key facts (Live/LateJoin, reserve/commit, file rolling,
  retention via `keep_files`, byte-offset resume) are mapped in DESIGN.md §1. **Note what is
  *not* in the header:** `file_roll_size`/`keep_files` are `Writer`-instance state, so any code
  that reopens a channel must re-supply them (this bit us — see Topic disk bounds below).
- **Verified: reopen-for-append** (`Writer::open_or_create` → `open_file`): a restarted
  writer reopens the latest segment without truncation, resumes at the persisted
  `write_position`, with bounded crash recovery (INV5). Load-bearing for §5; no special
  support needed.
- **Landed in xchannel for this project.** `format_version = 2`: `ChannelHeader` grew to 128
  bytes with `base_record_index` (intrinsic absolute index — killed the sidecar);
  `message_count` became a per-file *user*-record count; `Writer::next_record_index()`,
  `Reader::base_record_index()`, `WriterBuilder::base_record_index()`. `format_version = 3`
  (5.0.0) widened `channel_name` 20 → 48 bytes. Greenfield at every step: the current build
  refuses v0/v1/v2 files, no migrator. See the xchannel repo's `FORMAT.md` §3 + `CHANGELOG`.
  (`channel_name` **is** now set on every channel, and `validate_channel_name` caps names at
  `xchannel::CHANNEL_NAME_MAX` = 48 so the bound and the field cannot drift — see *Name stamping*
  below.)

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

_As of 2026-08-09, at tag **v0.2.0**. Its headline is the §4.1 duty cycle (one loop, poll-items,
no thread per connection) plus the correctness work found by reviewing everything since 0.0.1 —
see `CHANGELOG.md`. **0.2.0 breaks on disk**: no migrator, a 0.1.0 data directory is not carried
forward (slot-table wire version, and channels written before name stamping are refused)._
- Dep is published **`xchannel = "5.1.0"`**. `.justfile` present in every commit; every
  commit passes `just check` (cargo check + fmt --check + clippy --all-targets).
- **v1 complete and hardened.** External client process → `Client` RPC → local `xchanneld`
  → gossip discovery + membership → cross-node replication. Hardening done:
  - **Self-healing subscriptions**: the conductor resumes from the replica head,
    reconnects on drop (backoff), and is stoppable (`Subscription::stop`/`unsubscribe`,
    socket shutdown to interrupt blocked reads). Idempotent `subscribe` RPC.
  - **Control-plane reconnection**: maintenance re-dials dropped seeds (tracked outbound
    peers, deduped, bounded `connect_timeout`).
  - **Every channel owns a directory**: origins at `data_dir/<name>/log`, replicas at
    `data_dir/.replicas/<name>/log` (+ `log.1`, `log.2` segments). The `.replicas` subtree keeps
    a replica from colliding with a same-named origin. The per-channel directory keeps *channel
    names* and *xchannel's segment suffixes* in separate namespaces — names may contain dots, so
    a flat layout made `md.aapl.1` (a channel) indistinguishable from segment 1 of `md.aapl`, and
    made restart guess: retention unlinks segment 0, the unsuffixed file, so a rolled+pruned
    channel left nothing named after itself. Deletion is `remove_dir_all`, not a glob.
  - **A `Subscribe` for a channel this node hosts returns the origin**, not a replica — no
    self-replication over loopback, no second copy with its own retention.
  - **Discovery** (`doc/DISCOVERY.md`, implemented): `list_channels(prefix)` → snapshot +
    cursor under one lock; `watch_channels(cursor)` reads a **node-local discovery log** that is
    itself an xchannel (`data_dir/.discovery/log`), so watchers cost the daemon nothing and
    resume/retention/invalidation reuse the log's own semantics (revision = `RecordIndex`;
    restart = fresh `generation` ⇒ re-list). Records are `Upserted`/`Removed` only; publishing
    is gated on the merge actually changing the map.
  - **`SubscriptionStatus` RPC** (`Client::subscription_status` / `Node::subscription_status`):
    progress (`synced`, `head_at_connect`) reported separately from liveness (`owner_live`,
    `last_record_at_ms`) plus rebuild counts by cause, so "quiet" and "broken" never look alike.
    `head_at_connect` is a `SubscribeAck` snapshot — not a live head.
  - **Cross-process test** spawns the real `xchanneld` and replicates via `Client` across
    processes (reads the replica — only possible cross-process). `Client::subscribe`
    retries the replica open (async creation race).
- 109 tests across unit + two-node + client-RPC + cross-process, plus one ignored measurement
  harness (`measure_outbound_high_water_mark`); `just check` clean; release build clean.

### Topics (multi-producer fan-in) — design implemented, with documented deviations (`doc/TOPICS.md`)

The TOPICS.md design (§0–§8) is implemented on `main` (the `topics` branch was rebased in and is
gone — its commit hashes below are the post-rebase ones), with the deviations noted at the end of
this section (chiefly the §4.1 execution model).
**Phase 0 (the §1 prerequisites):**
- **`RegisterRejected`**: `claim_name` reserves the name before file creation and fails
  `AlreadyExists` on a lost collision (`bd54bc8`).
- **True `SubscribeAck.head`**: via `xchannel::Reader::head_record_index()` — dep bumped to
  **`xchannel = "4.1.0"`** (published; the head method is `2873410` in the xchannel repo)
  (`8c8c323`).
- **Liveness-gated resolution**: `resolve` → `HostUnreachable` (owner not live) vs `TimedOut`
  (unknown) (`8439fd6`).
- **Tombstones**: registry merge carries `(epoch, deleted)`; deregister, anti-resurrection,
  reclaim at `epoch+1`; permutation-convergence test (`2583899`).
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
  `msg_type` range (`914557e`).
- **Mux engine** (`core/mux.rs`): merges member `ReplicationSource`s into one topic `Writer` in
  arrival order with provenance, `max_batch_per_member` fairness, and **cursor recovery by
  scanning the topic tail** (no sidecar) — permutation-free but restart-safe (`c07efd3`).
- **Node API** (`create_topic`/`publish_to_topic`/`poll_muxes`/`run_mux`), members are ordinary
  registered channels; epoch = incarnation (`8be8b2f`).
- **Client RPC** (`CreateTopic`/`PublishToTopic`) + `main.rs` mux loop + **cross-process
  end-to-end test** (`this commit`).

**Phase 2 core (remote members) — landed.** A member on any node feeds a topic hosted on
another: `member_of` rides `ChannelIdentity` (`0247d5b`), and the topic owner's maintenance
loop (`attach_pending_members`) discovers members and attaches them — local ones by origin
file, remote ones by subscribing (a replica the mux reads, concurrent R+W across daemon
threads, which xchannel supports) (`813bf70`). `publish_to_topic` no longer requires the topic
to be local. Proven by a two-node test (member on B → topic on A). `add_member` is idempotent
so publish-time and discovery-time attach don't collide.

**Phase 2 lifecycle (§6) + observability (§8) — landed.** The full design (§0–§8) is now
implemented:
- **`TopicGap`** (`21c7340`): on resume, if a member aged records out of retention below the
  mux cursor, an attributed `TopicGap{member_ref, from, resumed_at}` is committed and the merge
  resumes at `earliest` — never a silent splice (§6.2).
- **Clean leave** (`85f18bf`): `Mux::remove_member` drains to head → `MemberClosed{final_index}`;
  the node detaches members whose registry entry is tombstoned/gone (drain + stop subscription).
- **Topic retirement** (`2f87d46`): `Node::deregister_topic` drains all members, writes a
  terminal marker, tombstones the topic channel, stops member subscriptions (§4.1).
- **`TopicOptions` + reaper** (`acdaa5c`): `create_topic(&TopicOptions)` carries geometry +
  `max_batch_per_member` + `member_reap_after`; the reaper tombstones a member whose owner has
  been unreachable past the (opt-in, default-never) threshold so its incarnation can be
  reclaimed — `Registry::reap` is the deliberate not-owner-only exception.
- **Observability** (`5c28651`): `Node::topic_status` → per-member `{merged, head, lag, state:
  Quiet|Active|Unreachable}`, topic head, gaps emitted, slot-table version (§8).

**Recovery data-loss fix** (`c994b04`, from a council review): `recover_cursors` originally keyed
cursors on the bare `member_ref` (max over the whole log) with the *latest* slot table — but
`member_ref` is a per-session counter reset each `Mux::open`, so two incarnations reusing a ref
across reopens conflated → a member could resume past its own head and **silently skip committed
records** (no `TopicGap`). Fixed by resolving each record's ref through the slot table **active at
that scan position** and keying the cursor on the resulting `(name, epoch)`. Also (`090e8d0`):
`next_ref` u16-exhaustion now errors instead of wrapping; the reaper dead-timer is keyed on
`(name, epoch)`; `add_member` emits a `TopicGap` for a fresh member with a pruned genesis and for
a resume that overshoots the source head (never `skip_to` past head). Failing repro landed first,
then the fix.

**Restart = reconstruct** (`doc/RESTART.md`): a restarted daemon **re-hosts its topics from disk**
(`Node::reconstruct_from_disk` at startup) with no persisted marker — a topic is content-sniffed
via its slot table (`mux::topic_config`), which also **carries the topic's whole writer config**
(`mux::TopicGeometry` = `region_size`/`mtu`/`file_roll_size`/`keep_files`) plus **each member's
merge cursor**; members are re-attached from that table (local origin / remote replica). A slot
table is emitted at the **head of every segment**, so one is always inside the retained window
(§6.3's late-consumer-decode promise, the restart sniff, and cursor recovery for a quiet member).
Proven by a cross-process daemon-restart test. Chose option (a) content-sniff over (b) an
xchannel header flag — keeps xchannel topic-agnostic, no cross-repo release.

**Name stamping** (post-0.1.0): a channel's log carries its own name (`ChannelHeader.channel_name`),
and `reconstruct_from_disk` believes the log over the directory — refusing a mismatch (counted in
`Reconstructed::skipped`) rather than serving one channel's records under another's name. Closes
the last hole in "the files are authoritative": geometry, absolute index, incarnation and topic
membership all self-described already; the *name* came from the directory. `generation` does not
help — it travels with the file, so a renamed directory looks consistent. Names capped at 48 bytes.
**Trap:** xchannel carries `generation` across a roll from the on-disk header but takes
`channel_name` from whoever built the `Writer`, so **every** writer that reopens a channel must
re-supply the name or it blank-stamps the segments it rolls — and those outlive retention. Three
writers do: the client's (most segments are written there), the mux's topic writer, the replication
sink.

**`MAX_PENDING_OUT` = 1 MiB, measured** (was a guessed 8 MiB). `stream::bench::measure_outbound_high_water_mark`
(ignored; `--release -- --ignored --nocapture measure_`) sweeps cap × record size: **throughput is
flat 4 KiB → 32 MiB**, so the cap never limits a keeping-up subscriber and only buys memory
exposure. Chosen small because **the real buffer is the origin's log** (no-custody) — RAM buffering
duplicates it and only delays a throttle that costs nothing; *retention* is what decides whether a
slow subscriber survives. Worst case at MAX_CONNECTIONS: 32 GiB → 4 GiB. The guaranteed bound is
`cap + one record` (a record is always queued whole; the cap gates only *starting* another).

**Promotion trigger** (§9, resolved): `NodeConfig::promoted_topics` / `XCHANNELD_PROMOTED_TOPICS`
gives named topics their own mux thread (§4.1 rung 2); `poll_muxes` **skips** them, so a promoted
topic leaves the shared budget rather than gaining a second poller. The thread exits on *identity*
(`Arc::ptr_eq` against the current mux), not on the name being absent — otherwise a
retire-and-recreate leaves a stale thread beside the new one. `TopicStatus::promoted` reports it.
Deliberately node config, **not** the `TopicOptions` field §9 proposed: a client must not be able
to make the daemon spawn threads, `TopicOptions` policy fields don't survive restart, and
scheduling is the node's business not the topic's. Automatic (lag-driven) promotion stays unbuilt
by choice.

**Duty cycle** (post-0.1.0, §4.1): `xchanneld` runs **one** `Node::run_duty_cycle` thread polling
replication sources + sinks + muxes as peer poll-items (256 records each per turn). Not
thread-per-connection: 32 subscriptions cost 0 extra threads (6 idle → 6). Required building what
§4.1 assumed existed — there was no shared forwarding loop, the daemon was thread-per-connection
with blocking IO. New in core: `transport::FramedConn` (non-blocking resumable framing —
`read_exact` cannot survive a partial frame), `stream::{ServerPollItem, ClientPollItem}`,
`MAX_PENDING_OUT` backpressure (replacing blocking `write_all`'s implicit kind).
**Establishment is NOT on the loop** — resolve/dial/handshake/`skip_to` are blocking and unbounded,
so a transient thread does the handshake and hands the connection over, then exits; handshakes are
bounded by `HANDSHAKE_TIMEOUT`. Reconnects are the conductor's (`service_subscriptions`, on the
maintenance tick), registered via `conducted` (weak refs) **not** the `subscriptions` map — keying
off the map would silently make a `Node::subscribe` handle stop self-healing unless the caller also
filed it there. `Node::run_mux` survives as §4.1 promotion rung 2 (mux outside the shared loop).
Accepted per §4.1's budget note: topics and replication now share a core.

**Per-mux locking** (post-0.1.0 fix): `muxes` is `HashMap<String, Arc<Mutex<Mux>>>`; the map lock
is held only to clone a handle, **never across mux IO**. **Lock order: map → mux, never the
reverse** — go through `mux_of`/`mux_handles` and it holds by construction. `poll_muxes` also no
longer aborts the sweep on the first error (`?`), which used to let one topic stall all the others.
Two correctness fixes came with it, both of which the coarse lock had been masking: `Mux::finish`
marks the engine **inert** (a poll holding a handle sampled just before `deregister_topic` can no
longer commit past the terminal marker), and `merge_one` advances the cursor **after** the commit,
not before (a failed commit used to consume a record the topic never held). An over-`mtu` member
record — reachable because the 18-byte provenance prefix can push a record over the topic's limit —
is now rejected and counted rather than erroring forever.

**Mux merge latency** (post-0.1.0 fix): `run_mux` takes a `MuxIdle` strategy instead of a fixed
interval. It used to `sleep(5ms)` after *every* poll, so merge latency was 0–5 ms for a hot
producer; it now backs off **only when a poll merged nothing** (spin → yield → park doubling
50 µs → 5 ms, reset on any work). Median merge latency measured cross-process: **5.06 ms → 1.3 µs**.
The poll loop is not negotiable — a member is another process's mmap'd log with nothing to wait on,
and blocking on one of N would starve the rest — so the idle strategy *is* the latency contract.
`XCHANNELD_MUX_MAX_PARK_US` caps the park (`0` = never park). Note the counterpart: plain channel
replication was always event-driven (`pump_one` → `read_blocking`) and never had this floor.

**Startup ordering** (post-0.1.0 fix): `reconstruct_from_disk` runs after the listeners are
**bound** but before any plane accepts on them, and before `connect_seeds`. It used to run after
the serve threads spawned, so at startup the daemon could answer from an empty registry — a wrong
answer (client goes and re-creates a channel that is on disk; peer gets an empty anti-entropy
snapshot), not a slow one. Bind-then-reconstruct-then-serve keeps `connect_or_spawn`'s
single-instance arbitration (decided by the bind) working while an early client just blocks in
`accept`. It returns `Reconstructed { topics, origins, skipped }`, logged at startup because the
scan is O(retained records).

**Tombstones and discovery** (post-0.1.0 fix): a locally-produced tombstone must be **published to
the local discovery log *and* announced to peers** — one call, `Node::disseminate_tombstone`.
`deregister_topic` and the member reaper did only the second, so a watcher on the originating node
kept a phantom source while every *other* daemon reported the removal correctly (a peer
republishes what it merges). Registration keeps its own path: `merge_tracked` decides whether the
map changed and publishing follows that verdict, which is what stops anti-entropy reconnects from
storming the log.

**Topic disk bounds** (post-0.1.0 fix): the four writer-config fields travel together as
`TopicGeometry` because `file_roll_size`/`keep_files` are **`Writer`-instance state in xchannel,
not header fields** — so they must be re-supplied on *every* `Mux::open`, not just at creation.
They weren't, so a topic never rolled or pruned however `TopicOptions` configured it. Two knock-on
invariants came with the fix: the **mux drives its own rolling** (`file_roll_size` is not given to
the writer) because `Writer` cannot report a roll it did internally and the table must land at each
segment head; and **slot entries carry cursors**, because once a topic prunes, a quiet member's
data records age out and it would otherwise recover as fresh and be re-merged. Recovery
**overwrites** from a table (never maxes) — `MemberRegressed` can legally move a cursor backwards.

**Honest remainders / known deviations:**
- **§4.1 execution model:** the mux runs on its **own thread** (`run_mux`), not as
  poll-items in the daemon's shared forwarding loop as §4.1 specifies (same engine/invariants,
  different scheduling); the promotion path is therefore unwired.
- Recovery is correct but scans from genesis — not the bounded §5.2 scan (needs reverse reads).
- Reconstruct covers **topics + non-topic origins** (the latter recovering geometry via xchannel
  4.2.0 `Reader::region_size()`/`mtu()`). Not re-hosted: **empty** topics (no slot table). Dep is
  `xchannel = "5.1.0"` (published; no patch). The version history: **4.3.0** `Reader::file_sequence()`
  (roll boundaries observable — consumed by `RecordFrame::starts_segment`); **4.4.0**
  `ChannelHeader.generation`, an opaque incarnation id stamped into every segment
  (`WriterBuilder::generation` / `Reader::generation()` / `Writer::generation()`) — the registry
  reclaim epoch goes here, so a replica's own files say which incarnation they hold; **5.0.0**
  `format_version = 3`, `channel_name` widened 20 → 48 bytes (greenfield; v2 files refused);
  **5.1.0** a reader refuses a roll into a segment that breaks the absolute numbering, so a
  replica rebuilt under a live client `Reader` fails loudly rather than splicing two histories
  (an inode check would not do: retention unlinks files under live readers routinely).
  `generation` carries the registry reclaim epoch on our origins, so a replica's own header says
  which incarnation it holds; `Subscribe` carries it and the source refuses a cross-incarnation
  resume (`StreamMsg::Diverged`).

**Council whole-branch review — both blockers fixed:** (1) member records using a reserved control
`msg_type` are now **rejected** at `merge_one` (never forge a control record into the topic —
`4aeba4e`); (2) restart no longer **resurrects** a deregistered channel (deregister deletes its
files — `7522df9`) nor **spuriously retires** a re-hosted member (detach only on a positive
registry signal; rehost re-registers local members with `member_of`). Remote members are
**re-subscribed on reconnect/restart** so stale replicas refresh (`324be1b`); `XCHANNELD_SEEDS`
now configures peering. Test gaps closed: 2-member restart + **2-node remote merge/resume** cross-
process tests. Added Socrates' ordering-contract paragraph (TOPICS §4.3: topic order is arrival-
order only — no causal/reproducible/cross-producer meaning; use per-member provenance).
Follow-up (perf, not correctness): reconstruct double-scans each topic.
- (Fixed post-0.1.0: the mux poll no longer holds a shared lock during IO — per-mux locks.)
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
2. **Cross-node collision notification** — `claim_name` rejects a collision the local registry
   already knows about, but a race resolved *after* this node served its client a `Writer` is
   still silent. Needs a server→client push the client RPC does not have (strictly one request →
   one reply). The one genuinely open piece of `RegisterRejected`.
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
