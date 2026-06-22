# xchannel-net — Design

A network of **node managers** that turn local [xchannel](https://github.com/dkumsh/xchannel) logs into
network-visible, replicated channels. One manager per node (`node ~= machine`, though
several managers may share a host). Managers provide a **discovery service** and a
**creation service**; the data plane replicates a channel's records from its single
owner to read-only replicas on subscribing nodes.

> **Prior art (influences, not a model we follow).** The *management layer* — Nodes, a
> name-based discovery/creation service, register-and-find-by-name — echoes ideas from
> several distributed messaging and naming systems, but our registry is a decentralized
> gossiped CRDT, not a central name server. The *data semantics* are a persistent,
> ordered, replayable single-writer log, so the data plane is **log replication / pub-sub**
> — closest in spirit to Aeron + Aeron Archive replication.

---

## 0. Implementation status (v1, as of 2026-06)

> **This is a design document — much of it describes the target design, not all of which
> is built.** This section is the authoritative map of what the code on disk actually
> does. Where a later section describes behavior that is designed but not yet implemented,
> it is tagged **(not yet — see §0)**. This is experimental, pre-1.0 software
> (`version = 0.0.0`); the wire protocol and on-disk layout may change without notice.

**Implemented and tested** (unit + cross-process integration tests, `cargo test` green):

- Single-writer log replication over TCP; replicas are **record-identical to the origin in
  steady-state operation** (§4), driven end-to-end by a spawned `xchanneld` in
  `tests/cross_process.rs`.
- Hand-rolled little-endian wire codec + length-delimited TCP transport, with bounded
  frame lengths and truncation/edge-case tests (`codec.rs`, `transport.rs`).
- CRDT registry merge `resolve_collision` — commutative, associative, idempotent (§2.1).
- Decentralized discovery: eager `RegistryDelta` broadcast + `RegistrySync` anti-entropy on
  (re)connect; membership heartbeats; owner-address resolution.
- Client↔daemon RPC (`create` / `subscribe`) and `connect_or_spawn` single-daemon bring-up.
- Self-healing subscriptions: resume from the replica head, reconnect on drop,
  stop/unsubscribe (§5.1, `node.rs::run_subscription`).
- Resume handshake (`Subscribe.from` / `SubscribeAck.start`) and `Gap` on retention underrun.

**Partial / known limitations:**

- **`SubscribeAck.head` is a placeholder** equal to `start`, not the true high-water index
  (§6.1). Harmless today because nothing consumes it; it would mislead any future HA
  failover (§9) built on the "synchronized once applied up to `head`" contract.
- **Membership liveness is tracked but not used in resolution.** `live_members` and the
  heartbeat timeout exist, but the resolve path uses `addr_of` regardless of liveness, so
  "known, owner unreachable" (§5.4) is not surfaced and stale peers are not pruned from
  lookups.
- **Partition reconvergence is not guaranteed.** Delta broadcast is best-effort (a peer that
  errors is dropped, no retry); reconvergence relies on `RegistrySync` at (re)connect. With
  no `seeds` configured (the binary's default is `seeds: vec![]`) and inbound-only links not
  re-dialed, a healed partition may not reconverge automatically — two nodes can keep
  divergent registries (and, with §"Name collisions" unimplemented, both believe they own a
  name).
- **Crash/restart resume is unverified here.** It relies on xchannel's `next_record_index()`
  equalling the durably-committed user-record count after a crash (read and reasoned about
  in §5.3) — but there is **no kill/restart test in this repo** exercising it.

**Not yet implemented** (designed below, absent from the code):

- **Restart = reconstruct (§5.2)** — there is no data-dir scan / re-register on startup. A
  restarted daemon does **not** automatically re-register hosted channels or re-attach
  replicas; recovery currently depends on clients reconnecting and re-declaring.
- **Registry tombstones / `Deregister` (§5.4)** — `Deregister` is a wire/codec shape only;
  the merge has no deleted-flag, so a name once registered cannot be removed and a stale
  `Register` cannot be tombstoned.
- **`RegisterRejected` collision notification** — wire/codec shape only; `register_origin`
  does not detect a lost collision or notify the client, so a losing registrant silently
  believes it owns the name (see §"Name collisions").
- **Stream multiplexing (§6)** — `StreamId` is hardcoded to `0`; one connection carries one
  subscription. The multiplexing described in §6 is not built.
- **Authentication / authorization / encryption (§8)** — none. All three planes are
  unauthenticated plaintext; any peer that can connect can register names, subscribe to and
  pull any channel's history, inject registry deltas, and heartbeat as any node. **Run only
  on trusted networks; defaults bind `127.0.0.1`.**

**Clock caveat.** The collision tiebreak `(min registered_at_nanos, then min NodeId)` uses
each owner's **wall clock** (`SystemTime::now`). "First-registrant-wins" therefore holds
only to the precision of clock synchronization across nodes; under skew the slowest clock
wins. There is no logical/Lamport clock yet.

---

## 1. The substrate we build on (xchannel facts that shape everything)

xchannel gives us, per channel:

- **Single writer, many readers**, cross-process via mmap.
- An **append-only, persistent log**: 16-byte self-describing record headers
  (`msg_type: u16`, `length: u32`, `user_meta_u64`), aligned payloads.
- **Replay**: a `Reader` opens `LateJoin` (from earliest retained file) or `Live`
  (from the current tail).
- **File rolling** with per-file sequence numbers; `Roll`/`Skip` markers; retention via
  `keep_files(n)`.
- A `Reader` resumes by **byte offset within a region** — a purely local notion.

Three consequences drive the whole design:

1. **One owner per channel.** Single-writer means each logical channel has exactly one
   authoritative `Writer` on exactly one node. Everyone else holds a **read-only
   replica**. ⇒ *No consensus on the data path.* The data plane is single-source fan-out.
2. **Records are self-contained.** `(msg_type, length, user_meta, payload)` is the whole
   wire unit. Replication = tail a `Reader`, ship each `User` record, `commit` it into a
   `Writer` on the far side. The replica is **record-identical**, and local clients read
   it with plain xchannel.
3. **Replay is free.** "Replay full history" = a `LateJoin` reader. "Live" = a `Live`
   reader. The manager drives xchannel readers; it owns no replay machinery of its own.

---

## 2. Locked design decisions

| Area | Decision | Rationale / consequence |
|---|---|---|
| **Owner death** | Channel **freezes** — no failover, no election. | Identical to plain xchannel when a writer stops. *Writer liveness* is an application concern. |
| **Discovery** | **Decentralized CRDT registry**; v1 dissemination = eager broadcast + join-time anti-entropy. | No SPOF, no central name server to bootstrap. Full epidemic gossip is *not* needed at the expected scale (≤100 nodes, LAN) — see §2.1. |
| **Namespace** | **Flat global names**, first-registrant-wins. | Identity = the name. Collisions resolved deterministically (below). *Tiebreak uses wall-clock timestamps — see §0 clock caveat; loser-notification not yet implemented.* |
| **Initial pull** | **Always full (retained) history.** | Any subscribing node materializes the whole channel, so any local reader (Live or LateJoin) is instantly serviceable. No lazy/backfill logic. |

### Two liveness concepts, kept separate

- **Writer liveness** — is the owner still publishing? *Not our problem* (app layer). A
  frozen channel is a normal state.
- **Membership liveness** — is a node's manager reachable for registry exchange /
  serving replication? *Ours.* Used to prune dead nodes from the mesh and to tell a subscriber
  "your source's manager is gone." Never conflate "no new messages" with "node down."

### 2.1 Why a CRDT registry, not epidemic gossip

The registry is a **decentralized, eventually-consistent map** `name → identity` that
every node holds in full. The decision is really two separable concerns, and only the
first is load-bearing:

1. **Convergence (the merge).** The registry is a **last-writer-wins map CRDT**: the
   per-name merge `resolve_collision` is commutative, associative, and idempotent
   (keyed by `(registered_at_nanos, NodeId)`). Whatever order or duplication of updates a
   node sees, every node converges to the same map. This is the property that matters,
   and it is independent of *how* updates travel.

2. **Dissemination (how deltas travel).** Because the merge is a CRDT, dissemination is a
   **swappable transport concern**. Epidemic gossip (random peer fanout, rounds,
   anti-entropy, SWIM-style failure detection) earns its complexity at hundreds–thousands
   of churny nodes over a WAN. The expected scale here is **≤100 nodes on a LAN**, where
   that machinery — especially a SWIM failure detector — is more to build and test than
   the data plane we actually care about.

**v1 dissemination — eager broadcast + join-time anti-entropy:**

- Each manager knows its peers (seed list / simple membership).
- On register/deregister, **push the delta directly to all peers** — one round, immediate
  convergence in the common case. Registrations are rare and the payload is tiny, so
  O(N) fanout at N ≤ 100 is a non-issue.
- On (re)connect to a peer, **pull its full registry and merge** — anti-entropy, but only
  at join, not a continuous background process.
- **Membership liveness** = plain periodic heartbeats + timeout. No SWIM.

Because the merge (concern 1) is fixed, swapping the broadcast out for real epidemic
gossip later — *if* node count ever justifies it — is a change to the delta transport
only, with the registry logic untouched.

### Name collisions

Flat names + eventual consistency ⇒ two nodes may register the same name before
convergence. Resolved by the CRDT merge — a **deterministic total order** every node
computes identically, with no coordination round:

```
winner = (min registered_at_nanos, then min NodeId)
```

The loser's manager *should* report `RegisterRejected { winner }` to its client. (See
`identity::ChannelIdentity::resolve_collision`.) **Note (not yet — see §0):** the merge and
tiebreak are implemented and tested, but `register_origin` does not yet detect a lost
collision or emit `RegisterRejected`, so a losing registrant currently believes it owns the
name. The tiebreak also depends on wall-clock timestamps — see the clock caveat in §0.

---

## 3. Architecture

```
┌──────────────────── node manager (one per node) ────────────────────┐
│                                                                       │
│  Control plane (low volume)          Data plane (high volume)         │
│  ─ registry: name → identity (CRDT)  ─ ReplicationSource: tails the   │
│  ─ delta broadcast + anti-entropy      owner's local channel via a    │
│  ─ membership heartbeats               Reader, ships User records     │
│  ─ register / subscribe RPC          ─ ReplicationSink: writes a       │
│  ─ creation service                    local replica Writer, exposes  │
│                                        a Reader to local clients      │
└───────────────────────────────────────────────────────────────────┘
        │                                       │
   control protocol                        stream protocol
   (metadata, RPC, registry)               (ordered records, resumable)
```

Control and data ride **separate connections**. Control is tiny and latency-tolerant;
data is bulk and throughput-sensitive. They must never share a pipe.

### Crate layout

| Crate | Role |
|---|---|
| `xchannel-net-core` | Transport-agnostic: `identity`, `wire` frames, `transport` trait, `replication` engines. No opinion on TCP vs RDMA vs local IPC. |
| `xchannel-net` | The node-manager daemon (binary **`xchanneld`**): `registry` (CRDT merge), discovery/creation service, concrete TCP wiring. |
| `xchannel-net-client` | Thin library clients link against to talk to their **local** manager. |

A client never talks to a remote node directly — only to its local manager, then
reads/writes a purely local xchannel (the master it owns, or a replica kept synced).

---

## 4. The replication data plane

### Owner side — `ReplicationSource`
- Opens a **`LateJoin` reader from the earliest retained sequence** (full history).
- Emits one `RecordFrame` per **`User`** record. `Roll`/`Skip` markers are local file
  artifacts and are **consumed and skipped** — they never cross the network.
- Tails the log like any other reader, so the single authoritative `Writer` is **never
  blocked** by slow subscribers. A slow subscriber reads from the persisted log.

### Subscriber side — `ReplicationSink`
- Builds a local replica `Writer` with **geometry compatible** with the source
  (`region_size`, `mtu` from the registry identity).
- For each received frame: assert contiguous `index` (detect gaps/reorder),
  `try_reserve(len)`, copy payload, `commit(msg_type, len, user_meta)`.
- The replica is record-identical; the manager hands local clients a plain
  `xchannel::Reader` over it (Live or LateJoin, the client's choice).

### Network offset / resumption
- xchannel byte offsets are local; **never** put them on the wire.
- The wire position is a **logical `RecordIndex`** counting only `User` records.
- Steady state needs no start negotiation (always full history ⇒ source opens from
  earliest). Only **resume-after-disconnect** carries `from: Some(index)`.

### Retention = the lag bound (be honest about gaps)
- "Full history" = **full *retained* history**. Retention (`keep_files`) bounds how far
  back the source can serve.
- If a resuming subscriber requests `from` older than the source retains, the source
  replies `Gap { earliest }` — an explicit, first-class error (cf. Kafka "offset out of
  range"), never a silent hole.

---

## 5. Node state & recovery — the no-custody principle

This is the property that most distinguishes xchannel-net from broker/messaging systems
(Kafka, NATS, Aeron Cluster). In those, `send()` transfers **custody**: the
intermediary becomes responsible for persisting, retaining, and replaying the data. Here,
**custody is never transferred.**

> **No-custody principle.** A node manager is a *forwarder + an awareness service*. It is
> **never responsible for data.** The owner/writer is fully responsible for everything it
> publishes; the durable truth is the owner's xchannel files on disk. Nodes maintain
> awareness (of peers and of their local clients) and move bytes — nothing more.

Two consequences:

### 5.1 The manager is not in its own master's data path

A writer client writes to its local xchannel via mmap **directly**; the manager only
*reads* that channel (a `Reader` feeding the `ReplicationSource`). So when a manager dies:

- **Local writes continue** — writer client → mmap file, manager uninvolved.
- **Remote forwarding pauses** — but nothing is lost; the data is the persistent log.
- **On restart, forwarding resumes** from the last `RecordIndex` forwarded. xchannel's
  replay *is* the recovery mechanism. Subscriber-side is symmetric: the replica is itself
  a persistent log a reader client reads via mmap even while its manager is down.

### 5.2 Restart = reconstruct, never restore from node-owned metadata

> **(Not yet — see §0.)** This section describes the intended recovery model. The code does
> not yet scan the data dir or re-register on startup; today a restarted daemon recovers
> only as clients reconnect and re-declare. The rest of this section is the design target.

A node persists **no separate registry/subscription database.** On restart it rebuilds
from three authoritative sources:

1. **Scan its own data directory** → re-register the master channels it hosts and
   re-attach replicas. Files are self-describing (channel name in the xchannel
   `ChannelHeader`); a replica's resume index is recovered by reading the replica (count
   its `User` records). *This is the persistence — but of the data the owner is
   responsible for, not node bookkeeping.*
2. **Anti-entropy with peers** (`RegistrySync`) → relearn the remote half of the registry.
3. **Clients reconnect** → live writers re-attach to masters; readers re-subscribe.

The **only** durable state a node owns is **stable identity + config**: `NodeId`
(participates in the collision tiebreak and peer identification — config-pinned, never
random per-boot), listen addresses, seed peers, data dir.

**Why not persist the working registry?** A persisted registry can drift (claim a channel
whose owner never returned), so it must be reconciled against files/clients/peers on
restart *anyway* — buying nothing while adding a staleness failure mode. Reconstruction is
self-healing.

> **Is "reconstruct" the same as "persist and continue from where it left off"?**
> In **outcome**, yes — the node loses nothing and resumes exactly where it was. The
> refinement is only in the *form* of persistence, and it splits along one line:
> **data + resume position** are persisted (in the data files) → the "continue from where
> it left off" half; **client intent** (who writes/subscribes to what) is *not* persisted
> as node metadata → it is re-declared by clients on reconnect. The node keeps no
> authoritative metadata store of its own, so nothing can drift from reality.

Ownership therefore attaches to **"this node holds the files,"** not to "a writer client
is currently live." A channel whose writer exited but whose files remain is **frozen but
fully serveable** (full-history replay) — the "freeze is normal" decision (§2) in action —
and a restarted node can re-register and serve it from files alone.

### 5.2.1 Inventory of a node's "current information"

Making the two halves concrete — *what* the current information is, and how each piece
continues from where it left off. Three buckets: **config-durable**, **data-durable** (in
the channel files, the owner's responsibility), and **ephemeral** (reconstructed from the
other two plus live participants).

| Current information | Durable? where | How it continues after restart |
|---|---|---|
| Node identity & config — `NodeId`, listen addrs, seeds, data dir, defaults | **Config** | Loaded at startup. `NodeId` must be stable (tiebreak + peer identity). |
| Master channel data + write position | **Data** (xchannel master files) | Writer client reopens-for-append (verified §5.3); resumes at persisted `write_position`. |
| Replica channel data + applied position | **Data** (replica files) | Resume index = `base + n` read from the replica's own `ChannelHeader` (`base_record_index` + user records held); sink resumes pulling. *Counting alone is wrong for a truncated replica* — see below. |
| Registry entries for **own** channels | **Data** (implied by master files) | Re-derived by scanning the data dir; re-registered and re-broadcast. |
| Registry entries for **remote** channels | Ephemeral (durable at *their* owners) | Re-learned via `RegistrySync` anti-entropy from peers. |
| Replication cursors (who is at which index) | **Ephemeral** | Subscriber recovers its own index from its replica and **re-asserts it on (re)subscribe** — *neither side persists a cursor.* |
| Client sessions (who is connected, what they want) | Ephemeral | Clients reconnect and re-declare (create / register / subscribe). |
| Membership view (which peers are live) | Ephemeral | Re-established via heartbeats. |

The elegant consequence is the cursor row: because the subscriber carries its resume
position (recoverable from its own replica) and re-asserts it on reconnect, **no node
persists per-subscriber replication cursors** — not the source, not the sink. Position is
data-durable on the subscriber side and flows back to the source as a subscribe parameter.

**No sidecar — the absolute index is intrinsic to xchannel (v2+).** `RecordIndex` is
**absolute / genesis-relative** (§4), so a replica whose genesis was retention-truncated
holds records `base..base+n` and its resume index is `base + n`, *not* `n`. Rather than
track that in a companion file, it lives in xchannel's own `ChannelHeader`:
`base_record_index` (the file's first absolute index, immutable) plus the per-file
`message_count` of user records. So the sink rebuilds everything it needs from the
replica's *own files* on restart:

- **Resume index** = `Reader::base_record_index()` (current file) + user records applied,
  i.e. the head — equivalently `Writer::next_record_index()` once the replica writer is
  reopened.
- **Geometry** (`region_size`, `mtu`) is already in the same header.

The sink creates the replica with `WriterBuilder::base_record_index(SubscribeAck.start)`
so the replica's headers carry absolute (not replica-local) indices. This was the
motivation for the xchannel v2 format change (see its `FORMAT.md` / `CHANGELOG`): it
removed the only reason a sidecar would have existed.

### 5.3 Verified substrate assumption — xchannel reopen-for-append

The recovery story is load-bearing on a writer being able to re-open an existing channel
and continue appending. **Verified in xchannel 3.0.1** (`src/lib.rs`
`Writer::open_or_create` → `find_latest_sequence` → `open_file`):

- A non-empty existing file is opened read/write **without truncation**; the writer adopts
  `next_hdr` from the channel header's `write_position` and resumes appending there
  (`src/lib.rs:574-589`).
- It reopens the **latest rolled sequence**, so append continues across prior rolls.
- It even performs bounded **crash recovery** (INV5, `src/lib.rs:604-640`): if a prior
  writer died between `commit` and `publish_wp`, it advances one orphaned record and
  verifies the pre-install signature; deeper/unrecoverable lag refuses with a clear error
  (fallback: `cleanup_channel_files` + fresh channel).

⇒ A restarted writer process resumes its master seamlessly; the node's `ReplicationSource`
then resumes forwarding from its remembered `RecordIndex`. No special support needed.

### 5.4 Refinements this surfaces (track in §8)

- **Registry needs tombstones.** A plain LWW map can't express "permanently deregistered";
  an old `Register` could resurrect a deleted name. Deregistration must be a tombstone
  (deleted-flag + timestamp) inside the same merge.
- **Registry liveness vs membership.** CRDT entries have no TTL. A channel whose owner node
  is not currently a live member is *listed-but-unreachable*; discovery should surface
  "known, owner unreachable" distinctly from "known and live."

---

## 6. Protocols (shapes; encoding TBD)

See `xchannel-net-core::wire`. Three planes on separate connections/listeners.

**Control plane** (`ControlMsg`, peer↔peer, low volume): `Register`, `Deregister`,
`RegistryDelta`, `RegistrySync`, `Heartbeat`, `RegisterRejected`. `RegistryDelta` is the
eager broadcast on register/deregister; `RegistrySync` is the join-time anti-entropy
exchange (full registry on (re)connect). Both feed the same CRDT merge. `Heartbeat` carries
the sender's stream address → membership (§9). (Discovery needs no RPC — a node answers
lookups from its local converged registry.)

**Client plane** (`ClientRequest`/`ClientReply`, local client↔daemon, request/reply):
`Create { name, options }` → `Created { path }`; `Subscribe { name, wait_ms }` →
`Subscribed { replica_path }` | `Error`. The daemon owns placement and returns a local path
the client opens (§7).

**Stream plane** (`StreamMsg`, high volume): `Subscribe`, `SubscribeAck`, `Record`, `Gap`.
A source→subscriber connection is designed to be **multiplexed** — one link carrying any
number of subscriptions, each keyed by a compact `StreamId` the source assigns, so the
(string) channel name is *not* repeated on every record. **(Not yet — see §0:** `StreamId`
is currently hardcoded to `0` and one connection carries one subscription.**)**

### 6.1 Subscribe / SubscribeAck — the resume handshake

This is where the §5.2.1 cursor contract is encoded: the **subscriber owns the cursor**,
the source persists none.

```
Subscribe    { name, from }                                  subscriber → source
SubscribeAck { name, stream_id, start, head, region_size, mtu, file_roll_size, keep_files }  source → subscriber
Record       { stream_id, frame{ index, msg_type, user_meta, payload } }   source → subscriber (xN)
Gap          { name, earliest, head }                        source → subscriber (in place of Ack)
```

- **`from`** = **absolute** next index wanted = `base + n` (the replica's
  `ChannelHeader.base_record_index` + records held), *not* a plain count — counting breaks
  for a truncated replica (§4, §5.2.1).
  `RecordIndex(0)` ⇔ empty replica ⇔ "full retained history". No other start negotiation
  exists (always-full-history decision).
- **`start`** = first index the source will send. `start == from` is a clean resume.
  `start > from` occurs **only** when `from == 0` and genesis was retention-truncated — the
  replica then legitimately begins at `start`. That is how a subscriber learns it did not
  receive genesis.
- **`head`** = source's high-water index at accept time. The subscriber is *synchronized*
  once it has applied up to `head`; historical replay and live tail are the **same**
  stream, so there is no explicit catch-up message. **(Not yet — see §0:** `head` is
  currently a placeholder equal to `start`, not the true high-water index.**)**
- **`region_size` / `mtu`** = the source's authoritative geometry, so the sink builds a
  replica `Writer` guaranteed to fit every record (the registry copy may be stale).
- **`file_roll_size` / `keep_files`** = the source's rolling + retention policy, so the
  replica inherits the origin's disk bounds instead of growing as one unbounded file
  (`0` ⇒ no rolling / unlimited). Carried in the owner's hosted `ChannelSource`.
- **`Gap`** replaces the Ack when `from > 0` and `earliest > from`: the subscriber fell
  behind retention and its partial replica can't be extended contiguously. `earliest`/`head`
  let it decide whether to discard and re-subscribe from `0`. (Policy: §8.)

Each `Record` carries its own `index` so the sink asserts contiguity before `commit`
(detects loss/reordering). It could be elided later as an optimization.

---

## 7. Client API (`xchannel-net-client`)

Clients are **separate processes** that reach their local `xchanneld` over the client
plane. Because a closure can't cross a process boundary, the cross-process API uses a
**serializable `ChannelOptions`** (`region_size`, `mtu`, `file_roll_size`, `keep_files`) —
*not* the `WriterBuilder` closure. (The in-process `Node::host_channel` keeps a closure;
the two layers are distinct.) The daemon owns placement and replies with a **local path**;
the client opens its own `Writer`/`Reader` (no-custody).

- `Client::connect(addr)` — explicit daemon endpoint (managed / multi-daemon).
- `Client::connect_or_spawn()` — the well-known default endpoint, auto-starting `xchanneld`
  if none is running (single-instance falls out of bind contention; §ops below).
- `create_channel(name, &ChannelOptions) -> Writer` — daemon precreates under `data_dir`,
  registers + announces, returns the path; the client opens the single `Writer`.
- `subscribe(name, SubscribeMode, wait) -> Reader` / `subscribe_path(name, wait) -> PathBuf`
  — daemon resolves the channel (registry) + owner address (membership), builds a synced
  replica, returns the replica path; the client opens a `Reader` (`Live`/`LateJoin`).
  `wait`: `None` blocks until available, `Some(d)` errors after `d`.

Placement-vs-shape split: the daemon dictates *where* (under `data_dir`, for serving +
restart rediscovery), the client dictates *how* (geometry/retention via `ChannelOptions`).

**Daemon lifecycle / multiple daemons.** Run several daemons explicitly with distinct
`stream`/`control`/`client` addresses + `data_dir`, and point clients at one via
`connect(addr)`. Or rely on the implicit single daemon: `connect_or_spawn()` connects to
the default client endpoint and, if refused, spawns `xchanneld`; concurrent first-clients
race to `bind()`, the losers exit, everyone converges on the winner — no lockfile needed.

Wire: the client plane carries `ClientRequest`/`ClientReply` (§6 `wire`) — a small
request/reply RPC distinct from the peer-gossip `ControlMsg` and the data-plane `StreamMsg`.

---

## 8. Open questions (next rounds)

- **Serialization** of wire frames (length-prefix + which codec).
- **Membership**: peer discovery (seed list vs config), heartbeat interval + timeout for
  membership liveness. (Dissemination itself is settled for v1: eager `RegistryDelta`
  broadcast + `RegistrySync` anti-entropy on connect — see §2.1. Revisit epidemic gossip
  only if node count outgrows ~100.)

### Future-scale dissemination (verified options)

All sit behind `core::dissemination::Dissemination`; swapping one in leaves the CRDT
registry untouched. Versions checked 2026-06:

| Crate | Version | Fit |
|---|---|---|
| **`foca`** | 1.0.0 | **Best fit.** SWIM membership; *runtime- and transport-agnostic*, `no_std + alloc`, **no forced tokio**. You drive its loop and supply the transport — slots behind the trait without imposing an async runtime. |
| `chitchat` | 0.11.0 | SWIM + Scuttlebutt KV reconciliation (close to a drop-in gossiped registry), but **hard-depends on tokio** — pulls an async runtime into an otherwise synchronous, low-latency project. Prior art more than dependency. |
| `libp2p` gossipsub | — | **Rejected.** Built for large open/adversarial WANs; async/tokio + heavy dep tree; provides dissemination but not our CRDT merge. Wrong shape for a trusted LAN. And the data plane must never ride a gossip mesh regardless. |
- **Transport**: TCP baseline; later a co-located shortcut (shared-mem / local IPC) under
  the same `Transport` trait.
- **Backpressure & retention coupling**: how aggressively replicas persist vs. source
  retention; what a subscriber does on `Gap`.
- **Security / auth** of inter-node and client–manager connections.
- **Multiple replicas of the same channel on one node** and dedup of replication streams.
- **Registry tombstones** (§5.4): deregistration as a deleted-flag + timestamp inside the
  CRDT merge, so a stale `Register` can't resurrect a removed name.
- **Registry liveness vs membership** (§5.4): surface "known, owner unreachable" distinctly
  from "known and live" by joining registry lookups to the membership view.

---

## 9. Future: redundancy & high availability (post-v1)

The no-custody design (§5) makes failover natural — *not for v1*, but worth scoping so v1
doesn't preclude it. Three properties supply the substrate: nodes are **stateless
forwarders**, replicas are **record-identical**, and the **subscriber owns the cursor**
(§5.2.1). So a client can re-point at a *different* replica and resume at the same
`RecordIndex`.

### 9.1 What it does and doesn't buy

- **Scope it honestly.** *Same-machine* redundancy protects against **process** failure
  (maintenance, upgrade, crash) — a co-located standby survives a restart. It does **not**
  survive machine loss (both co-located nodes die together). For durability HA, run
  redundant nodes on **different machines** (which `node ≈ machine` supports naturally).
  Same-machine redundancy's real niche is **zero-downtime upgrades**.
- **Much of the benefit is already free.** Because the manager is not in the client's data
  path (§5.1), a node restart is already non-destructive: clients keep reading/writing
  their local files; only network sync pauses, then resumes from the cursor. A hot standby
  only adds (a) eliminating that pause and (b) surviving an *unplanned* crash with no gap.
  Weigh that delta before building it.

### 9.2 Single-writer is preserved either way

- **Subscriber-side** (two replicas of a remote channel; reader fails over): trivially safe
  — replicas are read-only.
- **Owner-side**: two nodes may both *forward* the same master (xchannel allows many
  readers of one file), but never two *writers*. If the writer client itself dies, that is
  "freeze is normal" (§2) — redundancy improves *forwarding/discovery* availability, not
  writing. Generalizes later to a serving tier: any node holding a synced replica can serve
  it downstream; "standby" is just the 2-node case.

### 9.3 The two v1-forward-compatible hooks (cheap, do now)

1. **Absolute, source-authoritative `RecordIndex`** — intrinsic in xchannel v2's
   `base_record_index` (already shipped; §4/§5.2.1). Required for correct resume on a
   truncated replica regardless of HA; it is *also* what lets two replicas agree on
   numbering so a client can fail over between them.
2. **Resolve a channel name → a *set* of serving endpoints**, never a single hard-bound
   address. `ChannelIdentity` already separates `owner: NodeId` from transport; just don't
   assume "one address per channel" anywhere downstream.

With those two in place, full standby/failover (failure detection, endpoint-set
resolution, client-side switch) is a clean later layer, no redesign.
