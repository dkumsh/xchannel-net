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

## 0. Implementation status

> **This is a design document — much of it describes the target design, not all of which
> is built.** This section is the authoritative map of what the code on disk actually
> does. Where a later section describes behavior that is designed but not yet implemented,
> it is tagged **(not yet — see §0)**. This is experimental, pre-1.0 software; the wire
> protocol and on-disk layout may change without notice.

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
  stop/unsubscribe (§5.1). Establishment and reconnection are the conductor's
  (`Node::service_subscriptions`); forwarding is a poll-item on the duty cycle.
- Resume handshake (`Subscribe.from` / `SubscribeAck.start`), `Gap` on retention underrun and
  `Diverged` when the resume position is past the source's head — both decided before the
  source seeks, so an out-of-range resume fails loudly instead of blocking forever (§4).
- **True `SubscribeAck.head`** — the source advertises its real high-water index at accept
  time via `xchannel::Reader::head_record_index()` (§6.1), so a subscriber can detect when it
  has caught up to the frontier (`StreamClient::head`). Needs `xchannel ≥ 4.1.0`.
- **Liveness-gated resolution** — `resolve` requires the owner to be a live member (recent
  heartbeat), so it reports "known but owner unreachable" (`HostUnreachable`) distinctly from
  "channel unknown" (`TimedOut`), never handing back a stale address (§5.4).
- **Registry tombstones + reclaim (§5.4)** — the CRDT merge carries `(epoch, deleted)`: a
  tombstone dominates its generation (a stale `Register` can't resurrect a deregistered name)
  and a reclaim at `epoch + 1` lets a new owner retake the name. Tombstones are hidden from
  `get` but retained and propagated by anti-entropy. `Node::deregister` tombstones + announces;
  convergence is covered by a permutation test. Client-facing `Deregister` and `ForceDeregister`
  RPCs invoke it (the latter being the operator-only path to reclaim a *dead* owner's name), and
  merging a tombstone proactively retires any subscription held for that name. The reclaim guard
  judges the owner on how long it has been unreachable **from this node** — silence since the last
  direct contact, or, for an owner never contacted, how long this node has known of it and failed to
  reach it, a clock reset the instant contact is made. Never on this node's own uptime, which is not
  an observation about the owner at all: it made every daemon older than the threshold satisfy it
  unconditionally, so an owner that was alive and writing but merely unreachable from here could have
  its channel tombstoned.
- **Lost-collision detection (`RegisterRejected`)** — `claim_name` reserves the name before
  creating any file and fails with `AlreadyExists` if an earlier registration owns it, so the
  client is told rather than silently believing it owns the name (§"Name collisions").
  *Remaining: notifying a client whose ownership is lost to a cross-node race only after it has
  already been served a `Writer`.*

**Partial / known limitations:**

- **Stale peers are not pruned from the membership map.** Resolution now gates on liveness
  (below), but `Membership::forget_stale` is still not called by the maintenance loop, so
  stale entries linger (harmless to resolution, which ignores them, but unbounded).
- **Partition reconvergence is best-effort.** Delta broadcast drops a peer that errors and does
  not retry it; reconvergence relies on `RegistrySync` at (re)connect and on the mesh re-forming.
  The mesh **is** self-forming now (§2.2), so a node reachable from any seed ends up linked to
  every other, and a change is relayed hop-by-hop rather than stopping at the originator's
  neighbours. What is still not guaranteed: a node with **no** seeds configured (the binary's
  default is `seeds: vec![]`) knows nobody and nobody learns of it, and during a partition each
  side proceeds independently — both may believe they own a name. Lost-collision detection does
  not help there: `claim_name` can only reject a collision the *local* registry already knows
  about, and across a partition neither side knows about the other (§"Name collisions").
- **Crash resume is verified only for a quiesced writer.** Cross-process tests `SIGKILL` the
  daemon and respawn it, asserting every member resumes contiguously — which exercises the
  reopen path where the daemon *is* the writer (a topic's mux). What is still unverified is a
  kill **mid-commit**: the claim in §5.3 that `next_record_index()` equals the durably-committed
  user-record count after a torn write is read and reasoned about, not tested here.
- **Restart = reconstruct is implemented but not total (§5.2, `doc/RESTART.md`).**
  `Node::reconstruct_from_disk` scans the data dir at startup, re-registers plain origins
  (geometry from the channel header) and re-hosts topics (content-sniffed via their slot table,
  which carries the topic's writer configuration and members). Not recovered: an **empty** topic
  (no slot table to sniff), a plain origin's `member_of`, and a plain origin's rolling/retention
  policy — none of which is persisted anywhere, so the first reconciles via peer anti-entropy
  and the others fall back to defaults.

**Not yet implemented** (designed below, absent from the code):

- **Stream multiplexing (§6)** — `StreamId` is hardcoded to `0`; one connection carries one
  subscription. The multiplexing described in §6 is not built. (Connections no longer cost a
  thread each — the data plane is one duty cycle, `doc/TOPICS.md` §4.1 — so what multiplexing
  would still buy is sockets and handshakes, not scheduler load.)
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
| **Namespace** | **Flat global names**, first-registrant-wins. | Identity = the name. Collisions resolved deterministically (below). *Tiebreak uses wall-clock timestamps — see §0 clock caveat. A loser is notified at create time; a loss decided only after it was served a `Writer` still is not — see §2.1.* |
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

### 2.2 Forming the mesh

Peers are **not** discovered by broadcast, multicast or any membership protocol. A node dials the
control addresses in its seed list, and accepts whoever dials it. From that starting graph the mesh
**closes itself**, by two mechanisms that are separate and neither of which implies the other:

- **Gossiped control addresses.** A `Heartbeat` carries the sender's *control* address as well as
  its stream address, and knowledge about a third node travels as a `PeerHint`. A node that learns
  of a peer it holds no link to dials it. So any seed graph that is connected — a chain, a star,
  one well-known node — converges to a full mesh, which is the topology the rest of §2 assumes.
- **Relay on change.** A node that merges an inbound identity and finds its map actually changed
  forwards the winner to its other peers. Relaying only on a *change* is what terminates the flood:
  the registry merge is a total order and idempotent, so a given winning state can move a given
  node's map at most once, however many cycles the topology has. The relay skips the link it
  arrived on, which keeps a full mesh — where the relay is redundant, everyone having been told
  directly — to one round of no-op merges rather than one per peer.
- **Reply on loss.** The mirror of the above, and not optional: when the arriving state *loses* the
  merge, the winner goes back to the peer that sent it. Silence taught the sender nothing, and
  join-time anti-entropy only runs when a link is *established* — so on a link that stays up the two
  disagreed about that channel indefinitely, the sender resolving the wrong owner and, after a
  reclaim, serving a replica of an incarnation the mesh had retired. This terminates for the same
  reason the relay does, plus one step: a reply is sent only when the arriving state differs from the
  winner, so the reply — which *is* the winner — cannot provoke another.

Two properties are deliberate and worth keeping:

**Hearsay teaches addresses, never liveness.** A `PeerHint` is not a forwarded heartbeat. A
heartbeat means "I heard from this node"; forwarding one would make that claim on another node's
behalf, and *membership liveness is specifically this node's own ability to reach another*
(§5.4). `resolve` returns `HostUnreachable` from it, `force_deregister` guards a name reclaim on
it, the topic member reaper tombstones on it, and discovery reports `owner_live` from it — so
liveness by hearsay would let a node on the far side of a partition look reachable because a third
party vouched for it, which is exactly what `force_deregister` exists to refuse. A hint says only
"it exists, here is where"; liveness follows from dialling it and hearing for oneself.

**Both ends dial; the duplicate is collapsed afterwards.** Electing one dialler in advance — the
lower `NodeId`, say — is tidier and wrong, because the election happens before anyone knows whether
the elected node can actually *reach* the other. Under asymmetric reachability (a firewall, a NAT)
it can hand the job to the node that cannot dial, and the pair then never links although the other
direction would have worked first time. So both dial, and `dedup_links` resolves the resulting
duplicate knowing which direction succeeded.

Resolution has to be one both ends reach independently, or they would drop opposite links and be
left with none: **keep the link whose initiator has the lower `NodeId`**, and break a tie on the
link's two endpoint addresses, ordered. Each end knows, for each link, whether it dialled and who the
peer is, so both compute the same initiator; and one end's `(local, peer)` is the other's
`(peer, local)`, so ordering the pair names the *connection* identically on both sides. Neither half
may be anything local — a per-process link counter looks like a tie-break and is not one, because the
two ends number the same pair of links differently, each keeps the one the other drops, and the peers
are left with no link at all.

A link's peer is learned from its first heartbeat, which is also why dialling is gated on *node
identity* rather than dial address — an inbound link has no dial address, so address-based tracking
alone would call its peer unconnected and dial it a second time. The identity found at a dial address
is remembered for the same reason: a peer's advertised control address and the address we happened to
reach it on need not be the same, so without that memory a seed given under an alias was dialled,
deduplicated, and re-dialled on every tick forever.

**Dialling is bounded per tick, and it happens after the heartbeat.** The maintenance loop is also
the heartbeat loop, so what it spends dialling is taken out of this node's own liveness. Since the
candidate set grows with every address the mesh has ever mentioned, and an unreachable address costs
a full connect timeout, unbounded dialling let the *number of addresses this node knows of* set its
heartbeat period — and a node whose heartbeat exceeds the liveness timeout is declared dead by
everyone, which the topic member reaper then converts into tombstones for its live members' names.
Measured: 25 blackholed seeds delayed a cold start's first served request by 25 s; twelve unreachable
addresses were enough to have a live, actively-writing owner reported dead.

Three candidate lists — seeds, learned peers, and addresses claiming this node's own id — carry
**separate** budgets, so the worst-case tick is their sum, not one budget's worth. Sharing one would
let a long list of learned ghosts crowd out the seeds, and the seeds are the only addresses an operator
actually chose. The cap is what bounds the tick; the backoff reduces wasted work but cannot be relied
on to bound it, because past roughly seventy unreachable addresses the demand for retries exceeds what
a tick can spend and the loop simply saturates. A build-time assertion ties the sum to the liveness
timeout, because the failure it guards against is invisible: a heartbeat period that quietly grows past
`LIVENESS_TIMEOUT` looks like a healthy node right up to the moment every peer declares it dead.

Three things about the walk are load-bearing and were each wrong once. **Each list has its own cursor,
advanced by the candidates it actually consumed** — a single shared cursor reduced modulo each list in
turn pinned the learned walk to a constant index forever. **The penalty is charged for the attempt, not
for the failure**: an address can accept a connection and then drop the link (a hint naming a stream
port, or a peer whose control frames this release cannot decode), which costs a full dial while
recording nothing, so two such addresses consumed the entire budget every tick in perpetuity. And **the
gap escalates while attempts keep coming and decays once they stop** — with no exemption for an address
that once worked. An exemption keyed on "this address identified itself over a link we dialled there"
looked right and was a straight regression: that memo is never pruned, so it applied on every tick
forever and cleared the penalty before it could double, and a host powered off overnight was then
dialled every tick in perpetuity. Four such hosts took the heartbeat period from 0.5 s to a sustained
4.5 s. No exemption is needed, because a link that lasted longer than its own gap has already outlived
the penalty from the dial that created it, while a link dying *inside* its gap is flapping and ought to
back off.

Dialling is not the only blocking work in the tick, and the rest needed bounding too.

**Every control-plane write is bounded per frame, and every frame is bounded in size.** These writes
happen while holding the dissemination lock, which the heartbeat also needs, so an unbounded one is a
node-wide stall — and a socket send timeout is *not* such a bound: `write_all` retries whenever a
syscall moved a byte, so three unresponsive peers produced a 15 s heartbeat gap against a 10 s liveness
timeout, and a peer merely draining at 128 KiB/s produced 19 s without the timeout ever firing.
Registry frames are therefore capped at a few hundred identities (anti-entropy, which sends the whole
registry, is chunked), and **each burst of frames shares one deadline** — a budget per frame is not a
bound on a sequence of them, and every path that carries a whole registry is such a sequence: a peer
draining just fast enough to clear each individual check held the control plane for 13.4 s, four times
the liveness budget, without any bound firing.

That allowance is **per peer** and **derived from the burst's size against a minimum drain rate**, not
fixed. Per peer because a single deadline shared across peers is a global budget with a per-peer charge:
the write path checks its deadline before the first syscall, so once one slow peer had spent it, every
peer written afterwards failed with zero bytes and was evicted for a stall that was not its own —
measured at 0 of 5 peers surviving where the previous release kept 4 of 5 and delivered the whole delta. A fixed one
cannot be both large enough for a healthy peer and small enough to bound the tick, because the payload
is a whole registry: at the rate a real daemon drains, half a second buys under 7 MB, so a *healthy*
peer holding a large registry would have been unable to form a link at all. Expressed as a rate, the
policy reads "a peer slower than this is not keeping up", which holds whatever the registry size and
whatever the link. Small periodic frames — heartbeats, hints, departures — get a small fixed budget
instead, because they go to every peer in one pass and so cost P times whatever is chosen.

What that leaves, stated plainly rather than asserted away: a burst of R bytes may hold the
dissemination lock for up to `R / rate`. Bounding *that* requires not holding the lock across the write
— a per-peer outbox, which the stream plane already has — and is post-0.3.0 work. The build-time
assertion therefore covers the connect portion of a tick only, and says so.

Two different failures get two different tests. A peer still accepting bytes is working, however slowly,
and only its allowance judges it; a peer accepting **nothing** is wedged, and is cut off by a stall limit
instead — an allowance sized for a whole payload is far too much rope for a peer that is not moving, which
is how a single wedged peer held the control plane for 12.8 s on a 36 MiB burst. The stall limit sits well
above a TCP retransmission timeout, so a healthy peer that briefly cannot accept anything keeps its link.

A related ordering is load-bearing and easy to get backwards: a node **spawns its reader before** sending
its own join, so that it drains its peer while writing. Both ends of a pair dial, so both adopt at once;
without that, two nodes with a large registry fill each other's socket buffers, both hit the deadline,
and both drop the link — then retry identically for ever. Relays and
replies are coalesced to one frame per *source link* per pump cycle rather than one per identity: that
alone took a 200 000-identity delta from a 40 s heartbeat gap to 0.7 s.

**Member attachment skips what it cannot resolve.** Each remote member not yet replicating used to cost
a resolve with no bound on the count: fifty of them made a tick 10.5 s. A per-tick cap was worse than the
problem it fixed — a member whose owner is unreachable can *never* resolve, so it consumed a slot on every
tick for ever, and since the registry is a `BTreeMap` the same names took every slot in the same order and
members behind them were never attempted at all, so a topic silently stopped merging its live members.
Skipping a member whose owner is not a live member costs nothing (the liveness is already known) and
removes exactly that population, which made the cap unnecessary rather than merely sufficient: what
remains is a resolve that succeeds on its first pass and a connect on a thread of its own. The skip covers
the subscription only — attaching from a replica already on disk needs no reachable owner.

A build-time assertion ties the worst-case **connect** spend — count × connect timeout — to
`LIVENESS_TIMEOUT`, keeping a reserve back for the rest of the tick. That is all it computes, and its
docstring enumerates what it does not: the bursts, the heartbeat's per-peer allowances, and member
attachment. Two earlier versions claimed more — one written in truncating whole seconds, so a 900 ms
timeout counted as zero and it passed for any dial count whatever; one charging a join budget that is no
longer a constant. The reserve is a floor under what is left for the rest of the tick, not a proof that
the rest fits: at a 200 000-channel registry the uncovered terms can exceed it, and the term that makes
that possible is the burst, which the outbox closes.

**`NodeId`s must be unique; nothing negotiates them, and a duplicate is detected rather than
prevented.** A node generates 64 random bits from `/dev/urandom` on first start and keeps them in
`<data_dir>/.node_id`; `XCHANNELD_NODE_ID` overrides. There is no default, because a default is a
duplicate: two unconfigured daemons would share it silently. Uniqueness cannot be *guaranteed*
without a coordinator, and §2.1 rules out both a central registry and consensus at join — so the
design pays for detection instead. Across 100 nodes the chance any two random ids collide is
~3 × 10⁻¹⁶; the realistic source of duplicates is copying (a restored backup, a golden image
snapshotted after first start), which produces them with certainty and which no amount of entropy
addresses.

Detection is exact, not heuristic, and takes two forms. Two links reporting one id at two advertised
control addresses is two machines, since a machine advertises one control address; that case keeps
**both** links, because collapsing them would drop connectivity to a real peer in order to tidy away
a misconfiguration. The case that matters more is the one where the duplicated id is *ours*, which
has only a single link and so nothing to compare: a heartbeat that claims our id from an address that
is not ours is a twin, and the same heartbeat carrying *our own* advertised address is merely a link
we opened to ourselves — which a seed list naming every node produces routinely and which must never
be mistaken for a duplicate.

**A wildcard bind defeats all of this, so a node can advertise something other than what it bound**, and
the advertised value must be *per instance* — two nodes advertising one address are as indistinguishable
as two nodes advertising a wildcard, since it is advertised addresses that duplicate detection compares.
Both mistakes warn at startup.
Every mechanism above compares *advertised* control addresses, and a node bound to `0.0.0.0` advertises
exactly that: peers cannot dial it back, and — because every wildcard-bound node advertises the same
address — two of them sharing an id are indistinguishable, so their links are collapsed as duplicate
*links* rather than reported as a duplicate *identity*. That is the container deployment, which is also
the golden-image deployment, so the case the step-aside exists for was the case it could not see.
`XCHANNELD_ADVERTISE_CONTROL_ADDR` (and its stream counterpart) is the answer: bind the wildcard,
advertise something routable. A node that binds a wildcard and advertises nothing warns at startup.

For the second form to be reachable at all, a clone has to be able to *dial* its sibling — and the
ordinary dial candidates cannot contain it, because they come from membership and membership excludes
this node's own id by construction. So a fleet of clones seeded at a common bootstrap linked only to
the bootstrap, which saw the duplicate plainly and could do nothing about it, and the whole mechanism
was unreachable in precisely the deployment it was written for. The repair: a `PeerHint` naming *our
own* id at an address that is not ours is kept as a **dial candidate** — never as a member, since
hearsay confers no liveness, and never as grounds by itself, since a node restarted on an ephemeral
port would find peers relaying its own stale address. The hint earns a dial; the heartbeat that comes
back over that link is what decides, on direct evidence, as always.

A node that finds its own *generated* id duplicated and owns no channels discards it and stops with
status 3, so a supervisor's restart takes a fresh one. Every clone stands aside, not all but one: they
detect each other simultaneously and none has grounds to consider itself the original. That is
harmless, because none of them owned anything. Past the point where a node owns a channel, changing its
id would leave those channels registered to an owner that never returns, so from there this can only
warn — and the *verdict* is re-evaluated on every detection even though the *warning* is printed once,
so a node that owned a channel when it first noticed can still stand aside after it owns nothing.

A duplicate makes two nodes indistinguishable in the membership map (their addresses overwrite each
other), in channel ownership (`ChannelIdentity.owner`, and the `registered_at_nanos`/`NodeId`
collision tiebreak), and in link deduplication. For that reason a node never records *itself* in its
own membership map, from a heartbeat or a hint: doing so overwrote its own entry with a twin's
addresses, and since the dial candidates exclude this node's own id, the twin was then permanently
excluded from them — the two could never meet again, and the duplicate became unresolvable in
principle.

### Name collisions

Flat names + eventual consistency ⇒ two nodes may register the same name before
convergence. Resolved by the CRDT merge — a **deterministic total order** every node
computes identically, with no coordination round:

```
winner = (min registered_at_nanos, then min NodeId)
```

The loser's manager reports the rejection to its client. (See
`identity::ChannelIdentity::resolve_collision`.) `Node::claim_name` reserves the name through the
merge **before creating any file**, and fails the create with `AlreadyExists` if an earlier
registration owns it — so a losing registrant is told, and leaves no orphan origin file behind.

**Note (not yet — see §0):** that covers a collision the local registry already knows about, which
is the common case. A cross-node race resolved only *after* this node has already handed its
client a `Writer` is not covered: reporting it needs a server→client push the client RPC does not
have (it is strictly one request → one reply). The tiebreak also depends on wall-clock timestamps
— see the clock caveat in §0.

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
- Emits one `RecordFrame` per **`User`** record. `Skip` markers are local region artifacts and
  are **consumed and skipped** — they never cross the network.
- **Roll boundaries do cross, as an advisory hint** (amends the original "geometry is purely
  local" rule). xchannel consumes `Roll` markers transparently, so the source detects a roll by
  sampling `Reader::file_sequence()` (xchannel 4.3.0) around each read and sets
  `RecordFrame::starts_segment` on the record that follows one. **Why:** `keep_files` prunes by
  *file count*, so a replica that rolls on its own schedule retains a different window than the
  origin. An origin that rolls explicitly (`Writer::roll_file`, e.g. to begin every segment with
  a snapshot) with no `file_roll_size` set would leave its replicas rolling **never** — one
  unbounded file per channel per node. Mirroring the boundary makes `keep_files` mean the same
  thing on both sides. The hint rides *on* the record it precedes, so there is no separate
  signal to lose and a resuming subscriber re-derives it from the source's own segmentation.
  It stays **advisory**: this is awareness of the origin's geometry, not custody of it — a sink
  may ignore it (the mux does, since members' boundaries mean nothing to a merged topic).
- Tails the log like any other reader, so the single authoritative `Writer` is **never
  blocked** by slow subscribers. A slow subscriber reads from the persisted log.

### Subscriber side — `ReplicationSink`
- Builds a local replica `Writer` with **geometry compatible** with the source
  (`region_size`, `mtu` from the registry identity).
- For each received frame: assert contiguous `index` (detect gaps/reorder), roll first if
  `starts_segment` is set, `try_reserve(len)`, copy payload, `commit(msg_type, len, user_meta)`.
  Rolling on the hint is unconditional: repeating a roll after a crash between one and the
  first commit into the new segment costs one empty segment, while skipping it would misalign
  the replica permanently.
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
- If the subscriber's replica belongs to a **different incarnation** of the name, the source
  replies `Diverged { earliest, head }`. Two triggers, checked in that order:
  1. **Generation mismatch** — `Subscribe` carries the incarnation the replica holds
     (xchannel's `ChannelHeader.generation`, which for our origins *is* the registry's reclaim
     `epoch`), and the source compares it with its own. Precise, and it is the only check that
     fires when the new incarnation has already grown *past* the replica's length — the case
     where a resume would silently splice two unrelated logs whose indices line up.
  2. **`from` past `head`** — the channel has never held a record there. Imprecise, but
     independent of the generation plumbing and it catches an older subscriber.
  A mismatched replica can *also* look behind retention, so the generation check runs first;
  reporting a `Gap` there would name the wrong problem. `from == head` is not divergence
  (caught up), and `from == 0` is exempt — there is nothing to invalidate.
- Both refusals are decided **before** the source seeks to `from`. The seek reads forward and
  blocks until the channel reaches that index, so an unchecked out-of-range resume wedges both
  ends with no error raised anywhere — and, should the new log later grow past the index, the
  contiguity check would happily splice two unrelated channels into one replica. The recovery
  for either refusal is the same — discard the replica and re-subscribe from `RecordIndex(0)`
  — so `stream::subscribe` returns a typed `SubscribeError` that separates "rebuild" from
  "transient": retrying a rebuild case loops forever, while discarding a replica over a
  dropped connection would throw away a channel's history and re-pull it.

### Incarnation: whose files say what

The replica's *own header* records which incarnation it was built from — the source stamps its
generation into a freshly created replica via `SubscribeAck`, and xchannel keeps the on-disk
value when reopening, so a source cannot relabel an existing replica. This keeps the §5
no-node-owned-metadata rule intact: nothing extra is persisted, and a restarted daemon
rediscovers the incarnation by opening the files it already has. The origin likewise reports
its generation by reading *the log it is about to serve* rather than its registry entry — the
registry is eventually consistent and may be mid-convergence; the file is authoritative for
what is actually being served.

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

> **Implemented** (`Node::reconstruct_from_disk`, called at startup; see `doc/RESTART.md` for the
> mechanism and its accepted limits). This section remains the model; §0 lists what reconstruction
> does *not* recover, all of it state that was never persisted in the first place.

A node persists **no separate registry/subscription database.** On restart it rebuilds
from three authoritative sources:

1. **Scan its own data directory** → re-register the master channels it hosts and
   re-attach replicas, believing each log's own stamped name over the directory it sits in
   (`doc/RESTART.md`). Files are self-describing (channel name in the xchannel
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

- **Registry tombstones — implemented (§0).** A plain LWW map can't express "permanently
  deregistered"; an old `Register` could resurrect a deleted name. Deregistration is a
  tombstone inside the same merge, keyed by a `(epoch, deleted)` generation: a tombstone
  dominates its generation and a reclaim wins at `epoch + 1`. (Kept here for the rationale;
  a simple `deleted` + monotone timestamp is insufficient because registration is
  *first*-registrant-wins, so reclaim needs the epoch bump — see `resolve_collision`.)
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

**Stream plane** (`StreamMsg`, high volume): `Subscribe`, `SubscribeAck`, `Record`, `Gap`,
`Diverged`.
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
  stream, so there is no explicit catch-up message. Read from the source's own log at accept
  time (`Reader::head_record_index()`), so it is the real frontier — but a *snapshot* of it,
  which goes stale as soon as the source moves on.
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
`stream`/`control` addresses, `client` socket paths, and `data_dir`, and point clients at one
via `connect(path)`. Or rely on the implicit single daemon: `connect_or_spawn()` connects to
the default client socket and, if refused, spawns `xchanneld`; concurrent first-clients
race to `bind()` the socket, the losers exit, everyone converges on the winner — no lockfile
needed (a stale socket left by a crashed daemon is reclaimed: a probe connect that nobody
answers identifies it as dead, and it is unlinked before rebinding).

Two daemons must never share a `data_dir` (they'd corrupt each other's channel files). This
is enforced, not just documented: at startup each daemon takes an exclusive advisory lock
(`flock`) on `<data_dir>/.lock` and holds it for its lifetime; a second daemon on the same
dir fails fast with a clear message and exits. The lock is OS-released on exit, so a crashed
daemon leaves nothing to clean up (unlike a PID file). The default client socket lives under
`data_dir`, so distinct dirs also yield distinct sockets automatically — one fewer thing to
allocate per daemon.

Transport: stream and control are TCP (they cross hosts); the **client plane is a Unix
domain socket** — the client always talks to its *local* daemon, so the local hop needs no
network port, and filesystem permissions (the socket lives under the `0700` `data_dir`,
created `0600`) gate access rather than a loopback port any local process could reach.

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
  retention. *(What a subscriber does on `Gap` is **resolved** — auto-rebuild and report,
  §4: discard the replica, re-subscribe from `RecordIndex(0)`, and count the event so a
  rebuild is visible rather than silently absorbed. The rebuilt replica starts at the
  source's `earliest`, so it is honest about the history that retention removed.)*
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
