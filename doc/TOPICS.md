# xchannel-net — Topics (multi-producer fan-in) — Design

Status: **implemented on the `topics` branch** (with documented deviations — see the
implementation-status note after §1). This document is the design; the §1 prerequisites it
depended on have since landed. Section numbering here is local to this document;
cross-references of the form `DESIGN.md §N` refer to the main design document.

---

## 0. Summary

A **topic** is a named, single-writer channel whose records are produced by a
**multiplexer (mux)** — a logical merge unit, in v1 a set of poll-items in the node
manager's existing forwarding loop (§4.1). The mux consumes N **member channels** —
ordinary single-writer xchannel channels, local or replicated from remote nodes — and
commits their records into one totally-ordered **topic channel**. The topic channel is
then a plain channel: locally readable via mmap, network-visible via the existing
replication plane.

This is the missing multi-producer primitive, built without violating single-writer
discipline: producers never share a writer; the merge is performed by exactly one
process, and the merged order is **explicit, durable, and replayable** because it is
itself an xchannel log.

```
producer A ──▶ member chan A ─┐
producer B ──▶ member chan B ─┤                        ┌─▶ local readers (mmap)
                              ├─▶ MUX ──▶ topic chan ──┤
remote P  ──▶ member chan P   │  (one                  └─▶ replication plane
   │ (on node X)              │   writer)                   (existing, DESIGN.md §4)
   └─ replicated via existing ┘
      stream plane to mux node
```

Design maxim, inherited from the sequencer/LMAX lineage: *multi-producer topics in
broker systems hide the arbiter inside the broker; here the arbiter is a named process
and its decision record is a file.*

---

## 1. Prerequisites (hard ordering constraint)

The topic layer multiplies member churn, so registry weaknesses that are cosmetic today
become correctness bugs under topics. **These must land first:**

| Prerequisite | Why load-bearing for topics | Tracked |
| --- | --- | --- |
| **Registry tombstones** (deleted-flag + timestamp in the LWW merge) | Member leave must be expressible; a stale `Register` must not resurrect a departed member into a live topic. | `DESIGN.md` §5.4 |
| **Incarnation identity** — channels identified as `(name, incarnation)`, not bare name | A producer that dies and respawns creates a *new* log. Without incarnation, the mux splices two unrelated logs into one member slot (index discontinuity, silent corruption of the merge). | new (§3.2) |
| **`RegisterRejected` collision notification** | Topic-name and member-name collisions must be surfaced to the losing client, not silently absorbed. | `DESIGN.md` §0 |
| **Membership liveness used in resolution** | The mux must distinguish "member owner unreachable" from "member quiet" to drive drain/stall policy (§6). | `DESIGN.md` §0, §5.4 |
| **True `SubscribeAck.head`** | The mux uses `head` to detect catch-up state per member for fairness and drain decisions. | `DESIGN.md` §6.1 |

Building topics before these exists is explicitly rejected: member churn will exercise
every hole in the LWW map in production.

> **Implementation status** (on the `topics` branch). All §1 prerequisites landed (tombstones as
> an `(epoch, deleted)` generation; `RegisterRejected` at create time; liveness-gated resolution;
> true `SubscribeAck.head`). **Incarnation is realized as that `epoch`**: `member_id = (name,
> epoch)`, respawn reclaims at `epoch+1`, a crashed producer is bridged by the reaper (§6.1).
> Built and tested: the mux (record format, provenance option (b), fairness); **recovery keyed on
> `(name, epoch)` resolved positionally through the slot table** (§5 — a council review caught and
> fixed a data-loss bug where cursors keyed on the bare per-session `member_ref` conflated
> incarnations across reopens); local + remote members via `member_of` discovery; `TopicGap` on
> retention underrun / fresh-pruned-genesis / resume-overshoot (§6.2, never a silent splice);
> clean-leave drain → `MemberClosed` (§6.1); topic retirement + terminal marker (§4.1);
> `TopicOptions` + reaper (§6.1); status/observability (§8).
>
> **Known deviations / not-yet (honest):**
> - **Execution model (§4.1):** the mux runs on its **own thread** (`Node::run_mux`, a poll+sleep
>   loop), *not* as poll-items in the daemon's shared single-threaded forwarding loop as §4.1
>   specifies. Same engine and invariants; different scheduling. The §4.1 promotion path
>   (shared-loop → thread → process) is therefore not wired.
> - **Recovery cost (§5.2):** correct, but scans from genesis rather than the bounded
>   last-slot-table scan (a correct bound needs reverse reads, an xchannel feature).
> - Mux poll holds the `muxes` lock across IO; reserved `msg_type` range is fixed (not
>   per-`TopicOptions`); `deregister_topic`/`topic_status` are Node APIs (no client RPC yet).
> - The §9 open questions remain open **by design**.
>
> Not independently re-verified beyond the test suite. See the `xchannel-net-dev` skill's
> Current-status for per-commit references.

---

## 2. Locked decisions

| Area | Decision | Rationale / consequence |
| --- | --- | --- |
| **Topic = channel** | A topic channel is an ordinary xchannel channel with one writer (the mux). No new data-plane concepts. | Everything downstream (local read, replication, replay, retention, resume) is reused unchanged. |
| **One mux per topic** | The topic has exactly one owner node hosting exactly one mux. Ownership resolved by the existing first-registrant-wins tiebreak. | Reintroduces a per-topic leader, deliberately. Freeze-is-normal applies (§7). |
| **Mux death ⇒ topic freezes** | No election, no failover in this design. Members keep writing durably to their own channels; the *merge* pauses. | Consistent with `DESIGN.md` §2. Blast radius is honest: producers lose nothing (no-custody), consumers of the merged order wait. |
| **Topic order = mux arrival order** | The authoritative interleave is the order records reach the mux's merge loop at the owner node. No timestamp fairness, no cross-member ordering promises. | Deterministic *after the fact* (it is written down), not fair *ex ante*. Remote members are interleave-biased by replication lag and topology. Documented, not apologized for. |
| **Topic channel is authoritative data** | The interleave cannot be re-derived from member logs. The topic channel is owned data (the mux is a producer-client like any other), never a discardable cache. | Custody stays with the writer — the mux — per the no-custody principle. Retention policy on the topic channel is a real durability decision. |
| **Provenance is mandatory** | Every topic record carries `(member_id, member_index)` (§4.2). Not optional, not a debug feature. | Pays three ways: mux restart recovery (§5), downstream audit/traceability, gap detection. |
| **Gaps are recorded, not spliced** | If a member replica falls behind retention (`Gap`), the mux commits an explicit gap-marker record into the topic (§6.2). | Extends the "honest about gaps" doctrine (`DESIGN.md` §4) into the merged stream. Consumers see the hole and apply their own policy. |
| **Members are ordinary channels** | A member channel is created, registered, replicated, and retired exactly like any channel. "Membership" is registry metadata, not a new channel kind. | Remote publish requires zero new transport: the member replicates to the mux node via the existing stream plane and the mux consumes the replica like a local channel. |

---

## 3. Registry extension

### 3.1 Topic and membership entries

Two additions to the registry value space, both flowing through the existing CRDT merge:

- **Topic registration**: `name → TopicIdentity { owner: NodeId, incarnation, options,
  registered_at_nanos }`. A topic name and a channel name share one flat namespace
  (a topic *is* a channel from the consumer's perspective); collisions resolve by the
  existing tiebreak.
- **Membership declaration**: a member channel's registry entry gains an optional
  `member_of: Option<TopicRef>` where `TopicRef = { topic_name, member_id }`.
  `member_id` is a compact identity assigned at declaration (§3.2).

The client API surface (`xchannel-net-client`):

```
create_topic(name, &TopicOptions) -> ()            // registers topic; owner = local node
publish_to_topic(topic, &ChannelOptions) -> Writer // creates member channel tagged
                                                   // member_of = topic; returns the
                                                   // ordinary single Writer
subscribe(topic, mode, wait) -> Reader             // unchanged — a topic is a channel
```

`publish_to_topic` on a node that does not own the topic works identically: the member
channel is local to the producer; the registry propagates its membership; the topic
owner's manager reacts (§4.1).

### 3.2 Member identity and incarnation

`member_id = (producer_name, incarnation)` where `incarnation` is assigned once at
member creation (monotonic per name is sufficient; a coarse timestamp + NodeId pair is
acceptable given the existing clock caveat, `DESIGN.md` §0). The mux keys its input
slots, cursors, and provenance stamps on the full `member_id`.

**Rule: an incarnation is never resumed by a different log.** A respawned producer gets
a new incarnation and therefore a new member slot; the old slot drains and closes
(§6.1). This is the anti-splice invariant.

### 3.3 Tombstones

Member leave and topic retirement are tombstones in the CRDT merge (prerequisite, §1).
A tombstoned member is drained (§6.1) and its slot closed; a tombstoned topic causes
the mux to drain all members, commit a terminal marker (§4.2), and stop.

---

## 4. The mux engine

### 4.1 Placement and lifecycle

**Execution model (v1): the mux is not a process, a thread, or even necessarily a
task — it is a set of poll-items in the daemon's existing single-threaded forwarding
loop.** A mux slot is structurally identical to what that loop already does: a
`ReplicationSource` is "poll a reader, push records to a socket"; a mux is "poll N
member readers, push records to a topic `Writer`". Same poll structure, different
sink. Replication sources, replication sinks, and mux slots are therefore peer
poll-items in one loop.

A **mux is a logical unit** — its member set, per-member cursors, and topic writer —
not a scheduling boundary. One loop hosts any number of muxes (one per topic is the
natural grouping, not a requirement); recovery (§5) is per-topic regardless of how
they are scheduled.

**Promotion path.** Because the merge engine is plain xchannel readers + one writer,
a topic that outgrows the shared core is promoted without any protocol or semantic
change: shared loop poll-item → dedicated thread → separate supervised process (or a
standalone binary hosting the same engine with non-registry discovery). Chosen
per-topic by measured load; identical invariants at every rung. To keep all rungs
available, the merge engine **must be cut as a library crate** (consumed by
`xchanneld` and by any standalone host) rather than daemon-internal code.

**Budget coupling (the cost of sharing the loop).** Everything in the loop shares its
cycles, so §4.3's provisioning statement widens: a hot topic (sum of member rates)
competes with replication forwarding on the same core. Two rules follow:

- Bounded batches per **poll-item**, not just per member (§4.3), so one saturated
  topic cannot head-of-line-block replication of unrelated channels for a full drain.
- The topic writer's commit sits on the shared loop's critical path. It is a sub-µs
  xchannel commit, but it makes the loop a *writer of authoritative data*, not merely
  a forwarder: a stall in the topic channel's mmap path (e.g. a fresh-page fault on a
  region roll) briefly stalls forwarding too. Acceptable at these magnitudes; the
  per-topic promotion path above is the remedy if it ever is not.

Owner-node manager behavior on registry events:

- **Topic registered locally** → create the topic channel under `data_dir`, start the
  mux with zero members.
- **Member declared (any node)** with `member_of` = a topic this node owns →
  - member is **local**: open a `LateJoin` reader on it directly;
  - member is **remote**: subscribe via the existing stream plane (`DESIGN.md` §4–6);
    the resulting local replica is consumed identically to a local member.
  The mux replays the member **from genesis** (or from its recovered cursor, §5),
  so late discovery loses nothing — persistence removes the registration race.
- **Member tombstoned** → drain and close the slot (§6.1).
- **Topic tombstoned** → drain all, terminal marker, stop, deregister.

### 4.2 Record format on the topic channel

Topic records are the member payload plus a mandatory provenance stamp. Layout options,
decision deferred to implementation (open question §9):

- **(a) `user_meta_u64` packing**: `member_slot: u16 | member_index: u48`. Zero payload
  overhead; costs the application's `user_meta` on topic channels and caps
  member_index range / slot count.
- **(b) small prefix header** in the payload: `{ member_id_ref: u16, member_index: u64,
  original_user_meta: u64 }` (18 B), with a slot-table record mapping `member_id_ref →
  (producer_name, incarnation)` committed whenever membership changes. Preserves the
  member's `user_meta`; costs 18 B/record.

Option (b) is the default recommendation: topics are a convenience layer, and silently
consuming the application's `user_meta` is a trap. `msg_type` is passed through
unchanged; mux-originated control records (slot table, gap marker, terminal marker) use
a reserved `msg_type` range declared in `TopicOptions`.

### 4.3 Merge loop and ordering

Single-threaded loop over member readers (replicas or local), batch-draining each ready
member via `try_read_batch` and committing to the topic writer. Arrival order at this
loop **is** the topic order (locked, §2). Fairness knob: `max_batch_per_member` bounds
how long one hot member can monopolize the interleave; beyond that, no fairness is
promised or attempted.

Backpressure posture is inherited and honest: the mux is a reader of its members
(cannot slow them — no-custody holds) and the single writer of the topic (cannot be
slowed by topic consumers). The mux itself must be provisioned to sustain the **sum**
of member steady-state rates; if it falls behind, it lags on all members
simultaneously. Per-member lag (member head − last merged index) is a first-class
metric (§8).

---

## 5. Recovery — no sidecar, cursors from the output log

The mux persists **nothing** of its own. Restart reconstruction, in the spirit of
`DESIGN.md` §5.2:

1. Reopen the topic channel writer (`open_or_create` resume, verified `DESIGN.md` §5.3).
2. Scan the topic channel **tail backwards** to recover, per member slot, the highest
   `member_index` merged (provenance makes this a bounded scan: stop once every
   currently-registered member has been seen, or genesis/last slot-table record is
   reached).
3. Re-learn membership from the registry (anti-entropy), re-attach each member (local
   reader or re-subscribe with `from = recovered_index + 1` — the existing resume
   handshake, `DESIGN.md` §6.1).
4. Resume the merge loop.

Per-member cursors are thus data-durable in the topic channel itself and re-asserted on
resubscribe — the same shape as the subscriber-owns-the-cursor contract in
`DESIGN.md` §5.2.1, one level up. **Duplicate-safety invariant:** commit to the topic is
the only side effect, and the recovered cursor is derived from committed records only,
so a crash between member-read and topic-commit re-reads and re-commits at most the
in-flight record with the same `(member_id, member_index)` — consumers can deduplicate
on provenance, and the mux itself must skip any member record ≤ its recovered cursor.

---

## 6. Member lifecycle policy

### 6.1 Leave, drain, and the quiet/slow/dead distinction

Three observably different situations, three different behaviors:

- **Quiet** — member registered, owner live, no new records. Normal. Merge continues on
  other members. (Writer liveness is an application concern, `DESIGN.md` §2.)
- **Unreachable** — member's owner node fails membership liveness. Mux keeps the slot,
  surfaces "member unreachable" in status, resumes on reconnect via the standard
  handshake. No records are skipped; the topic simply doesn't contain that member's
  records for the duration (arrival order, working as specified).
- **Tombstoned (clean leave)** — drain the member to its final head (requires true
  `head`, §1), commit a `MemberClosed { member_id, final_index }` control record, close
  the slot. `member_id` is never reused (incarnation rule, §3.2).

A configurable **quiescence timeout** may auto-tombstone members whose owner has been
dead beyond a threshold (`TopicOptions.member_reap_after`), defaulting to *never* —
reaping is a policy the operator opts into.

### 6.2 Member gap policy

If a member resume hits retention underrun (`Gap { earliest }`), the mux commits an
explicit control record into the topic:

```
TopicGap { member_id, missing: [from, earliest), resumed_at: earliest }
```

then resumes the member from `earliest`. The hole is visible, attributed, and durable.
Downstream consumers choose their own severity (ignore / alarm / halt). Silent splicing
is prohibited.

### 6.3 Slot-table maintenance

Whenever membership changes (join, leave, gap-resume), the mux commits an updated
slot-table record so any `LateJoin` reader of the topic can decode provenance without
external metadata. Self-describing files, per the project's restart-reconstruct
doctrine.

---

## 7. Failure model and blast radius (stated honestly)

| Failure | Producers | Topic consumers | Data |
| --- | --- | --- | --- |
| Mux process dies | Unaffected — keep writing member channels (mmap, manager not in path) | Merge pauses; local reads of already-merged records continue | None lost; merge resumes from recovered cursors (§5) |
| Mux node dies | Local producers on other nodes unaffected; producers co-located with mux keep writing locally | Topic frozen until node returns (no election, locked §2) | Member data safe at owners; **merged interleave** exists only on mux node's disk — the topic channel's durability is the mux node's disk plus its topic subscribers, exactly the standard no-custody durability posture |
| One member's node dies | That producer's *new* records pause replicating; others unaffected | Topic continues without that member's records (arrival order) | Member data safe at its owner; merge resumes on reconnect |
| Member falls behind retention | — | `TopicGap` record appears | Hole is explicit and attributed (§6.2) |

The second row deserves emphasis: **topics concentrate risk at the mux node by
design.** If the merged order matters enough that its loss is unacceptable, subscribe
at least one other node to the topic channel (replication-as-durability, the standard
posture) — and say so in the topic's runbook.

Placement is operational, not cosmetic: remote members pay a replication hop to the
mux node, biasing the interleave toward co-located producers. Latency-sensitive
deployments should place the topic owner with the highest-rate or most
latency-critical producers.

## 8. Observability (minimum bar to ship)

Per topic, exported by the owner's manager: member count and per-member
`(state, head, merged, lag)`; topic head; merge rate; gap events; slot-table version.
The failure modes in §6–7 are only acceptable because they are *visible*; shipping the
mux without lag metrics is shipping the §4.3 provisioning hazard blind.

## 9. Open questions

- **Provenance layout** — §4.2 (a) vs (b); interaction with a future xchannel
  `user_header_kind` variant that could carry provenance natively in the record header.
- **Mux fairness under pathological skew** — is `max_batch_per_member` sufficient, or
  is a deficit-round-robin variant warranted? (Default answer: sufficient; revisit with
  evidence.)
- **Hierarchical topics** — a topic channel can itself be declared `member_of` another
  topic (muxes compose trivially since a topic is a channel). Allowed by construction;
  decide whether to *permit* it in v-topics-1 or gate it until cycle detection exists
  in the registry (a topic that is transitively a member of itself must be rejected).
- **Per-topic promotion policy** — the execution rungs are settled (§4.1: shared-loop
  poll-item → dedicated thread → separate process/standalone binary); open is the
  *trigger*: operator-configured per topic in `TopicOptions`, or automatic based on
  measured merge lag? (Default answer: operator-configured; automation later, with
  evidence.)
- **Standalone mux discovery** — the library-crate engine hosted outside `xchanneld`
  needs member discovery without the registry; the filesystem-watch scheme (channel
  files appearing under a watched dir, headers self-describing) is the candidate.
  Decide whether this ships as a maintained binary or an example.
- **Cross-topic transactional writes** — explicitly out of scope; noted only to record
  that it was considered and rejected (would reintroduce coordination on the data path).

## 10. Prior art

Same lineage as the parent document: the sequencer pattern (LMAX, exchange matching
engines) for "one arbiter, explicit total order"; Aeron's single-publisher streams +
archive for the substrate shape; Kafka topics as the contrast case — equivalent
convenience, with the arbitration performed inside the broker's partition lock,
non-durable and non-replayable. This design's one-sentence justification: *we write
the arbiter's decision down.*
