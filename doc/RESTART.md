# xchannel-net — restart = reconstruct (topic re-hosting)

Design note for wiring `DESIGN.md` §5.2 ("restart = reconstruct, never restore from
node-owned metadata") for **topics**. Scope here is topic re-hosting; general origin
re-registration is the broader §5.2 item and is noted where it intersects.

## Principle

The only durable node-owned state is a stable `NodeId` + config (`DESIGN.md` §5). Everything
else is reconstructed on restart from three sources: **(1) a data-dir scan, (2) peer
anti-entropy, (3) clients reconnecting.** A restarted daemon must therefore re-host the topics
it owns without any persisted "topics manifest".

## Problem

`Mux::open` is only called from `create_topic`. After a real restart the daemon comes up with
no muxes, so a pre-existing topic does not resume merging until a client re-issues
`create_topic`. The mux *engine* is already restart-safe (cursor recovery keyed on
`(name, epoch)`, positional — see `mux::recover_cursors`); only the **startup wiring** is missing.

## Mechanism — content-sniff (option a)

Reconstruct topics from the data dir itself, with **no new persisted metadata** (keeps the
layering clean: xchannel stays topic-agnostic; a topic is "just a channel"):

1. **Scan** `data_dir` for channel **directories** (skip `.replicas/` and other dot-entries).
   Each channel owns a directory holding its xchannel segments as `log`, `log.1`, … so the scan
   needs no heuristic: it never has to guess whether `md.aapl.4` is a channel or a rolled
   segment of `md.aapl`, and a channel whose segment 0 has been pruned by retention — the
   *unsuffixed* file, so nothing on disk would otherwise bear its name — still announces itself
   by its directory.
2. **Identify** topics by content: a topic channel carries mux **control records**; a channel
   with a decodable `SlotTable` record is a topic. (A regular channel colliding on `msg_type
   0xFFFF` *and* decoding as a valid slot table is vanishingly unlikely, and re-host of such a
   channel finds no matching member files and stays inert.)
3. **Re-host**: `Mux::open` (recovers per-member cursors from the tail) → register the topic
   channel in the registry + announce it → insert into the mux map.
4. **Re-attach members** named in the topic's most recent slot table: a **local** member by its
   origin log (`data_dir/<name>/log`), a **remote** member by its on-disk replica
   (`data_dir/.replicas/<name>/log`), re-registering it with `member_of = topic` so the normal
   discovery loop re-subscribes it once its owner is reachable.

On a **mesh**, peer anti-entropy also restores `member_of`, so the same discovery loop
re-subscribes remote members; the sniff is what makes an **isolated** single-node restart work.
Neither this nor a header flag (option b) is needed *in addition* on a mesh — peers already carry
the state. (b) — a generic opaque header field xchannel-net interprets — stays a documented
upgrade path if the limitations below ever bite.

## Keeping a slot table retained (correctness, independent of restart)

§6.3 promises a `LateJoin` reader can always decode provenance because the mux re-emits the slot
table on every membership change. But a topic with **stable membership** that rolls + prunes can
prune *all* its slot tables — breaking both a late consumer's `ref → (name, epoch)` decode and
this restart sniff. Fix: the mux **re-emits the slot table every `SLOT_TABLE_REFRESH` merged
records**, bounding staleness so a recent slot table is always within the retained window (given
`keep_files × file_roll_size` exceeds the refresh interval). Small control-record overhead;
required to honor §6.3.

## Limitations (accepted)

- **Empty topics** (created, no member ever attached ⇒ no slot table) are not identifiable and
  are not re-hosted; a reconnecting client re-creates them (the "clients reconnecting" leg).
- **`TopicOptions`** (reap threshold, `max_batch_per_member`) are not persisted; re-host uses
  defaults (reap reverts to *never* — safe; batch to default).
- **Remote members** resume from their on-disk replica and refresh once the owner is reachable
  again (membership rebuilt from heartbeats).
- **General origin re-registration** for non-topic channels **is now implemented** too:
  `reconstruct_from_disk` re-registers every channel it finds, recovering geometry from the
  header via `xchannel::Reader::region_size()` / `mtu()` (added in xchannel 4.2.0 — a generic,
  topic-agnostic accessor, *not* option (b)'s topic marker). `member_of` and rolling/retention
  are not persisted, so a re-registered member reconciles `member_of` via peer anti-entropy on a
  mesh (a local topic re-attaches its members from its own slot table regardless), and replicas
  of a reconstructed origin fall back to no rolling (same as in-process `host_channel`).
- **Option (b)** (a topic marker in the header) was **considered and rejected**: its only real
  benefit is surviving *empty* topics (no data, no members — a reconnecting client re-creates
  them), which isn't worth pushing the topic concept into the substrate. Not planned.

## Remote members on restart

A remote member re-attaches from its **stale on-disk replica**, so `attach_pending_members` must
**(re)start its subscription even when it's already attached** — otherwise the replica never
refreshes and the merge stalls after a restart. Peering is configured with `XCHANNELD_SEEDS`
(comma-separated control `host:port`); the maintenance loop re-dials seeds, so a restarted owner
that seeds to a live member re-learns it and resumes. (A restarted daemon rebinds a *new*
ephemeral port unless a fixed one is configured, so peers that seed *to it* by address must be
pointed at the new port — or have the restarted node seed *to them*.)

## Performance note (follow-up, not correctness)

Reconstruct is O(retained records): `topic_config` scans a channel to find its last slot table,
then `Mux::open`'s `recover_cursors` scans it again for cursors — two full passes per topic — and
each plain origin opens a reader for geometry. Fine for restart (infrequent), but worth folding
into a single scan / bounding later.

## Tests

Cross-process daemon-restart tests: a plain channel re-registers; a topic (1 member and 2
members) re-hosts and resumes without re-issuing `create_topic`; and two real daemons merge a
remote member and **resume across a restart** of the topic-owning node.
