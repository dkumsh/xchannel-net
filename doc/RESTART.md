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

1. **Scan** `data_dir` for channel base files (skip `.replicas/`, `.lock`, the client socket).
2. **Identify** topics by content: a topic channel carries mux **control records**; a channel
   with a decodable `SlotTable` record is a topic. (A regular channel colliding on `msg_type
   0xFFFF` *and* decoding as a valid slot table is vanishingly unlikely, and re-host of such a
   channel finds no matching member files and stays inert.)
3. **Re-host**: `Mux::open` (recovers per-member cursors from the tail) → register the topic
   channel in the registry + announce it → insert into the mux map.
4. **Re-attach members** named in the topic's most recent slot table: a **local** member by its
   origin file (`data_dir/<name>`), a **remote** member by its on-disk replica
   (`data_dir/.replicas/<name>`), re-registering it with `member_of = topic` so the normal
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

## Test

A cross-process daemon-restart test: spawn `xchanneld`, create a topic + members, write records,
**kill** the daemon, respawn on the same `data_dir`, and assert the topic re-hosts and resumes
merging **without** any client re-issuing `create_topic`.
