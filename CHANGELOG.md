# Changelog

All notable changes to xchannel-net are documented here. Versioning is pre-1.0 and
experimental: the wire protocol and on-disk layout may change without notice (see
`SECURITY.md`).

## Unreleased

**Topics — multi-producer fan-in** (`doc/TOPICS.md`): a set of single-writer member channels
merged by a **mux** into one totally-ordered **topic channel** (itself an ordinary xchannel
channel — locally readable, network-replicable), without violating single-writer discipline.

### Added
- **Mux engine**: merges member channels into a topic in arrival order, stamping every record
  with mandatory provenance (`member_ref`, `member_index`, original `user_meta` — 18-byte prefix,
  option (b)); `max_batch_per_member` fairness; per-topic slot-table control records mapping
  `member_ref → (name, epoch)`, re-emitted on membership change and periodically so a recent one
  is always retained.
- **Topic client API**: `create_topic` / `publish_to_topic` (RPC); `TopicOptions` (channel
  geometry + `max_batch_per_member` + `member_reap_after`); topics are read via ordinary
  `subscribe`.
- **Member lifecycle (§6)**: `TopicGap` on retention underrun / fresh-pruned genesis;
  `MemberRegressed` on a source that reset under the same identity; clean-leave drain →
  `MemberClosed`; topic retirement → terminal marker; an opt-in **reaper** (`member_reap_after`)
  that tombstones a member whose owner has been unreachable too long.
- **Remote members**: a member on any node feeds a topic hosted elsewhere, discovered via a
  `member_of` tag on `ChannelIdentity` and replicated to the mux node; re-subscribed on
  reconnect/restart so stale replicas refresh.
- **Registry tombstones + reclaim**: the CRDT merge carries an `(epoch, deleted)` generation —
  deregistration tombstones (a stale `Register` can't resurrect a name), and a reclaim wins at
  `epoch + 1`. Member incarnation is that epoch.
- **`RegisterRejected`**: a create that loses a name collision now fails fast (`AlreadyExists`)
  before any file is created, instead of silently believing it owns the name.
- **Liveness-gated resolution**: `resolve` distinguishes "owner unreachable" (`HostUnreachable`)
  from "channel unknown" (`TimedOut`).
- **True `SubscribeAck.head`**: the source advertises its real high-water index (via
  `xchannel::Reader::head_record_index()`).
- **Restart = reconstruct** (`doc/RESTART.md`): a restarted daemon re-hosts its topics from disk
  (identified by their self-describing slot table, which also carries geometry) and re-registers
  plain origins (geometry via `Reader::region_size()`/`mtu()`), resuming without a client
  re-issuing `create_topic`. Deregistration deletes channel files so a restart can't resurrect a
  retired name.
- **`XCHANNELD_SEEDS`**: configure seed peers (comma-separated control `host:port`) for the
  daemon to form/re-form the mesh.
- **Observability**: `Node::topic_status` — per-member `merged`/`head`/`lag`/`state`/`rejected`,
  topic head, gaps emitted, slot-table version (§8).

### Fixed
- **A resume position past the source's head is refused** (`StreamMsg::Diverged`) instead of
  hanging. After a deregistered name is reclaimed by a new owner, the new origin restarts at
  index 0 while other nodes still hold replicas of the old incarnation; their self-healing
  subscriptions then resumed at an index the new log had never reached. The only guard was
  "behind retention", which such a resume passes by being *greater*, so the origin's `skip_to`
  blocked forever waiting for records that would never be written — both ends wedged, nothing
  reported, the replica still serving old-incarnation data to local clients. Worse, if the new
  log ever grew past that index the sink would resume and splice two unrelated channels into
  one replica, indices lining up and the contiguity check none the wiser. Both resume checks
  now run before the seek. `from == head` (caught up) is unaffected.

### Changed
- Bumped `xchannel` **4.0.0 → 5.0.0**: new generic, topic-agnostic accessors
  (`Reader::head_record_index()`, `region_size()`, `mtu()`, `file_sequence()`),
  `ChannelHeader.generation` (an opaque incarnation id, where the registry's reclaim epoch
  will live), and `format_version = 3` widening `channel_name` from 20 to 48 bytes
  (greenfield — v2 files are refused).
- **Roll boundaries now replicate** (`RecordFrame::starts_segment`), amending the original rule
  that file geometry is purely local. The source detects a roll by sampling
  `Reader::file_sequence()` around each read and flags the record that follows it; the sink rolls
  before applying that record, so a replica is segment-aligned with its origin and not merely
  record-identical. **Why it matters:** `keep_files` prunes by file count, so an origin that rolls
  explicitly (`Writer::roll_file`) with no `file_roll_size` — the shape a snapshot-per-segment
  application wants — previously left its replicas rolling never, growing as one unbounded file
  per channel per node. The hint rides on the record it precedes (nothing to desynchronize, and a
  resuming subscriber re-derives it), stays advisory, and costs one byte per record. The mux
  ignores it: a member's boundaries carry no meaning for a merged topic.

### Notes
- Deliberate deviations from `doc/TOPICS.md`, all documented: the mux runs on its own thread
  (not §4.1's shared forwarding loop); recovery is correct but scans from genesis (not §5.2's
  bounded scan); empty topics aren't re-hosted on restart. The network planes remain
  unauthenticated (trusted-LAN only) — control-plane auth is unchanged Tier-1 work.

## 0.0.1 (2026-06-22)

First tagged release. A decentralized network of node managers (`xchanneld`) that replicate
single-writer xchannel logs across machines, with a flat global registry, peer gossip, and
self-healing subscriptions. Built on xchannel 4.0.0.

### Added
- **Node manager (`xchanneld`)** serving three planes: stream (data replication), control
  (registry gossip + membership heartbeats), and a local client RPC plane.
- **Decentralized registry**: last-writer-wins CRDT `ChannelName → ChannelIdentity`, flat
  global names, first-registrant-wins, converged by eager `RegistryDelta` broadcast +
  join-time anti-entropy.
- **Replication engine**: `ReplicationSource` tails an origin log, `ReplicationSink` rebuilds
  a record-identical replica; absolute `RecordIndex` via xchannel's `base_record_index`.
- **Self-healing subscriptions**: resolve → resume from replica head → stream → reconnect,
  until stopped; a reconnect never re-pulls history already on disk.
- **Replica retention inheritance**: replicas adopt the origin's `file_roll_size`/`keep_files`
  via `SubscribeAck`, so a replica's disk use is bounded whenever its origin's is.
- **Client library** (`xchannel-net-client`): `Client::connect` / `connect_or_spawn`,
  `create_channel`, `subscribe` / `subscribe_path`.
- **Security (Tier-0)**: channel-name allowlist (no traversal / `.replicas` collision),
  absolute daemon-spawn path (no `PATH` injection), lock-poison recovery, `MAX_CONNECTIONS`
  cap, `0700` data dir, 64 MiB frame cap. Full threat model in `SECURITY.md`.
- **Client plane over a Unix domain socket** under the `0700` data dir (created `0600`):
  permission-gated, no loopback port; `bind` arbitrates single-instance startup and reclaims
  stale sockets.
- **Single-daemon-per-`data_dir` guard**: exclusive `flock` on `<data_dir>/.lock`, so a
  second daemon on the same dir exits fast (OS-released on exit — no stale lockfile).

### Notes
- The network planes are unauthenticated plaintext; **trusted-LAN deployment only**.
  Authentication, authorization, and transport encryption are future (Tier-1) work.
