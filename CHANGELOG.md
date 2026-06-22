# Changelog

All notable changes to xchannel-net are documented here. Versioning is pre-1.0 and
experimental: the wire protocol and on-disk layout may change without notice (see
`SECURITY.md`).

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
