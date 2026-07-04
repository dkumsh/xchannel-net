//! The node manager (`xchanneld`) — hosting, serving, discovery, and subscribing.
//!
//! A [`Node`] ties together the pieces:
//! * **hosting** — [`host_channel`](Node::host_channel) creates an origin under `data_dir`,
//!   registers it, and announces it;
//! * **stream plane** — [`serve_stream`](Node::serve_stream) dispatches inbound
//!   subscriptions to per-connection `StreamServer` threads;
//! * **control plane** — [`serve_control`](Node::serve_control) /
//!   [`connect_control_peer`](Node::connect_control_peer) adopt peer links into
//!   [`BroadcastDissemination`], and [`run_maintenance`](Node::run_maintenance) emits
//!   heartbeats and merges gossiped identities into the [`Registry`];
//! * **subscribing** — [`subscribe`](Node::subscribe) resolves a channel via the registry +
//!   membership, connects to its owner's stream address, and builds a local replica.
//!
//! `Node` is cheaply cloneable (shared interior), so its loops run on their own threads.

use crate::NodeConfig;
use crate::broadcast::BroadcastDissemination;
use crate::registry::Registry;
use crate::util::MutexExt;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use xchannel::{Writer, WriterBuilder};
use xchannel_net_core::RecordIndex;
use xchannel_net_core::codec::{decode_client_request, encode_client_reply};
use xchannel_net_core::dissemination::Dissemination;
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::mux::{self, Mux};
use xchannel_net_core::stream::{self, ChannelSource, accept_subscription};
use xchannel_net_core::transport::{
    Listener, TcpListener, TcpTransport, Transport, UnixListener, UnixTransport,
};
use xchannel_net_core::wire::{ChannelOptions, ClientReply, ClientRequest, TopicOptions};

/// A node not heard from within this is dropped from the live set.
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on concurrent inbound stream + client connections (thread-exhaustion guard). Peer
/// control links are not capped — they come from configured/trusted seeds.
const MAX_CONNECTIONS: usize = 4096;

/// Per-member records merged per mux poll cycle — the fairness bound so one hot member can't
/// monopolize the interleave or head-of-line-block other topics on the shared loop
/// (`doc/TOPICS.md` §4.3).
const MAX_BATCH_PER_MEMBER: usize = 256;

/// Create a directory (and parents) and restrict it to the owner (`0700` on Unix), so
/// other local users can't read channel files beneath it.
fn ensure_private_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Validate a channel name before it is used as a filesystem path component. Allowlist
/// `[A-Za-z0-9._-]`, length 1..=200, and **no leading dot** — which rejects path traversal
/// (`/`, `\`, `..`), the current dir (`.`), and collisions with the internal `.replicas`
/// subtree, none of which can appear.
fn validate_channel_name(name: &str) -> io::Result<()> {
    let valid = (1..=200).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "channel name must be 1..=200 chars of [A-Za-z0-9._-] with no leading dot",
        ))
    }
}

/// RAII token counting one live connection against [`MAX_CONNECTIONS`].
struct ConnGuard(Arc<AtomicUsize>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Map a create-style result (which yields a local path the client opens) into a
/// [`ClientReply`]: the path on success, the error message otherwise.
fn created_or_error(r: io::Result<PathBuf>) -> ClientReply {
    match r {
        Ok(path) => ClientReply::Created {
            path: path.to_string_lossy().into_owned(),
        },
        Err(e) => ClientReply::Error {
            message: e.to_string(),
        },
    }
}

/// How a topic member is currently behaving (§6.1), derived from merge lag + owner liveness.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MemberState {
    /// Owner live and the merge is caught up (no pending records).
    Quiet,
    /// Owner live and the merge is behind (records pending).
    Active,
    /// Owner is not a live member — records aren't flowing (distinct from quiet).
    Unreachable,
}

/// Per-member row of a [`TopicStatus`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MemberInfo {
    pub name: String,
    pub epoch: u64,
    pub merged: u64,
    pub head: u64,
    pub lag: u64,
    /// Records dropped for using a reserved control `msg_type` (contract violations).
    pub rejected: u64,
    pub state: MemberState,
}

/// Observability snapshot of a hosted topic (§8).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopicStatus {
    pub members: Vec<MemberInfo>,
    pub topic_head: u64,
    pub gaps_emitted: u64,
    pub slot_table_version: u64,
}

#[derive(Clone)]
pub struct Node {
    config: Arc<NodeConfig>,
    /// Channels this node hosts (is the origin for): name → where + geometry to serve.
    hosted: Arc<Mutex<HashMap<String, ChannelSource>>>,
    /// Network-wide channel directory (CRDT), converged via dissemination.
    registry: Arc<Mutex<Registry>>,
    dissemination: Arc<Mutex<BroadcastDissemination>>,
    /// Actual bound stream address (set by `bind_stream`), used to resolve self-owned
    /// channels (a node never receives its own heartbeat into membership).
    bound_stream_addr: Arc<Mutex<Option<SocketAddr>>>,
    /// Live replica subscriptions this node maintains for clients, keyed by channel name.
    subscriptions: Arc<Mutex<HashMap<String, Subscription>>>,
    /// Muxes for topics this node owns, keyed by topic name. Each merges its member channels
    /// into the topic channel; polled by the mux loop (`doc/TOPICS.md` §4).
    muxes: Arc<Mutex<HashMap<String, Mux>>>,
    /// Per-topic member-reap threshold (§6.1), for topics that opted in (`member_reap_after`).
    /// Absent ⇒ never reap.
    topic_reap: Arc<Mutex<HashMap<String, Duration>>>,
    /// When a member's owner was first observed unreachable, keyed by `(name, epoch)` — drives
    /// the reaper's "dead beyond a threshold" decision. Keyed by incarnation so two generations
    /// of a name never share a timer. Cleared when the owner is live again.
    member_dead_since: Arc<Mutex<HashMap<(String, u64), Instant>>>,
    /// Count of live inbound stream/client connections (capped at [`MAX_CONNECTIONS`]).
    conns: Arc<AtomicUsize>,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        let dissemination =
            BroadcastDissemination::new(config.node_id, config.stream_addr, LIVENESS_TIMEOUT);
        Self {
            hosted: Arc::new(Mutex::new(HashMap::new())),
            registry: Arc::new(Mutex::new(Registry::new())),
            dissemination: Arc::new(Mutex::new(dissemination)),
            bound_stream_addr: Arc::new(Mutex::new(None)),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            muxes: Arc::new(Mutex::new(HashMap::new())),
            topic_reap: Arc::new(Mutex::new(HashMap::new())),
            member_dead_since: Arc::new(Mutex::new(HashMap::new())),
            conns: Arc::new(AtomicUsize::new(0)),
            config: Arc::new(config),
        }
    }

    /// Acquire a connection slot, or `None` if at [`MAX_CONNECTIONS`].
    fn acquire_conn(&self) -> Option<ConnGuard> {
        if self.conns.fetch_add(1, Ordering::Relaxed) >= MAX_CONNECTIONS {
            self.conns.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(ConnGuard(Arc::clone(&self.conns)))
        }
    }

    // ---------------- hosting ----------------

    /// Host a new origin channel under `data_dir`, register it, announce it, and return
    /// its local `Writer`. Placement + network geometry (`region_size`/`mtu`) are the
    /// daemon's (applied after `configure`, which owns the rest); `base_record_index` is
    /// forced to 0 (genesis).
    pub fn host_channel(
        &self,
        name: &str,
        region_size: u32,
        mtu: u32,
        configure: impl FnOnce(WriterBuilder) -> WriterBuilder,
    ) -> io::Result<Writer> {
        let path = self.channel_path(name)?;
        // Reserve the name first: a lost collision fails here, before any file is created.
        let identity = self.claim_name(name, region_size, mtu, None)?;
        if let Some(parent) = path.parent() {
            ensure_private_dir(parent)?;
        }
        let writer = configure(WriterBuilder::new(&path))
            .region_size(region_size as usize)
            .mtu(mtu as u64)
            .base_record_index(0)
            .build()?;
        // The `configure` closure is opaque, so any `file_roll_size`/`keep_files` it sets
        // can't be read back to advertise in the `SubscribeAck`. We therefore announce
        // `(0, 0)` (no rolling / unlimited) — which matches the WriterBuilder default, so
        // for an unconfigured channel origin and replicas agree. But if the closure *does*
        // set rolling/retention, the origin rolls-and-prunes while its replicas grow
        // unbounded (safe direction: replicas never drop records, only over-retain).
        // Clients that need replicas to inherit disk bounds should use the client RPC
        // (`create_for_client` / `ChannelOptions`), which propagates both fields.
        self.announce_hosted(&identity, path, 0, 0)?;
        Ok(writer)
    }

    /// Create + register an origin on behalf of a client (cross-process): precreate the
    /// channel under `data_dir` with `options` (no live writer kept — the client opens the
    /// single `Writer` itself), register + announce it, and return the path.
    pub fn create_for_client(&self, name: &str, options: ChannelOptions) -> io::Result<PathBuf> {
        self.create_origin(name, options, None)
    }

    /// Shared origin-creation path. `member_of` tags the channel as a topic member so the
    /// topic's owner can discover and attach it (§3.1); `None` for an ordinary channel.
    fn create_origin(
        &self,
        name: &str,
        options: ChannelOptions,
        member_of: Option<String>,
    ) -> io::Result<PathBuf> {
        let path = self.channel_path(name)?;
        // Reserve the name first: a lost collision fails here, before the file is precreated.
        let identity = self.claim_name(name, options.region_size, options.mtu, member_of)?;
        if let Some(parent) = path.parent() {
            ensure_private_dir(parent)?;
        }
        let mut builder = WriterBuilder::new(&path)
            .region_size(options.region_size as usize)
            .mtu(options.mtu as u64)
            .file_roll_size(options.file_roll_size)
            .base_record_index(0);
        if options.keep_files > 0 {
            builder = builder.keep_files(options.keep_files as u64);
        }
        builder.precreate()?; // file + header exist; no writer retained
        self.announce_hosted(
            &identity,
            path.clone(),
            options.file_roll_size,
            options.keep_files,
        )?;
        Ok(path)
    }

    /// Claim `name` for this node via the registry's first-registrant-wins merge. Returns
    /// the locally-owned identity, or `AlreadyExists` if another node's earlier registration
    /// already won the name — the collision notification the caller relays to its client
    /// (DESIGN.md §2.1, `RegisterRejected`). Claiming happens *before* any file is created,
    /// so a rejected registration leaves no orphan origin file behind.
    ///
    /// This detects a collision the local registry already knows about (the common case: the
    /// winner's registration has already reached this node). A cross-node race that only
    /// resolves after this node has already served its client a `Writer` is not covered here
    /// — that requires server-push notification the client RPC does not yet have (tracked as
    /// remaining work).
    fn claim_name(
        &self,
        name: &str,
        region_size: u32,
        mtu: u32,
        member_of: Option<String>,
    ) -> io::Result<ChannelIdentity> {
        // Choose epoch and merge under one lock so the reclaim generation can't shift between
        // reading it and registering. A tombstoned name is reclaimed at the next generation;
        // a live name is contested in its own generation (and lost if we're not the earliest).
        let mut reg = self.registry.lock_safe();
        let identity = ChannelIdentity {
            name: name.to_string(),
            owner: self.config.node_id,
            region_size,
            mtu,
            earliest_index: RecordIndex(0),
            registered_at_nanos: now_nanos(),
            epoch: reg.claim_epoch(name),
            deleted: false,
            member_of,
        };
        let winner = reg.merge(identity.clone());
        drop(reg);
        if winner.owner != self.config.node_id || winner.deleted {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "channel '{name}' already registered by node {} — registration rejected",
                    winner.owner.0
                ),
            ));
        }
        Ok(identity)
    }

    /// Deregister a channel this node owns: tombstone it in the registry, stop hosting it, and
    /// disseminate the tombstone so peers hide it too (and a stale `Register` can't resurrect
    /// it). Returns whether a live channel owned by this node was found to deregister.
    pub fn deregister(&self, name: &str) -> io::Result<bool> {
        let tombstone = self
            .registry
            .lock_safe()
            .deregister(name, self.config.node_id);
        let Some(tombstone) = tombstone else {
            return Ok(false);
        };
        self.hosted.lock_safe().remove(name);
        self.dissemination
            .lock_safe()
            .announce(std::slice::from_ref(&tombstone))?;
        Ok(true)
    }

    // ---------------- topics (multi-producer fan-in, doc/TOPICS.md) ----------------

    /// Create a topic this node owns: the topic channel is an ordinary channel (created +
    /// registered like any origin, so it is discoverable and subscribable), plus a [`Mux`]
    /// that merges its members into it. Returns the topic channel path. The mux is driven by
    /// [`run_mux`](Self::run_mux); members (local or remote) attach via
    /// [`attach_pending_members`](Self::attach_pending_members).
    pub fn create_topic(&self, name: &str, options: TopicOptions) -> io::Result<PathBuf> {
        let path = self.create_for_client(name, options.channel)?;
        let batch = if options.max_batch_per_member == 0 {
            MAX_BATCH_PER_MEMBER
        } else {
            options.max_batch_per_member as usize
        };
        let mux = Mux::open(
            &path,
            options.channel.region_size,
            options.channel.mtu,
            batch,
        )?;
        self.muxes.lock_safe().insert(name.to_string(), mux);
        if options.member_reap_after_ms != 0 {
            self.topic_reap.lock_safe().insert(
                name.to_string(),
                Duration::from_millis(options.member_reap_after_ms),
            );
        }
        Ok(path)
    }

    /// Create a member channel and attach it to a topic's mux. The member is an ordinary
    /// channel (created + registered like any origin; the caller opens its single `Writer` at
    /// the returned path) — "membership" is just its attachment to the mux. Its epoch (registry
    /// generation) is its incarnation, so a respawn is a distinct slot (TOPICS §3.2). Errors if
    /// this node does not own the topic (Phase 1 is local-members-only; remote members are
    /// Phase 2).
    pub fn publish_to_topic(
        &self,
        topic: &str,
        member: &str,
        options: ChannelOptions,
    ) -> io::Result<PathBuf> {
        // The member is an ordinary channel this node owns, tagged `member_of` so the topic's
        // owner discovers it. The topic may be owned by *any* node (§3.1): if we host it,
        // attach immediately; otherwise the owner attaches via `attach_pending_members` once
        // the `member_of` registration gossips to it. `add_member` is idempotent.
        let member_path = self.create_origin(member, options, Some(topic.to_string()))?;
        if self.muxes.lock_safe().contains_key(topic) {
            let epoch = self
                .registry
                .lock_safe()
                .get_raw(member)
                .map(|id| id.epoch)
                .unwrap_or(0);
            if let Some(mux) = self.muxes.lock_safe().get_mut(topic) {
                mux.add_member(member, epoch, &member_path)?;
            }
        }
        Ok(member_path)
    }

    /// Attach any not-yet-attached members of the topics this node hosts, discovered from the
    /// registry via `member_of` (§4.1). A **local** member is read straight from its origin
    /// file; a **remote** member is replicated here first (a stream subscription builds a local
    /// replica) and the mux then reads that replica identically. Idempotent and best-effort:
    /// a member whose replica isn't ready yet is retried on the next call. Runs on the
    /// maintenance loop, so it reacts as `member_of` registrations gossip in.
    pub fn attach_pending_members(&self) {
        let topics: Vec<String> = self.muxes.lock_safe().keys().cloned().collect();
        for topic in topics {
            // Members the registry says belong to this topic right now (live, non-tombstoned).
            let live: Vec<ChannelIdentity> = {
                let reg = self.registry.lock_safe();
                reg.iter()
                    .filter(|id| !id.deleted && id.member_of.as_deref() == Some(topic.as_str()))
                    .cloned()
                    .collect()
            };

            // Attach any not-yet-attached live member.
            for m in &live {
                let already = self
                    .muxes
                    .lock_safe()
                    .get(&topic)
                    .is_none_or(|mx| mx.has_member(&m.name, m.epoch));
                if already {
                    continue;
                }
                // Resolve the path the mux reads: a local origin, or a locally-synced replica
                // of a remote member (skip until the replica exists — retried next cycle).
                let path = if m.owner == self.config.node_id {
                    match self.channel_path(&m.name) {
                        Ok(p) => p,
                        Err(_) => continue,
                    }
                } else {
                    self.ensure_member_subscription(&m.name);
                    match self.replica_path(&m.name) {
                        Ok(p) if p.exists() => p,
                        _ => continue,
                    }
                };
                if let Some(mux) = self.muxes.lock_safe().get_mut(&topic) {
                    let _ = mux.add_member(&m.name, m.epoch, &path);
                }
            }

            // Detach members the registry no longer lists (tombstoned / left): clean-leave
            // drain → MemberClosed, then stop replicating a remote one (§6.1).
            let live_set: std::collections::HashSet<(String, u64)> =
                live.iter().map(|m| (m.name.clone(), m.epoch)).collect();
            let attached: Vec<(String, u64)> = self
                .muxes
                .lock_safe()
                .get(&topic)
                .map(|mx| mx.members().into_iter().map(|(n, e, _)| (n, e)).collect())
                .unwrap_or_default();
            for (name, epoch) in attached {
                if live_set.contains(&(name.clone(), epoch)) {
                    continue;
                }
                if let Some(mux) = self.muxes.lock_safe().get_mut(&topic) {
                    let _ = mux.remove_member(&name, epoch);
                }
                if let Some(sub) = self.subscriptions.lock_safe().remove(&name) {
                    sub.stop();
                }
            }
        }
    }

    /// Reap members whose owner has been an unreachable member beyond the topic's
    /// `member_reap_after` threshold (§6.1): tombstone them so a respawn can reclaim the name at
    /// a new incarnation. Opt-in per topic (default never). The tombstone is disseminated;
    /// [`attach_pending_members`](Self::attach_pending_members) then drains+detaches the slot.
    pub fn reap_dead_members(&self) {
        let topics: Vec<(String, Duration)> = self
            .topic_reap
            .lock_safe()
            .iter()
            .map(|(t, d)| (t.clone(), *d))
            .collect();
        for (topic, reap_after) in topics {
            let members: Vec<ChannelIdentity> = {
                let reg = self.registry.lock_safe();
                reg.iter()
                    .filter(|id| !id.deleted && id.member_of.as_deref() == Some(topic.as_str()))
                    .cloned()
                    .collect()
            };
            for m in members {
                let owner_live = m.owner == self.config.node_id
                    || self
                        .dissemination
                        .lock_safe()
                        .live_addr_of(m.owner)
                        .is_some();
                let key = (m.name.clone(), m.epoch);
                if owner_live {
                    self.member_dead_since.lock_safe().remove(&key);
                    continue;
                }
                let dead_for = {
                    let mut dead = self.member_dead_since.lock_safe();
                    let since = *dead.entry(key.clone()).or_insert_with(Instant::now);
                    since.elapsed()
                };
                if dead_for >= reap_after {
                    self.member_dead_since.lock_safe().remove(&key);
                    if let Some(tombstone) = self.registry.lock_safe().reap(&m.name) {
                        let _ = self
                            .dissemination
                            .lock_safe()
                            .announce(std::slice::from_ref(&tombstone));
                    }
                }
            }
        }
    }

    /// Ensure a self-healing subscription is replicating remote member `name` locally, so its
    /// replica can feed the mux. Reuses the subscription map (idempotent); a short resolve
    /// timeout keeps the maintenance loop responsive if the owner isn't reachable yet.
    fn ensure_member_subscription(&self, name: &str) {
        let live = self
            .subscriptions
            .lock_safe()
            .get(name)
            .is_some_and(|s| s.is_active());
        if live {
            return;
        }
        if let Ok(sub) = self.subscribe(name, Some(Duration::from_millis(200))) {
            self.subscriptions.lock_safe().insert(name.to_string(), sub);
        }
    }

    /// Merge whatever is ready across every hosted topic. Returns the total records merged.
    pub fn poll_muxes(&self) -> io::Result<usize> {
        let mut muxes = self.muxes.lock_safe();
        let mut total = 0;
        for mux in muxes.values_mut() {
            total += mux.poll()?;
        }
        Ok(total)
    }

    /// Drive the muxes forever: poll all hosted topics every `interval`. (Phase 1 runs this on
    /// its own thread; §4.1's shared-loop integration and per-topic promotion are later.)
    pub fn run_mux(&self, interval: Duration) {
        loop {
            let _ = self.poll_muxes();
            std::thread::sleep(interval);
        }
    }

    /// Number of members currently attached to a hosted topic's mux (for tests/observability).
    pub fn topic_member_count(&self, topic: &str) -> Option<usize> {
        self.muxes.lock_safe().get(topic).map(|m| m.members().len())
    }

    /// Restart reconstruction (`DESIGN.md` §5.2, `doc/RESTART.md`): scan `data_dir`, re-host every
    /// topic found on disk, and re-attach its members — with no persisted marker. A topic is
    /// identified by content (a decodable slot table, via `mux::topic_config`) and its geometry +
    /// membership come from that self-describing slot table. Call once at startup. Best-effort:
    /// a channel that fails to re-host is skipped (a client re-issuing `create_topic` recovers it).
    pub fn reconstruct_from_disk(&self) {
        let Ok(rd) = std::fs::read_dir(&self.config.data_dir) else {
            return;
        };
        let names: Vec<String> = rd
            .flatten()
            .filter(|e| e.path().is_file())
            .filter_map(|e| e.file_name().to_str().map(String::from))
            .collect();
        let present: std::collections::HashSet<&str> = names.iter().map(String::as_str).collect();
        for name in &names {
            if name.starts_with('.') {
                continue; // .lock and other dotfiles
            }
            // Skip a rolled segment "<base>.<n>" whose base channel is also present.
            if let Some((base, suffix)) = name.rsplit_once('.')
                && suffix.parse::<u64>().is_ok()
                && present.contains(base)
            {
                continue;
            }
            if validate_channel_name(name).is_err() {
                continue;
            }
            let path = self.config.data_dir.join(name);
            match mux::topic_config(&path) {
                Ok(Some(cfg)) => {
                    let _ = self.rehost_topic(name, &cfg);
                }
                // A plain origin (or a topic member): re-register it so it's discoverable and
                // subscribable again (§5.2). `Err`/`Ok(None)` from a non-channel file is ignored.
                Ok(None) => {
                    let _ = self.reregister_origin(name);
                }
                Err(_) => {}
            }
        }
    }

    /// Re-register a non-topic origin channel found on disk (helper for
    /// [`reconstruct_from_disk`]): recover its geometry from the channel header via
    /// `xchannel::Reader` and re-register + announce it under this node's ownership. `member_of`
    /// is not recoverable from disk (it was registry state); on a mesh, peer anti-entropy
    /// restores it, and a local topic re-attaches its members from its own slot table regardless.
    /// Rolling/retention policy is likewise not persisted, so replicas of a reconstructed origin
    /// fall back to no rolling (same as an in-process `host_channel`).
    fn reregister_origin(&self, name: &str) -> io::Result<()> {
        if self.hosted.lock_safe().contains_key(name) {
            return Ok(());
        }
        let path = self.channel_path(name)?;
        let reader = xchannel::ReaderBuilder::new(&path).late_join().build()?;
        let region_size = reader.region_size() as u32;
        let mtu = reader.mtu();
        drop(reader);
        let identity = self.claim_name(name, region_size, mtu, None)?;
        self.announce_hosted(&identity, path, 0, 0)
    }

    /// Re-host one topic from disk (helper for [`reconstruct_from_disk`]): re-register the topic
    /// channel we own, reopen its mux (which recovers per-member cursors from the tail), and
    /// re-attach the members named in its last slot table — a local member by its origin file, a
    /// remote member by its on-disk replica (refreshed later when its owner is reachable and the
    /// discovery loop re-subscribes). Members are not re-registered here (that is general origin
    /// reconstruction, §5.2); the mux reads their files directly.
    fn rehost_topic(&self, name: &str, cfg: &mux::TopicConfig) -> io::Result<()> {
        if self.muxes.lock_safe().contains_key(name) {
            return Ok(());
        }
        let path = self.channel_path(name)?;
        let identity = self.claim_name(name, cfg.region_size, cfg.mtu, None)?;
        self.announce_hosted(&identity, path.clone(), 0, 0)?;
        let mux = Mux::open(&path, cfg.region_size, cfg.mtu, MAX_BATCH_PER_MEMBER)?;
        self.muxes.lock_safe().insert(name.to_string(), mux);
        for (member, epoch) in &cfg.members {
            // Prefer a local origin, fall back to a remote replica; add_member's source open
            // fails harmlessly if a candidate file isn't present.
            for cand in [self.channel_path(member), self.replica_path(member)]
                .into_iter()
                .flatten()
            {
                let attached = self
                    .muxes
                    .lock_safe()
                    .get_mut(name)
                    .map(|mx| mx.add_member(member, *epoch, &cand).is_ok())
                    .unwrap_or(false);
                if attached {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Observability snapshot for a hosted topic (§8): the mux's merge health augmented with
    /// each member's owner liveness, so each member reads as `Active` (behind, owner live),
    /// `Quiet` (caught up, owner live), or `Unreachable` (owner not a live member). `None` if
    /// this node doesn't host the topic.
    pub fn topic_status(&self, topic: &str) -> Option<io::Result<TopicStatus>> {
        let mux_status = self.muxes.lock_safe().get(topic)?.status();
        let status = mux_status.map(|s| {
            let members = s
                .members
                .into_iter()
                .map(|m| {
                    let owner = self
                        .registry
                        .lock_safe()
                        .get_raw(&m.name)
                        .map(|id| id.owner);
                    let owner_live = match owner {
                        Some(o) => {
                            o == self.config.node_id
                                || self.dissemination.lock_safe().live_addr_of(o).is_some()
                        }
                        None => false,
                    };
                    let state = if !owner_live {
                        MemberState::Unreachable
                    } else if m.lag == 0 {
                        MemberState::Quiet
                    } else {
                        MemberState::Active
                    };
                    MemberInfo {
                        name: m.name,
                        epoch: m.epoch,
                        merged: m.merged,
                        head: m.head,
                        lag: m.lag,
                        rejected: m.rejected,
                        state,
                    }
                })
                .collect();
            TopicStatus {
                members,
                topic_head: s.topic_head,
                gaps_emitted: s.gaps_emitted,
                slot_table_version: s.slot_table_version,
            }
        });
        Some(status)
    }

    /// Retire a topic this node owns (§4.1): drain and close every member (each `MemberClosed`),
    /// commit a terminal marker, stop replicating remote members, then tombstone + announce the
    /// topic channel so peers hide it. Returns whether a hosted topic was found. Members keep
    /// their own channels; only the merge stops.
    pub fn deregister_topic(&self, topic: &str) -> io::Result<bool> {
        let mux = self.muxes.lock_safe().remove(topic);
        let Some(mut mux) = mux else {
            return Ok(false);
        };
        mux.finish()?; // drain all members + terminal marker
        drop(mux);
        self.topic_reap.lock_safe().remove(topic);

        // Stop replicating this topic's remote members.
        let members: Vec<String> = {
            let reg = self.registry.lock_safe();
            reg.iter()
                .filter(|id| id.member_of.as_deref() == Some(topic))
                .map(|id| id.name.clone())
                .collect()
        };
        for m in members {
            if let Some(sub) = self.subscriptions.lock_safe().remove(&m) {
                sub.stop();
            }
        }

        // Tombstone the topic channel itself so it's no longer discoverable.
        if let Some(tombstone) = self
            .registry
            .lock_safe()
            .deregister(topic, self.config.node_id)
        {
            self.hosted.lock_safe().remove(topic);
            self.dissemination
                .lock_safe()
                .announce(std::slice::from_ref(&tombstone))?;
        }
        Ok(true)
    }

    /// Announce a freshly-claimed origin to peers and record it in the hosted map (so
    /// `serve_stream` can resolve it). `file_roll_size`/`keep_files` are the origin's
    /// rolling+retention policy, carried in the hosted `ChannelSource` so subscribers'
    /// replicas inherit the same disk bounds via `SubscribeAck`.
    fn announce_hosted(
        &self,
        identity: &ChannelIdentity,
        path: PathBuf,
        file_roll_size: u64,
        keep_files: u32,
    ) -> io::Result<()> {
        self.dissemination
            .lock_safe()
            .announce(std::slice::from_ref(identity))?;
        self.hosted.lock_safe().insert(
            identity.name.clone(),
            ChannelSource {
                path,
                region_size: identity.region_size,
                mtu: identity.mtu,
                file_roll_size,
                keep_files,
            },
        );
        Ok(())
    }

    /// Path of an **origin** channel this node hosts: `data_dir/<name>`.
    fn channel_path(&self, name: &str) -> io::Result<PathBuf> {
        validate_channel_name(name)?;
        Ok(self.config.data_dir.join(name))
    }

    /// Path of a **replica** this node maintains: `data_dir/.replicas/<name>`. Kept in a
    /// separate subtree so a replica never collides with a same-named origin (notably for a
    /// node subscribing to a channel it also hosts).
    fn replica_path(&self, name: &str) -> io::Result<PathBuf> {
        self.channel_path(name)?; // validate the name
        Ok(self.config.data_dir.join(".replicas").join(name))
    }

    // ---------------- stream plane (serve) ----------------

    /// Bind the stream-plane listener and advertise its real address in heartbeats.
    pub fn bind_stream(&self) -> io::Result<TcpListener> {
        let listener = TcpListener::bind(self.config.stream_addr)?;
        let addr = listener.local_addr()?;
        self.dissemination.lock_safe().set_self_addr(addr);
        *self.bound_stream_addr.lock_safe() = Some(addr);
        Ok(listener)
    }

    /// Accept stream connections forever, dispatching each to its own thread serving one
    /// subscription against this node's hosted channels.
    pub fn serve_stream(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let Some(guard) = self.acquire_conn() else {
                continue; // at capacity — drop the connection
            };
            let hosted = Arc::clone(&self.hosted);
            std::thread::spawn(move || {
                let _guard = guard; // released when this connection's thread ends
                let resolve = |name: &str| hosted.lock_safe().get(name).cloned();
                if let Ok(mut server) = accept_subscription(conn, resolve) {
                    let _ = server.run();
                }
            });
        }
    }

    // ---------------- control plane (gossip) ----------------

    /// Bind the control-plane listener.
    pub fn bind_control(&self) -> io::Result<TcpListener> {
        TcpListener::bind(self.config.control_addr)
    }

    /// Accept peer control connections forever, adopting each as a dissemination peer
    /// (which sends our current registry as join-time anti-entropy + a heartbeat).
    pub fn serve_control(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let snapshot = self.registry_snapshot();
            let _ = self.dissemination.lock_safe().add_peer(conn, &snapshot);
        }
    }

    /// Connect to a peer's control address and adopt it as an outbound dissemination peer
    /// (deduped: a no-op if already connected). The connect happens outside the
    /// dissemination lock so a slow dial doesn't stall heartbeats/announces.
    pub fn connect_control_peer(&self, addr: SocketAddr) -> io::Result<()> {
        if self.dissemination.lock_safe().is_connected(addr) {
            return Ok(());
        }
        let conn = TcpTransport::connect(addr)?;
        let snapshot = self.registry_snapshot();
        self.dissemination
            .lock_safe()
            .add_outbound_peer(conn, addr, &snapshot)
    }

    /// (Re)connect to any configured seed peer not currently linked. Called at startup and
    /// each maintenance tick, so a dropped seed link is re-established. Uses a bounded dial
    /// timeout so a down seed doesn't stall the loop.
    pub fn connect_seeds(&self) {
        for addr in self.config.seeds.clone() {
            if self.dissemination.lock_safe().is_connected(addr) {
                continue;
            }
            if let Ok(conn) = TcpTransport::connect_timeout(&addr, Duration::from_secs(1)) {
                let snapshot = self.registry_snapshot();
                let _ = self
                    .dissemination
                    .lock_safe()
                    .add_outbound_peer(conn, addr, &snapshot);
            }
        }
    }

    fn registry_snapshot(&self) -> Vec<ChannelIdentity> {
        self.registry.lock_safe().iter().cloned().collect()
    }

    /// Periodic maintenance: reconnect dropped seeds, emit a heartbeat, and merge gossiped
    /// identities into the registry. Runs forever; the caller drives it on its own thread.
    pub fn run_maintenance(&self, interval: Duration) -> io::Result<()> {
        loop {
            self.connect_seeds();
            let pumped = {
                let mut d = self.dissemination.lock_safe();
                let _ = d.emit_heartbeat();
                d.pump()?
            };
            if !pumped.is_empty() {
                let mut reg = self.registry.lock_safe();
                for id in pumped {
                    reg.merge(id);
                }
            }
            // Retire members whose owner has been dead too long (opt-in), then react to
            // `member_of` registrations: attach live members, detach reaped/tombstoned ones.
            self.reap_dead_members();
            self.attach_pending_members();
            std::thread::sleep(interval);
        }
    }

    // ---------------- client plane (local client RPC) ----------------

    /// Bind the client-plane listener — a Unix domain socket at `client_path`. Who may drive
    /// the daemon is governed by filesystem permissions: the socket sits under the `0700`
    /// data dir and is itself created `0600`, so only the owner can connect (no loopback port
    /// any local process could reach). The bind also arbitrates single-instance startup: if a
    /// live daemon already owns the path the bind fails with `AddrInUse` (the loser of a
    /// `connect_or_spawn` race), while a stale socket left by a crashed daemon is detected
    /// (nothing answers a probe connect) and reclaimed.
    pub fn bind_client(&self) -> io::Result<UnixListener> {
        let path = &self.config.client_path;
        if let Some(parent) = path.parent() {
            ensure_private_dir(parent)?;
        }
        let listener = match UnixListener::bind(path) {
            Ok(l) => l,
            Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                // The path is taken. A live daemon answering a probe connect means we lost
                // the race; otherwise it's a stale socket from a crash — remove and rebind.
                if UnixTransport::connect(path).is_ok() {
                    return Err(e);
                }
                std::fs::remove_file(path)?;
                UnixListener::bind(path)?
            }
            Err(e) => return Err(e),
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(listener)
    }

    /// Accept local client connections forever, handling each on its own thread.
    pub fn serve_client(&self, mut listener: UnixListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let Some(guard) = self.acquire_conn() else {
                continue; // at capacity — drop the connection
            };
            let node = self.clone();
            std::thread::spawn(move || {
                let _guard = guard;
                node.handle_client(conn);
            });
        }
    }

    /// Serve a client connection: one request → one reply, until it disconnects.
    fn handle_client<T: Transport>(&self, mut conn: T) {
        while let Ok(bytes) = conn.recv_frame() {
            let reply = match decode_client_request(&bytes) {
                Ok(req) => self.handle_request(req),
                Err(e) => ClientReply::Error {
                    message: e.to_string(),
                },
            };
            if conn.send_frame(&encode_client_reply(&reply)).is_err() {
                break;
            }
        }
    }

    fn handle_request(&self, req: ClientRequest) -> ClientReply {
        match req {
            ClientRequest::Create { name, options } => {
                created_or_error(self.create_for_client(&name, options))
            }
            ClientRequest::Subscribe { name, wait_ms } => {
                // Idempotent: reuse a live subscription for this channel.
                if let Some(existing) = self.subscriptions.lock_safe().get(&name)
                    && existing.is_active()
                {
                    return ClientReply::Subscribed {
                        replica_path: existing.replica_path().to_string_lossy().into_owned(),
                    };
                }
                let wait = (wait_ms != 0).then(|| Duration::from_millis(wait_ms));
                match self.subscribe(&name, wait) {
                    Ok(sub) => {
                        let replica_path = sub.replica_path().to_string_lossy().into_owned();
                        self.subscriptions.lock_safe().insert(name, sub);
                        ClientReply::Subscribed { replica_path }
                    }
                    Err(e) => ClientReply::Error {
                        message: e.to_string(),
                    },
                }
            }
            ClientRequest::CreateTopic { name, options } => {
                created_or_error(self.create_topic(&name, options))
            }
            ClientRequest::PublishToTopic {
                topic,
                member,
                options,
            } => created_or_error(self.publish_to_topic(&topic, &member, options)),
        }
    }

    /// Sync progress of a subscription this node maintains (for clients), if any.
    pub fn subscription_synced(&self, name: &str) -> Option<u64> {
        self.subscriptions
            .lock_safe()
            .get(name)
            .map(|s| s.synced_index())
    }

    // ---------------- subscribing ----------------

    /// Subscribe to a channel by name and maintain a local replica under `data_dir` in a
    /// background thread. Returns a [`Subscription`] tracking sync progress + the replica
    /// path (a local reader client opens that path, in its own process).
    ///
    /// The background loop is **self-healing**: it resolves the owner, **resumes** from the
    /// replica's current head (so a reconnect or restart never re-pulls history it already
    /// has), streams until the connection drops, then **reconnects** — until [`stop`](
    /// Subscription::stop). `resolve_timeout` bounds only the *initial* resolution (so the
    /// RPC fails fast if the channel is unknown): `None` blocks, `Some(d)` errors after `d`.
    pub fn subscribe(
        &self,
        name: &str,
        resolve_timeout: Option<Duration>,
    ) -> io::Result<Subscription> {
        // Fail fast if the channel can't be resolved within the timeout.
        self.resolve(name, resolve_timeout)?;
        let replica_path = self.replica_path(name)?;
        if let Some(parent) = replica_path.parent() {
            ensure_private_dir(parent)?;
        }

        let stopped = Arc::new(AtomicBool::new(false));
        let synced = Arc::new(AtomicU64::new(0));
        let shutdown: Arc<Mutex<Option<TcpTransport>>> = Arc::new(Mutex::new(None));

        let node = self.clone();
        let (name_t, path_t, stopped_t, synced_t, shutdown_t) = (
            name.to_string(),
            replica_path.clone(),
            Arc::clone(&stopped),
            Arc::clone(&synced),
            Arc::clone(&shutdown),
        );
        let handle = std::thread::spawn(move || {
            node.run_subscription(name_t, path_t, stopped_t, synced_t, shutdown_t)
        });

        Ok(Subscription {
            replica_path,
            synced,
            stopped,
            shutdown,
            handle: Some(handle),
        })
    }

    /// The self-healing subscription loop: resolve → resume from replica head → stream →
    /// reconnect, until stopped. Failures back off and retry; `stop` interrupts a blocked
    /// read by shutting down the live socket.
    fn run_subscription(
        &self,
        name: String,
        replica_path: PathBuf,
        stopped: Arc<AtomicBool>,
        synced: Arc<AtomicU64>,
        shutdown: Arc<Mutex<Option<TcpTransport>>>,
    ) {
        const BACKOFF: Duration = Duration::from_millis(100);
        while !stopped.load(Ordering::Relaxed) {
            // Re-resolve each attempt (owner address may have changed); short timeout so we
            // keep re-checking `stopped`.
            let Ok((id, addr)) = self.resolve(&name, Some(Duration::from_millis(200))) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            // Resume from the replica's current head (0 if it doesn't exist yet).
            let from = self
                .replica_head(&replica_path, id.region_size)
                .unwrap_or(RecordIndex(0));
            synced.store(from.0, Ordering::Relaxed);

            let Ok(conn) = TcpTransport::connect(addr) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            let shutdown_handle = conn.try_clone().ok();
            let Ok(mut client) = stream::subscribe(conn, &name, from, &replica_path) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            *shutdown.lock_safe() = shutdown_handle;

            // Apply records until the connection drops or we're stopped.
            loop {
                if stopped.load(Ordering::Relaxed) {
                    return;
                }
                match client.recv_one() {
                    Ok(()) => synced.store(client.expected_index().0, Ordering::Relaxed),
                    Err(_) => break, // disconnected → reconnect (resuming from the new head)
                }
            }
            *shutdown.lock_safe() = None;
            if stopped.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(BACKOFF);
        }
    }

    /// Absolute head index of an existing replica (so a subscription resumes from there),
    /// or 0 if the replica doesn't exist yet. Reopens the channel briefly to read its head;
    /// `region_size` must match the on-disk geometry (taken from the registry identity).
    fn replica_head(&self, replica_path: &Path, region_size: u32) -> io::Result<RecordIndex> {
        if !replica_path.exists() {
            return Ok(RecordIndex(0));
        }
        let writer = WriterBuilder::new(replica_path)
            .region_size(region_size as usize)
            .build()?;
        Ok(RecordIndex(writer.next_record_index()))
    }

    /// Stop and forget a subscription this node maintains for a client. Returns whether one
    /// was found.
    pub fn unsubscribe(&self, name: &str) -> bool {
        if let Some(sub) = self.subscriptions.lock_safe().remove(name) {
            sub.stop();
            true
        } else {
            false
        }
    }

    /// Block (until `timeout`) until `name` is in the registry and its owner is a **live**
    /// member whose stream address is known. Requiring liveness (not just a last-known
    /// address) lets us distinguish, on timeout, two states DESIGN.md §5.4 keeps separate:
    /// `TimedOut` = the channel is unknown here, vs `HostUnreachable` = the channel is known
    /// but its owner is not a live member (owner unreachable) — the signal the mux needs to
    /// drive drain/stall policy rather than dial a stale address.
    fn resolve(
        &self,
        name: &str,
        timeout: Option<Duration>,
    ) -> io::Result<(ChannelIdentity, SocketAddr)> {
        let deadline = timeout.map(|t| Instant::now() + t);
        let mut owner_unreachable = false;
        loop {
            let identity = self.registry.lock_safe().get(name).cloned();
            if let Some(identity) = identity {
                // Self-owned channels resolve to our own (bound) stream address — a node
                // never records its own heartbeat into membership, and is trivially live.
                let addr = if identity.owner == self.config.node_id {
                    *self.bound_stream_addr.lock_safe()
                } else {
                    let a = self.dissemination.lock_safe().live_addr_of(identity.owner);
                    // Known channel but the owner is not a live member: unreachable, not
                    // unknown. Keep retrying (it may recover) but remember the distinction.
                    owner_unreachable = a.is_none();
                    a
                };
                if let Some(addr) = addr {
                    return Ok((identity, addr));
                }
            }
            if let Some(dl) = deadline
                && Instant::now() >= dl
            {
                return Err(if owner_unreachable {
                    io::Error::new(
                        io::ErrorKind::HostUnreachable,
                        format!("channel '{name}' known but its owner is not a live member"),
                    )
                } else {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("channel '{name}' not resolvable within timeout"),
                    )
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Handle to a self-healing subscription replicating a remote channel locally. Dropping it
/// stops the background loop.
pub struct Subscription {
    replica_path: PathBuf,
    synced: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
    /// The currently-live connection (if any), so [`stop`](Self::stop) can interrupt a
    /// blocked read by shutting it down.
    shutdown: Arc<Mutex<Option<TcpTransport>>>,
    handle: Option<JoinHandle<()>>,
}

impl Subscription {
    /// Local path of the replica; a reader client opens this (in its own process).
    pub fn replica_path(&self) -> &Path {
        &self.replica_path
    }

    /// Absolute index the replica has been synced to (the head). Grows as records arrive.
    pub fn synced_index(&self) -> u64 {
        self.synced.load(Ordering::Relaxed)
    }

    /// Whether the background loop is still running (not stopped).
    pub fn is_active(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }

    /// Stop the background loop: set the flag and shut down the live socket so a blocked
    /// read returns. Idempotent.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(conn) = self.shutdown.lock_safe().as_ref() {
            let _ = conn.shutdown();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.stop();
        // Best-effort join so the replica writer is released before we return.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xchannel::{ReaderBuilder, ReaderMode};
    use xchannel_net_core::NodeId;
    use xchannel_net_core::transport::TcpTransport;

    fn temp_dir(name: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("xchnet-node-{name}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn config(id: u64, data_dir: PathBuf) -> NodeConfig {
        let client_path = data_dir.join("client.sock");
        NodeConfig {
            node_id: NodeId(id),
            data_dir,
            control_addr: "127.0.0.1:0".parse().unwrap(),
            stream_addr: "127.0.0.1:0".parse().unwrap(),
            client_path,
            seeds: vec![],
        }
    }

    /// Start a node: bind both listeners and spawn serve_stream / serve_control /
    /// maintenance. Returns the node and its (stream_addr, control_addr).
    fn start(id: u64, dir: &str) -> (Node, SocketAddr, SocketAddr) {
        let node = Node::new(config(id, temp_dir(dir)));
        let stream_l = node.bind_stream().unwrap();
        let control_l = node.bind_control().unwrap();
        let stream_addr = stream_l.local_addr().unwrap();
        let control_addr = control_l.local_addr().unwrap();
        for (node, run) in [
            (node.clone(), Run::Stream(stream_l)),
            (node.clone(), Run::Control(control_l)),
        ] {
            std::thread::spawn(move || match run {
                Run::Stream(l) => {
                    let _ = node.serve_stream(l);
                }
                Run::Control(l) => {
                    let _ = node.serve_control(l);
                }
            });
        }
        let m = node.clone();
        std::thread::spawn(move || {
            let _ = m.run_maintenance(Duration::from_millis(5));
        });
        (node, stream_addr, control_addr)
    }

    enum Run {
        Stream(TcpListener),
        Control(TcpListener),
    }

    fn poll_until<R>(mut f: impl FnMut() -> Option<R>) -> R {
        for _ in 0..2000 {
            if let Some(r) = f() {
                return r;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("condition not met within timeout");
    }

    #[test]
    fn two_nodes_discover_and_replicate() {
        let (a, _a_stream, a_control) = start(1, "two-a");
        let (b, _b_stream, _b_control) = start(2, "two-b");
        let n = 40u64;

        // A hosts a channel and writes records, then drops the writer (so A's
        // ReplicationSource, opened when B connects, isn't concurrent with the writer in
        // this single process). Hosting before B links means B learns the channel via
        // A's join-time RegistrySync.
        {
            let mut w = a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            for i in 0..n {
                let p = format!("tick-{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }

        // B links to A's control plane → B receives A's registry (incl. md.aapl) and
        // learns A's stream address via heartbeat.
        b.connect_control_peer(a_control).unwrap();

        // B subscribes: resolves md.aapl → A's stream addr → builds a replica.
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();

        // The replica syncs all records purely through the two managers.
        poll_until(|| (sub.synced_index() == n).then_some(()));
        assert_eq!(sub.synced_index(), n);
    }

    #[test]
    fn subscription_stops_cleanly() {
        let (a, _a_stream, a_control) = start(11, "stop-a");
        let (b, _b_stream, _b_control) = start(12, "stop-b");
        let n = 10u64;
        {
            let mut w = a.host_channel("c", 1 << 20, 0, |x| x).unwrap();
            for i in 0..n {
                let p = format!("r{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }
        b.connect_control_peer(a_control).unwrap();

        let sub = b.subscribe("c", Some(Duration::from_secs(5))).unwrap();
        poll_until(|| (sub.synced_index() == n).then_some(()));
        assert!(sub.is_active());

        sub.stop();
        assert!(!sub.is_active());
        assert_eq!(sub.synced_index(), n, "sync frozen after stop");
        // Dropping `sub` joins the background thread; must not hang.
    }

    #[test]
    fn daemon_serves_a_hosted_channel_over_tcp() {
        let node = Node::new(config(1, temp_dir("serve")));
        let n = 25u64;
        let listener = node.bind_stream().unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = node.clone();
        std::thread::spawn(move || {
            let _ = serving.serve_stream(listener);
        });

        {
            let mut w = node.host_channel("md.aapl", 1 << 20, 0, |b| b).unwrap();
            for i in 0..n {
                let p = format!("v{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }

        let replica = temp_dir("serve-replica").join("chan");
        let conn = TcpTransport::connect(addr).unwrap();
        let mut client = stream::subscribe(conn, "md.aapl", RecordIndex(0), &replica).unwrap();
        for _ in 0..n {
            client.recv_one().unwrap();
        }
        assert_eq!(client.expected_index(), RecordIndex(n));
        drop(client);

        let mut r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let mut seen = 0u64;
        while let Some(m) = r.try_read().unwrap() {
            assert_eq!(m.header().user_meta_u64, seen);
            assert_eq!(m.payload(), format!("v{seen}").as_bytes());
            seen += 1;
        }
        assert_eq!(seen, n);
    }

    #[test]
    fn rejects_unsafe_channel_names() {
        // Path traversal, current-dir, separators, leading dot (incl. the .replicas
        // subtree), empty, and over-length names are all rejected before touching the FS.
        let long = "x".repeat(201);
        for bad in [
            "a/b",
            "..",
            ".",
            "../etc",
            "a\\b",
            ".hidden",
            ".replicas",
            "",
            long.as_str(),
        ] {
            assert_eq!(
                validate_channel_name(bad).unwrap_err().kind(),
                io::ErrorKind::InvalidInput,
                "should reject {bad:?}"
            );
        }
        // Reasonable names pass.
        for ok in ["md.aapl", "feed-1", "a_b.c", "X"] {
            validate_channel_name(ok).unwrap();
        }
    }

    #[test]
    fn create_rejected_when_name_already_owned_by_peer() {
        let node = Node::new(config(2, temp_dir("collision")));
        // A peer registered this name earlier (smaller timestamp), so it owns it under
        // first-registrant-wins. Seed the local registry with that winning entry.
        node.registry.lock_safe().merge(ChannelIdentity {
            name: "md.aapl".to_string(),
            owner: NodeId(1),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1, // earlier than any now_nanos()
            epoch: 0,
            deleted: false,
            member_of: None,
        });

        let err = node
            .create_for_client("md.aapl", ChannelOptions::default())
            .unwrap_err();
        assert_eq!(
            err.kind(),
            io::ErrorKind::AlreadyExists,
            "losing the name collision must be rejected, not silently accepted"
        );
        // The name is reserved before any file is created, so a rejection leaves no orphan.
        assert!(
            !node.channel_path("md.aapl").unwrap().exists(),
            "no origin file should be created for a rejected registration"
        );
    }

    #[test]
    fn resolve_distinguishes_unknown_from_unreachable_owner() {
        let node = Node::new(config(1, temp_dir("resolve-liveness")));
        let short = Some(Duration::from_millis(20));

        // Unknown channel → TimedOut.
        let unknown = node.resolve("nope", short).unwrap_err();
        assert_eq!(unknown.kind(), io::ErrorKind::TimedOut);

        // Known channel whose owner never heartbeats (not a live member) → HostUnreachable,
        // not a stale address and not "unknown".
        node.registry.lock_safe().merge(ChannelIdentity {
            name: "md.aapl".to_string(),
            owner: NodeId(99),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1,
            epoch: 0,
            deleted: false,
            member_of: None,
        });
        let unreachable = node.resolve("md.aapl", short).unwrap_err();
        assert_eq!(
            unreachable.kind(),
            io::ErrorKind::HostUnreachable,
            "a known channel with a non-live owner is unreachable, not unknown"
        );
    }

    #[test]
    fn create_topic_and_publish_members() {
        let node = Node::new(config(1, temp_dir("topics")));

        // A topic is an ordinary (registered, subscribable) channel plus a mux.
        node.create_topic("agg", TopicOptions::default()).unwrap();
        assert!(node.registry.lock_safe().get("agg").is_some());
        assert_eq!(node.topic_member_count("agg"), Some(0));

        // Members are ordinary registered channels attached to the mux.
        node.publish_to_topic("agg", "mem.a", ChannelOptions::default())
            .unwrap();
        node.publish_to_topic("agg", "mem.b", ChannelOptions::default())
            .unwrap();
        assert_eq!(node.topic_member_count("agg"), Some(2));
        assert!(node.registry.lock_safe().get("mem.a").is_some());

        // Publishing to a topic this node doesn't host is allowed (the topic may be remote):
        // the member is created and tagged `member_of`, for the owner to discover and attach.
        node.publish_to_topic("remote.topic", "mem.c", ChannelOptions::default())
            .unwrap();
        let m = node.registry.lock_safe().get("mem.c").cloned();
        assert_eq!(
            m.and_then(|id| id.member_of).as_deref(),
            Some("remote.topic")
        );
        // Not attached to any local mux (we don't host that topic).
        assert_eq!(node.topic_member_count("remote.topic"), None);
    }

    #[test]
    fn tombstoned_member_is_detached_from_the_mux() {
        let node = Node::new(config(1, temp_dir("detach")));
        node.create_topic("agg", TopicOptions::default()).unwrap();
        node.publish_to_topic("agg", "mem.a", ChannelOptions::default())
            .unwrap();
        assert_eq!(node.topic_member_count("agg"), Some(1));

        // The member's owner deregisters it; the sync loop drains + closes and detaches it.
        assert!(node.deregister("mem.a").unwrap());
        node.attach_pending_members();
        assert_eq!(node.topic_member_count("agg"), Some(0));
    }

    #[test]
    fn topic_status_reports_members_and_counters() {
        let node = Node::new(config(1, temp_dir("status")));
        node.create_topic("agg", TopicOptions::default()).unwrap();
        node.publish_to_topic("agg", "mem.a", ChannelOptions::default())
            .unwrap();

        let status = node.topic_status("agg").unwrap().unwrap();
        assert_eq!(status.members.len(), 1);
        let m = &status.members[0];
        assert_eq!(m.name, "mem.a");
        assert_eq!((m.merged, m.head, m.lag), (0, 0, 0), "no records yet");
        // Local member owned by this (live) node, caught up ⇒ Quiet.
        assert_eq!(m.state, MemberState::Quiet);
        assert!(status.slot_table_version >= 1, "a slot table was emitted");
        assert_eq!(status.gaps_emitted, 0);

        // A topic we don't host has no status.
        assert!(node.topic_status("nope").is_none());
    }

    #[test]
    fn reaper_tombstones_a_member_with_a_dead_owner() {
        let node = Node::new(config(1, temp_dir("reap")));
        node.create_topic(
            "agg",
            TopicOptions {
                member_reap_after_ms: 1, // opt in with a tiny threshold
                ..TopicOptions::default()
            },
        )
        .unwrap();

        // A member of "agg" owned by a node that is never a live member (owner 99).
        node.registry.lock_safe().merge(ChannelIdentity {
            name: "feed.x".to_string(),
            owner: NodeId(99),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1,
            epoch: 0,
            member_of: Some("agg".to_string()),
            deleted: false,
        });

        // First pass records "dead since now"; after the threshold elapses, the next pass reaps.
        node.reap_dead_members();
        assert!(node.registry.lock_safe().get("feed.x").is_some());
        std::thread::sleep(Duration::from_millis(3));
        node.reap_dead_members();
        assert!(
            node.registry.lock_safe().get("feed.x").is_none(),
            "a member whose owner stayed dead past the threshold is reaped (tombstoned)"
        );
    }

    #[test]
    fn deregister_topic_retires_mux_and_writes_terminal() {
        use xchannel_net_core::mux::MSG_TYPE_TERMINAL;

        let node = Node::new(config(1, temp_dir("retire")));
        let topic_path = node.create_topic("agg", TopicOptions::default()).unwrap();
        node.publish_to_topic("agg", "mem.a", ChannelOptions::default())
            .unwrap();
        assert_eq!(node.topic_member_count("agg"), Some(1));

        assert!(node.deregister_topic("agg").unwrap());
        assert_eq!(node.topic_member_count("agg"), None, "mux retired");
        assert!(
            node.registry.lock_safe().get("agg").is_none(),
            "topic channel tombstoned"
        );

        // The topic channel ends with a terminal marker.
        let mut r = ReaderBuilder::new(&topic_path)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let mut terminal = false;
        while let Some(m) = r.try_read().unwrap() {
            if m.header().message_type == MSG_TYPE_TERMINAL {
                terminal = true;
            }
        }
        assert!(terminal, "a terminal marker was committed");
    }

    #[test]
    fn remote_member_merges_into_topic_across_two_nodes() {
        use xchannel_net_core::mux::{Provenance, is_control};

        let (a, _a_stream, a_control) = start(1, "topic-a");
        let (b, _b_stream, _b_control) = start(2, "topic-b");

        // A hosts the topic and drives its mux.
        let topic_path = a.create_topic("agg", TopicOptions::default()).unwrap();
        {
            let a = a.clone();
            std::thread::spawn(move || a.run_mux(Duration::from_millis(2)));
        }

        // B hosts a member of "agg" (B does not own the topic) and writes to it, dropping the
        // writer before it's served (single-process test: avoid concurrent writer+reader on
        // B's origin).
        let n = 20u64;
        {
            let member_path = b
                .publish_to_topic("agg", "feed.b", ChannelOptions::default())
                .unwrap();
            let mut w = WriterBuilder::new(&member_path)
                .region_size(1 << 20)
                .build()
                .unwrap();
            for i in 0..n {
                let p = format!("m{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(1, p.len() as u32, i).unwrap();
            }
        }

        // Link B → A: A learns feed.b (member_of = agg) via gossip and B's stream address via
        // heartbeat, then A's maintenance loop subscribes to feed.b and its mux merges it.
        b.connect_control_peer(a_control).unwrap();

        // A's topic channel eventually holds all n member records, provenance-stamped.
        let mut bodies = poll_until(|| {
            let mut r = ReaderBuilder::new(&topic_path)
                .mode(ReaderMode::LateJoin)
                .build()
                .ok()?;
            let mut got = Vec::new();
            while let Some(m) = r.try_read().ok()? {
                if is_control(m.header().message_type) {
                    continue;
                }
                let (prov, body) = Provenance::split(m.payload()).ok()?;
                got.push((prov.member_index, body.to_vec()));
            }
            (got.len() as u64 == n).then_some(got)
        });

        bodies.sort_by_key(|(idx, _)| *idx);
        for (i, (idx, body)) in bodies.iter().enumerate() {
            assert_eq!(*idx, i as u64, "member indices contiguous from 0");
            assert_eq!(body, format!("m{i}").as_bytes(), "original body preserved");
        }
    }
}
