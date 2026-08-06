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
use xchannel_net_core::codec::{self, decode_client_request, encode_client_reply};
use xchannel_net_core::dissemination::Dissemination;
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::mux::{self, Mux};
use xchannel_net_core::stream::{self, ChannelSource, SubscribeError, accept_subscription};
use xchannel_net_core::transport::{
    Listener, TcpListener, TcpTransport, Transport, UnixListener, UnixTransport,
};
use xchannel_net_core::wire::{
    ChannelChange, ChannelInfo, ChannelOptions, ClientReply, ClientRequest, DiscoveryCursor,
    SubscriptionStatus, TopicOptions,
};
use xchannel_net_core::{NodeId, RecordIndex};

/// A node not heard from within this is dropped from the live set.
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on concurrent inbound stream + client connections (thread-exhaustion guard). Peer
/// control links are not capped — they come from configured/trusted seeds.
const MAX_CONNECTIONS: usize = 4096;

/// Per-member records merged per mux poll cycle — the fairness bound so one hot member can't
/// monopolize the interleave or head-of-line-block other topics on the shared loop
/// (`doc/TOPICS.md` §4.3).
const MAX_BATCH_PER_MEMBER: usize = 256;

/// Directory holding this node's **discovery log** — the record of registry changes clients
/// read to follow the channel set. Dot-prefixed, so no channel name can ever collide with it.
///
/// The log is **node-local**: it describes what this daemon has converged on, not a
/// network-wide fact, so it is never registered, replicated, or subscribable from a peer. It
/// is derived state — a restarted daemon discards it and starts a fresh one with a new
/// generation, which is how a client with a stale cursor learns to re-list instead of resuming
/// into an unrelated log. That makes it compatible with DESIGN §5's "the only durable
/// node-owned state is `NodeId` + config": nothing here is ever *restored*.
const DISCOVERY_DIR: &str = ".discovery";

/// Geometry of the discovery log. Registry changes are small and rare, so one page-ish region
/// is ample; the retention bound is what turns "your cursor is too old" into an explicit
/// answer rather than unbounded growth.
const DISCOVERY_REGION_SIZE: usize = 1 << 20;
const DISCOVERY_KEEP_FILES: u64 = 4;

/// Filename of the log inside a channel's own directory.
///
/// **Every channel owns a directory** — `data_dir/<name>/` for an origin,
/// `data_dir/.replicas/<name>/` for a replica — and xchannel's segments live inside it as
/// `log`, `log.1`, `log.2`, …
///
/// The alternative (a channel's log directly at `data_dir/<name>`) put channel names and
/// xchannel's segment suffixes in **one namespace**, and channel names may contain dots: the
/// files of a channel named `md.aapl.1` are indistinguishable from segment 1 of `md.aapl`.
/// That is not exotic — dots are the recommended separator. It also made restart recovery
/// guess: retention unlinks segment 0, which is the *unsuffixed* file, so a rolled channel
/// past its retention window left only `md.aapl.4`, `md.aapl.5` on disk, with nothing named
/// `md.aapl` for a scan to key on. A directory per channel removes the ambiguity by
/// construction — names live in the directory namespace, segment suffixes in the file
/// namespace — and makes deletion exact (`remove_dir_all`) rather than glob-matched.
const CHANNEL_LOG_FILE: &str = "log";

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
    /// This daemon's discovery log: `None` until first use, then the writer plus the
    /// generation stamped into it.
    discovery: Arc<Mutex<Option<Writer>>>,
    /// Incarnation of this daemon's discovery log — fresh per process, so a client's cursor
    /// from a previous run is recognisably stale.
    discovery_generation: u64,
    /// When this node started. Bounds how long an owner can be *known* to have been silent:
    /// a daemon that just came up has heard from nobody, and must not conclude every channel
    /// in the registry is abandoned.
    started_at: Instant,
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
            discovery: Arc::new(Mutex::new(None)),
            discovery_generation: now_nanos(),
            started_at: Instant::now(),
            config: Arc::new(config),
        }
    }

    /// Retire a channel owned by a node that is **gone**, so its name can be reclaimed
    /// elsewhere — the one path by which an application pinned to a dead host can be brought
    /// up somewhere else under the same name.
    ///
    /// This is deliberately **operator-invoked**, never automatic. "Owner death freezes the
    /// channel" is a locked decision, and a daemon that retired names on its own would be
    /// performing failover: under a partition each side sees the other as dead, and a reclaim
    /// at `epoch + 1` wins the merge — so an automatic reaper could destroy a channel whose
    /// owner is alive and still writing on the far side. A human asserting "that host is gone"
    /// turns that into an operator error rather than an emergent one.
    ///
    /// Refuses unless the owner has been unreachable for at least `config.reclaim_after`.
    /// An owner never heard from is judged against this node's own uptime, so a freshly
    /// started daemon — which has heard from nobody — cannot immediately declare every channel
    /// in the registry abandoned.
    ///
    /// After this returns `Ok(true)` the name is free: registering it produces `epoch + 1`, a
    /// distinct incarnation, and subscribers holding replicas of the old one are told they
    /// diverged and rebuild.
    pub fn force_deregister(&self, name: &str) -> io::Result<bool> {
        let Some(id) = self.registry.lock_safe().get(name).cloned() else {
            return Ok(false); // unknown or already tombstoned
        };
        if id.owner == self.config.node_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("channel '{name}' is owned by this node — use deregister"),
            ));
        }
        // Two independent conditions, because the threshold alone is not a safety property:
        // with a short (or zero) `reclaim_after`, elapsed silence would permit reclaiming a
        // node we heard from moments ago.
        if self
            .dissemination
            .lock_safe()
            .live_addr_of(id.owner)
            .is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                format!(
                    "owner of '{name}' (node {}) is a live member — it has not gone anywhere",
                    id.owner.0
                ),
            ));
        }
        // Silence we can actually vouch for: how long since we last heard from the owner, or
        // how long we have been listening at all if we never have.
        let unreachable_for = self
            .dissemination
            .lock_safe()
            .silent_for(id.owner)
            .unwrap_or_else(|| self.started_at.elapsed());
        if unreachable_for < self.config.reclaim_after {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                format!(
                    "owner of '{name}' (node {}) has been unreachable for {:?}, less than the \
                     {:?} required to reclaim it",
                    id.owner.0, unreachable_for, self.config.reclaim_after
                ),
            ));
        }
        let tombstone = self.registry.lock_safe().reap(name);
        let Some(tombstone) = tombstone else {
            return Ok(false);
        };
        self.publish_change(&self.change_of(&tombstone));
        self.dissemination
            .lock_safe()
            .announce(std::slice::from_ref(&tombstone))?;
        self.retire_subscription(name);
        Ok(true)
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
            .generation(identity.epoch)
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
            .base_record_index(0)
            // The registry's reclaim epoch *is* this log's incarnation: stamping it here is
            // what lets a subscriber's replica later say which incarnation it holds. Applied
            // only on creation — reopening keeps the on-disk value, so a restart re-hosting
            // this origin cannot relabel it.
            .generation(identity.epoch);
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
        let merged = reg.merge_tracked(identity.clone());
        drop(reg);
        if merged.changed {
            self.publish_change(&self.change_of(&merged.winner));
        }
        let winner = merged.winner;
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
        self.publish_change(&self.change_of(&tombstone));
        self.hosted.lock_safe().remove(name);
        self.dissemination
            .lock_safe()
            .announce(std::slice::from_ref(&tombstone))?;
        // Delete the on-disk channel so a later restart's data-dir scan can't resurrect this
        // deregistered name (`reconstruct_from_disk` has no tombstone to consult on an isolated
        // restart — the registry is rebuilt from scratch). Deregistration is deliberate removal.
        if let Ok(dir) = self.channel_dir(name) {
            let _ = std::fs::remove_dir_all(&dir);
        }
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

            for m in &live {
                let remote = m.owner != self.config.node_id;
                // Keep a **remote** member's replica fresh whether or not it's attached yet.
                // This is essential after a restart: reconstruct may re-attach a member from a
                // *stale* replica, so we must (re)start its subscription to resume its stream —
                // idempotent, a no-op if already replicating.
                if remote {
                    self.ensure_member_subscription(&m.name);
                }

                let already = self
                    .muxes
                    .lock_safe()
                    .get(&topic)
                    .is_none_or(|mx| mx.has_member(&m.name, m.epoch));
                if already {
                    continue;
                }
                // Resolve the path the mux reads: a local origin, or the remote member's
                // locally-synced replica (skip until it exists — retried next cycle).
                let path = if remote {
                    match self.replica_path(&m.name) {
                        Ok(p) if p.exists() => p,
                        _ => continue,
                    }
                } else {
                    match self.channel_path(&m.name) {
                        Ok(p) => p,
                        Err(_) => continue,
                    }
                };
                if let Some(mux) = self.muxes.lock_safe().get_mut(&topic) {
                    let _ = mux.add_member(&m.name, m.epoch, &path);
                }
            }

            // Detach members the registry says have **left**: clean-leave drain → MemberClosed,
            // then stop replicating a remote one (§6.1). Only a *positive* signal retires a
            // member — a tombstone, or its `member_of` moved to another topic. Mere **absence**
            // from the registry must NOT retire it (the registry may just be incomplete — e.g.
            // right after a restart, before reconstruct/gossip catches up); otherwise we'd drain
            // a member that never left.
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
                let left = self.registry.lock_safe().get_raw(&name).is_some_and(|id| {
                    id.deleted || id.member_of.as_deref() != Some(topic.as_str())
                });
                if !left {
                    continue; // absent, not departed — keep merging
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
        // One subdirectory per channel, so the scan needs no heuristic: no guessing whether
        // `md.aapl.4` is a channel or a rolled segment, and a channel whose segment 0 has been
        // pruned still announces itself by its directory.
        let names: Vec<String> = rd
            .flatten()
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().to_str().map(String::from))
            .filter(|name| !name.starts_with('.')) // .replicas and other dot-entries
            .filter(|name| validate_channel_name(name).is_ok())
            .collect();
        let mut topics: Vec<(String, mux::TopicConfig)> = Vec::new();
        let mut origins: Vec<String> = Vec::new();
        for name in &names {
            let Ok(path) = self.channel_path(name) else {
                continue;
            };
            match mux::topic_config(&path) {
                Ok(Some(cfg)) => topics.push((name.clone(), cfg)),
                Ok(None) => origins.push(name.clone()),
                Err(_) => {}
            }
        }
        // Re-host topics FIRST: rehost_topic re-registers each *local* member with its
        // `member_of`, so the origin pass then skips those (already hosted) rather than
        // re-registering them with `member_of = None` — which would make the detach pass retire a
        // member that never left.
        for (name, cfg) in &topics {
            let _ = self.rehost_topic(name, cfg);
        }
        // Then re-register the remaining plain origins (skips anything already hosted).
        for name in &origins {
            let _ = self.reregister_origin(name);
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
            // A member with a **local origin** is one we own: re-register it with `member_of`
            // (recovering its geometry via the header accessor) so it's back in the topic's live
            // set — otherwise the detach pass would drain a member that never left. Then attach
            // it from the origin. A member with only a **replica** is remote: attach the replica
            // and let peer anti-entropy restore its `member_of` (it's not ours to register).
            let origin = self.channel_path(member).ok().filter(|p| p.exists());
            if let Some(op) = &origin
                && let Ok(reader) = xchannel::ReaderBuilder::new(op).late_join().build()
            {
                let (rs, mtu) = (reader.region_size() as u32, reader.mtu());
                drop(reader);
                if let Ok(id) = self.claim_name(member, rs, mtu, Some(name.to_string())) {
                    let _ = self.announce_hosted(&id, op.clone(), 0, 0);
                }
            }
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
        // Delete the topic channel on disk so a later restart's data-dir scan can't re-host this
        // retired topic (the terminal marker above is best-effort for still-connected subscribers).
        if let Ok(dir) = self.channel_dir(topic) {
            let _ = std::fs::remove_dir_all(&dir);
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

    // ---------------- on-disk layout ----------------
    //
    // **INVARIANT: every channel path is built by one of the four helpers below.** Nothing
    // else may join a channel name onto `data_dir` — a stray `data_dir.join(name)` silently
    // reintroduces the flat layout for that one call site, and with it the collision between
    // channel names and xchannel's segment suffixes that [`CHANNEL_LOG_FILE`] describes.
    // The directory-per-channel guarantee is a convention these helpers uphold, not something
    // the type system or xchannel enforces: xchannel is handed a base path and appends `.1`,
    // `.2`, … to it, knowing nothing about directories.
    //
    // The convention stops here. Clients receive a finished path in `ClientReply` and open it
    // opaquely, so they never construct one and the layout can change again without touching
    // them.

    /// Directory holding an **origin** channel this node hosts: `data_dir/<name>`.
    fn channel_dir(&self, name: &str) -> io::Result<PathBuf> {
        validate_channel_name(name)?;
        Ok(self.config.data_dir.join(name))
    }

    /// Path of the log inside a channel's own directory.
    fn channel_path(&self, name: &str) -> io::Result<PathBuf> {
        Ok(self.channel_dir(name)?.join(CHANNEL_LOG_FILE))
    }

    /// Directory holding a **replica** this node maintains.
    fn replica_dir(&self, name: &str) -> io::Result<PathBuf> {
        validate_channel_name(name)?;
        Ok(self.config.data_dir.join(".replicas").join(name))
    }

    /// Path of the log inside a **replica**'s directory: `data_dir/.replicas/<name>/log`. The
    /// separate `.replicas` subtree keeps a replica from colliding with a same-named origin
    /// (notably when a node subscribes to a channel it also hosts).
    fn replica_path(&self, name: &str) -> io::Result<PathBuf> {
        Ok(self.replica_dir(name)?.join(CHANNEL_LOG_FILE))
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
                let mut retired = Vec::new();
                for id in pumped {
                    let name = id.name.clone();
                    if self.merge_and_publish(id).deleted {
                        retired.push(name);
                    }
                }
                // A tombstone we just learned about retires any subscription we hold for that
                // name. Without this the loop keeps re-resolving a channel the network has
                // agreed is gone, and local readers keep being handed a replica that will
                // never advance again — indistinguishable, from the outside, from a source
                // that has merely gone quiet.
                for name in retired {
                    self.retire_subscription(&name);
                }
            }
            // Retire members whose owner has been dead too long (opt-in), then react to
            // `member_of` registrations: attach live members, detach reaped/tombstoned ones.
            self.reap_dead_members();
            self.attach_pending_members();
            std::thread::sleep(interval);
        }
    }

    // ---------------- discovery ----------------

    /// Merge into the registry and publish the result to discovery **iff the map changed**.
    /// Anti-entropy re-merges a peer's whole registry on every reconnect, so publishing per
    /// merge rather than per change would turn each reconnect into a storm of no-ops.
    fn merge_and_publish(&self, incoming: ChannelIdentity) -> ChannelIdentity {
        let merged = self.registry.lock_safe().merge_tracked(incoming);
        if merged.changed {
            self.publish_change(&self.change_of(&merged.winner));
        }
        merged.winner
    }

    /// The discovery record describing an entry's current state.
    fn change_of(&self, id: &ChannelIdentity) -> ChannelChange {
        if id.deleted {
            return ChannelChange::Removed {
                name: id.name.clone(),
                epoch: id.epoch,
            };
        }
        let owner_live = id.owner == self.config.node_id
            || self
                .dissemination
                .lock_safe()
                .live_addr_of(id.owner)
                .is_some();
        ChannelChange::Upserted(ChannelInfo {
            name: id.name.clone(),
            owner: id.owner,
            epoch: id.epoch,
            owner_live,
            member_of: id.member_of.clone(),
            region_size: id.region_size,
            mtu: id.mtu,
            earliest_index: id.earliest_index,
        })
    }

    /// Append one change to the discovery log, opening (and resetting) it on first use.
    ///
    /// Best-effort by construction: discovery is an *awareness* service, and a client that
    /// misses a record recovers by re-listing. Failing a registration because a derived log
    /// could not be written would be the tail wagging the dog, so errors are swallowed here
    /// and the caller is never made to care.
    fn publish_change(&self, change: &ChannelChange) {
        let _ = self.with_discovery(|w| {
            let (msg_type, payload) = codec::encode_change(change);
            let buf = w.try_reserve(payload.len())?;
            buf.copy_from_slice(&payload);
            w.commit(msg_type, payload.len() as u32, 0)
        });
    }

    /// Run `f` against the discovery log's writer, creating the log on first use. Creation
    /// **wipes** any log left by a previous run: it is derived state, and resuming a client's
    /// cursor into a rebuilt log would be meaningless — the fresh `generation` is what tells
    /// a client that.
    fn with_discovery<R>(&self, f: impl FnOnce(&mut Writer) -> io::Result<R>) -> io::Result<R> {
        let mut slot = self.discovery.lock_safe();
        if slot.is_none() {
            let dir = self.config.data_dir.join(DISCOVERY_DIR);
            let _ = std::fs::remove_dir_all(&dir);
            ensure_private_dir(&dir)?;
            *slot = Some(
                WriterBuilder::new(dir.join(CHANNEL_LOG_FILE))
                    .region_size(DISCOVERY_REGION_SIZE)
                    .generation(self.discovery_generation)
                    // Bounded: a client that falls this far behind is told to re-list, which
                    // is cheaper than retaining changes nobody is reading.
                    .file_roll_size((DISCOVERY_REGION_SIZE * 2) as u64)
                    .keep_files(DISCOVERY_KEEP_FILES)
                    .build()?,
            );
        }
        f(slot.as_mut().expect("just initialized"))
    }

    /// Channels whose name starts with `prefix`, plus where to pick up the changes that follow.
    ///
    /// Both come from **one** registry lock, so there is no window between "what exists" and
    /// "what changed next" — the race a separate list-then-watch pair has to close with
    /// revisions.
    pub fn list_channels(&self, prefix: &str) -> io::Result<(Vec<ChannelInfo>, DiscoveryCursor)> {
        let live: Vec<NodeId> = self.dissemination.lock_safe().live_members();
        let (channels, from) = {
            let reg = self.registry.lock_safe();
            let channels: Vec<ChannelInfo> = reg
                .with_prefix(prefix)
                .map(|id| ChannelInfo {
                    name: id.name.clone(),
                    owner: id.owner,
                    epoch: id.epoch,
                    owner_live: id.owner == self.config.node_id || live.contains(&id.owner),
                    member_of: id.member_of.clone(),
                    region_size: id.region_size,
                    mtu: id.mtu,
                    earliest_index: id.earliest_index,
                })
                .collect();
            let from = self.with_discovery(|w| Ok(w.next_record_index()))?;
            (channels, RecordIndex(from))
        };
        Ok((
            channels,
            DiscoveryCursor {
                log_path: self
                    .config
                    .data_dir
                    .join(DISCOVERY_DIR)
                    .join(CHANNEL_LOG_FILE)
                    .to_string_lossy()
                    .into_owned(),
                generation: self.discovery_generation,
                from,
            },
        ))
    }

    /// Stop and forget a subscription for a name that has been tombstoned. Distinct from
    /// [`unsubscribe`](Self::unsubscribe) only in intent: this is the network telling us the
    /// channel is gone, not a client losing interest. The replica's files are left in place —
    /// the records it already holds are still valid history, and discarding them is the
    /// reader's call, not ours.
    fn retire_subscription(&self, name: &str) {
        if let Some(sub) = self.subscriptions.lock_safe().remove(name) {
            sub.stop();
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
                // A channel this node **hosts** is already local: hand back the origin and do
                // no replication at all. Otherwise every reader of a channel its own node owns
                // — the normal case when an application consumes the stream it also produces —
                // would pay for a second full copy on disk, streamed to itself over loopback
                // TCP, pruned on its own schedule. The origin is the same records, is always
                // ahead of any replica of it, and needs no subscription to stay current.
                if let Some(src) = self.hosted.lock_safe().get(&name) {
                    return ClientReply::Subscribed {
                        replica_path: src.path.to_string_lossy().into_owned(),
                    };
                }
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
            ClientRequest::Deregister { name } => match self.deregister(&name) {
                Ok(existed) => {
                    self.retire_subscription(&name);
                    ClientReply::Deregistered { existed }
                }
                Err(e) => ClientReply::Error {
                    message: e.to_string(),
                },
            },
            ClientRequest::ListChannels { prefix } => match self.list_channels(&prefix) {
                Ok((channels, cursor)) => ClientReply::Channels { channels, cursor },
                Err(e) => ClientReply::Error {
                    message: e.to_string(),
                },
            },
            ClientRequest::ForceDeregister { name } => match self.force_deregister(&name) {
                Ok(existed) => ClientReply::Deregistered { existed },
                Err(e) => ClientReply::Error {
                    message: e.to_string(),
                },
            },
            ClientRequest::SubscriptionStatus { name } => match self.subscription_status(&name) {
                Some(status) => ClientReply::Status(status),
                None => ClientReply::Error {
                    message: format!("channel '{name}' is neither hosted nor subscribed here"),
                },
            },
        }
    }

    /// Health of a channel this node reads: replication progress plus whether the machinery
    /// behind it is working. `None` if this node neither hosts nor subscribes to the name.
    ///
    /// Deliberately reports both halves, because a stalled number and a healthy quiet source
    /// look identical from `synced` alone: `owner_live` is *membership* liveness (the owner's
    /// manager is reachable — not that its application is still writing), and
    /// `last_record_at_ms` is the live staleness signal, since `head_at_connect` is a snapshot
    /// that goes stale as soon as the source moves on.
    pub fn subscription_status(&self, name: &str) -> Option<SubscriptionStatus> {
        // Hosted here ⇒ read from the origin; nothing to lag behind (see the `Subscribe`
        // handler, which hands such a client the origin path).
        if let Some(src) = self.hosted.lock_safe().get(name).cloned() {
            let head = xchannel::ReaderBuilder::new(&src.path)
                .late_join()
                .build()
                .ok()
                .and_then(|r| r.head_record_index().ok())
                .unwrap_or(0);
            let generation = self
                .registry
                .lock_safe()
                .get(name)
                .map(|id| id.epoch)
                .unwrap_or(0);
            return Some(SubscriptionStatus {
                local: true,
                active: true,
                synced: RecordIndex(head),
                head_at_connect: RecordIndex(head),
                owner: self.config.node_id,
                owner_live: true,
                generation,
                last_record_at_ms: 0,
                rebuilds_gap: 0,
                rebuilds_diverged: 0,
                last_rebuild_at_ms: 0,
            });
        }

        let subs = self.subscriptions.lock_safe();
        let sub = subs.get(name)?;
        let (owner, generation) = self
            .registry
            .lock_safe()
            .get(name)
            .map(|id| (id.owner, id.epoch))
            .unwrap_or((self.config.node_id, 0));
        let owner_live = self
            .dissemination
            .lock_safe()
            .live_members()
            .contains(&owner);
        Some(SubscriptionStatus {
            local: false,
            active: sub.is_active(),
            synced: RecordIndex(sub.synced_index()),
            head_at_connect: RecordIndex(sub.head_at_connect()),
            owner,
            owner_live,
            generation,
            last_record_at_ms: sub.last_record_at_ms().unwrap_or(0),
            rebuilds_gap: sub.rebuilds().gap(),
            rebuilds_diverged: sub.rebuilds().diverged(),
            last_rebuild_at_ms: sub.rebuilds().last_at_ms().unwrap_or(0),
        })
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
        let head_at_connect = Arc::new(AtomicU64::new(0));
        let last_record_at_ms = Arc::new(AtomicU64::new(0));
        let rebuilds = Arc::new(RebuildStats::default());
        let shutdown: Arc<Mutex<Option<TcpTransport>>> = Arc::new(Mutex::new(None));

        let node = self.clone();
        let progress = SubscriptionProgress {
            synced: Arc::clone(&synced),
            head_at_connect: Arc::clone(&head_at_connect),
            last_record_at_ms: Arc::clone(&last_record_at_ms),
            rebuilds: Arc::clone(&rebuilds),
        };
        let (name_t, path_t, stopped_t, shutdown_t) = (
            name.to_string(),
            replica_path.clone(),
            Arc::clone(&stopped),
            Arc::clone(&shutdown),
        );
        let handle = std::thread::spawn(move || {
            node.run_subscription(name_t, path_t, stopped_t, progress, shutdown_t)
        });

        Ok(Subscription {
            replica_path,
            synced,
            head_at_connect,
            last_record_at_ms,
            rebuilds,
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
        progress: SubscriptionProgress,
        shutdown: Arc<Mutex<Option<TcpTransport>>>,
    ) {
        let SubscriptionProgress {
            synced,
            head_at_connect,
            last_record_at_ms,
            rebuilds,
        } = progress;
        const BACKOFF: Duration = Duration::from_millis(100);
        while !stopped.load(Ordering::Relaxed) {
            // Re-resolve each attempt (owner address may have changed); short timeout so we
            // keep re-checking `stopped`.
            let Ok((id, addr)) = self.resolve(&name, Some(Duration::from_millis(200))) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            // Resume from the replica's current head (0 if it doesn't exist yet), carrying
            // the incarnation that replica holds so the source can refuse a resume across a
            // reclaim instead of splicing two logs.
            let (from, generation) = self
                .replica_position(&replica_path, id.region_size)
                .unwrap_or((RecordIndex(0), 0));
            synced.store(from.0, Ordering::Relaxed);

            let Ok(conn) = TcpTransport::connect(addr) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            let shutdown_handle = conn.try_clone().ok();
            let mut client = match stream::subscribe(conn, &name, from, generation, &replica_path) {
                Ok(client) => client,
                Err(SubscribeError::Rebuild { diverged, .. }) => {
                    // The source cannot extend this replica — it is behind retention, or it
                    // belongs to a previous incarnation of the name. Retrying the same
                    // position would loop forever (the answer will not change), so discard it
                    // and let the next attempt subscribe from scratch. Only ever taken for
                    // this classified failure: doing it on a transient error would throw away
                    // a whole channel's history over a dropped connection.
                    if let Ok(dir) = self.replica_dir(&name) {
                        let _ = std::fs::remove_dir_all(&dir);
                        // Recreate it: the next attempt's sink opens a writer *inside* this
                        // directory and will not create it.
                        let _ = ensure_private_dir(&dir);
                    }
                    synced.store(0, Ordering::Relaxed);
                    rebuilds.record(diverged);
                    std::thread::sleep(BACKOFF);
                    continue;
                }
                Err(_) => {
                    std::thread::sleep(BACKOFF);
                    continue;
                }
            };
            head_at_connect.store(client.head().0, Ordering::Relaxed);
            *shutdown.lock_safe() = shutdown_handle;

            // Apply records until the connection drops or we're stopped.
            loop {
                if stopped.load(Ordering::Relaxed) {
                    return;
                }
                match client.recv_one() {
                    Ok(()) => {
                        synced.store(client.expected_index().0, Ordering::Relaxed);
                        // One vDSO clock read per record, alongside a socket read and an mmap
                        // write — negligible, and it is the only *live* staleness signal a
                        // client has: `head_at_connect` goes stale the moment the source moves.
                        last_record_at_ms.store(now_nanos() / 1_000_000, Ordering::Relaxed);
                    }
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

    /// Where an existing replica stands: `(head index, generation)` — the resume position and
    /// the incarnation of the channel that position refers to. `(0, 0)` if there is no replica
    /// yet. Both come from the replica's own files, so nothing node-owned has to be persisted
    /// to survive a restart (DESIGN §5). Reopens the channel briefly; `region_size` must match
    /// the on-disk geometry (taken from the registry identity).
    fn replica_position(
        &self,
        replica_path: &Path,
        region_size: u32,
    ) -> io::Result<(RecordIndex, u64)> {
        if !replica_path.exists() {
            return Ok((RecordIndex(0), 0));
        }
        let writer = WriterBuilder::new(replica_path)
            .region_size(region_size as usize)
            .build()?;
        Ok((RecordIndex(writer.next_record_index()), writer.generation()))
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

/// How often a subscription had to **discard its replica and re-pull the channel**, split by
/// cause, plus when it last happened.
///
/// A rebuild is not a transient hiccup the loop can hide: the replica's contents are replaced,
/// and a local reader that had caught up sees history it already read, or a start index that
/// jumped forward. Recording it is what lets a consumer tell "quiet source" from "this source
/// was rebuilt under me" — the same two-liveness-concepts distinction the design insists on
/// elsewhere. Read via [`Subscription::rebuilds`].
#[derive(Default, Debug)]
pub struct RebuildStats {
    gap: AtomicU64,
    diverged: AtomicU64,
    last_at_ms: AtomicU64,
}

impl RebuildStats {
    /// Rebuilds caused by the replica falling behind the source's **retention** — the source
    /// no longer holds the records needed to extend it contiguously (`StreamMsg::Gap`). The
    /// rebuilt replica legitimately starts at the source's `earliest`, not at genesis.
    pub fn gap(&self) -> u64 {
        self.gap.load(Ordering::Relaxed)
    }

    /// Rebuilds caused by the replica belonging to a **different incarnation** of the name —
    /// it was reclaimed by a new owner and the old replica is not a prefix of the new log
    /// (`StreamMsg::Diverged`).
    pub fn diverged(&self) -> u64 {
        self.diverged.load(Ordering::Relaxed)
    }

    /// Unix-millis of the most recent rebuild, or `None` if there has never been one.
    pub fn last_at_ms(&self) -> Option<u64> {
        match self.last_at_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(ms),
        }
    }

    fn record(&self, diverged: bool) {
        let counter = if diverged { &self.diverged } else { &self.gap };
        counter.fetch_add(1, Ordering::Relaxed);
        self.last_at_ms
            .store(now_nanos() / 1_000_000, Ordering::Relaxed);
    }
}

/// The progress counters a subscription's background loop writes and its handle reads.
/// Bundled so the loop takes one parameter rather than a growing list of `Arc`s.
struct SubscriptionProgress {
    synced: Arc<AtomicU64>,
    head_at_connect: Arc<AtomicU64>,
    last_record_at_ms: Arc<AtomicU64>,
    rebuilds: Arc<RebuildStats>,
}

/// Handle to a self-healing subscription replicating a remote channel locally. Dropping it
/// stops the background loop.
pub struct Subscription {
    replica_path: PathBuf,
    synced: Arc<AtomicU64>,
    /// The source's head as advertised in the last `SubscribeAck` — a snapshot at connect
    /// time, not a live value; see [`SubscriptionStatus::head_at_connect`].
    head_at_connect: Arc<AtomicU64>,
    /// Unix-millis when a record was last applied; 0 = none yet.
    last_record_at_ms: Arc<AtomicU64>,
    rebuilds: Arc<RebuildStats>,
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

    /// Replica rebuilds this subscription has performed, by cause — see [`RebuildStats`].
    pub fn rebuilds(&self) -> &RebuildStats {
        &self.rebuilds
    }

    /// The source's head as of the last successful (re)connect.
    pub fn head_at_connect(&self) -> u64 {
        self.head_at_connect.load(Ordering::Relaxed)
    }

    /// Unix-millis when a record was last applied, or `None` if none has been.
    pub fn last_record_at_ms(&self) -> Option<u64> {
        match self.last_record_at_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(ms),
        }
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
            // Tests assert the guard's behavior explicitly; a long default would make every
            // reclaim test sleep.
            reclaim_after: Duration::from_millis(0),
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

    /// The relocation case end to end, arranged so only the generation check can catch it:
    /// the new incarnation grows *past* the length of the replica the subscriber is holding,
    /// so its resume position sits well inside `earliest..head` and the "past head" heuristic
    /// sees nothing wrong. Resuming there would append the new log's records onto the old
    /// log's — indices lining up perfectly, contiguity check satisfied, two unrelated channels
    /// silently spliced into one replica. The subscription must discard and rebuild instead.
    #[test]
    fn a_reclaimed_channel_rebuilds_the_replica_instead_of_splicing() {
        let (a, _a_stream, a_control) = start(41, "reclaim-a");
        let (b, _b_stream, _b_control) = start(42, "reclaim-b");
        let old_n = 3u64;

        // First incarnation: a short log, fully replicated to B.
        {
            let mut w = a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            for i in 0..old_n {
                let p = format!("old-{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }
        b.connect_control_peer(a_control).unwrap();
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();
        poll_until(|| (sub.synced_index() == old_n).then_some(()));
        drop(sub); // stops the loop and releases the replica writer

        // The name is retired and reclaimed — same name, brand-new log at `epoch + 1`,
        // holding far fewer records than the replica B is sitting on.
        assert!(a.deregister("md.aapl").unwrap());
        // Deliberately longer than the replica B holds, so `from` stays below the new head.
        let new_n = 40u64;
        {
            let mut w = poll_until(|| a.host_channel("md.aapl", 1 << 20, 0, |x| x).ok());
            for i in 0..new_n {
                let p = format!("new-{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }

        // B re-subscribes holding records of a log that no longer exists.
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();
        poll_until(|| (sub.synced_index() == new_n).then_some(()));
        drop(sub);

        // The replica is the new log in full — not the old one, and not the two spliced.
        let replica = b.replica_path("md.aapl").unwrap();
        let mut r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert_eq!(r.base_record_index(), 0, "rebuilt from genesis");
        let mut seen = 0u64;
        while let Some(m) = r.try_read().unwrap() {
            assert_eq!(
                m.payload(),
                format!("new-{seen}").as_bytes(),
                "replica must hold only the new incarnation's records"
            );
            seen += 1;
        }
        assert_eq!(seen, new_n, "old incarnation's records are gone");
    }

    /// Read the discovery log from `cursor`, the way a client does — the client crate's
    /// `ChannelWatch` wraps exactly this, and is exercised end-to-end in `tests/client_rpc.rs`.
    fn drain_changes(cursor: &DiscoveryCursor) -> Vec<ChannelChange> {
        let mut r = ReaderBuilder::new(&cursor.log_path)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert_eq!(r.generation(), cursor.generation, "same log incarnation");
        let mut index = r.base_record_index();
        while index < cursor.from.0 {
            r.try_read().unwrap().expect("cursor is within the log");
            index += 1;
        }
        let mut out = Vec::new();
        while let Some(m) = r.try_read().unwrap() {
            let (t, payload) = (m.header().message_type, m.payload().to_vec());
            out.push(codec::decode_change(t, &payload).unwrap());
        }
        out
    }

    /// The listing and the cursor come from one registry lock, so a channel registered
    /// *between* them cannot be missed — the race a separate list-then-watch pair needs
    /// revisions to close.
    #[test]
    fn listing_and_watching_cover_the_channel_set_without_a_gap() {
        let node = Node::new(config(141, temp_dir("discovery-list")));
        for name in ["fills.prod.a", "fills.prod.b", "fills.test.c", "md.aapl"] {
            drop(node.host_channel(name, 1 << 20, 0, |x| x).unwrap());
        }

        let (listed, cursor) = node.list_channels("fills.prod.").unwrap();
        let names: Vec<&str> = listed.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            ["fills.prod.a", "fills.prod.b"],
            "prefix matching is a range scan, in name order"
        );
        assert!(listed.iter().all(|c| c.owner == node.config.node_id));
        assert!(
            listed.iter().all(|c| c.owner_live),
            "we own these, so their owner is trivially reachable"
        );
        assert!(listed.iter().all(|c| c.member_of.is_none()));

        // Everything, and nothing, are both legal prefixes.
        assert_eq!(node.list_channels("").unwrap().0.len(), 4);
        assert!(node.list_channels("nothing.").unwrap().0.is_empty());

        // Changes after the cursor are exactly what the log carries.
        drop(
            node.host_channel("fills.prod.d", 1 << 20, 0, |x| x)
                .unwrap(),
        );
        assert!(node.deregister("fills.prod.a").unwrap());

        let seen = drain_changes(&cursor);
        assert_eq!(seen.len(), 2, "one upsert and one removal: {seen:?}");
        assert!(matches!(
            &seen[0],
            ChannelChange::Upserted(c) if c.name == "fills.prod.d"
        ));
        assert!(matches!(
            &seen[1],
            ChannelChange::Removed { name, .. } if name == "fills.prod.a"
        ));
    }

    /// Topic members are ordinary registered channels, so they appear in listings and must be
    /// distinguishable — a consumer wanting sources should not subscribe to the plumbing.
    #[test]
    fn listings_mark_topic_members() {
        let node = Node::new(config(142, temp_dir("discovery-members")));
        node.create_topic("fills.prod.topic", TopicOptions::default())
            .unwrap();
        node.publish_to_topic(
            "fills.prod.topic",
            "fills.prod.member",
            ChannelOptions::default(),
        )
        .unwrap();

        let (listed, _) = node.list_channels("fills.prod.").unwrap();
        let member = listed
            .iter()
            .find(|c| c.name == "fills.prod.member")
            .expect("the member is a channel like any other");
        assert_eq!(member.member_of.as_deref(), Some("fills.prod.topic"));
        let topic = listed
            .iter()
            .find(|c| c.name == "fills.prod.topic")
            .expect("and so is the topic");
        assert!(topic.member_of.is_none());
    }

    /// Anti-entropy re-merges a peer's entire registry on every reconnect. Publishing per
    /// *merge* rather than per *change* would make each reconnect a storm of no-ops.
    #[test]
    fn re_merging_unchanged_entries_publishes_nothing() {
        let node = Node::new(config(143, temp_dir("discovery-idempotent")));
        drop(node.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap());
        let (_, cursor) = node.list_channels("").unwrap();

        let identity = node.registry.lock_safe().get("md.aapl").cloned().unwrap();
        for _ in 0..5 {
            node.merge_and_publish(identity.clone());
        }

        assert!(
            drain_changes(&cursor).is_empty(),
            "re-merging an identical entry changes nothing, so it publishes nothing"
        );
    }

    /// An application restarting must reopen its own channel where it left off, even after
    /// retention has pruned genesis. `create_origin` always passes `base_record_index(0)`, so
    /// this pins the behavior that makes that harmless: on reopen the *on-disk* base wins, and
    /// the writer continues at the channel's true absolute index rather than restarting at 0.
    ///
    /// It also pins that a restart does **not** change the channel's generation. If it did,
    /// every subscriber would see a generation mismatch, conclude the name had been reclaimed,
    /// and discard a perfectly good replica — an app restart would trigger a network-wide
    /// re-pull of full history.
    #[test]
    fn an_origin_reopens_at_its_absolute_index_after_retention_pruned_genesis() {
        let node = Node::new(config(121, temp_dir("reopen-pruned")));
        let options = ChannelOptions {
            region_size: 1 << 20,
            mtu: 0,
            file_roll_size: 0, // roll only when the application says so
            keep_files: 2,
        };
        let path = node.create_for_client("md.aapl", options).unwrap();

        // Four segments of two records; `keep_files(2)` prunes back to the last two, taking
        // segment 0 — the file that carries genesis — with it.
        let mut index = 0u64;
        let write_two = |w: &mut Writer, index: &mut u64| {
            for _ in 0..2 {
                let buf = w.try_reserve(4).unwrap();
                buf.copy_from_slice(b"tick");
                w.commit(0, 4, *index).unwrap();
                *index += 1;
            }
        };
        {
            let mut w = WriterBuilder::new(&path)
                .region_size(options.region_size as usize)
                .keep_files(options.keep_files as u64)
                .build()
                .unwrap();
            write_two(&mut w, &mut index);
            for _ in 0..3 {
                w.roll_file().unwrap();
                write_two(&mut w, &mut index);
            }
        }
        let earliest = ReaderBuilder::new(&path)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap()
            .base_record_index();
        assert!(earliest > 0, "genesis must have been pruned");

        // The application restarts and asks for its channel again, exactly as on first start.
        let reopened = node.create_for_client("md.aapl", options).unwrap();
        assert_eq!(reopened, path);

        let mut w = WriterBuilder::new(&reopened)
            .region_size(options.region_size as usize)
            .keep_files(options.keep_files as u64)
            .build()
            .unwrap();
        assert_eq!(
            w.next_record_index(),
            index,
            "the writer must continue at the channel's absolute head, not restart at 0"
        );
        assert_eq!(
            w.generation(),
            0,
            "a restart is the same incarnation — a changed generation would make every \
             subscriber discard its replica"
        );

        // Appending continues the absolute numbering, and the pruned history stays pruned.
        write_two(&mut w, &mut index);
        assert_eq!(w.next_record_index(), index);
        drop(w);
        let r = ReaderBuilder::new(&path)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert_eq!(r.base_record_index(), earliest);
        assert_eq!(r.head_record_index().unwrap(), index);
    }

    /// The network-facing consequence of the previous test: a subscriber must ride through an
    /// application restart on the origin side. Applications restart routinely, and a restart
    /// that looked like a reclaim would have every subscriber discard its replica and re-pull
    /// the channel's full history — the exact opposite of what the incarnation check is for.
    #[test]
    fn an_application_restart_does_not_make_subscribers_rebuild() {
        let (a, _a_stream, a_control) = start(131, "restart-sub-a");
        let (b, _b_stream, _b_control) = start(132, "restart-sub-b");
        let options = ChannelOptions::default();
        let path = a.create_for_client("md.aapl", options).unwrap();

        let mut index = 0u64;
        let write_two = |index: &mut u64| {
            let mut w = WriterBuilder::new(&path)
                .region_size(options.region_size as usize)
                .build()
                .unwrap();
            for _ in 0..2 {
                let buf = w.try_reserve(4).unwrap();
                buf.copy_from_slice(b"tick");
                w.commit(0, 4, *index).unwrap();
                *index += 1;
            }
        };
        write_two(&mut index);

        b.connect_control_peer(a_control).unwrap();
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();
        poll_until(|| (sub.synced_index() == index).then_some(()));

        // The application restarts: same daemon, same channel, a fresh `create` and writer.
        assert_eq!(a.create_for_client("md.aapl", options).unwrap(), path);
        write_two(&mut index);

        poll_until(|| (sub.synced_index() == index).then_some(()));
        assert_eq!(sub.rebuilds().gap(), 0);
        assert_eq!(
            sub.rebuilds().diverged(),
            0,
            "an application restart is not a reclaim — the replica must be extended, not rebuilt"
        );
    }

    /// The relocation story end to end: an application pinned to a host that dies must be
    /// able to come back under the same name elsewhere. That needs a reclaim, which needs a
    /// name held by a dead owner to be retired by someone who does not own it.
    ///
    /// The owner here is a node this one has never heard from — the state a survivor is left
    /// in after the owning host is decommissioned.
    #[test]
    fn a_dead_owners_channel_can_be_reclaimed_elsewhere() {
        let node = Node::new(config(101, temp_dir("reclaim-dead")));
        node.registry.lock_safe().merge(ChannelIdentity {
            name: "md.aapl".into(),
            owner: NodeId(999),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1,
            epoch: 0,
            deleted: false,
            member_of: None,
        });

        assert!(node.force_deregister("md.aapl").unwrap());
        assert!(
            node.registry.lock_safe().get("md.aapl").is_none(),
            "the name must be free"
        );

        // The name is now hostable here — as a new incarnation, not a continuation.
        {
            let mut w = node.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            let buf = w.try_reserve(5).unwrap();
            buf.copy_from_slice(b"moved");
            w.commit(0, 5, 0).unwrap();
        }
        let reclaimed = node.registry.lock_safe().get("md.aapl").cloned().unwrap();
        assert_eq!(reclaimed.owner, NodeId(101));
        assert_eq!(
            reclaimed.epoch, 1,
            "a reclaim must be a new incarnation, so subscribers rebuild rather than splice"
        );
        // That incarnation is stamped into the log, which is what a subscriber's replica is
        // later compared against.
        let r = ReaderBuilder::new(node.channel_path("md.aapl").unwrap())
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert_eq!(r.generation(), reclaimed.epoch);
    }

    /// The guards on reclaiming. Taking a name from a node that is merely *quiet* would be
    /// failover — and across a partition it would retire a channel whose owner is alive and
    /// still writing, with the higher-epoch reclaim then winning the merge.
    #[test]
    fn force_deregister_refuses_live_owners_own_channels_and_fresh_daemons() {
        let (a, _a_stream, a_control) = start(111, "reclaim-guard-a");
        let (b, _b_stream, _b_control) = start(112, "reclaim-guard-b");
        {
            let mut w = a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            let buf = w.try_reserve(1).unwrap();
            buf.copy_from_slice(b"x");
            w.commit(0, 1, 0).unwrap();
        }
        b.connect_control_peer(a_control).unwrap();
        poll_until(|| {
            (b.registry.lock_safe().get("md.aapl").is_some()
                && b.dissemination
                    .lock_safe()
                    .live_addr_of(NodeId(111))
                    .is_some())
            .then_some(())
        });

        // A live owner is refused however long the configured floor is — B's `reclaim_after`
        // is zero, so only the liveness check can be stopping this.
        let err = b.force_deregister("md.aapl").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        assert!(err.to_string().contains("live member"), "{err}");

        // The owner must use the ordinary owner-only path for its own channels.
        let err = a.force_deregister("md.aapl").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("use deregister"), "{err}");

        // A daemon that just started has heard from nobody; it must not conclude on that basis
        // that every channel in the registry is abandoned.
        let mut cfg = config(113, temp_dir("reclaim-guard-c"));
        cfg.reclaim_after = Duration::from_secs(3600);
        let fresh = Node::new(cfg);
        fresh.registry.lock_safe().merge(ChannelIdentity {
            name: "theirs".into(),
            owner: NodeId(999),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1,
            epoch: 0,
            deleted: false,
            member_of: None,
        });
        let err = fresh.force_deregister("theirs").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        assert!(err.to_string().contains("unreachable for"), "{err}");

        // An unknown name is simply nothing to do.
        assert!(!fresh.force_deregister("nope").unwrap());
    }

    /// When the owner withdraws a channel, a subscriber must learn that its source is *gone*
    /// rather than merely quiet: the tombstone converges, and the subscription is retired
    /// instead of re-resolving a name the network has agreed no longer exists.
    #[test]
    fn a_tombstone_retires_a_subscriber() {
        let (a, _a_stream, a_control) = start(91, "tombstone-a");
        let (b, _b_stream, _b_control) = start(92, "tombstone-b");
        let n = 4u64;
        {
            let mut w = a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            for i in 0..n {
                let buf = w.try_reserve(4).unwrap();
                buf.copy_from_slice(b"tick");
                w.commit(0, 4, i).unwrap();
            }
        }
        b.connect_control_peer(a_control).unwrap();
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();
        poll_until(|| (sub.synced_index() == n).then_some(()));
        let replica = sub.replica_path().to_path_buf();
        b.subscriptions
            .lock_safe()
            .insert("md.aapl".to_string(), sub);

        // The owner withdraws it through the client RPC.
        let reply = a.handle_request(ClientRequest::Deregister {
            name: "md.aapl".into(),
        });
        assert_eq!(reply, ClientReply::Deregistered { existed: true });

        // B converges on the tombstone and retires its subscription.
        poll_until(|| {
            b.subscriptions
                .lock_safe()
                .get("md.aapl")
                .is_none()
                .then_some(())
        });
        assert!(
            b.subscription_status("md.aapl").is_none(),
            "a retired subscription must not keep reporting as a live source"
        );

        // The replica's files stay: the records it holds are still valid history, and
        // discarding them is the reader's decision, not ours.
        let mut r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let mut seen = 0u64;
        while r.try_read().unwrap().is_some() {
            seen += 1;
        }
        assert_eq!(seen, n, "history already replicated is not thrown away");

        // Deregistering again is not an error — it simply reports nothing was there.
        assert_eq!(
            a.handle_request(ClientRequest::Deregister {
                name: "md.aapl".into()
            }),
            ClientReply::Deregistered { existed: false }
        );
    }

    /// Status must distinguish a source that is merely quiet from one whose replication is
    /// broken — the whole point of reporting liveness separately from progress.
    #[test]
    fn status_reports_progress_and_liveness_separately() {
        let (a, _a_stream, a_control) = start(81, "status-a");
        let (b, _b_stream, _b_control) = start(82, "status-b");
        let n = 5u64;
        {
            let mut w = a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            for i in 0..n {
                let buf = w.try_reserve(4).unwrap();
                buf.copy_from_slice(b"tick");
                w.commit(0, 4, i).unwrap();
            }
        }

        // A hosts it: local, caught up by definition, nothing to lag behind.
        let local = a.subscription_status("md.aapl").unwrap();
        assert!(local.local && local.active && local.owner_live);
        assert_eq!(local.synced, RecordIndex(n));
        assert_eq!(local.owner, a.config.node_id);

        // B replicates it: not local, owner live, progress tracked, no rebuilds.
        b.connect_control_peer(a_control).unwrap();
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();
        poll_until(|| (sub.synced_index() == n).then_some(()));
        b.subscriptions
            .lock_safe()
            .insert("md.aapl".to_string(), sub);

        let remote = poll_until(|| {
            let s = b.subscription_status("md.aapl")?;
            (s.owner_live && s.synced == RecordIndex(n)).then_some(s)
        });
        assert!(!remote.local);
        assert!(remote.active);
        assert_eq!(remote.owner, a.config.node_id);
        assert_eq!(remote.head_at_connect, RecordIndex(n));
        assert_eq!(remote.rebuilds_gap, 0);
        assert_eq!(remote.rebuilds_diverged, 0);
        assert!(
            remote.last_record_at_ms > 0,
            "records arrived, so the live staleness signal must be set"
        );

        // An unknown channel is an error, not a fabricated healthy-looking zero status.
        assert!(b.subscription_status("md.unknown").is_none());
    }

    /// Subscribing to a channel this node hosts must hand back the **origin**, not start a
    /// replication loop against ourselves. An application that consumes the stream it also
    /// produces is the normal case, and a self-replica would double its disk and stream every
    /// record over loopback to arrive at a strictly staler copy of a local file.
    #[test]
    fn subscribing_to_a_locally_hosted_channel_returns_the_origin() {
        let node = Node::new(config(71, temp_dir("self-subscribe")));
        {
            let mut w = node.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            let buf = w.try_reserve(4).unwrap();
            buf.copy_from_slice(b"tick");
            w.commit(0, 4, 0).unwrap();
        }

        let reply = node.handle_request(ClientRequest::Subscribe {
            name: "md.aapl".into(),
            wait_ms: 1000,
        });
        let ClientReply::Subscribed { replica_path } = reply else {
            panic!("expected Subscribed, got {reply:?}");
        };
        assert_eq!(
            Path::new(&replica_path),
            node.channel_path("md.aapl").unwrap(),
            "the client must be pointed at the origin"
        );
        assert!(
            !node.replica_dir("md.aapl").unwrap().exists(),
            "no replica of our own channel should exist"
        );
        assert!(
            node.subscriptions.lock_safe().is_empty(),
            "and no subscription loop should be running"
        );
    }

    /// A subscriber that falls behind the source's retention must rebuild — and the rebuilt
    /// replica starts at the source's `earliest`, **not** at genesis. That is the "full
    /// *retained* history" contract: the records between genesis and `earliest` are gone, and
    /// the replica's headers must say so rather than pretending to start at 0.
    #[test]
    fn a_subscriber_behind_retention_rebuilds_from_earliest() {
        let (a, _a_stream, a_control) = start(61, "retention-a");
        let (b, _b_stream, _b_control) = start(62, "retention-b");

        // First, a short log that B replicates in full.
        let first = 3u64;
        {
            let mut w = a
                .host_channel("md.aapl", 1 << 20, 0, |x| x.keep_files(2))
                .unwrap();
            for i in 0..first {
                let buf = w.try_reserve(4).unwrap();
                buf.copy_from_slice(b"old!");
                w.commit(0, 4, i).unwrap();
            }
        }
        b.connect_control_peer(a_control).unwrap();
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();
        poll_until(|| (sub.synced_index() == first).then_some(()));
        drop(sub); // B stops here, holding records 0..3

        // While B is away the origin rolls repeatedly. `keep_files(2)` prunes each older
        // segment, so `earliest` climbs past the position B is holding.
        let origin = a.channel_path("md.aapl").unwrap();
        let mut index = first;
        {
            let mut w = WriterBuilder::new(&origin)
                .region_size(1 << 20)
                .keep_files(2)
                .build()
                .unwrap();
            for _ in 0..3 {
                w.roll_file().unwrap();
                for _ in 0..2 {
                    let buf = w.try_reserve(4).unwrap();
                    buf.copy_from_slice(b"new!");
                    w.commit(0, 4, index).unwrap();
                    index += 1;
                }
            }
        }
        let earliest = ReaderBuilder::new(&origin)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap()
            .base_record_index();
        assert!(
            earliest > first,
            "retention must have pruned past B's position ({earliest} vs {first})"
        );

        // B comes back asking to resume at 3, which the origin no longer retains.
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();
        poll_until(|| (sub.synced_index() == index).then_some(()));

        assert_eq!(sub.rebuilds().gap(), 1, "one rebuild, caused by retention");
        assert_eq!(
            sub.rebuilds().diverged(),
            0,
            "the name was never reclaimed — this is not divergence"
        );
        assert!(sub.rebuilds().last_at_ms().is_some());

        let replica = b.replica_path("md.aapl").unwrap();
        let r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert_eq!(
            r.base_record_index(),
            earliest,
            "the rebuilt replica must start at the source's earliest, not at genesis"
        );
    }

    /// Names that look like segment filenames must not collide. Channel names may contain
    /// dots (they are the recommended separator), so `md.aapl.1` is both a plausible channel
    /// name and what segment 1 of `md.aapl` used to be called. With a directory per channel the
    /// two cannot meet.
    #[test]
    fn a_channel_named_like_a_segment_does_not_collide() {
        let node = Node::new(config(51, temp_dir("segment-name")));

        // `md.aapl` rolls twice, so segments 1 and 2 exist.
        {
            let mut w = node.host_channel("md.aapl", 1 << 20, 0, |b| b).unwrap();
            for i in 0..3u64 {
                let buf = w.try_reserve(4).unwrap();
                buf.copy_from_slice(b"base");
                w.commit(0, 4, i).unwrap();
                w.roll_file().unwrap();
            }
        }
        // A channel whose *name* is what a segment used to be called.
        {
            let mut w = node.host_channel("md.aapl.1", 1 << 20, 0, |b| b).unwrap();
            let buf = w.try_reserve(5).unwrap();
            buf.copy_from_slice(b"other");
            w.commit(0, 5, 0).unwrap();
        }

        // Distinct directories, so neither adopted the other's files.
        let mut r = ReaderBuilder::new(node.channel_path("md.aapl.1").unwrap())
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let first = r.try_read().unwrap().unwrap().payload().to_vec();
        assert_eq!(first, b"other", "md.aapl.1 must hold its own records");
        assert!(r.try_read().unwrap().is_none(), "and only its own");
    }

    /// Retention unlinks segment 0 — the channel's *unsuffixed* file — so a rolled channel
    /// past its retention window has no file bearing its name. Its directory still does, which
    /// is what lets a restart re-host it instead of inventing channels named after the
    /// surviving segments.
    #[test]
    fn a_rolled_and_pruned_channel_is_rehosted_after_restart() {
        let dir = temp_dir("pruned-restart");
        let node = Node::new(config(52, dir.clone()));
        {
            let mut w = node
                .host_channel("md.aapl", 1 << 20, 0, |b| b.keep_files(2))
                .unwrap();
            for i in 0..4u64 {
                let buf = w.try_reserve(4).unwrap();
                buf.copy_from_slice(b"tick");
                w.commit(0, 4, i).unwrap();
                w.roll_file().unwrap();
            }
        }
        assert!(
            !node.channel_path("md.aapl").unwrap().exists(),
            "retention should have pruned segment 0, the unsuffixed file"
        );

        // A fresh daemon on the same data dir reconstructs from what is on disk.
        let restarted = Node::new(config(52, dir));
        restarted.reconstruct_from_disk();
        assert!(
            restarted.registry.lock_safe().get("md.aapl").is_some(),
            "the channel must be re-hosted under its own name"
        );
        for phantom in ["md.aapl.1", "md.aapl.2", "md.aapl.3", "log", "log.1"] {
            assert!(
                restarted.registry.lock_safe().get(phantom).is_none(),
                "a surviving segment must not be registered as a channel: {phantom}"
            );
        }
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
        let mut client = stream::subscribe(conn, "md.aapl", RecordIndex(0), 0, &replica).unwrap();
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
    fn reconstruct_does_not_resurrect_a_deregistered_channel() {
        let dir = temp_dir("noresurrect");
        // Session 1: one channel kept, one deregistered (which deletes its files).
        {
            let node = Node::new(config(1, dir.clone()));
            node.create_for_client("md.keep", ChannelOptions::default())
                .unwrap();
            node.create_for_client("md.gone", ChannelOptions::default())
                .unwrap();
            assert!(node.deregister("md.gone").unwrap());
        }
        // Session 2: fresh node (empty registry) on the same data_dir reconstructs from disk.
        let node2 = Node::new(config(1, dir.clone()));
        node2.reconstruct_from_disk();
        assert!(
            node2.registry.lock_safe().get("md.keep").is_some(),
            "a live channel is re-registered on restart"
        );
        assert!(
            node2.registry.lock_safe().get("md.gone").is_none(),
            "a deregistered channel is NOT resurrected (its files were deleted)"
        );
    }

    #[test]
    fn deregister_topic_retires_mux_and_deletes_files() {
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
        // The topic channel is deleted from disk, so a later restart won't resurrect it.
        // (The terminal marker `finish` writes is best-effort for still-connected subscribers;
        // Mux::finish's terminal emission is covered by a mux-level test.)
        assert!(!topic_path.exists(), "retired topic's files are removed");
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
