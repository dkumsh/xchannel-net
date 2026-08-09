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
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use xchannel::{Writer, WriterBuilder};
use xchannel_net_core::codec::{self, decode_client_request, encode_client_reply};
use xchannel_net_core::dissemination::Dissemination;
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::mux::{self, Mux};
use xchannel_net_core::stream::{
    self, ChannelSource, ClientPollItem, ServerPollItem, SubscribeError, accept_subscription,
};
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

/// Records a single duty-cycle poll-item may move per turn. Bounded **per poll-item**, not just
/// per member (`doc/TOPICS.md` §4.1 budget coupling): everything in the loop shares its cycles, so
/// one saturated subscription or topic must not head-of-line-block the rest for a full drain.
const MAX_BATCH_PER_POLL_ITEM: usize = 256;

/// How long establishment waits to resolve a channel before giving up and retrying next tick.
const RESOLVE_TIMEOUT: Duration = Duration::from_millis(200);

/// Bounded dial, so an unreachable owner costs one establishment thread a moment, not minutes.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// Bounded handshake read. A peer that connects and then says nothing must not pin the thread
/// performing the handshake — which, now that handshakes are not one-thread-per-connection for
/// their whole lifetime, would otherwise be a cheap way to exhaust them.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

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
/// `[A-Za-z0-9._-]`, length 1..=[`xchannel::CHANNEL_NAME_MAX`], and **no leading dot** — which
/// rejects path traversal (`/`, `\`, `..`), the current dir (`.`), and collisions with the
/// internal `.replicas` subtree, none of which can appear.
///
/// The length bound is xchannel's header field rather than a number of our own, because every
/// channel now **stamps its name into that header** — the limit and the field it has to fit are
/// the same fact, and writing it twice is how they drift. The allowlist is ASCII, so bytes and
/// chars agree. 48 bytes is roughly five dotted segments (`fills.prod.options-mm` is 21).
fn validate_channel_name(name: &str) -> io::Result<()> {
    let valid = (1..=xchannel::CHANNEL_NAME_MAX).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "channel name must be 1..={} chars of [A-Za-z0-9._-] with no leading dot",
                xchannel::CHANNEL_NAME_MAX
            ),
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

/// How the mux loop waits when a poll finds nothing to merge.
///
/// The merge **must** be a poll loop: a member is an mmap'd log written by another process, with
/// no notification to wait on, and there are N of them — so there is nothing to block on that
/// wouldn't starve the other members. That makes "what to do when idle" the *only* lever on merge
/// latency, and a flat sleep spends it badly. The original fixed 5 ms tick cost a record arriving
/// just after a poll up to 5 ms before it was even written to the topic: broker-class latency on
/// the one path that exists for aggregation, in a system whose whole premise is that the manager
/// is never in the way.
///
/// So: escalating backoff, the shape xchannel's own `Reader::wait_for_message` already uses
/// (1 µs doubling to 10 ms) and the shape Aeron calls an `IdleStrategy` — stay hot while records
/// are flowing, decay toward a cheap park when they are not.
///
/// **The CPU trade, stated plainly.** A busy mux never sleeps, and a *quiet* one decays to the same
/// 5 ms park the old loop took unconditionally — so neither extreme costs more than before. The
/// cost lands in between: a **bursty** topic spends its gaps spinning and yielding rather than
/// sleeping through them, which is latency bought with cycles. That is the intended trade for a
/// merge path, and it is tunable in both directions — raise `min_park` or cut the spin/yield counts
/// to give cycles back, or set `max_park` to zero to never park at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MuxIdle {
    /// Consecutive idle rounds spent on [`spin_loop`](std::hint::spin_loop) before yielding.
    pub max_spins: u32,
    /// Idle rounds spent on [`yield_now`](std::thread::yield_now) after the spins, before parking.
    pub max_yields: u32,
    /// First park, doubled on each further idle round.
    pub min_park: Duration,
    /// Ceiling on the park. **Zero means never park** — keep yielding, for a core dedicated to a
    /// latency-critical topic.
    pub max_park: Duration,
}

impl Default for MuxIdle {
    /// Tuned so a member producing faster than ~10 kHz keeps the loop inside the spin/yield phases
    /// (merge latency in microseconds), while a topic idle for ~10 ms decays to the 5 ms park that
    /// used to be the *unconditional* interval — so worst-case idle CPU is unchanged.
    fn default() -> Self {
        Self {
            max_spins: 512,
            max_yields: 128,
            min_park: Duration::from_micros(50),
            max_park: Duration::from_millis(5),
        }
    }
}

/// What [`MuxIdle`] prescribes for one idle round. Split out from the doing so the escalation is
/// testable without measuring elapsed time.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IdleAction {
    Spin,
    Yield,
    Park(Duration),
}

impl MuxIdle {
    /// What to do on consecutive idle round `round` (0-based; reset to 0 whenever a poll merges).
    fn action(&self, round: u32) -> IdleAction {
        if round < self.max_spins {
            return IdleAction::Spin;
        }
        let after_spins = round - self.max_spins;
        if after_spins < self.max_yields || self.max_park.is_zero() {
            return IdleAction::Yield;
        }
        // Double from `min_park`, clamped — `1 << 20` already saturates any sane `max_park`, and
        // the shift itself must not overflow on a loop that has been idle for hours.
        let steps = (after_spins - self.max_yields).min(20);
        IdleAction::Park(
            self.min_park
                .saturating_mul(1u32 << steps)
                .min(self.max_park),
        )
    }

    fn wait(&self, round: u32) {
        match self.action(round) {
            IdleAction::Spin => std::hint::spin_loop(),
            IdleAction::Yield => std::thread::yield_now(),
            IdleAction::Park(d) => std::thread::sleep(d),
        }
    }
}

/// What [`Node::reconstruct_from_disk`] rebuilt from the data dir at startup.
///
/// `skipped` counts channels found on disk that could not be re-hosted — a channel whose name is
/// already owned elsewhere, or whose files won't open. Worth surfacing rather than swallowing:
/// reconstruction is best-effort per channel, so a non-zero count is the only sign that a channel
/// present on disk is *not* being served.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Reconstructed {
    pub topics: usize,
    pub origins: usize,
    pub skipped: usize,
}

/// Observability snapshot of a hosted topic (§8).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopicStatus {
    pub members: Vec<MemberInfo>,
    pub topic_head: u64,
    pub gaps_emitted: u64,
    pub slot_table_version: u64,
    /// This topic merges on a thread of its own rather than on the shared duty cycle (§4.1
    /// rung 2). Reported so an operator can confirm the promotion config actually took effect.
    pub promoted: bool,
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
    /// Muxes for topics this node owns, keyed by topic name. **Each mux has its own lock**: the
    /// map lock is taken only long enough to clone a handle out, never across mux IO. A merge is
    /// the one thing in the daemon that does unbounded work while holding a lock, so a shared lock
    /// would make every topic's poll a head-of-line block on every other topic — and on
    /// `create_topic`, `topic_status`, and the maintenance loop's attach pass. The hotter the poll
    /// loop, the worse that gets, and the poll loop is now as hot as records arriving.
    ///
    /// **Lock order: map → mux, never the reverse.** Nothing may take the map lock while holding a
    /// mux lock. Go through [`mux_of`](Self::mux_of) / [`mux_handles`](Self::mux_handles) rather
    /// than working under the map guard, and the rule holds by construction.
    muxes: Arc<Mutex<HashMap<String, Arc<Mutex<Mux>>>>>,
    /// Per-topic member-reap threshold (§6.1), for topics that opted in (`member_reap_after`).
    /// Absent ⇒ never reap.
    topic_reap: Arc<Mutex<HashMap<String, Duration>>>,
    /// When a member's owner was first observed unreachable, keyed by `(name, epoch)` — drives
    /// the reaper's "dead beyond a threshold" decision. Keyed by incarnation so two generations
    /// of a name never share a timer. Cleared when the owner is live again.
    member_dead_since: Arc<Mutex<HashMap<(String, u64), Instant>>>,
    /// Count of live inbound stream/client connections (capped at [`MAX_CONNECTIONS`]).
    conns: Arc<AtomicUsize>,
    /// Where establishment hands finished connections to the duty cycle.
    duty: Arc<DutyInbox>,
    /// Subscriptions the conductor re-establishes when their connection drops.
    ///
    /// Held **weakly and separately from `subscriptions`** on purpose. `subscriptions` is the
    /// client-facing lookup map, and a caller of `Node::subscribe` need not file its handle there
    /// — under the old thread-per-subscription model such a subscription still self-healed,
    /// because the healing lived in its own thread. Keying the conductor off the map instead would
    /// have quietly made self-healing conditional on registration. Weak so that dropping the
    /// handle is enough to stop servicing it, with no deregistration step to forget.
    conducted: Arc<Mutex<Vec<(String, Weak<SubShared>)>>>,
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
            duty: Arc::new(DutyInbox::default()),
            conducted: Arc::new(Mutex::new(Vec::new())),
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
        self.disseminate_tombstone(&tombstone)?;
        self.retire_subscription(name);
        Ok(true)
    }

    /// Disseminate a tombstone this node just produced: publish it to the **local discovery log**
    /// and announce it to **peers**. Both, always — which is why there is one function rather than
    /// two calls at each site.
    ///
    /// The halves are easy to mistake for alternatives, and they are not. A peer that receives the
    /// announcement merges it and republishes it into *its own* discovery log
    /// ([`merge_and_publish`](Self::merge_and_publish)), so omitting the local publish leaves
    /// watchers on **this** node — and only this node — holding a phantom entry for a channel the
    /// whole network agrees is gone. That asymmetry makes the omission unusually hard to see: every
    /// other daemon reports the removal correctly.
    ///
    /// Registration takes a different path on purpose: `Registry::merge_tracked` decides whether
    /// anything actually changed, so [`claim_name`](Self::claim_name) publishes from that verdict
    /// and [`announce_hosted`](Self::announce_hosted) only announces. A tombstone has no such
    /// question — it was produced locally and is by construction a change.
    fn disseminate_tombstone(&self, tombstone: &ChannelIdentity) -> io::Result<()> {
        self.publish_change(&self.change_of(tombstone));
        self.dissemination
            .lock_safe()
            .announce(std::slice::from_ref(tombstone))
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
            .channel_name(name)?
            .build()?;
        // The `configure` closure is opaque, so any `file_roll_size`/`keep_files` it sets
        // can't be read back to advertise in the `SubscribeAck`. We therefore announce
        // `(0, 0)` (no rolling / unlimited) — which matches the WriterBuilder default, so
        // for an unconfigured channel origin and replicas agree. But if the closure *does*
        // set rolling/retention, the origin rolls-and-prunes while its replicas never prune.
        // (They do still *roll*: a replica follows the origin's boundaries via
        // `RecordFrame::starts_segment` regardless of what it was told. Only `keep_files`
        // is lost, so the growth is unbounded segments rather than one unbounded file.)
        // Safe direction — replicas never drop records, only over-retain. Clients that need
        // replicas to inherit disk bounds should use the client RPC (`create_for_client` /
        // `ChannelOptions`), which propagates both fields.
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
            .generation(identity.epoch)
            // And the name, for the same reason one level up: it is what makes the log say which
            // *channel* it is, rather than leaving that to the directory it happens to sit in.
            .channel_name(name)?;
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
        self.hosted.lock_safe().remove(name);
        self.disseminate_tombstone(&tombstone)?;
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
        // The mux's writer must be given the **full** channel configuration, not just the header
        // geometry: `create_for_client` precreated the file with the client's rolling/retention,
        // but those live on the `Writer` xchannel drops there, so a mux opened with geometry alone
        // would leave the topic growing as one unbounded file.
        let mux = Arc::new(Mutex::new(Mux::open(
            &path,
            name,
            &mux::TopicGeometry::from(&options.channel),
            batch,
        )?));
        self.muxes
            .lock_safe()
            .insert(name.to_string(), Arc::clone(&mux));
        self.promote_if_configured(name, &mux);
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
        if let Some(mux) = self.mux_of(topic) {
            let epoch = self
                .registry
                .lock_safe()
                .get_raw(member)
                .map(|id| id.epoch)
                .unwrap_or(0);
            mux.lock_safe().add_member(member, epoch, &member_path)?;
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
        for (topic, mux) in self.mux_handles() {
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

                if mux.lock_safe().has_member(&m.name, m.epoch) {
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
                let _ = mux.lock_safe().add_member(&m.name, m.epoch, &path);
            }

            // Detach members the registry says have **left**: clean-leave drain → MemberClosed,
            // then stop replicating a remote one (§6.1). Only a *positive* signal retires a
            // member — a tombstone, or its `member_of` moved to another topic. Mere **absence**
            // from the registry must NOT retire it (the registry may just be incomplete — e.g.
            // right after a restart, before reconstruct/gossip catches up); otherwise we'd drain
            // a member that never left.
            let live_set: std::collections::HashSet<(String, u64)> =
                live.iter().map(|m| (m.name.clone(), m.epoch)).collect();
            let attached: Vec<(String, u64)> = mux
                .lock_safe()
                .members()
                .into_iter()
                .map(|(n, e, _)| (n, e))
                .collect();
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
                let _ = mux.lock_safe().remove_member(&name, epoch);
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
                    let tombstone = self.registry.lock_safe().reap(&m.name);
                    if let Some(tombstone) = tombstone {
                        let _ = self.disseminate_tombstone(&tombstone);
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
        let mut total = 0;
        let mut failure = None;
        // Handles first, map lock released — then each topic merges under its own lock, so a busy
        // topic delays neither its neighbours nor anything else that needs the map.
        for (name, mux) in self.mux_handles() {
            // A promoted topic has a thread of its own (§4.1 rung 2). Skipping it here is what
            // makes that thread *dedicated* rather than a second poller of a shared topic — and
            // what keeps the promoted topic's records off the shared loop's budget, which is the
            // whole reason to promote one.
            if self.config.promoted_topics.contains(&name) {
                continue;
            }
            match mux.lock_safe().poll() {
                Ok(n) => total += n,
                // One topic failing must not abandon the rest of the sweep: they are independent
                // logs, and the old `?` let a single unopenable member stall every other topic.
                Err(e) => {
                    failure.get_or_insert(e);
                }
            }
        }
        match failure {
            // Report only when nothing merged anywhere, which is the sole case where the caller
            // behaves differently (back off rather than stay hot). Genuine per-topic error
            // reporting waits on the daemon having any logging at all.
            Some(e) if total == 0 => Err(e),
            _ => Ok(total),
        }
    }

    /// The mux for `topic`, if this node hosts it. Takes the map lock only to clone the handle.
    fn mux_of(&self, topic: &str) -> Option<Arc<Mutex<Mux>>> {
        self.muxes.lock_safe().get(topic).cloned()
    }

    /// A handle per hosted topic, with the map lock released before any of them is used.
    fn mux_handles(&self) -> Vec<(String, Arc<Mutex<Mux>>)> {
        self.muxes
            .lock_safe()
            .iter()
            .map(|(name, mux)| (name.clone(), Arc::clone(mux)))
            .collect()
    }

    /// Give `topic` its own thread if the operator configured it as promoted (§4.1 rung 2).
    /// Called wherever a mux enters the map, so creation and restart-reconstruction behave alike.
    fn promote_if_configured(&self, topic: &str, mux: &Arc<Mutex<Mux>>) {
        if !self.config.promoted_topics.contains(topic) {
            return;
        }
        let (node, topic, mux) = (self.clone(), topic.to_string(), Arc::clone(mux));
        std::thread::spawn(move || node.run_promoted_mux(topic, mux));
    }

    /// Poll one promoted topic on this thread until it stops being this node's mux for that name.
    ///
    /// Exits on **identity**, not on absence: the thread holds the very `Arc` it was promoted for
    /// and stops when the map no longer maps `topic` to *that* mux. Checking only "is the name
    /// still hosted" would leave a stale thread polling a retired mux alongside the new one after
    /// a retire-and-recreate.
    fn run_promoted_mux(&self, topic: String, mux: Arc<Mutex<Mux>>) {
        let idle = self.config.mux_idle;
        let mut round = 0u32;
        loop {
            match self.mux_of(&topic) {
                Some(current) if Arc::ptr_eq(&current, &mux) => {}
                _ => return, // retired, or replaced by a topic with its own thread
            }
            match mux.lock_safe().poll() {
                Ok(n) if n > 0 => round = 0,
                _ => {
                    idle.wait(round);
                    round = round.saturating_add(1);
                }
            }
        }
    }

    /// Drive the muxes and nothing else, on their own thread.
    ///
    /// `xchanneld` does **not** use this: it runs [`run_duty_cycle`](Self::run_duty_cycle), where
    /// muxes are poll-items alongside replication (§4.1). This remains the second rung of §4.1's
    /// promotion path — a mux engine hosted outside the shared loop, in a standalone process or a
    /// thread of its own — and is what an embedder driving `Mux` directly would use.
    pub fn run_mux(&self, idle: MuxIdle) {
        let mut round = 0u32;
        loop {
            match self.poll_muxes() {
                // Merged something: go straight back round. A producing member therefore never
                // waits on the clock — only on the merge itself.
                Ok(n) if n > 0 => round = 0,
                // Nothing to merge, or the poll failed. Back off either way: a persistent error
                // (a member file that will not open) must not become a hot loop.
                _ => {
                    idle.wait(round);
                    round = round.saturating_add(1);
                }
            }
        }
    }

    /// Number of members currently attached to a hosted topic's mux (for tests/observability).
    pub fn topic_member_count(&self, topic: &str) -> Option<usize> {
        self.mux_of(topic).map(|m| m.lock_safe().members().len())
    }

    /// Restart reconstruction (`DESIGN.md` §5.2, `doc/RESTART.md`): scan `data_dir`, re-host every
    /// topic found on disk, and re-attach its members — with no persisted marker. A topic is
    /// identified by content (a decodable slot table, via `mux::topic_config`) and its geometry +
    /// membership come from that self-describing slot table. Call once at startup. Best-effort:
    /// a channel that fails to re-host is skipped (a client re-issuing `create_topic` recovers it).
    ///
    /// **Call before any plane starts serving.** Until this returns, the registry is empty and the
    /// mux map holds no topics, so a client or peer answered earlier would be told this node hosts
    /// nothing — a wrong answer, not a slow one. Returns what it rebuilt so a caller can report it:
    /// the scan is O(retained records) (`doc/RESTART.md`), so on a large data dir this is
    /// noticeable startup latency that would otherwise look like a hang.
    pub fn reconstruct_from_disk(&self) -> Reconstructed {
        let Ok(rd) = std::fs::read_dir(&self.config.data_dir) else {
            return Reconstructed::default();
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
        let mut out = Reconstructed::default();
        for (name, cfg) in &topics {
            if self.rehost_topic(name, cfg).is_ok() {
                out.topics += 1;
            } else {
                out.skipped += 1;
            }
        }
        // Then re-register the remaining plain origins (skips anything already hosted).
        for name in &origins {
            if self.reregister_origin(name).is_ok() {
                out.origins += 1;
            } else {
                out.skipped += 1;
            }
        }
        out
    }

    /// The channel name stamped in the log's own header at `path`.
    ///
    /// This is what closes the last place the daemon trusted something other than its own files.
    /// Every other fact about a channel is recovered from its content — geometry from the header,
    /// absolute indices from `base_record_index`, incarnation from `generation`, a topic's members
    /// and cursors from its slot table — but the *name* came from the directory the log sat in. A
    /// data dir that has been migrated, restored, or hand-edited could therefore serve one
    /// channel's records under another's name, with nothing to catch it: the geometry is valid,
    /// the log is well formed, and `generation` agrees (it travels with the file, so a renamed
    /// directory looks perfectly consistent).
    fn stamped_name(path: &Path) -> io::Result<String> {
        Ok(xchannel::ReaderBuilder::new(path)
            .late_join()
            .build()?
            .channel_name()
            .into_owned())
    }

    /// Refuse a log whose header says it is a different channel from the directory it was found
    /// in. An unstamped log (all zeros) fails this too, which is deliberate: a guarantee that
    /// holds only for logs written by a new enough daemon is not one you can rely on.
    fn verify_stamped_name(path: &Path, expected: &str) -> io::Result<()> {
        let stamped = Self::stamped_name(path)?;
        if stamped != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{path:?} holds channel '{stamped}', but its directory says '{expected}' — \
                     refusing to serve one channel's records under another's name"
                ),
            ));
        }
        Ok(())
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
        Self::verify_stamped_name(&path, name)?;
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
        Self::verify_stamped_name(&path, name)?;
        let identity = self.claim_name(name, cfg.geometry.region_size, cfg.geometry.mtu, None)?;
        // A topic's disk bounds *are* recoverable (unlike a plain origin's): they ride the slot
        // table. So re-host with them — both on the writer, so the topic keeps rolling and
        // pruning, and in the announcement, so subscribers' replicas keep inheriting the bounds.
        self.announce_hosted(
            &identity,
            path.clone(),
            cfg.geometry.file_roll_size,
            cfg.geometry.keep_files,
        )?;
        let mux = Arc::new(Mutex::new(Mux::open(
            &path,
            name,
            &cfg.geometry,
            MAX_BATCH_PER_MEMBER,
        )?));
        self.muxes
            .lock_safe()
            .insert(name.to_string(), Arc::clone(&mux));
        self.promote_if_configured(name, &mux);
        for (member, epoch) in &cfg.members {
            // A member with a **local origin** is one we own: re-register it with `member_of`
            // (recovering its geometry via the header accessor) so it's back in the topic's live
            // set — otherwise the detach pass would drain a member that never left. Then attach
            // it from the origin. A member with only a **replica** is remote: attach the replica
            // and let peer anti-entropy restore its `member_of` (it's not ours to register).
            let origin = self
                .channel_path(member)
                .ok()
                .filter(|p| p.exists())
                .filter(|p| Self::verify_stamped_name(p, member).is_ok());
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
                let attached = mux.lock_safe().add_member(member, *epoch, &cand).is_ok();
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
        // Both guards are statement temporaries, so neither the map lock nor the mux lock is
        // held while the closure below takes the registry and dissemination locks.
        let mux_status = self.mux_of(topic)?.lock_safe().status();
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
                promoted: self.config.promoted_topics.contains(topic),
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
        let Some(mux) = mux else {
            return Ok(false);
        };
        // Removed from the map, but a concurrent poll may still hold a handle it sampled a moment
        // ago. `finish` retires the engine itself, so that poll merges nothing past the terminal
        // marker rather than racing us.
        mux.lock_safe().finish()?; // drain all members + terminal marker
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
            self.disseminate_tombstone(&tombstone)?;
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

    /// Accept stream connections forever, handshaking each and handing the result to the duty
    /// cycle as a poll-item.
    ///
    /// The handshake runs on a **transient** thread that exits as soon as it has one: it resolves
    /// the channel, decides `Gap`/`Diverged`, and seeks forward to the subscriber's resume index,
    /// which is unbounded work that must happen neither on the accept path (where it would delay
    /// every other subscriber's connect) nor on the duty cycle (where it would stall every
    /// poll-item). The *connection* then outlives the thread — that is the difference from
    /// thread-per-connection, and the whole point of §4.1.
    pub fn serve_stream(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let Some(guard) = self.acquire_conn() else {
                continue; // at capacity — drop the connection
            };
            let hosted = Arc::clone(&self.hosted);
            let duty = Arc::clone(&self.duty);
            std::thread::spawn(move || {
                // A peer that connects and then says nothing must not pin this thread.
                if conn.set_read_timeout(Some(HANDSHAKE_TIMEOUT)).is_err() {
                    return;
                }
                let resolve = |name: &str| hosted.lock_safe().get(name).cloned();
                let Ok(server) = accept_subscription(conn, resolve) else {
                    return; // guard drops here, releasing the connection slot
                };
                if let Ok(item) = ServerPollItem::adopt(server) {
                    // The slot stays taken for as long as the poll-item lives, not just for as
                    // long as this thread does.
                    duty.servers.lock_safe().push(HostedServer {
                        item,
                        _guard: guard,
                    });
                }
            });
        }
    }

    // ---------------- control plane (gossip) ----------------

    /// Bind the control-plane listener.
    pub fn bind_control(&self) -> io::Result<TcpListener> {
        let listener = TcpListener::bind(self.config.control_addr)?;
        // Advertise the address that actually accepts links, not the configured one — with `:0`
        // they differ, and a peer dialling the configured port would reach nothing.
        let addr = listener.local_addr()?;
        self.dissemination.lock_safe().set_self_control_addr(addr);
        Ok(listener)
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
    /// Whether to dial `addr` — no if a dial is already outstanding or in place, and no if we
    /// already hold a link to whatever node lives there.
    ///
    /// Checking by **node identity**, not just by dial address, is what stops the two from
    /// diverging: an inbound link has no dial address, so address-based tracking alone would call
    /// its peer unconnected and dial it a second time. An address we have never identified is
    /// always worth trying — that is the bootstrap case for a seed.
    fn should_dial(&self, addr: SocketAddr) -> bool {
        let d = self.dissemination.lock_safe();
        if d.is_connected(addr) {
            return false;
        }
        match d.node_at(addr) {
            Some(node) => !d.linked_nodes().contains(&node),
            None => true,
        }
    }

    /// Dial `addr` as a peer, best-effort.
    fn dial_peer(&self, addr: SocketAddr) {
        if let Ok(conn) = TcpTransport::connect_timeout(&addr, CONNECT_TIMEOUT) {
            let snapshot = self.registry_snapshot();
            let _ = self
                .dissemination
                .lock_safe()
                .add_outbound_peer(conn, addr, &snapshot);
        }
    }

    /// Dial peers we have *learned about* but hold no link to, so a seed graph closes itself into
    /// a full mesh.
    ///
    /// **Both ends of a pair dial.** Electing one — say the lower `NodeId` — looks tidier and is
    /// wrong: the election happens before anyone knows whether the elected node can actually reach
    /// the other. Under asymmetric reachability (a firewall, a NAT) it can hand the job to the
    /// node that cannot dial, and the pair then never links even though the other direction would
    /// have worked first time. So both dial, and the resulting duplicate is collapsed afterwards
    /// by `dedup_links`, which can decide it knowing who is actually reachable.
    pub fn connect_learned_peers(&self) {
        let candidates = self.dissemination.lock_safe().unconnected_peers();
        for (_, control_addr) in candidates {
            if self.should_dial(control_addr) {
                self.dial_peer(control_addr);
            }
        }
    }

    pub fn connect_seeds(&self) {
        for addr in self.config.seeds.clone() {
            // Same identity check as a learned peer. Without it, a seed link that lost the
            // duplicate tie-break would be re-dialled every tick and dropped again every tick.
            if self.should_dial(addr) {
                self.dial_peer(addr);
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
            self.connect_learned_peers();
            let pumped = {
                let mut d = self.dissemination.lock_safe();
                let _ = d.emit_heartbeat();
                // Forward peer knowledge learned since the last tick, so the mesh keeps closing
                // itself; only knowledge that was *new* to us is queued, so this goes quiet.
                d.relay_hints();
                // Collapse any duplicate links the cross-dial race produced.
                d.dedup_links();
                d.pump()?
            };
            if !pumped.is_empty() {
                let mut retired = Vec::new();
                for (from, id) in pumped {
                    let name = id.name.clone();
                    let merged = self.merge_and_publish(id);
                    // **Relay on change.** Without this a delta reaches only the originator's
                    // direct peers and a node two hops away stays ignorant until it opens a fresh
                    // link. Relaying only when the merge actually moved our map is what makes it
                    // terminate: the registry merge is a total order and idempotent, so a given
                    // winning state can change a given node's map at most once, whatever cycles
                    // the topology has.
                    if merged.changed {
                        let _ = self
                            .dissemination
                            .lock_safe()
                            .relay(from, std::slice::from_ref(&merged.winner));
                    }
                    if merged.winner.deleted {
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
            // Reconnect any subscription the duty cycle dropped — the self-healing half.
            self.service_subscriptions();
            std::thread::sleep(interval);
        }
    }

    // ---------------- discovery ----------------

    /// Merge into the registry and publish the result to discovery **iff the map changed**.
    /// Anti-entropy re-merges a peer's whole registry on every reconnect, so publishing per
    /// merge rather than per change would turn each reconnect into a storm of no-ops.
    fn merge_and_publish(&self, incoming: ChannelIdentity) -> crate::registry::Merged {
        let merged = self.registry.lock_safe().merge_tracked(incoming);
        if merged.changed {
            self.publish_change(&self.change_of(&merged.winner));
        }
        merged
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
        let shared = Arc::new(SubShared {
            replica_path,
            synced: AtomicU64::new(0),
            head_at_connect: AtomicU64::new(0),
            last_record_at_ms: AtomicU64::new(0),
            rebuilds: RebuildStats::default(),
            stopped: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            establishing: AtomicBool::new(false),
        });
        self.conducted
            .lock_safe()
            .push((name.to_string(), Arc::downgrade(&shared)));
        // Establish inline so a caller that has just been handed a `Subscription` is already
        // replicating, rather than waiting for the conductor's next tick. Reconnects are the
        // conductor's job (`service_subscriptions`).
        self.spawn_establish(name, &shared);
        Ok(Subscription { shared })
    }

    /// (Re)establish any subscription that is wanted but not currently connected. Called on the
    /// conductor tick — this is the self-healing half of a subscription, the part that used to be
    /// the reconnect half of what used to be one thread per subscription.
    fn service_subscriptions(&self) {
        let wanted: Vec<(String, Arc<SubShared>)> = {
            let mut conducted = self.conducted.lock_safe();
            // Forget subscriptions whose handle is gone or that have been stopped.
            conducted.retain(|(_, weak)| {
                weak.upgrade()
                    .is_some_and(|s| !s.stopped.load(Ordering::Relaxed))
            });
            conducted
                .iter()
                .filter_map(|(name, weak)| weak.upgrade().map(|s| (name.clone(), s)))
                .collect()
        };
        for (name, shared) in wanted {
            if shared.stopped.load(Ordering::Relaxed) || shared.connected.load(Ordering::Acquire) {
                continue;
            }
            self.spawn_establish(&name, &shared);
        }
    }

    /// Run one establishment attempt on a **transient thread**.
    ///
    /// Establishment is blocking by nature — resolve, dial, one frame each way, and a `skip_to`
    /// that reads forward to the resume index — and none of it may happen on the duty cycle, where
    /// a single unreachable peer or a long seek would stall every other poll-item. Nor on the
    /// conductor, for the same reason at a smaller scale. The thread exists only for the
    /// handshake and hands the finished connection to the duty cycle; it is not a
    /// thread-per-connection, which is what §4.1 set out to remove.
    fn spawn_establish(&self, name: &str, shared: &Arc<SubShared>) {
        // One attempt at a time per subscription.
        if shared
            .establishing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let (node, name, shared) = (self.clone(), name.to_string(), Arc::clone(shared));
        std::thread::spawn(move || {
            node.establish(&name, &shared);
            shared.establishing.store(false, Ordering::Release);
        });
    }

    /// One attempt to connect and hand a subscription's sink to the duty cycle. Failures are
    /// silent and simply retried on a later conductor tick, exactly as the old loop's backoff did.
    fn establish(&self, name: &str, shared: &Arc<SubShared>) {
        if shared.stopped.load(Ordering::Relaxed) || shared.connected.load(Ordering::Acquire) {
            return;
        }
        // Re-resolve each attempt — the owner's address may have changed.
        let Ok((id, addr)) = self.resolve(name, Some(RESOLVE_TIMEOUT)) else {
            return;
        };
        // Resume from the replica's current head (0 if it doesn't exist yet), carrying the
        // incarnation that replica holds so the source can refuse a resume across a reclaim
        // instead of splicing two logs.
        let (from, generation) = self
            .replica_position(&shared.replica_path, id.region_size)
            .unwrap_or((RecordIndex(0), 0));
        shared.synced.store(from.0, Ordering::Relaxed);

        let Ok(conn) = TcpTransport::connect_timeout(&addr, CONNECT_TIMEOUT) else {
            return;
        };
        // Bound the handshake: a peer that accepts and then says nothing must not pin this thread.
        if conn.set_read_timeout(Some(HANDSHAKE_TIMEOUT)).is_err() {
            return;
        }
        match stream::subscribe(conn, name, from, generation, &shared.replica_path) {
            Ok(client) => {
                shared
                    .head_at_connect
                    .store(client.head().0, Ordering::Relaxed);
                if let Ok(item) = ClientPollItem::adopt(client) {
                    // Publish *before* handing the item over, so the conductor cannot decide this
                    // subscription is idle and start a second connection alongside it.
                    shared.connected.store(true, Ordering::Release);
                    self.duty.clients.lock_safe().push(PolledSub {
                        shared: Arc::clone(shared),
                        item,
                    });
                }
            }
            Err(SubscribeError::Rebuild { diverged, .. }) => {
                // The source cannot extend this replica — it is behind retention, or it belongs to
                // a previous incarnation of the name. Retrying the same position would loop
                // forever (the answer will not change), so discard it and let the next attempt
                // subscribe from scratch. Only ever taken for this classified failure: doing it on
                // a transient error would throw away a whole channel's history over a dropped
                // connection.
                //
                // Safe to delete the files here precisely because `connected` is false: the duty
                // cycle holds no writer for this replica.
                if let Ok(dir) = self.replica_dir(name) {
                    let _ = std::fs::remove_dir_all(&dir);
                    // Recreate it: the next attempt's sink opens a writer *inside* this directory
                    // and will not create it.
                    let _ = ensure_private_dir(&dir);
                }
                shared.synced.store(0, Ordering::Relaxed);
                shared.rebuilds.record(diverged);
            }
            Err(_) => {}
        }
    }

    /// **The duty cycle** (`doc/TOPICS.md` §4.1): one thread polling every replication source,
    /// replication sink and mux as peer poll-items, each bounded to
    /// [`MAX_BATCH_PER_POLL_ITEM`] records per turn so none can head-of-line-block the others for
    /// a full drain.
    ///
    /// This is the loop §4.1 describes, and it comes with the coupling §4.1's budget note warns
    /// about, now real: a hot topic competes with replication forwarding for the same core, and a
    /// stall in one topic's mmap path briefly stalls forwarding too. That is the trade the shared
    /// loop makes in exchange for one thread instead of one per connection, and scheduling that is
    /// deterministic rather than at the mercy of N blocked threads waking in whatever order.
    ///
    /// Establishment is deliberately *not* here — see [`spawn_establish`](Self::spawn_establish).
    pub fn run_duty_cycle(&self, idle: MuxIdle) {
        let mut servers: Vec<HostedServer> = Vec::new();
        let mut clients: Vec<PolledSub> = Vec::new();
        let mut round = 0u32;
        loop {
            // Adopt whatever establishment finished since the last cycle.
            servers.append(&mut self.duty.servers.lock_safe());
            clients.append(&mut self.duty.clients.lock_safe());

            let mut work = 0usize;

            // Replication sources: forward to subscribers. A dropped connection simply retires the
            // poll-item; the subscriber reconnects and resumes from its replica head.
            servers.retain_mut(|s| match s.item.poll(MAX_BATCH_PER_POLL_ITEM) {
                Ok(n) => {
                    work += n;
                    true
                }
                Err(_) => false,
            });

            // Replication sinks: apply into replicas.
            //
            // Written as an explicit drain rather than `retain_mut` because the order matters:
            // `retain_mut` drops a rejected element *after* its closure returns, which would
            // publish `connected == false` while the replica `Writer` inside the item was still
            // alive — and the conductor may start rebuilding that replica the instant it sees the
            // flag. Here the item is dropped first, by hand, and the flag published after.
            let mut live = Vec::with_capacity(clients.len());
            for mut c in clients.drain(..) {
                if c.shared.stopped.load(Ordering::Relaxed) {
                    let _ = c.item.shutdown();
                    drop(c.item);
                    c.shared.connected.store(false, Ordering::Release);
                    continue;
                }
                match c.item.poll(MAX_BATCH_PER_POLL_ITEM) {
                    Ok(n) => {
                        if n > 0 {
                            work += n;
                            c.shared
                                .synced
                                .store(c.item.expected_index().0, Ordering::Relaxed);
                            c.shared
                                .last_record_at_ms
                                .store(now_nanos() / 1_000_000, Ordering::Relaxed);
                        }
                        live.push(c);
                    }
                    Err(_) => {
                        drop(c.item);
                        c.shared.connected.store(false, Ordering::Release);
                    }
                }
            }
            clients = live;

            // Mux slots: merge members into their topics.
            work += self.poll_muxes().unwrap_or(0);

            if work > 0 {
                round = 0;
            } else {
                idle.wait(round);
                round = round.saturating_add(1);
            }
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

/// State a subscription shares between the **conductor** that establishes its connection and the
/// **duty cycle** that forwards on it.
///
/// `connected` is the handoff. The conductor only (re)establishes — and only wipes a replica for a
/// rebuild — while it is false, which is what keeps it from deleting files under the replica
/// `Writer` the duty cycle is holding. The duty cycle drops the poll-item *before* clearing the
/// flag (with `Release`, paired with the conductor's `Acquire`), so "not connected" really does
/// mean "no writer is live".
struct SubShared {
    replica_path: PathBuf,
    synced: AtomicU64,
    /// The source's head as advertised in the last `SubscribeAck` — a snapshot at connect time,
    /// not a live value; see [`SubscriptionStatus::head_at_connect`].
    head_at_connect: AtomicU64,
    /// Unix-millis when a record was last applied; 0 = none yet.
    last_record_at_ms: AtomicU64,
    rebuilds: RebuildStats,
    stopped: AtomicBool,
    /// A poll-item for this subscription exists in the duty cycle.
    connected: AtomicBool,
    /// An establishment attempt is in flight, so the conductor does not start a second one.
    establishing: AtomicBool,
}

/// A subscription's sink as a duty-cycle poll-item, with the state its progress is reported into.
struct PolledSub {
    shared: Arc<SubShared>,
    item: ClientPollItem,
}

/// A served subscription as a duty-cycle poll-item. Carries the connection-count guard, which used
/// to be released by the per-connection thread ending and now lives as long as the poll-item.
struct HostedServer {
    item: ServerPollItem,
    _guard: ConnGuard,
}

/// Where establishment hands finished connections to the duty cycle.
///
/// Two queues rather than direct insertion because the duty cycle owns its poll-items outright —
/// nothing else may touch them while it is polling, and it should not have to take a lock per
/// item per cycle. It drains these once per cycle instead.
#[derive(Default)]
struct DutyInbox {
    servers: Mutex<Vec<HostedServer>>,
    clients: Mutex<Vec<PolledSub>>,
}

/// Handle to a self-healing subscription replicating a remote channel locally. Dropping it stops
/// the replication.
pub struct Subscription {
    shared: Arc<SubShared>,
}

impl Subscription {
    /// Local path of the replica; a reader client opens this (in its own process).
    pub fn replica_path(&self) -> &Path {
        &self.shared.replica_path
    }

    /// Absolute index the replica has been synced to (the head). Grows as records arrive.
    pub fn synced_index(&self) -> u64 {
        self.shared.synced.load(Ordering::Relaxed)
    }

    /// Replica rebuilds this subscription has performed, by cause — see [`RebuildStats`].
    pub fn rebuilds(&self) -> &RebuildStats {
        &self.shared.rebuilds
    }

    /// The source's head as of the last successful (re)connect.
    pub fn head_at_connect(&self) -> u64 {
        self.shared.head_at_connect.load(Ordering::Relaxed)
    }

    /// Unix-millis when a record was last applied, or `None` if none has been.
    pub fn last_record_at_ms(&self) -> Option<u64> {
        match self.shared.last_record_at_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(ms),
        }
    }

    /// Whether this subscription is still wanted (not stopped). Independent of whether it happens
    /// to be connected right now — a reconnecting subscription is still active.
    pub fn is_active(&self) -> bool {
        !self.shared.stopped.load(Ordering::Relaxed)
    }

    /// Stop replicating. Idempotent. The duty cycle notices on its next cycle and drops the
    /// poll-item, which shuts the socket and releases the replica writer.
    pub fn stop(&self) {
        self.shared.stopped.store(true, Ordering::Relaxed);
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.stop();
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
            promoted_topics: Default::default(),
            mux_idle: MuxIdle::default(),
        }
    }

    /// [`config`] with `topics` promoted onto threads of their own (§4.1 rung 2).
    fn config_promoting(id: u64, data_dir: PathBuf, topics: &[&str]) -> NodeConfig {
        NodeConfig {
            promoted_topics: topics.iter().map(|t| t.to_string()).collect(),
            // A park-heavy idle so a promoted topic's loop is not hammering its mux lock while a
            // test tries to take it. Not what is under test — just keeps the test prompt.
            mux_idle: MuxIdle {
                max_spins: 0,
                max_yields: 0,
                min_park: Duration::from_micros(200),
                max_park: Duration::from_millis(1),
            },
            ..config(id, data_dir)
        }
    }

    /// Start a node: bind both listeners and spawn serve_stream / serve_control /
    /// maintenance. Returns the node and its (stream_addr, control_addr).
    fn start(id: u64, dir: &str) -> (Node, SocketAddr, SocketAddr) {
        start_with(config(id, temp_dir(dir)))
    }

    /// [`start`] with configured seed peers, for topology tests.
    fn start_seeded(id: u64, dir: &str, seeds: &[SocketAddr]) -> (Node, SocketAddr, SocketAddr) {
        start_with(NodeConfig {
            seeds: seeds.to_vec(),
            ..config(id, temp_dir(dir))
        })
    }

    fn start_with(cfg: NodeConfig) -> (Node, SocketAddr, SocketAddr) {
        start_advertising(cfg, None)
    }

    /// [`start_with`], but advertising `advertise` as this node's control address instead of the
    /// one it bound. Simulates a node that can dial out but cannot be dialled: peers learn an
    /// address that refuses connections, exactly as they would through a firewall. Applied before
    /// any thread runs, so the real address is never gossiped even once.
    fn start_advertising(
        cfg: NodeConfig,
        advertise: Option<SocketAddr>,
    ) -> (Node, SocketAddr, SocketAddr) {
        let node = Node::new(cfg);
        let stream_l = node.bind_stream().unwrap();
        let control_l = node.bind_control().unwrap();
        if let Some(addr) = advertise {
            node.dissemination.lock_safe().set_self_control_addr(addr);
        }
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
        // The duty cycle: replication sources, sinks and muxes as peer poll-items (§4.1). Without
        // it a test node accepts and handshakes but never forwards a record.
        let d = node.clone();
        std::thread::spawn(move || d.run_duty_cycle(MuxIdle::default()));
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
    /// The mesh forms itself. A **chain** — C seeded only to B, B seeded only to A, and A and C
    /// with no knowledge of each other — must converge into direct links, and a channel
    /// registered at one end must reach the other.
    ///
    /// What makes this work is **gossiped control addresses**: before them a heartbeat carried
    /// only a *stream* address — where to fetch data, never where to open a peer link — so C could
    /// not have dialled A whatever it knew. Removing that makes this test fail.
    ///
    /// Removing delta *relay* does **not** make it fail, and that is worth stating plainly: once
    /// the mesh closes, C is adjacent to A and receives its registry as join-time anti-entropy on
    /// the new link. Relay covers the window before the mesh closes, and any pair that never
    /// manages to link at all; it is exercised in isolation by
    /// `a_delta_relays_across_a_chain_without_echoing_its_source`.
    #[test]
    fn a_seed_chain_closes_into_a_full_mesh() {
        let (a, _a_stream, a_control) = start(80, "mesh-a");
        let (b, _b_stream, b_control) = start_seeded(81, "mesh-b", &[a_control]);
        let (c, _c_stream, _c_control) = start_seeded(82, "mesh-c", &[b_control]);

        // A registers a channel. Only B is adjacent to it at this point.
        drop(a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap());

        // C learns the channel two hops away — that is the relay.
        poll_until(|| c.registry.lock_safe().get("md.aapl").map(|_| ()));

        // ...and A and C end up direct peers, each having heard from the other first-hand.
        // Liveness is what proves the link is real: it is only ever conferred by a heartbeat
        // received directly, never by hearsay.
        poll_until(|| {
            let a_sees_c = a.dissemination.lock_safe().live_addr_of(NodeId(82));
            let c_sees_a = c.dissemination.lock_safe().live_addr_of(NodeId(80));
            (a_sees_c.is_some() && c_sees_a.is_some()).then_some(())
        });

        // And C can now actually resolve the channel to its owner, which needs both the registry
        // entry and live membership for A.
        let (id, addr) = c.resolve("md.aapl", Some(Duration::from_secs(5))).unwrap();
        assert_eq!(id.owner, NodeId(80));
        // Compared against *C's* view: a node never records its own heartbeat, so A's membership
        // has no entry for A.
        assert_eq!(
            Some(addr),
            c.dissemination.lock_safe().live_addr_of(NodeId(80)),
            "C resolves the owner to the address it heard from A directly"
        );
    }

    /// **Asymmetric reachability.** A node behind a firewall can dial out but cannot be dialled.
    /// The mesh must still close, using the direction that works.
    ///
    /// Simulated by having A advertise a control address nothing listens on — which is exactly
    /// what a peer sees through a firewall: the address is known and the connection is refused.
    /// A can still reach everyone; nobody can reach A.
    ///
    /// Ids are chosen so the *old* rule would fail. It elected the lower `NodeId` as the dialler,
    /// so with A(90) unreachable and C(80) reachable it made C dial A — the one direction that
    /// cannot work — while A skipped C as "not my job". The pair never linked. Now both dial and
    /// the duplicate is collapsed afterwards, so A's outbound link is the one that survives.
    #[test]
    fn a_node_reachable_only_outbound_still_joins_the_mesh() {
        let (b, _b_stream, b_control) = start(85, "asym-b");
        // A is unreachable inbound from the outset: it only ever advertises an address that
        // refuses connections, so no peer can dial it.
        let unreachable: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let (a, _a_stream, _a_control) = start_advertising(
            NodeConfig {
                seeds: vec![b_control],
                ..config(90, temp_dir("asym-a"))
            },
            Some(unreachable),
        );
        let (c, _c_stream, _c_control) = start_seeded(80, "asym-c", &[b_control]);

        // A registers a channel; only B is adjacent to it at first.
        drop(a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap());

        // A and C must become directly linked — provable by liveness, which only a heartbeat
        // received first-hand confers.
        poll_until(|| {
            let a_sees_c = a.dissemination.lock_safe().live_addr_of(NodeId(80));
            let c_sees_a = c.dissemination.lock_safe().live_addr_of(NodeId(90));
            (a_sees_c.is_some() && c_sees_a.is_some()).then_some(())
        });

        // ...and C can resolve A's channel, which needs both the registry entry and live
        // membership for its owner.
        let (id, _addr) = c.resolve("md.aapl", Some(Duration::from_secs(5))).unwrap();
        assert_eq!(id.owner, NodeId(90));
    }

    /// A cross-dial race leaves one link, not two, and both ends keep the *same* one — if they
    /// resolved it differently they would be left with none.
    #[test]
    fn a_cross_dial_race_collapses_to_a_single_link() {
        let (a, _a_stream, a_control) = start(94, "dup-a");
        let (b, _b_stream, b_control) = start(95, "dup-b");

        // Seed each at the other, so both dial: exactly the race the tie-break used to prevent
        // by picking a dialler in advance.
        a.connect_control_peer(b_control).unwrap();
        b.connect_control_peer(a_control).unwrap();

        // Both settle on one link to the other, and it stays settled.
        poll_until(|| {
            let a_links = a.dissemination.lock_safe().linked_nodes();
            let b_links = b.dissemination.lock_safe().linked_nodes();
            (a_links.contains(&NodeId(95)) && b_links.contains(&NodeId(94))).then_some(())
        });
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(10));
            assert!(
                a.dissemination
                    .lock_safe()
                    .linked_nodes()
                    .contains(&NodeId(95)),
                "the surviving link must not be dropped by a later dedup pass"
            );
        }
    }

    /// Hearsay teaches an address but must never confer liveness. `live_members` has to keep
    /// meaning "nodes *this* node can reach" — `resolve` returns `HostUnreachable` from it,
    /// `force_deregister` guards a name reclaim on it, and the topic reaper tombstones on it. A
    /// node that looked live because a third party vouched for it would weaken exactly the guard
    /// that exists to stop a partition retiring a channel whose owner is alive.
    #[test]
    fn hearsay_teaches_an_address_but_never_liveness() {
        let mut m = xchannel_net_core::membership::Membership::new();
        let stream: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let control: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let timeout = Duration::from_secs(60);

        assert!(m.learn(NodeId(9), stream, control), "new to us");
        assert_eq!(
            m.known_peers(),
            vec![(NodeId(9), control)],
            "we know where it is, so we can dial it"
        );
        assert_eq!(
            m.live_addr_of(NodeId(9), timeout),
            None,
            "but we have not heard from it, so it is not live"
        );
        assert!(m.live_members(timeout).is_empty());
        assert_eq!(m.silent_for(NodeId(9)), None, "never heard from directly");

        // A direct heartbeat is what makes it live.
        m.record(NodeId(9), stream, control);
        assert_eq!(m.live_addr_of(NodeId(9), timeout), Some(stream));

        // And later hearsay must not undo that.
        m.learn(NodeId(9), stream, control);
        assert_eq!(
            m.live_addr_of(NodeId(9), timeout),
            Some(stream),
            "hearsay must never make a reachable node look unreachable"
        );
    }

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

    /// **Every** tombstone this node produces must reach its own discovery log, not just its
    /// peers. Two paths used to announce to peers and skip the local publish — the topic-retirement
    /// path (`deregister_topic`) and the member reaper — so a watcher on the reaping node kept a
    /// phantom source indefinitely while every *other* daemon reported the removal correctly,
    /// because a peer republishes what it merges. That asymmetry is what makes the omission worth
    /// a test rather than a reading.
    #[test]
    fn every_locally_produced_tombstone_reaches_the_discovery_log() {
        let node = Node::new(config(144, temp_dir("discovery-tombstones")));
        node.create_topic(
            "agg",
            TopicOptions {
                member_reap_after_ms: 1, // opt the reaper in with a tiny threshold
                ..TopicOptions::default()
            },
        )
        .unwrap();
        // A member of "agg" owned by a node that is never a live member, so the reaper takes it.
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
        // A plain channel to retire by the already-covered owner path, so this test also pins that
        // the shared helper did not break it.
        drop(node.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap());

        let (_, cursor) = node.list_channels("").unwrap();

        // Reap first: retiring the topic drops its reap threshold, so the reaper would no longer
        // consider its members.
        node.reap_dead_members(); // records "dead since now"
        std::thread::sleep(Duration::from_millis(3));
        node.reap_dead_members(); // past the threshold — tombstones it
        assert!(
            node.registry.lock_safe().get("feed.x").is_none(),
            "the reaper should have tombstoned the dead owner's member"
        );
        assert!(node.deregister("md.aapl").unwrap());
        assert!(node.deregister_topic("agg").unwrap());

        let removed: Vec<String> = drain_changes(&cursor)
            .into_iter()
            .filter_map(|c| match c {
                ChannelChange::Removed { name, .. } => Some(name),
                ChannelChange::Upserted(_) => None,
            })
            .collect();
        for name in ["md.aapl", "agg", "feed.x"] {
            assert!(
                removed.contains(&name.to_string()),
                "no Removed published for '{name}' — a watcher here keeps a phantom source \
                 (published: {removed:?})"
            );
        }
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
    /// A channel's log says which channel it is, and reconstruction believes the log over the
    /// directory it found it in.
    ///
    /// The name was the last thing about a channel that did not self-describe — geometry,
    /// absolute index, incarnation and a topic's whole membership all come from the files, but
    /// the name came from the directory. So a data dir that had been migrated, restored, or
    /// hand-edited could serve one channel's records under another's name with nothing to catch
    /// it: the geometry is valid, the log is well formed, and `generation` agrees, because it
    /// travels with the file and a renamed directory looks perfectly consistent.
    #[test]
    fn a_renamed_channel_directory_is_refused_not_served_under_the_wrong_name() {
        let dir = temp_dir("renamed");
        let node = Node::new(config(70, dir.clone()));
        {
            let mut w = node.host_channel("md.aapl", 1 << 20, 0, |b| b).unwrap();
            let buf = w.try_reserve(5).unwrap();
            buf.copy_from_slice(b"aapl!");
            w.commit(0, 5, 0).unwrap();
        }
        // Someone moves the channel's directory — a migration, a restore, a mistake.
        std::fs::rename(dir.join("md.aapl"), dir.join("md.msft")).unwrap();

        let restarted = Node::new(config(70, dir));
        let rebuilt = restarted.reconstruct_from_disk();
        assert!(
            restarted.registry.lock_safe().get("md.msft").is_none(),
            "a log stamped 'md.aapl' must not be served as 'md.msft'"
        );
        assert!(
            restarted.registry.lock_safe().get("md.aapl").is_none(),
            "nor under its real name, which no directory now claims"
        );
        assert_eq!(
            (rebuilt.origins, rebuilt.skipped),
            (0, 1),
            "and the refusal is counted, not silent"
        );
    }

    /// The stamp survives what would erase it: rolling past retention. Segment 0 carries the name
    /// from creation, but it is the *rolled* segments that outlive it — and xchannel takes
    /// `channel_name` from whoever built the writer when it rolls, so every writer that reopens a
    /// channel has to supply it or quietly produce blank-named segments.
    #[test]
    fn the_name_stamp_survives_rolling_past_retention() {
        let dir = temp_dir("stamp-rolled");
        let node = Node::new(config(71, dir.clone()));
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
            "retention should have pruned the original segment, which is the point"
        );
        let restarted = Node::new(config(71, dir));
        assert_eq!(restarted.reconstruct_from_disk().origins, 1);
        assert!(restarted.registry.lock_safe().get("md.aapl").is_some());
    }

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
        // Serving is now split: `serve_stream` handshakes, the duty cycle forwards (§4.1).
        let forwarding = node.clone();
        std::thread::spawn(move || forwarding.run_duty_cycle(MuxIdle::default()));

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

    /// `TopicOptions.channel` configures the topic channel — **all** of it, not just the header
    /// geometry. `file_roll_size`/`keep_files` live on the `Writer`, and the writer that matters is
    /// the mux's (the one `create_for_client` precreated with is dropped immediately), so a mux
    /// opened with geometry alone leaves the topic growing as a single unbounded file however the
    /// client configured it. They must also survive a restart, which is why they ride the slot
    /// table: a re-hosted topic that quietly stopped pruning is the same bug one restart later.
    #[test]
    fn a_topic_channel_honours_its_retention_and_keeps_it_across_a_restart() {
        let dir = temp_dir("topic-retention");
        let options = TopicOptions {
            channel: ChannelOptions {
                region_size: 1 << 20,
                mtu: 0,
                file_roll_size: 1 << 21,
                keep_files: 1,
            },
            ..TopicOptions::default()
        };
        let node = Node::new(config(60, dir.clone()));
        node.create_topic("agg", options).unwrap();
        let member = node
            .publish_to_topic("agg", "mem.a", ChannelOptions::default())
            .unwrap();

        // Enough traffic through the member to roll the topic past its retention window.
        let write_member = |from: u64, n: u64| {
            let mut w = WriterBuilder::new(&member)
                .region_size(1 << 20)
                .build()
                .unwrap();
            let payload = vec![0xABu8; 1024];
            for i in from..from + n {
                let buf = w.try_reserve(payload.len()).unwrap();
                buf.copy_from_slice(&payload);
                w.commit(1, payload.len() as u32, i).unwrap();
            }
        };
        write_member(0, 4000);
        // `max_batch_per_member` bounds each poll, so drain.
        while node.poll_muxes().unwrap() > 0 {}

        let topic_path = node.channel_path("agg").unwrap();
        let base_before = xchannel::ReaderBuilder::new(&topic_path)
            .late_join()
            .build()
            .unwrap()
            .base_record_index();
        assert!(
            base_before > 0,
            "the topic must roll and prune on the options it was created with"
        );

        // Restart: the re-hosted topic must recover those bounds from its slot table and keep
        // pruning, rather than silently reverting to one unbounded file.
        let restarted = Node::new(config(60, dir));
        restarted.reconstruct_from_disk();
        assert_eq!(restarted.topic_member_count("agg"), Some(1), "re-hosted");
        write_member(4000, 4000);
        while restarted.poll_muxes().unwrap() > 0 {}

        let base_after = xchannel::ReaderBuilder::new(&topic_path)
            .late_join()
            .build()
            .unwrap()
            .base_record_index();
        assert!(
            base_after > base_before,
            "a re-hosted topic must keep pruning (earliest retained index stuck at {base_before})"
        );
        // And the bounds are advertised again, so subscribers' replicas stay bounded too.
        let hosted = restarted.hosted.lock_safe().get("agg").cloned().unwrap();
        assert_eq!(
            (hosted.file_roll_size, hosted.keep_files),
            (1 << 21, 1),
            "re-hosted topic must advertise its retention to subscribers"
        );
    }

    /// Merging one topic must not block anything else. Each mux has its own lock and the map lock
    /// is taken only to clone a handle out, so pinning one mux — which is exactly what a long poll
    /// does — leaves the map and every other topic usable.
    ///
    /// Pinning the lock directly is the deterministic way to assert this: no timing, no sleeps.
    /// (Operations that legitimately touch *every* mux, `poll_muxes` and `attach_pending_members`,
    /// will of course wait for the pinned one — that is per-topic serialisation working, not
    /// head-of-line blocking.)
    #[test]
    fn a_busy_topic_blocks_neither_the_map_nor_its_neighbours() {
        let node = Node::new(config(1, temp_dir("mux-lock")));
        node.create_topic("agg.a", TopicOptions::default()).unwrap();
        node.create_topic("agg.b", TopicOptions::default()).unwrap();
        node.publish_to_topic("agg.b", "mem.b", ChannelOptions::default())
            .unwrap();

        // Pin agg.a as if it were mid-merge, for the whole block below.
        let a = node.mux_of("agg.a").expect("agg.a is hosted here");
        let busy = a.lock_safe();

        // Reading another topic's status: used to queue behind agg.a's poll.
        let status = node.topic_status("agg.b").expect("hosted").unwrap();
        assert_eq!(status.members.len(), 1);
        assert_eq!(node.topic_member_count("agg.b"), Some(1));
        // Map mutations: creating and retiring topics.
        node.create_topic("agg.c", TopicOptions::default()).unwrap();
        assert!(node.deregister_topic("agg.b").unwrap());
        // Attaching a member to another topic.
        node.publish_to_topic("agg.c", "mem.c", ChannelOptions::default())
            .unwrap();
        assert_eq!(node.topic_member_count("agg.c"), Some(1));

        drop(busy);
    }

    /// One topic's trouble must not abandon the sweep. Topics are independent logs; `poll_muxes`
    /// used to propagate the first error with `?`, so one topic that could not merge stalled every
    /// other topic on that node — a different set each round, since `HashMap` iteration order
    /// varies. Here the problem topic's `mtu` cannot fit its member's records, so it rejects them
    /// while its healthy neighbour must still merge in the same sweep.
    #[test]
    fn a_topic_that_cannot_merge_does_not_stall_the_others() {
        let node = Node::new(config(1, temp_dir("mux-poll-isolation")));
        // `agg.bad` cannot fit a 1 KiB member record: mtu is big enough for its own slot table
        // (~50 B) and nothing else.
        node.create_topic(
            "agg.bad",
            TopicOptions {
                channel: ChannelOptions {
                    mtu: 128,
                    ..ChannelOptions::default()
                },
                ..TopicOptions::default()
            },
        )
        .unwrap();
        node.create_topic("agg.good", TopicOptions::default())
            .unwrap();

        for (topic, member, len) in [("agg.bad", "mem.bad", 1024), ("agg.good", "mem.good", 8)] {
            let path = node
                .publish_to_topic(topic, member, ChannelOptions::default())
                .unwrap();
            let mut w = WriterBuilder::new(&path)
                .region_size(1 << 20)
                .build()
                .unwrap();
            let payload = vec![0xEEu8; len];
            let buf = w.try_reserve(payload.len()).unwrap();
            buf.copy_from_slice(&payload);
            w.commit(1, payload.len() as u32, 0).unwrap();
        }

        // The sweep visits both in unspecified order; the good topic must merge either way.
        let _ = node.poll_muxes();
        let good = node.topic_status("agg.good").expect("hosted").unwrap();
        assert_eq!(
            good.members[0].merged, 1,
            "the healthy topic merged its record despite a broken neighbour"
        );
        let bad = node.topic_status("agg.bad").expect("hosted").unwrap();
        assert_eq!(
            bad.members[0].rejected, 1,
            "the oversized record is rejected and counted, not silently dropped"
        );
        assert_eq!(
            bad.topic_head, 1,
            "and nothing but its slot table reached the broken topic's log"
        );
    }

    /// Promotion (§4.1 rung 2, and §9's promotion *trigger*): a configured topic merges on a
    /// thread of its own, and the shared duty cycle stops polling it.
    ///
    /// The skip is asserted independently of the dedicated thread by **pinning** the promoted
    /// topic's mux: with its own loop blocked, `poll_muxes` — the shared loop's merge step — must
    /// still merge the unpromoted topic and must not touch the promoted one. Without the skip a
    /// "dedicated" thread would just be a second poller, and the promoted topic's records would
    /// still be spending the shared loop's budget, which is the whole reason to promote it.
    #[test]
    fn a_promoted_topic_merges_on_its_own_thread_and_leaves_the_shared_loop() {
        let node = Node::new(config_promoting(1, temp_dir("promote"), &["agg.fast"]));
        let opts = ChannelOptions::default();
        node.create_topic("agg.fast", TopicOptions::default())
            .unwrap();
        node.create_topic("agg.slow", TopicOptions::default())
            .unwrap();
        let fast_member = node.publish_to_topic("agg.fast", "mem.f", opts).unwrap();
        let slow_member = node.publish_to_topic("agg.slow", "mem.s", opts).unwrap();

        assert!(
            node.topic_status("agg.fast").unwrap().unwrap().promoted,
            "status must report the promotion so an operator can confirm it took effect"
        );
        assert!(!node.topic_status("agg.slow").unwrap().unwrap().promoted);

        // Pin the promoted topic before there is anything to merge, so its own loop cannot get
        // there first and make the assertion below ambiguous.
        let fast_mux = node.mux_of("agg.fast").expect("hosted");
        let pinned = fast_mux.lock_safe();

        for path in [&fast_member, &slow_member] {
            let mut w = WriterBuilder::new(path)
                .region_size(1 << 20)
                .build()
                .unwrap();
            let buf = w.try_reserve(4).unwrap();
            buf.copy_from_slice(b"tick");
            w.commit(1, 4, 0).unwrap();
        }

        // The shared loop merges the unpromoted topic and skips the promoted one — note it does
        // not even block on the pinned mux, because it never reaches for it.
        assert_eq!(
            node.poll_muxes().unwrap(),
            1,
            "the shared loop must merge agg.slow and skip agg.fast"
        );

        // Released, the promoted topic's own thread merges its record with nobody else's help.
        drop(pinned);
        poll_until(|| {
            let s = node.topic_status("agg.fast").unwrap().unwrap();
            (s.members[0].merged == 1).then_some(())
        });
    }

    /// A promoted topic's thread exits when the topic is retired, rather than polling a mux that
    /// is no longer this node's. Exiting on *identity* (not merely on the name being absent) is
    /// what stops a retire-and-recreate leaving the old thread running beside the new one.
    #[test]
    fn a_promoted_topics_thread_exits_when_the_topic_is_retired() {
        let dir = temp_dir("promote-retire");
        let node = Node::new(config_promoting(1, dir, &["agg"]));
        node.create_topic("agg", TopicOptions::default()).unwrap();
        let first = node.mux_of("agg").expect("hosted");
        assert!(node.deregister_topic("agg").unwrap());

        // Re-create under the same name: a *different* mux, with its own thread.
        node.create_topic("agg", TopicOptions::default()).unwrap();
        let second = node.mux_of("agg").expect("re-hosted");
        assert!(
            !Arc::ptr_eq(&first, &second),
            "re-creating a topic must yield a new mux"
        );
        // The old thread must have let go of the retired mux; once it has, this is the only
        // remaining reference to it.
        poll_until(|| (Arc::strong_count(&first) == 1).then_some(()));

        // And the new topic still merges on its own thread.
        let member = node
            .publish_to_topic("agg", "mem.a", ChannelOptions::default())
            .unwrap();
        let mut w = WriterBuilder::new(&member)
            .region_size(1 << 20)
            .build()
            .unwrap();
        let buf = w.try_reserve(4).unwrap();
        buf.copy_from_slice(b"tick");
        w.commit(1, 4, 0).unwrap();
        drop(w);
        poll_until(|| {
            let s = node.topic_status("agg").unwrap().unwrap();
            (s.members[0].merged == 1).then_some(())
        });
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

    /// The idle escalation, asserted on the *decision* rather than on elapsed time so it is
    /// deterministic. The load-bearing property is the first line: round 0 — the round immediately
    /// after a poll came up empty — must be a spin, because that is the round a record arriving
    /// microseconds later has to wait through. The old loop made every round a 5 ms sleep.
    #[test]
    fn mux_idle_escalates_from_spin_to_a_capped_park() {
        let idle = MuxIdle {
            max_spins: 2,
            max_yields: 2,
            min_park: Duration::from_micros(50),
            max_park: Duration::from_micros(200),
        };
        assert_eq!(idle.action(0), IdleAction::Spin, "first idle round is hot");
        assert_eq!(idle.action(1), IdleAction::Spin);
        assert_eq!(idle.action(2), IdleAction::Yield);
        assert_eq!(idle.action(3), IdleAction::Yield);
        // Then park, doubling from min_park and clamped at max_park.
        assert_eq!(idle.action(4), IdleAction::Park(Duration::from_micros(50)));
        assert_eq!(idle.action(5), IdleAction::Park(Duration::from_micros(100)));
        assert_eq!(idle.action(6), IdleAction::Park(Duration::from_micros(200)));
        assert_eq!(
            idle.action(7),
            IdleAction::Park(Duration::from_micros(200)),
            "clamped, not doubling forever"
        );
        // A loop idle for hours must not overflow the shift.
        assert_eq!(
            idle.action(u32::MAX),
            IdleAction::Park(Duration::from_micros(200))
        );
    }

    /// `max_park: 0` means never park — the loop stays on `yield_now` forever, for a core given
    /// over to a latency-critical topic.
    #[test]
    fn mux_idle_with_no_park_ceiling_never_sleeps() {
        let idle = MuxIdle {
            max_spins: 1,
            max_yields: 1,
            max_park: Duration::ZERO,
            ..MuxIdle::default()
        };
        assert_eq!(idle.action(0), IdleAction::Spin);
        for round in [1, 2, 500, u32::MAX] {
            assert_eq!(
                idle.action(round),
                IdleAction::Yield,
                "round {round} must not park"
            );
        }
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
