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
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use xchannel::{Writer, WriterBuilder};
use xchannel_net_core::codec::{self, decode_client_request, encode_client_reply};
use xchannel_net_core::dissemination::{Dissemination, PeerId};
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
///
/// Half a second rather than a full one, because it is charged five times per maintenance tick and the
/// tick has a liveness budget to fit inside (see the assertion below). The target is a LAN, where a
/// reachable host completes a connect in well under a millisecond; what this really sizes is how long a
/// *blackholed* address costs, and shortening it makes those cheaper to discover, not harder.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

/// Peer dials attempted per maintenance tick, **per candidate list**.
///
/// The maintenance loop is also the heartbeat loop, and a dial to a blackholed address costs a
/// full `CONNECT_TIMEOUT`. Unbounded dialling therefore let the *number of addresses this node has
/// ever heard of* set its heartbeat period: a dozen decommissioned hosts pushed a tick past
/// `LIVENESS_TIMEOUT` and every peer declared this healthy node dead. Two dials per 500 ms tick
/// still closes a fresh mesh in well under a second per peer, while keeping the worst-case tick
/// far below the liveness budget.
///
/// Seeds and learned peers carry **separate** budgets, so a tick's worst case is the sum across
/// lists, not this number. Sharing one budget would let a long list of learned ghosts crowd out the
/// seeds, and the seeds are the only addresses an operator actually chose: they are how a partitioned
/// node finds its way back.
const MAX_DIALS_PER_TICK: usize = 2;

/// What [`Node::subscription_status`] needs from a subscription, copied out so the `subscriptions`
/// lock is released before any higher-ranked lock is taken. Every field is an atomic load.
struct SubscriptionSnapshot {
    active: bool,
    synced: u64,
    head_at_connect: u64,
    last_record_at_ms: Option<u64>,
    rebuilds_gap: u64,
    rebuilds_diverged: u64,
    last_rebuild_at_ms: Option<u64>,
}

/// Why an establishment attempt is being made — which decides whether the attach throttle applies.
///
/// The throttle exists to stop a *retry loop* from spinning: `service_subscriptions` re-establishes every
/// wanted-but-disconnected subscription on every tick, and without a penalty an unattachable one costs a
/// thread and a connect every 500 ms for ever. A **caller** asking for a subscription is not that loop.
/// Throttling it made `Node::subscribe` return a `Subscription` that never replicated, and the client's
/// own five-second wait for the replica then failed the call outright — while `subscribe`'s doc promised
/// the opposite ("already replicating, rather than waiting for the conductor's next tick").
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Establish {
    /// Someone asked for this subscription now. Never throttled, and clears any standing penalty.
    Caller,
    /// The conductor is retrying one that is wanted but not connected. Throttled.
    Retry,
}

/// A deterministic per-name spread over `[0, gap/2)`, so that many subscriptions failing for one
/// reason do not all retry on the same tick.
///
/// Without it, three thousand members of a dead owner escalate in lockstep and arrive as a single
/// three-thousand-thread herd every ceiling-length interval — measured. Derived from the name rather
/// than from a random source so it needs no dependency and is reproducible in a test.
fn jitter_for(name: &str, gap: Duration) -> Duration {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    let half = gap.as_micros().max(1) as u64 / 2;
    Duration::from_micros(h % half.max(1))
}

/// First retry gap for a subscription that cannot be established, doubling to
/// [`ATTACH_BACKOFF_MAX`].
///
/// Deliberately **not** the dialler's constants, which is the mistake this replaces. A peer link is
/// re-formed by *either* end dialling, so a minute of patience there costs nothing: the other side is
/// trying too. A subscription is one-directional — only the subscriber re-establishes it — so the same
/// minute is a minute of stale replica with nobody else working to end it. Measured: reusing the
/// dialler's 60 s ceiling left a subscriber 29.5 s behind after a 34 s owner outage, where a ceiling of
/// its own resumes in 0.50 s (`measure_resume_after_the_owners_daemon_restarts`).
const ATTACH_BACKOFF_MIN: Duration = Duration::from_millis(250);

/// Ceiling on that gap. Chosen below `xchannel-net-client`'s own five-second wait for a replica to
/// appear, so a throttled retry cannot outlast the caller that is waiting on it.
const ATTACH_BACKOFF_MAX: Duration = Duration::from_secs(4);

const _: () = assert!(
    ATTACH_BACKOFF_MAX.as_millis() < DIAL_BACKOFF_MAX.as_millis(),
    "an attach must recover faster than a dial: only the subscriber re-establishes a subscription, \
     whereas either end of a peer link re-dials it"
);

/// One address's dial penalty: when it was last tried, when it may be tried again, and the gap that
/// produced that. The attempt instant is what lets the gap decay rather than only ever grow.
#[derive(Clone, Copy)]
struct DialPenalty {
    attempted_at: Instant,
    next: Instant,
    gap: Duration,
}

/// Dials per tick spent on addresses claiming this node's own id.
///
/// Smaller than the others because a duplicate identity is a misconfiguration, not a topology: there
/// are one or two such addresses, they are worth reaching promptly, and they must never be able to
/// slow ordinary mesh formation. One per tick reaches a twin inside a second.
const MAX_TWIN_DIALS_PER_TICK: usize = 1;

/// The one part of a tick this can bound at build time: **the connects**.
///
/// A tick is serial and holds the dissemination lock across most of itself, and the tick *is* the
/// heartbeat, so if it runs past `LIVENESS_TIMEOUT` every peer declares this node dead and the topic
/// member reaper starts tombstoning its live members' names. That makes the tick's cost worth checking
/// mechanically rather than by argument — an earlier version of this assertion was written in
/// truncating whole seconds, so a 900 ms `CONNECT_TIMEOUT` counted as zero and it passed for any dial
/// budget whatever while the real worst case was ninety seconds.
///
/// **What it does not cover, stated explicitly so nobody reads it as a bound on the tick:**
///
/// * A registry burst (join, announce, relay, reply) is bounded by *size*, not by a constant:
///   `bytes / MIN_DRAIN_RATE`, floored at `PEER_BURST_MIN`. At 10 MB that is 2.5 s. There is no
///   constant here to check, and no constant that could be right — see `MIN_DRAIN_RATE`.
/// * A heartbeat costs `P × PEER_SMALL_FRAME_BUDGET` across P peers, once per wedged peer since each
///   failed write drops it.
/// * Member attachment costs one non-blocking resolve per not-yet-replicating member whose owner is
///   reachable — measured at ~34 µs each, so 10 000 members cost ~340 ms. There is no cap; the per-tick
///   cap that used to be here starved live members and was deleted.
/// * Control-plane writes are bounded in aggregate by `broadcast::TICK_WRITE_BUDGET`, which is the only
///   term here that is actually *checked* rather than measured.
///
/// Bounding the burst term properly means not holding the lock across the write — a per-peer outbox,
/// which the stream plane already has. Until then it is a documented limit, not a checked one.
const _: () = assert!(
    (2 * MAX_DIALS_PER_TICK + MAX_TWIN_DIALS_PER_TICK) as u128 * CONNECT_TIMEOUT.as_millis()
        + TICK_RESERVE.as_millis()
        <= LIVENESS_TIMEOUT.as_millis(),
    "the worst-case connect spend leaves less than TICK_RESERVE of LIVENESS_TIMEOUT for the rest of \
     the tick — reduce a dial budget or CONNECT_TIMEOUT"
);

/// How much of `LIVENESS_TIMEOUT` the assertion above keeps back for everything in a tick that is not
/// dialling: the pump and its writes, member attachment, subscription servicing, and the sleep.
///
/// **Not a claim that the rest fits inside it.** It cannot be: the burst term is sized by payload, so at a
/// 200 000-channel registry a single relay to a peer that is slow but still accepting bytes can exceed this
/// on its own, and a tick may contain several bursts. (A peer accepting *nothing* is bounded much more
/// tightly, by `transport::STALL_LIMIT`.) What it does is stop the *dialling* budget from being widened until
/// nothing is left — the failure that produced two earlier versions of the assertion above.
const TICK_RESERVE: Duration = Duration::from_secs(3);

/// First retry gap after a failed dial, doubling to [`DIAL_BACKOFF_MAX`].
///
/// Backoff is per *address* rather than per node, because the failing case is an address with no
/// node behind it — a stale hint, a decommissioned host — which by definition cannot be keyed on
/// an identity we never learned.
const DIAL_BACKOFF_MIN: Duration = Duration::from_secs(1);

/// Ceiling on the retry gap. A departed peer stays a candidate forever (it may come back), so this
/// is what makes remembering it cheap: one dial a minute, not one a tick.
const DIAL_BACKOFF_MAX: Duration = Duration::from_secs(60);

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
///
/// Only a directory this call **creates** is chmod'ed. Tightening one that already existed is not
/// ours to do and can fail outright: `XCHANNELD_CLIENT_PATH` may legitimately point at a shared
/// directory, and chmod'ing e.g. `/tmp` to `0700` fails with `EPERM` and took the whole daemon
/// down with it. The data dir itself is still restricted explicitly at startup, and every directory
/// holding channel bytes is created here, so nothing loses protection.
fn ensure_private_dir(path: &Path) -> io::Result<()> {
    let existed = path.is_dir();
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    if !existed {
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

/// What discovering a duplicate `NodeId` calls for.
///
/// Returned rather than acted on inside, so the decision is testable without touching the
/// process-global shutdown flags — and so a library function does not reach out and stop the
/// process on its own.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnDuplicateIdentity {
    /// Warned; there is nothing safe this node can do about it.
    Continue,
    /// This node discarded its **generated** id and should be restarted to take a fresh one. Safe
    /// only because it owns no channels: changing the id of a node that owns some would leave them
    /// registered to an owner that never returns.
    StepAside,
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

/// **Lock order — acquire in this sequence, never against it:**
///
/// ```text
/// dissemination → registry → {hosted, subscriptions, muxes, discovery, topic_reap, ...}
/// ```
///
/// `dissemination` outranks `registry` because a peer being adopted must be handed a registry
/// snapshot that cannot have moved since the delta broadcast that preceded it, which means taking
/// the snapshot *inside* the dissemination lock. The leaf locks are unordered among themselves; no
/// path holds two of them and reaches back up.
///
/// This is a documented invariant with nothing enforcing it, and it has now been violated twice — the
/// second time in the very commit that fixed the first, thirty lines from the fix. So the hazard is
/// worth naming exhaustively rather than by example. **A `MutexGuard` is a temporary, and where it
/// dies depends on the syntax around it** (edition 2024; verified by probing with `try_lock`):
///
/// | form | guard alive in the body? |
/// |---|---|
/// | `if <guard-expr> { }` / `while <guard-expr> { }` | no |
/// | `a && <guard-expr>` / `<guard-expr> && b` | no |
/// | `let P = <guard-expr> else { };` | **no** in the else block — dropped *before* it — and no after |
/// | `if let P = <guard-expr> { }` | **yes**, in the then-block |
/// | `match <guard-expr> { }` | **yes**, in every arm |
/// | `f(<guard-expr>, g())` | **yes**, for the whole call |
/// | `for p in <guard-expr> { }` | **yes**, for the whole loop |
/// | `while let P = <guard-expr> { }` | **yes**, in the body |
/// | `let g = <guard-expr>;` | **yes**, to the end of the block |
///
/// `let … else` is worth its own row rather than being lumped with `if let`, because its temporaries
/// drop *before* the else block runs while `if let`'s survive the then-block: the two forms that look
/// most alike behave oppositely.
///
/// The last row is the one that bites hardest, because it looks like the fix for the fourth:
///
/// ```ignore
/// let subs = self.subscriptions.lock_safe();
/// let sub = subs.get(name)?;                    // borrows the guard, so it lives to the end
/// let owner = self.registry.lock_safe().get(name);   // ...and this inverts the order
/// ```
///
/// Binding is only enough if nothing *borrows* from the guard. Copy what you need out of it inside a
/// block, and let the block end release it.
///
/// One more thing this diagram flattens: `dissemination` is not a single lock but a small lattice of
/// its own (`connected → membership → {dial_identity, hints}`, `link_peers → membership`,
/// `self_control → {conflicts, same_id_addrs}`), exercised by reader threads that hold no
/// `dissemination` guard at all. It is acyclic, and "no reader takes `connected` under `membership`"
/// is load-bearing there.
/// Subscriptions the duty cycle services, by name, held weakly so that dropping the handle is enough
/// to stop servicing one.
type Conducted = Arc<Mutex<Vec<(String, Weak<SubShared>)>>>;

#[derive(Clone)]
pub struct Node {
    config: Arc<NodeConfig>,
    /// This daemon's discovery log: `None` until first use, then the writer plus the
    /// generation stamped into it.
    discovery: Arc<Mutex<Option<Writer>>>,
    /// Incarnation of this daemon's discovery log — fresh per process, so a client's cursor
    /// from a previous run is recognisably stale.
    discovery_generation: u64,
    /// Which owners were live on the previous attach pass, so a transition back to live can be
    /// detected and used to forget the penalties that transition makes pointless.
    last_live_owners: Arc<Mutex<HashSet<NodeId>>>,
    /// Per-member attach penalty, for members whose subscription cannot be established.
    ///
    /// Deleting the per-tick resolve cap removed a starvation bug and left one behaviour behind it: a member
    /// that *can* be resolved but cannot be *attached* — the owner is live but refusing connections, at its
    /// own connection cap, or accepting and never answering — was retried on every tick for ever. Measured:
    /// three thousand such members held **three thousand threads** and made eight hundred connects a second,
    /// with nothing logged; ten thousand pushed a tick to 3.4 s. Each attempt holds a thread for up to the
    /// resolve, connect and handshake timeouts, so the retry rate *is* the thread count. The deleted per-tick
    /// cap used to hide this by throttling the queue to four attempts a tick.
    ///
    /// Consulted in `spawn_establish`, the choke point all callers reach — not in `attach_pending_members`,
    /// which was the first place it went and which throttles a loop that is not the expensive one.
    attach_backoff: Arc<Mutex<HashMap<String, DialPenalty>>>,
    /// `NodeId`s already complained about, so a permanent duplicate produces one warning rather
    /// than one per maintenance tick. Gates the *message* only — never the decision to stand
    /// aside, which has to be re-evaluated as often as it is detected.
    dup_reported: Arc<Mutex<HashSet<NodeId>>>,
    /// Per-owner "unreachable from here since" clock, for owners we have never had contact with.
    /// Bounded by the registry's owner set, which `note_unreachable_owners` prunes it to.
    owner_unreachable_since: Arc<Mutex<HashMap<NodeId, Instant>>>,
    /// Per-address dial penalty: when this address was last attempted, when it may next be tried,
    /// and the gap that produced that instant. Bounded by the candidate set.
    dial_backoff: Arc<Mutex<HashMap<SocketAddr, DialPenalty>>>,
    /// Where the last tick's dial budget stopped in each candidate list, so a list is walked
    /// round-robin rather than always from its head.
    ///
    /// **One cursor per list, because they have different lengths.** A single shared cursor was
    /// reduced modulo each list in turn, so the seed walk — usually one or two entries — reset it
    /// into `0..=S` on every tick and the learned walk started at a constant index forever. With one
    /// seed it was pinned to index 1 permanently, which is precisely the behaviour the rotation was
    /// added to prevent.
    dial_cursor_seeds: Arc<Mutex<usize>>,
    dial_cursor_learned: Arc<Mutex<usize>>,
    /// Walk position over addresses claiming this node's own id (the twin candidates).
    dial_cursor_twins: Arc<Mutex<usize>>,
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
    conducted: Conducted,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        let mut dissemination =
            BroadcastDissemination::new(config.node_id, config.stream_addr, LIVENESS_TIMEOUT);
        dissemination.set_self_name(config.node_name.clone());
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
            attach_backoff: Arc::new(Mutex::new(HashMap::new())),
            last_live_owners: Arc::new(Mutex::new(HashSet::new())),
            dup_reported: Arc::new(Mutex::new(HashSet::new())),
            owner_unreachable_since: Arc::new(Mutex::new(HashMap::new())),
            dial_backoff: Arc::new(Mutex::new(HashMap::new())),
            dial_cursor_seeds: Arc::new(Mutex::new(0)),
            dial_cursor_learned: Arc::new(Mutex::new(0)),
            dial_cursor_twins: Arc::new(Mutex::new(0)),
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
    /// Refuses unless the owner has been **unreachable from this node** for at least
    /// `config.reclaim_after` — measured either as silence since the last direct contact, or, for an
    /// owner we have never had contact with, as how long we have known of it and failed to reach it
    /// (`owner_unreachable_since`). Both are observations about the owner. A freshly started daemon
    /// has made neither for long enough, so it cannot declare every channel in the registry
    /// abandoned the moment it comes up.
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
                    self.label(id.owner)
                ),
            ));
        }
        let unreachable_for = self.unreachable_for(id.owner);
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
        // The live set once, not once per member: this used to take the dissemination lock for every
        // member of every topic on every tick — ten thousand acquisitions for a ten-thousand-member
        // topic, to answer a question that does not change during the pass.
        // Asking for the live set is also how an owner's return is noticed: a member whose owner has
        // just come back must not be made to wait out a penalty its own failures earned while the owner
        // was away. Keeping it is what left a subscriber 29.5 s behind a restarted owner.
        let live_nodes = self.live_owners_noting_revivals(
            self.dissemination
                .lock_safe()
                .live_members()
                .into_iter()
                .collect(),
        );
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
                // **Never spend a resolve on an owner we already know we cannot reach.** Such a member
                // can never resolve, and it used to be attempted on every tick for ever — which was
                // survivable until a per-tick cap was added, at which point the same few names (registry
                // order is `BTreeMap` order) took every slot in the same order and every member behind
                // them was never attempted at all. Three separate reproductions: eight dead-owner members
                // sorting early by name stopped a hosted topic from ever merging *any* of its live
                // members, silently.
                //
                // With this gate the cap became unnecessary rather than merely sufficient: a charged
                // resolve is now one whose owner is live, which `resolve` satisfies on its first
                // iteration, and the connect runs on a thread of its own. Measured at ~1 µs per skipped
                // member — five thousand of them cost a tick 5 ms — where the cap needed forty-eight
                // ticks to attach two hundred members. So the cap, its counter and its rotation cursor
                // are gone; deleting a mechanism beats testing one.
                //
                // The gate covers the *subscription* only. Attaching from a replica already on disk
                // needs no reachable owner, and skipping that too meant a member whose owner had merely
                // gone quiet was left unattached even with a complete local replica.
                if remote && live_nodes.contains(&m.owner) {
                    // Throttling lives in `spawn_establish`, which is where the threads are actually made.
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

    /// Ensure a self-healing subscription is replicating remote member `name` locally, so its replica can
    /// feed the mux. Reuses the subscription map, so it is idempotent and a no-op for a member already
    /// replicating.
    ///
    /// Resolves with a **zero** timeout. The caller only reaches this for a member whose owner is already
    /// a live member, so `resolve` succeeds on its first pass; any timeout here could be spent but never
    /// usefully waited, and spending it on the heartbeat's own thread is what made a per-tick cap look
    /// necessary.
    /// Whether a member's attach attempt is due, and charge the attempt if so.
    ///
    /// Shares `DialPenalty`'s escalate-and-decay shape with the dialler, for the same reason: an attempt
    /// that cannot succeed must not be repeated at the tick rate for ever. A member that attaches clears its
    /// penalty, so a peer coming back is picked up promptly.
    fn attach_due(&self, name: &str) -> bool {
        let mut backoff = self.attach_backoff.lock_safe();
        if let Some(p) = backoff.get(name)
            && Instant::now() < p.next
        {
            return false;
        }
        let now = Instant::now();
        let gap = match backoff.get(name) {
            Some(p) if p.attempted_at.elapsed() < ATTACH_BACKOFF_MAX * 2 => {
                (p.gap * 2).min(ATTACH_BACKOFF_MAX)
            }
            _ => ATTACH_BACKOFF_MIN,
        };
        backoff.insert(
            name.to_string(),
            DialPenalty {
                attempted_at: now,
                next: now + gap + jitter_for(name, gap),
                gap,
            },
        );
        true
    }

    /// Forget a name's attach penalty, so the next attempt is immediate.
    fn attach_now(&self, name: &str) {
        self.attach_backoff.lock_safe().remove(name);
    }

    /// `live_now`, having first dropped the attach penalties of members whose owner has just become
    /// reachable again.
    ///
    /// The throttle exists for an owner that is *live but not answering* — its stream plane refusing,
    /// or at its own connection cap — where retrying every tick achieves nothing. An owner that was
    /// **unreachable** and is now live is the opposite case: the reason for the previous failures is
    /// gone, and the only thing a penalty can do is delay the resume. That is the difference between a
    /// 0.50 s and a 29.5 s recovery from an ordinary owner restart.
    ///
    /// It returns the set it was given so that the attach pass has to go through it to learn who is
    /// live: a plain `clear_…(&live)` statement next to the one that computes `live` is a line anybody
    /// could delete without a test noticing, and the deletion is exactly the bug this fixes.
    fn live_owners_noting_revivals(&self, live_now: HashSet<NodeId>) -> HashSet<NodeId> {
        let revived: HashSet<NodeId> = {
            let mut was_live = self.last_live_owners.lock_safe();
            let revived = live_now.difference(&was_live).copied().collect();
            *was_live = live_now.clone();
            revived
        };
        if revived.is_empty() {
            return live_now;
        }
        let names: Vec<String> = self
            .registry
            .lock_safe()
            .iter()
            .filter(|id| revived.contains(&id.owner))
            .map(|id| id.name.clone())
            .collect();
        let mut backoff = self.attach_backoff.lock_safe();
        for name in names {
            backoff.remove(&name);
        }
        live_now
    }

    fn ensure_member_subscription(&self, name: &str) {
        let live = self
            .subscriptions
            .lock_safe()
            .get(name)
            .is_some_and(|s| s.is_active());
        if live {
            return;
        }
        // Zero, not a short wait: this is only reached for a member whose owner is already a live
        // member, so `resolve` succeeds on its first pass. A timeout here could only ever be *spent*,
        // never useful — and spending it on the heartbeat's thread is what made a cap seem necessary.
        if let Ok(sub) = self.subscribe_as(name, Some(Duration::ZERO), Establish::Retry) {
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
    /// [`reconstruct_from_disk`](Node::reconstruct_from_disk)): recover its geometry from the channel header via
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

    /// Re-host one topic from disk (helper for [`reconstruct_from_disk`](Node::reconstruct_from_disk)): re-register the topic
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
        //
        // **Bound before the `if let`.** A guard in the scrutinee lives for the whole body, so
        // writing this as `if let Some(t) = self.registry.lock_safe().deregister(..)` held the
        // registry lock across both the `hosted` lock and the announce — the exact inversion of this
        // type's documented lock order, and a hard deadlock against any thread dialling a peer or
        // accepting one (both of which take dissemination, then registry). It hung within a couple of
        // hundred retirements when a single inbound control connection arrived every 5 ms, and it hung
        // silently: the dissemination lock is the whole control plane, so the node stops heartbeating
        // and every peer declares it dead.
        let tombstone = self
            .registry
            .lock_safe()
            .deregister(topic, self.config.node_id);
        if let Some(tombstone) = tombstone {
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
        // Advertise what peers can reach, which is not always what was bound.
        let advertised = self.config.advertise_stream_addr.unwrap_or(addr);
        self.dissemination.lock_safe().set_self_addr(advertised);
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
        // Advertise the address that actually accepts links, not the configured one — with `:0` they
        // differ, and a peer dialling the configured port would reach nothing. An explicit
        // `advertise_control_addr` overrides both, for the wildcard-bind case where the bound address
        // is not something any peer can dial.
        let addr = listener.local_addr()?;
        let advertised = self.config.advertise_control_addr.unwrap_or(addr);
        self.dissemination
            .lock_safe()
            .set_self_control_addr(advertised);
        Ok(listener)
    }

    /// Accept peer control connections forever, adopting each as a dissemination peer
    /// (which sends our current registry as join-time anti-entropy + a heartbeat).
    ///
    /// Both take the registry snapshot **while holding the dissemination lock**, which is the only
    /// way the join-time anti-entropy can be complete. Snapshotting first and locking second left a
    /// window: a local registration in between broadcast its delta to the peers that existed at that
    /// moment — not including this one, which was not adopted yet — and then handed the new peer a
    /// snapshot taken before the change. The delta was lost to it until some later reconnect, and
    /// until then the two nodes disagreed about that channel.
    pub fn serve_control(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let mut d = self.dissemination.lock_safe();
            let snapshot = self.registry_snapshot();
            let _ = d.add_peer(conn, &snapshot);
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
        let mut d = self.dissemination.lock_safe();
        let snapshot = self.registry_snapshot();
        d.add_outbound_peer(conn, addr, &snapshot)
    }

    /// Shut down cleanly: tell peers we are leaving so they stop treating this node's channels as
    /// reachable, and remove the client socket so nothing is left claiming to be a live daemon.
    ///
    /// Everything else needs no unwinding. Records already committed are durable in their mmap,
    /// merge cursors are recomputed from the topic log on the next start rather than saved, a
    /// subscriber resumes from its own replica head, and the data-dir lock is released by the OS.
    /// That is why a hard kill is safe, and why this function is short.
    pub fn shutdown(&self) {
        self.dissemination.lock_safe().announce_leaving();
        let _ = std::fs::remove_file(&self.config.client_path);
    }

    /// How long the owner has been unreachable **from this node**, which is the only thing a
    /// reclaim may be judged on.
    ///
    /// Direct silence is preferred when we have it. When we do not — and relay plus `PeerHint` have
    /// made that the ordinary case, since a registry entry routinely arrives second-hand for a node
    /// we hold no link to — the answer is how long this node has known of the owner *while unable to
    /// reach it*, tracked by `note_unreachable_owners` on the maintenance tick and reset the instant
    /// contact is made.
    ///
    /// Be precise about what that second quantity is **not**: it is not a record of failed dials. An
    /// owner whose control address this node has never learned cannot be dialled at all, and its clock
    /// runs regardless. What the floor establishes is "known here, and not reachable from here, for
    /// this long" — the observation window behind an operator's assertion that a host is gone, not
    /// evidence that this node tried and failed.
    ///
    /// What it must **never** be is this node's own uptime, which is what it used to fall back to.
    /// That is not an observation about the owner at all: every daemon older than `reclaim_after`
    /// satisfied the floor unconditionally, so an owner that was alive and writing but merely
    /// unreachable from here could have its channel tombstoned on the strength of our having been up
    /// for a while. The field that held the start time is gone, so it cannot come back by accident.
    fn unreachable_for(&self, owner: NodeId) -> Duration {
        if let Some(silence) = self.dissemination.lock_safe().silent_for(owner) {
            return silence;
        }
        // Stamped here as well as on the tick, so the clock starts at the first question even if
        // maintenance is not running (a library embedding, or a node that has only just come up).
        // Starting at zero is the point: it means "we have no idea yet", which no non-zero floor
        // accepts.
        self.owner_unreachable_since
            .lock_safe()
            .entry(owner)
            .or_insert_with(Instant::now)
            .elapsed()
    }
    /// Keep the unreachable-since clock honest: start it for any registered owner we cannot reach,
    /// and **stop** it the moment we can. Called on the maintenance tick.
    ///
    /// Clearing matters more than starting. A node that flaps must not accumulate credit towards a
    /// reclaim across the reachable stretches in between, or a peer with an intermittent link would
    /// eventually look permanently gone.
    fn note_unreachable_owners(&self) {
        let owners: HashSet<NodeId> = self
            .registry
            .lock_safe()
            .iter()
            .filter(|i| !i.deleted && i.owner != self.config.node_id)
            .map(|i| i.owner)
            .collect();
        let d = self.dissemination.lock_safe();
        let live: HashSet<NodeId> = d.live_members().into_iter().collect();
        drop(d);
        let mut since = self.owner_unreachable_since.lock_safe();
        since.retain(|node, _| owners.contains(node) && !live.contains(node));
        for owner in owners.difference(&live) {
            since.entry(*owner).or_insert_with(Instant::now);
        }
    }

    /// A node's label for messages a person reads: its name if it advertised one, else its id.
    fn label(&self, node: NodeId) -> String {
        match self.dissemination.lock_safe().name_of(node) {
            Some(name) => format!("{name} ({})", node.0),
            None => node.0.to_string(),
        }
    }

    /// Two machines are using one `NodeId`. Say so, and — if our own id was generated and we own
    /// nothing yet — discard it so the next start picks a fresh one.
    ///
    /// Discarding is safe **only** while nothing references the id. Once this node owns a channel,
    /// changing its id would leave those channels registered to an owner that never comes back:
    /// peers keep the earlier registration, it wins the merge, and the channels are frozen until
    /// an operator reclaims the names. So past that point this can only warn.
    ///
    /// The case this actually rescues is a golden image snapshotted after the daemon's first start —
    /// every clone carries the same `.node_id` and owns nothing. **Every clone stands aside, not all
    /// but one**: they detect each other simultaneously and none has grounds to consider itself the
    /// original. That is harmless, because none of them owned anything, and each comes back with a
    /// fresh id.
    fn report_duplicate_identity(&self, conflicts: &[NodeId]) -> OnDuplicateIdentity {
        // Once per id, not once per tick. Detection is a standing condition, not an event: for the
        // cases below that can only warn, the maintenance loop re-detects the same duplicate twice a
        // second, which at forty lines every twenty seconds buries whatever else the operator was
        // reading — and none of the repeats say anything the first did not.
        let fresh: Vec<NodeId> = {
            let mut seen = self.dup_reported.lock_safe();
            conflicts
                .iter()
                .copied()
                .filter(|n| seen.insert(*n))
                .collect()
        };
        for node in &fresh {
            eprintln!(
                "xchanneld[{}]: WARNING: two peers claim NodeId {} at different control \
                 addresses — NodeIds must be unique. Channel ownership, membership and peer links \
                 are all keyed on them. Most likely a copied data directory or a cloned image.",
                self.config.node_id.0, node.0
            );
        }
        if !conflicts.contains(&self.config.node_id) {
            // Someone else's collision; nothing safe for us to do about it.
            return OnDuplicateIdentity::Continue;
        }
        // Below this point the id is ours. **The rate limit gates the message, never the verdict.**
        // Returning early for a repeat detection latched the first tick's answer forever: a node that
        // happened to own a channel when it first noticed could not stand aside later, once it owned
        // nothing — and standing aside is only ever safe in that later state.
        let say = fresh.contains(&self.config.node_id);
        if !self.config.id_generated {
            if say {
                eprintln!(
                    "xchanneld[{}]: its id was set explicitly (XCHANNELD_NODE_ID), so this daemon \
                     will not change it — resolve the duplicate and restart.",
                    self.config.node_id.0
                );
            }
            return OnDuplicateIdentity::Continue;
        }
        if !self.hosted.lock_safe().is_empty() {
            if say {
                eprintln!(
                    "xchanneld[{}]: this daemon already owns channels, so changing its id would \
                     orphan them — resolve the duplicate manually.",
                    self.config.node_id.0
                );
            }
            return OnDuplicateIdentity::Continue;
        }
        match crate::node_identity::discard(&self.config.data_dir) {
            Ok(()) => {
                // Stepping aside has to be more than deleting a file: the id is already in this
                // process, so carrying on would keep the duplicate live indefinitely and the only
                // thing that had changed would be that the file was gone too. Stop, and exit
                // non-zero so a supervisor brings us back — that restart is where the fresh id
                // comes from. Safe precisely because we own nothing.
                eprintln!(
                    "xchanneld[{}]: it owns nothing yet, so its generated id has been discarded \
                     and it is stopping; restarting takes a fresh one.",
                    self.config.node_id.0
                );
                OnDuplicateIdentity::StepAside
            }
            // Only step aside if the id was actually discarded. Exiting on a failed discard would
            // come back to the same duplicate and exit again — a restart loop, not a repair.
            Err(e) => {
                eprintln!(
                    "xchanneld[{}]: could not discard the duplicate node id ({e}) — continuing \
                     with it; resolve this manually.",
                    self.config.node_id.0
                );
                OnDuplicateIdentity::Continue
            }
        }
    }

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
        // Never dial our own advertised control address. A seed list naming every node in the mesh —
        // including this one — is the ordinary way an operator writes one, and it produced a permanent
        // link from this node to itself: a thread, two file descriptors and a heartbeat exchanged with
        // nobody, kept forever because a self-link never learns an identity and `dedup_links`
        // deliberately keeps a link whose peer it does not know.
        if addr == d.self_control_addr() {
            return false;
        }
        match d.node_at(addr) {
            Some(node) => !d.linked_nodes().contains(&node),
            None => true,
        }
    }

    /// Dial `addr` as a peer, best-effort.
    ///
    /// **The attempt is charged, not the failure.** Clearing the penalty on a successful *connect*
    /// looked right and let one address consume the budget forever: an address can accept a TCP
    /// connection and then drop the link — a seed or hint naming a peer's stream port instead of its
    /// control port, or a peer on a release whose control frames this one cannot decode — and that
    /// costs a full dial while recording nothing. The reader clears `connected` when the link dies,
    /// so the next tick found it unlinked, un-penalised, and due, and spent a dial on it again. Two
    /// such addresses starved every live peer behind them permanently.
    ///
    /// **No outcome-based reset either.** Resetting the gap when the join returned `Ok` looked like the
    /// principled version of this and is not: a write returning `Ok` means the bytes reached a socket
    /// buffer, not that a peer took them. Measured — a far end that accepts, *reads* the join, and hangs
    /// up returns `Ok` every time, so exactly the two cases this comment used to cite (a peer on an
    /// incompatible release, a seed naming a stream port) were pinned at the one-second floor for ever,
    /// each cycle costing a full registry-snapshot clone and a whole-registry write under the
    /// dissemination lock. The escalate-and-decay rule below needs no notion of success at all.
    fn dial_peer(&self, addr: SocketAddr) {
        {
            let now = Instant::now();
            let mut backoff = self.dial_backoff.lock_safe();
            // **Escalate on a recent history of attempts; start fresh without one.** The gap doubles
            // only while attempts keep coming, and an address whose last attempt is older than the
            // ceiling begins again at the floor. Without that decay the doubling was monotone for the
            // life of the process, so an address dialled once an hour — an ordinary peer reconnecting
            // — would eventually have to serve a full minute before anyone here could reach it.
            let gap = match backoff.get(&addr) {
                Some(p) if p.attempted_at.elapsed() < DIAL_BACKOFF_MAX * 2 => {
                    (p.gap * 2).min(DIAL_BACKOFF_MAX)
                }
                _ => DIAL_BACKOFF_MIN,
            };
            backoff.insert(
                addr,
                DialPenalty {
                    attempted_at: now,
                    next: now + gap,
                    gap,
                },
            );
        }
        if let Ok(conn) = TcpTransport::connect_timeout(&addr, CONNECT_TIMEOUT) {
            let mut d = self.dissemination.lock_safe();
            let snapshot = self.registry_snapshot();
            let _ = d.add_outbound_peer(conn, addr, &snapshot);
        }
    }

    /// Whether `addr` may be dialled now. Unknown addresses are always due — a first attempt is never
    /// delayed — and so is one whose gap has elapsed.
    ///
    /// **There is deliberately no exemption for an address that once worked.** There was one, and it
    /// is worth recording why it was wrong: it keyed on "has this address ever identified itself over
    /// a link we dialled there", and that memo is never pruned, so the exemption applied on every tick
    /// forever and cleared the penalty before it could ever double. A peer that *departed* — a host
    /// powered off overnight, precisely the population a backoff exists for — was then dialled every
    /// tick in perpetuity: four such addresses took the heartbeat period from 0.5 s to a sustained
    /// 4.5 s, 45 % of the liveness budget, where the previous release decayed to one attempt a minute.
    ///
    /// No exemption is needed. A link that lasted longer than its own gap has already outlived the
    /// penalty from the dial that created it, so `now >= next` holds and the reconnection is prompt on
    /// the merits. A link that dies *inside* its gap is flapping, and backing off is the right answer
    /// rather than something to excuse.
    fn dial_due(&self, addr: SocketAddr) -> bool {
        self.dial_backoff
            .lock_safe()
            .get(&addr)
            .is_none_or(|p| Instant::now() >= p.next)
    }

    /// Dial at most `budget` of `candidates`, skipping those already linked, those still in backoff,
    /// and those with no dialable address.
    ///
    /// Rotating rather than restarting is what keeps a long candidate list fair: taking the first
    /// two every time would let two permanently-dead addresses at the head of the list starve
    /// every live peer behind them, which is the same starvation the cap was added to prevent.
    fn dial_some(&self, candidates: &[SocketAddr], cursor: &Mutex<usize>, budget: usize) {
        if candidates.is_empty() {
            return;
        }
        let start = {
            let c = cursor.lock_safe();
            *c % candidates.len()
        };
        let mut spent = 0;
        for i in 0..candidates.len() {
            if spent == budget {
                break;
            }
            let addr = candidates[(start + i) % candidates.len()];
            // An unspecified address (`0.0.0.0`, `[::]`) is a bind wildcard, never a destination:
            // dialling it means dialling *this* host, so a peer that advertised its listen address
            // verbatim would have every one of its peers open a link to itself.
            if addr.ip().is_unspecified() || !self.dial_due(addr) || !self.should_dial(addr) {
                continue;
            }
            self.dial_peer(addr);
            spent += 1;
        }
        // Advance by what was actually consumed, so the next tick resumes past the addresses this
        // one spent its budget on. Setting the cursor to a fixed offset from `start` instead —
        // `start + 1` — discarded the walk and left it re-entering the list at the same place.
        *cursor.lock_safe() = start + spent;
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
        let (candidates, twins) = {
            let d = self.dissemination.lock_safe();
            let candidates: Vec<SocketAddr> = d
                .unconnected_peers()
                .into_iter()
                .map(|(_, control_addr)| control_addr)
                .collect();
            (candidates, d.same_id_candidates())
        };
        self.dial_some(&candidates, &self.dial_cursor_learned, MAX_DIALS_PER_TICK);
        // Addresses where a third party has reported *our own* id, on their own budget. They cannot
        // ride the learned-peer list: that list excludes our id by construction, which is exactly why
        // two clones of one data directory never met. Giving them a separate budget also keeps a
        // duplicate — a misconfiguration — from crowding out ordinary mesh formation.
        self.dial_some(&twins, &self.dial_cursor_twins, MAX_TWIN_DIALS_PER_TICK);
    }

    /// (Re)connect to configured seeds not currently linked. Called each maintenance tick, so a
    /// dropped seed link is re-established.
    ///
    /// Seeds and learned peers get **separate** budgets deliberately. Sharing one would let a long
    /// list of learned ghosts crowd out the seeds, and the seeds are the only addresses an operator
    /// actually chose — they are how a partitioned node finds its way back.
    pub fn connect_seeds(&self) {
        // Same identity check as a learned peer (inside `dial_some`). Without it, a seed link that
        // lost the duplicate tie-break would be re-dialled every tick and dropped again every tick.
        let seeds = self.config.seeds.clone();
        self.dial_some(&seeds, &self.dial_cursor_seeds, MAX_DIALS_PER_TICK);
    }

    fn registry_snapshot(&self) -> Vec<ChannelIdentity> {
        self.registry.lock_safe().iter().cloned().collect()
    }

    /// Periodic maintenance: reconnect dropped seeds, emit a heartbeat, and merge gossiped
    /// identities into the registry. Runs forever; the caller drives it on its own thread.
    pub fn run_maintenance(&self, interval: Duration) -> io::Result<()> {
        loop {
            // **Heartbeat first, dial second, and cap the dials.** Dialling is serial with a 1 s
            // timeout per unreachable address, and the candidate set grows with every node the mesh
            // has ever mentioned (membership is never pruned, and each new link re-teaches the whole
            // directory). Dialling first meant a dozen decommissioned hosts pushed the tick past
            // `LIVENESS_TIMEOUT`, so a healthy node was reported dead by everyone — and because the
            // topic member reaper keys on that same predicate, it then tombstoned live members'
            // names. Observed directly: 12 unreachable addresses were enough to flip a live,
            // actively-writing owner to `owner_live = false`.
            let pumped = {
                let mut d = self.dissemination.lock_safe();
                // Re-arm the tick's total write budget before anything writes. Per-peer allowances stop
                // one slow peer from evicting the others; this stops P of them from summing past
                // `LIVENESS_TIMEOUT` — 32 peers accepting nothing measured 10.10 s without it.
                d.begin_tick();
                let _ = d.emit_heartbeat();
                // Forward peer knowledge learned since the last tick, so the mesh keeps closing
                // itself; only knowledge that was *new* to us is queued, so this goes quiet.
                d.relay_hints();
                // Collapse any duplicate links the cross-dial race produced. Anything it reports
                // is not a duplicate *link* but a duplicate *identity* — two machines claiming one
                // `NodeId`.
                let conflicts = d.dedup_links();
                if !conflicts.is_empty() {
                    drop(d);
                    if self.report_duplicate_identity(&conflicts) == OnDuplicateIdentity::StepAside
                    {
                        crate::shutdown::request_restart();
                    }
                    self.dissemination.lock_safe().pump()?
                } else {
                    d.pump()?
                }
            };
            if !pumped.is_empty() {
                let mut retired = Vec::new();
                // **Accumulate, then send once per source.** One frame per identity meant one
                // dissemination-lock acquisition and one blocking write per identity, and a peer can
                // deliver any number of identities in a single frame: 200 000 of them produced a
                // forty-second heartbeat gap, four times `LIVENESS_TIMEOUT`, so every peer declared
                // this node dead and the topic member reaper began tombstoning its live members'
                // names. Coalescing bounds the whole pump to **one dissemination-lock acquisition** and
                // one *burst* per peer, whatever arrives — not one write: a burst is chunked, so it is
                // as many frames as the delta needs. The frames of one burst share a single deadline
                // derived from its size (`MIN_DRAIN_RATE`), which is what bounds it; a budget per frame
                // is not a bound on a sequence of them.
                let mut to_relay: HashMap<PeerId, Vec<ChannelIdentity>> = HashMap::new();
                let mut to_reply: HashMap<PeerId, Vec<ChannelIdentity>> = HashMap::new();
                for (from, id) in pumped {
                    let name = id.name.clone();
                    let incoming = id.clone();
                    let merged = self.merge_and_publish(id);
                    // **Relay on change.** Without this a delta reaches only the originator's
                    // direct peers and a node two hops away stays ignorant until it opens a fresh
                    // link. Relaying only when the merge actually moved our map is what makes it
                    // terminate: the registry merge is a total order and idempotent, so a given
                    // winning state can change a given node's map at most once, whatever cycles
                    // the topology has.
                    if merged.changed {
                        to_relay
                            .entry(from)
                            .or_default()
                            .push(merged.winner.clone());
                    } else if Self::lost_to(&incoming, &merged.winner) {
                        // **The sender is behind: tell it.** Our map did not move, so there is
                        // nothing to flood — but the peer that sent this is holding a state that
                        // lost, and nothing else will ever correct it. Anti-entropy only runs when a
                        // link is established, so on a link that stays up the two of us would have
                        // disagreed about this channel indefinitely: it would keep resolving the
                        // wrong owner, and after a reclaim it would keep serving a replica of an
                        // incarnation the mesh has retired.
                        to_reply
                            .entry(from)
                            .or_default()
                            .push(merged.winner.clone());
                    }
                    if merged.winner.deleted {
                        retired.push(name);
                    }
                }
                if !to_relay.is_empty() || !to_reply.is_empty() {
                    let mut d = self.dissemination.lock_safe();
                    for (from, delta) in to_relay {
                        let _ = d.relay(from, &delta);
                    }
                    for (to, delta) in to_reply {
                        let _ = d.reply(to, &delta);
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
            self.note_unreachable_owners();
            self.reap_dead_members();
            self.attach_pending_members();
            // After the heartbeat and the merge, never before, and bounded per tick.
            self.connect_seeds();
            self.connect_learned_peers();
            // Reconnect any subscription the duty cycle dropped — the self-healing half.
            self.service_subscriptions();
            std::thread::sleep(interval);
        }
    }

    // ---------------- discovery ----------------

    /// Whether `incoming` genuinely **lost** to `winner`, by the same key the registry merge orders on.
    ///
    /// The reply guard cannot be full struct equality. Two identities can tie on the ordering key —
    /// `(epoch, deleted, registered_at_nanos, owner)` — and still differ in a payload field the merge
    /// does not look at, in which case each node keeps its own, each sees a winner that "differs from
    /// incoming", and the two reply to each other every tick forever, never converging. Comparing the
    /// key instead means a tie sends nothing, which is correct: neither side can move the other, so
    /// there is nothing to say. No path in this tree produces such a tie today, but the guard should
    /// not depend on that continuing to be true.
    fn lost_to(incoming: &ChannelIdentity, winner: &ChannelIdentity) -> bool {
        let key = |id: &ChannelIdentity| (id.epoch, id.deleted, id.registered_at_nanos, id.owner);
        key(incoming) != key(winner)
    }

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
        // Forget any attach penalty with it. The map is keyed on the bare name, so a name reclaimed at
        // `epoch + 1` would otherwise inherit its predecessor's backoff — and nothing else prunes it.
        self.attach_now(name);
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
        // Bound first, for the same reason as `deregister_topic`: the guard would otherwise be held
        // across the `registry` lock below, inverting the leaf-locks-are-lowest rule and closing a
        // second cycle with that function's `registry → hosted`.
        let hosted = self.hosted.lock_safe().get(name).cloned();
        if let Some(src) = hosted {
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

        // Take what is needed and release the leaf before touching anything that outranks it. Binding
        // `sub` out of the guard is not enough — it *borrows* from the guard, so the guard would live to
        // the end of the function and span both locks below, which is this type's documented order run
        // backwards. Every field read here is an atomic load, so copying them is free.
        let held = {
            let subs = self.subscriptions.lock_safe();
            let sub = subs.get(name)?;
            SubscriptionSnapshot {
                active: sub.is_active(),
                synced: sub.synced_index(),
                head_at_connect: sub.head_at_connect(),
                last_record_at_ms: sub.last_record_at_ms(),
                rebuilds_gap: sub.rebuilds().gap(),
                rebuilds_diverged: sub.rebuilds().diverged(),
                last_rebuild_at_ms: sub.rebuilds().last_at_ms(),
            }
        };
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
            active: held.active,
            synced: RecordIndex(held.synced),
            head_at_connect: RecordIndex(held.head_at_connect),
            owner,
            owner_live,
            generation,
            last_record_at_ms: held.last_record_at_ms.unwrap_or(0),
            rebuilds_gap: held.rebuilds_gap,
            rebuilds_diverged: held.rebuilds_diverged,
            last_rebuild_at_ms: held.last_rebuild_at_ms.unwrap_or(0),
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
        self.subscribe_as(name, resolve_timeout, Establish::Caller)
    }

    /// [`subscribe`](Self::subscribe), stating whether this is a caller's request or the machinery
    /// re-attaching something on its own initiative. Only the latter is throttled.
    fn subscribe_as(
        &self,
        name: &str,
        resolve_timeout: Option<Duration>,
        why: Establish,
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
        self.spawn_establish(name, &shared, why);
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
            self.spawn_establish(&name, &shared, Establish::Retry);
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
    fn spawn_establish(&self, name: &str, shared: &Arc<SubShared>, why: Establish) {
        // A caller's request is not a retry loop: it is never throttled, and it forgives whatever the
        // loop had accumulated, because the request is fresh evidence that somebody wants this now.
        if why == Establish::Caller {
            self.attach_now(name);
        }
        // One attempt at a time per subscription.
        if shared
            .establishing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        // **And one attempt per backoff window.** This is the choke point every caller reaches, which is why
        // the throttle belongs here: putting it on `attach_pending_members` alone throttled the wrong loop —
        // `service_subscriptions` re-spawns for every wanted-but-unconnected subscription on every tick, so
        // three thousand members whose owner accepts and never answers produced three thousand live threads
        // and eight hundred connects a second, with nothing logged, unchanged by that fix. Each attempt holds
        // a thread for up to the resolve, connect and handshake timeouts, so the retry rate *is* the thread
        // count.
        if why == Establish::Retry && !self.attach_due(name) {
            shared.establishing.store(false, Ordering::Release);
            return;
        }
        let (node, name) = (self.clone(), name.to_string());
        let for_thread = Arc::clone(shared);
        let on_failure = Arc::clone(shared);
        // **`Builder`, not `spawn`.** A bare `thread::spawn` *panics* if the OS refuses a thread, and this
        // is called from the maintenance loop, which nothing supervises — so a container hitting its pids
        // limit would take out the one thread that emits heartbeats, and every peer would declare this node
        // dead while it was otherwise fine. Establishment is retried every tick, so a refused thread costs
        // one tick; a panicking one costs the node. The flag has to be cleared on that path too, or the
        // subscription would believe an attempt was still in flight for ever.
        let spawned = std::thread::Builder::new()
            .name(format!("establish-{name}"))
            .spawn(move || {
                node.establish(&name, &for_thread);
                for_thread.establishing.store(false, Ordering::Release);
            });
        if spawned.is_err() {
            on_failure.establishing.store(false, Ordering::Release);
        }
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
                    // It connected: forget the penalty, so a link that drops later is retried promptly.
                    self.attach_backoff.lock_safe().remove(name);
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
    /// `MAX_BATCH_PER_POLL_ITEM` records per turn so none can head-of-line-block the others for
    /// a full drain.
    ///
    /// This is the loop §4.1 describes, and it comes with the coupling §4.1's budget note warns
    /// about, now real: a hot topic competes with replication forwarding for the same core, and a
    /// stall in one topic's mmap path briefly stalls forwarding too. That is the trade the shared
    /// loop makes in exchange for one thread instead of one per connection, and scheduling that is
    /// deterministic rather than at the mercy of N blocked threads waking in whatever order.
    ///
    /// Establishment is deliberately *not* here — see `spawn_establish`.
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
            advertise_control_addr: None,
            advertise_stream_addr: None,
            node_name: format!("test-{id}"),
            id_generated: false,
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

    /// [`start`] without the maintenance loop: it serves both planes and answers peers, but never
    /// emits a heartbeat of its own accord. For tests about what a node's *silence* means, where a
    /// heartbeat every few milliseconds would undo the thing under test.
    fn start_quiet(id: u64, dir: &str) -> (Node, SocketAddr, SocketAddr) {
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
        (node, stream_addr, control_addr)
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

    /// Poll until a condition holds, or fail.
    ///
    /// The budget is generous on purpose. These tests drive real sockets, threads and mmapped logs, so a
    /// loaded machine can stretch work that normally takes milliseconds into seconds — and a bounded poll
    /// with no clock control is then a coin flip, which is how this suite acquired an intermittent failure
    /// that could not be reproduced in twenty-four consecutive runs. A larger budget costs nothing when
    /// the condition holds (it returns immediately) and cannot mask a genuine failure, only delay it.
    fn poll_until<R>(mut f: impl FnMut() -> Option<R>) -> R {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Some(r) = f() {
                return r;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("condition not met within timeout");
    }

    /// **A member that cannot be attached must not be retried every tick for ever.** Deleting the per-tick
    /// resolve cap fixed a starvation bug and exposed this: a thousand members whose owners are live but
    /// unreachable-in-practice cost ~9 % of a core in a permanent connect loop, silently. Attaching backs
    /// off per member now, the same way dialling backs off per address.
    #[test]
    fn a_member_that_cannot_attach_backs_off_instead_of_retrying_every_tick() {
        let node = Node::new(config(160, temp_dir("attach-backoff")));

        assert!(node.attach_due("mem.a"), "a first attempt is never delayed");
        assert!(
            !node.attach_due("mem.a"),
            "a second attempt in the same window must be refused, or the tick rate becomes the retry rate"
        );

        // The gap escalates while attempts keep coming...
        let gap = node.attach_backoff.lock_safe().get("mem.a").unwrap().gap;
        assert_eq!(gap, ATTACH_BACKOFF_MIN);
        node.attach_backoff
            .lock_safe()
            .get_mut("mem.a")
            .unwrap()
            .next = Instant::now();
        assert!(node.attach_due("mem.a"));
        assert_eq!(
            node.attach_backoff.lock_safe().get("mem.a").unwrap().gap,
            ATTACH_BACKOFF_MIN * 2,
            "a repeated failure must widen the gap"
        );

        // ...and a member that attaches clears it, so a peer coming back is picked up promptly.
        node.attach_now("mem.a");
        assert!(node.attach_due("mem.a"), "a cleared penalty means due now");
    }

    /// **The attach ceiling must stay inside what a waiting caller will tolerate.** This is the whole
    /// reason the attach backoff has constants of its own: it originally reused the dialler's, and a
    /// dial's patience is affordable only because *both* ends re-dial. Escalate an attach to the same
    /// minute and a client that asked for a subscription gets a handle that will not replicate for the
    /// next 60 s — while its own wait for the replica is 5 s, so the call simply fails.
    #[test]
    fn the_attach_gap_is_capped_well_inside_a_callers_patience() {
        let node = Node::new(config(160, temp_dir("attach-ceiling")));

        // Escalate as hard as the machinery allows: every attempt due immediately, so the gap only ever
        // doubles. Whatever it converges to is the worst case a caller can be made to wait.
        for _ in 0..64 {
            assert!(node.attach_due("mem.a"));
            node.attach_backoff
                .lock_safe()
                .get_mut("mem.a")
                .unwrap()
                .next = Instant::now();
        }
        let p = *node.attach_backoff.lock_safe().get("mem.a").unwrap();
        assert_eq!(p.gap, ATTACH_BACKOFF_MAX, "the gap must saturate, not grow");
        assert!(
            ATTACH_BACKOFF_MAX < Duration::from_secs(5),
            "the ceiling must be under xchannel-net-client's five-second wait for a replica, or a \
             throttled retry outlasts the caller waiting on it: {ATTACH_BACKOFF_MAX:?}"
        );
    }

    /// **A caller's request is never throttled.** The throttle is there to stop the conductor's retry
    /// loop from spinning; a client calling `subscribe` is fresh evidence that somebody wants this now,
    /// and `subscribe`'s own doc promises a handle that is *already* replicating. Throttling it returned
    /// a handle that would not attach for seconds, and the client's wait for the replica then failed.
    #[test]
    fn a_callers_subscribe_is_not_throttled_by_the_retry_backoff() {
        let node = Node::new(config(160, temp_dir("attach-caller")));

        // Burn the name's budget the way a failing retry loop would, then escalate it to the ceiling.
        for _ in 0..8 {
            node.attach_due("mem.a");
            node.attach_backoff
                .lock_safe()
                .get_mut("mem.a")
                .unwrap()
                .next = Instant::now();
        }
        // One more attempt, this time leaving its gap standing: the loop is now throttled.
        assert!(node.attach_due("mem.a"));
        assert!(
            !node.attach_due("mem.a"),
            "premise: the retry loop is now throttled for this name"
        );

        // A caller's establishment forgives it; the machinery's does not.
        let shared = Arc::new(SubShared {
            replica_path: PathBuf::from("/nonexistent"),
            synced: AtomicU64::new(0),
            head_at_connect: AtomicU64::new(0),
            last_record_at_ms: AtomicU64::new(0),
            rebuilds: RebuildStats::default(),
            stopped: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            establishing: AtomicBool::new(false),
        });
        node.spawn_establish("mem.a", &shared, Establish::Caller);
        assert!(
            !node.attach_backoff.lock_safe().contains_key("mem.a"),
            "a caller's request must clear the penalty, not queue behind it"
        );
    }

    /// **An owner coming back must not be waited out.** The failures that built the penalty were the
    /// owner being unreachable; once it is live again the penalty is pure delay. Measured on an ordinary
    /// source-daemon restart: 0.009 s to resume with this clearing, 22 s without it.
    #[test]
    fn an_owner_returning_to_life_clears_its_members_attach_penalties() {
        let node = Node::new(config(160, temp_dir("attach-revive")));
        let owner = NodeId(4242);
        node.registry.lock_safe().merge(ChannelIdentity {
            name: "mem.a".to_string(),
            owner,
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1,
            epoch: 0,
            deleted: false,
            member_of: Some("t.orders".into()),
        });

        // The owner is down and the retries have earned a penalty.
        node.live_owners_noting_revivals(HashSet::new());
        assert!(node.attach_due("mem.a"));
        assert!(
            !node.attach_due("mem.a"),
            "premise: the name is throttled while its owner is away"
        );

        // It comes back.
        assert_eq!(
            node.live_owners_noting_revivals(HashSet::from([owner])),
            HashSet::from([owner]),
            "the live set must come back unchanged; the clearing is a side effect, not a filter"
        );
        assert!(
            node.attach_due("mem.a"),
            "the penalty must go with the reason for it, or the resume waits out a stale gap"
        );

        // Still live on the next pass is not a transition, so an ongoing failure keeps backing off.
        assert!(!node.attach_due("mem.a"));
        node.live_owners_noting_revivals(HashSet::from([owner]));
        assert!(
            !node.attach_due("mem.a"),
            "a live owner that stays live must not clear the penalty every tick, or the throttle is gone"
        );
    }

    /// **Members must not escalate in lockstep.** Every member of a topic whose owner is away fails on
    /// the same tick, so a shared gap makes them all come due on the same tick too — 3000 members
    /// meaning 3000 simultaneous establishment threads, every ceiling-width window. The jitter is
    /// derived from the name so it needs no clock and no randomness, and it only ever *delays*.
    #[test]
    fn attach_retries_are_spread_so_members_do_not_come_due_together() {
        let node = Node::new(config(160, temp_dir("attach-jitter")));
        let names: Vec<String> = (0..1000).map(|i| format!("topic.member.{i}")).collect();

        // Every member fails on the same pass, which is the realistic case: their owner is away.
        let base = Instant::now();
        for n in &names {
            assert!(node.attach_due(n));
        }

        // When each becomes due again, bucketed at 100 ms. Without a spread every member carries the
        // same gap from the same pass and they all land in one bucket — a thousand establishment
        // threads on one tick, then nothing, for ever.
        let buckets: HashSet<u64> = {
            let backoff = node.attach_backoff.lock_safe();
            names
                .iter()
                .map(|n| (backoff[n].next.duration_since(base).as_micros() as u64) / 100_000)
                .collect()
        };
        assert!(
            buckets.len() > 1,
            "a thousand members must not all come due in the same 100 ms: {} bucket(s)",
            buckets.len()
        );

        // And the spread only ever delays, by at most half the gap, so it cannot push a member past
        // the ceiling the previous test pins.
        let latest = {
            let backoff = node.attach_backoff.lock_safe();
            names
                .iter()
                .map(|n| backoff[n].next.duration_since(base))
                .max()
                .unwrap()
        };
        assert!(
            latest < ATTACH_BACKOFF_MIN + ATTACH_BACKOFF_MIN / 2 + Duration::from_secs(1),
            "the spread must be a fraction of the gap, not a multiple of it: {latest:?}"
        );

        // Stable per name, or a member's own gap would jump around between attempts.
        assert_eq!(
            jitter_for("topic.member.7", Duration::from_secs(4)),
            jitter_for("topic.member.7", Duration::from_secs(4))
        );
    }

    /// **A retired subscription takes its penalty with it.** The map is keyed on the bare name, and
    /// nothing else prunes it, so a name reclaimed at `epoch + 1` — a respawned member, or an operator
    /// re-registering after a `force_deregister` — would inherit the dead incarnation's backoff and be
    /// throttled for its predecessor's failures.
    #[test]
    fn retiring_a_subscription_drops_its_attach_penalty() {
        let node = Node::new(config(160, temp_dir("attach-retire")));
        assert!(node.attach_due("mem.a"));
        assert!(!node.attach_due("mem.a"), "premise: throttled");
        node.retire_subscription("mem.a");
        assert!(
            node.attach_due("mem.a"),
            "a reclaimed name must start with a clean slate"
        );
    }

    /// **A cap over a deterministically-ordered walk starves whatever sorts last.** Reproduced three
    /// times independently: a topic whose first few members alphabetically have dead owners never
    /// merged *any* of its live members, because those members consumed the whole per-tick resolve
    /// budget on every tick for ever and the walk always reached them first. Registry order is
    /// `BTreeMap` order, so it was the same names every time — and nothing logged it.
    ///
    /// Two properties, both asserted here: a member whose owner cannot be reached costs nothing, and a
    /// live member behind it still gets attached.
    #[test]
    fn a_member_whose_owner_is_unreachable_cannot_starve_a_live_one() {
        let (a, _a_stream, a_control) = start(150, "attach-starve-a");
        let (b, _b_stream, _b_control) = start_seeded(151, "attach-starve-b", &[a_control]);
        a.create_topic("t.orders", TopicOptions::default()).unwrap();

        // Members owned by a node nobody can reach, named to sort *before* the live one. This is the
        // ordinary shape: an owner was decommissioned, and `member_reap_after` defaults to never, so
        // its member registrations stay in the CRDT for good.
        for i in 0..8 {
            a.registry.lock_safe().merge(ChannelIdentity {
                name: format!("aaa.{i}"),
                owner: NodeId(9999),
                region_size: 1 << 20,
                mtu: 0,
                earliest_index: RecordIndex(0),
                registered_at_nanos: 1,
                epoch: 0,
                deleted: false,
                member_of: Some("t.orders".into()),
            });
        }

        // A live member on B, sorting last.
        b.publish_to_topic("t.orders", "zzz.live", ChannelOptions::default())
            .unwrap();

        // It must attach despite the eight unreachable members ahead of it in the walk.
        poll_until(|| {
            a.topic_status("t.orders")
                .and_then(|s| s.ok())
                .filter(|s| s.members.iter().any(|m| m.name == "zzz.live"))
                .map(|_| ())
        });
    }

    /// **A duplicate is a standing condition, so the verdict must be re-evaluated, not latched.**
    /// Rate-limiting is for the message. A node that owned a channel when it first noticed the
    /// duplicate cannot stand aside then — that would orphan the channel — but it must be able to once
    /// it owns nothing, which is the only state in which standing aside is safe at all.
    #[test]
    fn a_duplicate_verdict_is_re_evaluated_not_latched_by_the_rate_limit() {
        let mut cfg = config(141, temp_dir("dup-verdict"));
        cfg.id_generated = true;
        let node = Node::new(cfg);
        crate::node_identity::resolve(&node.config.data_dir, None, Some("n".into())).unwrap();

        // While it owns a channel, standing aside would orphan it.
        let _w = node.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
        assert_eq!(
            node.report_duplicate_identity(&[NodeId(141)]),
            OnDuplicateIdentity::Continue
        );

        // Once it owns nothing, the same detection must reach the opposite conclusion — even though
        // the warning for this id has already been printed and will not be printed again.
        drop(_w);
        node.deregister("md.aapl").unwrap();
        assert_eq!(
            node.report_duplicate_identity(&[NodeId(141)]),
            OnDuplicateIdentity::StepAside,
            "the first tick's answer was latched, so this node could never stand aside"
        );
    }

    /// **The lock order is an invariant with nothing enforcing it, so assert it.**
    ///
    /// `deregister_topic` must not hold the registry lock while it disseminates the tombstone.
    /// Written as a direct probe rather than as a race: this thread takes the locks in the
    /// documented order (dissemination, then registry) while the retirement runs, which is exactly
    /// what `dial_peer` and `serve_control` do. If the retirement holds the registry lock across its
    /// announce, this thread can never acquire it and the two are deadlocked — so the probe is a
    /// bounded `try_lock` loop, which fails the test in two seconds instead of hanging the suite.
    #[test]
    fn retiring_a_topic_never_holds_the_registry_lock_while_disseminating() {
        let node = Node::new(config(140, temp_dir("lock-order")));
        node.create_topic("t.orders", TopicOptions::default())
            .unwrap();

        // Hold dissemination, as any dialling or accepting thread does.
        let held = node.dissemination.lock_safe();

        let worker = {
            let node = node.clone();
            std::thread::spawn(move || node.deregister_topic("t.orders"))
        };
        // Let the worker reach the point where it wants the dissemination lock we are holding.
        std::thread::sleep(Duration::from_millis(50));

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got_registry = false;
        while Instant::now() < deadline {
            if let Ok(guard) = node.registry.try_lock() {
                drop(guard);
                got_registry = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        // **The probe proves nothing unless the worker is actually blocked on the lock we hold.**
        // `deregister_topic` returns `Ok(true)` unconditionally, so asserting on its result showed only
        // that a mux existed: with the announce removed entirely the test still passed.
        assert!(
            !worker.is_finished(),
            "the retirement completed without ever wanting the dissemination lock, so acquiring the \
             registry lock below demonstrates nothing about the order"
        );
        // Release the worker either way, so it can finish and the test can end cleanly.
        drop(held);
        assert!(
            got_registry,
            "deregister_topic held the registry lock while waiting for the dissemination lock — \
             that inverts this type's lock order and deadlocks the whole control plane"
        );
        assert!(worker.join().unwrap().unwrap(), "the topic was retired");
    }

    /// The maintenance loop is also the heartbeat loop, so what it spends on dialling is taken
    /// directly out of this node's liveness. Unbounded, the candidate set — which grows with every
    /// address the mesh has ever mentioned — set the heartbeat period, and a dozen decommissioned
    /// hosts were enough to have every peer declare this healthy node dead, at which point the topic
    /// member reaper began tombstoning its live members' names.
    #[test]
    fn dialling_is_capped_per_tick_and_backs_off_per_address() {
        // Six addresses with nothing behind them: bind, keep the port, drop the listener.
        let dead: Vec<SocketAddr> = (0..6)
            .map(|_| {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                l.local_addr().unwrap()
            })
            .collect();
        let node = Node::new(NodeConfig {
            seeds: dead.clone(),
            ..config(120, temp_dir("dial-budget"))
        });

        node.connect_seeds();
        assert_eq!(
            node.dial_backoff.lock_safe().len(),
            MAX_DIALS_PER_TICK,
            "a tick must attempt at most MAX_DIALS_PER_TICK addresses, whatever the candidate count"
        );

        // A second tick must move on to *fresh* addresses rather than retry the two already known
        // to be unreachable — otherwise a permanently dead address at the head of the list starves
        // every live peer behind it, which is the same starvation the cap exists to prevent.
        node.connect_seeds();
        assert_eq!(
            node.dial_backoff.lock_safe().len(),
            2 * MAX_DIALS_PER_TICK,
            "addresses in backoff must be skipped, and the walk must continue where it left off"
        );

        // Every attempt carries a penalty that grows.
        for (addr, penalty) in node.dial_backoff.lock_safe().iter() {
            assert!(
                penalty.gap >= DIAL_BACKOFF_MIN && penalty.gap <= DIAL_BACKOFF_MAX,
                "{addr} has an out-of-range backoff {:?}",
                penalty.gap
            );
        }

        // The seed walk must not disturb the learned walk. One shared cursor was reduced modulo each
        // list in turn, so a one-seed configuration pinned the learned walk to a constant index
        // forever — the rotation existed only in the comments.
        assert_eq!(
            *node.dial_cursor_learned.lock_safe(),
            0,
            "dialling seeds moved the learned-peer cursor"
        );
    }

    /// **An address that accepts and then drops the link must still back off.** This is the case a
    /// success-only penalty missed entirely: a seed or hint naming a stream port, or a peer whose
    /// control frames this release cannot decode. The connect succeeds, so nothing was recorded; the
    /// link dies, so `connected` clears; and the next tick spends a dial on it again — forever, for
    /// as many such addresses as fit in the budget, starving every live peer behind them.
    #[test]
    fn an_address_that_accepts_and_drops_the_link_still_backs_off() {
        // A listener that accepts, **takes our join**, and then hangs up — which is what a peer on an
        // incompatible release or a seed naming a stream port actually does. Reading first matters: it
        // makes our writes succeed, so any reset keyed on the connect or the write "working" fires, and
        // the address is pinned at the floor for ever instead of backing off.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut conn) = conn else { continue };
                // One bounded read, then drop. The timeout matters: an unbounded read on a socket with
                // nothing left to send never returns, so the connection would never be closed and this
                // node would go on believing the link is up.
                let _ = conn.set_read_timeout(Some(Duration::from_millis(50)));
                let mut sink = [0u8; 4096];
                let _ = std::io::Read::read(&mut conn, &mut sink);
            }
        });

        let node = Node::new(NodeConfig {
            seeds: vec![addr],
            ..config(122, temp_dir("dial-accept-drop"))
        });
        node.connect_seeds();
        let gap = node
            .dial_backoff
            .lock_safe()
            .get(&addr)
            .expect("an address that accepted the connection and gave us nothing must be penalised")
            .gap;
        assert_eq!(gap, DIAL_BACKOFF_MIN);

        // ...and it must not be retried while that penalty stands, however many ticks run.
        for _ in 0..5 {
            node.connect_seeds();
        }
        let gap = node.dial_backoff.lock_safe().get(&addr).unwrap().gap;
        assert_eq!(
            gap, DIAL_BACKOFF_MIN,
            "the address was dialled again inside its own backoff window"
        );

        // **And it must keep escalating across attempts.** Asserting the floor after a single dial cannot
        // fail — the floor is where every first attempt starts — so force the gap due and dial again. This
        // is the assertion that catches a reset keyed on the connect or the write succeeding, which both do
        // for an address that accepts, reads, and hangs up.
        for expected in [DIAL_BACKOFF_MIN * 2, DIAL_BACKOFF_MIN * 4] {
            // Wait for the far end's hangup to be noticed, or there is nothing to re-dial: while the
            // link is believed up, declining to dial is correct.
            poll_until(|| (!node.dissemination.lock_safe().is_connected(addr)).then_some(()));
            node.dial_backoff.lock_safe().get_mut(&addr).unwrap().next = Instant::now();
            node.connect_seeds();
            assert_eq!(
                node.dial_backoff.lock_safe().get(&addr).unwrap().gap,
                expected,
                "an address that accepts and gives us nothing must keep backing off, not reset"
            );
        }
    }

    /// The other half of that rule, and the reason no "this address once worked" exemption is needed:
    /// **a link that outlives its own gap has already paid the penalty from the dial that created
    /// it.** So a departed peer is re-dialled promptly on the merits, while a *flapping* one — dying
    /// inside its gap — correctly waits. The exemption that used to be here keyed on a memo that is
    /// never pruned, so it applied every tick forever and the gap never doubled: four departed hosts
    /// cost a dial each per tick, taking the heartbeat period from 0.5 s to a sustained 4.5 s.
    #[test]
    fn a_penalty_expires_on_its_own_and_decays_when_attempts_stop() {
        let node = Node::new(config(125, temp_dir("dial-decay")));
        // Nothing listens here; the dial fails fast with ECONNREFUSED.
        let addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };

        node.dial_peer(addr);
        assert_eq!(
            node.dial_backoff.lock_safe().get(&addr).unwrap().gap,
            DIAL_BACKOFF_MIN
        );
        assert!(!node.dial_due(addr), "the gap has not elapsed yet");

        // An elapsed gap is the entire mechanism — no exemption of any kind is consulted.
        node.dial_backoff.lock_safe().get_mut(&addr).unwrap().next = Instant::now();
        assert!(
            node.dial_due(addr),
            "a peer whose link outlasted its gap must be re-dialled promptly, which is why no \
             permanent exemption is needed"
        );

        // Escalation, while attempts keep coming.
        node.dial_peer(addr);
        assert_eq!(
            node.dial_backoff.lock_safe().get(&addr).unwrap().gap,
            DIAL_BACKOFF_MIN * 2,
            "a repeated attempt must double the gap"
        );

        // Decay, once they stop: the doubling must not be monotone for the life of the process.
        node.dial_backoff
            .lock_safe()
            .get_mut(&addr)
            .unwrap()
            .attempted_at = Instant::now() - DIAL_BACKOFF_MAX * 3;
        node.dial_peer(addr);
        assert_eq!(
            node.dial_backoff.lock_safe().get(&addr).unwrap().gap,
            DIAL_BACKOFF_MIN,
            "a long-quiet address must start again at the floor"
        );
    }

    /// A wildcard address is a bind target, never a destination. A peer that advertises the address
    /// it bound — which is what every node does — teaches the mesh `0.0.0.0:7001`, and dialling that
    /// opens a link to *this* host instead of to the peer.
    #[test]
    fn a_wildcard_candidate_is_never_dialled() {
        let node = Node::new(NodeConfig {
            seeds: vec!["0.0.0.0:7001".parse().unwrap()],
            ..config(121, temp_dir("dial-wildcard"))
        });
        node.connect_seeds();
        assert!(
            node.dial_backoff.lock_safe().is_empty(),
            "an unspecified address must not even be attempted"
        );
    }

    /// **Convergence has to work in both directions.** A peer that sends a registry state which
    /// *loses* the merge learns nothing from the recipient's silence, and join-time anti-entropy
    /// only runs when a link is established — so on a link that stays up, the two would disagree
    /// about who owns a channel indefinitely.
    ///
    /// Arranged so only the reply can carry the winner: the link is established while A's registry
    /// is empty (so its anti-entropy snapshot is empty), and A learns the winner afterwards by a
    /// merge that announces nothing. That is the ordinary steady state — a node does not re-announce
    /// entries it registered long ago — and it is exactly the case a fresh node's late registration
    /// lands in.
    #[test]
    fn a_peer_holding_a_losing_state_is_told_the_winner() {
        let (a, _a_stream, a_control) = start(130, "reply-winner-a");
        let mut peer = TcpTransport::connect(a_control).unwrap();

        // A learns the winner *after* the link, and by merging rather than registering, so nothing
        // is announced and the snapshot the peer already received cannot have carried it.
        let winner = ChannelIdentity {
            name: "md.aapl".into(),
            owner: NodeId(130),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1,
            epoch: 0,
            deleted: false,
            member_of: None,
        };
        // Wait until A has actually adopted the link, so the merge below cannot land in the window
        // before the (empty) anti-entropy snapshot is sent.
        poll_until(|| (a.dissemination.lock_safe().peer_count() > 0).then_some(()));
        a.registry.lock_safe().merge(winner.clone());

        // A second name, so the reply has something to coalesce.
        let winner2 = ChannelIdentity {
            name: "md.msft".into(),
            ..winner.clone()
        };
        a.registry.lock_safe().merge(winner2.clone());

        // The peer announces states that lose: same names, later registration.
        let loser = ChannelIdentity {
            owner: NodeId(999),
            registered_at_nanos: u64::MAX,
            ..winner.clone()
        };
        let loser2 = ChannelIdentity {
            owner: NodeId(999),
            registered_at_nanos: u64::MAX,
            ..winner2.clone()
        };
        peer.send_frame(&codec::encode_control(
            &xchannel_net_core::wire::ControlMsg::RegistryDelta(vec![
                loser.clone(),
                loser2.clone(),
            ]),
        ))
        .unwrap();

        // A's map did not move, so there is nothing to flood — but the peer must still be corrected.
        // Bounded by frames as well as by time: A heartbeats continuously, so a test that only
        // relied on a read timeout would never stop reading if the reply never came.
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut replied = false;
        for _ in 0..500 {
            let Ok(frame) = peer.recv_frame() else { break };
            if let Ok(xchannel_net_core::wire::ControlMsg::RegistryDelta(ids)) =
                codec::decode_control(&frame)
            {
                assert!(
                    !ids.contains(&loser),
                    "A must not echo the losing state back as though it had won"
                );
                if ids.contains(&winner) {
                    // **One frame, both winners.** A frame can carry any number of identities, and
                    // replying per identity meant a lock acquisition and a blocking write per
                    // identity — 200 000 of them stalled the node's heartbeat for forty seconds.
                    assert!(
                        ids.contains(&winner2),
                        "the reply must coalesce a pump cycle's winners into one frame, not send \
                         one frame per identity"
                    );
                    replied = true;
                    break;
                }
            }
        }
        assert!(
            replied,
            "A held the winner and said nothing: the peer that sent the losing state would keep it \
             forever, since anti-entropy only runs when a link is established"
        );
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
        let (_b, _b_stream, b_control) = start_seeded(81, "mesh-b", &[a_control]);
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
        let (_b, _b_stream, b_control) = start(85, "asym-b");
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

    /// Discovering that another machine shares our **generated** id must actually make this node
    /// step aside — not merely delete the id file and carry on with the duplicate still live.
    ///
    /// Deleting the file alone would be the worst of both: the id is already in this process, so
    /// nothing about the collision changes, and now the record of it is gone too. The rescue this
    /// exists for is a golden image snapshotted after first start, where every clone shares one id
    /// and owns nothing; if the clones do not actually stop, none of them ever takes a fresh id.
    #[test]
    fn a_duplicate_generated_id_makes_an_empty_node_step_aside() {
        let dir = temp_dir("dup-step-aside");
        let node = Node::new(NodeConfig {
            id_generated: true,
            ..config(4242, dir.clone())
        });
        // Pretend the id came from `.node_id`, as a generated one does.
        std::fs::write(crate::node_identity::id_path(&dir), "id=4242\n").unwrap();

        assert_eq!(
            node.report_duplicate_identity(&[NodeId(4242)]),
            OnDuplicateIdentity::StepAside,
            "it owns nothing, so it must stand down rather than keep the duplicate live"
        );
        assert!(
            !crate::node_identity::is_persisted(&dir),
            "and the id must actually be discarded, or the restart changes nothing"
        );
    }

    /// Once a node owns a channel its id is referenced by that channel's registry entry, so
    /// changing it would leave the channel owned by an id that never comes back — frozen until an
    /// operator reclaims the name. Past that point the only safe action is to complain.
    #[test]
    fn a_duplicate_id_is_only_warned_about_once_the_node_owns_something() {
        let dir = temp_dir("dup-owns");
        let node = Node::new(NodeConfig {
            id_generated: true,
            ..config(4243, dir.clone())
        });
        std::fs::write(crate::node_identity::id_path(&dir), "id=4243\n").unwrap();
        drop(node.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap());

        assert_eq!(
            node.report_duplicate_identity(&[NodeId(4243)]),
            OnDuplicateIdentity::Continue
        );
        assert!(
            crate::node_identity::is_persisted(&dir),
            "a node that owns channels must keep its id"
        );
    }

    /// An operator-set id is not ours to discard, and someone else's collision is not ours to act
    /// on at all.
    #[test]
    fn a_configured_id_and_a_third_partys_collision_are_only_warned_about() {
        let dir = temp_dir("dup-configured");
        let configured = Node::new(NodeConfig {
            id_generated: false,
            ..config(4244, dir.clone())
        });
        std::fs::write(crate::node_identity::id_path(&dir), "id=4244\n").unwrap();
        assert_eq!(
            configured.report_duplicate_identity(&[NodeId(4244)]),
            OnDuplicateIdentity::Continue,
            "an explicitly configured id is the operator's to fix"
        );
        assert!(crate::node_identity::is_persisted(&dir));

        let other = Node::new(NodeConfig {
            id_generated: true,
            ..config(4245, temp_dir("dup-other"))
        });
        assert_eq!(
            other.report_duplicate_identity(&[NodeId(9999)]),
            OnDuplicateIdentity::Continue,
            "two other nodes colliding is not this node's problem to solve"
        );
    }

    /// A clean shutdown makes a peer drop the departing node from its live set **at once**,
    /// instead of waiting out `LIVENESS_TIMEOUT`. That is the whole point of announcing it: for ten
    /// seconds otherwise, a subscriber keeps believing the departed node's channels are reachable
    /// and keeps trying to replicate from them.
    #[test]
    fn a_clean_shutdown_makes_peers_drop_the_node_immediately() {
        // A runs **without a maintenance loop**. `shutdown()` announces the departure but does not
        // stop the daemon's threads — in a real daemon the process exits immediately afterwards, but a
        // test node lives on, and a node that keeps heartbeating puts itself straight back into its
        // peer's live set. The assertion below would then be chasing a state that exists only between
        // two heartbeats, which is a race that a loaded machine loses.
        let (a, _a_stream, a_control) = start_quiet(96, "leave-a");
        let (b, _b_stream, _b_control) = start_seeded(97, "leave-b", &[a_control]);

        // B sees A as live once their link is up.
        poll_until(|| b.dissemination.lock_safe().live_addr_of(NodeId(96)));

        a.shutdown();

        // ...and stops, without ten seconds passing. The address is retained — B still knows where
        // A was, it just knows A is gone.
        poll_until(|| {
            b.dissemination
                .lock_safe()
                .live_addr_of(NodeId(96))
                .is_none()
                .then_some(())
        });
        assert!(
            b.dissemination.lock_safe().node_at(a_control).is_some(),
            "a departure forgets liveness, not the address"
        );
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

        assert!(m.learn(NodeId(9), stream, control, "n9"), "new to us");
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
        m.record(NodeId(9), stream, control, "n9");
        assert_eq!(m.live_addr_of(NodeId(9), timeout), Some(stream));

        // And later hearsay must not undo that.
        m.learn(NodeId(9), stream, control, "n9");
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
        // A floor, so the liveness check is not the only thing standing between this test and a reclaim.
        let (b, _b_stream, _b_control) = start_with(NodeConfig {
            reclaim_after: Duration::from_secs(300),
            ..config(112, temp_dir("reclaim-guard-b"))
        });
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

        // **Refusal is the property; which guard refuses is timing.** Liveness is a ten-second window with
        // no clock control here, so a loaded machine can deschedule this thread long enough for A to fall
        // out of it, and the refusal then cites silence rather than liveness.
        //
        // B's floor is deliberately *not* zero, which is what makes this safe to assert once. With a zero
        // floor the liveness check is the only guard, so a single unlucky sample would let the reclaim
        // **succeed** — and an earlier version of this test retried the call to get the message it wanted,
        // which meant the retry could itself tombstone the channel and destroy the premise. Never retry a
        // destructive call to make an assertion pass.
        let err = b.force_deregister("md.aapl").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        let why = err.to_string();
        assert!(
            why.contains("live member") || why.contains("unreachable for"),
            "a reclaim must be refused while the owner is live or too recently silent: {why}"
        );

        // The owner must use the ordinary owner-only path for its own channels.
        let err = a.force_deregister("md.aapl").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("use deregister"), "{err}");

        // A node that has never had contact with an owner must judge it on how long *the owner* has
        // been unreachable from here — which for a daemon that has only just learned of it is no
        // time at all. It must not substitute its own uptime, which is not an observation about the
        // owner and which every long-running daemon satisfies unconditionally.
        let mut cfg = config(113, temp_dir("reclaim-guard-c"));
        cfg.reclaim_after = Duration::from_secs(300);
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

        // Once the owner *has* been unreachable from here for the whole floor, the reclaim is
        // allowed — otherwise a survivor could never relocate the name of a host that was
        // decommissioned before it ever met it.
        fresh
            .owner_unreachable_since
            .lock_safe()
            .insert(NodeId(999), Instant::now() - Duration::from_secs(600));
        assert!(fresh.force_deregister("theirs").unwrap());

        // An unknown name is simply nothing to do.
        assert!(!fresh.force_deregister("nope").unwrap());
    }

    /// The other half of the reclaim guard: a node that **has** had contact can reclaim once the owner is
    /// gone. Refusing here too would make the guard useless — a graceful departure clears liveness, and if
    /// that also erased the record that contact ever happened, no node could reclaim a name from a peer
    /// that said goodbye.
    ///
    /// A is started **without a maintenance loop**: `shutdown()` announces the departure but does not stop
    /// a node's threads, so a heartbeating A would put itself straight back into B's live set and this
    /// would be a race rather than a test.
    #[test]
    fn a_departed_owners_name_can_be_reclaimed_by_a_node_that_knew_it() {
        let (a, _a_stream, a_control) = start_quiet(114, "reclaim-departed-a");
        let (b, _b_stream, _b_control) = start(115, "reclaim-departed-b");
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
                    .live_addr_of(NodeId(114))
                    .is_some())
            .then_some(())
        });

        a.shutdown();
        poll_until(|| {
            b.dissemination
                .lock_safe()
                .live_addr_of(NodeId(114))
                .is_none()
                .then_some(())
        });
        assert!(
            b.force_deregister("md.aapl").unwrap(),
            "B heard from A and then A left, so B can say how long it has been gone"
        );
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
