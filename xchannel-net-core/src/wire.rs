//! Wire frames for the two protocols.
//!
//! These are the on-the-wire *shapes*; the concrete encoding (length-prefix, varint,
//! etc.) is deferred until we pick a serialization in a later step. Keeping them as
//! plain Rust types first lets us reason about the protocol before committing bytes.

use crate::identity::{ChannelIdentity, ChannelName};
use crate::{NodeId, RecordIndex, StreamId};
use std::net::SocketAddr;

/// One self-describing log record as it travels on the data plane.
///
/// The record fields mirror an xchannel `User` record exactly; `Skip` markers are local
/// artifacts of the source's region geometry and never cross the network. `starts_segment`
/// is the one exception to "file geometry is local" — see its docs.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RecordFrame {
    /// Logical position in the stream (counts only `User` records).
    pub index: RecordIndex,
    /// Application discriminant (xchannel `message_type`).
    pub msg_type: u16,
    /// Opaque per-message metadata (xchannel `user_meta_u64`).
    pub user_meta: u64,
    /// The origin rolled to a new segment immediately before this record — i.e. this is the
    /// first record of a new file at the source. An **advisory** hint the sink may ignore:
    /// honoring it makes the replica's file boundaries mirror the origin's, which is what
    /// makes `keep_files` retention mean the same thing on both sides. Without it a replica
    /// rolls only when its own `file_roll_size` says so, and an origin that rolls explicitly
    /// (`Writer::roll_file`, e.g. to start each segment with a snapshot) with no
    /// `file_roll_size` set would leave its replicas growing as one unbounded file.
    ///
    /// The boundary travels *with* the record it precedes, so it cannot desynchronize: there
    /// is no separate signal to lose, and a resuming subscriber gets the flag re-derived from
    /// the source's own segmentation on reconnect. Never set on the first record a source
    /// produces (nothing to roll away from). Carrying it costs one byte per record.
    pub starts_segment: bool,
    /// Payload bytes.
    pub payload: Vec<u8>,
}

/// Control-plane messages: low volume, latency-tolerant, separate connection.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ControlMsg {
    /// A client asks its local manager to register a channel it owns.
    Register(ChannelIdentity),
    /// Owner withdraws a channel it registered.
    Deregister { name: ChannelName, owner: NodeId },
    /// Eager broadcast of registry changes, pushed to all peers on register/deregister.
    /// Fed into the CRDT merge; idempotent, so duplicates and reordering are harmless.
    RegistryDelta(Vec<ChannelIdentity>),
    /// Join-time anti-entropy: the sender's full registry, exchanged on (re)connect so a
    /// peer catches up on anything it missed while disconnected.
    RegistrySync(Vec<ChannelIdentity>),
    /// Node membership heartbeat (membership liveness, distinct from writer liveness).
    /// Carries the sender's stream-plane address so peers can resolve `owner: NodeId`
    /// (from a [`ChannelIdentity`]) to where they must connect to subscribe — the
    /// separate-membership-map approach (DESIGN §9: identity stays address-free).
    Heartbeat {
        node: NodeId,
        addr: SocketAddr,
        /// Where to open a peer link to the sender, so a node that learns of it can dial it and
        /// the mesh forms itself from any connected seed graph.
        control_addr: SocketAddr,
    },
    /// What a peer knows *about a third node* — its addresses, and nothing about its liveness.
    ///
    /// Deliberately not a relayed [`Heartbeat`]. A heartbeat means "I heard from this node";
    /// forwarding one would make that claim on someone else's behalf, and membership liveness is
    /// specifically *this* node's ability to reach another (DESIGN §5.4). A hint means only "it
    /// exists, here is where" — enough to dial it and find out first-hand.
    ///
    /// Relayed only when it teaches the receiver something it did not know, which is what makes
    /// the flood terminate on a mesh with cycles.
    PeerHint {
        node: NodeId,
        addr: SocketAddr,
        control_addr: SocketAddr,
    },
    /// Registration was rejected because another registration won the name.
    RegisterRejected { name: ChannelName, winner: NodeId },
}

/// Stream-plane messages on a source→subscriber connection: high volume, ordered,
/// resumable. The connection is **multiplexed** — one TCP (or other) link carries any
/// number of subscriptions, each identified by the [`StreamId`] the source assigns in
/// [`SubscribeAck`](StreamMsg::SubscribeAck).
///
/// Cursor ownership (DESIGN.md §5.2.1): the **subscriber** carries its resume position
/// — recovered from its own replica (count of applied `User` records) — and re-asserts
/// it via [`Subscribe::from`](StreamMsg::Subscribe). The source persists no per-subscriber
/// cursor; on reconnect it simply streams from where the subscriber says it is.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum StreamMsg {
    /// Subscriber → source: open a stream for `name`, resuming at `from`.
    ///
    /// `from` is the **absolute** index the subscriber wants next = `base + n`, where
    /// `base` is its replica's first absolute index (read from the replica's
    /// `ChannelHeader.base_record_index`) and `n` the records it holds. It is *not* a plain
    /// count — counting breaks for a retention-truncated replica (`base > 0`); see
    /// [`RecordIndex`](crate::RecordIndex).
    /// `RecordIndex(0)` ⇔ empty replica ⇔ "full retained history". Per the "always full
    /// history" decision there is no other start negotiation.
    Subscribe {
        name: ChannelName,
        from: RecordIndex,
        /// The incarnation the subscriber's replica holds — `xchannel`'s
        /// `ChannelHeader.generation`, which for a channel created by this system is the
        /// registry's reclaim `epoch`. `0` when the subscriber has no replica yet (`from ==
        /// 0`), and also for any channel whose name has never been reclaimed, so the check
        /// this feeds is inert on the common path.
        ///
        /// The source compares it against its own before seeking: a mismatch means the two
        /// sides are looking at different logs that merely share a name, and no resume
        /// position can be valid. Sent in `Subscribe` rather than checked against the ack
        /// because the seek — which blocks indefinitely on an unreachable position — happens
        /// before the ack is written.
        generation: u64,
    },

    /// Source → subscriber: subscription accepted; records for it will carry `stream_id`.
    ///
    /// * `start` — the first index the source will actually send. `start == from` is a
    ///   clean resume. `start > from` happens only when `from == 0` and genesis has been
    ///   retention-truncated; the replica then legitimately begins at `start` (full
    ///   *retained* history). A non-zero `from` that the source can't satisfy yields
    ///   [`Gap`](StreamMsg::Gap) instead, never a silent jump.
    /// * `head` — the source's current high-water index (committed `User` record count) at
    ///   accept time. The subscriber is "synchronized" once it has applied up to `head`;
    ///   historical replay and live tail are the same stream (no explicit catch-up signal).
    /// * `region_size` / `mtu` — the source channel's authoritative geometry, so the sink
    ///   builds a replica `Writer` guaranteed to fit every record (the registry copy may be
    ///   stale; the source is the source of truth).
    /// * `file_roll_size` / `keep_files` — the source's rolling + retention policy, so the
    ///   replica inherits the same bounds rather than growing as one unbounded file
    ///   (`file_roll_size = 0` ⇒ no rolling; `keep_files = 0` ⇒ unlimited retention).
    SubscribeAck {
        name: ChannelName,
        stream_id: StreamId,
        start: RecordIndex,
        head: RecordIndex,
        region_size: u32,
        mtu: u32,
        file_roll_size: u64,
        keep_files: u32,
        /// The source's incarnation, stamped into a freshly created replica so the replica's
        /// own files record which log they were built from — and can say so on the next
        /// resume without any node-owned metadata (DESIGN §5). Ignored when reopening an
        /// existing replica: xchannel keeps the on-disk value, so a replica cannot be
        /// relabelled by a source that claims otherwise.
        generation: u64,
    },

    /// Source → subscriber: one replicated record on `stream_id`. The frame carries its
    /// own `index` so the sink asserts contiguity before applying.
    Record {
        stream_id: StreamId,
        frame: RecordFrame,
    },

    /// Source → subscriber, in place of `SubscribeAck`: the subscriber's non-zero `from`
    /// is older than what the source still retains (`earliest > from`), so its partial
    /// replica cannot be extended contiguously — an explicit, non-silent gap (cf. Kafka
    /// "offset out of range"). `earliest`/`head` let the subscriber decide whether to
    /// discard its replica and re-subscribe from `RecordIndex(0)` to rebuild from
    /// `earliest`. (Handling policy is an open question — DESIGN.md §8.)
    Gap {
        name: ChannelName,
        earliest: RecordIndex,
        head: RecordIndex,
    },

    /// Source → subscriber, in place of `SubscribeAck`: the subscriber's replica is **not a
    /// continuation of this channel**, so no resume position it offers can be valid. Distinct
    /// from [`Gap`](StreamMsg::Gap), which means "your position is real but too old" — here the
    /// position is meaningless, and the only recovery is to discard the replica and
    /// re-subscribe from `RecordIndex(0)`.
    ///
    /// Two triggers, checked in this order:
    ///
    /// 1. **Generation mismatch** — the subscriber's replica carries a different
    ///    `ChannelHeader.generation` than the source. Precise: it holds however far apart the
    ///    two logs are, including when the new incarnation has already grown past the old
    ///    one's length.
    /// 2. **`from` past `head`** — the source has never held a record at that index, so the
    ///    replica cannot be a prefix of it. Imprecise but independent of the generation
    ///    plumbing, and it catches a subscriber too old to send one.
    ///
    /// The mismatch is checked first: a replica from another incarnation may *also* look
    /// behind retention, and reporting that would name the wrong problem. Detecting either
    /// *before* the source seeks is what keeps that seek from blocking forever on records that
    /// will never exist.
    ///
    /// `earliest`/`head` describe what the source actually holds, so the subscriber can report
    /// the divergence rather than merely retrying.
    Diverged {
        name: ChannelName,
        earliest: RecordIndex,
        head: RecordIndex,
    },
}

/// Channel geometry/retention a client requests when creating a channel. Unlike the
/// in-process `WriterBuilder` closure, this is serializable so it can cross the
/// client↔daemon link; the daemon applies it (and owns placement + genesis base).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ChannelOptions {
    pub region_size: u32,
    /// Max payload bytes; 0 = unlimited.
    pub mtu: u32,
    /// Bytes per segment before rolling; 0 = no rolling.
    pub file_roll_size: u64,
    /// Rolled files to retain; 0 = unlimited.
    pub keep_files: u32,
}

impl Default for ChannelOptions {
    fn default() -> Self {
        Self {
            region_size: 1 << 20, // 1 MiB
            mtu: 0,
            file_roll_size: 0,
            keep_files: 0,
        }
    }
}

/// Options for creating a topic (multi-producer fan-in, `doc/TOPICS.md` §3.1). Wraps the
/// topic channel's geometry plus mux policy; serializable so it crosses the client↔daemon link.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TopicOptions {
    /// Geometry/retention of the topic channel itself (the merged output).
    pub channel: ChannelOptions,
    /// Fairness bound: records merged per member per poll cycle (§4.3). `0` ⇒ daemon default.
    pub max_batch_per_member: u32,
    /// Auto-reap a member whose owner has been an unreachable member for at least this long
    /// (§6.1). `0` ⇒ **never** reap (the default; reaping is an operator opt-in).
    pub member_reap_after_ms: u64,
}

impl Default for TopicOptions {
    fn default() -> Self {
        Self {
            channel: ChannelOptions::default(),
            max_batch_per_member: 0,
            member_reap_after_ms: 0,
        }
    }
}

/// One channel as reported by discovery. Flattened rather than wrapping a `ChannelIdentity`,
/// which already contains `name` and `owner` and would carry them twice.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChannelInfo {
    pub name: ChannelName,
    pub owner: NodeId,
    /// Incarnation of this **name**. A change means the name was reclaimed and this is a
    /// different log: a consumer holding per-channel state must reset it, not extend it.
    pub epoch: u64,
    /// Whether `owner` is currently a live member. Discovery must report "known, owner
    /// unreachable" distinctly from "known and live" (DESIGN §5) — otherwise a consumer
    /// cannot tell a frozen channel from a healthy quiet one.
    pub owner_live: bool,
    /// `Some(topic)` if this is a **topic member**. Members are ordinary registered channels,
    /// so a listing of `fills.prod.` returns the topic *and* every producer feeding it;
    /// consumers wanting sources rather than plumbing filter on this.
    pub member_of: Option<ChannelName>,
    pub region_size: u32,
    pub mtu: u32,
    /// Earliest index still retained at the source — how much history a subscriber can expect.
    pub earliest_index: RecordIndex,
}

/// A record in the **discovery log**: how one name changed.
///
/// Only two shapes, because a last-writer-wins map cannot honestly report more. There is no
/// `Added` vs `Replaced` distinction: the winner for a name can change with no user action at
/// all, when a later-arriving but earlier-registered identity wins the collision. A consumer
/// applies each record to its own map and compares `epoch` to decide whether a name it
/// already knows is the same log or a new incarnation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChannelChange {
    /// The name gained an entry, or its entry changed.
    Upserted(ChannelInfo),
    /// The name was tombstoned.
    Removed { name: ChannelName, epoch: u64 },
}

/// Where a client should start reading the discovery log, handed out with a listing snapshot.
///
/// The snapshot and `from` are taken under one registry lock, so there is no window between
/// "what exists" and "what changed next" — the race that a separate list-then-watch pair has
/// to close with revisions.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DiscoveryCursor {
    /// Local path of the discovery log; the client opens it with plain xchannel.
    pub log_path: String,
    /// Incarnation of the log. A restarted daemon starts a fresh one, so a client resuming
    /// with a stale cursor sees this change and knows to re-list rather than resume into an
    /// unrelated log.
    pub generation: u64,
    /// Absolute index of the first record the client has not already accounted for in the
    /// snapshot.
    pub from: RecordIndex,
}

/// Health of one channel this node reads, as reported to a local client.
///
/// The design insists that "no new records" and "replication is broken" must never look alike
/// (DESIGN: writer liveness vs membership liveness), so this carries both halves: how far along
/// we are, and whether the machinery behind that number is working.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SubscriptionStatus {
    /// The channel is hosted by **this** node, so it is read from the origin directly and
    /// there is no subscription: nothing can lag, and `owner_live` is trivially true.
    pub local: bool,
    /// The background replication loop is running (`false` once stopped, or when `local`).
    pub active: bool,
    /// Absolute index the local copy has reached.
    pub synced: RecordIndex,
    /// The source's head **as of the last successful (re)connect** — from `SubscribeAck`, which
    /// is a snapshot, not a live value. `head_at_connect - synced` measures catch-up progress
    /// after connecting; it says nothing about how far the source has run on since. Use
    /// [`last_record_at_ms`](Self::last_record_at_ms) for live staleness.
    pub head_at_connect: RecordIndex,
    /// Node hosting the authoritative writer.
    pub owner: NodeId,
    /// Whether that owner is currently a **live member** (recent heartbeat). This is
    /// *membership* liveness — it says the owner's manager is reachable, not that its
    /// application is still writing.
    pub owner_live: bool,
    /// Incarnation of the channel being read. A change means the name was reclaimed and this
    /// is a different log; a consumer holding per-channel state must reset it.
    pub generation: u64,
    /// Unix-millis when a record was last applied; `0` if none has been since the loop started.
    /// This is the live signal: a source that is merely quiet still has a recent `synced`, while
    /// a broken one goes stale here while `owner_live` may still read true.
    pub last_record_at_ms: u64,
    /// Replica rebuilds caused by falling behind the source's retention.
    pub rebuilds_gap: u64,
    /// Replica rebuilds caused by the name being reclaimed (a different incarnation).
    pub rebuilds_diverged: u64,
    /// Unix-millis of the most recent rebuild; `0` if there has never been one.
    pub last_rebuild_at_ms: u64,
}

/// Client → local daemon request (the client↔manager control protocol). A client never
/// talks to remote nodes; it asks its local daemon, which handles registration,
/// discovery, and replication, and replies with a local path the client opens itself.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ClientRequest {
    /// Create + register an origin channel this node owns. The daemon precreates the file
    /// under its `data_dir` and replies [`Created`](ClientReply::Created) with the path;
    /// the client opens the single `Writer`.
    Create {
        name: ChannelName,
        options: ChannelOptions,
    },
    /// Subscribe to a channel. The daemon ensures a local replica is being synced and
    /// replies [`Subscribed`](ClientReply::Subscribed) with the replica path; the client
    /// opens a `Reader`. `wait_ms` is the resolve timeout (0 = block until available).
    Subscribe { name: ChannelName, wait_ms: u64 },
    /// Create a topic (multi-producer fan-in) this node owns: an ordinary channel plus a mux
    /// that merges its members into it (`doc/TOPICS.md`). Replies
    /// [`Created`](ClientReply::Created) with the topic channel path (a consumer subscribes to
    /// it like any channel).
    CreateTopic {
        name: ChannelName,
        options: TopicOptions,
    },
    /// Create a member channel and attach it to `topic`'s mux; replies
    /// [`Created`](ClientReply::Created) with the member channel path, whose single `Writer`
    /// the producer opens. (Phase 1: the topic must be hosted on the local node.)
    PublishToTopic {
        topic: ChannelName,
        member: ChannelName,
        options: ChannelOptions,
    },
    /// Withdraw a channel this node owns: tombstone it in the registry, disseminate that, and
    /// delete its files. Only the owner may do this — a request for a name owned elsewhere
    /// reports `existed: false` rather than removing anything.
    Deregister { name: ChannelName },
    /// List channels whose name starts with `prefix` (empty matches all), and say where to
    /// pick up the changes that follow. Replies [`Channels`](ClientReply::Channels).
    ListChannels { prefix: String },
    /// Retire a channel owned by a node that is **gone**, so its name can be reclaimed here.
    /// Refused unless the owner has been unreachable past the daemon's `reclaim_after` floor.
    /// This is the deliberate exception to owner-only deregistration; see
    /// [`Node::force_deregister`](../../xchannel_net/node/struct.Node.html#method.force_deregister)
    /// for why it is operator-invoked rather than automatic.
    ForceDeregister { name: ChannelName },
    /// Ask how a channel this node reads is doing — replication progress and whether the
    /// machinery behind it is healthy. Replies [`Status`](ClientReply::Status), or
    /// [`Error`](ClientReply::Error) if this node neither hosts nor subscribes to the name.
    SubscriptionStatus { name: ChannelName },
}

/// Local daemon → client reply.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ClientReply {
    /// Channel created; open a `Writer` at this local path (with the requested options).
    Created { path: String },
    /// Replica is being synced; open a `Reader` at this local path.
    Subscribed { replica_path: String },
    /// A discovery listing plus the cursor to continue from.
    Channels {
        channels: Vec<ChannelInfo>,
        cursor: DiscoveryCursor,
    },
    /// The channel was withdrawn; `existed` is false if this node did not own a live channel
    /// by that name (already deregistered, never registered, or owned by another node).
    Deregistered { existed: bool },
    /// Health of a channel this node reads (reply to
    /// [`SubscriptionStatus`](ClientRequest::SubscriptionStatus)).
    Status(SubscriptionStatus),
    /// The request failed (name taken by another owner, resolve timeout, IO error, …).
    Error { message: String },
}
