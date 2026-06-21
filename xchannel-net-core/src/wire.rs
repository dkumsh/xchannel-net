//! Wire frames for the two protocols.
//!
//! These are the on-the-wire *shapes*; the concrete encoding (length-prefix, varint,
//! etc.) is deferred until we pick a serialization in a later step. Keeping them as
//! plain Rust types first lets us reason about the protocol before committing bytes.

use crate::identity::{ChannelIdentity, ChannelName};
use crate::{NodeId, RecordIndex, StreamId};

/// One self-describing log record as it travels on the data plane.
///
/// This mirrors an xchannel `User` record exactly — `Roll`/`Skip` markers are local
/// artifacts of the source's file geometry and never cross the network. The receiving
/// side re-frames into its own replica `Writer`, making its own rolling decisions.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RecordFrame {
    /// Logical position in the stream (counts only `User` records).
    pub index: RecordIndex,
    /// Application discriminant (xchannel `message_type`).
    pub msg_type: u16,
    /// Opaque per-message metadata (xchannel `user_meta_u64`).
    pub user_meta: u64,
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
    Heartbeat { node: NodeId },
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
    SubscribeAck {
        name: ChannelName,
        stream_id: StreamId,
        start: RecordIndex,
        head: RecordIndex,
        region_size: u32,
        mtu: u32,
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
}
