//! `xchannel-net-core` — transport-agnostic core for replicating [`xchannel`] logs
//! across a network of node managers.
//!
//! This crate holds the pieces that have no opinion about *how* bytes move between
//! nodes (TCP, RDMA, local IPC): the channel identity model, the wire frames the
//! control and stream protocols exchange, the [`Transport`] abstraction, and the
//! replication source/sink engines that bridge an xchannel log to a byte stream.
//!
//! See `DESIGN.md` at the repo root for the architecture this implements.

pub mod codec;
pub mod dissemination;
pub mod identity;
pub mod transport;
pub mod wire;

pub mod replication;
pub mod stream;

/// Stable identifier for a node manager within the mesh.
///
/// Nodes are logical: `node ~= machine` is the mental model, but two managers may
/// run on one host. The id participates in the registration tiebreak for flat global
/// channel names, so it must be unique and stable for a node's lifetime.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeId(pub u64);

/// **Absolute, source-authoritative** logical position of a `User` record within a
/// channel's lifetime — the count of `User` records from genesis (record 0 = the first
/// ever published), regardless of retention or which file/segment holds it.
///
/// Two properties make this the right resume token (DESIGN.md §4, §5, §9):
///
/// * **Transport-independent.** Deliberately *not* an xchannel byte offset.
/// * **Genesis-relative, not replica-relative.** A replica whose genesis was
///   retention-truncated holds records `base..base+n` — *counting its records gives `n`,
///   not the absolute index.* Resume position is `base + n`.
///
/// This maps **directly onto xchannel's on-disk format** (v2+): every channel file's
/// `ChannelHeader` carries `base_record_index` (the file's first absolute index) and a
/// per-file `message_count` of user records, so the absolute index is intrinsic to the
/// data — no separate tracking or sidecar. The origin reads it via
/// `Writer::next_record_index()` (the head) and a replica via `Reader::base_record_index()`
/// plus the records it has applied. A replica is created with
/// `WriterBuilder::base_record_index(start)` so its own header self-describes absolute
/// (not replica-local) indices.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RecordIndex(pub u64);

/// Compact identifier the source assigns to one accepted subscription, so that many
/// channels can be multiplexed over a single stream connection without repeating the
/// (string) channel name on every record. Scoped to one source→subscriber connection.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamId(pub u32);
