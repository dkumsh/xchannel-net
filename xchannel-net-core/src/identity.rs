//! Channel identity — what the registry propagates and what subscribers match on.

use crate::{NodeId, RecordIndex};

/// Flat, globally-unique channel name. First-registrant-wins; duplicates are rejected.
pub type ChannelName = String;

/// The metadata record describing one channel, propagated verbatim by the registry.
///
/// Every node converges on a map `ChannelName -> ChannelIdentity` (a last-writer-wins
/// CRDT; see `resolve_collision`). A node
/// only ever *writes* the channel it owns; everyone else holds a read-only replica
/// built to be record-compatible using the geometry advertised here.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChannelIdentity {
    pub name: ChannelName,
    /// Node hosting the single authoritative writer for this channel.
    pub owner: NodeId,

    // --- xchannel geometry, so replicas are built compatibly ---
    /// Region size of the source channel (bytes).
    pub region_size: u32,
    /// MTU of the source channel (0 = unlimited).
    pub mtu: u32,

    // --- replication bounds ---
    /// Earliest record index still retained at the source. Because we always pull
    /// full history, this tells a subscriber whether it received true genesis (0) or
    /// a retention-truncated start.
    ///
    /// **Currently always `0`, and it could not be kept current if it were not.** The merge below
    /// is a total order on the *key* — `(epoch, deleted, registered_at_nanos, NodeId)` — none of
    /// which changes when an owner's retention moves the floor, so a re-registration carrying a new
    /// value ties with the entry already held and is discarded. Nothing depends on it today: the
    /// authoritative retention floor reaches a subscriber in `SubscribeAck.start`, computed live
    /// from the source's own log at subscribe time, and that is what a `Gap` is reported against.
    /// Making this field mean anything would need the merge to consider it — which is a change to
    /// the CRDT, not to the registration path.
    pub earliest_index: RecordIndex,

    // --- registration tiebreak (deterministic first-registrant-wins) ---
    /// Wall-clock registration time at the owner, used as the primary tiebreak key.
    pub registered_at_nanos: u64,

    // --- tombstone / reclaim generation (see `resolve_collision`) ---
    /// Reclaim generation. A first registration of a never-used name is `0`; reclaiming a
    /// *tombstoned* name uses `prev.epoch + 1`. Higher epoch always wins the merge, so a
    /// fresh owner can take over a deregistered name while a stale in-flight registration
    /// of the old generation cannot resurrect it.
    pub epoch: u64,
    /// Tombstone flag. A `deleted` entry is retained in the registry map (so it keeps
    /// beating stale re-registrations of its generation) but hidden from
    /// [`Registry::get`](crate) — a deregistered name reads as absent.
    pub deleted: bool,

    /// If this channel is a **topic member**, the topic it feeds (`doc/TOPICS.md` §3.1). The
    /// topic's owner discovers members through this field (gossiped like any identity) and
    /// attaches them to its mux — local members by their origin file, remote members via a
    /// stream subscription. `None` for an ordinary channel.
    pub member_of: Option<ChannelName>,
}

impl ChannelIdentity {
    /// Deterministic total order resolving which entry occupies a name, computed identically
    /// on every node with no coordination. Winner = the maximum under this lexicographic key:
    ///
    /// 1. **higher `epoch` wins** — a reclaim (new generation) supersedes any older
    ///    generation, tombstone or not;
    /// 2. within an epoch, **a tombstone (`deleted`) beats a live registration** — delete is
    ///    terminal for its generation, so a stale `Register` of that generation can't revive it;
    /// 3. within an epoch, between two same-liveness entries, **earliest `registered_at_nanos`
    ///    wins, then lowest `NodeId`** — the original first-registrant-wins tiebreak.
    ///
    /// Being a total order makes the merge commutative, associative, and idempotent, so the
    /// registry converges regardless of delta order or duplication.
    pub fn resolve_collision<'a>(
        a: &'a ChannelIdentity,
        b: &'a ChannelIdentity,
    ) -> &'a ChannelIdentity {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match a.epoch.cmp(&b.epoch) {
            Greater => return a,
            Less => return b,
            Equal => {}
        }
        // Same generation: a tombstone dominates a live entry.
        match (a.deleted, b.deleted) {
            (true, false) => return a,
            (false, true) => return b,
            _ => {}
        }
        // Same generation and liveness: earliest registration, then lowest NodeId.
        match a.registered_at_nanos.cmp(&b.registered_at_nanos) {
            Less => a,
            Greater => b,
            Equal if a.owner <= b.owner => a,
            Equal => b,
        }
    }
}
