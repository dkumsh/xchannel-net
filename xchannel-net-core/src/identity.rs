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
    pub earliest_index: RecordIndex,

    // --- registration tiebreak (deterministic first-registrant-wins) ---
    /// Wall-clock registration time at the owner, used as the primary tiebreak key.
    pub registered_at_nanos: u64,
}

impl ChannelIdentity {
    /// Deterministic total order used to resolve a name collision between two
    /// concurrent registrations. Every node computes the same winner without
    /// coordination: earliest registration wins; `NodeId` breaks exact ties.
    ///
    /// Returns the registration that *wins* the name.
    pub fn resolve_collision<'a>(
        a: &'a ChannelIdentity,
        b: &'a ChannelIdentity,
    ) -> &'a ChannelIdentity {
        match a.registered_at_nanos.cmp(&b.registered_at_nanos) {
            std::cmp::Ordering::Less => a,
            std::cmp::Ordering::Greater => b,
            std::cmp::Ordering::Equal if a.owner <= b.owner => a,
            std::cmp::Ordering::Equal => b,
        }
    }
}
