//! The dissemination boundary.
//!
//! Convergence (the CRDT merge in the registry) and *dissemination* (how registry
//! deltas physically reach peers) are deliberately separate concerns — see DESIGN.md
//! §2.1. This trait is that seam. Because the registry merge is a last-writer-wins
//! CRDT (commutative, associative, idempotent), any implementation here is correct
//! regardless of delivery order or duplication, so they are interchangeable:
//!
//! * **v1** — `BroadcastDissemination` (in the `xchannel-net` daemon): eager push to all
//!   peers + join-time anti-entropy + heartbeat liveness. ~right for ≤100 LAN nodes.
//! * **future, at larger scale** — a SWIM-backed impl. The surgical fit is the
//!   [`foca`](https://crates.io/crates/foca) crate (v1.0.0): runtime- *and*
//!   transport-agnostic, `no_std + alloc`, no forced tokio — you drive its event loop
//!   and supply the transport, so it slots behind this trait without dragging an async
//!   runtime into the project. (Contrast [`chitchat`](https://crates.io/crates/chitchat)
//!   v0.11.0 — SWIM + Scuttlebutt KV, but hard-depends on tokio.)
//!
//! Swapping v1 for either is a change *behind this trait only*; the registry, the wire
//! identity type, and the merge are untouched.

use crate::NodeId;
use crate::identity::ChannelIdentity;
use std::io;

/// Opaque handle for the peer link a message arrived on, so a relay can skip its source.
/// [`NO_PEER`] denotes "not from a peer" (a locally originated change).
pub type PeerId = u64;

/// A [`PeerId`] that matches no link.
pub const NO_PEER: PeerId = 0;

/// How a node manager spreads registry state across the mesh and observes membership.
///
/// The node manager drives it: call [`announce`](Dissemination::announce) when the local
/// registry changes, and call [`pump`](Dissemination::pump) regularly, merging whatever
/// it returns back into the local [`Registry`](crate::identity::ChannelIdentity)'s map.
pub trait Dissemination: Send {
    /// Push a local registry change out to the cluster (eager broadcast in v1).
    ///
    /// Delivery is best-effort and may duplicate or reorder; the downstream CRDT merge
    /// makes that harmless. Anti-entropy on (re)connect closes any gaps from drops.
    fn announce(&mut self, delta: &[ChannelIdentity]) -> io::Result<()>;

    /// Forward a change that arrived from `from` to every *other* peer.
    ///
    /// This is what makes a partial mesh converge: without it a delta reaches only the
    /// originator's direct peers, and a node two hops away learns nothing until it happens to
    /// open a fresh link and receive a full anti-entropy sync. The caller relays only when its
    /// merge actually **changed** the map, which is what terminates the flood — the registry
    /// merge is a total order and idempotent, so any given winning state can change a given
    /// node's map at most once, however many cycles the topology has.
    ///
    /// Skipping the source is an optimisation, not a correctness requirement (the echo would be
    /// absorbed by the same suppression); on a full mesh, where the relay is redundant anyway, it
    /// is what keeps the redundancy to one round instead of one round per peer.
    fn relay(&mut self, from: PeerId, delta: &[ChannelIdentity]) -> io::Result<()>;

    /// Send a change back to **only** the peer it came from, because that peer is behind.
    ///
    /// [`relay`](Dissemination::relay) covers the case where the arriving state wins; this covers
    /// the case where it *loses*. Without it, convergence was one-directional: a peer holding a
    /// stale entry sent it, every recipient's merge left its own map unchanged, nothing was sent
    /// back, and the sender kept its stale entry indefinitely — anti-entropy only runs when a link
    /// is established, so a link that stays up never corrects it. Two nodes could then disagree
    /// about who owns a channel for as long as they stayed connected.
    ///
    /// Terminates for the same reason `relay` does, with one extra step: a reply is only sent when
    /// the arriving state differs from the winner, so the reply itself — which *is* the winner —
    /// cannot provoke another.
    fn reply(&mut self, to: PeerId, delta: &[ChannelIdentity]) -> io::Result<()>;

    /// Drive inbound traffic and housekeeping (anti-entropy exchange, heartbeats /
    /// failure detection) and return any channel identities received from peers for the
    /// caller to merge into its registry. Does the work that is currently available and
    /// returns; the caller decides the cadence.
    fn pump(&mut self) -> io::Result<Vec<(PeerId, ChannelIdentity)>>;

    /// Nodes currently considered reachable — *membership* liveness, distinct from
    /// *writer* liveness (a channel can be frozen-but-healthy; see DESIGN.md §2).
    fn live_members(&self) -> Vec<NodeId>;
}
