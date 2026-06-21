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

    /// Drive inbound traffic and housekeeping (anti-entropy exchange, heartbeats /
    /// failure detection) and return any channel identities received from peers for the
    /// caller to merge into its registry. Does the work that is currently available and
    /// returns; the caller decides the cadence.
    fn pump(&mut self) -> io::Result<Vec<ChannelIdentity>>;

    /// Nodes currently considered reachable — *membership* liveness, distinct from
    /// *writer* liveness (a channel can be frozen-but-healthy; see DESIGN.md §2).
    fn live_members(&self) -> Vec<NodeId>;
}
