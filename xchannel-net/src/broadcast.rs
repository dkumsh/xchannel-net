//! v1 dissemination: eager broadcast + join-time anti-entropy + heartbeat liveness.
//!
//! The concrete [`Dissemination`] for ≤100 LAN nodes (DESIGN.md §2.1). On a local
//! registry change it pushes a `RegistryDelta` to every known peer; on (re)connect it
//! exchanges a full `RegistrySync`; membership liveness is plain heartbeats + timeout.
//! No epidemic gossip, no SWIM — when scale demands it, a `foca`-backed impl replaces
//! this one behind the same trait (the registry merge is untouched).

use std::io;
use xchannel_net_core::NodeId;
use xchannel_net_core::dissemination::Dissemination;
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::transport::Transport;

/// One peer connection plus the liveness bookkeeping for it.
#[allow(dead_code)] // fields consumed once announce/pump are implemented
struct Peer<T: Transport> {
    node: NodeId,
    conn: T,
    // last_heartbeat: Instant — populated once we wire timers.
}

/// Eager-broadcast dissemination over a set of peer [`Transport`] connections.
pub struct BroadcastDissemination<T: Transport> {
    _peers: Vec<Peer<T>>,
}

impl<T: Transport> BroadcastDissemination<T> {
    pub fn new() -> Self {
        Self { _peers: Vec::new() }
    }
}

impl<T: Transport> Default for BroadcastDissemination<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Transport> Dissemination for BroadcastDissemination<T> {
    fn announce(&mut self, _delta: &[ChannelIdentity]) -> io::Result<()> {
        // Encode ControlMsg::RegistryDelta(delta) and send_frame to every peer.
        unimplemented!("eager broadcast of RegistryDelta to all peers")
    }

    fn pump(&mut self) -> io::Result<Vec<ChannelIdentity>> {
        // Read available frames from peers: RegistryDelta / RegistrySync -> collect
        // identities to merge; Heartbeat -> refresh liveness; new peer -> send sync.
        unimplemented!("drain peer frames; return identities to merge; refresh liveness")
    }

    fn live_members(&self) -> Vec<NodeId> {
        // Peers whose last heartbeat is within the timeout.
        unimplemented!("return peers within heartbeat timeout")
    }
}
