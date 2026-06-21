//! Membership map — `NodeId → stream address`, with heartbeat-based liveness.
//!
//! This is the *separate membership map* (DESIGN §9): a [`ChannelIdentity`] stays
//! address-free (it names only `owner: NodeId`), and a subscriber resolves that owner to a
//! concrete stream address here. Entries are (re)stamped by inbound `Heartbeat`s, so a
//! node is "live" iff it was heard from within a timeout.
//!
//! [`ChannelIdentity`]: crate::identity::ChannelIdentity

use crate::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

struct Member {
    addr: SocketAddr,
    last_seen: Instant,
}

/// Known peers and where to reach them, refreshed by heartbeats.
#[derive(Default)]
pub struct Membership {
    members: HashMap<NodeId, Member>,
}

impl Membership {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record (or refresh) a peer's address and mark it seen now. A node may change
    /// address across restarts; the latest heartbeat wins.
    pub fn record(&mut self, node: NodeId, addr: SocketAddr) {
        self.members.insert(
            node,
            Member {
                addr,
                last_seen: Instant::now(),
            },
        );
    }

    /// The last-known address of `node`, regardless of liveness (callers that care about
    /// liveness filter via [`live_members`](Self::live_members)).
    pub fn addr_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.members.get(&node).map(|m| m.addr)
    }

    /// Nodes heard from within `timeout`.
    pub fn live_members(&self, timeout: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        self.members
            .iter()
            .filter(|(_, m)| now.duration_since(m.last_seen) <= timeout)
            .map(|(&n, _)| n)
            .collect()
    }

    /// Drop entries not heard from within `timeout`. Returns the pruned nodes.
    pub fn forget_stale(&mut self, timeout: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        let stale: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(_, m)| now.duration_since(m.last_seen) > timeout)
            .map(|(&n, _)| n)
            .collect();
        for n in &stale {
            self.members.remove(n);
        }
        stale
    }

    /// Number of known peers (live or not).
    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], p))
    }

    #[test]
    fn records_and_resolves_addresses() {
        let mut m = Membership::new();
        m.record(NodeId(1), addr(7001));
        m.record(NodeId(2), addr(7002));
        assert_eq!(m.addr_of(NodeId(1)), Some(addr(7001)));
        assert_eq!(m.addr_of(NodeId(2)), Some(addr(7002)));
        assert_eq!(m.addr_of(NodeId(3)), None);
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn latest_heartbeat_wins_on_address_change() {
        let mut m = Membership::new();
        m.record(NodeId(1), addr(7001));
        m.record(NodeId(1), addr(8001)); // restarted on a new port
        assert_eq!(m.addr_of(NodeId(1)), Some(addr(8001)));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn liveness_expires_after_timeout() {
        let mut m = Membership::new();
        m.record(NodeId(1), addr(7001));
        assert_eq!(m.live_members(Duration::from_secs(60)), vec![NodeId(1)]);

        std::thread::sleep(Duration::from_millis(20));
        assert!(m.live_members(Duration::from_millis(5)).is_empty());

        // addr_of still resolves until explicitly pruned.
        assert_eq!(m.addr_of(NodeId(1)), Some(addr(7001)));
        assert_eq!(m.forget_stale(Duration::from_millis(5)), vec![NodeId(1)]);
        assert!(m.is_empty());
    }
}
