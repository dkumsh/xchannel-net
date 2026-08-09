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
    /// Stream-plane address — where to subscribe to a channel this node owns.
    addr: SocketAddr,
    /// Control-plane address — where to open a peer link, so a node learned second-hand can
    /// be dialled and become a direct peer.
    control_addr: SocketAddr,
    /// Human-readable label, for messages an operator reads. Cosmetic only — never a key, never a
    /// tie-break, so a duplicate is confusing rather than incorrect.
    name: String,
    /// When this node was last heard from **directly**. `None` for a node known only by
    /// hearsay: a peer told us it exists, but we have never had a link to it.
    ///
    /// The distinction is the whole reason relayed knowledge is a separate thing from a
    /// heartbeat. `live_members` has to keep meaning "nodes *this* node can reach", because
    /// `resolve` decides `HostUnreachable` on it, `force_deregister` guards a name reclaim on
    /// it, the topic member reaper tombstones on it, and discovery reports `owner_live` from
    /// it. Liveness by hearsay would let a node on the far side of a partition look reachable
    /// because a third party said so — exactly the case `force_deregister` exists to refuse.
    last_seen: Option<Instant>,
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

    /// Record a peer heard from **directly**: refresh its addresses and its liveness. A node
    /// may change address across restarts; the latest heartbeat wins.
    ///
    /// Returns whether this taught us something new — an unknown node, or one that moved.
    /// The caller relays on `true` and stays quiet on `false`, which is what stops a steady
    /// stream of heartbeats from becoming a steady stream of relays.
    pub fn record(
        &mut self,
        node: NodeId,
        addr: SocketAddr,
        control_addr: SocketAddr,
        name: &str,
    ) -> bool {
        let novel = self
            .members
            .get(&node)
            .is_none_or(|m| m.addr != addr || m.control_addr != control_addr);
        self.members.insert(
            node,
            Member {
                addr,
                control_addr,
                name: name.to_string(),
                last_seen: Some(Instant::now()),
            },
        );
        novel
    }

    /// Record a peer we were *told about* by another peer: learn where it is, but do **not**
    /// confer liveness — we have not heard from it ourselves. Returns whether this was new.
    ///
    /// Never downgrades an existing entry: if we already have a direct link to this node, its
    /// `last_seen` is preserved, so hearsay can refresh an address without ever making a node
    /// look less reachable than it is.
    pub fn learn(
        &mut self,
        node: NodeId,
        addr: SocketAddr,
        control_addr: SocketAddr,
        name: &str,
    ) -> bool {
        match self.members.get_mut(&node) {
            Some(m) => {
                let novel = m.addr != addr || m.control_addr != control_addr;
                m.addr = addr;
                m.control_addr = control_addr;
                m.name = name.to_string();
                novel
            }
            None => {
                self.members.insert(
                    node,
                    Member {
                        addr,
                        control_addr,
                        name: name.to_string(),
                        last_seen: None,
                    },
                );
                true
            }
        }
    }

    /// Everything we know about every node: `(node, stream addr, control addr)`. Sent once to a
    /// newly adopted peer so a joiner learns the whole mesh from a single seed link.
    pub fn directory(&self) -> Vec<(NodeId, SocketAddr, SocketAddr, String)> {
        self.members
            .iter()
            .map(|(&n, m)| (n, m.addr, m.control_addr, m.name.clone()))
            .collect()
    }

    /// The human-readable label for `node`, if known and non-empty. Used only in messages.
    pub fn name_of(&self, node: NodeId) -> Option<&str> {
        self.members
            .get(&node)
            .map(|m| m.name.as_str())
            .filter(|n| !n.is_empty())
    }

    /// Which node is reachable at `control` — the reverse of [`known_peers`](Self::known_peers),
    /// so a dial candidate can be checked against the links we already hold.
    pub fn node_at(&self, control: SocketAddr) -> Option<NodeId> {
        self.members
            .iter()
            .find(|(_, m)| m.control_addr == control)
            .map(|(&n, _)| n)
    }

    /// Every node we know of and the control address to reach it on — the candidate set for
    /// forming links, whether we heard from it directly or were told about it.
    pub fn known_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        self.members
            .iter()
            .map(|(&n, m)| (n, m.control_addr))
            .collect()
    }

    /// The last-known address of `node`, regardless of liveness (callers that care about
    /// liveness filter via [`live_members`](Self::live_members)).
    pub fn addr_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.members.get(&node).map(|m| m.addr)
    }

    /// The address of `node`, but only if it was heard from within `timeout` — i.e. a live
    /// member. `None` if the node is unknown *or* known-but-stale; the caller treats the
    /// stale case as "owner unreachable", distinct from "channel unknown".
    pub fn live_addr_of(&self, node: NodeId, timeout: Duration) -> Option<SocketAddr> {
        self.members
            .get(&node)
            .filter(|m| m.last_seen.is_some_and(|t| t.elapsed() <= timeout))
            .map(|m| m.addr)
    }

    /// How long since `node` was last heard from, or `None` if it has never been heard from
    /// at all. Callers deciding whether an owner is *gone* (rather than momentarily silent)
    /// need the duration, not just the live/not-live verdict.
    pub fn silent_for(&self, node: NodeId) -> Option<Duration> {
        self.members
            .get(&node)
            .and_then(|m| m.last_seen)
            .map(|t| t.elapsed())
    }

    /// Nodes heard from within `timeout`.
    pub fn live_members(&self, timeout: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        self.members
            .iter()
            .filter(|(_, m)| {
                m.last_seen
                    .is_some_and(|t| now.duration_since(t) <= timeout)
            })
            .map(|(&n, _)| n)
            .collect()
    }

    /// Drop entries not heard from within `timeout`. Returns the pruned nodes.
    pub fn forget_stale(&mut self, timeout: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        let stale: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(_, m)| m.last_seen.is_none_or(|t| now.duration_since(t) > timeout))
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
        m.record(NodeId(1), addr(7001), addr(7001 + 1000), "n");
        m.record(NodeId(2), addr(7002), addr(7002 + 1000), "n");
        assert_eq!(m.addr_of(NodeId(1)), Some(addr(7001)));
        assert_eq!(m.addr_of(NodeId(2)), Some(addr(7002)));
        assert_eq!(m.addr_of(NodeId(3)), None);
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn latest_heartbeat_wins_on_address_change() {
        let mut m = Membership::new();
        m.record(NodeId(1), addr(7001), addr(7001 + 1000), "n");
        m.record(NodeId(1), addr(8001), addr(8001 + 1000), "n"); // restarted on a new port
        assert_eq!(m.addr_of(NodeId(1)), Some(addr(8001)));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn liveness_expires_after_timeout() {
        let mut m = Membership::new();
        m.record(NodeId(1), addr(7001), addr(7001 + 1000), "n");
        assert_eq!(m.live_members(Duration::from_secs(60)), vec![NodeId(1)]);

        std::thread::sleep(Duration::from_millis(20));
        assert!(m.live_members(Duration::from_millis(5)).is_empty());

        // addr_of still resolves until explicitly pruned.
        assert_eq!(m.addr_of(NodeId(1)), Some(addr(7001)));
        assert_eq!(m.forget_stale(Duration::from_millis(5)), vec![NodeId(1)]);
        assert!(m.is_empty());
    }

    #[test]
    fn live_addr_of_gates_on_liveness() {
        let mut m = Membership::new();
        m.record(NodeId(1), addr(7001), addr(7001 + 1000), "n");
        // Fresh: live_addr_of returns the address.
        assert_eq!(
            m.live_addr_of(NodeId(1), Duration::from_secs(60)),
            Some(addr(7001))
        );
        // Unknown node: None (distinct from "known but stale", but same return here).
        assert_eq!(m.live_addr_of(NodeId(2), Duration::from_secs(60)), None);

        std::thread::sleep(Duration::from_millis(20));
        // Stale beyond the timeout: None, even though addr_of still knows the address.
        assert_eq!(m.live_addr_of(NodeId(1), Duration::from_millis(5)), None);
        assert_eq!(m.addr_of(NodeId(1)), Some(addr(7001)));
    }
}
