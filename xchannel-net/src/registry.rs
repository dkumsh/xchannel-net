//! The decentralized channel registry — a last-writer-wins map CRDT.
//!
//! Eventually consistent: registry deltas (eager broadcast) and full syncs (join-time
//! anti-entropy) both merge into the local map. Because [`ChannelIdentity::resolve_collision`]
//! is commutative, associative, and idempotent, every node converges on the same map and
//! agrees on each name's winner with no coordination round — independent of how deltas
//! travel. See DESIGN.md §2.1.

use std::collections::HashMap;
use xchannel_net_core::identity::{ChannelIdentity, ChannelName};

#[derive(Default)]
pub struct Registry {
    channels: HashMap<ChannelName, ChannelIdentity>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge an incoming identity (local registration or a peer delta/sync). Returns the winner
    /// now occupying the name — equal to `incoming` iff it won.
    pub fn merge(&mut self, incoming: ChannelIdentity) -> ChannelIdentity {
        let winner = match self.channels.get(&incoming.name) {
            Some(existing) => ChannelIdentity::resolve_collision(existing, &incoming).clone(),
            None => incoming.clone(),
        };
        self.channels.insert(winner.name.clone(), winner.clone());
        winner
    }

    pub fn get(&self, name: &str) -> Option<&ChannelIdentity> {
        self.channels.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ChannelIdentity> {
        self.channels.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xchannel_net_core::{NodeId, RecordIndex};

    fn ident(name: &str, owner: u64, at: u64) -> ChannelIdentity {
        ChannelIdentity {
            name: name.to_string(),
            owner: NodeId(owner),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: at,
        }
    }

    #[test]
    fn earlier_registration_wins_the_name() {
        let mut r = Registry::new();
        let first = r.merge(ident("md.aapl", 1, 100));
        assert_eq!(first.owner, NodeId(1));
        // A later registration of the same name does not steal it.
        let still = r.merge(ident("md.aapl", 2, 200));
        assert_eq!(still.owner, NodeId(1));
    }

    #[test]
    fn exact_tie_breaks_on_node_id() {
        let mut r = Registry::new();
        r.merge(ident("x", 5, 100));
        let w = r.merge(ident("x", 2, 100));
        assert_eq!(
            w.owner,
            NodeId(2),
            "lower NodeId wins an exact timestamp tie"
        );
    }
}
