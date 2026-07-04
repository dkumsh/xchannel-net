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

    /// Merge an incoming identity (local registration, peer delta/sync, or a tombstone).
    /// Returns the winner now occupying the name — equal to `incoming` iff it won. Tombstones
    /// are retained in the map (they must keep beating stale re-registrations and propagate via
    /// anti-entropy); [`get`](Self::get) hides them so a deregistered name reads as absent.
    pub fn merge(&mut self, incoming: ChannelIdentity) -> ChannelIdentity {
        let winner = match self.channels.get(&incoming.name) {
            Some(existing) => ChannelIdentity::resolve_collision(existing, &incoming).clone(),
            None => incoming.clone(),
        };
        self.channels.insert(winner.name.clone(), winner.clone());
        winner
    }

    /// The live identity for `name`, or `None` if unknown **or tombstoned**.
    pub fn get(&self, name: &str) -> Option<&ChannelIdentity> {
        self.channels.get(name).filter(|id| !id.deleted)
    }

    /// The raw entry for `name` including a tombstone — used to compute the reclaim epoch and
    /// to deregister. Callers serving/resolving channels want [`get`](Self::get) instead.
    pub fn get_raw(&self, name: &str) -> Option<&ChannelIdentity> {
        self.channels.get(name)
    }

    /// The epoch a fresh local registration of `name` should use: `0` for a never-used name,
    /// the current generation for a live name (so it competes — and loses — under
    /// first-registrant-wins rather than stealing it), and `prev.epoch + 1` to reclaim a
    /// tombstoned name into a new generation.
    pub fn claim_epoch(&self, name: &str) -> u64 {
        match self.channels.get(name) {
            Some(id) if id.deleted => id.epoch + 1,
            Some(id) => id.epoch,
            None => 0,
        }
    }

    /// Tombstone `name` if it is currently live and owned by `owner`. Returns the tombstone
    /// identity to disseminate, or `None` if the name is unknown, already tombstoned, or owned
    /// by someone else (only the owner may deregister). The tombstone keeps the same
    /// generation, so it dominates the live registration it replaces.
    pub fn deregister(
        &mut self,
        name: &str,
        owner: xchannel_net_core::NodeId,
    ) -> Option<ChannelIdentity> {
        let current = self.channels.get(name)?;
        if current.deleted || current.owner != owner {
            return None;
        }
        let mut tombstone = current.clone();
        tombstone.deleted = true;
        Some(self.merge(tombstone))
    }

    /// All raw entries including tombstones — for anti-entropy, which must carry deletions so
    /// a reconnecting peer learns them (else it could resurrect a deregistered name).
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
            epoch: 0,
            deleted: false,
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

    #[test]
    fn deregister_tombstones_and_hides_the_name() {
        let mut r = Registry::new();
        r.merge(ident("md.aapl", 1, 100));
        assert!(r.get("md.aapl").is_some());

        // Only the owner may deregister.
        assert!(r.deregister("md.aapl", NodeId(2)).is_none());
        let tomb = r
            .deregister("md.aapl", NodeId(1))
            .expect("owner deregisters");
        assert!(tomb.deleted);
        // Hidden from get(), but retained (raw) so it keeps dominating and propagates.
        assert!(r.get("md.aapl").is_none());
        assert!(r.get_raw("md.aapl").unwrap().deleted);
    }

    #[test]
    fn stale_register_cannot_resurrect_a_tombstone() {
        let mut r = Registry::new();
        r.merge(ident("md.aapl", 1, 100));
        r.deregister("md.aapl", NodeId(1)).unwrap();
        // A late/reordered Register of the same generation (epoch 0) must not revive it.
        r.merge(ident("md.aapl", 1, 100));
        assert!(
            r.get("md.aapl").is_none(),
            "tombstone dominates its generation"
        );
    }

    #[test]
    fn a_new_generation_reclaims_a_tombstoned_name() {
        let mut r = Registry::new();
        r.merge(ident("md.aapl", 1, 100));
        r.deregister("md.aapl", NodeId(1)).unwrap();

        // Reclaim: fresh owner registers with the next epoch (as claim_epoch dictates).
        let epoch = r.claim_epoch("md.aapl");
        assert_eq!(epoch, 1, "reclaim uses prev.epoch + 1");
        let mut reclaim = ident("md.aapl", 2, 200);
        reclaim.epoch = epoch;
        let w = r.merge(reclaim);
        assert_eq!(w.owner, NodeId(2));
        assert!(!w.deleted);
        assert_eq!(r.get("md.aapl").unwrap().owner, NodeId(2));
    }

    /// The merge is a CRDT: every ordering of the same event set converges to the same map.
    /// Exhaustively permute register/tombstone/reclaim events and assert one final winner.
    #[test]
    fn merge_converges_under_every_ordering() {
        // Events for one name across two generations: two racing gen-0 registers, a gen-0
        // tombstone, and a gen-1 reclaim. Whatever order/duplication they arrive in, the map
        // must converge to the reclaim (highest epoch, live).
        let reg_a = ident("c", 1, 100); // gen 0, owner 1, earlier
        let reg_b = ident("c", 2, 150); // gen 0, owner 2, later
        let mut tomb = reg_a.clone();
        tomb.deleted = true; // gen-0 tombstone
        let mut reclaim = ident("c", 3, 300);
        reclaim.epoch = 1; // gen-1 reclaim, owner 3

        let events = [reg_a, reg_b, tomb, reclaim.clone()];
        let n = events.len();

        // Reference result: merge in natural order.
        let mut reference = Registry::new();
        for e in &events {
            reference.merge(e.clone());
        }
        let expect = reference.get_raw("c").cloned().unwrap();
        assert_eq!(expect, reclaim, "reclaim (highest epoch, live) must win");

        // Every permutation, with a duplicate appended, converges to the same entry.
        for perm in permutations(&(0..n).collect::<Vec<_>>()) {
            let mut r = Registry::new();
            for &i in &perm {
                r.merge(events[i].clone());
            }
            r.merge(events[perm[0]].clone()); // idempotence: a duplicate changes nothing
            assert_eq!(
                r.get_raw("c").cloned().unwrap(),
                expect,
                "diverged on ordering {perm:?}"
            );
        }
    }

    /// All permutations of `items` (n! — fine for the tiny event sets here).
    fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut out = Vec::new();
        for i in 0..items.len() {
            let mut rest = items.to_vec();
            let head = rest.remove(i);
            for mut p in permutations(&rest) {
                p.insert(0, head);
                out.push(p);
            }
        }
        out
    }
}
