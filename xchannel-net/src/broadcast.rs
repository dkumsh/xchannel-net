//! v1 dissemination: eager broadcast + join-time anti-entropy + heartbeat liveness.
//!
//! The concrete [`Dissemination`] for ≤100 LAN nodes (DESIGN §2.1). On a local registry
//! change it pushes a `RegistryDelta` to every peer; on connect it sends a full
//! `RegistrySync` (anti-entropy); membership liveness is plain heartbeats + timeout. No
//! epidemic gossip, no SWIM — when scale demands it a `foca`-backed impl replaces this
//! one behind the same trait, registry merge untouched.
//!
//! Concrete over TCP (the control plane needs `try_clone` to read and send concurrently).
//! Each peer gets a **reader thread** that decodes inbound control frames into a shared
//! inbound queue (registry deltas/syncs) and the shared [`Membership`] (heartbeats); the
//! send side stays here for `announce` / heartbeat emission. `pump` drains the queue.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use xchannel_net_core::NodeId;
use xchannel_net_core::codec::{decode_control, encode_control, identities_encoded_len};
use xchannel_net_core::dissemination::{Dissemination, NO_PEER, PeerId};
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::membership::Membership;
use xchannel_net_core::transport::{TcpTransport, Transport};
use xchannel_net_core::wire::ControlMsg;

use crate::util::MutexExt;

/// Inbound registry identities, tagged with the link they arrived on so a relay can skip it.
type Inbox = Arc<Mutex<VecDeque<(PeerId, ChannelIdentity)>>>;
/// Peer knowledge worth forwarding: `(source link, node, stream addr, control addr)`. Queued by
/// reader threads (which cannot broadcast — the send halves live on the struct) and drained by
/// [`BroadcastDissemination::relay_hints`].
type Hints = Arc<Mutex<VecDeque<(PeerId, NodeId, SocketAddr, SocketAddr, String)>>>;
type SharedMembership = Arc<Mutex<Membership>>;
/// Dial addresses of outbound peer links currently believed connected (for dedup +
/// reconnection). An outbound peer's reader removes its address here on disconnect.
type Connected = Arc<Mutex<HashSet<SocketAddr>>>;
/// Which node sits at the far end of each link, learned from its first heartbeat. Written by
/// reader threads; read when deduplicating links.
type LinkPeers = Arc<Mutex<HashMap<PeerId, (NodeId, SocketAddr)>>>;
/// Which node was found at a **dial address**, remembered after its first heartbeat.
///
/// A peer's advertised control address and the address we happened to dial it on need not be the
/// same — a seed list naming `127.0.0.1:7001` for a node that advertises `10.0.0.5:7001` is the
/// ordinary case — so membership alone cannot answer "is the node at this dial address already
/// linked?". Without an answer the dial was repeated every tick, deduplicated every tick, and the
/// pair churned at the maintenance cadence forever.
type DialIdentity = Arc<Mutex<HashMap<SocketAddr, NodeId>>>;
/// The control address this node advertises. Shared with the reader threads because it is the only
/// way to tell a *twin* (another machine claiming our `NodeId`) from a *self-link* (a seed list that
/// names this node), and it is not known until the control listener has bound.
type SelfControl = Arc<Mutex<SocketAddr>>;
/// Addresses at which *our own* `NodeId` has been reported by a third party.
///
/// Kept apart from membership on purpose: this is a **dial candidate**, not a member. Recording it as
/// a member would put a twin's addresses under our own id, and hearsay must never confer liveness.
type SameIdAddrs = Arc<Mutex<HashSet<SocketAddr>>>;
/// `NodeId`s a reader has caught being used by more than one machine, including our own. Reader
/// threads cannot resolve this — they have no send halves and no access to the data directory — so
/// they record it and [`BroadcastDissemination::dedup_links`] hands it to the node.
type Conflicts = Arc<Mutex<HashSet<NodeId>>>;

/// A name for a connection that **both** of its ends compute identically: its two endpoint
/// addresses, ordered. One end's `(local, peer)` is the other's `(peer, local)`, so ordering the
/// pair cancels the asymmetry.
type LinkKey = (SocketAddr, SocketAddr);

fn link_key(local: SocketAddr, peer: SocketAddr) -> LinkKey {
    if local <= peer {
        (local, peer)
    } else {
        (peer, local)
    }
}

/// Per-syscall slice, so no single `write` parks indefinitely. The per-peer allowance is what actually
/// decides; this only keeps a blocked syscall from outliving it — which means it has to be **smaller
/// than the smallest allowance**, or that allowance cannot be honoured at all. It was 100 ms against a
/// 50 ms small-frame budget, so the 50 ms bound was measured at 104 ms.
const PEER_WRITE_SLICE: Duration = Duration::from_millis(20);

/// The minimum rate a peer must sustain to keep its link during a registry burst.
///
/// **A burst's deadline is derived from its size, not fixed.** That distinction is the whole design:
/// a fixed budget cannot be both large enough for a healthy peer and small enough to bound the tick,
/// because the payload is a whole registry and registries have no bound. Sized as a time, 500 ms buys
/// 6.8 MB at the ~13.6 MB/s a real daemon drains at — so a *healthy* peer with a 200 000-channel
/// registry would have failed it. Sized as a rate, the same policy reads "a peer slower than this is
/// not keeping up", which is true independently of how big the registry is or how fast the link is.
///
/// 4 MiB/s is roughly a third of what an idle daemon on a LAN sustains, so a peer three times degraded
/// still keeps its link. Below it, the peer is dropped rather than allowed to hold the control plane.
///
/// The residual, stated plainly because it cannot be fixed by choosing a better number: a burst of R
/// bytes may hold the dissemination lock for up to `R / MIN_DRAIN_RATE` **per unresponsive peer**.
/// Measured against a peer that never drains, the hold is `0.58 s + 1.36 × (R / rate)` — so it crosses
/// `LIVENESS_TIMEOUT` at about **28 MiB of registry, ~300 000 identities** at 48-character names, not the
/// 40 MB the arithmetic alone suggests. The 1.36× is unexplained; payload accounting was verified to
/// 0.04 % and the snapshot clone measured at 34–48 ms, so it is a property of the write loop. Treat the
/// arithmetic as ~40 % optimistic.
///
/// Bounding this requires not holding the lock across the write — a per-peer outbox, as the stream plane
/// already has (`FramedConn`). That is post-0.3.0 work, and ~28 MiB is the number that says when it stops
/// being optional.
const MIN_DRAIN_RATE: u64 = 4 << 20;

/// Floor under a burst's deadline, so a small delta is not given a microscopic budget.
const PEER_BURST_MIN: Duration = Duration::from_millis(500);

/// Budget for a single *small* frame — a heartbeat, a hint, a departure notice. Tens of bytes, so a
/// peer that cannot take one in this long is wedged rather than slow.
///
/// Deliberately much smaller than a burst's, because these are the frames sent to **every** peer in one
/// pass: each peer gets this allowance of its own, so a heartbeat broadcast costs P times it in the worst
/// case, and at the ≤100-node target a 250 ms budget would have made that 25 s. Each failed write also
/// drops its peer, so the cost is paid once per wedged peer, not once per round.
const PEER_SMALL_FRAME_BUDGET: Duration = Duration::from_millis(50);

/// How long **each peer** may take over a burst of `bytes` before it is treated as not keeping up.
///
/// A `Duration`, not an `Instant`: the allowance belongs to a peer, and turning it into a shared
/// wall-clock deadline made one peer's stall everybody's eviction.
///
/// `bytes` is the **exact** encoded size (`identities_encoded_len`), not an estimate. An estimate is the
/// wrong tool here in a specific direction: guess low and the deadline is too tight, so a healthy peer
/// is dropped because somebody else's channel names were long. A 128-byte-per-identity guess undercounts
/// a member with a 48-character name and a 48-character topic by about 12 %.
fn burst_allowance(bytes: usize) -> Duration {
    let allowed = Duration::from_micros((bytes as u64).saturating_mul(1_000_000) / MIN_DRAIN_RATE);
    // The ceiling is defensive arithmetic, not policy. `Instant + Duration` panics on overflow, and a
    // saturating multiply feeding it could produce a duration measured in geological time; an hour is
    // already 14 GB at this rate, far past any registry that fits in memory, so no realistic payload is
    // affected. The *absence* of a policy ceiling is deliberate and documented on `MIN_DRAIN_RATE`: a
    // real ceiling would drop healthy peers again, which is the whole thing this shape avoids.
    allowed.clamp(PEER_BURST_MIN, Duration::from_secs(3600))
}

/// Identities per control frame.
///
/// Anti-entropy sends the whole registry on every reconnect, and a coalesced relay can carry a whole
/// pump cycle, so without a cap one frame could be megabytes — a 10.6 MB frame was measured taking
/// 6.1 s to write to a stalled peer, all of it under the dissemination lock. Chunking keeps any single
/// frame far below `MAX_FRAME_LEN` and bounds what a receiver must buffer to decode one. It no longer
/// interacts with the write bound at all — that is sized by total bytes, not by frame count. The receiver
/// merges each frame independently, and the merge is idempotent and order-free, so splitting is invisible
/// to it.
const MAX_IDENTITIES_PER_FRAME: usize = 256;

/// Rough encoded size of one `PeerHint`. Only used to size a deadline, and the 500 ms floor absorbs
/// the error until thousands of hints are pending at once.
const HINT_ENCODED_BYTES: usize = 160;

/// Cap on remembered addresses claiming our own id. A genuine duplicate is one or two; the bound is
/// there because the source is unauthenticated gossip.
const MAX_SAME_ID_ADDRS: usize = 8;

/// One peer link: the send half, plus what is needed to resolve a duplicate deterministically.
struct Peer {
    id: PeerId,
    conn: TcpTransport,
    /// This connection's symmetric name — the second half of the tie-break.
    key: LinkKey,
    /// Whether **we** dialled this link. Half of the tie-break: the initiator's `NodeId` is
    /// `self_node` for an outbound link and the peer's for an inbound one, and both ends compute
    /// the same value for the same link.
    outbound: bool,
    /// What is left of **this peer's own** allowance for the burst currently being written, drawn
    /// down by the time its own writes actually cost.
    ///
    /// This exists because the alternative — one absolute deadline shared by every peer — is a global
    /// budget with a per-peer charge, and those do not compose. `write_all_by` checks its deadline
    /// *before* the first syscall, so once one slow peer had spent the shared instant, every peer
    /// visited afterwards failed with **zero bytes written** and was shut down and evicted for a stall
    /// that was not its own. Measured three ways independently: where the previous release kept 4 of 5
    /// peers and delivered the full delta, the shared deadline kept **0 of 5** and held the lock 7×
    /// longer. A per-peer budget also means the loop order stops being load-bearing — with a shared
    /// instant, iterating peers outer instead of chunks outer silently dropped 3 of 4 *healthy* peers,
    /// and nothing said so.
    burst_left: Duration,
}

/// Which node opened a link: ourselves if we dialled, otherwise the peer. Both ends of a link
/// compute the same answer, which is what lets them resolve a duplicate without negotiating.
fn initiator(p: &Peer, peer_node: NodeId, self_node: NodeId) -> NodeId {
    if p.outbound { self_node } else { peer_node }
}

/// Eager-broadcast dissemination over a set of peer TCP connections.
pub struct BroadcastDissemination {
    /// This node's identity + the stream address it advertises in heartbeats.
    self_node: NodeId,
    self_addr: SocketAddr,
    /// A node is "live" if heard from within this timeout.
    liveness_timeout: Duration,
    /// The control address this node advertises, so peers can dial it back and a node that
    /// learns of us second-hand can form a direct link.
    self_control_addr: SelfControl,
    /// Human-readable label for this node, gossiped for display only.
    self_name: String,
    /// Send halves of peer connections, each with a stable id (broadcast target for
    /// deltas/heartbeats). Ids are stable across peer removal, unlike positions.
    peers: Vec<Peer>,
    next_peer_id: PeerId,
    hints: Hints,
    link_peers: LinkPeers,
    /// `NodeId`s seen on two machines, as detected by the reader threads.
    conflicts: Conflicts,
    /// Addresses where a third party has reported *our* id — the twin dial candidates.
    same_id_addrs: SameIdAddrs,
    /// Filled by per-peer reader threads; drained by [`pump`](Self::pump).
    inbox: Inbox,
    membership: SharedMembership,
    connected: Connected,
    dial_identity: DialIdentity,
}

impl BroadcastDissemination {
    pub fn new(self_node: NodeId, self_addr: SocketAddr, liveness_timeout: Duration) -> Self {
        Self {
            self_node,
            self_addr,
            self_control_addr: Arc::new(Mutex::new(self_addr)),
            self_name: String::new(),
            liveness_timeout,
            peers: Vec::new(),
            next_peer_id: NO_PEER + 1,
            hints: Arc::new(Mutex::new(VecDeque::new())),
            link_peers: Arc::new(Mutex::new(HashMap::new())),
            conflicts: Arc::new(Mutex::new(HashSet::new())),
            same_id_addrs: Arc::new(Mutex::new(HashSet::new())),
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            membership: Arc::new(Mutex::new(Membership::new())),
            connected: Arc::new(Mutex::new(HashSet::new())),
            dial_identity: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adopt an **inbound** peer connection (the peer dialed us; we don't know its dial
    /// address, so it isn't tracked for reconnection — it reconnects to us).
    pub fn add_peer(
        &mut self,
        transport: TcpTransport,
        initial_sync: &[ChannelIdentity],
    ) -> io::Result<()> {
        self.adopt(transport, None, initial_sync)
    }

    /// Adopt an **outbound** peer connection dialed to `addr`, tracking it so it's deduped
    /// and reconnected (its reader clears the tracking on disconnect).
    pub fn add_outbound_peer(
        &mut self,
        transport: TcpTransport,
        addr: SocketAddr,
        initial_sync: &[ChannelIdentity],
    ) -> io::Result<()> {
        self.connected.lock_safe().insert(addr);
        let r = self.adopt(transport, Some(addr), initial_sync);
        if r.is_err() {
            self.connected.lock_safe().remove(&addr);
        }
        r
    }

    /// How many peer links are currently held, in either direction.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Whether an outbound link to `addr` is currently believed connected.
    pub fn is_connected(&self, addr: SocketAddr) -> bool {
        self.connected.lock_safe().contains(&addr)
    }

    /// Send the join-time `RegistrySync` + `Heartbeat` + directory, then spawn the reader thread and
    /// retain the send half. `addr` is `Some` for outbound links (tracked for reconnection).
    ///
    /// **Reader first, and shut the socket down on every failure.** A failed adopt used to leak: the
    /// reader owns a `try_clone` dup, so returning early dropped only the send half and left the
    /// connection ESTABLISHED with a thread parked in `recv_frame` forever — one thread, one descriptor
    /// and one orphaned socket per attempt, and callers discard this error, so the dialler retried the
    /// same address next tick and leaked again.
    ///
    /// The tidier-looking repair — do the writes first, so a failure has nothing to unwind — is wrong,
    /// and worth recording so it is not tried again. **Both ends of a pair dial**, so both adopt at the
    /// same time; if neither reads until its own writes finish, two nodes with a large registry each
    /// write megabytes into a socket nobody is draining, both fill, both time out, and both drop the
    /// link — for ever, because they retry identically. Spawning the reader first means each end drains
    /// the other's join while sending its own, which is what makes a simultaneous adopt work at all.
    fn adopt(
        &mut self,
        mut transport: TcpTransport,
        addr: Option<SocketAddr>,
        initial_sync: &[ChannelIdentity],
    ) -> io::Result<()> {
        // Every control-plane write happens while holding the dissemination lock, which is also what
        // the heartbeat needs, so an unbounded write is a node-wide stall. `PEER_WRITE_SLICE` keeps a
        // single syscall from parking; the deadline each send is given is the bound that matters.
        // **Not `let _ =`.** This is the sole precondition of every write bound below: `write_all_by`
        // only consults its deadline *between* syscalls, so without a per-syscall timeout a single
        // `write` parks indefinitely and every deadline in this file silently stops existing.
        transport.set_write_timeout(Some(PEER_WRITE_SLICE))?;
        let (local, peer) = transport.endpoints()?;
        let key = link_key(local, peer);
        let reader = transport.try_clone()?;
        let id = self.next_peer_id;
        self.next_peer_id += 1;
        spawn_reader(
            reader,
            id,
            Arc::clone(&self.inbox),
            Arc::clone(&self.hints),
            Arc::clone(&self.membership),
            Arc::clone(&self.connected),
            Arc::clone(&self.link_peers),
            addr,
            self.self_node,
            Arc::clone(&self.self_control_addr),
            Arc::clone(&self.conflicts),
            Arc::clone(&self.same_id_addrs),
            Arc::clone(&self.dial_identity),
        );

        // One deadline for the whole exchange, not one per frame — see `MIN_DRAIN_RATE`. On any failure
        // the socket is shut down, which is what stops the reader thread above from outliving this
        // call.
        match self.write_join(&mut transport, initial_sync) {
            Ok(()) => {
                self.peers.push(Peer {
                    id,
                    conn: transport,
                    key,
                    outbound: addr.is_some(),
                    burst_left: PEER_BURST_MIN,
                });
                Ok(())
            }
            Err(e) => {
                let _ = transport.shutdown();
                Err(e)
            }
        }
    }

    /// The join-time exchange: the chunked registry, a heartbeat, and the directory. Split out so
    /// `adopt` has exactly one error path to clean up.
    fn write_join(
        &self,
        transport: &mut TcpTransport,
        initial_sync: &[ChannelIdentity],
    ) -> io::Result<()> {
        // Derived from the payload, for the reason given on `MIN_DRAIN_RATE`: a fixed 500 ms could not
        // fit a healthy peer's whole registry, so it turned "this peer is too slow" into "this registry
        // is too big" and made large deployments unable to form links at all. The floor keeps a small
        // join generous.
        // A join writes to one socket that is not yet in `peers`, so it carries its own deadline
        // rather than drawing on a per-peer budget. Sized the same way.
        let deadline = Instant::now()
            + burst_allowance(
                identities_encoded_len(initial_sync) + self.membership.lock_safe().len() * 160,
            );
        // Chunked, because anti-entropy carries the entire registry and one frame of it was measured at
        // 10.6 MB taking 6.1 s to write to a stalled peer. The receiver merges each frame independently
        // and the merge is idempotent and order-free, so splitting is invisible to it.
        for batch in initial_sync.chunks(MAX_IDENTITIES_PER_FRAME) {
            transport.send_frame_by(
                &encode_control(&ControlMsg::RegistrySync(batch.to_vec())),
                deadline,
            )?;
        }
        transport.send_frame_by(&encode_control(&self.heartbeat()), deadline)?;
        // Introduce everyone we already know, so a joiner learns the whole mesh from one link instead of
        // only its seed. Sent once per link, not periodically.
        //
        // Bind the directory *before* the loop: a guard temporary in a `for` head lives to the end of the
        // loop, so this held the membership lock across every blocking send. One peer that stopped
        // reading would stall every reader thread waiting on that lock.
        let directory = self.membership.lock_safe().directory();
        for (node, addr, control_addr, name) in directory {
            if node == self.self_node {
                continue;
            }
            transport.send_frame_by(
                &encode_control(&ControlMsg::PeerHint {
                    node,
                    addr,
                    control_addr,
                    name,
                }),
                deadline,
            )?;
        }
        Ok(())
    }

    /// Collapse duplicate links to the same node, keeping exactly one.
    ///
    /// Both ends of a newly-discovered pair dial each other, so a cross-dial race is normal
    /// rather than exceptional — which is the price of not electing a dialler in advance. It has
    /// to be, because election has to happen before anyone knows whether the elected node can
    /// actually reach the other: under asymmetric reachability (a firewall, a NAT) the wrong
    /// choice means the pair never links at all.
    ///
    /// Resolution must be one both ends reach independently, or they would drop opposite links
    /// and be left with none. **Keep the link whose initiator has the lower `NodeId`** — each end
    /// knows, for each link, whether it dialled and who the peer is, so both compute the same
    /// initiator for the same link. Ties (two links with the *same* initiator, which two dial
    /// addresses for one peer produce) break on the link's own symmetric name, so that ordering is
    /// shared too. It used to break on `PeerId`, a per-process counter: the two ends numbered the
    /// same pair of links differently, each kept the one the other dropped, and the peers were left
    /// with **no** link at all — then re-dialled and did it again every tick.
    /// Returns any `NodeId` found on **two different machines** — see the note in the body.
    pub fn dedup_links(&mut self) -> Vec<NodeId> {
        let ids = self.link_peers.lock_safe().clone();
        let self_node = self.self_node;

        // **Two links claiming one id at different control addresses are not a duplicate link —
        // they are a duplicate *identity*.** Collapsing them would be the worst possible response:
        // it would drop connectivity to a real peer to tidy up a misconfiguration. The addresses
        // make the distinction exact, with no heuristic: one machine advertises one control
        // address, so two of them means two machines.
        let mut addrs: HashMap<NodeId, HashSet<SocketAddr>> = HashMap::new();
        for p in &self.peers {
            if let Some(&(node, control)) = ids.get(&p.id) {
                addrs.entry(node).or_default().insert(control);
            }
        }
        let mut conflicted: HashSet<NodeId> = addrs
            .iter()
            .filter(|(_, a)| a.len() > 1)
            .map(|(&n, _)| n)
            .collect();
        // Two links are only *one* of the two ways a duplicate shows up, and not the way that
        // matters most. When the duplicated id is **ours**, there is nothing to compare: a twin
        // claiming our id appears on a single link, so this set was empty and the node never learned
        // it had to step aside — the exact case (a cloned image) that the step-aside exists for.
        // The readers catch it instead, by noticing a heartbeat that claims our id from an address
        // that is not ours, and leave it here.
        conflicted.extend(self.conflicts.lock_safe().iter().copied());

        let key = |p: &Peer| {
            ids.get(&p.id)
                .filter(|(node, _)| !conflicted.contains(node))
                .map(|&(node, _)| (node, initiator(p, node, self_node)))
        };

        // The winning (initiator, link) per node.
        let mut winner: HashMap<NodeId, (NodeId, LinkKey)> = HashMap::new();
        for p in &self.peers {
            if let Some((node, init)) = key(p) {
                let cand = (init, p.key);
                winner
                    .entry(node)
                    .and_modify(|best| {
                        if cand < *best {
                            *best = cand;
                        }
                    })
                    .or_insert(cand);
            }
        }

        let (keep, drop): (Vec<Peer>, Vec<Peer>) =
            self.peers.drain(..).partition(|p| match key(p) {
                // Identity not learned yet — keep it; the next tick decides.
                None => true,
                Some((node, init)) => winner.get(&node) == Some(&(init, p.key)),
            });
        for p in drop {
            // Shut the socket so the far end's reader unblocks now rather than whenever the OS
            // notices, and so its `connected` tracking clears promptly.
            let _ = p.conn.shutdown();
        }
        self.peers = keep;
        conflicted.into_iter().collect()
    }

    /// Nodes we currently hold a link to, by identity rather than by dial address — an inbound
    /// link has no dial address, so address-based tracking alone would call its peer unconnected
    /// and dial it again.
    pub fn linked_nodes(&self) -> HashSet<NodeId> {
        let ids = self.link_peers.lock_safe();
        self.peers
            .iter()
            .filter_map(|p| ids.get(&p.id).map(|&(node, _)| node))
            .collect()
    }

    /// Which node is known to sit at `addr`, if any — by its advertised control address first, and
    /// otherwise by what a previous dial to that exact address turned out to reach.
    ///
    /// Membership takes precedence because it is current: a cached dial outcome is only evidence
    /// about the past, and a node whose address was reused by another would otherwise keep answering
    /// with the wrong identity.
    pub fn node_at(&self, addr: SocketAddr) -> Option<NodeId> {
        self.membership
            .lock_safe()
            .node_at(addr)
            .or_else(|| self.dial_identity.lock_safe().get(&addr).copied())
    }

    /// This node's own heartbeat frame.
    fn heartbeat(&self) -> ControlMsg {
        ControlMsg::Heartbeat {
            node: self.self_node,
            addr: self.self_addr,
            control_addr: *self.self_control_addr.lock_safe(),
            name: self.self_name.clone(),
        }
    }

    /// Forward queued peer knowledge to every link except the one it came from. Called on the
    /// maintenance tick; a hint is only queued when it taught this node something, so a mesh
    /// with cycles goes quiet once everyone knows everyone.
    pub fn relay_hints(&mut self) {
        let pending: Vec<_> = self.hints.lock_safe().drain(..).collect();
        // One allowance per peer, spanning every hint. Each hint used to get its own budget, so a
        // forming 20-node mesh meant 19 hints × 19 peers = 361 independent ones in a single tick — and
        // then sizing it by `pending.len()` alone undercounted by the peer multiplier, since every hint
        // goes to every peer. Per-peer, the size each peer actually receives *is* `pending.len()`, so
        // the multiplier disappears rather than needing to be remembered.
        self.begin_burst(burst_allowance(pending.len() * HINT_ENCODED_BYTES));
        for (from, node, addr, control_addr, name) in pending {
            if node == self.self_node {
                continue; // never gossip about ourselves second-hand; our heartbeat says it
            }
            let frame = encode_control(&ControlMsg::PeerHint {
                node,
                addr,
                control_addr,
                name,
            });
            self.send_except(from, &frame);
        }
    }

    /// Tell every peer this node is leaving, so they mark it not-live now rather than waiting out
    /// the liveness timeout. Best-effort and synchronous: the write happens before returning, so by
    /// the time this does the notice is on the wire (or that peer has been dropped for not taking it).
    pub fn announce_leaving(&mut self) {
        let frame = encode_control(&ControlMsg::Leaving {
            node: self.self_node,
        });
        self.begin_burst(PEER_SMALL_FRAME_BUDGET);
        self.broadcast(&frame);
    }

    /// Addresses a third party has reported *our own* `NodeId` at, and which we hold no link to.
    ///
    /// These are the twin candidates. Dialling one is how a clone meets its sibling — see the
    /// `PeerHint` arm of the reader — and the heartbeat that arrives over the resulting link is what
    /// turns suspicion into the exact comparison `dedup_links` reports.
    pub fn same_id_candidates(&self) -> Vec<SocketAddr> {
        let connected = self.connected.lock_safe();
        self.same_id_addrs
            .lock_safe()
            .iter()
            .filter(|addr| !connected.contains(addr))
            .copied()
            .collect()
    }

    /// Peers we know of but hold no outbound link to, as `(node, control address)`.
    pub fn unconnected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        let connected = self.connected.lock_safe();
        self.membership
            .lock_safe()
            .known_peers()
            .into_iter()
            .filter(|(node, control)| *node != self.self_node && !connected.contains(control))
            .collect()
    }

    /// Set the human-readable label gossiped with this node's heartbeat.
    pub fn set_self_name(&mut self, name: String) {
        self.self_name = name;
    }

    /// The label a peer advertised, if known.
    pub fn name_of(&self, node: NodeId) -> Option<String> {
        self.membership
            .lock_safe()
            .name_of(node)
            .map(str::to_string)
    }

    /// Set the control address advertised to peers — used after binding the control listener to
    /// an ephemeral port, so what we gossip is the address that actually accepts connections.
    pub fn set_self_control_addr(&mut self, addr: SocketAddr) {
        *self.self_control_addr.lock_safe() = addr;
    }

    /// The control address this node advertises — so the dialler can decline to dial itself.
    pub fn self_control_addr(&self) -> SocketAddr {
        *self.self_control_addr.lock_safe()
    }

    /// Send a `Heartbeat` (this node + its address) to every peer. The caller drives the
    /// cadence; peers refresh our membership entry on receipt.
    pub fn emit_heartbeat(&mut self) -> io::Result<()> {
        let hb = encode_control(&self.heartbeat());
        // A per-peer budget, and a small one: this frame goes to *every* peer in one pass, so its cost
        // is P times whatever is chosen here. At the ≤100-node target a 250 ms budget made the
        // heartbeat broadcast — the very thing the liveness timeout measures — worth 25 s.
        self.begin_burst(PEER_SMALL_FRAME_BUDGET);
        self.broadcast(&hb);
        Ok(())
    }

    /// Resolve a peer's current stream address (last heartbeat wins).
    pub fn addr_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.membership.lock_safe().addr_of(node)
    }

    /// Resolve a peer's stream address only if it is a **live** member (heartbeat within the
    /// liveness timeout); `None` if unknown or stale. Lets resolution distinguish "owner
    /// unreachable" from "owner live" instead of handing back a stale address.
    pub fn live_addr_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.membership
            .lock_safe()
            .live_addr_of(node, self.liveness_timeout)
    }

    /// How long since we last had **direct** evidence of `node` — a heartbeat, or its own notice
    /// that it was leaving; `None` if we have never had contact, including for a node known only by
    /// hearsay. Used to judge whether an owner is *gone* rather than momentarily silent, so `None`
    /// has to mean "cannot say", never "gone for ages".
    pub fn silent_for(&self, node: NodeId) -> Option<std::time::Duration> {
        self.membership.lock_safe().silent_for(node)
    }

    /// Set the stream address advertised in heartbeats — used after binding the stream
    /// listener to an ephemeral port (`:0`), so peers learn the real address.
    pub fn set_self_addr(&mut self, addr: SocketAddr) {
        self.self_addr = addr;
    }

    /// Best-effort broadcast to all peers; drops peers whose send fails (disconnected).
    fn broadcast(&mut self, frame: &[u8]) {
        self.send_except(NO_PEER, frame);
    }

    /// Give every peer its own allowance for the burst about to be written. Called once per burst,
    /// before the frames — so a peer's budget spans its own chunks and nobody else's.
    fn begin_burst(&mut self, allowance: Duration) {
        for p in &mut self.peers {
            p.burst_left = allowance;
        }
    }

    /// Broadcast to every peer except `from`; drops peers whose send fails (disconnected).
    fn send_except(&mut self, from: PeerId, frame: &[u8]) {
        self.peers.retain_mut(|p| {
            if p.id == from {
                return true;
            }
            // Each peer against its own remaining allowance, and charged only for its own time.
            let started = Instant::now();
            let sent = p.conn.send_frame_by(frame, started + p.burst_left);
            p.burst_left = p.burst_left.saturating_sub(started.elapsed());
            match sent {
                Ok(()) => true,
                Err(_) => {
                    // Shut it down rather than just dropping our handle, so the reader thread on
                    // the other half unblocks and cleans up instead of lingering forever.
                    let _ = p.conn.shutdown();
                    false
                }
            }
        });
    }
}

impl Dissemination for BroadcastDissemination {
    fn announce(&mut self, delta: &[ChannelIdentity]) -> io::Result<()> {
        // One deadline for the whole burst, not one per chunk. A budget per chunk is not a bound on a
        // sequence of chunks — this diff's own `write_join` says so — and a peer draining just fast
        // enough to clear each 250 ms check individually held the lock for the sum of them: measured at
        // 13.4 s for a 600 000-identity relay, four times over the liveness budget, with no bound
        // firing at any point and the peer never dropped.
        self.begin_burst(burst_allowance(identities_encoded_len(delta)));
        for batch in delta.chunks(MAX_IDENTITIES_PER_FRAME) {
            let frame = encode_control(&ControlMsg::RegistryDelta(batch.to_vec()));
            self.broadcast(&frame);
        }
        Ok(())
    }

    fn relay(&mut self, from: PeerId, delta: &[ChannelIdentity]) -> io::Result<()> {
        self.begin_burst(burst_allowance(identities_encoded_len(delta)));
        for batch in delta.chunks(MAX_IDENTITIES_PER_FRAME) {
            let frame = encode_control(&ControlMsg::RegistryDelta(batch.to_vec()));
            self.send_except(from, &frame);
        }
        Ok(())
    }

    fn reply(&mut self, to: PeerId, delta: &[ChannelIdentity]) -> io::Result<()> {
        self.begin_burst(burst_allowance(identities_encoded_len(delta)));
        for batch in delta.chunks(MAX_IDENTITIES_PER_FRAME) {
            let frame = encode_control(&ControlMsg::RegistryDelta(batch.to_vec()));
            let Some(p) = self.peers.iter_mut().find(|p| p.id == to) else {
                return Ok(()); // already reaped, by an earlier batch or another send
            };
            let started = Instant::now();
            let sent = p.conn.send_frame_by(&frame, started + p.burst_left);
            p.burst_left = p.burst_left.saturating_sub(started.elapsed());
            if sent.is_err() {
                let _ = p.conn.shutdown();
                self.peers.retain(|p| p.id != to);
                return Ok(());
            }
        }
        Ok(())
    }

    fn pump(&mut self) -> io::Result<Vec<(PeerId, ChannelIdentity)>> {
        let mut q = self.inbox.lock_safe();
        Ok(q.drain(..).collect())
    }

    fn live_members(&self) -> Vec<NodeId> {
        self.membership
            .lock_safe()
            .live_members(self.liveness_timeout)
    }
}

/// Per-peer reader loop: decode inbound control frames until the connection drops.
/// `RegistryDelta`/`RegistrySync` identities go to the inbox for the node to merge;
/// `Heartbeat`s refresh membership. Client→manager frames (`Register`, …) are not expected
/// on a peer link and are ignored.
#[allow(clippy::too_many_arguments)]
fn spawn_reader(
    mut reader: TcpTransport,
    id: PeerId,
    inbox: Inbox,
    hints: Hints,
    membership: SharedMembership,
    connected: Connected,
    link_peers: LinkPeers,
    addr: Option<SocketAddr>,
    self_node: NodeId,
    self_control: SelfControl,
    conflicts: Conflicts,
    same_id_addrs: SameIdAddrs,
    dial_identity: DialIdentity,
) {
    let dial_addr = addr;
    std::thread::spawn(move || {
        while let Ok(bytes) = reader.recv_frame() {
            let Ok(msg) = decode_control(&bytes) else {
                break;
            };
            match msg {
                ControlMsg::RegistryDelta(ids) | ControlMsg::RegistrySync(ids) => {
                    inbox.lock_safe().extend(ids.into_iter().map(|i| (id, i)));
                }
                // A direct heartbeat: confers liveness *and* teaches addresses. Queue a hint only
                // when it told us something new, so a steady heartbeat stream does not become a
                // steady relay stream.
                ControlMsg::Heartbeat {
                    node,
                    addr,
                    control_addr,
                    name,
                } => {
                    if node == self_node {
                        // Either we dialled ourselves, or another machine is using our id — and the
                        // advertised control address separates the two exactly, because a heartbeat
                        // always carries the sender's own advertised address rather than the address
                        // it was reached on. Same address ⇒ this link is a loop back to us, which a
                        // seed list naming every node (including this one) produces routinely.
                        if control_addr != *self_control.lock_safe() {
                            conflicts.lock_safe().insert(node);
                        }
                        // **Never `record` under our own id, in either case.** Doing so overwrote
                        // our own membership entry with the twin's addresses — and membership is what
                        // `live_addr_of` answers from, so a third party's subscribers were handed
                        // whichever of the two machines heartbeated most recently. Leaving the link's
                        // identity unclaimed also keeps a self-link out of the tie-break, where it
                        // would compete with a real peer's link.
                        //
                        // Note what this is *not*: it is not what kept two clones from meeting. Dial
                        // candidates exclude our own id by construction, in `unconnected_peers`, so
                        // they could not have met whatever this entry said. The `PeerHint` arm below
                        // is where that is repaired.
                        continue;
                    }
                    // A heartbeat is the only thing that says who is on the far end of this link,
                    // which is what makes duplicate links resolvable.
                    link_peers.lock_safe().insert(id, (node, control_addr));
                    // `dial_addr`, not the heartbeat's `addr` field: one is where we reached this
                    // peer, the other is the stream address it advertises, and the shadowing here is
                    // exactly the confusion this rename avoids.
                    if let Some(dialled) = dial_addr {
                        dial_identity.lock_safe().insert(dialled, node);
                    }
                    if membership
                        .lock_safe()
                        .record(node, addr, control_addr, &name)
                    {
                        hints
                            .lock_safe()
                            .push_back((id, node, addr, control_addr, name));
                    }
                }
                // Hearsay: learn where the node is, but grant it no liveness — we have not heard
                // from it, and `live_members` must keep meaning "reachable by us".
                ControlMsg::PeerHint {
                    node,
                    addr,
                    control_addr,
                    name,
                } => {
                    if node == self_node {
                        // Hearsay about ourselves. It must not enter membership — our own heartbeat
                        // is the authority on our addresses, and `learn`ing them from a third party
                        // would overwrite that entry with a twin's.
                        //
                        // But it must not be discarded either, because **this is the only way two
                        // clones of one data directory ever hear of each other.** Neither dials the
                        // other: dial candidates come from membership, which excludes our own id by
                        // construction. So a fleet of clones seeded at a common bootstrap stayed
                        // undetected forever — each clone linked only to the bootstrap, which saw the
                        // duplicate plainly and could do nothing about it. That is the golden-image
                        // case the step-aside exists for, and it was unreachable.
                        //
                        // Keep the address as a *dial candidate*. Hearsay alone must not condemn an
                        // identity — a node that restarted on an ephemeral port would find peers
                        // relaying its own stale address and delete its identity over it — so the
                        // hint only earns a dial, and the heartbeat that comes back over that link is
                        // what decides twin-versus-self-link, on direct evidence, as always.
                        if control_addr != *self_control.lock_safe() {
                            let mut same = same_id_addrs.lock_safe();
                            // A real duplicate is one or two addresses. Cap it: this is fed by
                            // unauthenticated gossip, and an unbounded dial-candidate set that any
                            // peer can grow is a way to spend this node's whole dial budget.
                            if same.len() < MAX_SAME_ID_ADDRS {
                                same.insert(control_addr);
                            }
                        }
                        continue;
                    }
                    if membership
                        .lock_safe()
                        .learn(node, addr, control_addr, &name)
                    {
                        hints
                            .lock_safe()
                            .push_back((id, node, addr, control_addr, name));
                    }
                }
                // A peer leaving cleanly. Not relayed: the mesh is self-forming, so in the
                // topology this produces every node is already adjacent to the departing one. A
                // node that somehow is not adjacent falls back to the liveness timeout, exactly as
                // it would for a peer that crashed.
                // Only the peer on *this* link may announce its own departure. Accepting a third
                // party's id would let one node mark another not-live, which under a duplicate id
                // meant a departing twin silenced its still-serving sibling.
                ControlMsg::Leaving { node }
                    if link_peers.lock_safe().get(&id).map(|(n, _)| *n) == Some(node) =>
                {
                    membership.lock_safe().retire(node);
                }
                _ => {} // not expected on a peer link
            }
        }
        // **Shut the socket down.** The reader owns only a `try_clone` dup of the fd, so dropping
        // it leaves the connection ESTABLISHED and our *send* half writable. Nothing else prunes
        // `peers` except a send failure, and `dedup_links` deliberately keeps a link whose identity
        // it does not know — which this exit has just erased. The result was a zombie peer that
        // (a) leaked an fd and a thread every tick as `connected` was cleared and the node re-dialled,
        // (b) eventually wedged `emit_heartbeat` inside `write_all` while holding the dissemination
        // lock, stalling the entire node silently, and (c) shared an initiator with its own
        // replacement, so the far end's tie-break kept the zombie and killed every fresh link.
        // Shutting down makes the far end's next write fail, so its `send_except` reaps the peer.
        let _ = reader.shutdown();
        // Connection dropped: forget who was on it, and clear outbound tracking so the node
        // reconnects.
        link_peers.lock_safe().remove(&id);
        if let Some(addr) = addr {
            connected.lock_safe().remove(&addr);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use xchannel_net_core::RecordIndex;
    use xchannel_net_core::transport::{Listener, TcpListener};

    /// Poll a condition for up to two seconds. The reader runs on its own thread, so anything it
    /// records is observed rather than awaited.
    fn poll_for<R>(mut f: impl FnMut() -> Option<R>) -> R {
        for _ in 0..2000 {
            if let Some(r) = f() {
                return r;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("condition not met within timeout");
    }

    fn ident(name: &str, owner: u64) -> ChannelIdentity {
        ChannelIdentity {
            name: name.to_string(),
            owner: NodeId(owner),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(0),
            registered_at_nanos: 1,
            epoch: 0,
            deleted: false,
            member_of: None,
        }
    }

    /// **A burst's allowance must scale with the burst.** A fixed one turned "this peer is too slow"
    /// into "this registry is too big": at the ~13.6 MB/s a real daemon drains at, 500 ms buys 6.8 MB,
    /// so a *healthy* peer holding a 200 000-channel registry could not have completed a join. Measured
    /// since: a fast peer takes a 35.9 MiB join in 0.030 s, which the size-derived allowance gives it
    /// nine seconds to do.
    #[test]
    fn a_bursts_allowance_scales_with_its_payload() {
        assert_eq!(
            burst_allowance(0),
            PEER_BURST_MIN,
            "a small burst gets the floor, not a microscopic budget"
        );
        let big = burst_allowance(40 << 20);
        assert!(
            big > Duration::from_secs(9) && big < Duration::from_secs(11),
            "a 40 MiB burst should be allowed about 10 s at 4 MiB/s, got {big:?}"
        );
        assert!(
            big > burst_allowance(1 << 20),
            "the allowance must scale with the payload, or it is a registry-size limit in disguise"
        );
    }

    /// **One slow peer must not evict the others.** The allowance is per peer precisely because a shared
    /// deadline is a global budget with a per-peer charge: `write_all_by` checks its deadline before its
    /// first syscall, so every peer written *after* the budget was spent failed with zero bytes and was
    /// evicted for somebody else's stall. Three reviewers measured the same thing independently — 4 of 5
    /// peers kept before the change, 0 of 5 after.
    ///
    /// Exercises `send_except` directly with explicit budgets rather than going through `announce`,
    /// because otherwise the outcome turns on whether the payload happens to exceed this machine's
    /// socket buffers — which is a property of the machine, not of the code under test.
    #[test]
    fn a_stalled_peer_does_not_spend_another_peers_budget() {
        let mut wedged_l = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut healthy_l = TcpListener::bind("127.0.0.1:0").unwrap();
        let (wa, ha) = (
            wedged_l.local_addr().unwrap(),
            healthy_l.local_addr().unwrap(),
        );
        let wedged_accept = std::thread::spawn(move || wedged_l.accept().unwrap());
        let healthy_accept = std::thread::spawn(move || healthy_l.accept().unwrap());
        let wedged = TcpTransport::connect(wa).unwrap();
        let healthy = TcpTransport::connect(ha).unwrap();
        // Accepted and never read from, versus accepted and drained eagerly.
        let _held = wedged_accept.join().unwrap();
        let drain = healthy_accept.join().unwrap();
        std::thread::spawn(move || {
            let mut conn = drain.into_stream();
            let mut sink = [0u8; 1 << 16];
            while std::io::Read::read(&mut conn, &mut sink).unwrap_or(0) > 0 {}
        });

        let mut d = BroadcastDissemination::new(
            NodeId(1),
            "127.0.0.1:9501".parse().unwrap(),
            Duration::from_secs(60),
        );
        // The wedged peer goes in **first**: that is the ordering where a shared deadline did the most
        // damage, because everything after it was evicted without a byte attempted.
        for (conn, budget) in [
            (wedged, Duration::from_millis(50)),
            (healthy, Duration::from_secs(10)),
        ] {
            let (local, peer) = conn.endpoints().unwrap();
            let id = d.next_peer_id;
            d.next_peer_id += 1;
            conn.set_write_timeout(Some(PEER_WRITE_SLICE)).unwrap();
            d.peers.push(Peer {
                id,
                conn,
                key: link_key(local, peer),
                outbound: false,
                burst_left: budget,
            });
        }

        // Larger than any socket buffer, so the peer that never reads must exhaust its own budget.
        let frame = vec![0u8; 16 << 20];
        d.send_except(NO_PEER, &frame);

        assert_eq!(
            d.peer_count(),
            1,
            "the peer that stalled should be the only one dropped; the draining peer must keep its \
             link and its own budget"
        );
        assert!(
            d.peers[0].burst_left > Duration::from_secs(5),
            "the surviving peer was charged for the stalled peer's time"
        );
    }

    /// **Both ends adopt at the same time, and a large registry must not deadlock them.** Both ends of a
    /// pair dial, so simultaneous adoption is the normal case rather than a race — and if a node did not
    /// drain its peer's join while sending its own, two nodes with a big registry would each write into a
    /// socket nobody is reading, both fill, both hit the join deadline, and both drop the link. They
    /// would then retry identically, for ever. This is why the reader is spawned *before* the join writes
    /// and not after, however much tidier the other order looks.
    #[test]
    fn two_nodes_adopting_each_other_at_once_survive_a_large_registry() {
        // Sized to exceed the socket buffers rather than to model a realistic registry: at this size
        // (~18 MB each way) the wrong ordering makes *both* sides time out, and this one completes in a
        // fifth of a second. Below roughly a megabyte the buffers absorb the whole join and the hazard
        // is invisible, which is why the number is this large.
        let registry: Vec<ChannelIdentity> =
            (0..300_000).map(|i| ident(&format!("c.{i}"), 1)).collect();

        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let accept = std::thread::spawn(move || listener.accept().unwrap());
        let a_to_b = TcpTransport::connect(listen_addr).unwrap();
        let b_to_a = accept.join().unwrap();

        // Adopt from both sides at once, each pushing its whole registry at the other.
        let sync = registry.clone();
        let a = std::thread::spawn(move || {
            let mut a = BroadcastDissemination::new(
                NodeId(1),
                "127.0.0.1:9401".parse().unwrap(),
                Duration::from_secs(60),
            );
            a.add_peer(a_to_b, &sync).map(|()| a.peer_count())
        });
        let b = std::thread::spawn(move || {
            let mut b = BroadcastDissemination::new(
                NodeId(2),
                "127.0.0.1:9402".parse().unwrap(),
                Duration::from_secs(60),
            );
            b.add_peer(b_to_a, &registry).map(|()| b.peer_count())
        });

        assert_eq!(
            a.join().unwrap().expect("A's join must complete"),
            1,
            "A dropped the link during a simultaneous adopt"
        );
        assert_eq!(
            b.join().unwrap().expect("B's join must complete"),
            1,
            "B dropped the link during a simultaneous adopt"
        );
    }

    /// **Hearsay about our own id has to survive as a dial candidate, or the golden-image case is
    /// undetectable.** Two clones of one data directory seeded at a common bootstrap link only to the
    /// bootstrap; neither dials the other, because dial candidates come from membership and membership
    /// excludes our own id by construction. The bootstrap sees the duplicate plainly and can do
    /// nothing about it. The `PeerHint` naming our own id at the sibling's address is the only thread
    /// back to the sibling, and dropping it made the step-aside unreachable in exactly the deployment
    /// it was written for.
    ///
    /// The hint must earn a *dial* and nothing more: it must not become a member (hearsay confers no
    /// liveness) and it must not by itself condemn the identity, because a node restarted on an
    /// ephemeral port would find peers relaying its own stale address.
    #[test]
    fn hearsay_about_our_own_id_becomes_a_dial_candidate_and_not_a_member() {
        let ours: SocketAddr = "127.0.0.1:9201".parse().unwrap();
        let sibling: SocketAddr = "127.0.0.1:9202".parse().unwrap();

        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let accept = std::thread::spawn(move || listener.accept().unwrap());
        let mut bootstrap = TcpTransport::connect(listen_addr).unwrap();
        let near = accept.join().unwrap();

        let mut us = BroadcastDissemination::new(NodeId(7), ours, Duration::from_secs(60));
        us.set_self_control_addr(ours);
        us.add_peer(near, &[]).unwrap();

        // What a bootstrap relays when two clones share an id: our own id, at an address that is not
        // ours. Plus a hint carrying our *own* address, which must be ignored — that is just the
        // bootstrap telling us about ourselves.
        for addr in [sibling, ours] {
            bootstrap
                .send_frame(&encode_control(&ControlMsg::PeerHint {
                    node: NodeId(7),
                    addr,
                    control_addr: addr,
                    name: "clone".into(),
                }))
                .unwrap();
        }

        let candidates = poll_for(|| {
            let c = us.same_id_candidates();
            (!c.is_empty()).then_some(c)
        });
        assert_eq!(
            candidates,
            vec![sibling],
            "the sibling's address must be a dial candidate, and our own must not"
        );
        assert_eq!(
            us.addr_of(NodeId(7)),
            None,
            "hearsay about our own id must never enter membership"
        );
        assert!(
            us.dedup_links().is_empty(),
            "a hint is not evidence of a twin — only a heartbeat over a real link is"
        );
    }

    /// **The duplicate that matters is the one where the duplicated id is ours**, and it is invisible
    /// to link deduplication: a twin claiming our `NodeId` shows up on a *single* link, so there are
    /// never two links to compare. Before this, a cloned image — every copy carrying the same
    /// `.node_id` — was never reported, and the step-aside that exists precisely for that case never
    /// ran.
    ///
    /// The advertised control address is what separates it from a link we opened to *ourselves*,
    /// which a seed list naming every node in the mesh produces routinely and which must not be
    /// mistaken for a duplicate. A heartbeat always carries the sender's own advertised address, so
    /// the comparison is exact rather than a heuristic.
    #[test]
    fn a_twin_claiming_our_id_is_reported_and_a_link_to_ourselves_is_not() {
        let ours: SocketAddr = "127.0.0.1:9101".parse().unwrap();
        let theirs: SocketAddr = "127.0.0.1:9102".parse().unwrap();

        for (advertised, expect_conflict) in [(theirs, true), (ours, false)] {
            let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let listen_addr = listener.local_addr().unwrap();
            let accept = std::thread::spawn(move || listener.accept().unwrap());
            let mut far = TcpTransport::connect(listen_addr).unwrap();
            let near = accept.join().unwrap();

            let mut us = BroadcastDissemination::new(NodeId(7), ours, Duration::from_secs(60));
            us.set_self_control_addr(ours);
            us.add_peer(near, &[]).unwrap();

            // The far end claims *our* id. Only the address it advertises distinguishes a twin from
            // this node reaching itself.
            far.send_frame(&encode_control(&ControlMsg::Heartbeat {
                node: NodeId(7),
                addr: advertised,
                control_addr: advertised,
                name: "twin".into(),
            }))
            .unwrap();

            let mut conflicts = Vec::new();
            for _ in 0..2000 {
                conflicts = us.dedup_links();
                if !conflicts.is_empty() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                conflicts.contains(&NodeId(7)),
                expect_conflict,
                "advertised {advertised}, ours {ours}"
            );

            // Either way, our own membership entry must not have been overwritten with the far
            // end's addresses. It was — `record` was called under our own id — and membership is what
            // `live_addr_of` answers from, so a third party's subscribers were sent to whichever of
            // the two machines had heartbeated last.
            assert_eq!(
                us.addr_of(NodeId(7)),
                None,
                "a node must never record itself in its own membership map"
            );
        }
    }

    /// Connect two dissemination instances over loopback TCP and verify that an announce
    /// propagates and that heartbeats populate the membership address map.
    #[test]
    fn delta_propagates_and_membership_learns_address() {
        let a_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let b_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let timeout = Duration::from_secs(60);

        // B listens; A connects to B. Both wrap their ends as dissemination peers.
        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let accept = std::thread::spawn(move || listener.accept().unwrap());

        let a_to_b = TcpTransport::connect(listen_addr).unwrap();
        let b_to_a = accept.join().unwrap();

        let mut a = BroadcastDissemination::new(NodeId(1), a_addr, timeout);
        let mut b = BroadcastDissemination::new(NodeId(2), b_addr, timeout);
        // A registers "md.aapl" in its initial sync; both exchange heartbeats on add_peer.
        a.add_peer(a_to_b, &[ident("md.aapl", 1)]).unwrap();
        b.add_peer(b_to_a, &[]).unwrap();

        // A announces a new channel after the link is up.
        a.announce(&[ident("md.msft", 1)]).unwrap();

        // B should receive both the initial sync ("md.aapl") and the delta ("md.msft").
        // pump() is destructive, so accumulate across polls.
        let mut received: Vec<ChannelIdentity> = Vec::new();
        poll_until(|| {
            received.extend(b.pump().unwrap().into_iter().map(|(_, id)| id));
            (received.len() >= 2).then_some(())
        });
        received.sort_by(|x, y| x.name.cmp(&y.name));
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].name, "md.aapl");
        assert_eq!(received[1].name, "md.msft");

        // B learned A's advertised stream address from A's heartbeat.
        let a_seen = poll_until(|| b.addr_of(NodeId(1)));
        assert_eq!(a_seen, a_addr);
        assert_eq!(b.live_members(), vec![NodeId(1)]);
    }

    /// Link two dissemination instances over loopback, returning their ends.
    fn link(a: &mut BroadcastDissemination, b: &mut BroadcastDissemination) {
        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = std::thread::spawn(move || listener.accept().unwrap());
        let a_end = TcpTransport::connect(addr).unwrap();
        let b_end = accept.join().unwrap();
        a.add_peer(a_end, &[]).unwrap();
        b.add_peer(b_end, &[]).unwrap();
    }

    /// Relay across two hops: `A — B — C`, with no A–C link. B forwards what it merged, so C
    /// learns a change it was never sent directly — and B does **not** echo it back to A.
    ///
    /// Isolated here rather than at the node level, because a node-level chain closes itself into
    /// a full mesh and then every node is adjacent to every other: the channel would arrive by
    /// join-time anti-entropy on the new link whether or not anything relayed. Relay is what
    /// covers the window before the mesh closes, and any pair that never manages to link.
    #[test]
    fn a_delta_relays_across_a_chain_without_echoing_its_source() {
        let t = Duration::from_secs(60);
        let mut a = BroadcastDissemination::new(NodeId(1), "127.0.0.1:9001".parse().unwrap(), t);
        let mut b = BroadcastDissemination::new(NodeId(2), "127.0.0.1:9002".parse().unwrap(), t);
        let mut c = BroadcastDissemination::new(NodeId(3), "127.0.0.1:9003".parse().unwrap(), t);
        link(&mut a, &mut b);
        link(&mut b, &mut c);

        a.announce(&[ident("md.aapl", 1)]).unwrap();

        // B receives it, tagged with the link it came in on.
        let (from, id) = poll_until(|| b.pump().unwrap().into_iter().next());
        assert_eq!(id.name, "md.aapl");
        b.relay(from, &[id]).unwrap();

        // C learns it two hops from the origin.
        let (_, at_c) = poll_until(|| c.pump().unwrap().into_iter().next());
        assert_eq!(at_c.name, "md.aapl");

        // And A is not sent its own change back.
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            a.pump().unwrap().is_empty(),
            "the relay must skip the link it arrived on"
        );
    }

    /// Two machines claiming one `NodeId` must be **detected**, not tidied away. Dedup exists to
    /// collapse two links to the *same* node; two links to *different* nodes that share an id look
    /// identical to it, and collapsing them would drop connectivity to a real peer in order to
    /// resolve a misconfiguration.
    ///
    /// The advertised control address is what makes the distinction exact rather than heuristic:
    /// one machine advertises one control address, so two of them under one id means two machines.
    #[test]
    fn one_node_id_on_two_machines_is_reported_and_both_links_kept() {
        let t = Duration::from_secs(60);
        let mut hub = BroadcastDissemination::new(NodeId(1), "127.0.0.1:9101".parse().unwrap(), t);
        // Two distinct nodes, both misconfigured as NodeId(7), at different control addresses.
        let mut twin_a =
            BroadcastDissemination::new(NodeId(7), "127.0.0.1:9107".parse().unwrap(), t);
        twin_a.set_self_control_addr("127.0.0.1:9207".parse().unwrap());
        let mut twin_b =
            BroadcastDissemination::new(NodeId(7), "127.0.0.1:9108".parse().unwrap(), t);
        twin_b.set_self_control_addr("127.0.0.1:9208".parse().unwrap());
        link(&mut hub, &mut twin_a);
        link(&mut hub, &mut twin_b);

        // Wait until both links have identified themselves, then dedup.
        let conflicts = poll_until(|| {
            let c = hub.dedup_links();
            (!c.is_empty()).then_some(c)
        });
        assert_eq!(conflicts, vec![NodeId(7)], "the duplicate id is reported");
        assert_eq!(
            hub.peers.len(),
            2,
            "both links survive — they are different machines, not a duplicate link"
        );

        // And it stays reported rather than being silently resolved on a later pass.
        assert_eq!(hub.dedup_links(), vec![NodeId(7)]);
        assert_eq!(hub.peers.len(), 2);
    }

    /// The cosmetic name rides the heartbeat and is readable back, so operator-facing messages can
    /// say `fra-mm-01` instead of a random 64-bit number.
    #[test]
    fn a_peers_name_is_learned_from_its_heartbeat() {
        let t = Duration::from_secs(60);
        let mut a = BroadcastDissemination::new(NodeId(1), "127.0.0.1:9111".parse().unwrap(), t);
        let mut b = BroadcastDissemination::new(NodeId(2), "127.0.0.1:9112".parse().unwrap(), t);
        b.set_self_name("fra-mm-01".to_string());
        link(&mut a, &mut b);
        assert_eq!(poll_until(|| a.name_of(NodeId(2))), "fra-mm-01".to_string());
    }

    /// Spin briefly until `f` yields `Some` (reader threads run asynchronously).
    fn poll_until<R>(mut f: impl FnMut() -> Option<R>) -> R {
        for _ in 0..1000 {
            if let Some(r) = f() {
                return r;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("condition not met within timeout");
    }
}
