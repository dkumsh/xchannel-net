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

use std::collections::{HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use xchannel_net_core::NodeId;
use xchannel_net_core::codec::{decode_control, encode_control};
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
type Hints = Arc<Mutex<VecDeque<(PeerId, NodeId, SocketAddr, SocketAddr)>>>;
type SharedMembership = Arc<Mutex<Membership>>;
/// Dial addresses of outbound peer links currently believed connected (for dedup +
/// reconnection). An outbound peer's reader removes its address here on disconnect.
type Connected = Arc<Mutex<HashSet<SocketAddr>>>;

/// Eager-broadcast dissemination over a set of peer TCP connections.
pub struct BroadcastDissemination {
    /// This node's identity + the stream address it advertises in heartbeats.
    self_node: NodeId,
    self_addr: SocketAddr,
    /// A node is "live" if heard from within this timeout.
    liveness_timeout: Duration,
    /// The control address this node advertises, so peers can dial it back and a node that
    /// learns of us second-hand can form a direct link.
    self_control_addr: SocketAddr,
    /// Send halves of peer connections, each with a stable id (broadcast target for
    /// deltas/heartbeats). Ids are stable across peer removal, unlike positions.
    peers: Vec<(PeerId, TcpTransport)>,
    next_peer_id: PeerId,
    hints: Hints,
    /// Filled by per-peer reader threads; drained by [`pump`](Self::pump).
    inbox: Inbox,
    membership: SharedMembership,
    connected: Connected,
}

impl BroadcastDissemination {
    pub fn new(self_node: NodeId, self_addr: SocketAddr, liveness_timeout: Duration) -> Self {
        Self {
            self_node,
            self_addr,
            self_control_addr: self_addr,
            liveness_timeout,
            peers: Vec::new(),
            next_peer_id: NO_PEER + 1,
            hints: Arc::new(Mutex::new(VecDeque::new())),
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            membership: Arc::new(Mutex::new(Membership::new())),
            connected: Arc::new(Mutex::new(HashSet::new())),
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

    /// Whether an outbound link to `addr` is currently believed connected.
    pub fn is_connected(&self, addr: SocketAddr) -> bool {
        self.connected.lock_safe().contains(&addr)
    }

    /// Spawn a reader thread, send join-time `RegistrySync` + a first `Heartbeat`, and
    /// retain the send half. `addr` is `Some` for outbound links (tracked for reconnection).
    fn adopt(
        &mut self,
        transport: TcpTransport,
        addr: Option<SocketAddr>,
        initial_sync: &[ChannelIdentity],
    ) -> io::Result<()> {
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
            addr,
        );

        let mut send = transport;
        send.send_frame(&encode_control(&ControlMsg::RegistrySync(
            initial_sync.to_vec(),
        )))?;
        send.send_frame(&encode_control(&self.heartbeat()))?;
        // Introduce everyone we already know, so a joiner learns the whole mesh from one link
        // instead of only its seed. Sent once per link, not periodically.
        for (node, addr, control_addr) in self.membership.lock_safe().directory() {
            if node == self.self_node {
                continue;
            }
            send.send_frame(&encode_control(&ControlMsg::PeerHint {
                node,
                addr,
                control_addr,
            }))?;
        }
        self.peers.push((id, send));
        Ok(())
    }

    /// This node's own heartbeat frame.
    fn heartbeat(&self) -> ControlMsg {
        ControlMsg::Heartbeat {
            node: self.self_node,
            addr: self.self_addr,
            control_addr: self.self_control_addr,
        }
    }

    /// Forward queued peer knowledge to every link except the one it came from. Called on the
    /// maintenance tick; a hint is only queued when it taught this node something, so a mesh
    /// with cycles goes quiet once everyone knows everyone.
    pub fn relay_hints(&mut self) {
        let pending: Vec<_> = self.hints.lock_safe().drain(..).collect();
        for (from, node, addr, control_addr) in pending {
            if node == self.self_node {
                continue; // never gossip about ourselves second-hand; our heartbeat says it
            }
            let frame = encode_control(&ControlMsg::PeerHint {
                node,
                addr,
                control_addr,
            });
            self.send_except(from, &frame);
        }
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

    /// Set the control address advertised to peers — used after binding the control listener to
    /// an ephemeral port, so what we gossip is the address that actually accepts connections.
    pub fn set_self_control_addr(&mut self, addr: SocketAddr) {
        self.self_control_addr = addr;
    }

    /// Send a `Heartbeat` (this node + its address) to every peer. The caller drives the
    /// cadence; peers refresh our membership entry on receipt.
    pub fn emit_heartbeat(&mut self) -> io::Result<()> {
        let hb = encode_control(&self.heartbeat());
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

    /// How long since `node` was last heard from; `None` if never. Used to judge whether an
    /// owner is *gone* rather than momentarily silent.
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

    /// Broadcast to every peer except `from`; drops peers whose send fails (disconnected).
    fn send_except(&mut self, from: PeerId, frame: &[u8]) {
        self.peers
            .retain_mut(|(id, p)| *id == from || p.send_frame(frame).is_ok());
    }
}

impl Dissemination for BroadcastDissemination {
    fn announce(&mut self, delta: &[ChannelIdentity]) -> io::Result<()> {
        let frame = encode_control(&ControlMsg::RegistryDelta(delta.to_vec()));
        self.broadcast(&frame);
        Ok(())
    }

    fn relay(&mut self, from: PeerId, delta: &[ChannelIdentity]) -> io::Result<()> {
        let frame = encode_control(&ControlMsg::RegistryDelta(delta.to_vec()));
        self.send_except(from, &frame);
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
    addr: Option<SocketAddr>,
) {
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
                } => {
                    if membership.lock_safe().record(node, addr, control_addr) {
                        hints.lock_safe().push_back((id, node, addr, control_addr));
                    }
                }
                // Hearsay: learn where the node is, but grant it no liveness — we have not heard
                // from it, and `live_members` must keep meaning "reachable by us".
                ControlMsg::PeerHint {
                    node,
                    addr,
                    control_addr,
                } => {
                    if membership.lock_safe().learn(node, addr, control_addr) {
                        hints.lock_safe().push_back((id, node, addr, control_addr));
                    }
                }
                _ => {} // not expected on a peer link
            }
        }
        // Connection dropped: clear outbound tracking so the node reconnects this seed.
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
