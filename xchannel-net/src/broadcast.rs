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
use xchannel_net_core::dissemination::Dissemination;
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::membership::Membership;
use xchannel_net_core::transport::{TcpTransport, Transport};
use xchannel_net_core::wire::ControlMsg;

type Inbox = Arc<Mutex<VecDeque<ChannelIdentity>>>;
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
    /// Send halves of peer connections (broadcast target for deltas/heartbeats).
    peers: Vec<TcpTransport>,
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
            liveness_timeout,
            peers: Vec::new(),
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
        self.connected.lock().unwrap().insert(addr);
        let r = self.adopt(transport, Some(addr), initial_sync);
        if r.is_err() {
            self.connected.lock().unwrap().remove(&addr);
        }
        r
    }

    /// Whether an outbound link to `addr` is currently believed connected.
    pub fn is_connected(&self, addr: SocketAddr) -> bool {
        self.connected.lock().unwrap().contains(&addr)
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
        spawn_reader(
            reader,
            Arc::clone(&self.inbox),
            Arc::clone(&self.membership),
            Arc::clone(&self.connected),
            addr,
        );

        let mut send = transport;
        send.send_frame(&encode_control(&ControlMsg::RegistrySync(
            initial_sync.to_vec(),
        )))?;
        send.send_frame(&encode_control(&ControlMsg::Heartbeat {
            node: self.self_node,
            addr: self.self_addr,
        }))?;
        self.peers.push(send);
        Ok(())
    }

    /// Send a `Heartbeat` (this node + its address) to every peer. The caller drives the
    /// cadence; peers refresh our membership entry on receipt.
    pub fn emit_heartbeat(&mut self) -> io::Result<()> {
        let hb = encode_control(&ControlMsg::Heartbeat {
            node: self.self_node,
            addr: self.self_addr,
        });
        self.broadcast(&hb);
        Ok(())
    }

    /// Resolve a peer's current stream address (last heartbeat wins).
    pub fn addr_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.membership.lock().unwrap().addr_of(node)
    }

    /// Set the stream address advertised in heartbeats — used after binding the stream
    /// listener to an ephemeral port (`:0`), so peers learn the real address.
    pub fn set_self_addr(&mut self, addr: SocketAddr) {
        self.self_addr = addr;
    }

    /// Best-effort broadcast to all peers; drops peers whose send fails (disconnected).
    fn broadcast(&mut self, frame: &[u8]) {
        self.peers.retain_mut(|p| p.send_frame(frame).is_ok());
    }
}

impl Dissemination for BroadcastDissemination {
    fn announce(&mut self, delta: &[ChannelIdentity]) -> io::Result<()> {
        let frame = encode_control(&ControlMsg::RegistryDelta(delta.to_vec()));
        self.broadcast(&frame);
        Ok(())
    }

    fn pump(&mut self) -> io::Result<Vec<ChannelIdentity>> {
        let mut q = self.inbox.lock().unwrap();
        Ok(q.drain(..).collect())
    }

    fn live_members(&self) -> Vec<NodeId> {
        self.membership
            .lock()
            .unwrap()
            .live_members(self.liveness_timeout)
    }
}

/// Per-peer reader loop: decode inbound control frames until the connection drops.
/// `RegistryDelta`/`RegistrySync` identities go to the inbox for the node to merge;
/// `Heartbeat`s refresh membership. Client→manager frames (`Register`, …) are not expected
/// on a peer link and are ignored.
fn spawn_reader(
    mut reader: TcpTransport,
    inbox: Inbox,
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
                    inbox.lock().unwrap().extend(ids);
                }
                ControlMsg::Heartbeat { node, addr } => {
                    membership.lock().unwrap().record(node, addr);
                }
                _ => {} // not expected on a peer link
            }
        }
        // Connection dropped: clear outbound tracking so the node reconnects this seed.
        if let Some(addr) = addr {
            connected.lock().unwrap().remove(&addr);
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
            received.extend(b.pump().unwrap());
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
