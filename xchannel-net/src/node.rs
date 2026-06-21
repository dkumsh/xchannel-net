//! The node manager (`xchanneld`) — hosting, serving, discovery, and subscribing.
//!
//! A [`Node`] ties together the pieces:
//! * **hosting** — [`host_channel`](Node::host_channel) creates an origin under `data_dir`,
//!   registers it, and announces it;
//! * **stream plane** — [`serve_stream`](Node::serve_stream) dispatches inbound
//!   subscriptions to per-connection `StreamServer` threads;
//! * **control plane** — [`serve_control`](Node::serve_control) /
//!   [`connect_control_peer`](Node::connect_control_peer) adopt peer links into
//!   [`BroadcastDissemination`], and [`run_maintenance`](Node::run_maintenance) emits
//!   heartbeats and merges gossiped identities into the [`Registry`];
//! * **subscribing** — [`subscribe`](Node::subscribe) resolves a channel via the registry +
//!   membership, connects to its owner's stream address, and builds a local replica.
//!
//! `Node` is cheaply cloneable (shared interior), so its loops run on their own threads.

use crate::NodeConfig;
use crate::broadcast::BroadcastDissemination;
use crate::registry::Registry;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use xchannel::{Writer, WriterBuilder};
use xchannel_net_core::RecordIndex;
use xchannel_net_core::dissemination::Dissemination;
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::stream::{self, ChannelSource, accept_subscription};
use xchannel_net_core::transport::{Listener, TcpListener, TcpTransport};

/// A node not heard from within this is dropped from the live set.
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(10);

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
pub struct Node {
    config: Arc<NodeConfig>,
    /// Channels this node hosts (is the origin for): name → where + geometry to serve.
    hosted: Arc<Mutex<HashMap<String, ChannelSource>>>,
    /// Network-wide channel directory (CRDT), converged via dissemination.
    registry: Arc<Mutex<Registry>>,
    dissemination: Arc<Mutex<BroadcastDissemination>>,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        let dissemination =
            BroadcastDissemination::new(config.node_id, config.stream_addr, LIVENESS_TIMEOUT);
        Self {
            hosted: Arc::new(Mutex::new(HashMap::new())),
            registry: Arc::new(Mutex::new(Registry::new())),
            dissemination: Arc::new(Mutex::new(dissemination)),
            config: Arc::new(config),
        }
    }

    // ---------------- hosting ----------------

    /// Host a new origin channel under `data_dir`, register it, announce it, and return
    /// its local `Writer`. Placement + network geometry (`region_size`/`mtu`) are the
    /// daemon's (applied after `configure`, which owns the rest); `base_record_index` is
    /// forced to 0 (genesis).
    pub fn host_channel(
        &self,
        name: &str,
        region_size: u32,
        mtu: u32,
        configure: impl FnOnce(WriterBuilder) -> WriterBuilder,
    ) -> io::Result<Writer> {
        let path = self.channel_path(name)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let writer = configure(WriterBuilder::new(&path))
            .region_size(region_size as usize)
            .mtu(mtu as u64)
            .base_record_index(0)
            .build()?;

        let identity = ChannelIdentity {
            name: name.to_string(),
            owner: self.config.node_id,
            region_size,
            mtu,
            earliest_index: RecordIndex(0),
            registered_at_nanos: now_nanos(),
        };
        self.registry.lock().unwrap().merge(identity.clone());
        self.dissemination
            .lock()
            .unwrap()
            .announce(std::slice::from_ref(&identity))?;
        self.hosted.lock().unwrap().insert(
            name.to_string(),
            ChannelSource {
                path,
                region_size,
                mtu,
            },
        );
        Ok(writer)
    }

    fn channel_path(&self, name: &str) -> io::Result<PathBuf> {
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "channel name must be a non-empty, path-safe identifier (no separators)",
            ));
        }
        Ok(self.config.data_dir.join(name))
    }

    // ---------------- stream plane (serve) ----------------

    /// Bind the stream-plane listener and advertise its real address in heartbeats.
    pub fn bind_stream(&self) -> io::Result<TcpListener> {
        let listener = TcpListener::bind(self.config.stream_addr)?;
        self.dissemination
            .lock()
            .unwrap()
            .set_self_addr(listener.local_addr()?);
        Ok(listener)
    }

    /// Accept stream connections forever, dispatching each to its own thread serving one
    /// subscription against this node's hosted channels.
    pub fn serve_stream(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let hosted = Arc::clone(&self.hosted);
            std::thread::spawn(move || {
                let resolve = |name: &str| hosted.lock().unwrap().get(name).cloned();
                if let Ok(mut server) = accept_subscription(conn, resolve) {
                    let _ = server.run();
                }
            });
        }
    }

    // ---------------- control plane (gossip) ----------------

    /// Bind the control-plane listener.
    pub fn bind_control(&self) -> io::Result<TcpListener> {
        TcpListener::bind(self.config.control_addr)
    }

    /// Accept peer control connections forever, adopting each as a dissemination peer
    /// (which sends our current registry as join-time anti-entropy + a heartbeat).
    pub fn serve_control(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let snapshot = self.registry_snapshot();
            let _ = self.dissemination.lock().unwrap().add_peer(conn, &snapshot);
        }
    }

    /// Connect to a peer's control address and adopt it as a dissemination peer.
    pub fn connect_control_peer(&self, addr: SocketAddr) -> io::Result<()> {
        let conn = TcpTransport::connect(addr)?;
        let snapshot = self.registry_snapshot();
        self.dissemination.lock().unwrap().add_peer(conn, &snapshot)
    }

    /// Connect to all configured seed peers (best-effort; unreachable seeds are skipped).
    pub fn connect_seeds(&self) {
        let seeds = self.config.seeds.clone();
        for addr in seeds {
            let _ = self.connect_control_peer(addr);
        }
    }

    fn registry_snapshot(&self) -> Vec<ChannelIdentity> {
        self.registry.lock().unwrap().iter().cloned().collect()
    }

    /// Periodic maintenance: emit a heartbeat and merge any gossiped identities into the
    /// registry. Runs forever; the caller drives it on its own thread.
    pub fn run_maintenance(&self, interval: Duration) -> io::Result<()> {
        loop {
            let pumped = {
                let mut d = self.dissemination.lock().unwrap();
                let _ = d.emit_heartbeat();
                d.pump()?
            };
            if !pumped.is_empty() {
                let mut reg = self.registry.lock().unwrap();
                for id in pumped {
                    reg.merge(id);
                }
            }
            std::thread::sleep(interval);
        }
    }

    // ---------------- subscribing ----------------

    /// Subscribe to a channel by name: resolve it via the registry + membership, connect
    /// to its owner's stream address, and build a local replica under `data_dir` in a
    /// background thread. Returns a [`Subscription`] tracking sync progress and the replica
    /// path (a local reader client opens that path).
    ///
    /// `resolve_timeout`: `None` blocks until the channel is known and its owner reachable;
    /// `Some(d)` errors after `d`. v1 always starts a *fresh* replica (`from = 0`).
    pub fn subscribe(
        &self,
        name: &str,
        resolve_timeout: Option<Duration>,
    ) -> io::Result<Subscription> {
        let (_identity, owner_addr) = self.resolve(name, resolve_timeout)?;
        let replica_path = self.channel_path(name)?;
        if let Some(parent) = replica_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = TcpTransport::connect(owner_addr)?;
        let mut client = stream::subscribe(conn, name, RecordIndex(0), &replica_path)?;
        let synced = Arc::new(AtomicU64::new(client.expected_index().0));
        let synced_thread = Arc::clone(&synced);
        let handle = std::thread::spawn(move || {
            // Apply records until the connection drops, publishing progress.
            while client.recv_one().is_ok() {
                synced_thread.store(client.expected_index().0, Ordering::Relaxed);
            }
        });

        Ok(Subscription {
            replica_path,
            synced,
            _handle: handle,
        })
    }

    /// Block (until `timeout`) until `name` is in the registry and its owner's address is
    /// known via membership.
    fn resolve(
        &self,
        name: &str,
        timeout: Option<Duration>,
    ) -> io::Result<(ChannelIdentity, SocketAddr)> {
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            let identity = self.registry.lock().unwrap().get(name).cloned();
            if let Some(identity) = identity
                && let Some(addr) = self.dissemination.lock().unwrap().addr_of(identity.owner)
            {
                return Ok((identity, addr));
            }
            if let Some(dl) = deadline
                && Instant::now() >= dl
            {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("channel '{name}' not resolvable within timeout"),
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Handle to an in-progress subscription replicating a remote channel locally.
pub struct Subscription {
    replica_path: PathBuf,
    synced: Arc<AtomicU64>,
    _handle: JoinHandle<()>,
}

impl Subscription {
    /// Local path of the replica; a reader client opens this (in its own process).
    pub fn replica_path(&self) -> &Path {
        &self.replica_path
    }

    /// Absolute index up to which the replica has been synced (records applied = this
    /// minus the start index). Grows as the stream is consumed.
    pub fn synced_index(&self) -> u64 {
        self.synced.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xchannel::{ReaderBuilder, ReaderMode};
    use xchannel_net_core::NodeId;
    use xchannel_net_core::transport::TcpTransport;

    fn temp_dir(name: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("xchnet-node-{name}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn config(id: u64, data_dir: PathBuf) -> NodeConfig {
        NodeConfig {
            node_id: NodeId(id),
            data_dir,
            control_addr: "127.0.0.1:0".parse().unwrap(),
            stream_addr: "127.0.0.1:0".parse().unwrap(),
            seeds: vec![],
        }
    }

    /// Start a node: bind both listeners and spawn serve_stream / serve_control /
    /// maintenance. Returns the node and its (stream_addr, control_addr).
    fn start(id: u64, dir: &str) -> (Node, SocketAddr, SocketAddr) {
        let node = Node::new(config(id, temp_dir(dir)));
        let stream_l = node.bind_stream().unwrap();
        let control_l = node.bind_control().unwrap();
        let stream_addr = stream_l.local_addr().unwrap();
        let control_addr = control_l.local_addr().unwrap();
        for (node, run) in [
            (node.clone(), Run::Stream(stream_l)),
            (node.clone(), Run::Control(control_l)),
        ] {
            std::thread::spawn(move || match run {
                Run::Stream(l) => {
                    let _ = node.serve_stream(l);
                }
                Run::Control(l) => {
                    let _ = node.serve_control(l);
                }
            });
        }
        let m = node.clone();
        std::thread::spawn(move || {
            let _ = m.run_maintenance(Duration::from_millis(5));
        });
        (node, stream_addr, control_addr)
    }

    enum Run {
        Stream(TcpListener),
        Control(TcpListener),
    }

    fn poll_until<R>(mut f: impl FnMut() -> Option<R>) -> R {
        for _ in 0..2000 {
            if let Some(r) = f() {
                return r;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("condition not met within timeout");
    }

    #[test]
    fn two_nodes_discover_and_replicate() {
        let (a, _a_stream, a_control) = start(1, "two-a");
        let (b, _b_stream, _b_control) = start(2, "two-b");
        let n = 40u64;

        // A hosts a channel and writes records, then drops the writer (so A's
        // ReplicationSource, opened when B connects, isn't concurrent with the writer in
        // this single process). Hosting before B links means B learns the channel via
        // A's join-time RegistrySync.
        {
            let mut w = a.host_channel("md.aapl", 1 << 20, 0, |x| x).unwrap();
            for i in 0..n {
                let p = format!("tick-{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }

        // B links to A's control plane → B receives A's registry (incl. md.aapl) and
        // learns A's stream address via heartbeat.
        b.connect_control_peer(a_control).unwrap();

        // B subscribes: resolves md.aapl → A's stream addr → builds a replica.
        let sub = b
            .subscribe("md.aapl", Some(Duration::from_secs(5)))
            .unwrap();

        // The replica syncs all records purely through the two managers.
        poll_until(|| (sub.synced_index() == n).then_some(()));
        assert_eq!(sub.synced_index(), n);
    }

    #[test]
    fn daemon_serves_a_hosted_channel_over_tcp() {
        let node = Node::new(config(1, temp_dir("serve")));
        let n = 25u64;
        let listener = node.bind_stream().unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = node.clone();
        std::thread::spawn(move || {
            let _ = serving.serve_stream(listener);
        });

        {
            let mut w = node.host_channel("md.aapl", 1 << 20, 0, |b| b).unwrap();
            for i in 0..n {
                let p = format!("v{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }

        let replica = temp_dir("serve-replica").join("chan");
        let conn = TcpTransport::connect(addr).unwrap();
        let mut client = stream::subscribe(conn, "md.aapl", RecordIndex(0), &replica).unwrap();
        for _ in 0..n {
            client.recv_one().unwrap();
        }
        assert_eq!(client.expected_index(), RecordIndex(n));
        drop(client);

        let mut r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let mut seen = 0u64;
        while let Some(m) = r.try_read().unwrap() {
            assert_eq!(m.header().user_meta_u64, seen);
            assert_eq!(m.payload(), format!("v{seen}").as_bytes());
            seen += 1;
        }
        assert_eq!(seen, n);
    }

    #[test]
    fn rejects_unsafe_channel_name() {
        let node = Node::new(config(1, temp_dir("unsafe")));
        let err = node
            .host_channel("a/b", 1 << 20, 0, |b| b)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
