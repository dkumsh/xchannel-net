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
use crate::util::MutexExt;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use xchannel::{Writer, WriterBuilder};
use xchannel_net_core::RecordIndex;
use xchannel_net_core::codec::{decode_client_request, encode_client_reply};
use xchannel_net_core::dissemination::Dissemination;
use xchannel_net_core::identity::ChannelIdentity;
use xchannel_net_core::stream::{self, ChannelSource, accept_subscription};
use xchannel_net_core::transport::{Listener, TcpListener, TcpTransport, Transport};
use xchannel_net_core::wire::{ChannelOptions, ClientReply, ClientRequest};

/// A node not heard from within this is dropped from the live set.
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on concurrent inbound stream + client connections (thread-exhaustion guard). Peer
/// control links are not capped — they come from configured/trusted seeds.
const MAX_CONNECTIONS: usize = 4096;

/// Create a directory (and parents) and restrict it to the owner (`0700` on Unix), so
/// other local users can't read channel files beneath it.
fn ensure_private_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Validate a channel name before it is used as a filesystem path component. Allowlist
/// `[A-Za-z0-9._-]`, length 1..=200, and **no leading dot** — which rejects path traversal
/// (`/`, `\`, `..`), the current dir (`.`), and collisions with the internal `.replicas`
/// subtree, none of which can appear.
fn validate_channel_name(name: &str) -> io::Result<()> {
    let valid = (1..=200).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "channel name must be 1..=200 chars of [A-Za-z0-9._-] with no leading dot",
        ))
    }
}

/// RAII token counting one live connection against [`MAX_CONNECTIONS`].
struct ConnGuard(Arc<AtomicUsize>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

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
    /// Actual bound stream address (set by `bind_stream`), used to resolve self-owned
    /// channels (a node never receives its own heartbeat into membership).
    bound_stream_addr: Arc<Mutex<Option<SocketAddr>>>,
    /// Live replica subscriptions this node maintains for clients, keyed by channel name.
    subscriptions: Arc<Mutex<HashMap<String, Subscription>>>,
    /// Count of live inbound stream/client connections (capped at [`MAX_CONNECTIONS`]).
    conns: Arc<AtomicUsize>,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        let dissemination =
            BroadcastDissemination::new(config.node_id, config.stream_addr, LIVENESS_TIMEOUT);
        Self {
            hosted: Arc::new(Mutex::new(HashMap::new())),
            registry: Arc::new(Mutex::new(Registry::new())),
            dissemination: Arc::new(Mutex::new(dissemination)),
            bound_stream_addr: Arc::new(Mutex::new(None)),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            conns: Arc::new(AtomicUsize::new(0)),
            config: Arc::new(config),
        }
    }

    /// Acquire a connection slot, or `None` if at [`MAX_CONNECTIONS`].
    fn acquire_conn(&self) -> Option<ConnGuard> {
        if self.conns.fetch_add(1, Ordering::Relaxed) >= MAX_CONNECTIONS {
            self.conns.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(ConnGuard(Arc::clone(&self.conns)))
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
            ensure_private_dir(parent)?;
        }
        let writer = configure(WriterBuilder::new(&path))
            .region_size(region_size as usize)
            .mtu(mtu as u64)
            .base_record_index(0)
            .build()?;
        self.register_origin(name, path, region_size, mtu)?;
        Ok(writer)
    }

    /// Create + register an origin on behalf of a client (cross-process): precreate the
    /// channel under `data_dir` with `options` (no live writer kept — the client opens the
    /// single `Writer` itself), register + announce it, and return the path.
    pub fn create_for_client(&self, name: &str, options: ChannelOptions) -> io::Result<PathBuf> {
        let path = self.channel_path(name)?;
        if let Some(parent) = path.parent() {
            ensure_private_dir(parent)?;
        }
        let mut builder = WriterBuilder::new(&path)
            .region_size(options.region_size as usize)
            .mtu(options.mtu as u64)
            .file_roll_size(options.file_roll_size)
            .base_record_index(0);
        if options.keep_files > 0 {
            builder = builder.keep_files(options.keep_files as u64);
        }
        builder.precreate()?; // file + header exist; no writer retained
        self.register_origin(name, path.clone(), options.region_size, options.mtu)?;
        Ok(path)
    }

    /// Register a locally-hosted origin in the registry, announce it to peers, and record
    /// it in the hosted map (so `serve_stream` can resolve it).
    fn register_origin(
        &self,
        name: &str,
        path: PathBuf,
        region_size: u32,
        mtu: u32,
    ) -> io::Result<()> {
        let identity = ChannelIdentity {
            name: name.to_string(),
            owner: self.config.node_id,
            region_size,
            mtu,
            earliest_index: RecordIndex(0),
            registered_at_nanos: now_nanos(),
        };
        self.registry.lock_safe().merge(identity.clone());
        self.dissemination
            .lock()
            .unwrap()
            .announce(std::slice::from_ref(&identity))?;
        self.hosted.lock_safe().insert(
            name.to_string(),
            ChannelSource {
                path,
                region_size,
                mtu,
            },
        );
        Ok(())
    }

    /// Path of an **origin** channel this node hosts: `data_dir/<name>`.
    fn channel_path(&self, name: &str) -> io::Result<PathBuf> {
        validate_channel_name(name)?;
        Ok(self.config.data_dir.join(name))
    }

    /// Path of a **replica** this node maintains: `data_dir/.replicas/<name>`. Kept in a
    /// separate subtree so a replica never collides with a same-named origin (notably for a
    /// node subscribing to a channel it also hosts).
    fn replica_path(&self, name: &str) -> io::Result<PathBuf> {
        self.channel_path(name)?; // validate the name
        Ok(self.config.data_dir.join(".replicas").join(name))
    }

    // ---------------- stream plane (serve) ----------------

    /// Bind the stream-plane listener and advertise its real address in heartbeats.
    pub fn bind_stream(&self) -> io::Result<TcpListener> {
        let listener = TcpListener::bind(self.config.stream_addr)?;
        let addr = listener.local_addr()?;
        self.dissemination.lock_safe().set_self_addr(addr);
        *self.bound_stream_addr.lock_safe() = Some(addr);
        Ok(listener)
    }

    /// Accept stream connections forever, dispatching each to its own thread serving one
    /// subscription against this node's hosted channels.
    pub fn serve_stream(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let Some(guard) = self.acquire_conn() else {
                continue; // at capacity — drop the connection
            };
            let hosted = Arc::clone(&self.hosted);
            std::thread::spawn(move || {
                let _guard = guard; // released when this connection's thread ends
                let resolve = |name: &str| hosted.lock_safe().get(name).cloned();
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
            let _ = self.dissemination.lock_safe().add_peer(conn, &snapshot);
        }
    }

    /// Connect to a peer's control address and adopt it as an outbound dissemination peer
    /// (deduped: a no-op if already connected). The connect happens outside the
    /// dissemination lock so a slow dial doesn't stall heartbeats/announces.
    pub fn connect_control_peer(&self, addr: SocketAddr) -> io::Result<()> {
        if self.dissemination.lock_safe().is_connected(addr) {
            return Ok(());
        }
        let conn = TcpTransport::connect(addr)?;
        let snapshot = self.registry_snapshot();
        self.dissemination
            .lock()
            .unwrap()
            .add_outbound_peer(conn, addr, &snapshot)
    }

    /// (Re)connect to any configured seed peer not currently linked. Called at startup and
    /// each maintenance tick, so a dropped seed link is re-established. Uses a bounded dial
    /// timeout so a down seed doesn't stall the loop.
    pub fn connect_seeds(&self) {
        for addr in self.config.seeds.clone() {
            if self.dissemination.lock_safe().is_connected(addr) {
                continue;
            }
            if let Ok(conn) = TcpTransport::connect_timeout(&addr, Duration::from_secs(1)) {
                let snapshot = self.registry_snapshot();
                let _ = self
                    .dissemination
                    .lock()
                    .unwrap()
                    .add_outbound_peer(conn, addr, &snapshot);
            }
        }
    }

    fn registry_snapshot(&self) -> Vec<ChannelIdentity> {
        self.registry.lock_safe().iter().cloned().collect()
    }

    /// Periodic maintenance: reconnect dropped seeds, emit a heartbeat, and merge gossiped
    /// identities into the registry. Runs forever; the caller drives it on its own thread.
    pub fn run_maintenance(&self, interval: Duration) -> io::Result<()> {
        loop {
            self.connect_seeds();
            let pumped = {
                let mut d = self.dissemination.lock_safe();
                let _ = d.emit_heartbeat();
                d.pump()?
            };
            if !pumped.is_empty() {
                let mut reg = self.registry.lock_safe();
                for id in pumped {
                    reg.merge(id);
                }
            }
            std::thread::sleep(interval);
        }
    }

    // ---------------- client plane (local client RPC) ----------------

    /// Bind the client-plane listener (local client↔daemon RPC).
    pub fn bind_client(&self) -> io::Result<TcpListener> {
        TcpListener::bind(self.config.client_addr)
    }

    /// Accept local client connections forever, handling each on its own thread.
    pub fn serve_client(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let Some(guard) = self.acquire_conn() else {
                continue; // at capacity — drop the connection
            };
            let node = self.clone();
            std::thread::spawn(move || {
                let _guard = guard;
                node.handle_client(conn);
            });
        }
    }

    /// Serve a client connection: one request → one reply, until it disconnects.
    fn handle_client(&self, mut conn: TcpTransport) {
        while let Ok(bytes) = conn.recv_frame() {
            let reply = match decode_client_request(&bytes) {
                Ok(req) => self.handle_request(req),
                Err(e) => ClientReply::Error {
                    message: e.to_string(),
                },
            };
            if conn.send_frame(&encode_client_reply(&reply)).is_err() {
                break;
            }
        }
    }

    fn handle_request(&self, req: ClientRequest) -> ClientReply {
        match req {
            ClientRequest::Create { name, options } => match self.create_for_client(&name, options)
            {
                Ok(path) => ClientReply::Created {
                    path: path.to_string_lossy().into_owned(),
                },
                Err(e) => ClientReply::Error {
                    message: e.to_string(),
                },
            },
            ClientRequest::Subscribe { name, wait_ms } => {
                // Idempotent: reuse a live subscription for this channel.
                if let Some(existing) = self.subscriptions.lock_safe().get(&name)
                    && existing.is_active()
                {
                    return ClientReply::Subscribed {
                        replica_path: existing.replica_path().to_string_lossy().into_owned(),
                    };
                }
                let wait = (wait_ms != 0).then(|| Duration::from_millis(wait_ms));
                match self.subscribe(&name, wait) {
                    Ok(sub) => {
                        let replica_path = sub.replica_path().to_string_lossy().into_owned();
                        self.subscriptions.lock_safe().insert(name, sub);
                        ClientReply::Subscribed { replica_path }
                    }
                    Err(e) => ClientReply::Error {
                        message: e.to_string(),
                    },
                }
            }
        }
    }

    /// Sync progress of a subscription this node maintains (for clients), if any.
    pub fn subscription_synced(&self, name: &str) -> Option<u64> {
        self.subscriptions
            .lock()
            .unwrap()
            .get(name)
            .map(|s| s.synced_index())
    }

    // ---------------- subscribing ----------------

    /// Subscribe to a channel by name and maintain a local replica under `data_dir` in a
    /// background thread. Returns a [`Subscription`] tracking sync progress + the replica
    /// path (a local reader client opens that path, in its own process).
    ///
    /// The background loop is **self-healing**: it resolves the owner, **resumes** from the
    /// replica's current head (so a reconnect or restart never re-pulls history it already
    /// has), streams until the connection drops, then **reconnects** — until [`stop`](
    /// Subscription::stop). `resolve_timeout` bounds only the *initial* resolution (so the
    /// RPC fails fast if the channel is unknown): `None` blocks, `Some(d)` errors after `d`.
    pub fn subscribe(
        &self,
        name: &str,
        resolve_timeout: Option<Duration>,
    ) -> io::Result<Subscription> {
        // Fail fast if the channel can't be resolved within the timeout.
        self.resolve(name, resolve_timeout)?;
        let replica_path = self.replica_path(name)?;
        if let Some(parent) = replica_path.parent() {
            ensure_private_dir(parent)?;
        }

        let stopped = Arc::new(AtomicBool::new(false));
        let synced = Arc::new(AtomicU64::new(0));
        let shutdown: Arc<Mutex<Option<TcpTransport>>> = Arc::new(Mutex::new(None));

        let node = self.clone();
        let (name_t, path_t, stopped_t, synced_t, shutdown_t) = (
            name.to_string(),
            replica_path.clone(),
            Arc::clone(&stopped),
            Arc::clone(&synced),
            Arc::clone(&shutdown),
        );
        let handle = std::thread::spawn(move || {
            node.run_subscription(name_t, path_t, stopped_t, synced_t, shutdown_t)
        });

        Ok(Subscription {
            replica_path,
            synced,
            stopped,
            shutdown,
            handle: Some(handle),
        })
    }

    /// The self-healing subscription loop: resolve → resume from replica head → stream →
    /// reconnect, until stopped. Failures back off and retry; `stop` interrupts a blocked
    /// read by shutting down the live socket.
    fn run_subscription(
        &self,
        name: String,
        replica_path: PathBuf,
        stopped: Arc<AtomicBool>,
        synced: Arc<AtomicU64>,
        shutdown: Arc<Mutex<Option<TcpTransport>>>,
    ) {
        const BACKOFF: Duration = Duration::from_millis(100);
        while !stopped.load(Ordering::Relaxed) {
            // Re-resolve each attempt (owner address may have changed); short timeout so we
            // keep re-checking `stopped`.
            let Ok((id, addr)) = self.resolve(&name, Some(Duration::from_millis(200))) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            // Resume from the replica's current head (0 if it doesn't exist yet).
            let from = self
                .replica_head(&replica_path, id.region_size)
                .unwrap_or(RecordIndex(0));
            synced.store(from.0, Ordering::Relaxed);

            let Ok(conn) = TcpTransport::connect(addr) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            let shutdown_handle = conn.try_clone().ok();
            let Ok(mut client) = stream::subscribe(conn, &name, from, &replica_path) else {
                std::thread::sleep(BACKOFF);
                continue;
            };
            *shutdown.lock_safe() = shutdown_handle;

            // Apply records until the connection drops or we're stopped.
            loop {
                if stopped.load(Ordering::Relaxed) {
                    return;
                }
                match client.recv_one() {
                    Ok(()) => synced.store(client.expected_index().0, Ordering::Relaxed),
                    Err(_) => break, // disconnected → reconnect (resuming from the new head)
                }
            }
            *shutdown.lock_safe() = None;
            if stopped.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(BACKOFF);
        }
    }

    /// Absolute head index of an existing replica (so a subscription resumes from there),
    /// or 0 if the replica doesn't exist yet. Reopens the channel briefly to read its head;
    /// `region_size` must match the on-disk geometry (taken from the registry identity).
    fn replica_head(&self, replica_path: &Path, region_size: u32) -> io::Result<RecordIndex> {
        if !replica_path.exists() {
            return Ok(RecordIndex(0));
        }
        let writer = WriterBuilder::new(replica_path)
            .region_size(region_size as usize)
            .build()?;
        Ok(RecordIndex(writer.next_record_index()))
    }

    /// Stop and forget a subscription this node maintains for a client. Returns whether one
    /// was found.
    pub fn unsubscribe(&self, name: &str) -> bool {
        if let Some(sub) = self.subscriptions.lock_safe().remove(name) {
            sub.stop();
            true
        } else {
            false
        }
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
            let identity = self.registry.lock_safe().get(name).cloned();
            if let Some(identity) = identity {
                // Self-owned channels resolve to our own (bound) stream address — a node
                // never records its own heartbeat into membership.
                let addr = if identity.owner == self.config.node_id {
                    *self.bound_stream_addr.lock_safe()
                } else {
                    self.dissemination.lock_safe().addr_of(identity.owner)
                };
                if let Some(addr) = addr {
                    return Ok((identity, addr));
                }
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

/// Handle to a self-healing subscription replicating a remote channel locally. Dropping it
/// stops the background loop.
pub struct Subscription {
    replica_path: PathBuf,
    synced: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
    /// The currently-live connection (if any), so [`stop`](Self::stop) can interrupt a
    /// blocked read by shutting it down.
    shutdown: Arc<Mutex<Option<TcpTransport>>>,
    handle: Option<JoinHandle<()>>,
}

impl Subscription {
    /// Local path of the replica; a reader client opens this (in its own process).
    pub fn replica_path(&self) -> &Path {
        &self.replica_path
    }

    /// Absolute index the replica has been synced to (the head). Grows as records arrive.
    pub fn synced_index(&self) -> u64 {
        self.synced.load(Ordering::Relaxed)
    }

    /// Whether the background loop is still running (not stopped).
    pub fn is_active(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }

    /// Stop the background loop: set the flag and shut down the live socket so a blocked
    /// read returns. Idempotent.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(conn) = self.shutdown.lock_safe().as_ref() {
            let _ = conn.shutdown();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.stop();
        // Best-effort join so the replica writer is released before we return.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
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
            client_addr: "127.0.0.1:0".parse().unwrap(),
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
    fn subscription_stops_cleanly() {
        let (a, _a_stream, a_control) = start(11, "stop-a");
        let (b, _b_stream, _b_control) = start(12, "stop-b");
        let n = 10u64;
        {
            let mut w = a.host_channel("c", 1 << 20, 0, |x| x).unwrap();
            for i in 0..n {
                let p = format!("r{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }
        b.connect_control_peer(a_control).unwrap();

        let sub = b.subscribe("c", Some(Duration::from_secs(5))).unwrap();
        poll_until(|| (sub.synced_index() == n).then_some(()));
        assert!(sub.is_active());

        sub.stop();
        assert!(!sub.is_active());
        assert_eq!(sub.synced_index(), n, "sync frozen after stop");
        // Dropping `sub` joins the background thread; must not hang.
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
    fn rejects_unsafe_channel_names() {
        // Path traversal, current-dir, separators, leading dot (incl. the .replicas
        // subtree), empty, and over-length names are all rejected before touching the FS.
        let long = "x".repeat(201);
        for bad in [
            "a/b",
            "..",
            ".",
            "../etc",
            "a\\b",
            ".hidden",
            ".replicas",
            "",
            long.as_str(),
        ] {
            assert_eq!(
                validate_channel_name(bad).unwrap_err().kind(),
                io::ErrorKind::InvalidInput,
                "should reject {bad:?}"
            );
        }
        // Reasonable names pass.
        for ok in ["md.aapl", "feed-1", "a_b.c", "X"] {
            validate_channel_name(ok).unwrap();
        }
    }
}
