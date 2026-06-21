//! The node manager (`xchanneld`) — hosting local channels and serving subscriptions.
//!
//! This is the data-plane half of the daemon: it hosts channels under the node's
//! `data_dir`, and its stream-listener accept loop dispatches each inbound subscription to
//! its own thread running a [`StreamServer`](xchannel_net_core::stream::StreamServer).
//!
//! The control plane (CRDT registry gossip, client RPC, and registry-driven *subscriber*
//! routing — resolving an owner's address to pull a replica) lands in a later increment
//! alongside dissemination; this module covers "serve channels this node hosts".

use crate::NodeConfig;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use xchannel::{Writer, WriterBuilder};
use xchannel_net_core::stream::{ChannelSource, accept_subscription};
use xchannel_net_core::transport::{Listener, TcpListener};

/// A node manager. Cheap to clone (shared interior), so the accept loop can run on its own
/// thread while the rest of the process keeps a handle.
#[derive(Clone)]
pub struct Node {
    config: Arc<NodeConfig>,
    /// Channels this node hosts (is the origin for): name → where + geometry to serve.
    hosted: Arc<Mutex<HashMap<String, ChannelSource>>>,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config: Arc::new(config),
            hosted: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Host a new origin channel under `data_dir` and return its local `Writer`.
    ///
    /// Placement is the daemon's (path under `data_dir`, for serving + restart
    /// rediscovery). The network geometry — `region_size`/`mtu`, which subscribers need to
    /// build compatible replicas — is also the daemon's, so it is applied *after* the
    /// caller's `configure` closure and wins; `base_record_index` is forced to 0 (genesis).
    /// `configure` owns the rest (`file_roll_size`, `keep_files`, …).
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

    /// Resolve a channel name to a file path under `data_dir`. Names must be a single,
    /// path-safe component (no separators) — channels live as `data_dir/<name>[.N]`.
    fn channel_path(&self, name: &str) -> io::Result<PathBuf> {
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "channel name must be a non-empty, path-safe identifier (no separators)",
            ));
        }
        Ok(self.config.data_dir.join(name))
    }

    /// Bind the stream-plane listener (`config.stream_addr`).
    pub fn stream_listener(&self) -> io::Result<TcpListener> {
        TcpListener::bind(self.config.stream_addr)
    }

    /// Accept stream connections forever, dispatching each to its own thread that serves a
    /// single subscription against this node's hosted channels. Returns only if the
    /// listener itself errors; per-connection failures (unknown channel, gap, disconnect)
    /// are isolated to their thread.
    pub fn serve_stream(&self, mut listener: TcpListener) -> io::Result<()> {
        loop {
            let conn = listener.accept()?;
            let hosted = Arc::clone(&self.hosted);
            std::thread::spawn(move || {
                let resolve = |name: &str| hosted.lock().unwrap().get(name).cloned();
                // Handshake (Subscribe → Ack/Gap), then stream until the peer disconnects.
                if let Ok(mut server) = accept_subscription(conn, resolve) {
                    let _ = server.run();
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xchannel::{ReaderBuilder, ReaderMode};
    use xchannel_net_core::stream;
    use xchannel_net_core::transport::TcpTransport;
    use xchannel_net_core::{NodeId, RecordIndex};

    fn temp_dir(name: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("xchnet-node-{name}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn test_config(data_dir: PathBuf) -> NodeConfig {
        NodeConfig {
            node_id: NodeId(1),
            data_dir,
            control_addr: "127.0.0.1:0".parse().unwrap(),
            stream_addr: "127.0.0.1:0".parse().unwrap(),
            seeds: vec![],
        }
    }

    #[test]
    fn daemon_serves_a_hosted_channel_over_tcp() {
        let node = Node::new(test_config(temp_dir("serve")));
        let n = 25u64;

        // Start the accept/dispatch loop on its own thread.
        let listener = node.stream_listener().unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = node.clone();
        std::thread::spawn(move || {
            let _ = serving.serve_stream(listener);
        });

        // Host a channel, write records, then drop the writer *before* anyone connects —
        // so the daemon's ReplicationSource (opened on connect) is not concurrent with the
        // writer in this one process. (In deployment, writer and daemon are separate
        // processes, so this constraint is a test artifact only.)
        {
            let mut w = node.host_channel("md.aapl", 1 << 20, 0, |b| b).unwrap();
            for i in 0..n {
                let p = format!("v{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }

        // Subscribe as a client and build a replica from the daemon's stream.
        let replica = temp_dir("serve-replica").join("chan");
        let conn = TcpTransport::connect(addr).unwrap();
        let mut client = stream::subscribe(conn, "md.aapl", RecordIndex(0), &replica).unwrap();
        for _ in 0..n {
            client.recv_one().unwrap();
        }
        assert_eq!(client.expected_index(), RecordIndex(n));
        drop(client);

        // The replica, built entirely through the daemon over TCP, is record-identical.
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
        let node = Node::new(test_config(temp_dir("unsafe")));
        let err = node
            .host_channel("a/b", 1 << 20, 0, |b| b)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
