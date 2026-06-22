//! `xchanneld` — the xchannel-net node-manager daemon entry point.
//!
//! Configures from environment, then binds and serves all three planes: the stream
//! (data) plane, the control plane (registry gossip + membership heartbeats), and the
//! client RPC plane, alongside a periodic maintenance loop. See DESIGN.md for the
//! architecture and README.md for current implementation status.

use std::net::SocketAddr;
use std::path::PathBuf;
use xchannel_net::NodeConfig;
use xchannel_net::node::Node;
use xchannel_net_core::NodeId;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn main() -> std::io::Result<()> {
    let node_id = env_or("XCHANNELD_NODE_ID", "1")
        .parse::<u64>()
        .expect("XCHANNELD_NODE_ID must be a u64");
    let stream_addr: SocketAddr = env_or("XCHANNELD_STREAM_ADDR", "127.0.0.1:7000")
        .parse()
        .expect("XCHANNELD_STREAM_ADDR must be host:port");
    let control_addr: SocketAddr = env_or("XCHANNELD_CONTROL_ADDR", "127.0.0.1:7001")
        .parse()
        .expect("XCHANNELD_CONTROL_ADDR must be host:port");
    let data_dir = PathBuf::from(env_or("XCHANNELD_DATA_DIR", "/tmp/xchanneld"));
    // Client plane is a Unix domain socket (local-only, permission-gated); defaults under
    // the data dir so the 0700 directory restricts who can reach the daemon.
    let client_path = PathBuf::from(env_or(
        "XCHANNELD_CLIENT_PATH",
        &data_dir.join("client.sock").to_string_lossy(),
    ));

    let config = NodeConfig {
        node_id: NodeId(node_id),
        data_dir,
        control_addr,
        stream_addr,
        client_path,
        seeds: vec![],
    };
    std::fs::create_dir_all(&config.data_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config.data_dir, std::fs::Permissions::from_mode(0o700))?;
    }

    // Single-daemon-per-data_dir guard: hold an exclusive advisory lock on `<data_dir>/.lock`
    // for the life of the process. Two daemons sharing a data dir would corrupt each other's
    // channel files; this fails the second one fast with a clear message. The leading dot
    // keeps the lock file from colliding with any channel name (those can't start with `.`),
    // and the OS releases the flock automatically on exit — no stale lock to clean up.
    let _data_dir_lock = {
        let lock_path = config.data_dir.join(".lock");
        let lock_file = std::fs::File::create(&lock_path)?;
        match lock_file.try_lock() {
            Ok(()) => lock_file,
            Err(std::fs::TryLockError::WouldBlock) => {
                eprintln!(
                    "xchanneld[{node_id}]: another daemon already holds data_dir {} \
                     (lock {}) — exiting",
                    config.data_dir.display(),
                    lock_path.display(),
                );
                std::process::exit(1);
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
    };

    let client_path = config.client_path.clone();
    let node = Node::new(config);
    let stream_listener = node.bind_stream()?;
    let control_listener = node.bind_control()?;
    let client_listener = node.bind_client()?;
    eprintln!(
        "xchanneld[{}]: stream {} | control {} | client {}",
        node_id,
        stream_listener.local_addr()?,
        control_listener.local_addr()?,
        client_path.display(),
    );

    // Security: the network planes are unauthenticated plaintext (see SECURITY.md). Warn
    // loudly when stream/control are bound off-loopback, where any reachable host can
    // register names, pull any channel's history, and inject registry/membership gossip.
    // (The client plane is a permission-gated local Unix socket, not a network port.)
    for (plane, addr) in [("stream", stream_addr), ("control", control_addr)] {
        if !addr.ip().is_loopback() {
            eprintln!(
                "xchanneld[{node_id}]: WARNING: {plane} plane bound to non-loopback {addr} \
                 — all planes are UNAUTHENTICATED plaintext; any reachable host can \
                 register, subscribe, and gossip. Bind only to trusted networks. See \
                 SECURITY.md."
            );
        }
    }

    node.connect_seeds();
    for (node, run) in [
        (node.clone(), Plane::Control(control_listener)),
        (node.clone(), Plane::Client(client_listener)),
    ] {
        std::thread::spawn(move || match run {
            Plane::Control(l) => node.serve_control(l),
            Plane::Client(l) => node.serve_client(l),
        });
    }
    {
        let node = node.clone();
        std::thread::spawn(move || {
            let _ = node.run_maintenance(std::time::Duration::from_millis(500));
        });
    }
    node.serve_stream(stream_listener)
}

enum Plane {
    Control(xchannel_net_core::transport::TcpListener),
    Client(xchannel_net_core::transport::UnixListener),
}
