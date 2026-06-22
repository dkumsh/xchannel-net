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
    let client_addr: SocketAddr = env_or("XCHANNELD_CLIENT_ADDR", "127.0.0.1:7002")
        .parse()
        .expect("XCHANNELD_CLIENT_ADDR must be host:port");

    let config = NodeConfig {
        node_id: NodeId(node_id),
        data_dir: PathBuf::from(env_or("XCHANNELD_DATA_DIR", "/tmp/xchanneld")),
        control_addr,
        stream_addr,
        client_addr,
        seeds: vec![],
    };
    std::fs::create_dir_all(&config.data_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config.data_dir, std::fs::Permissions::from_mode(0o700))?;
    }

    let node = Node::new(config);
    let stream_listener = node.bind_stream()?;
    let control_listener = node.bind_control()?;
    let client_listener = node.bind_client()?;
    eprintln!(
        "xchanneld[{}]: stream {} | control {} | client {}",
        node_id,
        stream_listener.local_addr()?,
        control_listener.local_addr()?,
        client_listener.local_addr()?,
    );

    // Security: all planes are unauthenticated plaintext (see SECURITY.md). Warn loudly
    // when any plane is bound off-loopback, where any reachable host can register names,
    // pull any channel's history, and inject registry/membership gossip.
    for (plane, addr) in [
        ("stream", stream_addr),
        ("control", control_addr),
        ("client", client_addr),
    ] {
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
    Client(xchannel_net_core::transport::TcpListener),
}
