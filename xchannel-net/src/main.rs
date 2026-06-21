//! `xchanneld` — the xchannel-net node-manager daemon entry point.
//!
//! Minimal wiring: configure from environment, then run the stream-plane accept/dispatch
//! loop. The control plane (registry gossip, client RPC, subscriber-side routing) is not
//! wired here yet — see DESIGN.md.

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

    let config = NodeConfig {
        node_id: NodeId(node_id),
        data_dir: PathBuf::from(env_or("XCHANNELD_DATA_DIR", "/tmp/xchanneld")),
        control_addr,
        stream_addr,
        seeds: vec![],
    };
    std::fs::create_dir_all(&config.data_dir)?;

    let node = Node::new(config);
    let listener = node.stream_listener()?;
    eprintln!(
        "xchanneld[{}]: serving stream plane on {}",
        node_id,
        listener.local_addr()?
    );
    node.serve_stream(listener)
}
