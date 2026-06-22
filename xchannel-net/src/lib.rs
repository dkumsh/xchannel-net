//! `xchannel-net` — the node manager.
//!
//! One manager runs per node. It owns:
//!
//! * the **registry**: a decentralized last-writer-wins CRDT map
//!   `ChannelName -> ChannelIdentity` (flat global names, first-registrant-wins),
//!   disseminated by eager delta broadcast + join-time anti-entropy (see DESIGN.md §2.1);
//! * the **discovery / creation service** clients call to register or subscribe;
//! * the **replication source/sink** wiring from `xchannel-net-core` plus a concrete
//!   TCP transport.
//!
//! The data plane preserves xchannel's single-writer invariant end-to-end: the owner
//! node holds the only `Writer`; every other node holds read-only replicas.

pub mod broadcast;
pub mod node;
pub mod registry;
mod util;

/// Node manager configuration.
pub struct NodeConfig {
    pub node_id: xchannel_net_core::NodeId,
    /// Directory under which local origin channels and replicas live.
    pub data_dir: std::path::PathBuf,
    /// Control-plane listen address (peer gossip).
    pub control_addr: std::net::SocketAddr,
    /// Stream-plane listen address (serving subscriptions).
    pub stream_addr: std::net::SocketAddr,
    /// Client-plane Unix-domain-socket path (local client↔daemon RPC). Lives under
    /// `data_dir` so the `0700` directory restricts who can reach the daemon.
    pub client_path: std::path::PathBuf,
    /// Seed peers to exchange registry state with on startup.
    pub seeds: Vec<std::net::SocketAddr>,
}
