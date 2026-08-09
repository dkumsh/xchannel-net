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
pub mod node_identity;
pub mod registry;
pub mod shutdown;
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
    /// Safety floor for [`Node::force_deregister`](node::Node::force_deregister): how long an
    /// owner must have been unreachable before another node may retire its name so it can be
    /// reclaimed elsewhere.
    ///
    /// This gates an **operator-invoked** action; nothing reclaims automatically. Owner death
    /// freezing a channel is a locked design decision, and an automatic reclaim would be
    /// failover by another name — worse, under a partition each side would see the other as
    /// dead and could retire names whose owners are alive and still writing, with the
    /// higher-epoch reclaim then winning the merge and destroying a live channel. Requiring a
    /// human to assert "that host is gone" makes that an operator error rather than an
    /// emergent behavior; this threshold is the guard against asserting it too soon.
    pub reclaim_after: std::time::Duration,

    /// Topics promoted off the shared duty cycle onto a **thread of their own** — rung 2 of
    /// `doc/TOPICS.md` §4.1's promotion path. Empty (the default) leaves every topic on the
    /// shared loop.
    ///
    /// §9 left the promotion *trigger* open and offered "operator-configured per topic in
    /// `TopicOptions`" as its default answer. It lives here instead, in node config, for three
    /// reasons — see §9 for the full argument:
    ///
    /// 1. **Authority.** `TopicOptions` is client-supplied. A client that could set
    ///    `dedicated: true` could make the daemon spawn threads, which is a resource lever no
    ///    client should hold on a plane that is trusted rather than authenticated.
    /// 2. **Durability.** `TopicOptions`' policy fields are not persisted, so a promoted topic
    ///    would silently drop back to the shared loop on the next daemon restart — a latency
    ///    regression precisely where it would go unnoticed. Node config *is* durable state by
    ///    design (DESIGN.md §5: "the only durable node-owned state is stable `NodeId` + config").
    /// 3. **Locality.** Promotion describes how *this node* schedules, not what the topic is. A
    ///    topic reclaimed by another owner should not carry the previous owner's core budget.
    pub promoted_topics: std::collections::HashSet<String>,

    /// How the duty cycle — and each promoted topic's own loop — waits when it finds no work.
    pub mux_idle: node::MuxIdle,

    /// Human-readable label for this node, gossiped for display only — never a key, never a
    /// tie-break. It exists because an auto-generated `NodeId` is unreadable, and an operator has
    /// to be able to tell which machine a message is about.
    pub node_name: String,

    /// Whether `node_id` was **generated** by this daemon rather than supplied by the operator.
    /// A generated id may be discarded if it turns out to be a duplicate and nothing references it
    /// yet; a configured one is the operator's to fix.
    pub id_generated: bool,
}
