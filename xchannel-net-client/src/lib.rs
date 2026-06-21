//! `xchannel-net-client` — what writer and reader clients link against.
//!
//! Clients never talk to remote nodes directly; they talk to their *local* node
//! manager, which handles registration, discovery, and replication. The client then
//! reads/writes a purely local xchannel — the master (if it owns the channel) or a
//! replica the manager keeps synchronized.

use std::io;
use xchannel_net_core::identity::ChannelName;

/// How much of a channel's timeline a subscriber wants to see, expressed in xchannel's
/// own terms. The manager always replicates *full history* into the local replica
/// (per design); this only selects where the returned `Reader` starts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubscribeMode {
    /// Start at the replica's tail — only records arriving after subscription.
    Live,
    /// Start from the earliest record in the replica.
    LateJoin,
}

/// Behavior when subscribing to a channel the network doesn't know about yet.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WaitPolicy {
    /// Block until some node registers the channel.
    Block,
    /// Poll periodically until it appears.
    Poll { interval: std::time::Duration },
    /// Fail immediately if unknown.
    FailFast,
}

/// Handle to the local node manager.
pub struct Client {
    _private: (),
}

impl Client {
    /// Connect to the local node manager's control endpoint.
    pub fn connect(_control_addr: std::net::SocketAddr) -> io::Result<Self> {
        unimplemented!("open control transport to the local manager")
    }

    /// Create a channel owned by this node and register it network-wide, returning the
    /// local `xchannel::Writer` for the origin.
    ///
    /// The manager owns *placement* (the file lives under the node's `data_dir` so it can
    /// be served and rediscovered on restart — DESIGN §5.2). The caller owns *shape*: the
    /// manager seeds `WriterBuilder::new(<resolved path>)` and hands it to `configure`, so
    /// every builder option is available with no client-side duplication. `WriterBuilder`
    /// has no path setter, so `configure` cannot override placement.
    ///
    /// Do **not** set `base_record_index` in `configure` — it is manager-owned and stays 0
    /// (genesis) for a new origin; it exists for replicas (see [`ReplicationSink`] usage).
    ///
    /// The manager registers the channel by reading `region_size`/`mtu` from the resulting
    /// channel header, so replicas can be built compatibly.
    ///
    /// ```ignore
    /// let w = client.create_channel("md.aapl", |b| {
    ///     b.region_size(1 << 20).file_roll_size(1 << 30).keep_files(8)
    /// })?;
    /// ```
    ///
    /// [`ReplicationSink`]: xchannel_net_core::replication::ReplicationSink
    pub fn create_channel(
        &self,
        _name: &str,
        _configure: impl FnOnce(xchannel::WriterBuilder) -> xchannel::WriterBuilder,
    ) -> io::Result<xchannel::Writer> {
        unimplemented!(
            "resolve path under data_dir; new(path) -> configure -> build; register; return Writer"
        )
    }

    /// Register an already-created local channel as network-visible. Full-control path for
    /// a caller who built the `Writer` itself with plain `xchannel::WriterBuilder`; the
    /// manager reads the channel header for geometry and registers it.
    ///
    /// Caveat: unless `path` is under the node's `data_dir`, the manager cannot rediscover
    /// the channel on restart (DESIGN §5.2) — keep it under `data_dir`, or re-register
    /// after a restart.
    pub fn register_existing(&self, _name: &str, _path: &std::path::Path) -> io::Result<()> {
        unimplemented!("send ControlMsg::Register (geometry from header); handle RegisterRejected")
    }

    /// Subscribe to a channel by name and obtain a local `xchannel::Reader` over the
    /// (replicated) channel once it is available per `wait`.
    pub fn subscribe(
        &self,
        _name: &str,
        _mode: SubscribeMode,
        _wait: WaitPolicy,
    ) -> io::Result<xchannel::Reader> {
        unimplemented!("resolve via discovery; ensure replica synced; return local Reader")
    }
}

/// Re-exported so callers can name channels without depending on core directly.
pub type Name = ChannelName;
