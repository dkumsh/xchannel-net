//! `xchannel-net-client` — what writer and reader client *processes* link against.
//!
//! A client never talks to remote nodes; it talks to its **local** `xchanneld` daemon over
//! the client plane, which handles registration, discovery, and replication. The daemon
//! owns placement and replies with a local path; the client opens its own `Writer`/`Reader`
//! on that path (no-custody — the writer writes the mmap directly; the daemon only tails
//! and forwards it).
//!
//! Two ways to reach the daemon, both over the local client-plane **Unix domain socket**
//! (so access is gated by filesystem permissions, not an open loopback port):
//! * [`Client::connect`] — an explicit socket path (run multiple daemons yourself and pick one);
//! * [`Client::connect_or_spawn`] — the well-known default socket, auto-starting a daemon
//!   if none is running (single-instance falls out of bind contention on the path).
//!
//! ```no_run
//! use std::time::Duration;
//! use xchannel_net_client::{Client, SubscribeMode};
//! use xchannel_net_core::wire::ChannelOptions;
//!
//! let mut client = Client::connect_or_spawn()?;
//!
//! // Produce: the daemon registers the name, you get the channel's single Writer.
//! let mut w = client.create_channel("md.aapl", &ChannelOptions::default())?;
//! let buf = w.try_reserve(4)?;
//! buf.copy_from_slice(b"tick");
//! w.commit(0, 4, 0)?;
//!
//! // Consume: the daemon locates the owner and keeps a local replica synced; you read it.
//! let mut r = client.subscribe(
//!     "fills.prod.mm",
//!     SubscribeMode::LateJoin,
//!     Some(Duration::from_secs(5)),
//! )?;
//! while let Some(msg) = r.try_read()? {
//!     let _ = msg.payload();
//! }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! `no_run` because it would start a daemon; it is still compiled, so the shape of this
//! example — which the crate README repeats — cannot drift away from the API.

use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use xchannel::{Reader, ReaderBuilder, ReaderMode, Writer, WriterBuilder};
use xchannel_net_core::codec::{decode_change, decode_client_reply, encode_client_request};
use xchannel_net_core::transport::{Transport, UnixTransport};
use xchannel_net_core::wire::{
    ChannelChange, ChannelInfo, ChannelOptions, ClientReply, ClientRequest, DiscoveryCursor,
    TopicOptions,
};

pub use xchannel_net_core::wire::ChannelOptions as Options;
pub use xchannel_net_core::wire::SubscriptionStatus;

/// Well-known default client-plane socket path for the implicit single local daemon. Mirrors
/// the daemon's `XCHANNELD_CLIENT_PATH` default (inside its `$HOME/.xchannel-net` data dir).
pub use xchannel_net_core::paths::default_client_path;

/// Where a subscriber's returned `Reader` starts. The replica always holds full retained
/// history; this only selects the read position.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubscribeMode {
    /// Start at the replica's tail — only records arriving after subscription.
    Live,
    /// Start from the earliest record in the replica.
    LateJoin,
}

impl From<SubscribeMode> for ReaderMode {
    fn from(m: SubscribeMode) -> Self {
        match m {
            SubscribeMode::Live => ReaderMode::Live,
            SubscribeMode::LateJoin => ReaderMode::LateJoin,
        }
    }
}

/// A connection to the local node-manager daemon. Synchronous request/reply; not shared
/// across threads (one in-flight request at a time).
pub struct Client {
    conn: UnixTransport,
}

impl Client {
    /// Connect to a daemon's client-plane socket (explicit; for managed / multi-daemon
    /// setups). Errors if no daemon is listening there.
    pub fn connect<P: AsRef<Path>>(client_path: P) -> io::Result<Self> {
        Ok(Self {
            conn: UnixTransport::connect(client_path)?,
        })
    }

    /// Connect to the default local daemon ([`default_client_path`]), auto-starting one if
    /// none is running. The spawned `xchanneld` (located via `$XCHANNELD_BIN` or beside the
    /// current executable) uses its own default socket/data dir; if two clients race, only
    /// one daemon wins the `bind()` and the rest connect to it.
    pub fn connect_or_spawn() -> io::Result<Self> {
        let path = default_client_path()?;
        if let Ok(client) = Self::connect(&path) {
            return Ok(client);
        }
        spawn_daemon()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(client) = Self::connect(&path) {
                return Ok(client);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    ErrorKind::TimedOut,
                    "spawned xchanneld did not become reachable",
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn request(&mut self, req: &ClientRequest) -> io::Result<ClientReply> {
        self.conn.send_frame(&encode_client_request(req))?;
        decode_client_reply(&self.conn.recv_frame()?)
    }

    /// Create + register an origin channel owned by this node, returning the local `Writer`
    /// for it. The daemon precreates the file under its `data_dir`; the client opens the
    /// single writer with the same `options`.
    pub fn create_channel(&mut self, name: &str, options: &ChannelOptions) -> io::Result<Writer> {
        match self.request(&ClientRequest::Create {
            name: name.to_string(),
            options: *options,
        })? {
            ClientReply::Created { path } => open_writer(&path, name, options),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// Create a topic (multi-producer fan-in) owned by the local node: an ordinary channel
    /// plus a mux that merges its members into it (`doc/TOPICS.md`). A consumer reads the
    /// merged stream by `subscribe`-ing to `name` like any channel.
    pub fn create_topic(&mut self, name: &str, options: &TopicOptions) -> io::Result<()> {
        match self.request(&ClientRequest::CreateTopic {
            name: name.to_string(),
            options: *options,
        })? {
            ClientReply::Created { .. } => Ok(()),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// Create member channel `member` and attach it to `topic`'s mux, returning the member's
    /// single `Writer` (records the producer writes are merged, in arrival order, into the
    /// topic channel). Phase 1: `topic` must be hosted on the local daemon.
    pub fn publish_to_topic(
        &mut self,
        topic: &str,
        member: &str,
        options: &ChannelOptions,
    ) -> io::Result<Writer> {
        match self.request(&ClientRequest::PublishToTopic {
            topic: topic.to_string(),
            member: member.to_string(),
            options: *options,
        })? {
            // The member's own name — this writer is the member channel's, not the topic's.
            ClientReply::Created { path } => open_writer(&path, member, options),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// Subscribe and return the local replica path (the daemon keeps it synced). Use this
    /// when you want to open the reader yourself (custom `ReaderBuilder` options).
    ///
    /// `wait`: `None` blocks until the channel is available; `Some(d)` errors after `d`.
    pub fn subscribe_path(&mut self, name: &str, wait: Option<Duration>) -> io::Result<PathBuf> {
        let wait_ms = wait.map(|d| d.as_millis() as u64).unwrap_or(0);
        match self.request(&ClientRequest::Subscribe {
            name: name.to_string(),
            wait_ms,
        })? {
            ClientReply::Subscribed { replica_path } => Ok(PathBuf::from(replica_path)),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// Withdraw a channel this node owns: tombstone it network-wide and delete its files.
    /// Returns whether a live channel of that name was actually owned here — `false` covers
    /// "already gone" and "owned by another node" alike, since neither is an error.
    ///
    /// Subscribers converge on the tombstone and retire their subscriptions; their replicas
    /// keep the history they already hold, which stays valid, but will never advance again.
    pub fn deregister(&mut self, name: &str) -> io::Result<bool> {
        match self.request(&ClientRequest::Deregister {
            name: name.to_string(),
        })? {
            ClientReply::Deregistered { existed } => Ok(existed),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// List channels whose name starts with `prefix` (empty matches everything), and get the
    /// cursor to follow what changes next.
    ///
    /// Both halves come from one registry lock in the daemon, so there is no window between
    /// them: pass the cursor to [`watch_channels`](Self::watch_channels) and nothing that
    /// happens in between is missed.
    ///
    /// The listing is what the **local** daemon has converged on, not a network-wide truth,
    /// and it reports the current state of each name rather than the history of how it got
    /// there — see `doc/DISCOVERY.md`.
    pub fn list_channels(
        &mut self,
        prefix: &str,
    ) -> io::Result<(Vec<ChannelInfo>, DiscoveryCursor)> {
        match self.request(&ClientRequest::ListChannels {
            prefix: prefix.to_string(),
        })? {
            ClientReply::Channels { channels, cursor } => Ok((channels, cursor)),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// Follow registry changes from `cursor`. The daemon is not involved: the discovery log is
    /// an ordinary xchannel, so watchers cost it nothing and any number of them can read the
    /// same log.
    pub fn watch_channels(&mut self, cursor: &DiscoveryCursor) -> io::Result<ChannelWatch> {
        ChannelWatch::open(cursor)
    }

    /// Retire a channel whose owning node is **gone**, freeing its name to be reclaimed here.
    /// Returns whether a live channel of that name existed to retire.
    ///
    /// The daemon refuses unless the owner has been unreachable past its `reclaim_after`
    /// floor. This is an operator action, not automatic recovery: owner death freezes a
    /// channel by design, and a daemon retiring names on its own would be failover — across a
    /// partition it could retire a channel whose owner is alive and still writing, with the
    /// reclaim then winning the merge. Call it only when the host is genuinely gone.
    ///
    /// After it succeeds, `create_channel` under the same name produces a new incarnation;
    /// subscribers holding replicas of the old one rebuild rather than splicing the two.
    pub fn force_deregister(&mut self, name: &str) -> io::Result<bool> {
        match self.request(&ClientRequest::ForceDeregister {
            name: name.to_string(),
        })? {
            ClientReply::Deregistered { existed } => Ok(existed),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// Health of a channel this node reads — replication progress plus whether the machinery
    /// behind it is working. Errors if the local daemon neither hosts nor subscribes to `name`.
    ///
    /// Use it to tell a **quiet** source from a **broken** one, which `synced` alone cannot:
    /// `owner_live` reports whether the owner's manager is reachable (not whether its
    /// application is still writing — those are separate concerns), and `last_record_at_ms`
    /// is the live staleness signal.
    pub fn subscription_status(&mut self, name: &str) -> io::Result<SubscriptionStatus> {
        match self.request(&ClientRequest::SubscriptionStatus {
            name: name.to_string(),
        })? {
            ClientReply::Status(status) => Ok(status),
            ClientReply::Error { message } => Err(rpc_error(message)),
            _ => Err(unexpected()),
        }
    }

    /// Subscribe and return a `Reader` over the replica, opened in `mode`.
    ///
    /// The daemon builds the replica asynchronously, so the file may not exist the instant
    /// the RPC returns; this retries the open briefly until it appears.
    pub fn subscribe(
        &mut self,
        name: &str,
        mode: SubscribeMode,
        wait: Option<Duration>,
    ) -> io::Result<Reader> {
        let path = self.subscribe_path(name, wait)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match ReaderBuilder::new(&path).mode(mode.into()).build() {
                Ok(reader) => return Ok(reader),
                Err(e) if e.kind() == ErrorKind::NotFound && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// A reader over the daemon's discovery log, positioned at a [`DiscoveryCursor`].
///
/// Changes are delivered as [`ChannelChange`] records. Apply each to your own map and compare
/// `epoch` to tell "same channel" from "this name was reclaimed and is now a different log".
pub struct ChannelWatch {
    reader: Reader,
}

impl ChannelWatch {
    /// Open the discovery log at `cursor`. Errors if the log has been replaced (the daemon
    /// restarted) or has already trimmed past the cursor — both mean "list again".
    pub fn open(cursor: &DiscoveryCursor) -> io::Result<Self> {
        let mut reader = ReaderBuilder::new(&cursor.log_path)
            .mode(ReaderMode::LateJoin)
            .build()?;
        // A daemon restart wipes the log and starts a fresh one, so a cursor from a previous
        // run points into a different log entirely — its indices mean nothing here.
        if reader.generation() != cursor.generation {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "discovery log was replaced (the daemon restarted) — list again",
            ));
        }
        let mut index = reader.base_record_index();
        if index > cursor.from.0 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "discovery log has advanced past this cursor (retains from {index}, \
                     asked for {}) — list again",
                    cursor.from.0
                ),
            ));
        }
        // xchannel has no seek-by-index; skip forward to the cursor.
        while index < cursor.from.0 {
            if reader.try_read()?.is_none() {
                break; // caught up to a head that has since been trimmed of nothing
            }
            index += 1;
        }
        Ok(Self { reader })
    }

    /// The next change, or `None` if none is pending. Non-blocking.
    pub fn try_next(&mut self) -> io::Result<Option<ChannelChange>> {
        let Some(m) = self.reader.try_read()? else {
            return Ok(None);
        };
        let msg_type = m.header().message_type;
        let payload = m.payload().to_vec();
        decode_change(msg_type, &payload).map(Some)
    }
}

/// Open the single `Writer` for a channel the daemon has created.
///
/// `name` is stamped so that **every segment this writer rolls** says which channel it belongs to.
/// It cannot be left to the reopen to carry: xchannel takes `generation` from the on-disk header
/// when it rolls but takes `channel_name` from whoever built the writer, so a writer that omits it
/// silently produces blank-named segments — and the daemon, which identifies channels on disk by
/// that stamp, would then refuse to re-host the channel after retention pruned the original
/// segment. The client holds the writer for every plain origin and topic member, so this is where
/// most of a channel's segments are actually written.
fn open_writer(path: &str, name: &str, options: &ChannelOptions) -> io::Result<Writer> {
    let mut builder = WriterBuilder::new(path)
        .region_size(options.region_size as usize)
        .mtu(options.mtu as u64)
        .file_roll_size(options.file_roll_size)
        .channel_name(name)?;
    if options.keep_files > 0 {
        builder = builder.keep_files(options.keep_files as u64);
    }
    builder.build()
}

/// Resolve the `xchanneld` binary to an **absolute** path — never a bare `PATH` search,
/// which would let an attacker who controls `PATH` get code execution as the client's user.
/// `$XCHANNELD_BIN` (if set) must be absolute; otherwise we look for `xchanneld` beside the
/// current executable (the normal co-install layout).
fn daemon_binary() -> io::Result<PathBuf> {
    if let Ok(p) = std::env::var("XCHANNELD_BIN") {
        let p = PathBuf::from(p);
        if !p.is_absolute() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "XCHANNELD_BIN must be an absolute path",
            ));
        }
        return Ok(p);
    }
    let exe = std::env::current_exe()?;
    let dir = exe.parent().ok_or_else(|| {
        io::Error::new(
            ErrorKind::NotFound,
            "cannot locate current executable's directory",
        )
    })?;
    Ok(dir.join("xchanneld"))
}

fn spawn_daemon() -> io::Result<()> {
    std::process::Command::new(daemon_binary()?)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|_| ())
}

fn rpc_error(message: String) -> io::Error {
    io::Error::other(message)
}

fn unexpected() -> io::Error {
    io::Error::new(ErrorKind::InvalidData, "unexpected reply from daemon")
}
