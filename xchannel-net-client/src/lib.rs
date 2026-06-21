//! `xchannel-net-client` — what writer and reader client *processes* link against.
//!
//! A client never talks to remote nodes; it talks to its **local** `xchanneld` daemon over
//! the client plane, which handles registration, discovery, and replication. The daemon
//! owns placement and replies with a local path; the client opens its own `Writer`/`Reader`
//! on that path (no-custody — the writer writes the mmap directly; the daemon only tails
//! and forwards it).
//!
//! Two ways to reach the daemon:
//! * [`Client::connect`] — an explicit address (run multiple daemons yourself and pick one);
//! * [`Client::connect_or_spawn`] — the well-known default endpoint, auto-starting a daemon
//!   if none is running (single-instance falls out of bind contention).

use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use xchannel::{Reader, ReaderBuilder, ReaderMode, Writer, WriterBuilder};
use xchannel_net_core::codec::{decode_client_reply, encode_client_request};
use xchannel_net_core::transport::{TcpTransport, Transport};
use xchannel_net_core::wire::{ChannelOptions, ClientReply, ClientRequest};

pub use xchannel_net_core::wire::ChannelOptions as Options;

/// Well-known default client-plane endpoint for the implicit single local daemon.
pub const DEFAULT_CLIENT_ADDR: &str = "127.0.0.1:7002";

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
    conn: TcpTransport,
}

impl Client {
    /// Connect to a daemon's client-plane address (explicit; for managed / multi-daemon
    /// setups). Errors if no daemon is listening there.
    pub fn connect(client_addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            conn: TcpTransport::connect(client_addr)?,
        })
    }

    /// Connect to the default local daemon ([`DEFAULT_CLIENT_ADDR`]), auto-starting one if
    /// none is running. The spawned `xchanneld` (located via `$XCHANNELD_BIN` or `PATH`)
    /// uses its own default addresses/data dir; if two clients race, only one daemon wins
    /// the `bind()` and the rest connect to it.
    pub fn connect_or_spawn() -> io::Result<Self> {
        let addr: SocketAddr = DEFAULT_CLIENT_ADDR
            .parse()
            .expect("DEFAULT_CLIENT_ADDR is valid");
        if let Ok(client) = Self::connect(addr) {
            return Ok(client);
        }
        spawn_daemon()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(client) = Self::connect(addr) {
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
            ClientReply::Created { path } => open_writer(&path, options),
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

    /// Subscribe and return a `Reader` over the replica, opened in `mode`.
    pub fn subscribe(
        &mut self,
        name: &str,
        mode: SubscribeMode,
        wait: Option<Duration>,
    ) -> io::Result<Reader> {
        let path = self.subscribe_path(name, wait)?;
        ReaderBuilder::new(path).mode(mode.into()).build()
    }
}

fn open_writer(path: &str, options: &ChannelOptions) -> io::Result<Writer> {
    let mut builder = WriterBuilder::new(path)
        .region_size(options.region_size as usize)
        .mtu(options.mtu as u64)
        .file_roll_size(options.file_roll_size);
    if options.keep_files > 0 {
        builder = builder.keep_files(options.keep_files as u64);
    }
    builder.build()
}

fn spawn_daemon() -> io::Result<()> {
    let bin = std::env::var("XCHANNELD_BIN").unwrap_or_else(|_| "xchanneld".to_string());
    std::process::Command::new(bin)
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
