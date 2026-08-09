//! `xchanneld` — the xchannel-net node-manager daemon entry point.
//!
//! Binds and serves all three planes: the stream (data) plane, the control plane (registry gossip and
//! membership heartbeats), and the client RPC plane, alongside a periodic maintenance loop. See
//! `doc/DESIGN.md` for the architecture.
//!
//! **This is a crate of its own so that `clap` cannot reach a library.** The node manager is
//! `xchannel-net`, which depends on nothing but `xchannel`; a program that runs it needs an argument
//! parser, and confining that to the program is the difference between a promise and a convention.
//!
//! Every option is available as a flag **and** as an environment variable. The flags exist because a
//! setting discoverable only from a README is not discoverable; the environment variables exist because
//! that is what actually configures a daemon in practice — systemd `Environment=`, docker `-e`,
//! Kubernetes `env:`, and `Client::connect_or_spawn`, which starts this binary with an inherited
//! environment and no argv at all.

use clap::Parser;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use xchannel_net::NodeConfig;
use xchannel_net::node::{MuxIdle, Node};

/// Every environment variable this daemon reads. Used to warn about the ones it *doesn't*.
const KNOWN_ENV: &[&str] = &[
    "XCHANNELD_NODE_ID",
    "XCHANNELD_NODE_NAME",
    "XCHANNELD_DATA_DIR",
    "XCHANNELD_CLIENT_PATH",
    "XCHANNELD_STREAM_ADDR",
    "XCHANNELD_CONTROL_ADDR",
    "XCHANNELD_ADVERTISE_STREAM_ADDR",
    "XCHANNELD_ADVERTISE_CONTROL_ADDR",
    "XCHANNELD_SEEDS",
    "XCHANNELD_RECLAIM_AFTER_MS",
    "XCHANNELD_PROMOTED_TOPICS",
    "XCHANNELD_MUX_MAX_PARK_US",
    // Read by the *client* when it spawns this binary, so it is legitimate to see here.
    "XCHANNELD_BIN",
];

#[derive(Parser, Debug)]
#[command(
    name = "xchanneld",
    about = "xchannel-net node manager: registry, discovery, and single-writer log replication",
    long_about = None,
    version
)]
struct Args {
    /// Stable node identity. Generated once into `<data-dir>/.node_id` if not given — there is
    /// deliberately no default, because a default is a duplicate: two unconfigured daemons would
    /// silently share an identity, and channel ownership, membership and peer links are all keyed on it.
    #[arg(long, env = "XCHANNELD_NODE_ID")]
    node_id: Option<u64>,

    /// Human-readable label, gossiped for display only. Defaults to this host's name. Never a key and
    /// never a tie-break, so a duplicate is confusing rather than incorrect.
    #[arg(long, env = "XCHANNELD_NODE_NAME")]
    node_name: Option<String>,

    /// Channel files, replicas, the node's identity and the client socket. Defaults to
    /// `$HOME/.xchannel-net`. One daemon per directory, enforced by a lock. Must be a local
    /// filesystem: channels are memory-mapped.
    #[arg(long, env = "XCHANNELD_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Client-plane Unix socket. Defaults to `<data-dir>/client.sock`; clients look for it there, so
    /// change both or neither.
    #[arg(long, env = "XCHANNELD_CLIENT_PATH")]
    client_path: Option<PathBuf>,

    /// Stream (data) plane address to **bind**.
    #[arg(long, env = "XCHANNELD_STREAM_ADDR", default_value = "127.0.0.1:7000")]
    stream_addr: SocketAddr,

    /// Control plane address to **bind** — registry gossip and heartbeats.
    #[arg(long, env = "XCHANNELD_CONTROL_ADDR", default_value = "127.0.0.1:7001")]
    control_addr: SocketAddr,

    /// What to advertise to peers as the stream address, when it must differ from what was bound.
    #[arg(long, env = "XCHANNELD_ADVERTISE_STREAM_ADDR")]
    advertise_stream_addr: Option<SocketAddr>,

    /// What to advertise to peers as the control address. Needed when binding a wildcard: peers gossip
    /// whatever is advertised, and `0.0.0.0` is not something any of them can dial. Must be **this
    /// instance's own** address — it is what duplicate-`NodeId` detection compares.
    #[arg(long, env = "XCHANNELD_ADVERTISE_CONTROL_ADDR")]
    advertise_control_addr: Option<SocketAddr>,

    /// Peer control addresses to form the mesh with. Repeat the flag, or give one comma-separated list.
    /// Without any, the daemon runs standalone until something dials it.
    ///
    /// Taken as raw strings rather than parsed addresses so that an **empty** value means "none".
    /// `XCHANNELD_SEEDS=""` is what a script produces when its seed list happens to be empty, and a typed
    /// list would try to parse the empty string as an address and refuse to start — which is exactly what
    /// the cross-process tests caught the moment this daemon moved to `clap`.
    #[arg(long, env = "XCHANNELD_SEEDS", value_delimiter = ',')]
    seeds: Vec<String>,

    /// How long an owner must have been unreachable *from this node* before an operator may reclaim its
    /// channel name. Generous on purpose: reclaiming too eagerly can destroy a live channel across a
    /// partition, while reclaiming late costs only a wait.
    #[arg(long, env = "XCHANNELD_RECLAIM_AFTER_MS", default_value = "300000")]
    reclaim_after_ms: u64,

    /// Topics given a merge thread of their own instead of the shared duty cycle. Repeat the flag, or
    /// give one comma-separated list. Node config rather than a topic option on purpose: spawning a
    /// thread is the operator's call, not any client's, and this survives a restart.
    ///
    /// Empty entries are dropped, for the same reason as `--seeds`: an unset-but-present variable must
    /// mean "none", not "a topic whose name is the empty string".
    #[arg(long, env = "XCHANNELD_PROMOTED_TOPICS", value_delimiter = ',')]
    promoted_topics: Vec<String>,

    /// Cap on how long an idle duty cycle parks, in microseconds. `0` never parks, for a box where the
    /// data plane is worth a core.
    #[arg(long, env = "XCHANNELD_MUX_MAX_PARK_US")]
    mux_max_park_us: Option<u64>,
}

/// Parse a comma-separated address list, treating blank entries as absent. Returns the offending entry on
/// failure so the caller can name the flag *and* the variable in one message.
fn parse_addrs(raw: &[String]) -> Result<Vec<SocketAddr>, String> {
    raw.iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<SocketAddr>().map_err(|_| s.to_string()))
        .collect()
}

/// Warn about `XCHANNELD_*` variables this daemon does not read.
///
/// Neither flags nor `clap`'s environment support can catch this: a misspelled *flag* is an error, but a
/// misspelled *variable* is simply absent, and the daemon starts with a default while the operator
/// believes they configured something. `XCHANNELD_SEEDZ=10.0.0.5:7001` gets you a standalone node and
/// not a word of complaint. Since environment is how a daemon is configured in practice, that is the
/// case worth a warning.
fn warn_unknown_env() {
    for (key, _) in std::env::vars() {
        if key.starts_with("XCHANNELD_") && !KNOWN_ENV.contains(&key.as_str()) {
            eprintln!(
                "xchanneld: WARNING: {key} is set but not recognised — it is being ignored. Run \
                 `xchanneld --help` for the options this daemon reads."
            );
        }
    }
}

fn main() -> std::io::Result<()> {
    // **First thing, before any other work.** A signal arriving in the first few milliseconds should not
    // be a hard kill just because the handler was not installed yet. (A hard kill *there* is safe —
    // nothing is bound and nothing is committed — so the only cost was peers waiting out the liveness
    // timeout for a node that had barely started. But a claim in a comment should be true.)
    xchannel_net::shutdown::install();

    let args = Args::parse();
    warn_unknown_env();

    // Per-user and persistent by default (`$HOME/.xchannel-net`). Shared with the client through
    // `core::paths`, because `Client::connect_or_spawn` finds the implicit daemon by this path and the
    // two computing it differently would look like "no daemon running".
    let data_dir = match args.data_dir {
        Some(d) => d,
        None => xchannel_net_core::paths::default_data_dir()?,
    };
    // The client plane is a Unix socket (local-only, permission-gated); under the data dir by default, so
    // the `0700` directory decides who can drive the daemon.
    let client_path = args
        .client_path
        .unwrap_or_else(|| data_dir.join(xchannel_net_core::paths::CLIENT_SOCKET_NAME));

    let configured_id = args.node_id;
    let node_name = args.node_name;
    let stream_addr = args.stream_addr;
    let control_addr = args.control_addr;
    let advertise_stream_addr = args.advertise_stream_addr;
    let advertise_control_addr = args.advertise_control_addr;
    let seeds = parse_addrs(&args.seeds).map_err(|bad| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("--seeds / XCHANNELD_SEEDS: {bad:?} is not a host:port address"),
        )
    })?;
    let reclaim_after = Duration::from_millis(args.reclaim_after_ms);
    let promoted_topics: HashSet<String> = args
        .promoted_topics
        .iter()
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(String::from)
        .collect();
    // How the duty cycle — and each promoted topic's own loop — waits when it finds no work.
    let mux_idle = MuxIdle {
        max_park: args
            .mux_max_park_us
            .map_or(MuxIdle::default().max_park, Duration::from_micros),
        ..MuxIdle::default()
    };

    // The data dir must exist before the identity is resolved: a first-ever start *writes*
    // `.node_id` into it.
    std::fs::create_dir_all(&data_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))?;
    }
    // Channels on disk but no identity file means the id was removed while its data was kept.
    // That is not the same as a fresh node: the channels will be re-registered under a *new* owner
    // with a later timestamp, peers will keep the earlier registration, and those channels end up
    // owned by an id that never returns — frozen until an operator reclaims the names. Warn before
    // generating, because after this point it has already happened.
    let had_channels = std::fs::read_dir(&data_dir)
        .map(|rd| {
            rd.flatten()
                .any(|e| e.path().is_dir() && !e.file_name().to_string_lossy().starts_with('.'))
        })
        .unwrap_or(false);
    if had_channels
        && configured_id.is_none()
        && !xchannel_net::node_identity::is_persisted(&data_dir)
    {
        eprintln!(
            "xchanneld: WARNING: {} holds channels but no {} — this node is about to take a NEW \
             identity, and peers that remember the old owner will keep those channels registered \
             to it, leaving them frozen. If this is a fresh node, clear the data dir; if it is the \
             same node, restore its identity file.",
            data_dir.display(),
            xchannel_net::node_identity::NODE_ID_FILE,
        );
    }

    let identity = xchannel_net::node_identity::resolve(&data_dir, configured_id, node_name)
        .expect("resolve this node's identity");
    let node_id = identity.id.0;

    let config = NodeConfig {
        node_id: identity.id,
        data_dir,
        control_addr,
        stream_addr,
        advertise_control_addr,
        advertise_stream_addr,
        client_path,
        seeds,
        reclaim_after,
        promoted_topics,
        mux_idle,
        node_name: identity.name.clone(),
        id_generated: identity.generated,
    };

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
    // A configured id is config, not state: nothing is persisted, so there is no creation time to
    // report and printing the `0` that stands in for one only invites the question.
    eprintln!(
        "xchanneld[{}]: node {} ({})",
        node_id,
        identity.name,
        if identity.generated {
            format!("generated, created at {} ms", identity.created_at_ms)
        } else {
            "configured via XCHANNELD_NODE_ID".to_string()
        },
    );
    // Show both when they differ, since which one peers receive is the thing that matters.
    let shown = |bound: SocketAddr, advertised: Option<SocketAddr>| match advertised {
        Some(a) if a != bound => format!("{bound} (advertised {a})"),
        _ => bound.to_string(),
    };
    eprintln!(
        "xchanneld[{}]: stream {} | control {} | client {}",
        node_id,
        shown(stream_listener.local_addr()?, advertise_stream_addr),
        shown(control_listener.local_addr()?, advertise_control_addr),
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
        // A wildcard bind is a perfectly good way to *listen* and a useless thing to *advertise*,
        // and this node advertises exactly what it bound. Peers gossip the address onwards, so one
        // wildcard-bound node teaches the whole mesh an address nobody can dial. Everything then
        // depends on this node dialling out first — and duplicate-id detection, which distinguishes
        // a twin from a self-link by comparing advertised control addresses, degrades too, because
        // two wildcard-bound machines advertise the *same* address.
        let advertised = match plane {
            "stream" => advertise_stream_addr,
            _ => advertise_control_addr,
        };
        // An advertised wildcard is worse than no advertised address: it silences this warning while
        // restoring exactly the defect the setting exists to cure, so it is warned about on its own
        // terms. The value has to be *per instance* — two nodes advertising one address are
        // indistinguishable to duplicate detection, which compares advertised addresses.
        if advertised.is_some_and(|a| a.ip().is_unspecified()) {
            eprintln!(
                "xchanneld[{node_id}]: WARNING: XCHANNELD_ADVERTISE_{}_ADDR is a wildcard address, \
                 which no peer can dial and which every node using it advertises identically — so two \
                 nodes sharing a NodeId cannot be told apart and a cloned image never stands down. Set \
                 it to this instance's own routable address.",
                plane.to_uppercase(),
            );
        }
        if addr.ip().is_unspecified() && advertised.is_none() {
            eprintln!(
                "xchanneld[{node_id}]: WARNING: {plane} plane bound to the wildcard address {addr} \
                 and nothing else was given to advertise, so that is what peers will be told. They \
                 cannot dial it back, so links form only in the direction this node opens them; and \
                 because every wildcard-bound node advertises the *same* address, two nodes sharing a \
                 NodeId become indistinguishable — their links are collapsed as duplicates instead of \
                 reported, so a cloned image never stands down. Set XCHANNELD_ADVERTISE_{}_ADDR to an \
                 address peers can reach.",
                plane.to_uppercase(),
            );
        }
    }

    // Restart = reconstruct (DESIGN.md §5.2, doc/RESTART.md): re-host topics and re-register
    // origins found on disk, so a restarted daemon resumes without waiting for a client to
    // re-declare.
    //
    // **Before anything serves or gossips**, because until it returns this node's registry is
    // empty and it hosts no topics. A client answered in that window is told the channel does not
    // exist and a peer is handed an empty anti-entropy snapshot — wrong answers, not slow ones,
    // and the client's would send it off to create a channel that is already on disk. Binding
    // first and reconstructing second means an early client blocks in `accept` until the daemon
    // can answer properly; the listeners are already bound, so `connect_or_spawn`'s
    // single-instance arbitration still resolves immediately. Reconstructing before
    // `connect_seeds` also makes the *first* snapshot we send a peer already complete.
    let rebuilt = node.reconstruct_from_disk();
    if rebuilt != Default::default() {
        eprintln!(
            "xchanneld[{node_id}]: reconstructed {} topic(s), {} origin(s), {} skipped",
            rebuilt.topics, rebuilt.origins, rebuilt.skipped
        );
    }

    // Serve *before* dialling. `connect_seeds` is serial with a 1 s timeout per address, so a
    // cold start against a seed list containing unreachable members stalled here for seconds —
    // measured at 25 s for 25 blackholed seeds — with the listeners already bound. A peer's TCP
    // connect therefore succeeded, it recorded us as connected, and then waited: the node looked
    // ready and served nothing. The maintenance loop dials anyway, so nothing is lost by letting
    // it do the work.
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
    {
        // **The duty cycle** (doc/TOPICS.md §4.1): one thread polling every replication source,
        // replication sink and mux as peer poll-items — except topics promoted onto their own
        // thread, which it skips. It backs off only when a whole cycle found nothing to do, so a
        // producing member or a streaming subscriber is never waiting on a clock.
        let node = node.clone();
        std::thread::spawn(move || node.run_duty_cycle(mux_idle));
    }
    {
        let node = node.clone();
        std::thread::spawn(move || {
            let _ = node.serve_stream(stream_listener);
        });
    }

    // Wait for SIGTERM/SIGINT. The planes keep running on their own threads until the process
    // exits; there is nothing to unwind, because a hard kill is safe (see `shutdown`).
    xchannel_net::shutdown::wait(Duration::from_millis(100));
    eprintln!("xchanneld[{node_id}]: shutting down");
    node.shutdown();
    if xchannel_net::shutdown::restart_wanted() {
        // Non-zero so a supervisor restarts us: this stop exists *to be* restarted.
        std::process::exit(3);
    }
    Ok(())
}

enum Plane {
    Control(xchannel_net_core::transport::TcpListener),
    Client(xchannel_net_core::transport::UnixListener),
}
