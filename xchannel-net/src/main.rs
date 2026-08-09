//! `xchanneld` — the xchannel-net node-manager daemon entry point.
//!
//! Configures from environment, then binds and serves all three planes: the stream
//! (data) plane, the control plane (registry gossip + membership heartbeats), and the
//! client RPC plane, alongside a periodic maintenance loop. See DESIGN.md for the
//! architecture and README.md for current implementation status.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use xchannel_net::NodeConfig;
use xchannel_net::node::{MuxIdle, Node};
use xchannel_net_core::NodeId;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn main() -> std::io::Result<()> {
    // A node's identity is generated once and kept in its data dir; `XCHANNELD_NODE_ID` overrides
    // it for deployments that need deterministic ids. There is deliberately **no default**: the
    // old `1` meant two unconfigured daemons silently shared an identity, and everything —
    // channel ownership, membership, peer links — is keyed on it.
    let configured_id = std::env::var("XCHANNELD_NODE_ID")
        .ok()
        .map(|v| v.parse::<u64>().expect("XCHANNELD_NODE_ID must be a u64"));
    let node_name = std::env::var("XCHANNELD_NODE_NAME").ok();
    let stream_addr: SocketAddr = env_or("XCHANNELD_STREAM_ADDR", "127.0.0.1:7000")
        .parse()
        .expect("XCHANNELD_STREAM_ADDR must be host:port");
    let control_addr: SocketAddr = env_or("XCHANNELD_CONTROL_ADDR", "127.0.0.1:7001")
        .parse()
        .expect("XCHANNELD_CONTROL_ADDR must be host:port");
    // Per-user and persistent by default (`$HOME/.xchannel-net`). Shared with the client through
    // `core::paths`, because `Client::connect_or_spawn` finds the implicit daemon by this path and
    // the two computing it differently would look like "no daemon running".
    let data_dir = match std::env::var_os("XCHANNELD_DATA_DIR") {
        Some(d) => PathBuf::from(d),
        None => xchannel_net_core::paths::default_data_dir()?,
    };
    // Client plane is a Unix domain socket (local-only, permission-gated); defaults under
    // the data dir so the 0700 directory restricts who can reach the daemon.
    let client_path = match std::env::var_os("XCHANNELD_CLIENT_PATH") {
        Some(p) => PathBuf::from(p),
        None => data_dir.join(xchannel_net_core::paths::CLIENT_SOCKET_NAME),
    };

    // Seed peers to exchange registry state with on startup: `XCHANNELD_SEEDS` is a
    // comma-separated list of control-plane `host:port` addresses. Without it a daemon runs
    // standalone (no peers) — the maintenance loop re-dials these to (re)form the mesh.
    let seeds: Vec<SocketAddr> = env_or("XCHANNELD_SEEDS", "")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse()
                .expect("XCHANNELD_SEEDS entries must be host:port")
        })
        .collect();

    // Safety floor for reclaiming a dead owner's channel name (see
    // `Node::force_deregister`). Deliberately generous by default — reclaiming too eagerly
    // can destroy a live channel across a partition, while reclaiming late costs only a wait.
    let reclaim_after = std::env::var("XCHANNELD_RECLAIM_AFTER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(300));

    // Topics promoted onto a thread of their own — rung 2 of doc/TOPICS.md §4.1's promotion
    // path. Comma-separated topic names; empty (the default) leaves everything on the shared duty
    // cycle. Deliberately node config rather than a `TopicOptions` field: spawning a thread is the
    // operator's call, not any client's, and unlike `TopicOptions` this survives a restart (see
    // `NodeConfig::promoted_topics`).
    let promoted_topics: std::collections::HashSet<String> =
        env_or("XCHANNELD_PROMOTED_TOPICS", "")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

    // How the duty cycle — and each promoted topic's own loop — waits when it finds no work.
    // `XCHANNELD_MUX_MAX_PARK_US` caps the park; `0` means never park (keep yielding), for a box
    // where the data plane is worth a core.
    let mux_idle = MuxIdle {
        max_park: std::env::var("XCHANNELD_MUX_MAX_PARK_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
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

    // Install before anything else runs, so a signal during startup is not simply fatal.
    xchannel_net::shutdown::install();

    let client_path = config.client_path.clone();
    let node = Node::new(config);
    let stream_listener = node.bind_stream()?;
    let control_listener = node.bind_control()?;
    let client_listener = node.bind_client()?;
    eprintln!(
        "xchanneld[{}]: node {} ({}), created {}",
        node_id,
        identity.name,
        if identity.generated {
            "generated"
        } else {
            "configured"
        },
        identity.created_at_ms,
    );
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
    Ok(())
}

enum Plane {
    Control(xchannel_net_core::transport::TcpListener),
    Client(xchannel_net_core::transport::UnixListener),
}
