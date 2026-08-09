//! Cross-process integration: spawn the real `xchanneld` binary, then drive it from this
//! (separate) process via `Client` — create + write a channel, subscribe, and read the
//! replica back. Because client and daemon are distinct processes, the test can both write
//! the origin and read the replica without tripping xchannel's same-process writer+reader
//! rule (the constraint that forces all the in-process tests to be sequential).

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use xchannel_net_client::{Client, SubscribeMode};
use xchannel_net_core::RecordIndex;
use xchannel_net_core::mux::{Provenance, is_control};
use xchannel_net_core::stream;
use xchannel_net_core::transport::TcpTransport;
use xchannel_net_core::wire::{ChannelOptions, TopicOptions};

/// Kills the spawned daemon on drop (even if the test panics).
struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Daemon {
    /// Wait for the daemon to exit of its own accord, up to `timeout`.
    fn wait_for_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.0.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(_) => return None,
            }
        }
        None
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("xchnet-xproc-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Spawn `xchanneld` on ephemeral TCP ports with its client plane at a known socket path
/// under `data_dir`. The client plane is a Unix socket, so we hand the daemon the path
/// rather than discovering a port from its banner.
fn spawn_daemon(data_dir: &Path) -> (Daemon, PathBuf) {
    let client_path = data_dir.join("client.sock");
    let child = Command::new(env!("CARGO_BIN_EXE_xchanneld"))
        .env("XCHANNELD_NODE_ID", "1")
        .env("XCHANNELD_STREAM_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CONTROL_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CLIENT_PATH", &client_path)
        .env("XCHANNELD_DATA_DIR", data_dir)
        .spawn()
        .expect("spawn xchanneld");
    (Daemon(child), client_path)
}

/// Spawn `xchanneld` with a node id, data dir, and seed control addresses, capturing its
/// startup banner to recover the ephemeral control-plane address it bound. A background thread
/// drains the rest of stderr so the daemon never blocks on a full pipe.
fn spawn_daemon_seeded(
    node_id: u64,
    data_dir: &Path,
    seeds: &[SocketAddr],
) -> (Daemon, PathBuf, SocketAddr) {
    let client_path = data_dir.join("client.sock");
    let seeds_str = seeds
        .iter()
        .map(SocketAddr::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut child = Command::new(env!("CARGO_BIN_EXE_xchanneld"))
        .env("XCHANNELD_NODE_ID", node_id.to_string())
        .env("XCHANNELD_STREAM_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CONTROL_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CLIENT_PATH", &client_path)
        .env("XCHANNELD_DATA_DIR", data_dir)
        .env("XCHANNELD_SEEDS", seeds_str)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn xchanneld");
    let mut reader = BufReader::new(child.stderr.take().unwrap());
    // Banner: "xchanneld[N]: stream <a> | control <b> | client <c>".
    let control = loop {
        let mut line = String::new();
        assert!(
            reader.read_line(&mut line).unwrap() > 0,
            "daemon exited before printing its banner"
        );
        if let Some(addr) = line
            .split("control ")
            .nth(1)
            .and_then(|rest| rest.split(" |").next())
            .and_then(|s| s.trim().parse::<SocketAddr>().ok())
        {
            break addr;
        }
    };
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });
    (Daemon(child), client_path, control)
}

/// Spawn a daemon and recover both banner addresses plus its pid, for tests that need to observe
/// the process itself rather than just talk to it.
fn spawn_daemon_observed(data_dir: &Path) -> (Daemon, PathBuf, SocketAddr, u32) {
    let client_path = data_dir.join("client.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_xchanneld"))
        .env("XCHANNELD_NODE_ID", "1")
        .env("XCHANNELD_STREAM_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CONTROL_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CLIENT_PATH", &client_path)
        .env("XCHANNELD_DATA_DIR", data_dir)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn xchanneld");
    let pid = child.id();
    let mut reader = BufReader::new(child.stderr.take().unwrap());
    let stream_addr = loop {
        let mut line = String::new();
        assert!(
            reader.read_line(&mut line).unwrap() > 0,
            "daemon exited before printing its banner"
        );
        if let Some(addr) = line
            .split("stream ")
            .nth(1)
            .and_then(|rest| rest.split(" |").next())
            .and_then(|s| s.trim().parse::<SocketAddr>().ok())
        {
            break addr;
        }
    };
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });
    (Daemon(child), client_path, stream_addr, pid)
}

/// Live thread count of a process, from `/proc/<pid>/status`.
#[cfg(target_os = "linux")]
fn threads_of(pid: u32) -> usize {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).expect("daemon is running");
    status
        .lines()
        .find_map(|l| l.strip_prefix("Threads:"))
        .and_then(|v| v.trim().parse().ok())
        .expect("Threads: line")
}

fn connect_with_retry(path: &Path) -> Client {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(c) = Client::connect(path) {
            return c;
        }
        assert!(Instant::now() < deadline, "daemon never became connectable");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn client_replicates_through_a_spawned_daemon() {
    let data_dir = temp_dir("daemon");
    let (_daemon, client_path) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(&client_path);

    let n = 50u64;

    // Create the channel and write records — the test process is the writer client.
    {
        let mut w = client
            .create_channel("md.aapl", &ChannelOptions::default())
            .unwrap();
        for i in 0..n {
            let payload = format!("rec-{i}").into_bytes();
            let buf = w.try_reserve(payload.len()).unwrap();
            buf.copy_from_slice(&payload);
            w.commit((i % 9) as u16, payload.len() as u32, i).unwrap();
        }
    }

    // Subscribe and read the replica the daemon builds — a different process reading what
    // the daemon writes (allowed) of what we wrote to the origin (also a different process).
    let mut reader = client
        .subscribe(
            "md.aapl",
            SubscribeMode::LateJoin,
            Some(Duration::from_secs(5)),
        )
        .unwrap();

    let mut seen = 0u64;
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen < n && Instant::now() < deadline {
        if let Some(m) = reader
            .read_blocking(Some(Duration::from_millis(200)))
            .unwrap()
        {
            assert_eq!(m.header().message_type, (seen % 9) as u16);
            assert_eq!(m.header().user_meta_u64, seen);
            assert_eq!(m.payload(), format!("rec-{seen}").as_bytes());
            seen += 1;
        }
    }
    assert_eq!(
        seen, n,
        "replica should receive every record through the daemon"
    );
}

/// Reconstruction runs **before any plane serves**, so the *first* answer a restarted daemon gives
/// already reflects what is on disk. `list_channels` is the sharp instrument for this: unlike
/// `subscribe` it has no wait parameter and no retry, so this asserts the contract with zero
/// tolerance — one RPC, and the rebuilt registry must already be in it. A client that got an empty
/// listing here would conclude the channels are gone and go create them, which is a wrong answer
/// rather than a slow one.
///
/// Honest about its reach: this pins the contract, it does not reliably *reproduce* the race it
/// guards. With the old ordering (reconstruct after the serve threads spawn) a data dir this small
/// is rebuilt long before a client can connect, so the window is real but vanishingly narrow here.
/// It has teeth against a regression that makes reconstruction slow or moves it later still.
#[test]
fn a_restarted_daemon_serves_nothing_until_it_has_rebuilt_from_disk() {
    let data_dir = temp_dir("reconstruct-before-serve");
    let opts = ChannelOptions::default();

    // Session 1: one plain origin and one topic with a member — the two reconstruction paths.
    {
        let (daemon1, client_path) = spawn_daemon(&data_dir);
        let mut client = connect_with_retry(&client_path);
        drop(client.create_channel("md.x", &opts).unwrap());
        client
            .create_topic("agg", &TopicOptions::default())
            .unwrap();
        let mut w = client.publish_to_topic("agg", "mem.a", &opts).unwrap();
        let buf = w.try_reserve(2).unwrap();
        buf.copy_from_slice(b"m0");
        w.commit(1, 2, 0).unwrap();
        // Let the mux merge, so the topic has a slot table to be content-sniffed by.
        let mut reader = client
            .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(5)))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut merged = false;
        while !merged && Instant::now() < deadline {
            if let Some(m) = reader
                .read_blocking(Some(Duration::from_millis(200)))
                .unwrap()
                && !is_control(m.header().message_type)
            {
                merged = true;
            }
        }
        assert!(
            merged,
            "session 1 must merge, so a slot table exists on disk"
        );
        drop(daemon1);
    }

    // Session 2: the very first RPC — no polling, no deadline.
    let (_daemon2, client_path) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(&client_path);
    let (channels, _) = client.list_channels("").unwrap();
    let names: Vec<&str> = channels.iter().map(|c| c.name.as_str()).collect();
    for expected in ["md.x", "agg", "mem.a"] {
        assert!(
            names.contains(&expected),
            "'{expected}' missing from the first listing a restarted daemon served — it answered \
             before finishing reconstruction (got {names:?})"
        );
    }
}

/// Merge latency: how long after a producer commits to a member channel does the record appear in
/// the topic. This is the number the mux loop's idle strategy decides, and it used to be decided
/// badly — a flat 5 ms sleep after *every* poll, so a producer on a hot stream waited the full tick
/// each time.
///
/// Measured across processes (the real path: this process writes the member, the daemon merges,
/// this process reads the topic) and with a **tight `try_read` spin** rather than `read_blocking`,
/// because xchannel's own blocking read has a 1 µs-doubling backoff that would otherwise be
/// measured instead of the mux's.
///
/// Asserts the **median** over many samples, not a single one: a median is insensitive to the
/// scheduler hiccups that make single-sample latency assertions flaky, while still being decisive
/// here. Old loop: ~5 ms, every sample. New loop: microseconds. The 1 ms bound sits ~5× below the
/// old behaviour and orders of magnitude above the new one, so it is neither flaky nor toothless.
#[test]
fn a_record_merges_into_its_topic_without_waiting_on_a_poll_tick() {
    let data_dir = temp_dir("mux-latency");
    let (_daemon, client_path) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(&client_path);
    client
        .create_topic("agg", &TopicOptions::default())
        .unwrap();
    let mut w = client
        .publish_to_topic("agg", "mem.a", &ChannelOptions::default())
        .unwrap();
    // Locally hosted ⇒ this reads the topic origin the daemon writes.
    let mut reader = client
        .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(5)))
        .unwrap();

    // Commit one record and spin until it surfaces in the topic; `None` if it never does.
    let mut round_trip = |i: u64| -> Option<Duration> {
        let payload = i.to_le_bytes();
        let started = Instant::now();
        let buf = w.try_reserve(payload.len()).unwrap();
        buf.copy_from_slice(&payload);
        w.commit(1, payload.len() as u32, i).unwrap();
        let deadline = started + Duration::from_secs(5);
        while Instant::now() < deadline {
            match reader.try_read().unwrap() {
                Some(m) if !is_control(m.header().message_type) => return Some(started.elapsed()),
                Some(_) => {} // a slot table — keep looking
                None => std::hint::spin_loop(),
            }
        }
        None
    };

    // Warm up: the first records also pay mux attach and slot-table emission.
    for i in 0..10 {
        assert!(round_trip(i).is_some(), "warm-up record {i} never merged");
    }
    let mut samples: Vec<Duration> = (10..60)
        .map(|i| round_trip(i).unwrap_or_else(|| panic!("record {i} never merged")))
        .collect();
    samples.sort_unstable();

    let median = samples[samples.len() / 2];
    let worst = *samples.last().unwrap();
    assert!(
        median < Duration::from_millis(1),
        "median merge latency {median:?} (worst {worst:?}) — the mux loop is waiting on a clock \
         instead of on records"
    );
    eprintln!(
        "merge latency over {} samples: median {:?}, p90 {:?}, worst {:?}",
        samples.len(),
        median,
        samples[samples.len() * 9 / 10],
        worst
    );
}

/// The point of the duty cycle (`doc/TOPICS.md` §4.1): a connection is a **poll-item**, not a
/// thread. The daemon used to spawn one thread per subscriber and keep it for the connection's
/// life, so 32 subscribers cost 32 parked threads; now one loop forwards them all, and the only
/// threads a connection touches are the transient handshake it exits from.
///
/// Asserts the steady state rather than an instant, because handshake threads are genuinely alive
/// for a moment — the claim is that they do not *accumulate*. Under the old model the count would
/// sit at baseline + 32 forever and no amount of settling would bring it down.
#[cfg(target_os = "linux")]
#[test]
fn subscriptions_do_not_cost_the_daemon_a_thread_each() {
    const SUBSCRIBERS: usize = 32;
    let data_dir = temp_dir("duty-threads");
    let (_daemon, client_path, stream_addr, pid) = spawn_daemon_observed(&data_dir);
    let mut client = connect_with_retry(&client_path);
    let mut w = client
        .create_channel("md.aapl", &ChannelOptions::default())
        .unwrap();
    let baseline = threads_of(pid);

    // Subscribe straight to the stream plane, so each subscriber is a real served connection.
    let mut subs = Vec::new();
    for i in 0..SUBSCRIBERS {
        let replica = data_dir.join(format!("sub-{i}")).join("log");
        std::fs::create_dir_all(replica.parent().unwrap()).unwrap();
        let conn = TcpTransport::connect(stream_addr).unwrap();
        subs.push(
            stream::subscribe(conn, "md.aapl", RecordIndex(0), 0, &replica)
                .unwrap_or_else(|e| panic!("subscriber {i} handshake: {e}")),
        );
    }

    // One record, received by every subscriber — proof all 32 are live poll-items, not merely
    // accepted sockets.
    let buf = w.try_reserve(4).unwrap();
    buf.copy_from_slice(b"tick");
    w.commit(0, 4, 0).unwrap();
    for (i, s) in subs.iter_mut().enumerate() {
        s.recv_one()
            .unwrap_or_else(|e| panic!("subscriber {i} never received the record: {e}"));
    }

    // Handshake threads have exited by now, or will imminently; the count must settle near
    // baseline instead of growing with the subscriber count.
    let allowance = baseline + SUBSCRIBERS / 4;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut threads = threads_of(pid);
    while threads > allowance && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        threads = threads_of(pid);
    }
    assert!(
        threads <= allowance,
        "{SUBSCRIBERS} live subscriptions left the daemon at {threads} threads (baseline \
         {baseline}) — connections are still costing a thread each"
    );
    eprintln!("daemon threads: {baseline} idle -> {threads} with {SUBSCRIBERS} live subscriptions");
}

/// `SIGTERM` is handled: the daemon exits of its own accord, successfully, and takes its client
/// socket with it.
///
/// Worth testing end to end because the handler is hand-rolled — the project has no `libc`
/// dependency, so `signal(2)` is declared against the C runtime directly, and "did the signal
/// actually reach a handler" is not something a unit test on the flag can answer.
#[test]
fn sigterm_shuts_the_daemon_down_cleanly() {
    let data_dir = temp_dir("sigterm");
    // A short socket path: a Unix socket address has a hard length limit that the temp dir plus a
    // long test name can exceed.
    let client_path = std::path::PathBuf::from("/tmp/xchnet-sigterm.sock");
    let _ = std::fs::remove_file(&client_path);
    let mut child = Command::new(env!("CARGO_BIN_EXE_xchanneld"))
        .env("XCHANNELD_DATA_DIR", &data_dir)
        .env("XCHANNELD_CLIENT_PATH", &client_path)
        .env("XCHANNELD_STREAM_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CONTROL_ADDR", "127.0.0.1:0")
        .spawn()
        .expect("spawn xchanneld");
    let pid = child.id();

    // Wait until it is actually serving, so the signal is not racing startup.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !client_path.exists() {
        assert!(
            Instant::now() < deadline,
            "daemon never bound its client plane"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let sent = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(sent.success());

    let mut daemon = Daemon(child);
    let status = daemon
        .wait_for_exit(Duration::from_secs(10))
        .expect("daemon should exit on SIGTERM rather than needing to be killed");
    assert!(
        status.success(),
        "a requested shutdown is not a failure: {status:?}"
    );
    assert!(
        !client_path.exists(),
        "a clean shutdown removes its client socket, so nothing is left claiming to be a daemon"
    );
}

#[test]
fn plain_channel_reregisters_after_daemon_restart() {
    let data_dir = temp_dir("reregister");
    let opts = ChannelOptions::default();

    // Session 1: create a plain channel, write 5 records, drop the writer, kill the daemon.
    {
        let (daemon1, client_path) = spawn_daemon(&data_dir);
        let mut client = connect_with_retry(&client_path);
        let mut w = client.create_channel("md.x", &opts).unwrap();
        for i in 0..5u64 {
            let p = format!("r{i}").into_bytes();
            let buf = w.try_reserve(p.len()).unwrap();
            buf.copy_from_slice(&p);
            w.commit(1, p.len() as u32, i).unwrap();
        }
        drop(daemon1);
    }

    // Session 2: respawn on the same data_dir. reconstruct must re-register md.x (recovering its
    // geometry from the header) so a subscriber can resolve + replicate it — no re-create.
    let (_daemon2, client_path) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(&client_path);
    let mut reader = client
        .subscribe(
            "md.x",
            SubscribeMode::LateJoin,
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < 5 && Instant::now() < deadline {
        if let Some(m) = reader
            .read_blocking(Some(Duration::from_millis(200)))
            .unwrap()
        {
            got.push(m.payload().to_vec());
        }
    }
    let expected: Vec<Vec<u8>> = (0..5).map(|i| format!("r{i}").into_bytes()).collect();
    assert_eq!(
        got, expected,
        "plain channel re-registered and served after restart"
    );
}

/// Two real daemons: a member on node B feeds a topic hosted on node A, across the network,
/// and the merge resumes after A restarts. Exercises the cross-process remote-member path +
/// seed-based peering + restart re-host of a remote member (the highest-risk distributed path).
#[test]
fn remote_member_merges_and_resumes_across_two_daemons() {
    let dir_a = temp_dir("2node-a");
    let dir_b = temp_dir("2node-b");
    let opts = ChannelOptions::default();

    // Node A hosts the topic; node B seeds to A and hosts the member.
    let (daemon_a, a_client, a_control) = spawn_daemon_seeded(1, &dir_a, &[]);
    let (_daemon_b, b_client, b_control) = spawn_daemon_seeded(2, &dir_b, &[a_control]);

    let mut client_a = connect_with_retry(&a_client);
    client_a
        .create_topic("agg", &TopicOptions::default())
        .unwrap();

    let mut client_b = connect_with_retry(&b_client);
    let mut wb = client_b.publish_to_topic("agg", "mem.b", &opts).unwrap();
    for i in 0..5u64 {
        let p = format!("b{i}").into_bytes();
        let buf = wb.try_reserve(p.len()).unwrap();
        buf.copy_from_slice(&p);
        wb.commit(1, p.len() as u32, i).unwrap();
    }

    // A discovers mem.b via gossip, subscribes to B, and merges it into the topic.
    let read_topic_bodies = |client: &mut Client, want: usize| -> Vec<Vec<u8>> {
        let mut reader = client
            .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(8)))
            .unwrap();
        let mut bodies = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(15);
        while bodies.len() < want && Instant::now() < deadline {
            if let Some(m) = reader
                .read_blocking(Some(Duration::from_millis(200)))
                .unwrap()
            {
                if is_control(m.header().message_type) {
                    continue;
                }
                let (_p, body) = Provenance::split(m.payload()).unwrap();
                bodies.push(body.to_vec());
            }
        }
        bodies
    };
    assert_eq!(
        read_topic_bodies(&mut client_a, 5),
        (0..5)
            .map(|i| format!("b{i}").into_bytes())
            .collect::<Vec<_>>(),
        "remote member merged across the network"
    );

    // Restart A; B keeps running and writing. A rebinds a fresh ephemeral port, so we seed the
    // restarted A to B (B's address is stable) — A re-dials B, relearns the member, re-subscribes,
    // and resumes merging.
    drop(daemon_a);
    let _daemon_a2 = spawn_daemon_seeded(1, &dir_a, &[b_control]).0;
    for i in 5..10u64 {
        let p = format!("b{i}").into_bytes();
        let buf = wb.try_reserve(p.len()).unwrap();
        buf.copy_from_slice(&p);
        wb.commit(1, p.len() as u32, i).unwrap();
    }
    let mut client_a = connect_with_retry(&a_client);
    assert_eq!(
        read_topic_bodies(&mut client_a, 10),
        (0..10)
            .map(|i| format!("b{i}").into_bytes())
            .collect::<Vec<_>>(),
        "remote member resumed merging after node A restarted"
    );
}

#[test]
fn topic_with_two_members_rehosts_and_resumes_after_restart() {
    let data_dir = temp_dir("restart2");
    let opts = ChannelOptions::default();
    let (mut wa, mut wb);

    // Session 1: topic + two members, 3 records each, confirm 6 merged, kill daemon.
    {
        let (daemon1, client_path) = spawn_daemon(&data_dir);
        let mut client = connect_with_retry(&client_path);
        client
            .create_topic("agg", &TopicOptions::default())
            .unwrap();
        wa = client.publish_to_topic("agg", "mem.a", &opts).unwrap();
        wb = client.publish_to_topic("agg", "mem.b", &opts).unwrap();
        for i in 0..3u64 {
            for (w, tag) in [(&mut wa, "a"), (&mut wb, "b")] {
                let p = format!("{tag}{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(1, p.len() as u32, i).unwrap();
            }
        }
        let mut reader = client
            .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(5)))
            .unwrap();
        let mut seen = 0;
        let deadline = Instant::now() + Duration::from_secs(10);
        while seen < 6 && Instant::now() < deadline {
            if let Some(m) = reader
                .read_blocking(Some(Duration::from_millis(200)))
                .unwrap()
                && !is_control(m.header().message_type)
            {
                seen += 1;
            }
        }
        assert_eq!(seen, 6, "session 1 merged all 6 records");
        drop(daemon1);
    }

    // Session 2: respawn; both members must re-attach and resume — neither spuriously retired.
    let (_daemon2, client_path) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(&client_path);
    for i in 3..6u64 {
        for (w, tag) in [(&mut wa, "a"), (&mut wb, "b")] {
            let p = format!("{tag}{i}").into_bytes();
            let buf = w.try_reserve(p.len()).unwrap();
            buf.copy_from_slice(&p);
            w.commit(1, p.len() as u32, i).unwrap();
        }
    }

    let mut reader = client
        .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(5)))
        .unwrap();
    let mut by_member: HashMap<u8, Vec<u64>> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while by_member.values().map(Vec::len).sum::<usize>() < 12 && Instant::now() < deadline {
        if let Some(m) = reader
            .read_blocking(Some(Duration::from_millis(200)))
            .unwrap()
        {
            if is_control(m.header().message_type) {
                continue;
            }
            let (prov, body) = Provenance::split(m.payload()).unwrap();
            by_member
                .entry(body[0])
                .or_default()
                .push(prov.member_index);
        }
    }
    // Each member contributed 6 records (0..5), contiguous — resumed across the restart.
    for tag in [b'a', b'b'] {
        let mut idx = by_member.remove(&tag).unwrap_or_default();
        idx.sort_unstable();
        assert_eq!(
            idx,
            (0..6).collect::<Vec<_>>(),
            "member {} resumed contiguously",
            tag as char
        );
    }
}

#[test]
fn topic_rehosts_and_resumes_after_daemon_restart() {
    let data_dir = temp_dir("restart");
    let opts = ChannelOptions::default();

    // A member Writer the *test* process holds across the daemon restart (no-custody: the writer
    // writes the mmap directly, independent of the daemon).
    let mut writer;

    // --- Session 1: create topic + member, write 5, confirm all merged, then kill the daemon.
    {
        let (daemon1, client_path) = spawn_daemon(&data_dir);
        let mut client = connect_with_retry(&client_path);
        client
            .create_topic("agg", &TopicOptions::default())
            .unwrap();
        writer = client.publish_to_topic("agg", "mem.a", &opts).unwrap();
        for i in 0..5u64 {
            let p = format!("m{i}").into_bytes();
            let buf = writer.try_reserve(p.len()).unwrap();
            buf.copy_from_slice(&p);
            writer.commit(1, p.len() as u32, i).unwrap();
        }
        // Wait until all 5 have merged into the topic channel (so they're durable on disk).
        let mut reader = client
            .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(5)))
            .unwrap();
        let mut seen = 0;
        let deadline = Instant::now() + Duration::from_secs(10);
        while seen < 5 && Instant::now() < deadline {
            if let Some(m) = reader
                .read_blocking(Some(Duration::from_millis(200)))
                .unwrap()
                && !is_control(m.header().message_type)
            {
                seen += 1;
            }
        }
        assert_eq!(seen, 5, "session 1 merged all 5 records");
        drop(daemon1); // kill; the test keeps `writer`
    }

    // --- Session 2: respawn on the SAME data_dir. No create_topic is re-issued.
    let (_daemon2, client_path) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(&client_path);

    // Producer resumes through the same member Writer; the re-hosted mux must merge the new
    // records, contiguous with the pre-restart ones.
    for i in 5..10u64 {
        let p = format!("m{i}").into_bytes();
        let buf = writer.try_reserve(p.len()).unwrap();
        buf.copy_from_slice(&p);
        writer.commit(1, p.len() as u32, i).unwrap();
    }

    let mut reader = client
        .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(5)))
        .unwrap();
    let mut bodies = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while bodies.len() < 10 && Instant::now() < deadline {
        if let Some(m) = reader
            .read_blocking(Some(Duration::from_millis(200)))
            .unwrap()
        {
            if is_control(m.header().message_type) {
                continue;
            }
            let (_prov, body) = Provenance::split(m.payload()).unwrap();
            bodies.push(body.to_vec());
        }
    }
    let expected: Vec<Vec<u8>> = (0..10).map(|i| format!("m{i}").into_bytes()).collect();
    assert_eq!(
        bodies, expected,
        "topic re-hosted on restart and resumed merging without re-issuing create_topic"
    );
}

#[test]
fn topic_merges_local_members_end_to_end() {
    let data_dir = temp_dir("topic");
    let (_daemon, client_path) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(&client_path);
    let opts = ChannelOptions::default();

    client
        .create_topic("agg", &TopicOptions::default())
        .unwrap();

    // Two producers publish member channels and write to them; the daemon's mux merges them
    // into the "agg" topic channel with provenance. Bodies chosen so each is identifiable.
    let a_bodies: Vec<Vec<u8>> = (0..3).map(|i| format!("a{i}").into_bytes()).collect();
    let b_bodies: Vec<Vec<u8>> = (0..2).map(|i| format!("b{i}").into_bytes()).collect();
    {
        let mut wa = client.publish_to_topic("agg", "mem.a", &opts).unwrap();
        for (i, body) in a_bodies.iter().enumerate() {
            let buf = wa.try_reserve(body.len()).unwrap();
            buf.copy_from_slice(body);
            wa.commit(1, body.len() as u32, 100 + i as u64).unwrap();
        }
    }
    {
        let mut wb = client.publish_to_topic("agg", "mem.b", &opts).unwrap();
        for (i, body) in b_bodies.iter().enumerate() {
            let buf = wb.try_reserve(body.len()).unwrap();
            buf.copy_from_slice(body);
            wb.commit(2, body.len() as u32, 200 + i as u64).unwrap();
        }
    }

    // A consumer subscribes to the topic like any channel and decodes provenance.
    let total = a_bodies.len() + b_bodies.len();
    let mut reader = client
        .subscribe("agg", SubscribeMode::LateJoin, Some(Duration::from_secs(5)))
        .unwrap();

    // Collect data records (skipping mux control records) grouped by member_ref.
    let mut by_ref: HashMap<u16, Vec<(u64, u64, Vec<u8>)>> = HashMap::new();
    let mut count = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while count < total && Instant::now() < deadline {
        if let Some(m) = reader
            .read_blocking(Some(Duration::from_millis(200)))
            .unwrap()
        {
            if is_control(m.header().message_type) {
                continue;
            }
            let (prov, body) = Provenance::split(m.payload()).unwrap();
            by_ref.entry(prov.member_ref).or_default().push((
                prov.member_index,
                prov.orig_user_meta,
                body.to_vec(),
            ));
            count += 1;
        }
    }
    assert_eq!(count, total, "every member record should reach the topic");
    assert_eq!(by_ref.len(), 2, "two distinct members");

    // Each member's records arrive in order, contiguous from index 0, preserving the original
    // body and user_meta (provenance option (b)). Match by the recovered bodies, not by ref
    // (the arrival interleave across members is timing-dependent and not asserted).
    let mut groups: Vec<Vec<(u64, u64, Vec<u8>)>> = by_ref.into_values().collect();
    for g in &groups {
        for (k, (idx, _, _)) in g.iter().enumerate() {
            assert_eq!(*idx, k as u64, "per-member indices are contiguous from 0");
        }
    }
    groups.sort_by_key(|g| g.len());
    let bodies = |g: &Vec<(u64, u64, Vec<u8>)>| -> Vec<Vec<u8>> {
        g.iter().map(|(_, _, b)| b.clone()).collect()
    };
    assert_eq!(bodies(&groups[0]), b_bodies, "member b: 2 records in order");
    assert_eq!(bodies(&groups[1]), a_bodies, "member a: 3 records in order");
    // Original user_meta preserved in provenance (not consumed by the topic record).
    assert_eq!(groups[1][0].1, 100);
    assert_eq!(groups[0][0].1, 200);
}
