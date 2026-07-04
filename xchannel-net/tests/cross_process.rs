//! Cross-process integration: spawn the real `xchanneld` binary, then drive it from this
//! (separate) process via `Client` — create + write a channel, subscribe, and read the
//! replica back. Because client and daemon are distinct processes, the test can both write
//! the origin and read the replica without tripping xchannel's same-process writer+reader
//! rule (the constraint that forces all the in-process tests to be sequential).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use xchannel_net_client::{Client, SubscribeMode};
use xchannel_net_core::mux::{Provenance, is_control};
use xchannel_net_core::wire::{ChannelOptions, TopicOptions};

/// Kills the spawned daemon on drop (even if the test panics).
struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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
