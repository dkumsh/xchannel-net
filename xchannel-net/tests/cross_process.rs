//! Cross-process integration: spawn the real `xchanneld` binary, then drive it from this
//! (separate) process via `Client` — create + write a channel, subscribe, and read the
//! replica back. Because client and daemon are distinct processes, the test can both write
//! the origin and read the replica without tripping xchannel's same-process writer+reader
//! rule (the constraint that forces all the in-process tests to be sequential).

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use xchannel_net_client::{Client, SubscribeMode};
use xchannel_net_core::wire::ChannelOptions;

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

/// Spawn `xchanneld` on ephemeral ports and parse the client-plane address it prints.
fn spawn_daemon(data_dir: &std::path::Path) -> (Daemon, SocketAddr) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_xchanneld"))
        .env("XCHANNELD_NODE_ID", "1")
        .env("XCHANNELD_STREAM_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CONTROL_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_CLIENT_ADDR", "127.0.0.1:0")
        .env("XCHANNELD_DATA_DIR", data_dir)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn xchanneld");

    // First stderr line: "xchanneld[1]: stream <a> | control <b> | client <c>".
    let stderr = child.stderr.take().unwrap();
    let mut line = String::new();
    BufReader::new(stderr).read_line(&mut line).unwrap();
    let client_addr: SocketAddr = line
        .rsplit("client ")
        .next()
        .expect("client addr in banner")
        .trim()
        .parse()
        .expect("parse client addr");
    (Daemon(child), client_addr)
}

fn connect_with_retry(addr: SocketAddr) -> Client {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(c) = Client::connect(addr) {
            return c;
        }
        assert!(Instant::now() < deadline, "daemon never became connectable");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn client_replicates_through_a_spawned_daemon() {
    let data_dir = temp_dir("daemon");
    let (_daemon, client_addr) = spawn_daemon(&data_dir);
    let mut client = connect_with_retry(client_addr);

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
