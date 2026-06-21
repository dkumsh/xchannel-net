//! Integration test: a `Client` drives a (single, in-process) `xchanneld` `Node` over the
//! client plane — create a channel, write to it, then subscribe and have the daemon build
//! a synced replica. Exercises the full client↔daemon RPC + self-subscription wiring.
//!
//! Same-process caveat: the test process holds the origin `Writer` while the daemon's
//! `ReplicationSource` would read it — so we drop the writer before subscribing (in
//! deployment the writer and daemon are separate processes, where this is unnecessary).
//! We assert sync via the daemon-side `subscription_synced` rather than opening a replica
//! `Reader` in-process.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use xchannel_net::NodeConfig;
use xchannel_net::node::Node;
use xchannel_net_client::Client;
use xchannel_net_core::NodeId;
use xchannel_net_core::wire::ChannelOptions;

fn temp_dir(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("xchnet-clientrpc-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn poll_until<R>(mut f: impl FnMut() -> Option<R>) -> R {
    for _ in 0..3000 {
        if let Some(r) = f() {
            return r;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("condition not met within timeout");
}

#[test]
fn client_creates_and_subscribes_via_daemon() {
    let node = Node::new(NodeConfig {
        node_id: NodeId(1),
        data_dir: temp_dir("daemon"),
        control_addr: loopback(),
        stream_addr: loopback(),
        client_addr: loopback(),
        seeds: vec![],
    });

    let stream_l = node.bind_stream().unwrap();
    let client_l = node.bind_client().unwrap();
    let client_addr = client_l.local_addr().unwrap();

    {
        let n = node.clone();
        std::thread::spawn(move || {
            let _ = n.serve_stream(stream_l);
        });
    }
    {
        let n = node.clone();
        std::thread::spawn(move || {
            let _ = n.serve_client(client_l);
        });
    }

    let n = 30u64;
    let mut client = Client::connect(client_addr).unwrap();

    // Create a channel through the daemon and write to the returned Writer; drop it before
    // subscribing (single-process caveat).
    {
        let mut w = client
            .create_channel("md.aapl", &ChannelOptions::default())
            .unwrap();
        for i in 0..n {
            let p = format!("px-{i}").into_bytes();
            let buf = w.try_reserve(p.len()).unwrap();
            buf.copy_from_slice(&p);
            w.commit(0, p.len() as u32, i).unwrap();
        }
    }

    // Subscribe through the daemon: it resolves the self-owned channel, connects to its own
    // stream plane, and builds a replica. We get the local replica path back.
    let replica_path = client
        .subscribe_path("md.aapl", Some(Duration::from_secs(5)))
        .unwrap();
    assert!(
        replica_path.exists(),
        "replica file should have been created"
    );

    // The daemon syncs all records into the replica.
    poll_until(|| (node.subscription_synced("md.aapl") == Some(n)).then_some(()));
    assert_eq!(node.subscription_synced("md.aapl"), Some(n));
}

#[test]
fn subscribe_to_unknown_channel_times_out() {
    let node = Node::new(NodeConfig {
        node_id: NodeId(2),
        data_dir: temp_dir("unknown"),
        control_addr: loopback(),
        stream_addr: loopback(),
        client_addr: loopback(),
        seeds: vec![],
    });
    let stream_l = node.bind_stream().unwrap();
    let client_l = node.bind_client().unwrap();
    let client_addr = client_l.local_addr().unwrap();
    {
        let n = node.clone();
        std::thread::spawn(move || {
            let _ = n.serve_stream(stream_l);
        });
    }
    {
        let n = node.clone();
        std::thread::spawn(move || {
            let _ = n.serve_client(client_l);
        });
    }

    let mut client = Client::connect(client_addr).unwrap();
    let err = client
        .subscribe_path("does.not.exist", Some(Duration::from_millis(200)))
        .unwrap_err();
    // The daemon's resolve times out and replies Error.
    assert!(
        err.to_string().to_lowercase().contains("timeout")
            || err.to_string().contains("not resolvable")
    );
}
