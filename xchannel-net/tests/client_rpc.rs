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
use xchannel::{ReaderBuilder, ReaderMode};
use xchannel_net::NodeConfig;
use xchannel_net::node::{MuxIdle, Node};
use xchannel_net_client::Client;
use xchannel_net_core::NodeId;
use xchannel_net_core::wire::{ChannelChange, ChannelOptions};

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

#[test]
fn client_creates_and_subscribes_via_daemon() {
    let node_data_dir = temp_dir("daemon");
    let client_path = node_data_dir.join("client.sock");
    let node = Node::new(NodeConfig {
        node_id: NodeId(1),
        data_dir: node_data_dir.clone(),
        control_addr: loopback(),
        stream_addr: loopback(),
        client_path: client_path.clone(),
        seeds: vec![],
        reclaim_after: Duration::from_secs(300),
        promoted_topics: Default::default(),
        mux_idle: MuxIdle::default(),
        node_name: "client-rpc-test".to_string(),
        id_generated: false,
    });

    let stream_l = node.bind_stream().unwrap();
    let client_l = node.bind_client().unwrap();

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
    {
        // The duty cycle forwards; `serve_stream` only handshakes (doc/TOPICS.md §4.1).
        let n = node.clone();
        std::thread::spawn(move || n.run_duty_cycle(MuxIdle::default()));
    }

    let n = 30u64;
    let mut client = Client::connect(&client_path).unwrap();

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

    // Subscribe through the daemon. The channel is hosted *here*, so the daemon hands back
    // the origin itself rather than replicating the node to itself: no second copy on disk,
    // no loopback stream, and the path is immediately readable and always current.
    let path = client
        .subscribe_path("md.aapl", Some(Duration::from_secs(5)))
        .unwrap();
    assert!(
        !path.starts_with(node_data_dir.join(".replicas")),
        "a locally hosted channel must not be replicated to itself: {path:?}"
    );
    assert_eq!(
        node.subscription_synced("md.aapl"),
        None,
        "and no subscription loop should have been started"
    );

    // Everything written through the client is readable at the returned path.
    let mut r = ReaderBuilder::new(&path)
        .mode(ReaderMode::LateJoin)
        .build()
        .unwrap();
    let mut seen = 0u64;
    while let Some(m) = r.try_read().unwrap() {
        assert_eq!(m.payload(), format!("px-{seen}").as_bytes());
        seen += 1;
    }
    assert_eq!(seen, n);
}

/// Discovery end to end through the client: list, then follow what changes next. The daemon
/// is not involved in the second half — the discovery log is an ordinary xchannel.
#[test]
fn client_lists_and_watches_channels() {
    let data_dir = temp_dir("discovery");
    let client_path = data_dir.join("client.sock");
    let node = Node::new(NodeConfig {
        node_id: NodeId(3),
        data_dir,
        control_addr: loopback(),
        stream_addr: loopback(),
        client_path: client_path.clone(),
        seeds: vec![],
        reclaim_after: Duration::from_secs(300),
        promoted_topics: Default::default(),
        mux_idle: MuxIdle::default(),
        node_name: "client-rpc-test".to_string(),
        id_generated: false,
    });
    let client_l = node.bind_client().unwrap();
    {
        let n = node.clone();
        std::thread::spawn(move || {
            let _ = n.serve_client(client_l);
        });
    }
    {
        // The duty cycle forwards; `serve_stream` only handshakes (doc/TOPICS.md §4.1).
        let n = node.clone();
        std::thread::spawn(move || n.run_duty_cycle(MuxIdle::default()));
    }

    let mut client = Client::connect(&client_path).unwrap();
    let opts = ChannelOptions::default();
    drop(client.create_channel("fills.prod.a", &opts).unwrap());
    drop(client.create_channel("md.aapl", &opts).unwrap());

    let (listed, cursor) = client.list_channels("fills.prod.").unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "fills.prod.a");
    assert_eq!(listed[0].epoch, 0);
    assert!(listed[0].owner_live);

    // A watch opened at the cursor sees exactly what happens after the listing.
    let mut watch = client.watch_channels(&cursor).unwrap();
    assert!(
        watch.try_next().unwrap().is_none(),
        "nothing has changed yet"
    );

    drop(client.create_channel("fills.prod.b", &opts).unwrap());
    assert!(client.deregister("fills.prod.a").unwrap());

    let mut changes = Vec::new();
    while let Some(c) = watch.try_next().unwrap() {
        changes.push(c);
    }
    assert!(matches!(
        &changes[0],
        ChannelChange::Upserted(c) if c.name == "fills.prod.b"
    ));
    assert!(matches!(
        &changes[1],
        ChannelChange::Removed { name, .. } if name == "fills.prod.a"
    ));
    assert_eq!(changes.len(), 2, "and nothing else: {changes:?}");
}

#[test]
fn subscribe_to_unknown_channel_times_out() {
    let data_dir = temp_dir("unknown");
    let client_path = data_dir.join("client.sock");
    let node = Node::new(NodeConfig {
        node_id: NodeId(2),
        data_dir,
        control_addr: loopback(),
        stream_addr: loopback(),
        client_path: client_path.clone(),
        seeds: vec![],
        reclaim_after: Duration::from_secs(300),
        promoted_topics: Default::default(),
        mux_idle: MuxIdle::default(),
        node_name: "client-rpc-test".to_string(),
        id_generated: false,
    });
    let stream_l = node.bind_stream().unwrap();
    let client_l = node.bind_client().unwrap();
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
    {
        // The duty cycle forwards; `serve_stream` only handshakes (doc/TOPICS.md §4.1).
        let n = node.clone();
        std::thread::spawn(move || n.run_duty_cycle(MuxIdle::default()));
    }

    let mut client = Client::connect(&client_path).unwrap();
    let err = client
        .subscribe_path("does.not.exist", Some(Duration::from_millis(200)))
        .unwrap_err();
    // The daemon's resolve times out and replies Error.
    assert!(
        err.to_string().to_lowercase().contains("timeout")
            || err.to_string().contains("not resolvable")
    );
}
