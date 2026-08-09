# xchannel-net-client

Client library for [xchannel-net](https://github.com/dkumsh/xchannel-net). Talks to the `xchanneld`
node manager **on the same machine**, over a permission-gated Unix socket, and hands back
ordinary [xchannel](https://github.com/dkumsh/xchannel) readers and writers over local files.

That is the whole shape of the system from an application's point of view: the daemon does
discovery, ownership and replication; your process does memory-mapped reads and writes with
nothing in the path. A client never opens a connection to a remote node.

```rust
use std::time::Duration;
use xchannel_net_client::{Client, SubscribeMode};
use xchannel_net_core::wire::ChannelOptions;

let mut client = Client::connect_or_spawn()?;   // starts a local daemon if none is running

// Produce: the daemon registers the name, you get the channel's single Writer.
let mut w = client.create_channel("md.aapl", &ChannelOptions::default())?;
let buf = w.try_reserve(4)?;
buf.copy_from_slice(b"tick");
w.commit(0, 4, 0)?;

// Consume: the daemon locates the owner and keeps a local replica synced; you read it.
let mut r = client.subscribe(
    "fills.prod.mm",
    SubscribeMode::LateJoin,
    Some(Duration::from_secs(5)),
)?;
while let Some(msg) = r.try_read()? {
    let _ = msg.payload();
}
```

(The same example is a compiled doctest on the crate root, so it cannot drift from the API.)

Also here: `list_channels` / `watch_channels` for prefix discovery, `create_topic` and
`publish_to_topic` for multi-producer fan-in, `subscription_status` for replication health,
and `deregister` / `force_deregister` to retire a name.

## Things worth knowing

**Subscribing to a channel this node hosts gives you the origin**, not a second copy — the
common case of consuming a stream you also produce costs nothing.

**A channel is one writer.** `create_channel` hands you *the* writer; there is no second one
anywhere in the mesh. To merge several producers into one ordered log, use a topic, which
does the merge in one named place and writes the result down.

**`SubscribeMode::LateJoin` replays the retained history; `Live` starts at the tail.** Where
retention has truncated history, a replica is honest about it rather than pretending to start
at genesis.

**Status reports progress and liveness separately.** A quiet source and a broken one look
identical from a sync position alone, so `subscription_status` reports both.

## Security

The client plane is a `0600` Unix socket under the daemon's `0700` data directory: local
access is governed by filesystem permissions, and there is no network port to reach. The
daemon's *network* planes, however, are unauthenticated plaintext — see
[`SECURITY.md`](https://github.com/dkumsh/xchannel-net/blob/main/SECURITY.md). Trusted networks only.

One-directional compatibility: an old client against a new daemon is fine, but a new client
against an old daemon gets a decode error on an unknown request. Treat that as "feature
unavailable".

Experimental and pre-1.0: the wire protocol and on-disk layout change between releases,
without migrators. Unix only. Full architecture in
[`DESIGN.md`](https://github.com/dkumsh/xchannel-net/blob/main/doc/DESIGN.md), which ships in this
crate under `doc/`.
