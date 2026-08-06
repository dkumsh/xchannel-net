# xchannel-net Design Proposal: Channel Discovery API

## Status

**Proposed** (revised — the transport question below is now decided).

---

## Motivation

`xchannel-net` lets a client create and subscribe to channels **by exact name**. For a
distributed position service, distributed monitoring, or metrics aggregation, the interesting
set of channels is not known in advance: producers come and go, and every consumer needs to
find them.

Today an application must keep its own registry of channel names — duplicating information
the daemon already has, and getting it wrong in a different way on every restart.

This proposal exposes that information, and nothing more.

---

## Goals

* Enumerate existing channels.
* Learn about channels that appear later.
* Learn when a channel is withdrawn or replaced by a new incarnation.
* No startup race between "list" and "watch".
* No change to the replication protocol or to per-channel subscriptions.
* No server-side multiplexing of data streams.

## Non-goals

Explicitly **not** introduced: group subscriptions, wildcard data streams, merged channels,
server-side fan-in, or filtering of channel *contents*. Discovery finds channels; the
existing `Subscribe` moves data. Keeping those separate is what stops discovery from
acquiring ordering, buffering, and backpressure questions that belong to the data plane.

---

## What the registry can actually promise

The registry is a **last-writer-wins map CRDT** (`registry.rs`, DESIGN §2.1) converged by
eager `RegistryDelta` broadcast and `RegistrySync` anti-entropy. Two consequences shape the
whole API, and stating them plainly avoids promising things the substrate cannot do:

1. **Per-name convergence, not an event log.** Intermediate states are collapsed by design.
   Two rapid changes to one name may be observed once; an add-then-remove that happens while
   a consumer is away is observed as a removal, never as two events. The loser of a name
   collision is never observable at all.

2. **A local view.** `ListChannels` answers "what has *this daemon* converged on", not "what
   exists". A channel registered a millisecond ago on a partitioned peer is simply not here
   yet.

So the guarantee this API offers is:

> A consumer that keeps up eventually observes the **current state of every name** whose
> entry changed after its resume point.

That is weaker than "no events are lost" — and it is exactly what a position service needs,
because such a service cares about the current set of sources, not about the history of how
the set came to be.

---

## Names and prefixes

Discovery is by **namespace prefix**, using the existing dot convention:

```
fills.prod.            matches   fills.prod.options-mm
                                 fills.prod.futures-mm
                                 fills.prod.arbitrage
```

`/` is **not** available: `validate_channel_name` allows `[A-Za-z0-9._-]` only, so
`fills/prod/app-a` is not a legal channel name. Prefix matching is plain string prefix — no
wildcards, no globbing, no escape characters to get wrong.

A prefix is **not** a channel name and must not be validated as one (`fills.prod.` has a
trailing separator; the empty prefix means "everything"). It gets its own type and its own
rules: at most `CHANNEL_NAME_MAX` bytes, drawn from the same character set, empty permitted.

`Registry` becomes a `BTreeMap` so a prefix scan is a range query rather than a walk of every
channel, with deterministic ordering for free. That is a one-line change with no semantic
effect, and it matters once prefixes are the primary discovery primitive.

---

## ChannelInfo

```rust
struct ChannelInfo {
    name: ChannelName,
    owner: NodeId,
    /// Incarnation of this *name*. A change means the name was reclaimed and this is a
    /// different log — a consumer holding per-channel state must reset it, not extend it.
    epoch: u64,
    /// Whether `owner` is currently a live member. DESIGN §5 requires discovery to report
    /// "known, owner unreachable" distinctly from "known and live"; without it a consumer
    /// cannot tell a frozen channel from a healthy quiet one.
    owner_live: bool,
    /// `Some(topic)` if this channel is a **topic member**. Members are ordinary registered
    /// channels, so a listing of `fills.prod.` returns the topic *and* every producer feeding
    /// it. Consumers that want sources, not plumbing, filter on this.
    member_of: Option<ChannelName>,
    /// Geometry and retention bound, already carried by the registry.
    region_size: u32,
    mtu: u32,
    earliest_index: RecordIndex,
}
```

Notably absent is a wrapper around `ChannelIdentity`: the identity already contains `name`
and `owner`, so nesting it would carry both twice. Tombstoned entries are **never** listed —
`Registry::iter` yields them (anti-entropy needs them) and the listing must filter, exactly
as `Registry::get` does.

---

## Transport: discovery is itself a channel

This is the decision the first draft deferred. The client plane is strictly one request →
one reply on a thread per connection (`node.rs::handle_client`), so a streaming watch would
be the system's first server→client push.

**Decision: the daemon publishes registry changes into a local xchannel, and clients read it
with a plain `Reader`.**

```
                       ListChannels(prefix)                    ┌──────────────┐
    Application  ─────────────────────────────────────────────▶│              │
                 ◀── snapshot + { log_path, generation, head } │   xchanneld  │
                                                               │              │
                       (reads the discovery log directly)      │  registry ── ┼──▶ discovery log
    Application  ◀═════════════════════════════════════════════┼──────────────┘   (local, mmap)
                       Subscribe(name)  — unchanged
```

One RPC returns, under a single registry lock, the matching snapshot **and** the discovery
log's current head. The client applies the snapshot, then reads the log from that head with
ordinary xchannel. There is no gap between the two and no separate "watch" call, which
removes the race the first draft needed revisions to close.

Why this rather than a push stream or a long poll:

* **It costs the daemon nothing per watcher.** A push stream or a long poll parks a thread
  and a connection slot per waiting client (`MAX_CONNECTIONS`, 4096). Readers of an xchannel
  are mmap consumers; after the path handoff the daemon is not involved at all.
* **Resume, retention and invalidation already exist.** The revision *is* a `RecordIndex`.
  "Your revision is too old" *is* the retained-history bound. A daemon restart discards the
  log and starts a new one with a fresh `generation`, so a client that resumes with a stale
  cursor sees the generation change and knows to re-snapshot — the "revision + daemon
  instance id" problem solved by machinery that already shipped.
* **No new backpressure design.** A slow watcher falls behind a retained log and is told so,
  rather than needing a per-watcher buffer policy on the control plane.
* **It dogfoods the substrate**, the same way a topic is "just a channel".

Trade-offs, stated honestly: prefix filtering moves to the client (cheap at the expected
scale — the whole registry is small, and the alternative is the daemon maintaining per-watcher
filters); and the daemon writes one record per registry change, which is a rare event.

The discovery log is **node-local**: it describes this daemon's convergence, not a
network-wide fact. It therefore lives outside the channel namespace (under a dot-prefixed
directory clients cannot name) and is never registered, never replicated, and never
subscribable from a peer.

Because the log is derived state that is rebuilt rather than restored, it does not violate
DESIGN §5's "the only durable node-owned state is `NodeId` + config" — a restarted daemon
starts a fresh one.

### Records

```rust
enum ChannelChange {
    /// A name gained an entry, or its entry changed (owner, epoch, liveness).
    Upserted(ChannelInfo),
    /// A name was tombstoned.
    Removed { name: ChannelName, epoch: u64 },
}
```

Deliberately **not** modelled as `Added` / `Replaced` / `Existing`: the map is
last-writer-wins, so "upsert" is the only thing it can honestly report. A consumer applies
each record to its own map and compares `epoch` to decide whether a name it already knows is
the same log or a new incarnation. `Replaced` as a distinct event would imply the daemon
tracked a transition it does not observe — the winner for a name can change without any user
action, as a later-arriving but earlier-registered identity wins the collision
(`identity.rs::resolve_collision`).

Only merges that **change** the map emit a record. `RegistrySync` re-merges an entire peer
registry on every reconnect; emitting a record per merge would turn each reconnect into a
storm of no-ops.

---

## Example: a position service starting up

```rust
let (sources, cursor) = client.list_channels("fills.prod.")?;
for c in sources.iter().filter(|c| c.member_of.is_none()) {
    subscribe_and_replay(c);
}
// Ready: the snapshot is complete by construction — the RPC returned it whole.

let mut log = client.open_discovery_log(&cursor)?;   // a plain xchannel Reader
loop {
    match log.next()? {
        Upserted(c) if c.epoch != known_epoch(&c.name) => reset_and_resubscribe(c),
        Upserted(c) => update_liveness(c),
        Removed { name, .. } => drop_source(&name),
    }
}
```

The "am I ready yet?" question that a merged snapshot-then-stream design needs a `Synced`
marker to answer is answered here by the RPC returning.

---

## Failure semantics

The first draft described "registry synchronization is interrupted", which does not
correspond to anything in the system: there is no synchronization session, and the local map
is always readable. The real modes are:

| Mode | What the consumer sees | Recovery |
|---|---|---|
| Daemon restart | Discovery log has a new `generation` | Re-`ListChannels`, rebuild local map |
| Consumer too slow / away too long | `Gap` on the log (records aged out) | Re-`ListChannels` |
| Local daemon partitioned | Nothing — the log simply goes quiet | See below |

The third is the dangerous one, and it is the "two liveness concepts" trap (DESIGN §5) in
discovery clothing: a consumer cannot distinguish "no channels have changed" from "my daemon
has been cut off and is telling me about a world that no longer exists". `owner_live` per
channel covers the common case (a dead owner's channels are visibly unreachable), but a
consumer that needs to trust the *set* should also be given the daemon's own convergence
health — live peer count, time since the last delta — either on `ListChannels` or as a
periodic record in the log.

**Tombstones and GC are coupled to this.** `Removed` is durable today only because tombstones
are retained forever (`registry.rs`), which is also an unbounded-growth problem. Whenever
tombstone GC is added, a consumer that was away longer than the GC horizon can miss a
`Removed` and keep a phantom source. The horizon must therefore exceed the discovery log's
retention, so that "too old to have missed a removal" is always caught as a `Gap` first.

---

## Compatibility

Additive. Existing clients keep calling `Create` / `Subscribe` unchanged.

Compatibility is **one-directional**, which the first draft overstated as "fully backwards
compatible": an old client against a new daemon is fine, but a *new* client against an *old*
daemon sends an unknown `ClientRequest` tag and gets a decode error — the hand-rolled codec
has no version negotiation. Clients that must tolerate an older daemon should treat that
error as "discovery unavailable" and fall back to a configured source list.

---

## Future extensions

* Labels (`role=market-data`, `venue=CME`) and prefix+label queries.
* Convergence health as a first-class field rather than a suggestion.
* Authentication and visibility filtering — the client plane is a `0600` Unix socket today,
  so listing is already limited to local users who can reach the daemon.

---

## Summary

Two operations, one of which is a handshake rather than a stream:

```
ListChannels(prefix) -> (snapshot, cursor)     // snapshot + where to start reading
open the discovery log at cursor               // plain xchannel, no daemon involvement
Subscribe(name)                                // unchanged
```

This provides race-free discovery of a dynamic channel set with no external registry, no new
replication semantics, no server-side fan-in, and no per-watcher cost on the daemon — while
being honest that what the registry converges on is the current state of each name, not the
history of how it got there.
