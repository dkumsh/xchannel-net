# Changelog

All notable changes to xchannel-net are documented here. Versioning is pre-1.0 and
experimental: the wire protocol and on-disk layout may change without notice (see
`SECURITY.md`).

## 0.3.0 (2026-08-09)

**A node now joins a mesh without being told how.** It generates its own identity on first start,
learns its peers from the ones it already has, and closes any connected seed graph into the full mesh
the rest of the design assumes. Configuration went from "give every node an id and a peer list" to
"name one seed" — and, on a single host, to nothing at all.

Alongside that: a per-user data directory instead of `/tmp`, graceful shutdown, and the correctness
work a pre-release review turned up — a leaked link that could silently wedge a node, convergence that
only worked in one direction, a link tie-break the two ends computed differently, and a dial loop that
could get a healthy node declared dead by everyone.

**Breaking:** the control-plane wire format and the default data directory both change. Stop every
node before starting any new one; see *Upgrading from 0.2.x* in the README.

### Added
- **The mesh forms itself** (`doc/DESIGN.md` §2.2). Peers used to be exactly what you configured: a
  node dialled its `XCHANNELD_SEEDS`, accepted whoever dialled it, and that was the whole membership.
  Nothing was relayed — a heartbeat updated local membership and stopped there, a registry delta
  merged from a peer was never forwarded — so on any topology short of a full mesh, a change reached
  the originator's neighbours and no further, and a node two hops away was invisible.

  Two mechanisms, separate and neither implying the other:
  - **Gossiped control addresses.** `Heartbeat` now carries the sender's control address as well as
    its stream address, and knowledge about a third node travels as a new `PeerHint`. A node dials
    peers it learns about, so any *connected* seed graph — a chain, a star, one well-known node —
    closes into a full mesh.
  - **Relay on change.** A node that merges an inbound identity and finds its map actually changed
    forwards the winner to its other peers. Relaying only on a *change* is what terminates the flood:
    the registry merge is a total order and idempotent, so a given winning state can move a given
    node's map at most once, whatever cycles the topology has. The relay skips the link it arrived on.

  **Hearsay teaches addresses, never liveness** — a `PeerHint` is deliberately not a forwarded
  heartbeat. Membership liveness means *this* node's ability to reach another, and `resolve`,
  `force_deregister`'s reclaim guard, the topic member reaper and discovery's `owner_live` all depend
  on that reading. A node that looked reachable because a third party vouched for it would weaken
  exactly the guard that stops a partition retiring a channel whose owner is alive and writing.

  **Both ends of a pair dial**, and the duplicate link is collapsed afterwards by `dedup_links`.
  Electing one dialler in advance — the lower `NodeId` — was the first shape of this and it is wrong:
  the election happens before anyone knows whether the elected node can *reach* the other, so under
  asymmetric reachability (a firewall, a NAT) it hands the job to the node that cannot dial and the
  pair never links, though the other direction would have worked first time.

  Honest about what each half buys: with the mesh closing, **relay is not what makes a chain
  converge** — the new link delivers the registry as join-time anti-entropy anyway. Relay covers the
  window before the mesh closes, and any pair that never manages to link. The tests say so rather than
  implying otherwise.

- **A node generates its own identity; no configuration needed to join.** `XCHANNELD_NODE_ID` used to
  default to `1`, so two unconfigured daemons silently shared an identity — and everything is keyed on
  it: channel ownership, the collision tiebreak, membership, and now peer links. There is no default
  any more. A node generates 64 random bits from `/dev/urandom` on first start and keeps them in
  `<data_dir>/.node_id` (dot-prefixed, so no channel name can collide), alongside the time it was
  created. `XCHANNELD_NODE_ID` still overrides, for deployments that need deterministic ids.

  This is the durable state `DESIGN.md` §5 already sanctions — "the only durable node-owned state is
  stable `NodeId` + config" — and the id belongs with the data directory because that is what defines
  the node: the channels it owns live there.

  **Uniqueness is probabilistic, backed by exact detection.** It cannot be guaranteed without a
  coordinator, and the design rules out both a central registry and consensus at join. Across 100
  nodes the chance any two random ids collide is ~3 × 10⁻¹⁶ — far below the chance of undetected
  corruption in the data being replicated. Deliberately **not** a timestamp: a nanosecond clock
  reading's entropy is only the spread of start times, so it fails hardest in the case that matters
  (a fleet provisioned together, sharing a clocksource), it inherits the clock's ability to move
  backwards, and the ordering it would buy is worth nothing — `NodeId` order breaks a name collision
  only when two `registered_at_nanos` are *exactly* equal.

- **Two machines claiming one `NodeId` are detected exactly, in both of the shapes that happen.** Two
  links reporting one id at two advertised control addresses is provably two machines, since a machine
  advertises one control address; `dedup_links` previously *collapsed* that case, dropping connectivity
  to a real peer in order to tidy away a misconfiguration, and now reports it and keeps both links.
  The other shape is the one that matters more and had no detection at all: when the duplicated id is
  **ours**, there is only a single link and so nothing to compare. A heartbeat claiming our id from an
  address that is not ours is a twin; the same heartbeat carrying *our own* advertised address is
  merely a link we opened to ourselves, which a seed list naming every node produces routinely and
  which must not be mistaken for a duplicate.

  A node that finds its own **generated** id duplicated and owns no channels discards it and stops
  with status `3`, so a supervisor's restart takes a fresh one — the whole point being that deleting
  the file while carrying on would leave the duplicate live and change nothing but the file. Once a
  node owns a channel, changing its id would orphan it, so past that point this can only warn. The
  warning prints **once per id**, not once per tick; the *verdict* is re-evaluated on every detection,
  so a node that owned a channel when it first noticed can still stand aside once it owns nothing.

  Reaching the case this was written for — a golden image snapshotted after first start — needs one
  more thing, because a clone cannot *dial* its sibling: dial candidates come from membership, and
  membership excludes this node's own id by construction. Clones seeded at a common bootstrap therefore
  linked only to the bootstrap, which saw the duplicate plainly and could do nothing about it. So a
  `PeerHint` naming **our own** id at an address that is not ours is now kept as a dial candidate —
  never as a member (hearsay confers no liveness) and never as grounds by itself (a node restarted on
  an ephemeral port would find peers relaying its own stale address). The hint earns a dial; the
  heartbeat that returns over that link decides, on direct evidence. Verified end to end: two clones of
  one data directory, each seeded only at a bootstrap, now both detect the duplicate, discard their ids
  and exit 3. Every clone stands aside rather than all but one — they detect each other simultaneously
  and none has grounds to consider itself the original, which is harmless because none owned anything.

- **A cosmetic node name and creation time**, gossiped with the heartbeat and stored in membership.
  `XCHANNELD_NODE_NAME`, defaulting to the hostname. Never a key, never a tie-break — a duplicate name
  is confusing, not incorrect. It exists because an auto-generated id is unreadable, so without it
  automatic identity would be a downgrade for whoever operates this: `owner of 'fills.prod.mm' (node
  14523907234)` becomes `(node fra-mm-01)`.

- **A warning when a data directory holds channels but no `.node_id`.** Removing the id file while
  keeping the data is not the same as a fresh node: the channels are re-registered under a new owner
  with a later timestamp, peers keep the earlier registration, and those channels end up owned by an
  id that never returns — frozen until an operator reclaims the names.

- **Graceful shutdown on `SIGTERM`/`SIGINT`.** The daemon exits of its own accord, tells its peers it
  is leaving (a new `ControlMsg::Leaving`) so they mark it not-live immediately instead of waiting out
  the ten-second liveness timeout, and removes its client socket. Hand-rolled against `signal(2)`
  rather than adding a `libc` dependency, and covered end to end by a test that sends a real `SIGTERM`
  and requires a successful exit — "did the signal reach a handler" is not something a unit test on the
  flag can answer.

  Explicitly **courtesy, not safety**: it exists for promptness and has nothing to unwind. A hard kill
  was already safe and remains so. That property is now stated in the READMEs, where it belongs — it
  is unusual enough to be worth advertising: the daemon is never in a writer's path, committed records
  are durable in their mmap, merge cursors and resume positions are recomputed from the logs rather
  than saved, and a restart reconstructs rather than restores. The cross-process tests `SIGKILL` a
  running daemon and assert contiguous resume.

  A **second** signal restores the default disposition and kills the process outright. Without that
  escape hatch a daemon wedged anywhere in its shutdown path would swallow every further signal and
  could only be ended with `SIGKILL`.

### Changed
- **The default data directory is now `$HOME/.xchannel-net`, not `/tmp/xchanneld`** (breaking, for the
  daemon *and* the client). Nothing is migrated and the old location is not read; see the README's
  upgrade note. The old default was wrong in ways that got worse as durability work landed:
  - `/tmp` is `tmpfs` on most systems, so channels — memory-mapped *files* — were held in RAM. That
    silently contradicts the durability the design rests on: a power cut lost everything, and channel
    bytes counted against memory.
  - `/tmp` is cleared on reboot, so a node lost its `.node_id` with its channels and came back as a
    **different node** every time. Peers keep the earlier registration, so its old channels stayed
    owned by an id that never returned — frozen until an operator reclaimed the names. The default
    arranged for exactly the orphaning the previous release added a warning about.
  - `/tmp` has a world-writable parent and a predictable path, so the directory or a symlink at it
    could be pre-created by anyone. The daemon fails closed, but a per-user directory removes the
    possibility instead of surviving it.
  - Two users on one machine collided, and the data-dir lock meant the second daemon just exited.

  There is **no fallback** when `HOME` is unset — it is an error naming `XCHANNELD_DATA_DIR`. A silent
  second choice is how data ends up somewhere nobody looks, and a service without `HOME` is exactly the
  deployment that should say where its data goes. A *network* home directory needs it set too: channels
  are memory-mapped, and mmap coherence over NFS or SMB is not something this relies on.

  The default now lives in `xchannel_net_core::paths`, shared by both sides, because
  `Client::connect_or_spawn` finds the implicit daemon by that path: if the two computed it
  differently, zero-config startup would fail looking like "no daemon running". Consequently
  `xchannel_net_client::DEFAULT_CLIENT_PATH` (a `&'static str`) is **replaced** by
  `default_client_path() -> io::Result<PathBuf>` — a per-user path cannot be a constant.

  Running several nodes on one host still means giving each its own `XCHANNELD_DATA_DIR`; the default
  is deliberately one flat per-user directory, since one daemon per user is the case that should need
  no configuration at all.
- **Wire (breaking):** `ControlMsg::Heartbeat` gains `control_addr`; `ControlMsg::PeerHint` and
  `ControlMsg::Leaving` are new. A 0.2.x daemon and a newer one cannot share a control plane, and the
  failure is not graceful — an unrecognised frame drops the link, so the two versions reconnect and
  drop each other continuously.
- `Dissemination::pump` returns each identity with the peer link it arrived on, and gains `relay` (to
  forward without echoing the source) and `reply` (to correct the source).
- **A `Leaving` notice is only accepted for the peer on its own link.** Accepting a third party's id
  would let one node mark another not-live; under a duplicate id it meant a departing twin silenced its
  still-serving sibling.
- The startup line no longer reports `created 0` for a configured id. A configured id is config, not
  state — nothing is persisted, so there is no creation time to report.
- The daemon warns when a plane is bound to a wildcard address, because it advertises what it bound:
  peers gossip `0.0.0.0:7001` onwards and none of them can dial it back, and duplicate-id detection —
  which separates a twin from a self-link by comparing advertised control addresses — is weakened,
  since two wildcard-bound machines advertise the *same* address.

### Fixed
Everything from here down was found by a pre-release review of the four changes above.

- **A dropped peer link leaked its fd, its thread and its send half — and could silently wedge the
  node.** The reader owns a `try_clone` dup of the socket, so when its loop exited, dropping it left
  the connection `ESTABLISHED` and *our* send half writable. Nothing prunes the peer list except a
  failed send, and `dedup_links` deliberately keeps a link whose identity it does not know — which the
  exit had just erased. The result was a zombie peer that leaked an fd and a thread on every
  reconnection attempt, eventually wedged `emit_heartbeat` inside `write_all` while holding the
  dissemination lock (stalling the whole node with no error anywhere), and shared an initiator with its
  own replacement, so the far end's tie-break kept the zombie and killed every fresh link. The reader
  now shuts the socket down on exit, and a failed send shuts down before dropping.
- **Convergence only worked in one direction.** A peer that sent a registry state which *lost* the
  merge learned nothing from the recipient's silence, and join-time anti-entropy only runs when a link
  is *established* — so on a link that stayed up, the two disagreed about that channel indefinitely:
  the sender resolved the wrong owner, and after a reclaim it kept serving a replica of an incarnation
  the mesh had retired. The winner now goes back to the sender. It terminates for the same reason the
  relay does, plus one step: a reply is sent only when the arriving state differs from the winner, so
  the reply cannot provoke another.
- **The link tie-break was not one.** Two links with the same initiator — which two dial addresses for
  one peer produce — were resolved on a *per-process* link counter, so the two ends numbered the same
  pair of links differently, each kept the one the other dropped, and the peers were left with **no**
  link at all; then they re-dialled and did it again, every tick. Ties now break on the link's two
  endpoint addresses, ordered, which both ends compute identically because one end's `(local, peer)` is
  the other's `(peer, local)`.
- **A seed named under an alias churned at the maintenance cadence.** A peer's advertised control
  address and the address we happened to dial it on need not be the same, so membership alone could not
  answer "is the node at this dial address already linked?" — the link was dialled, deduplicated, and
  re-dialled forever. The identity found at a dial address is now remembered.
- **Dialling could get a healthy node declared dead by everyone.** The maintenance loop is also the
  heartbeat loop, and it dialled *before* emitting a heartbeat, serially, with a one-second timeout per
  unreachable address and no cap — so the number of addresses the mesh had ever mentioned set this
  node's heartbeat period. Twelve unreachable addresses were enough to flip a live, actively-writing
  owner to `owner_live = false` on its peers; because the topic member reaper keys on the same
  predicate, it then began tombstoning that node's live members' names. The heartbeat now goes first and
  the dialling is capped.

  Measured before and after, with 25 dead seeds and 100 unreachable learned peers: heartbeat period
  max **4.50 s** against a 10 s liveness timeout, and a healthy owner reported dead in **0 of 60**
  samples where it had previously been 15 of 25. A 100-node mesh (4950 links, all live) closes in
  **13.3 s** and a registration crosses it in 0.28 s; a 50-node mesh holds steady with **6** TCP opens
  across the whole mesh in 30 s.

  Three details of the cap were each wrong first, and are the kind that look like tuning and are not:
  - **Each candidate list has its own cursor**, advanced by what it consumed. A single cursor reduced
    modulo each list in turn — seeds are usually one or two entries — pinned the learned walk to a
    constant index forever, so the rotation existed only in the comments.
  - **The penalty is charged for the attempt, not for the failure.** An address can accept a connection
    and then drop the link — a hint or seed naming a stream port, or a peer whose control frames this
    release cannot decode — which costs a full dial and recorded nothing, so two such addresses consumed
    the whole budget every tick, permanently, and the node joined only in the inbound direction. The
    penalty is forgiven once an address has *identified itself* over a link dialled there, which is a
    real peer whose link merely dropped.
  - **Seeds, learned peers and same-id candidates carry separate budgets**, so a tick's worst case is
    their sum — five dials, five seconds — not the two the constant's own doc claimed. A build-time
    assertion now ties that sum to `LIVENESS_TIMEOUT`, because a heartbeat period that quietly grows
    past it looks healthy right until every peer declares this node dead.

- **A peer that stopped reading could stall the entire node.** Every control-plane write is a blocking
  `write_all` made while holding the dissemination lock, so one unresponsive peer stalled the heartbeat
  along with everything else. Two halves: peer sockets now carry a **write timeout** (a failed write is
  already how a dead peer is reaped), and registry relays and replies are **coalesced to one frame per
  peer per pump cycle** instead of one per identity, which also collapses N lock acquisitions to one.
  Measured on the unfixed code: a single frame carrying 200 000 losing identities produced a **40.09 s
  heartbeat gap** — four times the liveness timeout — and a ≥20 s client-plane outage. A control run
  where the same identities *relayed* instead of replying peaked at 1.79 s, isolating the cause.

- **A `deregister_topic` deadlocked the whole control plane against a peer being dialled or accepted.**
  Taking the registry snapshot under the dissemination lock (below) gave the node a lock *order*, and
  this function violated it invisibly: `if let Some(t) = self.registry.lock_safe().deregister(..)` holds
  the guard for the whole body, across both the `hosted` lock and the announce. Two independent
  reproductions hung within a couple of hundred retirements with a single inbound control connection
  arriving every 5 ms, and hung silently — the dissemination lock is the entire control plane, so the
  node stops heartbeating and every peer declares it dead. `subscription_status` held `hosted` across
  the registry lock the same way, closing a second cycle. Both now bind before the `if let`; the lock
  order is written on the `Node` type, along with the temporary-scope hazard that caused it, and a test
  asserts the invariant directly rather than racing for it.

- **A node listing itself among its seeds kept a permanent link to itself** — a thread, two descriptors
  and a heartbeat exchanged with nobody, held forever because a self-link never learns an identity and
  link deduplication deliberately keeps a link whose peer it does not know. Dialling now declines this
  node's own advertised control address. (A seed list naming every node in the mesh is how an operator
  writes one.)

- **`reply` compared whole identities where the merge orders on a key.** Two entries tying on
  `(epoch, deleted, registered_at_nanos, owner)` but differing in a payload field would have had each
  node replying its own version to the other every tick forever, converging on nothing. No path in the
  tree produces such a tie today; the guard should not depend on that staying true.

- **Signal handling:** the flag is set *before* the handler disarms itself, so a fast double `^C` cannot
  kill the process before the graceful path has been asked for; the disarm covers both signals, since an
  operator who presses `^C` and then reaches for `kill` should find the second signal fatal; and
  handlers are installed as the daemon's first action rather than after the data directory, identity and
  lock, which had made a signal in the first milliseconds a hard kill despite a comment claiming
  otherwise.
- **Startup served nothing for as long as its seed list took to dial.** `connect_seeds` ran before any
  plane thread was spawned — measured at 25 s against 25 blackholed seeds, with the listeners already
  bound, so a peer's TCP connect succeeded and then waited: the node looked ready and answered nothing.
  The planes now start first and the maintenance loop does the dialling.
- **The reclaim guard vouched for silence it had never observed.** `force_deregister` fell back to this
  node's own *uptime* when it had never heard from the owner directly — which relay and `PeerHint` have
  made the ordinary case, since a registry entry now routinely arrives second-hand for a node we hold no
  link to. Every daemon older than `reclaim_after` therefore satisfied the floor unconditionally, so an
  owner that was alive and writing but merely unreachable from here could have its channel tombstoned —
  precisely what the guard exists to refuse. One consequence worth stating because it is a behaviour
  change: a node that announced `Leaving` now has to serve out the full floor before its names can be
  reclaimed, where the uptime fallback allowed it immediately. That is the safer reading — a restart
  announces `Leaving` too, and a rolling restart must not make channels reclaimable. It now measures how
  long the owner has been unreachable **from this node**: silence since the last direct contact, or, failing that, how long this node has
  known of the owner and failed to reach it, a clock reset the instant contact is made. Membership keeps
  a departure instant so that a peer which said goodbye stays distinguishable from one never met —
  otherwise the fix would have made a graceful departure unreclaimable.
- **A node recorded *itself* in its own membership map** whenever a twin's heartbeat or a hint about
  itself arrived, overwriting its own entry with the twin's addresses. Since the dial candidates exclude
  this node's own id, the twin was then permanently excluded from them: the two could never meet again,
  and the duplicate became unresolvable in principle.
- **The membership lock was held across every blocking send.** A guard temporary in a `for` head lives
  to the end of the loop, so introducing a new peer to the directory held the lock while writing to it.
  One peer that stopped reading stalled every reader thread waiting on that lock.
- **Join-time anti-entropy could be incomplete.** The registry snapshot was taken *before* the
  dissemination lock, so a local registration in between broadcast its delta to the peers that existed
  at that moment — not the one being adopted — and then handed the new peer a snapshot from before the
  change. The delta was lost to it until some later reconnect. The snapshot is now taken under the lock.
- Two error messages that named no path: binding the client socket now says *which* path was too long
  for `SUN_LEN` (the path is derived from the data directory, so it is not in front of whoever reads
  the message), and `ChannelIdentity::earliest_index` is documented as the dead field it currently is —
  the merge is a total order on the registration *key*, so a re-registration carrying a new retention
  floor ties with the entry already held and is discarded. Nothing depends on it: the authoritative
  floor reaches a subscriber in `SubscribeAck.start`, computed live at subscribe time.
- `signal(2)` is declared with its real signature (`Option<extern "C" fn(i32)>`) rather than a `usize`
  the compiler could not check, and `SIG_DFL` needs no magic constant.
- **`XCHANNELD_CLIENT_PATH` outside the data dir killed startup.** Binding the client plane chmod'ed
  the socket's *parent* directory to `0700`, so pointing it at a shared directory — `/tmp/x.sock` —
  tried to chmod `/tmp`, failed with `EPERM`, and took the daemon down. Only a directory the daemon
  creates is restricted now; the data dir is still tightened explicitly at startup and every directory
  holding channel bytes is created by that path, so nothing loses protection. Found by the SIGTERM test.

## 0.2.1 (2026-08-09)

**Documentation and packaging only — no behaviour change.** 0.2.0's published crates contained
source and nothing else, so a crates.io page said nothing about the crate and a vendored copy
carried none of the reasoning behind it. Same code, same wire protocol, same on-disk layout.

### Added
- **Per-crate READMEs**, and the workspace docs now ship *inside* each published crate. Until now
  the tarballs were source only: `README`, `LICENSE`, `SECURITY.md`, `CHANGELOG.md` and `doc/` all
  live at the workspace root, and cargo packages only what sits inside a crate's own directory — so
  a crates.io page showed no description of the crate and a vendored copy carried none of the
  reasoning. Each crate now symlinks the root documents into itself, which cargo dereferences when
  packaging (verified: the archives contain regular files, no links), keeping one source of truth
  rather than three drifting copies.
- The client crate's usage example is a **compiled `no_run` doctest** on the crate root, so the
  snippet its README shows cannot drift away from the API.

### Fixed
- **The root README was stale.** It linked to `DESIGN.md` at a path the file left when docs moved
  to `doc/`, claimed `version = 0.0.1`, and listed restart-time reconstruction, registry tombstones
  and collision-rejection notices as unimplemented — all three have shipped. It also never
  mentioned topics.

## 0.2.0 (2026-08-09)

**The data plane became a duty cycle** (`doc/TOPICS.md` §4.1): one loop polls replication sources,
replication sinks and mux slots as peer poll-items, replacing a thread per connection — a daemon
serving 32 subscriptions now runs the same **6 threads it runs idle**. Merge latency fell from
~5 ms to ~1 µs, and a topic hot enough to want its own core can be promoted onto one.

The rest is correctness work found by reviewing everything since 0.0.1: topics that never applied
the retention they were configured with, a merge cursor that could claim progress the topic's log
did not have, tombstones that reached every daemon *except* the one that produced them, and a
restarted daemon that answered questions before it had finished rebuilding from disk. Channel names
are now stamped into the log itself, closing the last fact about a channel that was read from the
filesystem rather than from its content.

**Breaking — on-disk.** No migrator; a data directory written by 0.1.0 is not carried forward.
A topic log's slot tables are refused (new wire version), and any channel written before name
stamping is refused rather than re-hosted. Both are deliberate: a guarantee that held only for
files written by a new enough daemon would not be one you could rely on.

**Breaking — API.** `ReplicationSink::open` and `Mux::open` take the channel name;
`Node::run_mux` is superseded by `Node::run_duty_cycle` for daemon use; `NodeConfig` gains
`promoted_topics` and `mux_idle`; channel names are capped at 48 bytes
(`xchannel::CHANNEL_NAME_MAX`), down from 200.

### Added
- **Per-topic promotion** (`doc/TOPICS.md` §4.1 rung 2, resolving §9's open *trigger* question).
  `XCHANNELD_PROMOTED_TOPICS` — comma-separated topic names — gives a topic its own mux thread, and
  the shared duty cycle **skips** it, so a promoted topic leaves the shared budget rather than
  merely gaining a second poller. `TopicStatus::promoted` reports the effective state. The thread
  exits when the map no longer holds *that* mux (identity, not name), so retire-and-recreate cannot
  leave a stale thread polling alongside the new one.

  §9 proposed this as a `TopicOptions` field; it is node config instead. `TopicOptions` is
  client-supplied, and a client that could set it could make the daemon spawn threads; its policy
  fields do not survive a restart, so a promotion would silently lapse; and promotion describes how
  a *node* schedules rather than what a topic is. Automatic, lag-driven promotion remains unbuilt
  on purpose — it needs evidence, and a daemon that spawned threads in reaction to load would be
  the kind of emergent behavior the design refuses elsewhere.

### Changed
- **A channel's name is now stamped into its log, and reconstruction believes the log over the
  directory.** The name was the last thing about a channel that did not self-describe: geometry
  came from the header, absolute indices from `base_record_index`, incarnation from `generation`,
  and a topic's whole membership from its slot table — but the name came from the *directory the
  files sat in*. A data dir that had been migrated, restored or hand-edited could therefore serve
  one channel's records under another's name with nothing to catch it, since the geometry is valid,
  the log is well formed, and `generation` agrees (it travels with the file, so a renamed directory
  looks perfectly consistent). `reconstruct_from_disk` now refuses a mismatch and counts it as
  `skipped`; replicas are stamped and checked the same way.

  **Channel names are capped at 48 bytes** (`xchannel::CHANNEL_NAME_MAX`), down from 200 — the
  limit is now that constant rather than a number of ours, so the bound and the field it must fit
  cannot drift. This is what xchannel 5.0.0's `format_version = 3` widened the field for; until now
  nothing wrote it. 48 bytes is roughly five dotted segments (`fills.prod.options-mm` is 21).
  **On-disk change**: a channel written before this carries no stamp and is refused rather than
  re-hosted — a guarantee that held only for logs written by a new enough daemon would not be one
  you could rely on.

  Every writer that *reopens* a channel has to supply the name, not just the one that creates it:
  xchannel carries `generation` across a roll from the on-disk header but takes `channel_name` from
  whoever built the writer, so a writer that omits it silently produces blank-named segments — and
  those are the ones that outlive retention. Fixed in all three: the client's writer (which is the
  writer for every plain origin and topic member), the mux's topic writer, and the replication sink.

- **`MAX_PENDING_OUT` reduced 8 MiB → 1 MiB, from measurement.** The 8 MiB was a guess made while
  building the duty cycle. `stream::bench::measure_outbound_high_water_mark` (an ignored harness;
  `cargo test -p xchannel-net-core --release -- --ignored --nocapture measure_`) sweeps the cap
  against record size, and two runs agreed on the finding that matters: **throughput is flat from
  4 KiB to 32 MiB**, at every record size from 64 B to 256 KiB. The cap is simply not what limits a
  subscriber that keeps up — 8 MiB was never approached (peak buffered stayed under 3.5 MiB with a
  32 MiB cap, and under 300 KiB for records ≤ 1 KiB).

  Since it buys no throughput, it should be as small as still leaves margin — and that follows from
  no-custody rather than from the benchmark: **the real buffer is the origin's log on disk.**
  Holding megabytes in RAM duplicates what the log already holds durably and only delays the
  throttle, while throttling costs nothing, because the records stay in the log and the subscriber
  resumes from them. What decides whether a slow subscriber survives is *retention*, not this.
  1 MiB keeps ~4× margin over the largest peak seen under a keeping-up subscriber and cuts the
  worst case at `MAX_CONNECTIONS` (4096) from 32 GiB to 4 GiB. `ServerPollItem::set_max_pending_out`
  / `pending_out` exist so the sweep can measure it rather than assert it.
- **The bound is now pinned by a test.** `a_stalled_subscriber_cannot_grow_the_origins_buffer`
  asserts a subscriber that stops reading cannot push the origin past `MAX_PENDING_OUT + one
  record` — one record, not one batch, because a record is always queued whole and the cap only
  gates starting another. It also asserts the throttle actually engaged, so the bound is not
  vacuously satisfied by the socket absorbing everything.
- **The data plane is now a duty cycle** (`doc/TOPICS.md` §4.1). `Node::run_duty_cycle` polls
  replication sources, replication sinks and mux slots as peer poll-items in one loop, each bounded
  to 256 records per turn. It replaces thread-per-connection: a daemon serving 32 subscriptions
  runs the same **6 threads it runs idle** (it would previously have run 38), and scheduling is one
  loop's rather than N blocked threads waking in whatever order the kernel chooses.

  §4.1 described muxes as poll-items in "the daemon's *existing* forwarding loop", but no such loop
  existed — the daemon was thread-per-connection with blocking IO — so it had to be built:
  `FramedConn` (non-blocking, resumable framing, because `read_exact` cannot survive a partial
  frame), `ServerPollItem`/`ClientPollItem` (poll-item forms of the stream protocol), and explicit
  backpressure via `MAX_PENDING_OUT`, which a blocking `write_all` used to provide for free.

  **Establishment deliberately stays off the loop.** Resolving, dialling, handshaking and seeking
  to a resume index are blocking and unbounded; on the duty cycle one unreachable peer would stall
  every poll-item. A transient thread does the handshake and hands the connection over, then exits
  — the connection outliving the thread is the whole difference. Handshakes are now bounded by
  `HANDSHAKE_TIMEOUT`, so a peer that connects and says nothing cannot pin one.

  **The coupling §4.1 warns about is now real**: a hot topic competes with replication forwarding
  for the same core, and a stall in one topic's mmap path briefly stalls forwarding. That is the
  trade the shared loop makes, and it is why the per-topic promotion path (rung 2, `Node::run_mux`)
  is kept.

  Subscription self-healing moved to the conductor, and is registered independently of the
  client-facing `subscriptions` map — keying it off that map would have quietly made a
  `Node::subscribe` handle stop reconnecting unless the caller also filed it, which the old
  per-subscription thread did regardless.
- **Slot-table wire format** gains a leading version byte, the topic's `file_roll_size`/`keep_files`,
  and a per-entry `cursor`. A table of another version is **refused** rather than decoded at the
  wrong offsets, since misreading one misattributes `member_ref → (name, epoch)` — the failure class
  of the recovery-conflation bug. **On-disk format change**: a 0.1.0 topic log is not re-hosted on
  upgrade (its tables no longer decode, so it reconstructs as a plain channel); pre-existing topic
  data directories are not migrated.

### Fixed
- **Topics no longer share one lock across their merges.** `poll_muxes` held the map lock for the
  whole sweep, and a merge is the one thing the daemon does that is unbounded while holding a lock
  — so every topic's poll was a head-of-line block on every other topic, and on `create_topic`,
  `topic_status` and the maintenance loop's attach pass. The hotter the poll loop, the worse it
  got, and the loop is now as hot as records arriving. Each mux carries its own lock; the map lock
  is taken only to clone a handle out. **Lock order is map → mux, never the reverse.** `poll_muxes`
  also no longer abandons the sweep on the first error, which used to let one unmergeable topic
  stall all the others — a different set each round, since `HashMap` order varies.
- **Nothing is committed after a topic's terminal marker.** `Mux::finish` now marks the engine
  inert, so a poll still holding a handle sampled just before `deregister_topic` merges nothing
  more, and a second `finish` does not write a second marker. The coarse map lock had been
  preventing this by accident; per-mux locking would have exposed it.
- **A failed commit no longer consumes the record it failed on.** `merge_one` advanced the member
  cursor *before* committing, so a commit error left the cursor claiming progress the topic's log
  did not have — the shape of the `recover_cursors` conflation bug, reached from the other
  direction. The cursor now advances only once the record is durable.
- **A member record too large for its topic is rejected, not retried forever.** The 18-byte
  provenance prefix can push a record that fitted its own channel over the *topic's* `mtu`. That is
  a permanent contract violation, so it is now dropped and counted like a reserved-`msg_type`
  record — leaving a visible hole in the member's index sequence — rather than surfacing as a
  commit error indistinguishable from a transient one.
- **Merge latency no longer waits on a poll tick.** `run_mux` slept a flat 5 ms after *every* poll,
  including ones that merged, so a producer on a hot stream paid the full tick on each record
  before it was even written to the topic — broker-class latency on the one path built for
  aggregation. The loop now backs off only when a poll finds nothing, via a `MuxIdle` strategy that
  escalates spin → yield → park (doubling from 50 µs to a 5 ms ceiling) and resets the moment
  anything merges. Same shape as xchannel's own `Reader::wait_for_message` backoff and Aeron's
  `IdleStrategy`. Measured across processes: **median 5.06 ms → 1.3 µs** (worst 6 µs over 50
  samples). A quiet topic parks exactly as long as it used to, so idle CPU is unchanged.
  `XCHANNELD_MUX_MAX_PARK_US` caps the park; `0` means never park (keep yielding) for a box where a
  topic's merge latency is worth a core. Polling is not a shortcut here: a member is an mmap'd log
  written by another process with nothing to wait on, and blocking on one of N would starve the
  rest — so the idle strategy *is* the latency contract.
- **Every tombstone this node produces now reaches its own discovery log.** Retiring a topic
  (`deregister_topic`) and reaping a member whose owner had died both announced to peers but
  skipped the local publish, so a client watching discovery **on that node** never saw the
  `Removed` and kept a phantom source indefinitely. The asymmetry made it hard to spot: a peer
  republishes whatever it merges, so every *other* daemon reported the removal correctly. The two
  halves — publish locally, announce to peers — are now one operation (`disseminate_tombstone`)
  rather than two calls to remember at each site. Registration keeps its separate path on purpose:
  there, `Registry::merge_tracked` decides whether anything changed and publishing follows that
  verdict, whereas a locally-produced tombstone is a change by construction.
- **A topic channel now honours its own rolling and retention.** `TopicOptions.channel` carries
  `file_roll_size`/`keep_files`, but only `region_size`/`mtu` reached the mux's writer — and those
  two are the only ones xchannel keeps in the channel header. `create_topic` precreated the file
  with the full options and immediately dropped that writer; the mux's writer, the one that
  actually writes every merged record, was built from geometry alone. So a topic **never rolled and
  never pruned**, growing as one unbounded file however it was configured, while its
  `SubscribeAck` advertised bounds the origin wasn't applying. The four fields now travel together
  as `mux::TopicGeometry` — into the writer on every open (not just at creation), into the slot
  table, and back out on re-host.
- **A topic's disk bounds survive a restart.** They ride the slot table alongside the geometry, so
  a re-hosted topic keeps rolling and pruning and re-advertises the bounds to subscribers. Without
  this the bug returned one restart later, since a `Writer`'s rolling policy is not in the header.
- **A quiet member's merge cursor survives the topic's own retention.** Slot entries now carry the
  cursor, so a member whose data records have aged out of the *topic* is not mistaken for a fresh
  member and re-merged from its own genesis. Enabling retention on topics without this would have
  traded an unbounded-disk bug for a duplication one. Recovery treats a table's cursors as
  authoritative at that scan position (an overwrite, not a max) because a cursor can legally move
  backwards there — `MemberRegressed` resets a member that restarted onto a shorter log.
- **A slot table is now emitted at the head of every segment**, making "a recent slot table is
  always retained" (§6.3, and the restart content-sniff) true by construction. The previous
  guarantee was a record-counted refresh (`SLOT_TABLE_REFRESH`) held against a byte-counted
  retention window — units that do not compose, so a window narrower than the refresh interval
  could retain no table at all. Since xchannel's `Writer` cannot report a roll it performed
  internally, the mux now **drives its own rolling** (`file_roll_size` is no longer handed to the
  writer) and emits the table before the first record of each new segment. `keep_files` remains the
  writer's job. The byte counter resets on reopen, so the first segment after a restart can reach
  ~2× `file_roll_size`.
- **A restarted daemon no longer serves before it has rebuilt from disk.**
  `reconstruct_from_disk` ran *after* the client and control planes began serving, so for a window
  at startup the daemon answered from an empty registry: a client was told its channel did not
  exist and would go create one already on disk, and a peer got an empty anti-entropy snapshot.
  Reconstruction now runs after the listeners are **bound** but before anything accepts on them,
  so an early client blocks until the daemon can answer properly while `connect_or_spawn`'s
  single-instance arbitration (decided by the bind) still resolves immediately. Doing it before
  `connect_seeds` also makes the first snapshot sent to a peer complete. Since this is now blocking
  startup work on a scan that is O(retained records), `reconstruct_from_disk` returns a
  `Reconstructed { topics, origins, skipped }` summary and the daemon logs it — `skipped` being the
  only signal that a channel present on disk is not being served.

### Docs
- Corrected claims in `DESIGN.md`, `RESTART.md`, `TOPICS.md` and the dev skill that 0.1.0's work
  had made false: restart-reconstruct, true `SubscribeAck.head`, lost-collision detection and the
  client `Deregister` RPC were all still described as unbuilt; the `xchannel` dep was cited as
  4.0.0 (it is 5.1.0); topics were described as living on a `topics` branch that has since been
  rebased into `main` and deleted, and the skill's per-commit references pointed at that branch's
  orphaned hashes. Two claims were narrowed rather than deleted, since only part of each had
  landed: cross-node collision notification is still absent (only a *locally known* collision is
  rejected), and crash resume is tested only for a quiesced writer, not a kill mid-commit.
  Separately, `RESTART.md` and `Node::host_channel` still described a reconstructed origin's
  replicas as "no rolling" — since roll mirroring they do roll (following the origin's
  boundaries) and simply never prune.

## 0.1.0 (2026-08-07)

**Topics — multi-producer fan-in** (`doc/TOPICS.md`): a set of single-writer member channels
merged by a **mux** into one totally-ordered **topic channel** (itself an ordinary xchannel
channel — locally readable, network-replicable), without violating single-writer discipline.

### Added
- **Mux engine**: merges member channels into a topic in arrival order, stamping every record
  with mandatory provenance (`member_ref`, `member_index`, original `user_meta` — 18-byte prefix,
  option (b)); `max_batch_per_member` fairness; per-topic slot-table control records mapping
  `member_ref → (name, epoch)`, re-emitted on membership change and periodically so a recent one
  is always retained.
- **Topic client API**: `create_topic` / `publish_to_topic` (RPC); `TopicOptions` (channel
  geometry + `max_batch_per_member` + `member_reap_after`); topics are read via ordinary
  `subscribe`.
- **Member lifecycle (§6)**: `TopicGap` on retention underrun / fresh-pruned genesis;
  `MemberRegressed` on a source that reset under the same identity; clean-leave drain →
  `MemberClosed`; topic retirement → terminal marker; an opt-in **reaper** (`member_reap_after`)
  that tombstones a member whose owner has been unreachable too long.
- **Remote members**: a member on any node feeds a topic hosted elsewhere, discovered via a
  `member_of` tag on `ChannelIdentity` and replicated to the mux node; re-subscribed on
  reconnect/restart so stale replicas refresh.
- **Registry tombstones + reclaim**: the CRDT merge carries an `(epoch, deleted)` generation —
  deregistration tombstones (a stale `Register` can't resurrect a name), and a reclaim wins at
  `epoch + 1`. Member incarnation is that epoch.
- **`RegisterRejected`**: a create that loses a name collision now fails fast (`AlreadyExists`)
  before any file is created, instead of silently believing it owns the name.
- **Liveness-gated resolution**: `resolve` distinguishes "owner unreachable" (`HostUnreachable`)
  from "channel unknown" (`TimedOut`).
- **True `SubscribeAck.head`**: the source advertises its real high-water index (via
  `xchannel::Reader::head_record_index()`).
- **Restart = reconstruct** (`doc/RESTART.md`): a restarted daemon re-hosts its topics from disk
  (identified by their self-describing slot table, which also carries geometry) and re-registers
  plain origins (geometry via `Reader::region_size()`/`mtu()`), resuming without a client
  re-issuing `create_topic`. Deregistration deletes channel files so a restart can't resurrect a
  retired name.
- **`XCHANNELD_SEEDS`**: configure seed peers (comma-separated control `host:port`) for the
  daemon to form/re-form the mesh.
- **Channel discovery** (`doc/DISCOVERY.md`): `Client::list_channels(prefix)` returns the
  matching channels **and** a cursor, both taken under one registry lock so nothing can slip
  between "what exists" and "what changes next"; `Client::watch_channels(cursor)` follows the
  changes. Discovery is itself a channel — the daemon publishes registry changes into a
  node-local xchannel that clients read with a plain `Reader`, so watchers cost the daemon
  nothing (a push stream or long poll would park a thread and a connection slot each), and
  resume, retention and invalidation are the log's existing semantics rather than new ones: the
  revision *is* a `RecordIndex`, "too old" *is* the retained-history bound, and a restarted
  daemon starts a fresh log whose `generation` tells a stale cursor to re-list. Records are
  `Upserted`/`Removed` only — a last-writer-wins map cannot honestly report `Added` vs
  `Replaced`, since the winner for a name can change with no user action when a later-arriving
  but earlier-registered identity wins the collision. Only merges that *change* the map publish,
  so anti-entropy reconnects do not storm. `ChannelInfo` carries `epoch` (a change means the
  name was reclaimed and is a different log), `owner_live`, and `member_of` (topic members are
  ordinary channels and would otherwise silently pollute every listing).
- `Registry` is now a `BTreeMap`: prefix matching is a range scan rather than a full walk, and
  listings are ordered.
- **`ForceDeregister` client RPC** (`Client::force_deregister`) — retire a channel whose
  owning node is **gone**, so its name can be reclaimed and an application pinned to a dead host
  can come back under the same name elsewhere. This is the deliberate exception to owner-only
  deregistration, and it is **operator-invoked, never automatic**: owner death freezing a
  channel is a locked design decision, and a daemon retiring names on its own would be failover
  — across a partition each side sees the other as dead, and a reclaim at `epoch + 1` wins the
  merge, so an automatic reaper could destroy a channel whose owner is alive and still writing.
  Two independent guards: the owner must not be a live member, *and* it must have been silent
  for at least `NodeConfig::reclaim_after` (`XCHANNELD_RECLAIM_AFTER_MS`, default 5 min). An
  owner never heard from is judged against this node's own uptime, so a freshly started daemon
  cannot declare every channel in the registry abandoned. Completes the relocation path opened
  by incarnation-aware subscriptions: after a reclaim the name registers at `epoch + 1`, and
  subscribers holding replicas of the old incarnation rebuild rather than splice.
- **`Deregister` client RPC** (`Client::deregister`): withdraw a channel this node owns —
  tombstone it, disseminate that, delete its files. Returns whether a live channel of that name
  was actually owned here; "already gone" and "owned elsewhere" both report `false` rather than
  erroring. The machinery existed but had no client-facing way to invoke it.
- **A tombstone now retires subscribers proactively.** Merging a `deleted` identity stops any
  subscription held for that name, instead of leaving the loop re-resolving a channel the
  network has agreed is gone — which local readers could not distinguish from a source that had
  merely gone quiet. The replica's files are left in place: the history it already holds is
  still valid, and discarding it is the reader's call.
- **`SubscriptionStatus` client RPC** (`Client::subscription_status`): per-channel replication
  health — `synced`, `head_at_connect`, `owner`, `owner_live`, `generation`,
  `last_record_at_ms`, and rebuild counts by cause. It reports progress and liveness
  *separately* because `synced` alone cannot tell a quiet source from a broken one:
  `owner_live` is membership liveness (the owner's manager is reachable, not that its
  application is still writing), and `last_record_at_ms` is the live staleness signal, since
  `head_at_connect` is a snapshot from the last `SubscribeAck` and goes stale as soon as the
  source moves on. A channel hosted locally reports `local: true` and is caught up by
  definition. An unknown channel is an error rather than a fabricated healthy-looking zero.
- **Observability**: `Node::topic_status` — per-member `merged`/`head`/`lag`/`state`/`rejected`,
  topic head, gaps emitted, slot-table version (§8).

### Fixed
- **Subscribing to a locally hosted channel no longer replicates the node to itself.** The
  daemon now hands back the origin path. Previously it resolved its own channel, connected to
  its own stream plane over loopback, and built a second full copy under `.replicas` — pruned
  on its own schedule and always strictly staler than the file next to it. An application that
  consumes the stream it also produces is the normal case (a position service reading every
  fills channel, its own included), so this doubled disk and stream traffic for every
  self-owned channel on every node.
- **Every channel now owns a directory** — origins at `data_dir/<name>/log`, replicas at
  `data_dir/.replicas/<name>/log`, with xchannel's segments (`log.1`, `log.2`, …) inside it.
  The flat layout put channel names and segment suffixes in one namespace, and channel names may
  contain dots (the recommended separator), so the files of a channel named `md.aapl.1` were
  indistinguishable from segment 1 of `md.aapl` — and `WriterBuilder::build` reopens an existing
  path rather than failing, so the second registrant would have adopted the first's segment.
  It also broke restart recovery: retention unlinks segment 0, which is the *unsuffixed* file, so
  a channel that had rolled past its retention window left only `md.aapl.4`, `md.aapl.5` on disk
  with nothing bearing its name. `reconstruct_from_disk` then registered phantom channels named
  after the surviving segments and never re-hosted the real one — silent loss of every rolled
  channel across a restart. The scan is now "one subdirectory, one channel", with no heuristic,
  and channel deletion is an exact `remove_dir_all` instead of a filename glob.
  **On-disk layout change**: pre-existing data directories are not migrated.
- **A replica from a different incarnation of a name is never resumed onto.** `Subscribe` now
  carries the incarnation the subscriber's replica holds — xchannel's `ChannelHeader.generation`,
  which for our origins is the registry's reclaim `epoch` — and the source refuses the resume if
  it differs from its own. This is the check that matters once a reclaimed channel has grown
  past the replica's length: the resume position then sits comfortably inside the source's
  range, so nothing about it looks wrong, and the sink would append the new log's records onto
  the old log's with the indices lining up and the contiguity check satisfied — two unrelated
  channels silently spliced into one replica. The incarnation lives in the replica's own header
  (stamped from `SubscribeAck` at creation, immutable on reopen), so no node-owned metadata is
  persisted and a restarted daemon rediscovers it by opening the files it already has.
- **A refused resume now rebuilds instead of retrying forever.** `stream::subscribe` returns a
  typed `SubscribeError` separating "discard the replica and re-subscribe from 0" (`Gap` or
  `Diverged`) from transient transport failures; the subscription loop acts on it. Both cases
  previously retried the same unserviceable position at 10 Hz indefinitely. The distinction is
  load-bearing in both directions: retrying a rebuild case never converges, and rebuilding on a
  dropped connection would discard a whole channel's history and re-pull it. A replica rebuilt
  after a retention `Gap` starts at the source's `earliest`, not at genesis — the "full
  *retained* history" contract, so the replica's headers are honest about what retention removed.
  This resolves the `Gap`-handling open question in DESIGN §8.
- **Rebuilds are counted, not silently absorbed** (`Subscription::rebuilds()` →
  `RebuildStats`): tallies by cause (retention `Gap` vs `Diverged`) plus the time of the last
  one. A rebuild replaces the replica's contents under any local reader, so it must be
  observable — the same reason the design refuses to let "quiet" and "broken" look alike.
- **A resume position past the source's head is refused** (`StreamMsg::Diverged`) instead of
  hanging. After a deregistered name is reclaimed by a new owner, the new origin restarts at
  index 0 while other nodes still hold replicas of the old incarnation; their self-healing
  subscriptions then resumed at an index the new log had never reached. The only guard was
  "behind retention", which such a resume passes by being *greater*, so the origin's `skip_to`
  blocked forever waiting for records that would never be written — both ends wedged, nothing
  reported, the replica still serving old-incarnation data to local clients. Worse, if the new
  log ever grew past that index the sink would resume and splice two unrelated channels into
  one replica, indices lining up and the contiguity check none the wiser. Both resume checks
  now run before the seek. `from == head` (caught up) is unaffected.

### Changed
- Bumped `xchannel` **4.0.0 → 5.0.0**: new generic, topic-agnostic accessors
  (`Reader::head_record_index()`, `region_size()`, `mtu()`, `file_sequence()`),
  `ChannelHeader.generation` (an opaque incarnation id, where the registry's reclaim epoch
  will live), and `format_version = 3` widening `channel_name` from 20 to 48 bytes
  (greenfield — v2 files are refused).
- **Roll boundaries now replicate** (`RecordFrame::starts_segment`), amending the original rule
  that file geometry is purely local. The source detects a roll by sampling
  `Reader::file_sequence()` around each read and flags the record that follows it; the sink rolls
  before applying that record, so a replica is segment-aligned with its origin and not merely
  record-identical. **Why it matters:** `keep_files` prunes by file count, so an origin that rolls
  explicitly (`Writer::roll_file`) with no `file_roll_size` — the shape a snapshot-per-segment
  application wants — previously left its replicas rolling never, growing as one unbounded file
  per channel per node. The hint rides on the record it precedes (nothing to desynchronize, and a
  resuming subscriber re-derives it), stays advisory, and costs one byte per record. The mux
  ignores it: a member's boundaries carry no meaning for a merged topic.

### Notes
- **Application restart on an origin is pinned by tests**, since the position-service workload
  restarts routinely and prunes aggressively: reopening a channel whose retention has already
  removed genesis continues at the channel's absolute head rather than restarting at 0 (the
  builder's `base_record_index(0)` is ignored on reopen — the on-disk base wins), and it does
  **not** change the channel's generation. A changed generation would read as a reclaim to every
  subscriber, making an ordinary restart trigger a network-wide discard and re-pull of full
  history; a two-node test asserts subscribers extend their replicas instead.
- Deliberate deviations from `doc/TOPICS.md`, all documented: the mux runs on its own thread
  (not §4.1's shared forwarding loop); recovery is correct but scans from genesis (not §5.2's
  bounded scan); empty topics aren't re-hosted on restart. The network planes remain
  unauthenticated (trusted-LAN only) — control-plane auth is unchanged Tier-1 work.

## 0.0.1 (2026-06-22)

First tagged release. A decentralized network of node managers (`xchanneld`) that replicate
single-writer xchannel logs across machines, with a flat global registry, peer gossip, and
self-healing subscriptions. Built on xchannel 4.0.0.

### Added
- **Node manager (`xchanneld`)** serving three planes: stream (data replication), control
  (registry gossip + membership heartbeats), and a local client RPC plane.
- **Decentralized registry**: last-writer-wins CRDT `ChannelName → ChannelIdentity`, flat
  global names, first-registrant-wins, converged by eager `RegistryDelta` broadcast +
  join-time anti-entropy.
- **Replication engine**: `ReplicationSource` tails an origin log, `ReplicationSink` rebuilds
  a record-identical replica; absolute `RecordIndex` via xchannel's `base_record_index`.
- **Self-healing subscriptions**: resolve → resume from replica head → stream → reconnect,
  until stopped; a reconnect never re-pulls history already on disk.
- **Replica retention inheritance**: replicas adopt the origin's `file_roll_size`/`keep_files`
  via `SubscribeAck`, so a replica's disk use is bounded whenever its origin's is.
- **Client library** (`xchannel-net-client`): `Client::connect` / `connect_or_spawn`,
  `create_channel`, `subscribe` / `subscribe_path`.
- **Security (Tier-0)**: channel-name allowlist (no traversal / `.replicas` collision),
  absolute daemon-spawn path (no `PATH` injection), lock-poison recovery, `MAX_CONNECTIONS`
  cap, `0700` data dir, 64 MiB frame cap. Full threat model in `SECURITY.md`.
- **Client plane over a Unix domain socket** under the `0700` data dir (created `0600`):
  permission-gated, no loopback port; `bind` arbitrates single-instance startup and reclaims
  stale sockets.
- **Single-daemon-per-`data_dir` guard**: exclusive `flock` on `<data_dir>/.lock`, so a
  second daemon on the same dir exits fast (OS-released on exit — no stale lockfile).

### Notes
- The network planes are unauthenticated plaintext; **trusted-LAN deployment only**.
  Authentication, authorization, and transport encryption are future (Tier-1) work.
