# Security Policy

> **TL;DR — xchannel-net is unauthenticated by design and intended for trusted networks
> only.** There is no authentication, authorization, or encryption on any of its three
> network planes. Any host that can open a TCP connection to a node manager can register
> channel names, subscribe to and pull the full history of any channel, and inject registry
> and membership gossip. **Do not expose `xchanneld` ports to untrusted hosts.**

This is experimental, pre-1.0 software (`version = 0.0.0`). The wire protocol and on-disk
layout may change without notice.

## Threat model / scope

xchannel-net targets a **trusted LAN of cooperating nodes (≤ ~100, single
administrative domain)**, the deployment for which the design was chosen (see
[`DESIGN.md`](DESIGN.md) §2.1). Within that model, **all mesh peers and all local clients
are fully trusted**. The project does *not* defend against a malicious participant; it is
not built for open, adversarial, or WAN deployment.

By default the daemon binds all planes to `127.0.0.1`, which keeps it reachable only from
the local host. Cross-machine replication requires binding a routable address — at which
point every item below is reachable by anything on that network.

## What a network-reachable actor can currently do

These follow directly from the absence of authentication (see `DESIGN.md` §0 and §8); they
are properties of the current design, not bugs:

- **Channel-name hijack / registry poisoning.** The registry is a last-writer-wins CRDT
  whose tiebreak is `(min registered_at_nanos, then min NodeId)`. A peer can send a
  `RegistryDelta` for any name with `registered_at_nanos = 0` and an arbitrary `owner`,
  which beats any honest registration and is sticky. There is no proof that the `owner`
  field corresponds to the sender.
- **Traffic redirection / MITM.** `Heartbeat { node, addr }` lets any peer claim to be any
  `NodeId` at any address; "latest heartbeat wins" membership means an attacker can redirect
  subscribers for a channel to an address it controls.
- **History exfiltration.** Any connector to the stream plane can `Subscribe` to any hosted
  channel by name and pull its full retained history.
- **Channel squatting / unsolicited writes.** Any connector to the client plane can create
  channels (writing files under `data_dir`).
- **Resource exhaustion.** The registry has no TTL, tombstones, or aggregate size cap, so a
  peer can grow it without bound across connections. Stream and client connections are
  capped (a fixed `MAX_CONNECTIONS`), but peer control links are not, and there is no
  per-connection read timeout or rate limit. (Replicas inherit the origin's
  rolling/retention via the `SubscribeAck`, so a replica's disk use is unbounded only if
  its origin is.)

What is *defended*: the wire codec is bounds-checked, refuses attacker-controlled
pre-allocation, and caps individual frame size; there is no `unsafe` in the workspace and
no network-reachable panic — and even if a thread did panic, locks recover from poisoning
rather than cascading; channel names are validated against an allowlist (`[A-Za-z0-9._-]`,
no leading dot), rejecting separators, traversal (`..`), and `.replicas` collisions; the
data directory is created owner-only (`0700` on Unix); and the client's daemon auto-spawn
resolves an absolute binary path, never a `PATH` search.

## Operational guidance

- **Bind to loopback or a trusted, isolated network only.** Keep the defaults
  (`127.0.0.1`) unless you control the network. `xchanneld` emits a warning at startup when
  any plane is bound to a non-loopback address.
- **Place untrusted boundaries outside the mesh.** If nodes must communicate across an
  untrusted network, tunnel the connections (e.g. WireGuard, an mTLS proxy, or an SSH
  tunnel) rather than exposing the ports directly.
- **Treat the data directory as sensitive** — it is created owner-only (`0700` on Unix),
  but still holds replicated channel contents in cleartext; protect it with appropriate
  filesystem ownership.

Authentication, authorization, and transport encryption are tracked as future work
(`DESIGN.md` §8) and are not yet implemented.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue. Use GitHub's
private vulnerability reporting ("Report a vulnerability" under the **Security** tab of the
[repository](https://github.com/dkumsh/xchannel-net)). Given the project's trusted-network
threat model, findings that require a malicious mesh participant are considered out of scope
unless they escape that model (e.g. a panic, memory-safety issue, or path traversal
reachable from a single connection).
