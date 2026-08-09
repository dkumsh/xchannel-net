//! A node's durable identity: a unique `NodeId`, plus a human-readable label for everything that
//! has to be read by a person.
//!
//! `DESIGN.md` §5 says the only durable node-owned state is a **stable `NodeId` + config**. This is
//! that state. The id is generated once, on first start, and kept in `<data_dir>/.node_id` — dot
//! prefixed, so no channel name can ever collide with it. It belongs with the data directory
//! because that is what defines the node: the channels it owns live there.
//!
//! **Uniqueness is probabilistic, backed by detection.** There is no way to guarantee it without a
//! coordinator — a central registry or consensus at join — and the design rules out both. So the id
//! is 64 random bits: across 100 nodes the chance any two collide is about 3 × 10⁻¹⁶, orders of
//! magnitude below the chance of undetected corruption in the data being replicated. Random
//! collision is not the risk worth engineering against.
//!
//! The risk worth engineering against is **copying** — a restored backup, or a golden image
//! snapshotted after the daemon's first start. That produces identical ids with certainty rather
//! than by chance, and no amount of entropy helps. What helps is that two nodes claiming one id are
//! *detectable*, exactly and not heuristically, the moment they meet: see
//! `BroadcastDissemination::dedup_links`.
//!
//! Deliberately **not** a timestamp. A nanosecond clock reading looks unique but its entropy is
//! only the spread of start times, so it fails hardest in the case that matters — a fleet
//! provisioned together, sharing a clocksource, generating ids inside the same microsecond. It also
//! inherits the clock's ability to move backwards (§0's clock caveat), and the ordering it would
//! buy is worth nothing here: `NodeId` order breaks a name collision only when two
//! `registered_at_nanos` are *exactly* equal, and the link-initiator preference is arbitrary. Bits
//! spent on time are bits taken from collision resistance, for a property nothing reads.

use std::io;
use std::path::{Path, PathBuf};
use xchannel_net_core::NodeId;

/// File under the data dir holding this node's identity. Dot-prefixed like `.lock` and
/// `.replicas`, so it can never be confused with a channel (names may not start with `.`).
pub const NODE_ID_FILE: &str = ".node_id";

/// A node's identity: what peers key on, and what an operator reads.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NodeIdentity {
    pub id: NodeId,
    /// Unix-millis when this identity was first generated. Cosmetic, like the name — it tells an
    /// operator which of two boxes is the newer one, and it is written down because a random id
    /// says nothing about when it came into being.
    pub created_at_ms: u64,
    /// Human-readable label — for logs, errors and listings, and **nothing else**. Never a map key,
    /// never a tie-break, no correctness depends on it, so a duplicate name is merely confusing.
    /// It exists because a random id is unreadable, which would otherwise make auto-generation a
    /// downgrade for whoever has to operate this.
    pub name: String,
    /// Whether the id was **generated here** rather than supplied by the operator. A generated id
    /// may be discarded and regenerated if it turns out to be a duplicate; a configured one may
    /// not — that is the operator's to fix.
    pub generated: bool,
}

/// Resolve this node's identity, generating and persisting one on first start.
///
/// `configured` is `XCHANNELD_NODE_ID` if the operator set it — that always wins, for deployments
/// that need deterministic ids. Otherwise `<data_dir>/.node_id` is read, or created.
pub fn resolve(
    data_dir: &Path,
    configured: Option<u64>,
    name: Option<String>,
) -> io::Result<NodeIdentity> {
    let name = name.unwrap_or_else(hostname);
    if let Some(id) = configured {
        // A configured id is config, not state — nothing is persisted, so there is no creation
        // time to report.
        return Ok(NodeIdentity {
            id: NodeId(id),
            created_at_ms: 0,
            name,
            generated: false,
        });
    }
    let path = id_path(data_dir);
    if let Some((id, created_at_ms)) = read_file(&path)? {
        return Ok(NodeIdentity {
            id: NodeId(id),
            created_at_ms,
            name,
            generated: true,
        });
    }
    let id = random_u64()?;
    let created_at_ms = now_ms();
    // `key=value` rather than a bare number, so more cosmetic metadata can be added without
    // another format decision.
    std::fs::write(&path, format!("id={id}\ncreated_at_ms={created_at_ms}\n"))?;
    restrict(&path);
    Ok(NodeIdentity {
        id: NodeId(id),
        created_at_ms,
        name,
        generated: true,
    })
}

/// Discard a generated id so the next start picks a fresh one. Used when a node discovers it shares
/// its id with another and owns nothing yet, which is the only moment changing it is safe.
pub fn discard(data_dir: &Path) -> io::Result<()> {
    let path = id_path(data_dir);
    match std::fs::remove_file(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// Whether `<data_dir>/.node_id` exists.
pub fn is_persisted(data_dir: &Path) -> bool {
    id_path(data_dir).exists()
}

pub fn id_path(data_dir: &Path) -> PathBuf {
    data_dir.join(NODE_ID_FILE)
}

/// Parse the identity file: `id=<u64>` required, `created_at_ms=<u64>` optional.
fn read_file(path: &Path) -> io::Result<Option<(u64, u64)>> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut id = None;
    let mut created = 0u64;
    for line in text.lines() {
        match line.trim().split_once('=') {
            Some(("id", v)) => id = v.trim().parse().ok(),
            Some(("created_at_ms", v)) => created = v.trim().parse().unwrap_or(0),
            _ => {}
        }
    }
    // Refuse rather than invent one: silently generating a fresh id would orphan every channel in
    // this data directory, because peers would keep the earlier registration under the old owner.
    let id = id.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{path:?} has no usable `id=` line — refusing to guess this node's identity"),
        )
    })?;
    Ok(Some((id, created)))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 64 random bits from the OS. `/dev/urandom` rather than a hash of the clock, because the whole
/// point is entropy that does not correlate with when the node started.
fn random_u64() -> io::Result<u64> {
    use std::io::Read;
    let mut buf = [0u8; 8];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

/// This host's name, for the human-readable label. Best-effort: an empty or unreadable hostname
/// just means the label falls back to the id, which is still correct, only less friendly.
fn hostname() -> String {
    for path in ["/proc/sys/kernel/hostname", "/etc/hostname"] {
        if let Ok(s) = std::fs::read_to_string(path) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    String::new()
}

#[cfg(unix)]
fn restrict(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}
#[cfg(not(unix))]
fn restrict(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("xchnet-nodeid-{name}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// First start generates and persists; every later start reads the same id back. Stability
    /// matters because channel ownership references it — a node that came back with a new id would
    /// find its own channels owned by someone who no longer exists.
    #[test]
    fn an_id_is_generated_once_and_then_reused() {
        let dir = temp("stable");
        let first = resolve(&dir, None, Some("n1".into())).unwrap();
        assert!(first.generated);
        assert!(is_persisted(&dir));
        let second = resolve(&dir, None, Some("n1".into())).unwrap();
        assert_eq!(first.id, second.id, "the id must survive a restart");

        // Discarding it is how a node becomes a *different* node.
        discard(&dir).unwrap();
        let third = resolve(&dir, None, Some("n1".into())).unwrap();
        assert_ne!(first.id, third.id, "a discarded id is not reused");
    }

    /// An operator-set id wins and is not persisted — it is config, not state, and marking it
    /// `generated: false` is what stops the daemon from discarding something it was told to use.
    #[test]
    fn a_configured_id_wins_and_is_never_discarded() {
        let dir = temp("configured");
        let ident = resolve(&dir, Some(7), Some("n7".into())).unwrap();
        assert_eq!(ident.id, NodeId(7));
        assert!(!ident.generated);
        assert!(
            !is_persisted(&dir),
            "a configured id is not written to disk"
        );
    }

    /// Two independently generated ids differ. Not a proof of uniqueness — nothing can be — but it
    /// catches the mistake that would matter: an id derived from something constant.
    #[test]
    fn generated_ids_are_not_constant() {
        let (a, b) = (temp("rand-a"), temp("rand-b"));
        let x = resolve(&a, None, None).unwrap().id;
        let y = resolve(&b, None, None).unwrap().id;
        assert_ne!(x, y);
        assert_ne!(x, NodeId(0), "a zero id would suggest entropy was not read");
    }

    #[test]
    fn a_corrupt_id_file_is_an_error_not_a_guess() {
        let dir = temp("corrupt");
        std::fs::write(id_path(&dir), "not-a-number").unwrap();
        let err = resolve(&dir, None, None).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
