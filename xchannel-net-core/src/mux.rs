//! Topic (multi-producer fan-in) record format — the on-disk shape a **mux** writes into a
//! topic channel (`doc/TOPICS.md` §4.2, provenance option (b)).
//!
//! A topic channel is an ordinary xchannel channel written by exactly one mux. Every record
//! it commits carries a mandatory **provenance** stamp so the merged order is auditable, gap-
//! detectable, and — crucially — recoverable after a mux restart by scanning the topic's own
//! tail (§5). Provenance rides an 18-byte prefix on the record payload (option (b)), which
//! preserves the member's original `user_meta_u64` instead of consuming it (option (a)):
//!
//! ```text
//! topic record payload = [ member_ref: u16 | member_index: u64 | orig_user_meta: u64 ] ++ body
//!                          └──────────────────── 18-byte provenance ─────────────────┘
//! ```
//!
//! `member_ref` indexes a **slot table** (a control record the mux re-emits whenever
//! membership changes, §6.3) that maps `member_ref → (member name, epoch)`, so a `LateJoin`
//! reader of the topic can decode provenance with no external metadata. Mux-originated
//! control records (slot table, gap, member-closed, terminal) use a reserved `msg_type`
//! range; member data records carry their original `msg_type`, which must fall below it.

use std::io;

/// Bytes of the provenance prefix prepended to every topic record payload.
pub const PROVENANCE_LEN: usize = 2 + 8 + 8;

/// `msg_type` values at or above this are reserved for mux control records; a member's data
/// records must use a `msg_type` below it. (A fixed high range for v1; `TopicOptions` could
/// make it configurable later — TOPICS §4.2.)
pub const RESERVED_MSG_TYPE_MIN: u16 = 0xFFF0;

/// Slot-table record: `member_ref → (name, epoch)` for every current member (§6.3).
pub const MSG_TYPE_SLOT_TABLE: u16 = 0xFFFF;
/// `TopicGap` control record: a member fell behind retention; the hole is attributed (§6.2).
pub const MSG_TYPE_TOPIC_GAP: u16 = 0xFFFE;
/// `MemberClosed` control record: a member was drained and its slot closed (§6.1).
pub const MSG_TYPE_MEMBER_CLOSED: u16 = 0xFFFD;
/// Terminal marker: the topic was retired; the mux drained all members and stopped (§4.1).
pub const MSG_TYPE_TERMINAL: u16 = 0xFFFC;

/// Whether `msg_type` is a mux control record (vs a member data record).
#[inline]
pub fn is_control(msg_type: u16) -> bool {
    msg_type >= RESERVED_MSG_TYPE_MIN
}

/// Provenance stamp carried by every topic record (§4.2). `member_index` is the record's
/// absolute index *in its member channel*, so a consumer can detect per-member gaps and the
/// mux can recover per-member cursors from the topic tail (§5).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Provenance {
    /// Index into the current slot table identifying the source member.
    pub member_ref: u16,
    /// The record's absolute index within its member channel.
    pub member_index: u64,
    /// The member record's original `user_meta_u64`, preserved (option (b)).
    pub orig_user_meta: u64,
}

impl Provenance {
    /// Serialize to the fixed 18-byte little-endian prefix.
    pub fn to_prefix(&self) -> [u8; PROVENANCE_LEN] {
        let mut b = [0u8; PROVENANCE_LEN];
        b[0..2].copy_from_slice(&self.member_ref.to_le_bytes());
        b[2..10].copy_from_slice(&self.member_index.to_le_bytes());
        b[10..18].copy_from_slice(&self.orig_user_meta.to_le_bytes());
        b
    }

    /// Parse the prefix from the front of a topic record payload, returning the provenance
    /// and the remaining original body. Errors if the payload is shorter than the prefix.
    pub fn split(payload: &[u8]) -> io::Result<(Provenance, &[u8])> {
        if payload.len() < PROVENANCE_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "topic record shorter than provenance prefix",
            ));
        }
        let (head, body) = payload.split_at(PROVENANCE_LEN);
        let prov = Provenance {
            member_ref: u16::from_le_bytes(head[0..2].try_into().unwrap()),
            member_index: u64::from_le_bytes(head[2..10].try_into().unwrap()),
            orig_user_meta: u64::from_le_bytes(head[10..18].try_into().unwrap()),
        };
        Ok((prov, body))
    }
}

/// Build the topic record payload for a member data record: the provenance prefix followed by
/// the member's original body.
pub fn frame_data(prov: Provenance, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PROVENANCE_LEN + body.len());
    out.extend_from_slice(&prov.to_prefix());
    out.extend_from_slice(body);
    out
}

/// One slot-table entry: which `(name, epoch)` a `member_ref` denotes. `epoch` is the
/// member channel's registry generation — the incarnation, so a respawned producer (new
/// epoch) is a distinct slot and never spliced onto the old one (TOPICS §3.2, decision:
/// incarnation = epoch).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SlotEntry {
    pub member_ref: u16,
    pub name: String,
    pub epoch: u64,
}

/// Encode a slot table as a control-record payload: `u16 count`, then per entry
/// `u16 member_ref, u64 epoch, u16 name_len, name`.
pub fn encode_slot_table(entries: &[SlotEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    for e in entries {
        out.extend_from_slice(&e.member_ref.to_le_bytes());
        out.extend_from_slice(&e.epoch.to_le_bytes());
        let name = e.name.as_bytes();
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
    }
    out
}

/// Decode a slot-table control-record payload produced by [`encode_slot_table`].
pub fn decode_slot_table(mut b: &[u8]) -> io::Result<Vec<SlotEntry>> {
    fn take<'a>(b: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
        if b.len() < n {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated slot table",
            ));
        }
        let (head, rest) = b.split_at(n);
        *b = rest;
        Ok(head)
    }
    let count = u16::from_le_bytes(take(&mut b, 2)?.try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let member_ref = u16::from_le_bytes(take(&mut b, 2)?.try_into().unwrap());
        let epoch = u64::from_le_bytes(take(&mut b, 8)?.try_into().unwrap());
        let name_len = u16::from_le_bytes(take(&mut b, 2)?.try_into().unwrap()) as usize;
        let name = std::str::from_utf8(take(&mut b, name_len)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "slot name not UTF-8"))?
            .to_string();
        out.push(SlotEntry {
            member_ref,
            name,
            epoch,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provenance_round_trips_and_preserves_body() {
        let prov = Provenance {
            member_ref: 7,
            member_index: 123_456_789,
            orig_user_meta: 0xDEAD_BEEF_CAFE,
        };
        let body = b"the original member payload";
        let payload = frame_data(prov, body);
        assert_eq!(payload.len(), PROVENANCE_LEN + body.len());

        let (got, rest) = Provenance::split(&payload).unwrap();
        assert_eq!(got, prov);
        assert_eq!(rest, body);
    }

    #[test]
    fn empty_body_still_round_trips() {
        let prov = Provenance {
            member_ref: 0,
            member_index: 0,
            orig_user_meta: 0,
        };
        let payload = frame_data(prov, b"");
        let (got, rest) = Provenance::split(&payload).unwrap();
        assert_eq!(got, prov);
        assert!(rest.is_empty());
    }

    #[test]
    fn split_rejects_short_payload() {
        let err = Provenance::split(&[0u8; PROVENANCE_LEN - 1]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn slot_table_round_trips() {
        let entries = vec![
            SlotEntry {
                member_ref: 0,
                name: "md.aapl".to_string(),
                epoch: 0,
            },
            SlotEntry {
                member_ref: 1,
                name: "md.msft".to_string(),
                epoch: 3,
            },
        ];
        assert_eq!(
            decode_slot_table(&encode_slot_table(&entries)).unwrap(),
            entries
        );
    }

    #[test]
    fn control_msg_types_are_distinguished_from_data() {
        assert!(is_control(MSG_TYPE_SLOT_TABLE));
        assert!(is_control(MSG_TYPE_TOPIC_GAP));
        assert!(is_control(MSG_TYPE_MEMBER_CLOSED));
        assert!(is_control(MSG_TYPE_TERMINAL));
        assert!(!is_control(0));
        assert!(!is_control(RESERVED_MSG_TYPE_MIN - 1));
    }
}
