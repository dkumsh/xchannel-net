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

use crate::RecordIndex;
use crate::replication::ReplicationSource;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use xchannel::{ReaderBuilder, Writer, WriterBuilder};

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

/// Body of a [`MSG_TYPE_TOPIC_GAP`] control record: on resume, member `member_ref` could
/// not be extended contiguously because records `[from, resumed_at)` had aged out of its
/// retention, so the mux skipped to `resumed_at` (§6.2). The hole is explicit, attributed,
/// and durable — consumers choose their own severity. Never silently spliced.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TopicGap {
    pub member_ref: u16,
    pub from: u64,
    pub resumed_at: u64,
}

impl TopicGap {
    /// Serialize to the 18-byte control-record payload (`u16 member_ref, u64 from, u64
    /// resumed_at`).
    pub fn to_payload(&self) -> [u8; 18] {
        let mut b = [0u8; 18];
        b[0..2].copy_from_slice(&self.member_ref.to_le_bytes());
        b[2..10].copy_from_slice(&self.from.to_le_bytes());
        b[10..18].copy_from_slice(&self.resumed_at.to_le_bytes());
        b
    }

    /// Parse a `TopicGap` control-record payload.
    pub fn decode(b: &[u8]) -> io::Result<TopicGap> {
        if b.len() < 18 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TopicGap record too short",
            ));
        }
        Ok(TopicGap {
            member_ref: u16::from_le_bytes(b[0..2].try_into().unwrap()),
            from: u64::from_le_bytes(b[2..10].try_into().unwrap()),
            resumed_at: u64::from_le_bytes(b[10..18].try_into().unwrap()),
        })
    }
}

/// Body of a [`MSG_TYPE_MEMBER_CLOSED`] control record: member `member_ref` left cleanly and
/// was drained to `final_index` (its head at close), then its slot was closed (§6.1). The
/// `member_ref`/incarnation is never reused, so a later record for it can't be confused with a
/// respawn.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MemberClosed {
    pub member_ref: u16,
    pub final_index: u64,
}

impl MemberClosed {
    /// Serialize to the 10-byte control-record payload (`u16 member_ref, u64 final_index`).
    pub fn to_payload(&self) -> [u8; 10] {
        let mut b = [0u8; 10];
        b[0..2].copy_from_slice(&self.member_ref.to_le_bytes());
        b[2..10].copy_from_slice(&self.final_index.to_le_bytes());
        b
    }

    /// Parse a `MemberClosed` control-record payload.
    pub fn decode(b: &[u8]) -> io::Result<MemberClosed> {
        if b.len() < 10 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MemberClosed record too short",
            ));
        }
        Ok(MemberClosed {
            member_ref: u16::from_le_bytes(b[0..2].try_into().unwrap()),
            final_index: u64::from_le_bytes(b[2..10].try_into().unwrap()),
        })
    }
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

// ---------------- the mux engine ----------------

/// One member the mux is merging: its `member_ref`/identity, the tailing source over the
/// member channel, and the next member-index to merge.
struct Slot {
    member_ref: u16,
    name: String,
    epoch: u64,
    source: ReplicationSource,
    cursor: u64,
}

/// A **mux**: the single writer of one topic channel, merging N member channels into it in
/// arrival order with mandatory provenance (`doc/TOPICS.md` §4). This is the transport-free
/// engine (plain xchannel readers + one writer), so it runs equally as a poll-item in the
/// daemon's forwarding loop or inside a standalone host (§4.1). Local members only for now;
/// a remote member is just a local replica of it, consumed identically (Phase 2).
///
/// The mux persists nothing of its own: per-member cursors are recovered on restart by
/// scanning the topic's own tail (§5), since every record self-describes its origin.
pub struct Mux {
    topic: Writer,
    slots: Vec<Slot>,
    next_ref: u16,
    max_batch_per_member: usize,
    /// Per-`(name, epoch)` highest member-index already merged, recovered from the topic tail
    /// at open; consumed as members (re)attach so they resume without re-merging.
    recovered: HashMap<(String, u64), u64>,
    /// Count of `TopicGap` records emitted (observability, §8).
    gaps_emitted: u64,
    /// Count of slot-table emissions — bumps on every membership change (§8 slot-table version).
    slot_table_version: u64,
}

/// Per-member merge status (§8): how far the mux has merged vs the member's head.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MemberStat {
    pub name: String,
    pub epoch: u64,
    /// Absolute index of the next record to merge (= records merged so far).
    pub merged: u64,
    /// The member channel's current head.
    pub head: u64,
    /// `head - merged` — how far behind the merge is for this member.
    pub lag: u64,
}

/// A snapshot of a mux's merge health (§8 "minimum bar to ship"). Combine with owner liveness
/// (at the node layer) to classify a member as quiet / behind / unreachable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MuxStatus {
    pub members: Vec<MemberStat>,
    /// The topic channel's head (all records: data + control).
    pub topic_head: u64,
    pub gaps_emitted: u64,
    pub slot_table_version: u64,
}

impl Mux {
    /// Open (or reopen) the mux for a topic channel at `path`. Recovers per-member cursors
    /// from any existing topic content first, then opens the topic writer for append. A member
    /// data record must use a `msg_type` below [`RESERVED_MSG_TYPE_MIN`] (the reserved range is
    /// the mux's control records) — that is the member contract, not enforced here.
    pub fn open(
        path: &Path,
        region_size: u32,
        mtu: u32,
        max_batch_per_member: usize,
    ) -> io::Result<Self> {
        let recovered = match recover_cursors(path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e),
        };
        let topic = WriterBuilder::new(path)
            .region_size(region_size as usize)
            .mtu(mtu as u64)
            .build()?;
        Ok(Self {
            topic,
            slots: Vec::new(),
            next_ref: 0,
            max_batch_per_member: max_batch_per_member.max(1),
            recovered,
            gaps_emitted: 0,
            slot_table_version: 0,
        })
    }

    /// Snapshot the mux's merge health (§8): per-member merged/head/lag, topic head, gaps
    /// emitted, and slot-table version. Reading each member's head is a cheap header read.
    pub fn status(&self) -> io::Result<MuxStatus> {
        let mut members = Vec::with_capacity(self.slots.len());
        for s in &self.slots {
            let head = s.source.head()?.0;
            members.push(MemberStat {
                name: s.name.clone(),
                epoch: s.epoch,
                merged: s.cursor,
                head,
                lag: head.saturating_sub(s.cursor),
            });
        }
        Ok(MuxStatus {
            members,
            topic_head: self.topic.next_record_index(),
            gaps_emitted: self.gaps_emitted,
            slot_table_version: self.slot_table_version,
        })
    }

    /// Attach a member channel at `member_path`, identified by `(name, epoch)` (epoch = its
    /// registry generation / incarnation). Resumes from the recovered cursor if this member
    /// was merged before, else from genesis — late discovery loses nothing (§4.1). Re-emits
    /// the slot table so a topic reader can decode the new member's provenance (§6.3).
    pub fn add_member(&mut self, name: &str, epoch: u64, member_path: &Path) -> io::Result<()> {
        // Idempotent: attaching an already-attached (name, epoch) is a no-op, so the publish
        // path and the discovery loop can both call it safely.
        if self.has_member(name, epoch) {
            return Ok(());
        }
        let member_ref = self.next_ref;
        self.next_ref += 1;
        let (mut source, earliest) = ReplicationSource::open(member_path)?;
        let head = source.head()?.0;
        let recovered = self.recovered.get(&(name.to_string(), epoch)).copied();

        // Where to resume, and whether a hole must be recorded. `want` is the next index we
        // believe we need: `recovered + 1` on resume, else genesis. We never skip_to past a
        // hole or past the source head silently (§6.2) — every discontinuity is an attributed
        // `TopicGap`. `gap = Some((from, resumed_at))`.
        let want = recovered.map(|max| max + 1).unwrap_or(0);
        let (start, gap) = if want < earliest.0 {
            // Records `[want, earliest)` aged out of retention (or, for a fresh member, its
            // genesis was pruned before we ever saw it: `want = 0 < earliest`). Resume at
            // `earliest`, attributing the hole.
            (earliest.0, Some((want, earliest.0)))
        } else if want > head {
            // Our cursor is past the source's head: the member reset/respawned under the same
            // `(name, epoch)` (an upstream §3.2 anti-splice violation). Never skip_to past
            // `head` — that would hang or strand the new incarnation's records. Resume at
            // `head` and record the anomaly.
            (head, Some((want, head)))
        } else {
            (want, None)
        };

        source.skip_to(RecordIndex(start))?;
        self.slots.push(Slot {
            member_ref,
            name: name.to_string(),
            epoch,
            source,
            cursor: start,
        });
        self.emit_slot_table()?;
        if let Some((from, resumed_at)) = gap {
            self.emit_topic_gap(member_ref, from, resumed_at)?;
        }
        Ok(())
    }

    /// Merge whatever is currently ready from each member into the topic, in arrival order,
    /// bounded to `max_batch_per_member` records per member per call so one hot member cannot
    /// monopolize the interleave (§4.3). Returns the number of records merged this call.
    pub fn poll(&mut self) -> io::Result<usize> {
        let max_batch = self.max_batch_per_member;
        let mut merged = 0;
        for i in 0..self.slots.len() {
            for _ in 0..max_batch {
                if self.merge_one(i)? {
                    merged += 1;
                } else {
                    break;
                }
            }
        }
        Ok(merged)
    }

    /// Merge the next ready record from slot `i` into the topic with provenance. Returns
    /// whether a record was merged (`false` = the member is currently caught up).
    fn merge_one(&mut self, i: usize) -> io::Result<bool> {
        let Some(frame) = self.slots[i].source.try_next_frame()? else {
            return Ok(false);
        };
        let prov = Provenance {
            member_ref: self.slots[i].member_ref,
            member_index: frame.index.0,
            orig_user_meta: frame.user_meta,
        };
        let payload = frame_data(prov, &frame.payload);
        let buf = self.topic.try_reserve(payload.len())?;
        buf.copy_from_slice(&payload);
        self.topic.commit(frame.msg_type, payload.len() as u32, 0)?;
        self.slots[i].cursor = frame.index.0 + 1;
        Ok(true)
    }

    /// Clean leave (§6.1): drain member `(name, epoch)` to its current head, commit a
    /// [`MemberClosed`] marker with that final index, close the slot, and re-emit the slot
    /// table. Returns whether the member was attached. The `member_ref` is retired, never
    /// reused — a stale record for it can't be mistaken for a respawn (incarnation rule).
    pub fn remove_member(&mut self, name: &str, epoch: u64) -> io::Result<bool> {
        let Some(pos) = self
            .slots
            .iter()
            .position(|s| s.name == name && s.epoch == epoch)
        else {
            return Ok(false);
        };
        while self.merge_one(pos)? {} // drain to head
        let member_ref = self.slots[pos].member_ref;
        let final_index = self.slots[pos].cursor;
        self.emit_member_closed(member_ref, final_index)?;
        self.slots.remove(pos);
        self.emit_slot_table()?;
        Ok(true)
    }

    /// Retire the topic (§4.1): drain every member to its head (each with a `MemberClosed`
    /// marker), then commit a terminal marker. The mux holds no members afterwards; the caller
    /// drops it and deregisters the topic channel.
    pub fn finish(&mut self) -> io::Result<()> {
        let members: Vec<(String, u64)> = self
            .slots
            .iter()
            .map(|s| (s.name.clone(), s.epoch))
            .collect();
        for (name, epoch) in members {
            self.remove_member(&name, epoch)?;
        }
        let buf = self.topic.try_reserve(0)?;
        debug_assert!(buf.is_empty());
        self.topic.commit(MSG_TYPE_TERMINAL, 0, 0)?;
        Ok(())
    }

    /// Commit a [`MemberClosed`] control record.
    fn emit_member_closed(&mut self, member_ref: u16, final_index: u64) -> io::Result<()> {
        let payload = MemberClosed {
            member_ref,
            final_index,
        }
        .to_payload();
        let buf = self.topic.try_reserve(payload.len())?;
        buf.copy_from_slice(&payload);
        self.topic
            .commit(MSG_TYPE_MEMBER_CLOSED, payload.len() as u32, 0)?;
        Ok(())
    }

    /// Whether a member with this `(name, epoch)` is already attached.
    pub fn has_member(&self, name: &str, epoch: u64) -> bool {
        self.slots
            .iter()
            .any(|s| s.name == name && s.epoch == epoch)
    }

    /// Current members as `(name, epoch, next_cursor)` — for observability/tests.
    pub fn members(&self) -> Vec<(String, u64, u64)> {
        self.slots
            .iter()
            .map(|s| (s.name.clone(), s.epoch, s.cursor))
            .collect()
    }

    /// Commit a [`TopicGap`] control record attributing a retention hole to a member (§6.2).
    fn emit_topic_gap(&mut self, member_ref: u16, from: u64, resumed_at: u64) -> io::Result<()> {
        let payload = TopicGap {
            member_ref,
            from,
            resumed_at,
        }
        .to_payload();
        let buf = self.topic.try_reserve(payload.len())?;
        buf.copy_from_slice(&payload);
        self.topic
            .commit(MSG_TYPE_TOPIC_GAP, payload.len() as u32, 0)?;
        self.gaps_emitted += 1;
        Ok(())
    }

    /// Commit an updated slot table so a `LateJoin` topic reader can decode every member_ref.
    fn emit_slot_table(&mut self) -> io::Result<()> {
        let entries: Vec<SlotEntry> = self
            .slots
            .iter()
            .map(|s| SlotEntry {
                member_ref: s.member_ref,
                name: s.name.clone(),
                epoch: s.epoch,
            })
            .collect();
        let payload = encode_slot_table(&entries);
        let buf = self.topic.try_reserve(payload.len())?;
        buf.copy_from_slice(&payload);
        self.topic
            .commit(MSG_TYPE_SLOT_TABLE, payload.len() as u32, 0)?;
        self.slot_table_version += 1;
        Ok(())
    }
}

/// Recover per-`(name, epoch)` highest merged member-index by scanning a topic channel's tail
/// (§5). Provenance makes cursors data-durable in the topic itself — no sidecar. Returns an
/// empty map if the topic doesn't exist yet.
///
/// Phase 1 scans from genesis for simplicity; §5's bounded scan (stop at the last slot-table
/// record once every current member is seen) is a later optimization.
fn recover_cursors(path: &Path) -> io::Result<HashMap<(String, u64), u64>> {
    let mut reader = ReaderBuilder::new(path).late_join().build()?;
    // The ref → (name, epoch) mapping *in force at the current scan position*. `member_ref` is
    // a per-session counter reset on every `Mux::open`, so the same ref denotes different
    // members across reopens — we must resolve each record's ref through the slot table active
    // when that record was written (positional), NOT the latest one. Keying the recovered
    // cursor on the bare ref (max over the whole log) would conflate incarnations and resume a
    // member past its own head, silently skipping its committed records.
    let mut active: HashMap<u16, (String, u64)> = HashMap::new();
    let mut out: HashMap<(String, u64), u64> = HashMap::new();
    while let Some(m) = reader.try_read()? {
        let mt = m.header().message_type;
        if mt == MSG_TYPE_SLOT_TABLE {
            active = decode_slot_table(m.payload())?
                .into_iter()
                .map(|e| (e.member_ref, (e.name, e.epoch)))
                .collect();
        } else if !is_control(mt) {
            let (prov, _) = Provenance::split(m.payload())?;
            if let Some(member) = active.get(&prov.member_ref) {
                let e = out.entry(member.clone()).or_insert(0);
                *e = (*e).max(prov.member_index);
            }
        }
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

    // ---- engine tests ----

    const REGION: usize = 1 << 20;
    const REGION_U32: u32 = REGION as u32;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("xchnet-mux-{name}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Append `(msg_type, user_meta, payload)` records to a member channel (creates or reopens
    /// for append), then drop the writer so a same-process reader can consume it.
    fn write_member(base: &Path, recs: &[(u16, u64, &[u8])]) {
        let mut w = WriterBuilder::new(base)
            .region_size(REGION)
            .build()
            .unwrap();
        for &(mt, um, payload) in recs {
            let buf = w.try_reserve(payload.len()).unwrap();
            buf.copy_from_slice(payload);
            w.commit(mt, payload.len() as u32, um).unwrap();
        }
    }

    /// Read the topic's data records as `(msg_type, provenance, body)`, skipping control
    /// records; also returns how many slot-table records were seen.
    fn read_topic(path: &Path) -> (Vec<(u16, Provenance, Vec<u8>)>, usize) {
        let mut r = ReaderBuilder::new(path).late_join().build().unwrap();
        let mut data = Vec::new();
        let mut slot_tables = 0;
        while let Some(m) = r.try_read().unwrap() {
            let mt = m.header().message_type;
            if mt == MSG_TYPE_SLOT_TABLE {
                slot_tables += 1;
                continue;
            }
            if is_control(mt) {
                continue;
            }
            let (prov, body) = Provenance::split(m.payload()).unwrap();
            data.push((mt, prov, body.to_vec()));
        }
        (data, slot_tables)
    }

    #[test]
    fn merges_two_members_with_provenance() {
        let dir = temp_dir("merge");
        let (topic, m_a, m_b) = (dir.join("topic"), dir.join("a"), dir.join("b"));
        write_member(&m_a, &[(1, 100, b"a0"), (1, 101, b"a1")]);
        write_member(&m_b, &[(2, 200, b"b0")]);

        let mut mux = Mux::open(&topic, REGION_U32, 0, 16).unwrap();
        mux.add_member("a", 0, &m_a).unwrap();
        mux.add_member("b", 0, &m_b).unwrap();
        assert_eq!(mux.poll().unwrap(), 3);
        drop(mux);

        let (data, slot_tables) = read_topic(&topic);
        assert!(
            slot_tables >= 1,
            "slot table emitted for provenance decoding"
        );
        assert_eq!(data.len(), 3);

        // member_ref 0 = "a" (added first): indices 0,1 with original msg_type/user_meta/body.
        let a: Vec<_> = data.iter().filter(|(_, p, _)| p.member_ref == 0).collect();
        assert_eq!(a.len(), 2);
        assert_eq!(
            (a[0].0, a[0].1.member_index, a[0].1.orig_user_meta),
            (1, 0, 100)
        );
        assert_eq!(a[0].2, b"a0");
        assert_eq!(a[1].1.member_index, 1);
        assert_eq!(a[1].2, b"a1");

        // member_ref 1 = "b".
        let b: Vec<_> = data.iter().filter(|(_, p, _)| p.member_ref == 1).collect();
        assert_eq!(b.len(), 1);
        assert_eq!(
            (b[0].0, b[0].1.member_index, b[0].1.orig_user_meta),
            (2, 0, 200)
        );
        assert_eq!(b[0].2, b"b0");
    }

    /// Append `count` ~1 KiB records (absolute indices continue across calls) with rolling +
    /// retention, so old records prune once enough files accumulate.
    fn write_member_rolling(base: &Path, count: u64, roll: u64, keep: u32) {
        let mut b = WriterBuilder::new(base)
            .region_size(REGION)
            .file_roll_size(roll);
        if keep > 0 {
            b = b.keep_files(keep as u64);
        }
        let mut w = b.build().unwrap();
        let payload = vec![0xCDu8; 1024];
        for i in 0..count {
            let buf = w.try_reserve(payload.len()).unwrap();
            buf.copy_from_slice(&payload);
            w.commit(1, payload.len() as u32, i).unwrap();
        }
    }

    /// Append `n` records `<tag><i>` (indices continue across calls); drop the writer after.
    fn append_records(base: &Path, tag: &str, n: u64) {
        let mut w = WriterBuilder::new(base)
            .region_size(REGION)
            .build()
            .unwrap();
        for i in 0..n {
            let p = format!("{tag}{i}").into_bytes();
            let buf = w.try_reserve(p.len()).unwrap();
            buf.copy_from_slice(&p);
            w.commit(1, p.len() as u32, i).unwrap();
        }
    }

    /// Two distinct members reuse `member_ref` 0 across reopens (member "a" in session 1, "b"
    /// in session 2, because `next_ref` resets each open). Recovery must key cursors on
    /// (name, epoch) positionally — NOT max over the bare ref — or "b" inherits "a"'s far-ahead
    /// index and silently skips its own committed records. (Council-verified data-loss repro.)
    #[test]
    fn recovery_does_not_conflate_two_members_sharing_a_ref() {
        let dir = temp_dir("refswap");
        let (topic, a, b) = (dir.join("topic"), dir.join("a"), dir.join("b"));

        // Session 1: "a" (ref 0) merges 100 records (member_index 0..99).
        append_records(&a, "a", 100);
        {
            let mut mux = Mux::open(&topic, REGION_U32, 0, 4096).unwrap();
            mux.add_member("a", 0, &a).unwrap();
            assert_eq!(mux.poll().unwrap(), 100);
        }

        // Session 2: "a" is gone; "b" attaches and (next_ref reset) also gets ref 0, merging
        // its first 10 records (member_index 0..9).
        append_records(&b, "b", 10);
        {
            let mut mux = Mux::open(&topic, REGION_U32, 0, 4096).unwrap();
            mux.add_member("b", 0, &b).unwrap();
            assert_eq!(mux.poll().unwrap(), 10);
        }

        // "b" produces more (now member_index 0..149).
        append_records(&b, "b", 140);

        // Session 3: reopen and resume "b". Correct recovery resumes at 10; the bug resumes at
        // 100 (inheriting "a"'s max under ref 0) and skips b's records 10..99.
        {
            let mut mux = Mux::open(&topic, REGION_U32, 0, 4096).unwrap();
            mux.add_member("b", 0, &b).unwrap();
            mux.poll().unwrap();
        }

        // Every one of b's 150 records must appear exactly once, contiguously.
        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let mut b_indices = Vec::new();
        while let Some(m) = r.try_read().unwrap() {
            if is_control(m.header().message_type) {
                continue;
            }
            let (prov, body) = Provenance::split(m.payload()).unwrap();
            if body.starts_with(b"b") {
                b_indices.push(prov.member_index);
            }
        }
        b_indices.sort_unstable();
        let expected: Vec<u64> = (0..150).collect();
        assert_eq!(
            b_indices, expected,
            "b must not skip its own committed records"
        );
    }

    #[test]
    fn remove_member_drains_then_emits_member_closed() {
        let dir = temp_dir("close");
        let (topic, member) = (dir.join("topic"), dir.join("m"));
        write_member(&member, &[(1, 0, b"m0"), (1, 1, b"m1"), (1, 2, b"m2")]);

        let mut mux = Mux::open(&topic, REGION_U32, 0, 1).unwrap();
        mux.add_member("m", 0, &member).unwrap();
        // Merge only one (bounded batch = 1), leaving records behind.
        assert_eq!(mux.poll().unwrap(), 1);

        // Clean leave drains the rest (indices 1,2), then closes the slot.
        assert!(mux.remove_member("m", 0).unwrap());
        assert!(mux.members().is_empty());
        drop(mux);

        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let mut data = 0;
        let mut closed = None;
        while let Some(m) = r.try_read().unwrap() {
            match m.header().message_type {
                MSG_TYPE_MEMBER_CLOSED => closed = Some(MemberClosed::decode(m.payload()).unwrap()),
                mt if !is_control(mt) => data += 1,
                _ => {}
            }
        }
        assert_eq!(
            data, 3,
            "all three records were merged (one polled, two drained)"
        );
        let closed = closed.expect("a MemberClosed marker");
        assert_eq!(closed.final_index, 3, "drained to head (next index = 3)");
    }

    #[test]
    fn resume_retention_underrun_records_a_topic_gap() {
        let dir = temp_dir("gap");
        let (topic, member) = (dir.join("topic"), dir.join("m"));
        let roll = (REGION as u64) * 2;
        let keep = 1u32;

        // Member writes a few records; the mux merges them (cursor → 3).
        write_member_rolling(&member, 3, roll, keep);
        {
            let mut mux = Mux::open(&topic, REGION_U32, 0, 64).unwrap();
            mux.add_member("m", 0, &member).unwrap();
            assert_eq!(mux.poll().unwrap(), 3);
            assert_eq!(mux.members()[0].2, 3);
        }

        // While the mux is down, the member writes far more with tight retention, pruning the
        // unmerged records (index ≥ 3) below a new earliest.
        write_member_rolling(&member, 6000, roll, keep);
        let earliest = ReplicationSource::open(&member).unwrap().1.0;
        assert!(
            earliest > 3,
            "member should prune past the mux cursor (earliest={earliest})"
        );

        // Reopen: the mux can't resume contiguously, so it records a TopicGap and skips ahead.
        let mut mux = Mux::open(&topic, REGION_U32, 0, 64).unwrap();
        mux.add_member("m", 0, &member).unwrap();
        assert_eq!(
            mux.members()[0].2,
            earliest,
            "resumes at earliest after the gap"
        );
        drop(mux);

        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let mut gaps = Vec::new();
        while let Some(m) = r.try_read().unwrap() {
            if m.header().message_type == MSG_TYPE_TOPIC_GAP {
                gaps.push(TopicGap::decode(m.payload()).unwrap());
            }
        }
        assert_eq!(gaps.len(), 1, "exactly one attributed gap");
        assert_eq!(gaps[0].from, 3);
        assert_eq!(gaps[0].resumed_at, earliest);
    }

    #[test]
    fn fresh_member_with_pruned_genesis_records_a_gap() {
        let dir = temp_dir("freshgap");
        let (topic, m) = (dir.join("topic"), dir.join("m"));
        // Write enough with tight retention that genesis prunes away (earliest > 0) before the
        // mux ever sees the member.
        write_member_rolling(&m, 6000, (REGION as u64) * 2, 1);
        let earliest = ReplicationSource::open(&m).unwrap().1.0;
        assert!(
            earliest > 0,
            "genesis should have pruned (earliest={earliest})"
        );

        let mut mux = Mux::open(&topic, REGION_U32, 0, 64).unwrap(); // fresh topic, no cursor
        mux.add_member("m", 0, &m).unwrap();
        assert_eq!(
            mux.members()[0].2,
            earliest,
            "fresh member begins at earliest"
        );
        drop(mux);

        // The pruned prefix [0, earliest) is recorded as an attributed gap, not spliced away.
        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let mut gaps = Vec::new();
        while let Some(m) = r.try_read().unwrap() {
            if m.header().message_type == MSG_TYPE_TOPIC_GAP {
                gaps.push(TopicGap::decode(m.payload()).unwrap());
            }
        }
        assert_eq!(gaps.len(), 1);
        assert_eq!((gaps[0].from, gaps[0].resumed_at), (0, earliest));
    }

    #[test]
    fn recovers_cursors_after_restart_without_duplication() {
        let dir = temp_dir("recover");
        let (topic, m_a) = (dir.join("topic"), dir.join("a"));
        write_member(&m_a, &[(1, 0, b"a0"), (1, 1, b"a1")]);

        {
            let mut mux = Mux::open(&topic, REGION_U32, 0, 16).unwrap();
            mux.add_member("a", 0, &m_a).unwrap();
            assert_eq!(mux.poll().unwrap(), 2);
        }
        // The producer writes one more record while the mux is down.
        write_member(&m_a, &[(1, 2, b"a2")]);

        // Reopen: recovery must resume "a" at index 2, not re-merge 0,1.
        let mut mux = Mux::open(&topic, REGION_U32, 0, 16).unwrap();
        mux.add_member("a", 0, &m_a).unwrap();
        assert_eq!(mux.members()[0].2, 2, "resumes at recovered cursor");
        assert_eq!(mux.poll().unwrap(), 1, "only the new record merges");
        drop(mux);

        let (data, _) = read_topic(&topic);
        let indices: Vec<u64> = data.iter().map(|(_, p, _)| p.member_index).collect();
        assert_eq!(indices, vec![0, 1, 2], "no duplicates, contiguous resume");
    }

    /// COUNCIL FINDING (do-not-ship): `recover_cursors` conflates records that share a
    /// reused `member_ref` across mux reopens, silently dropping a member's committed data
    /// on restart.
    ///
    /// Root cause: `member_ref` is a per-session counter reset to 0 on every `Mux::open`
    /// (`next_ref: 0`), while the topic log persists across reopens. `Provenance` carries no
    /// epoch, and `recover_cursors` takes `max(member_index)` over the *bare* `member_ref`
    /// across the whole log (mux.rs:543), then attributes that max via the *latest* slot
    /// table (mux.rs:536-540). So once ref 0 has denoted two incarnations, the survivor
    /// inherits the other's max and resumes past its own cursor — records skipped, no
    /// `TopicGap` (the gap check at mux.rs:361 fires only on retention underrun).
    ///
    /// This repro removes ordering nondeterminism entirely: a single member "a" reclaimed at
    /// a new epoch (the supported tombstone→reclaim lifecycle, §3.2/§6.1) on a fresh log. It
    /// asserts the CORRECT behavior and therefore FAILS on current HEAD (resumes at 5, not 2).
    ///
    /// The fix must key recovery on `(name, epoch)` positionally (track the active slot table
    /// as the tail is scanned), or make `member_ref` durable across reopens (seed `next_ref`
    /// above every ref ever written). Note: had epoch2's head been *below* the conflated
    /// cursor, `add_member` would instead **hang forever** in `skip_to` (`read_blocking(None)`
    /// past head) — a second, worse manifestation of the same defect.
    #[test]
    fn recovery_must_not_conflate_reused_member_ref_across_incarnations() {
        let dir = temp_dir("conflate");
        let topic = dir.join("topic");
        let a1 = dir.join("a-inc1"); // incarnation epoch=1's channel
        let a2 = dir.join("a-inc2"); // incarnation epoch=2's channel — a fresh log, restarts at index 0

        // Boot 1: member "a"@epoch1 merges indices 0..=4. First attach ⇒ member_ref 0.
        write_member(
            &a1,
            &[
                (1, 0, b"e1-0"),
                (1, 1, b"e1-1"),
                (1, 2, b"e1-2"),
                (1, 3, b"e1-3"),
                (1, 4, b"e1-4"),
            ],
        );
        {
            let mut mux = Mux::open(&topic, REGION_U32, 0, 64).unwrap();
            mux.add_member("a", 1, &a1).unwrap();
            assert_eq!(mux.poll().unwrap(), 5);
        }

        // Boot 2: "a" is reclaimed at epoch2 on a fresh log and merges only indices 0,1 so far.
        // `next_ref` reset to 0 ⇒ epoch2 reuses member_ref 0. The topic now holds ref-0 data
        // from BOTH incarnations, and the latest slot table maps ref 0 → (a, epoch 2).
        write_member(&a2, &[(1, 0, b"e2-0"), (1, 1, b"e2-1")]);
        {
            let mut mux = Mux::open(&topic, REGION_U32, 0, 64).unwrap();
            mux.add_member("a", 2, &a2).unwrap();
            assert_eq!(
                mux.poll().unwrap(),
                2,
                "epoch2 merges its first two records"
            );
            assert_eq!(
                mux.members()[0].2,
                2,
                "epoch2 cursor is 2 before the restart"
            );
        }

        // epoch2 keeps producing while the mux is down: indices 2..=7 (its head becomes 8).
        write_member(
            &a2,
            &[
                (1, 2, b"e2-2"),
                (1, 3, b"e2-3"),
                (1, 4, b"e2-4"),
                (1, 5, b"e2-5"),
                (1, 6, b"e2-6"),
                (1, 7, b"e2-7"),
            ],
        );

        // Boot 3: recover "a"@epoch2's cursor. It truly merged through index 1, so it must
        // resume at 2 and merge 2..=7. The bug: recovery maxes ref-0 across BOTH incarnations
        // (max = 4, from epoch1) and resumes epoch2 at 5, silently dropping indices 2,3,4.
        let mut mux = Mux::open(&topic, REGION_U32, 0, 64).unwrap();
        mux.add_member("a", 2, &a2).unwrap();
        assert_eq!(
            mux.members()[0].2,
            2,
            "epoch2 must resume at its own cursor (2), not epoch1's conflated max+1 (5)"
        );
        assert_eq!(
            mux.poll().unwrap(),
            6,
            "indices 2..=7 must all merge; conflation drops 2,3,4 and merges only 5,6,7"
        );
    }
}
