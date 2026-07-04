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
        let recovered = self.recovered.get(&(name.to_string(), epoch)).copied();

        // Where to resume. A previously-merged member resumes at `recovered + 1`; a fresh one
        // starts at genesis. Either way, if the member has aged records out of retention below
        // our resume point, we can't extend contiguously.
        let want = recovered.map(|max| max + 1).unwrap_or(0);
        let gap = recovered.is_some() && want < earliest.0;
        let start = want.max(earliest.0);

        source.skip_to(RecordIndex(start))?;
        self.slots.push(Slot {
            member_ref,
            name: name.to_string(),
            epoch,
            source,
            cursor: start,
        });
        self.emit_slot_table()?;
        // Retention underrun on resume: records `[want, earliest)` are gone. Record it
        // explicitly and attributed (§6.2) rather than silently jumping the merge forward.
        if gap {
            self.emit_topic_gap(member_ref, want, earliest.0)?;
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
                let Some(frame) = self.slots[i].source.try_next_frame()? else {
                    break;
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
                merged += 1;
            }
        }
        Ok(merged)
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
    let mut ref_to_member: HashMap<u16, (String, u64)> = HashMap::new();
    let mut max_index: HashMap<u16, u64> = HashMap::new();
    while let Some(m) = reader.try_read()? {
        let mt = m.header().message_type;
        if mt == MSG_TYPE_SLOT_TABLE {
            // The latest slot table wins the ref → member mapping.
            ref_to_member = decode_slot_table(m.payload())?
                .into_iter()
                .map(|e| (e.member_ref, (e.name, e.epoch)))
                .collect();
        } else if !is_control(mt) {
            let (prov, _) = Provenance::split(m.payload())?;
            let e = max_index.entry(prov.member_ref).or_insert(0);
            *e = (*e).max(prov.member_index);
        }
    }
    let mut out = HashMap::new();
    for (member_ref, idx) in max_index {
        if let Some(member) = ref_to_member.get(&member_ref) {
            out.insert(member.clone(), idx);
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
}
