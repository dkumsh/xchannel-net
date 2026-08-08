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

/// The mux re-emits the slot table at least once per this many merged records, even without a
/// membership change or a roll. A **secondary** bound on staleness, for the §6.3 promise that a
/// `LateJoin` consumer can always decode provenance; the load-bearing guarantee is the
/// per-segment one below.
pub const SLOT_TABLE_REFRESH: u64 = 4096;

/// Estimated per-record framing overhead in a segment (xchannel's `MessageHeader` is 16 bytes and
/// records are 8-aligned). Used only to decide when to roll, so an inexact figure changes how
/// closely a segment tracks `file_roll_size` — never correctness.
const RECORD_OVERHEAD: u64 = 16;

/// Slot-table record: `member_ref → (name, epoch)` for every current member (§6.3).
pub const MSG_TYPE_SLOT_TABLE: u16 = 0xFFFF;
/// `TopicGap` control record: a member fell behind retention; the hole is attributed (§6.2).
pub const MSG_TYPE_TOPIC_GAP: u16 = 0xFFFE;
/// `MemberClosed` control record: a member was drained and its slot closed (§6.1).
pub const MSG_TYPE_MEMBER_CLOSED: u16 = 0xFFFD;
/// Terminal marker: the topic was retired; the mux drained all members and stopped (§4.1).
pub const MSG_TYPE_TERMINAL: u16 = 0xFFFC;
/// `MemberRegressed` control record: a member's recovered cursor was past its source head — the
/// member reset/respawned under the same `(name, epoch)` (an upstream §3.2 violation).
pub const MSG_TYPE_MEMBER_REGRESSED: u16 = 0xFFFB;

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

/// Body of a [`MSG_TYPE_MEMBER_REGRESSED`] control record: on resume the recovered cursor
/// (`expected`) was **past** member `member_ref`'s current source head (`head`) — the member's
/// log went backwards (reset/respawn under the same `(name, epoch)`, an upstream §3.2 anti-splice
/// violation). The mux resumed at `head` rather than skipping past it. Distinct from
/// [`TopicGap`] (whose `[from, resumed_at)` is a forward hole); here `expected > head`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MemberRegressed {
    pub member_ref: u16,
    pub expected: u64,
    pub head: u64,
}

impl MemberRegressed {
    /// Serialize to the 18-byte control-record payload (`u16 member_ref, u64 expected, u64 head`).
    pub fn to_payload(&self) -> [u8; 18] {
        let mut b = [0u8; 18];
        b[0..2].copy_from_slice(&self.member_ref.to_le_bytes());
        b[2..10].copy_from_slice(&self.expected.to_le_bytes());
        b[10..18].copy_from_slice(&self.head.to_le_bytes());
        b
    }

    /// Parse a `MemberRegressed` control-record payload.
    pub fn decode(b: &[u8]) -> io::Result<MemberRegressed> {
        if b.len() < 18 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MemberRegressed record too short",
            ));
        }
        Ok(MemberRegressed {
            member_ref: u16::from_le_bytes(b[0..2].try_into().unwrap()),
            expected: u64::from_le_bytes(b[2..10].try_into().unwrap()),
            head: u64::from_le_bytes(b[10..18].try_into().unwrap()),
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

/// The topic channel's own writer configuration: xchannel geometry **and** its disk bounds.
///
/// These four travel together everywhere — building the topic `Writer`, riding the slot table so
/// the topic self-describes how to reopen it (`doc/RESTART.md`), and being advertised to
/// subscribers so their replicas inherit the same bounds. Splitting them is what let
/// `file_roll_size`/`keep_files` be dropped on the floor: they are *writer-instance* state in
/// xchannel, not header fields, so a topic whose writer is rebuilt without them (every
/// `Mux::open`, including the one right after `create_topic` precreated the file *with* them)
/// silently never rolls and never prunes.
// No `Default`: a zero `region_size` cannot open a writer, so a defaulted value would only ever
// be a bug. Build one from `ChannelOptions` (which has a real default) or from a slot table.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TopicGeometry {
    pub region_size: u32,
    /// Max payload; `0` = unlimited.
    pub mtu: u32,
    /// Roll to a new segment past this many bytes; `0` = roll only when asked.
    pub file_roll_size: u64,
    /// Retain at most this many segments; `0` = keep everything.
    pub keep_files: u32,
}

impl From<&crate::wire::ChannelOptions> for TopicGeometry {
    /// A topic channel is an ordinary channel, so the options a client asks for *are* its writer
    /// configuration — all four fields, not just the two that live in the header.
    fn from(o: &crate::wire::ChannelOptions) -> Self {
        Self {
            region_size: o.region_size,
            mtu: o.mtu,
            file_roll_size: o.file_roll_size,
            keep_files: o.keep_files,
        }
    }
}

/// One slot-table entry: which `(name, epoch)` a `member_ref` denotes, and how far that member
/// had been merged when the table was written. `epoch` is the member channel's registry
/// generation — the incarnation, so a respawned producer (new epoch) is a distinct slot and never
/// spliced onto the old one (TOPICS §3.2, decision: incarnation = epoch).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SlotEntry {
    pub member_ref: u16,
    pub name: String,
    pub epoch: u64,
    /// The member's merge cursor — the next member index to merge, i.e. `Slot::cursor`. Carried
    /// so recovery survives the topic's *own* retention: once a topic rolls and prunes (which it
    /// does as soon as `keep_files` is set), a member that has been quiet long enough for all of
    /// its data records to age out would otherwise resolve to "never seen" and be re-merged from
    /// scratch. The periodic re-emit ([`SLOT_TABLE_REFRESH`]) guarantees a recent table is always
    /// retained, so every member's cursor is too.
    pub cursor: u64,
}

/// A decoded slot table: the topic channel's writer configuration (so restart reconstruction can
/// reopen it without a persisted marker — `doc/RESTART.md`) plus its current members and their
/// merge cursors.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SlotTable {
    pub geometry: TopicGeometry,
    pub members: Vec<SlotEntry>,
}

/// Wire version of the slot-table payload. Bumped when the layout changes; a table written by a
/// different version is **refused** rather than misread, because misreading one silently
/// misattributes provenance — the failure class of the `member_ref` conflation bug.
///
/// Version 1 (0.1.0) had no version byte and began with `u32 region_size`, whose low byte is `0`
/// for any page-multiple region size, so a v1 table cannot be mistaken for a v2 one.
const SLOT_TABLE_VERSION: u8 = 2;

/// Encode a slot table as a control-record payload:
/// `u8 version, u32 region_size, u32 mtu, u64 file_roll_size, u32 keep_files, u16 count`, then
/// per entry `u16 member_ref, u64 epoch, u64 cursor, u16 name_len, name`. The topic's writer
/// configuration rides the table (which is periodically re-emitted, so a recent copy is always
/// retained) so a topic self-describes how to reopen it.
pub fn encode_slot_table(geometry: &TopicGeometry, entries: &[SlotEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(SLOT_TABLE_VERSION);
    out.extend_from_slice(&geometry.region_size.to_le_bytes());
    out.extend_from_slice(&geometry.mtu.to_le_bytes());
    out.extend_from_slice(&geometry.file_roll_size.to_le_bytes());
    out.extend_from_slice(&geometry.keep_files.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    for e in entries {
        out.extend_from_slice(&e.member_ref.to_le_bytes());
        out.extend_from_slice(&e.epoch.to_le_bytes());
        out.extend_from_slice(&e.cursor.to_le_bytes());
        let name = e.name.as_bytes();
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
    }
    out
}

/// Decode a slot-table control-record payload produced by [`encode_slot_table`]. Refuses a
/// payload written by a different [`SLOT_TABLE_VERSION`].
pub fn decode_slot_table(mut b: &[u8]) -> io::Result<SlotTable> {
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
    let version = take(&mut b, 1)?[0];
    if version != SLOT_TABLE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("slot table version {version} != supported {SLOT_TABLE_VERSION}"),
        ));
    }
    let region_size = u32::from_le_bytes(take(&mut b, 4)?.try_into().unwrap());
    let mtu = u32::from_le_bytes(take(&mut b, 4)?.try_into().unwrap());
    let file_roll_size = u64::from_le_bytes(take(&mut b, 8)?.try_into().unwrap());
    let keep_files = u32::from_le_bytes(take(&mut b, 4)?.try_into().unwrap());
    let count = u16::from_le_bytes(take(&mut b, 2)?.try_into().unwrap()) as usize;
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        let member_ref = u16::from_le_bytes(take(&mut b, 2)?.try_into().unwrap());
        let epoch = u64::from_le_bytes(take(&mut b, 8)?.try_into().unwrap());
        let cursor = u64::from_le_bytes(take(&mut b, 8)?.try_into().unwrap());
        let name_len = u16::from_le_bytes(take(&mut b, 2)?.try_into().unwrap()) as usize;
        let name = std::str::from_utf8(take(&mut b, name_len)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "slot name not UTF-8"))?
            .to_string();
        members.push(SlotEntry {
            member_ref,
            name,
            epoch,
            cursor,
        });
    }
    Ok(SlotTable {
        geometry: TopicGeometry {
            region_size,
            mtu,
            file_roll_size,
            keep_files,
        },
        members,
    })
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
    /// Member records dropped for using a reserved control `msg_type` (contract violation —
    /// see `merge_one`). Surfaced in [`MemberStat`] for observability.
    rejected: u64,
}

/// A **mux**: the single writer of one topic channel, merging N member channels into it in
/// arrival order with mandatory provenance (`doc/TOPICS.md` §4). This is the transport-free
/// engine (plain xchannel readers + one writer), so it runs equally as a poll-item in the
/// daemon's forwarding loop or inside a standalone host (§4.1). Local members only for now;
/// a remote member is just a local replica of it, consumed identically (Phase 2).
///
/// The mux persists nothing of its own: per-member cursors are recovered on restart by scanning
/// the topic's own tail (§5), since every record self-describes its origin — and, for a member
/// whose records have aged out of the topic's retention, from the cursor its slot table carries.
pub struct Mux {
    topic: Writer,
    /// The topic channel's writer configuration, embedded in every slot table so the topic
    /// self-describes how to reopen it on restart (`doc/RESTART.md`).
    geometry: TopicGeometry,
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
    /// Records merged since the last slot-table emission; drives the [`SLOT_TABLE_REFRESH`]
    /// secondary re-emit.
    since_slot_table: u64,
    /// Approximate bytes committed into the current segment. The **mux drives its own rolling**
    /// (rather than letting the writer's `file_roll_size` do it) for one reason: it must emit a
    /// slot table at the *head of every segment*, and xchannel's `Writer` exposes no way to
    /// observe a roll it performed internally. That placement is what makes "a slot table is
    /// always inside the retained window" exact — retention prunes whole *segments*, so a table
    /// at each segment head survives any `keep_files ≥ 1`. A record-counted refresh alone cannot
    /// promise that: [`SLOT_TABLE_REFRESH`] counts records while retention counts bytes, so a
    /// window narrower than the refresh interval can hold no table at all.
    ///
    /// Reset to 0 on reopen even though the segment may be partly full, so the first segment after
    /// a restart can reach ~2× `file_roll_size`. Bounded, and the alternative needs a byte-offset
    /// accessor xchannel does not expose.
    bytes_in_segment: u64,
    /// Set once [`finish`](Self::finish) has written the terminal marker. A retired mux is
    /// **inert** — it merges nothing more and accepts no members.
    ///
    /// This is what makes "nothing is committed after the terminal marker" an invariant of the mux
    /// rather than of whoever happens to be holding it. It matters as soon as a mux can be shared:
    /// a poll loop that sampled the topic set just before retirement still holds a handle, and
    /// without this would merge records *past* the marker that says the topic ended.
    finished: bool,
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
    /// Records dropped for using a reserved control `msg_type` (contract violations).
    pub rejected: u64,
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
    /// Open (or reopen) the mux for a topic channel at `path`, configured by `geometry`. Recovers
    /// per-member cursors from any existing topic content first, then opens the topic writer for
    /// append. A member data record must use a `msg_type` below [`RESERVED_MSG_TYPE_MIN`] (the
    /// reserved range is the mux's control records) — that is the member contract, not enforced
    /// here.
    ///
    /// `geometry.file_roll_size`/`keep_files` must be passed on **every** open, not just the
    /// first: xchannel holds them on the `Writer`, not in the channel header, so a reopen that
    /// omits them produces a topic that never rolls and never prunes however it was created. They
    /// ride the slot table precisely so a restart can supply them again
    /// ([`topic_config`]).
    pub fn open(
        path: &Path,
        geometry: &TopicGeometry,
        max_batch_per_member: usize,
    ) -> io::Result<Self> {
        let recovered = match recover_cursors(path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e),
        };
        // `file_roll_size` is deliberately *not* handed to the writer: the mux rolls explicitly so
        // it can put a slot table at each segment head (see `bytes_in_segment`). `keep_files` is
        // the writer's job — retention sweeps on roll, whoever triggered it.
        let mut builder = WriterBuilder::new(path)
            .region_size(geometry.region_size as usize)
            .mtu(geometry.mtu as u64);
        if geometry.keep_files > 0 {
            builder = builder.keep_files(geometry.keep_files as u64);
        }
        let topic = builder.build()?;
        Ok(Self {
            topic,
            geometry: *geometry,
            slots: Vec::new(),
            next_ref: 0,
            max_batch_per_member: max_batch_per_member.max(1),
            recovered,
            gaps_emitted: 0,
            slot_table_version: 0,
            since_slot_table: 0,
            bytes_in_segment: 0,
            finished: false,
        })
    }

    /// Commit one record into the topic and account its size against the current segment. The
    /// single commit path, so nothing can write to the topic without being counted (which would
    /// silently defeat rolling) — and so the reserve/copy/commit triple appears once.
    fn commit_record(&mut self, msg_type: u16, payload: &[u8]) -> io::Result<()> {
        let buf = self.topic.try_reserve(payload.len())?;
        buf.copy_from_slice(payload);
        self.topic.commit(msg_type, payload.len() as u32, 0)?;
        self.bytes_in_segment += RECORD_OVERHEAD + payload.len().next_multiple_of(8) as u64;
        Ok(())
    }

    /// Roll to a new segment if the current one has reached `file_roll_size`, and start the new
    /// segment with a slot table. Called only before committing a *data* record, so a segment
    /// always opens with a table and control records never trigger a roll on their own.
    fn roll_if_full(&mut self) -> io::Result<()> {
        if self.geometry.file_roll_size == 0 || self.bytes_in_segment < self.geometry.file_roll_size
        {
            return Ok(());
        }
        self.topic.roll_file()?;
        self.bytes_in_segment = 0;
        self.emit_slot_table()
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
                rejected: s.rejected,
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
        if self.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mux is retired (terminal marker written) — cannot attach a member",
            ));
        }
        // Idempotent: attaching an already-attached (name, epoch) is a no-op, so the publish
        // path and the discovery loop can both call it safely.
        if self.has_member(name, epoch) {
            return Ok(());
        }
        // `member_ref` is a u16 slot index; exhausting it in a single session would wrap and
        // collide (two live members sharing a ref breaks provenance + recovery). Fail loudly
        // instead — a restart resets the counter and recovery re-keys on (name, epoch).
        let member_ref = self.next_ref;
        self.next_ref = self.next_ref.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "mux member_ref space exhausted (65536 attaches this session) — restart the mux",
            )
        })?;
        let (mut source, earliest) = ReplicationSource::open(member_path)?;
        let head = source.head()?.0;
        let recovered = self.recovered.get(&(name.to_string(), epoch)).copied();

        // Where to resume, and whether a hole must be recorded. `want` is the next index we
        // believe we need: `recovered + 1` on resume, else genesis. We never skip_to past a
        // hole or past the source head silently (§6.2) — every discontinuity is an attributed
        // `TopicGap`. `gap = Some((from, resumed_at))`.
        let want = recovered.map(|max| max + 1).unwrap_or(0);
        // What to record for a discontinuity, if any.
        enum Mark {
            None,
            /// Forward hole `[from, resumed_at)` aged out (§6.2).
            Gap(u64, u64),
            /// Cursor `expected` past source `head` — the member's log regressed (§3.2 violation).
            Regressed(u64, u64),
        }
        let (start, mark) = if want < earliest.0 {
            // Records `[want, earliest)` aged out of retention (or, for a fresh member, its
            // genesis was pruned before we ever saw it: `want = 0 < earliest`). Resume at
            // `earliest`, attributing the hole.
            (earliest.0, Mark::Gap(want, earliest.0))
        } else if want > head {
            // Our cursor is past the source's head: the member reset/respawned under the same
            // `(name, epoch)` (an upstream §3.2 anti-splice violation). Never skip_to past
            // `head` — that would hang or strand the new incarnation's records. Resume at
            // `head` and record it as a distinct backwards-regression marker (not a `TopicGap`,
            // whose interval is forward).
            (head, Mark::Regressed(want, head))
        } else {
            (want, Mark::None)
        };

        source.skip_to(RecordIndex(start))?;
        self.slots.push(Slot {
            member_ref,
            name: name.to_string(),
            epoch,
            source,
            cursor: start,
            rejected: 0,
        });
        self.emit_slot_table()?;
        match mark {
            Mark::None => {}
            Mark::Gap(from, resumed_at) => self.emit_topic_gap(member_ref, from, resumed_at)?,
            Mark::Regressed(expected, head) => {
                self.emit_member_regressed(member_ref, expected, head)?
            }
        }
        Ok(())
    }

    /// Merge whatever is currently ready from each member into the topic, in arrival order,
    /// bounded to `max_batch_per_member` records per member per call so one hot member cannot
    /// monopolize the interleave (§4.3). Returns the number of records merged this call.
    pub fn poll(&mut self) -> io::Result<usize> {
        if self.finished {
            return Ok(0); // retired: never commit past the terminal marker
        }
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
    ///
    /// Two outcomes other than "merged", and the difference between them matters:
    ///
    /// * **Rejected** — the record can *never* be merged into this topic, so it is dropped, the
    ///   `rejected` counter bumps, the cursor advances past it, and the topic keeps moving. The
    ///   loss is visible: provenance makes the missing `member_index` a hole a consumer can see.
    ///   Stalling the whole topic forever on one unacceptable record would be worse.
    /// * **Errored** — the commit failed for a reason that may not repeat (mapping, disk). The
    ///   cursor is **not** advanced, so the topic's own log stays the truth about what it holds
    ///   and a restart re-reads from the right place.
    ///
    /// A member's [`starts_segment`](crate::wire::RecordFrame::starts_segment) is deliberately
    /// ignored: it is advisory, and members' file boundaries carry no meaning for a topic whose
    /// records interleave many of them. The topic rolls on its own geometry.
    fn merge_one(&mut self, i: usize) -> io::Result<bool> {
        loop {
            let Some(frame) = self.slots[i].source.try_next_frame()? else {
                return Ok(false);
            };

            // **Contract enforcement (never trust the member's `msg_type`).** A member data
            // record must use a `msg_type` below `RESERVED_MSG_TYPE_MIN`. Committing a member
            // frame whose type is in the reserved range verbatim would **forge a mux control
            // record** (e.g. a slot table) into the authoritative topic log — poisoning recovery
            // and even rewriting the topic geometry on the next restart. Reject it.
            if is_control(frame.msg_type) {
                self.slots[i].rejected += 1;
                self.slots[i].cursor = frame.index.0 + 1;
                continue;
            }

            let prov = Provenance {
                member_ref: self.slots[i].member_ref,
                member_index: frame.index.0,
                orig_user_meta: frame.user_meta,
            };
            let payload = frame_data(prov, &frame.payload);

            // A record that does not fit the topic's `mtu` is the same kind of permanent contract
            // violation as a reserved `msg_type` — the provenance prefix pushes a member record
            // that only just fitted its *own* channel over the topic's limit. Checked here rather
            // than left to `try_reserve`, because an error would be indistinguishable from a
            // transient one: the loop would retry forever, or (worse) advance past a different
            // record each round and quietly shred the member's stream.
            if self.geometry.mtu > 0 && payload.len() as u64 > self.geometry.mtu as u64 {
                self.slots[i].rejected += 1;
                self.slots[i].cursor = frame.index.0 + 1;
                continue;
            }

            // Roll *before* the record, so the slot table that opens the new segment precedes any
            // data in it and every retained window therefore contains one.
            self.roll_if_full()?;
            self.commit_record(frame.msg_type, &payload)?;
            // Only once the record is durably in the topic. Advancing before the commit would let
            // a failed commit consume a record the topic does not hold — the cursor claiming
            // progress that no reader can see, which is the shape of the conflation bug in
            // `recover_cursors`, arrived at from the other direction.
            self.slots[i].cursor = frame.index.0 + 1;
            // Secondary staleness bound, for a topic that never rolls — see SLOT_TABLE_REFRESH.
            self.since_slot_table += 1;
            if self.since_slot_table >= SLOT_TABLE_REFRESH {
                self.emit_slot_table()?;
            }
            return Ok(true);
        }
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
        if self.finished {
            return Ok(()); // idempotent — one terminal marker, not one per caller
        }
        let members: Vec<(String, u64)> = self
            .slots
            .iter()
            .map(|s| (s.name.clone(), s.epoch))
            .collect();
        for (name, epoch) in members {
            self.remove_member(&name, epoch)?;
        }
        self.commit_record(MSG_TYPE_TERMINAL, &[])?;
        self.finished = true;
        Ok(())
    }

    /// Commit a [`MemberClosed`] control record.
    fn emit_member_closed(&mut self, member_ref: u16, final_index: u64) -> io::Result<()> {
        let payload = MemberClosed {
            member_ref,
            final_index,
        }
        .to_payload();
        self.commit_record(MSG_TYPE_MEMBER_CLOSED, &payload)
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
        self.commit_record(MSG_TYPE_TOPIC_GAP, &payload)?;
        self.gaps_emitted += 1;
        Ok(())
    }

    /// Commit a [`MemberRegressed`] control record (source head went backwards on resume).
    fn emit_member_regressed(
        &mut self,
        member_ref: u16,
        expected: u64,
        head: u64,
    ) -> io::Result<()> {
        let payload = MemberRegressed {
            member_ref,
            expected,
            head,
        }
        .to_payload();
        self.commit_record(MSG_TYPE_MEMBER_REGRESSED, &payload)
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
                cursor: s.cursor,
            })
            .collect();
        let payload = encode_slot_table(&self.geometry, &entries);
        self.commit_record(MSG_TYPE_SLOT_TABLE, &payload)?;
        self.slot_table_version += 1;
        self.since_slot_table = 0;
        Ok(())
    }
}

/// Recover per-`(name, epoch)` highest merged member-index by scanning a topic channel's tail
/// (§5). Provenance makes cursors data-durable in the topic itself — no sidecar. Returns an
/// empty map if the topic doesn't exist yet.
///
/// Phase 1 scans from genesis for simplicity; §5's bounded scan (stop at the last slot-table
/// record once every current member is seen) is a later optimization.
/// The information restart reconstruction needs to re-host a topic: its writer configuration and
/// its last-known members. `topic_config` returns `Some` iff the channel at `path` is a topic (its
/// content carries a decodable `SlotTable` — the self-describing marker, no persisted flag).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopicConfig {
    /// Geometry **and disk bounds** — pass this straight back to [`Mux::open`], and advertise the
    /// bounds to subscribers, so a re-hosted topic rolls and prunes exactly as it did before the
    /// restart.
    pub geometry: TopicGeometry,
    /// `(name, epoch)` of every member in the most recent slot table (may be empty).
    pub members: Vec<(String, u64)>,
}

/// Identify whether the channel at `path` is a **topic** and, if so, return its writer
/// configuration + last membership (`doc/RESTART.md`, option (a) content-sniff). `Some` iff the
/// channel carries a decodable `SlotTable` record; `None` for an ordinary channel, a topic that
/// never had a member (no slot table), or a topic whose tables are all of an unsupported
/// [`SLOT_TABLE_VERSION`]. Used by restart reconstruction to re-host topics and re-attach members.
pub fn topic_config(path: &Path) -> io::Result<Option<TopicConfig>> {
    let mut reader = ReaderBuilder::new(path).late_join().build()?;
    let mut cfg: Option<TopicConfig> = None;
    while let Some(m) = reader.try_read()? {
        if m.header().message_type == MSG_TYPE_SLOT_TABLE
            && let Ok(table) = decode_slot_table(m.payload())
        {
            cfg = Some(TopicConfig {
                geometry: table.geometry,
                members: table
                    .members
                    .into_iter()
                    .map(|e| (e.name, e.epoch))
                    .collect(),
            });
        }
    }
    Ok(cfg)
}

/// **Invariant: this scan MUST see every retained record, in order, from the start of the retained
/// window.** Recovery attributes a member's max index by resolving its ref through the slot table
/// *in force at each record*, so it must observe both a member's data records and the slot table
/// that maps its ref. A naive mid-log start is the **dual of the member_ref conflation bug**: a
/// live-but-quiet member whose records sit before the start point would resolve to `None` →
/// treated as fresh → `want = 0` → its retained records **silently re-merged (duplication)**.
/// `recovery_quiet_member_far_behind_tail_is_not_duplicated` guards this.
///
/// The window is the *retained* one, not true genesis — a topic with `keep_files` set prunes its
/// own tail, which is why every slot table carries each member's [`cursor`](SlotEntry::cursor).
/// A member's cursor therefore survives its data records aging out, and the periodic re-emit
/// ([`SLOT_TABLE_REFRESH`]) is what guarantees a table is always inside the window.
/// `a_quiet_members_cursor_survives_the_topics_own_retention` guards this.
///
/// This is also what a future bounded scan (§5.2) can key on: starting at the last retained slot
/// table is sound *because* that table names every member and its cursor — but the bound must
/// still be proven against both guard tests above.
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
            let table = decode_slot_table(m.payload())?;
            active = table
                .members
                .iter()
                .map(|e| (e.member_ref, (e.name.clone(), e.epoch)))
                .collect();
            // A table's cursors **overwrite** what the scan has accumulated, rather than being
            // maxed into it: they are authoritative as of this position, and a cursor can legally
            // move *backwards* here — `MemberRegressed` resets a member that restarted onto a
            // shorter log under the same `(name, epoch)`. Taking a max would resurrect the stale
            // higher cursor and skip the new log's records, which is the very bug this function
            // was rewritten to fix. `cursor == 0` means nothing merged yet, which is "absent".
            for e in &table.members {
                let key = (e.name.clone(), e.epoch);
                match e.cursor.checked_sub(1) {
                    Some(highest_merged) => out.insert(key, highest_merged),
                    None => out.remove(&key),
                };
            }
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
                cursor: 0,
            },
            SlotEntry {
                member_ref: 1,
                name: "md.msft".to_string(),
                epoch: 3,
                cursor: 4_000_000_000,
            },
        ];
        // Disk bounds ride the table alongside the geometry: without them a restart reopens the
        // topic writer with no rolling and no retention, whatever it was created with.
        let geometry = TopicGeometry {
            region_size: 1 << 20,
            mtu: 7,
            file_roll_size: 64 << 20,
            keep_files: 3,
        };
        let encoded = encode_slot_table(&geometry, &entries);
        let table = decode_slot_table(&encoded).unwrap();
        assert_eq!(table.geometry, geometry);
        assert_eq!(table.members, entries);
    }

    /// A slot table from a different wire version is **refused**, not misread: silently decoding
    /// one at the wrong offsets misattributes `member_ref → (name, epoch)`, which is the failure
    /// class of the conflation bug below. 0.1.0's table had no version byte and began with
    /// `u32 region_size`, whose low byte is 0 for any page-multiple region.
    #[test]
    fn a_slot_table_of_another_version_is_refused() {
        let mut encoded = encode_slot_table(&geom(REGION_U32, 0), &[]);
        encoded[0] = SLOT_TABLE_VERSION.wrapping_add(1);
        let err = decode_slot_table(&encoded).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("version"), "{err}");

        // A v1 (0.1.0) table: no version byte, `u32 region_size` first.
        let mut v1 = Vec::new();
        v1.extend_from_slice(&REGION_U32.to_le_bytes());
        v1.extend_from_slice(&0u32.to_le_bytes());
        v1.extend_from_slice(&0u16.to_le_bytes());
        assert!(decode_slot_table(&v1).is_err(), "a v1 table is not misread");
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

    /// Topic writer config with no rolling or retention — the default for tests that don't
    /// exercise the topic's own disk bounds.
    fn geom(region_size: u32, mtu: u32) -> TopicGeometry {
        TopicGeometry {
            region_size,
            mtu,
            file_roll_size: 0,
            keep_files: 0,
        }
    }

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

        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 16).unwrap();
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
    /// A buggy/hostile member writing a record with a reserved control `msg_type` (e.g.
    /// `MSG_TYPE_SLOT_TABLE`) must NOT forge a control record into the topic: the mux drops it
    /// (counting it) and merges the member's valid records around it. Recovery/topic geometry
    /// stay intact.
    #[test]
    fn member_reserved_msg_type_is_rejected_not_forged() {
        let dir = temp_dir("forge");
        let (topic, m) = (dir.join("topic"), dir.join("m"));

        // A valid record, then a forged "slot table" (reserved type) with a slot-table-shaped
        // payload, then another valid record.
        let forged = encode_slot_table(&geom(1, 1), &[]);
        {
            let mut w = WriterBuilder::new(&m).region_size(REGION).build().unwrap();
            for (mt, body) in [
                (1u16, b"a".as_slice()),
                (MSG_TYPE_SLOT_TABLE, forged.as_slice()),
                (1u16, b"c".as_slice()),
            ] {
                let buf = w.try_reserve(body.len()).unwrap();
                buf.copy_from_slice(body);
                w.commit(mt, body.len() as u32, 0).unwrap();
            }
        }

        let (region, mtu) = (REGION_U32, 4096u32);
        let mut mux = Mux::open(&topic, &geom(region, mtu), 4096).unwrap();
        mux.add_member("m", 0, &m).unwrap();
        mux.poll().unwrap();
        assert_eq!(
            mux.status().unwrap().members[0].rejected,
            1,
            "the forged record was dropped"
        );
        drop(mux);

        // The topic's geometry is the mux's own, NOT the forged slot table's (region/mtu 1) —
        // the forgery never entered the log.
        let cfg = topic_config(&topic).unwrap().expect("a topic");
        assert_eq!(
            cfg.geometry,
            geom(region, mtu),
            "geometry not rewritten by forgery"
        );

        // Only the two valid records are present as data; no forged member_index gap-fill.
        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let mut data = Vec::new();
        while let Some(msg) = r.try_read().unwrap() {
            if is_control(msg.header().message_type) {
                continue;
            }
            let (_, body) = Provenance::split(msg.payload()).unwrap();
            data.push(body.to_vec());
        }
        assert_eq!(data, vec![b"a".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn recovery_does_not_conflate_two_members_sharing_a_ref() {
        let dir = temp_dir("refswap");
        let (topic, a, b) = (dir.join("topic"), dir.join("a"), dir.join("b"));

        // Session 1: "a" (ref 0) merges 100 records (member_index 0..99).
        append_records(&a, "a", 100);
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
            mux.add_member("a", 0, &a).unwrap();
            assert_eq!(mux.poll().unwrap(), 100);
        }

        // Session 2: "a" is gone; "b" attaches and (next_ref reset) also gets ref 0, merging
        // its first 10 records (member_index 0..9).
        append_records(&b, "b", 10);
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
            mux.add_member("b", 0, &b).unwrap();
            assert_eq!(mux.poll().unwrap(), 10);
        }

        // "b" produces more (now member_index 0..149).
        append_records(&b, "b", 140);

        // Session 3: reopen and resume "b". Correct recovery resumes at 10; the bug resumes at
        // 100 (inheriting "a"'s max under ref 0) and skips b's records 10..99.
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
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
    fn source_regression_records_member_regressed_not_a_gap() {
        let dir = temp_dir("regress");
        let (topic, m) = (dir.join("topic"), dir.join("m"));
        append_records(&m, "m", 100); // indices 0..99
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
            mux.add_member("m", 0, &m).unwrap();
            assert_eq!(mux.poll().unwrap(), 100);
        }

        // The member respawns onto a fresh, shorter log under the SAME (name, epoch) — an
        // upstream §3.2 violation (a proper respawn would bump the epoch).
        xchannel::cleanup_channel_files(&m);
        append_records(&m, "m", 5); // fresh indices 0..4
        let head = ReplicationSource::open(&m).unwrap().0.head().unwrap().0;
        assert_eq!(head, 5);

        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
            mux.add_member("m", 0, &m).unwrap();
            assert_eq!(
                mux.members()[0].2,
                head,
                "resumes at head, never skip_to past it"
            );
        }

        // The anomaly is a MemberRegressed (expected > head), not a backwards TopicGap.
        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let (mut regressed, mut gaps) = (None, 0);
        while let Some(msg) = r.try_read().unwrap() {
            match msg.header().message_type {
                MSG_TYPE_MEMBER_REGRESSED => {
                    regressed = Some(MemberRegressed::decode(msg.payload()).unwrap())
                }
                MSG_TYPE_TOPIC_GAP => gaps += 1,
                _ => {}
            }
        }
        assert_eq!(gaps, 0, "no backwards TopicGap emitted");
        let reg = regressed.expect("a MemberRegressed marker");
        assert_eq!((reg.expected, reg.head), (100, 5));
    }

    #[test]
    fn finish_drains_members_then_writes_terminal() {
        let dir = temp_dir("finish");
        let (topic, m) = (dir.join("topic"), dir.join("m"));
        write_member(&m, &[(1, 0, b"m0"), (1, 1, b"m1")]);

        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 1).unwrap();
        mux.add_member("m", 0, &m).unwrap();
        assert_eq!(mux.poll().unwrap(), 1); // batch=1, one merged, one pending
        mux.finish().unwrap();
        assert!(mux.members().is_empty());
        drop(mux);

        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let (mut data, mut closed, mut terminal) = (0, 0, 0);
        while let Some(msg) = r.try_read().unwrap() {
            match msg.header().message_type {
                MSG_TYPE_MEMBER_CLOSED => closed += 1,
                MSG_TYPE_TERMINAL => terminal += 1,
                mt if !is_control(mt) => data += 1,
                _ => {}
            }
        }
        assert_eq!(data, 2, "finish drained both member records");
        assert_eq!(closed, 1, "member drained + closed");
        assert_eq!(terminal, 1, "a terminal marker was committed");
    }

    /// A member record too big for the topic's `mtu` is **rejected and counted**, and the member
    /// keeps flowing afterwards — it must not wedge the topic, and it must not be *silently*
    /// skipped either.
    ///
    /// The provenance prefix is what makes this reachable at all: a record that fitted its own
    /// channel exactly can exceed the topic's limit by those 18 bytes. Leaving it to `try_reserve`
    /// would surface as an ordinary commit error, indistinguishable from a transient one — and the
    /// cursor used to advance *before* the commit, so each retry consumed a different record and
    /// quietly shredded the member's stream.
    #[test]
    fn an_oversized_member_record_is_rejected_and_the_member_keeps_flowing() {
        let dir = temp_dir("mtu-reject");
        let (topic, m) = (dir.join("topic"), dir.join("m"));
        let big = vec![0xAAu8; 512];
        write_member(&m, &[(1, 0, b"small"), (1, 1, &big), (1, 2, b"after")]);

        let geometry = TopicGeometry {
            region_size: REGION_U32,
            mtu: 256,
            file_roll_size: 0,
            keep_files: 0,
        };
        let mut mux = Mux::open(&topic, &geometry, 64).unwrap();
        mux.add_member("m", 0, &m).unwrap();
        assert_eq!(mux.poll().unwrap(), 2, "the two that fit are merged");

        let status = mux.status().unwrap();
        assert_eq!(
            status.members[0].rejected, 1,
            "the oversized one is counted"
        );
        assert_eq!(
            status.members[0].merged, 3,
            "and the member is not stuck on it"
        );
        drop(mux);

        // The hole is visible in provenance: indices 0 and 2 present, 1 missing.
        let (data, _) = read_topic(&topic);
        let indices: Vec<u64> = data.iter().map(|(_, p, _)| p.member_index).collect();
        assert_eq!(
            indices,
            vec![0, 2],
            "a visible gap, not a silent truncation"
        );
    }

    /// A retired mux is **inert**: nothing is ever committed after the terminal marker, however
    /// long someone holds a handle to it.
    ///
    /// This is an invariant of the engine rather than of its caller on purpose. Once muxes are
    /// individually locked, `deregister_topic` can remove one from the map while a poll loop is
    /// still holding a handle it sampled moments earlier — and that poll would otherwise merge
    /// records *past* the marker that says the topic ended. The coarse map lock used to prevent
    /// that by accident; this makes it true by construction.
    #[test]
    fn a_retired_mux_merges_nothing_more() {
        let dir = temp_dir("retired");
        let (topic, m) = (dir.join("topic"), dir.join("m"));
        write_member(&m, &[(1, 0, b"before")]);

        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 64).unwrap();
        mux.add_member("m", 0, &m).unwrap();
        assert_eq!(mux.poll().unwrap(), 1);
        mux.finish().unwrap();

        // The producer keeps writing, and a stale handle keeps polling.
        write_member(&m, &[(1, 1, b"after"), (1, 2, b"later")]);
        assert_eq!(mux.poll().unwrap(), 0, "a retired mux merges nothing");
        assert!(
            mux.add_member("m2", 0, &m).is_err(),
            "and accepts no new member"
        );
        mux.finish().unwrap(); // idempotent — must not write a second marker
        drop(mux);

        // Exactly one terminal marker, and nothing after it.
        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let (mut terminals, mut after_terminal) = (0, 0);
        while let Some(msg) = r.try_read().unwrap() {
            if msg.header().message_type == MSG_TYPE_TERMINAL {
                terminals += 1;
            } else if terminals > 0 {
                after_terminal += 1;
            }
        }
        assert_eq!(terminals, 1, "one terminal marker, not one per finish call");
        assert_eq!(after_terminal, 0, "nothing committed past the marker");
    }

    #[test]
    fn remove_member_drains_then_emits_member_closed() {
        let dir = temp_dir("close");
        let (topic, member) = (dir.join("topic"), dir.join("m"));
        write_member(&member, &[(1, 0, b"m0"), (1, 1, b"m1"), (1, 2, b"m2")]);

        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 1).unwrap();
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
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 64).unwrap();
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
        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 64).unwrap();
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

    /// Guards the genesis-scan invariant on `recover_cursors` (see its doc): a live-but-quiet
    /// member "a" whose records sit early in the log — far behind lots of later "b" activity —
    /// must still have its cursor recovered, so it is NOT re-merged (duplicated) on resume. A
    /// future bounded scan that started mid-log would resolve "a" to fresh and re-merge it; this
    /// test would then fail.
    #[test]
    fn recovery_quiet_member_far_behind_tail_is_not_duplicated() {
        let dir = temp_dir("quiet");
        let (topic, a, b) = (dir.join("topic"), dir.join("a"), dir.join("b"));
        append_records(&a, "a", 5); // a: indices 0..4, then quiet forever
        append_records(&b, "b", 5);

        // Session 1: both members merged.
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
            mux.add_member("a", 0, &a).unwrap();
            mux.add_member("b", 0, &b).unwrap();
            assert_eq!(mux.poll().unwrap(), 10);
        }
        // "b" produces a long tail while "a" stays quiet.
        append_records(&b, "b", 2000);
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
            mux.add_member("a", 0, &a).unwrap();
            mux.add_member("b", 0, &b).unwrap();
            mux.poll().unwrap();
        }
        // Session 3: reopen. "a" (records far behind the tail) must resume at 5, not re-merge.
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 4096).unwrap();
            mux.add_member("a", 0, &a).unwrap();
            assert_eq!(
                mux.members()[0].2,
                5,
                "quiet member's cursor recovered, not reset"
            );
            mux.poll().unwrap();
        }

        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        let mut a_count = 0;
        while let Some(m) = r.try_read().unwrap() {
            if is_control(m.header().message_type) {
                continue;
            }
            let (_, body) = Provenance::split(m.payload()).unwrap();
            if body.starts_with(b"a") {
                a_count += 1;
            }
        }
        assert_eq!(
            a_count, 5,
            "quiet member's records appear exactly once (no duplication)"
        );
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

        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 64).unwrap(); // fresh topic, no cursor
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

    /// Rolling + retention on the **topic channel itself** must be applied on every `Mux::open`,
    /// not just when the channel is created. xchannel keeps `file_roll_size`/`keep_files` on the
    /// `Writer`, not in the header, so a reopen that omits them yields a topic that never rolls
    /// and never prunes — one unbounded file per topic — however it was created. That is the shape
    /// of the bug this test pins: `create_topic` precreates the file with the client's bounds and
    /// then drops that writer, so the mux's writer is the only one that matters.
    #[test]
    fn a_reopened_topic_keeps_rolling_and_pruning() {
        let dir = temp_dir("bounds");
        let (topic, m) = (dir.join("topic"), dir.join("m"));
        let bounded = TopicGeometry {
            region_size: REGION_U32,
            mtu: 0,
            file_roll_size: (REGION as u64) * 2,
            keep_files: 1,
        };

        // Session 1 creates the topic and merges enough to roll at least once.
        write_member_rolling(&m, 3000, (REGION as u64) * 2, 0);
        {
            let mut mux = Mux::open(&topic, &bounded, 100_000).unwrap();
            mux.add_member("m", 0, &m).unwrap();
            assert_eq!(mux.poll().unwrap(), 3000);
        }
        // Session 2 *reopens* it and merges more. If the reopen dropped the bounds, everything
        // below lands in one file that is never pruned.
        write_member_rolling(&m, 3000, (REGION as u64) * 2, 0);
        {
            let mut mux = Mux::open(&topic, &bounded, 100_000).unwrap();
            mux.add_member("m", 0, &m).unwrap();
            assert_eq!(mux.poll().unwrap(), 3000);
        }

        let base = ReaderBuilder::new(&topic)
            .late_join()
            .build()
            .unwrap()
            .base_record_index();
        assert!(
            base > 0,
            "topic must have rolled *and* pruned (earliest retained index is still 0)"
        );
        let segments = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("topic"))
            .count();
        assert!(
            segments <= 2,
            "keep_files(1) must bound the topic to ~1 rolled segment, found {segments}"
        );
    }

    /// Once a topic prunes its own tail (which it does as soon as `keep_files` is set), a member
    /// that has been **quiet** long enough for all of its data records to age out of the topic must
    /// still have its cursor recovered — otherwise it looks like a brand-new member, resumes from
    /// its own genesis, and its retained history is **re-merged into the topic (duplication)**.
    ///
    /// This is why every slot table carries each member's cursor: the periodic re-emit keeps a
    /// table inside the retained window even when a member's records are long gone. Enabling
    /// retention on topics without this would have traded an unbounded-disk bug for a duplication
    /// bug. Companion to `recovery_quiet_member_far_behind_tail_is_not_duplicated`, which covers
    /// the same member being far behind the tail but still *retained*.
    #[test]
    fn a_quiet_members_cursor_survives_the_topics_own_retention() {
        let dir = temp_dir("quietprune");
        let (topic, a, b) = (dir.join("topic"), dir.join("a"), dir.join("b"));
        let bounded = TopicGeometry {
            region_size: REGION_U32,
            mtu: 0,
            file_roll_size: (REGION as u64) * 2,
            keep_files: 1,
        };

        // "a" writes 5 records and goes quiet forever; "b" then floods the topic.
        append_records(&a, "a", 5);
        write_member_rolling(&b, 12_000, (REGION as u64) * 2, 0);
        {
            let mut mux = Mux::open(&topic, &bounded, 1_000_000).unwrap();
            mux.add_member("a", 0, &a).unwrap();
            mux.add_member("b", 0, &b).unwrap();
            assert_eq!(mux.poll().unwrap(), 5 + 12_000);
        }

        // Preconditions, asserted so a failure below explains itself: the topic pruned, none of
        // "a"'s data records survive in the retained window, and a slot table naming "a" does.
        let mut r = ReaderBuilder::new(&topic).late_join().build().unwrap();
        assert!(r.base_record_index() > 0, "topic should have pruned");
        let (mut a_records, mut a_in_table) = (0, None);
        while let Some(msg) = r.try_read().unwrap() {
            let mt = msg.header().message_type;
            if mt == MSG_TYPE_SLOT_TABLE {
                let table = decode_slot_table(msg.payload()).unwrap();
                if let Some(e) = table.members.iter().find(|e| e.name == "a") {
                    a_in_table = Some(e.cursor);
                }
            } else if !is_control(mt) {
                let (_, body) = Provenance::split(msg.payload()).unwrap();
                if body.starts_with(b"a") {
                    a_records += 1;
                }
            }
        }
        assert_eq!(
            a_records, 0,
            "the quiet member's data records must have aged out for this test to mean anything"
        );
        assert_eq!(
            a_in_table,
            Some(5),
            "a retained slot table must still carry the quiet member's cursor"
        );

        // Reopen: "a" must resume at 5 from that table, not re-merge its 5 records.
        let mut mux = Mux::open(&topic, &bounded, 1_000_000).unwrap();
        mux.add_member("a", 0, &a).unwrap();
        assert_eq!(
            mux.members()[0].2,
            5,
            "quiet member's cursor recovered from the slot table, not reset to 0"
        );
        assert_eq!(
            mux.poll().unwrap(),
            0,
            "nothing to re-merge — the member is caught up"
        );
    }

    #[test]
    fn recovers_cursors_after_restart_without_duplication() {
        let dir = temp_dir("recover");
        let (topic, m_a) = (dir.join("topic"), dir.join("a"));
        write_member(&m_a, &[(1, 0, b"a0"), (1, 1, b"a1")]);

        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 16).unwrap();
            mux.add_member("a", 0, &m_a).unwrap();
            assert_eq!(mux.poll().unwrap(), 2);
        }
        // The producer writes one more record while the mux is down.
        write_member(&m_a, &[(1, 2, b"a2")]);

        // Reopen: recovery must resume "a" at index 2, not re-merge 0,1.
        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 16).unwrap();
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
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 64).unwrap();
            mux.add_member("a", 1, &a1).unwrap();
            assert_eq!(mux.poll().unwrap(), 5);
        }

        // Boot 2: "a" is reclaimed at epoch2 on a fresh log and merges only indices 0,1 so far.
        // `next_ref` reset to 0 ⇒ epoch2 reuses member_ref 0. The topic now holds ref-0 data
        // from BOTH incarnations, and the latest slot table maps ref 0 → (a, epoch 2).
        write_member(&a2, &[(1, 0, b"e2-0"), (1, 1, b"e2-1")]);
        {
            let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 64).unwrap();
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
        let mut mux = Mux::open(&topic, &geom(REGION_U32, 0), 64).unwrap();
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
