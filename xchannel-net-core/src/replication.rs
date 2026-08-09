//! Replication engines: the bridge between an xchannel log and a byte stream.
//!
//! Two halves, both transport-agnostic (they deal in [`RecordFrame`]s, not sockets):
//!
//! * [`ReplicationSource`] runs on the **origin** node. It tails the origin's local
//!   channel as an ordinary xchannel `Reader` (so the single authoritative `Writer` is
//!   never blocked by slow subscribers) and emits one [`RecordFrame`] per `User` record.
//!   It opens `LateJoin` from the earliest retained record (full retained history).
//!
//! * [`ReplicationSink`] runs on a **subscriber** node. It receives `RecordFrame`s and
//!   re-frames them into a local replica via `try_reserve`/`commit`, producing a
//!   record-identical xchannel log that local clients read with plain xchannel.
//!
//! Absolute [`RecordIndex`] is intrinsic to xchannel v2: the source seeds its running
//! index from the reader's `base_record_index()` at open (the earliest retained file's
//! base) and increments per record — and because each rolled file's base accumulates, the
//! running counter stays equal to the next file's base across rolls, with no per-record
//! header read. The sink seeds the replica's `base_record_index` from the stream `start`
//! so the replica's own headers carry absolute indices.
//!
//! Segmentation is mirrored too: the source reports a roll as
//! [`RecordFrame::starts_segment`] on the record that follows it, and the sink rolls before
//! applying that record. Replicas are therefore record-identical *and* segment-aligned, which
//! is what makes the origin's `keep_files` retention mean the same thing on the replica — see
//! [`RecordFrame::starts_segment`] for why the alternative (each side rolling on its own
//! `file_roll_size`) does not bound a replica's disk at all when the origin rolls explicitly.

use crate::RecordIndex;
use crate::wire::RecordFrame;
use std::io;
use std::path::Path;
use xchannel::{ReaderBuilder, Writer, WriterBuilder};

/// Origin-side: tails a local channel and produces stream records.
pub struct ReplicationSource {
    reader: xchannel::Reader,
    /// Absolute index of the next record [`next_frame`](Self::next_frame) will return.
    next_index: u64,
}

impl ReplicationSource {
    /// Open a source over the local channel at `path`, starting from the earliest
    /// retained record. Returns the source plus that earliest absolute index, so the
    /// caller can detect retention truncation (`earliest > requested from` ⇒ `Gap`).
    pub fn open(path: &Path) -> io::Result<(Self, RecordIndex)> {
        let reader = ReaderBuilder::new(path).late_join().build()?;
        let earliest = reader.base_record_index();
        Ok((
            Self {
                reader,
                next_index: earliest,
            },
            RecordIndex(earliest),
        ))
    }

    /// The absolute index of the next record to be produced.
    #[inline]
    pub fn position(&self) -> RecordIndex {
        RecordIndex(self.next_index)
    }

    /// The channel's incarnation id (`ChannelHeader.generation`), read from the log itself
    /// rather than from any registry entry: the file is authoritative for what is actually
    /// being served, while a registry is eventually consistent and may be mid-convergence.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.reader.generation()
    }

    /// The channel's current head (high-water absolute index) at the time of call —
    /// independent of this source's read cursor. Used to advertise the true `head` in
    /// `SubscribeAck` so a subscriber can tell when it has caught up to the frontier.
    #[inline]
    pub fn head(&self) -> io::Result<RecordIndex> {
        Ok(RecordIndex(self.reader.head_record_index()?))
    }

    /// Block until the next `User` record is available and return it as a frame.
    /// `Roll`/`Skip` markers are consumed by the reader and never surface here; a roll is
    /// reported as [`RecordFrame::starts_segment`] on the record that follows it.
    pub fn next_frame(&mut self) -> io::Result<RecordFrame> {
        loop {
            let index = self.next_index;
            let segment = self.reader.file_sequence();
            let frame = self.reader.read_blocking(None)?.map(|m| RecordFrame {
                index: RecordIndex(index),
                msg_type: m.header().message_type,
                user_meta: m.header().user_meta_u64,
                starts_segment: false, // filled in below, once the reader is not borrowed
                payload: m.payload().to_vec(),
            });
            if let Some(mut frame) = frame {
                frame.starts_segment = self.rolled_since(segment);
                self.next_index += 1;
                return Ok(frame);
            }
        }
    }

    /// Non-blocking variant: the next frame if one is committed, else `None`.
    pub fn try_next_frame(&mut self) -> io::Result<Option<RecordFrame>> {
        let index = self.next_index;
        let segment = self.reader.file_sequence();
        let frame = self.reader.try_read()?.map(|m| RecordFrame {
            index: RecordIndex(index),
            msg_type: m.header().message_type,
            user_meta: m.header().user_meta_u64,
            starts_segment: false, // filled in below, once the reader is not borrowed
            payload: m.payload().to_vec(),
        });
        let Some(mut frame) = frame else {
            return Ok(None);
        };
        frame.starts_segment = self.rolled_since(segment);
        self.next_index += 1;
        Ok(Some(frame))
    }

    /// Did the reader follow a roll while producing the record just read? `segment` is the
    /// segment ordinal sampled *before* the read; a change means the record that read
    /// returned is the first of a new file at the origin (xchannel consumes the `Roll`
    /// marker transparently, so this comparison is the only way to see it).
    ///
    /// This holds across a resume, which is the case that matters most: [`skip_to`](Self::skip_to)
    /// stops *before* the record that crosses a roll, so the crossing lands on the first read
    /// that produces a frame and the resuming subscriber is told to roll before applying it.
    /// A source's very first frame reports `false` on its own — a `LateJoin` reader opens
    /// positioned at the start of a segment's records, so that read crosses nothing.
    #[inline]
    fn rolled_since(&self, segment: u64) -> bool {
        self.reader.file_sequence() != segment
    }

    /// Advance to absolute index `from` without materializing frames — used to serve a
    /// resuming subscriber's `Subscribe{from}` (xchannel has no seek-by-index, so this
    /// reads forward from the current position; that cost is inherent). Assumes
    /// `earliest <= from`; the caller handles `from < earliest` as a `Gap`. A `from`
    /// beyond the current head blocks until the channel reaches it.
    pub fn skip_to(&mut self, from: RecordIndex) -> io::Result<()> {
        while self.next_index < from.0 {
            if self.reader.read_blocking(None)?.is_some() {
                self.next_index += 1;
            }
        }
        Ok(())
    }
}

/// Subscriber-side: writes received records into a local replica channel.
pub struct ReplicationSink {
    writer: Writer,
    /// Absolute index the next [`apply`](Self::apply)ed frame must carry.
    expected_index: u64,
}

impl ReplicationSink {
    /// Create (or reopen) the local replica for a channel. `region_size`/`mtu` and the
    /// `file_roll_size`/`keep_files` rolling+retention policy all come from the source's
    /// `SubscribeAck`, so the replica mirrors the origin's geometry *and* its disk bounds
    /// rather than growing unbounded. `start` is the first absolute index the source will
    /// send; it seeds the replica's `base_record_index` so the replica self-describes
    /// absolute indices. Reopening an existing replica ignores `start` (the on-disk base
    /// wins) and resumes from its head.
    ///
    /// `generation` is the source's incarnation, likewise stamped only on creation, so the
    /// replica's own header records which log it was built from. Reopening an existing
    /// replica whose generation differs is rejected: it means this replica belongs to a
    /// previous incarnation of the name and cannot be extended. That normally cannot happen
    /// — the origin rejects such a resume during the handshake — but an *empty* replica
    /// resumes at index 0, which the origin's check deliberately waves through, so this is
    /// the backstop for that case.
    ///
    /// `name` is stamped the same way, and checked the same way, so a replica's own files say
    /// *which channel* they hold and not merely which incarnation. Without it the only thing
    /// identifying a replica is the directory it sits in, and a replica directory that has been
    /// renamed or moved would be extended with another channel's records — with the generation
    /// check none the wiser, since two never-reclaimed channels both carry generation 0.
    // Geometry and identity travel together by design (`doc/TOPICS.md`): a replica built with a
    // different region size, mtu, roll size or retention than its origin is not a replica. Splitting
    // them into a struct here would hide the coupling the parameter list makes obvious.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        path: &Path,
        name: &str,
        region_size: u32,
        mtu: u32,
        file_roll_size: u64,
        keep_files: u32,
        start: RecordIndex,
        generation: u64,
    ) -> io::Result<Self> {
        let mut builder = WriterBuilder::new(path)
            .region_size(region_size as usize)
            .mtu(mtu as u64)
            .file_roll_size(file_roll_size)
            .base_record_index(start.0)
            .generation(generation)
            .channel_name(name)?;
        if keep_files > 0 {
            builder = builder.keep_files(keep_files as u64);
        }
        let writer = builder.build()?;
        if writer.generation() != generation {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "replica generation {} != source generation {} — replica belongs to a \
                     previous incarnation of this channel",
                    writer.generation(),
                    generation
                ),
            ));
        }
        // The writer has no name accessor, so read the stamp back through a reader. Only on
        // (re)connect, never on the record path.
        let stamped = ReaderBuilder::new(path)
            .late_join()
            .build()?
            .channel_name()
            .into_owned();
        if stamped != name {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "replica at {path:?} holds channel '{stamped}', not '{name}' — refusing to \
                     extend one channel's replica with another's records"
                ),
            ));
        }
        let expected_index = writer.next_record_index();
        Ok(Self {
            writer,
            expected_index,
        })
    }

    /// The absolute index the next applied frame must have (the replica head).
    #[inline]
    pub fn expected_index(&self) -> RecordIndex {
        RecordIndex(self.expected_index)
    }

    /// Apply one received frame to the replica, after verifying it is the contiguous next
    /// index (detects loss/reordering before it corrupts the replica).
    ///
    /// [`RecordFrame::starts_segment`] rolls the replica first, so its file boundaries — and
    /// therefore what `keep_files` prunes — mirror the origin's. Rolling is unconditional when
    /// the flag is set: if this sink was reopened after a crash between a roll and the first
    /// commit into the new segment, the roll repeats and leaves one empty segment behind,
    /// which costs a retention slot until it ages out but cannot lose records. Skipping the
    /// roll instead would silently misalign the replica for the rest of the channel's life.
    pub fn apply(&mut self, frame: &RecordFrame) -> io::Result<()> {
        if frame.index.0 != self.expected_index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "non-contiguous record: replica expects index {}, frame is {}",
                    self.expected_index, frame.index.0
                ),
            ));
        }
        if frame.starts_segment {
            self.writer.roll_file()?;
        }
        let len = frame.payload.len();
        let buf = self.writer.try_reserve(len)?;
        buf.copy_from_slice(&frame.payload);
        self.writer
            .commit(frame.msg_type, len as u32, frame.user_meta)?;
        self.expected_index += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xchannel::ReaderMode;

    const REGION: usize = 1 << 20; // 1 MiB, a page-size multiple
    const REGION_U32: u32 = REGION as u32;

    /// Fresh temp base path `<tmp>/<name>/chan`, with any prior dir removed.
    fn temp_base(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("xchnet-repl-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("chan")
    }

    /// Write `n` records with recognizable msg_type/user_meta/payload, then drop the
    /// writer (a same-process reader must not run concurrently with the writer).
    fn write_records(base: &Path, n: u64) {
        let mut w = WriterBuilder::new(base)
            .region_size(REGION)
            .build()
            .unwrap();
        for i in 0..n {
            let payload = format!("record-{i}").into_bytes();
            let buf = w.try_reserve(payload.len()).unwrap();
            buf.copy_from_slice(&payload);
            w.commit((i % 7) as u16, payload.len() as u32, i * 1000)
                .unwrap();
        }
    }

    /// Write `groups` of records, rolling between groups — the position-service pattern: no
    /// `file_roll_size`, so every boundary is one the application chose.
    fn write_segments(base: &Path, groups: &[u64], keep_files: u32) {
        let mut builder = WriterBuilder::new(base).region_size(REGION);
        if keep_files > 0 {
            builder = builder.keep_files(keep_files as u64);
        }
        let mut w = builder.build().unwrap();
        let mut index = 0u64;
        for (g, &n) in groups.iter().enumerate() {
            if g > 0 {
                w.roll_file().unwrap();
            }
            for _ in 0..n {
                let payload = format!("record-{index}").into_bytes();
                let buf = w.try_reserve(payload.len()).unwrap();
                buf.copy_from_slice(&payload);
                w.commit(1, payload.len() as u32, index).unwrap();
                index += 1;
            }
        }
    }

    /// Absolute indices at which the channel on disk begins a segment, discovered the way the
    /// source discovers them: sampling `file_sequence()` around each read. The first entry is
    /// the earliest retained index, so a pruned channel reports where it actually starts.
    fn segment_starts(base: &Path) -> Vec<u64> {
        let mut r = ReaderBuilder::new(base)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let mut index = r.base_record_index();
        let mut starts = vec![index];
        loop {
            let before = r.file_sequence();
            if r.try_read().unwrap().is_none() {
                return starts;
            }
            if r.file_sequence() != before {
                starts.push(index);
            }
            index += 1;
        }
    }

    /// The origin's segmentation must survive replication: a roll rides on the first record of
    /// the new segment, and the sink reproduces the boundary. Without it, a replica whose
    /// `file_roll_size` is 0 never rolls at all.
    /// A replica's files say which channel they hold, so a replica directory that has been
    /// renamed or moved is refused rather than extended with another channel's records. The
    /// `generation` check cannot catch this on its own: two never-reclaimed channels both carry
    /// generation 0, so nothing about the mismatch looks wrong.
    #[test]
    fn a_replica_refuses_to_be_extended_by_a_different_channel() {
        let replica = temp_base("wrong-name-replica");
        {
            let sink =
                ReplicationSink::open(&replica, "md.aapl", REGION_U32, 0, 0, 0, RecordIndex(0), 0)
                    .unwrap();
            drop(sink);
        }
        let Err(err) =
            ReplicationSink::open(&replica, "md.msft", REGION_U32, 0, 0, 0, RecordIndex(0), 0)
        else {
            panic!("a replica of md.aapl must not be extended as md.msft");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("md.aapl"), "{err}");
    }

    #[test]
    fn roll_boundaries_replicate() {
        let origin = temp_base("roll-origin");
        let replica = temp_base("roll-replica");
        write_segments(&origin, &[2, 2, 2], 0);

        let (mut source, _) = ReplicationSource::open(&origin).unwrap();
        let mut frames = Vec::new();
        while let Some(f) = source.try_next_frame().unwrap() {
            frames.push(f);
        }
        assert_eq!(frames.len(), 6);
        let flagged: Vec<u64> = frames
            .iter()
            .filter(|f| f.starts_segment)
            .map(|f| f.index.0)
            .collect();
        assert_eq!(
            flagged,
            vec![2, 4],
            "one hint per roll, on the record that follows it — never on the first record"
        );

        {
            let mut sink =
                ReplicationSink::open(&replica, "chan", REGION_U32, 0, 0, 0, RecordIndex(0), 0)
                    .unwrap();
            for f in &frames {
                sink.apply(f).unwrap();
            }
        }
        assert_eq!(segment_starts(&replica), vec![0, 2, 4]);
        assert_eq!(segment_starts(&replica), segment_starts(&origin));
    }

    /// The payoff: with app-driven rolls, `keep_files` prunes the same window on both sides.
    /// A replica that only rolled on its own `file_roll_size` (0 here — the origin sets none)
    /// would keep every record in one ever-growing file.
    #[test]
    fn replica_retention_matches_the_origin_under_app_driven_rolls() {
        let origin = temp_base("retain-origin");
        let replica = temp_base("retain-replica");
        write_segments(&origin, &[3, 3, 3, 3, 3], 2);

        let (mut source, earliest) = ReplicationSource::open(&origin).unwrap();
        assert!(
            earliest.0 > 0,
            "origin should have pruned its earliest segments"
        );

        {
            let mut sink =
                ReplicationSink::open(&replica, "chan", REGION_U32, 0, 0, 2, earliest, 0).unwrap();
            while let Some(f) = source.try_next_frame().unwrap() {
                sink.apply(&f).unwrap();
            }
        }

        // Segment *ordinals* differ (the replica's files start at 0, the origin's survivors
        // don't) — what must match is where the boundaries fall and how much is retained.
        let o = ReaderBuilder::new(&origin)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let rp = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert_eq!(
            rp.base_record_index(),
            o.base_record_index(),
            "replica must retain the same window as the origin"
        );
        assert_eq!(segment_starts(&replica), segment_starts(&origin));
    }

    /// A subscriber that reconnects exactly at a roll boundary must still be told to roll:
    /// `skip_to` stops before the record that crosses the roll, so the crossing lands on the
    /// first frame the resumed source produces.
    #[test]
    fn resume_at_a_boundary_still_reports_it() {
        let origin = temp_base("resume-boundary");
        write_segments(&origin, &[2, 2], 0);

        let (mut source, _) = ReplicationSource::open(&origin).unwrap();
        source.skip_to(RecordIndex(2)).unwrap();
        let f = source.next_frame().unwrap();
        assert_eq!(f.index, RecordIndex(2));
        assert!(
            f.starts_segment,
            "record 2 opens a new segment; a resuming replica must roll before applying it"
        );
    }

    #[test]
    fn source_to_sink_round_trip() {
        let origin = temp_base("origin");
        let replica = temp_base("replica");
        let n = 50u64;
        write_records(&origin, n);

        // Drain the origin via the source.
        let (mut source, earliest) = ReplicationSource::open(&origin).unwrap();
        assert_eq!(earliest, RecordIndex(0), "genesis, nothing pruned");
        let mut frames = Vec::new();
        while let Some(f) = source.try_next_frame().unwrap() {
            frames.push(f);
        }
        assert_eq!(frames.len() as u64, n);
        assert_eq!(frames[0].index, RecordIndex(0));
        assert_eq!(frames[n as usize - 1].index, RecordIndex(n - 1));

        // Apply into the replica.
        {
            let mut sink =
                ReplicationSink::open(&replica, "chan", REGION_U32, 0, 0, 0, earliest, 0).unwrap();
            for f in &frames {
                sink.apply(f).unwrap();
            }
            assert_eq!(sink.expected_index(), RecordIndex(n));
        }

        // The replica is record-identical to the origin.
        let mut r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let mut seen = 0u64;
        while let Some(m) = r.try_read().unwrap() {
            assert_eq!(m.header().message_type, (seen % 7) as u16);
            assert_eq!(m.header().user_meta_u64, seen * 1000);
            assert_eq!(m.payload(), format!("record-{seen}").as_bytes());
            seen += 1;
        }
        assert_eq!(seen, n);
    }

    #[test]
    fn sink_inherits_rolling_and_retention() {
        // A replica opened with the origin's file_roll_size + keep_files rolls and prunes
        // like the origin, instead of growing as one unbounded file. We prove both
        // propagated by checking that early records were pruned (earliest retained base > 0).
        let replica = temp_base("retain");
        let region = REGION as u32; // 1 MiB
        let file_roll_size = (REGION as u64) * 2; // 2 regions/file → frequent rolls
        let keep_files = 2u32;

        {
            let mut sink = ReplicationSink::open(
                &replica,
                "chan",
                region,
                0,
                file_roll_size,
                keep_files,
                RecordIndex(0),
                0,
            )
            .unwrap();
            // Enough ~1 KiB records to fill many files and force pruning past keep_files.
            let payload = vec![0xABu8; 1024];
            for i in 0..6000u64 {
                sink.apply(&RecordFrame {
                    index: RecordIndex(i),
                    msg_type: 0,
                    user_meta: i,
                    starts_segment: false,
                    payload: payload.clone(),
                })
                .unwrap();
            }
        }

        // Pruning happened ⇒ the earliest retained file no longer starts at genesis.
        let r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert!(
            r.base_record_index() > 0,
            "replica should have rolled and pruned early records (base = {})",
            r.base_record_index()
        );
    }

    #[test]
    fn sink_rejects_non_contiguous_frame() {
        let replica = temp_base("noncontig");
        let mut sink =
            ReplicationSink::open(&replica, "chan", REGION_U32, 0, 0, 0, RecordIndex(0), 0)
                .unwrap();

        sink.apply(&RecordFrame {
            index: RecordIndex(0),
            msg_type: 1,
            user_meta: 0,
            starts_segment: false,
            payload: vec![1, 2, 3],
        })
        .unwrap();

        // Skips index 1.
        let err = sink
            .apply(&RecordFrame {
                index: RecordIndex(2),
                msg_type: 1,
                user_meta: 0,
                starts_segment: false,
                payload: vec![4, 5, 6],
            })
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn source_skip_to_resumes_at_index() {
        let origin = temp_base("skip");
        write_records(&origin, 20);

        let (mut source, _earliest) = ReplicationSource::open(&origin).unwrap();
        source.skip_to(RecordIndex(5)).unwrap();
        assert_eq!(source.position(), RecordIndex(5));

        let f = source.next_frame().unwrap();
        assert_eq!(f.index, RecordIndex(5));
        assert_eq!(f.payload, b"record-5");
    }
}
