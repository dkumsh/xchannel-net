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

    /// Block until the next `User` record is available and return it as a frame.
    /// `Roll`/`Skip` markers are consumed by the reader and never surface here.
    pub fn next_frame(&mut self) -> io::Result<RecordFrame> {
        loop {
            let index = self.next_index;
            let frame = self.reader.read_blocking(None)?.map(|m| RecordFrame {
                index: RecordIndex(index),
                msg_type: m.header().message_type,
                user_meta: m.header().user_meta_u64,
                payload: m.payload().to_vec(),
            });
            if let Some(frame) = frame {
                self.next_index += 1;
                return Ok(frame);
            }
        }
    }

    /// Non-blocking variant: the next frame if one is committed, else `None`.
    pub fn try_next_frame(&mut self) -> io::Result<Option<RecordFrame>> {
        let index = self.next_index;
        let frame = self.reader.try_read()?.map(|m| RecordFrame {
            index: RecordIndex(index),
            msg_type: m.header().message_type,
            user_meta: m.header().user_meta_u64,
            payload: m.payload().to_vec(),
        });
        if frame.is_some() {
            self.next_index += 1;
        }
        Ok(frame)
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
    /// Create (or reopen) the local replica for a channel. `region_size`/`mtu` come from
    /// the source's authoritative geometry (the `SubscribeAck`), so every record fits.
    /// `start` is the first absolute index the source will send; it seeds the replica's
    /// `base_record_index` so the replica self-describes absolute indices. Reopening an
    /// existing replica ignores `start` (the on-disk base wins) and resumes from its head.
    pub fn open(path: &Path, region_size: u32, mtu: u32, start: RecordIndex) -> io::Result<Self> {
        let writer = WriterBuilder::new(path)
            .region_size(region_size as usize)
            .mtu(mtu as u64)
            .base_record_index(start.0)
            .build()?;
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
            let mut sink = ReplicationSink::open(&replica, REGION_U32, 0, earliest).unwrap();
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
    fn sink_rejects_non_contiguous_frame() {
        let replica = temp_base("noncontig");
        let mut sink = ReplicationSink::open(&replica, REGION_U32, 0, RecordIndex(0)).unwrap();

        sink.apply(&RecordFrame {
            index: RecordIndex(0),
            msg_type: 1,
            user_meta: 0,
            payload: vec![1, 2, 3],
        })
        .unwrap();

        // Skips index 1.
        let err = sink
            .apply(&RecordFrame {
                index: RecordIndex(2),
                msg_type: 1,
                user_meta: 0,
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
