//! Replication engines: the bridge between an xchannel log and a byte stream.
//!
//! Two halves, both transport-agnostic:
//!
//! * [`ReplicationSource`] runs on the **owner** node. It tails the owner's local
//!   channel as an ordinary xchannel `Reader` (so the single authoritative `Writer`
//!   is never blocked by slow subscribers) and emits a [`RecordFrame`] per `User`
//!   record. It always opens from the earliest retained record (full history).
//!
//! * [`ReplicationSink`] runs on a **subscriber** node. It receives `RecordFrame`s
//!   and re-frames them into a local replica via `try_reserve`/`commit`, producing a
//!   record-identical (not byte-identical) xchannel log that local clients read with
//!   plain xchannel in Live or LateJoin mode.
//!
//! Both are scaffolds: the method bodies are the next implementation step, but the
//! shapes pin down the contract.

use crate::RecordIndex;
use crate::wire::RecordFrame;
use std::io;

/// Owner-side: tails a local channel and produces stream records.
pub struct ReplicationSource {
    // reader: xchannel::Reader,     // opened LateJoin from earliest sequence
    // next_index: RecordIndex,
    _private: (),
}

impl ReplicationSource {
    /// Open a source over the local channel at `path`, starting from the earliest
    /// retained record. Returns the source plus the earliest index actually available
    /// (so the subscriber can detect retention truncation).
    pub fn open(_path: &std::path::Path) -> io::Result<(Self, RecordIndex)> {
        unimplemented!("tail xchannel::Reader (LateJoin from earliest); count User records")
    }

    /// Block for and return the next `User` record as a frame. `Roll`/`Skip` markers
    /// are consumed and skipped internally — they never become frames.
    pub fn next_frame(&mut self) -> io::Result<RecordFrame> {
        unimplemented!("Reader::read_blocking; map User record -> RecordFrame; bump index")
    }
}

/// Subscriber-side: writes received records into a local replica channel.
pub struct ReplicationSink {
    // writer: xchannel::Writer,     // the local replica
    // expected_index: RecordIndex,
    _private: (),
}

impl ReplicationSink {
    /// Create (or open) the local replica for a channel, built with geometry
    /// compatible with the source. `start` is the first index the source will send.
    pub fn open(
        _path: &std::path::Path,
        _region_size: u32,
        _mtu: u32,
        _start: RecordIndex,
    ) -> io::Result<Self> {
        unimplemented!("build xchannel::Writer for the replica; remember expected index")
    }

    /// Apply one received frame to the replica. Verifies `frame.index` is the expected
    /// next index (detects gaps/reordering) before `try_reserve`/`commit`.
    pub fn apply(&mut self, _frame: &RecordFrame) -> io::Result<()> {
        unimplemented!("assert contiguous index; try_reserve(len); copy payload; commit(...)")
    }
}
