//! Stream-plane protocol — drives the replication engines over a [`Transport`].
//!
//! This is the wire choreography of DESIGN §6.1, transport-agnostic (generic over
//! [`Transport`]). One subscription per connection for v1 (a fixed [`StreamId`];
//! multiplexing many channels over one connection is a later refinement).
//!
//! * **Origin side:** [`accept_subscription`] performs the handshake (read `Subscribe`,
//!   resolve the channel, send `SubscribeAck` or `Gap`) and returns a [`StreamServer`]
//!   that streams `Record`s via [`StreamServer::run`] / `pump_one`.
//! * **Subscriber side:** [`subscribe`] sends `Subscribe`, consumes `SubscribeAck`, opens
//!   the replica [`ReplicationSink`], and returns a [`StreamClient`] that applies `Record`s
//!   via [`StreamClient::run`] / `recv_one`.

use crate::codec::{decode_stream, encode_stream};
use crate::replication::{ReplicationSink, ReplicationSource};
use crate::transport::Transport;
use crate::wire::StreamMsg;
use crate::{RecordIndex, StreamId};
use std::io;
use std::path::{Path, PathBuf};

/// Single-subscription-per-connection id (v1). Multiplexing would assign these per ack.
const STREAM_ID: StreamId = StreamId(0);

#[inline]
fn invalid(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// What the origin side needs to serve a channel: where its files live and the geometry
/// to advertise so the subscriber builds a compatible replica. Supplied by the manager's
/// registry/path resolution.
#[derive(Clone, Debug)]
pub struct ChannelSource {
    pub path: PathBuf,
    pub region_size: u32,
    pub mtu: u32,
    /// Rolling + retention policy to advertise so the replica inherits the origin's disk
    /// bounds (`0` = no rolling / unlimited retention).
    pub file_roll_size: u64,
    pub keep_files: u32,
}

// ---------------- origin side ----------------

/// Origin-side handshake: read the `Subscribe`, resolve the channel via `resolve`, and
/// reply with `SubscribeAck` (then stream via the returned server), or refuse the resume
/// position with `Gap` / `Diverged`.
///
/// Both refusals are decided **before** seeking, because the seek is the dangerous step: it
/// reads forward to `from` and blocks indefinitely on a position the channel has not reached.
/// Errors after sending `Gap` if a resuming subscriber is older than the retained history, or
/// `Diverged` if it is ahead of the head; errors with `NotFound` if `resolve` doesn't know the
/// channel.
pub fn accept_subscription<T: Transport>(
    mut transport: T,
    resolve: impl Fn(&str) -> Option<ChannelSource>,
) -> io::Result<StreamServer<T>> {
    let (name, from, their_generation) = match decode_stream(&transport.recv_frame()?)? {
        StreamMsg::Subscribe {
            name,
            from,
            generation,
        } => (name, from, generation),
        _ => return Err(invalid("first stream frame must be Subscribe")),
    };

    let src = resolve(&name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("unknown channel: {name}"))
    })?;
    let (mut source, earliest) = ReplicationSource::open(&src.path)?;
    // The channel's true high-water index at accept time (read from the newest segment,
    // independent of where we resume from). Lets the subscriber detect catch-up to the
    // frontier; the record flow itself is unaffected. Read before the checks below, which
    // need it to bound the acceptable resume range.
    let head = source.head()?;

    // Different incarnation: the subscriber's replica was built from another log that merely
    // shares this name (the name was deregistered and reclaimed). Checked first and before
    // anything else, because such a replica can *also* look behind retention or past the head,
    // and those would name the wrong problem. A subscriber with no replica (`from == 0`) has
    // nothing to invalidate, so it is exempt — and a never-reclaimed channel has generation 0
    // on both sides, making this inert on the common path.
    let generation = source.generation();
    if from.0 > 0 && their_generation != generation {
        transport.send_frame(&encode_stream(&StreamMsg::Diverged {
            name,
            earliest,
            head,
        }))?;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "subscriber diverged: replica generation {their_generation} != channel \
                 generation {generation} — the name was reclaimed"
            ),
        ));
    }

    // Retention gap: a non-zero `from` older than what we still retain can't be served
    // contiguously. (`from == 0` is a fresh subscriber and accepts truncated history.)
    if from.0 > 0 && from.0 < earliest.0 {
        transport.send_frame(&encode_stream(&StreamMsg::Gap {
            name,
            earliest,
            head: earliest,
        }))?;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "subscriber behind retention: from {} < earliest {}",
                from.0, earliest.0
            ),
        ));
    }

    // Ahead of the head: this channel has never held a record at `from`, so the subscriber's
    // replica cannot be a prefix of it — it was built from a different incarnation of the
    // name (a deregistered name reclaimed by a new owner starts over at index 0). Refusing
    // here is what stops `skip_to` from blocking forever waiting for records that will never
    // be written; letting it through would also, once the new log eventually grew past
    // `from`, splice two unrelated channels into one replica with the contiguity check none
    // the wiser. `from == head` is not divergence — that is a subscriber that is simply
    // caught up.
    if from.0 > head.0 {
        transport.send_frame(&encode_stream(&StreamMsg::Diverged {
            name,
            earliest,
            head,
        }))?;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "subscriber diverged: from {} is past head {} — replica is not a prefix of \
                 this channel",
                from.0, head.0
            ),
        ));
    }

    let start = RecordIndex(from.0.max(earliest.0));
    source.skip_to(start)?;
    transport.send_frame(&encode_stream(&StreamMsg::SubscribeAck {
        name,
        stream_id: STREAM_ID,
        start,
        head,
        region_size: src.region_size,
        mtu: src.mtu,
        file_roll_size: src.file_roll_size,
        keep_files: src.keep_files,
        generation,
    }))?;

    Ok(StreamServer { transport, source })
}

/// Streams `Record`s for an accepted subscription. Owns its connection, so the manager
/// runs one per connection thread.
pub struct StreamServer<T: Transport> {
    transport: T,
    source: ReplicationSource,
}

impl<T: Transport> StreamServer<T> {
    /// Block for the next record and send it. Errors when the connection drops.
    pub fn pump_one(&mut self) -> io::Result<()> {
        let frame = self.source.next_frame()?;
        self.transport
            .send_frame(&encode_stream(&StreamMsg::Record {
                stream_id: STREAM_ID,
                frame,
            }))
    }

    /// Send the next record if one is already committed; `Ok(false)` if none pending.
    pub fn try_pump_one(&mut self) -> io::Result<bool> {
        match self.source.try_next_frame()? {
            Some(frame) => {
                self.transport
                    .send_frame(&encode_stream(&StreamMsg::Record {
                        stream_id: STREAM_ID,
                        frame,
                    }))?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Stream forever (real use). Returns `Err` when the connection drops.
    pub fn run(&mut self) -> io::Result<()> {
        loop {
            self.pump_one()?;
        }
    }
}

// ---------------- subscriber side ----------------

/// Why a [`subscribe`] handshake failed — split by **what the caller must do about it**,
/// which is the only distinction that matters at the call site.
///
/// Retrying a rebuild case with the same resume position loops forever (the source's answer
/// will not change), while discarding a replica over a transient network error would throw
/// away a whole channel's history and re-pull it. So the two must not be conflated, and an
/// untyped `io::Error` cannot express the difference without string matching.
#[derive(Debug)]
pub enum SubscribeError {
    /// The resume position is unserviceable and the replica must be **discarded and rebuilt**
    /// from `RecordIndex(0)`. Either the replica is behind the source's retention
    /// ([`Gap`](StreamMsg::Gap)) or it is not a prefix of this channel at all
    /// ([`Diverged`](StreamMsg::Diverged)); `diverged` distinguishes them for reporting, since
    /// the recovery is identical but the causes are not.
    Rebuild {
        diverged: bool,
        earliest: RecordIndex,
        head: RecordIndex,
        detail: String,
    },
    /// Transport, protocol or IO failure. Retrying the same position may well succeed.
    Io(io::Error),
}

impl SubscribeError {
    /// Whether recovery requires discarding the replica and re-subscribing from the start.
    #[inline]
    pub fn requires_rebuild(&self) -> bool {
        matches!(self, Self::Rebuild { .. })
    }
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rebuild { detail, .. } => f.write_str(detail),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SubscribeError {}

impl From<io::Error> for SubscribeError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<SubscribeError> for io::Error {
    fn from(e: SubscribeError) -> Self {
        match e {
            SubscribeError::Io(e) => e,
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        }
    }
}

/// Subscriber-side handshake: send `Subscribe { name, from, generation }`, consume the reply,
/// and on `SubscribeAck` open the replica at `replica_path` seeded with the ack's
/// geometry/start/generation.
///
/// `generation` is the incarnation the caller's existing replica holds (0 if it has none);
/// the source refuses the resume if it does not match its own. See [`SubscribeError`] for the
/// caller's obligation on failure.
pub fn subscribe<T: Transport>(
    mut transport: T,
    name: &str,
    from: RecordIndex,
    generation: u64,
    replica_path: &Path,
) -> Result<StreamClient<T>, SubscribeError> {
    transport.send_frame(&encode_stream(&StreamMsg::Subscribe {
        name: name.to_string(),
        from,
        generation,
    }))?;
    match decode_stream(&transport.recv_frame()?)? {
        StreamMsg::SubscribeAck {
            start,
            head,
            region_size,
            mtu,
            file_roll_size,
            keep_files,
            generation,
            ..
        } => {
            let sink = ReplicationSink::open(
                replica_path,
                region_size,
                mtu,
                file_roll_size,
                keep_files,
                start,
                generation,
            )?;
            Ok(StreamClient {
                transport,
                sink,
                head,
            })
        }
        StreamMsg::Gap { earliest, head, .. } => Err(SubscribeError::Rebuild {
            diverged: false,
            earliest,
            head,
            detail: format!("gap: source's earliest retained index is {}", earliest.0),
        }),
        StreamMsg::Diverged { earliest, head, .. } => Err(SubscribeError::Rebuild {
            diverged: true,
            earliest,
            head,
            detail: format!(
                "diverged: replica is not a prefix of this channel (source holds {}..{})",
                earliest.0, head.0
            ),
        }),
        _ => Err(invalid("expected SubscribeAck, Gap or Diverged").into()),
    }
}

/// Applies streamed `Record`s into the local replica. Owns its connection.
pub struct StreamClient<T: Transport> {
    transport: T,
    sink: ReplicationSink,
    head: RecordIndex,
}

impl<T: Transport> StreamClient<T> {
    /// The absolute index the next received record must carry (the replica head).
    #[inline]
    pub fn expected_index(&self) -> RecordIndex {
        self.sink.expected_index()
    }

    /// The source's high-water index as of the `SubscribeAck` — the frontier this
    /// subscriber is catching up to. Applied records `< head` are historical replay;
    /// reaching `head` means synchronized as of accept time.
    #[inline]
    pub fn head(&self) -> RecordIndex {
        self.head
    }

    /// Receive and apply one record. Errors when the connection drops or a gap appears.
    pub fn recv_one(&mut self) -> io::Result<()> {
        match decode_stream(&self.transport.recv_frame()?)? {
            StreamMsg::Record { frame, .. } => self.sink.apply(&frame),
            StreamMsg::Gap { earliest, .. } => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("mid-stream gap at {}", earliest.0),
            )),
            _ => Err(invalid("expected Record")),
        }
    }

    /// Apply forever (real use). Returns `Err` when the connection drops.
    pub fn run(&mut self) -> io::Result<()> {
        loop {
            self.recv_one()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Listener;
    use crate::transport::{TcpListener, TcpTransport};
    use xchannel::{ReaderBuilder, ReaderMode, WriterBuilder};

    const REGION: usize = 1 << 20;

    fn temp_base(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("xchnet-stream-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("chan")
    }

    fn write_records(base: &Path, n: u64) {
        let mut w = WriterBuilder::new(base)
            .region_size(REGION)
            .build()
            .unwrap();
        for i in 0..n {
            let payload = format!("rec-{i}").into_bytes();
            let buf = w.try_reserve(payload.len()).unwrap();
            buf.copy_from_slice(&payload);
            w.commit((i % 5) as u16, payload.len() as u32, i * 10)
                .unwrap();
        }
    }

    #[test]
    fn replicates_a_channel_over_tcp() {
        let origin = temp_base("origin");
        let replica = temp_base("replica");
        let n = 30u64;
        write_records(&origin, n);

        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let origin_path = origin.clone();
        let server = std::thread::spawn(move || {
            let conn = listener.accept().unwrap();
            let resolve = |name: &str| {
                (name == "md.aapl").then(|| ChannelSource {
                    path: origin_path.clone(),
                    region_size: REGION as u32,
                    mtu: 0,
                    file_roll_size: 0,
                    keep_files: 0,
                })
            };
            let mut srv = accept_subscription(conn, resolve).unwrap();
            // Drain the records currently in the origin, then let the connection close.
            while srv.try_pump_one().unwrap() {}
        });

        let conn = TcpTransport::connect(addr).unwrap();
        let mut client = subscribe(conn, "md.aapl", RecordIndex(0), 0, &replica).unwrap();
        // The ack advertises the origin's true head (all n records committed before accept).
        assert_eq!(
            client.head(),
            RecordIndex(n),
            "SubscribeAck head is the real frontier"
        );
        for _ in 0..n {
            client.recv_one().unwrap();
        }
        assert_eq!(client.expected_index(), RecordIndex(n));
        drop(client);
        server.join().unwrap();

        // The replica, built entirely from the TCP stream, is record-identical.
        let mut r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        let mut seen = 0u64;
        while let Some(m) = r.try_read().unwrap() {
            assert_eq!(m.header().message_type, (seen % 5) as u16);
            assert_eq!(m.header().user_meta_u64, seen * 10);
            assert_eq!(m.payload(), format!("rec-{seen}").as_bytes());
            seen += 1;
        }
        assert_eq!(seen, n);
    }

    /// A resume position past the source's head means the replica was built from a different
    /// incarnation of the name. The origin must refuse it *before* seeking: `skip_to` would
    /// otherwise block forever on records that will never be written, wedging both sides with
    /// no error anywhere — and if the new log ever did grow past that index, the sink would
    /// splice two unrelated channels together with the contiguity check none the wiser.
    #[test]
    fn resume_past_head_is_refused_as_divergence() {
        // A "reclaimed" origin: same name, brand-new log holding only 3 records.
        let origin = temp_base("diverged-origin");
        let replica = temp_base("diverged-replica");
        write_records(&origin, 3);

        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let origin_path = origin.clone();
        let server = std::thread::spawn(move || {
            let conn = listener.accept().unwrap();
            let resolve = |name: &str| {
                (name == "md.aapl").then(|| ChannelSource {
                    path: origin_path.clone(),
                    region_size: REGION as u32,
                    mtu: 0,
                    file_roll_size: 0,
                    keep_files: 0,
                })
            };
            accept_subscription(conn, resolve).map(|_| ()).unwrap_err()
        });

        // The subscriber still holds 5000 records of the previous incarnation.
        let conn = TcpTransport::connect(addr).unwrap();
        let err = match subscribe(conn, "md.aapl", RecordIndex(5000), 0, &replica) {
            Ok(_) => panic!("expected divergence, not an ack"),
            Err(e) => e,
        };
        assert!(
            err.requires_rebuild(),
            "recovery is a rebuild, not a retry: {err}"
        );
        assert!(
            matches!(err, SubscribeError::Rebuild { diverged: true, .. }),
            "must be reported as divergence, not as a retention gap: {err}"
        );

        let server_err = server.join().unwrap();
        assert_eq!(server_err.kind(), io::ErrorKind::InvalidData);
        assert!(server_err.to_string().contains("past head"));
    }

    /// Serve `origin` under the name `md.aapl` for exactly one subscribe attempt, returning
    /// the address and the server thread (whose result is the origin-side outcome).
    fn serve_once(
        origin: PathBuf,
    ) -> (
        std::net::SocketAddr,
        std::thread::JoinHandle<io::Result<()>>,
    ) {
        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let conn = listener.accept().unwrap();
            let resolve = |name: &str| {
                (name == "md.aapl").then(|| ChannelSource {
                    path: origin.clone(),
                    region_size: REGION as u32,
                    mtu: 0,
                    file_roll_size: 0,
                    keep_files: 0,
                })
            };
            accept_subscription(conn, resolve).map(|_| ())
        });
        (addr, handle)
    }

    /// The precise check: a replica from a different incarnation is refused even when its
    /// resume position looks perfectly serviceable (well inside `earliest..head`), which is
    /// exactly where the `from > head` heuristic sees nothing wrong.
    #[test]
    fn generation_mismatch_is_refused_even_when_the_position_looks_valid() {
        let origin = temp_base("gen-mismatch-origin");
        let replica = temp_base("gen-mismatch-replica");
        {
            let mut w = WriterBuilder::new(&origin)
                .region_size(REGION)
                .generation(7) // the name was reclaimed; this is incarnation 7
                .build()
                .unwrap();
            for i in 0..10u64 {
                let p = format!("rec-{i}").into_bytes();
                let buf = w.try_reserve(p.len()).unwrap();
                buf.copy_from_slice(&p);
                w.commit(0, p.len() as u32, i).unwrap();
            }
        }

        let (addr, server) = serve_once(origin);
        let conn = TcpTransport::connect(addr).unwrap();
        // Position 5 is inside 0..10 — nothing about it looks wrong on its own.
        let err = match subscribe(conn, "md.aapl", RecordIndex(5), 3, &replica) {
            Ok(_) => panic!("a replica of incarnation 3 must not be extended from incarnation 7"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            SubscribeError::Rebuild { diverged: true, .. }
        ));
        let server_err = server.join().unwrap().unwrap_err();
        assert!(server_err.to_string().contains("generation"));
    }

    /// A subscriber with no replica is exempt: `from == 0` has nothing to invalidate, so it
    /// is served whatever generation it happens to claim.
    #[test]
    fn a_fresh_subscriber_is_exempt_from_the_generation_check() {
        let origin = temp_base("gen-fresh-origin");
        let replica = temp_base("gen-fresh-replica");
        {
            let mut w = WriterBuilder::new(&origin)
                .region_size(REGION)
                .generation(7)
                .build()
                .unwrap();
            let buf = w.try_reserve(3).unwrap();
            buf.copy_from_slice(b"abc");
            w.commit(0, 3, 0).unwrap();
        }

        let (addr, server) = serve_once(origin);
        let conn = TcpTransport::connect(addr).unwrap();
        let client = subscribe(conn, "md.aapl", RecordIndex(0), 0, &replica)
            .expect("a fresh subscriber must be served");
        drop(client);
        let _ = server.join().unwrap();

        // ...and the replica it creates is stamped with the source's incarnation, so its own
        // files answer the question on the next resume.
        let r = ReaderBuilder::new(&replica)
            .mode(ReaderMode::LateJoin)
            .build()
            .unwrap();
        assert_eq!(r.generation(), 7);
    }

    /// `from == head` is a caught-up subscriber, not divergence — the boundary the check
    /// must not overshoot.
    #[test]
    fn resume_exactly_at_head_is_accepted() {
        let origin = temp_base("at-head-origin");
        let replica = temp_base("at-head-replica");
        write_records(&origin, 4);

        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let origin_path = origin.clone();
        let server = std::thread::spawn(move || {
            let conn = listener.accept().unwrap();
            let resolve = |name: &str| {
                (name == "md.aapl").then(|| ChannelSource {
                    path: origin_path.clone(),
                    region_size: REGION as u32,
                    mtu: 0,
                    file_roll_size: 0,
                    keep_files: 0,
                })
            };
            accept_subscription(conn, resolve).map(|_| ()).unwrap()
        });

        let conn = TcpTransport::connect(addr).unwrap();
        let client = subscribe(conn, "md.aapl", RecordIndex(4), 0, &replica)
            .expect("a caught-up resume must be accepted");
        assert_eq!(client.expected_index(), RecordIndex(4));
        drop(client);
        server.join().unwrap();
    }

    #[test]
    fn unknown_channel_is_rejected() {
        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let conn = listener.accept().unwrap();
            // resolve always returns None -> NotFound.
            accept_subscription(conn, |_| None).map(|_| ()).unwrap_err()
        });

        let conn = TcpTransport::connect(addr).unwrap();
        let replica = temp_base("unknown-replica");
        let err = match subscribe(conn, "nope", RecordIndex(0), 0, &replica) {
            Ok(_) => panic!("expected an error for an unknown channel"),
            Err(e) => e,
        };
        // Subscriber sees the connection close (no ack) as an unexpected EOF.
        let SubscribeError::Io(err) = err else {
            panic!("a closed connection is transient, not a rebuild: {err}");
        };
        assert!(matches!(
            err.kind(),
            io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
        ));
        let server_err = server.join().unwrap();
        assert_eq!(server_err.kind(), io::ErrorKind::NotFound);
    }
}
