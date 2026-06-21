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
}

// ---------------- origin side ----------------

/// Origin-side handshake: read the `Subscribe`, resolve the channel via `resolve`, and
/// reply with `SubscribeAck` (then stream via the returned server) or `Gap`.
///
/// Errors after sending `Gap` if a resuming subscriber (`from > 0`) is older than the
/// retained history; errors with `NotFound` if `resolve` doesn't know the channel.
pub fn accept_subscription<T: Transport>(
    mut transport: T,
    resolve: impl Fn(&str) -> Option<ChannelSource>,
) -> io::Result<StreamServer<T>> {
    let (name, from) = match decode_stream(&transport.recv_frame()?)? {
        StreamMsg::Subscribe { name, from } => (name, from),
        _ => return Err(invalid("first stream frame must be Subscribe")),
    };

    let src = resolve(&name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("unknown channel: {name}"))
    })?;
    let (mut source, earliest) = ReplicationSource::open(&src.path)?;

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

    let start = RecordIndex(from.0.max(earliest.0));
    source.skip_to(start)?;
    transport.send_frame(&encode_stream(&StreamMsg::SubscribeAck {
        name,
        stream_id: STREAM_ID,
        start,
        // Best-effort lower bound; a precise live head is a manager refinement (the
        // manager can read the origin's head when it has it). Not load-bearing for the
        // data flow — the subscriber just applies records as they arrive.
        head: start,
        region_size: src.region_size,
        mtu: src.mtu,
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

/// Subscriber-side handshake: send `Subscribe { name, from }`, consume the reply, and on
/// `SubscribeAck` open the replica at `replica_path` seeded with the ack's geometry/start.
/// Errors on `Gap` (behind retention) — the caller decides whether to discard the replica
/// and re-subscribe from `RecordIndex(0)`.
pub fn subscribe<T: Transport>(
    mut transport: T,
    name: &str,
    from: RecordIndex,
    replica_path: &Path,
) -> io::Result<StreamClient<T>> {
    transport.send_frame(&encode_stream(&StreamMsg::Subscribe {
        name: name.to_string(),
        from,
    }))?;
    match decode_stream(&transport.recv_frame()?)? {
        StreamMsg::SubscribeAck {
            start,
            region_size,
            mtu,
            ..
        } => {
            let sink = ReplicationSink::open(replica_path, region_size, mtu, start)?;
            Ok(StreamClient { transport, sink })
        }
        StreamMsg::Gap { earliest, .. } => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("gap: source's earliest retained index is {}", earliest.0),
        )),
        _ => Err(invalid("expected SubscribeAck or Gap")),
    }
}

/// Applies streamed `Record`s into the local replica. Owns its connection.
pub struct StreamClient<T: Transport> {
    transport: T,
    sink: ReplicationSink,
}

impl<T: Transport> StreamClient<T> {
    /// The absolute index the next received record must carry (the replica head).
    #[inline]
    pub fn expected_index(&self) -> RecordIndex {
        self.sink.expected_index()
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
                })
            };
            let mut srv = accept_subscription(conn, resolve).unwrap();
            // Drain the records currently in the origin, then let the connection close.
            while srv.try_pump_one().unwrap() {}
        });

        let conn = TcpTransport::connect(addr).unwrap();
        let mut client = subscribe(conn, "md.aapl", RecordIndex(0), &replica).unwrap();
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
        let err = match subscribe(conn, "nope", RecordIndex(0), &replica) {
            Ok(_) => panic!("expected an error for an unknown channel"),
            Err(e) => e,
        };
        // Subscriber sees the connection close (no ack) as an unexpected EOF.
        assert!(matches!(
            err.kind(),
            io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
        ));
        let server_err = server.join().unwrap();
        assert_eq!(server_err.kind(), io::ErrorKind::NotFound);
    }
}
