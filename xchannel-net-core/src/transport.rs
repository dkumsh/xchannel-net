//! Transport abstraction.
//!
//! The replication engine is written against this trait so the substrate can be TCP
//! today and RDMA or a local-IPC shortcut tomorrow (the user explicitly wants the
//! option of "other IPC or local channels"). Intentionally minimal for now — a
//! framed, reliable, ordered byte pipe — and synchronous to match xchannel's blocking
//! reader model. An async variant can come later behind the same conceptual contract.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

/// A reliable, ordered, message-framed bidirectional connection between two nodes.
pub trait Transport: Send {
    /// Send one length-delimited frame.
    fn send_frame(&mut self, bytes: &[u8]) -> io::Result<()>;
    /// Receive the next length-delimited frame, blocking until one arrives.
    fn recv_frame(&mut self) -> io::Result<Vec<u8>>;
}

/// A listener that accepts inbound [`Transport`] connections from peer nodes/clients.
pub trait Listener: Send {
    type Conn: Transport;
    fn accept(&mut self) -> io::Result<Self::Conn>;
}

/// Upper bound on a single frame, to bound the allocation a (possibly corrupt or hostile)
/// length prefix can request. Generous for typical use (records are bounded by the
/// channel's region size — commonly ≤ a few MiB — and registry syncs are small), while
/// capping the per-frame allocation an attacker can force. A deployment using regions
/// larger than this would need to raise it.
pub const MAX_FRAME_LEN: usize = 64 << 20; // 64 MiB

/// Write one length-delimited frame (`u32` LE length prefix + body) to any writer. Shared
/// by every [`Transport`] so the framing can't drift between substrates.
/// `write_all`, but abandoning the frame once `deadline` passes rather than only once a single
/// syscall has waited out its own timeout.
///
/// The distinction is the whole point: `SO_SNDTIMEO` restarts on any syscall that moved a byte, so a
/// slow or stalled peer can stretch one `write_all` without limit. Progress is required *and* the
/// total is bounded.
fn write_all_by<W: Write>(
    w: &mut W,
    mut bytes: &[u8],
    deadline: std::time::Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "peer did not accept the frame within its write budget",
            ));
        }
        match w.write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "peer accepted no bytes",
                ));
            }
            Ok(n) => bytes = &bytes[n..],
            // The per-syscall timeout expiring is not itself fatal — the deadline above decides.
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn send_framed<W: Write>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds MAX_FRAME_LEN",
        ));
    }
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)
}

/// Read one length-delimited frame from any reader, capping the prefix-driven allocation at
/// [`MAX_FRAME_LEN`] so a corrupt/hostile length can't force an unbounded `Vec`.
fn recv_framed<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "incoming frame length exceeds MAX_FRAME_LEN",
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Baseline TCP [`Transport`]: a `u32` little-endian length prefix followed by the frame
/// body. `TCP_NODELAY` is set so small control/handshake frames are not Nagle-delayed.
///
/// This is the std-only default substrate; an RDMA or local-IPC transport can implement
/// the same trait later without touching the engines built on top of it.
pub struct TcpTransport {
    stream: TcpStream,
}

impl TcpTransport {
    /// Connect to `addr` and wrap the stream.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Self::from_stream(TcpStream::connect(addr)?)
    }

    /// Connect with a bounded timeout — used for periodic peer reconnection so a down peer
    /// doesn't stall the caller for the OS default connect timeout.
    pub fn connect_timeout(
        addr: &std::net::SocketAddr,
        timeout: std::time::Duration,
    ) -> io::Result<Self> {
        Self::from_stream(TcpStream::connect_timeout(addr, timeout)?)
    }

    /// Wrap an already-connected stream (e.g. one returned by [`TcpListener::accept`]).
    pub fn from_stream(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Duplicate the handle to the same connection. Both refer to one socket — used to run
    /// a blocking reader on one half while the other half sends (e.g. the control plane's
    /// per-peer reader thread vs. broadcast sends). Reads and writes are independent.
    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            stream: self.stream.try_clone()?,
        })
    }

    /// Shut down the connection in both directions. A blocking `recv_frame` on this socket
    /// (e.g. on another clone) returns promptly with an error — used to interrupt a reader
    /// thread on stop/unsubscribe.
    pub fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Both)
    }

    /// Bound how long a **single** write syscall may block.
    ///
    /// This is `SO_SNDTIMEO`, and on its own it is *not* a bound on sending a frame — a fact worth
    /// stating plainly here because relying on it as one was a real defect. `write_all` loops, and the
    /// timeout restarts on every syscall that moved even one byte, so a peer that drains slowly (or
    /// that stalls with a partly-filled buffer) stretches a single frame arbitrarily: measured at
    /// 4.1 s per wedged peer against a 2 s setting, and 19 s for a peer draining at 128 KiB/s where
    /// the timeout never fired at all. Use [`send_frame_within`](Self::send_frame_within) for a bound
    /// on the frame; this is only the slice that keeps each syscall from parking forever.
    pub fn set_write_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }

    /// Send a length-delimited frame, abandoning it once `deadline` passes.
    ///
    /// The bound the control plane needs, and it is a *deadline* rather than a per-frame budget for two
    /// reasons learned the hard way. A budget per frame is not a bound on a sequence of frames — the
    /// join handshake chunks a whole registry, and a peer draining just fast enough to clear each frame
    /// individually stalled the sequence for the sum of them, all under the dissemination lock. And the
    /// deadline must be derived from *one peer's* allowance, not shared across peers: `write_all_by`
    /// checks it before the first syscall, so a deadline already spent by somebody else fails this peer
    /// with zero bytes written.
    ///
    /// A frame abandoned part-way leaves the stream desynchronized, so the error is not recoverable for
    /// this connection: every caller must drop **and shut down** the peer, which is what they already do
    /// for any send error.
    pub fn send_frame_by(&mut self, bytes: &[u8], deadline: std::time::Instant) -> io::Result<()> {
        if bytes.len() > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds MAX_FRAME_LEN",
            ));
        }
        let len = (bytes.len() as u32).to_le_bytes();
        write_all_by(&mut self.stream, &len, deadline)?;
        write_all_by(&mut self.stream, bytes, deadline)
    }

    /// Bound how long a blocking read may wait. Used on the handshake, so a peer that connects
    /// and then says nothing cannot pin the thread performing it. Cleared by
    /// [`FramedConn::new`](crate::transport::FramedConn::new), which makes the socket
    /// non-blocking instead.
    pub fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// The connection's two endpoints, `(local, peer)`.
    ///
    /// Used to key a link identically at both of its ends: the pair is the same two addresses on
    /// either side, merely swapped, so ordering it yields a name for the *connection* that both
    /// nodes compute without exchanging anything. The ephemeral port makes it unique per
    /// connection.
    pub fn endpoints(&self) -> io::Result<(std::net::SocketAddr, std::net::SocketAddr)> {
        Ok((self.stream.local_addr()?, self.stream.peer_addr()?))
    }

    /// Give up the wrapped stream, to hand a connection from a blocking handshake to a polled
    /// [`FramedConn`]. Safe precisely because `recv_framed` reads exactly the frames it parses and
    /// buffers nothing, so no bytes are stranded in the discarded wrapper.
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }
}

impl Transport for TcpTransport {
    fn send_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        send_framed(&mut self.stream, bytes)
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        recv_framed(&mut self.stream)
    }
}

/// Outbound bytes buffered on one connection before its poll-item stops producing. The duty
/// cycle's replacement for the backpressure a blocking `write_all` used to give for free: a
/// subscriber that stops reading must stop us reading its source, not grow a buffer without
/// bound. A subscriber held off long enough falls behind the origin's retention and is told so
/// with a `Gap` on its next resume — the documented outcome, reached honestly.
///
/// **Measured**, not guessed — see `stream::bench::measure_outbound_high_water_mark`, which sweeps
/// this against record size. Two runs agreed on the finding that matters: **throughput is flat
/// from 4 KiB to 32 MiB**, at every record size from 64 B to 256 KiB. The cap simply is not what
/// limits a subscriber that is keeping up; the earlier 8 MiB was never even approached (peak
/// buffered stayed under 3.5 MiB with a 32 MiB cap, and under 300 KiB for records ≤ 1 KiB).
///
/// So the number buys nothing but exposure, and the right size is the smallest that still leaves
/// margin. That follows from no-custody (DESIGN.md §5) rather than from the benchmark: **the real
/// buffer is the origin's log on disk.** Holding megabytes of records in RAM duplicates what the
/// log already holds durably, and buys only a slightly later throttle — while throttling costs
/// nothing, because the records stay in the log and the subscriber resumes from them. What decides
/// whether a slow subscriber survives is *retention*, not this.
///
/// 1 MiB keeps roughly 4× margin over the largest peak seen under a keeping-up subscriber, leaves
/// headroom for links less forgiving than the loopback the sweep runs on, and cuts the worst case
/// at [`MAX_CONNECTIONS`](https://docs.rs/) — 4096 connections — from 32 GiB to 4 GiB.
///
/// The bound is `MAX_PENDING_OUT + one record`: a record is always queued whole, and the cap only
/// gates *starting* another. `a_stalled_subscriber_cannot_grow_the_origins_buffer` pins that.
pub const MAX_PENDING_OUT: usize = 1 << 20; // 1 MiB — see the measurement above

/// How much to try to read from a socket per attempt.
const READ_CHUNK: usize = 64 << 10;

/// Compact the inbound buffer once this many consumed bytes sit at its front.
const COMPACT_AFTER: usize = 256 << 10;

/// A **non-blocking, resumable** framed connection — the form a duty cycle needs.
///
/// [`Transport`] cannot be polled. `recv_framed` uses `read_exact`, so a socket that has delivered
/// only half a frame either blocks the caller (fine for a thread, fatal for a shared loop) or, if
/// the socket were merely made non-blocking, discards the bytes it already read. Framing state
/// therefore has to outlive the call: an inbound buffer that accumulates until a frame is whole,
/// and an outbound buffer that drains as the socket allows.
///
/// Deliberately not a `Transport` implementation: the trait promises "receive the next frame,
/// blocking until one arrives", which is exactly the promise a poll-item must not make. The two
/// coexist — handshakes stay blocking (they run off the data path), and only forwarding is polled.
pub struct FramedConn {
    stream: TcpStream,
    inbox: Vec<u8>,
    inbox_off: usize,
    outbox: Vec<u8>,
    outbox_off: usize,
}

impl FramedConn {
    /// Take over an established stream for polled use. The handshake that preceded it must have
    /// left no buffered bytes — true of [`Transport`], which reads exactly the frames it parses.
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            inbox: Vec::new(),
            inbox_off: 0,
            outbox: Vec::new(),
            outbox_off: 0,
        })
    }

    /// The next complete frame, or `None` if one has not arrived yet. Never blocks.
    pub fn try_recv_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        loop {
            if let Some(frame) = self.take_buffered_frame()? {
                return Ok(Some(frame));
            }
            // Read straight into the tail of the inbox rather than via a scratch buffer, so a
            // partial frame costs no copy while it waits for the rest of itself.
            let filled = self.inbox.len();
            self.inbox.resize(filled + READ_CHUNK, 0);
            match self.stream.read(&mut self.inbox[filled..]) {
                Ok(0) => {
                    self.inbox.truncate(filled);
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "peer closed the connection",
                    ));
                }
                Ok(n) => self.inbox.truncate(filled + n),
                Err(e) => {
                    self.inbox.truncate(filled);
                    match e.kind() {
                        io::ErrorKind::WouldBlock => return Ok(None),
                        io::ErrorKind::Interrupted => continue,
                        _ => return Err(e),
                    }
                }
            }
        }
    }

    /// Split one whole frame off the front of the inbox, if one is there.
    fn take_buffered_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        let available = self.inbox.len() - self.inbox_off;
        if available < 4 {
            return Ok(None);
        }
        let prefix: [u8; 4] = self.inbox[self.inbox_off..self.inbox_off + 4]
            .try_into()
            .expect("4 bytes");
        let len = u32::from_le_bytes(prefix) as usize;
        if len > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incoming frame length exceeds MAX_FRAME_LEN",
            ));
        }
        if available < 4 + len {
            return Ok(None);
        }
        let body = self.inbox_off + 4;
        let frame = self.inbox[body..body + len].to_vec();
        self.inbox_off = body + len;
        // Drop consumed bytes: cheaply when the buffer is spent (the steady state), by one
        // memmove when a long run of small frames has left a large consumed prefix.
        if self.inbox_off == self.inbox.len() {
            self.inbox.clear();
            self.inbox_off = 0;
        } else if self.inbox_off >= COMPACT_AFTER {
            self.inbox.drain(..self.inbox_off);
            self.inbox_off = 0;
        }
        Ok(Some(frame))
    }

    /// Queue a frame and push as much as the socket will take. Never blocks; whatever does not
    /// fit stays buffered for [`flush`](Self::flush).
    pub fn queue_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds MAX_FRAME_LEN",
            ));
        }
        self.outbox
            .extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        self.outbox.extend_from_slice(bytes);
        self.flush()?;
        Ok(())
    }

    /// Drain buffered outbound bytes. `Ok(true)` once nothing is left pending.
    pub fn flush(&mut self) -> io::Result<bool> {
        while self.outbox_off < self.outbox.len() {
            match self.stream.write(&self.outbox[self.outbox_off..]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "socket accepted no bytes",
                    ));
                }
                Ok(n) => self.outbox_off += n,
                Err(e) => match e.kind() {
                    io::ErrorKind::WouldBlock => return Ok(false),
                    io::ErrorKind::Interrupted => continue,
                    _ => return Err(e),
                },
            }
        }
        self.outbox.clear();
        self.outbox_off = 0;
        Ok(true)
    }

    /// Bytes still waiting to go out — the backpressure signal against [`MAX_PENDING_OUT`].
    pub fn pending_out(&self) -> usize {
        self.outbox.len() - self.outbox_off
    }

    /// Shut the connection down in both directions.
    pub fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Both)
    }
}

/// TCP [`Listener`] yielding [`TcpTransport`] connections.
pub struct TcpListener {
    inner: std::net::TcpListener,
}

impl TcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: std::net::TcpListener::bind(addr)?,
        })
    }

    /// The bound local address (useful when binding to port 0).
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }
}

impl Listener for TcpListener {
    type Conn = TcpTransport;
    fn accept(&mut self) -> io::Result<TcpTransport> {
        let (stream, _peer) = self.inner.accept()?;
        TcpTransport::from_stream(stream)
    }
}

/// Unix-domain-socket [`Transport`] — the local client plane. Same framing as
/// [`TcpTransport`], but reachable only through a filesystem path, so who may talk to the
/// daemon is governed by directory/file permissions (the daemon places the socket under its
/// `0700` data dir) instead of being open to any local process that can reach a loopback
/// port. Cross-host planes stay on TCP; this is strictly the same-host client hop.
#[cfg(unix)]
pub struct UnixTransport {
    stream: std::os::unix::net::UnixStream,
}

#[cfg(unix)]
impl UnixTransport {
    /// Connect to a daemon's client-plane socket at `path`.
    pub fn connect<P: AsRef<std::path::Path>>(path: P) -> io::Result<Self> {
        Ok(Self {
            stream: std::os::unix::net::UnixStream::connect(path)?,
        })
    }

    /// Wrap an already-accepted stream (e.g. one from [`UnixListener::accept`]).
    pub fn from_stream(stream: std::os::unix::net::UnixStream) -> Self {
        Self { stream }
    }

    /// Duplicate the handle to the same connection (independent read/write halves), mirroring
    /// [`TcpTransport::try_clone`].
    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            stream: self.stream.try_clone()?,
        })
    }

    /// Shut down the connection in both directions, so a blocking `recv_frame` returns.
    pub fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Both)
    }
}

#[cfg(unix)]
impl Transport for UnixTransport {
    fn send_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        send_framed(&mut self.stream, bytes)
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        recv_framed(&mut self.stream)
    }
}

/// Unix-domain-socket [`Listener`] yielding [`UnixTransport`] connections. `bind` is a thin
/// wrapper; stale-socket cleanup and single-instance arbitration are daemon policy (see
/// `Node::bind_client`), kept out of this primitive.
#[cfg(unix)]
pub struct UnixListener {
    inner: std::os::unix::net::UnixListener,
}

#[cfg(unix)]
impl UnixListener {
    pub fn bind<P: AsRef<std::path::Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        // Name the path in the error. A Unix socket address has a hard length limit (~104 bytes)
        // and `bind` reports exceeding it as a bare "path must be shorter than SUN_LEN", which says
        // nothing about *which* path — and the path is usually derived (data dir + `client.sock`),
        // so it is not in front of whoever reads the message either.
        std::os::unix::net::UnixListener::bind(path)
            .map(|inner| Self { inner })
            .map_err(|e| io::Error::new(e.kind(), format!("binding {}: {e}", path.display())))
    }
}

#[cfg(unix)]
impl Listener for UnixListener {
    type Conn = UnixTransport;
    fn accept(&mut self) -> io::Result<UnixTransport> {
        let (stream, _addr) = self.inner.accept()?;
        Ok(UnixTransport::from_stream(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **A per-syscall timeout is not a per-frame bound**, and relying on it as one was a real defect:
    /// `write_all` retries whenever a syscall moved even one byte, so a peer that drains slowly — or
    /// stalls with a partly-filled buffer — stretches a single frame without limit. Measured before
    /// this: 4.1 s per wedged peer against a 2 s setting, and 19 s for a peer draining at 128 KiB/s
    /// where the timeout never fired at all.
    ///
    /// Written against a peer that accepts the connection and never reads a byte, which is what a
    /// stopped, swapping or paused daemon looks like from here.
    #[test]
    fn a_frame_write_gives_up_on_its_budget_even_while_bytes_are_moving() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept and never read. Held open for the duration, so the failure is a stalled peer rather
        // than a closed connection.
        let held = std::thread::spawn(move || listener.accept().unwrap().0);

        let mut conn = TcpTransport::connect(addr).unwrap();
        conn.set_write_timeout(Some(std::time::Duration::from_millis(50)))
            .unwrap();

        // Comfortably larger than any socket buffer, so the write cannot simply be absorbed.
        let big = vec![0u8; 8 << 20];
        let budget = std::time::Duration::from_millis(300);
        let started = std::time::Instant::now();
        let err = conn
            .send_frame_by(&big, std::time::Instant::now() + budget)
            .unwrap_err();
        let took = started.elapsed();

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(
            took < budget * 4,
            "the frame took {took:?} against a {budget:?} budget — the bound is on the syscall \
             again, not on the frame"
        );
        drop(held.join().unwrap());
    }

    /// The case a per-syscall timeout cannot catch at all: a peer that **is** draining, just far too
    /// slowly. Every syscall moves bytes, so `SO_SNDTIMEO` never fires and `write_all` runs to
    /// completion however long that takes — measured at a 19 s heartbeat gap from a single peer reading
    /// at 128 KiB/s, with the timeout set to 2 s. Only a deadline over the whole frame bounds this.
    #[test]
    fn a_frame_write_gives_up_on_a_peer_that_drains_too_slowly() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Stopped explicitly rather than by EOF: after the assertion the socket still holds megabytes,
        // and draining them at this rate would take a minute of test time for no added confidence.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let trickle = {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                let (mut conn, _) = listener.accept().unwrap();
                let mut buf = [0u8; 4096];
                // ~40 KiB/s: always progressing, never fast enough.
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if std::io::Read::read(&mut conn, &mut buf).unwrap_or(0) == 0 {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            })
        };

        let mut conn = TcpTransport::connect(addr).unwrap();
        conn.set_write_timeout(Some(std::time::Duration::from_millis(50)))
            .unwrap();
        let big = vec![0u8; 8 << 20];
        let budget = std::time::Duration::from_millis(300);
        let started = std::time::Instant::now();
        let err = conn
            .send_frame_by(&big, std::time::Instant::now() + budget)
            .unwrap_err();
        let took = started.elapsed();

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(
            took < budget * 4,
            "the write ran for {took:?} against a {budget:?} budget: a peer that drains slowly can \
             still hold the control plane for as long as it likes"
        );
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(conn);
        let _ = trickle.join();
    }

    /// The bound must not fire on a peer that is simply reading, however large the frame.
    #[test]
    fn a_draining_peer_is_not_dropped_for_a_large_frame() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let reader = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let mut sink = Vec::new();
            std::io::Read::read_to_end(&mut conn, &mut sink).unwrap();
            sink.len()
        });

        let mut conn = TcpTransport::connect(addr).unwrap();
        conn.set_write_timeout(Some(std::time::Duration::from_millis(50)))
            .unwrap();
        let big = vec![7u8; 8 << 20];
        conn.send_frame_by(
            &big,
            std::time::Instant::now() + std::time::Duration::from_secs(10),
        )
        .expect("a peer that reads must never be dropped for the size of a frame");
        drop(conn);
        assert_eq!(reader.join().unwrap(), big.len() + 4);
    }

    #[test]
    fn tcp_round_trips_frames_including_empty() {
        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut conn = listener.accept().unwrap();
            for _ in 0..2 {
                let f = conn.recv_frame().unwrap();
                conn.send_frame(&f).unwrap(); // echo
            }
        });

        let mut client = TcpTransport::connect(addr).unwrap();
        for payload in [b"hello frame".as_slice(), b"".as_slice()] {
            client.send_frame(payload).unwrap();
            assert_eq!(client.recv_frame().unwrap(), payload);
        }
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn uds_round_trips_frames_including_empty() {
        let mut dir = std::env::temp_dir();
        dir.push("xchnet-uds-roundtrip");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("sock");

        let mut listener = UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            let mut conn = listener.accept().unwrap();
            for _ in 0..2 {
                let f = conn.recv_frame().unwrap();
                conn.send_frame(&f).unwrap(); // echo
            }
        });

        let mut client = UnixTransport::connect(&path).unwrap();
        for payload in [b"hello frame".as_slice(), b"".as_slice()] {
            client.send_frame(payload).unwrap();
            assert_eq!(client.recv_frame().unwrap(), payload);
        }
        server.join().unwrap();
    }

    /// The property the duty cycle depends on and the blocking transport cannot give: a frame
    /// that arrives in pieces is reassembled across polls, with `None` (not a block, not a loss)
    /// in between. Sent byte-at-a-time with the receiver polling in between, so every frame is
    /// split at every possible offset.
    #[test]
    fn framed_conn_reassembles_frames_split_across_polls() {
        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let payloads: Vec<Vec<u8>> = vec![b"first".to_vec(), Vec::new(), vec![0x5A; 5000]];
        let expected = payloads.clone();

        let writer = std::thread::spawn(move || {
            let mut stream = TcpTransport::connect(addr).unwrap().into_stream();
            let mut wire = Vec::new();
            for p in &payloads {
                wire.extend_from_slice(&(p.len() as u32).to_le_bytes());
                wire.extend_from_slice(p);
            }
            // One byte per write, so the reader sees every partial-frame state there is.
            for b in wire {
                stream.write_all(&[b]).unwrap();
            }
            stream
        });

        let mut server = FramedConn::new(listener.accept().unwrap().into_stream()).unwrap();
        let mut got: Vec<Vec<u8>> = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while got.len() < expected.len() {
            assert!(std::time::Instant::now() < deadline, "timed out: {got:?}");
            match server.try_recv_frame().unwrap() {
                Some(frame) => got.push(frame),
                None => std::hint::spin_loop(), // nothing whole yet — the point of the test
            }
        }
        assert_eq!(got, expected);
        drop(writer.join().unwrap());
    }

    /// Outbound bytes survive a socket that will not take them all at once, and `pending_out`
    /// reports the backlog the poll-item throttles on.
    #[test]
    fn framed_conn_buffers_what_the_socket_will_not_take() {
        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let reader = std::thread::spawn(move || listener.accept().unwrap());
        let mut sender =
            FramedConn::new(TcpTransport::connect(addr).unwrap().into_stream()).unwrap();
        let mut peer = reader.join().unwrap();

        // Write far more than any socket buffer will absorb without a reader draining it.
        let big = vec![0xC3u8; 1 << 20];
        for _ in 0..64 {
            sender.queue_frame(&big).unwrap();
        }
        assert!(
            sender.pending_out() > 0,
            "a socket that cannot take 64 MiB must leave a backlog to flush"
        );

        // Draining the peer lets the backlog flush without losing or reordering anything.
        let drained = std::thread::spawn(move || {
            let mut seen = 0;
            for _ in 0..64 {
                assert_eq!(peer.recv_frame().unwrap().len(), big.len());
                seen += 1;
            }
            seen
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !sender.flush().unwrap() {
            assert!(std::time::Instant::now() < deadline, "flush never drained");
        }
        assert_eq!(sender.pending_out(), 0);
        assert_eq!(drained.join().unwrap(), 64);
    }

    #[test]
    fn tcp_carries_encoded_messages() {
        use crate::wire::{RecordFrame, StreamMsg};
        use crate::{RecordIndex, StreamId};

        let msg = StreamMsg::Record {
            stream_id: StreamId(3),
            frame: RecordFrame {
                index: RecordIndex(101),
                msg_type: 7,
                user_meta: 0xDEAD_BEEF,
                starts_segment: false,
                payload: vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
            },
        };

        let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut conn = listener.accept().unwrap();
            let blob = conn.recv_frame().unwrap();
            conn.send_frame(&blob).unwrap(); // echo the raw bytes
        });

        let mut client = TcpTransport::connect(addr).unwrap();
        client
            .send_frame(&crate::codec::encode_stream(&msg))
            .unwrap();
        let echoed = client.recv_frame().unwrap();
        assert_eq!(crate::codec::decode_stream(&echoed).unwrap(), msg);
        server.join().unwrap();
    }
}
