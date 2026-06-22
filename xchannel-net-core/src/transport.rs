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
}

impl Transport for TcpTransport {
    fn send_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        send_framed(&mut self.stream, bytes)
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        recv_framed(&mut self.stream)
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
        Ok(Self {
            inner: std::os::unix::net::UnixListener::bind(path)?,
        })
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
