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
/// length prefix can request. Far above any real message — records are bounded by the
/// channel's region size, which is orders of magnitude smaller.
pub const MAX_FRAME_LEN: usize = 1 << 30; // 1 GiB

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
        if bytes.len() > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds MAX_FRAME_LEN",
            ));
        }
        self.stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
        self.stream.write_all(bytes)?;
        Ok(())
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incoming frame length exceeds MAX_FRAME_LEN",
            ));
        }
        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf)?;
        Ok(buf)
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
