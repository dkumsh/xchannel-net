//! Transport abstraction.
//!
//! The replication engine is written against this trait so the substrate can be TCP
//! today and RDMA or a local-IPC shortcut tomorrow (the user explicitly wants the
//! option of "other IPC or local channels"). Intentionally minimal for now — a
//! framed, reliable, ordered byte pipe — and synchronous to match xchannel's blocking
//! reader model. An async variant can come later behind the same conceptual contract.

use std::io;

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
