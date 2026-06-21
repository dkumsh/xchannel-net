//! Hand-rolled little-endian wire codec — zero external dependencies.
//!
//! Encoding is little-endian throughout (matching xchannel's framing). The
//! [`Transport`](crate::transport::Transport) owns length-delimiting of whole frames, so
//! this codec only maps **one** [`ControlMsg`]/[`StreamMsg`] to/from its bytes — there is
//! no outer length prefix here.
//!
//! Layout per message: a 1-byte variant tag, then fields. Fixed-width integers are LE;
//! variable-length bytes/strings are `u32`-length-prefixed. The `StreamMsg::Record` hot
//! path is a flat fixed header + payload (no per-field framing beyond the payload length).
//!
//! `encode_*_into(&mut Vec<u8>, …)` are the primitives — the caller supplies (and can
//! reuse) the buffer, which matters on the per-record hot path. `encode_*` are convenience
//! wrappers that allocate.
//!
//! Decode borrows from the input slice and copies only what the owned wire types require
//! (today, `RecordFrame::payload`). A borrowed zero-copy read path can be added later for
//! the hot read side.

use crate::identity::ChannelIdentity;
use crate::wire::{ControlMsg, RecordFrame, StreamMsg};
use crate::{NodeId, RecordIndex, StreamId};
use std::io;

#[inline]
fn invalid(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

// ---------- low-level writer (appends to a caller-owned buffer) ----------

struct W<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> W<'a> {
    #[inline]
    fn new(buf: &'a mut Vec<u8>) -> Self {
        Self { buf }
    }
    #[inline]
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    #[inline]
    fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    /// `u32` length prefix + raw bytes. Messages are bounded well under 4 GiB.
    #[inline]
    fn bytes(&mut self, b: &[u8]) {
        debug_assert!(
            b.len() <= u32::MAX as usize,
            "wire field exceeds u32 length"
        );
        self.u32(b.len() as u32);
        self.buf.extend_from_slice(b);
    }
    #[inline]
    fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }
    /// Socket address as its text form (handles v4/v6 uniformly).
    #[inline]
    fn addr(&mut self, a: std::net::SocketAddr) {
        self.str(&a.to_string());
    }
}

// ---------- low-level reader (borrows the input frame) ----------

struct R<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> R<'a> {
    #[inline]
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    #[inline]
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| invalid("length overflow"))?;
        if end > self.buf.len() {
            return Err(invalid("unexpected end of frame"));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    #[inline]
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    #[inline]
    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    #[inline]
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    #[inline]
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    #[inline]
    fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    #[inline]
    fn str(&mut self) -> io::Result<String> {
        let b = self.bytes()?;
        std::str::from_utf8(b)
            .map(str::to_owned)
            .map_err(|_| invalid("invalid utf-8 in string field"))
    }
    #[inline]
    fn addr(&mut self) -> io::Result<std::net::SocketAddr> {
        self.str()?
            .parse()
            .map_err(|_| invalid("invalid socket address field"))
    }
    /// Reject a frame with trailing bytes — a sign of version/shape mismatch.
    #[inline]
    fn finish(self) -> io::Result<()> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(invalid("trailing bytes after frame body"))
        }
    }
}

// ---------- ChannelIdentity ----------

fn put_identity(w: &mut W, id: &ChannelIdentity) {
    w.str(&id.name);
    w.u64(id.owner.0);
    w.u32(id.region_size);
    w.u32(id.mtu);
    w.u64(id.earliest_index.0);
    w.u64(id.registered_at_nanos);
}

fn get_identity(r: &mut R) -> io::Result<ChannelIdentity> {
    Ok(ChannelIdentity {
        name: r.str()?,
        owner: NodeId(r.u64()?),
        region_size: r.u32()?,
        mtu: r.u32()?,
        earliest_index: RecordIndex(r.u64()?),
        registered_at_nanos: r.u64()?,
    })
}

fn put_identities(w: &mut W, ids: &[ChannelIdentity]) {
    w.u32(ids.len() as u32);
    for id in ids {
        put_identity(w, id);
    }
}

fn get_identities(r: &mut R) -> io::Result<Vec<ChannelIdentity>> {
    let n = r.u32()? as usize;
    // Don't pre-allocate `n` blindly — a corrupt count must not OOM us; the loop is
    // bounded by the frame length via `take`.
    let mut v = Vec::new();
    for _ in 0..n {
        v.push(get_identity(r)?);
    }
    Ok(v)
}

// ---------- ControlMsg ----------

mod control_tag {
    pub const REGISTER: u8 = 0;
    pub const DEREGISTER: u8 = 1;
    pub const REGISTRY_DELTA: u8 = 2;
    pub const REGISTRY_SYNC: u8 = 3;
    pub const HEARTBEAT: u8 = 4;
    pub const REGISTER_REJECTED: u8 = 5;
}

/// Encode a control-plane message into `buf` (appended; caller clears for reuse).
pub fn encode_control_into(buf: &mut Vec<u8>, m: &ControlMsg) {
    let mut w = W::new(buf);
    match m {
        ControlMsg::Register(id) => {
            w.u8(control_tag::REGISTER);
            put_identity(&mut w, id);
        }
        ControlMsg::Deregister { name, owner } => {
            w.u8(control_tag::DEREGISTER);
            w.str(name);
            w.u64(owner.0);
        }
        ControlMsg::RegistryDelta(ids) => {
            w.u8(control_tag::REGISTRY_DELTA);
            put_identities(&mut w, ids);
        }
        ControlMsg::RegistrySync(ids) => {
            w.u8(control_tag::REGISTRY_SYNC);
            put_identities(&mut w, ids);
        }
        ControlMsg::Heartbeat { node, addr } => {
            w.u8(control_tag::HEARTBEAT);
            w.u64(node.0);
            w.addr(*addr);
        }
        ControlMsg::RegisterRejected { name, winner } => {
            w.u8(control_tag::REGISTER_REJECTED);
            w.str(name);
            w.u64(winner.0);
        }
    }
}

/// Convenience: allocate a fresh buffer.
pub fn encode_control(m: &ControlMsg) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_control_into(&mut buf, m);
    buf
}

pub fn decode_control(bytes: &[u8]) -> io::Result<ControlMsg> {
    let mut r = R::new(bytes);
    let m = match r.u8()? {
        control_tag::REGISTER => ControlMsg::Register(get_identity(&mut r)?),
        control_tag::DEREGISTER => ControlMsg::Deregister {
            name: r.str()?,
            owner: NodeId(r.u64()?),
        },
        control_tag::REGISTRY_DELTA => ControlMsg::RegistryDelta(get_identities(&mut r)?),
        control_tag::REGISTRY_SYNC => ControlMsg::RegistrySync(get_identities(&mut r)?),
        control_tag::HEARTBEAT => ControlMsg::Heartbeat {
            node: NodeId(r.u64()?),
            addr: r.addr()?,
        },
        control_tag::REGISTER_REJECTED => ControlMsg::RegisterRejected {
            name: r.str()?,
            winner: NodeId(r.u64()?),
        },
        _ => return Err(invalid("unknown ControlMsg tag")),
    };
    r.finish()?;
    Ok(m)
}

// ---------- StreamMsg ----------

mod stream_tag {
    pub const SUBSCRIBE: u8 = 0;
    pub const SUBSCRIBE_ACK: u8 = 1;
    pub const RECORD: u8 = 2;
    pub const GAP: u8 = 3;
}

/// Encode a stream-plane message into `buf` (appended; caller clears for reuse).
pub fn encode_stream_into(buf: &mut Vec<u8>, m: &StreamMsg) {
    let mut w = W::new(buf);
    match m {
        StreamMsg::Subscribe { name, from } => {
            w.u8(stream_tag::SUBSCRIBE);
            w.str(name);
            w.u64(from.0);
        }
        StreamMsg::SubscribeAck {
            name,
            stream_id,
            start,
            head,
            region_size,
            mtu,
        } => {
            w.u8(stream_tag::SUBSCRIBE_ACK);
            w.str(name);
            w.u32(stream_id.0);
            w.u64(start.0);
            w.u64(head.0);
            w.u32(*region_size);
            w.u32(*mtu);
        }
        StreamMsg::Record { stream_id, frame } => {
            // Hot path: flat fixed header + payload, no per-field framing.
            w.u8(stream_tag::RECORD);
            w.u32(stream_id.0);
            w.u64(frame.index.0);
            w.u16(frame.msg_type);
            w.u64(frame.user_meta);
            w.bytes(&frame.payload);
        }
        StreamMsg::Gap {
            name,
            earliest,
            head,
        } => {
            w.u8(stream_tag::GAP);
            w.str(name);
            w.u64(earliest.0);
            w.u64(head.0);
        }
    }
}

/// Convenience: allocate a fresh buffer.
pub fn encode_stream(m: &StreamMsg) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_stream_into(&mut buf, m);
    buf
}

pub fn decode_stream(bytes: &[u8]) -> io::Result<StreamMsg> {
    let mut r = R::new(bytes);
    let m = match r.u8()? {
        stream_tag::SUBSCRIBE => StreamMsg::Subscribe {
            name: r.str()?,
            from: RecordIndex(r.u64()?),
        },
        stream_tag::SUBSCRIBE_ACK => StreamMsg::SubscribeAck {
            name: r.str()?,
            stream_id: StreamId(r.u32()?),
            start: RecordIndex(r.u64()?),
            head: RecordIndex(r.u64()?),
            region_size: r.u32()?,
            mtu: r.u32()?,
        },
        stream_tag::RECORD => {
            let stream_id = StreamId(r.u32()?);
            let index = RecordIndex(r.u64()?);
            let msg_type = r.u16()?;
            let user_meta = r.u64()?;
            let payload = r.bytes()?.to_vec();
            StreamMsg::Record {
                stream_id,
                frame: RecordFrame {
                    index,
                    msg_type,
                    user_meta,
                    payload,
                },
            }
        }
        stream_tag::GAP => StreamMsg::Gap {
            name: r.str()?,
            earliest: RecordIndex(r.u64()?),
            head: RecordIndex(r.u64()?),
        },
        _ => return Err(invalid("unknown StreamMsg tag")),
    };
    r.finish()?;
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(name: &str, owner: u64) -> ChannelIdentity {
        ChannelIdentity {
            name: name.to_string(),
            owner: NodeId(owner),
            region_size: 1 << 20,
            mtu: 0,
            earliest_index: RecordIndex(7),
            registered_at_nanos: 123_456_789,
        }
    }

    fn control_cases() -> Vec<ControlMsg> {
        vec![
            ControlMsg::Register(ident("md.aapl", 1)),
            ControlMsg::Deregister {
                name: "md.aapl".into(),
                owner: NodeId(1),
            },
            ControlMsg::RegistryDelta(vec![ident("a", 1), ident("b", 2)]),
            ControlMsg::RegistrySync(vec![]),
            ControlMsg::Heartbeat {
                node: NodeId(42),
                addr: "127.0.0.1:7000".parse().unwrap(),
            },
            ControlMsg::RegisterRejected {
                name: "dup".into(),
                winner: NodeId(9),
            },
        ]
    }

    fn stream_cases() -> Vec<StreamMsg> {
        vec![
            StreamMsg::Subscribe {
                name: "md.aapl".into(),
                from: RecordIndex(0),
            },
            StreamMsg::SubscribeAck {
                name: "md.aapl".into(),
                stream_id: StreamId(3),
                start: RecordIndex(100),
                head: RecordIndex(250),
                region_size: 1 << 20,
                mtu: 0,
            },
            StreamMsg::Record {
                stream_id: StreamId(3),
                frame: RecordFrame {
                    index: RecordIndex(101),
                    msg_type: 7,
                    user_meta: 0xDEAD_BEEF,
                    payload: vec![1, 2, 3, 4, 5],
                },
            },
            StreamMsg::Record {
                stream_id: StreamId(3),
                frame: RecordFrame {
                    index: RecordIndex(102),
                    msg_type: 0,
                    user_meta: 0,
                    payload: vec![], // empty payload round-trips
                },
            },
            StreamMsg::Gap {
                name: "md.aapl".into(),
                earliest: RecordIndex(50),
                head: RecordIndex(250),
            },
        ]
    }

    #[test]
    fn control_round_trips() {
        for m in control_cases() {
            let bytes = encode_control(&m);
            assert_eq!(decode_control(&bytes).unwrap(), m);
        }
    }

    #[test]
    fn stream_round_trips() {
        for m in stream_cases() {
            let bytes = encode_stream(&m);
            assert_eq!(decode_stream(&bytes).unwrap(), m);
        }
    }

    #[test]
    fn encode_into_reuses_buffer() {
        let mut buf = Vec::new();
        for m in stream_cases() {
            buf.clear();
            encode_stream_into(&mut buf, &m);
            assert_eq!(decode_stream(&buf).unwrap(), m);
        }
    }

    #[test]
    fn truncated_frame_errors() {
        let bytes = encode_stream(&stream_cases()[2]); // a Record
        for cut in 0..bytes.len() {
            assert!(decode_stream(&bytes[..cut]).is_err(), "cut {cut} must fail");
        }
    }

    #[test]
    fn trailing_bytes_error() {
        let mut bytes = encode_control(&ControlMsg::Heartbeat {
            node: NodeId(1),
            addr: "127.0.0.1:1".parse().unwrap(),
        });
        bytes.push(0xAB);
        assert!(decode_control(&bytes).is_err());
    }

    #[test]
    fn unknown_tag_errors() {
        assert!(decode_control(&[0xFF]).is_err());
        assert!(decode_stream(&[0xFF]).is_err());
    }

    #[test]
    fn empty_frame_errors() {
        assert!(decode_control(&[]).is_err());
        assert!(decode_stream(&[]).is_err());
    }
}
