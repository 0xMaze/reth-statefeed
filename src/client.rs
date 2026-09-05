//! Small reusable building blocks for local statefeed consumers.

use alloy_primitives::U256;
use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::wire::Envelope;

/// A length-prefixed protobuf stream reader.
#[derive(Debug)]
pub struct FrameReader<R> {
    inner: R,
    max_frame_bytes: usize,
}

impl<R> FrameReader<R> {
    /// Wraps a stream and rejects payloads larger than `max_frame_bytes` before allocating them.
    pub const fn new(inner: R, max_frame_bytes: usize) -> Self {
        Self {
            inner,
            max_frame_bytes,
        }
    }

    /// Returns the wrapped stream.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// Reads one envelope. A clean EOF before a new length prefix returns `Ok(None)`.
    pub async fn read(&mut self) -> Result<Option<Envelope>, ClientError> {
        let mut length = [0u8; 4];
        match self.inner.read(&mut length[..1]).await? {
            0 => return Ok(None),
            1 => self.inner.read_exact(&mut length[1..]).await?,
            _ => unreachable!("the read buffer is one byte"),
        };

        let length = u32::from_be_bytes(length) as usize;
        if length > self.max_frame_bytes {
            return Err(ClientError::FrameTooLarge {
                actual: length,
                limit: self.max_frame_bytes,
            });
        }
        let mut payload = vec![0u8; length];
        self.inner.read_exact(&mut payload).await?;
        // Decode from an owned `Bytes` buffer so Prost can slice large `bytes` fields (notably
        // complete projections) without allocating and copying them a second time.
        Ok(Some(Envelope::decode(bytes::Bytes::from(payload))?))
    }
}

/// Zero-copy access to the packed values carried by snapshot and block events.
#[derive(Clone, Copy, Debug)]
pub struct PackedValues<'a> {
    bytes: &'a [u8],
}

impl<'a> PackedValues<'a> {
    /// Validates that there is exactly one 32-byte value for every dictionary key.
    pub fn new(bytes: &'a [u8], key_count: usize) -> Result<Self, ClientError> {
        let expected = key_count
            .checked_mul(32)
            .ok_or(ClientError::ProjectionLengthOverflow)?;
        if bytes.len() != expected {
            return Err(ClientError::InvalidProjectionLength {
                actual: bytes.len(),
                expected,
            });
        }
        Ok(Self { bytes })
    }

    /// Number of values in this projection.
    pub const fn len(&self) -> usize {
        self.bytes.len() / 32
    }

    /// Whether the projection contains no keys.
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Decodes one value by dense `key_id`.
    pub fn get(&self, key_id: usize) -> Option<U256> {
        let start = key_id.checked_mul(32)?;
        let end = start.checked_add(32)?;
        let value = self.bytes.get(start..end)?;
        Some(U256::from_be_slice(value))
    }

    /// Returns the original packed representation.
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// Errors produced before an event is accepted by a consumer.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Local socket read failed.
    #[error("statefeed stream I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Protobuf payload is malformed.
    #[error("invalid statefeed protobuf: {0}")]
    Decode(#[from] prost::DecodeError),
    /// Peer advertised a frame beyond the configured allocation limit.
    #[error("statefeed frame is {actual} bytes, limit is {limit}")]
    FrameTooLarge {
        /// Length from the wire prefix.
        actual: usize,
        /// Local consumer limit.
        limit: usize,
    },
    /// `key_count * 32` cannot fit in `usize`.
    #[error("statefeed projection length overflow")]
    ProjectionLengthOverflow,
    /// Packed projection length does not match its dictionary.
    #[error("statefeed projection is {actual} bytes, expected {expected}")]
    InvalidProjectionLength {
        /// Bytes received.
        actual: usize,
        /// Bytes required by the active dictionary.
        expected: usize,
    },
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::wire::{PROTOCOL_VERSION, envelope};

    #[test]
    fn packed_values_are_indexed_without_copying() {
        let mut bytes = vec![0u8; 64];
        bytes[31] = 7;
        bytes[63] = 9;
        let values = PackedValues::new(&bytes, 2).unwrap();
        assert_eq!(values.get(0), Some(U256::from(7)));
        assert_eq!(values.get(1), Some(U256::from(9)));
        assert_eq!(values.get(2), None);
        assert_eq!(values.get(usize::MAX), None);
    }

    #[tokio::test]
    async fn frame_reader_decodes_one_envelope() {
        let envelope = Envelope {
            protocol_version: PROTOCOL_VERSION,
            boot_id: vec![0; 16].into(),
            sequence: 1,
            config_generation: 1,
            emitted_at_monotonic_ns: 1,
            event: Some(envelope::Event::Snapshot(crate::wire::Snapshot {
                block: None,
                values: Bytes::new(),
            })),
        };
        let bytes = crate::wire::encode_frame(&envelope, 1024).unwrap();
        let mut reader = FrameReader::new(bytes.as_slice(), 1024);
        assert_eq!(reader.read().await.unwrap(), Some(envelope));
        assert_eq!(reader.read().await.unwrap(), None);
    }
}
