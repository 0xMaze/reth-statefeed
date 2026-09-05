//! Versioned protobuf messages and length-prefixed Unix-stream framing.
//!
//! Types are declared directly with `prost` derives so building the node does not depend on a
//! system `protoc` binary. Field numbers are part of the public protocol and must never be reused.

use bytes::Bytes;
use prost::Message;
use thiserror::Error;

/// Current statefeed wire protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Capability bit: full projections are emitted for candidate blocks.
pub const CAP_FULL_PROJECTIONS: u64 = 1 << 0;
/// Capability bit: validated lifecycle notifications are available.
pub const CAP_VALIDATED: u64 = 1 << 1;
/// Capability bit: canonical lifecycle notifications are available.
pub const CAP_CANONICAL: u64 = 1 << 2;
/// Capability bit: rejected-payload notifications are available.
pub const CAP_REJECTED: u64 = 1 << 3;
/// Capability bit: pre-validation executed projections are available.
pub const CAP_EXECUTED: u64 = 1 << 4;

/// Common envelope for every server-to-consumer message.
#[derive(Clone, PartialEq, Message)]
pub struct Envelope {
    /// Wire protocol version used to encode this message.
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    /// Random process incarnation id; changes after every restart.
    #[prost(bytes = "bytes", tag = "2")]
    pub boot_id: Bytes,
    /// Monotonic publication sequence within a boot id.
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
    /// Watch configuration generation used by the payload.
    #[prost(uint64, tag = "4")]
    pub config_generation: u64,
    /// `CLOCK_MONOTONIC`-style emission timestamp relative to process start.
    #[prost(uint64, tag = "5")]
    pub emitted_at_monotonic_ns: u64,
    /// Event payload.
    #[prost(oneof = "envelope::Event", tags = "10, 11, 12, 13, 14, 15, 16, 17")]
    pub event: Option<envelope::Event>,
}

/// Envelope payload variants.
pub mod envelope {
    use prost::Oneof;

    use super::{
        BlockRejected, BlockState, BlockValidated, CanonicalHead, ConfigActivated, Gap, Hello,
        Snapshot,
    };

    /// One concrete statefeed event.
    #[derive(Clone, PartialEq, Oneof)]
    pub enum Event {
        /// Connection metadata and chain identity.
        #[prost(message, tag = "10")]
        Hello(Hello),
        /// Active dictionary and its generation.
        #[prost(message, tag = "11")]
        ConfigActivated(ConfigActivated),
        /// Complete anchored state projection.
        #[prost(message, tag = "12")]
        Snapshot(Snapshot),
        /// Complete candidate projection.
        #[prost(message, tag = "13")]
        BlockState(BlockState),
        /// Validation upgrade for an early candidate.
        #[prost(message, tag = "14")]
        BlockValidated(BlockValidated),
        /// Rejection of an early candidate.
        #[prost(message, tag = "15")]
        BlockRejected(BlockRejected),
        /// Canonical head transition.
        #[prost(message, tag = "16")]
        CanonicalHead(CanonicalHead),
        /// Stream discontinuity requiring recovery.
        #[prost(message, tag = "17")]
        Gap(Gap),
    }
}

/// Initial connection metadata.
#[derive(Clone, PartialEq, Message)]
pub struct Hello {
    /// Semver of the statefeed binary.
    #[prost(string, tag = "1")]
    pub service_version: String,
    /// EVM chain id.
    #[prost(uint64, tag = "2")]
    pub chain_id: u64,
    /// Genesis block hash as exactly 32 bytes.
    #[prost(bytes = "vec", tag = "3")]
    pub genesis_hash: Vec<u8>,
    /// Bitset of `CAP_*` constants.
    #[prost(uint64, tag = "4")]
    pub capabilities: u64,
}

/// Dense dictionary for one atomically activated watch generation.
#[derive(Clone, PartialEq, Message)]
pub struct ConfigActivated {
    /// Keys ordered by `key_id`; projection values use the same order.
    #[prost(message, repeated, tag = "1")]
    pub keys: Vec<WatchKey>,
}

/// One dictionary entry.
#[derive(Clone, PartialEq, Message)]
pub struct WatchKey {
    /// Dense projection index.
    #[prost(uint32, tag = "1")]
    pub key_id: u32,
    /// Stable operator-defined identity.
    #[prost(string, tag = "2")]
    pub id: String,
    /// Ethereum address as exactly 20 bytes.
    #[prost(bytes = "vec", tag = "3")]
    pub address: Vec<u8>,
    /// Physical storage key as exactly 32 bytes.
    #[prost(bytes = "vec", tag = "4")]
    pub slot: Vec<u8>,
}

/// Minimal block identity shared by projection events.
#[derive(Clone, PartialEq, Message)]
pub struct BlockRef {
    /// Block number.
    #[prost(uint64, tag = "1")]
    pub number: u64,
    /// Block hash as exactly 32 bytes.
    #[prost(bytes = "vec", tag = "2")]
    pub hash: Vec<u8>,
    /// Parent hash as exactly 32 bytes.
    #[prost(bytes = "vec", tag = "3")]
    pub parent_hash: Vec<u8>,
    /// Consensus block timestamp.
    #[prost(uint64, tag = "4")]
    pub timestamp: u64,
}

/// Complete canonical state at an anchored block.
#[derive(Clone, PartialEq, Message)]
pub struct Snapshot {
    /// Canonical anchor.
    #[prost(message, optional, tag = "1")]
    pub block: Option<BlockRef>,
    /// Contiguous `32 * key_count` bytes ordered by the active dictionary.
    ///
    /// Value `i` occupies `values[i * 32..(i + 1) * 32]` in big-endian form. Packing the
    /// projection into one protobuf field avoids one allocation and one length prefix per key.
    #[prost(bytes = "bytes", tag = "2")]
    pub values: Bytes,
}

/// Lifecycle stage attached to a complete block projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum BlockStage {
    /// Missing or unknown lifecycle stage. Consumers must reject it.
    Unspecified = 0,
    /// EVM execution completed but validation has not.
    Executed = 1,
    /// Consensus and state-root validation completed successfully.
    Validated = 2,
}

/// Complete projection for one candidate block.
#[derive(Clone, PartialEq, Message)]
pub struct BlockState {
    /// Candidate lifecycle stage.
    #[prost(enumeration = "BlockStage", tag = "1")]
    pub stage: i32,
    /// Candidate block identity.
    #[prost(message, optional, tag = "2")]
    pub block: Option<BlockRef>,
    /// Contiguous `32 * key_count` bytes ordered by the active dictionary.
    #[prost(bytes = "bytes", tag = "3")]
    pub values: Bytes,
    /// Little-endian bitset where bit `i` means key `i` changed in this block.
    #[prost(bytes = "bytes", tag = "4")]
    pub changed_bitmap: Bytes,
}

/// Validation upgrade for a previously emitted early candidate.
#[derive(Clone, PartialEq, Message)]
pub struct BlockValidated {
    /// Validated block hash.
    #[prost(bytes = "vec", tag = "1")]
    pub block_hash: Vec<u8>,
}

/// Invalidates a previously emitted early candidate.
#[derive(Clone, PartialEq, Message)]
pub struct BlockRejected {
    /// Rejected block hash.
    #[prost(bytes = "vec", tag = "1")]
    pub block_hash: Vec<u8>,
    /// Stable machine-readable category.
    #[prost(string, tag = "2")]
    pub reason: String,
}

/// Canonical chain transition selected by forkchoice.
#[derive(Clone, PartialEq, Message)]
pub struct CanonicalHead {
    /// Previously selected head as exactly 32 bytes.
    #[prost(bytes = "vec", tag = "2")]
    pub previous_block_hash: Vec<u8>,
    /// Complete canonical block identity.
    #[prost(message, optional, tag = "3")]
    pub block: Option<BlockRef>,
    /// Complete packed canonical projection; see [`Snapshot::values`].
    #[prost(bytes = "bytes", tag = "4")]
    pub values: Bytes,
    /// Little-endian bitset of values changed from the previous canonical head.
    #[prost(bytes = "bytes", tag = "5")]
    pub changed_bitmap: Bytes,
}

/// Announces loss of continuity.
#[derive(Clone, PartialEq, Message)]
pub struct Gap {
    /// Last sequence known to be contiguous.
    #[prost(uint64, tag = "1")]
    pub last_contiguous_sequence: u64,
    /// Stable machine-readable cause.
    #[prost(string, tag = "2")]
    pub reason: String,
}

/// Failure to encode a bounded wire frame.
#[derive(Debug, Error)]
pub enum EncodeError {
    /// Protobuf payload is larger than configured.
    #[error("encoded statefeed message is {actual} bytes, limit is {limit}")]
    FrameTooLarge {
        /// Encoded protobuf bytes.
        actual: usize,
        /// Configured payload limit.
        limit: usize,
    },
    /// The length prefix cannot represent the payload.
    #[error("encoded statefeed message cannot fit a u32 length prefix")]
    LengthOverflow,
}

/// Encodes one protobuf envelope with a four-byte big-endian length prefix.
pub fn encode_frame(envelope: &Envelope, max_frame_bytes: usize) -> Result<Vec<u8>, EncodeError> {
    let payload_len = envelope.encoded_len();
    if payload_len > max_frame_bytes {
        return Err(EncodeError::FrameTooLarge {
            actual: payload_len,
            limit: max_frame_bytes,
        });
    }
    let length = u32::try_from(payload_len).map_err(|_| EncodeError::LengthOverflow)?;
    let frame_capacity = payload_len
        .checked_add(4)
        .ok_or(EncodeError::LengthOverflow)?;
    let mut frame = Vec::with_capacity(frame_capacity);
    frame.extend_from_slice(&length.to_be_bytes());
    envelope
        .encode(&mut frame)
        .expect("Vec has enough capacity and prost encoding is infallible");
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_prefix_matches_protobuf_payload() {
        let envelope = Envelope {
            protocol_version: PROTOCOL_VERSION,
            boot_id: vec![1; 16].into(),
            sequence: 1,
            config_generation: 1,
            emitted_at_monotonic_ns: 10,
            event: Some(envelope::Event::Gap(Gap {
                last_contiguous_sequence: 0,
                reason: "test".into(),
            })),
        };
        let frame = encode_frame(&envelope, 1024).unwrap();
        let payload_len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(payload_len, frame.len() - 4);
        assert_eq!(Envelope::decode(&frame[4..]).unwrap(), envelope);
    }

    #[test]
    fn rejects_oversized_frame() {
        let envelope = Envelope {
            protocol_version: PROTOCOL_VERSION,
            boot_id: vec![0; 1_024].into(),
            sequence: 0,
            config_generation: 0,
            emitted_at_monotonic_ns: 0,
            event: None,
        };
        assert!(matches!(
            encode_frame(&envelope, 64),
            Err(EncodeError::FrameTooLarge { .. })
        ));
    }
}
