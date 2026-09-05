//! Minimal protocol consumer for smoke tests and integration examples.

use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    path::PathBuf,
};

use clap::Parser;
use eyre::{Result, ensure};
use reth_statefeed::{
    client::{FrameReader, PackedValues},
    wire::{BlockRef, BlockStage, CAP_EXECUTED, PROTOCOL_VERSION, envelope},
};
use tokio::net::UnixStream;

#[derive(Debug, Parser)]
#[command(about = "Print and validate a local reth-statefeed stream")]
struct Args {
    /// Unix socket exported by reth-statefeed.
    #[arg(long, default_value = "/run/reth-statefeed/statefeed.sock")]
    socket: PathBuf,
    /// Maximum accepted protobuf payload size.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    max_frame_bytes: usize,
    /// Maximum candidate ancestry retained by this diagnostic consumer.
    #[arg(long, default_value_t = 4096)]
    candidate_cache_blocks: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.candidate_cache_blocks > 0,
        "candidate_cache_blocks must be greater than zero"
    );
    let stream = UnixStream::connect(&args.socket).await?;
    let mut reader = FrameReader::new(stream, args.max_frame_bytes);
    let mut cursor = StreamCursor::default();
    let mut active_generation = None;
    let mut key_count = 0;
    let mut boot_id = None;
    let mut capabilities = 0;
    let mut candidates = CandidateTracker::new(args.candidate_cache_blocks);

    while let Some(envelope) = reader.read().await? {
        ensure!(
            envelope.protocol_version == PROTOCOL_VERSION,
            "unsupported protocol version {}",
            envelope.protocol_version
        );
        ensure!(envelope.boot_id.len() == 16, "boot_id is not 16 bytes");
        if let Some(expected) = &boot_id {
            ensure!(
                expected == &envelope.boot_id,
                "boot_id changed within one connection"
            );
        } else {
            boot_id = Some(envelope.boot_id.clone());
        }
        let sequence = envelope.sequence;
        let generation = envelope.config_generation;
        let event = envelope
            .event
            .ok_or_else(|| eyre::eyre!("envelope has no event"))?;
        cursor.observe(sequence, generation, &event)?;
        match event {
            envelope::Event::Hello(hello) => {
                ensure!(
                    hello.genesis_hash.len() == 32,
                    "genesis hash is not 32 bytes"
                );
                capabilities = hello.capabilities;
                println!(
                    "hello seq={sequence} chain_id={} service={} capabilities=0x{:x}",
                    hello.chain_id, hello.service_version, hello.capabilities
                );
            }
            envelope::Event::ConfigActivated(config) => {
                if let Some(active) = active_generation {
                    ensure!(
                        generation >= active,
                        "config generation regressed from {active} to {generation}"
                    );
                }
                ensure!(
                    config
                        .keys
                        .iter()
                        .enumerate()
                        .all(|(index, key)| key.key_id as usize == index),
                    "dictionary key ids are not dense"
                );
                ensure!(
                    config
                        .keys
                        .iter()
                        .all(|key| key.address.len() == 20 && key.slot.len() == 32),
                    "dictionary contains an invalid address or slot length"
                );
                key_count = config.keys.len();
                active_generation = Some(generation);
                candidates.clear();
                println!("config seq={sequence} generation={generation} keys={key_count}");
            }
            envelope::Event::Snapshot(snapshot) => {
                validate_generation(active_generation, generation)?;
                let block = validate_block_ref(snapshot.block.as_ref())?;
                candidates.clear();
                candidates.insert(block.hash.clone(), block.parent_hash.clone());
                let values = PackedValues::new(&snapshot.values, key_count)?;
                println!(
                    "snapshot seq={sequence} generation={generation} block={} values={}",
                    block_number(snapshot.block.as_ref()),
                    values.len()
                );
            }
            envelope::Event::BlockState(state) => {
                validate_generation(active_generation, generation)?;
                let block = validate_block_ref(state.block.as_ref())?;
                let stage = BlockStage::try_from(state.stage)
                    .map_err(|_| eyre::eyre!("unknown block stage {}", state.stage))?;
                ensure!(stage != BlockStage::Unspecified, "unspecified block stage");
                if stage == BlockStage::Executed {
                    ensure!(
                        capabilities & CAP_EXECUTED != 0,
                        "peer emitted EXECUTED without advertising CAP_EXECUTED"
                    );
                }
                ensure!(
                    state.changed_bitmap.len() == key_count.div_ceil(8),
                    "candidate changed bitmap has the wrong length"
                );
                let values = PackedValues::new(&state.values, key_count)?;
                candidates.insert(block.hash.clone(), block.parent_hash.clone());
                println!(
                    "block seq={sequence} generation={generation} number={} stage={} values={}",
                    block_number(state.block.as_ref()),
                    format_args!("{stage:?}"),
                    values.len()
                );
            }
            envelope::Event::CanonicalHead(head) => {
                validate_generation(active_generation, generation)?;
                let block = validate_block_ref(head.block.as_ref())?;
                ensure!(
                    head.previous_block_hash.len() == 32,
                    "previous canonical hash is not 32 bytes"
                );
                ensure!(
                    head.changed_bitmap.is_empty()
                        || head.changed_bitmap.len() == key_count.div_ceil(8),
                    "canonical changed bitmap has the wrong length"
                );
                let values = PackedValues::new(&head.values, key_count)?;
                candidates.insert(block.hash.clone(), block.parent_hash.clone());
                println!(
                    "canonical seq={sequence} generation={generation} number={} values={}",
                    block_number(head.block.as_ref()),
                    values.len()
                );
            }
            envelope::Event::BlockValidated(event) => {
                validate_generation(active_generation, generation)?;
                ensure!(
                    event.block_hash.len() == 32,
                    "validated hash is not 32 bytes"
                );
                if candidates.contains_key(&event.block_hash) {
                    println!(
                        "validated seq={sequence} generation={generation} hash=0x{}",
                        alloy_primitives::hex::encode(event.block_hash)
                    );
                } else {
                    println!(
                        "ignored-unknown-validation seq={sequence} generation={generation} hash=0x{}",
                        alloy_primitives::hex::encode(event.block_hash)
                    );
                }
            }
            envelope::Event::BlockRejected(event) => {
                validate_generation(active_generation, generation)?;
                ensure!(
                    event.block_hash.len() == 32,
                    "rejected hash is not 32 bytes"
                );
                ensure!(!event.reason.is_empty(), "rejection reason is empty");
                candidates.remove_tree(&event.block_hash);
                println!(
                    "rejected seq={sequence} generation={generation} hash=0x{} reason={}",
                    alloy_primitives::hex::encode(event.block_hash),
                    event.reason
                );
            }
            envelope::Event::Gap(gap) => {
                validate_generation(active_generation, generation)?;
                ensure!(!gap.reason.is_empty(), "gap reason is empty");
                candidates.clear();
                println!(
                    "gap seq={sequence} generation={generation} last_contiguous={} reason={}",
                    gap.last_contiguous_sequence, gap.reason
                )
            }
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum StreamPhase {
    #[default]
    Hello,
    Config {
        baseline: u64,
        generation: u64,
    },
    InitialSnapshot {
        baseline: u64,
        generation: u64,
    },
    Live {
        last_sequence: u64,
        snapshot_required: bool,
    },
}

#[derive(Debug, Default)]
struct StreamCursor {
    phase: StreamPhase,
}

impl StreamCursor {
    fn observe(&mut self, sequence: u64, generation: u64, event: &envelope::Event) -> Result<()> {
        self.phase = match self.phase {
            StreamPhase::Hello => {
                ensure!(
                    matches!(event, envelope::Event::Hello(_)),
                    "first frame is not Hello"
                );
                StreamPhase::Config {
                    baseline: sequence,
                    generation,
                }
            }
            StreamPhase::Config {
                baseline,
                generation: handshake_generation,
            } => {
                ensure!(
                    matches!(event, envelope::Event::ConfigActivated(_)),
                    "second frame is not ConfigActivated"
                );
                validate_handshake_position(sequence, generation, baseline, handshake_generation)?;
                StreamPhase::InitialSnapshot {
                    baseline,
                    generation,
                }
            }
            StreamPhase::InitialSnapshot {
                baseline,
                generation: handshake_generation,
            } => {
                ensure!(
                    matches!(event, envelope::Event::Snapshot(_)),
                    "third frame is not Snapshot"
                );
                validate_handshake_position(sequence, generation, baseline, handshake_generation)?;
                StreamPhase::Live {
                    last_sequence: baseline,
                    snapshot_required: false,
                }
            }
            StreamPhase::Live {
                last_sequence,
                snapshot_required,
            } => {
                ensure!(
                    !matches!(event, envelope::Event::Hello(_)),
                    "Hello appeared after the handshake"
                );
                if snapshot_required {
                    ensure!(
                        matches!(event, envelope::Event::Snapshot(_)),
                        "expected Snapshot immediately after Gap or ConfigActivated"
                    );
                    ensure!(
                        Some(sequence) == last_sequence.checked_add(1),
                        "snapshot sequence {sequence} does not follow {last_sequence}"
                    );
                    StreamPhase::Live {
                        last_sequence: sequence,
                        snapshot_required: false,
                    }
                } else if let envelope::Event::Gap(gap) = event {
                    ensure!(
                        gap.last_contiguous_sequence == last_sequence,
                        "gap claims contiguous sequence {}, expected {last_sequence}",
                        gap.last_contiguous_sequence
                    );
                    ensure!(
                        sequence > last_sequence,
                        "gap sequence {sequence} does not advance past {last_sequence}"
                    );
                    StreamPhase::Live {
                        last_sequence: sequence,
                        snapshot_required: true,
                    }
                } else {
                    ensure!(
                        Some(sequence) == last_sequence.checked_add(1),
                        "live sequence {sequence} does not follow {last_sequence}"
                    );
                    StreamPhase::Live {
                        last_sequence: sequence,
                        snapshot_required: matches!(event, envelope::Event::ConfigActivated(_)),
                    }
                }
            }
        };
        Ok(())
    }
}

fn validate_handshake_position(
    sequence: u64,
    generation: u64,
    baseline: u64,
    handshake_generation: u64,
) -> Result<()> {
    ensure!(
        sequence == baseline,
        "handshake sequence {sequence} does not match baseline {baseline}"
    );
    ensure!(
        generation == handshake_generation,
        "handshake generation changed from {handshake_generation} to {generation}"
    );
    Ok(())
}

fn validate_generation(active: Option<u64>, event: u64) -> Result<()> {
    ensure!(
        active == Some(event),
        "event generation {event} does not match active generation {active:?}"
    );
    Ok(())
}

fn validate_block_ref(block: Option<&BlockRef>) -> Result<&BlockRef> {
    let block = block.ok_or_else(|| eyre::eyre!("event has no block reference"))?;
    ensure!(block.hash.len() == 32, "block hash is not 32 bytes");
    ensure!(
        block.parent_hash.len() == 32,
        "parent block hash is not 32 bytes"
    );
    Ok(block)
}

fn block_number(block: Option<&reth_statefeed::wire::BlockRef>) -> String {
    block
        .map(|block| block.number.to_string())
        .unwrap_or_else(|| "missing".into())
}

#[derive(Debug)]
struct CandidateTracker {
    parents: HashMap<Vec<u8>, Vec<u8>>,
    insertion_order: VecDeque<Vec<u8>>,
    limit: usize,
}

impl CandidateTracker {
    fn new(limit: usize) -> Self {
        Self {
            parents: HashMap::with_capacity(limit.min(4096)),
            insertion_order: VecDeque::with_capacity(limit.min(4096)),
            limit,
        }
    }

    fn insert(&mut self, hash: Vec<u8>, parent_hash: Vec<u8>) {
        match self.parents.entry(hash) {
            Entry::Occupied(mut entry) => {
                entry.insert(parent_hash);
            }
            Entry::Vacant(entry) => {
                self.insertion_order.push_back(entry.key().clone());
                entry.insert(parent_hash);
            }
        }
        while self.parents.len() > self.limit {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.parents.remove(&oldest);
            }
        }
    }

    fn clear(&mut self) {
        self.parents.clear();
        self.insertion_order.clear();
    }

    fn contains_key(&self, hash: &[u8]) -> bool {
        self.parents.contains_key(hash)
    }

    fn remove_tree(&mut self, root: &[u8]) {
        let mut pending = vec![root.to_vec()];
        while let Some(parent) = pending.pop() {
            pending.extend(
                self.parents
                    .iter()
                    .filter(|(_, parent_hash)| parent_hash.as_slice() == parent)
                    .map(|(hash, _)| hash.clone()),
            );
            self.parents.remove(&parent);
        }
        self.insertion_order
            .retain(|hash| self.parents.contains_key(hash));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_removes_known_descendants() {
        let canonical = vec![1; 32];
        let rejected = vec![2; 32];
        let child = vec![3; 32];
        let sibling = vec![4; 32];
        let mut candidates = CandidateTracker::new(8);
        candidates.insert(canonical.clone(), vec![0; 32]);
        candidates.insert(rejected.clone(), canonical.clone());
        candidates.insert(child.clone(), rejected.clone());
        candidates.insert(sibling.clone(), canonical.clone());

        candidates.remove_tree(&rejected);

        assert!(candidates.contains_key(&canonical));
        assert!(candidates.contains_key(&sibling));
        assert!(!candidates.contains_key(&rejected));
        assert!(!candidates.contains_key(&child));
    }

    #[test]
    fn candidate_tracking_is_bounded() {
        let mut candidates = CandidateTracker::new(2);
        candidates.insert(vec![1; 32], vec![0; 32]);
        candidates.insert(vec![2; 32], vec![1; 32]);
        candidates.insert(vec![3; 32], vec![2; 32]);

        assert!(!candidates.contains_key(&[1; 32]));
        assert!(candidates.contains_key(&[2; 32]));
        assert!(candidates.contains_key(&[3; 32]));
    }

    #[test]
    fn stream_cursor_enforces_handshake_and_gap_recovery() {
        let mut cursor = StreamCursor::default();
        cursor
            .observe(
                7,
                3,
                &envelope::Event::Hello(reth_statefeed::wire::Hello::default()),
            )
            .unwrap();
        cursor
            .observe(
                7,
                3,
                &envelope::Event::ConfigActivated(reth_statefeed::wire::ConfigActivated::default()),
            )
            .unwrap();
        cursor
            .observe(
                7,
                3,
                &envelope::Event::Snapshot(reth_statefeed::wire::Snapshot::default()),
            )
            .unwrap();
        cursor
            .observe(
                10,
                3,
                &envelope::Event::Gap(reth_statefeed::wire::Gap {
                    last_contiguous_sequence: 7,
                    reason: "encode_failure".into(),
                }),
            )
            .unwrap();

        assert!(
            cursor
                .observe(
                    11,
                    3,
                    &envelope::Event::BlockValidated(
                        reth_statefeed::wire::BlockValidated::default(),
                    ),
                )
                .is_err()
        );
        cursor
            .observe(
                11,
                3,
                &envelope::Event::Snapshot(reth_statefeed::wire::Snapshot::default()),
            )
            .unwrap();
    }

    #[test]
    fn stream_cursor_rejects_unannounced_sequence_holes() {
        let mut cursor = StreamCursor::default();
        cursor
            .observe(
                0,
                1,
                &envelope::Event::Hello(reth_statefeed::wire::Hello::default()),
            )
            .unwrap();
        cursor
            .observe(
                0,
                1,
                &envelope::Event::ConfigActivated(reth_statefeed::wire::ConfigActivated::default()),
            )
            .unwrap();
        cursor
            .observe(
                0,
                1,
                &envelope::Event::Snapshot(reth_statefeed::wire::Snapshot::default()),
            )
            .unwrap();

        assert!(
            cursor
                .observe(
                    2,
                    1,
                    &envelope::Event::BlockValidated(
                        reth_statefeed::wire::BlockValidated::default(),
                    ),
                )
                .is_err()
        );
    }
}
