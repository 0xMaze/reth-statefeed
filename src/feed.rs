//! Non-blocking producer used by Reth's validation thread.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

use alloy_primitives::{B256, U256};
use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use metrics::{Counter, Gauge, Histogram};
use reth::revm::revm::database::BundleState;
use tokio::sync::oneshot;

use crate::watch::{AddressWatch, BlockChanges, Generation, SlotChange, WatchSet};

const ENQUEUE_LOCKED: u64 = 1 << 63;
const DROP_COUNT_MASK: u64 = !ENQUEUE_LOCKED;

/// Identity and ordering data for an executed block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Block number.
    pub number: u64,
    /// Block hash.
    pub hash: B256,
    /// Parent block hash.
    pub parent_hash: B256,
    /// Consensus timestamp.
    pub timestamp: u64,
}

/// Numbered consensus checkpoint resolved by Reth while applying forkchoice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointMeta {
    /// Block number.
    pub number: u64,
    /// Block hash.
    pub hash: B256,
}

/// Complete forkchoice view associated with one applied `VALID` FCU.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForkchoiceMeta {
    /// Selected canonical head.
    pub head: CheckpointMeta,
    /// Effective safe checkpoint coherent with `head`, if Reth has one.
    pub safe: Option<CheckpointMeta>,
    /// Effective finalized checkpoint coherent with `head`, if Reth has one.
    pub finalized: Option<CheckpointMeta>,
}

/// Applied forkchoice data captured on the Engine API thread before head metadata is resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppliedForkchoiceMeta {
    /// Head hash selected by the FCU. Its number is resolved on the publisher/snapshot path.
    pub head_hash: B256,
    /// Effective safe checkpoint retained by Reth, if one is coherent with the selected head.
    pub safe: Option<CheckpointMeta>,
    /// Effective finalized checkpoint retained by Reth.
    pub finalized: Option<CheckpointMeta>,
}

impl AppliedForkchoiceMeta {
    /// Resolves the selected head number and removes retained checkpoints above an ancestor head.
    #[inline]
    pub(crate) fn resolve(self, head_number: u64) -> ForkchoiceMeta {
        // A valid FCU can select an ancestor while Reth retains a newer safe checkpoint
        // internally. Such a checkpoint is not coherent with this downstream view.
        let coherent =
            |checkpoint: CheckpointMeta| (checkpoint.number <= head_number).then_some(checkpoint);
        ForkchoiceMeta {
            head: CheckpointMeta {
                number: head_number,
                hash: self.head_hash,
            },
            safe: self.safe.and_then(coherent),
            finalized: self.finalized.and_then(coherent),
        }
    }
}

/// Compact events transferred from the engine thread to the publisher.
// Keeping the common four-change delta inline avoids a heap allocation on the validator hot path.
// The bounded queue fixes total memory independently of this enum's stack size.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum FeedEvent {
    /// EVM execution completed and only its watched changes were extracted.
    Executed {
        /// Time at which the execution observer started processing the block.
        observed_at: Instant,
        /// Watch generation observed by this execution callback.
        generation: Generation,
        /// Block identity.
        block: BlockMeta,
        /// Absolute watched post-state changes.
        changes: BlockChanges,
    },
    /// Post-execution validation promoted a previously emitted candidate.
    Validated {
        /// Time at which validation completed.
        observed_at: Instant,
        /// Watch generation active when validation completed.
        generation: Generation,
        /// Validated block hash.
        block_hash: B256,
    },
    /// A fully validated block for a path that did not pass through the early observer.
    ValidatedFallback {
        /// Time at which the fallback started processing the block.
        observed_at: Instant,
        /// Watch generation observed by this callback.
        generation: Generation,
        /// Block identity.
        block: BlockMeta,
        /// Absolute watched post-state changes.
        changes: BlockChanges,
    },
    /// Internal sync/backfill changed canonical state without applying an Engine API FCU.
    CanonicalStateReset {
        /// Time at which the reset observer received the transition.
        observed_at: Instant,
        /// Watch generation active when the reset was observed.
        generation: Generation,
        /// New canonical head installed by the internal reset.
        head: CheckpointMeta,
    },
    /// A `VALID` forkchoice update was accepted and fully applied by Reth.
    ForkchoiceApplied {
        /// Time at which the engine completed the forkchoice transition.
        observed_at: Instant,
        /// Watch generation active when forkchoice was applied.
        generation: Generation,
        /// Hash-only head plus already-resolved effective checkpoints.
        view: AppliedForkchoiceMeta,
    },
    /// Validation rejected a block or payload.
    Rejected {
        /// Time at which validation returned the rejection.
        observed_at: Instant,
        /// Watch generation active for this validation.
        generation: Generation,
        /// Rejected block hash.
        block_hash: B256,
        /// Stable machine-readable category.
        reason: &'static str,
    },
}

/// Publisher control plane kept separate from latency-sensitive engine data.
#[derive(Debug)]
pub(crate) enum ControlEvent {
    /// Requests an atomic switch to a prevalidated watch set.
    ActivateConfig {
        /// New immutable dictionary. The publisher snapshots it before making it hot-path active.
        watch_set: Arc<WatchSet>,
        /// Completion of the publisher-side snapshot and activation transaction.
        ack: oneshot::Sender<bool>,
    },
}

/// Result of a non-blocking config activation enqueue attempt.
pub(crate) enum ActivationRequest {
    Queued(oneshot::Receiver<bool>),
    Full,
    Disconnected,
}

#[derive(Debug)]
struct ProducerInner {
    watch_set: ArcSwap<WatchSet>,
    tx: Sender<FeedEvent>,
    control_tx: Sender<ControlEvent>,
    control_rx: Mutex<Option<Receiver<ControlEvent>>>,
    /// High bit serializes enqueue attempts; low bits retain loss until publisher recovery.
    enqueue_state: AtomicU64,
    loss_tx: Sender<()>,
    loss_rx: Receiver<()>,
    total_dropped_events: AtomicU64,
    metrics: ProducerMetrics,
}

#[derive(Debug)]
struct ProducerMetrics {
    extract_duration: Histogram,
    enqueue_duration: Histogram,
    queue_depth: Gauge,
    queued_events: Counter,
    dropped_events: Counter,
}

impl ProducerMetrics {
    fn new() -> Self {
        Self {
            extract_duration: metrics::histogram!("statefeed.engine.extract.duration_seconds"),
            enqueue_duration: metrics::histogram!("statefeed.engine.enqueue.duration_seconds"),
            queue_depth: metrics::gauge!("statefeed.engine.queue.depth"),
            queued_events: metrics::counter!("statefeed.engine.events.queued_total"),
            dropped_events: metrics::counter!("statefeed.engine.events.dropped_total"),
        }
    }
}

/// Cheap cloneable handle called directly by block validation.
#[derive(Clone, Debug)]
pub struct FeedProducer {
    inner: Arc<ProducerInner>,
}

impl FeedProducer {
    /// Creates a producer and the single receiver consumed by the publisher.
    pub fn channel(watch_set: Arc<WatchSet>, capacity: usize) -> (Self, Receiver<FeedEvent>) {
        let (tx, rx) = crossbeam_channel::bounded(capacity);
        let (control_tx, control_rx) = crossbeam_channel::bounded(1);
        let (loss_tx, loss_rx) = crossbeam_channel::bounded(1);
        let producer = Self {
            inner: Arc::new(ProducerInner {
                watch_set: ArcSwap::new(watch_set),
                tx,
                control_tx,
                control_rx: Mutex::new(Some(control_rx)),
                enqueue_state: AtomicU64::new(0),
                loss_tx,
                loss_rx,
                total_dropped_events: AtomicU64::new(0),
                metrics: ProducerMetrics::new(),
            }),
        };
        (producer, rx)
    }

    /// Atomically replaces the hot-path watch set.
    pub fn activate(&self, watch_set: Arc<WatchSet>) {
        self.inner.watch_set.store(watch_set);
    }

    /// Returns the currently active immutable watch set.
    #[inline]
    pub fn watch_set(&self) -> Arc<WatchSet> {
        self.inner.watch_set.load_full()
    }

    /// Filters an executed Reth bundle and queues one compact block event without waiting.
    pub fn publish_executed(&self, block: BlockMeta, bundle: &BundleState) {
        let observed_at = Instant::now();
        let watch_set = self.inner.watch_set.load();
        let changes = extract_changes(&watch_set, bundle);
        self.inner
            .metrics
            .extract_duration
            .record(observed_at.elapsed().as_secs_f64());
        self.try_send(FeedEvent::Executed {
            observed_at,
            generation: watch_set.generation(),
            block,
            changes,
        });
    }

    /// Queues a hash-only validation promotion without rescanning the execution bundle.
    pub fn publish_validated(&self, block_hash: B256) {
        let observed_at = Instant::now();
        let generation = self.inner.watch_set.load().generation();
        self.try_send(FeedEvent::Validated {
            observed_at,
            generation,
            block_hash,
        });
    }

    /// Filters a validated bundle when no early execution callback was available for this path.
    pub fn publish_validated_fallback(&self, block: BlockMeta, bundle: &BundleState) {
        let observed_at = Instant::now();
        let watch_set = self.inner.watch_set.load();
        let changes = extract_changes(&watch_set, bundle);
        self.inner
            .metrics
            .extract_duration
            .record(observed_at.elapsed().as_secs_f64());
        self.try_send(FeedEvent::ValidatedFallback {
            observed_at,
            generation: watch_set.generation(),
            block,
            changes,
        });
    }

    /// Requests a canonical re-anchor after an internal sync/backfill reset.
    pub fn publish_canonical_state_reset(&self, head: CheckpointMeta) {
        let observed_at = Instant::now();
        let generation = self.inner.watch_set.load().generation();
        self.try_send(FeedEvent::CanonicalStateReset {
            observed_at,
            generation,
            head,
        });
    }

    /// Queues an applied forkchoice ordering fence without waiting.
    #[inline]
    pub fn publish_forkchoice_applied(&self, view: AppliedForkchoiceMeta) {
        let observed_at = Instant::now();
        let generation = self.inner.watch_set.load().generation();
        self.try_send(FeedEvent::ForkchoiceApplied {
            observed_at,
            generation,
            view,
        });
    }

    /// Queues a validation rejection without formatting the full validation error on the hot path.
    pub fn publish_rejected(&self, block_hash: B256, reason: &'static str) {
        let observed_at = Instant::now();
        let generation = self.inner.watch_set.load().generation();
        self.try_send(FeedEvent::Rejected {
            observed_at,
            generation,
            block_hash,
            reason,
        });
    }

    /// Requests a generation activation without modifying the current hot-path watch set.
    ///
    /// The publisher loads and broadcasts an anchored snapshot first, then calls [`Self::activate`].
    /// This ordering prevents block events from referencing a dictionary consumers have not seen.
    pub(crate) fn request_activation(&self, watch_set: Arc<WatchSet>) -> ActivationRequest {
        let (ack, completion) = oneshot::channel();
        match self
            .inner
            .control_tx
            .try_send(ControlEvent::ActivateConfig { watch_set, ack })
        {
            Ok(()) => ActivationRequest::Queued(completion),
            // Losing a reload request does not break state continuity for the active generation.
            // Treating it as a data gap would force an unnecessary provider snapshot.
            Err(TrySendError::Full(_)) => ActivationRequest::Full,
            Err(TrySendError::Disconnected(_)) => ActivationRequest::Disconnected,
        }
    }

    /// Number of hot-path events dropped by overflow or while a gap marker was pending.
    pub fn dropped_events(&self) -> u64 {
        self.inner.total_dropped_events.load(Ordering::Relaxed)
    }

    /// Samples queue depth and returns loss that must be recovered before the dequeued event.
    ///
    /// Detection on the consumer side closes the multi-producer race where an event can pass a
    /// producer-side gap check immediately before another producer records overflow. An overflow
    /// implies the bounded queue still contains work, so the publisher is guaranteed to observe
    /// the loss before processing a later dequeued event, even if the engine becomes idle.
    #[inline]
    pub(crate) fn take_pending_loss(&self) -> u64 {
        self.inner
            .metrics
            .queue_depth
            .set(self.inner.tx.len() as f64);
        let mut state = self.inner.enqueue_state.load(Ordering::Acquire);
        loop {
            if state & ENQUEUE_LOCKED != 0 || state == 0 {
                return 0;
            }
            match self.inner.enqueue_state.compare_exchange_weak(
                state,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return state,
                Err(observed) => state = observed,
            }
        }
    }

    /// Wake-up channel used when overflow happens after the publisher drained the data queue.
    pub(crate) fn loss_notifications(&self) -> Receiver<()> {
        self.inner.loss_rx.clone()
    }

    /// Single-consumer control channel for configuration transactions.
    pub(crate) fn take_control_events(&self) -> Receiver<ControlEvent> {
        self.inner
            .control_rx
            .lock()
            .expect("statefeed control receiver mutex poisoned")
            .take()
            .expect("statefeed control receiver can only have one publisher")
    }

    #[inline]
    fn try_send(&self, event: FeedEvent) {
        let enqueue_started = Instant::now();
        if self
            .inner
            .enqueue_state
            .compare_exchange(0, ENQUEUE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            // Never wait on a concurrent producer or pending recovery. Linearize this loss in the
            // same state word so no later event can acquire the enqueue reservation first.
            self.record_drop();
            self.notify_loss();
            self.record_enqueue_metrics(enqueue_started);
            return;
        }

        match self.inner.tx.try_send(event) {
            Ok(()) => self.inner.metrics.queued_events.increment(1),
            Err(TrySendError::Full(_)) => self.record_drop(),
            Err(TrySendError::Disconnected(_)) => {}
        }
        let previous = self
            .inner
            .enqueue_state
            .fetch_and(DROP_COUNT_MASK, Ordering::Release);
        if previous & DROP_COUNT_MASK != 0 {
            self.notify_loss();
        }
        self.record_enqueue_metrics(enqueue_started);
    }

    #[inline]
    fn record_enqueue_metrics(&self, started_at: Instant) {
        self.inner
            .metrics
            .enqueue_duration
            .record(started_at.elapsed().as_secs_f64());
    }

    #[inline]
    fn record_drop(&self) {
        let _ =
            self.inner
                .enqueue_state
                .fetch_update(Ordering::Release, Ordering::Relaxed, |state| {
                    let locked = state & ENQUEUE_LOCKED;
                    let dropped = (state & DROP_COUNT_MASK)
                        .saturating_add(1)
                        .min(DROP_COUNT_MASK);
                    Some(locked | dropped)
                });
        self.record_drop_metrics();
    }

    #[inline]
    fn notify_loss(&self) {
        let _ = self.inner.loss_tx.try_send(());
    }

    #[inline]
    fn record_drop_metrics(&self) {
        self.inner
            .total_dropped_events
            .fetch_add(1, Ordering::Relaxed);
        self.inner.metrics.dropped_events.increment(1);
    }
}

/// Extracts only configured storage updates from a complete block execution bundle.
///
/// Complexity is proportional to the configured watch set rather than the potentially very large
/// block state diff. Values are absolute post-state values, which makes applying a delta
/// idempotent.
pub fn extract_changes(watch_set: &WatchSet, bundle: &BundleState) -> BlockChanges {
    let mut changes = BlockChanges::new();
    let state = bundle.state();

    if watch_set.addresses().len() <= state.len() {
        for address_watch in watch_set.addresses() {
            if let Some(account) = state.get(&address_watch.address) {
                extract_account_changes(address_watch, account, &mut changes);
            }
        }
    } else {
        for (address, account) in state {
            if let Some(address_watch) = watch_set.address(address) {
                extract_account_changes(address_watch, account, &mut changes);
            }
        }
    }

    changes
}

#[inline]
fn extract_account_changes(
    address_watch: &AddressWatch,
    account: &reth::revm::revm::database::states::BundleAccount,
    changes: &mut BlockChanges,
) {
    let destroyed = account.was_destroyed();

    if destroyed || address_watch.slots.len() <= account.storage.len() {
        for watched in &address_watch.slots {
            if let Some(slot) = account.storage.get(&watched.slot) {
                if slot.is_changed() || destroyed {
                    changes.push(SlotChange {
                        key_id: watched.key_id,
                        new_value: slot.present_value(),
                    });
                }
            } else if destroyed {
                // A destroyed account clears storage even when a particular key is absent from
                // the explicit per-block storage map.
                changes.push(SlotChange {
                    key_id: watched.key_id,
                    new_value: U256::ZERO,
                });
            }
        }
    } else {
        // The account may have a small storage diff while the configured projection contains
        // thousands of keys. Iterating the smaller side avoids O(watch_count) hash lookups.
        for (slot_key, slot) in &account.storage {
            if !slot.is_changed() {
                continue;
            }
            let key_id = if address_watch.slot_to_key.is_empty() {
                address_watch
                    .slots
                    .binary_search_by_key(slot_key, |watched| watched.slot)
                    .ok()
                    .map(|index| address_watch.slots[index].key_id)
            } else {
                address_watch.slot_to_key.get(slot_key).copied()
            };
            if let Some(key_id) = key_id {
                changes.push(SlotChange {
                    key_id,
                    new_value: slot.present_value(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;
    use reth::revm::revm::{
        database::{
            BundleState,
            states::{AccountStatus, StorageSlot},
        },
        state::AccountInfo,
    };

    use super::*;
    use crate::config::WatchConfig;

    fn publish_test_event(producer: &FeedProducer, number: u64) {
        producer.publish_canonical_state_reset(CheckpointMeta {
            number,
            hash: B256::with_last_byte(number as u8),
        });
    }

    #[test]
    fn extracts_only_changed_watched_slots() {
        let address = Address::with_last_byte(1);
        let watched_slot = B256::with_last_byte(1);
        let ignored_slot = U256::from(2);
        let watch_set = WatchSet::compile(
            1,
            &[WatchConfig {
                id: "watched".into(),
                address,
                slot: watched_slot,
            }],
        );

        let mut bundle = BundleState::default();
        let mut storage: reth::revm::revm::database::states::StorageWithOriginalValues =
            Default::default();
        storage.insert(
            U256::from(1),
            StorageSlot::new_changed(U256::from(1), U256::from(2)),
        );
        storage.insert(
            ignored_slot,
            StorageSlot::new_changed(U256::from(3), U256::from(4)),
        );
        bundle.state.insert(
            address,
            reth::revm::revm::database::states::BundleAccount::new(
                Some(AccountInfo::default()),
                Some(AccountInfo::default()),
                storage,
                AccountStatus::Changed,
            ),
        );

        let changes = extract_changes(&watch_set, &bundle);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key_id, 0);
        assert_eq!(changes[0].new_value, U256::from(2));
    }

    #[test]
    fn sparse_bundle_uses_address_index_across_many_watched_accounts() {
        let configured = (0..128)
            .map(|index| WatchConfig {
                id: format!("account.{index}"),
                address: Address::with_last_byte(index as u8),
                slot: B256::ZERO,
            })
            .collect::<Vec<_>>();
        let watch_set = WatchSet::compile(1, &configured);
        let touched = Address::with_last_byte(127);
        let mut storage: reth::revm::revm::database::states::StorageWithOriginalValues =
            Default::default();
        storage.insert(
            U256::ZERO,
            StorageSlot::new_changed(U256::ZERO, U256::from(9)),
        );
        let mut bundle = BundleState::default();
        bundle.state.insert(
            touched,
            reth::revm::revm::database::states::BundleAccount::new(
                Some(AccountInfo::default()),
                Some(AccountInfo::default()),
                storage,
                AccountStatus::Changed,
            ),
        );

        let changes = extract_changes(&watch_set, &bundle);
        assert_eq!(
            changes.as_slice(),
            &[SlotChange {
                key_id: 127,
                new_value: U256::from(9),
            }]
        );
    }

    #[test]
    fn account_destruction_clears_watched_slots_absent_from_delta() {
        let address = Address::with_last_byte(1);
        let watch_set = WatchSet::compile(
            1,
            &[WatchConfig {
                id: "cleared".into(),
                address,
                slot: B256::with_last_byte(3),
            }],
        );
        let mut bundle = BundleState::default();
        bundle.state.insert(
            address,
            reth::revm::revm::database::states::BundleAccount::new(
                Some(AccountInfo::default()),
                None,
                Default::default(),
                AccountStatus::Destroyed,
            ),
        );

        let changes = extract_changes(&watch_set, &bundle);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].new_value, U256::ZERO);
    }

    #[test]
    fn queue_overflow_is_counted_before_a_dequeued_event_is_processed() {
        let watch_set = Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, receiver) = FeedProducer::channel(watch_set, 2);

        publish_test_event(&producer, 1);
        publish_test_event(&producer, 2);
        publish_test_event(&producer, 3);
        assert_eq!(producer.dropped_events(), 1);

        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::CanonicalStateReset { .. }
        ));
        assert_eq!(producer.take_pending_loss(), 1);
        publish_test_event(&producer, 4);
        assert_eq!(producer.dropped_events(), 1);

        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::CanonicalStateReset { .. }
        ));
        assert_eq!(producer.take_pending_loss(), 0);
        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::CanonicalStateReset { .. }
        ));
    }

    #[test]
    fn concurrent_producer_loss_is_observed_before_queued_data() {
        let watch_set = Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, receiver) = FeedProducer::channel(watch_set, 1);

        publish_test_event(&producer, 1);
        publish_test_event(&producer, 2);
        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::CanonicalStateReset { .. }
        ));

        std::thread::scope(|scope| {
            for number in 3..11 {
                let producer = producer.clone();
                scope.spawn(move || {
                    publish_test_event(&producer, number);
                });
            }
        });

        assert!(producer.take_pending_loss() > 0);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn data_cannot_overtake_loss_after_queue_space_is_freed() {
        let watch_set = Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, receiver) = FeedProducer::channel(watch_set, 1);

        publish_test_event(&producer, 1);
        publish_test_event(&producer, 2);
        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::CanonicalStateReset { .. }
        ));

        // Capacity is available again, but the stream must recover before accepting later data.
        publish_test_event(&producer, 3);
        assert!(receiver.try_recv().is_err());
        assert_eq!(producer.take_pending_loss(), 2);
    }

    #[test]
    fn publisher_observes_a_drop_without_waiting_for_another_engine_event() {
        let watch_set = Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, receiver) = FeedProducer::channel(watch_set, 2);

        publish_test_event(&producer, 1);
        publish_test_event(&producer, 2);
        publish_test_event(&producer, 3);
        assert_eq!(producer.dropped_events(), 1);

        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::CanonicalStateReset { .. }
        ));
        assert_eq!(producer.take_pending_loss(), 1);
        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::CanonicalStateReset { .. }
        ));
    }

    #[test]
    fn reload_control_contention_does_not_create_a_data_gap() {
        let watch_set = Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, _receiver) = FeedProducer::channel(Arc::clone(&watch_set), 1);
        publish_test_event(&producer, 1);

        let next = Arc::new(WatchSet::compile(
            2,
            &[WatchConfig {
                id: "next".into(),
                address: Address::ZERO,
                slot: B256::with_last_byte(1),
            }],
        ));
        let first = producer.request_activation(Arc::clone(&next));
        assert!(matches!(first, ActivationRequest::Queued(_)));
        assert!(matches!(
            producer.request_activation(next),
            ActivationRequest::Full
        ));
        assert_eq!(producer.dropped_events(), 0);
    }

    #[test]
    fn reload_reports_disconnection_after_publisher_exits() {
        let watch_set = Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, _receiver) = FeedProducer::channel(Arc::clone(&watch_set), 1);
        drop(producer.take_control_events());

        assert!(matches!(
            producer.request_activation(watch_set),
            ActivationRequest::Disconnected
        ));
        assert_eq!(producer.dropped_events(), 0);
    }

    #[test]
    fn executed_projection_and_validation_promotion_are_distinct_events() {
        let watch_set = Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, receiver) = FeedProducer::channel(watch_set, 2);
        let block = BlockMeta {
            number: 1,
            hash: B256::with_last_byte(1),
            parent_hash: B256::ZERO,
            timestamp: 1,
        };

        producer.publish_executed(block, &BundleState::default());
        producer.publish_validated(block.hash);

        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::Executed {
                block: observed,
                ..
            } if observed == block
        ));
        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::Validated { block_hash, .. } if block_hash == block.hash
        ));
    }

    #[test]
    fn applied_forkchoice_enqueues_one_fixed_size_view() {
        let watch_set = Arc::new(WatchSet::compile(
            7,
            &[WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        ));
        let (producer, receiver) = FeedProducer::channel(watch_set, 1);
        let view = AppliedForkchoiceMeta {
            head_hash: B256::with_last_byte(42),
            safe: Some(CheckpointMeta {
                number: 41,
                hash: B256::with_last_byte(41),
            }),
            finalized: Some(CheckpointMeta {
                number: 40,
                hash: B256::with_last_byte(40),
            }),
        };

        producer.publish_forkchoice_applied(view);

        assert!(matches!(
            receiver.recv().unwrap(),
            FeedEvent::ForkchoiceApplied {
                generation: 7,
                view: observed,
                ..
            } if observed == view
        ));
    }
}
