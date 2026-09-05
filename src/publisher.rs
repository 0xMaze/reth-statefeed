//! Projection assembly, bounded fan-out, and Unix socket serving.

use std::{
    collections::{HashSet, VecDeque},
    fs,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    os::unix::net::UnixStream as StdUnixStream,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use alloy_primitives::{B256, map::HashMap};
use arc_swap::ArcSwap;
use bytes::Bytes;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use eyre::{Context, Result, eyre};
use metrics::{Counter, Gauge, Histogram};
use notify::Watcher;
use smallvec::SmallVec;
use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
    sync::{Semaphore, broadcast, mpsc, watch},
    task::{JoinHandle, JoinSet},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    config::{Config, StreamConfig},
    feed::{
        ActivationRequest, AppliedForkchoiceMeta, BlockMeta, CheckpointMeta, ControlEvent,
        FeedEvent, FeedProducer, ForkchoiceMeta,
    },
    watch::{BlockChanges, WatchSet},
    wire::{
        self, BlockRef, BlockRejected, BlockStage, BlockState, BlockValidated,
        CAP_CANDIDATE_RETIREMENT, CAP_CANONICAL, CAP_EXECUTED, CAP_FORKCHOICE_APPLIED,
        CAP_FULL_PROJECTIONS, CAP_REJECTED, CAP_VALIDATED, CandidatesRetired, CanonicalHead,
        CheckpointRef, ConfigActivated, Envelope, ForkchoiceApplied, ForkchoiceView, Gap, Hello,
        PROTOCOL_VERSION, RetirementReason, Snapshot, WatchKey, envelope,
    },
};

/// Complete state projection anchored at one block.
#[derive(Clone, Debug)]
pub struct Projection {
    /// Block represented by `values`.
    pub block: BlockMeta,
    /// Dictionary used by the values array.
    pub watch_set: Arc<WatchSet>,
    /// Packed big-endian values ordered by dense key id (`32 * watch_set.len()` bytes).
    pub values: Bytes,
    /// Keys changed by this block, little-endian within each byte.
    pub changed_bitmap: Bytes,
}

/// Canonical projection together with the instant at which its head hash was confirmed.
#[derive(Clone, Debug)]
pub struct CanonicalSnapshot {
    /// Complete projection at the selected canonical head.
    pub projection: Projection,
    /// Local monotonic instant immediately before confirming the canonical head.
    ///
    /// Canonical callbacks observed no later than this instant are already covered by the
    /// snapshot. Later callbacks must be replayed even if they carry the previous generation.
    pub anchored_at: Instant,
    /// Head/safe/finalized view confirmed around the same provider reads as `projection`.
    pub forkchoice: ForkchoiceMeta,
}

/// Synchronous provider abstraction used outside the engine hot path.
pub trait SnapshotSource: Send + Sync + 'static {
    /// Loads a complete snapshot at the latest canonical head.
    fn load_latest(&self, watch_set: Arc<WatchSet>) -> Result<CanonicalSnapshot>;

    /// Loads a complete snapshot at a known block hash.
    fn load_at(&self, watch_set: Arc<WatchSet>, block_hash: B256) -> Result<Projection>;
}

#[derive(Debug)]
struct PublishedState {
    canonical: Arc<Projection>,
    /// Latest complete forkchoice view coherent with `canonical`.
    ///
    /// This is temporarily absent after `CanonicalHead` and before its matching
    /// `ForkchoiceApplied`; new handshakes are refused during that narrow interval.
    forkchoice: Option<ForkchoiceMeta>,
    /// First global stream sequence whose effects are included in `canonical`.
    effective_sequence: u64,
    /// A gap was committed and no replacement snapshot has been committed yet.
    recovering: bool,
}

#[derive(Clone, Debug)]
struct PublishedFrame {
    sequence: u64,
    bytes: Bytes,
}

#[derive(Debug)]
struct Shared {
    boot_id: Bytes,
    chain_id: u64,
    genesis_hash: B256,
    started_at: Instant,
    /// Last allocated sequence. It can temporarily lead publication while a frame is encoded.
    sequence: AtomicU64,
    /// Last sequence committed as the baseline for new consumer handshakes.
    ///
    /// This is stored immediately before the broadcast send, so it can briefly lead the ring.
    /// A new consumer may consequently treat that frame as pre-connection history, which is valid:
    /// reconnect guarantees a canonical snapshot but does not replay speculative candidates.
    published_sequence: AtomicU64,
    max_frame_bytes: usize,
    capabilities: u64,
    published: ArcSwap<PublishedState>,
    frames: broadcast::Sender<PublishedFrame>,
    metrics: PublisherMetrics,
}

const BASE_CAPABILITIES: u64 = CAP_FULL_PROJECTIONS
    | CAP_VALIDATED
    | CAP_CANONICAL
    | CAP_REJECTED
    | CAP_FORKCHOICE_APPLIED
    | CAP_CANDIDATE_RETIREMENT;

const fn advertised_capabilities(publish_executed: bool) -> u64 {
    BASE_CAPABILITIES | if publish_executed { CAP_EXECUTED } else { 0 }
}

#[derive(Debug)]
struct PublisherMetrics {
    encode_duration: Histogram,
    projection_duration: Histogram,
    validated_end_to_end: Histogram,
    executed_end_to_end: Histogram,
    canonical_end_to_end: Histogram,
    forkchoice_end_to_end: Histogram,
    rejected_end_to_end: Histogram,
    snapshot_duration: Histogram,
    socket_send_duration: Histogram,
    frame_bytes: Histogram,
    published_frames: Counter,
    validated_events: Counter,
    executed_events: Counter,
    canonical_events: Counter,
    rejected_events: Counter,
    forkchoice_events: Counter,
    retired_candidates: Counter,
    snapshot_events: Counter,
    config_events: Counter,
    gap_events: Counter,
    consumer_gaps: Counter,
    candidate_parent_cache_misses: Counter,
    candidates_cached: Gauge,
    candidate_projections_cached: Gauge,
    connected_consumers: Gauge,
    config_generation: Gauge,
}

impl PublisherMetrics {
    fn new() -> Self {
        Self {
            encode_duration: metrics::histogram!("statefeed.publisher.encode.duration_seconds"),
            projection_duration: metrics::histogram!(
                "statefeed.publisher.projection.duration_seconds"
            ),
            validated_end_to_end: metrics::histogram!(
                "statefeed.latency.end_to_end_seconds",
                "event" => "validated"
            ),
            executed_end_to_end: metrics::histogram!(
                "statefeed.latency.end_to_end_seconds",
                "event" => "executed"
            ),
            canonical_end_to_end: metrics::histogram!(
                "statefeed.latency.end_to_end_seconds",
                "event" => "canonical"
            ),
            forkchoice_end_to_end: metrics::histogram!(
                "statefeed.latency.end_to_end_seconds",
                "event" => "forkchoice_applied"
            ),
            rejected_end_to_end: metrics::histogram!(
                "statefeed.latency.end_to_end_seconds",
                "event" => "rejected"
            ),
            snapshot_duration: metrics::histogram!("statefeed.snapshot.duration_seconds"),
            socket_send_duration: metrics::histogram!("statefeed.socket.send.duration_seconds"),
            frame_bytes: metrics::histogram!("statefeed.publisher.frame_bytes"),
            published_frames: metrics::counter!("statefeed.publisher.frames_total"),
            validated_events: metrics::counter!(
                "statefeed.events.total",
                "type" => "validated"
            ),
            executed_events: metrics::counter!("statefeed.events.total", "type" => "executed"),
            canonical_events: metrics::counter!(
                "statefeed.events.total",
                "type" => "canonical"
            ),
            rejected_events: metrics::counter!("statefeed.events.total", "type" => "rejected"),
            forkchoice_events: metrics::counter!(
                "statefeed.events.total",
                "type" => "forkchoice_applied"
            ),
            retired_candidates: metrics::counter!("statefeed.candidates.retired_total"),
            snapshot_events: metrics::counter!("statefeed.events.total", "type" => "snapshot"),
            config_events: metrics::counter!("statefeed.events.total", "type" => "config"),
            gap_events: metrics::counter!("statefeed.events.total", "type" => "gap"),
            consumer_gaps: metrics::counter!("statefeed.consumer.gaps_total"),
            candidate_parent_cache_misses: metrics::counter!(
                "statefeed.candidates.parent_cache_misses_total"
            ),
            candidates_cached: metrics::gauge!("statefeed.candidates.cached"),
            candidate_projections_cached: metrics::gauge!(
                "statefeed.candidates.projections_cached"
            ),
            connected_consumers: metrics::gauge!("statefeed.consumers.connected"),
            config_generation: metrics::gauge!("statefeed.config.generation"),
        }
    }

    fn record_event(&self, event: &envelope::Event) {
        match event {
            envelope::Event::BlockState(state) => match BlockStage::try_from(state.stage) {
                Ok(BlockStage::Executed) => self.executed_events.increment(1),
                Ok(BlockStage::Validated) => self.validated_events.increment(1),
                _ => {}
            },
            envelope::Event::BlockValidated(_) => self.validated_events.increment(1),
            envelope::Event::CanonicalHead(_) => self.canonical_events.increment(1),
            envelope::Event::Snapshot(_) => self.snapshot_events.increment(1),
            envelope::Event::ConfigActivated(_) => self.config_events.increment(1),
            envelope::Event::Gap(_) => self.gap_events.increment(1),
            envelope::Event::BlockRejected(_) => self.rejected_events.increment(1),
            envelope::Event::ForkchoiceApplied(_) => self.forkchoice_events.increment(1),
            envelope::Event::CandidatesRetired(event) => {
                self.retired_candidates
                    .increment(event.block_hashes.len() as u64);
            }
            envelope::Event::Hello(_) => {}
        }
    }
}

impl Shared {
    fn monotonic_ns(&self) -> u64 {
        self.started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    fn envelope(&self, sequence: u64, generation: u64, event: envelope::Event) -> Envelope {
        Envelope {
            protocol_version: PROTOCOL_VERSION,
            boot_id: self.boot_id.clone(),
            sequence,
            config_generation: generation,
            emitted_at_monotonic_ns: self.monotonic_ns(),
            event: Some(event),
        }
    }

    fn publish(&self, generation: u64, event: envelope::Event) -> Result<u64> {
        self.publish_inner(generation, event, None)
    }

    /// Publishes an event that atomically changes the state used by reconnect handshakes.
    fn publish_state(
        &self,
        generation: u64,
        event: envelope::Event,
        canonical: Arc<Projection>,
        forkchoice: Option<ForkchoiceMeta>,
        recovering: bool,
    ) -> Result<u64> {
        self.publish_inner(generation, event, Some((canonical, forkchoice, recovering)))
    }

    fn publish_inner(
        &self,
        generation: u64,
        event: envelope::Event,
        next_state: Option<(Arc<Projection>, Option<ForkchoiceMeta>, bool)>,
    ) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let envelope = self.envelope(sequence, generation, event);
        let encode_started = Instant::now();
        let frame = wire::encode_frame(&envelope, self.max_frame_bytes)?;
        self.metrics
            .encode_duration
            .record(encode_started.elapsed().as_secs_f64());
        self.metrics
            .record_event(envelope.event.as_ref().expect("event is always populated"));
        self.metrics.frame_bytes.record(frame.len() as f64);
        self.metrics.published_frames.increment(1);
        if let Some((canonical, forkchoice, recovering)) = next_state {
            // Publish projection/recovery state before committing its sequence. An acquire load of
            // `published_sequence` can therefore never observe the transition without also being
            // able to observe the matching handshake state. `effective_sequence` handles the
            // inverse race, where the ArcSwap update is observed before the baseline load.
            self.published.store(Arc::new(PublishedState {
                canonical,
                forkchoice,
                effective_sequence: sequence,
                recovering,
            }));
        }
        // Commit the handshake baseline before exposing the frame. With the opposite ordering, a
        // consumer could subscribe after `send` but read the old baseline before this store,
        // permanently missing a non-canonical frame published in that narrow window.
        self.published_sequence.store(sequence, Ordering::Release);
        let _ = self.frames.send(PublishedFrame {
            sequence,
            bytes: Bytes::from(frame),
        });
        Ok(sequence)
    }
}

/// Owns publisher/server tasks and coordinates graceful shutdown.
#[derive(Debug)]
pub struct ServiceHandle {
    shutdown: watch::Sender<bool>,
    publisher_shutdown: Sender<()>,
    publisher_thread: thread::JoinHandle<()>,
    async_tasks: Vec<JoinHandle<()>>,
}

/// Immutable identity and tuning passed when the local statefeed service starts.
#[derive(Clone, Debug)]
pub struct ServiceOptions {
    /// Watched TOML path used by the reload task.
    pub config_path: PathBuf,
    /// Socket, buffer, and publisher-thread settings.
    pub stream: StreamConfig,
    /// EVM chain id advertised to consumers.
    pub chain_id: u64,
    /// Genesis hash advertised to consumers.
    pub genesis_hash: B256,
}

impl ServiceHandle {
    /// Stops accepting clients, drains task shutdown, and removes the socket path.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.publisher_shutdown.try_send(());
        for task in self.async_tasks {
            if let Err(error) = task.await {
                warn!(target: "statefeed", %error, "statefeed task failed during shutdown");
            }
        }
        match tokio::task::spawn_blocking(move || self.publisher_thread.join()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => warn!(target: "statefeed", "statefeed publisher thread panicked"),
            Err(error) => {
                warn!(target: "statefeed", %error, "statefeed publisher join task failed")
            }
        }
    }
}

/// Starts projection publication and the Unix socket server.
pub async fn start_service(
    options: ServiceOptions,
    receiver: Receiver<FeedEvent>,
    producer: FeedProducer,
    source: Arc<dyn SnapshotSource>,
    watch_set: Arc<WatchSet>,
) -> Result<ServiceHandle> {
    let ServiceOptions {
        config_path,
        stream: stream_config,
        chain_id,
        genesis_hash,
    } = options;
    stream_config.validate()?;
    if watch_set.is_empty() {
        return Err(eyre!("statefeed watch set must contain at least one key"));
    }
    let metrics = PublisherMetrics::new();
    let snapshot_started = Instant::now();
    let initial_snapshot = load_latest(Arc::clone(&source), Arc::clone(&watch_set)).await?;
    validate_projection_dictionary(&initial_snapshot.projection, &watch_set)?;
    validate_snapshot_view(&initial_snapshot)?;
    metrics
        .snapshot_duration
        .record(snapshot_started.elapsed().as_secs_f64());
    let initial_anchored_at = initial_snapshot.anchored_at;
    let initial_forkchoice = initial_snapshot.forkchoice;
    let initial = Arc::new(initial_snapshot.projection);
    metrics
        .config_generation
        .set(initial.watch_set.generation() as f64);

    let (frames, _) = broadcast::channel(stream_config.consumer_buffer);
    let shared = Arc::new(Shared {
        boot_id: Bytes::copy_from_slice(Uuid::new_v4().as_bytes()),
        chain_id,
        genesis_hash,
        started_at: Instant::now(),
        sequence: AtomicU64::new(0),
        published_sequence: AtomicU64::new(0),
        max_frame_bytes: stream_config.max_frame_bytes,
        capabilities: advertised_capabilities(stream_config.publish_executed),
        published: ArcSwap::from_pointee(PublishedState {
            canonical: Arc::clone(&initial),
            forkchoice: Some(initial_forkchoice),
            effective_sequence: 0,
            recovering: false,
        }),
        frames,
        metrics,
    });
    validate_handshake_frames(&shared, &initial, initial_forkchoice)?;
    let (listener, socket_cleanup) = bind_socket(&stream_config.socket, stream_config.socket_mode)?;

    let (shutdown, shutdown_rx) = watch::channel(false);
    let (publisher_shutdown, publisher_shutdown_rx) = crossbeam_channel::bounded(1);
    let publisher_cpu = stream_config.publisher_cpu;
    let spin_duration = Duration::from_micros(stream_config.publisher_spin_us);
    let candidate_policy = CandidatePolicy::from(&stream_config);
    let publisher_thread = thread::Builder::new()
        .name("statefeed-publisher".into())
        .spawn({
            let producer = producer.clone();
            let shared = Arc::clone(&shared);
            move || {
                run_publisher(PublisherThread {
                    receiver,
                    producer,
                    source,
                    shared,
                    initial,
                    initial_anchored_at,
                    initial_forkchoice,
                    candidate_policy,
                    shutdown: publisher_shutdown_rx,
                    publisher_cpu,
                    spin_duration,
                })
            }
        })
        .wrap_err("failed to spawn statefeed publisher thread")?;
    let server_task = tokio::spawn(run_server(
        listener,
        socket_cleanup,
        Arc::clone(&shared),
        stream_config.max_consumers,
        shutdown_rx,
    ));
    let reload_task = tokio::spawn(run_config_reloader(
        config_path,
        stream_config.clone(),
        producer,
        shutdown.subscribe(),
    ));

    info!(
        target: "statefeed",
        path = %stream_config.socket.display(),
        keys = shared.published.load().canonical.watch_set.len(),
        "statefeed Unix socket started"
    );

    Ok(ServiceHandle {
        shutdown,
        publisher_shutdown,
        publisher_thread,
        async_tasks: vec![server_task, reload_task],
    })
}

async fn load_latest(
    source: Arc<dyn SnapshotSource>,
    watch_set: Arc<WatchSet>,
) -> Result<CanonicalSnapshot> {
    tokio::task::spawn_blocking(move || source.load_latest(watch_set))
        .await
        .wrap_err("statefeed snapshot worker panicked")?
}

struct PublisherThread {
    receiver: Receiver<FeedEvent>,
    producer: FeedProducer,
    source: Arc<dyn SnapshotSource>,
    shared: Arc<Shared>,
    initial: Arc<Projection>,
    initial_anchored_at: Instant,
    initial_forkchoice: ForkchoiceMeta,
    candidate_policy: CandidatePolicy,
    shutdown: Receiver<()>,
    publisher_cpu: Option<usize>,
    spin_duration: Duration,
}

#[derive(Clone, Copy, Debug)]
struct CandidatePolicy {
    projection_limit: usize,
    metadata_limit: usize,
    retention: Duration,
    work_budget: usize,
}

impl From<&StreamConfig> for CandidatePolicy {
    fn from(stream: &StreamConfig) -> Self {
        Self {
            projection_limit: stream.candidate_cache_blocks,
            metadata_limit: stream.candidate_metadata_entries,
            retention: stream.candidate_retention,
            work_budget: stream.retirement_work_budget,
        }
    }
}

fn run_publisher(thread: PublisherThread) {
    let PublisherThread {
        receiver,
        producer,
        source,
        shared,
        initial,
        initial_anchored_at,
        initial_forkchoice,
        candidate_policy,
        shutdown,
        publisher_cpu,
        spin_duration,
    } = thread;
    if let Some(cpu) = publisher_cpu
        && !core_affinity::set_for_current(core_affinity::CoreId { id: cpu })
    {
        warn!(target: "statefeed", cpu, "failed to pin statefeed publisher thread");
    }

    let mut publisher = Publisher::new_configured(
        producer,
        source,
        shared,
        initial,
        initial_anchored_at,
        initial_forkchoice,
        candidate_policy,
    );
    let loss_notifications = publisher.producer.loss_notifications();
    let control_events = publisher.producer.take_control_events();
    let maintenance_interval = candidate_policy
        .retention
        .checked_div(4)
        .unwrap_or(Duration::from_millis(10))
        .clamp(Duration::from_millis(10), Duration::from_secs(1));
    let maintenance = crossbeam_channel::tick(maintenance_interval);

    loop {
        if shutdown_requested(&shutdown) {
            break;
        }
        if !recover_pending_loss(&mut publisher, &shutdown) {
            break;
        }
        if maintenance.try_recv().is_ok() && !run_maintenance(&mut publisher, &shutdown) {
            break;
        }
        let event = match control_events.try_recv() {
            Ok(event) => PublisherEvent::Control(event),
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => match receiver.try_recv() {
                Ok(event) => PublisherEvent::Data(event),
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => {
                    let spin_started = Instant::now();
                    let mut event = None;
                    let mut loss_wakeup = false;
                    while spin_started.elapsed() < spin_duration {
                        if shutdown_requested(&shutdown) {
                            return;
                        }
                        if loss_notifications.try_recv().is_ok() {
                            loss_wakeup = true;
                            break;
                        }
                        match control_events.try_recv() {
                            Ok(received) => {
                                event = Some(PublisherEvent::Control(received));
                                break;
                            }
                            Err(TryRecvError::Disconnected) => return,
                            Err(TryRecvError::Empty) => {}
                        }
                        match receiver.try_recv() {
                            Ok(received) => {
                                event = Some(PublisherEvent::Data(received));
                                break;
                            }
                            Err(TryRecvError::Disconnected) => return,
                            Err(TryRecvError::Empty) => std::hint::spin_loop(),
                        }
                    }
                    if loss_wakeup {
                        continue;
                    }

                    match event {
                        Some(event) => event,
                        None => crossbeam_channel::select! {
                            recv(shutdown) -> _ => break,
                            recv(loss_notifications) -> _ => continue,
                            recv(maintenance) -> _ => {
                                if !run_maintenance(&mut publisher, &shutdown) {
                                    break;
                                }
                                continue;
                            },
                            recv(control_events) -> event => match event {
                                Ok(event) => PublisherEvent::Control(event),
                                Err(_) => break,
                            },
                            recv(receiver) -> event => match event {
                                Ok(event) => PublisherEvent::Data(event),
                                Err(_) => break,
                            },
                        },
                    }
                }
            },
        };

        if !recover_pending_loss(&mut publisher, &shutdown) {
            break;
        }
        let result = match event {
            PublisherEvent::Data(event) => publisher.handle(event),
            PublisherEvent::Control(event) => publisher.handle_control(event),
        };
        if let Err(error) = result {
            error!(target: "statefeed", %error, "failed to publish statefeed event");
            if !publisher.recover_until_success("publisher_error", &shutdown) {
                break;
            }
        }
    }
}

// Boxing `FeedEvent` here would add an allocation to every publisher dequeue merely to shrink a
// short-lived stack discriminant. The publisher thread already reserves space for the same event.
#[allow(clippy::large_enum_variant)]
enum PublisherEvent {
    Data(FeedEvent),
    Control(ControlEvent),
}

fn run_maintenance(publisher: &mut Publisher, shutdown: &Receiver<()>) -> bool {
    if let Err(error) = publisher.maintain() {
        error!(target: "statefeed", %error, "failed to maintain candidate lifecycle");
        return publisher.recover_until_success("candidate_maintenance_error", shutdown);
    }
    true
}

fn recover_pending_loss(publisher: &mut Publisher, shutdown: &Receiver<()>) -> bool {
    loop {
        let dropped_events = publisher.producer.take_pending_loss();
        if dropped_events == 0 {
            return true;
        }
        warn!(
            target: "statefeed",
            dropped_events,
            "engine-to-publisher queue overflowed"
        );
        if !publisher.recover_until_success("engine_queue_overflow", shutdown) {
            return false;
        }
    }
}

#[inline]
fn shutdown_requested(shutdown: &Receiver<()>) -> bool {
    matches!(
        shutdown.try_recv(),
        Ok(()) | Err(TryRecvError::Disconnected)
    )
}

struct Publisher {
    producer: FeedProducer,
    source: Arc<dyn SnapshotSource>,
    shared: Arc<Shared>,
    candidates: HashMap<B256, CandidateEntry>,
    deferred_executed: HashMap<B256, DeferredExecuted>,
    children_by_parent: HashMap<B256, SmallVec<[B256; 2]>>,
    projection_order: VecDeque<(u64, B256)>,
    metadata_order: VecDeque<(u64, B256)>,
    tombstones: HashSet<B256>,
    tombstone_order: VecDeque<B256>,
    next_incarnation: u64,
    projection_count: usize,
    cache_limit: usize,
    metadata_limit: usize,
    candidate_retention: Duration,
    retirement_work_budget: usize,
    canonical_hash: B256,
    last_finalized: Option<CheckpointMeta>,
    finality_sweep: Option<FinalitySweep>,
    snapshot_anchored_at: Instant,
}

#[derive(Debug)]
struct CandidateEntry {
    block: BlockMeta,
    projection: Option<Arc<Projection>>,
    stage: CandidateStage,
    /// True while consumers are expected to retain this candidate.
    tracked: bool,
    /// First sequence of the current consumer-visible lifecycle incarnation.
    first_sequence: u64,
    incarnation: u64,
    expires_at: Instant,
}

#[derive(Debug)]
struct DeferredExecuted {
    block: BlockMeta,
    changes: BlockChanges,
    incarnation: u64,
    expires_at: Instant,
}

#[derive(Debug)]
struct FinalitySweep {
    checkpoint: CheckpointMeta,
    pending: VecDeque<(u64, B256)>,
    classifications: HashMap<B256, FinalityClassification>,
    active: Option<FinalityWalk>,
}

#[derive(Debug)]
struct FinalityWalk {
    incarnation: u64,
    root: B256,
    current: B256,
    path: SmallVec<[B256; 8]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FinalityClassification {
    Compatible,
    Conflict,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CandidateStage {
    Executed,
    Validated,
}

impl CandidateStage {
    const fn wire(self) -> BlockStage {
        match self {
            Self::Executed => BlockStage::Executed,
            Self::Validated => BlockStage::Validated,
        }
    }
}

impl Publisher {
    fn new_configured(
        producer: FeedProducer,
        source: Arc<dyn SnapshotSource>,
        shared: Arc<Shared>,
        initial: Arc<Projection>,
        initial_anchored_at: Instant,
        initial_forkchoice: ForkchoiceMeta,
        policy: CandidatePolicy,
    ) -> Self {
        let CandidatePolicy {
            projection_limit: cache_limit,
            metadata_limit,
            retention: candidate_retention,
            work_budget: retirement_work_budget,
        } = policy;
        let canonical_hash = initial.block.hash;
        let mut candidates = HashMap::default();
        candidates.reserve(metadata_limit);
        candidates.insert(
            canonical_hash,
            CandidateEntry {
                block: initial.block,
                projection: Some(Arc::clone(&initial)),
                stage: CandidateStage::Validated,
                tracked: true,
                first_sequence: 0,
                incarnation: 1,
                expires_at: Instant::now() + candidate_retention,
            },
        );
        let projection_order = VecDeque::from([(1, canonical_hash)]);
        let publisher = Self {
            producer,
            source,
            shared,
            candidates,
            deferred_executed: HashMap::default(),
            children_by_parent: HashMap::default(),
            projection_order,
            metadata_order: VecDeque::from([(1, canonical_hash)]),
            tombstones: HashSet::with_capacity(metadata_limit),
            tombstone_order: VecDeque::with_capacity(metadata_limit),
            next_incarnation: 2,
            projection_count: 1,
            cache_limit,
            metadata_limit,
            candidate_retention,
            retirement_work_budget,
            canonical_hash,
            last_finalized: initial_forkchoice.finalized,
            finality_sweep: None,
            snapshot_anchored_at: initial_anchored_at,
        };
        publisher.update_candidate_metric();
        publisher
    }

    #[cfg(test)]
    fn new(
        producer: FeedProducer,
        source: Arc<dyn SnapshotSource>,
        shared: Arc<Shared>,
        initial: Arc<Projection>,
        initial_anchored_at: Instant,
        cache_limit: usize,
    ) -> Self {
        let initial_forkchoice = ForkchoiceMeta {
            head: CheckpointMeta {
                number: initial.block.number,
                hash: initial.block.hash,
            },
            safe: None,
            finalized: None,
        };
        Self::new_configured(
            producer,
            source,
            shared,
            initial,
            initial_anchored_at,
            initial_forkchoice,
            CandidatePolicy {
                projection_limit: cache_limit,
                metadata_limit: cache_limit.saturating_mul(8),
                retention: Duration::from_secs(120),
                work_budget: 256,
            },
        )
    }

    fn handle(&mut self, event: FeedEvent) -> Result<()> {
        match event {
            FeedEvent::Executed {
                observed_at,
                generation,
                block,
                changes,
            } => {
                if observed_at <= self.snapshot_anchored_at {
                    return Ok(());
                }
                let published =
                    self.project_candidate(generation, block, &changes, CandidateStage::Executed)?;
                if published {
                    self.shared
                        .metrics
                        .executed_end_to_end
                        .record(observed_at.elapsed().as_secs_f64());
                }
                Ok(())
            }
            FeedEvent::Validated {
                observed_at,
                generation,
                block_hash,
            } => {
                let published = self.promote_validated(generation, block_hash)?;
                if published {
                    self.shared
                        .metrics
                        .validated_end_to_end
                        .record(observed_at.elapsed().as_secs_f64());
                }
                Ok(())
            }
            FeedEvent::ValidatedFallback {
                observed_at,
                generation,
                block,
                changes,
            } => {
                if observed_at <= self.snapshot_anchored_at {
                    return Ok(());
                }
                let published =
                    self.project_candidate(generation, block, &changes, CandidateStage::Validated)?;
                if published {
                    self.shared
                        .metrics
                        .validated_end_to_end
                        .record(observed_at.elapsed().as_secs_f64());
                }
                Ok(())
            }
            FeedEvent::CanonicalStateReset {
                observed_at,
                generation,
                head,
            } => {
                let active_generation = self
                    .shared
                    .published
                    .load()
                    .canonical
                    .watch_set
                    .generation();
                if generation > active_generation {
                    return Err(eyre!(
                        "received canonical reset generation {generation} before active generation {active_generation}"
                    ));
                }
                if observed_at <= self.snapshot_anchored_at {
                    return Ok(());
                }
                debug!(target: "statefeed", number = head.number, hash = %head.hash, "re-anchoring after canonical state reset");
                self.announce_gap("canonical_state_reset")?;
                self.reanchor()?;
                Ok(())
            }
            FeedEvent::ForkchoiceApplied {
                observed_at,
                generation,
                view,
            } => {
                let published = self.forkchoice_applied(generation, observed_at, view)?;
                if published {
                    self.shared
                        .metrics
                        .forkchoice_end_to_end
                        .record(observed_at.elapsed().as_secs_f64());
                }
                Ok(())
            }
            FeedEvent::Rejected {
                observed_at,
                generation,
                block_hash,
                reason,
            } => {
                let published = self.rejected(generation, block_hash, reason)?;
                if published {
                    self.shared
                        .metrics
                        .rejected_end_to_end
                        .record(observed_at.elapsed().as_secs_f64());
                }
                Ok(())
            }
        }
    }

    fn handle_control(&mut self, event: ControlEvent) -> Result<()> {
        match event {
            ControlEvent::ActivateConfig { watch_set, ack } => {
                let activated = match self.activate(watch_set) {
                    Ok(()) => true,
                    Err(error) => {
                        warn!(target: "statefeed", %error, "statefeed config activation rejected");
                        false
                    }
                };
                let _ = ack.send(activated);
                Ok(())
            }
        }
    }

    fn project_candidate(
        &mut self,
        generation: u64,
        block: BlockMeta,
        changes: &[crate::watch::SlotChange],
        stage: CandidateStage,
    ) -> Result<bool> {
        let projection_started = Instant::now();
        let published = self.shared.published.load();
        let watch_set = Arc::clone(&published.canonical.watch_set);
        let canonical_number = published.canonical.block.number;
        let active_generation = watch_set.generation();
        drop(published);
        if generation < active_generation {
            // A validation that started before an atomic reload may finish afterwards.
            return Ok(false);
        }
        if generation > active_generation {
            return Err(eyre!(
                "received block generation {} before active generation {}",
                generation,
                active_generation
            ));
        }
        if self.tombstones.contains(&block.hash) {
            return Ok(false);
        }
        if let Some(candidate) = self.candidates.get(&block.hash) {
            if candidate.block != block {
                return Err(eyre!(
                    "candidate hash {} was observed with inconsistent block metadata",
                    block.hash
                ));
            }
            if candidate.projection.is_some() {
                if stage == CandidateStage::Validated && candidate.stage == CandidateStage::Executed
                {
                    return self.promote_validated(generation, block.hash);
                }
                return Ok(false);
            }
        }
        if block.number <= canonical_number && !self.candidates.contains_key(&block.parent_hash) {
            // Buffered validation output can predate the initial snapshot taken after node launch.
            return Ok(false);
        }

        let mut values = if let Some(parent) = self
            .candidates
            .get(&block.parent_hash)
            .and_then(|candidate| candidate.projection.as_ref())
        {
            let expected_number = parent
                .block
                .number
                .checked_add(1)
                .ok_or_else(|| eyre!("parent block number overflow for {}", block.hash))?;
            if block.number != expected_number {
                return Err(eyre!(
                    "invalid projection ancestry for {}: parent {} is block {}, child is block {}",
                    block.hash,
                    block.parent_hash,
                    parent.block.number,
                    block.number
                ));
            }
            if parent.watch_set.generation() != generation
                || parent.values.len() != watch_set.len().saturating_mul(32)
            {
                return Err(eyre!(
                    "watch generation mismatch while projecting block {}",
                    block.hash
                ));
            }
            parent.values.to_vec()
        } else {
            if stage == CandidateStage::Executed {
                // An early candidate is not guaranteed to be queryable by hash yet. A missing
                // parent therefore defers this speculative projection until validation, without
                // delaying the publisher on a provider read.
                self.shared
                    .metrics
                    .candidate_parent_cache_misses
                    .increment(1);
                debug!(
                    target: "statefeed",
                    block = %block.hash,
                    parent = %block.parent_hash,
                    "deferring executed candidate because its parent is not cached"
                );
                self.insert_deferred_executed(block, BlockChanges::from_slice(changes))?;
                return Ok(false);
            }
            // A bounded fork cache can legitimately evict a late candidate's parent. Read that
            // exact parent instead of the child: validation is already complete, but the engine
            // may not have inserted the child into the tree provider yet. Applying the absolute
            // watched delta to the parent projection remains exact and avoids that race.
            self.shared
                .metrics
                .candidate_parent_cache_misses
                .increment(1);
            let snapshot_started = Instant::now();
            let projection = match self
                .source
                .load_at(Arc::clone(&watch_set), block.parent_hash)
            {
                Ok(projection) => projection,
                Err(error) => {
                    // BlockchainProvider can recover canonical/pending state by hash, but an
                    // arbitrary old side fork may already be gone. Dropping that one candidate
                    // is preferable to falsely invalidating the otherwise contiguous stream.
                    debug!(
                        target: "statefeed",
                        %error,
                        block = %block.hash,
                        parent = %block.parent_hash,
                        "cannot reconstruct validated candidate from its parent"
                    );
                    return Ok(false);
                }
            };
            self.shared
                .metrics
                .snapshot_duration
                .record(snapshot_started.elapsed().as_secs_f64());
            validate_projection_dictionary(&projection, &watch_set)?;
            let expected_number = projection
                .block
                .number
                .checked_add(1)
                .ok_or_else(|| eyre!("parent block number overflow for {}", block.hash))?;
            if projection.block.hash != block.parent_hash || expected_number != block.number {
                return Err(eyre!(
                    "validated parent projection mismatch for {}/{}: expected parent {}, got {}/{}",
                    block.number,
                    block.hash,
                    block.parent_hash,
                    projection.block.number,
                    projection.block.hash
                ));
            }
            validate_projection_shape(&projection)?;
            projection.values.to_vec()
        };

        let mut changed_bitmap = vec![0u8; watch_set.len().div_ceil(8)];
        for change in changes {
            let index = change.key_id as usize;
            let offset = index.saturating_mul(32);
            let Some(value) = values.get_mut(offset..offset.saturating_add(32)) else {
                return Err(eyre!("invalid key id {} in block delta", change.key_id));
            };
            let next = change.new_value.to_be_bytes::<32>();
            if value != next {
                value.copy_from_slice(&next);
                changed_bitmap[index / 8] |= 1 << (index % 8);
            }
        }

        let projection = Arc::new(Projection {
            block,
            watch_set: Arc::clone(&watch_set),
            values: Bytes::from(values),
            changed_bitmap: Bytes::from(changed_bitmap),
        });

        let first_sequence = self.shared.publish(
            generation,
            envelope::Event::BlockState(block_state(&projection, stage.wire())),
        )?;
        self.shared
            .metrics
            .projection_duration
            .record(projection_started.elapsed().as_secs_f64());
        self.insert_candidate(projection, stage, true, first_sequence)?;
        Ok(true)
    }

    #[cfg(test)]
    fn validated(
        &mut self,
        generation: u64,
        block: BlockMeta,
        changes: &[crate::watch::SlotChange],
    ) -> Result<bool> {
        self.project_candidate(generation, block, changes, CandidateStage::Validated)
    }

    fn promote_validated(&mut self, generation: u64, block_hash: B256) -> Result<bool> {
        let active_generation = self
            .shared
            .published
            .load()
            .canonical
            .watch_set
            .generation();
        if generation < active_generation {
            return Ok(false);
        }
        if generation > active_generation {
            return Err(eyre!(
                "received validation generation {generation} before active generation {active_generation}"
            ));
        }

        if let Some(candidate) = self.candidates.get(&block_hash) {
            if candidate.stage == CandidateStage::Validated {
                return Ok(false);
            }
            let Some(projection) = candidate.projection.as_ref() else {
                self.candidates
                    .get_mut(&block_hash)
                    .expect("candidate exists until this synchronous update")
                    .stage = CandidateStage::Validated;
                return Ok(false);
            };
            if projection.watch_set.generation() != generation {
                return Ok(false);
            }

            self.shared.publish(
                generation,
                envelope::Event::BlockValidated(BlockValidated {
                    block_hash: block_hash.to_vec(),
                }),
            )?;
            self.candidates
                .get_mut(&block_hash)
                .expect("candidate exists until this synchronous update")
                .stage = CandidateStage::Validated;
            return Ok(true);
        }

        let Some(deferred) = self.deferred_executed.get(&block_hash) else {
            // Reconnecting consumers do not receive non-canonical candidate history either. A
            // hash-only promotion is meaningful only when this generation saw EXECUTED.
            return Ok(false);
        };
        let block = deferred.block;
        let changes = deferred.changes.clone();
        self.project_candidate(generation, block, &changes, CandidateStage::Validated)
    }

    fn activate(&mut self, watch_set: Arc<WatchSet>) -> Result<()> {
        let current_generation = self
            .shared
            .published
            .load()
            .canonical
            .watch_set
            .generation();
        if watch_set.generation() <= current_generation {
            return Ok(());
        }

        let snapshot_started = Instant::now();
        let loaded = self.source.load_latest(Arc::clone(&watch_set))?;
        validate_snapshot_view(&loaded)?;
        let forkchoice = loaded.forkchoice;
        let projection = Arc::new(loaded.projection);
        validate_projection_dictionary(&projection, &watch_set)?;
        self.shared
            .metrics
            .snapshot_duration
            .record(snapshot_started.elapsed().as_secs_f64());
        let config_event = envelope::Event::ConfigActivated(config_activated(&watch_set));
        let snapshot_event = envelope::Event::Snapshot(snapshot(&projection, forkchoice));
        validate_event_frame(&self.shared, watch_set.generation(), config_event.clone())?;
        validate_projection_frames(&self.shared, &projection, forkchoice)?;

        self.snapshot_anchored_at = loaded.anchored_at;
        self.reset_candidates(Arc::clone(&projection), forkchoice);

        // Make reconnect handshakes observe the new dictionary and snapshot before advertising it
        // to existing consumers. Live engine events remain on the old generation until activate().
        self.shared.publish_state(
            watch_set.generation(),
            config_event,
            Arc::clone(&projection),
            Some(forkchoice),
            false,
        )?;
        self.shared
            .publish(watch_set.generation(), snapshot_event)?;
        self.producer.activate(watch_set);
        self.shared
            .metrics
            .config_generation
            .set(projection.watch_set.generation() as f64);
        info!(
            target: "statefeed",
            generation = projection.watch_set.generation(),
            keys = projection.watch_set.len(),
            block = %projection.block.hash,
            "activated statefeed configuration"
        );
        Ok(())
    }

    fn rejected(
        &mut self,
        generation: u64,
        block_hash: B256,
        reason: &'static str,
    ) -> Result<bool> {
        let active_generation = self
            .shared
            .published
            .load()
            .canonical
            .watch_set
            .generation();
        if generation < active_generation {
            return Ok(false);
        }
        if generation > active_generation {
            return Err(eyre!(
                "received rejection generation {generation} before active generation {active_generation}"
            ));
        }
        let rejectable = self
            .candidates
            .get(&block_hash)
            .is_some_and(|candidate| candidate.stage == CandidateStage::Executed)
            || self.deferred_executed.contains_key(&block_hash);
        if !rejectable {
            // REJECTED is a terminal transition only for an EXECUTED candidate emitted by this
            // generation. In particular, a malformed duplicate newPayload must not invalidate a
            // block that is already VALIDATED or CANONICAL.
            return Ok(false);
        }

        self.shared.publish(
            generation,
            envelope::Event::BlockRejected(BlockRejected {
                block_hash: block_hash.to_vec(),
                reason: reason.into(),
            }),
        )?;
        self.remove_candidate_tree(block_hash, true);
        Ok(true)
    }

    fn canonical(
        &mut self,
        generation: u64,
        observed_at: Instant,
        expected_block_number: Option<u64>,
        block_hash: B256,
    ) -> Result<bool> {
        let active_generation = self
            .shared
            .published
            .load()
            .canonical
            .watch_set
            .generation();
        if generation > active_generation {
            return Err(eyre!(
                "received canonical generation {generation} before active generation {active_generation}"
            ));
        }

        if observed_at <= self.snapshot_anchored_at {
            // The provider selected its snapshot head after this callback was observed, so the
            // snapshot already covers it. Number-based filtering is deliberately avoided here:
            // forkchoice may legitimately reorg to a shorter execution chain.
            return Ok(false);
        }

        let current = self.shared.published.load();
        if block_hash == current.canonical.block.hash {
            // Reth invokes the hook when forkchoice merely reaffirms the existing head as well.
            return Ok(false);
        }
        let previous_projection = Arc::clone(&current.canonical);
        let watch_set = Arc::clone(&previous_projection.watch_set);
        drop(current);

        let projection = if generation == active_generation {
            self.candidates
                .get(&block_hash)
                .and_then(|candidate| candidate.projection.as_ref().map(Arc::clone))
        } else {
            // A callback queued while a reload snapshot was being built still carries the old
            // generation. Its old projection cannot be reused, but its canonical transition must
            // be replayed against the new dictionary.
            None
        };
        let projection = if let Some(projection) = projection {
            projection
        } else {
            let snapshot_started = Instant::now();
            let projection = Arc::new(self.source.load_at(watch_set, block_hash)?);
            self.shared
                .metrics
                .snapshot_duration
                .record(snapshot_started.elapsed().as_secs_f64());
            projection
        };
        validate_projection_dictionary(&projection, &previous_projection.watch_set)?;
        if projection.block.hash != block_hash
            || expected_block_number.is_some_and(|number| projection.block.number != number)
        {
            return Err(eyre!(
                "canonical projection identity mismatch: callback {:?}/{block_hash}, projection {}/{}",
                expected_block_number,
                projection.block.number,
                projection.block.hash
            ));
        }
        validate_projection_shape(&projection)?;
        let changed_bitmap = canonical_changed_bitmap(&previous_projection, &projection)?;

        let previous = self.canonical_hash;
        // Keep the last *published* canonical head protected while inserting a reconstructed
        // projection. Cache enforcement can publish retirement events, and retiring `previous`
        // before consumers receive CanonicalHead(previous -> block_hash) would violate stream
        // ordering. The newly inserted projection is the newest eviction candidate, so protecting
        // `previous` is sufficient until the canonical frame is committed below.
        self.insert_candidate(projection.clone(), CandidateStage::Validated, true, 0)?;
        let sequence = self.shared.publish_state(
            projection.watch_set.generation(),
            envelope::Event::CanonicalHead(CanonicalHead {
                previous_block_hash: previous.to_vec(),
                block: Some(block_ref(projection.block)),
                values: projection.values.clone(),
                changed_bitmap,
            }),
            Arc::clone(&projection),
            None,
            false,
        )?;
        self.canonical_hash = block_hash;
        self.schedule_expiry(previous);
        if let Some(candidate) = self.candidates.get_mut(&block_hash)
            && candidate.first_sequence == 0
        {
            candidate.first_sequence = sequence;
        }
        self.shared
            .metrics
            .canonical_end_to_end
            .record(observed_at.elapsed().as_secs_f64());
        Ok(true)
    }

    fn forkchoice_applied(
        &mut self,
        generation: u64,
        observed_at: Instant,
        applied: AppliedForkchoiceMeta,
    ) -> Result<bool> {
        let published = self.shared.published.load();
        let active_generation = published.canonical.watch_set.generation();
        drop(published);
        if generation > active_generation {
            return Err(eyre!(
                "received forkchoice generation {generation} before active generation {active_generation}"
            ));
        }
        if observed_at <= self.snapshot_anchored_at {
            return Ok(false);
        }

        // `ForkchoiceApplied` is the sole externally visible source of canonical truth. Resolve
        // the numbered projection here, outside the Engine API hot path.
        if self.canonical_hash != applied.head_hash {
            self.canonical(generation, observed_at, None, applied.head_hash)?;
        }
        let published = self.shared.published.load();
        if published.canonical.block.hash != applied.head_hash {
            return Err(eyre!(
                "applied forkchoice head {} does not match canonical projection {}/{}",
                applied.head_hash,
                published.canonical.block.number,
                published.canonical.block.hash
            ));
        }
        let projection = Arc::clone(&published.canonical);
        let active_generation = projection.watch_set.generation();
        drop(published);
        let view = applied.resolve(projection.block.number);
        validate_forkchoice_checkpoints(view)?;

        // Never deduplicate this event: its sequence is the consumer's forkchoice view id even
        // when head, safe, and finalized all reaffirm the previous values.
        self.shared.publish_state(
            active_generation,
            envelope::Event::ForkchoiceApplied(ForkchoiceApplied {
                view: Some(forkchoice_view(view)),
            }),
            projection,
            Some(view),
            false,
        )?;

        // The Reth hook reports the effective finalized checkpoint, including a retained value
        // when the FCU supplied zero. `None` therefore means no coherent checkpoint is known.
        if let Some(finalized) = view.finalized
            && Some(finalized) != self.last_finalized
        {
            self.last_finalized = Some(finalized);
            self.begin_finality_sweep(finalized);
            self.process_finality_budget(active_generation, self.retirement_work_budget)?;
        }
        Ok(true)
    }

    fn announce_gap(&self, reason: &'static str) -> Result<()> {
        let published = self.shared.published.load();
        let generation = published.canonical.watch_set.generation();
        let canonical = Arc::clone(&published.canonical);
        let last_contiguous_sequence = self.shared.published_sequence.load(Ordering::Acquire);
        self.shared.publish_state(
            generation,
            envelope::Event::Gap(Gap {
                last_contiguous_sequence,
                reason: reason.into(),
            }),
            canonical,
            None,
            true,
        )?;
        Ok(())
    }

    fn reanchor(&mut self) -> Result<()> {
        let watch_set = self.shared.published.load().canonical.watch_set.clone();
        let snapshot_started = Instant::now();
        let loaded = self.source.load_latest(Arc::clone(&watch_set))?;
        validate_snapshot_view(&loaded)?;
        let forkchoice = loaded.forkchoice;
        let projection = Arc::new(loaded.projection);
        validate_projection_dictionary(&projection, &watch_set)?;
        self.shared
            .metrics
            .snapshot_duration
            .record(snapshot_started.elapsed().as_secs_f64());
        validate_projection_frames(&self.shared, &projection, forkchoice)?;
        self.snapshot_anchored_at = loaded.anchored_at;
        self.reset_candidates(Arc::clone(&projection), forkchoice);
        self.shared.publish_state(
            projection.watch_set.generation(),
            envelope::Event::Snapshot(snapshot(&projection, forkchoice)),
            projection,
            Some(forkchoice),
            false,
        )?;
        Ok(())
    }

    /// Keeps the stream in recovery until a fresh canonical anchor is available.
    ///
    /// Continuing to publish candidates after a `Gap` without a subsequent snapshot would make a
    /// consumer's recovery state ambiguous. Provider errors are expected to be transient, so the
    /// dedicated publisher thread retries with bounded backoff while the engine queue remains
    /// non-blocking and may coalesce further loss into another gap.
    fn recover_until_success(&mut self, reason: &'static str, shutdown: &Receiver<()>) -> bool {
        let mut gap_announced = self.shared.published.load().recovering;
        let mut retry_delay = Duration::from_millis(10);
        loop {
            if !gap_announced {
                match self.announce_gap(reason) {
                    Ok(()) => gap_announced = true,
                    Err(error) => {
                        error!(target: "statefeed", %error, "failed to announce statefeed recovery gap");
                    }
                }
            }

            if gap_announced {
                match self.reanchor() {
                    Ok(()) => return true,
                    Err(error) => {
                        error!(target: "statefeed", %error, "failed to recover statefeed snapshot; retrying");
                    }
                }
            }

            match shutdown.recv_timeout(retry_delay) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => return false,
                Err(RecvTimeoutError::Timeout) => {}
            }
            retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
        }
    }

    fn insert_candidate(
        &mut self,
        projection: Arc<Projection>,
        stage: CandidateStage,
        tracked: bool,
        first_sequence: u64,
    ) -> Result<()> {
        let generation = projection.watch_set.generation();
        let hash = projection.block.hash;
        let block = projection.block;
        let replaced_deferred = self.deferred_executed.remove(&hash);
        let existing_incarnation = self
            .candidates
            .get(&hash)
            .map(|entry| entry.incarnation)
            .or_else(|| replaced_deferred.as_ref().map(|entry| entry.incarnation));
        let incarnation = existing_incarnation.unwrap_or_else(|| self.allocate_incarnation());
        let expires_at = Instant::now() + self.candidate_retention;
        let mut gained_projection = true;
        match self.candidates.entry(hash) {
            alloy_primitives::map::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                gained_projection = entry.projection.is_none();
                entry.block = block;
                entry.projection = Some(projection);
                entry.stage = stage;
                if tracked && !entry.tracked {
                    entry.first_sequence = first_sequence;
                }
                entry.tracked |= tracked;
                entry.expires_at = expires_at;
            }
            alloy_primitives::map::Entry::Vacant(vacant) => {
                vacant.insert(CandidateEntry {
                    block,
                    projection: Some(projection),
                    stage,
                    tracked,
                    first_sequence,
                    incarnation,
                    expires_at,
                });
                if replaced_deferred.is_none() {
                    self.add_child(block.parent_hash, hash);
                }
                if existing_incarnation.is_none() {
                    self.metadata_order.push_back((incarnation, hash));
                    self.queue_finality_check(incarnation, hash);
                }
            }
        }
        if gained_projection {
            self.projection_count += 1;
            self.projection_order.push_back((incarnation, hash));
        }
        self.trim_projection_cache(generation, hash)?;
        self.trim_metadata(generation)?;
        self.update_candidate_metric();
        Ok(())
    }

    fn insert_deferred_executed(&mut self, block: BlockMeta, changes: BlockChanges) -> Result<()> {
        if self.tombstones.contains(&block.hash) {
            return Ok(());
        }
        let hash = block.hash;
        if !self.candidates.contains_key(&hash) {
            let existing_incarnation = self
                .deferred_executed
                .get(&hash)
                .map(|entry| entry.incarnation);
            let incarnation = existing_incarnation.unwrap_or_else(|| self.allocate_incarnation());
            let inserted = existing_incarnation.is_none();
            self.deferred_executed.insert(
                hash,
                DeferredExecuted {
                    block,
                    changes,
                    incarnation,
                    expires_at: Instant::now() + self.candidate_retention,
                },
            );
            if inserted {
                self.add_child(block.parent_hash, hash);
                self.metadata_order.push_back((incarnation, hash));
                self.queue_finality_check(incarnation, hash);
            }
            let generation = self
                .shared
                .published
                .load()
                .canonical
                .watch_set
                .generation();
            self.trim_metadata(generation)?;
            self.update_candidate_metric();
        }
        Ok(())
    }

    fn maintain(&mut self) -> Result<()> {
        let now = Instant::now();
        let generation = self
            .shared
            .published
            .load()
            .canonical
            .watch_set
            .generation();
        let finality_work =
            self.process_finality_budget(generation, self.retirement_work_budget)?;
        let ttl_budget = self.retirement_work_budget.saturating_sub(finality_work);
        let mut due = Vec::with_capacity(ttl_budget);
        due.extend(
            self.candidates
                .iter()
                .filter(|(hash, entry)| **hash != self.canonical_hash && entry.expires_at <= now)
                .map(|(hash, _)| *hash)
                .take(ttl_budget),
        );
        if due.len() < ttl_budget {
            due.extend(
                self.deferred_executed
                    .iter()
                    .filter(|(hash, entry)| {
                        **hash != self.canonical_hash && entry.expires_at <= now
                    })
                    .map(|(hash, _)| *hash)
                    .take(ttl_budget - due.len()),
            );
        }
        let mut expired = Vec::with_capacity(due.len());
        for hash in due {
            if self.remove_one(hash, false) {
                expired.push(hash);
            }
        }
        self.publish_retired(generation, &expired, RetirementReason::RetentionExpired)?;
        self.update_candidate_metric();
        Ok(())
    }

    fn begin_finality_sweep(&mut self, checkpoint: CheckpointMeta) {
        let pending: VecDeque<_> = self.metadata_order.iter().copied().collect();
        let mut classifications = HashMap::default();
        classifications.reserve(pending.len());
        self.finality_sweep = Some(FinalitySweep {
            checkpoint,
            pending,
            classifications,
            active: None,
        });
    }

    fn queue_finality_check(&mut self, incarnation: u64, hash: B256) {
        if let Some(sweep) = &mut self.finality_sweep {
            sweep.pending.push_back((incarnation, hash));
        } else if let Some(checkpoint) = self.last_finalized {
            self.finality_sweep = Some(FinalitySweep {
                checkpoint,
                pending: VecDeque::from([(incarnation, hash)]),
                classifications: HashMap::default(),
                active: None,
            });
        }
        if self
            .finality_sweep
            .as_ref()
            .is_some_and(|sweep| sweep.pending.len() > self.metadata_limit.saturating_mul(2))
        {
            let mut sweep = self
                .finality_sweep
                .take()
                .expect("finality sweep was checked above");
            sweep.pending.retain(|(incarnation, hash)| {
                self.current_incarnation(*hash) == Some(*incarnation)
            });
            self.finality_sweep = Some(sweep);
        }
    }

    fn process_finality_budget(&mut self, generation: u64, budget: usize) -> Result<usize> {
        let Some(mut sweep) = self.finality_sweep.take() else {
            return Ok(0);
        };
        let mut work = 0;
        let mut retired = Vec::new();
        while work < budget {
            let mut walk = if let Some(walk) = sweep.active.take() {
                walk
            } else {
                let Some((incarnation, hash)) = sweep.pending.pop_front() else {
                    break;
                };
                if self.current_incarnation(hash) != Some(incarnation)
                    || hash == self.canonical_hash
                {
                    work += 1;
                    continue;
                }
                FinalityWalk {
                    incarnation,
                    root: hash,
                    current: hash,
                    path: SmallVec::new(),
                }
            };

            // One budget unit performs at most one metadata lookup or follows one ancestry edge.
            // Memoized path compression makes total classification work linear in cached metadata
            // instead of rescanning a deep shared ancestry for every candidate.
            work += 1;
            let classification =
                if let Some(classification) = sweep.classifications.get(&walk.current).copied() {
                    Some(classification)
                } else {
                    self.advance_finality_walk(&mut walk, sweep.checkpoint)
                };

            let Some(classification) = classification else {
                sweep.active = Some(walk);
                continue;
            };

            if classification != FinalityClassification::Unknown {
                sweep.classifications.insert(walk.current, classification);
                for hash in &walk.path {
                    sweep.classifications.insert(*hash, classification);
                }
            }
            if classification == FinalityClassification::Conflict
                && self.current_incarnation(walk.root) == Some(walk.incarnation)
                && self.remove_one(walk.root, true)
            {
                retired.push(walk.root);
            }
        }
        if !sweep.pending.is_empty() || sweep.active.is_some() {
            self.finality_sweep = Some(sweep);
        }
        self.publish_retired(generation, &retired, RetirementReason::FinalizedConflict)?;
        self.update_candidate_metric();
        Ok(work)
    }

    fn advance_finality_walk(
        &self,
        walk: &mut FinalityWalk,
        finalized: CheckpointMeta,
    ) -> Option<FinalityClassification> {
        let Some(block) = self.block_meta(walk.current) else {
            return Some(FinalityClassification::Unknown);
        };
        if block.number == finalized.number {
            return Some(if block.hash == finalized.hash {
                FinalityClassification::Compatible
            } else {
                FinalityClassification::Conflict
            });
        }
        if block.number < finalized.number {
            return Some(FinalityClassification::Compatible);
        }
        let Some(expected_parent_number) = block.number.checked_sub(1) else {
            return Some(FinalityClassification::Unknown);
        };
        let Some(parent) = self.block_meta(block.parent_hash) else {
            // A consensus-terminal parent makes every descendant terminal as well. This preserves
            // classification when an earlier bounded step already removed the parent.
            return Some(if self.tombstones.contains(&block.parent_hash) {
                FinalityClassification::Conflict
            } else {
                FinalityClassification::Unknown
            });
        };
        if parent.number != expected_parent_number {
            return Some(FinalityClassification::Unknown);
        }
        walk.path.push(walk.current);
        walk.current = parent.hash;
        None
    }

    fn block_meta(&self, hash: B256) -> Option<BlockMeta> {
        self.candidates
            .get(&hash)
            .map(|entry| entry.block)
            .or_else(|| self.deferred_executed.get(&hash).map(|entry| entry.block))
    }

    fn current_incarnation(&self, hash: B256) -> Option<u64> {
        self.candidates
            .get(&hash)
            .map(|entry| entry.incarnation)
            .or_else(|| {
                self.deferred_executed
                    .get(&hash)
                    .map(|entry| entry.incarnation)
            })
    }

    fn trim_projection_cache(&mut self, generation: u64, inserted: B256) -> Result<()> {
        let mut retired = Vec::new();
        let mut examined = 0;
        while self.projection_count > self.cache_limit {
            let Some((incarnation, oldest)) = self.projection_order.pop_front() else {
                break;
            };
            examined += 1;
            let is_current_projection = self.candidates.get(&oldest).is_some_and(|candidate| {
                candidate.incarnation == incarnation && candidate.projection.is_some()
            });
            if !is_current_projection {
                continue;
            }
            if oldest == self.canonical_hash {
                self.projection_order.push_back((incarnation, oldest));
            } else if let Some(candidate) = self.candidates.get_mut(&oldest)
                && candidate.projection.take().is_some()
            {
                self.projection_count -= 1;
                if candidate.tracked {
                    candidate.tracked = false;
                    retired.push(oldest);
                }
            }
            if examined > self.projection_order.len().saturating_add(1) {
                return Err(eyre!(
                    "cannot evict candidate projection after inserting {inserted}"
                ));
            }
        }
        self.publish_retired(generation, &retired, RetirementReason::CacheEvicted)
    }

    fn trim_metadata(&mut self, generation: u64) -> Result<()> {
        let mut retired = Vec::new();
        while self.metadata_len() > self.metadata_limit {
            let Some((incarnation, hash)) = self.metadata_order.pop_front() else {
                return Err(eyre!("candidate metadata bound cannot be enforced"));
            };
            let current_incarnation = self.current_incarnation(hash);
            if current_incarnation != Some(incarnation) {
                continue;
            }
            if hash == self.canonical_hash {
                self.metadata_order.push_back((incarnation, hash));
                continue;
            }
            if self.remove_one(hash, false) {
                retired.push(hash);
            }
        }
        self.publish_retired(generation, &retired, RetirementReason::CacheEvicted)
    }

    fn remove_candidate_tree(&mut self, root: B256, consensus_terminal: bool) {
        if root == self.canonical_hash {
            return;
        }
        let mut pending = vec![root];
        while let Some(hash) = pending.pop() {
            if let Some(children) = self.children_by_parent.remove(&hash) {
                pending.extend(children);
            }
            self.remove_one(hash, consensus_terminal);
        }
        self.update_candidate_metric();
    }

    /// Removes one metadata record and returns whether consumers still tracked its projection.
    fn remove_one(&mut self, hash: B256, consensus_terminal: bool) -> bool {
        let mut parent = None;
        let mut tracked = false;
        if let Some(candidate) = self.candidates.remove(&hash) {
            parent = Some(candidate.block.parent_hash);
            tracked = candidate.tracked;
            if candidate.projection.is_some() {
                self.projection_count = self.projection_count.saturating_sub(1);
            }
        }
        if let Some(deferred) = self.deferred_executed.remove(&hash) {
            parent = Some(deferred.block.parent_hash);
        }
        if let Some(parent) = parent
            && let Some(children) = self.children_by_parent.get_mut(&parent)
        {
            children.retain(|child| *child != hash);
            if children.is_empty() {
                self.children_by_parent.remove(&parent);
            }
        }
        if consensus_terminal {
            self.insert_tombstone(hash);
        }
        self.compact_order_queues_if_needed();
        tracked
    }

    fn publish_retired(
        &self,
        generation: u64,
        hashes: &[B256],
        reason: RetirementReason,
    ) -> Result<()> {
        if hashes.is_empty() {
            return Ok(());
        }
        // Keep batches below the configured frame bound even with a deliberately tiny frame.
        let batch_size = self.shared.max_frame_bytes.saturating_sub(256).max(32) / 34;
        for batch in hashes.chunks(batch_size.max(1)) {
            self.shared.publish(
                generation,
                envelope::Event::CandidatesRetired(CandidatesRetired {
                    block_hashes: batch.iter().map(|hash| hash.to_vec()).collect(),
                    reason: reason as i32,
                }),
            )?;
        }
        Ok(())
    }

    fn add_child(&mut self, parent: B256, child: B256) {
        let children = self.children_by_parent.entry(parent).or_default();
        if !children.contains(&child) {
            children.push(child);
        }
    }

    fn schedule_expiry(&mut self, hash: B256) {
        if hash == self.canonical_hash {
            return;
        }
        if let Some(candidate) = self.candidates.get_mut(&hash) {
            candidate.expires_at = Instant::now() + self.candidate_retention;
        } else if let Some(deferred) = self.deferred_executed.get_mut(&hash) {
            deferred.expires_at = Instant::now() + self.candidate_retention;
        }
    }

    fn allocate_incarnation(&mut self) -> u64 {
        let incarnation = self.next_incarnation;
        self.next_incarnation = self.next_incarnation.wrapping_add(1).max(1);
        incarnation
    }

    fn insert_tombstone(&mut self, hash: B256) {
        if self.tombstones.insert(hash) {
            self.tombstone_order.push_back(hash);
        }
        while self.tombstones.len() > self.metadata_limit {
            if let Some(oldest) = self.tombstone_order.pop_front() {
                self.tombstones.remove(&oldest);
            }
        }
    }

    fn metadata_len(&self) -> usize {
        self.candidates.len() + self.deferred_executed.len()
    }

    fn compact_order_queues_if_needed(&mut self) {
        if self.metadata_order.len() > self.metadata_limit.saturating_mul(2) {
            self.metadata_order.retain(|(incarnation, hash)| {
                self.candidates
                    .get(hash)
                    .map(|entry| entry.incarnation)
                    .or_else(|| {
                        self.deferred_executed
                            .get(hash)
                            .map(|entry| entry.incarnation)
                    })
                    == Some(*incarnation)
            });
        }
        if self.projection_order.len() > self.cache_limit.saturating_mul(2) {
            self.projection_order.retain(|(incarnation, hash)| {
                self.candidates.get(hash).is_some_and(|entry| {
                    entry.incarnation == *incarnation && entry.projection.is_some()
                })
            });
        }
    }

    fn update_candidate_metric(&self) {
        self.shared
            .metrics
            .candidates_cached
            .set(self.metadata_len() as f64);
        self.shared
            .metrics
            .candidate_projections_cached
            .set(self.projection_count as f64);
    }

    fn reset_candidates(&mut self, projection: Arc<Projection>, view: ForkchoiceMeta) {
        self.canonical_hash = projection.block.hash;
        self.last_finalized = view.finalized;
        self.candidates.clear();
        self.deferred_executed.clear();
        self.children_by_parent.clear();
        self.projection_order.clear();
        self.metadata_order.clear();
        self.tombstones.clear();
        self.tombstone_order.clear();
        self.finality_sweep = None;
        self.projection_count = 1;
        let incarnation = self.allocate_incarnation();
        self.candidates.insert(
            projection.block.hash,
            CandidateEntry {
                block: projection.block,
                projection: Some(Arc::clone(&projection)),
                stage: CandidateStage::Validated,
                tracked: true,
                first_sequence: 0,
                incarnation,
                expires_at: Instant::now() + self.candidate_retention,
            },
        );
        self.projection_order
            .push_back((incarnation, projection.block.hash));
        self.metadata_order
            .push_back((incarnation, projection.block.hash));
        self.update_candidate_metric();
    }
}

async fn run_config_reloader(
    config_path: PathBuf,
    immutable_stream_config: StreamConfig,
    producer: FeedProducer,
    mut shutdown: watch::Receiver<bool>,
) {
    // Filesystem backends can emit large bursts for one atomic save. One pending notification is
    // enough because reload reads the complete file and the loop drains/coalesces the burst.
    let expected_name = config_path.file_name().map(ToOwned::to_owned);
    let (events_tx, mut events_rx) = mpsc::channel(1);
    let mut watcher =
        match notify::recommended_watcher(move |event: notify::Result<notify::Event>| match event {
            Ok(event)
                if event
                    .paths
                    .iter()
                    .any(|path| path.file_name() == expected_name.as_deref()) =>
            {
                let _ = events_tx.try_send(());
            }
            Ok(_) => {}
            Err(error) => warn!(target: "statefeed", %error, "config watcher error"),
        }) {
            Ok(watcher) => watcher,
            Err(error) => {
                error!(target: "statefeed", %error, "failed to create config file watcher");
                return;
            }
        };
    let parent = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let Err(error) = watcher.watch(parent, notify::RecursiveMode::NonRecursive) {
        error!(target: "statefeed", %error, path = %parent.display(), "failed to watch config directory");
        return;
    }

    let mut sighup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(signal) => signal,
        Err(error) => {
            error!(target: "statefeed", %error, "failed to install SIGHUP handler");
            return;
        }
    };
    let mut next_generation = producer.watch_set().generation().saturating_add(1);
    let mut reload_requested = false;

    loop {
        if !reload_requested {
            let should_reload = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                    false
                }
                signal = sighup.recv() => {
                    if signal.is_none() {
                        break;
                    }
                    true
                },
                event = events_rx.recv() => {
                    if event.is_none() {
                        break;
                    }
                    true
                }
            };
            if !should_reload {
                continue;
            }
        }
        reload_requested = false;

        // Editors commonly replace a file through several rapid rename/write events. Coalesce the
        // burst so only the final complete file is parsed.
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        while events_rx.try_recv().is_ok() {}

        let config = match Config::load(&config_path) {
            Ok(config) => config,
            Err(error) => {
                warn!(target: "statefeed", %error, "statefeed config reload rejected");
                continue;
            }
        };
        if config.stream != immutable_stream_config {
            warn!(
                target: "statefeed",
                "stream settings cannot be changed by reload; restart the process to apply them"
            );
            continue;
        }

        if producer.watch_set().matches_config(&config.watch) {
            debug!(target: "statefeed", "statefeed watch config is unchanged");
            continue;
        }

        let watch_set = Arc::new(WatchSet::compile(next_generation, &config.watch));
        let mut retry_delay = Duration::from_millis(25);
        loop {
            let retry = match producer.request_activation(Arc::clone(&watch_set)) {
                ActivationRequest::Queued(completion) => {
                    let activated = tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return;
                            }
                            false
                        }
                        completed = completion => match completed {
                            Ok(activated) => activated,
                            Err(_) => return,
                        }
                    };
                    if activated {
                        next_generation = next_generation.saturating_add(1);
                        break;
                    }
                    warn!(target: "statefeed", "statefeed config activation failed; retrying");
                    true
                }
                ActivationRequest::Full => {
                    warn!(target: "statefeed", "statefeed config queue is full; retrying activation");
                    true
                }
                ActivationRequest::Disconnected => return,
            };
            debug_assert!(retry);

            // A newer filesystem event supersedes the failed request; otherwise retain and retry
            // this exact generation so transient provider errors cannot lose a valid config.
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                signal = sighup.recv() => {
                    if signal.is_none() {
                        return;
                    }
                    reload_requested = true;
                }
                event = events_rx.recv() => {
                    if event.is_none() {
                        return;
                    }
                    reload_requested = true;
                }
                () = tokio::time::sleep(retry_delay) => {}
            }
            if reload_requested {
                break;
            }
            retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
        }
    }
}

fn bind_socket(path: &Path, socket_mode: u32) -> Result<(UnixListener, SocketCleanup)> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .wrap_err_with(|| format!("failed to create socket directory {}", parent.display()))?;
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            match StdUnixStream::connect(path) {
                Ok(_) => {
                    return Err(eyre!(
                        "refusing to replace active statefeed socket {}",
                        path.display()
                    ));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::NotFound
                    ) => {}
                Err(error) => {
                    return Err(error).wrap_err_with(|| {
                        format!(
                            "cannot prove that existing statefeed socket {} is stale",
                            path.display()
                        )
                    });
                }
            }
            fs::remove_file(path)
                .wrap_err_with(|| format!("failed to remove stale socket {}", path.display()))?;
        }
        Ok(_) => {
            return Err(eyre!(
                "refusing to replace non-socket path {}",
                path.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).wrap_err("failed to inspect statefeed socket path"),
    }

    let listener = UnixListener::bind(path)
        .wrap_err_with(|| format!("failed to bind statefeed socket {}", path.display()))?;
    let metadata = fs::symlink_metadata(path)
        .wrap_err_with(|| format!("failed to inspect bound socket {}", path.display()))?;
    let cleanup = SocketCleanup {
        path: path.to_owned(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    fs::set_permissions(path, fs::Permissions::from_mode(socket_mode))
        .wrap_err_with(|| format!("failed to set permissions on {}", path.display()))?;
    Ok((listener, cleanup))
}

async fn run_server(
    listener: UnixListener,
    _socket_cleanup: SocketCleanup,
    shared: Arc<Shared>,
    max_consumers: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut consumers = JoinSet::new();
    let permits = Arc::new(Semaphore::new(max_consumers));
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            completed = consumers.join_next(), if !consumers.is_empty() => {
                match completed {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => {
                        debug!(target: "statefeed", %error, "statefeed consumer disconnected");
                    }
                    Some(Err(error)) => {
                        warn!(target: "statefeed", %error, "statefeed consumer task failed");
                    }
                    None => {}
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                            warn!(target: "statefeed", max_consumers, "statefeed consumer limit reached");
                            continue;
                        };
                        let shared = Arc::clone(&shared);
                        let consumer_shutdown = shutdown.clone();
                        consumers.spawn(async move {
                            let _permit = permit;
                            serve_consumer(stream, shared, consumer_shutdown).await
                        });
                    }
                    Err(error) => warn!(target: "statefeed", %error, "failed to accept statefeed consumer"),
                }
            }
        }
    }
    consumers.abort_all();
    while consumers.join_next().await.is_some() {}
}

fn validate_handshake_frames(
    shared: &Shared,
    projection: &Projection,
    forkchoice: ForkchoiceMeta,
) -> Result<()> {
    let generation = projection.watch_set.generation();
    let events = [
        envelope::Event::Hello(Hello {
            service_version: env!("CARGO_PKG_VERSION").into(),
            chain_id: shared.chain_id,
            genesis_hash: shared.genesis_hash.to_vec(),
            capabilities: shared.capabilities,
        }),
        envelope::Event::ConfigActivated(config_activated(&projection.watch_set)),
    ];
    for event in events {
        validate_event_frame(shared, generation, event)?;
    }
    validate_projection_frames(shared, projection, forkchoice)?;
    Ok(())
}

fn validate_projection_frames(
    shared: &Shared,
    projection: &Projection,
    forkchoice: ForkchoiceMeta,
) -> Result<()> {
    validate_projection_shape(projection)?;
    let generation = projection.watch_set.generation();
    let mut worst_case_block = block_state(projection, BlockStage::Validated);
    worst_case_block.changed_bitmap =
        Bytes::from(vec![0xff; projection.watch_set.len().div_ceil(8)]);
    let canonical = CanonicalHead {
        previous_block_hash: B256::ZERO.to_vec(),
        block: Some(block_ref(projection.block)),
        values: projection.values.clone(),
        changed_bitmap: worst_case_block.changed_bitmap.clone(),
    };
    for event in [
        envelope::Event::Snapshot(snapshot(projection, forkchoice)),
        envelope::Event::BlockState(worst_case_block),
        envelope::Event::CanonicalHead(canonical),
    ] {
        validate_event_frame(shared, generation, event)?;
    }
    Ok(())
}

/// Returns keys changed by the canonical transition, not merely by `next` relative to its parent.
///
/// The candidate bitmap can be reused for the overwhelmingly common direct-head advance. Reorgs
/// and provider-loaded projections require one linear comparison of the already packed values so
/// downstream dependency indexes never miss a value changed by switching branches.
fn canonical_changed_bitmap(previous: &Projection, next: &Projection) -> Result<Bytes> {
    let key_count = next.watch_set.len();
    let bitmap_len = key_count.div_ceil(8);
    if previous.watch_set.generation() != next.watch_set.generation()
        || previous.values.len() != next.values.len()
    {
        return Err(eyre!(
            "cannot compare canonical projections from different watch dictionaries"
        ));
    }

    if next.block.parent_hash == previous.block.hash && next.changed_bitmap.len() == bitmap_len {
        return Ok(next.changed_bitmap.clone());
    }

    let mut changed = vec![0u8; bitmap_len];
    for (index, (before, after)) in previous
        .values
        .chunks_exact(32)
        .zip(next.values.chunks_exact(32))
        .enumerate()
    {
        if before != after {
            changed[index / 8] |= 1 << (index % 8);
        }
    }
    Ok(changed.into())
}

fn validate_projection_dictionary(projection: &Projection, expected: &Arc<WatchSet>) -> Result<()> {
    if Arc::ptr_eq(&projection.watch_set, expected) {
        return Ok(());
    }

    let actual = &projection.watch_set;
    let dictionaries_match = actual.generation() == expected.generation()
        && actual.keys().len() == expected.keys().len()
        && actual
            .keys()
            .iter()
            .zip(expected.keys())
            .all(|(actual, expected)| {
                actual.key_id == expected.key_id
                    && actual.id == expected.id
                    && actual.address == expected.address
                    && actual.slot == expected.slot
            });
    if !dictionaries_match {
        return Err(eyre!(
            "snapshot source returned a projection for a different watch dictionary"
        ));
    }
    Ok(())
}

fn validate_snapshot_view(snapshot: &CanonicalSnapshot) -> Result<()> {
    let block = snapshot.projection.block;
    let view = snapshot.forkchoice;
    if view.head.number != block.number || view.head.hash != block.hash {
        return Err(eyre!(
            "snapshot forkchoice head {}/{} does not match projection {}/{}",
            view.head.number,
            view.head.hash,
            block.number,
            block.hash
        ));
    }
    validate_forkchoice_checkpoints(view)
}

fn validate_forkchoice_checkpoints(view: ForkchoiceMeta) -> Result<()> {
    for checkpoint in [view.safe, view.finalized].into_iter().flatten() {
        if checkpoint.number > view.head.number {
            return Err(eyre!(
                "forkchoice checkpoint {}/{} is ahead of head {}/{}",
                checkpoint.number,
                checkpoint.hash,
                view.head.number,
                view.head.hash
            ));
        }
    }
    if let (Some(safe), Some(finalized)) = (view.safe, view.finalized)
        && finalized.number > safe.number
    {
        return Err(eyre!(
            "finalized checkpoint {}/{} is ahead of safe checkpoint {}/{}",
            finalized.number,
            finalized.hash,
            safe.number,
            safe.hash
        ));
    }
    Ok(())
}

fn validate_projection_shape(projection: &Projection) -> Result<()> {
    let expected_values = projection
        .watch_set
        .len()
        .checked_mul(32)
        .ok_or_else(|| eyre!("statefeed projection length overflow"))?;
    if projection.values.len() != expected_values {
        return Err(eyre!(
            "projection at {} has {} value bytes, expected {expected_values}",
            projection.block.hash,
            projection.values.len()
        ));
    }

    let expected_bitmap = projection.watch_set.len().div_ceil(8);
    if !projection.changed_bitmap.is_empty() && projection.changed_bitmap.len() != expected_bitmap {
        return Err(eyre!(
            "projection at {} has {} changed-bitmap bytes, expected zero or {expected_bitmap}",
            projection.block.hash,
            projection.changed_bitmap.len()
        ));
    }
    Ok(())
}

fn validate_event_frame(shared: &Shared, generation: u64, event: envelope::Event) -> Result<()> {
    // Use maximum-width monotonic fields. A preflight with zero would be a few protobuf bytes
    // smaller and could let a configuration fail only after the process had run long enough.
    let mut envelope = shared.envelope(u64::MAX, generation, event);
    envelope.emitted_at_monotonic_ns = u64::MAX;
    wire::encode_frame(&envelope, shared.max_frame_bytes)?;
    Ok(())
}

async fn serve_consumer(
    mut stream: UnixStream,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    shared.metrics.connected_consumers.increment(1.0);
    let _consumer_guard = ConsumerGuard(shared.metrics.connected_consumers.clone());

    // Subscribe before reading the publication baseline. Frames committed after the baseline are
    // then guaranteed to reach this receiver. A frame committed just before the read may be sent
    // slightly later, but its sequence is at or below `baseline` and is intentionally treated as
    // pre-connection history; reconnect only promises the canonical snapshot, not old candidates.
    let mut frames = shared.frames.subscribe();
    let committed_sequence = shared.published_sequence.load(Ordering::Acquire);
    let published = shared.published.load_full();
    if published.recovering {
        return Err(eyre!(
            "statefeed recovery is in progress; reconnect after the replacement snapshot"
        ));
    }
    let Some(forkchoice) = published.forkchoice else {
        return Err(eyre!(
            "statefeed forkchoice transition is in progress; reconnect after ForkchoiceApplied"
        ));
    };
    if forkchoice.head.hash != published.canonical.block.hash
        || forkchoice.head.number != published.canonical.block.number
    {
        return Err(eyre!(
            "statefeed handshake state has an incoherent canonical/forkchoice head"
        ));
    }
    // The ArcSwap transition can become visible just before its sequence is committed. Tagging
    // handshake state with that effective sequence prevents buffered canonical/config frames from
    // replaying an older projection over the newer authoritative snapshot.
    let baseline = committed_sequence.max(published.effective_sequence);
    let generation = published.canonical.watch_set.generation();

    write_event(
        &mut stream,
        &shared,
        baseline,
        generation,
        envelope::Event::Hello(Hello {
            service_version: env!("CARGO_PKG_VERSION").into(),
            chain_id: shared.chain_id,
            genesis_hash: shared.genesis_hash.to_vec(),
            capabilities: shared.capabilities,
        }),
    )
    .await?;
    write_event(
        &mut stream,
        &shared,
        baseline,
        generation,
        envelope::Event::ConfigActivated(config_activated(&published.canonical.watch_set)),
    )
    .await?;
    write_event(
        &mut stream,
        &shared,
        baseline,
        generation,
        envelope::Event::Snapshot(snapshot(&published.canonical, forkchoice)),
    )
    .await?;

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(())
                }
            }
            received = frames.recv() => match received {
                Ok(frame) if frame.sequence > baseline => {
                    write_frame(&mut stream, &shared, &frame.bytes).await?
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    shared.metrics.consumer_gaps.increment(1);
                    return Err(eyre!("consumer lagged by {skipped} statefeed frames"));
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

struct ConsumerGuard(Gauge);

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}

async fn write_event(
    stream: &mut UnixStream,
    shared: &Shared,
    sequence: u64,
    generation: u64,
    event: envelope::Event,
) -> Result<()> {
    let envelope = shared.envelope(sequence, generation, event);
    let frame = wire::encode_frame(&envelope, shared.max_frame_bytes)?;
    write_frame(stream, shared, &frame).await?;
    Ok(())
}

async fn write_frame(stream: &mut UnixStream, shared: &Shared, frame: &[u8]) -> Result<()> {
    let send_started = Instant::now();
    tokio::time::timeout(Duration::from_secs(1), stream.write_all(frame))
        .await
        .wrap_err("statefeed consumer write timed out")??;
    shared
        .metrics
        .socket_send_duration
        .record(send_started.elapsed().as_secs_f64());
    Ok(())
}

fn config_activated(watch_set: &WatchSet) -> ConfigActivated {
    ConfigActivated {
        keys: watch_set
            .keys()
            .iter()
            .map(|key| WatchKey {
                key_id: key.key_id,
                id: key.id.to_string(),
                address: key.address.to_vec(),
                slot: key.slot.to_vec(),
            })
            .collect(),
    }
}

fn snapshot(projection: &Projection, forkchoice: ForkchoiceMeta) -> Snapshot {
    Snapshot {
        block: Some(block_ref(projection.block)),
        values: projection.values.clone(),
        forkchoice: Some(forkchoice_view(forkchoice)),
    }
}

fn forkchoice_view(view: ForkchoiceMeta) -> ForkchoiceView {
    ForkchoiceView {
        head: Some(checkpoint_ref(view.head)),
        safe: view.safe.map(checkpoint_ref),
        finalized: view.finalized.map(checkpoint_ref),
    }
}

fn checkpoint_ref(checkpoint: CheckpointMeta) -> CheckpointRef {
    CheckpointRef {
        number: checkpoint.number,
        hash: checkpoint.hash.to_vec(),
    }
}

fn block_state(projection: &Projection, stage: BlockStage) -> BlockState {
    BlockState {
        stage: stage as i32,
        block: Some(block_ref(projection.block)),
        values: projection.values.clone(),
        changed_bitmap: projection.changed_bitmap.clone(),
    }
}

fn block_ref(block: BlockMeta) -> BlockRef {
    BlockRef {
        number: block.number,
        hash: block.hash.to_vec(),
        parent_hash: block.parent_hash.to_vec(),
        timestamp: block.timestamp,
    }
}

#[cfg(test)]
fn encode_values(values: &[alloy_primitives::U256]) -> Bytes {
    let mut encoded = Vec::with_capacity(values.len() * 32);
    for value in values {
        encoded.extend_from_slice(&value.to_be_bytes::<32>());
    }
    Bytes::from(encoded)
}

#[derive(Debug)]
struct SocketCleanup {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        match fs::symlink_metadata(&self.path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && metadata.dev() == self.device
                    && metadata.ino() == self.inode =>
            {
                if let Err(error) = fs::remove_file(&self.path) {
                    warn!(target: "statefeed", %error, path = %self.path.display(), "failed to remove statefeed socket");
                }
            }
            Ok(_) => warn!(
                target: "statefeed",
                path = %self.path.display(),
                "statefeed socket path was replaced; refusing to remove it"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(target: "statefeed", %error, path = %self.path.display(), "failed to inspect statefeed socket during cleanup")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use prost::Message;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::config::WatchConfig;

    #[derive(Clone)]
    struct MockSource {
        projection: Projection,
    }

    impl SnapshotSource for MockSource {
        fn load_latest(&self, watch_set: Arc<WatchSet>) -> Result<CanonicalSnapshot> {
            let mut projection = self.projection.clone();
            projection.watch_set = watch_set;
            Ok(CanonicalSnapshot {
                forkchoice: forkchoice_for(projection.block),
                projection,
                anchored_at: Instant::now(),
            })
        }

        fn load_at(&self, watch_set: Arc<WatchSet>, block_hash: B256) -> Result<Projection> {
            let mut projection = self.projection.clone();
            projection.watch_set = watch_set;
            projection.block.hash = block_hash;
            Ok(projection)
        }
    }

    struct FailOnceSource {
        projection: Projection,
        attempts: AtomicU64,
    }

    impl SnapshotSource for FailOnceSource {
        fn load_latest(&self, watch_set: Arc<WatchSet>) -> Result<CanonicalSnapshot> {
            if self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(eyre!("injected transient snapshot failure"));
            }
            let mut projection = self.projection.clone();
            projection.watch_set = watch_set;
            Ok(CanonicalSnapshot {
                forkchoice: forkchoice_for(projection.block),
                projection,
                anchored_at: Instant::now(),
            })
        }

        fn load_at(&self, watch_set: Arc<WatchSet>, block_hash: B256) -> Result<Projection> {
            let mut projection = self.projection.clone();
            projection.watch_set = watch_set;
            projection.block.hash = block_hash;
            Ok(projection)
        }
    }

    fn watch_set() -> Arc<WatchSet> {
        Arc::new(WatchSet::compile(
            1,
            &[WatchConfig {
                id: "value".into(),
                address: alloy_primitives::Address::ZERO,
                slot: B256::ZERO,
            }],
        ))
    }

    fn forkchoice_for(block: BlockMeta) -> ForkchoiceMeta {
        ForkchoiceMeta {
            head: CheckpointMeta {
                number: block.number,
                hash: block.hash,
            },
            safe: None,
            finalized: None,
        }
    }

    fn applied_forkchoice(view: ForkchoiceMeta) -> AppliedForkchoiceMeta {
        AppliedForkchoiceMeta {
            head_hash: view.head.hash,
            safe: view.safe,
            finalized: view.finalized,
        }
    }

    fn decode_published(receiver: &mut broadcast::Receiver<PublishedFrame>) -> Envelope {
        let frame = receiver.try_recv().expect("expected published frame");
        Envelope::decode(&frame.bytes[4..]).expect("valid published envelope")
    }

    #[test]
    fn executed_capability_tracks_the_startup_setting() {
        assert_eq!(advertised_capabilities(false) & CAP_EXECUTED, 0);
        assert_ne!(advertised_capabilities(true) & CAP_EXECUTED, 0);
        assert_ne!(advertised_capabilities(false) & CAP_FORKCHOICE_APPLIED, 0);
        assert_ne!(advertised_capabilities(false) & CAP_CANDIDATE_RETIREMENT, 0);
        assert_eq!(
            advertised_capabilities(false) | CAP_EXECUTED,
            advertised_capabilities(true)
        );
    }

    fn publisher_fixture() -> (
        Publisher,
        Arc<Shared>,
        broadcast::Receiver<PublishedFrame>,
        Arc<Projection>,
    ) {
        let initial = Arc::new(Projection {
            block: BlockMeta {
                number: 10,
                hash: B256::with_last_byte(10),
                parent_hash: B256::with_last_byte(9),
                timestamp: 10,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::from(1)]),
            changed_bitmap: Bytes::new(),
        });
        let (frames, frame_rx) = broadcast::channel(16);
        let shared = Arc::new(Shared {
            boot_id: Bytes::from_static(&[0; 16]),
            chain_id: 1,
            genesis_hash: B256::ZERO,
            started_at: Instant::now(),
            sequence: AtomicU64::new(0),
            published_sequence: AtomicU64::new(0),
            max_frame_bytes: 4096,
            capabilities: CAP_FULL_PROJECTIONS
                | CAP_EXECUTED
                | CAP_VALIDATED
                | CAP_CANONICAL
                | CAP_REJECTED,
            published: ArcSwap::from_pointee(PublishedState {
                canonical: Arc::clone(&initial),
                forkchoice: Some(forkchoice_for(initial.block)),
                effective_sequence: 0,
                recovering: false,
            }),
            frames,
            metrics: PublisherMetrics::new(),
        });
        let (producer, _rx) = FeedProducer::channel(watch_set(), 16);
        let publisher = Publisher::new(
            producer,
            Arc::new(MockSource {
                projection: initial.as_ref().clone(),
            }),
            Arc::clone(&shared),
            Arc::clone(&initial),
            Instant::now(),
            16,
        );
        (publisher, shared, frame_rx, initial)
    }

    #[test]
    fn executed_candidate_is_promoted_without_rebuilding_its_projection() {
        let (mut publisher, shared, mut frame_rx, initial) = publisher_fixture();
        let block_hash = B256::with_last_byte(11);
        publisher
            .project_candidate(
                1,
                BlockMeta {
                    number: 11,
                    hash: block_hash,
                    parent_hash: initial.block.hash,
                    timestamp: 11,
                },
                &[crate::watch::SlotChange {
                    key_id: 0,
                    new_value: U256::from(2),
                }],
                CandidateStage::Executed,
            )
            .unwrap();

        let frame = frame_rx.try_recv().unwrap();
        let executed = Envelope::decode(&frame.bytes[4..]).unwrap();
        let Some(envelope::Event::BlockState(state)) = executed.event else {
            panic!("expected executed block state")
        };
        assert_eq!(BlockStage::try_from(state.stage), Ok(BlockStage::Executed));
        assert_eq!(
            value_at(
                &publisher.candidates[&block_hash]
                    .projection
                    .as_ref()
                    .unwrap()
                    .values,
                0,
            ),
            U256::from(2)
        );

        publisher.promote_validated(1, block_hash).unwrap();

        let frame = frame_rx.try_recv().unwrap();
        let validated = Envelope::decode(&frame.bytes[4..]).unwrap();
        assert!(matches!(
            validated.event,
            Some(envelope::Event::BlockValidated(BlockValidated { block_hash: hash }))
                if hash == block_hash.as_slice()
        ));
        assert_eq!(
            publisher.candidates[&block_hash].stage,
            CandidateStage::Validated
        );

        let sequence = shared.published_sequence.load(Ordering::Acquire);
        publisher.promote_validated(1, block_hash).unwrap();
        assert_eq!(shared.published_sequence.load(Ordering::Acquire), sequence);
    }

    #[test]
    fn every_applied_forkchoice_is_published_even_when_the_view_is_unchanged() {
        let (mut publisher, shared, mut frame_rx, initial) = publisher_fixture();
        let view = forkchoice_for(initial.block);

        assert!(
            publisher
                .forkchoice_applied(1, Instant::now(), applied_forkchoice(view))
                .unwrap()
        );
        assert!(
            publisher
                .forkchoice_applied(1, Instant::now(), applied_forkchoice(view))
                .unwrap()
        );

        let first = decode_published(&mut frame_rx);
        let second = decode_published(&mut frame_rx);
        assert!(matches!(
            first.event,
            Some(envelope::Event::ForkchoiceApplied(_))
        ));
        assert!(matches!(
            second.event,
            Some(envelope::Event::ForkchoiceApplied(_))
        ));
        assert_eq!(second.sequence, first.sequence + 1);
        assert_eq!(shared.published.load().forkchoice, Some(view));
    }

    #[tokio::test]
    async fn canonical_transition_is_not_exposed_to_new_consumers_before_its_fcu_fence() {
        let (mut publisher, shared, mut frame_rx, initial) = publisher_fixture();
        let next = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        publisher.validated(1, next, &[]).unwrap();
        let _candidate = decode_published(&mut frame_rx);

        publisher
            .canonical(1, Instant::now(), Some(next.number), next.hash)
            .unwrap();
        let canonical = decode_published(&mut frame_rx);
        assert!(matches!(
            canonical.event,
            Some(envelope::Event::CanonicalHead(_))
        ));
        assert!(shared.published.load().forkchoice.is_none());

        let (server, _client) = UnixStream::pair().unwrap();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let error = serve_consumer(server, Arc::clone(&shared), shutdown_rx)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("forkchoice transition"));

        publisher
            .forkchoice_applied(1, Instant::now(), applied_forkchoice(forkchoice_for(next)))
            .unwrap();
        let forkchoice = decode_published(&mut frame_rx);
        assert!(matches!(
            forkchoice.event,
            Some(envelope::Event::ForkchoiceApplied(_))
        ));
        assert_eq!(forkchoice.sequence, canonical.sequence + 1);
    }

    #[test]
    fn applied_forkchoice_atomically_drives_canonical_transition_then_fence() {
        let (mut publisher, shared, mut frame_rx, initial) = publisher_fixture();
        let next = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        publisher.validated(1, next, &[]).unwrap();
        let _candidate = decode_published(&mut frame_rx);

        publisher
            .forkchoice_applied(1, Instant::now(), applied_forkchoice(forkchoice_for(next)))
            .unwrap();

        let canonical = decode_published(&mut frame_rx);
        let Some(envelope::Event::CanonicalHead(canonical_head)) = canonical.event else {
            panic!("expected canonical head before applied forkchoice fence")
        };
        assert_eq!(
            canonical_head.previous_block_hash,
            initial.block.hash.to_vec()
        );
        assert_eq!(canonical_head.block.unwrap().hash, next.hash.to_vec());

        let fence = decode_published(&mut frame_rx);
        assert!(matches!(
            fence.event,
            Some(envelope::Event::ForkchoiceApplied(_))
        ));
        assert_eq!(fence.sequence, canonical.sequence + 1);
        assert_eq!(
            shared.published.load().forkchoice,
            Some(forkchoice_for(next))
        );
    }

    #[test]
    fn reconstructed_head_does_not_retire_the_published_canonical_before_transition() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        publisher.cache_limit = 2;
        let side = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        publisher.validated(1, side, &[]).unwrap();
        let _side_candidate = decode_published(&mut frame_rx);

        // This same-height head is absent from the candidate cache, so canonical() reconstructs it
        // through SnapshotSource while the projection cache is already full (H + side).
        let next_hash = B256::repeat_byte(10);
        publisher
            .canonical(1, Instant::now(), Some(initial.block.number), next_hash)
            .unwrap();

        let retirement = decode_published(&mut frame_rx);
        let Some(envelope::Event::CandidatesRetired(retirement)) = retirement.event else {
            panic!("expected unrelated side-candidate eviction before canonical transition")
        };
        assert_eq!(retirement.block_hashes, vec![side.hash.to_vec()]);
        assert!(
            publisher.candidates[&initial.block.hash]
                .projection
                .is_some()
        );

        let canonical = decode_published(&mut frame_rx);
        let Some(envelope::Event::CanonicalHead(canonical)) = canonical.event else {
            panic!("expected canonical transition after cache enforcement")
        };
        assert_eq!(canonical.previous_block_hash, initial.block.hash.to_vec());
        assert_eq!(canonical.block.unwrap().hash, next_hash.to_vec());
    }

    #[test]
    fn finalized_checkpoint_retires_only_provably_conflicting_candidates() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        let selected = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        let sibling = BlockMeta {
            number: 11,
            hash: B256::repeat_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        let sibling_child = BlockMeta {
            number: 12,
            hash: B256::repeat_byte(12),
            parent_hash: sibling.hash,
            timestamp: 12,
        };
        for block in [selected, sibling, sibling_child] {
            publisher.validated(1, block, &[]).unwrap();
            let _ = decode_published(&mut frame_rx);
        }
        publisher
            .canonical(1, Instant::now(), Some(selected.number), selected.hash)
            .unwrap();
        let _canonical = decode_published(&mut frame_rx);
        let view = ForkchoiceMeta {
            head: CheckpointMeta {
                number: selected.number,
                hash: selected.hash,
            },
            safe: Some(CheckpointMeta {
                number: selected.number,
                hash: selected.hash,
            }),
            finalized: Some(CheckpointMeta {
                number: selected.number,
                hash: selected.hash,
            }),
        };

        publisher
            .forkchoice_applied(1, Instant::now(), applied_forkchoice(view))
            .unwrap();

        let fence = decode_published(&mut frame_rx);
        let retirement = decode_published(&mut frame_rx);
        assert!(matches!(
            fence.event,
            Some(envelope::Event::ForkchoiceApplied(_))
        ));
        let Some(envelope::Event::CandidatesRetired(retirement)) = retirement.event else {
            panic!("expected exact finality retirement batch")
        };
        assert_eq!(
            RetirementReason::try_from(retirement.reason),
            Ok(RetirementReason::FinalizedConflict)
        );
        let retired: HashSet<_> = retirement.block_hashes.into_iter().collect();
        assert_eq!(
            retired,
            HashSet::from([sibling.hash.to_vec(), sibling_child.hash.to_vec()])
        );
        assert!(publisher.candidates.contains_key(&selected.hash));
        assert!(!publisher.candidates.contains_key(&sibling.hash));
        assert!(!publisher.candidates.contains_key(&sibling_child.hash));
    }

    #[test]
    fn absent_finalized_checkpoint_does_not_erase_known_finality() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        let selected = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        publisher.validated(1, selected, &[]).unwrap();
        let _candidate = decode_published(&mut frame_rx);
        publisher
            .canonical(1, Instant::now(), Some(selected.number), selected.hash)
            .unwrap();
        let _canonical = decode_published(&mut frame_rx);

        let finalized = CheckpointMeta {
            number: selected.number,
            hash: selected.hash,
        };
        publisher
            .forkchoice_applied(
                1,
                Instant::now(),
                applied_forkchoice(ForkchoiceMeta {
                    head: finalized,
                    safe: Some(finalized),
                    finalized: Some(finalized),
                }),
            )
            .unwrap();
        let _finalized_fence = decode_published(&mut frame_rx);
        assert_eq!(publisher.last_finalized, Some(finalized));

        // An absent coherent checkpoint is not evidence that established finality was reverted.
        publisher
            .forkchoice_applied(
                1,
                Instant::now(),
                applied_forkchoice(forkchoice_for(selected)),
            )
            .unwrap();
        let _zero_checkpoint_fence = decode_published(&mut frame_rx);
        assert_eq!(publisher.last_finalized, Some(finalized));

        let conflicting = BlockMeta {
            hash: B256::repeat_byte(11),
            ..selected
        };
        publisher.validated(1, conflicting, &[]).unwrap();
        let _conflicting_candidate = decode_published(&mut frame_rx);
        publisher.maintain().unwrap();

        let retirement = decode_published(&mut frame_rx);
        let Some(envelope::Event::CandidatesRetired(retirement)) = retirement.event else {
            panic!("expected finality retirement after absent-checkpoint FCU")
        };
        assert_eq!(
            RetirementReason::try_from(retirement.reason),
            Ok(RetirementReason::FinalizedConflict)
        );
        assert_eq!(retirement.block_hashes, vec![conflicting.hash.to_vec()]);
    }

    #[test]
    fn finality_sweep_obeys_the_per_iteration_work_budget() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        for suffix in 11..=14 {
            publisher
                .validated(
                    1,
                    BlockMeta {
                        number: 11,
                        hash: B256::with_last_byte(suffix),
                        parent_hash: initial.block.hash,
                        timestamp: 11,
                    },
                    &[],
                )
                .unwrap();
            let _ = decode_published(&mut frame_rx);
        }
        publisher.begin_finality_sweep(CheckpointMeta {
            number: initial.block.number,
            hash: initial.block.hash,
        });
        let before = publisher.finality_sweep.as_ref().unwrap().pending.len();

        assert_eq!(publisher.process_finality_budget(1, 1).unwrap(), 1);
        assert_eq!(
            publisher.finality_sweep.as_ref().unwrap().pending.len(),
            before - 1
        );
    }

    #[test]
    fn finality_budget_counts_ancestry_edges_instead_of_whole_candidates() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        let mut parent = initial.block;
        for number in 11..=14 {
            let block = BlockMeta {
                number,
                hash: B256::with_last_byte(number as u8),
                parent_hash: parent.hash,
                timestamp: number,
            };
            publisher.validated(1, block, &[]).unwrap();
            let _ = decode_published(&mut frame_rx);
            parent = block;
        }
        let incarnation = publisher.current_incarnation(parent.hash).unwrap();
        publisher.finality_sweep = Some(FinalitySweep {
            checkpoint: CheckpointMeta {
                number: initial.block.number,
                hash: initial.block.hash,
            },
            pending: VecDeque::from([(incarnation, parent.hash)]),
            classifications: HashMap::default(),
            active: None,
        });

        assert_eq!(publisher.process_finality_budget(1, 1).unwrap(), 1);
        let active = publisher
            .finality_sweep
            .as_ref()
            .and_then(|sweep| sweep.active.as_ref())
            .expect("deep ancestry walk remains incremental");
        assert_eq!(active.current, B256::with_last_byte(13));
        assert!(publisher.candidates.contains_key(&parent.hash));

        assert_eq!(publisher.process_finality_budget(1, 3).unwrap(), 3);
        assert!(publisher.finality_sweep.is_some());
        assert_eq!(publisher.process_finality_budget(1, 1).unwrap(), 1);
        assert!(publisher.finality_sweep.is_none());
        assert!(publisher.candidates.contains_key(&parent.hash));
    }

    #[test]
    fn projection_eviction_is_explicit_and_keeps_bounded_ancestry_metadata() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        publisher.cache_limit = 2;
        let first = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        let second = BlockMeta {
            number: 11,
            hash: B256::repeat_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        publisher.validated(1, first, &[]).unwrap();
        let _ = decode_published(&mut frame_rx);
        publisher.validated(1, second, &[]).unwrap();
        let _ = decode_published(&mut frame_rx);
        let retirement = decode_published(&mut frame_rx);

        let Some(envelope::Event::CandidatesRetired(retirement)) = retirement.event else {
            panic!("expected cache eviction retirement")
        };
        assert_eq!(
            RetirementReason::try_from(retirement.reason),
            Ok(RetirementReason::CacheEvicted)
        );
        assert_eq!(retirement.block_hashes, vec![first.hash.to_vec()]);
        assert!(publisher.candidates[&first.hash].projection.is_none());
        assert_eq!(publisher.projection_count, 2);

        // Local eviction is not consensus terminal: the same hash can be reconstructed later.
        assert!(publisher.validated(1, first, &[]).unwrap());
        assert!(publisher.candidates[&first.hash].projection.is_some());
    }

    #[test]
    fn retention_expiry_is_explicit_and_consensus_rejection_is_tombstoned() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        publisher.candidate_retention = Duration::from_millis(1);
        let candidate = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        publisher.validated(1, candidate, &[]).unwrap();
        let _ = decode_published(&mut frame_rx);
        std::thread::sleep(Duration::from_millis(2));
        publisher.maintain().unwrap();
        let retirement = decode_published(&mut frame_rx);
        assert!(matches!(
            retirement.event,
            Some(envelope::Event::CandidatesRetired(CandidatesRetired {
                reason,
                ..
            })) if RetirementReason::try_from(reason) == Ok(RetirementReason::RetentionExpired)
        ));
        assert!(!publisher.candidates.contains_key(&candidate.hash));

        let executed = BlockMeta {
            hash: B256::with_last_byte(12),
            ..candidate
        };
        publisher
            .project_candidate(1, executed, &[], CandidateStage::Executed)
            .unwrap();
        let _ = decode_published(&mut frame_rx);
        publisher
            .rejected(1, executed.hash, "block_validation_failed")
            .unwrap();
        let _ = decode_published(&mut frame_rx);
        assert!(publisher.tombstones.contains(&executed.hash));
        assert!(
            !publisher
                .project_candidate(1, executed, &[], CandidateStage::Executed)
                .unwrap()
        );
    }

    #[test]
    fn recovery_announces_one_gap_across_transient_snapshot_failure() {
        let (mut publisher, shared, mut frame_rx, initial) = publisher_fixture();
        publisher.source = Arc::new(FailOnceSource {
            projection: initial.as_ref().clone(),
            attempts: AtomicU64::new(0),
        });
        let (_shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(1);

        publisher.announce_gap("test_gap").unwrap();
        assert!(publisher.reanchor().is_err());
        assert!(shared.published.load().recovering);
        assert!(publisher.recover_until_success("test_gap", &shutdown_rx));
        assert!(!shared.published.load().recovering);

        let first = frame_rx.try_recv().unwrap();
        let second = frame_rx.try_recv().unwrap();
        let first = Envelope::decode(&first.bytes[4..]).unwrap();
        let second = Envelope::decode(&second.bytes[4..]).unwrap();
        assert!(matches!(first.event, Some(envelope::Event::Gap(_))));
        assert!(matches!(second.event, Some(envelope::Event::Snapshot(_))));
        assert!(frame_rx.try_recv().is_err());
    }

    #[test]
    fn canonical_state_reset_publishes_gap_then_anchored_snapshot() {
        let (mut publisher, shared, mut frame_rx, initial) = publisher_fixture();
        publisher
            .handle(FeedEvent::CanonicalStateReset {
                observed_at: Instant::now(),
                generation: 1,
                head: CheckpointMeta {
                    number: initial.block.number,
                    hash: initial.block.hash,
                },
            })
            .unwrap();

        let gap = decode_published(&mut frame_rx);
        assert!(matches!(gap.event, Some(envelope::Event::Gap(_))));
        let snapshot = decode_published(&mut frame_rx);
        assert!(matches!(snapshot.event, Some(envelope::Event::Snapshot(_))));
        assert_eq!(snapshot.sequence, gap.sequence + 1);
        assert!(!shared.published.load().recovering);
    }

    #[tokio::test]
    async fn consumer_handshake_is_refused_while_recovery_is_unanchored() {
        let (publisher, _shared, _frame_rx, _initial) = publisher_fixture();
        publisher.announce_gap("test_gap").unwrap();
        let (server, _client) = UnixStream::pair().unwrap();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = serve_consumer(server, Arc::clone(&publisher.shared), shutdown_rx)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("recovery is in progress"));
    }

    #[test]
    fn unknown_validation_promotion_is_ignored() {
        let (mut publisher, shared, _frame_rx, _initial) = publisher_fixture();

        publisher
            .promote_validated(1, B256::with_last_byte(99))
            .unwrap();

        assert_eq!(shared.published_sequence.load(Ordering::Acquire), 0);
    }

    #[test]
    fn deferred_executed_candidate_is_reconstructed_only_after_validation() {
        let (mut publisher, shared, mut frame_rx, _initial) = publisher_fixture();
        let block = BlockMeta {
            number: 12,
            hash: B256::with_last_byte(12),
            parent_hash: B256::with_last_byte(11),
            timestamp: 12,
        };
        publisher.source = Arc::new(MockSource {
            projection: Projection {
                block: BlockMeta {
                    number: 11,
                    hash: block.parent_hash,
                    parent_hash: B256::with_last_byte(10),
                    timestamp: 11,
                },
                watch_set: watch_set(),
                values: encode_values(&[U256::from(7)]),
                changed_bitmap: Bytes::new(),
            },
        });
        let changes = [crate::watch::SlotChange {
            key_id: 0,
            new_value: U256::from(8),
        }];

        publisher
            .project_candidate(1, block, &changes, CandidateStage::Executed)
            .unwrap();

        assert!(publisher.deferred_executed.contains_key(&block.hash));
        assert!(!publisher.candidates.contains_key(&block.hash));
        assert_eq!(shared.published_sequence.load(Ordering::Acquire), 0);

        publisher.promote_validated(1, block.hash).unwrap();

        assert!(!publisher.deferred_executed.contains_key(&block.hash));
        assert_eq!(
            publisher.candidates[&block.hash].stage,
            CandidateStage::Validated
        );
        assert_eq!(
            value_at(
                &publisher.candidates[&block.hash]
                    .projection
                    .as_ref()
                    .unwrap()
                    .values,
                0,
            ),
            U256::from(8)
        );
        let frame = frame_rx.try_recv().unwrap();
        let envelope = Envelope::decode(&frame.bytes[4..]).unwrap();
        let Some(envelope::Event::BlockState(state)) = envelope.event else {
            panic!("expected a full validated fallback")
        };
        assert_eq!(BlockStage::try_from(state.stage), Ok(BlockStage::Validated));
    }

    #[test]
    fn reload_discards_executed_candidate_before_old_validation_arrives() {
        let (mut publisher, shared, _frame_rx, initial) = publisher_fixture();
        let block = BlockMeta {
            number: 11,
            hash: B256::with_last_byte(11),
            parent_hash: initial.block.hash,
            timestamp: 11,
        };
        publisher
            .project_candidate(1, block, &[], CandidateStage::Executed)
            .unwrap();
        let next_watch_set = Arc::new(WatchSet::compile(
            2,
            &[WatchConfig {
                id: "value".into(),
                address: alloy_primitives::Address::ZERO,
                slot: B256::ZERO,
            }],
        ));

        publisher.activate(next_watch_set).unwrap();
        let sequence = shared.published_sequence.load(Ordering::Acquire);
        publisher.promote_validated(1, block.hash).unwrap();

        assert_eq!(shared.published_sequence.load(Ordering::Acquire), sequence);
        assert!(!publisher.candidates.contains_key(&block.hash));
        assert!(!publisher.deferred_executed.contains_key(&block.hash));
    }

    #[test]
    fn config_activation_ack_reports_transient_failure_then_success() {
        let (mut publisher, _shared, _frame_rx, initial) = publisher_fixture();
        publisher.source = Arc::new(FailOnceSource {
            projection: initial.as_ref().clone(),
            attempts: AtomicU64::new(0),
        });
        let next_watch_set = Arc::new(WatchSet::compile(
            2,
            &[WatchConfig {
                id: "next".into(),
                address: alloy_primitives::Address::ZERO,
                slot: B256::ZERO,
            }],
        ));

        let (first_ack, mut first_completion) = tokio::sync::oneshot::channel();
        publisher
            .handle_control(ControlEvent::ActivateConfig {
                watch_set: Arc::clone(&next_watch_set),
                ack: first_ack,
            })
            .unwrap();
        assert_eq!(first_completion.try_recv(), Ok(false));
        assert_eq!(publisher.producer.watch_set().generation(), 1);

        let (second_ack, mut second_completion) = tokio::sync::oneshot::channel();
        publisher
            .handle_control(ControlEvent::ActivateConfig {
                watch_set: next_watch_set,
                ack: second_ack,
            })
            .unwrap();
        assert_eq!(second_completion.try_recv(), Ok(true));
        assert_eq!(publisher.producer.watch_set().generation(), 2);
    }

    #[test]
    fn rejection_removes_executed_candidate_and_descendants() {
        let (mut publisher, _shared, _frame_rx, initial) = publisher_fixture();
        let parent_hash = B256::with_last_byte(11);
        publisher
            .project_candidate(
                1,
                BlockMeta {
                    number: 11,
                    hash: parent_hash,
                    parent_hash: initial.block.hash,
                    timestamp: 11,
                },
                &[],
                CandidateStage::Executed,
            )
            .unwrap();
        let child_hash = B256::with_last_byte(12);
        publisher
            .project_candidate(
                1,
                BlockMeta {
                    number: 12,
                    hash: child_hash,
                    parent_hash,
                    timestamp: 12,
                },
                &[],
                CandidateStage::Executed,
            )
            .unwrap();
        let deferred_hash = B256::with_last_byte(13);
        publisher
            .insert_deferred_executed(
                BlockMeta {
                    number: 13,
                    hash: deferred_hash,
                    parent_hash: child_hash,
                    timestamp: 13,
                },
                BlockChanges::new(),
            )
            .unwrap();

        publisher
            .rejected(1, parent_hash, "block_validation_failed")
            .unwrap();

        assert!(!publisher.candidates.contains_key(&parent_hash));
        assert!(!publisher.candidates.contains_key(&child_hash));
        assert!(!publisher.deferred_executed.contains_key(&deferred_hash));
        assert!(publisher.candidates.contains_key(&initial.block.hash));
    }

    #[test]
    fn rejection_cannot_invalidate_validated_or_unknown_blocks() {
        let (mut publisher, shared, mut frame_rx, initial) = publisher_fixture();
        let validated_hash = B256::with_last_byte(11);
        publisher
            .project_candidate(
                1,
                BlockMeta {
                    number: 11,
                    hash: validated_hash,
                    parent_hash: initial.block.hash,
                    timestamp: 11,
                },
                &[],
                CandidateStage::Validated,
            )
            .unwrap();
        let _validated_frame = frame_rx.try_recv().unwrap();
        let sequence = shared.published_sequence.load(Ordering::Acquire);

        assert!(
            !publisher
                .rejected(1, validated_hash, "block_validation_failed")
                .unwrap()
        );
        assert!(
            !publisher
                .rejected(1, B256::with_last_byte(99), "block_validation_failed")
                .unwrap()
        );
        assert_eq!(shared.published_sequence.load(Ordering::Acquire), sequence);
        assert_eq!(
            publisher.candidates[&validated_hash].stage,
            CandidateStage::Validated
        );
        assert!(frame_rx.try_recv().is_err());
    }

    #[test]
    fn no_op_delta_does_not_set_changed_bitmap() {
        let (mut publisher, _shared, mut frame_rx, initial) = publisher_fixture();
        publisher
            .project_candidate(
                1,
                BlockMeta {
                    number: 11,
                    hash: B256::with_last_byte(11),
                    parent_hash: initial.block.hash,
                    timestamp: 11,
                },
                &[crate::watch::SlotChange {
                    key_id: 0,
                    new_value: U256::from(1),
                }],
                CandidateStage::Executed,
            )
            .unwrap();

        let frame = frame_rx.try_recv().unwrap();
        let envelope = Envelope::decode(&frame.bytes[4..]).unwrap();
        let Some(envelope::Event::BlockState(state)) = envelope.event else {
            panic!("expected block state")
        };
        assert_eq!(state.changed_bitmap.as_ref(), &[0]);
    }

    #[test]
    fn projection_wire_values_are_fixed_width() {
        let projection = Projection {
            block: BlockMeta {
                number: 1,
                hash: B256::with_last_byte(1),
                parent_hash: B256::ZERO,
                timestamp: 2,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::from(7)]),
            changed_bitmap: vec![1].into(),
        };
        let message = block_state(&projection, BlockStage::Validated);
        assert_eq!(message.values.len(), 32);
        assert_eq!(message.values[31], 7);
    }

    #[test]
    fn projection_shape_rejects_truncated_provider_data() {
        let projection = Projection {
            block: BlockMeta {
                number: 1,
                hash: B256::with_last_byte(1),
                parent_hash: B256::ZERO,
                timestamp: 2,
            },
            watch_set: watch_set(),
            values: Bytes::new(),
            changed_bitmap: Bytes::new(),
        };

        assert!(validate_projection_shape(&projection).is_err());
    }

    #[test]
    fn snapshot_requires_a_coherent_forkchoice_head() {
        let projection = Projection {
            block: BlockMeta {
                number: 1,
                hash: B256::with_last_byte(1),
                parent_hash: B256::ZERO,
                timestamp: 1,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::ZERO]),
            changed_bitmap: Bytes::new(),
        };
        let snapshot = CanonicalSnapshot {
            forkchoice: ForkchoiceMeta {
                head: CheckpointMeta {
                    number: 2,
                    hash: B256::with_last_byte(2),
                },
                safe: None,
                finalized: None,
            },
            projection,
            anchored_at: Instant::now(),
        };

        assert!(validate_snapshot_view(&snapshot).is_err());
    }

    #[test]
    fn projection_dictionary_rejects_a_snapshot_source_contract_violation() {
        let expected = watch_set();
        let wrong = Arc::new(WatchSet::compile(
            expected.generation(),
            &[WatchConfig {
                id: "other".into(),
                address: alloy_primitives::Address::with_last_byte(1),
                slot: B256::with_last_byte(1),
            }],
        ));
        let projection = Projection {
            block: BlockMeta {
                number: 1,
                hash: B256::with_last_byte(1),
                parent_hash: B256::ZERO,
                timestamp: 2,
            },
            watch_set: wrong,
            values: encode_values(&[U256::ZERO]),
            changed_bitmap: Bytes::new(),
        };

        assert!(validate_projection_dictionary(&projection, &expected).is_err());
    }

    #[test]
    fn canonical_bitmap_is_relative_to_the_previous_head() {
        let watch_set = watch_set();
        let previous = Projection {
            block: BlockMeta {
                number: 10,
                hash: B256::with_last_byte(10),
                parent_hash: B256::with_last_byte(9),
                timestamp: 10,
            },
            watch_set: Arc::clone(&watch_set),
            values: encode_values(&[U256::from(1)]),
            changed_bitmap: Bytes::new(),
        };
        let mut next = Projection {
            block: BlockMeta {
                number: 10,
                hash: B256::repeat_byte(10),
                parent_hash: B256::repeat_byte(9),
                timestamp: 11,
            },
            watch_set,
            values: encode_values(&[U256::from(2)]),
            // This is the candidate's delta from its fork parent, not from `previous`.
            changed_bitmap: vec![0].into(),
        };

        let bitmap = canonical_changed_bitmap(&previous, &next).unwrap();
        assert_eq!(bitmap.as_ref(), &[1]);

        next.block.parent_hash = previous.block.hash;
        next.changed_bitmap = vec![1].into();
        assert_eq!(
            canonical_changed_bitmap(&previous, &next).unwrap(),
            next.changed_bitmap
        );

        next.changed_bitmap = Bytes::new();
        assert_eq!(
            canonical_changed_bitmap(&previous, &next).unwrap().as_ref(),
            &[1]
        );
    }

    #[test]
    fn sibling_candidates_inherit_independent_parent_state() {
        let initial = Arc::new(Projection {
            block: BlockMeta {
                number: 10,
                hash: B256::with_last_byte(10),
                parent_hash: B256::with_last_byte(9),
                timestamp: 10,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::from(1)]),
            changed_bitmap: Vec::new().into(),
        });
        let (frames, _) = broadcast::channel(16);
        let shared = Arc::new(Shared {
            boot_id: Bytes::from_static(&[0; 16]),
            chain_id: 1,
            genesis_hash: B256::ZERO,
            started_at: Instant::now(),
            sequence: AtomicU64::new(0),
            published_sequence: AtomicU64::new(0),
            max_frame_bytes: 4096,
            capabilities: CAP_FULL_PROJECTIONS | CAP_VALIDATED | CAP_CANONICAL | CAP_REJECTED,
            published: ArcSwap::from_pointee(PublishedState {
                canonical: Arc::clone(&initial),
                forkchoice: Some(forkchoice_for(initial.block)),
                effective_sequence: 0,
                recovering: false,
            }),
            frames,
            metrics: PublisherMetrics::new(),
        });
        let (producer, _rx) = FeedProducer::channel(watch_set(), 16);
        let source = Arc::new(MockSource {
            projection: initial.as_ref().clone(),
        });
        let mut publisher = Publisher::new(
            producer,
            source,
            Arc::clone(&shared),
            Arc::clone(&initial),
            Instant::now(),
            16,
        );

        let first = B256::with_last_byte(11);
        publisher
            .validated(
                1,
                BlockMeta {
                    number: 11,
                    hash: first,
                    parent_hash: initial.block.hash,
                    timestamp: 11,
                },
                &[crate::watch::SlotChange {
                    key_id: 0,
                    new_value: U256::from(2),
                }],
            )
            .unwrap();

        let sibling = B256::repeat_byte(11);
        publisher
            .validated(
                1,
                BlockMeta {
                    number: 11,
                    hash: sibling,
                    parent_hash: initial.block.hash,
                    timestamp: 11,
                },
                &[],
            )
            .unwrap();

        assert_eq!(
            value_at(
                &publisher.candidates[&first]
                    .projection
                    .as_ref()
                    .unwrap()
                    .values,
                0,
            ),
            U256::from(2)
        );
        assert_eq!(
            value_at(
                &publisher.candidates[&sibling]
                    .projection
                    .as_ref()
                    .unwrap()
                    .values,
                0,
            ),
            U256::from(1)
        );

        let first_child = B256::with_last_byte(12);
        publisher
            .validated(
                1,
                BlockMeta {
                    number: 12,
                    hash: first_child,
                    parent_hash: first,
                    timestamp: 12,
                },
                &[crate::watch::SlotChange {
                    key_id: 0,
                    new_value: U256::from(3),
                }],
            )
            .unwrap();
        let sibling_child = B256::repeat_byte(12);
        publisher
            .validated(
                1,
                BlockMeta {
                    number: 12,
                    hash: sibling_child,
                    parent_hash: sibling,
                    timestamp: 12,
                },
                &[crate::watch::SlotChange {
                    key_id: 0,
                    new_value: U256::from(4),
                }],
            )
            .unwrap();

        publisher
            .canonical(1, Instant::now(), Some(12), first_child)
            .unwrap();
        assert_eq!(
            value_at(&shared.published.load().canonical.values, 0),
            U256::from(3)
        );
        publisher
            .canonical(1, Instant::now(), Some(12), sibling_child)
            .unwrap();
        assert_eq!(publisher.canonical_hash, sibling_child);
        assert_eq!(
            value_at(&shared.published.load().canonical.values, 0),
            U256::from(4)
        );

        let sequence = shared.published_sequence.load(Ordering::Acquire);
        publisher
            .canonical(1, Instant::now(), Some(12), sibling_child)
            .unwrap();
        assert_eq!(
            shared.published_sequence.load(Ordering::Acquire),
            sequence,
            "a reaffirmed forkchoice head must not produce another transition"
        );

        publisher
            .canonical(1, Instant::now(), Some(10), initial.block.hash)
            .unwrap();
        assert_eq!(
            publisher.canonical_hash, initial.block.hash,
            "a legitimate forkchoice reversion to a shorter head must be accepted"
        );
    }

    #[test]
    fn validated_cache_miss_reads_the_parent_without_a_global_gap() {
        let initial = Arc::new(Projection {
            block: BlockMeta {
                number: 10,
                hash: B256::with_last_byte(10),
                parent_hash: B256::with_last_byte(9),
                timestamp: 10,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::from(1)]),
            changed_bitmap: Bytes::new(),
        });
        let candidate = BlockMeta {
            number: 12,
            hash: B256::with_last_byte(12),
            parent_hash: B256::with_last_byte(11),
            timestamp: 12,
        };
        let source_projection = Projection {
            block: BlockMeta {
                number: 11,
                hash: candidate.parent_hash,
                parent_hash: initial.block.hash,
                timestamp: 11,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::from(7)]),
            changed_bitmap: Bytes::new(),
        };
        let (frames, _) = broadcast::channel(16);
        let shared = Arc::new(Shared {
            boot_id: Bytes::from_static(&[0; 16]),
            chain_id: 1,
            genesis_hash: B256::ZERO,
            started_at: Instant::now(),
            sequence: AtomicU64::new(0),
            published_sequence: AtomicU64::new(0),
            max_frame_bytes: 4096,
            capabilities: CAP_FULL_PROJECTIONS | CAP_VALIDATED | CAP_CANONICAL | CAP_REJECTED,
            published: ArcSwap::from_pointee(PublishedState {
                canonical: Arc::clone(&initial),
                forkchoice: Some(forkchoice_for(initial.block)),
                effective_sequence: 0,
                recovering: false,
            }),
            frames,
            metrics: PublisherMetrics::new(),
        });
        let (producer, _rx) = FeedProducer::channel(watch_set(), 16);
        let mut publisher = Publisher::new(
            producer,
            Arc::new(MockSource {
                projection: source_projection,
            }),
            Arc::clone(&shared),
            initial,
            Instant::now(),
            16,
        );

        publisher.validated(1, candidate, &[]).unwrap();

        assert_eq!(
            value_at(
                &publisher.candidates[&candidate.hash]
                    .projection
                    .as_ref()
                    .unwrap()
                    .values,
                0,
            ),
            U256::from(7)
        );
        assert_eq!(shared.published_sequence.load(Ordering::Acquire), 1);
    }

    #[test]
    fn validated_candidate_must_directly_extend_its_cached_parent() {
        let initial = Arc::new(Projection {
            block: BlockMeta {
                number: 10,
                hash: B256::with_last_byte(10),
                parent_hash: B256::with_last_byte(9),
                timestamp: 10,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::from(1)]),
            changed_bitmap: Bytes::new(),
        });
        let (frames, _) = broadcast::channel(16);
        let shared = Arc::new(Shared {
            boot_id: Bytes::from_static(&[0; 16]),
            chain_id: 1,
            genesis_hash: B256::ZERO,
            started_at: Instant::now(),
            sequence: AtomicU64::new(0),
            published_sequence: AtomicU64::new(0),
            max_frame_bytes: 4096,
            capabilities: CAP_FULL_PROJECTIONS | CAP_VALIDATED | CAP_CANONICAL | CAP_REJECTED,
            published: ArcSwap::from_pointee(PublishedState {
                canonical: Arc::clone(&initial),
                forkchoice: Some(forkchoice_for(initial.block)),
                effective_sequence: 0,
                recovering: false,
            }),
            frames,
            metrics: PublisherMetrics::new(),
        });
        let (producer, _rx) = FeedProducer::channel(watch_set(), 16);
        let mut publisher = Publisher::new(
            producer,
            Arc::new(MockSource {
                projection: initial.as_ref().clone(),
            }),
            shared,
            Arc::clone(&initial),
            Instant::now(),
            16,
        );

        let error = publisher
            .validated(
                1,
                BlockMeta {
                    number: 12,
                    hash: B256::with_last_byte(12),
                    parent_hash: initial.block.hash,
                    timestamp: 12,
                },
                &[],
            )
            .unwrap_err();

        assert!(error.to_string().contains("invalid projection ancestry"));
    }

    #[test]
    fn snapshot_anchor_filters_old_callbacks_but_replays_later_previous_generation() {
        let initial = Arc::new(Projection {
            block: BlockMeta {
                number: 10,
                hash: B256::with_last_byte(10),
                parent_hash: B256::with_last_byte(9),
                timestamp: 10,
            },
            watch_set: watch_set(),
            values: encode_values(&[U256::from(1)]),
            changed_bitmap: Vec::new().into(),
        });
        let (frames, _) = broadcast::channel(16);
        let shared = Arc::new(Shared {
            boot_id: Bytes::from_static(&[0; 16]),
            chain_id: 1,
            genesis_hash: B256::ZERO,
            started_at: Instant::now(),
            sequence: AtomicU64::new(0),
            published_sequence: AtomicU64::new(0),
            max_frame_bytes: 4096,
            capabilities: CAP_FULL_PROJECTIONS | CAP_VALIDATED | CAP_CANONICAL | CAP_REJECTED,
            published: ArcSwap::from_pointee(PublishedState {
                canonical: Arc::clone(&initial),
                forkchoice: Some(forkchoice_for(initial.block)),
                effective_sequence: 0,
                recovering: false,
            }),
            frames,
            metrics: PublisherMetrics::new(),
        });
        let (producer, _rx) = FeedProducer::channel(watch_set(), 16);
        let source = Arc::new(MockSource {
            projection: initial.as_ref().clone(),
        });
        let snapshot_anchored_at = Instant::now();
        let mut publisher = Publisher::new(
            producer,
            source,
            shared,
            Arc::clone(&initial),
            snapshot_anchored_at,
            16,
        );

        publisher
            .canonical(
                0,
                snapshot_anchored_at
                    .checked_sub(Duration::from_millis(1))
                    .unwrap(),
                Some(11),
                B256::with_last_byte(11),
            )
            .unwrap();
        assert_eq!(publisher.canonical_hash, initial.block.hash);

        let reorg = B256::repeat_byte(11);
        publisher
            .canonical(0, Instant::now(), Some(10), reorg)
            .unwrap();
        assert_eq!(
            publisher.canonical_hash, reorg,
            "a transition observed during reload must survive the generation switch"
        );
    }

    #[tokio::test]
    async fn unix_consumer_receives_handshake_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("statefeed.sock");
        let config_path = directory.path().join("statefeed.toml");
        fs::write(&config_path, "# watched only for reload notifications\n").unwrap();
        let stream_config = StreamConfig {
            publish_executed: false,
            socket: socket.clone(),
            socket_mode: 0o660,
            queue_capacity: 16,
            candidate_cache_blocks: 8,
            candidate_metadata_entries: 64,
            candidate_retention: Duration::from_secs(120),
            retirement_work_budget: 16,
            consumer_buffer: 16,
            max_consumers: 4,
            max_frame_bytes: 4096,
            publisher_cpu: None,
            publisher_spin_us: 0,
        };
        let watch_set = watch_set();
        let initial = Projection {
            block: BlockMeta {
                number: 1,
                hash: B256::with_last_byte(1),
                parent_hash: B256::ZERO,
                timestamp: 1,
            },
            watch_set: Arc::clone(&watch_set),
            values: encode_values(&[U256::from(9)]),
            changed_bitmap: Vec::new().into(),
        };
        let source = Arc::new(MockSource {
            projection: initial,
        });
        let (producer, receiver) = FeedProducer::channel(Arc::clone(&watch_set), 16);
        let service = start_service(
            ServiceOptions {
                config_path,
                stream: stream_config,
                chain_id: 1,
                genesis_hash: B256::repeat_byte(7),
            },
            receiver,
            producer,
            source,
            watch_set,
        )
        .await
        .unwrap();

        let mut client = UnixStream::connect(&socket).await.unwrap();
        let hello = read_frame(&mut client).await;
        let config = read_frame(&mut client).await;
        let snapshot = read_frame(&mut client).await;
        let Some(envelope::Event::Hello(hello)) = hello.event else {
            panic!("expected hello")
        };
        assert_eq!(hello.capabilities & CAP_EXECUTED, 0);
        assert_ne!(hello.capabilities & CAP_FORKCHOICE_APPLIED, 0);
        assert_ne!(hello.capabilities & CAP_CANDIDATE_RETIREMENT, 0);
        assert!(matches!(
            config.event,
            Some(envelope::Event::ConfigActivated(_))
        ));
        let Some(envelope::Event::Snapshot(snapshot)) = snapshot.event else {
            panic!("expected snapshot")
        };
        assert!(snapshot.forkchoice.is_some());

        drop(client);
        service.shutdown().await;
        assert!(!socket.exists());
    }

    #[tokio::test]
    async fn socket_binding_refuses_to_replace_a_live_listener() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("statefeed.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();

        let error = bind_socket(&socket, 0o660).unwrap_err();
        assert!(error.to_string().contains("active statefeed socket"));
        assert!(socket.exists());

        drop(listener);
    }

    #[tokio::test]
    async fn socket_binding_replaces_a_stale_listener() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("statefeed.sock");
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());

        let (listener, cleanup) = bind_socket(&socket, 0o660).unwrap();
        assert!(socket.exists());
        drop(listener);
        drop(cleanup);
        assert!(!socket.exists());
    }

    #[tokio::test]
    async fn socket_cleanup_does_not_remove_a_replacement_path() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("statefeed.sock");
        let (listener, cleanup) = bind_socket(&socket, 0o660).unwrap();

        fs::remove_file(&socket).unwrap();
        fs::write(&socket, b"replacement").unwrap();
        drop(cleanup);

        assert_eq!(fs::read(&socket).unwrap(), b"replacement");
        drop(listener);
    }

    async fn read_frame(stream: &mut UnixStream) -> Envelope {
        let mut length = [0u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut payload = vec![0u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut payload).await.unwrap();
        Envelope::decode(payload.as_slice()).unwrap()
    }

    fn value_at(values: &[u8], key_id: usize) -> U256 {
        U256::from_be_slice(&values[key_id * 32..(key_id + 1) * 32])
    }
}
