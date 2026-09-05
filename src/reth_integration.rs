//! Reth 2.5.2 integration: validator decoration and anchored snapshot reads.

use std::{fmt, sync::Arc, time::Instant};

use alloy_primitives::B256;
use eyre::{Result, eyre};
use reth::{
    builder::{
        AddOnsContext, ConfigureEngineEvm, FullNodeComponents,
        rpc::{BasicEngineValidatorBuilder, EngineValidatorBuilder, PayloadValidatorBuilder},
    },
    payload::PayloadBuilderResources,
    primitives::{AlloyBlockHeader, EthPrimitives, NodePrimitives, SealedBlock},
    providers::{BlockExecutionOutput, HeaderProvider, ProviderResult, StateProviderFactory},
};
use reth_chain_state::ExecutedBlock;
use reth_engine_tree::tree::{
    CacheWaitDurations, EngineApiTreeState, EngineValidator, ExecutedBlockInfo, ExecutionObserver,
    TreeConfig, ValidationOutcome, WaitForCaches, error::InsertPayloadError,
    payload_validator::TreeCtx,
};
use reth_node_api::{
    BuiltPayloadExecutedBlock, InvalidPayloadAttributesError, NewPayloadError, PayloadTypes,
};
use reth_node_ethereum::{EthEngineTypes, EthereumEngineValidatorBuilder, EthereumNode};
use reth_storage_overlay::OverlayManager;

use crate::{
    feed::{BlockMeta, FeedProducer},
    publisher::{CanonicalSnapshot, Projection, SnapshotSource},
    watch::WatchSet,
};

type EthHeader = <EthPrimitives as NodePrimitives>::BlockHeader;
type EthBlock = <EthPrimitives as NodePrimitives>::Block;
type EthExecutionData = <EthEngineTypes as PayloadTypes>::ExecutionData;
type EthPayloadAttributes = <EthEngineTypes as PayloadTypes>::PayloadAttributes;

/// Prevents a continuously advancing head from monopolizing a snapshot worker.
const MAX_CANONICAL_SNAPSHOT_ATTEMPTS: usize = 4;

/// Builds the stock Ethereum engine validator and decorates it with statefeed publication.
#[derive(Clone)]
pub struct StatefeedEngineValidatorBuilder {
    inner: BasicEngineValidatorBuilder<EthereumEngineValidatorBuilder>,
    producer: FeedProducer,
    publish_executed: bool,
}

impl StatefeedEngineValidatorBuilder {
    /// Creates a builder around Reth's stock validator implementation.
    pub fn new(producer: FeedProducer, publish_executed: bool) -> Self {
        Self {
            inner: BasicEngineValidatorBuilder::default(),
            producer,
            publish_executed,
        }
    }
}

impl fmt::Debug for StatefeedEngineValidatorBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatefeedEngineValidatorBuilder")
            .field("inner", &self.inner)
            .field("publish_executed", &self.publish_executed)
            .finish_non_exhaustive()
    }
}

impl<N> EngineValidatorBuilder<N> for StatefeedEngineValidatorBuilder
where
    N: FullNodeComponents<Types = EthereumNode, Evm: ConfigureEngineEvm<EthExecutionData>>,
{
    type EngineValidator = StatefeedEngineValidator<
        reth_engine_tree::tree::BasicEngineValidator<
            N::Provider,
            N::Evm,
            <EthereumEngineValidatorBuilder as PayloadValidatorBuilder<N>>::Validator,
        >,
    >;

    async fn build_tree_validator(
        self,
        ctx: &AddOnsContext<'_, N>,
        tree_config: TreeConfig,
        overlay_manager: OverlayManager<EthPrimitives>,
    ) -> Result<Self::EngineValidator> {
        let Self {
            inner,
            producer,
            publish_executed,
        } = self;
        let mut validator = inner
            .build_tree_validator(ctx, tree_config, overlay_manager)
            .await?;
        if publish_executed {
            validator = validator.with_execution_observer(Arc::new(StatefeedExecutionObserver {
                producer: producer.clone(),
            }));
        }
        Ok(StatefeedEngineValidator {
            inner: validator,
            producer,
            publish_executed,
        })
    }
}

#[derive(Debug)]
struct StatefeedExecutionObserver {
    producer: FeedProducer,
}

impl ExecutionObserver<EthPrimitives> for StatefeedExecutionObserver {
    #[inline]
    fn on_executed(
        &self,
        info: ExecutedBlockInfo,
        output: &BlockExecutionOutput<<EthPrimitives as NodePrimitives>::Receipt>,
    ) {
        self.producer.publish_executed(
            BlockMeta {
                number: info.block.block.number,
                hash: info.block.block.hash,
                parent_hash: info.block.parent,
                timestamp: info.timestamp,
            },
            &output.state,
        );
    }
}

/// Transparent engine validator wrapper that publishes successful block execution output.
#[derive(Debug)]
pub struct StatefeedEngineValidator<V> {
    inner: V,
    producer: FeedProducer,
    publish_executed: bool,
}

impl<V> EngineValidator<EthEngineTypes, EthPrimitives> for StatefeedEngineValidator<V>
where
    V: EngineValidator<EthEngineTypes, EthPrimitives>,
{
    fn validate_payload_attributes_against_header(
        &self,
        attr: &EthPayloadAttributes,
        header: &EthHeader,
    ) -> Result<(), InvalidPayloadAttributesError> {
        self.inner
            .validate_payload_attributes_against_header(attr, header)
    }

    fn convert_payload_to_block(
        &self,
        payload: EthExecutionData,
    ) -> Result<SealedBlock<EthBlock>, NewPayloadError> {
        self.inner.convert_payload_to_block(payload)
    }

    fn validate_payload(
        &mut self,
        payload: EthExecutionData,
        ctx: TreeCtx<'_, EthPrimitives>,
    ) -> ValidationOutcome<EthPrimitives> {
        let block_hash = payload.block_hash();
        let outcome = self.inner.validate_payload(payload, ctx);
        match &outcome {
            Ok(output) => self.on_validated(&output.executed_block),
            Err(error) => self.publish_rejected_if_invalid(block_hash, error),
        }
        outcome
    }

    fn validate_block(
        &mut self,
        block: SealedBlock<EthBlock>,
        ctx: TreeCtx<'_, EthPrimitives>,
    ) -> ValidationOutcome<EthPrimitives> {
        let block_hash = block.hash();
        let outcome = self.inner.validate_block(block, ctx);
        match &outcome {
            Ok(output) => self.on_validated(&output.executed_block),
            Err(error) => self.publish_rejected_if_invalid(block_hash, error),
        }
        outcome
    }

    fn on_inserted_executed_block(
        &self,
        block: BuiltPayloadExecutedBlock<EthPrimitives>,
    ) -> ProviderResult<ExecutedBlock<EthPrimitives>> {
        let executed = self.inner.on_inserted_executed_block(block)?;
        // Built payload insertion bypasses `validate_block_with_state`, so it has no early
        // execution callback. Publish the complete validated projection as an explicit fallback.
        self.publish_validated_fallback(&executed);
        Ok(executed)
    }

    fn on_canonical_head_changed(&self, hash: B256, state: &EngineApiTreeState<EthPrimitives>) {
        // Tree state already reflects the new head. Publish before delegating because the stock
        // hook may start txpool prewarming and is unrelated to statefeed correctness.
        self.producer
            .publish_canonical(state.tree_state().canonical_block_number(), hash);
        self.inner.on_canonical_head_changed(hash, state);
    }

    fn payload_builder_resources(
        &self,
        parent_hash: B256,
        parent_header: &EthHeader,
        timestamp: u64,
        state: &mut EngineApiTreeState<EthPrimitives>,
    ) -> PayloadBuilderResources {
        self.inner
            .payload_builder_resources(parent_hash, parent_header, timestamp, state)
    }
}

impl<V> StatefeedEngineValidator<V> {
    #[inline]
    fn on_validated(&self, executed: &ExecutedBlock<EthPrimitives>) {
        if self.publish_executed {
            self.producer
                .publish_validated(executed.recovered_block().hash());
        } else {
            self.publish_validated_fallback(executed);
        }
    }

    #[inline]
    fn publish_validated_fallback(&self, executed: &ExecutedBlock<EthPrimitives>) {
        let block = executed.recovered_block();
        let header = block.header();
        self.producer.publish_validated_fallback(
            BlockMeta {
                number: header.number(),
                hash: block.hash(),
                parent_hash: header.parent_hash(),
                timestamp: header.timestamp(),
            },
            &executed.execution_output.state,
        );
    }

    #[inline]
    fn publish_rejected_if_invalid(&self, block_hash: B256, error: &InsertPayloadError<EthBlock>) {
        let reason = match error {
            // Payload conversion/sidecar checks happen before the execution observer. Their
            // claimed hash can collide with an already valid candidate, so they cannot be a
            // lifecycle rejection for anything statefeed previously emitted as EXECUTED.
            InsertPayloadError::Payload(_) => None,
            InsertPayloadError::Block(error) if error.kind().is_validation_error() => {
                Some("block_validation_failed")
            }
            // Provider and internal execution errors do not prove that the payload is invalid.
            InsertPayloadError::Block(_) => None,
        };
        if let Some(reason) = reason {
            self.producer.publish_rejected(block_hash, reason);
        }
    }
}

impl<V> WaitForCaches for StatefeedEngineValidator<V>
where
    V: WaitForCaches,
{
    fn wait_for_caches(&self) -> CacheWaitDurations {
        self.inner.wait_for_caches()
    }
}

/// Reads snapshots directly from a Reth provider, never from JSON-RPC.
#[derive(Clone, Debug)]
pub struct RethSnapshotSource<P> {
    provider: P,
}

impl<P> RethSnapshotSource<P> {
    /// Wraps the full node provider exposed after launch.
    pub const fn new(provider: P) -> Self {
        Self { provider }
    }
}

impl<P> SnapshotSource for RethSnapshotSource<P>
where
    P: StateProviderFactory + HeaderProvider<Header = EthHeader> + Clone + Send + Sync + 'static,
{
    fn load_latest(&self, watch_set: Arc<WatchSet>) -> Result<CanonicalSnapshot> {
        for attempt in 1..=MAX_CANONICAL_SNAPSHOT_ATTEMPTS {
            let selected = self.provider.chain_info()?;
            let projection = match self.load_at(Arc::clone(&watch_set), selected.best_hash) {
                Ok(projection) => projection,
                Err(error) => {
                    // A reorg can make the selected hash unavailable to BlockchainProvider while
                    // the projection is being read. Retry only when the canonical head confirms
                    // that this was a race; errors at a stable head remain actionable.
                    let confirmed = self.provider.chain_info()?;
                    if confirmed.best_hash != selected.best_hash
                        && attempt < MAX_CANONICAL_SNAPSHOT_ATTEMPTS
                    {
                        continue;
                    }
                    return Err(error);
                }
            };

            // Timestamp immediately before confirmation. A callback observed no later than this
            // instant is covered by the confirmed head; later callbacks are conservatively
            // replayed by the publisher after reload/recovery.
            let anchored_at = Instant::now();
            let confirmed = self.provider.chain_info()?;
            if confirmed.best_hash == selected.best_hash {
                return Ok(CanonicalSnapshot {
                    projection,
                    anchored_at,
                });
            }

            if attempt == MAX_CANONICAL_SNAPSHOT_ATTEMPTS {
                return Err(eyre!(
                    "canonical head kept changing while reading statefeed snapshot (last attempted {}, now {})",
                    selected.best_hash,
                    confirmed.best_hash
                ));
            }
        }

        unreachable!("canonical snapshot loop always returns")
    }

    fn load_at(&self, watch_set: Arc<WatchSet>, block_hash: B256) -> Result<Projection> {
        let header = self
            .provider
            .sealed_header_by_hash(block_hash)?
            .ok_or_else(|| eyre!("header not found for statefeed snapshot: {block_hash}"))?;
        let state = self.provider.state_by_block_hash(block_hash)?;
        let mut values = Vec::with_capacity(watch_set.len().saturating_mul(32));
        for key in watch_set.keys() {
            let value = state.storage(key.address, key.slot)?.unwrap_or_default();
            values.extend_from_slice(&value.to_be_bytes::<32>());
        }

        Ok(Projection {
            block: BlockMeta {
                number: header.number(),
                hash: block_hash,
                parent_hash: header.parent_hash(),
                timestamp: header.timestamp(),
            },
            watch_set,
            values: values.into(),
            changed_bitmap: Vec::new().into(),
        })
    }
}
