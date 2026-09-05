#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

// Required for `override_allocator_on_supported_platforms`.
#[cfg(all(feature = "jemalloc", unix))]
use reth_cli_util::allocator::tikv_jemalloc_sys as _;

use std::{path::PathBuf, sync::Arc};

use clap::{Args, Parser};
use eyre::{Result, eyre};
use reth::{
    builder::rpc::{BasicEngineApiBuilder, Identity, RpcAddOns},
    chainspec::EthereumChainSpecParser,
    cli::Cli,
};
use reth_node_ethereum::{
    EthereumAddOns, EthereumEngineValidatorBuilder, EthereumEthApiBuilder, EthereumNode,
};
use reth_statefeed::{
    config::Config,
    feed::FeedProducer,
    publisher::{ServiceOptions, start_service},
    reth_integration::{
        AppliedForkchoiceTracker, RethSnapshotSource, StatefeedEngineValidatorBuilder,
    },
    watch::WatchSet,
};
use tracing::info;

#[derive(Args, Clone, Debug)]
struct StatefeedArgs {
    /// TOML file containing the local stream and watched storage keys.
    #[arg(
        long = "statefeed.config",
        env = "RETH_STATEFEED_CONFIG",
        value_name = "PATH"
    )]
    statefeed_config: Option<PathBuf>,
}

fn main() {
    #[cfg(feature = "jit")]
    {
        match reth_node_ethereum::node::maybe_run_jit_helper() {
            Ok(std::ops::ControlFlow::Break(())) => return,
            Ok(std::ops::ControlFlow::Continue(())) => {}
            Err(err) => {
                eprintln!("Error: {err:?}");
                std::process::exit(1);
            }
        }
    }

    reth_cli_util::sigsegv_handler::install();

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: this runs before the process starts any worker threads.
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) = Cli::<EthereumChainSpecParser, StatefeedArgs>::parse().run(run_node) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

async fn run_node(
    builder: reth::builder::WithLaunchContext<
        reth::builder::NodeBuilder<reth_db::DatabaseEnv, reth::chainspec::ChainSpec>,
    >,
    args: StatefeedArgs,
) -> Result<()> {
    let config_path = args
        .statefeed_config
        .ok_or_else(|| eyre!("--statefeed.config is required when launching the node"))?;
    let config = Config::load(&config_path)?;
    let watch_set = Arc::new(WatchSet::compile(1, &config.watch));
    let (producer, receiver) =
        FeedProducer::channel(Arc::clone(&watch_set), config.stream.queue_capacity);
    let forkchoice_tracker = AppliedForkchoiceTracker::default();

    let chain_spec = Arc::clone(&builder.config().chain);
    let chain_id = chain_spec.chain().id();
    let genesis_hash = chain_spec.genesis_hash();
    let validator_builder = StatefeedEngineValidatorBuilder::new(
        producer.clone(),
        forkchoice_tracker.clone(),
        config.stream.publish_executed,
    );
    let add_ons = EthereumAddOns::new(RpcAddOns::new(
        EthereumEthApiBuilder::<alloy_network::Ethereum>::default(),
        EthereumEngineValidatorBuilder::default(),
        BasicEngineApiBuilder::<EthereumEngineValidatorBuilder>::default(),
        validator_builder,
        Identity::new(),
        Identity::new(),
    ));

    info!(
        target: "statefeed",
        config = %config_path.display(),
        watched_keys = watch_set.len(),
        "launching reth-statefeed"
    );
    let handle = builder
        .with_types::<EthereumNode>()
        .with_components(EthereumNode::components())
        .with_add_ons(add_ons)
        .launch_with_debug_capabilities()
        .await?;

    let snapshot_source = Arc::new(RethSnapshotSource::new(
        handle.node.provider.clone(),
        forkchoice_tracker,
    ));
    let service = start_service(
        ServiceOptions {
            config_path,
            stream: config.stream,
            chain_id,
            genesis_hash,
        },
        receiver,
        producer,
        snapshot_source,
        watch_set,
    )
    .await?;

    let result = handle.wait_for_node_exit().await;
    service.shutdown().await;
    result
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn extension_arguments_do_not_collide_with_reth_arguments() {
        Cli::<EthereumChainSpecParser, StatefeedArgs>::command().debug_assert();
    }
}
