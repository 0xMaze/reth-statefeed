use std::{hint::black_box, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256, U256};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use reth::revm::revm::{
    database::{
        BundleState,
        states::{AccountStatus, BundleAccount, StorageSlot, StorageWithOriginalValues},
    },
    state::AccountInfo,
};
use reth_statefeed::{
    config::WatchConfig,
    feed::{BlockMeta, CheckpointMeta, FeedProducer, ForkchoiceMeta, extract_changes},
    watch::WatchSet,
};

const ADDRESS: Address = Address::repeat_byte(0x11);

fn watch_set(count: usize) -> Arc<WatchSet> {
    let watch = (0..count)
        .map(|index| WatchConfig {
            id: format!("key.{index}"),
            address: ADDRESS,
            slot: slot(index),
        })
        .collect::<Vec<_>>();
    Arc::new(WatchSet::compile(1, &watch))
}

fn distributed_watch_set(count: usize) -> Arc<WatchSet> {
    let watch = (0..count)
        .map(|index| WatchConfig {
            id: format!("contract.{index}"),
            address: address(index),
            slot: B256::ZERO,
        })
        .collect::<Vec<_>>();
    Arc::new(WatchSet::compile(1, &watch))
}

fn bundle(changed: usize) -> BundleState {
    if changed == 0 {
        return BundleState::default();
    }

    let mut storage = StorageWithOriginalValues::default();
    for index in 0..changed {
        storage.insert(
            U256::from(index),
            StorageSlot::new_changed(U256::ZERO, U256::from(index + 1)),
        );
    }
    let mut bundle = BundleState::default();
    bundle.state.insert(
        ADDRESS,
        BundleAccount::new(
            Some(AccountInfo::default()),
            Some(AccountInfo::default()),
            storage,
            AccountStatus::Changed,
        ),
    );
    bundle
}

fn slot(index: usize) -> B256 {
    B256::from(U256::from(index).to_be_bytes::<32>())
}

fn address(index: usize) -> Address {
    let value = U256::from(index.saturating_add(1)).to_be_bytes::<32>();
    Address::from_slice(&value[12..])
}

fn distributed_bundle(changed: usize) -> BundleState {
    let mut bundle = BundleState::default();
    for index in 0..changed {
        let mut storage = StorageWithOriginalValues::default();
        storage.insert(
            U256::ZERO,
            StorageSlot::new_changed(U256::ZERO, U256::from(index + 1)),
        );
        bundle.state.insert(
            address(index),
            BundleAccount::new(
                Some(AccountInfo::default()),
                Some(AccountInfo::default()),
                storage,
                AccountStatus::Changed,
            ),
        );
    }
    bundle
}

fn extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_changes");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    for count in [10, 100, 1_000, 10_000] {
        let watch_set = watch_set(count);
        for (case, changed) in [("untouched", 0), ("sparse", 4), ("all", count)] {
            let bundle = bundle(changed);
            group.bench_with_input(
                BenchmarkId::new(case, count),
                &(Arc::clone(&watch_set), bundle),
                |b, (watch_set, bundle)| {
                    b.iter(|| black_box(extract_changes(watch_set, black_box(bundle))))
                },
            );
        }
    }
    group.finish();
}

fn distributed_address_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_changes_distributed_addresses");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    for count in [10, 100, 1_000, 10_000] {
        let watch_set = distributed_watch_set(count);
        let bundle = distributed_bundle(4);
        group.bench_with_input(
            BenchmarkId::new("four_touched", count),
            &(watch_set, bundle),
            |b, (watch_set, bundle)| {
                b.iter(|| black_box(extract_changes(watch_set, black_box(bundle))))
            },
        );
    }
    group.finish();
}

fn extraction_enqueue_round_trip(c: &mut Criterion) {
    let watch_set = watch_set(1_000);
    let bundle = bundle(4);
    let (producer, receiver) = FeedProducer::channel(Arc::clone(&watch_set), 16);
    let block = BlockMeta {
        number: 1,
        hash: B256::with_last_byte(1),
        parent_hash: B256::ZERO,
        timestamp: 1,
    };

    c.bench_function("extract_enqueue_dequeue/1000_sparse", |b| {
        b.iter(|| {
            producer.publish_executed(block, black_box(&bundle));
            black_box(receiver.try_recv().unwrap());
        })
    });
}

fn forkchoice_enqueue_round_trip(c: &mut Criterion) {
    let (producer, receiver) = FeedProducer::channel(watch_set(1), 16);
    let view = ForkchoiceMeta {
        head: CheckpointMeta {
            number: 1,
            hash: B256::with_last_byte(1),
        },
        safe: None,
        finalized: None,
    };

    c.bench_function("forkchoice_enqueue_dequeue", |b| {
        b.iter(|| {
            producer.publish_forkchoice_applied(black_box(view));
            black_box(receiver.try_recv().unwrap());
        })
    });
}

criterion_group!(
    benches,
    extraction,
    distributed_address_extraction,
    extraction_enqueue_round_trip,
    forkchoice_enqueue_round_trip
);
criterion_main!(benches);
