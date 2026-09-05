# reth-statefeed

`reth-statefeed` is an Ethereum Reth binary that publishes a configurable low-latency projection
of selected physical storage slots to local consumers. It is protocol-agnostic: addresses and
already-derived 32-byte storage keys live in TOML, while PSM/GSM formulas and routing decisions
remain downstream.

The node uses the isolated fork commit `6d512bdb2bdb442228201c7bb7765009bcd8299e`, a two-commit
patch series on top of the exact upstream Reth `v2.5.2` commit
`5a6940e351fed80458fe6c9da8581cbe4b8bd036`.
It uses the stock Reth datadir and CLI; no slots are compiled into the binary.

## Current lifecycle

The implemented path publishes:

- an anchored canonical snapshot on startup, reconnect, recovery, and config reload;
- optionally, a full self-contained projection immediately after `EXECUTED`, followed by a
  hash-only `BlockValidated` promotion or `REJECTED` invalidation;
- a full `VALIDATED` projection when the early observer is disabled or a Reth insertion path does
  not pass through it;
- a full self-contained projection on every `CANONICAL` transition;
- a machine-readable `REJECTED` notification for invalid blocks/payloads;
- explicit `Gap` followed by a fresh snapshot after engine-queue loss;
- atomic watch-set generations on file changes or `SIGHUP`.

Set `stream.publish_executed = true` at startup to advertise `CAP_EXECUTED` and enable the earliest
pre-state-root stream. It defaults to `false` for a controlled rollout. The Reth fork contains only
a generic execution-observer hook and complete canonical-head hook coverage; it has no knowledge of
slots, statefeed, protobuf, or transport.

## Latency design

The execution observer only loads an immutable watch set, filters `BundleState`, and performs a
non-blocking bounded `try_send`. It performs no DB/RPC reads, protobuf work, socket I/O, `await`, or
consumer-dependent blocking.

Watched changes are extracted exactly once. Successful validation emits only a block hash, so the
large execution bundle is neither cloned nor scanned again.

If an early candidate's parent has already left the bounded projection cache, the publisher keeps
only its compact watched delta and does no provider I/O on the early path. A rare exact-parent
provider fallback happens only after validation, producing a complete `VALIDATED` projection
without racing the child's insertion into the engine tree.

The publisher runs on its own OS thread. It can be pinned to a reserved logical CPU and optionally
busy-spin for a short interval before parking. Watched lookups scan the smaller of the account's
storage diff and its configured slots. Candidate projections are kept as one packed buffer and
protobuf serialization is done once per event; all consumers share that encoded frame.

## Build

Rust is pinned by `rust-toolchain.toml`.

```shell
cargo build --locked --profile maxperf --bin reth-statefeed
```

For development:

```shell
cargo check --locked --all-targets
cargo test --locked --all-targets
```

## Configure

Copy [`config.example.toml`](config.example.toml) and replace its address/slot entries. A Solidity
mapping entry must be supplied as the final `keccak256(abi.encode(key, base_slot))` coordinate.

```toml
[stream]
publish_executed = false
socket = "/run/reth-statefeed/statefeed.sock"
socket_mode = 0o660
queue_capacity = 8192
candidate_cache_blocks = 128
consumer_buffer = 256
max_consumers = 64
max_frame_bytes = 4194304

# Optional: use only a CPU reserved for this work.
# publisher_cpu = 3
publisher_spin_us = 0

[[watch]]
id = "psm.balance"
address = "0x0000000000000000000000000000000000000000"
slot = "0x0000000000000000000000000000000000000000000000000000000000000000"
```

The socket defaults to mode `0660`. Its parent directory is created when missing. Configure
`socket_mode` and the process user/group so only the intended local consumers can connect.

Buffer counts are hard bounds, but their byte cost depends on the watch set. Size them together:
roughly `candidate_cache_blocks * projection_bytes + consumer_buffer * max_frame_bytes`, plus up to
`queue_capacity` sparse deltas. Keep `max_frame_bytes` close to the validated frame requirement for
the configured dictionary instead of treating the 4 MiB default as a target allocation.

## Run and migrate from stock Reth

Stop the existing Reth process first, then launch this binary with the same chain, datadir, ports,
and other node arguments:

```shell
./target/maxperf/reth-statefeed node \
  --statefeed.config /etc/reth-statefeed/statefeed.toml \
  --datadir /var/lib/reth \
  --metrics 127.0.0.1:9001
```

Do not run stock Reth and `reth-statefeed` concurrently against one datadir. Normal Reth database
migrations still apply when moving to 2.5.2, so keep the same rollback/backup procedure used for an
upstream Reth upgrade.

The watched set reloads after an atomic file save or:

```shell
kill -HUP <reth-statefeed-pid>
```

Stream settings are intentionally immutable during reload; restart to change buffers, socket path,
CPU affinity, spin duration, or `publish_executed`. A rejected config leaves the current generation
active. Rewriting an identical watch dictionary is a no-op and does not allocate a generation or
read a new snapshot. Queue pressure and transient snapshot-provider failures are retried with the
latest coalesced request, so a valid reload is not silently lost.

## Consume

The canonical schema is [`proto/statefeed/v1/statefeed.proto`](proto/statefeed/v1/statefeed.proto).
Messages use a four-byte big-endian length prefix followed by protobuf. Every projection stores
exactly `32 * key_count` big-endian bytes; key `i` occupies `values[i*32..(i+1)*32]`.

A new connection receives:

```text
Hello -> ConfigActivated(dictionary) -> Snapshot -> live events
```

The included reader validates framing, handshake/live sequence ordering, process identity, protocol
generation, enum values, fixed hash/address/slot widths, dense key IDs, bitmaps, and projection
length. Its candidate ancestry cache is bounded and configurable with
`--candidate-cache-blocks`:

```shell
cargo run --locked --bin statefeed-dump -- \
  --socket /run/reth-statefeed/statefeed.sock
```

Consumers must discard state on a changed `boot_id`, never mix generations or block hashes, and
stop treating old state as authoritative after `Gap` until the following `Snapshot` arrives.
Connections accepted while recovery is between those two events are closed and should reconnect;
this prevents a stale pre-gap snapshot from being presented as authoritative. Concurrent consumers
are bounded by `stream.max_consumers`. Speculative candidates also need a downstream TTL: bounded
publisher-cache eviction is not a protocol rejection and therefore emits no terminal event.

## Metrics and benchmarks

Metrics are registered in Reth's normal recorder and become available through its `--metrics`
endpoint. They cover extraction, enqueue, projection building, snapshots, protobuf encoding, UDS
writes, end-to-end internal latency, queue depth/overflow, candidate count, consumers, gaps, frame
size, and active generation. The exact metric catalog is in
[`docs/METRICS.md`](docs/METRICS.md).

Run the microbenchmarks with:

```shell
cargo bench --locked --bench hot_path
```

They cover 10, 100, 1,000, and 10,000 watched keys with untouched, sparse, all-changed, and
distributed-address bundles, plus an extraction/enqueue/dequeue round trip. Treat local results as
a single-thread regression signal, not production queue latency; final p99 targets must be measured
on the production-like Linux host. Recorded baselines are in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).
