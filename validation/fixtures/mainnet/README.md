# Mainnet historical fixtures

Each JSON file contains the complete ordered production watch dictionary, its values at a pinned
block, and quote results returned by the deployed contracts for fixed probe amounts. The fixtures
are committed so the pure evaluator regressions do not need RPC access.

Regenerate a fixture from `validation/` with an archive endpoint:

```sh
MAINNET_RPC_URL=ws://your-archive-node:8546 \
FIXTURE_BLOCK=25912757 \
FIXTURE_BLOCK_HASH=0xfa331c6c11df54016c7c0ddf48aeb9c61e7ff07e5170da485cd858096e490b6d \
FIXTURE_TIMESTAMP=1788630887 \
FIXTURE_PATH=fixtures/mainnet/25912757.json \
forge script script/GenerateHistoricalFixture.s.sol:GenerateHistoricalFixture
```

The generator validates chain id, block hash, and timestamp before writing. Generated changes must
be reviewed together with `historical-cases.toml` and the deployment guards.

The `*-real-swap-prestate.json` fixture is intentionally separate. A block snapshot is post-state,
while quote reproduction for a transaction must use its transaction-start state. Its three storage
words come from `debug_traceTransaction` with `prestateTracer`; the fixture also pins the successful
receipt's block hash and transaction index. `HistoricalFixturesTest` feeds those exact words into
the local integer model and requires the result to equal the emitted `SellAsset` amounts. The online
suite independently retrieves that log from the pinned block and transaction hash.
