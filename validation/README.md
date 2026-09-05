# Ethereum conversion-state validation

This Foundry project independently validates the Ethereum mainnet conversion watch manifest. It
forks one immutable block, compares local integer models with deployed contracts, records storage
reads, mutates watched fields, checks time-dependent Aave math, and verifies fail-closed proxy
guards.

The suite covers six independent failure modes:

1. differential quote and real balance-delta checks against deployed contracts;
2. bidirectional storage read-set completeness against the production TOML manifest;
3. watched-slot mutation checks, including explicit proofs for deliberately ignored Aave reads;
4. consensus-timestamp changes with otherwise identical Aave storage;
5. fail-closed behavior for unknown proxy implementations and deployment dependencies.
6. historical regressions at pinned block hashes, including upgrades, fees, capacity exhaustion,
   frozen state, and a real mainnet swap reconstructed from its transaction-start prestate.

USDD PSM capacity is additionally checked at the exact execution boundary derived from Vat and
GemJoin state: `capacity` succeeds while `capacity + 1` reverts.

Dependencies are managed by Soldeer and are intentionally isolated from the parent Rust crate.

```sh
forge soldeer install
MAINNET_RPC_URL=ws://your-archive-node:8546 forge test
```

The fast historical fixture suite is hermetic and does not use RPC:

```sh
forge test --match-contract HistoricalFixturesTest
```

The online historical suite re-checks the same cases against an archive node, verifies the pinned
hash and timestamp, records the deployed contracts' storage reads, exercises capacity boundaries,
and locates the real swap log by transaction hash:

```sh
MAINNET_RPC_URL=ws://your-archive-node:8546 \
  forge test --match-contract HistoricalOnlineTest
```

The current anchor is pinned in `test/ForkTestBase.t.sol`; historical cases are versioned in
`historical-cases.toml`, with their complete watched-state snapshots under `fixtures/mainnet/`.
Moving any anchor is an explicit registry update: review implementation guards, storage coverage,
and the generated fixture diff before changing it.
