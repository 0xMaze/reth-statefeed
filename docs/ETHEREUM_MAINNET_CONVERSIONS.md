# Ethereum mainnet conversion registry

Status: first verified registry for the conversion-edge implementation  
Chain id: `1`  
Verification block: `25,912,757`  
Block hash: `0xfa331c6c11df54016c7c0ddf48aeb9c61e7ff07e5170da485cd858096e490b6d`  
Block timestamp: `2026-09-05T17:54:47Z`

The deploy-specific statefeed dictionary is
[`config.ethereum-mainnet-conversions.toml`](../config.ethereum-mainnet-conversions.toml).
It contains 87 unique physical coordinates, or 2,784 raw value bytes per complete projection.
The binary remains protocol-agnostic: contract interpretation and formulas belong to the consumer.

## Verification method

Every coordinate was checked against the anchored block using all three of:

1. official deployment registries and verified protocol source;
2. source-level storage layout and mapping-key derivation;
3. `debug_traceCall` with `prestateTracer`, plus direct `eth_getStorageAt`/getter comparisons.

The traced method sets were intentionally split into:

- **quote** — changes the integer amount returned by an edge;
- **capacity** — changes the largest executable amount;
- **guard** — can disable an otherwise valid quote or change its semantics.

Executor-owned balances, allowances, nonces, and address-specific blacklist entries are not global
edge state and are not part of this dictionary. They remain the responsibility of the inventory and
transaction layers.

## Tokens

| Symbol | Address | Decimals |
|---|---|---:|
| DAI | `0x6B175474E89094C44Da98b954EedeAC495271d0F` | 18 |
| USDS | `0xdC035D45d973E3EC169d2276DDab16f1e407384F` | 18 |
| USDC | `0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48` | 6 |
| USDT | `0xdAC17F958D2ee523a2206206994597C13D831ec7` | 6 |
| GHO | `0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f` | 18 |
| USDD | `0x4f8e5DE400DE08B164e7421b3EE387f461beCD1A` | 18 |

## Sky edges

### Deployments and entrypoints

| Component | Address | Direction / entrypoint |
|---|---|---|
| DaiUsds | `0x3225737a9Bbb6473CB4a45b7244ACa2BeFdB276A` | DAI → USDS: `daiToUsds(address,uint256)` |
| DaiUsds | same | USDS → DAI: `usdsToDai(address,uint256)` |
| LitePSM USDC-A | `0xf6e72Db5454dd049d0788e411b06CfAF16853042` | USDC → DAI: `sellGem(address,uint256)` |
| LitePSM USDC-A | same | DAI → USDC: `buyGem(address,uint256)` |
| USDS LitePSM wrapper | `0xA188EEC8F81263234dA3622A406892F3D630f98c` | USDC → USDS: `sellGem(address,uint256)` |
| USDS LitePSM wrapper | same | USDS → USDC: `buyGem(address,uint256)` |
| LitePSM pocket | `0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341` | holds USDC liquidity |
| DaiJoin | `0x9759A6aC90977b93B58547B4A71c78317F391A28` | legacy DAI adapter |
| UsdsJoin | `0x3C0f895007CA717Aa01c8693e59DF1e8C3777FEB` | USDS adapter |

DAI ↔ USDS has a static 1:1 amount function and no mutable quote parameter. The dictionary only
contains its small fail-closed guard set: the USDS implementation, token mint authorization,
adapter authorization, and the legacy DaiJoin `live` value used by USDS → DAI.

For LitePSM, with `scale = 10^12`:

```text
gross = gem_amount * scale
sell fee = floor(gross * tin / 10^18)
USDC -> DAI/USDS output = gross - sell fee

buy fee = floor(gross * tout / 10^18)
DAI/USDS input required for exact USDC output = gross + buy fee
```

`buyGem` is exact-output in USDC. An exact-input graph edge must invert the last expression using
integer-safe search or an algebraic estimate followed by correction; it must not use floating point.

The quote set is LitePSM slots `3` (`tin`) and `4` (`tout`). `HALTED = 2^256 - 1` disables the
corresponding direction. Capacity is:

- USDC → DAI/USDS: current DAI balance of LitePSM, measured against post-fee output;
- DAI/USDS → USDC: `min(USDC.balanceOf(pocket), USDC.allowance(pocket, LitePSM))`.

`buf` is deliberately excluded. It controls future keeper `fill()` behavior, not the amount that is
executable from current balances. A `fill()` changes the watched DAI cash slot directly.

At the verification block, `tin = 0`, `tout = 0`, LitePSM DAI cash was approximately 806.3 million
DAI, and pocket liquidity approximately 3.966 billion USDC.

Official sources: [Sky chainlog](https://chainlog.skyeco.com/api/mainnet/active.json),
[DaiUsds](https://github.com/sky-ecosystem/usds/blob/master/src/DaiUsds.sol), and
[DssLitePsm](https://github.com/makerdao/dss-lite-psm/blob/master/src/DssLitePsm.sol).

## Aave GHO GSM edges

### Current topology

The Ethereum GSM proxy names still say `GSM_USDC` and `GSM_USDT`, but their current implementations
use static Aave ERC-4626 tokens as underlying assets:

```text
USDC <-> waEthUSDC <-> GHO
USDT <-> waEthUSDT <-> GHO
```

These must be separate ordinary graph edges. Treating the GSM as direct USDC/GHO or USDT/GHO would
quote the wrong unit and is unsafe.

| Component | Address | Relevant methods |
|---|---|---|
| waEthUSDC | `0xD4fa2D31b7968E448877f69A96DE69f5de8cD23E` | `previewDeposit`, `previewRedeem`, `deposit`, `redeem` |
| waEthUSDT | `0x7Bc3485026Ac48b6cf9BaF0A377477Fff5703Af8` | same |
| GSM waEthUSDC | `0x3A3868898305f04beC7FEa77BecFf04C13444112` | `sellAsset`, `buyAsset`, four quote getters |
| GSM waEthUSDT | `0x882285E62656b9623AF136Ce3078c6BdCc33F5E3` | same |
| GhoReserve | `0x54C58157DeF387A880AE62332D1445f03adbE7E9` | `getUsage(address)` |
| Aave V3 Pool | `0x87870Bca3F3fD6335C3F4ce8392D69350B4fa4E2` | normalized income and reserve limits |

GSM entrypoint semantics:

- `sellAsset(maxAssetAmount, receiver)` sells static-aToken shares and returns GHO;
- `buyAsset(minAssetAmount, receiver)` buys an exact minimum number of shares with GHO;
- `getGhoAmountForSellAsset` and `getGhoAmountForBuyAsset` quote by share amount;
- `getAssetAmountForSellAsset` and `getAssetAmountForBuyAsset` invert by GHO amount.

The static-aToken conversion rate depends on Aave normalized income. It can change with block time
even if no watched slot changes in that block. The consumer therefore must evaluate the Aave linear
interest formula using the consensus timestamp in `BlockRef.timestamp`; wall-clock time is not an
acceptable substitute for an anchored projection.

The quote set contains:

- GSM packed slot `55`: fee-strategy address, frozen flag, and seized flag;
- static-aToken proxy/initialization guards;
- Aave Pool reserve slots used by normalized income;
- the Aave Pool and static-aToken implementation slots.

The capacity set additionally contains:

- GSM packed slot `56`: exposure cap and current exposure;
- GSM slot `58`: GhoReserve address;
- GhoReserve packed `limit/used` mapping entry for each GSM;
- GhoReserve GHO cash;
- Aave reserve configuration and scaled aToken supply used by `maxDeposit`/`maxMint`.

`Pool.getReserveData()` also reads and returns variable-borrow state, the variable-debt-token
address, and the provider's `MOCK_STABLE_DEBT` address. The wrapper does not consume those return
fields. They are deliberately excluded from the hot dictionary; fork mutation tests verify that
changing them leaves `maxDeposit` unchanged. The watched packed reserve slot `base + 8` contains
low `uint128 accruedToTreasury`, which affects `maxDeposit`, and the co-located high
`uint128 virtualUnderlyingBalance` exposed by the Pool.

Packed values are decoded as follows:

```text
GSM slot 55: low 160 bits feeStrategy, bits 160..167 isFrozen, bits 168..175 isSeized
GSM slot 56: low uint128 exposureCap, high uint128 currentExposure
GhoReserve usage: low uint128 limit, high uint128 used
```

The code/immutable guard set at the verification block was:

| Dependency | Expected address/value |
|---|---|
| USDC GSM implementation | `0x320be97b4d10b6d20a05cae53a479fa2a0187e8e` |
| USDT GSM implementation | `0x31fe806ead0a800e68627aa49bab478d20a28788` |
| USDC GSM fee strategy | `0x06fbDE909B43f01202E3C6207De1D27cC208AcC1` |
| USDT GSM fee strategy | `0xfDB0090A92d20EE39d82ac680477b1F58f0A23dE` |
| USDC GSM price strategy | `0xEE73e0c5Cc8E4cAf400baB5239860696Ff44D64f` |
| USDT GSM price strategy | `0x19804d58eF1721E199E59e10A028991ED1CfaCE9` |
| static-aToken implementation | `0x487c2c53c0866f0a73ae317bd1a28f63adcd9ad1` |
| Aave Pool implementation | `0x728a138a4823392c2efa55e028d434f526fe03cf` |
| GhoReserve implementation | `0x4f381f0827cb081b3ce2b7d7062402d43c4efbe6` |

The price-strategy addresses are immutable in their respective GSM implementations, so they are
validated as implementation metadata rather than separate storage coordinates.

When selling shares for GHO, the maximum is bounded by both remaining GSM exposure and remaining
GhoReserve allowance/cash. When buying shares, it is bounded by current exposure and the amount of
GHO usage that can be restored. Base-token wrapper capacity must then be intersected with the
static-aToken edge capacity.

At the verification block:

| GSM | Buy fee | Sell fee | Exposure cap | Current exposure | Reserve limit / used |
|---|---:|---:|---:|---:|---:|
| waEthUSDC | 10 bps | 0 bps | 175,000,000 shares | 14.165076 shares | 210m / 16.75821 GHO |
| waEthUSDT | 15 bps | 0 bps | 85,000,000 shares | 23,287,192.725515 shares | 100m / 27.34357358985m GHO |

Fee and price strategy parameters are immutable in their strategy bytecode. Their addresses come
from GSM slot `55` or the GSM implementation. A changed strategy/implementation guard must disable
the edge until its bytecode and immutables are re-discovered; silently continuing with old constants
is not allowed.

Official sources: [Aave Ethereum address book](https://github.com/aave-dao/aave-address-book/blob/main/src/GhoEthereum.sol),
[GSM implementation](https://github.com/aave-dao/gho-origin/blob/main/src/contracts/facilitators/gsm/Gsm.sol),
[GSM ERC-4626 extension](https://github.com/aave-dao/gho-origin/blob/main/src/contracts/facilitators/gsm/Gsm4626.sol), and
[GhoReserve](https://github.com/aave-dao/gho-origin/blob/main/src/contracts/facilitators/gsm/GhoReserve.sol).

## USDD PSM edges

### Deployments and entrypoints

| Component | Address | Direction / entrypoint |
|---|---|---|
| USDT PSM | `0xce355440c00014a229bbec030a2b8f8eb45a2897` | USDT → USDD: `sellGem(address,uint256)` |
| USDT PSM | same | USDD → USDT: `buyGem(address,uint256)` |
| USDC PSM | `0x12d0351f68035a41d13fc8324562e2d51b7a3b93` | USDC → USDD: `sellGem(address,uint256)` |
| USDC PSM | same | USDD → USDC: `buyGem(address,uint256)` |
| USDT Join | `0x217e42CEB2eAE9ECB788fDF0e31c806c531760A3` | USDT adapter |
| USDC Join | `0x9A7E1B324060dB7342aeA08c0dc56F55CEd6F519` | USDC adapter |
| USDD Join | `0x983dfef6d71862d809e239845da5a959492f63b8` | USDD adapter |
| Vat | `0xFf77F6209239DEB2c076179499f2346b0032097f` | debt, ceilings, urns, liveness |

With `scale = 10^12`, USDD uses the same fee shape as LitePSM:

```text
gross = gem_amount * scale
gem -> USDD output = gross - floor(gross * tin / 10^18)
USDD input for exact gem output = gross + floor(gross * tout / 10^18)
```

Unlike LitePSM, a sell mints against Vat collateral. Exact executable capacity depends on the
per-ilk `Art/rate/spot/line/dust`, global `debt/Line/live`, and the PSM urn. Buy capacity is bounded
by urn `ink/art` and the corresponding Join's external token balance. The dictionary also contains
the relevant Join/Vat/token authorization and liveness guards.

USDT deserves one extra guard: its Join checks the token's `upgradedAddress`, and legacy USDT can
enable a transfer fee. The dictionary therefore includes `deprecated/upgradedAddress`,
`basisPointsRate`, `maximumFee`, and the Join implementation allowlist. The current values are
non-deprecated, zero upgraded address, and zero transfer fee.

At the verification block:

| PSM | `tin` | `tout` | Sell / buy enabled | Existing buy-side liquidity |
|---|---:|---:|---|---:|
| USDT | 0 bps | 0 bps | yes / yes | about 7.488m USDT |
| USDC | 20 bps | 0 bps | yes / yes | about 0.010381 USDC by urn state |

The USDC PSM should therefore be considered effectively one-way at that block: USDC → USDD has
roughly 10m USDC of ilk debt-ceiling headroom, while USDD → USDC has negligible inventory.

Official sources: [USDD deployment addresses](https://docs.usdd.io/developers/deployment-addresses)
and [USDD PSM source](https://github.com/decentralized-usd/psm/blob/main/src/psm.sol).

## Consumer fail-closed rules

The initial evaluator should enforce these rules before returning an edge quote:

1. Match every proxy/strategy address guard against the registry version understood by the binary.
2. Reject halted, paused, frozen, seized, caged, disabled, or unauthorized directions.
3. Compute with checked 256-bit integer arithmetic and the contract's exact rounding direction.
4. Intersect protocol capacity with executor inventory and allowance outside this global projection.
5. Bind the result to `(boot_id, config_generation, block_hash, stage)` and use the matching block
   timestamp for time-dependent Aave math.
6. On an unknown implementation or invariant mismatch, remove the edge and trigger registry
   discovery; never fall back to stale constants.

## Adding another deployment

For a new conversion contract:

1. resolve proxies and immutable dependencies at a fixed block;
2. trace all public quote functions for at least `0`, `1`, one normal amount, and boundary values;
3. trace capacity and liveness getters separately;
4. derive every mapping coordinate and compare it to the trace;
5. compare the local integer evaluator against on-chain getters over randomized and boundary inputs;
6. add only global quote/capacity/guard keys to the statefeed dictionary;
7. make implementation/strategy changes fail closed.

This discovery workflow belongs in tooling and deployment data, not in the Reth hot path.
