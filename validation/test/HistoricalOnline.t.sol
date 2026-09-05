// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Vm} from "forge-std-1.9.7/src/Vm.sol";
import {AaveModel, PsmModel, UsddCapacityModel} from "../src/ConversionModels.sol";
import {Deployments} from "../src/Deployments.sol";
import {HistoricalCase, HistoricalScenarios} from "../src/HistoricalCase.sol";
import {RegistryGuards} from "../src/RegistryGuards.sol";
import {IERC20Like, IGsm, IUsddPsm, IVatLike} from "../src/Interfaces.sol";
import {ForkTestBase} from "./ForkTestBase.t.sol";

contract HistoricalOnlineTest is ForkTestBase {
    bytes32 private constant SELL_ASSET_TOPIC = keccak256("SellAsset(address,address,uint256,uint256,uint256)");

    HistoricalCase[] private cases;

    function setUp() public override {
        super.setUp();
        string memory document = vm.readFile("historical-cases.toml");
        HistoricalCase[] memory decoded = abi.decode(vm.parseToml(document, ".case"), (HistoricalCase[]));
        for (uint256 i; i < decoded.length; ++i) {
            cases.push(decoded[i]);
        }
    }

    function test_historicalForkCasesMatchPinnedChainState() external {
        assertEq(cases.length, 7, "unexpected historical case count");
        for (uint256 i; i < cases.length; ++i) {
            HistoricalCase storage historicalCase = cases[i];
            _selectPinnedFork(historicalCase.blockNumber, historicalCase.blockHash, historicalCase.timestamp);
            _runCase(historicalCase);
        }
    }

    function _runCase(HistoricalCase storage historicalCase) private {
        bytes32 scenario = HistoricalScenarios.id(historicalCase.scenario);
        assertEq(
            historicalCase.overlay,
            scenario == HistoricalScenarios.FROZEN_OVERLAY ? "gsm_usdc_frozen" : "none",
            "unexpected historical overlay"
        );
        bytes32 poolImplementation = vm.load(Deployments.AAVE_POOL, Deployments.EIP1967_IMPLEMENTATION_SLOT);
        assertEq(poolImplementation, historicalCase.expectedPoolImplementation, historicalCase.id);

        if (scenario == HistoricalScenarios.UPGRADE_BEFORE) {
            assertFalse(
                RegistryGuards.supportedImplementation(Deployments.AAVE_POOL, poolImplementation), historicalCase.id
            );
        } else if (scenario == HistoricalScenarios.UPGRADE_AFTER) {
            assertTrue(
                RegistryGuards.supportedImplementation(Deployments.AAVE_POOL, poolImplementation), historicalCase.id
            );
            _assertAaveQuotes(Deployments.GSM_USDC, Deployments.AAVE_USDC_RESERVE_BASE, 10);
        } else if (scenario == HistoricalScenarios.REAL_SWAP) {
            _assertRealSwap(historicalCase);
        } else if (scenario == HistoricalScenarios.NORMAL) {
            _assertAaveQuotes(Deployments.GSM_USDC, Deployments.AAVE_USDC_RESERVE_BASE, 10);
            _assertAaveQuotes(Deployments.GSM_USDT, Deployments.AAVE_USDT_RESERVE_BASE, 15);
        } else if (scenario == HistoricalScenarios.NONZERO_FEE) {
            _assertNonzeroFees();
        } else if (scenario == HistoricalScenarios.NEAR_CAPACITY) {
            _assertNearCapacityBoundary();
        } else if (scenario == HistoricalScenarios.FROZEN_OVERLAY) {
            _assertFrozenOverlay();
        } else {
            assertTrue(false, string.concat("unknown historical scenario: ", historicalCase.scenario));
        }
    }

    function _assertAaveQuotes(address gsm, bytes32 reserveBase, uint256 buyFeeBps) private {
        vm.record();
        uint256 shares = 1_000_000e6;
        uint256 gho = 1_000_000 ether;
        uint256 rate = _localRate(reserveBase);

        (uint256 asset, uint256 total, uint256 gross, uint256 fee) = IGsm(gsm).getGhoAmountForBuyAsset(shares);
        _assertQuote(asset, total, gross, fee, AaveModel.getGhoAmountForBuyAsset(shares, rate, buyFeeBps));
        (asset, total, gross, fee) = IGsm(gsm).getGhoAmountForSellAsset(shares);
        _assertQuote(asset, total, gross, fee, AaveModel.getGhoAmountForSellAsset(shares, rate, 0));
        (asset, total, gross, fee) = IGsm(gsm).getAssetAmountForBuyAsset(gho);
        _assertQuote(asset, total, gross, fee, AaveModel.getAssetAmountForBuyAsset(gho, rate, buyFeeBps));
        (asset, total, gross, fee) = IGsm(gsm).getAssetAmountForSellAsset(gho);
        _assertQuote(asset, total, gross, fee, AaveModel.getAssetAmountForSellAsset(gho, rate, 0));

        _assertOnlyWatchedReads(gsm);
        _assertOnlyWatchedReads(gsm == Deployments.GSM_USDC ? Deployments.WA_ETH_USDC : Deployments.WA_ETH_USDT);
        _assertOnlyWatchedReads(Deployments.AAVE_POOL);
    }

    function _assertNonzeroFees() private {
        _assertAaveQuotes(Deployments.GSM_USDC, Deployments.AAVE_USDC_RESERVE_BASE, 10);

        vm.record();
        (, uint256 total, uint256 gross, uint256 fee) = IGsm(Deployments.GSM_USDC).getGhoAmountForBuyAsset(1_000_000e6);
        assertGt(fee, 0, "Aave buy fee must be non-zero");
        assertEq(total, gross + fee);

        uint256 tin = IUsddPsm(Deployments.USDD_USDC_PSM).tin();
        assertGt(tin, 0, "USDD sell fee must be non-zero");
        uint256 grossUsdd = 1_000_000e6 * 1e12;
        assertLt(PsmModel.sell(1_000_000e6, tin), grossUsdd);
        _assertOnlyWatchedReads(Deployments.USDD_USDC_PSM);
    }

    function _assertNearCapacityBoundary() private {
        vm.record();
        UsddCapacityModel.VatState memory state = _usddState();
        uint256 capacity =
            UsddCapacityModel.buyCapacity(state, IERC20Like(Deployments.USDC).balanceOf(Deployments.USDD_USDC_JOIN));
        assertGt(capacity, 0, "capacity unexpectedly zero");
        assertLt(capacity, 1e6, "case is no longer near-empty");
        _assertOnlyWatchedReads(Deployments.USDD_VAT);
        _assertOnlyWatchedReads(Deployments.USDC);

        uint256 tout = uint256(vm.load(Deployments.USDD_USDC_PSM, bytes32(uint256(2))));
        uint256 required = PsmModel.buy(capacity + 1, tout);
        deal(Deployments.USDD, address(this), required);
        _approve(Deployments.USDD, Deployments.USDD_USDC_PSM, required);
        vm.expectRevert();
        IUsddPsm(Deployments.USDD_USDC_PSM).buyGem(address(this), capacity + 1);
        IUsddPsm(Deployments.USDD_USDC_PSM).buyGem(address(this), capacity);
    }

    function _assertFrozenOverlay() private {
        bytes32 live = vm.load(Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT);
        bytes32 frozen = bytes32(uint256(live) | (uint256(1) << 160));
        vm.store(Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT, frozen);

        vm.record();
        assertFalse(IGsm(Deployments.GSM_USDC).canSwap());
        _assertOnlyWatchedReads(Deployments.GSM_USDC);
        assertFalse(
            RegistryGuards.supportedGsm(
                Deployments.GSM_USDC, frozen, vm.load(Deployments.GSM_USDC, Deployments.GSM_GHO_RESERVE_SLOT)
            )
        );
        vm.expectRevert(bytes("GSM_FROZEN"));
        IGsm(Deployments.GSM_USDC).sellAsset(1, address(this));
    }

    function _assertRealSwap(HistoricalCase storage historicalCase) private {
        _assertRealSwapPrestate(historicalCase);

        bytes32[] memory topics = new bytes32[](1);
        topics[0] = SELL_ASSET_TOPIC;
        Vm.EthGetLogs[] memory logs =
            vm.eth_getLogs(historicalCase.blockNumber, historicalCase.blockNumber, Deployments.GSM_USDC, topics);

        bool found;
        for (uint256 i; i < logs.length; ++i) {
            if (logs[i].transactionHash != historicalCase.transactionHash) continue;
            assertEq(logs[i].blockHash, historicalCase.blockHash, historicalCase.id);
            assertEq(logs[i].blockNumber, historicalCase.blockNumber, historicalCase.id);
            assertEq(logs[i].transactionIndex, historicalCase.transactionIndex, historicalCase.id);
            assertFalse(logs[i].removed, historicalCase.id);
            (uint256 assetAmount, uint256 ghoAmount, uint256 fee) =
                abi.decode(logs[i].data, (uint256, uint256, uint256));
            assertEq(assetAmount, uint256(historicalCase.eventAssetAmount), historicalCase.id);
            assertEq(ghoAmount, uint256(historicalCase.eventGhoAmount), historicalCase.id);
            assertEq(fee, uint256(historicalCase.eventFee), historicalCase.id);

            vm.record();
            (uint256 quotedAsset, uint256 quotedGho, uint256 gross, uint256 quotedFee) =
                IGsm(Deployments.GSM_USDC).getGhoAmountForSellAsset(assetAmount);
            AaveModel.Quote memory expected =
                AaveModel.getGhoAmountForSellAsset(assetAmount, _localRate(Deployments.AAVE_USDC_RESERVE_BASE), 0);
            _assertQuote(quotedAsset, quotedGho, gross, quotedFee, expected);
            assertEq(quotedGho, ghoAmount, historicalCase.id);
            assertEq(quotedFee, fee, historicalCase.id);
            _assertOnlyWatchedReads(Deployments.GSM_USDC);
            _assertOnlyWatchedReads(Deployments.WA_ETH_USDC);
            _assertOnlyWatchedReads(Deployments.AAVE_POOL);
            found = true;
            break;
        }
        assertTrue(found, "pinned real swap log missing");
    }

    function _assertRealSwapPrestate(HistoricalCase storage historicalCase) private view {
        assertTrue(bytes(historicalCase.prestateFixture).length != 0, "real swap prestate fixture missing");
        string memory json = vm.readFile(historicalCase.prestateFixture);
        assertEq(vm.parseJsonBytes32(json, ".block_hash"), historicalCase.blockHash, historicalCase.id);
        assertEq(vm.parseJsonUint(json, ".block_number"), historicalCase.blockNumber, historicalCase.id);
        assertEq(vm.parseJsonUint(json, ".timestamp"), historicalCase.timestamp, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".transaction_hash"), historicalCase.transactionHash, historicalCase.id);
        assertEq(vm.parseJsonUint(json, ".transaction_index"), historicalCase.transactionIndex, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".event_asset_amount"), historicalCase.eventAssetAmount, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".event_gho_amount"), historicalCase.eventGhoAmount, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".event_fee"), historicalCase.eventFee, historicalCase.id);
        assertEq(vm.parseJsonString(json, ".source"), "debug_traceTransaction/prestateTracer");
        assertEq(vm.parseJsonAddress(json, ".gsm_address"), Deployments.GSM_USDC);
        assertEq(vm.parseJsonBytes32(json, ".gsm_fee_and_flags_slot"), Deployments.GSM_FEE_AND_FLAGS_SLOT);
        assertEq(vm.parseJsonAddress(json, ".pool_address"), Deployments.AAVE_POOL);
        assertEq(
            vm.parseJsonBytes32(json, ".pool_index_and_rate_slot"),
            bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 1)
        );
        assertEq(
            vm.parseJsonBytes32(json, ".pool_deficit_and_timestamps_slot"),
            bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 3)
        );

        bytes32 indexAndRate = vm.parseJsonBytes32(json, ".pool_index_and_rate");
        bytes32 deficitAndTimestamps = vm.parseJsonBytes32(json, ".pool_deficit_and_timestamps");
        assertNotEq(
            indexAndRate,
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 1)),
            "transaction prestate unexpectedly equals block post-state"
        );
        assertNotEq(
            deficitAndTimestamps,
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 3)),
            "transaction prestate unexpectedly equals block post-state"
        );

        uint256 rate = AaveModel.normalizedIncome(indexAndRate, deficitAndTimestamps, historicalCase.timestamp);
        AaveModel.Quote memory quote =
            AaveModel.getGhoAmountForSellAsset(uint256(historicalCase.eventAssetAmount), rate, 0);
        assertEq(quote.assetAmount, uint256(historicalCase.eventAssetAmount), historicalCase.id);
        assertEq(quote.totalGho, uint256(historicalCase.eventGhoAmount), historicalCase.id);
        assertEq(quote.fee, uint256(historicalCase.eventFee), historicalCase.id);
    }

    function _usddState() private view returns (UsddCapacityModel.VatState memory state) {
        IVatLike vat = IVatLike(Deployments.USDD_VAT);
        (state.art, state.rate, state.spot, state.line, state.dust) = vat.ilks(Deployments.USDD_USDC_ILK);
        (state.urnInk, state.urnArt) = vat.urns(Deployments.USDD_USDC_ILK, Deployments.USDD_USDC_PSM);
        state.debt = vat.debt();
        state.globalLine = vat.Line();
    }

    function _localRate(bytes32 reserveBase) private view returns (uint256) {
        return AaveModel.normalizedIncome(
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 1)),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 3)),
            block.timestamp
        );
    }

    function _assertQuote(uint256 asset, uint256 total, uint256 gross, uint256 fee, AaveModel.Quote memory expected)
        private
        pure
    {
        assertEq(asset, expected.assetAmount);
        assertEq(total, expected.totalGho);
        assertEq(gross, expected.grossGho);
        assertEq(fee, expected.fee);
    }
}
