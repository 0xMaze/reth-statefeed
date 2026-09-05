// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Math} from "@openzeppelin-contracts-5.4.0/utils/math/Math.sol";
import {Test} from "forge-std-1.9.7/src/Test.sol";
import {AaveModel, UsddCapacityModel} from "../src/ConversionModels.sol";
import {Deployments} from "../src/Deployments.sol";
import {HistoricalCase, HistoricalScenarios} from "../src/HistoricalCase.sol";
import {RegistryGuards} from "../src/RegistryGuards.sol";

contract HistoricalFixturesTest is Test {
    struct Watch {
        address account;
        string id;
        bytes32 slot;
    }

    struct Snapshot {
        address[] accounts;
        bytes32[] slots;
        bytes32[] values;
    }

    HistoricalCase[] private cases;
    Watch[] private manifest;
    bytes32 private manifestHash;

    function setUp() public {
        string memory caseDocument = vm.readFile("historical-cases.toml");
        HistoricalCase[] memory decodedCases = abi.decode(vm.parseToml(caseDocument, ".case"), (HistoricalCase[]));
        for (uint256 i; i < decodedCases.length; ++i) {
            cases.push(decodedCases[i]);
        }

        string memory manifestDocument = vm.readFile("../config.ethereum-mainnet-conversions.toml");
        Watch[] memory decodedManifest = abi.decode(vm.parseToml(manifestDocument, ".watch"), (Watch[]));
        for (uint256 i; i < decodedManifest.length; ++i) {
            manifest.push(decodedManifest[i]);
        }
        manifestHash = keccak256(bytes(manifestDocument));
    }

    function test_allHistoricalFixturesAreHermeticAndMatchLocalModels() external view {
        assertEq(cases.length, 7, "unexpected historical case count");
        for (uint256 i; i < cases.length; ++i) {
            _assertFixture(cases[i]);
        }
    }

    function _assertFixture(HistoricalCase storage historicalCase) private view {
        string memory json = vm.readFile(historicalCase.fixture);
        assertEq(vm.parseJsonBytes32(json, ".block_hash"), historicalCase.blockHash, historicalCase.id);
        assertEq(vm.parseJsonUint(json, ".block_number"), historicalCase.blockNumber, historicalCase.id);
        assertEq(vm.parseJsonUint(json, ".timestamp"), historicalCase.timestamp, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".manifest_hash"), manifestHash, historicalCase.id);

        Snapshot memory snapshot = Snapshot({
            accounts: vm.parseJsonAddressArray(json, ".accounts"),
            slots: vm.parseJsonBytes32Array(json, ".slots"),
            values: vm.parseJsonBytes32Array(json, ".values")
        });
        _assertDictionary(json, snapshot, historicalCase.id);

        bytes32 scenario = HistoricalScenarios.id(historicalCase.scenario);
        assertEq(
            historicalCase.overlay,
            scenario == HistoricalScenarios.FROZEN_OVERLAY ? "gsm_usdc_frozen" : "none",
            "unexpected historical overlay"
        );
        if (scenario == HistoricalScenarios.REAL_SWAP) {
            assertTrue(bytes(historicalCase.prestateFixture).length != 0, "real swap prestate fixture missing");
        } else {
            assertEq(historicalCase.prestateFixture, "", "unexpected prestate fixture");
        }
        bytes32 poolImplementation = _value(snapshot, Deployments.AAVE_POOL, Deployments.EIP1967_IMPLEMENTATION_SLOT);
        assertEq(poolImplementation, historicalCase.expectedPoolImplementation, historicalCase.id);
        if (scenario == HistoricalScenarios.UPGRADE_BEFORE) {
            assertFalse(
                RegistryGuards.supportedImplementation(Deployments.AAVE_POOL, poolImplementation), historicalCase.id
            );
            return;
        }
        assertTrue(RegistryGuards.supportedImplementation(Deployments.AAVE_POOL, poolImplementation), historicalCase.id);

        if (
            scenario == HistoricalScenarios.UPGRADE_AFTER || scenario == HistoricalScenarios.REAL_SWAP
                || scenario == HistoricalScenarios.NORMAL || scenario == HistoricalScenarios.NONZERO_FEE
        ) {
            _assertUsdcPath(json, snapshot, historicalCase.timestamp);
            _assertGsmGuard(snapshot, Deployments.GSM_USDC, true);
        }
        if (scenario == HistoricalScenarios.NORMAL || scenario == HistoricalScenarios.NONZERO_FEE) {
            _assertUsdtPath(json, snapshot, historicalCase.timestamp);
            _assertGsmGuard(snapshot, Deployments.GSM_USDT, true);
        } else if (scenario == HistoricalScenarios.UPGRADE_AFTER) {
            // Pool V3.5 is supported here, while the then-current USDT GSM fee strategy is not.
            _assertGsmGuard(snapshot, Deployments.GSM_USDT, false);
        }

        if (scenario == HistoricalScenarios.NEAR_CAPACITY) {
            _assertUsddCapacity(json, snapshot, false, ".usdd_usdc_capacity");
        } else if (scenario == HistoricalScenarios.FROZEN_OVERLAY) {
            bytes32 feeAndFlags = bytes32(
                uint256(_value(snapshot, Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT))
                    | (uint256(1) << 160)
            );
            bytes32 reserve = _value(snapshot, Deployments.GSM_USDC, Deployments.GSM_GHO_RESERVE_SLOT);
            assertFalse(RegistryGuards.supportedGsm(Deployments.GSM_USDC, feeAndFlags, reserve));
        } else if (scenario == HistoricalScenarios.REAL_SWAP) {
            _assertRealSwapPrestate(snapshot, historicalCase);
        }
    }

    function _assertRealSwapPrestate(Snapshot memory snapshot, HistoricalCase storage historicalCase) private view {
        string memory json = vm.readFile(historicalCase.prestateFixture);
        assertEq(vm.parseJsonBytes32(json, ".block_hash"), historicalCase.blockHash, historicalCase.id);
        assertEq(vm.parseJsonUint(json, ".block_number"), historicalCase.blockNumber, historicalCase.id);
        assertEq(vm.parseJsonUint(json, ".timestamp"), historicalCase.timestamp, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".transaction_hash"), historicalCase.transactionHash, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".event_asset_amount"), historicalCase.eventAssetAmount, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".event_gho_amount"), historicalCase.eventGhoAmount, historicalCase.id);
        assertEq(vm.parseJsonBytes32(json, ".event_fee"), historicalCase.eventFee, historicalCase.id);
        _assertRealSwapCoordinates(json);
        assertEq(vm.parseJsonUint(json, ".transaction_index"), historicalCase.transactionIndex, historicalCase.id);

        bytes32 feeAndFlags = vm.parseJsonBytes32(json, ".gsm_fee_and_flags");
        assertEq(
            feeAndFlags,
            _value(snapshot, Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT),
            "unexpected GSM state transition"
        );
        assertTrue(
            RegistryGuards.supportedGsm(
                Deployments.GSM_USDC,
                feeAndFlags,
                _value(snapshot, Deployments.GSM_USDC, Deployments.GSM_GHO_RESERVE_SLOT)
            ),
            historicalCase.id
        );

        bytes32 indexAndRate = vm.parseJsonBytes32(json, ".pool_index_and_rate");
        bytes32 deficitAndTimestamps = vm.parseJsonBytes32(json, ".pool_deficit_and_timestamps");
        assertNotEq(
            indexAndRate,
            _value(snapshot, Deployments.AAVE_POOL, bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 1)),
            "transaction prestate unexpectedly equals block post-state"
        );
        assertNotEq(
            deficitAndTimestamps,
            _value(snapshot, Deployments.AAVE_POOL, bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 3)),
            "transaction prestate unexpectedly equals block post-state"
        );

        uint256 rate = AaveModel.normalizedIncome(indexAndRate, deficitAndTimestamps, historicalCase.timestamp);
        AaveModel.Quote memory quote =
            AaveModel.getGhoAmountForSellAsset(uint256(historicalCase.eventAssetAmount), rate, 0);
        assertEq(quote.assetAmount, uint256(historicalCase.eventAssetAmount), historicalCase.id);
        assertEq(quote.totalGho, uint256(historicalCase.eventGhoAmount), historicalCase.id);
        assertEq(quote.fee, uint256(historicalCase.eventFee), historicalCase.id);
    }

    function _assertRealSwapCoordinates(string memory json) private pure {
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
    }

    function _assertGsmGuard(Snapshot memory snapshot, address gsm, bool expected) private pure {
        bool supported = RegistryGuards.supportedGsm(
            gsm,
            _value(snapshot, gsm, Deployments.GSM_FEE_AND_FLAGS_SLOT),
            _value(snapshot, gsm, Deployments.GSM_GHO_RESERVE_SLOT)
        );
        assertEq(supported, expected);
    }

    function _assertUsdcPath(string memory json, Snapshot memory snapshot, uint256 timestamp) private pure {
        _assertAavePath(
            json,
            snapshot,
            Deployments.AAVE_USDC_RESERVE_BASE,
            Deployments.AAVE_A_USDC,
            10,
            ".aave_usdc_quotes",
            ".aave_usdc_wrapper",
            vm.parseJsonUint(json, ".probe_asset"),
            vm.parseJsonUint(json, ".probe_gho"),
            timestamp
        );
    }

    function _assertUsdtPath(string memory json, Snapshot memory snapshot, uint256 timestamp) private pure {
        _assertAavePath(
            json,
            snapshot,
            Deployments.AAVE_USDT_RESERVE_BASE,
            Deployments.AAVE_A_USDT,
            15,
            ".aave_usdt_quotes",
            ".aave_usdt_wrapper",
            vm.parseJsonUint(json, ".probe_asset"),
            vm.parseJsonUint(json, ".probe_gho"),
            timestamp
        );
    }

    function _assertDictionary(string memory json, Snapshot memory snapshot, string memory label) private view {
        string[] memory ids = vm.parseJsonStringArray(json, ".ids");
        assertEq(snapshot.accounts.length, manifest.length, label);
        assertEq(snapshot.slots.length, manifest.length, label);
        assertEq(snapshot.values.length, manifest.length, label);
        assertEq(ids.length, manifest.length, label);
        for (uint256 i; i < manifest.length; ++i) {
            assertEq(snapshot.accounts[i], manifest[i].account, label);
            assertEq(snapshot.slots[i], manifest[i].slot, label);
            assertEq(ids[i], manifest[i].id, label);
        }
    }

    function _assertAavePath(
        string memory json,
        Snapshot memory snapshot,
        bytes32 reserveBase,
        address aToken,
        uint256 buyFeeBps,
        string memory quoteKey,
        string memory wrapperKey,
        uint256 probeAsset,
        uint256 probeGho,
        uint256 timestamp
    ) private pure {
        bytes32[] memory oracle = vm.parseJsonBytes32Array(json, quoteKey);
        assertEq(oracle.length, 16, quoteKey);
        uint256 rate = _aaveRate(snapshot, reserveBase, timestamp);
        _assertQuote(oracle, 0, AaveModel.getGhoAmountForBuyAsset(probeAsset, rate, buyFeeBps));
        _assertQuote(oracle, 4, AaveModel.getGhoAmountForSellAsset(probeAsset, rate, 0));
        _assertQuote(oracle, 8, AaveModel.getAssetAmountForBuyAsset(probeGho, rate, buyFeeBps));
        _assertQuote(oracle, 12, AaveModel.getAssetAmountForSellAsset(probeGho, rate, 0));

        bytes32[] memory wrapper = vm.parseJsonBytes32Array(json, wrapperKey);
        assertEq(wrapper.length, 7, wrapperKey);
        assertEq(uint256(wrapper[0]), AaveModel.convertToAssets(probeAsset, rate, Math.Rounding.Floor));
        assertEq(uint256(wrapper[1]), AaveModel.convertToShares(probeAsset, rate, Math.Rounding.Floor));
        assertEq(uint256(wrapper[2]), AaveModel.convertToShares(probeAsset, rate, Math.Rounding.Floor));
        assertEq(uint256(wrapper[3]), AaveModel.convertToAssets(probeAsset, rate, Math.Rounding.Ceil));
        assertEq(uint256(wrapper[4]), AaveModel.convertToShares(probeAsset, rate, Math.Rounding.Ceil));
        assertEq(uint256(wrapper[5]), AaveModel.convertToAssets(probeAsset, rate, Math.Rounding.Floor));
        _assertMaxDeposit(snapshot, reserveBase, aToken, timestamp, uint256(wrapper[6]));
    }

    function _assertMaxDeposit(
        Snapshot memory snapshot,
        bytes32 reserveBase,
        address aToken,
        uint256 timestamp,
        uint256 oracle
    ) private pure {
        uint256 expected = AaveModel.maxDeposit(
            _value(snapshot, Deployments.AAVE_POOL, reserveBase),
            _value(snapshot, Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 1)),
            _value(snapshot, Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 3)),
            _value(snapshot, Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 8)),
            uint256(_value(snapshot, aToken, bytes32(uint256(54)))),
            timestamp
        );
        assertEq(oracle, expected);
    }

    function _assertUsddCapacity(string memory json, Snapshot memory snapshot, bool usdt, string memory key)
        private
        pure
    {
        address psm = usdt ? Deployments.USDD_USDT_PSM : Deployments.USDD_USDC_PSM;
        address gem = usdt ? Deployments.USDT : Deployments.USDC;
        bytes32 ilk = usdt ? Deployments.USDD_USDT_ILK : Deployments.USDD_USDC_ILK;
        uint256 ilkSlot = 2;
        bytes32 ilkBase = keccak256(abi.encode(ilk, ilkSlot));
        bytes32 urnBase = keccak256(abi.encode(psm, keccak256(abi.encode(ilk, uint256(3)))));

        UsddCapacityModel.VatState memory state = UsddCapacityModel.VatState({
            art: uint256(_value(snapshot, Deployments.USDD_VAT, ilkBase)),
            rate: uint256(_value(snapshot, Deployments.USDD_VAT, bytes32(uint256(ilkBase) + 1))),
            spot: uint256(_value(snapshot, Deployments.USDD_VAT, bytes32(uint256(ilkBase) + 2))),
            line: uint256(_value(snapshot, Deployments.USDD_VAT, bytes32(uint256(ilkBase) + 3))),
            dust: uint256(_value(snapshot, Deployments.USDD_VAT, bytes32(uint256(ilkBase) + 4))),
            debt: uint256(_value(snapshot, Deployments.USDD_VAT, bytes32(uint256(7)))),
            globalLine: uint256(_value(snapshot, Deployments.USDD_VAT, bytes32(uint256(9)))),
            urnInk: uint256(_value(snapshot, Deployments.USDD_VAT, urnBase)),
            urnArt: uint256(_value(snapshot, Deployments.USDD_VAT, bytes32(uint256(urnBase) + 1)))
        });
        uint256 joinBalance = uint256(
            _value(
                snapshot,
                gem,
                usdt
                    ? bytes32(0x91355112cd40c99060c242e835b171556fe0aa354e76f2edb6cde6e28bdde1dc)
                    : bytes32(0x9fb277d7a188accde6928af0ac176a01d02a09420e3f6e486abfc29bca8ed826)
            )
        );

        bytes32[] memory expected = vm.parseJsonBytes32Array(json, key);
        assertEq(expected.length, 2, key);
        assertEq(uint256(expected[0]), UsddCapacityModel.sellCapacity(state), key);
        assertEq(uint256(expected[1]), UsddCapacityModel.buyCapacity(state, joinBalance), key);
    }

    function _aaveRate(Snapshot memory snapshot, bytes32 reserveBase, uint256 timestamp)
        private
        pure
        returns (uint256)
    {
        return AaveModel.normalizedIncome(
            _value(snapshot, Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 1)),
            _value(snapshot, Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 3)),
            timestamp
        );
    }

    function _value(Snapshot memory snapshot, address account, bytes32 slot) private pure returns (bytes32) {
        for (uint256 i; i < snapshot.values.length; ++i) {
            if (snapshot.accounts[i] == account && snapshot.slots[i] == slot) return snapshot.values[i];
        }
        revert("fixture coordinate missing");
    }

    function _assertQuote(bytes32[] memory oracle, uint256 offset, AaveModel.Quote memory expected) private pure {
        assertEq(uint256(oracle[offset]), expected.assetAmount);
        assertEq(uint256(oracle[offset + 1]), expected.totalGho);
        assertEq(uint256(oracle[offset + 2]), expected.grossGho);
        assertEq(uint256(oracle[offset + 3]), expected.fee);
    }
}
