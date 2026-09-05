// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Math} from "@openzeppelin-contracts-5.4.0/utils/math/Math.sol";
import {AaveModel} from "../src/ConversionModels.sol";
import {Deployments} from "../src/Deployments.sol";
import {IERC4626Like, IAavePoolLike, IGsm} from "../src/Interfaces.sol";
import {ForkTestBase} from "./ForkTestBase.t.sol";

contract AaveTimeTest is ForkTestBase {
    function testFuzz_usdcQuotesFollowConsensusTimestampWithoutStorageWrites(uint32 rawDelta) external {
        _assertTimeDependentPath(
            Deployments.USDC,
            Deployments.WA_ETH_USDC,
            Deployments.GSM_USDC,
            Deployments.AAVE_USDC_RESERVE_BASE,
            10,
            bound(uint256(rawDelta), 0, 365 days)
        );
    }

    function testFuzz_usdtQuotesFollowConsensusTimestampWithoutStorageWrites(uint32 rawDelta) external {
        _assertTimeDependentPath(
            Deployments.USDT,
            Deployments.WA_ETH_USDT,
            Deployments.GSM_USDT,
            Deployments.AAVE_USDT_RESERVE_BASE,
            15,
            bound(uint256(rawDelta), 0, 365 days)
        );
    }

    function test_quoteChangesWithTimestampWhileWatchedWordsRemainIdentical() external {
        bytes32 reserveBase = Deployments.AAVE_USDC_RESERVE_BASE;
        bytes32 indexSlot = bytes32(uint256(reserveBase) + 1);
        bytes32 timestampSlot = bytes32(uint256(reserveBase) + 3);
        bytes32 indexBefore = vm.load(Deployments.AAVE_POOL, indexSlot);
        bytes32 timestampsBefore = vm.load(Deployments.AAVE_POOL, timestampSlot);

        uint256 shares = 100_000_000e6;
        (, uint256 ghoBefore,,) = IGsm(Deployments.GSM_USDC).getGhoAmountForSellAsset(shares);
        vm.warp(block.timestamp + 30 days);
        (, uint256 ghoAfter,,) = IGsm(Deployments.GSM_USDC).getGhoAmountForSellAsset(shares);

        assertEq(vm.load(Deployments.AAVE_POOL, indexSlot), indexBefore);
        assertEq(vm.load(Deployments.AAVE_POOL, timestampSlot), timestampsBefore);
        assertGt(ghoAfter, ghoBefore, "timestamp must affect the quote");
    }

    function _assertTimeDependentPath(
        address asset,
        address wrapper,
        address gsm,
        bytes32 reserveBase,
        uint256 buyFeeBps,
        uint256 delta
    ) private {
        bytes32 indexAndRate = vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 1));
        bytes32 timestamps = vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 3));
        vm.warp(FORK_TIMESTAMP + delta);

        uint256 expectedRate = AaveModel.normalizedIncome(indexAndRate, timestamps, block.timestamp);
        assertEq(
            IAavePoolLike(Deployments.AAVE_POOL).getReserveNormalizedIncome(asset), expectedRate, "normalized income"
        );

        uint256 shares = 17_123_456e6;
        assertEq(
            IERC4626Like(wrapper).previewRedeem(shares),
            AaveModel.convertToAssets(shares, expectedRate, Math.Rounding.Floor),
            "wrapper redemption"
        );

        (uint256 actualAsset, uint256 actualTotal, uint256 actualGross, uint256 actualFee) =
            IGsm(gsm).getGhoAmountForBuyAsset(shares);
        AaveModel.Quote memory expected = AaveModel.getGhoAmountForBuyAsset(shares, expectedRate, buyFeeBps);
        assertEq(actualAsset, expected.assetAmount, "GSM asset");
        assertEq(actualTotal, expected.totalGho, "GSM total");
        assertEq(actualGross, expected.grossGho, "GSM gross");
        assertEq(actualFee, expected.fee, "GSM fee");
    }
}
