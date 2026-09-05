// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Math} from "@openzeppelin-contracts-5.4.0/utils/math/Math.sol";
import {AaveModel, PsmModel} from "../src/ConversionModels.sol";
import {Deployments} from "../src/Deployments.sol";
import {
    IERC20Like,
    IERC4626Like,
    IAavePoolLike,
    IGhoReserve,
    IGsm,
    ILitePsm,
    IUsddPsm,
    IVatLike
} from "../src/Interfaces.sol";
import {ForkTestBase} from "./ForkTestBase.t.sol";

contract MockFixedFeeStrategy {
    uint256 private immutable BUY_FEE_BPS;
    uint256 private immutable SELL_FEE_BPS;

    constructor(uint256 buyFeeBps_, uint256 sellFeeBps_) {
        BUY_FEE_BPS = buyFeeBps_;
        SELL_FEE_BPS = sellFeeBps_;
    }

    function getBuyFee(uint256 gross) external view returns (uint256) {
        return Math.mulDiv(gross, BUY_FEE_BPS, 10_000, Math.Rounding.Ceil);
    }

    function getSellFee(uint256 gross) external view returns (uint256) {
        return Math.mulDiv(gross, SELL_FEE_BPS, 10_000, Math.Rounding.Ceil);
    }

    function getGrossAmountFromTotalBought(uint256 total) external view returns (uint256) {
        return total == 0 ? 0 : Math.mulDiv(total, 10_000, 10_000 + BUY_FEE_BPS);
    }

    function getGrossAmountFromTotalSold(uint256 total) external view returns (uint256) {
        return total == 0 ? 0 : Math.mulDiv(total, 10_000, 10_000 - SELL_FEE_BPS, Math.Rounding.Ceil);
    }
}

contract SlotMutationTest is ForkTestBase {
    bytes32 private constant MOCK_STABLE_DEBT_PROVIDER_SLOT =
        0xb035f62398c2f37b04e1eceb7c8e682f004b880b099118069ef0a8d3cb0fcdae;

    function testFuzz_mutatingSkyTinChangesExecutionExactly(uint32 rawGem, uint64 rawFee) external {
        uint256 gemAmount = bound(uint256(rawGem), 1, 100_000e6);
        uint256 fee = bound(uint256(rawFee), 0, 1e18);
        vm.store(Deployments.SKY_LITE_PSM, bytes32(uint256(3)), bytes32(fee));

        uint256 expected = PsmModel.sell(gemAmount, fee);
        deal(Deployments.USDC, address(this), gemAmount);
        _approve(Deployments.USDC, Deployments.SKY_LITE_PSM, gemAmount);
        uint256 returned = ILitePsm(Deployments.SKY_LITE_PSM).sellGem(address(this), gemAmount);

        assertEq(returned, expected);
    }

    function testFuzz_mutatingSkyToutChangesExecutionExactly(uint32 rawGem, uint64 rawFee) external {
        uint256 gemAmount = bound(uint256(rawGem), 1, 100_000e6);
        uint256 fee = bound(uint256(rawFee), 0, 1e18);
        vm.store(Deployments.SKY_LITE_PSM, bytes32(uint256(4)), bytes32(fee));

        uint256 expected = PsmModel.buy(gemAmount, fee);
        deal(Deployments.DAI, address(this), expected);
        _approve(Deployments.DAI, Deployments.SKY_LITE_PSM, expected);
        uint256 returned = ILitePsm(Deployments.SKY_LITE_PSM).buyGem(address(this), gemAmount);

        assertEq(returned, expected);
    }

    function test_mutatingUsddFeeAndEnabledSlotsChangesExecution() external {
        uint256 gemAmount = 100e6;
        uint256 tin = 333e14; // 333 bps in WAD precision.
        vm.store(Deployments.USDD_USDC_PSM, bytes32(uint256(1)), bytes32(tin));

        deal(Deployments.USDC, address(this), gemAmount);
        _approve(Deployments.USDC, Deployments.USDD_USDC_JOIN, gemAmount);
        uint256 beforeUsdd = IERC20Like(Deployments.USDD).balanceOf(address(this));
        IUsddPsm(Deployments.USDD_USDC_PSM).sellGem(address(this), gemAmount);
        assertEq(IERC20Like(Deployments.USDD).balanceOf(address(this)) - beforeUsdd, PsmModel.sell(gemAmount, tin));

        vm.store(Deployments.USDD_USDC_PSM, bytes32(uint256(3)), bytes32(0));
        vm.expectRevert(bytes("UsddPsm/sell-not-enabled"));
        IUsddPsm(Deployments.USDD_USDC_PSM).sellGem(address(this), 1);

        vm.store(Deployments.USDD_USDC_PSM, bytes32(uint256(4)), bytes32(0));
        vm.expectRevert(bytes("UsddPsm/buy-not-enabled"));
        IUsddPsm(Deployments.USDD_USDC_PSM).buyGem(address(this), 1);
    }

    function test_mutatingAaveReserveWordsChangesPoolWrapperAndGsmTogether() external {
        bytes32 base = Deployments.AAVE_USDC_RESERVE_BASE;
        bytes32 indexAndRateSlot = bytes32(uint256(base) + 1);
        bytes32 timestampSlot = bytes32(uint256(base) + 3);

        uint128 index = 1_075_123_456_789_012_345_678_901_234;
        uint128 rate = 37_500_000_000_000_000_000_000_000;
        uint40 lastUpdate = uint40(block.timestamp - 30 days);
        bytes32 indexAndRate = bytes32(uint256(index) | (uint256(rate) << 128));
        bytes32 oldTimestamps = vm.load(Deployments.AAVE_POOL, timestampSlot);
        uint256 timestampMask = uint256(type(uint40).max) << 128;
        bytes32 timestamps = bytes32((uint256(oldTimestamps) & ~timestampMask) | (uint256(lastUpdate) << 128));

        vm.store(Deployments.AAVE_POOL, indexAndRateSlot, indexAndRate);
        vm.store(Deployments.AAVE_POOL, timestampSlot, timestamps);

        uint256 localRate = AaveModel.normalizedIncome(indexAndRate, timestamps, block.timestamp);
        assertEq(IAavePoolLike(Deployments.AAVE_POOL).getReserveNormalizedIncome(Deployments.USDC), localRate);

        uint256 amount = 1_234_567e6;
        assertEq(
            IERC4626Like(Deployments.WA_ETH_USDC).previewDeposit(amount),
            AaveModel.convertToShares(amount, localRate, Math.Rounding.Floor)
        );

        (uint256 asset, uint256 total, uint256 gross, uint256 fee) =
            IGsm(Deployments.GSM_USDC).getGhoAmountForBuyAsset(amount);
        AaveModel.Quote memory expected = AaveModel.getGhoAmountForBuyAsset(amount, localRate, 10);
        _assertQuote(asset, total, gross, fee, expected);
    }

    function test_aaveFieldsReturnedButIgnoredByMaxDepositDoNotChangeOutput() external {
        uint256 usdcBefore = IERC4626Like(Deployments.WA_ETH_USDC).maxDeposit(address(0));
        uint256 usdtBefore = IERC4626Like(Deployments.WA_ETH_USDT).maxDeposit(address(0));

        vm.store(
            Deployments.AAVE_POOL,
            bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 2),
            keccak256("mutated USDC variable borrow data")
        );
        vm.store(
            Deployments.AAVE_POOL,
            bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 6),
            keccak256("mutated USDC variable debt token")
        );
        vm.store(
            Deployments.AAVE_POOL,
            bytes32(uint256(Deployments.AAVE_USDT_RESERVE_BASE) + 2),
            keccak256("mutated USDT variable borrow data")
        );
        vm.store(
            Deployments.AAVE_POOL,
            bytes32(uint256(Deployments.AAVE_USDT_RESERVE_BASE) + 6),
            keccak256("mutated USDT variable debt token")
        );
        vm.store(
            Deployments.AAVE_POOL_ADDRESSES_PROVIDER,
            MOCK_STABLE_DEBT_PROVIDER_SLOT,
            bytes32(uint256(uint160(address(0xBEEF))))
        );

        assertEq(IERC4626Like(Deployments.WA_ETH_USDC).maxDeposit(address(0)), usdcBefore);
        assertEq(IERC4626Like(Deployments.WA_ETH_USDT).maxDeposit(address(0)), usdtBefore);
    }

    function test_mutatingAaveConfigurationAccrualAndVirtualBalanceMatchesCapacityModel() external {
        bytes32 base = Deployments.AAVE_USDC_RESERVE_BASE;
        bytes32 originalConfig = vm.load(Deployments.AAVE_POOL, base);

        vm.store(Deployments.AAVE_POOL, base, bytes32(uint256(originalConfig) | (uint256(1) << 60)));
        _assertLocalMaxDeposit(0);

        vm.store(Deployments.AAVE_POOL, base, bytes32(uint256(originalConfig) | (uint256(1) << 57)));
        _assertLocalMaxDeposit(0);

        vm.store(Deployments.AAVE_POOL, base, bytes32(uint256(originalConfig) & ~(uint256(1) << 56)));
        _assertLocalMaxDeposit(0);

        vm.store(Deployments.AAVE_POOL, base, originalConfig);
        bytes32 accruedAndVirtualSlot = bytes32(uint256(base) + 8);
        uint128 accrued = 12_345_678e6;
        uint128 virtualBalance = 876_543_210e6;
        vm.store(
            Deployments.AAVE_POOL, accruedAndVirtualSlot, bytes32(uint256(accrued) | (uint256(virtualBalance) << 128))
        );
        vm.store(Deployments.AAVE_A_USDC, bytes32(uint256(54)), bytes32(uint256(100_000_000e6)));

        _assertLocalMaxDeposit(type(uint256).max);
        assertEq(IAavePoolLike(Deployments.AAVE_POOL).getVirtualUnderlyingBalance(Deployments.USDC), virtualBalance);
    }

    function test_mutatingPackedGsmFeeStrategyChangesQuoteExactly() external {
        MockFixedFeeStrategy strategy = new MockFixedFeeStrategy(37, 23);
        bytes32 feeAndFlags = bytes32(uint256(uint160(address(strategy))));
        vm.store(Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT, feeAndFlags);

        uint256 rate = _localRate(Deployments.AAVE_USDC_RESERVE_BASE);
        uint256 shares = 9_876_543e6;
        (uint256 asset, uint256 total, uint256 gross, uint256 fee) =
            IGsm(Deployments.GSM_USDC).getGhoAmountForSellAsset(shares);
        AaveModel.Quote memory expected = AaveModel.getGhoAmountForSellAsset(shares, rate, 23);
        _assertQuote(asset, total, gross, fee, expected);
    }

    function test_mutatingPackedGsmExposureChangesCapacityGetters() external {
        uint128 cap = 123_456_789e6;
        uint128 current = 23_456_789e6;
        vm.store(Deployments.GSM_USDC, Deployments.GSM_EXPOSURE_SLOT, bytes32(uint256(cap) | (uint256(current) << 128)));
        assertEq(IGsm(Deployments.GSM_USDC).getExposureCap(), cap);
        assertEq(IGsm(Deployments.GSM_USDC).getAvailableLiquidity(), current);
        assertEq(IGsm(Deployments.GSM_USDC).getAvailableUnderlyingExposure(), cap - current);
    }

    function test_mutatingPackedReserveUsageChangesCapacityGetter() external {
        uint128 limit = 210_000_000 ether;
        uint128 used = 42_000_000 ether;
        vm.store(
            Deployments.GHO_RESERVE,
            Deployments.GHO_RESERVE_USDC_USAGE_SLOT,
            bytes32(uint256(limit) | (uint256(used) << 128))
        );
        (uint256 actualLimit, uint256 actualUsed) = IGhoReserve(Deployments.GHO_RESERVE).getUsage(Deployments.GSM_USDC);
        assertEq(actualLimit, limit);
        assertEq(actualUsed, used);
    }

    function test_mutatingPackedGsmFlagsDisablesSwaps() external {
        bytes32 feeAndFlags = bytes32(uint256(uint160(Deployments.GSM_USDC_FEE_STRATEGY)));
        bytes32 frozen = bytes32(uint256(feeAndFlags) | (uint256(1) << 160));
        vm.store(Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT, frozen);
        assertTrue(IGsm(Deployments.GSM_USDC).getIsFrozen());
        assertFalse(IGsm(Deployments.GSM_USDC).canSwap());

        bytes32 seized = bytes32(uint256(feeAndFlags) | (uint256(1) << 168));
        vm.store(Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT, seized);
        assertTrue(IGsm(Deployments.GSM_USDC).getIsSeized());
        assertFalse(IGsm(Deployments.GSM_USDC).canSwap());
    }

    function test_mutatingUsddVatMappingCoordinatesChangesTypedGetters() external {
        bytes32 ilkBase = keccak256(abi.encode(Deployments.USDD_USDT_ILK, uint256(2)));
        bytes32 urnBase = keccak256(
            abi.encode(Deployments.USDD_USDT_PSM, keccak256(abi.encode(Deployments.USDD_USDT_ILK, uint256(3))))
        );
        _assertWatched(Deployments.USDD_VAT, ilkBase, "derived USDT ilk base missing");
        _assertWatched(Deployments.USDD_VAT, urnBase, "derived USDT urn base missing");

        uint256[5] memory ilkValues = [uint256(101), 1e27, 2e27, 3e45, 4e45];
        for (uint256 i; i < ilkValues.length; ++i) {
            vm.store(Deployments.USDD_VAT, bytes32(uint256(ilkBase) + i), bytes32(ilkValues[i]));
        }
        vm.store(Deployments.USDD_VAT, urnBase, bytes32(uint256(77)));
        vm.store(Deployments.USDD_VAT, bytes32(uint256(urnBase) + 1), bytes32(uint256(88)));

        (uint256 art, uint256 rate, uint256 spot, uint256 line, uint256 dust) =
            IVatLike(Deployments.USDD_VAT).ilks(Deployments.USDD_USDT_ILK);
        assertEq(art, ilkValues[0]);
        assertEq(rate, ilkValues[1]);
        assertEq(spot, ilkValues[2]);
        assertEq(line, ilkValues[3]);
        assertEq(dust, ilkValues[4]);
        (uint256 ink, uint256 urnArt) =
            IVatLike(Deployments.USDD_VAT).urns(Deployments.USDD_USDT_ILK, Deployments.USDD_USDT_PSM);
        assertEq(ink, 77);
        assertEq(urnArt, 88);
    }

    function _localRate(bytes32 reserveBase) private view returns (uint256) {
        return AaveModel.normalizedIncome(
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 1)),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 3)),
            block.timestamp
        );
    }

    function _assertLocalMaxDeposit(uint256 exactExpected) private view {
        bytes32 base = Deployments.AAVE_USDC_RESERVE_BASE;
        uint256 expected = AaveModel.maxDeposit(
            vm.load(Deployments.AAVE_POOL, base),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(base) + 1)),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(base) + 3)),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(base) + 8)),
            uint256(vm.load(Deployments.AAVE_A_USDC, bytes32(uint256(54)))),
            block.timestamp
        );
        assertEq(IERC4626Like(Deployments.WA_ETH_USDC).maxDeposit(address(0)), expected);
        if (exactExpected != type(uint256).max) assertEq(expected, exactExpected);
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
