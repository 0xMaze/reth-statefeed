// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Math} from "@openzeppelin-contracts-5.4.0/utils/math/Math.sol";
import {AaveModel, PsmModel, UsddCapacityModel} from "../src/ConversionModels.sol";
import {Deployments} from "../src/Deployments.sol";
import {
    IDaiUsds,
    IERC20Like,
    IERC4626Like,
    IAavePoolLike,
    IGsm,
    ILitePsm,
    IUsddPsm,
    IVatLike
} from "../src/Interfaces.sol";
import {ForkTestBase} from "./ForkTestBase.t.sol";

contract DifferentialQuotesTest is ForkTestBase {
    uint256 private constant ONE_USDC = 1e6;

    function testFuzz_skyLitePsmSellMatchesLocalModel(uint64 rawAmount) external {
        uint256 gemAmount = bound(uint256(rawAmount), 1, 10_000_000e6);
        uint256 tin = uint256(vm.load(Deployments.SKY_LITE_PSM, bytes32(uint256(3))));
        uint256 expected = PsmModel.sell(gemAmount, tin);

        deal(Deployments.USDC, address(this), gemAmount);
        _approve(Deployments.USDC, Deployments.SKY_LITE_PSM, gemAmount);

        uint256 beforeDai = IERC20Like(Deployments.DAI).balanceOf(address(this));
        uint256 returned = ILitePsm(Deployments.SKY_LITE_PSM).sellGem(address(this), gemAmount);
        uint256 received = IERC20Like(Deployments.DAI).balanceOf(address(this)) - beforeDai;

        assertEq(returned, expected, "LitePSM sell return differs");
        assertEq(received, expected, "LitePSM sell balance delta differs");
    }

    function testFuzz_skyLitePsmBuyMatchesLocalModel(uint64 rawAmount) external {
        uint256 gemAmount = bound(uint256(rawAmount), 1, 10_000_000e6);
        uint256 tout = uint256(vm.load(Deployments.SKY_LITE_PSM, bytes32(uint256(4))));
        uint256 expected = PsmModel.buy(gemAmount, tout);

        deal(Deployments.DAI, address(this), expected);
        _approve(Deployments.DAI, Deployments.SKY_LITE_PSM, expected);

        uint256 beforeUsdc = IERC20Like(Deployments.USDC).balanceOf(address(this));
        uint256 returned = ILitePsm(Deployments.SKY_LITE_PSM).buyGem(address(this), gemAmount);
        uint256 received = IERC20Like(Deployments.USDC).balanceOf(address(this)) - beforeUsdc;

        assertEq(returned, expected, "LitePSM buy return differs");
        assertEq(received, gemAmount, "LitePSM buy balance delta differs");
    }

    function test_daiUsdsExecutesOneToOneInBothDirections() external {
        uint256 amount = 137_123.456789 ether;

        deal(Deployments.DAI, address(this), amount);
        _approve(Deployments.DAI, Deployments.DAI_USDS, amount);
        uint256 beforeUsds = IERC20Like(Deployments.USDS).balanceOf(address(this));
        IDaiUsds(Deployments.DAI_USDS).daiToUsds(address(this), amount);
        assertEq(IERC20Like(Deployments.USDS).balanceOf(address(this)) - beforeUsds, amount);

        _approve(Deployments.USDS, Deployments.DAI_USDS, amount);
        uint256 beforeDai = IERC20Like(Deployments.DAI).balanceOf(address(this));
        IDaiUsds(Deployments.DAI_USDS).usdsToDai(address(this), amount);
        assertEq(IERC20Like(Deployments.DAI).balanceOf(address(this)) - beforeDai, amount);
    }

    function test_skyUsdsWrapperExecutesAgainstSameLitePsmState() external {
        uint256 gemAmount = 123_456e6;
        uint256 tin = uint256(vm.load(Deployments.SKY_LITE_PSM, bytes32(uint256(3))));
        uint256 tout = uint256(vm.load(Deployments.SKY_LITE_PSM, bytes32(uint256(4))));

        deal(Deployments.USDC, address(this), gemAmount);
        _approve(Deployments.USDC, Deployments.SKY_USDS_PSM, gemAmount);
        uint256 beforeUsds = IERC20Like(Deployments.USDS).balanceOf(address(this));
        uint256 usdsOut = ILitePsm(Deployments.SKY_USDS_PSM).sellGem(address(this), gemAmount);
        assertEq(usdsOut, PsmModel.sell(gemAmount, tin));
        assertEq(IERC20Like(Deployments.USDS).balanceOf(address(this)) - beforeUsds, usdsOut);

        uint256 usdsIn = PsmModel.buy(gemAmount, tout);
        deal(Deployments.USDS, address(this), usdsIn);
        _approve(Deployments.USDS, Deployments.SKY_USDS_PSM, usdsIn);
        uint256 beforeUsdc = IERC20Like(Deployments.USDC).balanceOf(address(this));
        uint256 returned = ILitePsm(Deployments.SKY_USDS_PSM).buyGem(address(this), gemAmount);
        assertEq(returned, usdsIn);
        assertEq(IERC20Like(Deployments.USDC).balanceOf(address(this)) - beforeUsdc, gemAmount);
    }

    function test_usddPsmSellBalanceDeltasMatchLocalModel() external {
        _assertUsddSell(Deployments.USDD_USDT_PSM, Deployments.USDT, Deployments.USDD_USDT_JOIN, 10_000e6);
        _assertUsddSell(Deployments.USDD_USDC_PSM, Deployments.USDC, Deployments.USDD_USDC_JOIN, ONE_USDC);
    }

    function test_usddPsmBuyBalanceDeltasMatchLocalModel() external {
        _assertUsddBuy(Deployments.USDD_USDT_PSM, Deployments.USDT, 1_000e6);
        // The anchored USDC urn has only about 0.010381 USDC of buy-side liquidity.
        _assertUsddBuy(Deployments.USDD_USDC_PSM, Deployments.USDC, 1_000);
    }

    function test_usddUsdtPsmSellCapacityMatchesVatBoundary() external {
        _assertUsddSellCapacity(
            Deployments.USDD_USDT_PSM, Deployments.USDT, Deployments.USDD_USDT_JOIN, Deployments.USDD_USDT_ILK
        );
    }

    function test_usddUsdcPsmSellCapacityMatchesVatBoundary() external {
        _assertUsddSellCapacity(
            Deployments.USDD_USDC_PSM, Deployments.USDC, Deployments.USDD_USDC_JOIN, Deployments.USDD_USDC_ILK
        );
    }

    function test_usddUsdtPsmBuyCapacityMatchesVatAndInventoryBoundary() external {
        _assertUsddBuyCapacity(
            Deployments.USDD_USDT_PSM, Deployments.USDT, Deployments.USDD_USDT_JOIN, Deployments.USDD_USDT_ILK
        );
    }

    function test_usddUsdcPsmBuyCapacityMatchesVatAndInventoryBoundary() external {
        _assertUsddBuyCapacity(
            Deployments.USDD_USDC_PSM, Deployments.USDC, Deployments.USDD_USDC_JOIN, Deployments.USDD_USDC_ILK
        );
    }

    function testFuzz_aaveUsdcGsmQuotesMatchLocalModel(uint64 rawShares, uint128 rawGho) external view {
        uint256 shares = bound(uint256(rawShares), 0, 100_000_000e6);
        uint256 gho = bound(uint256(rawGho), 0, 100_000_000 ether);
        _assertGsmQuotes(Deployments.GSM_USDC, Deployments.AAVE_USDC_RESERVE_BASE, 10, shares, gho);
    }

    function testFuzz_aaveUsdtGsmQuotesMatchLocalModel(uint64 rawShares, uint128 rawGho) external view {
        uint256 shares = bound(uint256(rawShares), 0, 100_000_000e6);
        uint256 gho = bound(uint256(rawGho), 0, 100_000_000 ether);
        _assertGsmQuotes(Deployments.GSM_USDT, Deployments.AAVE_USDT_RESERVE_BASE, 15, shares, gho);
    }

    function testFuzz_aaveWrappersMatchLocalRateModel(uint64 rawAmount) external view {
        uint256 amount = bound(uint256(rawAmount), 0, 1_000_000_000e6);
        _assertWrapper(Deployments.WA_ETH_USDC, Deployments.USDC, Deployments.AAVE_USDC_RESERVE_BASE, amount);
        _assertWrapper(Deployments.WA_ETH_USDT, Deployments.USDT, Deployments.AAVE_USDT_RESERVE_BASE, amount);
    }

    function test_aaveWrapperMaxDepositMatchesMinimalLocalStateModel() external view {
        _assertMaxDeposit(Deployments.WA_ETH_USDC, Deployments.AAVE_A_USDC, Deployments.AAVE_USDC_RESERVE_BASE);
        _assertMaxDeposit(Deployments.WA_ETH_USDT, Deployments.AAVE_A_USDT, Deployments.AAVE_USDT_RESERVE_BASE);
    }

    function _assertUsddSell(address psm, address gem, address join, uint256 gemAmount) private {
        uint256 tin = uint256(vm.load(psm, bytes32(uint256(1))));
        uint256 expected = PsmModel.sell(gemAmount, tin);

        deal(gem, address(this), gemAmount);
        _approve(gem, join, gemAmount);
        uint256 beforeUsdd = IERC20Like(Deployments.USDD).balanceOf(address(this));
        IUsddPsm(psm).sellGem(address(this), gemAmount);

        assertEq(IERC20Like(Deployments.USDD).balanceOf(address(this)) - beforeUsdd, expected);
    }

    function _assertUsddBuy(address psm, address gem, uint256 gemAmount) private {
        uint256 tout = uint256(vm.load(psm, bytes32(uint256(2))));
        uint256 expected = PsmModel.buy(gemAmount, tout);

        deal(Deployments.USDD, address(this), expected);
        _approve(Deployments.USDD, psm, expected);
        uint256 beforeGem = IERC20Like(gem).balanceOf(address(this));
        IUsddPsm(psm).buyGem(address(this), gemAmount);

        assertEq(IERC20Like(gem).balanceOf(address(this)) - beforeGem, gemAmount);
    }

    function _assertUsddSellCapacity(address psm, address gem, address join, bytes32 ilk) private {
        UsddCapacityModel.VatState memory state = _usddVatState(psm, ilk);
        uint256 capacity = UsddCapacityModel.sellCapacity(state);
        assertGt(capacity, 0, "expected non-zero sell capacity at anchor");

        deal(gem, address(this), capacity + 1);
        _approve(gem, join, capacity + 1);
        vm.expectRevert();
        IUsddPsm(psm).sellGem(address(this), capacity + 1);

        IUsddPsm(psm).sellGem(address(this), capacity);
    }

    function _assertUsddBuyCapacity(address psm, address gem, address join, bytes32 ilk) private {
        UsddCapacityModel.VatState memory state = _usddVatState(psm, ilk);
        uint256 capacity = UsddCapacityModel.buyCapacity(state, IERC20Like(gem).balanceOf(join));
        assertGt(capacity, 0, "expected non-zero buy capacity at anchor");

        uint256 usddRequired = PsmModel.buy(capacity + 1, uint256(vm.load(psm, bytes32(uint256(2)))));
        deal(Deployments.USDD, address(this), usddRequired);
        _approve(Deployments.USDD, psm, usddRequired);
        vm.expectRevert();
        IUsddPsm(psm).buyGem(address(this), capacity + 1);

        IUsddPsm(psm).buyGem(address(this), capacity);
    }

    function _usddVatState(address psm, bytes32 ilk) private view returns (UsddCapacityModel.VatState memory state) {
        IVatLike vat = IVatLike(Deployments.USDD_VAT);
        (state.art, state.rate, state.spot, state.line, state.dust) = vat.ilks(ilk);
        (state.urnInk, state.urnArt) = vat.urns(ilk, psm);
        state.debt = vat.debt();
        state.globalLine = vat.Line();
    }

    function _assertGsmQuotes(address gsm, bytes32 reserveBase, uint256 buyFeeBps, uint256 shares, uint256 gho)
        private
        view
    {
        uint256 rate = _localRate(reserveBase);
        AaveModel.Quote memory expected;
        (uint256 a, uint256 t, uint256 g, uint256 f) = IGsm(gsm).getGhoAmountForBuyAsset(shares);
        expected = AaveModel.getGhoAmountForBuyAsset(shares, rate, buyFeeBps);
        _assertQuote(a, t, g, f, expected, "GSM buy-by-asset");

        (a, t, g, f) = IGsm(gsm).getGhoAmountForSellAsset(shares);
        expected = AaveModel.getGhoAmountForSellAsset(shares, rate, 0);
        _assertQuote(a, t, g, f, expected, "GSM sell-by-asset");

        (a, t, g, f) = IGsm(gsm).getAssetAmountForBuyAsset(gho);
        expected = AaveModel.getAssetAmountForBuyAsset(gho, rate, buyFeeBps);
        _assertQuote(a, t, g, f, expected, "GSM buy-by-GHO");

        (a, t, g, f) = IGsm(gsm).getAssetAmountForSellAsset(gho);
        expected = AaveModel.getAssetAmountForSellAsset(gho, rate, 0);
        _assertQuote(a, t, g, f, expected, "GSM sell-by-GHO");
    }

    function _assertWrapper(address wrapper, address asset, bytes32 reserveBase, uint256 amount) private view {
        uint256 rate = _localRate(reserveBase);
        assertEq(IAavePoolLike(Deployments.AAVE_POOL).getReserveNormalizedIncome(asset), rate);
        assertEq(
            IERC4626Like(wrapper).convertToAssets(amount), AaveModel.convertToAssets(amount, rate, Math.Rounding.Floor)
        );
        assertEq(IERC4626Like(wrapper).previewMint(amount), AaveModel.convertToAssets(amount, rate, Math.Rounding.Ceil));
        assertEq(
            IERC4626Like(wrapper).convertToShares(amount), AaveModel.convertToShares(amount, rate, Math.Rounding.Floor)
        );
        assertEq(
            IERC4626Like(wrapper).previewWithdraw(amount), AaveModel.convertToShares(amount, rate, Math.Rounding.Ceil)
        );
    }

    function testFuzz_composedAaveBaseTokenToGhoEdgesMatchLocalModel(uint64 rawAssets) external view {
        uint256 assets = bound(uint256(rawAssets), 0, 50_000_000e6);
        _assertBaseToGho(Deployments.WA_ETH_USDC, Deployments.GSM_USDC, Deployments.AAVE_USDC_RESERVE_BASE, assets);
        _assertBaseToGho(Deployments.WA_ETH_USDT, Deployments.GSM_USDT, Deployments.AAVE_USDT_RESERVE_BASE, assets);
    }

    function testFuzz_composedAaveGhoToBaseTokenEdgesMatchLocalModel(uint128 rawGho) external view {
        uint256 gho = bound(uint256(rawGho), 0, 50_000_000 ether);
        _assertGhoToBase(Deployments.WA_ETH_USDC, Deployments.GSM_USDC, Deployments.AAVE_USDC_RESERVE_BASE, 10, gho);
        _assertGhoToBase(Deployments.WA_ETH_USDT, Deployments.GSM_USDT, Deployments.AAVE_USDT_RESERVE_BASE, 15, gho);
    }

    function _localRate(bytes32 reserveBase) private view returns (uint256) {
        bytes32 indexAndRate = vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 1));
        bytes32 timestamps = vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 3));
        return AaveModel.normalizedIncome(indexAndRate, timestamps, block.timestamp);
    }

    function _assertMaxDeposit(address wrapper, address aToken, bytes32 reserveBase) private view {
        uint256 expected = AaveModel.maxDeposit(
            vm.load(Deployments.AAVE_POOL, reserveBase),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 1)),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 3)),
            vm.load(Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 8)),
            uint256(vm.load(aToken, bytes32(uint256(54)))),
            block.timestamp
        );
        assertEq(IERC4626Like(wrapper).maxDeposit(address(0)), expected);
    }

    function _assertBaseToGho(address wrapper, address gsm, bytes32 reserveBase, uint256 assets) private view {
        uint256 rate = _localRate(reserveBase);
        uint256 actualShares = IERC4626Like(wrapper).previewDeposit(assets);
        uint256 expectedShares = AaveModel.convertToShares(assets, rate, Math.Rounding.Floor);
        assertEq(actualShares, expectedShares, "base-to-wrapper shares");

        (uint256 actualAsset, uint256 actualGho, uint256 actualGross, uint256 actualFee) =
            IGsm(gsm).getGhoAmountForSellAsset(actualShares);
        AaveModel.Quote memory expected = AaveModel.getGhoAmountForSellAsset(expectedShares, rate, 0);
        _assertQuote(actualAsset, actualGho, actualGross, actualFee, expected, "base-to-GHO");
    }

    function _assertGhoToBase(address wrapper, address gsm, bytes32 reserveBase, uint256 buyFeeBps, uint256 gho)
        private
        view
    {
        uint256 rate = _localRate(reserveBase);
        (uint256 actualShares, uint256 actualTotal, uint256 actualGross, uint256 actualFee) =
            IGsm(gsm).getAssetAmountForBuyAsset(gho);
        AaveModel.Quote memory expected = AaveModel.getAssetAmountForBuyAsset(gho, rate, buyFeeBps);
        _assertQuote(actualShares, actualTotal, actualGross, actualFee, expected, "GHO-to-wrapper");

        uint256 actualAssets = IERC4626Like(wrapper).previewRedeem(actualShares);
        uint256 expectedAssets = AaveModel.convertToAssets(expected.assetAmount, rate, Math.Rounding.Floor);
        assertEq(actualAssets, expectedAssets, "wrapper-to-base assets");
    }

    function _assertQuote(
        uint256 asset,
        uint256 total,
        uint256 gross,
        uint256 fee,
        AaveModel.Quote memory expected,
        string memory label
    ) private pure {
        assertEq(asset, expected.assetAmount, string.concat(label, ": asset"));
        assertEq(total, expected.totalGho, string.concat(label, ": total"));
        assertEq(gross, expected.grossGho, string.concat(label, ": gross"));
        assertEq(fee, expected.fee, string.concat(label, ": fee"));
    }
}
