// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Deployments} from "../src/Deployments.sol";
import {IERC20Like, IERC4626Like, IGhoReserve, IGsm, ILitePsm, IUsddPsm, IVatLike} from "../src/Interfaces.sol";
import {ForkTestBase} from "./ForkTestBase.t.sol";

interface IAuthLike {
    function wards(address account) external view returns (uint256);
}

interface ICanLike {
    function can(address source, address destination) external view returns (uint256);
}

interface ILiveLike {
    function live() external view returns (uint256);
}

interface IImplementationAllowlist {
    function implementations(address implementation) external view returns (uint256);
}

interface IUsdtGuards {
    function paused() external view returns (bool);
    function deprecated() external view returns (bool);
    function upgradedAddress() external view returns (address);
    function basisPointsRate() external view returns (uint256);
    function maximumFee() external view returns (uint256);
}

contract SlotCoverageTest is ForkTestBase {
    bytes32 private constant MOCK_STABLE_DEBT_PROVIDER_SLOT =
        0xb035f62398c2f37b04e1eceb7c8e682f004b880b099118069ef0a8d3cb0fcdae;

    function test_everyManifestCoordinateIsExercisedByValidationScenario() external {
        vm.record();
        _touchGsmQuotes(Deployments.GSM_USDC);
        _touchGsmQuotes(Deployments.GSM_USDT);
        _touchGsmCapacity(Deployments.GSM_USDC);
        _touchGsmCapacity(Deployments.GSM_USDT);
        IGhoReserve(Deployments.GHO_RESERVE).getUsage(Deployments.GSM_USDC);
        IGhoReserve(Deployments.GHO_RESERVE).getUsage(Deployments.GSM_USDT);
        IERC20Like(Deployments.GHO).balanceOf(Deployments.GHO_RESERVE);
        IERC4626Like(Deployments.WA_ETH_USDC).maxDeposit(address(0));
        IERC4626Like(Deployments.WA_ETH_USDT).maxDeposit(address(0));

        ILitePsm(Deployments.SKY_LITE_PSM).tin();
        ILitePsm(Deployments.SKY_LITE_PSM).tout();
        IERC20Like(Deployments.DAI).balanceOf(Deployments.SKY_LITE_PSM);
        IERC20Like(Deployments.USDC).balanceOf(Deployments.SKY_LITE_PSM_POCKET);
        IERC20Like(Deployments.USDC).allowance(Deployments.SKY_LITE_PSM_POCKET, Deployments.SKY_LITE_PSM);
        ILiveLike(Deployments.SKY_DAI_JOIN).live();
        IAuthLike(Deployments.USDS).wards(Deployments.SKY_USDS_JOIN);
        IAuthLike(Deployments.DAI).wards(Deployments.SKY_DAI_JOIN);
        ICanLike(Deployments.SKY_VAT).can(Deployments.DAI_USDS, Deployments.SKY_DAI_JOIN);
        ICanLike(Deployments.SKY_VAT).can(Deployments.DAI_USDS, Deployments.SKY_USDS_JOIN);

        _touchUsddPsm(Deployments.USDD_USDT_PSM, Deployments.USDD_USDT_ILK);
        _touchUsddPsm(Deployments.USDD_USDC_PSM, Deployments.USDD_USDC_ILK);
        IERC20Like(Deployments.USDT).balanceOf(Deployments.USDD_USDT_JOIN);
        IERC20Like(Deployments.USDC).balanceOf(Deployments.USDD_USDC_JOIN);
        ILiveLike(Deployments.USDD_USDT_JOIN).live();
        ILiveLike(Deployments.USDD_USDC_JOIN).live();
        ILiveLike(Deployments.USDD_JOIN).live();
        IAuthLike(Deployments.USDD_USDT_JOIN).wards(Deployments.USDD_USDT_PSM);
        IAuthLike(Deployments.USDD_USDC_JOIN).wards(Deployments.USDD_USDC_PSM);
        IAuthLike(Deployments.USDD_VAT).wards(Deployments.USDD_USDT_JOIN);
        IAuthLike(Deployments.USDD_VAT).wards(Deployments.USDD_USDC_JOIN);
        IAuthLike(Deployments.USDD).wards(Deployments.USDD_JOIN);
        ICanLike(Deployments.USDD_VAT).can(Deployments.USDD_USDT_PSM, Deployments.USDD_JOIN);
        ICanLike(Deployments.USDD_VAT).can(Deployments.USDD_USDC_PSM, Deployments.USDD_JOIN);
        IImplementationAllowlist(Deployments.USDD_USDT_JOIN).implementations(address(0));
        IUsdtGuards(Deployments.USDT).paused();
        IUsdtGuards(Deployments.USDT).deprecated();
        IUsdtGuards(Deployments.USDT).upgradedAddress();
        IUsdtGuards(Deployments.USDT).basisPointsRate();
        IUsdtGuards(Deployments.USDT).maximumFee();
        IUsdtGuards(Deployments.USDC).paused();

        for (uint256 i; i < watches.length; ++i) {
            (bytes32[] memory reads,) = vm.accesses(watches[i].account);
            assertTrue(
                _contains(reads, watches[i].slot), string.concat("manifest coordinate not exercised: ", watches[i].id)
            );
        }
    }

    function test_aaveQuoteReadSetIsFullyRepresentedByManifest() external {
        vm.record();
        _touchGsmQuotes(Deployments.GSM_USDC);
        _touchGsmQuotes(Deployments.GSM_USDT);

        _assertOnlyWatchedReads(Deployments.GSM_USDC);
        _assertOnlyWatchedReads(Deployments.GSM_USDT);
        _assertOnlyWatchedReads(Deployments.WA_ETH_USDC);
        _assertOnlyWatchedReads(Deployments.WA_ETH_USDT);
        _assertOnlyWatchedReads(Deployments.AAVE_POOL);
    }

    function test_aaveCapacityReadSetIsFullyRepresentedByManifest() external {
        vm.record();
        _touchGsmCapacity(Deployments.GSM_USDC);
        _touchGsmCapacity(Deployments.GSM_USDT);
        IGhoReserve(Deployments.GHO_RESERVE).getUsage(Deployments.GSM_USDC);
        IGhoReserve(Deployments.GHO_RESERVE).getUsage(Deployments.GSM_USDT);
        IERC20Like(Deployments.GHO).balanceOf(Deployments.GHO_RESERVE);
        IERC4626Like(Deployments.WA_ETH_USDC).maxDeposit(address(0));
        IERC4626Like(Deployments.WA_ETH_USDT).maxDeposit(address(0));

        _assertOnlyWatchedReads(Deployments.GSM_USDC);
        _assertOnlyWatchedReads(Deployments.GSM_USDT);
        _assertOnlyWatchedReads(Deployments.GHO_RESERVE);
        _assertOnlyWatchedReads(Deployments.GHO);
        _assertOnlyWatchedReads(Deployments.WA_ETH_USDC);
        _assertOnlyWatchedReads(Deployments.WA_ETH_USDT);
        _assertOnlyWatchedOrIgnoredAaveReads(Deployments.AAVE_POOL);
        _assertOnlyWatchedReads(Deployments.AAVE_A_USDC);
        _assertOnlyWatchedReads(Deployments.AAVE_A_USDT);
        _assertOnlyWatchedOrIgnoredAaveReads(Deployments.AAVE_POOL_ADDRESSES_PROVIDER);
        _assertRead(
            Deployments.AAVE_POOL_ADDRESSES_PROVIDER,
            MOCK_STABLE_DEBT_PROVIDER_SLOT,
            "expected ignored Aave provider read missing"
        );
        _assertIgnoredReserveReads(Deployments.AAVE_USDC_RESERVE_BASE);
        _assertIgnoredReserveReads(Deployments.AAVE_USDT_RESERVE_BASE);
    }

    function test_skyQuoteCapacityAndGuardReadSetIsFullyRepresentedByManifest() external {
        vm.record();
        ILitePsm(Deployments.SKY_LITE_PSM).tin();
        ILitePsm(Deployments.SKY_LITE_PSM).tout();
        IERC20Like(Deployments.DAI).balanceOf(Deployments.SKY_LITE_PSM);
        IERC20Like(Deployments.USDC).balanceOf(Deployments.SKY_LITE_PSM_POCKET);
        IERC20Like(Deployments.USDC).allowance(Deployments.SKY_LITE_PSM_POCKET, Deployments.SKY_LITE_PSM);
        ILiveLike(Deployments.SKY_DAI_JOIN).live();
        IAuthLike(Deployments.USDS).wards(Deployments.SKY_USDS_JOIN);
        IAuthLike(Deployments.DAI).wards(Deployments.SKY_DAI_JOIN);
        ICanLike(Deployments.SKY_VAT).can(Deployments.DAI_USDS, Deployments.SKY_DAI_JOIN);
        ICanLike(Deployments.SKY_VAT).can(Deployments.DAI_USDS, Deployments.SKY_USDS_JOIN);

        _assertOnlyWatchedReads(Deployments.SKY_LITE_PSM);
        _assertOnlyWatchedReads(Deployments.DAI);
        _assertOnlyWatchedReads(Deployments.USDS);
        _assertOnlyWatchedReads(Deployments.USDC);
        _assertOnlyWatchedReads(Deployments.SKY_DAI_JOIN);
        _assertOnlyWatchedReads(Deployments.SKY_VAT);
    }

    function test_usddQuoteCapacityAndGuardReadSetIsFullyRepresentedByManifest() external {
        vm.record();
        _touchUsddPsm(Deployments.USDD_USDT_PSM, Deployments.USDD_USDT_ILK);
        _touchUsddPsm(Deployments.USDD_USDC_PSM, Deployments.USDD_USDC_ILK);

        IERC20Like(Deployments.USDT).balanceOf(Deployments.USDD_USDT_JOIN);
        IERC20Like(Deployments.USDC).balanceOf(Deployments.USDD_USDC_JOIN);
        ILiveLike(Deployments.USDD_USDT_JOIN).live();
        ILiveLike(Deployments.USDD_USDC_JOIN).live();
        ILiveLike(Deployments.USDD_JOIN).live();
        IAuthLike(Deployments.USDD_USDT_JOIN).wards(Deployments.USDD_USDT_PSM);
        IAuthLike(Deployments.USDD_USDC_JOIN).wards(Deployments.USDD_USDC_PSM);
        IAuthLike(Deployments.USDD_VAT).wards(Deployments.USDD_USDT_JOIN);
        IAuthLike(Deployments.USDD_VAT).wards(Deployments.USDD_USDC_JOIN);
        IAuthLike(Deployments.USDD).wards(Deployments.USDD_JOIN);
        ICanLike(Deployments.USDD_VAT).can(Deployments.USDD_USDT_PSM, Deployments.USDD_JOIN);
        ICanLike(Deployments.USDD_VAT).can(Deployments.USDD_USDC_PSM, Deployments.USDD_JOIN);
        IImplementationAllowlist(Deployments.USDD_USDT_JOIN).implementations(address(0));
        IUsdtGuards(Deployments.USDT).paused();
        IUsdtGuards(Deployments.USDT).deprecated();
        IUsdtGuards(Deployments.USDT).upgradedAddress();
        IUsdtGuards(Deployments.USDT).basisPointsRate();
        IUsdtGuards(Deployments.USDT).maximumFee();
        IUsdtGuards(Deployments.USDC).paused();

        _assertOnlyWatchedReads(Deployments.USDD_USDT_PSM);
        _assertOnlyWatchedReads(Deployments.USDD_USDC_PSM);
        _assertOnlyWatchedReads(Deployments.USDD_VAT);
        _assertOnlyWatchedReads(Deployments.USDD_USDT_JOIN);
        _assertOnlyWatchedReads(Deployments.USDD_USDC_JOIN);
        _assertOnlyWatchedReads(Deployments.USDD_JOIN);
        _assertOnlyWatchedReads(Deployments.USDD);
        _assertOnlyWatchedReads(Deployments.USDT);
        _assertOnlyWatchedReads(Deployments.USDC);
    }

    function _touchGsmQuotes(address gsm) private view {
        IGsm(gsm).getGhoAmountForBuyAsset(123_456e6);
        IGsm(gsm).getGhoAmountForSellAsset(123_456e6);
        IGsm(gsm).getAssetAmountForBuyAsset(123_456 ether);
        IGsm(gsm).getAssetAmountForSellAsset(123_456 ether);
    }

    function _touchGsmCapacity(address gsm) private view {
        IGsm(gsm).getAvailableUnderlyingExposure();
        IGsm(gsm).getAvailableLiquidity();
        IGsm(gsm).getExposureCap();
        IGsm(gsm).getUsed();
        IGsm(gsm).getLimit();
        IGsm(gsm).getFeeStrategy();
        IGsm(gsm).getGhoReserve();
        IGsm(gsm).getIsFrozen();
        IGsm(gsm).getIsSeized();
    }

    function _touchUsddPsm(address psm, bytes32 ilk) private view {
        IUsddPsm(psm).tin();
        IUsddPsm(psm).tout();
        IUsddPsm(psm).sellEnabled();
        IUsddPsm(psm).buyEnabled();
        IVatLike(Deployments.USDD_VAT).ilks(ilk);
        IVatLike(Deployments.USDD_VAT).urns(ilk, psm);
        IVatLike(Deployments.USDD_VAT).debt();
        IVatLike(Deployments.USDD_VAT).Line();
        IVatLike(Deployments.USDD_VAT).live();
    }

    function _assertOnlyWatchedOrIgnoredAaveReads(address account) private {
        (bytes32[] memory reads,) = vm.accesses(account);
        for (uint256 i; i < reads.length; ++i) {
            assertTrue(
                _isWatched(account, reads[i]) || _isIgnoredAaveRead(account, reads[i]),
                string.concat("unexpected Aave read at ", vm.toString(account), "/", vm.toString(reads[i]))
            );
        }
    }

    function _isIgnoredAaveRead(address account, bytes32 slot) private pure returns (bool) {
        if (account == Deployments.AAVE_POOL_ADDRESSES_PROVIDER) {
            return slot == MOCK_STABLE_DEBT_PROVIDER_SLOT;
        }
        if (account != Deployments.AAVE_POOL) return false;

        return slot == bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 2)
            || slot == bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 6)
            || slot == bytes32(uint256(Deployments.AAVE_USDT_RESERVE_BASE) + 2)
            || slot == bytes32(uint256(Deployments.AAVE_USDT_RESERVE_BASE) + 6);
    }

    function _assertIgnoredReserveReads(bytes32 reserveBase) private {
        _assertRead(
            Deployments.AAVE_POOL, bytes32(uint256(reserveBase) + 2), "expected ignored variable-borrow read missing"
        );
        _assertRead(
            Deployments.AAVE_POOL,
            bytes32(uint256(reserveBase) + 6),
            "expected ignored variable-debt-token read missing"
        );
    }
}
