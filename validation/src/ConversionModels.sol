// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Math} from "@openzeppelin-contracts-5.4.0/utils/math/Math.sol";

library PsmModel {
    uint256 internal constant WAD = 1e18;
    uint256 internal constant TO_18 = 1e12;

    function sell(uint256 gemAmount, uint256 tin) internal pure returns (uint256) {
        uint256 gross = gemAmount * TO_18;
        return gross - Math.mulDiv(gross, tin, WAD);
    }

    function buy(uint256 gemAmount, uint256 tout) internal pure returns (uint256) {
        uint256 gross = gemAmount * TO_18;
        return gross + Math.mulDiv(gross, tout, WAD);
    }

    function buyLiquidity(uint256 balance, uint256 allowance) internal pure returns (uint256) {
        return Math.min(balance, allowance);
    }
}

library UsddCapacityModel {
    uint256 internal constant TO_18 = 1e12;

    struct VatState {
        uint256 art;
        uint256 rate;
        uint256 spot;
        uint256 line;
        uint256 dust;
        uint256 debt;
        uint256 globalLine;
        uint256 urnInk;
        uint256 urnArt;
    }

    /// @dev Returns the largest whole six-decimal gem amount accepted by Vat.frob(+wad, +wad).
    function sellCapacity(VatState memory state) internal pure returns (uint256) {
        if (state.rate == 0) return 0;

        uint256 ilkDebt = state.art * state.rate;
        if (ilkDebt > state.line || state.debt > state.globalLine) return 0;

        uint256 maxDart = Math.min((state.line - ilkDebt) / state.rate, (state.globalLine - state.debt) / state.rate);
        uint256 minDart;

        uint256 collateralValue = state.urnInk * state.spot;
        uint256 urnDebt = state.urnArt * state.rate;
        if (state.spot < state.rate) {
            if (collateralValue < urnDebt) return 0;
            maxDart = Math.min(maxDart, (collateralValue - urnDebt) / (state.rate - state.spot));
        } else if (state.spot == state.rate) {
            if (collateralValue < urnDebt) return 0;
        } else if (collateralValue < urnDebt) {
            minDart = Math.ceilDiv(urnDebt - collateralValue, state.spot - state.rate);
        }

        uint256 gemCapacity = maxDart / TO_18;
        uint256 executableDart = gemCapacity * TO_18;
        if (executableDart < minDart) return 0;
        if ((state.urnArt + executableDart) * state.rate < state.dust) return 0;
        return gemCapacity;
    }

    /// @dev Returns the largest whole six-decimal gem amount accepted by Vat.frob(-wad, -wad)
    /// and backed by the GemJoin token inventory.
    function buyCapacity(VatState memory state, uint256 joinBalance) internal pure returns (uint256) {
        if (state.rate == 0) return 0;

        uint256 maxDart = Math.min(Math.min(state.urnInk, state.urnArt), state.art);
        uint256 joinDart = joinBalance > type(uint256).max / TO_18 ? type(uint256).max : joinBalance * TO_18;
        maxDart = Math.min(maxDart, joinDart);
        uint256 minDart;

        uint256 collateralValue = state.urnInk * state.spot;
        uint256 urnDebt = state.urnArt * state.rate;
        if (state.spot > state.rate) {
            if (collateralValue < urnDebt) return 0;
            maxDart = Math.min(maxDart, (collateralValue - urnDebt) / (state.spot - state.rate));
        } else if (state.spot == state.rate) {
            if (collateralValue < urnDebt) return 0;
        } else if (collateralValue < urnDebt) {
            minDart = Math.ceilDiv(urnDebt - collateralValue, state.rate - state.spot);
        }

        uint256 gemCapacity = maxDart / TO_18;
        uint256 executableDart = gemCapacity * TO_18;
        if (executableDart < minDart) return 0;

        if (executableDart < state.urnArt && (state.urnArt - executableDart) * state.rate < state.dust) {
            uint256 minimumResidualArt = Math.ceilDiv(state.dust, state.rate);
            if (minimumResidualArt >= state.urnArt) return 0;
            gemCapacity = Math.min(gemCapacity, (state.urnArt - minimumResidualArt) / TO_18);
        }
        return gemCapacity;
    }
}

library AaveModel {
    uint256 internal constant RAY = 1e27;
    uint256 internal constant HALF_RAY = 0.5e27;
    uint256 internal constant SECONDS_PER_YEAR = 365 days;
    uint256 internal constant BPS = 10_000;
    uint256 internal constant ASSET_UNITS = 1e6;
    uint256 internal constant PRICE_RATIO = 1e18;

    struct Quote {
        uint256 assetAmount;
        uint256 totalGho;
        uint256 grossGho;
        uint256 fee;
    }

    function unpackReserve(bytes32 indexAndRate, bytes32 deficitAndTimestamps)
        internal
        pure
        returns (uint128 liquidityIndex, uint128 liquidityRate, uint40 lastUpdateTimestamp)
    {
        uint256 indexAndRateWord = uint256(indexAndRate);
        liquidityIndex = uint128(indexAndRateWord);
        liquidityRate = uint128(indexAndRateWord >> 128);
        lastUpdateTimestamp = uint40(uint256(deficitAndTimestamps) >> 128);
    }

    function normalizedIncome(bytes32 indexAndRate, bytes32 deficitAndTimestamps, uint256 timestamp)
        internal
        pure
        returns (uint256)
    {
        (uint128 index, uint128 rate, uint40 lastUpdate) = unpackReserve(indexAndRate, deficitAndTimestamps);
        require(timestamp >= lastUpdate, "timestamp before reserve update");
        if (timestamp == lastUpdate) return index;

        uint256 linearInterest = RAY + (uint256(rate) * (timestamp - lastUpdate)) / SECONDS_PER_YEAR;
        return (linearInterest * uint256(index) + HALF_RAY) / RAY;
    }

    function convertToAssets(uint256 shares, uint256 rate, Math.Rounding rounding) internal pure returns (uint256) {
        return Math.mulDiv(shares, rate, RAY, rounding);
    }

    function convertToShares(uint256 assets, uint256 rate, Math.Rounding rounding) internal pure returns (uint256) {
        return Math.mulDiv(assets, RAY, rate, rounding);
    }

    function getGhoAmountForBuyAsset(uint256 shares, uint256 rate, uint256 buyFeeBps)
        internal
        pure
        returns (Quote memory quote)
    {
        uint256 gross = _assetPriceInGho(shares, rate, Math.Rounding.Ceil);
        uint256 total = gross + _fee(gross, buyFeeBps);
        uint256 finalGross = buyFeeBps == 0 ? total : Math.mulDiv(total, BPS, BPS + buyFeeBps);
        uint256 finalAsset = _ghoPriceInAsset(finalGross, rate, Math.Rounding.Floor);
        uint256 finalFee = total - finalGross;
        return Quote({assetAmount: finalAsset, totalGho: finalGross + finalFee, grossGho: finalGross, fee: finalFee});
    }

    function getGhoAmountForSellAsset(uint256 shares, uint256 rate, uint256 sellFeeBps)
        internal
        pure
        returns (Quote memory quote)
    {
        uint256 gross = _assetPriceInGho(shares, rate, Math.Rounding.Floor);
        uint256 received = gross - _fee(gross, sellFeeBps);
        uint256 finalGross =
            sellFeeBps == 0 ? received : Math.mulDiv(received, BPS, BPS - sellFeeBps, Math.Rounding.Ceil);
        uint256 finalAsset = _ghoPriceInAsset(finalGross, rate, Math.Rounding.Ceil);
        uint256 finalFee = finalGross - received;
        return Quote({assetAmount: finalAsset, totalGho: finalGross - finalFee, grossGho: finalGross, fee: finalFee});
    }

    function getAssetAmountForBuyAsset(uint256 maxGho, uint256 rate, uint256 buyFeeBps)
        internal
        pure
        returns (Quote memory quote)
    {
        uint256 gross = buyFeeBps == 0 ? maxGho : Math.mulDiv(maxGho, BPS, BPS + buyFeeBps);
        uint256 assetAmount = _ghoPriceInAsset(gross, rate, Math.Rounding.Floor);
        uint256 finalGross = _assetPriceInGho(assetAmount, rate, Math.Rounding.Ceil);
        uint256 finalFee = _fee(finalGross, buyFeeBps);
        return Quote({assetAmount: assetAmount, totalGho: finalGross + finalFee, grossGho: finalGross, fee: finalFee});
    }

    function getAssetAmountForSellAsset(uint256 minGho, uint256 rate, uint256 sellFeeBps)
        internal
        pure
        returns (Quote memory quote)
    {
        uint256 gross = sellFeeBps == 0 ? minGho : Math.mulDiv(minGho, BPS, BPS - sellFeeBps, Math.Rounding.Ceil);
        uint256 assetAmount = _ghoPriceInAsset(gross, rate, Math.Rounding.Ceil);
        uint256 finalGross = _assetPriceInGho(assetAmount, rate, Math.Rounding.Floor);
        uint256 finalFee = _fee(finalGross, sellFeeBps);
        return Quote({assetAmount: assetAmount, totalGho: finalGross - finalFee, grossGho: finalGross, fee: finalFee});
    }

    function unpackFeeAndFlags(bytes32 word) internal pure returns (address feeStrategy, bool frozen, bool seized) {
        uint256 value = uint256(word);
        feeStrategy = address(uint160(value));
        frozen = ((value >> 160) & 0xff) != 0;
        seized = ((value >> 168) & 0xff) != 0;
    }

    function unpackExposure(bytes32 word) internal pure returns (uint128 cap, uint128 current) {
        uint256 value = uint256(word);
        cap = uint128(value);
        current = uint128(value >> 128);
    }

    function unpackUsage(bytes32 word) internal pure returns (uint128 limit, uint128 used) {
        uint256 value = uint256(word);
        limit = uint128(value);
        used = uint128(value >> 128);
    }

    function maxDeposit(
        bytes32 configuration,
        bytes32 indexAndRate,
        bytes32 deficitAndTimestamps,
        bytes32 accruedAndVirtualBalance,
        uint256 scaledTotalSupply,
        uint256 timestamp
    ) internal pure returns (uint256) {
        uint256 config = uint256(configuration);
        bool active = ((config >> 56) & 1) != 0;
        bool frozen = ((config >> 57) & 1) != 0;
        bool paused = ((config >> 60) & 1) != 0;
        if (!active || frozen || paused) return 0;

        uint256 supplyCap = (config >> 116) & ((uint256(1) << 36) - 1);
        if (supplyCap == 0) return type(uint256).max;

        uint256 decimals = (config >> 48) & 0xff;
        require(decimals <= 77, "invalid reserve decimals");
        uint256 capInAssetUnits = supplyCap * (10 ** decimals);
        uint256 accruedToTreasury = uint128(uint256(accruedAndVirtualBalance));
        uint256 rate = normalizedIncome(indexAndRate, deficitAndTimestamps, timestamp);
        uint256 currentSupply = Math.mulDiv(scaledTotalSupply + accruedToTreasury, rate, RAY, Math.Rounding.Ceil);
        return currentSupply >= capInAssetUnits ? 0 : capInAssetUnits - currentSupply;
    }

    function _assetPriceInGho(uint256 shares, uint256 rate, Math.Rounding rounding) private pure returns (uint256) {
        uint256 vaultAssets = convertToAssets(shares, rate, rounding);
        return Math.mulDiv(vaultAssets, PRICE_RATIO, ASSET_UNITS, rounding);
    }

    function _ghoPriceInAsset(uint256 gho, uint256 rate, Math.Rounding rounding) private pure returns (uint256) {
        uint256 vaultAssets = Math.mulDiv(gho, ASSET_UNITS, PRICE_RATIO, rounding);
        return convertToShares(vaultAssets, rate, rounding);
    }

    function _fee(uint256 gross, uint256 feeBps) private pure returns (uint256) {
        return Math.mulDiv(gross, feeBps, BPS, Math.Rounding.Ceil);
    }
}

library GuardModel {
    function addressInWord(bytes32 word) internal pure returns (address) {
        return address(uint160(uint256(word)));
    }

    function implementationMatches(bytes32 word, address expected) internal pure returns (bool) {
        return addressInWord(word) == expected;
    }

    function gsmEnabled(bytes32 feeAndFlags, address expectedFeeStrategy) internal pure returns (bool) {
        (address feeStrategy, bool frozen, bool seized) = AaveModel.unpackFeeAndFlags(feeAndFlags);
        return feeStrategy == expectedFeeStrategy && !frozen && !seized;
    }
}
