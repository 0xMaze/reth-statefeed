// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

interface IERC20Like {
    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
}

interface IDaiUsds {
    function daiToUsds(address receiver, uint256 amount) external;
    function usdsToDai(address receiver, uint256 amount) external;
}

interface ILitePsm {
    function tin() external view returns (uint256);
    function tout() external view returns (uint256);
    function sellGem(address receiver, uint256 gemAmount) external returns (uint256 daiOut);
    function buyGem(address receiver, uint256 gemAmount) external returns (uint256 daiIn);
}

interface IUsddPsm {
    function tin() external view returns (uint256);
    function tout() external view returns (uint256);
    function sellEnabled() external view returns (uint256);
    function buyEnabled() external view returns (uint256);
    function sellGem(address receiver, uint256 gemAmount) external;
    function buyGem(address receiver, uint256 gemAmount) external;
}

interface IERC4626Like {
    function convertToAssets(uint256 shares) external view returns (uint256);
    function convertToShares(uint256 assets) external view returns (uint256);
    function previewDeposit(uint256 assets) external view returns (uint256);
    function previewMint(uint256 shares) external view returns (uint256);
    function previewWithdraw(uint256 assets) external view returns (uint256);
    function previewRedeem(uint256 shares) external view returns (uint256);
    function maxDeposit(address receiver) external view returns (uint256);
}

interface IAavePoolLike {
    function getReserveNormalizedIncome(address asset) external view returns (uint256);
    function getVirtualUnderlyingBalance(address asset) external view returns (uint128);
}

interface IGsm {
    function sellAsset(uint256 maxAmount, address receiver) external returns (uint256, uint256);

    function getGhoAmountForBuyAsset(uint256 assetAmount)
        external
        view
        returns (uint256 finalAssetAmount, uint256 ghoSold, uint256 grossAmount, uint256 fee);

    function getGhoAmountForSellAsset(uint256 assetAmount)
        external
        view
        returns (uint256 finalAssetAmount, uint256 ghoBought, uint256 grossAmount, uint256 fee);

    function getAssetAmountForBuyAsset(uint256 ghoAmount)
        external
        view
        returns (uint256 assetAmount, uint256 finalGhoAmount, uint256 grossAmount, uint256 fee);

    function getAssetAmountForSellAsset(uint256 ghoAmount)
        external
        view
        returns (uint256 assetAmount, uint256 finalGhoAmount, uint256 grossAmount, uint256 fee);

    function getAvailableUnderlyingExposure() external view returns (uint256);
    function getAvailableLiquidity() external view returns (uint256);
    function getExposureCap() external view returns (uint128);
    function getUsed() external view returns (uint256);
    function getLimit() external view returns (uint256);
    function getFeeStrategy() external view returns (address);
    function getGhoReserve() external view returns (address);
    function getIsFrozen() external view returns (bool);
    function getIsSeized() external view returns (bool);
    function canSwap() external view returns (bool);
}

interface IGhoReserve {
    function getUsage(address entity) external view returns (uint256 limit, uint256 used);
}

interface IVatLike {
    function ilks(bytes32 ilk)
        external
        view
        returns (uint256 art, uint256 rate, uint256 spot, uint256 line, uint256 dust);
    function urns(bytes32 ilk, address urn) external view returns (uint256 ink, uint256 art);
    function debt() external view returns (uint256);
    // Upstream Vat ABI uses an uppercase public storage getter.
    // forge-lint: disable-next-line(mixed-case-function)
    function Line() external view returns (uint256);
    function live() external view returns (uint256);
}

interface IGemJoinLike {
    function live() external view returns (uint256);
}
