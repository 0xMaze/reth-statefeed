// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Script} from "forge-std-1.9.7/src/Script.sol";
import {UsddCapacityModel} from "../src/ConversionModels.sol";
import {Deployments} from "../src/Deployments.sol";
import {IERC20Like, IERC4626Like, IGsm, IVatLike} from "../src/Interfaces.sol";

/// @notice Generates a deterministic watched-state snapshot and deployed-contract quote oracle.
/// @dev Run without broadcasting. The fixture path must stay under `validation/fixtures`.
contract GenerateHistoricalFixture is Script {
    uint256 internal constant PROBE_ASSET = 1_000_000e6;
    uint256 internal constant PROBE_GHO = 1_000_000 ether;

    struct Watch {
        address account;
        string id;
        bytes32 slot;
    }

    function run() external {
        uint256 blockNumber = vm.envUint("FIXTURE_BLOCK");
        bytes32 expectedHash = vm.envBytes32("FIXTURE_BLOCK_HASH");
        uint256 expectedTimestamp = vm.envUint("FIXTURE_TIMESTAMP");
        string memory outputPath = vm.envString("FIXTURE_PATH");

        uint256 hashCheckFork = vm.createFork("mainnet", blockNumber + 1);
        vm.selectFork(hashCheckFork);
        require(blockhash(blockNumber) == expectedHash, "fixture block hash mismatch");

        vm.createSelectFork("mainnet", blockNumber);
        require(block.chainid == 1, "fixture chain mismatch");
        require(block.timestamp == expectedTimestamp, "fixture timestamp mismatch");

        string memory manifest = vm.readFile("../config.ethereum-mainnet-conversions.toml");
        Watch[] memory watches = abi.decode(vm.parseToml(manifest, ".watch"), (Watch[]));
        address[] memory accounts = new address[](watches.length);
        string[] memory ids = new string[](watches.length);
        bytes32[] memory slots = new bytes32[](watches.length);
        bytes32[] memory values = new bytes32[](watches.length);
        for (uint256 i; i < watches.length; ++i) {
            accounts[i] = watches[i].account;
            ids[i] = watches[i].id;
            slots[i] = watches[i].slot;
            values[i] = vm.load(watches[i].account, watches[i].slot);
        }

        string memory objectKey = "historical-fixture";
        vm.serializeAddress(objectKey, "accounts", accounts);
        vm.serializeBytes32(objectKey, "aave_usdc_quotes", _gsmQuotes(Deployments.GSM_USDC));
        vm.serializeBytes32(objectKey, "aave_usdt_quotes", _gsmQuotes(Deployments.GSM_USDT));
        vm.serializeBytes32(objectKey, "aave_usdc_wrapper", _wrapperQuotes(Deployments.WA_ETH_USDC));
        vm.serializeBytes32(objectKey, "aave_usdt_wrapper", _wrapperQuotes(Deployments.WA_ETH_USDT));
        vm.serializeBytes32(objectKey, "block_hash", expectedHash);
        vm.serializeUint(objectKey, "block_number", blockNumber);
        vm.serializeString(objectKey, "ids", ids);
        vm.serializeBytes32(objectKey, "manifest_hash", keccak256(bytes(manifest)));
        vm.serializeUint(objectKey, "probe_asset", PROBE_ASSET);
        vm.serializeUint(objectKey, "probe_gho", PROBE_GHO);
        vm.serializeBytes32(objectKey, "slots", slots);
        vm.serializeUint(objectKey, "timestamp", expectedTimestamp);
        vm.serializeBytes32(objectKey, "usdd_usdc_capacity", _usddCapacity(false));
        vm.serializeBytes32(objectKey, "usdd_usdt_capacity", _usddCapacity(true));
        string memory json = vm.serializeBytes32(objectKey, "values", values);
        vm.writeJson(json, outputPath);
    }

    function _gsmQuotes(address gsm) private view returns (bytes32[] memory values) {
        values = new bytes32[](16);
        (uint256 asset, uint256 total, uint256 gross, uint256 fee) = IGsm(gsm).getGhoAmountForBuyAsset(PROBE_ASSET);
        _storeQuote(values, 0, asset, total, gross, fee);
        (asset, total, gross, fee) = IGsm(gsm).getGhoAmountForSellAsset(PROBE_ASSET);
        _storeQuote(values, 4, asset, total, gross, fee);
        (asset, total, gross, fee) = IGsm(gsm).getAssetAmountForBuyAsset(PROBE_GHO);
        _storeQuote(values, 8, asset, total, gross, fee);
        (asset, total, gross, fee) = IGsm(gsm).getAssetAmountForSellAsset(PROBE_GHO);
        _storeQuote(values, 12, asset, total, gross, fee);
    }

    function _wrapperQuotes(address wrapper) private view returns (bytes32[] memory values) {
        values = new bytes32[](7);
        values[0] = bytes32(IERC4626Like(wrapper).convertToAssets(PROBE_ASSET));
        values[1] = bytes32(IERC4626Like(wrapper).convertToShares(PROBE_ASSET));
        values[2] = bytes32(IERC4626Like(wrapper).previewDeposit(PROBE_ASSET));
        values[3] = bytes32(IERC4626Like(wrapper).previewMint(PROBE_ASSET));
        values[4] = bytes32(IERC4626Like(wrapper).previewWithdraw(PROBE_ASSET));
        values[5] = bytes32(IERC4626Like(wrapper).previewRedeem(PROBE_ASSET));
        values[6] = bytes32(IERC4626Like(wrapper).maxDeposit(address(0)));
    }

    function _usddCapacity(bool usdt) private view returns (bytes32[] memory values) {
        address psm = usdt ? Deployments.USDD_USDT_PSM : Deployments.USDD_USDC_PSM;
        address gem = usdt ? Deployments.USDT : Deployments.USDC;
        address join = usdt ? Deployments.USDD_USDT_JOIN : Deployments.USDD_USDC_JOIN;
        bytes32 ilk = usdt ? Deployments.USDD_USDT_ILK : Deployments.USDD_USDC_ILK;

        IVatLike vat = IVatLike(Deployments.USDD_VAT);
        UsddCapacityModel.VatState memory state;
        (state.art, state.rate, state.spot, state.line, state.dust) = vat.ilks(ilk);
        (state.urnInk, state.urnArt) = vat.urns(ilk, psm);
        state.debt = vat.debt();
        state.globalLine = vat.Line();

        values = new bytes32[](2);
        values[0] = bytes32(UsddCapacityModel.sellCapacity(state));
        values[1] = bytes32(UsddCapacityModel.buyCapacity(state, IERC20Like(gem).balanceOf(join)));
    }

    function _storeQuote(
        bytes32[] memory target,
        uint256 offset,
        uint256 asset,
        uint256 total,
        uint256 gross,
        uint256 fee
    ) private pure {
        target[offset] = bytes32(asset);
        target[offset + 1] = bytes32(total);
        target[offset + 2] = bytes32(gross);
        target[offset + 3] = bytes32(fee);
    }
}
