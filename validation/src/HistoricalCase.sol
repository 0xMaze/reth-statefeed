// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

/// @dev Field order follows the alphabetical order emitted by `vm.parseToml`.
struct HistoricalCase {
    bytes32 blockHash;
    uint256 blockNumber;
    bytes32 eventAssetAmount;
    bytes32 eventFee;
    bytes32 eventGhoAmount;
    bytes32 expectedPoolImplementation;
    string fixture;
    string id;
    string overlay;
    string prestateFixture;
    string scenario;
    uint256 timestamp;
    bytes32 transactionHash;
    uint256 transactionIndex;
}

library HistoricalScenarios {
    bytes32 internal constant NORMAL = keccak256("normal");
    bytes32 internal constant NONZERO_FEE = keccak256("nonzero_fee");
    bytes32 internal constant NEAR_CAPACITY = keccak256("near_capacity");
    bytes32 internal constant FROZEN_OVERLAY = keccak256("frozen_overlay");
    bytes32 internal constant UPGRADE_BEFORE = keccak256("upgrade_before");
    bytes32 internal constant UPGRADE_AFTER = keccak256("upgrade_after");
    bytes32 internal constant REAL_SWAP = keccak256("real_swap");

    function id(string memory scenario) internal pure returns (bytes32) {
        return keccak256(bytes(scenario));
    }
}
