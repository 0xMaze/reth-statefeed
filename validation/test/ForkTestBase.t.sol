// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Test} from "forge-std-1.9.7/src/Test.sol";

abstract contract ForkTestBase is Test {
    uint256 internal constant FORK_BLOCK = 25_912_757;
    bytes32 internal constant FORK_HASH = 0xfa331c6c11df54016c7c0ddf48aeb9c61e7ff07e5170da485cd858096e490b6d;
    uint256 internal constant FORK_TIMESTAMP = 1_788_630_887;

    struct Watch {
        address account;
        string id;
        bytes32 slot;
    }

    Watch[] internal watches;

    function setUp() public virtual {
        _selectPinnedFork(FORK_BLOCK, FORK_HASH, FORK_TIMESTAMP);
        _loadManifest();
    }

    function _selectPinnedFork(uint256 blockNumber, bytes32 blockHash, uint256 timestamp) internal {
        uint256 hashCheckFork = vm.createFork("mainnet", blockNumber + 1);
        vm.selectFork(hashCheckFork);
        assertEq(blockhash(blockNumber), blockHash, "fork block hash changed");

        vm.createSelectFork("mainnet", blockNumber);
        assertEq(block.chainid, 1, "wrong chain");
        assertEq(block.number, blockNumber, "wrong block");
        assertEq(block.timestamp, timestamp, "fork timestamp changed");
    }

    function _loadManifest() private {
        string memory document = vm.readFile("../config.ethereum-mainnet-conversions.toml");
        bytes memory encoded = vm.parseToml(document, ".watch");
        Watch[] memory decoded = abi.decode(encoded, (Watch[]));
        for (uint256 i; i < decoded.length; ++i) {
            watches.push(decoded[i]);
        }
        assertEq(decoded.length, 87, "unexpected manifest size");
    }

    function _isWatched(address account, bytes32 slot) internal view returns (bool) {
        for (uint256 i; i < watches.length; ++i) {
            if (watches[i].account == account && watches[i].slot == slot) return true;
        }
        return false;
    }

    function _assertWatched(address account, bytes32 slot, string memory label) internal view {
        assertTrue(_isWatched(account, slot), label);
    }

    function _contains(bytes32[] memory values, bytes32 needle) internal pure returns (bool) {
        for (uint256 i; i < values.length; ++i) {
            if (values[i] == needle) return true;
        }
        return false;
    }

    function _assertRead(address account, bytes32 slot, string memory label) internal {
        (bytes32[] memory reads,) = vm.accesses(account);
        assertTrue(_contains(reads, slot), label);
    }

    function _assertOnlyWatchedReads(address account) internal {
        (bytes32[] memory reads,) = vm.accesses(account);
        for (uint256 i; i < reads.length; ++i) {
            assertTrue(
                _isWatched(account, reads[i]),
                string.concat("unwatched read at ", vm.toString(account), "/", vm.toString(reads[i]))
            );
        }
    }

    function _approve(address token, address spender, uint256 amount) internal {
        (bool success, bytes memory result) =
            token.call(abi.encodeWithSelector(bytes4(keccak256("approve(address,uint256)")), spender, amount));
        assertTrue(success && (result.length == 0 || abi.decode(result, (bool))), "approve failed");
    }
}
