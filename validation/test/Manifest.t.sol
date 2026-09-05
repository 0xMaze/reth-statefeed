// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {ForkTestBase} from "./ForkTestBase.t.sol";

contract ManifestTest is ForkTestBase {
    function test_manifestLoadsCoordinatesWithoutASecondSourceOfTruth() external view {
        assertEq(watches[0].account, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48);
        assertEq(watches[0].id, "token.usdc.proxy.admin");
        assertEq(watches[0].slot, 0x10d6a54a4754c8869d6886b5f5d7fbfa5b4522237ea5c60d11bc4e7a1ff9390b);
    }
}
