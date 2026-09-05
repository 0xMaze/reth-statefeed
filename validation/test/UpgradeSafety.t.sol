// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Deployments} from "../src/Deployments.sol";
import {RegistryGuards} from "../src/RegistryGuards.sol";
import {ForkTestBase} from "./ForkTestBase.t.sol";

contract UpgradeSafetyTest is ForkTestBase {
    bytes32 private constant USDC_PROXY_ADMIN_SLOT = 0x10d6a54a4754c8869d6886b5f5d7fbfa5b4522237ea5c60d11bc4e7a1ff9390b;
    bytes32 private constant WRAPPER_INITIALIZED_AND_ASSET_SLOT =
        0x0773e532dfede91f04b12a73d3d2acd361424f41f76b4fb79f090161e36b4e00;

    function test_currentProxyImplementationsAreExplicitlySupported() external view {
        address[10] memory proxies = [
            Deployments.USDC,
            Deployments.USDS,
            Deployments.AAVE_POOL,
            Deployments.AAVE_A_USDC,
            Deployments.AAVE_A_USDT,
            Deployments.WA_ETH_USDC,
            Deployments.WA_ETH_USDT,
            Deployments.GSM_USDC,
            Deployments.GSM_USDT,
            Deployments.GHO_RESERVE
        ];

        for (uint256 i; i < proxies.length; ++i) {
            bytes32 slot = RegistryGuards.implementationSlot(proxies[i]);
            _assertWatched(proxies[i], slot, "implementation guard missing from manifest");
            assertTrue(
                RegistryGuards.supportedImplementation(proxies[i], vm.load(proxies[i], slot)),
                "anchored implementation rejected"
            );
        }
    }

    function testFuzz_anyProxyImplementationChangeFailsClosed(uint8 rawIndex, address replacement) external pure {
        address[10] memory proxies = [
            Deployments.USDC,
            Deployments.USDS,
            Deployments.AAVE_POOL,
            Deployments.AAVE_A_USDC,
            Deployments.AAVE_A_USDT,
            Deployments.WA_ETH_USDC,
            Deployments.WA_ETH_USDT,
            Deployments.GSM_USDC,
            Deployments.GSM_USDT,
            Deployments.GHO_RESERVE
        ];
        address proxy = proxies[bound(uint256(rawIndex), 0, proxies.length - 1)];
        address expected = RegistryGuards.expectedImplementation(proxy);
        vm.assume(replacement != expected);

        assertFalse(
            RegistryGuards.supportedImplementation(proxy, bytes32(uint256(uint160(replacement)))),
            "unknown implementation accepted"
        );
    }

    function test_currentGsmDependenciesAndFlagsAreSupported() external view {
        _assertCurrentGsm(Deployments.GSM_USDC);
        _assertCurrentGsm(Deployments.GSM_USDT);
    }

    function test_usdcAdminAndAaveReserveTokenPointersFailClosed() external view {
        bytes32 usdcImplementation = vm.load(Deployments.USDC, Deployments.USDC_IMPLEMENTATION_SLOT);
        bytes32 usdcAdmin = vm.load(Deployments.USDC, USDC_PROXY_ADMIN_SLOT);
        assertTrue(RegistryGuards.supportedUsdcProxy(usdcImplementation, usdcAdmin));
        assertFalse(RegistryGuards.supportedUsdcProxy(usdcImplementation, bytes32(uint256(uint160(address(0xBEEF))))));

        bytes32 aUsdc = vm.load(Deployments.AAVE_POOL, bytes32(uint256(Deployments.AAVE_USDC_RESERVE_BASE) + 4));
        bytes32 aUsdt = vm.load(Deployments.AAVE_POOL, bytes32(uint256(Deployments.AAVE_USDT_RESERVE_BASE) + 4));
        assertTrue(RegistryGuards.supportedReserveAToken(Deployments.USDC, aUsdc));
        assertTrue(RegistryGuards.supportedReserveAToken(Deployments.USDT, aUsdt));
        assertFalse(RegistryGuards.supportedReserveAToken(Deployments.USDC, bytes32(uint256(uint160(address(0xCAFE))))));
        assertFalse(RegistryGuards.supportedReserveAToken(address(0xBEEF), aUsdc));
    }

    function test_gsmUnknownStrategyFreezeSeizeAndReserveChangeFailClosed() external view {
        bytes32 liveUsdc = vm.load(Deployments.GSM_USDC, Deployments.GSM_FEE_AND_FLAGS_SLOT);
        bytes32 reserve = vm.load(Deployments.GSM_USDC, Deployments.GSM_GHO_RESERVE_SLOT);

        assertFalse(
            RegistryGuards.supportedGsm(Deployments.GSM_USDC, bytes32(uint256(uint160(address(0xBEEF)))), reserve),
            "unknown fee strategy accepted"
        );
        assertFalse(
            RegistryGuards.supportedGsm(
                Deployments.GSM_USDC, bytes32(uint256(liveUsdc) | (uint256(1) << 160)), reserve
            ),
            "frozen GSM accepted"
        );
        assertFalse(
            RegistryGuards.supportedGsm(
                Deployments.GSM_USDC, bytes32(uint256(liveUsdc) | (uint256(1) << 168)), reserve
            ),
            "seized GSM accepted"
        );
        assertFalse(
            RegistryGuards.supportedGsm(Deployments.GSM_USDC, liveUsdc, bytes32(uint256(uint160(address(0xCAFE))))),
            "unknown reserve accepted"
        );
    }

    function test_wrapperUnderlyingOrInitializationChangeFailsClosed() external view {
        bytes32 usdc = vm.load(Deployments.WA_ETH_USDC, WRAPPER_INITIALIZED_AND_ASSET_SLOT);
        bytes32 usdt = vm.load(Deployments.WA_ETH_USDT, WRAPPER_INITIALIZED_AND_ASSET_SLOT);
        _assertWatched(Deployments.WA_ETH_USDC, WRAPPER_INITIALIZED_AND_ASSET_SLOT, "USDC wrapper guard missing");
        _assertWatched(Deployments.WA_ETH_USDT, WRAPPER_INITIALIZED_AND_ASSET_SLOT, "USDT wrapper guard missing");
        assertTrue(RegistryGuards.supportedWrapper(Deployments.WA_ETH_USDC, usdc));
        assertTrue(RegistryGuards.supportedWrapper(Deployments.WA_ETH_USDT, usdt));

        assertFalse(
            RegistryGuards.supportedWrapper(
                Deployments.WA_ETH_USDC,
                bytes32((uint256(usdc) & ~uint256(type(uint160).max)) | uint160(address(0xBEEF)))
            )
        );
        assertFalse(
            RegistryGuards.supportedWrapper(
                Deployments.WA_ETH_USDC, bytes32((uint256(usdc) & uint256(type(uint160).max)) | (uint256(7) << 160))
            )
        );
    }

    function test_unknownDeploymentAlwaysFailsClosed() external pure {
        assertEq(RegistryGuards.expectedImplementation(address(0xBEEF)), address(0));
        assertFalse(RegistryGuards.supportedImplementation(address(0xBEEF), bytes32(0)));
        assertFalse(RegistryGuards.supportedGsm(address(0xBEEF), bytes32(0), bytes32(0)));
        assertFalse(RegistryGuards.supportedWrapper(address(0xBEEF), bytes32(0)));
    }

    function _assertCurrentGsm(address gsm) private view {
        _assertWatched(gsm, Deployments.GSM_FEE_AND_FLAGS_SLOT, "GSM fee/flags guard missing");
        _assertWatched(gsm, Deployments.GSM_GHO_RESERVE_SLOT, "GSM reserve guard missing");
        assertTrue(
            RegistryGuards.supportedGsm(
                gsm, vm.load(gsm, Deployments.GSM_FEE_AND_FLAGS_SLOT), vm.load(gsm, Deployments.GSM_GHO_RESERVE_SLOT)
            ),
            "anchored GSM guard rejected"
        );
    }
}
