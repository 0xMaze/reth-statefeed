// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {GuardModel} from "./ConversionModels.sol";
import {Deployments} from "./Deployments.sol";

/// @notice Pure fail-closed interpretation of deployment-dependent snapshot words.
/// @dev Unknown deployments and implementations are intentionally rejected. A registry update must
///      accompany every supported upgrade.
library RegistryGuards {
    address internal constant USDC_PROXY_ADMIN = 0x807a96288A1A408dBC13DE2b1d087d10356395d2;
    address internal constant USDC_IMPLEMENTATION = 0x43506849D7C04F9138D1A2050bbF3A0c054402dd;
    address internal constant USDS_IMPLEMENTATION = 0x1923DfeE706A8E78157416C29cBCCFDe7cdF4102;
    address internal constant AAVE_POOL_IMPLEMENTATION = 0x728a138A4823392C2EFA55e028d434F526fE03CF;
    address internal constant A_TOKEN_IMPLEMENTATION = 0xadC45Df3cf1584624C97338BEF33363BF5b97AdA;
    address internal constant STATIC_A_TOKEN_IMPLEMENTATION = 0x487c2C53c0866F0A73ae317bD1A28F63ADcD9aD1;
    address internal constant GSM_USDC_IMPLEMENTATION = 0x320Be97B4d10b6d20a05cAE53a479Fa2A0187e8e;
    address internal constant GSM_USDT_IMPLEMENTATION = 0x31fE806EAd0A800E68627aA49bAb478d20a28788;
    address internal constant GHO_RESERVE_IMPLEMENTATION = 0x4f381f0827cB081b3ce2B7D7062402d43c4eFBe6;

    function implementationSlot(address proxy) internal pure returns (bytes32) {
        if (proxy == Deployments.USDC) return Deployments.USDC_IMPLEMENTATION_SLOT;
        return Deployments.EIP1967_IMPLEMENTATION_SLOT;
    }

    function expectedImplementation(address proxy) internal pure returns (address) {
        if (proxy == Deployments.USDC) return USDC_IMPLEMENTATION;
        if (proxy == Deployments.USDS) return USDS_IMPLEMENTATION;
        if (proxy == Deployments.AAVE_POOL) return AAVE_POOL_IMPLEMENTATION;
        if (proxy == Deployments.AAVE_A_USDC || proxy == Deployments.AAVE_A_USDT) {
            return A_TOKEN_IMPLEMENTATION;
        }
        if (proxy == Deployments.WA_ETH_USDC || proxy == Deployments.WA_ETH_USDT) {
            return STATIC_A_TOKEN_IMPLEMENTATION;
        }
        if (proxy == Deployments.GSM_USDC) return GSM_USDC_IMPLEMENTATION;
        if (proxy == Deployments.GSM_USDT) return GSM_USDT_IMPLEMENTATION;
        if (proxy == Deployments.GHO_RESERVE) return GHO_RESERVE_IMPLEMENTATION;
        return address(0);
    }

    function supportedImplementation(address proxy, bytes32 implementationWord) internal pure returns (bool) {
        address expected = expectedImplementation(proxy);
        return expected != address(0) && GuardModel.implementationMatches(implementationWord, expected);
    }

    function supportedUsdcProxy(bytes32 implementationWord, bytes32 adminWord) internal pure returns (bool) {
        return supportedImplementation(Deployments.USDC, implementationWord)
            && GuardModel.addressInWord(adminWord) == USDC_PROXY_ADMIN;
    }

    function supportedReserveAToken(address asset, bytes32 aTokenWord) internal pure returns (bool) {
        address expected;
        if (asset == Deployments.USDC) {
            expected = Deployments.AAVE_A_USDC;
        } else if (asset == Deployments.USDT) {
            expected = Deployments.AAVE_A_USDT;
        } else {
            return false;
        }
        return GuardModel.addressInWord(aTokenWord) == expected;
    }

    function supportedGsm(address gsm, bytes32 feeAndFlags, bytes32 reserveWord) internal pure returns (bool) {
        address expectedFee;
        if (gsm == Deployments.GSM_USDC) {
            expectedFee = Deployments.GSM_USDC_FEE_STRATEGY;
        } else if (gsm == Deployments.GSM_USDT) {
            expectedFee = Deployments.GSM_USDT_FEE_STRATEGY;
        } else {
            return false;
        }

        return GuardModel.gsmEnabled(feeAndFlags, expectedFee)
            && GuardModel.addressInWord(reserveWord) == Deployments.GHO_RESERVE;
    }

    function supportedWrapper(address wrapper, bytes32 initializedAndAsset) internal pure returns (bool) {
        address expectedAsset;
        if (wrapper == Deployments.WA_ETH_USDC) {
            expectedAsset = Deployments.USDC;
        } else if (wrapper == Deployments.WA_ETH_USDT) {
            expectedAsset = Deployments.USDT;
        } else {
            return false;
        }

        uint256 word = uint256(initializedAndAsset);
        uint256 initializedVersion = (word >> 160) & 0xff;
        return address(uint160(word)) == expectedAsset && initializedVersion == 6;
    }
}
