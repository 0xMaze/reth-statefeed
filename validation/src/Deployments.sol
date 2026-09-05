// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

library Deployments {
    address internal constant DAI = 0x6B175474E89094C44Da98b954EedeAC495271d0F;
    address internal constant USDS = 0xdC035D45d973E3EC169d2276DDab16f1e407384F;
    address internal constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address internal constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;
    address internal constant GHO = 0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f;
    address internal constant USDD = 0x4f8e5DE400DE08B164E7421B3EE387f461beCD1A;

    address internal constant DAI_USDS = 0x3225737a9Bbb6473CB4a45b7244ACa2BeFdB276A;
    address internal constant SKY_LITE_PSM = 0xf6e72Db5454dd049d0788e411b06CfAF16853042;
    address internal constant SKY_USDS_PSM = 0xA188EEC8F81263234dA3622A406892F3D630f98c;
    address internal constant SKY_LITE_PSM_POCKET = 0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341;
    address internal constant SKY_DAI_JOIN = 0x9759A6Ac90977b93B58547b4A71c78317f391A28;
    address internal constant SKY_USDS_JOIN = 0x3C0f895007CA717Aa01c8693e59DF1e8C3777FEB;
    address internal constant SKY_VAT = 0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B;

    address internal constant AAVE_POOL = 0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2;
    address internal constant AAVE_POOL_ADDRESSES_PROVIDER = 0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e;
    address internal constant AAVE_A_USDC = 0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c;
    address internal constant AAVE_A_USDT = 0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a;
    address internal constant WA_ETH_USDC = 0xD4fa2D31b7968E448877f69A96DE69f5de8cD23E;
    address internal constant WA_ETH_USDT = 0x7Bc3485026Ac48b6cf9BaF0A377477Fff5703Af8;
    address internal constant GSM_USDC = 0x3A3868898305f04beC7FEa77BecFf04C13444112;
    address internal constant GSM_USDT = 0x882285E62656b9623AF136Ce3078c6BdCc33F5E3;
    address internal constant GHO_RESERVE = 0x54C58157DeF387A880AE62332D1445f03adbE7E9;
    address internal constant GSM_USDC_FEE_STRATEGY = 0x06fbDE909B43f01202E3C6207De1D27cC208AcC1;
    address internal constant GSM_USDT_FEE_STRATEGY = 0xfDB0090A92d20EE39d82ac680477b1F58f0A23dE;

    address internal constant USDD_VAT = 0xFf77F6209239DEB2c076179499f2346b0032097f;
    address internal constant USDD_USDT_PSM = 0xcE355440c00014A229bbEc030A2B8f8EB45a2897;
    address internal constant USDD_USDC_PSM = 0x12d0351F68035a41D13fc8324562e2d51B7A3b93;
    address internal constant USDD_USDT_JOIN = 0x217e42CEB2eAE9ECB788fDF0e31c806c531760A3;
    address internal constant USDD_USDC_JOIN = 0x9A7E1B324060dB7342aeA08c0dc56F55CEd6F519;
    address internal constant USDD_JOIN = 0x983DFef6d71862d809e239845Da5A959492f63b8;

    bytes32 internal constant EIP1967_IMPLEMENTATION_SLOT =
        0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;
    bytes32 internal constant USDC_IMPLEMENTATION_SLOT =
        0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3;

    bytes32 internal constant AAVE_USDC_RESERVE_BASE =
        0xed960c71bd5fa1333658850f076b35ec5565086b606556c3dd36a916b43ddf20;
    bytes32 internal constant AAVE_USDT_RESERVE_BASE =
        0xca6decca4edae0c692b2b0c41376a54b812edb060282d36e07a7060ccb58244c;

    bytes32 internal constant GSM_FEE_AND_FLAGS_SLOT = bytes32(uint256(55));
    bytes32 internal constant GSM_EXPOSURE_SLOT = bytes32(uint256(56));
    bytes32 internal constant GSM_GHO_RESERVE_SLOT = bytes32(uint256(58));

    bytes32 internal constant GHO_RESERVE_USDC_USAGE_SLOT =
        0x64691f9c88e2e6b4867d44adbd591939e35af0e00a67553c81587d5f02897686;
    bytes32 internal constant GHO_RESERVE_USDT_USAGE_SLOT =
        0x2974bc752e3ee88955eca3b54fbeb8b99f22148d2fe0c02a03af5ee36a60a42d;

    bytes32 internal constant USDD_USDT_ILK = bytes32("PSM-USDT-A");
    bytes32 internal constant USDD_USDC_ILK = bytes32("PSM-USDC-A");
}
