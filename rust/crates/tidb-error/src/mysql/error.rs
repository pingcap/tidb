// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct SQLError, NewErr, and NewErrf translation.

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

use super::{message_by_code, mysql_state};

/// Portable ErrBadConn message from the Go source.
pub const ERR_BAD_CONN: &str = "connection was bad";
/// Portable ErrMalformPacket message from the Go source.
pub const ERR_MALFORM_PACKET: &str = "malform packet error";

// BEGIN GENERATED GO STRCONV ISPRINT RANGES
// Generated from Go `strconv.IsPrint` for parser/mysql fmt parity.
const GO_IS_PRINT16: &[u16] = &[
    0x0020, 0x007e, 0x00a1, 0x0377, 0x037a, 0x037f, 0x0384, 0x0556, 0x0559, 0x058a, 0x058d, 0x05c7,
    0x05d0, 0x05ea, 0x05ef, 0x05f4, 0x0606, 0x070d, 0x0710, 0x074a, 0x074d, 0x07b1, 0x07c0, 0x07fa,
    0x07fd, 0x082d, 0x0830, 0x085b, 0x085e, 0x086a, 0x0870, 0x088e, 0x0898, 0x098c, 0x098f, 0x0990,
    0x0993, 0x09b2, 0x09b6, 0x09b9, 0x09bc, 0x09c4, 0x09c7, 0x09c8, 0x09cb, 0x09ce, 0x09d7, 0x09d7,
    0x09dc, 0x09e3, 0x09e6, 0x09fe, 0x0a01, 0x0a0a, 0x0a0f, 0x0a10, 0x0a13, 0x0a39, 0x0a3c, 0x0a42,
    0x0a47, 0x0a48, 0x0a4b, 0x0a4d, 0x0a51, 0x0a51, 0x0a59, 0x0a5e, 0x0a66, 0x0a76, 0x0a81, 0x0ab9,
    0x0abc, 0x0acd, 0x0ad0, 0x0ad0, 0x0ae0, 0x0ae3, 0x0ae6, 0x0af1, 0x0af9, 0x0b0c, 0x0b0f, 0x0b10,
    0x0b13, 0x0b39, 0x0b3c, 0x0b44, 0x0b47, 0x0b48, 0x0b4b, 0x0b4d, 0x0b55, 0x0b57, 0x0b5c, 0x0b63,
    0x0b66, 0x0b77, 0x0b82, 0x0b8a, 0x0b8e, 0x0b95, 0x0b99, 0x0b9f, 0x0ba3, 0x0ba4, 0x0ba8, 0x0baa,
    0x0bae, 0x0bb9, 0x0bbe, 0x0bc2, 0x0bc6, 0x0bcd, 0x0bd0, 0x0bd0, 0x0bd7, 0x0bd7, 0x0be6, 0x0bfa,
    0x0c00, 0x0c39, 0x0c3c, 0x0c4d, 0x0c55, 0x0c5a, 0x0c5d, 0x0c5d, 0x0c60, 0x0c63, 0x0c66, 0x0c6f,
    0x0c77, 0x0cb9, 0x0cbc, 0x0ccd, 0x0cd5, 0x0cd6, 0x0cdd, 0x0ce3, 0x0ce6, 0x0cf3, 0x0d00, 0x0d4f,
    0x0d54, 0x0d63, 0x0d66, 0x0d96, 0x0d9a, 0x0dbd, 0x0dc0, 0x0dc6, 0x0dca, 0x0dca, 0x0dcf, 0x0ddf,
    0x0de6, 0x0def, 0x0df2, 0x0df4, 0x0e01, 0x0e3a, 0x0e3f, 0x0e5b, 0x0e81, 0x0ebd, 0x0ec0, 0x0ed9,
    0x0edc, 0x0edf, 0x0f00, 0x0f6c, 0x0f71, 0x0fda, 0x1000, 0x10c7, 0x10cd, 0x10cd, 0x10d0, 0x124d,
    0x1250, 0x125d, 0x1260, 0x128d, 0x1290, 0x12b5, 0x12b8, 0x12c5, 0x12c8, 0x1315, 0x1318, 0x135a,
    0x135d, 0x137c, 0x1380, 0x1399, 0x13a0, 0x13f5, 0x13f8, 0x13fd, 0x1400, 0x169c, 0x16a0, 0x16f8,
    0x1700, 0x1715, 0x171f, 0x1736, 0x1740, 0x1753, 0x1760, 0x1773, 0x1780, 0x17dd, 0x17e0, 0x17e9,
    0x17f0, 0x17f9, 0x1800, 0x1819, 0x1820, 0x1878, 0x1880, 0x18aa, 0x18b0, 0x18f5, 0x1900, 0x192b,
    0x1930, 0x193b, 0x1940, 0x1940, 0x1944, 0x196d, 0x1970, 0x1974, 0x1980, 0x19ab, 0x19b0, 0x19c9,
    0x19d0, 0x19da, 0x19de, 0x1a1b, 0x1a1e, 0x1a7c, 0x1a7f, 0x1a89, 0x1a90, 0x1a99, 0x1aa0, 0x1aad,
    0x1ab0, 0x1ace, 0x1b00, 0x1b4c, 0x1b50, 0x1bf3, 0x1bfc, 0x1c37, 0x1c3b, 0x1c49, 0x1c4d, 0x1c88,
    0x1c90, 0x1cba, 0x1cbd, 0x1cc7, 0x1cd0, 0x1cfa, 0x1d00, 0x1f15, 0x1f18, 0x1f1d, 0x1f20, 0x1f45,
    0x1f48, 0x1f4d, 0x1f50, 0x1f7d, 0x1f80, 0x1fd3, 0x1fd6, 0x1fef, 0x1ff2, 0x1ffe, 0x2010, 0x2027,
    0x2030, 0x205e, 0x2070, 0x2071, 0x2074, 0x209c, 0x20a0, 0x20c0, 0x20d0, 0x20f0, 0x2100, 0x218b,
    0x2190, 0x2426, 0x2440, 0x244a, 0x2460, 0x2b73, 0x2b76, 0x2cf3, 0x2cf9, 0x2d27, 0x2d2d, 0x2d2d,
    0x2d30, 0x2d67, 0x2d6f, 0x2d70, 0x2d7f, 0x2d96, 0x2da0, 0x2e5d, 0x2e80, 0x2ef3, 0x2f00, 0x2fd5,
    0x2ff0, 0x2ffb, 0x3001, 0x3096, 0x3099, 0x30ff, 0x3105, 0x31e3, 0x31f0, 0xa48c, 0xa490, 0xa4c6,
    0xa4d0, 0xa62b, 0xa640, 0xa6f7, 0xa700, 0xa7ca, 0xa7d0, 0xa7d9, 0xa7f2, 0xa82c, 0xa830, 0xa839,
    0xa840, 0xa877, 0xa880, 0xa8c5, 0xa8ce, 0xa8d9, 0xa8e0, 0xa953, 0xa95f, 0xa97c, 0xa980, 0xa9d9,
    0xa9de, 0xaa36, 0xaa40, 0xaa4d, 0xaa50, 0xaa59, 0xaa5c, 0xaac2, 0xaadb, 0xaaf6, 0xab01, 0xab06,
    0xab09, 0xab0e, 0xab11, 0xab16, 0xab20, 0xab6b, 0xab70, 0xabed, 0xabf0, 0xabf9, 0xac00, 0xd7a3,
    0xd7b0, 0xd7c6, 0xd7cb, 0xd7fb, 0xf900, 0xfa6d, 0xfa70, 0xfad9, 0xfb00, 0xfb06, 0xfb13, 0xfb17,
    0xfb1d, 0xfbc2, 0xfbd3, 0xfd8f, 0xfd92, 0xfdc7, 0xfdcf, 0xfdcf, 0xfdf0, 0xfe19, 0xfe20, 0xfe6b,
    0xfe70, 0xfefc, 0xff01, 0xffbe, 0xffc2, 0xffc7, 0xffca, 0xffcf, 0xffd2, 0xffd7, 0xffda, 0xffdc,
    0xffe0, 0xffee, 0xfffc, 0xfffd,
];

const GO_IS_NOT_PRINT16: &[u16] = &[
    0x00ad, 0x038b, 0x038d, 0x03a2, 0x0530, 0x0590, 0x061c, 0x06dd, 0x083f, 0x085f, 0x08e2, 0x0984,
    0x09a9, 0x09b1, 0x09de, 0x0a04, 0x0a29, 0x0a31, 0x0a34, 0x0a37, 0x0a3d, 0x0a5d, 0x0a84, 0x0a8e,
    0x0a92, 0x0aa9, 0x0ab1, 0x0ab4, 0x0ac6, 0x0aca, 0x0b00, 0x0b04, 0x0b29, 0x0b31, 0x0b34, 0x0b5e,
    0x0b84, 0x0b91, 0x0b9b, 0x0b9d, 0x0bc9, 0x0c0d, 0x0c11, 0x0c29, 0x0c45, 0x0c49, 0x0c57, 0x0c8d,
    0x0c91, 0x0ca9, 0x0cb4, 0x0cc5, 0x0cc9, 0x0cdf, 0x0cf0, 0x0d0d, 0x0d11, 0x0d45, 0x0d49, 0x0d80,
    0x0d84, 0x0db2, 0x0dbc, 0x0dd5, 0x0dd7, 0x0e83, 0x0e85, 0x0e8b, 0x0ea4, 0x0ea6, 0x0ec5, 0x0ec7,
    0x0ecf, 0x0f48, 0x0f98, 0x0fbd, 0x0fcd, 0x10c6, 0x1249, 0x1257, 0x1259, 0x1289, 0x12b1, 0x12bf,
    0x12c1, 0x12d7, 0x1311, 0x1680, 0x176d, 0x1771, 0x180e, 0x191f, 0x1a5f, 0x1b7f, 0x1f58, 0x1f5a,
    0x1f5c, 0x1f5e, 0x1fb5, 0x1fc5, 0x1fdc, 0x1ff5, 0x208f, 0x2b96, 0x2d26, 0x2da7, 0x2daf, 0x2db7,
    0x2dbf, 0x2dc7, 0x2dcf, 0x2dd7, 0x2ddf, 0x2e9a, 0x3040, 0x3130, 0x318f, 0x321f, 0xa7d2, 0xa7d4,
    0xa9ce, 0xa9ff, 0xab27, 0xab2f, 0xfb37, 0xfb3d, 0xfb3f, 0xfb42, 0xfb45, 0xfe53, 0xfe67, 0xfe75,
    0xffe7,
];

const GO_IS_PRINT32: &[u32] = &[
    0x010000, 0x01004d, 0x010050, 0x01005d, 0x010080, 0x0100fa, 0x010100, 0x010102, 0x010107,
    0x010133, 0x010137, 0x01019c, 0x0101a0, 0x0101a0, 0x0101d0, 0x0101fd, 0x010280, 0x01029c,
    0x0102a0, 0x0102d0, 0x0102e0, 0x0102fb, 0x010300, 0x010323, 0x01032d, 0x01034a, 0x010350,
    0x01037a, 0x010380, 0x0103c3, 0x0103c8, 0x0103d5, 0x010400, 0x01049d, 0x0104a0, 0x0104a9,
    0x0104b0, 0x0104d3, 0x0104d8, 0x0104fb, 0x010500, 0x010527, 0x010530, 0x010563, 0x01056f,
    0x0105bc, 0x010600, 0x010736, 0x010740, 0x010755, 0x010760, 0x010767, 0x010780, 0x0107ba,
    0x010800, 0x010805, 0x010808, 0x010838, 0x01083c, 0x01083c, 0x01083f, 0x01089e, 0x0108a7,
    0x0108af, 0x0108e0, 0x0108f5, 0x0108fb, 0x01091b, 0x01091f, 0x010939, 0x01093f, 0x01093f,
    0x010980, 0x0109b7, 0x0109bc, 0x0109cf, 0x0109d2, 0x010a06, 0x010a0c, 0x010a35, 0x010a38,
    0x010a3a, 0x010a3f, 0x010a48, 0x010a50, 0x010a58, 0x010a60, 0x010a9f, 0x010ac0, 0x010ae6,
    0x010aeb, 0x010af6, 0x010b00, 0x010b35, 0x010b39, 0x010b55, 0x010b58, 0x010b72, 0x010b78,
    0x010b91, 0x010b99, 0x010b9c, 0x010ba9, 0x010baf, 0x010c00, 0x010c48, 0x010c80, 0x010cb2,
    0x010cc0, 0x010cf2, 0x010cfa, 0x010d27, 0x010d30, 0x010d39, 0x010e60, 0x010ead, 0x010eb0,
    0x010eb1, 0x010efd, 0x010f27, 0x010f30, 0x010f59, 0x010f70, 0x010f89, 0x010fb0, 0x010fcb,
    0x010fe0, 0x010ff6, 0x011000, 0x01104d, 0x011052, 0x011075, 0x01107f, 0x0110c2, 0x0110d0,
    0x0110e8, 0x0110f0, 0x0110f9, 0x011100, 0x011147, 0x011150, 0x011176, 0x011180, 0x0111f4,
    0x011200, 0x011241, 0x011280, 0x0112a9, 0x0112b0, 0x0112ea, 0x0112f0, 0x0112f9, 0x011300,
    0x01130c, 0x01130f, 0x011310, 0x011313, 0x011344, 0x011347, 0x011348, 0x01134b, 0x01134d,
    0x011350, 0x011350, 0x011357, 0x011357, 0x01135d, 0x011363, 0x011366, 0x01136c, 0x011370,
    0x011374, 0x011400, 0x011461, 0x011480, 0x0114c7, 0x0114d0, 0x0114d9, 0x011580, 0x0115b5,
    0x0115b8, 0x0115dd, 0x011600, 0x011644, 0x011650, 0x011659, 0x011660, 0x01166c, 0x011680,
    0x0116b9, 0x0116c0, 0x0116c9, 0x011700, 0x01171a, 0x01171d, 0x01172b, 0x011730, 0x011746,
    0x011800, 0x01183b, 0x0118a0, 0x0118f2, 0x0118ff, 0x011906, 0x011909, 0x011909, 0x01190c,
    0x011938, 0x01193b, 0x011946, 0x011950, 0x011959, 0x0119a0, 0x0119a7, 0x0119aa, 0x0119d7,
    0x0119da, 0x0119e4, 0x011a00, 0x011a47, 0x011a50, 0x011aa2, 0x011ab0, 0x011af8, 0x011b00,
    0x011b09, 0x011c00, 0x011c45, 0x011c50, 0x011c6c, 0x011c70, 0x011c8f, 0x011c92, 0x011cb6,
    0x011d00, 0x011d36, 0x011d3a, 0x011d47, 0x011d50, 0x011d59, 0x011d60, 0x011d98, 0x011da0,
    0x011da9, 0x011ee0, 0x011ef8, 0x011f00, 0x011f3a, 0x011f3e, 0x011f59, 0x011fb0, 0x011fb0,
    0x011fc0, 0x011ff1, 0x011fff, 0x012399, 0x012400, 0x012474, 0x012480, 0x012543, 0x012f90,
    0x012ff2, 0x013000, 0x01342f, 0x013440, 0x013455, 0x014400, 0x014646, 0x016800, 0x016a38,
    0x016a40, 0x016a69, 0x016a6e, 0x016ac9, 0x016ad0, 0x016aed, 0x016af0, 0x016af5, 0x016b00,
    0x016b45, 0x016b50, 0x016b77, 0x016b7d, 0x016b8f, 0x016e40, 0x016e9a, 0x016f00, 0x016f4a,
    0x016f4f, 0x016f87, 0x016f8f, 0x016f9f, 0x016fe0, 0x016fe4, 0x016ff0, 0x016ff1, 0x017000,
    0x0187f7, 0x018800, 0x018cd5, 0x018d00, 0x018d08, 0x01aff0, 0x01b122, 0x01b132, 0x01b132,
    0x01b150, 0x01b152, 0x01b155, 0x01b155, 0x01b164, 0x01b167, 0x01b170, 0x01b2fb, 0x01bc00,
    0x01bc6a, 0x01bc70, 0x01bc7c, 0x01bc80, 0x01bc88, 0x01bc90, 0x01bc99, 0x01bc9c, 0x01bc9f,
    0x01cf00, 0x01cf2d, 0x01cf30, 0x01cf46, 0x01cf50, 0x01cfc3, 0x01d000, 0x01d0f5, 0x01d100,
    0x01d126, 0x01d129, 0x01d172, 0x01d17b, 0x01d1ea, 0x01d200, 0x01d245, 0x01d2c0, 0x01d2d3,
    0x01d2e0, 0x01d2f3, 0x01d300, 0x01d356, 0x01d360, 0x01d378, 0x01d400, 0x01d49f, 0x01d4a2,
    0x01d4a2, 0x01d4a5, 0x01d4a6, 0x01d4a9, 0x01d50a, 0x01d50d, 0x01d546, 0x01d54a, 0x01d6a5,
    0x01d6a8, 0x01d7cb, 0x01d7ce, 0x01da8b, 0x01da9b, 0x01daaf, 0x01df00, 0x01df1e, 0x01df25,
    0x01df2a, 0x01e000, 0x01e018, 0x01e01b, 0x01e02a, 0x01e030, 0x01e06d, 0x01e08f, 0x01e08f,
    0x01e100, 0x01e12c, 0x01e130, 0x01e13d, 0x01e140, 0x01e149, 0x01e14e, 0x01e14f, 0x01e290,
    0x01e2ae, 0x01e2c0, 0x01e2f9, 0x01e2ff, 0x01e2ff, 0x01e4d0, 0x01e4f9, 0x01e7e0, 0x01e8c4,
    0x01e8c7, 0x01e8d6, 0x01e900, 0x01e94b, 0x01e950, 0x01e959, 0x01e95e, 0x01e95f, 0x01ec71,
    0x01ecb4, 0x01ed01, 0x01ed3d, 0x01ee00, 0x01ee24, 0x01ee27, 0x01ee3b, 0x01ee42, 0x01ee42,
    0x01ee47, 0x01ee54, 0x01ee57, 0x01ee64, 0x01ee67, 0x01ee9b, 0x01eea1, 0x01eebb, 0x01eef0,
    0x01eef1, 0x01f000, 0x01f02b, 0x01f030, 0x01f093, 0x01f0a0, 0x01f0ae, 0x01f0b1, 0x01f0f5,
    0x01f100, 0x01f1ad, 0x01f1e6, 0x01f202, 0x01f210, 0x01f23b, 0x01f240, 0x01f248, 0x01f250,
    0x01f251, 0x01f260, 0x01f265, 0x01f300, 0x01f6d7, 0x01f6dc, 0x01f6ec, 0x01f6f0, 0x01f6fc,
    0x01f700, 0x01f776, 0x01f77b, 0x01f7d9, 0x01f7e0, 0x01f7eb, 0x01f7f0, 0x01f7f0, 0x01f800,
    0x01f80b, 0x01f810, 0x01f847, 0x01f850, 0x01f859, 0x01f860, 0x01f887, 0x01f890, 0x01f8ad,
    0x01f8b0, 0x01f8b1, 0x01f900, 0x01fa53, 0x01fa60, 0x01fa6d, 0x01fa70, 0x01fa7c, 0x01fa80,
    0x01fa88, 0x01fa90, 0x01fac5, 0x01face, 0x01fadb, 0x01fae0, 0x01fae8, 0x01faf0, 0x01faf8,
    0x01fb00, 0x01fbca, 0x01fbf0, 0x01fbf9, 0x020000, 0x02a6df, 0x02a700, 0x02b739, 0x02b740,
    0x02b81d, 0x02b820, 0x02cea1, 0x02ceb0, 0x02ebe0, 0x02f800, 0x02fa1d, 0x030000, 0x03134a,
    0x031350, 0x0323af, 0x0e0100, 0x0e01ef,
];

const GO_IS_NOT_PRINT32: &[u16] = &[
    0x000c, 0x0027, 0x003b, 0x003e, 0x018f, 0x039e, 0x057b, 0x058b, 0x0593, 0x0596, 0x05a2, 0x05b2,
    0x05ba, 0x0786, 0x07b1, 0x0809, 0x0836, 0x0856, 0x08f3, 0x0a04, 0x0a14, 0x0a18, 0x0e7f, 0x0eaa,
    0x10bd, 0x1135, 0x11e0, 0x1212, 0x1287, 0x1289, 0x128e, 0x129e, 0x1304, 0x1329, 0x1331, 0x1334,
    0x133a, 0x145c, 0x1914, 0x1917, 0x1936, 0x1c09, 0x1c37, 0x1ca8, 0x1d07, 0x1d0a, 0x1d3b, 0x1d3e,
    0x1d66, 0x1d69, 0x1d8f, 0x1d92, 0x1f11, 0x246f, 0x6a5f, 0x6abf, 0x6b5a, 0x6b62, 0xaff4, 0xaffc,
    0xafff, 0xd455, 0xd49d, 0xd4ad, 0xd4ba, 0xd4bc, 0xd4c4, 0xd506, 0xd515, 0xd51d, 0xd53a, 0xd53f,
    0xd545, 0xd551, 0xdaa0, 0xe007, 0xe022, 0xe025, 0xe7e7, 0xe7ec, 0xe7ef, 0xe7ff, 0xee04, 0xee20,
    0xee23, 0xee28, 0xee33, 0xee38, 0xee3a, 0xee48, 0xee4a, 0xee4c, 0xee50, 0xee53, 0xee58, 0xee5a,
    0xee5c, 0xee5e, 0xee60, 0xee63, 0xee6b, 0xee73, 0xee78, 0xee7d, 0xee7f, 0xee8a, 0xeea4, 0xeeaa,
    0xf0c0, 0xf0d0, 0xfabe, 0xfb93,
];

fn go_is_print(character: char) -> bool {
    let value = u32::from(character);
    if value <= 0xff {
        return (0x20..=0x7e).contains(&value) || ((0xa1..=0xff).contains(&value) && value != 0xad);
    }
    if value < 1 << 16 {
        let value = value as u16;
        let index = GO_IS_PRINT16.partition_point(|candidate| *candidate < value);
        if index >= GO_IS_PRINT16.len()
            || value < GO_IS_PRINT16[index & !1]
            || GO_IS_PRINT16[index | 1] < value
        {
            return false;
        }
        return GO_IS_NOT_PRINT16.binary_search(&value).is_err();
    }
    let index = GO_IS_PRINT32.partition_point(|candidate| *candidate < value);
    if index >= GO_IS_PRINT32.len()
        || value < GO_IS_PRINT32[index & !1]
        || GO_IS_PRINT32[index | 1] < value
    {
        return false;
    }
    if value >= 0x20000 {
        return true;
    }
    GO_IS_NOT_PRINT32
        .binary_search(&((value - 0x10000) as u16))
        .is_err()
}
// END GENERATED GO STRCONV ISPRINT RANGES

/// Global argument-redaction mode used by source error constructors.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum RedactionMode {
    /// Preserve arguments.
    #[default]
    Disabled = 0,
    /// Replace sensitive arguments with a question mark.
    Enabled = 1,
    /// Surround sensitive arguments with redaction markers.
    Marker = 2,
}

static REDACTION_MODE: AtomicU8 = AtomicU8::new(RedactionMode::Disabled as u8);

/// Sets the process-wide source-compatible error argument redaction mode.
pub fn set_redaction_mode(mode: RedactionMode) {
    REDACTION_MODE.store(mode as u8, Ordering::Relaxed);
}

/// Returns the process-wide error argument redaction mode.
#[must_use]
pub fn redaction_mode() -> RedactionMode {
    match REDACTION_MODE.load(Ordering::Relaxed) {
        1 => RedactionMode::Enabled,
        2 => RedactionMode::Marker,
        _ => RedactionMode::Disabled,
    }
}

/// Owned representation of a Go fmt argument.
///
/// Typed forms preserve Go's success and `%!verb(type=value)` mismatch paths;
/// the source catalog vocabulary itself is string, decimal, display, and
/// fixed/dynamic string precision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormatArg {
    display: String,
    debug: String,
    type_name: String,
    precision: Option<isize>,
    kind: FormatKind,
    character: Option<char>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormatKind {
    String,
    Signed,
    Unsigned,
    Bool,
    Float,
    Char,
    Nil,
    Custom,
}

impl FormatArg {
    /// Constructs an argument with explicit Go display/debug/type forms.
    #[must_use]
    pub fn new(
        display: impl Into<String>,
        debug: impl Into<String>,
        type_name: impl Into<String>,
    ) -> Self {
        Self {
            display: display.into(),
            debug: debug.into(),
            type_name: type_name.into(),
            precision: None,
            kind: FormatKind::Custom,
            character: None,
        }
    }

    /// Constructs the Go nil interface representation.
    #[must_use]
    pub fn nil() -> Self {
        Self {
            kind: FormatKind::Nil,
            ..Self::new("<nil>", "<nil>", "<nil>")
        }
    }
}

impl From<&str> for FormatArg {
    fn from(value: &str) -> Self {
        Self {
            kind: FormatKind::String,
            ..Self::new(value, format!("{value:?}"), "string")
        }
    }
}

impl From<String> for FormatArg {
    fn from(value: String) -> Self {
        Self {
            kind: FormatKind::String,
            ..Self::new(value.clone(), format!("{value:?}"), "string")
        }
    }
}

macro_rules! integer_format_arg {
    ($($type:ty => $go_name:literal => $kind:ident),+ $(,)?) => {$(
        impl From<$type> for FormatArg {
            fn from(value: $type) -> Self {
                let kind = FormatKind::$kind;
                Self {
                    display: value.to_string(),
                    debug: if kind == FormatKind::Unsigned {
                        format!("0x{value:x}")
                    } else {
                        value.to_string()
                    },
                    type_name: $go_name.to_owned(),
                    precision: isize::try_from(value).ok(),
                    kind,
                    character: None,
                }
            }
        }
    )+};
}

integer_format_arg! {
    i8 => "int8" => Signed, i16 => "int16" => Signed, i32 => "int32" => Signed,
    i64 => "int64" => Signed, isize => "int" => Signed,
    u8 => "uint8" => Unsigned, u16 => "uint16" => Unsigned, u32 => "uint32" => Unsigned,
    u64 => "uint64" => Unsigned, usize => "uint" => Unsigned,
}

impl From<bool> for FormatArg {
    fn from(value: bool) -> Self {
        Self {
            kind: FormatKind::Bool,
            ..Self::new(value.to_string(), value.to_string(), "bool")
        }
    }
}

macro_rules! float_format_arg {
    ($($type:ty => $go_name:literal),+ $(,)?) => {$(
        impl From<$type> for FormatArg {
            fn from(value: $type) -> Self {
                let display = if value.is_nan() {
                    "NaN".to_owned()
                } else if value == <$type>::INFINITY {
                    "+Inf".to_owned()
                } else if value == <$type>::NEG_INFINITY {
                    "-Inf".to_owned()
                } else if value != 0.0 && (value.abs() >= 1e6 || value.abs() < 1e-4) {
                    let scientific = format!("{value:e}");
                    let (mantissa, exponent) = scientific
                        .split_once('e')
                        .expect("Rust scientific formatting contains e");
                    let exponent = exponent.parse::<i32>().expect("numeric exponent");
                    format!("{mantissa}e{exponent:+03}")
                } else {
                    value.to_string()
                };
                Self {
                    kind: FormatKind::Float,
                    ..Self::new(display.clone(), display, $go_name)
                }
            }
        }
    )+};
}
float_format_arg! { f32 => "float32", f64 => "float64" }

impl From<char> for FormatArg {
    fn from(value: char) -> Self {
        let codepoint = u32::from(value).to_string();
        Self {
            display: codepoint.clone(),
            debug: codepoint,
            type_name: "int32".to_owned(),
            precision: None,
            kind: FormatKind::Char,
            character: Some(value),
        }
    }
}

/// MySQL wire error information produced while executing SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlError {
    /// Protocol error number.
    pub code: u16,
    /// Rendered error message.
    pub message: String,
    /// Five-byte SQLSTATE string.
    pub state: &'static str,
}

impl SqlError {
    /// Source NewErr constructor.
    #[must_use]
    pub fn new(code: u16, args: &[FormatArg]) -> Self {
        let message = message_by_code(code).map_or_else(
            || sprint(args),
            |entry| format_template(entry.raw, entry.redact_arg_pos, args),
        );
        Self {
            code,
            message,
            state: mysql_state(code),
        }
    }

    /// Source NewErrf constructor.
    #[must_use]
    pub fn new_f(code: u16, format: &str, redact_arg_pos: &[usize], args: &[FormatArg]) -> Self {
        Self {
            code,
            message: format_template(format, redact_arg_pos, args),
            state: mysql_state(code),
        }
    }
}

// fmt.Sprint inserts a space between adjacent operands only when neither one
// is a string. This subtle rule matters for unknown error codes.
fn sprint(args: &[FormatArg]) -> String {
    let mut output = String::new();
    let mut previous_was_string = false;
    for (index, argument) in args.iter().enumerate() {
        let is_string = argument.type_name == "string";
        if index != 0 && !previous_was_string && !is_string {
            output.push(' ');
        }
        output.push_str(&argument.display);
        previous_was_string = is_string;
    }
    output
}

impl fmt::Display for SqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ERROR {} ({}): {}",
            self.code, self.state, self.message
        )
    }
}

impl Error for SqlError {}

fn mismatch(verb: u8, argument: &FormatArg) -> String {
    if argument.kind == FormatKind::Nil {
        format!("%!{}(<nil>)", char::from(verb))
    } else {
        format!(
            "%!{}({}={})",
            char::from(verb),
            argument.type_name,
            argument.display
        )
    }
}

fn escaped_character(character: char, quote: char, ascii_only: bool) -> String {
    if ascii_only && !character.is_ascii() {
        return if u32::from(character) <= 0xffff {
            format!("\\u{:04x}", u32::from(character))
        } else {
            format!("\\U{:08x}", u32::from(character))
        };
    }
    let escaped = match character {
        '\u{7}' => "\\a".to_owned(),
        '\u{8}' => "\\b".to_owned(),
        '\u{c}' => "\\f".to_owned(),
        '\n' => "\\n".to_owned(),
        '\r' => "\\r".to_owned(),
        '\t' => "\\t".to_owned(),
        '\u{b}' => "\\v".to_owned(),
        '\\' => "\\\\".to_owned(),
        '\'' if quote == '\'' => "\\'".to_owned(),
        '"' if quote == '"' => "\\\"".to_owned(),
        value if u32::from(value) < 0x80 && !go_is_print(value) => {
            format!("\\x{:02x}", u32::from(value))
        }
        value if u32::from(value) <= 0xffff && !go_is_print(value) => {
            format!("\\u{:04x}", u32::from(value))
        }
        value if !go_is_print(value) => format!("\\U{:08x}", u32::from(value)),
        value => value.to_string(),
    };
    escaped
}

fn quoted_character_with_ascii(character: char, ascii_only: bool) -> String {
    format!("'{}'", escaped_character(character, '\'', ascii_only))
}

fn quoted_character(character: char) -> String {
    quoted_character_with_ascii(character, false)
}

fn go_can_backquote(character: char) -> bool {
    if !character.is_ascii() {
        return character != '\u{feff}';
    }
    character == '\t' || ((' '..='~').contains(&character) && character != '`')
}

fn quoted_string(value: &str, alternate: bool, ascii_only: bool) -> String {
    if alternate && value.chars().all(go_can_backquote) {
        return format!("`{value}`");
    }
    let mut rendered = String::with_capacity(value.len() + 2);
    rendered.push('"');
    for character in value.chars() {
        rendered.push_str(&escaped_character(character, '"', ascii_only));
    }
    rendered.push('"');
    rendered
}

fn integer_character(argument: &FormatArg) -> char {
    let codepoint = match argument.kind {
        FormatKind::Signed => argument
            .display
            .parse::<i128>()
            .ok()
            .and_then(|value| u32::try_from(value).ok()),
        FormatKind::Unsigned => argument
            .display
            .parse::<u128>()
            .ok()
            .and_then(|value| u32::try_from(value).ok()),
        FormatKind::Char => argument.character.map(u32::from),
        _ => None,
    };
    codepoint
        .and_then(char::from_u32)
        .unwrap_or(char::REPLACEMENT_CHARACTER)
}

fn truncate(value: &str, precision: Option<usize>) -> String {
    precision.map_or_else(
        || value.to_owned(),
        |maximum| value.chars().take(maximum).collect(),
    )
}

#[derive(Clone, Copy, Debug, Default)]
struct FormatSpec {
    alternate: bool,
    left: bool,
    plus: bool,
    space: bool,
    zero: bool,
    width: Option<usize>,
    precision: Option<usize>,
}

fn pad_width(value: String, spec: FormatSpec, numeric: bool) -> String {
    let Some(padding) = spec
        .width
        .map(|width| width.saturating_sub(value.chars().count()))
        .filter(|padding| *padding > 0)
    else {
        return value;
    };
    if spec.left {
        return format!("{value}{}", " ".repeat(padding));
    }
    if spec.zero {
        let prefix_length = if numeric {
            let sign_length = usize::from(value.starts_with(['+', '-', ' ']));
            let rest = &value[sign_length..];
            sign_length
                + if rest.starts_with("0x")
                    || rest.starts_with("0X")
                    || rest.starts_with("0b")
                    || rest.starts_with("0o")
                {
                    2
                } else {
                    0
                }
        } else {
            0
        };
        let (prefix, rest) = value.split_at(prefix_length);
        return format!("{prefix}{}{rest}", "0".repeat(padding));
    }
    format!("{}{value}", " ".repeat(padding))
}

fn pad_hex_float_width(value: String, spec: FormatSpec) -> String {
    let Some(padding) = spec
        .width
        .map(|width| width.saturating_sub(value.chars().count()))
        .filter(|padding| *padding > 0 && spec.zero && !spec.left)
    else {
        return pad_width(value, spec, true);
    };
    let sign_length = usize::from(value.starts_with(['+', '-', ' ']));
    let (sign, magnitude) = value.split_at(sign_length);
    format!("{sign}{}{magnitude}", "0".repeat(padding))
}

fn decimal_integer(argument: &FormatArg, spec: FormatSpec) -> String {
    let value = argument.display.as_str();
    let (negative, digits) = value
        .strip_prefix('-')
        .map_or((false, value), |digits| (true, digits));
    let sign = if negative {
        "-"
    } else if spec.plus {
        "+"
    } else if spec.space {
        " "
    } else {
        ""
    };
    let digits = if spec.precision == Some(0) && digits == "0" {
        ""
    } else {
        digits
    };
    let zeroes = spec
        .precision
        .map(|minimum| minimum.saturating_sub(digits.len()))
        .unwrap_or(0);
    let width_spec = FormatSpec {
        zero: spec.zero && spec.precision.is_none(),
        ..spec
    };
    pad_width(
        format!("{sign}{}{digits}", "0".repeat(zeroes)),
        width_spec,
        true,
    )
}

fn binary_float_hex(argument: &FormatArg, upper: bool, spec: FormatSpec) -> String {
    if let Some(value) = special_float(argument, spec) {
        return value;
    }
    let (negative, exponent_bits, fraction, stored_fraction_bits, bias, subnormal_exponent) =
        if argument.type_name == "float32" {
            let bits = argument
                .display
                .parse::<f32>()
                .expect("stored finite float32")
                .to_bits();
            (
                bits >> 31 != 0,
                u64::from((bits >> 23) & 0xff),
                u64::from(bits & 0x7f_ffff),
                23_u32,
                127_i32,
                -149_i32,
            )
        } else {
            let bits = argument
                .display
                .parse::<f64>()
                .expect("stored finite float64")
                .to_bits();
            (
                bits >> 63 != 0,
                (bits >> 52) & 0x7ff,
                bits & 0x000f_ffff_ffff_ffff,
                52_u32,
                1023_i32,
                -1074_i32,
            )
        };
    if exponent_bits == 0 && fraction == 0 {
        let fraction = spec.precision.map_or_else(
            || {
                if spec.alternate {
                    if upper {
                        ".".to_owned()
                    } else {
                        ".0000".to_owned()
                    }
                } else {
                    String::new()
                }
            },
            |precision| {
                if precision == 0 {
                    String::new()
                } else {
                    format!(".{}", "0".repeat(precision))
                }
            },
        );
        let sign = if negative {
            "-"
        } else if spec.plus {
            "+"
        } else if spec.space {
            " "
        } else {
            ""
        };
        let point = if spec.alternate && fraction.is_empty() {
            "."
        } else {
            &fraction
        };
        let raw = format!(
            "{sign}{}{point}{}+00",
            if upper { "0X0" } else { "0x0" },
            if upper { 'P' } else { 'p' }
        );
        return pad_hex_float_width(raw, spec);
    }
    let (mut exponent, fraction, fraction_bits) = if exponent_bits == 0 {
        let highest_bit = 63 - fraction.leading_zeros();
        let fraction_without_leader = fraction ^ (1_u64 << highest_bit);
        (
            subnormal_exponent + i32::try_from(highest_bit).expect("fraction bit index"),
            fraction_without_leader,
            highest_bit,
        )
    } else {
        (
            i32::try_from(exponent_bits).expect("small exponent") - bias,
            fraction,
            stored_fraction_bits,
        )
    };

    let exact_nibbles = fraction_bits.div_ceil(4);
    let aligned_fraction = fraction << (exact_nibbles * 4 - fraction_bits);
    let fraction = if let Some(precision) = spec.precision {
        let target_bits = precision.saturating_mul(4);
        if target_bits >= usize::try_from(fraction_bits).expect("small fraction width") {
            let exact_nibbles = usize::try_from(exact_nibbles).expect("small nibble count");
            let mut digits = if exact_nibbles == 0 {
                String::new()
            } else if upper {
                format!("{aligned_fraction:0exact_nibbles$X}")
            } else {
                format!("{aligned_fraction:0exact_nibbles$x}")
            };
            digits.push_str(&"0".repeat(precision.saturating_sub(exact_nibbles)));
            digits
        } else {
            let target_bits = u32::try_from(target_bits).expect("target below fraction width");
            let dropped = fraction_bits - target_bits;
            let significand = (1_u64 << fraction_bits) | fraction;
            let mut retained = significand >> dropped;
            let remainder = significand & ((1_u64 << dropped) - 1);
            let halfway = 1_u64 << (dropped - 1);
            if remainder > halfway || remainder == halfway && retained & 1 == 1 {
                retained += 1;
            }
            if retained == 2_u64 << target_bits {
                exponent += 1;
                retained = 1_u64 << target_bits;
            }
            let fraction = retained ^ (1_u64 << target_bits);
            if precision == 0 {
                String::new()
            } else if upper {
                format!("{fraction:0precision$X}")
            } else {
                format!("{fraction:0precision$x}")
            }
        }
    } else {
        let exact_nibbles = usize::try_from(exact_nibbles).expect("small nibble count");
        let mut digits = if upper {
            format!("{aligned_fraction:0exact_nibbles$X}")
        } else {
            format!("{aligned_fraction:0exact_nibbles$x}")
        };
        while digits.ends_with('0') {
            digits.pop();
        }
        digits
    };
    let fraction = if spec.alternate && !upper && spec.precision.is_none() && fraction.len() < 4 {
        format!("{fraction}{}", "0".repeat(4 - fraction.len()))
    } else {
        fraction
    };
    let point = if fraction.is_empty() && !spec.alternate {
        String::new()
    } else {
        format!(".{fraction}")
    };
    let prefix = if upper { "0X1" } else { "0x1" };
    let exponent_marker = if upper { 'P' } else { 'p' };
    let sign = if negative {
        "-"
    } else if spec.plus {
        "+"
    } else if spec.space {
        " "
    } else {
        ""
    };
    pad_hex_float_width(
        format!("{sign}{prefix}{point}{exponent_marker}{exponent:+03}"),
        spec,
    )
}

fn apply_float_sign(value: String, spec: FormatSpec) -> String {
    if value.starts_with(['+', '-']) || value == "NaN" {
        value
    } else if spec.plus {
        format!("+{value}")
    } else if spec.space {
        format!(" {value}")
    } else {
        value
    }
}

fn special_float(argument: &FormatArg, spec: FormatSpec) -> Option<String> {
    let value = match argument.display.as_str() {
        "+Inf" if spec.space && !spec.plus => " Inf".to_owned(),
        "+Inf" => "+Inf".to_owned(),
        "-Inf" => "-Inf".to_owned(),
        "NaN" if spec.plus => "+NaN".to_owned(),
        "NaN" if spec.space => " NaN".to_owned(),
        "NaN" => "NaN".to_owned(),
        _ => return None,
    };
    Some(pad_width(
        value,
        FormatSpec {
            zero: false,
            ..spec
        },
        true,
    ))
}

fn scientific_float(argument: &FormatArg, upper: bool, spec: FormatSpec) -> String {
    if let Some(value) = special_float(argument, spec) {
        return value;
    }
    let precision = spec.precision.unwrap_or(6);
    let raw = if argument.type_name == "float32" {
        let value = argument
            .display
            .parse::<f32>()
            .expect("stored finite float32");
        format!("{value:.precision$e}")
    } else {
        let value = argument
            .display
            .parse::<f64>()
            .expect("stored finite float64");
        format!("{value:.precision$e}")
    };
    let (mut mantissa, exponent) = raw.split_once('e').expect("scientific format exponent");
    let owned_mantissa;
    if spec.alternate && !mantissa.contains('.') {
        owned_mantissa = format!("{mantissa}.");
        mantissa = &owned_mantissa;
    }
    let exponent = exponent.parse::<i32>().expect("numeric exponent");
    let marker = if upper { 'E' } else { 'e' };
    pad_width(
        apply_float_sign(format!("{mantissa}{marker}{exponent:+03}"), spec),
        spec,
        true,
    )
}

fn general_float(argument: &FormatArg, upper: bool, spec: FormatSpec) -> String {
    if let Some(value) = special_float(argument, spec) {
        return value;
    }
    if spec.precision.is_none() && !spec.alternate {
        let rendered = if upper {
            argument.display.replace('e', "E")
        } else {
            argument.display.clone()
        };
        return pad_width(apply_float_sign(rendered, spec), spec, true);
    }
    let value = argument
        .display
        .parse::<f64>()
        .expect("stored finite float");
    let precision = spec.precision.unwrap_or(6).max(1);
    let exponent = if value == 0.0 {
        0
    } else {
        let significant_fraction = precision - 1;
        let rounded_scientific = if argument.type_name == "float32" {
            let value = argument.display.parse::<f32>().expect("stored float32");
            format!("{value:.significant_fraction$e}")
        } else {
            format!("{value:.significant_fraction$e}")
        };
        rounded_scientific
            .split_once('e')
            .expect("scientific exponent")
            .1
            .parse::<i32>()
            .expect("numeric exponent")
    };
    let mut rendered = if exponent < -4 || exponent >= i32::try_from(precision).unwrap_or(i32::MAX)
    {
        let scientific_spec = FormatSpec {
            width: None,
            plus: false,
            space: false,
            zero: false,
            left: false,
            precision: Some(precision - 1),
            ..spec
        };
        scientific_float(argument, upper, scientific_spec)
    } else {
        let decimal_places =
            usize::try_from(i32::try_from(precision).unwrap_or(i32::MAX) - exponent - 1)
                .unwrap_or(0);
        if argument.type_name == "float32" {
            let value = argument.display.parse::<f32>().expect("stored float32");
            format!("{value:.decimal_places$}")
        } else {
            format!("{value:.decimal_places$}")
        }
    };
    if !spec.alternate {
        let exponent = rendered.find(['e', 'E']);
        let suffix = exponent.map(|position| rendered.split_off(position));
        if rendered.contains('.') {
            while rendered.ends_with('0') {
                rendered.pop();
            }
            if rendered.ends_with('.') {
                rendered.pop();
            }
        }
        if let Some(suffix) = suffix {
            rendered.push_str(&suffix);
        }
    } else if !rendered.contains('.') {
        if let Some(position) = rendered.find(['e', 'E']) {
            rendered.insert(position, '.');
        } else {
            rendered.push('.');
        }
    }
    if upper {
        rendered = rendered.replace('e', "E");
    }
    pad_width(apply_float_sign(rendered, spec), spec, true)
}

fn binary_float_decimal(argument: &FormatArg, spec: FormatSpec) -> String {
    if let Some(value) = special_float(argument, spec) {
        return value;
    }
    let (negative, mantissa, exponent) = if argument.type_name == "float32" {
        let bits = argument
            .display
            .parse::<f32>()
            .expect("stored finite float32")
            .to_bits();
        let exponent_bits = (bits >> 23) & 0xff;
        let fraction = bits & 0x7f_ffff;
        if exponent_bits == 0 {
            (bits >> 31 != 0, u64::from(fraction), -149)
        } else {
            (
                bits >> 31 != 0,
                u64::from((1 << 23) | fraction),
                i32::try_from(exponent_bits).expect("8-bit exponent") - 127 - 23,
            )
        }
    } else {
        let bits = argument
            .display
            .parse::<f64>()
            .expect("stored finite float64")
            .to_bits();
        let exponent_bits = (bits >> 52) & 0x7ff;
        let fraction = bits & 0x000f_ffff_ffff_ffff;
        if exponent_bits == 0 {
            (bits >> 63 != 0, fraction, -1074)
        } else {
            (
                bits >> 63 != 0,
                (1_u64 << 52) | fraction,
                i32::try_from(exponent_bits).expect("11-bit exponent") - 1023 - 52,
            )
        }
    };
    let sign = if negative {
        "-"
    } else if spec.plus {
        "+"
    } else if spec.space {
        " "
    } else {
        ""
    };
    pad_width(format!("{sign}{mantissa}p{exponent:+}"), spec, true)
}

fn radix_integer(argument: &FormatArg, verb: u8, spec: FormatSpec) -> String {
    let (negative, magnitude) = if argument.kind == FormatKind::Signed {
        let value = argument
            .display
            .parse::<i128>()
            .expect("stored signed integer");
        (value < 0, value.unsigned_abs())
    } else if argument.kind == FormatKind::Char {
        (
            false,
            u128::from(u32::from(argument.character.expect("char argument"))),
        )
    } else {
        (
            false,
            argument
                .display
                .parse::<u128>()
                .expect("stored unsigned integer"),
        )
    };
    let mut digits = match verb {
        b'b' => format!("{magnitude:b}"),
        b'o' | b'O' => format!("{magnitude:o}"),
        b'x' => format!("{magnitude:x}"),
        b'X' => format!("{magnitude:X}"),
        _ => unreachable!("radix integer verb"),
    };
    if spec.precision == Some(0) && magnitude == 0 {
        digits.clear();
    } else if let Some(precision) = spec.precision {
        digits.insert_str(0, &"0".repeat(precision.saturating_sub(digits.len())));
    }
    if verb == b'o' && spec.alternate && !digits.is_empty() && !digits.starts_with('0') {
        digits.insert(0, '0');
    }
    if verb == b'O' && spec.alternate && magnitude != 0 && !digits.starts_with('0') {
        digits.insert(0, '0');
    }
    let prefix = match verb {
        b'b' if spec.alternate && !digits.is_empty() => "0b",
        b'O' if !digits.is_empty() => "0o",
        b'x' if spec.alternate && !digits.is_empty() => "0x",
        b'X' if spec.alternate && !digits.is_empty() => "0X",
        _ => "",
    };
    let sign = if digits.is_empty() {
        ""
    } else if negative {
        "-"
    } else if spec.plus {
        "+"
    } else if spec.space {
        " "
    } else {
        ""
    };
    let width_spec = FormatSpec {
        width: spec
            .width
            .map(|width| width + usize::from(spec.zero && spec.precision.is_none()) * prefix.len()),
        zero: spec.zero && spec.precision.is_none(),
        ..spec
    };
    pad_width(format!("{sign}{prefix}{digits}"), width_spec, true)
}

fn unicode_integer(argument: &FormatArg, spec: FormatSpec) -> String {
    let value = match argument.kind {
        FormatKind::Signed => {
            let value = argument
                .display
                .parse::<i128>()
                .expect("stored signed integer");
            if value < 0 {
                u128::from(value as u64)
            } else {
                u128::try_from(value).expect("nonnegative signed integer")
            }
        }
        FormatKind::Unsigned => argument
            .display
            .parse::<u128>()
            .expect("stored unsigned integer"),
        FormatKind::Char => u128::from(u32::from(argument.character.expect("char argument"))),
        _ => unreachable!("Unicode integer kind"),
    };
    let digits = spec.precision.unwrap_or(4).max(4);
    let rendered = format!("U+{value:0digits$X}");
    let character = u32::try_from(value).ok().and_then(char::from_u32);
    let rendered = if spec.alternate && character.is_some_and(go_is_print) {
        format!(
            "{rendered} {}",
            quoted_character(character.expect("checked printable character"))
        )
    } else {
        rendered
    };
    pad_width(
        rendered,
        FormatSpec {
            zero: false,
            ..spec
        },
        false,
    )
}

fn render_argument(verb: u8, spec: FormatSpec, argument: &FormatArg) -> String {
    match verb {
        b's' => match argument.kind {
            FormatKind::String | FormatKind::Custom => {
                pad_width(truncate(&argument.display, spec.precision), spec, false)
            }
            _ => mismatch(verb, argument),
        },
        b'd' => match argument.kind {
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char | FormatKind::Custom => {
                decimal_integer(argument, spec)
            }
            _ => mismatch(verb, argument),
        },
        b'q' => match argument.kind {
            FormatKind::String => pad_width(
                quoted_string(
                    &truncate(&argument.display, spec.precision),
                    spec.alternate,
                    spec.plus,
                ),
                spec,
                false,
            ),
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char => pad_width(
                quoted_character_with_ascii(integer_character(argument), spec.plus),
                spec,
                false,
            ),
            _ => mismatch(verb, argument),
        },
        b'c' => match argument.kind {
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char => {
                pad_width(integer_character(argument).to_string(), spec, false)
            }
            _ => mismatch(verb, argument),
        },
        b'v' => {
            let go_syntax = spec.alternate;
            let spec = FormatSpec {
                alternate: false,
                plus: false,
                ..spec
            };
            match argument.kind {
                FormatKind::String if go_syntax => pad_width(
                    quoted_string(&truncate(&argument.display, spec.precision), false, false),
                    spec,
                    false,
                ),
                FormatKind::String => {
                    pad_width(truncate(&argument.display, spec.precision), spec, false)
                }
                FormatKind::Unsigned if go_syntax => radix_integer(
                    argument,
                    b'x',
                    FormatSpec {
                        alternate: true,
                        ..spec
                    },
                ),
                FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char => {
                    decimal_integer(argument, spec)
                }
                FormatKind::Float => general_float(argument, false, spec),
                FormatKind::Custom if go_syntax => {
                    pad_width(truncate(&argument.debug, spec.precision), spec, false)
                }
                FormatKind::Custom => {
                    pad_width(truncate(&argument.display, spec.precision), spec, false)
                }
                _ => pad_width(argument.display.clone(), spec, false),
            }
        }
        b'T' if argument.kind == FormatKind::Nil => {
            pad_width(argument.type_name.clone(), spec, false)
        }
        b'T' => pad_width(truncate(&argument.type_name, spec.precision), spec, false),
        b't' if argument.kind == FormatKind::Bool => {
            pad_width(argument.display.clone(), spec, false)
        }
        b'f' | b'F' if argument.kind == FormatKind::Float => {
            if let Some(value) = special_float(argument, spec) {
                value
            } else {
                let precision = spec.precision.unwrap_or(6);
                let mut rendered = if argument.type_name == "float32" {
                    argument.display.parse::<f32>().map_or_else(
                        |_| argument.display.clone(),
                        |value| format!("{value:.precision$}"),
                    )
                } else {
                    argument.display.parse::<f64>().map_or_else(
                        |_| argument.display.clone(),
                        |value| format!("{value:.precision$}"),
                    )
                };
                if spec.alternate && !rendered.contains('.') {
                    rendered.push('.');
                }
                pad_width(apply_float_sign(rendered, spec), spec, true)
            }
        }
        b'e' | b'E' if argument.kind == FormatKind::Float => {
            scientific_float(argument, verb == b'E', spec)
        }
        b'g' | b'G' if argument.kind == FormatKind::Float => {
            general_float(argument, verb == b'G', spec)
        }
        b'b' if argument.kind == FormatKind::Float => binary_float_decimal(argument, spec),
        b'b' | b'o' | b'O' | b'x' | b'X'
            if matches!(
                argument.kind,
                FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char
            ) =>
        {
            radix_integer(argument, verb, spec)
        }
        b'U' if matches!(
            argument.kind,
            FormatKind::Signed | FormatKind::Unsigned | FormatKind::Char
        ) =>
        {
            unicode_integer(argument, spec)
        }
        b'x' | b'X' if argument.kind == FormatKind::String => {
            let upper = verb == b'X';
            let bytes = argument.display.as_bytes();
            let bytes = &bytes[..spec.precision.unwrap_or(bytes.len()).min(bytes.len())];
            let prefix = if spec.alternate {
                if upper {
                    "0X"
                } else {
                    "0x"
                }
            } else {
                ""
            };
            let mut rendered = bytes
                .iter()
                .map(|byte| {
                    if upper {
                        format!("{}{byte:02X}", if spec.space { prefix } else { "" })
                    } else {
                        format!("{}{byte:02x}", if spec.space { prefix } else { "" })
                    }
                })
                .collect::<Vec<_>>()
                .join(if spec.space { " " } else { "" });
            if spec.alternate && !spec.space && !bytes.is_empty() {
                rendered.insert_str(0, prefix);
            }
            pad_width(rendered, spec, false)
        }
        b'x' | b'X' if argument.kind == FormatKind::Float => {
            binary_float_hex(argument, verb == b'X', spec)
        }
        _ => mismatch(verb, argument),
    }
}

fn format_template(template: &str, redact_positions: &[usize], args: &[FormatArg]) -> String {
    let bytes = template.as_bytes();
    let mut output = String::with_capacity(template.len());
    let mut cursor = 0;
    let mut argument_index = 0;

    while cursor < bytes.len() {
        if bytes[cursor] != b'%' {
            let next = bytes[cursor..]
                .iter()
                .position(|byte| *byte == b'%')
                .map_or(bytes.len(), |offset| cursor + offset);
            output.push_str(&template[cursor..next]);
            cursor = next;
            continue;
        }
        if bytes.get(cursor + 1) == Some(&b'%') {
            output.push('%');
            cursor += 2;
            continue;
        }

        cursor += 1;
        let mut spec = FormatSpec::default();
        while let Some(flag @ (b'#' | b'-' | b'+' | b' ' | b'0')) = bytes.get(cursor).copied() {
            match flag {
                b'#' => spec.alternate = true,
                b'-' => spec.left = true,
                b'+' => spec.plus = true,
                b' ' => spec.space = true,
                b'0' => spec.zero = true,
                _ => unreachable!(),
            }
            cursor += 1;
        }
        let width_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor > width_start {
            spec.width = template[width_start..cursor].parse().ok();
        }

        let mut bad_precision = false;
        if bytes.get(cursor) == Some(&b'.') {
            cursor += 1;
            if bytes.get(cursor) == Some(&b'*') {
                let dynamic = args.get(argument_index).and_then(|arg| arg.precision);
                argument_index += 1;
                match dynamic {
                    Some(value) if value >= 0 => spec.precision = usize::try_from(value).ok(),
                    _ => bad_precision = true,
                }
                cursor += 1;
            } else {
                let start = cursor;
                while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                    cursor += 1;
                }
                spec.precision = template[start..cursor].parse().ok();
            }
        }

        let Some(verb) = bytes.get(cursor).copied() else {
            output.push_str("%!(NOVERB)");
            break;
        };
        cursor += 1;
        if bad_precision {
            output.push_str("%!(BADPREC)");
        }
        let Some(argument) = args.get(argument_index) else {
            output.push_str("%!");
            output.push(char::from(verb));
            output.push_str("(MISSING)");
            continue;
        };
        let sensitive = redact_positions.contains(&argument_index);
        argument_index += 1;

        // Go redaction happens before fmt.Sprintf: Enabled replaces the
        // interface argument with the string "?". The original verb still
        // applies, including fmt's type-mismatch diagnostics and `%#v`
        // quoting. Marker mode instead wraps the original verb rendering.
        let redacted;
        let argument = if sensitive && redaction_mode() == RedactionMode::Enabled {
            redacted = FormatArg::from("?");
            &redacted
        } else {
            argument
        };
        let rendered = render_argument(verb, spec, argument);
        match (sensitive, redaction_mode()) {
            (true, RedactionMode::Marker) => {
                output.push('‹');
                for character in rendered.chars() {
                    output.push(character);
                    if matches!(character, '‹' | '›') {
                        output.push(character);
                    }
                }
                output.push('›');
            }
            _ => output.push_str(&rendered),
        }
    }

    if argument_index < args.len() {
        output.push_str("%!(EXTRA ");
        for (offset, argument) in args[argument_index..].iter().enumerate() {
            if offset != 0 {
                output.push_str(", ");
            }
            output.push_str(&argument.type_name);
            output.push('=');
            output.push_str(&argument.display);
        }
        output.push(')');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::errcode::{ErrDupEntry, ErrNoDB};

    #[test]
    fn source_error_rendering_and_redaction() {
        assert_eq!(
            SqlError::new(ErrNoDB, &[]).to_string(),
            "ERROR 1046 (3D000): No database selected"
        );
        assert_eq!(
            SqlError::new_f(0, "customized error", &[], &[]).to_string(),
            "ERROR 0 (HY000): customized error"
        );

        set_redaction_mode(RedactionMode::Enabled);
        assert_eq!(
            SqlError::new_f(
                ErrDupEntry,
                "Duplicate entry '%-.64s' for key '%-.192s'",
                &[0],
                &[FormatArg::from("secret"), FormatArg::from("primary")]
            )
            .message,
            "Duplicate entry '?' for key 'primary'"
        );

        let arguments = [
            FormatArg::from("secret"),
            FormatArg::from(7_i64),
            FormatArg::from("value"),
            FormatArg::from("debug"),
        ];
        assert_eq!(
            SqlError::new_f(0, "%s %d %v %#v", &[0, 1, 2, 3], &arguments).message,
            "? %!d(string=?) ? \"?\""
        );

        set_redaction_mode(RedactionMode::Marker);
        assert_eq!(
            SqlError::new_f(0, "%s %d %v %#v", &[0, 1, 2, 3], &arguments).message,
            "‹secret› ‹7› ‹value› ‹\"debug\"›"
        );
        set_redaction_mode(RedactionMode::Disabled);
    }
}
