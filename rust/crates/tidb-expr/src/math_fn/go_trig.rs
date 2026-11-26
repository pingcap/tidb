// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bit-exact transcriptions of Go's `math.Sin`, `math.Cos`, and `math.Tan`.
//!
//! MySQL compatibility is mostly arbitrary, and floating-point results are
//! the arbitrary part: TiDB evaluates `SIN`/`COS`/`TAN`/`COT` through Go's
//! standard library, whose Cephes-derived implementations round differently
//! from the system libm that `f64::sin`/`f64::cos`/`f64::tan` call -- by one
//! ulp on ordinary inputs like `cot(1)` (`0x3fe48e...08` vs `...06`), which
//! the differential suite catches. So instead of calling libm we transcribe
//! Go's own algorithms (Go src/math/sin.go, tan.go, and trig_reduce.go,
//! BSD licensed) so every input rounds exactly where Go rounds.
//!
//! Go's `haveArchSin/Cos/Tan` fast paths are assembly for other GOARCHes;
//! amd64 and arm64 use the Go bodies. The arm64 path reproduces the
//! multiply-add contraction that Go applies there.

/// Pi/4 split into three parts (Go sin.go/tan.go `PI4A/B/C`).
const PI4A: f64 = 7.853_981_256_484_985e-1;
const PI4B: f64 = 3.774_894_707_930_798e-8;
const PI4C: f64 = 2.695_151_429_079_059_5e-15;

const REDUCE_THRESHOLD: f64 = 1.0 * (1u64 << 29) as f64;

// sin coefficients (Go `_sin`).
const SIN: [f64; 6] = [
    1.589_623_015_765_465_6e-10,
    -2.505_074_776_285_780_7e-8,
    2.755_731_362_138_572_2e-6,
    -1.984_126_982_958_954e-4,
    8.333_333_333_322_118e-3,
    -1.666_666_666_666_663e-1,
];

// cos coefficients (Go `_cos`).
const COS: [f64; 6] = [
    -1.135_853_652_138_768_2e-11,
    2.087_570_084_197_473e-9,
    -2.755_731_417_929_674e-7,
    2.480_158_728_885_170_4e-5,
    -1.388_888_888_887_305_6e-3,
    4.166_666_666_666_659_5e-2,
];

// tan coefficients (Go `_tanP`, `_tanQ`).
const TAN_P: [f64; 3] = [
    -1.309_369_391_813_837_9e4,
    1.153_516_648_385_874_2e6,
    -1.795_652_519_764_848_8e7,
];
const TAN_Q: [f64; 5] = [
    1.0,
    1.368_129_634_706_929_6e4,
    -1.320_892_344_402_109_7e6,
    2.500_838_018_233_579e7,
    -5.386_957_559_294_546_4e7,
];

/// 4/pi as 20 fixed-point digits (Go trig_reduce.go `mPi4`).
const M_PI4: [u64; 20] = [
    0x0000000000000001,
    0x45f306dc9c882a53,
    0xf84eafa3ea69bb81,
    0xb6c52b3278872083,
    0xfca2c757bd778ac3,
    0x6e48dc74849ba5c0,
    0x0c925dd413a32439,
    0xfc3bd63962534e7d,
    0xd1046bea5d768909,
    0xd338e04d68befc82,
    0x7323ac7306a673e9,
    0x3908bf177bf25076,
    0x3ff12fffbc0b301f,
    0xde5e2316b414da3e,
    0xda6cfd9e4f96136e,
    0x9e8c7ecd3cbfd45a,
    0xea4f758fd7cbe2f6,
    0x7a0e73ef14a525d4,
    0xd7f6bf623f1aba10,
    0xac06608df8f6d757,
];

/// Go shifts are defined for amounts >= width (result 0); Rust's are not,
/// so the reduction uses saturating helpers.
#[inline]
fn shl(a: u64, s: u32) -> u64 {
    if s >= 64 {
        0
    } else {
        a << s
    }
}

#[inline]
fn shr(a: u64, s: u32) -> u64 {
    if s >= 64 {
        0
    } else {
        a >> s
    }
}

use super::mul_add;

#[inline(always)]
fn reduce_near<const FUSED: bool>(x: f64, multiple: f64) -> f64 {
    let first = mul_add::<FUSED>(multiple, -PI4A, x);
    let second = mul_add::<FUSED>(multiple, -PI4B, first);
    mul_add::<FUSED>(multiple, -PI4C, second)
}

#[inline(always)]
fn horner<const FUSED: bool, const N: usize>(x: f64, coefficients: [f64; N]) -> f64 {
    let mut value = coefficients[0];
    for coefficient in &coefficients[1..] {
        value = mul_add::<FUSED>(value, x, *coefficient);
    }
    value
}

/// Go `trigReduce`: reduce x >= pi/4 to (octant j, reduced argument z).
fn trig_reduce(x: f64) -> (u64, f64) {
    const SHIFT: i64 = 52;
    const MASK: i64 = 0x7ff;
    const BIAS: i64 = 1023;
    const PI4: f64 = core::f64::consts::FRAC_PI_4;
    if x < PI4 {
        return (0, x);
    }
    // x = ix * 2**exp with the implicit mantissa bit made explicit.
    let mut ix = x.to_bits();
    let exp = ((ix >> SHIFT) & MASK as u64) as i64 - BIAS - SHIFT;
    ix &= !((MASK as u64) << SHIFT);
    ix |= 1u64 << SHIFT;
    // Three consecutive 64-bit digits of 4/pi aligned to exponent -61.
    let d = exp + 61;
    let digit = (d / 64) as usize;
    let bitshift = (d % 64) as u32;
    let digits = |i: usize| -> u64 { shl(M_PI4[i], bitshift) | shr(M_PI4[i + 1], 64 - bitshift) };
    let z0 = digits(digit);
    let z1 = digits(digit + 1);
    let z2 = digits(digit + 2);
    // Multiply mantissa by the digits; keep the upper two words.
    let z2hi = ((((z2 as u128) * (ix as u128)) >> 64) as u64, ());
    let z2hi = z2hi.0;
    let prod1 = (z1 as u128) * (ix as u128);
    let z1hi = (prod1 >> 64) as u64;
    let z1lo = prod1 as u64;
    let z0lo = z0.wrapping_mul(ix);
    let (lo, c) = z1lo.overflowing_add(z2hi);
    let (hi, _overflow) = z0lo.overflowing_add(z1hi.wrapping_add(c as u64));
    // Top three bits are the octant.
    let mut j = hi >> 61;
    // Remaining fraction becomes the reduced float.
    let mut hi = (hi << 3) | shr(lo, 61);
    let lz = hi.leading_zeros();
    let e = (BIAS as u64).wrapping_sub((lz + 1) as u64);
    hi = shl(hi, lz + 1) | shr(lo, 63 - lz);
    hi >>= 64 - SHIFT as u32;
    hi |= e << SHIFT;
    let mut z = f64::from_bits(hi);
    // Map zeros to origin.
    if j & 1 == 1 {
        j += 1;
        j &= 7;
        z -= 1.0;
    }
    // Fractional part times pi/4.
    (j, z * PI4)
}

/// Go `math.Tan`, bit-exact.
#[inline]
pub(crate) fn go_tan(x: f64) -> f64 {
    tan::<{ super::GO_MATH_FMA }>(x)
}

#[inline]
pub(crate) fn tan<const FUSED: bool>(x: f64) -> f64 {
    // special cases
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return super::GO_NAN;
    }
    let sign = x.to_bits() & (1 << 63);
    let x = x.abs();
    let (j, z) = if x >= REDUCE_THRESHOLD {
        trig_reduce(x)
    } else {
        // Map the adjacent odd octant to its even neighbor.
        let j = ((x * (4.0 / core::f64::consts::PI)) as u64 + 1) & !1;
        (j, reduce_near::<FUSED>(x, j as f64))
    };
    let zz = z * z;
    let mut y = if zz > 1e-14 {
        let numerator = zz * horner::<FUSED, 3>(zz, TAN_P);
        let denominator = horner::<FUSED, 4>(
            zz,
            [
                mul_add::<FUSED>(z, z, TAN_Q[1]),
                TAN_Q[2],
                TAN_Q[3],
                TAN_Q[4],
            ],
        );
        mul_add::<FUSED>(z, numerator / denominator, z)
    } else {
        z
    };
    if j & 2 == 2 {
        y = -1.0 / y;
    }
    f64::from_bits(y.to_bits() ^ sign)
}

/// Go `math.Sin`, bit-exact.
#[inline]
pub(crate) fn go_sin(x: f64) -> f64 {
    sin::<{ super::GO_MATH_FMA }>(x)
}

#[inline]
pub(crate) fn sin<const FUSED: bool>(x: f64) -> f64 {
    // special cases
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return super::GO_NAN;
    }
    // make argument positive but save the sign
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    let (mut j, z) = if x >= REDUCE_THRESHOLD {
        trig_reduce(x)
    } else {
        let mut j = (x * (4.0 / core::f64::consts::PI)) as u64;
        let mut yf = j as f64;
        // map zeros to origin
        if j & 1 == 1 {
            j += 1;
            yf += 1.0;
        }
        j &= 7;
        let z = reduce_near::<FUSED>(x, yf);
        (j, z)
    };
    // reflect in x axis
    if j > 3 {
        sign = !sign;
        j -= 4;
    }
    let zz = z * z;
    let mut y = if j == 1 || j == 2 {
        mul_add::<FUSED>(
            zz * zz,
            horner::<FUSED, 6>(zz, COS),
            mul_add::<FUSED>(-0.5, zz, 1.0),
        )
    } else {
        mul_add::<FUSED>(z * zz, horner::<FUSED, 6>(zz, SIN), z)
    };
    if sign {
        y = -y;
    }
    y
}

/// Go `math.Cos`, bit-exact.
#[inline]
pub(crate) fn go_cos(x: f64) -> f64 {
    cos::<{ super::GO_MATH_FMA }>(x)
}

#[inline]
pub(crate) fn cos<const FUSED: bool>(x: f64) -> f64 {
    // special cases
    if x.is_nan() || x.is_infinite() {
        return super::GO_NAN;
    }
    // make argument positive
    let mut sign = false;
    let x = x.abs();
    let (mut j, z) = if x >= REDUCE_THRESHOLD {
        trig_reduce(x)
    } else {
        let mut j = (x * (4.0 / core::f64::consts::PI)) as u64;
        let mut yf = j as f64;
        // map zeros to origin
        if j & 1 == 1 {
            j += 1;
            yf += 1.0;
        }
        j &= 7;
        let z = reduce_near::<FUSED>(x, yf);
        (j, z)
    };
    if j > 3 {
        j -= 4;
        sign = !sign;
    }
    if j > 1 {
        sign = !sign;
    }
    let zz = z * z;
    let mut y = if j == 1 || j == 2 {
        mul_add::<FUSED>(z * zz, horner::<FUSED, 6>(zz, SIN), z)
    } else {
        mul_add::<FUSED>(
            zz * zz,
            horner::<FUSED, 6>(zz, COS),
            mul_add::<FUSED>(-0.5, zz, 1.0),
        )
    };
    if sign {
        y = -y;
    }
    y
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden vectors computed by running Go 1.25's own `math.Sin/Cos/Tan`
    /// (see scripts note in the crate docs): the whole point of this module
    /// is that these calls agree bit-for-bit with Go on every input.
    #[test]
    fn matches_go_bit_for_bit_on_golden_vectors() {
        // (input, sin, cos, tan) bits, produced by `go run` over
        // {-100.5, -3.75, -1.0, -1e-9, 0.0, 1e-9, 0.5, 1.0, 2.0, 3.14159,
        //  10.25, 100.5, 1e8, 1e9, 5.3e8, 1e15}.
        let goldens: &[(u64, u64, u64, u64)] = &[
            (
                0xc059200000000000,
                0x3f9fb3f833470ff1,
                0x3feffc12adaecec1,
                0x3f9fb7dcab49130d,
            ), // -100.5
            (
                0xc00e000000000000,
                0x3fe24a3af6750622,
                0xbfea4205b28667f6,
                0xbfe64a2502b0ca3b,
            ), // -3.75
            (
                0xbff0000000000000,
                0xbfeaed548f090cee,
                0x3fe14a280fb5068c,
                0xbff8eb245cbee3a5,
            ), // -1
            (
                0xbe112e0be826d695,
                0xbe112e0be826d695,
                0x3ff0000000000000,
                0xbe112e0be826d695,
            ), // -1e-09
            (0x0, 0x0, 0x3ff0000000000000, 0x0), // 0
            (
                0x3e112e0be826d695,
                0x3e112e0be826d695,
                0x3ff0000000000000,
                0x3e112e0be826d695,
            ), // 1e-09
            (
                0x3fe0000000000000,
                0x3fdeaee8744b05f0,
                0x3fec1528065b7d50,
                0x3fe17b4f5bf3474a,
            ), // 0.5
            (
                0x3ff0000000000000,
                0x3feaed548f090cee,
                0x3fe14a280fb5068c,
                0x3ff8eb245cbee3a5,
            ), // 1
            (
                0x4000000000000000,
                0x3fed18f6ead1b445,
                0xbfdaa22657537205,
                0xc0017af62e0950f8,
            ), // 2
            (
                0x400921f9f01b866e,
                0x3ec6428a6aa44cd1,
                0xbfefffffffff8420,
                0xbec6428a6aa4a2fd,
            ), // 3.14159
            (
                0x4024800000000000,
                0xbfe782a648605b2a,
                0xbfe5b5670532f73c,
                0x3ff153f48c125ae1,
            ), // 10.25
            (
                0x4059200000000000,
                0xbf9fb3f833470ff1,
                0x3feffc12adaecec1,
                0xbf9fb7dcab49130d,
            ), // 100.5
            (
                0x4197d78400000000,
                0x3fedcffca623a20b,
                0xbfd741b388a8c029,
                0xc004829e83f49589,
            ), // 1e+08
            (
                0x41cdcd6500000000,
                0x3fe1778cae83c69a,
                0x3feacff8c7364234,
                0x3fe4d8b249e3dba5,
            ), // 1e+09
            (
                0x41bf972880000000,
                0xbfeb283be499a2bd,
                0x3fe0ed0c5923fb27,
                0xbff9abe5d8168959,
            ), // 5.3e+08
            (
                0x430c6bf526340000,
                0x3feb76f88136ceba,
                0xbfe06c154609d33e,
                0xbffac23600a95be5,
            ), // 1e+15
        ];
        for &(in_bits, sin_bits, cos_bits, tan_bits) in goldens {
            let x = f64::from_bits(in_bits);
            assert_eq!(sin::<false>(x).to_bits(), sin_bits, "sin({x})");
            assert_eq!(cos::<false>(x).to_bits(), cos_bits, "cos({x})");
            assert_eq!(tan::<false>(x).to_bits(), tan_bits, "tan({x})");
        }
        // Special cases follow Go too.
        assert!(sin::<false>(f64::INFINITY).is_nan());
        assert!(cos::<false>(f64::NEG_INFINITY).is_nan());
        assert_eq!(tan::<false>(-0.0f64).to_bits(), (-0.0f64).to_bits());
    }

    /// The regression that motivated this module: `cot(1)` must produce
    /// Go's `0x3fe48e...` answer, not libm's one-ulp-different neighbor.
    #[test]
    fn cot_one_matches_go_engine_value() {
        assert_eq!(1.0 / tan::<false>(1.0), 0.6420926159343308f64);
    }

    /// Large arguments exercise the Payne-Hanek path (`trig_reduce`).
    #[test]
    fn large_arguments_reduce_like_go() {
        for x in [5.3e8f64, 1e9, 1.0000001e9, 1e15] {
            let (j, z) = trig_reduce(x);
            assert!(j < 8, "octant out of range for {x}");
            assert!(z.abs() <= core::f64::consts::FRAC_PI_4 * (1.0 + 1e-12));
        }
    }
}

// Go `math.Xatan` (src/math/atan.go), bit-exact. The rational approximation
// over |x| <= 0.66.
fn go_xatan<const FUSED: bool>(x: f64) -> f64 {
    const P0: f64 = -8.750_608_600_031_904_122_785e-1;
    const P1: f64 = -1.615_753_718_733_365_076_637e1;
    const P2: f64 = -7.500_855_792_314_704_667_340e1;
    const P3: f64 = -1.228_866_684_490_136_173_410e2;
    const P4: f64 = -6.485_021_904_942_025_371_773e1;
    const Q0: f64 = 2.485_846_490_142_306_297_962e1;
    const Q1: f64 = 1.650_270_098_316_988_542_046e2;
    const Q2: f64 = 4.328_810_604_912_902_668_951e2;
    const Q3: f64 = 4.853_903_996_359_136_964_868e2;
    const Q4: f64 = 1.945_506_571_482_613_964_425e2;
    let z = x * x;
    let p = horner::<FUSED, 5>(z, [P0, P1, P2, P3, P4]);
    let q = horner::<FUSED, 5>(z, [mul_add::<FUSED>(x, x, Q0), Q1, Q2, Q3, Q4]);
    mul_add::<FUSED>(x, z * p / q, x)
}

/// Go `math.Satan` (src/math/atan.go), bit-exact: the argument reduction
/// over the three |x| regimes.
fn go_satan<const FUSED: bool>(x: f64) -> f64 {
    const MOREBITS: f64 = 6.123_233_995_736_765_886_130e-17; // pi/2 = PIO2 + Morebits
    const TAN3PIO8: f64 = 2.414_213_562_373_095_048_80; // tan(3*pi/8)
    if x <= 0.66 {
        return go_xatan::<FUSED>(x);
    }
    if x > TAN3PIO8 {
        return core::f64::consts::FRAC_PI_2 - go_xatan::<FUSED>(1.0 / x) + MOREBITS;
    }
    core::f64::consts::FRAC_PI_4 + go_xatan::<FUSED>((x - 1.0) / (x + 1.0)) + 0.5 * MOREBITS
}

/// Go `math.Atan` (src/math/atan.go), bit-exact. TiDB's `ATAN`/`ATAN2`
/// evaluate through go's standard library, whose Cephes-derived arctangent
/// rounds one ulp away from the system libm on ordinary inputs.
#[inline]
pub(crate) fn go_atan(x: f64) -> f64 {
    atan::<{ super::GO_MATH_FMA }>(x)
}

#[inline]
pub(crate) fn atan<const FUSED: bool>(x: f64) -> f64 {
    if x == 0.0 {
        return x;
    }
    if x.is_nan() {
        // The Go polynomial quiets signaling NaNs and retains the payload.
        return f64::from_bits(x.to_bits() | (1 << 51));
    }
    if x > 0.0 {
        return go_satan::<FUSED>(x);
    }
    -go_satan::<FUSED>(-x)
}

/// Go `math.Atan2` (src/math/atan2.go), bit-exact: the special-case table
/// and the quadrant fix-up over [`go_atan`].
#[inline]
pub(crate) fn go_atan2(y: f64, x: f64) -> f64 {
    atan2::<{ super::GO_MATH_FMA }>(y, x)
}

#[inline]
pub(crate) fn atan2<const FUSED: bool>(y: f64, x: f64) -> f64 {
    // special cases
    if y.is_nan() || x.is_nan() {
        return super::GO_NAN;
    }
    if y == 0.0 {
        if x >= 0.0 && !x.is_sign_negative() {
            return 0.0f64.copysign(y);
        }
        return core::f64::consts::PI.copysign(y);
    }
    if x == 0.0 {
        return core::f64::consts::FRAC_PI_2.copysign(y);
    }
    if x.is_infinite() {
        if x > 0.0 {
            if y.is_infinite() {
                return core::f64::consts::FRAC_PI_4.copysign(y);
            }
            return 0.0f64.copysign(y);
        }
        if y.is_infinite() {
            return (3.0 * core::f64::consts::FRAC_PI_4).copysign(y);
        }
        return core::f64::consts::PI.copysign(y);
    }
    if y.is_infinite() {
        return core::f64::consts::FRAC_PI_2.copysign(y);
    }
    // Call atan and determine the quadrant.
    let q = atan::<FUSED>(y / x);
    if x < 0.0 {
        if q <= 0.0 {
            return q + core::f64::consts::PI;
        }
        return q - core::f64::consts::PI;
    }
    q
}

/// Go math.Asin reduces the interval to the arctangent polynomial.
#[inline]
pub(crate) fn asin<const FUSED: bool>(input: f64) -> f64 {
    if input == 0.0 {
        return input;
    }
    if input.is_nan() {
        return f64::from_bits(input.to_bits() | (1 << 51));
    }
    let x = input.abs();
    let reduced = mul_add::<FUSED>(-x, x, 1.0).sqrt();
    let result = if x > 0.7 {
        core::f64::consts::FRAC_PI_2 - go_satan::<FUSED>(reduced / x)
    } else {
        go_satan::<FUSED>(x / reduced)
    };
    result.copysign(input)
}
