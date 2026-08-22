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
//! on amd64 they are disabled and the pure-Go bodies below are what runs,
//! which these ports follow line for line.

/// Pi/4 split into three parts (Go sin.go/tan.go `PI4A/B/C`).
const PI4A: f64 = 7.85398125648498535156e-1;
const PI4B: f64 = 3.77489470793079817668e-8;
const PI4C: f64 = 2.69515142907905952645e-15;

const REDUCE_THRESHOLD: f64 = 1.0 * (1u64 << 29) as f64;

// sin coefficients (Go `_sin`).
const SIN: [f64; 6] = [
    1.58962301576546568060e-10,
    -2.50507477628578072866e-8,
    2.75573136213857245213e-6,
    -1.98412698295895385996e-4,
    8.33333333332211858878e-3,
    -1.66666666666666307295e-1,
];

// cos coefficients (Go `_cos`).
const COS: [f64; 6] = [
    -1.13585365213876817300e-11,
    2.08757008419747316778e-9,
    -2.75573141792967388112e-7,
    2.48015872888517045348e-5,
    -1.38888888888730564116e-3,
    4.16666666666665929218e-2,
];

// tan coefficients (Go `_tanP`, `_tanQ`).
const TAN_P: [f64; 3] = [
    -1.30936939181383777646e4,
    1.15351664838587416140e6,
    -1.79565251976484877988e7,
];
const TAN_Q: [f64; 5] = [
    1.00000000000000000000e0,
    1.36812963470692954678e4,
    -1.32089234440210967447e6,
    2.50083801823357915839e7,
    -5.38695755929454629881e7,
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
    let digits = |i: usize| -> u64 {
        shl(M_PI4[i], bitshift) | shr(M_PI4[i + 1], 64 - bitshift)
    };
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
pub(crate) fn go_tan(x: f64) -> f64 {
    // special cases
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return f64::NAN;
    }
    // make argument positive but save the sign
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    let (j, z) = if x >= REDUCE_THRESHOLD {
        trig_reduce(x)
    } else {
        // integer part of x/(Pi/4), as integer for tests on the phase angle
        let mut j = (x * (4.0 / core::f64::consts::PI)) as u64;
        let mut yf = j as f64;
        // map zeros and singularities to origin
        if j & 1 == 1 {
            j += 1;
            yf += 1.0;
        }
        let z = ((x - yf * PI4A) - yf * PI4B) - yf * PI4C;
        (j, z)
    };
    let zz = z * z;
    let mut y = if zz > 1e-14 {
        z + z
            * (zz * (((TAN_P[0] * zz) + TAN_P[1]) * zz + TAN_P[2])
                / ((((zz + TAN_Q[1]) * zz + TAN_Q[2]) * zz + TAN_Q[3]) * zz + TAN_Q[4]))
    } else {
        z
    };
    if j & 2 == 2 {
        y = -1.0 / y;
    }
    if sign {
        y = -y;
    }
    y
}

/// Go `math.Sin`, bit-exact.
pub(crate) fn go_sin(x: f64) -> f64 {
    // special cases
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return f64::NAN;
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
        let z = ((x - yf * PI4A) - yf * PI4B) - yf * PI4C;
        (j, z)
    };
    // reflect in x axis
    let mut sign = sign;
    if j > 3 {
        sign = !sign;
        j -= 4;
    }
    let zz = z * z;
    let mut y = if j == 1 || j == 2 {
        1.0 - 0.5 * zz
            + zz * zz
                * ((((((COS[0] * zz) + COS[1]) * zz + COS[2]) * zz + COS[3]) * zz + COS[4]) * zz
                    + COS[5])
    } else {
        z + z
            * zz
                * ((((((SIN[0] * zz) + SIN[1]) * zz + SIN[2]) * zz + SIN[3]) * zz + SIN[4]) * zz
                    + SIN[5])
    };
    if sign {
        y = -y;
    }
    y
}

/// Go `math.Cos`, bit-exact.
pub(crate) fn go_cos(x: f64) -> f64 {
    // special cases
    if x.is_nan() || x.is_infinite() {
        return f64::NAN;
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
        let z = ((x - yf * PI4A) - yf * PI4B) - yf * PI4C;
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
        z + z
            * zz
                * ((((((SIN[0] * zz) + SIN[1]) * zz + SIN[2]) * zz + SIN[3]) * zz + SIN[4]) * zz
                    + SIN[5])
    } else {
        1.0 - 0.5 * zz
            + zz * zz
                * ((((((COS[0] * zz) + COS[1]) * zz + COS[2]) * zz + COS[3]) * zz + COS[4]) * zz
                    + COS[5])
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
            (0xc059200000000000, 0x3f9fb3f833470ff1, 0x3feffc12adaecec1, 0x3f9fb7dcab49130d), // -100.5
            (0xc00e000000000000, 0x3fe24a3af6750622, 0xbfea4205b28667f6, 0xbfe64a2502b0ca3b), // -3.75
            (0xbff0000000000000, 0xbfeaed548f090cee, 0x3fe14a280fb5068c, 0xbff8eb245cbee3a5), // -1
            (0xbe112e0be826d695, 0xbe112e0be826d695, 0x3ff0000000000000, 0xbe112e0be826d695), // -1e-09
            (0x0, 0x0, 0x3ff0000000000000, 0x0), // 0
            (0x3e112e0be826d695, 0x3e112e0be826d695, 0x3ff0000000000000, 0x3e112e0be826d695), // 1e-09
            (0x3fe0000000000000, 0x3fdeaee8744b05f0, 0x3fec1528065b7d50, 0x3fe17b4f5bf3474a), // 0.5
            (0x3ff0000000000000, 0x3feaed548f090cee, 0x3fe14a280fb5068c, 0x3ff8eb245cbee3a5), // 1
            (0x4000000000000000, 0x3fed18f6ead1b445, 0xbfdaa22657537205, 0xc0017af62e0950f8), // 2
            (0x400921f9f01b866e, 0x3ec6428a6aa44cd1, 0xbfefffffffff8420, 0xbec6428a6aa4a2fd), // 3.14159
            (0x4024800000000000, 0xbfe782a648605b2a, 0xbfe5b5670532f73c, 0x3ff153f48c125ae1), // 10.25
            (0x4059200000000000, 0xbf9fb3f833470ff1, 0x3feffc12adaecec1, 0xbf9fb7dcab49130d), // 100.5
            (0x4197d78400000000, 0x3fedcffca623a20b, 0xbfd741b388a8c029, 0xc004829e83f49589), // 1e+08
            (0x41cdcd6500000000, 0x3fe1778cae83c69a, 0x3feacff8c7364234, 0x3fe4d8b249e3dba5), // 1e+09
            (0x41bf972880000000, 0xbfeb283be499a2bd, 0x3fe0ed0c5923fb27, 0xbff9abe5d8168959), // 5.3e+08
            (0x430c6bf526340000, 0x3feb76f88136ceba, 0xbfe06c154609d33e, 0xbffac23600a95be5), // 1e+15
        ];
        for &(in_bits, sin_bits, cos_bits, tan_bits) in goldens {
            let x = f64::from_bits(in_bits);
            assert_eq!(go_sin(x).to_bits(), sin_bits, "sin({x})");
            assert_eq!(go_cos(x).to_bits(), cos_bits, "cos({x})");
            assert_eq!(go_tan(x).to_bits(), tan_bits, "tan({x})");
        }
        // Special cases follow Go too.
        assert!(go_sin(f64::INFINITY).is_nan());
        assert!(go_cos(f64::NEG_INFINITY).is_nan());
        assert_eq!(go_tan(-0.0f64).to_bits(), (-0.0f64).to_bits());
    }

    /// The regression that motivated this module: `cot(1)` must produce
    /// Go's `0x3fe48e...` answer, not libm's one-ulp-different neighbor.
    #[test]
    fn cot_one_matches_go_engine_value() {
        assert_eq!(1.0 / go_tan(1.0), 0.6420926159343308f64);
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
