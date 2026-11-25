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

//! Bit-exact transcriptions of Go's `math.Exp` and `math.Log`/`math.Log10`.
//!
//! Same rationale as [`super::go_trig`]: TiDB evaluates `EXP`/`LOG10`
//! through Go's standard library, whose implementations round one ulp away
//! from the system libm on ordinary inputs (`EXP(1.5)` is
//! `4.481689070338065` in go, `...0645` from libm), which the differential
//! suite catches. These transcriptions follow Go's math/log.go,
//! math/exp_amd64.s and math/exp_arm64.s (BSD licensed). The arm64 path
//! uses the arm64 Exp implementation and Go's fused logarithm polynomial.

/// Go `math.Exp`'s amd64 `archExp` (src/math/exp_amd64.s), bit-exact over
/// the FMA path the oracle's CPU takes (`useFMA` is CPUID-gated on; every
/// FMA-capable amd64 runs this branch). The method is SLEEF-derived
/// (Shibata, ISC'10): argument reduction against the upper/LOWER split LN2
/// (`LN2U`/`LN2L`, NOT log.go's pair), a 0.0625 pre-scale, then a
/// degree-13 Taylor polynomial evaluated with FMA (single-rounding
/// multiplies via `f64::mul_add`, which is the correctly rounded fused
/// form the assembly's `VFMADD` produces). `k` rounds to nearest-even via
/// `CVTSD2SL`, not truncation.
fn exp_amd64(x: f64) -> f64 {
    const OVERFLOW: f64 = 7.097_827_128_933_84e2;
    const LOG2E: f64 = 1.442_695_040_888_963_407_359_924_681_001_892_0;
    const LN2U: f64 = 0.693_147_180_559_662_956_511_601_805_686_950_683_593_75;
    const LN2L: f64 = 0.282_352_905_630_315_771_225_884_481_750_134_360_255_254_120_68e-12;

    // not-finite: (bits & ~sign) >= PosInf catches NaN and +Inf; -Inf
    // returns 0.
    if x.is_nan() || x == f64::INFINITY {
        return x;
    }
    if x == f64::NEG_INFINITY {
        return 0.0;
    }
    if x > OVERFLOW {
        return f64::INFINITY;
    }

    // CVTSD2SL: round-to-nearest-even conversion to int32.
    let k_f64 = LOG2E * x;
    let k_i32 = k_f64.round_ties_even() as i32;
    let k = f64::from(k_i32);

    // FMA argument reduction against the two-part LN2.
    let mut x0 = (-LN2U).mul_add(k, x);
    x0 = (-LN2L).mul_add(k, x0);
    // reduce argument
    x0 *= 0.0625;
    // Taylor series evaluation: the Horner chain INCLUDES the +1.0 term
    // (asm line 149), then FOUR (2+X0) squarings compound the 0.0625
    // pre-scale (asm lines 151-158), and the trailing FMA adds the final 1.0.
    let mut x1: f64 = 2.480_158_730_158_730_158_7e-5;
    x1 = x1.mul_add(x0, 1.984_126_984_126_984_127_0e-4);
    x1 = x1.mul_add(x0, 1.388_888_888_888_888_888_9e-3);
    x1 = x1.mul_add(x0, 8.333_333_333_333_333_333_3e-3);
    x1 = x1.mul_add(x0, 4.166_666_666_666_666_666_7e-2);
    x1 = x1.mul_add(x0, 1.666_666_666_666_666_666_7e-1);
    x1 = x1.mul_add(x0, 0.5);
    x1 = x1.mul_add(x0, 1.0);
    x0 *= x1;
    for _ in 0..4 {
        x1 = x0 + 2.0;
        x0 *= x1;
    }
    x0 += 1.0;

    // return fr * 2**exponent
    let mut bx = k_i32 + 0x3ff;
    if bx <= 0 {
        if bx < -52 {
            return 0.0;
        }
        bx += 0x3fe;
        let first = f64::from_bits((bx as u64) << 52);
        let second = f64::from_bits((1u64) << 52);
        return first * second * x0;
    }
    if bx >= 0x7ff {
        return f64::INFINITY;
    }
    x0 * f64::from_bits((bx as u64) << 52)
}

/// Go `math.Frexp` (src/math/frexp.go), bit-exact: `(frac, exp)` with
/// `f = frac × 2**exp`, `0.5 <= |frac| < 1`.
#[inline]
pub(crate) fn frexp(f: f64) -> (f64, i32) {
    let abs_bits = f.to_bits() & 0x7fff_ffff_ffff_ffff;
    if abs_bits == 0 || abs_bits >= f64::INFINITY.to_bits() {
        return (f, 0);
    }
    frexp_finite(f)
}

#[inline]
fn frexp_finite(f: f64) -> (f64, i32) {
    // Go `normalize`: scale subnormals into the normal range first.
    const NORMALIZE_SCALE: f64 = 4_503_599_627_370_496.0; // 2**52
    let (f, mut exp) = if f.abs() < f64::MIN_POSITIVE {
        (f * NORMALIZE_SCALE, -52)
    } else {
        (f, 0)
    };
    let x = f.to_bits();
    exp += (((x >> 52) & 0x7ff) as i32) - 1022;
    let frac = f64::from_bits((x & !(0x7ffu64 << 52)) | (1022u64 << 52));
    (frac, exp)
}

/// Go `math.Log` (src/math/log.go), bit-exact.
pub(crate) fn go_log(x: f64) -> f64 {
    // special cases
    if x.is_nan() || (x.is_infinite() && x > 0.0) {
        return x;
    }
    if x < 0.0 {
        return f64::NAN;
    }
    if x == 0.0 {
        return f64::NEG_INFINITY;
    }

    log_positive::<{ super::GO_MATH_FMA }>(x)
}

/// The vector signatures check the domain before calling this function.
#[inline]
pub(crate) fn log_positive<const FUSED: bool>(x: f64) -> f64 {
    if !x.is_finite() {
        return x;
    }
    let (fraction, exponent) = frexp_finite(x);
    log_parts::<FUSED>(fraction, exponent)
}

#[inline]
fn log_parts<const FUSED: bool>(mut f1: f64, mut ki: i32) -> f64 {
    use super::mul_add;
    const LN2_HI: f64 = 6.931_471_803_691_238_164_90e-1; /* 3fe62e42 fee00000 */
    const LN2_LO: f64 = 1.908_214_929_270_587_700_02e-10; /* 3dea39ef 35793c76 */
    const L1: f64 = 6.666_666_666_666_735_130e-1; /* 3FE55555 55555593 */
    const L2: f64 = 3.999_999_999_940_941_908e-1; /* 3FD99999 9997FA04 */
    const L3: f64 = 2.857_142_874_366_239_149e-1; /* 3FD24924 94229359 */
    const L4: f64 = 2.222_219_843_214_978_396e-1; /* 3FCC71C5 1D8E78AF */
    const L5: f64 = 1.818_357_216_161_805_012e-1; /* 3FC74664 96CB03DE */
    const L6: f64 = 1.531_383_769_920_937_332e-1; /* 3FC39A09 D078C69F */
    const L7: f64 = 1.479_819_860_511_658_591e-1; /* 3FC2F112 DF3E5244 */

    if f1 < core::f64::consts::SQRT_2 / 2.0 {
        f1 *= 2.0;
        ki -= 1;
    }
    let f = f1 - 1.0;
    let k = ki as f64;

    // compute
    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    let p1 = mul_add::<FUSED>(
        mul_add::<FUSED>(mul_add::<FUSED>(s4, L7, L5), s4, L3),
        s4,
        L1,
    );
    let p2 = mul_add::<FUSED>(mul_add::<FUSED>(s4, L6, L4), s4, L2);
    let big_r = mul_add::<FUSED>(s2, p1, s4 * p2);
    let half_f = 0.5 * f;
    let inner = mul_add::<FUSED>(s, mul_add::<FUSED>(half_f, f, big_r), k * LN2_LO);
    let tail = mul_add::<FUSED>(half_f, f, -inner) - f;
    mul_add::<FUSED>(k, LN2_HI, -tail)
}

/// Go arm64 `archExp` from math/exp_arm64.s.
#[inline]
fn exp_arm64(x: f64) -> f64 {
    const OVERFLOW: f64 = 709.782_712_893_384;
    const UNDERFLOW: f64 = -745.133_219_101_941_1;
    if x.is_nan() {
        return x;
    }
    if x > OVERFLOW {
        return f64::INFINITY;
    }
    if x < UNDERFLOW {
        return 0.0;
    }
    exp_arm64_in_range::<false>(x)
}

/// For `|x| < 700`, the result is finite and normal.
#[inline]
pub(crate) fn exp_arm64_normal(x: f64) -> f64 {
    exp_arm64_in_range::<true>(x)
}

#[inline]
fn exp_arm64_in_range<const NORMAL: bool>(x: f64) -> f64 {
    const LN2_HI: f64 = 0.693_147_180_369_123_8;
    const LN2_LO: f64 = 1.908_214_929_270_587_7e-10;
    const P1: f64 = 1.666_666_666_666_666_6e-1;
    const P2: f64 = -2.777_777_777_701_559_3e-3;
    const P3: f64 = 6.613_756_321_437_934e-5;
    const P4: f64 = -1.653_390_220_546_525_2e-6;
    const P5: f64 = 4.138_136_797_057_238_5e-8;

    if x.abs() < f64::from_bits(0x3e30_0000_0000_0000) {
        return 1.0 + x;
    }
    let k = x.mul_add(core::f64::consts::LOG2_E, 0.5_f64.copysign(x)) as i32;
    let factor = f64::from(k);
    let hi = (-LN2_HI).mul_add(factor, x);
    let lo = factor * LN2_LO;
    let r = hi - lo;
    let t = r * r;
    let polynomial = P5
        .mul_add(t, P4)
        .mul_add(t, P3)
        .mul_add(t, P2)
        .mul_add(t, P1);
    let c = (-t).mul_add(polynomial, r);
    let y = 1.0 - ((lo - r * c / (2.0 - c)) - hi);
    // Add k to the exponent field. Go's denormal path adds 52 before
    // constructing the value, then applies the exact 2**-52 multiplier.
    let scaled = y.to_bits().wrapping_add((k as u64) << 52);
    if NORMAL || (scaled as i64) >= (1 << 52) {
        f64::from_bits(scaled)
    } else {
        f64::from_bits(scaled.wrapping_add(52 << 52)) * f64::from_bits(0x3cb0_0000_0000_0000)
    }
}

#[inline]
pub(crate) fn exp<const FUSED: bool>(x: f64) -> f64 {
    if FUSED { exp_arm64(x) } else { exp_amd64(x) }
}

pub(crate) fn go_exp(x: f64) -> f64 {
    exp::<{ super::GO_MATH_FMA }>(x)
}

#[inline]
pub(crate) fn log2<const FUSED: bool>(x: f64) -> f64 {
    if !x.is_finite() {
        return super::mul_add::<FUSED>(x, core::f64::consts::LOG2_E, 0.0);
    }
    let (fraction, exponent) = frexp_finite(x);
    if fraction == 0.5 {
        return f64::from(exponent - 1);
    }
    super::mul_add::<FUSED>(
        log_parts::<FUSED>(fraction, 0),
        core::f64::consts::LOG2_E,
        f64::from(exponent),
    )
}

const INV_LN10: f64 = 0.434_294_481_903_251_827_651_128_918_916_605_082_294_167_230_997_174_2;

/// Go `math.Log10` (src/math/log10.go), bit-exact: `Log(x) * (1/Ln10)`.
/// TiDB's `LOG10` evaluates through go's standard library.
pub(crate) fn go_log10(x: f64) -> f64 {
    go_log(x) * INV_LN10
}

#[inline]
pub(crate) fn log10<const FUSED: bool>(x: f64) -> f64 {
    // go log10.go: `return Log(x) * (1 / Ln10)` — `1 / Ln10` is an UNTYPED
    // constant division, evaluated in arbitrary precision and rounded once
    // to float64. An f64 `1.0 / LN_10` rounds differently and made
    // `LOG10(100)` answer 1.9999999999999998 where go prints 2 (captured
    // via `go run` against math.Log10). The literal below is the
    // correctly rounded reciprocal.
    log_positive::<FUSED>(x) * INV_LN10
}

/// Go `math.Ldexp` (src/math/ldexp.go): `frac * 2**exp`, with the same
/// denormal and overflow handling.
pub(crate) fn ldexp(frac: f64, exp: i32) -> f64 {
    const SHIFT: u32 = 52;
    const MASK: u64 = 0x7ff;
    const BIAS: i32 = 1023;
    if frac == 0.0 || frac.is_infinite() || frac.is_nan() {
        return frac;
    }
    // Go `normalize`.
    let (frac, extra) = if frac.abs() < f64::from_bits(1) * 4_503_599_627_370_496.0 {
        (frac * 4_503_599_627_370_496.0, -52)
    } else {
        (frac, 0)
    };
    let mut exp = exp.saturating_add(extra);
    let mut x = frac.to_bits();
    exp = exp.saturating_add(((x >> SHIFT) & MASK) as i32 - BIAS);
    if exp < -1075 {
        return 0.0_f64.copysign(frac);
    }
    if exp > 1023 {
        return if frac < 0.0 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    let mut m = 1.0_f64;
    if exp < -1022 {
        // denormal
        exp += 53;
        m = 1.0 / 9_007_199_254_740_992.0; // 2**-53
    }
    x &= !(MASK << SHIFT);
    x |= ((exp + BIAS) as u64) << SHIFT;
    m * f64::from_bits(x)
}

/// Go `isOddInt` (src/math/pow.go).
fn go_is_odd_int(x: f64) -> bool {
    if x.abs() >= 9_007_199_254_740_992.0 {
        return false;
    }
    let xi = x.trunc();
    let xf = x - xi;
    xf == 0.0 && (xi as i64) & 1 == 1
}

/// Go `math.Pow` (src/math/pow.go), bit-exact. Rust's `powf` is libm's and
/// disagrees in the last digit for many ordinary arguments
/// (`POW(1.5, 1.5)` is `1.8371173070873834` in Go).
pub(crate) fn go_pow(x: f64, y: f64) -> f64 {
    pow::<{ super::GO_MATH_FMA }>(x, y)
}

#[inline]
pub(crate) fn pow<const FUSED: bool>(x: f64, y: f64) -> f64 {
    if y == 0.0 || x == 1.0 {
        return 1.0;
    }
    if y == 1.0 {
        return x;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        if y < 0.0 {
            if x.is_sign_negative() && go_is_odd_int(y) {
                return f64::INFINITY.copysign(x);
            }
            return f64::INFINITY;
        }
        if y > 0.0 {
            if x.is_sign_negative() && go_is_odd_int(y) {
                return x;
            }
            return 0.0;
        }
    } else if y.is_infinite() {
        if x == -1.0 {
            return 1.0;
        }
        if (x.abs() < 1.0) == (y > 0.0) {
            return 0.0;
        }
        return f64::INFINITY;
    } else if x.is_infinite() {
        if x < 0.0 {
            return pow::<FUSED>(1.0 / x, -y); // Pow(-0, -y)
        }
        if y < 0.0 {
            return 0.0;
        }
        if y > 0.0 {
            return f64::INFINITY;
        }
    } else if y == 0.5 {
        return x.sqrt();
    } else if y == -0.5 {
        return 1.0 / x.sqrt();
    }

    let abs_y = y.abs();
    let mut yi = abs_y.trunc();
    let mut yf = abs_y - yi;
    if yf != 0.0 && x < 0.0 {
        return f64::NAN;
    }
    if yi >= 9_223_372_036_854_775_808.0 {
        // yi is a large even int that overflows (or underflows to 0) for
        // every x except -1 (x == 1 was handled above).
        if x == -1.0 {
            return 1.0;
        }
        if (x.abs() < 1.0) == (y > 0.0) {
            return 0.0;
        }
        return f64::INFINITY;
    }

    // ans = a1 * 2**ae (= 1 for now).
    let mut a1 = 1.0_f64;
    let mut ae: i32 = 0;

    // ans *= x**yf
    if yf != 0.0 {
        if yf > 0.5 {
            yf -= 1.0;
            yi += 1.0;
        }
        let fraction_log = yf * log_positive::<FUSED>(x);
        // The fractional power has |yf| <= 0.5. Log of a positive finite
        // f64 is between -745 and 710, so this exponent is in the normal range.
        a1 = if FUSED {
            exp_arm64_normal(fraction_log)
        } else {
            exp_amd64(fraction_log)
        };
    }

    // ans *= x**yi
    let (mut x1, mut xe) = frexp(x);
    let mut i = yi as i64;
    while i != 0 {
        if xe < -(1 << 12) || (1 << 12) < xe {
            // catastrophic overflow: ae += xe*i would overflow
            ae += xe;
            break;
        }
        if i & 1 == 1 {
            a1 *= x1;
            ae += xe;
        }
        x1 *= x1;
        xe <<= 1;
        if x1 < 0.5 {
            x1 += x1;
            xe -= 1;
        }
        i >>= 1;
    }

    // ans = a1 * 2**ae; if y < 0 { ans = 1 / ans }, but in the opposite order
    if y < 0.0 {
        a1 = 1.0 / a1;
        ae = -ae;
    }
    ldexp(a1, ae)
}
