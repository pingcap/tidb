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
//! suite catches. These transcriptions follow Go src/math/exp.go and
//! log.go line for line (BSD licensed); on amd64 Go's `haveArchExp`/
//! `haveArchLog*` are false, so the pure-Go bodies below are exactly what
//! runs there.

/// Go `math.Exp`'s amd64 `archExp` (src/math/exp_amd64.s), bit-exact over
/// the FMA path the oracle's CPU takes (`useFMA` is CPUID-gated on; every
/// FMA-capable amd64 runs this branch). The method is SLEEF-derived
/// (Shibata, ISC'10): argument reduction against the upper/LOWER split LN2
/// (`LN2U`/`LN2L`, NOT log.go's pair), a 0.0625 pre-scale, then a
/// degree-13 Taylor polynomial evaluated with FMA (single-rounding
/// multiplies via `f64::mul_add`, which is the correctly rounded fused
/// form the assembly's `VFMADD` produces). `k` rounds to nearest-even via
/// `CVTSD2SL`, not truncation.
pub(crate) fn go_exp(x: f64) -> f64 {
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
fn go_frexp(f: f64) -> (f64, i32) {
    if f == 0.0 || f.is_infinite() || f.is_nan() {
        return (f, 0);
    }
    // Go `normalize`: scale subnormals into the normal range first.
    const NORMALIZE_SCALE: f64 = 4_503_599_627_370_496.0; // 2**52
    let (f, mut exp) = if f.abs() < f64::from_bits(1) {
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
fn go_log(x: f64) -> f64 {
    const LN2_HI: f64 = 6.931_471_803_691_238_164_90e-1; /* 3fe62e42 fee00000 */
    const LN2_LO: f64 = 1.908_214_929_270_587_700_02e-10; /* 3dea39ef 35793c76 */
    const L1: f64 = 6.666_666_666_666_735_130e-1; /* 3FE55555 55555593 */
    const L2: f64 = 3.999_999_999_940_941_908e-1; /* 3FD99999 9997FA04 */
    const L3: f64 = 2.857_142_874_366_239_149e-1; /* 3FD24924 94229359 */
    const L4: f64 = 2.222_219_843_214_978_396e-1; /* 3FCC71C5 1D8E78AF */
    const L5: f64 = 1.818_357_216_161_805_012e-1; /* 3FC74664 96CB03DE */
    const L6: f64 = 1.531_383_769_920_937_332e-1; /* 3FC39A09 D078C69F */
    const L7: f64 = 1.479_819_860_511_658_591e-1; /* 3FC2F112 DF3E5244 */

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

    // reduce
    let (mut f1, mut ki) = go_frexp(x);
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
    let t1 = s2 * (L1 + s4 * (L3 + s4 * (L5 + s4 * L7)));
    let t2 = s4 * (L2 + s4 * (L4 + s4 * L6));
    let big_r = t1 + t2;
    let hfsq = 0.5 * f * f;
    k * LN2_HI - ((hfsq - (s * (hfsq + big_r) + k * LN2_LO)) - f)
}

/// Go `math.Log10` (src/math/log10.go), bit-exact: `Log(x) * (1/Ln10)`.
/// TiDB's `LOG10` evaluates through go's standard library.
pub(crate) fn go_log10(x: f64) -> f64 {
    // go log10.go: `return Log(x) * (1 / Ln10)` — `1 / Ln10` is an UNTYPED
    // constant division, evaluated in arbitrary precision and rounded once
    // to float64. An f64 `1.0 / LN_10` rounds differently and made
    // `LOG10(100)` answer 1.9999999999999998 where go prints 2 (captured
    // via `go run` against math.Log10). The literal below is the
    // correctly rounded reciprocal.
    go_log(x) * 0.434_294_481_903_251_827_651_128_918_916_605_082_294_167_230_997_174_2
}
