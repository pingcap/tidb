// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// See LICENSE-GO in this crate.

//! Float rendering by unrounded scaling from Go 1.27.4 internal/strconv/uscale.go.

#[path = "powers.rs"]
mod powers;

const DECIMAL_POWERS: [u64; 19] = {
    let mut powers = [1; 19];
    let mut i = 1;
    while i < powers.len() {
        powers[i] = powers[i - 1] * 10;
        i += 1;
    }
    powers
};

const POWERS_OF_FIVE: [u128; 39] = {
    let mut powers = [1; 39];
    let mut i = 1;
    while i < powers.len() {
        powers[i] = powers[i - 1] * 5;
        i += 1;
    }
    powers
};

pub(super) fn split_f64(value: f64) -> (u64, i32) {
    let bits = value.to_bits();
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let mut mantissa = bits & ((1 << 52) - 1);
    if exponent_bits != 0 {
        mantissa |= 1 << 52;
    }
    (mantissa, exponent_bits.max(1) - 1075)
}

/// Space for the exact coefficient of any finite binary64. Its largest
/// denominator is 2^1074, so even a 53-bit mantissa times 5^1074 takes
/// fewer than 768 decimal digits.
pub(super) const EXACT_DECIMAL_CAPACITY: usize = 768;

/// Writes the exact decimal coefficient of mantissa * 2^exponent and returns
/// its length and decimal point position.
pub(super) fn exact_decimal(
    mantissa: u64,
    exponent: i32,
    output: &mut [u8; EXACT_DECIMAL_CAPACITY],
) -> (usize, i32) {
    const RADIX: u64 = 1_000_000_000;
    const LIMB_DIGITS: usize = 9;
    let fraction = (-exponent).max(0);
    let (base, chunk_width, factor) = if exponent < 0 {
        (5u64, 14, 5u64.pow(14))
    } else {
        (2, 34, 1 << 34)
    };
    let mut power = exponent.unsigned_abs();

    let mut limbs = [0u32; EXACT_DECIMAL_CAPACITY.div_ceil(LIMB_DIGITS)];
    limbs[0] = (mantissa % RADIX) as u32;
    limbs[1] = (mantissa / RADIX) as u32;
    let mut used = 1 + usize::from(limbs[1] != 0);
    while power != 0 {
        let step = power.min(chunk_width);
        let factor = if step == chunk_width {
            factor
        } else {
            base.pow(step)
        };
        // Both 2^34 and 5^14 fit in a u64 product with one radix digit.
        let mut carry = 0;
        for limb in &mut limbs[..used] {
            let product = u64::from(*limb) * factor + carry;
            *limb = (product % RADIX) as u32;
            carry = product / RADIX;
        }
        while carry != 0 {
            limbs[used] = (carry % RADIX) as u32;
            used += 1;
            carry /= RADIX;
        }
        power -= step;
    }

    let mut buffer = itoa::Buffer::new();
    let first = buffer.format(limbs[used - 1]).as_bytes();
    let mut len = first.len();
    output[..len].copy_from_slice(first);
    for &limb in limbs[..used - 1].iter().rev() {
        let digits = buffer.format(limb).as_bytes();
        let end = len + LIMB_DIGITS;
        let start = end - digits.len();
        output[len..start].fill(b'0');
        output[start..end].copy_from_slice(digits);
        len = end;
    }
    (len, len as i32 - fraction)
}

/// Returns the rounded coefficient and its decimal exponent for widths
/// supported by Go's fast path.
pub(super) fn convert(value: f64, format: u8, precision: i32) -> Option<(u64, i32)> {
    let (mantissa, binary_exponent) = split_f64(value);
    let exponent = binary_exponent + 52;
    let digits = match format {
        b'f' if exponent >= 0 => 1 + i64::from(precision) + i64::from(log10_pow2(exponent + 1)),
        b'f' => 1 + i64::from(precision) - i64::from(log10_pow2(-exponent)),
        b'e' | b'E' => i64::from(precision) + 1,
        b'g' | b'G' => i64::from(precision),
        _ => return None,
    };
    if digits <= 0 || mantissa == 0 {
        return Some((0, 0));
    }
    if digits > 18 {
        return None;
    }

    let normalization = mantissa.leading_zeros();
    let mantissa = mantissa << normalization;
    let binary_exponent = binary_exponent - normalization as i32;
    let mut power = digits as i32 - 1 - log10_pow2(binary_exponent + 63);
    let mut scaled = Scaler::new(binary_exponent, power).apply(mantissa);
    if scaled >= (DECIMAL_POWERS[digits as usize] << 2) - 2 {
        scaled = divide_by_ten(scaled);
        power -= 1;
    }
    if format == b'f' {
        while power > precision {
            scaled = divide_by_ten(scaled);
            power -= 1;
        }
    }
    // Low bits represent the half bit and the sticky bit. On ties use the
    // parity of the integer part to round to even.
    Some((round(scaled), -power))
}

/// Rounds a fixed-point value whose scaled mantissa fits in 128 bits.
/// Precision contributes one factor of five and one binary shift per digit.
pub(super) fn exact_fixed(value: f64, precision: i32) -> Option<u128> {
    let five_to_precision = *POWERS_OF_FIVE.get(precision as usize)?;
    let (mantissa, exponent) = split_f64(value);
    let mantissa = u128::from(mantissa).checked_mul(five_to_precision)?;
    let shift = exponent + precision;
    if shift >= 0 {
        return (shift as u32 <= mantissa.leading_zeros() && shift < 128)
            .then(|| mantissa << shift);
    }
    let discarded_bits = shift.unsigned_abs();
    if discarded_bits > 128 {
        return Some(0);
    }
    let half = 1_u128 << (discarded_bits - 1);
    let remainder = mantissa & (half | (half - 1));
    let kept = mantissa.checked_shr(discarded_bits).unwrap_or(0);
    Some(kept + u128::from(remainder > half || (remainder == half && kept & 1 != 0)))
}

/// Returns the shortest coefficient that still rounds to the original float.
pub(super) fn shortest(value: f64, bit_size: u32) -> (u64, i32) {
    if bit_size == 32 {
        shortest_bits::<23, 8>(u64::from((value as f32).to_bits()))
    } else {
        shortest_bits::<52, 11>(value.to_bits())
    }
}

fn shortest_bits<const FRACTION_BITS: i32, const EXPONENT_BITS: i32>(bits: u64) -> (u64, i32) {
    let bias = (1 << (EXPONENT_BITS - 1)) - 1;
    let min_exponent = 1 - bias - 63;
    let exponent_mask = (1 << EXPONENT_BITS) - 1;
    let exponent_bits = ((bits >> FRACTION_BITS) & exponent_mask) as i32;
    let mut mantissa = bits & ((1 << FRACTION_BITS) - 1);
    if exponent_bits != 0 {
        mantissa |= 1 << FRACTION_BITS;
    }
    let normalization = mantissa.leading_zeros();
    let mantissa = mantissa << normalization;
    let exponent = exponent_bits.max(1) - bias - FRACTION_BITS - normalization as i32;

    // A power of two above the normal boundary has twice the spacing to
    // its right neighbor as to its left neighbor.
    let low_bits = 63 - FRACTION_BITS;
    let (lower, upper, odd, power);
    if mantissa == 1 << 63 && exponent > min_exponent {
        power = -(((exponent + low_bits) * 631_305 - 261_663) >> 21);
        lower = mantissa - (1 << (low_bits - 2));
        upper = mantissa + (1 << (low_bits - 1));
        odd = (mantissa >> low_bits) & 1;
    } else {
        let low_bits = low_bits + (min_exponent - exponent).max(0);
        power = -log10_pow2(exponent + low_bits);
        lower = mantissa - (1 << (low_bits - 1));
        upper = mantissa + (1 << (low_bits - 1));
        odd = (mantissa >> low_bits) & 1;
    }
    let scaler = Scaler::new(exponent, power);
    let first = (scaler.apply(lower) + odd + 3) >> 2;
    let last = (scaler.apply(upper) - odd) >> 2;
    let shorter = last / 10;
    if shorter * 10 >= first {
        (shorter, 1 - power)
    } else if first < last {
        (round(scaler.apply(mantissa)), -power)
    } else {
        (first, -power)
    }
}

/// Bounds the exact decimal digits of a nonzero dyadic number. After
/// removing powers of two from the mantissa, each remaining denominator
/// bit can contribute at most one decimal place.
pub(super) fn exact_digits_bound(value: f64) -> i32 {
    let (mantissa, exponent) = split_f64(value);
    let fraction = (-(exponent + mantissa.trailing_zeros() as i32)).max(0);
    let magnitude = exponent + (64 - mantissa.leading_zeros()) as i32;
    (log10_pow2(magnitude) + 1 + fraction).max(1)
}

fn round(value: u64) -> u64 {
    (value + 1 + ((value >> 2) & 1)) >> 2
}

fn log10_pow2(exponent: i32) -> i32 {
    (exponent * 78_913) >> 18
}

fn divide_by_ten(value: u64) -> u64 {
    (value / 10) | (value & 1) | u64::from(!value.is_multiple_of(10))
}

struct Scaler {
    high: u64,
    low: u64,
    shift: i32,
}

impl Scaler {
    fn new(exponent: i32, power: i32) -> Self {
        let (high, low) = powers::POW10[(power - powers::MIN_POWER) as usize];
        let log2_pow10 = (power * 108_853) >> 15;
        let shift = -(exponent + log2_pow10 + 3);
        debug_assert!((0..64).contains(&shift));
        Self { high, low, shift }
    }

    /// Scales a left-justified mantissa, retaining two rounding bits.
    fn apply(&self, mantissa: u64) -> u64 {
        let product = u128::from(mantissa) * u128::from(self.high);
        let top = (product >> 64) as u64;
        let shifted = top >> self.shift;
        if shifted << self.shift != top {
            return shifted | 1;
        }
        let middle = product as u64;
        let correction = ((u128::from(mantissa) * u128::from(self.low)) >> 64) as u64;
        let top = top - u64::from(middle < correction);
        (top >> self.shift) | u64::from(middle.wrapping_sub(correction) > 1)
    }
}
