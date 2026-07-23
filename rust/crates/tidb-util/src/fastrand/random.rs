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

use super::uint32;

const WYRAND_INCREMENT: u64 = 0xa076_1d64_78bd_642f;
const WYRAND_XOR: u64 = 0xe703_7ed1_a0b4_28db;

/// Source `wyrand`; private outside the package.
#[derive(Clone, Copy)]
pub(super) struct Wyrand(u64);

impl Wyrand {
    pub(super) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// Source `(*wyrand).Next`.
    pub(super) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(WYRAND_INCREMENT);
        wymix(self.0, self.0 ^ WYRAND_XOR)
    }
}

const fn wymix(a: u64, b: u64) -> u64 {
    let product = (a as u128) * (b as u128);
    (product as u64) ^ ((product >> 64) as u64)
}

/// Generates source-shaped random ASCII bytes while excluding NUL and `$`.
///
/// The signed length preserves Go's negative-`make([]byte, size)` panic
/// boundary instead of silently narrowing the API to `usize`.
#[must_use]
pub fn buf(size: isize) -> Vec<u8> {
    let size = usize::try_from(size).expect("makeslice: len out of range");
    let mut result = vec![0; size];
    let mut random = Wyrand::new(u64::from(uint32()));
    for byte in &mut result {
        let reduced = ((u64::from(random.next() as u32) * 127) >> 32) as u8;
        *byte = if reduced == 0 || reduced == b'$' {
            reduced + 1
        } else {
            reduced
        };
    }
    result
}

/// Returns a pseudo-random `u32` in `[0, n)`.
#[must_use]
pub fn uint32_n(n: u32) -> u32 {
    ((u64::from(uint32()) * u64::from(n)) >> 32) as u32
}

/// Returns a pseudo-random `u64` in `[0, n)`.
///
/// Go's unsigned arithmetic makes `n == 0` take the power-of-two mask branch
/// and return the full generated value. `wrapping_sub` preserves that edge.
#[must_use]
pub fn uint64_n(n: u64) -> u64 {
    let value = (u64::from(uint32()) << 32) + u64::from(uint32());
    let mask = n.wrapping_sub(1);
    if n & mask == 0 {
        value & mask
    } else {
        value % n
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::{buf, uint32_n, uint64_n, wymix, Wyrand};

    #[test]
    fn TestRand() {
        assert!(uint32_n(1024) < 1024);
        assert!(uint64_n(1_u64 << 63) < 1_u64 << 63);

        let _ = buf(20);
        let mut observed = [false; 256];
        for _ in 0..1024 {
            observed[uint32_n(256) as usize] = true;
        }
        assert!(observed.iter().filter(|seen| !**seen).count() < 24);
    }

    #[test]
    fn source_wyrand_vectors_and_mix_are_exact() {
        let mut random = Wyrand::new(0);
        let first = random.next();
        let second = random.next();
        assert_eq!(first, wymix(0xa076_1d64_78bd_642f, 0x4775_63b5_d809_4cf4));
        assert_eq!(second, wymix(0x40ec_3ac8_f17a_c85e, 0xa7ef_4419_51ce_e085));
    }

    #[test]
    fn buf_preserves_source_alphabet_and_empty_boundary() {
        assert!(buf(0).is_empty());
        for byte in buf(16_384) {
            assert!((1..=126).contains(&byte));
            assert_ne!(byte, b'$');
        }
    }

    #[test]
    #[should_panic(expected = "makeslice: len out of range")]
    fn negative_buf_size_preserves_go_make_panic() {
        let _ = buf(-1);
    }

    #[test]
    fn zero_bounds_preserve_unsigned_source_arithmetic() {
        assert_eq!(uint32_n(0), 0);
        let _ = uint64_n(0);
    }
}
