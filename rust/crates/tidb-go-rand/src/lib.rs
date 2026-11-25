//! Go `math/rand`'s seeded generator: `NewSource` (the additive lagged
//! Fibonacci `rngSource` of `rng.go`) and the `Rand` methods Lightning's
//! row-id sharding reads. A ported caller that seeds Go's generator must
//! draw the same numbers, because they are written into keys.

mod cooked;

use cooked::RNG_COOKED;

const RNG_LEN: usize = 607;
const RNG_TAP: usize = 273;
const RNG_MASK: u64 = (1 << 63) - 1;
const SEED_MODULUS: u64 = (1 << 31) - 1;
const SEED_MULTIPLIER: u64 = 48_271;

/// Multiplication modulo Go's `seedrand` modulus for two reduced operands.
const fn multiply_seed(left: u64, right: u64) -> u64 {
    let product = left * right;
    // 2^31 is congruent to 1. This fold leaves less than twice the modulus.
    let reduced = (product & SEED_MODULUS) + (product >> 31);
    if reduced >= SEED_MODULUS {
        reduced - SEED_MODULUS
    } else {
        reduced
    }
}

const fn seed_multiplier(steps: usize) -> u64 {
    let mut result = 1;
    let mut step = 0;
    while step < steps {
        result = multiply_seed(result, SEED_MULTIPLIER);
        step += 1;
    }
    result
}

/// Go `rand.New(rand.NewSource(seed))`.
#[derive(Clone)]
pub struct Rand {
    tap: usize,
    feed: usize,
    vec: [i64; RNG_LEN],
}

impl Rand {
    /// Go `rand.New(rand.NewSource(seed))`.
    pub fn new(seed: i64) -> Self {
        let mut rng = Self { tap: 0, feed: 0, vec: [0; RNG_LEN] };
        rng.seed(seed);
        rng
    }

    /// Go `Rand.Seed` (`rngSource.Seed`).
    pub fn seed(&mut self, seed: i64) {
        self.tap = 0;
        self.feed = RNG_LEN - RNG_TAP;
        let seed = seed.rem_euclid(SEED_MODULUS as i64) as u64;
        let seed = if seed == 0 { 89_482_311 } else { seed };
        // Go discards 20 draws, then packs three successive draws into each
        // register entry. Advance all three positions by that stride.
        let mut high = multiply_seed(seed, seed_multiplier(21));
        let mut middle = multiply_seed(seed, seed_multiplier(22));
        let mut low = multiply_seed(seed, seed_multiplier(23));
        const STRIDE: u64 = seed_multiplier(3);
        for (value, &cooked) in self.vec.iter_mut().zip(RNG_COOKED.iter()) {
            *value = ((high << 40) ^ (middle << 20) ^ low) as i64 ^ cooked;
            high = multiply_seed(high, STRIDE);
            middle = multiply_seed(middle, STRIDE);
            low = multiply_seed(low, STRIDE);
        }
    }

    /// Go `rngSource.Uint64`.
    pub fn uint64(&mut self) -> u64 {
        self.tap = if self.tap == 0 { RNG_LEN - 1 } else { self.tap - 1 };
        self.feed = if self.feed == 0 { RNG_LEN - 1 } else { self.feed - 1 };
        let x = self.vec[self.feed].wrapping_add(self.vec[self.tap]);
        self.vec[self.feed] = x;
        x as u64
    }

    /// Go `Rand.Int63`.
    pub fn int63(&mut self) -> i64 {
        (self.uint64() & RNG_MASK) as i64
    }

    /// Go `Rand.Uint32`.
    pub fn uint32(&mut self) -> u32 {
        (self.int63() >> 31) as u32
    }

    /// Go `Rand.Int31`.
    pub fn int31(&mut self) -> i32 {
        (self.int63() >> 32) as i32
    }

    /// Go `Rand.Int63n`.
    ///
    /// # Panics
    /// `n` is not positive, as Go panics.
    pub fn int63n(&mut self, n: i64) -> i64 {
        assert!(n > 0, "invalid argument to Int63n");
        if n & (n - 1) == 0 {
            return self.int63() & (n - 1);
        }
        let max = ((1u64 << 63) - 1 - (1u64 << 63) % n as u64) as i64;
        let mut v = self.int63();
        while v > max {
            v = self.int63();
        }
        v % n
    }

    /// Go `Rand.Int31n`.
    ///
    /// # Panics
    /// `n` is not positive, as Go panics.
    pub fn int31n(&mut self, n: i32) -> i32 {
        int31n_from(n, || self.int31())
    }

    /// Go `Rand.Intn`.
    ///
    /// # Panics
    /// `n` is not positive, as Go panics.
    pub fn intn(&mut self, n: i64) -> i64 {
        assert!(n > 0, "invalid argument to Intn");
        if n <= i64::from(i32::MAX) {
            return i64::from(self.int31n(n as i32));
        }
        self.int63n(n)
    }
}

fn int31n_from(n: i32, mut draw: impl FnMut() -> i32) -> i32 {
    assert!(n > 0, "invalid argument to Int31n");
    if n & (n - 1) == 0 {
        return draw() & (n - 1);
    }
    let max = ((1u32 << 31) - 1 - (1u32 << 31) % n as u32) as i32;
    let mut v = draw();
    while v > max {
        v = draw();
    }
    v % n
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values printed by Go 1.27.3 (`rand.New(rand.NewSource(seed))`).
    #[test]
    fn matches_go() {
        let want: &[(i64, [i64; 3], u32)] = include!("../testdata/go_rand.txt");
        for (seed, int63s, uint32) in want {
            let mut r = Rand::new(*seed);
            for v in int63s {
                assert_eq!(r.int63(), *v, "seed {seed}");
            }
            assert_eq!(r.uint32(), *uint32, "seed {seed}");
        }
    }

    /// Go 1.27.3 `rand.New(rand.NewSource(seed)).Intn(n)`, printed by
    /// `zzprobe/e05sst`.
    #[test]
    fn intn_matches_go() {
        let want: &[(i64, i64, [i64; 4])] = include!("../testdata/go_rand_intn.txt");
        for (seed, n, values) in want {
            let mut r = Rand::new(*seed);
            for v in values {
                assert_eq!(r.intn(*n), *v, "seed {seed} n {n}");
            }
        }
    }

    #[test]
    fn int31n_resamples_outside_the_equal_size_buckets() {
        let bound = (1 << 30) + 1;
        let mut samples = [i32::MAX, bound, bound - 1].into_iter();
        assert_eq!(int31n_from(bound, || samples.next().unwrap()), bound - 1);
        assert_eq!(samples.next(), None);
    }
}

/// Go `rand.Int31n`, the package function over the default random source.
///
/// # Panics
/// `n` is not positive, as Go panics.
pub fn int31n(n: i32) -> i32 {
    // Go's default package source uses the runtime's lock-free random state.
    // The separately seeded Rand type keeps its deterministic source above.
    int31n_from(n, || (tidb_fastrand::uint32() >> 1) as i32)
}
