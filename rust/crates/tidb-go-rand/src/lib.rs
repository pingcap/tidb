//! Go `math/rand`'s seeded generator: `NewSource` (the additive lagged
//! Fibonacci `rngSource` of `rng.go`) and the `Rand` methods Lightning's
//! row-id sharding reads. A ported caller that seeds Go's generator must
//! draw the same numbers, because they are written into keys.

mod cooked;

use cooked::RNG_COOKED;

const RNG_LEN: usize = 607;
const RNG_TAP: usize = 273;
const RNG_MASK: u64 = (1 << 63) - 1;
const INT32_MAX: i64 = (1 << 31) - 1;

/// Go `seedrand`: x[n+1] = 48271 * x[n] mod (2**31 - 1).
fn seedrand(x: i32) -> i32 {
    const A: i32 = 48271;
    const Q: i32 = 44488;
    const R: i32 = 3399;
    let hi = x / Q;
    let lo = x % Q;
    let x = A * lo - R * hi;
    if x < 0 {
        x + INT32_MAX as i32
    } else {
        x
    }
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
        let mut seed = seed % INT32_MAX;
        if seed < 0 {
            seed += INT32_MAX;
        }
        if seed == 0 {
            seed = 89_482_311;
        }
        let mut x = seed as i32;
        for i in -20..RNG_LEN as i64 {
            x = seedrand(x);
            if i >= 0 {
                let mut u = i64::from(x) << 40;
                x = seedrand(x);
                u ^= i64::from(x) << 20;
                x = seedrand(x);
                u ^= i64::from(x);
                u ^= RNG_COOKED[i as usize];
                self.vec[i as usize] = u;
            }
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
}
