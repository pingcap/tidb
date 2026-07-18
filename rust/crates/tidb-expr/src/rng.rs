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

//! MySQL's two-seed pseudo-random generator, ported structurally from
//! `pkg/util/mathutil/rand.go`.

const MAX_RAND_VALUE: u32 = 0x3fff_ffff;

/// The per-session/per-expression state used by `RAND()` and constant
/// `RAND(N)`. There is deliberately no platform RNG dependency: TiDB's own
/// recurrence is the SQL contract once its two seeds are fixed.
#[derive(Debug, Clone)]
pub struct MysqlRng {
    seed1: u32,
    seed2: u32,
}

impl Default for MysqlRng {
    fn default() -> Self {
        Self::new_with_seed(0)
    }
}

impl MysqlRng {
    /// `mathutil.NewWithSeed`: preserve Go's conversion to `uint32` before
    /// taking the 30-bit modulus, including negative/large signed seeds.
    pub fn new_with_seed(seed: i64) -> Self {
        let seed1 = (seed.wrapping_mul(0x1_0001).wrapping_add(55_555_555) as u32) % MAX_RAND_VALUE;
        let seed2 = (seed.wrapping_mul(0x1000_0001) as u32) % MAX_RAND_VALUE;
        Self { seed1, seed2 }
    }

    /// `MysqlRng.Gen` from TiDB's source.
    pub fn gen(&mut self) -> f64 {
        self.seed1 = ((u64::from(self.seed1) * 3 + u64::from(self.seed2))
            % u64::from(MAX_RAND_VALUE)) as u32;
        self.seed2 = ((u64::from(self.seed1) + u64::from(self.seed2) + 33)
            % u64::from(MAX_RAND_VALUE)) as u32;
        f64::from(self.seed1) / f64::from(MAX_RAND_VALUE)
    }

    /// Replaces the first raw seed, as TiDB's session-only `rand_seed1`
    /// sysvar does after its own typed normalization.
    pub fn set_seed1(&mut self, seed: u32) {
        self.seed1 = seed;
    }

    /// Replaces the second raw seed, as TiDB's session-only `rand_seed2`
    /// sysvar does after its own typed normalization.
    pub fn set_seed2(&mut self, seed: u32) {
        self.seed2 = seed;
    }
}

#[cfg(test)]
mod tests {
    use super::MysqlRng;

    #[test]
    fn source_seed_vectors_match() {
        for (seed, first, second) in [
            (0, 0.15522042769493574, 0.620881741513388),
            (1, 0.40540353712197724, 0.8716141803857071),
            (-1, 0.9050373219931845, 0.37014932126752037),
            (i64::MAX, 0.9050373219931845, 0.37014932126752037),
        ] {
            let mut rng = MysqlRng::new_with_seed(seed);
            assert_eq!(rng.gen(), first);
            assert_eq!(rng.gen(), second);
        }
    }

    #[test]
    fn source_raw_seed_vectors_match() {
        let mut rng = MysqlRng::default();
        rng.set_seed1(10_000_000);
        rng.set_seed2(1_000_000);
        assert_eq!(rng.gen(), 0.028870999839968048);
        assert_eq!(rng.gen(), 0.11641535266900002);
        assert_eq!(rng.gen(), 0.49546379455874096);
        assert_eq!(rng.seed1, 532_000_198);
        assert_eq!(rng.seed2, 689_000_330);
    }
}
