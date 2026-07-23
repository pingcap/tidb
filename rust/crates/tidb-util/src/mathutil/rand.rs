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

use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_RAND_VALUE: u32 = 0x3fff_ffff;

#[derive(Clone, Copy, Debug)]
struct State {
    seed1: u32,
    seed2: u32,
}

/// MySQL's two-seed random number generator.
#[derive(Debug)]
pub struct MysqlRng {
    state: Mutex<State>,
}

impl MysqlRng {
    /// Creates the RNG with the exact Go wrapping/truncation seed derivation.
    #[must_use]
    pub fn new_with_seed(seed: i64) -> Self {
        let seed1 = seed.wrapping_mul(0x1_0001).wrapping_add(55_555_555) as u32 % MAX_RAND_VALUE;
        let seed2 = seed.wrapping_mul(0x1000_0001) as u32 % MAX_RAND_VALUE;
        Self {
            state: Mutex::new(State { seed1, seed2 }),
        }
    }

    /// Creates the RNG from the current Unix nanosecond timestamp.
    #[must_use]
    pub fn new_with_time() -> Self {
        let now = SystemTime::now();
        let seed = match now.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos() as i64,
            Err(error) => -(error.duration().as_nanos() as i64),
        };
        Self::new_with_seed(seed)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Generates the next value in `[0, 1)`.
    pub fn gen(&self) -> f64 {
        let mut state = self.lock();
        state.seed1 = state.seed1.wrapping_mul(3).wrapping_add(state.seed2) % MAX_RAND_VALUE;
        state.seed2 = state.seed1.wrapping_add(state.seed2).wrapping_add(33) % MAX_RAND_VALUE;
        f64::from(state.seed1) / f64::from(MAX_RAND_VALUE)
    }

    /// Replaces the first seed without normalization, matching the source.
    pub fn set_seed1(&self, seed: u32) {
        self.lock().seed1 = seed;
    }

    /// Replaces the second seed without normalization, matching the source.
    pub fn set_seed2(&self, seed: u32) {
        self.lock().seed2 = seed;
    }

    /// Returns the first seed.
    pub fn get_seed1(&self) -> u32 {
        self.lock().seed1
    }

    /// Returns the second seed.
    pub fn get_seed2(&self) -> u32 {
        self.lock().seed2
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn TestRandWithTime() {
        let rng1 = MysqlRng::new_with_time();
        thread::sleep(Duration::from_millis(1));
        let rng2 = MysqlRng::new_with_time();
        let got1 = rng1.gen();
        let got2 = rng2.gen();
        assert!((0.0..1.0).contains(&got1));
        assert_ne!(got1, rng1.gen());
        assert!((0.0..1.0).contains(&got2));
        assert_ne!(got2, rng2.gen());
        assert_ne!(got1, got2);
    }

    #[test]
    fn TestRandWithSeed() {
        let tests = [
            (0, 0.155_220_427_694_935_74, 0.620_881_741_513_388),
            (1, 0.405_403_537_121_977_24, 0.871_614_180_385_707_1),
            (-1, 0.905_037_321_993_184_5, 0.370_149_321_267_520_37),
            (i64::MAX, 0.905_037_321_993_184_5, 0.370_149_321_267_520_37),
        ];
        for (seed, once, twice) in tests {
            let rng = MysqlRng::new_with_seed(seed);
            assert_eq!(rng.gen(), once);
            assert_eq!(rng.gen(), twice);
        }
    }

    #[test]
    fn TestRandWithSeed1AndSeed2() {
        let rng = MysqlRng::new_with_time();
        rng.set_seed1(10_000_000);
        rng.set_seed2(1_000_000);

        assert_eq!(rng.gen(), 0.028_870_999_839_968_048);
        assert_eq!(rng.gen(), 0.116_415_352_669_000_02);
        assert_eq!(rng.gen(), 0.495_463_794_558_740_96);
        assert_eq!(rng.get_seed1(), 532_000_198);
        assert_eq!(rng.get_seed2(), 689_000_330);
    }
}
