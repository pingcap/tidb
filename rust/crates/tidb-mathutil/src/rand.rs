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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_RAND_VALUE: u32 = 0x3fff_ffff;

fn pack_seeds(seed1: u32, seed2: u32) -> u64 {
    u64::from(seed1) | (u64::from(seed2) << 32)
}

/// MySQL's two-seed random number generator.
pub struct MysqlRng {
    // Each draw and seed change acts on the whole pair. Relaxed ordering is
    // sufficient because the state does not guard other data.
    seeds: AtomicU64,
    contention: Mutex<()>,
}

impl std::fmt::Debug for MysqlRng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlRng").finish_non_exhaustive()
    }
}

impl MysqlRng {
    /// Creates the RNG with the exact Go wrapping/truncation seed derivation.
    pub fn new_with_seed(seed: i64) -> Self {
        let seed1 = seed.wrapping_mul(0x1_0001).wrapping_add(55_555_555) as u32 % MAX_RAND_VALUE;
        let seed2 = seed.wrapping_mul(0x1000_0001) as u32 % MAX_RAND_VALUE;
        Self {
            seeds: AtomicU64::new(pack_seeds(seed1, seed2)),
            contention: Mutex::new(()),
        }
    }

    /// Creates the RNG from the current Unix nanosecond timestamp.
    pub fn new_with_time() -> Self {
        let now = SystemTime::now();
        let seed = match now.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos() as i64,
            Err(error) => -(error.duration().as_nanos() as i64),
        };
        Self::new_with_seed(seed)
    }

    fn update_seeds(&self, update: impl Fn(u64) -> u64) -> u64 {
        let previous = self.seeds.load(Ordering::Relaxed);
        let next = update(previous);
        if self
            .seeds
            .compare_exchange(previous, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
        // Serialize retries so concurrent callers do not repeatedly
        // invalidate one another's attempts under sustained contention.
        let _guard = self
            .contention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            let previous = self.seeds.load(Ordering::Relaxed);
            let next = update(previous);
            if self
                .seeds
                .compare_exchange_weak(previous, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Generates the next value in `[0, 1)`.
    pub fn gen(&self) -> f64 {
        let next = self.update_seeds(|seeds| {
            let seed1 = seeds as u32;
            let seed2 = (seeds >> 32) as u32;
            let next_seed1 = seed1.wrapping_mul(3).wrapping_add(seed2) % MAX_RAND_VALUE;
            let next_seed2 = next_seed1.wrapping_add(seed2).wrapping_add(33) % MAX_RAND_VALUE;
            pack_seeds(next_seed1, next_seed2)
        });
        f64::from(next as u32) / f64::from(MAX_RAND_VALUE)
    }

    /// Replaces the first seed without normalization, matching the source.
    pub fn set_seed1(&self, seed: u32) {
        self.update_seeds(|seeds| pack_seeds(seed, (seeds >> 32) as u32));
    }

    /// Replaces the second seed without normalization, matching the source.
    pub fn set_seed2(&self, seed: u32) {
        self.update_seeds(|seeds| pack_seeds(seeds as u32, seed));
    }

    /// Returns the first seed.
    pub fn get_seed1(&self) -> u32 {
        self.seeds.load(Ordering::Relaxed) as u32
    }

    /// Returns the second seed.
    pub fn get_seed2(&self) -> u32 {
        (self.seeds.load(Ordering::Relaxed) >> 32) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_rand_with_time() {
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
    fn test_rand_with_seed() {
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
    fn concurrent_draws_advance_the_same_seed_stream() {
        const WORKERS: usize = 8;
        const DRAWS_PER_WORKER: usize = 2_048;
        let serial = MysqlRng::new_with_seed(42);
        let mut expected = (0..WORKERS * DRAWS_PER_WORKER)
            .map(|_| serial.gen().to_bits())
            .collect::<Vec<_>>();
        expected.sort_unstable();

        let shared = MysqlRng::new_with_seed(42);
        let ready = std::sync::Barrier::new(WORKERS);
        let mut actual = thread::scope(|scope| {
            let workers = (0..WORKERS)
                .map(|_| {
                    scope.spawn(|| {
                        ready.wait();
                        (0..DRAWS_PER_WORKER)
                            .map(|_| shared.gen().to_bits())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        actual.sort_unstable();

        assert_eq!(actual, expected);
        assert_eq!(shared.get_seed1(), serial.get_seed1());
        assert_eq!(shared.get_seed2(), serial.get_seed2());
    }

    #[test]
    fn test_rand_with_seed_1_and_seed_2() {
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
