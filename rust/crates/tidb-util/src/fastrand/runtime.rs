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

//! Native implementation of the observable `pkg/util/fastrand/runtime.go`
//! random-number contract.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(target_pointer_width = "64")]
use super::random::Wyrand;

const FALLBACK_INCREMENT: u64 = 0xa076_1d64_78bd_642f;
static FALLBACK_SEED: AtomicU64 = AtomicU64::new(0xe703_7ed1_a0b4_28db);

thread_local! {
    static RANDOM: Cell<RuntimeRandom> = Cell::new(RuntimeRandom::new(initial_seed()));
}

#[cfg(target_pointer_width = "64")]
type RuntimeRandom = Wyrand;

#[cfg(target_pointer_width = "32")]
#[derive(Clone, Copy)]
struct RuntimeRandom(u64);

#[cfg(target_pointer_width = "32")]
impl RuntimeRandom {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u32 {
        #[cfg(target_endian = "little")]
        let (mut s1, s0) = (self.0 as u32, (self.0 >> 32) as u32);
        #[cfg(target_endian = "big")]
        let (mut s1, s0) = ((self.0 >> 32) as u32, self.0 as u32);

        s1 ^= s1 << 17;
        s1 = s1 ^ s0 ^ s1 >> 7 ^ s0 >> 16;

        #[cfg(target_endian = "little")]
        {
            self.0 = u64::from(s0) | (u64::from(s1) << 32);
        }
        #[cfg(target_endian = "big")]
        {
            self.0 = (u64::from(s0) << 32) | u64::from(s1);
        }
        s0.wrapping_add(s1)
    }
}

fn initial_seed() -> u64 {
    let mut bytes = [0; 8];
    if getrandom::fill(&mut bytes).is_ok() {
        return u64::from_ne_bytes(bytes);
    }

    // Go's runtime.cheaprand cannot report initialization failure. Preserve
    // that infallible contract with a unique monotonic fallback mixed with the
    // current clock; this path is not a cryptographic promise.
    let sequence = FALLBACK_SEED.fetch_add(FALLBACK_INCREMENT, Ordering::Relaxed);
    let clock = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    sequence ^ clock
}

/// Returns a lock-free pseudo-random `u32`.
pub fn uint32() -> u32 {
    RANDOM.with(|state| {
        let mut random = state.get();
        #[cfg(target_pointer_width = "64")]
        let value = random.next() as u32;
        #[cfg(target_pointer_width = "32")]
        let value = random.next();
        state.set(random);
        value
    })
}
