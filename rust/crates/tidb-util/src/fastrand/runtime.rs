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

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::random::Wyrand;

const FALLBACK_INCREMENT: u64 = 0xa076_1d64_78bd_642f;
static FALLBACK_SEED: AtomicU64 = AtomicU64::new(0xe703_7ed1_a0b4_28db);

thread_local! {
    static RANDOM: Cell<Wyrand> = Cell::new(Wyrand::new(initial_seed()));
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
#[must_use]
pub fn uint32() -> u32 {
    RANDOM.with(|state| {
        let mut random = state.get();
        let value = random.next() as u32;
        state.set(random);
        value
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::uint32;

    #[test]
    fn runtime_source_is_thread_local_lock_free_and_progresses() {
        let values = (0..32).map(|_| uint32()).collect::<HashSet<_>>();
        assert!(values.len() > 1);

        let workers = (0..8)
            .map(|_| std::thread::spawn(|| (0..128).map(|_| uint32()).collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(worker.join().expect("random worker").len(), 128);
        }
    }
}
