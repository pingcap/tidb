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

//! Test scaffolding shared by this crate's test modules.
//!
//! This has no Go counterpart file: Go's tests reach straight for
//! `math/rand`'s `Perm`, `Intn`, and `fmt.Appendf`. No random-number crate is
//! reachable in this offline workspace, so the two draws Go's tests need are
//! provided here over splitmix64. Nothing observable depends on the generator
//! — the tests use randomness only to avoid inserting in sorted order.

/// Go's `prefix` in `lockstore_test.go`, which pairs with `keyFormat`.
pub(crate) const KEY_PREFIX: &str = "ls";

/// Go `numToKey`: `fmt.Appendf(nil, "%s%020d", "ls", n)`.
pub(crate) fn num_to_key(n: usize) -> Vec<u8> {
    format!("{KEY_PREFIX}{n:020}").into_bytes()
}

/// A stand-in for `math/rand.Rand` covering the two draws Go's tests make.
#[derive(Debug)]
pub(crate) struct TestRand {
    state: u64,
}

impl TestRand {
    /// Go seeds from `time.Now().Unix()` / `UnixNano()`; so does this.
    pub(crate) fn new() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Self { state: seed | 1 }
    }

    fn uint64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Go `(*Rand).Intn`.
    pub(crate) fn below(&mut self, n: usize) -> usize {
        (self.uint64() % n as u64) as usize
    }
}

/// Go `rand.Perm`: a pseudo-random permutation of `0..n`.
pub(crate) fn perm(rng: &mut TestRand, n: usize) -> Vec<usize> {
    let mut m: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = rng.below(i + 1);
        m.swap(i, j);
    }
    m
}
