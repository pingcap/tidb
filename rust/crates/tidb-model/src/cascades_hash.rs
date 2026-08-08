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

//! The `HashInt64` subset of `pkg/planner/cascades/base.Hasher` used by
//! `pkg/meta/model.TableInfo.Hash64` and `IndexInfo.Hash64`.
//!
//! Go deliberately hashes one whole integer per FNV-1a step. Rust's standard
//! [`std::hash::Hasher`] byte stream is a different contract, so model identity
//! uses this source-shaped state machine instead.

/// Go cascades `offset64`, copied from the standard FNV-1a implementation.
pub const CASCADES_OFFSET64: u64 = 14_695_981_039_346_656_037;
/// Go cascades `prime64`.
pub const CASCADES_PRIME64: u64 = 1_099_511_628_211;

/// The part of Go `base.Hasher` consumed by model identity methods.
pub trait HashInt64 {
    /// Hashes one signed 64-bit integer as one whole FNV-1a state step.
    fn hash_int64(&mut self, value: i64);
}

/// Source-shaped cascades FNV-1a state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CascadesHasher {
    state: u64,
}

impl Default for CascadesHasher {
    fn default() -> Self {
        Self {
            state: CASCADES_OFFSET64,
        }
    }
}

impl CascadesHasher {
    /// Go `NewHashEqualer`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `Reset`'s hash-state rule.
    pub fn reset(&mut self) {
        self.state = CASCADES_OFFSET64;
    }

    /// Go `Sum64`.
    #[must_use]
    pub fn sum64(&self) -> u64 {
        self.state
    }
}

impl HashInt64 for CascadesHasher {
    fn hash_int64(&mut self, value: i64) {
        self.state = (self.state ^ value as u64).wrapping_mul(CASCADES_PRIME64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_int64_is_one_whole_integer_step() {
        let vectors = [
            (0, 12_638_153_115_695_167_455),
            (1, 12_638_152_016_183_539_244),
            (-1, 5_808_589_858_502_755_950),
            (i64::MIN, 3_414_781_078_840_391_647),
            (i64::MAX, 15_031_961_895_357_531_758),
        ];
        for (value, expected) in vectors {
            let mut hasher = CascadesHasher::new();
            hasher.hash_int64(value);
            assert_eq!(hasher.sum64(), expected);
            hasher.reset();
            assert_eq!(hasher.sum64(), CASCADES_OFFSET64);
        }
    }
}
