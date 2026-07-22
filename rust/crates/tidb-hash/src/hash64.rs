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

//! Dependency-inversion hashing contract from `pkg/parser/util/hash64.go`.

/// Primitive values that TiDB planner objects can append to a 64-bit hash.
///
/// Go's `int` is represented as `i64` because the supported TiDB Rust target is
/// 64-bit. A Go `rune` is represented by its signed 32-bit code point.
pub trait IHasher {
    /// Hashes a boolean value.
    fn hash_bool(&mut self, value: bool);
    /// Hashes a signed machine-sized integer on TiDB's 64-bit target.
    fn hash_int(&mut self, value: i64);
    /// Hashes a signed 64-bit integer.
    fn hash_int64(&mut self, value: i64);
    /// Hashes an unsigned 64-bit integer.
    fn hash_uint64(&mut self, value: u64);
    /// Hashes an IEEE-754 double-precision value.
    fn hash_float64(&mut self, value: f64);
    /// Hashes a Go rune represented as a signed 32-bit code point.
    fn hash_rune(&mut self, value: i32);
    /// Hashes the arbitrary bytes of a Go string.
    ///
    /// Go strings are not required to contain valid UTF-8, so `&[u8]` is the
    /// only lossless Rust representation of the source method's input domain.
    fn hash_string(&mut self, value: &[u8]);
    /// Hashes one byte.
    fn hash_byte(&mut self, value: u8);
    /// Hashes a byte slice.
    fn hash_bytes(&mut self, value: &[u8]);
    /// Resets the accumulated hash state.
    fn reset(&mut self);
    /// Returns the accumulated 64-bit hash.
    fn sum64(&self) -> u64;
}
