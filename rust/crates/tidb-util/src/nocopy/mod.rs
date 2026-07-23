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

//! Complete transcreation of `pkg/util/nocopy`.
//!
//! `nocopy.go` maps to this module and `BUILD.bazel` maps to the `tidb-util`
//! manifest. The package has no tests, `TestMain`, benchmarks, fuzz targets,
//! examples, fixtures, generated files, or build-tag variants.
//!
//! Go's marker relies on `go vet` recognizing its `Lock` method. Rust makes
//! implicit copying impossible directly: this zero-sized marker intentionally
//! implements neither [`Copy`] nor [`Clone`]. Explicit ownership moves remain
//! valid, just as moving an owning Rust value remains valid generally.

/// Zero-sized marker that prevents an embedding Rust type from becoming
/// implicitly copyable.
///
/// ```compile_fail
/// use tidb_util::nocopy::NoCopy;
///
/// let marker = NoCopy::new();
/// let moved = marker;
/// let copied_again = marker;
/// ```
#[derive(Debug, Default)]
pub struct NoCopy;

impl NoCopy {
    /// Constructs the source zero value.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Source-compatible no-op `sync.Locker.Lock` method.
    pub const fn lock(&self) {}

    /// Source-compatible no-op `sync.Locker.Unlock` method.
    pub const fn unlock(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_zero_value_and_no_op_methods_are_preserved() {
        let marker = NoCopy::new();
        marker.lock();
        marker.unlock();
        assert_eq!(std::mem::size_of_val(&marker), 0);
    }
}
