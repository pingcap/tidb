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

//! Complete transcreation of `pkg/util/israce`.
//!
//! Go's mutually exclusive `race` and `!race` source files map to the
//! `tidb-util/race` Cargo feature. The feature is explicit because stable Rust
//! has no portable compile-time predicate for ThreadSanitizer instrumentation;
//! sanitizer builds enable it alongside their sanitizer flags. `BUILD.bazel`
//! maps to the Cargo manifest. The package has no tests, `TestMain`,
//! benchmarks, fuzz targets, examples, fixtures, or generated files.

/// Whether this build uses the source-equivalent race configuration.
pub const RACE_ENABLED: bool = cfg!(feature = "race");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_variant_matches_the_source_tag_contract() {
        assert_eq!(RACE_ENABLED, cfg!(feature = "race"));
    }
}
