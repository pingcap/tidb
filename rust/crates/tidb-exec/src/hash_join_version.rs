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

//! Hash-join version values from `pkg/executor/join/joinversion/join_version.go`.
//!
//! TiDB distinguishes the legacy hash join (v1) from the optimized hash join
//! (v2). This leaf preserves the source literals and the case-insensitive
//! optimized-version predicate; system-variable validation, session mutation,
//! planner GA gates, and runtime join implementation remain external.

use std::sync::atomic::{AtomicBool, Ordering};

/// Legacy hash-join version (v1).
pub const HASH_JOIN_VERSION_LEGACY: &str = "legacy";

/// Optimized hash-join version (v2).
pub const HASH_JOIN_VERSION_OPTIMIZED: &str = "optimized";

/// TiFlash's default hash-join version.
pub const TIFLASH_HASH_JOIN_VERSION_DEFAULT: &str = HASH_JOIN_VERSION_LEGACY;

/// Returns true when a version selects the optimized hash join.
#[must_use]
pub fn is_optimized_version(version: &str) -> bool {
    version.eq_ignore_ascii_case(HASH_JOIN_VERSION_OPTIMIZED)
}

/// Whether hash join v2 is used for join kinds that are not yet GA.
///
/// Go declares `UseHashJoinV2ForNonGAJoin = false` and then flips it to `true`
/// in `init()` so tests exercise v2 across every join kind; a release build is
/// expected to reset it. This mutable global mirrors that: it starts at the
/// post-`init()` value (`true`) and can be toggled at runtime, matching the
/// per-kind GA rollout the Go comment describes.
static USE_HASH_JOIN_V2_FOR_NON_GA_JOIN: AtomicBool = AtomicBool::new(true);

/// Reads `UseHashJoinV2ForNonGAJoin`.
#[must_use]
pub fn use_hash_join_v2_for_non_ga_join() -> bool {
    USE_HASH_JOIN_V2_FOR_NON_GA_JOIN.load(Ordering::Relaxed)
}

/// Sets `UseHashJoinV2ForNonGAJoin` (release builds reset it to `false`).
pub fn set_use_hash_join_v2_for_non_ga_join(value: bool) {
    USE_HASH_JOIN_V2_FOR_NON_GA_JOIN.store(value, Ordering::Relaxed);
}

/// Returns true when hash join v2 is supported in the current environment.
///
/// Go guards on `!heapObjectsCanMove() && sizeOfUintptr >= sizeOfUnsafePointer`
/// because v2 stores raw row pointers as `uintptr`, which is unsafe under a
/// moving garbage collector. Rust has no moving GC, so the heap-move edge case
/// cannot occur and only the pointer-width invariant remains — `usize` and a
/// thin pointer are the same width on every supported target, so this holds.
#[must_use]
pub fn is_hash_join_v2_supported() -> bool {
    const SIZE_OF_UINTPTR: usize = std::mem::size_of::<usize>();
    const SIZE_OF_UNSAFE_POINTER: usize = std::mem::size_of::<*const ()>();
    SIZE_OF_UINTPTR >= SIZE_OF_UNSAFE_POINTER
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimized_predicate_is_case_insensitive() {
        assert!(is_optimized_version("optimized"));
        assert!(is_optimized_version("OPTIMIZED"));
        assert!(is_optimized_version("Optimized"));
        assert!(!is_optimized_version("legacy"));
        assert!(!is_optimized_version(""));
        assert_eq!(TIFLASH_HASH_JOIN_VERSION_DEFAULT, HASH_JOIN_VERSION_LEGACY);
    }

    #[test]
    fn non_ga_flag_starts_true_and_toggles() {
        // Mirrors Go's init(): the post-init value is true.
        assert!(use_hash_join_v2_for_non_ga_join());
        set_use_hash_join_v2_for_non_ga_join(false);
        assert!(!use_hash_join_v2_for_non_ga_join());
        set_use_hash_join_v2_for_non_ga_join(true);
        assert!(use_hash_join_v2_for_non_ga_join());
    }

    #[test]
    fn v2_supported_without_moving_gc() {
        assert!(is_hash_join_v2_supported());
    }
}
