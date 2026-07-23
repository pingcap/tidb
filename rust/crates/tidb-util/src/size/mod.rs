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

//! Complete transcreation of `pkg/util/size`.
//!
//! `size.go` maps to this module and `BUILD.bazel` maps to the `tidb-util`
//! manifest. The package has no tests, `TestMain`, benchmarks, fuzz targets,
//! examples, fixtures, generated files, or build-tag variants.
//!
//! These values deliberately retain the Go source ABI used by TiDB memory
//! accounting. In particular, they do not claim that an arbitrary Rust
//! container has the same header size as its Go counterpart.

const WORD_SIZE: i64 = std::mem::size_of::<usize>() as i64;

/// One kibibyte, named `KB` to match the source API.
pub const KB: u64 = 1_024;
/// One mebibyte.
pub const MB: u64 = KB * 1_024;
/// One gibibyte.
pub const GB: u64 = MB * 1_024;
/// One tebibyte.
pub const TB: u64 = GB * 1_024;
/// One pebibyte.
pub const PB: u64 = TB * 1_024;

/// Size of a Go slice header, excluding its elements.
pub const SIZE_OF_SLICE: i64 = WORD_SIZE * 3;
/// Size of one Go byte.
pub const SIZE_OF_BYTE: i64 = 1;
/// Size of a Go string header.
pub const SIZE_OF_STRING: i64 = WORD_SIZE * 2;
/// Size of one Go bool.
pub const SIZE_OF_BOOL: i64 = 1;
/// Size of a Go pointer.
pub const SIZE_OF_POINTER: i64 = WORD_SIZE;
/// Size of a Go interface header, excluding its dynamic value.
pub const SIZE_OF_INTERFACE: i64 = WORD_SIZE * 2;
/// Size of one Go `float64`.
pub const SIZE_OF_FLOAT64: i64 = 8;
/// Size of one Go `uint64`.
pub const SIZE_OF_UINT64: i64 = 8;
/// Size of one Go `int32`.
pub const SIZE_OF_INT32: i64 = 4;
/// Size of one architecture-width Go `int`.
pub const SIZE_OF_INT: i64 = WORD_SIZE;
/// Size of one Go `uint8`.
pub const SIZE_OF_UINT8: i64 = 1;
/// Size of one architecture-width Go `uint`.
pub const SIZE_OF_UINT: i64 = WORD_SIZE;
/// Size of a Go function value.
pub const SIZE_OF_FUNC: i64 = WORD_SIZE;
/// Size of one Go `int64`.
pub const SIZE_OF_INT64: i64 = 8;
/// Size of a Go map value, excluding its backing map.
pub const SIZE_OF_MAP: i64 = WORD_SIZE;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_constant_table_is_exact_for_the_target_word_size() {
        assert_eq!(
            (KB, MB, GB, TB, PB),
            (1 << 10, 1 << 20, 1 << 30, 1 << 40, 1 << 50)
        );
        assert_eq!(SIZE_OF_SLICE, WORD_SIZE * 3);
        assert_eq!(SIZE_OF_BYTE, 1);
        assert_eq!(SIZE_OF_STRING, WORD_SIZE * 2);
        assert_eq!(SIZE_OF_BOOL, 1);
        assert_eq!(SIZE_OF_POINTER, WORD_SIZE);
        assert_eq!(SIZE_OF_INTERFACE, WORD_SIZE * 2);
        assert_eq!(SIZE_OF_FLOAT64, 8);
        assert_eq!(SIZE_OF_UINT64, 8);
        assert_eq!(SIZE_OF_INT32, 4);
        assert_eq!(SIZE_OF_INT, WORD_SIZE);
        assert_eq!(SIZE_OF_UINT8, 1);
        assert_eq!(SIZE_OF_UINT, WORD_SIZE);
        assert_eq!(SIZE_OF_FUNC, WORD_SIZE);
        assert_eq!(SIZE_OF_INT64, 8);
        assert_eq!(SIZE_OF_MAP, WORD_SIZE);
    }
}
