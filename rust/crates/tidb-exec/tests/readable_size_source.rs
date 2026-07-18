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

//! Source-backed tests for human-readable byte-size parsing.

use tidb_exec::readable_size::readable_size_to_bytes;

#[test]
fn readable_size_preserves_source_suffixes_and_parse_boundaries() {
    // Source: pkg/executor/inspection_result.go:437-459.
    // Direct Go coverage: pkg/executor/inspection_result_internal_test.go:22
    // (TestConvertReadableSizeToByteSize).
    for (input, expected) in [
        ("100", 100),
        ("100B", 100),
        ("1KiB", 1024),
        ("1MiB", 1_048_576),
        ("1GiB", 1_073_741_824),
        ("1TiB", 1_099_511_627_776),
        ("1PiB", 1_125_899_906_842_624),
    ] {
        assert_eq!(readable_size_to_bytes(input), Ok(expected));
    }

    for input in ["abc", "100KB", "KiB", ""] {
        assert!(readable_size_to_bytes(input).is_err(), "{input}");
    }
}

#[test]
fn readable_size_preserves_uint64_product_behavior() {
    assert_eq!(readable_size_to_bytes("-1"), Ok(u64::MAX));
    assert_eq!(readable_size_to_bytes("-1KiB"), Ok(u64::MAX - 1023));
}
