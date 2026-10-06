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

//! Dependency-closed vectors for `pkg/planner/cascades/base/hash_equaler.go`.
//!
//! Direct Go anchors are `TestStringLen` at line 33, `TestStructType` at line
//! 88, and `TestHash64a` at line 113 in
//! `pkg/planner/cascades/base/hash_equaler_test.go`.

use tidb_planner::hash_equaler::{new_hash_equaler, Hasher};

#[test]
fn hash_string_frames_byte_length_before_runes() {
    let mut first = new_hash_equaler();
    let mut second = new_hash_equaler();
    first.hash_string("abc");
    first.hash_string("def");
    second.hash_string("abcdef");
    second.hash_string("");
    assert_ne!(first.sum64(), second.sum64());
}

#[test]
fn equal_field_sequences_have_equal_hashes() {
    let mut first = new_hash_equaler();
    let mut second = new_hash_equaler();
    first.hash_int(1);
    first.hash_string("abc");
    second.hash_int(1);
    second.hash_string("abc");
    assert_eq!(first.sum64(), second.sum64());
}

#[test]
fn primitive_hash_updates_and_reset_are_deterministic() {
    let mut first = new_hash_equaler();
    let mut second = new_hash_equaler();
    first.hash_bool(true);
    second.hash_bool(true);
    first.hash_bool(false);
    second.hash_bool(false);
    first.hash_int(199);
    second.hash_int(199);
    first.hash_int64(13_534_523_462_346);
    second.hash_int64(13_534_523_462_346);
    first.hash_uint64(13_534_523_462_346);
    second.hash_uint64(13_534_523_462_346);
    first.hash_float64(1.5);
    second.hash_float64(1.5);
    first.hash_string("hello");
    second.hash_string("hello");
    first.hash_bytes(b"world");
    second.hash_bytes(b"world");
    for character in ['我', '是', '谁'] {
        first.hash_rune(character as i32);
        second.hash_rune(character as i32);
    }
    assert_eq!(first.sum64(), second.sum64());

    first.set_cache(vec![1, 2, 3]);
    assert_eq!(first.cache(), &[1, 2, 3]);
    first.reset();
    second.reset();
    assert_eq!(first.sum64(), second.sum64());
    assert!(first.cache().is_empty());
}
