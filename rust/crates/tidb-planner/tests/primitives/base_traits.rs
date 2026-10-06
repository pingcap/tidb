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

//! Dependency-closed vectors for `pkg/planner/cascades/base/base.go`.
//!
//! The source interface contracts are exercised by
//! `pkg/planner/cascades/base/hash_equaler_test.go`: `TestStringLen` at line
//! 33, `TestStructType` at line 88, and `TestHash64a` at line 113.

use std::any::Any;

use tidb_planner::base_traits::{Equals, Hash64, HashEquals};
use tidb_planner::hash_equaler::{new_hash_equaler, Hasher};

struct TmpStr {
    first: String,
    second: String,
}

impl Hash64 for TmpStr {
    fn hash64(&self, hasher: &mut dyn Hasher) {
        hasher.hash_string(&self.first);
        hasher.hash_string(&self.second);
    }
}

#[derive(Debug)]
struct StructA {
    number: i64,
    text: String,
}

impl Hash64 for StructA {
    fn hash64(&self, hasher: &mut dyn Hasher) {
        hasher.hash_int(self.number);
        hasher.hash_string(&self.text);
    }
}

impl Equals for StructA {
    fn equals(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .is_some_and(|value| self.number == value.number && self.text == value.text)
    }
}

#[derive(Debug)]
struct StructB {
    number: i64,
    text: String,
}

impl Hash64 for StructB {
    fn hash64(&self, hasher: &mut dyn Hasher) {
        hasher.hash_int(self.number);
        hasher.hash_string(&self.text);
    }
}

impl Equals for StructB {
    fn equals(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .is_some_and(|value| self.number == value.number && self.text == value.text)
    }
}

fn assert_hash_equals<T: HashEquals>(_: &T) {}

#[test]
fn hash64_preserves_field_framing_from_source_string_test() {
    let first = TmpStr {
        first: "abc".to_owned(),
        second: "def".to_owned(),
    };
    let second = TmpStr {
        first: "abcdef".to_owned(),
        second: String::new(),
    };
    let mut first_hasher = new_hash_equaler();
    let mut second_hasher = new_hash_equaler();
    first.hash64(&mut first_hasher);
    second.hash64(&mut second_hasher);
    assert_ne!(first_hasher.sum64(), second_hasher.sum64());
}

#[test]
fn different_concrete_types_can_hash_equally_but_not_compare_equal() {
    let first = StructA {
        number: 1,
        text: "abc".to_owned(),
    };
    let second = StructB {
        number: 1,
        text: "abc".to_owned(),
    };
    let mut first_hasher = new_hash_equaler();
    let mut second_hasher = new_hash_equaler();
    first.hash64(&mut first_hasher);
    second.hash64(&mut second_hasher);
    assert_eq!(first_hasher.sum64(), second_hasher.sum64());
    assert!(!first.equals(&second));
    assert_hash_equals(&first);
    assert_hash_equals(&second);
}

#[test]
fn hasher_contract_keeps_primitive_sequence_and_reset() {
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
    first.hash_string("hello");
    second.hash_string("hello");
    first.hash_bytes(b"world");
    second.hash_bytes(b"world");
    for character in ['我', '是', '谁'] {
        first.hash_rune(character as i32);
        second.hash_rune(character as i32);
    }
    assert_eq!(first.sum64(), second.sum64());

    first.reset();
    second.reset();
    first.hash_string("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
    second.hash_string("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
    assert_eq!(first.sum64(), second.sum64());
}
