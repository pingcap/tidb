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

//! Direct source contract for `pkg/parser/util/hash64.go`.

use tidb_hash::IHasher;

#[derive(Default)]
struct RecordingHasher {
    calls: Vec<String>,
    sum: u64,
}

impl IHasher for RecordingHasher {
    fn hash_bool(&mut self, value: bool) {
        self.calls.push(format!("bool:{value}"));
    }

    fn hash_int(&mut self, value: i64) {
        self.calls.push(format!("int:{value}"));
    }

    fn hash_int64(&mut self, value: i64) {
        self.calls.push(format!("int64:{value}"));
    }

    fn hash_uint64(&mut self, value: u64) {
        self.calls.push(format!("uint64:{value}"));
    }

    fn hash_float64(&mut self, value: f64) {
        self.calls.push(format!("float64:{value}"));
    }

    fn hash_rune(&mut self, value: i32) {
        self.calls.push(format!("rune:{value}"));
    }

    fn hash_string(&mut self, value: &str) {
        self.calls.push(format!("string:{value}"));
    }

    fn hash_byte(&mut self, value: u8) {
        self.calls.push(format!("byte:{value}"));
    }

    fn hash_bytes(&mut self, value: &[u8]) {
        self.calls.push(format!("bytes:{value:?}"));
    }

    fn reset(&mut self) {
        self.calls.clear();
        self.sum = 0;
    }

    fn sum64(&self) -> u64 {
        self.sum
    }
}

#[test]
fn interface_exposes_every_go_method_with_source_widths() {
    let mut hasher = RecordingHasher::default();
    hasher.hash_bool(true);
    hasher.hash_int(-1);
    hasher.hash_int64(i64::MIN);
    hasher.hash_uint64(u64::MAX);
    hasher.hash_float64(1.5);
    hasher.hash_rune('界' as i32);
    hasher.hash_string("TiDB");
    hasher.hash_byte(0xff);
    hasher.hash_bytes(&[0, 1, 2]);

    assert_eq!(hasher.calls.len(), 9);
    assert_eq!(hasher.sum64(), 0);
    hasher.reset();
    assert!(hasher.calls.is_empty());
}
