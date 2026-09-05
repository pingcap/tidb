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

#![allow(missing_docs)]

use foldhash::fast::SeedableRandomState;
use hashbrown::HashMap;
use tidb_hack::{get_bytes_from_ptr, slice, string, to_swiss_map, MemAwareMap, MutableBytes};

#[deny(unused_must_use)]
#[test]
fn source_return_values_may_be_ignored_like_go() {
    let bytes = MutableBytes::new(b"source".to_vec());
    string(&bytes);
    slice("source");
    // SAFETY: the pointer remains valid for the duration of the discarded
    // slice, matching the source helper's caller-owned contract.
    unsafe { get_bytes_from_ptr(b"source".as_ptr(), 6) };

    let map = HashMap::<i64, i64>::new();
    to_swiss_map(&map);
    let wrapped = to_swiss_map(&map);
    wrapped.cap();
    wrapped.size();

    MemAwareMap::<i64, i64>::new(0);
    let mut tracked = MemAwareMap::<i64, i64>::new(0);
    tracked.init(HashMap::with_hasher(SeedableRandomState::default()));
    tracked.count();
    tracked.is_empty();
    tracked.contains_key(&1);
    tracked.get(&1);
    tracked.len();
    tracked.real_bytes();
}
