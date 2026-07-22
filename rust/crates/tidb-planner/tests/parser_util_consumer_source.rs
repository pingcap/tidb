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

//! Consumer proof for the `pkg/parser/util.IHasher` dependency boundary.

use tidb_hash::IHasher;
use tidb_planner::hash_equaler::{new_hash_equaler, Hasher};

fn append_parser_owned_values(hasher: &mut dyn IHasher) {
    hasher.hash_bool(true);
    hasher.hash_int(-7);
    hasher.hash_string("TiDB");
}

fn append_through_planner_alias(hasher: &mut dyn Hasher) {
    hasher.hash_byte(0xff);
}

#[test]
fn planner_hasher_implements_the_parser_owned_interface() {
    let mut hasher = new_hash_equaler();
    append_parser_owned_values(&mut hasher);
    append_through_planner_alias(&mut hasher);
    let first = hasher.sum64();
    hasher.set_cache(vec![1, 2, 3]);
    assert_eq!(hasher.cache(), &[1, 2, 3]);
    hasher.reset();
    append_parser_owned_values(&mut hasher);
    append_through_planner_alias(&mut hasher);
    assert_eq!(hasher.sum64(), first);
}
