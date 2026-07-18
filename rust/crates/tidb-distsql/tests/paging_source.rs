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

//! Direct source vectors for DistSQL paging growth and seek policy.

use tidb_distsql::{
    calculate_seek_count, grow_paging_size, PagingConfig, MIN_ALLOWED_MAX_PAGING_SIZE,
    MIN_PAGING_SIZE,
};

#[test]
fn source_grow_paging_size_matches_go() {
    assert_eq!(
        grow_paging_size(MIN_PAGING_SIZE, MIN_ALLOWED_MAX_PAGING_SIZE),
        MIN_PAGING_SIZE * 2
    );
    assert_eq!(
        grow_paging_size(MIN_ALLOWED_MAX_PAGING_SIZE, MIN_ALLOWED_MAX_PAGING_SIZE),
        MIN_ALLOWED_MAX_PAGING_SIZE
    );
    assert_eq!(
        grow_paging_size(
            MIN_ALLOWED_MAX_PAGING_SIZE / 2 + 1,
            MIN_ALLOWED_MAX_PAGING_SIZE
        ),
        MIN_ALLOWED_MAX_PAGING_SIZE
    );
}

#[test]
fn source_calculate_seek_count_matches_go() {
    const PAGING_GROWING_SUM: u64 = ((2 << 7) - 1) * MIN_PAGING_SIZE;

    for (expected_count, expected_seeks) in [
        (0, 0.0),
        (1, 1.0),
        (MIN_PAGING_SIZE, 1.0),
        (PAGING_GROWING_SUM, 8.0),
        (PAGING_GROWING_SUM + 1, 9.0),
        (PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE, 9.0),
    ] {
        assert!((calculate_seek_count(expected_count) - expected_seeks).abs() <= 0.1);
    }
}

#[test]
fn paging_config_defaults_consume_the_policy_authority() {
    let defaults = PagingConfig::source_defaults();
    assert_eq!(PagingConfig::default(), defaults);
    assert!(!defaults.enabled);
    assert_eq!(defaults.min_size, MIN_PAGING_SIZE);
    assert_eq!(defaults.max_size, MIN_ALLOWED_MAX_PAGING_SIZE);
    assert_eq!(defaults.size_bytes, 0);
}
