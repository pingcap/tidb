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

//! Ordered missing-handle detection from `pkg/executor/distsql.go`.
//!
//! TiDB walks expected handles in source order, consumes matching entries from
//! the obtained set, and stops after the expected cardinality difference has
//! been reported. KV handle encoding, lookup workers, and consistency reporting
//! remain outside this dependency-closed collection helper.

use std::collections::BTreeSet;

/// Returns expected handles absent from `obtained`, consuming matches visited
/// before the source cardinality bound is reached.
///
/// The source computes capacity from `expected.len() - obtained.len()` and
/// therefore assumes the obtained set is no larger than the expected list.
/// This implementation keeps that contract explicit with a checked subtraction
/// rather than silently changing the invalid-input behavior.
#[must_use]
pub fn get_lack_handles(expected: &[i64], obtained: &mut BTreeSet<i64>) -> Vec<i64> {
    let difference_count = expected
        .len()
        .checked_sub(obtained.len())
        .expect("obtained handle set cannot exceed expected handles");
    let mut missing = Vec::with_capacity(difference_count);
    let mut reported = 0;

    for &handle in expected {
        if !obtained.remove(&handle) {
            missing.push(handle);
            reported += 1;
            if reported == difference_count {
                break;
            }
        }
    }
    missing
}
