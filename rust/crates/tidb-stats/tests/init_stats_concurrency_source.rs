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

//! Source-backed tests for initialization concurrency policy.

use tidb_stats::init_stats_concurrency;

#[test]
fn source_init_stats_concurrency_clamps_normal_mode() {
    assert_eq!(init_stats_concurrency(0, false), 2);
    assert_eq!(init_stats_concurrency(7, false), 3);
    assert_eq!(init_stats_concurrency(64, false), 16);
}

#[test]
fn source_init_stats_concurrency_uses_two_fewer_in_force_mode() {
    assert_eq!(init_stats_concurrency(0, true), 2);
    assert_eq!(init_stats_concurrency(4, true), 2);
    assert_eq!(init_stats_concurrency(8, true), 6);
    assert_eq!(init_stats_concurrency(32, true), 16);
}

#[test]
fn source_init_stats_concurrency_preserves_signed_low_inputs() {
    assert_eq!(init_stats_concurrency(-1, false), 2);
    assert_eq!(init_stats_concurrency(i64::MIN, true), 16);
}
