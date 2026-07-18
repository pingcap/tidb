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

//! Source-backed tests for synchronous statistics-load concurrency thresholds.

use tidb_stats::sync_load_concurrency_for_cpu;

#[test]
fn sync_load_concurrency_matches_go_thresholds() {
    assert_eq!(sync_load_concurrency_for_cpu(0), 5);
    assert_eq!(sync_load_concurrency_for_cpu(8), 5);
    assert_eq!(sync_load_concurrency_for_cpu(9), 6);
    assert_eq!(sync_load_concurrency_for_cpu(16), 6);
    assert_eq!(sync_load_concurrency_for_cpu(17), 8);
    assert_eq!(sync_load_concurrency_for_cpu(32), 8);
    assert_eq!(sync_load_concurrency_for_cpu(33), 10);
    assert_eq!(sync_load_concurrency_for_cpu(128), 10);
}
