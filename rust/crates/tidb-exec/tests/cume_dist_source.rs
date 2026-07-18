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

//! Source-backed tests for cumulative-distribution peer ranking.

use tidb_exec::cume_dist::{cumulative_distribution, CumeDistState};

fn assert_close(actual: &[f64], expected: &[f64]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert!((actual - expected).abs() < 1e-15, "{actual} != {expected}");
    }
}

#[test]
fn cume_dist_window_vectors_match_source() {
    // Source: pkg/executor/aggfuncs/func_cume_dist.go:43-57.
    // Direct Go coverage: pkg/executor/aggfuncs/window_func_test.go:172
    // (TestWindowFunctions), including one-row, all-peer, and unique-key
    // CUME_DIST cases.
    assert_close(&cumulative_distribution(&[1]), &[1.0]);
    assert_close(&cumulative_distribution(&[1, 1]), &[1.0, 1.0]);
    assert_close(
        &cumulative_distribution(&[1, 2, 3, 4]),
        &[0.25, 0.5, 0.75, 1.0],
    );
    assert_close(
        &cumulative_distribution(&[1, 1, 2, 3]),
        &[0.5, 0.5, 0.75, 1.0],
    );
}

#[test]
fn cume_dist_partial_state_size_and_empty_boundary_match_source() {
    // Direct Go coverage: pkg/executor/aggfuncs/func_cume_dist_test.go:25
    // (TestMemCumeDist), whose memory contract is tied to the partial state.
    assert_eq!(
        CumeDistState::partial_state_size(),
        std::mem::size_of::<CumeDistState>()
    );
    assert!(CumeDistState::partial_state_size() > 0);
    assert!(cumulative_distribution(&[]).is_empty());

    let mut state = CumeDistState::new(&[4, 4, 7]);
    assert!((state.next().expect("first row") - 2.0 / 3.0).abs() < 1e-15);
    assert!((state.next().expect("peer row") - 2.0 / 3.0).abs() < 1e-15);
    assert!((state.next().expect("last row") - 1.0).abs() < 1e-15);
    assert_eq!(state.next(), None);
}
