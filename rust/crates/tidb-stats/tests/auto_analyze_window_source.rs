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

//! Source-backed tests for the auto-analysis daily window.

use tidb_stats::AutoAnalysisTimeWindow;

const fn minute(hour: u16, minute: u16) -> u16 {
    hour * 60 + minute
}

#[test]
fn source_window_includes_boundaries_and_excludes_outside_minutes() {
    let window = AutoAnalysisTimeWindow::new(Some(minute(1, 0)), Some(minute(5, 0)));
    assert!(window.is_within_time_window(minute(1, 0)));
    assert!(window.is_within_time_window(minute(3, 0)));
    assert!(window.is_within_time_window(minute(5, 0)));
    assert!(!window.is_within_time_window(minute(0, 59)));
    assert!(!window.is_within_time_window(minute(5, 1)));
}

#[test]
fn source_window_crosses_midnight() {
    let window = AutoAnalysisTimeWindow::new(Some(minute(22, 0)), Some(minute(2, 0)));
    assert!(window.is_within_time_window(minute(23, 30)));
    assert!(window.is_within_time_window(minute(0, 30)));
    assert!(window.is_within_time_window(minute(2, 0)));
    assert!(!window.is_within_time_window(minute(12, 0)));
}

#[test]
fn source_empty_window_never_matches() {
    assert!(!AutoAnalysisTimeWindow::new(None, None).is_within_time_window(minute(12, 0)));
    assert!(
        !AutoAnalysisTimeWindow::new(Some(minute(1, 0)), None).is_within_time_window(minute(1, 0))
    );
    assert!(
        !AutoAnalysisTimeWindow::new(None, Some(minute(5, 0))).is_within_time_window(minute(5, 0))
    );
}
