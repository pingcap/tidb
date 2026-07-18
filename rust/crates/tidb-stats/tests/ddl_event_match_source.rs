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

//! Source-backed tests for first-match DDL event selection.

use tidb_stats::find_event_with_timeout;

#[test]
fn source_find_event_with_timeout_returns_first_match() {
    let events = [1_u8, 2, 1, 3];
    assert_eq!(find_event_with_timeout(&events, 1), Some(1));
    assert_eq!(find_event_with_timeout(&events, 3), Some(3));
}

#[test]
fn source_find_event_with_timeout_models_timeout_without_match() {
    assert_eq!(find_event_with_timeout::<u8>(&[], 1), None);
    assert_eq!(find_event_with_timeout(&[2_u8, 3, 4], 1), None);
}
