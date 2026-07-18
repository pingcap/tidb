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

//! Source-backed tests for slow-log rule composition.

use tidb_exec::slow_log_match::{match_rules, should_write_slow_log, UNSET_CONNECTION_ID};

fn rule(conditions: &[bool]) -> Vec<bool> {
    conditions.to_vec()
}

#[test]
fn slow_log_match_preserves_and_or_precedence() {
    // Source: pkg/executor/adapter_slow_log.go:94-158 and
    // pkg/executor/adapter_test.go:206-398 (TestShouldWriteSlowLog).
    assert_eq!(UNSET_CONNECTION_ID, -1);
    assert!(!match_rules(&[]));
    assert!(!match_rules(&[rule(&[true, false]), rule(&[false, false])]));
    assert!(match_rules(&[rule(&[true, false]), rule(&[true, true])]));
    assert!(match_rules(&[rule(&[])]));

    let no_rules: Option<&[Vec<bool>]> = None;
    let session_miss = vec![rule(&[false])];
    let connection_hit = vec![rule(&[true])];
    let global_hit = vec![rule(&[true])];
    let global_miss = vec![rule(&[false])];

    assert!(!should_write_slow_log(Some(&session_miss), None, None));
    assert!(should_write_slow_log(
        Some(&session_miss),
        Some(&connection_hit),
        Some(&global_miss)
    ));
    assert!(should_write_slow_log(
        Some(&session_miss),
        Some(&global_miss),
        Some(&global_hit)
    ));
    assert!(!should_write_slow_log(
        Some(&session_miss),
        Some(&global_miss),
        Some(&global_miss)
    ));
    assert!(!should_write_slow_log(no_rules, no_rules, no_rules));
}
