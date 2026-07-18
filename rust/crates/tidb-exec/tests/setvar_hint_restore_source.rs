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

//! Source-backed tests for SET_VAR hint restore metadata.

use tidb_exec::setvar_hint_restore::SetVarHintRestore;

#[test]
fn setvar_hint_restore_keeps_first_old_value() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1371-1380 and
    // pkg/planner/core/stats_test.go:284-291.
    let mut restore = SetVarHintRestore::new();
    assert_eq!(restore.old_value("tidb_opt_a"), None);
    restore.record("tidb_opt_a", "old-a");
    restore.record("tidb_opt_a", "new-a");
    restore.record("tidb_opt_b", "old-b");
    assert_eq!(restore.old_value("tidb_opt_a"), Some("old-a"));
    assert_eq!(restore.old_value("tidb_opt_b"), Some("old-b"));
    assert_eq!(restore.old_value("missing"), None);
    assert_eq!(
        restore.entries().collect::<Vec<_>>(),
        vec![("tidb_opt_a", "old-a"), ("tidb_opt_b", "old-b")]
    );
}

#[test]
fn setvar_hint_restore_clear_removes_statement_metadata() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1373-1378. A nil map is
    // lazily initialized by the first record; clear models statement reset.
    let mut restore = SetVarHintRestore::default();
    restore.record("tidb_opt_a", "old-a");
    restore.clear();
    assert_eq!(restore.old_value("tidb_opt_a"), None);
    restore.record("tidb_opt_a", "fresh-a");
    assert_eq!(restore.old_value("tidb_opt_a"), Some("fresh-a"));
}
