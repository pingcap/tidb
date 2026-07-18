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

//! Source-contract tests for statement-context reference/freeze state.

use tidb_exec::statement_refcount::{
    ReferenceCount, REFERENCE_COUNT_IS_FROZEN, REFERENCE_COUNT_NO_REFERENCE,
};

#[test]
fn reference_count_matches_source_freeze_lifecycle() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:92-125.
    let count = ReferenceCount::new();
    assert_eq!(count.load(), REFERENCE_COUNT_NO_REFERENCE);
    assert!(count.try_increase());
    assert_eq!(count.load(), 1);
    assert!(count.try_increase());
    assert_eq!(count.load(), 2);
    count.decrease();
    assert_eq!(count.load(), 1);
    count.decrease();
    assert_eq!(count.load(), REFERENCE_COUNT_NO_REFERENCE);

    assert!(count.try_freeze());
    assert_eq!(count.load(), REFERENCE_COUNT_IS_FROZEN);
    assert!(!count.try_increase());
    assert!(!count.try_freeze());
    count.unfreeze();
    assert_eq!(count.load(), REFERENCE_COUNT_NO_REFERENCE);
    assert!(count.try_increase());
    count.decrease();
}

#[test]
fn reference_count_freeze_rejects_active_references() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:102-120 and
    // pkg/sessionctx/variable/session.go:2070-2081.
    let count = ReferenceCount::from_value(2);
    assert!(!count.try_freeze());
    assert_eq!(count.load(), 2);
    count.decrease();
    assert!(!count.try_freeze());
    count.decrease();
    assert!(count.try_freeze());
    count.unfreeze();
}
