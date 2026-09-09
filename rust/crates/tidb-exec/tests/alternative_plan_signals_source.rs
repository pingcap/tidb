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

//! Source-backed tests for alternative logical-plan signal state.

use tidb_exec::alternative_plan_signals::AlternativePlanSignals;

#[test]
fn alternative_plan_signal_marks_match_source_transitions() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:697-725 and
    // pkg/planner/core/casetest/correlated/correlated_test.go:106-164.
    let mut signals = AlternativePlanSignals::default();
    signals.mark_decorrelated_apply();
    assert!(signals.decorrelated_apply);
    signals.mark_same_order_index_join();
    assert!(signals.same_order_index_join);
    signals.mark_order_aware_join_reorder();
    assert!(signals.order_aware_join_reorder);
    signals.mark_prefer_correlate();
    assert!(signals.prefer_correlate);
    signals.mark_semi_join_rewrite();
    assert!(signals.semi_join_rewrite);
    signals.mark_mixed_storage_engines();
    assert!(signals.mixed_storage_engines);
    signals.mark_missing_tiflash_path();
    assert!(signals.missing_tiflash_path);
    signals.mark_store_type_hint();
    assert!(signals.has_store_type_hint);
}

#[test]
fn alternative_plan_signal_reset_clears_all_eleven_source_fields() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:475-535,701-730.
    let mut signals = AlternativePlanSignals {
        decorrelated_apply: true,
        same_order_index_join: true,
        order_aware_join_reorder: true,
        fts_like_fallback: true,
        has_predicate_context_match: true,
        prefer_correlate: true,
        semi_join_rewrite: true,
        fts_function_is_used: true,
        mixed_storage_engines: true,
        missing_tiflash_path: true,
        has_store_type_hint: true,
    };
    signals.reset();
    assert_eq!(signals, AlternativePlanSignals::default());
}
