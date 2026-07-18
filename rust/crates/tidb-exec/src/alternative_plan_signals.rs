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

//! Alternative logical-plan round signals from `stmtctx.go`.
//!
//! The optimizer records these statement-local booleans while constructing
//! candidate plans. This leaf owns the source mark-to-true transitions and
//! the reset boundary for all eight signals. It does not decide whether an
//! alternative round is enabled, run planner rules, inspect SQL, trigger
//! failpoints, compare costs, or attach to a live `StatementContext`.

/// Statement-local signals shared by alternative logical-plan rounds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AlternativePlanSignals {
    /// At least one Apply was decorrelated into a Join.
    pub decorrelated_apply: bool,
    /// The first round produced a same-order index-join candidate.
    pub same_order_index_join: bool,
    /// A round produced an order-aware join-reorder candidate.
    pub order_aware_join_reorder: bool,
    /// FTS MATCH predicates should use the LIKE-style fallback mode.
    pub fts_like_fallback: bool,
    /// A direct-boolean-context MATCH predicate was seen.
    pub has_predicate_context_match: bool,
    /// A non-correlated IN subquery can try the correlate-to-Apply round.
    pub prefer_correlate: bool,
    /// A semi join can try an additional SEMI_JOIN_REWRITE round.
    pub semi_join_rewrite: bool,
    /// The statement uses `FTS_MATCH_WORD`.
    pub fts_function_is_used: bool,
}

impl AlternativePlanSignals {
    /// Clears every source alternative-plan signal for a new statement round.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Records a decorrelated Apply/Join candidate.
    pub fn mark_decorrelated_apply(&mut self) {
        self.decorrelated_apply = true;
    }

    /// Records a same-order index-join candidate.
    pub fn mark_same_order_index_join(&mut self) {
        self.same_order_index_join = true;
    }

    /// Records an order-aware join-reorder candidate.
    pub fn mark_order_aware_join_reorder(&mut self) {
        self.order_aware_join_reorder = true;
    }

    /// Records a correlate-to-Apply candidate.
    pub fn mark_prefer_correlate(&mut self) {
        self.prefer_correlate = true;
    }

    /// Records a semi-join-rewrite candidate.
    pub fn mark_semi_join_rewrite(&mut self) {
        self.semi_join_rewrite = true;
    }
}
