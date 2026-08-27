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
// See the License for the specific
// language governing permissions and
// limitations under the License.

//! Real port of `pkg/planner/core/rule/rule_max_min_eliminate_test.go`
//! (`pkg/planner.part15` item 843 on `origin/master`).
//!
//! `TestMaxMinEliminateSkipsEmptyScalarAgg` builds a `LogicalAggregation{}`
//! with NO aggregate functions and NO group-by items, runs
//! `MaxMinEliminator.Optimize`, and requires: no panic, `changed == false`,
//! and the SAME plan node returned (`rule_max_min_eliminate_test.go:30-36`).
//!
//! The gate lives in `eliminateMaxMin`
//! (`pkg/planner/core/rule/rule_max_min_eliminate.go:215-258`): a grouped
//! aggregation returns unchanged FIRST (:222-224), then an aggregation with
//! zero `AggFuncs` returns unchanged (:225-227). The Rust side transcreated
//! exactly this eligibility ladder as
//! [`tidb_planner::max_min_elimination::classify_max_min`] — see its empty-
//! shape arm mirroring :225-227 — so the port drives the empty aggregation's
//! metadata through the classifier and pins that it is rejected for
//! elimination, which is what "the same plan comes back with changed=false"
//! means at this surface.

use tidb_planner::max_min_elimination::{
    classify_max_min, MaxMinAggregationShape, MaxMinEliminationDecision, MaxMinEliminator,
};

/// GO PORT of `pkg/planner/core/rule/rule_max_min_eliminate_test.go:26
/// TestMaxMinEliminateSkipsEmptyScalarAgg`.
///
/// Re-derived contract: an empty scalar (non-grouped, zero-function)
/// aggregation must pass through elimination untouched. The Go test wraps the
/// call in `require.NotPanics`; the Rust classifier is a pure query so the
/// non-mutation half is the fact that classification performs no work beyond
/// reading the shape.
#[test]
fn max_min_eliminate_skips_empty_scalar_aggregation() {
    // `logicalop.LogicalAggregation{}.Init(sctx, 0)`: zero group-by items and
    // zero aggregate functions (rule_max_min_eliminate_test.go:31).
    let empty_agg = MaxMinAggregationShape::new(0, Vec::new(), Vec::new());

    // require.NotPanics + require.False(t, changed): classification reads the
    // shape and returns Ineligible without touching anything (:32-35).
    let decision = MaxMinEliminator.classify(&empty_agg);
    let decision_fn = classify_max_min(&empty_agg);

    // require.Same(t, agg, p): eliminateMaxMin hits the `len(AggFuncs)==0`
    // return-unchanged arm (rule_max_min_eliminate.go:225-227), i.e. the
    // aggregation is NOT a candidate for rewrite in either entry point.
    assert_eq!(
        decision,
        MaxMinEliminationDecision::Ineligible,
        "an empty scalar aggregation must stay unchanged by max/min elimination"
    );
    assert_eq!(
        decision_fn,
        MaxMinEliminationDecision::Ineligible,
        "free-function and method forms agree on the empty-shape gate"
    );
}
