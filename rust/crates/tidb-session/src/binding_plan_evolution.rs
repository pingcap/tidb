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

//! Go `pkg/bindinfo`, covering the single file `binding_plan_evolution.go`.
//!
//! LABELING: a **COMPLETE port of one file**, and therefore still a **SEED for
//! the package** -- `pkg/bindinfo` also holds `binding.go`,
//! `binding_handle.go`, `binding_auto.go`, `binding_match.go` and more, and
//! this module claims exactly one of them alongside [`crate::binding`],
//! [`crate::binding_cache`] and [`crate::binding_utils`].
//!
//! The file itself is partial UPSTREAM: `llmBasedPlanPerfPredictor`
//! (`binding_plan_evolution.go` lines 100-109) is a `// TODO: implement this`
//! stub in Go that only allocates zeroed outputs. It comes across as the same
//! stub -- [`LlmBasedPlanPerfPredictor`] -- so the Rust side is not less
//! complete than its source, just as empty.
//!
//! # What this file is
//!
//! When binding plan evolution has several candidate plans for one statement,
//! something has to pick a winner. This file is the picker: a
//! [`PlanPerfPredictor`] scores candidates and explains itself, and the only
//! working implementation is [`RuleBasedPlanPerfPredictor`], three rules
//! applied in order (`binding_plan_evolution.go` lines 27-98):
//!
//! 1. any simple `PointGet` / `BatchPointGet` plan wins outright;
//! 2. otherwise the best `scan_rows_per_returned_row` wins if it is more than
//!    twice as good as the runner-up;
//! 3. otherwise the leader wins only if its latency, scan rows AND
//!    latency-per-returned-row are each at least twice as good as EVERY other
//!    candidate's.
//!
//! If no rule fires every score stays 0, meaning "no plan is recommended".
//!
//! ## The index quirk, preserved
//!
//! Go sorts `plans` IN PLACE before rules 2 and 3, but keeps writing into
//! `scores[0]` / `explanations[0]`. So rule 1's indices are ORIGINAL-order
//! indices while rules 2 and 3 write slot 0, which after the sort names the
//! best-sorted plan rather than the caller's first plan. That is not tidied up
//! here: [`RuleBasedPlanPerfPredictor::perf_predicate`] returns scores indexed
//! exactly the way Go's are, which is what `TestRuleBasedPlanPerfPredictor`
//! pins.
//!
//! # Boundaries
//!
//! * `// boundary:` `pkg/bindinfo/binding_auto.go` `BindingPlanInfo`
//!   (lines 50-65) -- the real struct embeds `*Binding` and carries
//!   `AvgReturnedRows`, `Recommend` and `Reason` on top of the statistics.
//!   The predictor reads only `Plan`, `ExecTimes`, `AvgLatency`,
//!   `AvgScanRows`, `LatencyPerReturnRow` and `ScanRowsPerReturnRow`, so
//!   [`BindingPlanInfo`] here carries exactly those six fields. The embedded
//!   binding and the recommendation fields belong to `binding_auto.go`, which
//!   is SQL-execution bound and out of scope.
//! * `// boundary:` `pkg/bindinfo/binding_auto.go` `IsSimplePointPlan`
//!   (lines 298-322) -- ported alongside as [`is_simple_point_plan`] because
//!   it is a pure string-parsing leaf with no execution dependency. Nothing
//!   else from `binding_auto.go` (`bindingAuto`, `ExplorePlansForSQL`,
//!   `planGenerator`, the `StmtStats` readers) is ported.
//! * `// boundary:` Go's `PlanPerfPredictor` interface returns
//!   `(scores, explanations, err)` and neither implementation can fail. The
//!   Rust trait keeps the error slot as
//!   [`Result`]`<`[`Prediction`]`, `[`DriverError`]`>` -- the same error type
//!   [`crate::binding_cache`] uses -- so a future LLM-backed predictor has
//!   somewhere to report failure.
//!
//! # Narrowings
//!
//! * `sort.Slice` (line 66) is an UNSTABLE sort driven by a comparator that is
//!   not a strict weak ordering: on equal `ScanRowsPerReturnRow` it reports
//!   "less" only when all three of latency, scan rows and latency-per-row are
//!   smaller, so two such plans can compare "not less" in both directions
//!   while a third orders them. Handing that to Rust's `sort_by` risks a
//!   total-order panic, so the ordering is reproduced with an explicit
//!   insertion sort over Go's own `less` predicate: deterministic, and
//!   identical to Go wherever the comparator is consistent.
//! * The sort mutates the caller's slice in Go. Here the input is borrowed
//!   immutably and the sort runs over a vector of references, so the caller's
//!   order is untouched. Only the returned scores/explanations are observable
//!   in the test, and those are unaffected.

use tidb_executor::DriverError;

/// The scores and explanations a predictor produced, one entry per candidate.
///
/// Go returns two parallel slices; keeping them in one struct makes the
/// "same length as `plans`" invariant impossible to break.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Prediction {
    /// Score per plan. A score of 1 marks the recommended plan; all-zero means
    /// no plan is recommended.
    pub scores: Vec<f64>,
    /// Human-readable justification for a non-zero score, empty otherwise.
    pub explanations: Vec<String>,
}

/// The subset of `BindingPlanInfo` the performance predictor reads.
///
/// `// boundary:` `pkg/bindinfo/binding_auto.go` lines 50-65 -- see the module
/// header for what was left behind.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BindingPlanInfo {
    /// The `EXPLAIN ANALYZE`-shaped plan text, parsed by
    /// [`is_simple_point_plan`].
    pub plan: String,
    /// How many times this plan ran. Zero anywhere means "no execution info",
    /// which disables rules 2 and 3 entirely.
    pub exec_times: i64,
    /// Average statement latency.
    pub avg_latency: f64,
    /// Average number of rows scanned.
    pub avg_scan_rows: f64,
    /// Latency divided by returned rows.
    pub latency_per_return_row: f64,
    /// Scanned rows divided by returned rows.
    pub scan_rows_per_return_row: f64,
}

/// Scores plan candidates and explains the scores.
///
/// Go's `PlanPerfPredictor` (`binding_plan_evolution.go` lines 21-25). All
/// scores zero means no plan is recommended.
pub trait PlanPerfPredictor {
    /// Score `plans`, returning one score and one explanation per plan.
    fn perf_predicate(&self, plans: &[BindingPlanInfo]) -> Result<Prediction, DriverError>;
}

/// Scores plans according to a fixed set of rules.
///
/// Go's `ruleBasedPlanPerfPredictor` (`binding_plan_evolution.go` lines
/// 27-98). If any plan hits any rule, its score is 1 and every other score
/// stays 0.
///
/// Rules:
/// 1. If any plan is a simple `PointGet` or `BatchPointGet`, recommend it.
/// 2. If `scan_rows_per_return_row` of a plan is 50% better than the others',
///    recommend it.
/// 3. If latency, scan rows and latency-per-returned-row of a plan are all 50%
///    better than every other plan's, recommend it.
#[derive(Debug, Clone, Copy, Default)]
pub struct RuleBasedPlanPerfPredictor;

impl PlanPerfPredictor for RuleBasedPlanPerfPredictor {
    fn perf_predicate(&self, plans: &[BindingPlanInfo]) -> Result<Prediction, DriverError> {
        let mut out = Prediction {
            scores: vec![0.0; plans.len()],
            explanations: vec![String::new(); plans.len()],
        };

        if plans.is_empty() {
            return Ok(out);
        }
        if plans.len() == 1 {
            out.scores[0] = 1.0;
            return Ok(out);
        }

        // rule 1 -- indexed in the caller's original order, since Go runs this
        // loop before the sort below.
        for (i, cur) in plans.iter().enumerate() {
            if is_simple_point_plan(&cur.plan) {
                out.scores[i] = 1.0;
                out.explanations[i] =
                    "Simple PointGet or BatchPointGet is the best plan".to_owned();
                return Ok(out);
            }
        }

        // Without execution info there is nothing for rules 2 and 3 to compare.
        if plans.iter().any(|p| p.exec_times == 0) {
            return Ok(out);
        }

        // Sort for rules 2 and 3: only the leading plan can be a candidate.
        let mut sorted: Vec<&BindingPlanInfo> = plans.iter().collect();
        insertion_sort_by(&mut sorted, |a, b| plan_less(a, b));

        // rule 2
        if sorted[0].scan_rows_per_return_row < sorted[1].scan_rows_per_return_row / 2.0 {
            out.scores[0] = 1.0;
            out.explanations[0] =
                "Plan's scan_rows_per_returned_row is 50% better than others'".to_owned();
            return Ok(out);
        }

        // rule 3
        for i in 1..sorted.len() {
            let hit_rule3 = sorted[0].avg_latency <= sorted[i].avg_latency / 2.0
                && sorted[0].avg_scan_rows <= sorted[i].avg_scan_rows / 2.0
                && sorted[0].latency_per_return_row <= sorted[i].latency_per_return_row / 2.0;
            if !hit_rule3 {
                break;
            }
            if i == sorted.len() - 1 {
                // the last one
                out.scores[0] = 1.0;
                out.explanations[0] =
                    "Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'"
                        .to_owned();
                return Ok(out);
            }
        }

        Ok(out)
    }
}

/// Go's `sort.Slice` comparator from `binding_plan_evolution.go` lines 66-73.
///
/// Note the tie branch: when `scan_rows_per_return_row` matches, a plan counts
/// as "less" only if it wins on latency AND scan rows AND latency-per-row, so
/// neither of two tied plans need be less than the other.
fn plan_less(a: &BindingPlanInfo, b: &BindingPlanInfo) -> bool {
    if a.scan_rows_per_return_row == b.scan_rows_per_return_row {
        return a.avg_latency < b.avg_latency
            && a.avg_scan_rows < b.avg_scan_rows
            && a.latency_per_return_row < b.latency_per_return_row;
    }
    a.scan_rows_per_return_row < b.scan_rows_per_return_row
}

/// Stable insertion sort over a Go-style `less` predicate.
///
/// See the module header: Go's comparator is not a strict weak ordering, so
/// this stands in for `sort.Slice` rather than delegating to `sort_by`.
fn insertion_sort_by<T: Copy>(items: &mut [T], less: impl Fn(&T, &T) -> bool) {
    for i in 1..items.len() {
        let mut j = i;
        while j > 0 && less(&items[j], &items[j - 1]) {
            items.swap(j, j - 1);
            j -= 1;
        }
    }
}

/// Leverages an LLM to score plans.
///
/// Go's `llmBasedPlanPerfPredictor` (`binding_plan_evolution.go` lines
/// 100-109) is a `// TODO: implement this` stub returning zeroed slices; so is
/// this.
#[derive(Debug, Clone, Copy, Default)]
pub struct LlmBasedPlanPerfPredictor;

impl PlanPerfPredictor for LlmBasedPlanPerfPredictor {
    fn perf_predicate(&self, plans: &[BindingPlanInfo]) -> Result<Prediction, DriverError> {
        // TODO: implement this
        Ok(Prediction {
            scores: vec![0.0; plans.len()],
            explanations: vec![String::new(); plans.len()],
        })
    }
}

/// Checks whether the plan is a simple point plan.
///
/// `// boundary:` `pkg/bindinfo/binding_auto.go` lines 298-322, ported here
/// because it is the one leaf of that file the predictor needs.
///
/// A plan qualifies when every non-blank line's leading operator is the `id`
/// header or one of `Point_Get`, `Batch_Point_Get`, `Selection`,
/// `Projection`. An empty plan never qualifies.
pub fn is_simple_point_plan(plan: &str) -> bool {
    let mut empty = true;
    // If the plan only contains Point_Get, Batch_Point_Get, Selection and
    // Projection, it's a simple point plan.
    for line in plan.split('\n') {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        empty = false;
        let operator_name = line.split(' ').next().unwrap_or("");
        // TODO: these hard-coding lines are a temporary implementation,
        // refactor this part later.
        if operator_name == "id" // the first line with column names
            || operator_name.contains("Point_Get")
            || operator_name.contains("Batch_Point_Get")
            || operator_name.contains("Selection")
            || operator_name.contains("Projection")
        {
            continue;
        }
        return false;
    }
    !empty
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go: pkg/bindinfo/binding_plan_evolution_test.go
    // TestRuleBasedPlanPerfPredictor (lines 23-85), ported whole with the
    // plan texts and expected values byte-exact.
    #[test]
    fn the_rules_fire_in_the_order_go_pins() {
        let point_plan = "       id  task    estRows operator info  actRows execution info  memory          disk
        Projection_4    root    1       plus(test.t.a, 1)->Column#3     0       time:173µs, open:24.9µs, close:8.92µs, loops:1, Concurrency:OFF                         380 Bytes       N/A
        └─Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A";
        let batch_point_plan = "id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Projection_4            root    3.00    plus(test.t.a, 1)->Column#3                             0       time:218.3µs, open:14.5µs, close:9.79µs, loops:1, Concurrency:OFF                                                                 145 Bytes       N/A
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   ";
        let non_point_plan = "       id                      task            estRows operator info                           actRows execution info memory          disk
        TableReader_5           root            10000   data:TableFullScan_4                    0       time:456.3µs, open:141µs, close:6.79µs, loops:1, cop_task: {num: 1, max: 241.3µs, proc_keys: 0, copr_cache_hit_ratio: 0.00, build_task_duration: 91.5µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:203.9µs}}      182 Bytes       N/A
        └─TableFullScan_4       cop[tikv]       10000   table:t, keep order:false, stats:pseudo 0       tikv_task:{time:155.2µs, loops:0}                                                                                                                                                                                                         N/A             N/A ";

        // Test rule 1
        let mut p1 = BindingPlanInfo {
            plan: non_point_plan.to_owned(),
            avg_latency: 100.0,
            exec_times: 100,
            avg_scan_rows: 100.0,
            latency_per_return_row: 100.0,
            scan_rows_per_return_row: 100.0,
        };
        let mut p2 = BindingPlanInfo {
            plan: point_plan.to_owned(),
            avg_latency: 100.0,
            exec_times: 100,
            avg_scan_rows: 100.0,
            latency_per_return_row: 100.0,
            scan_rows_per_return_row: 100.0,
        };

        let p = RuleBasedPlanPerfPredictor;

        let got = p
            .perf_predicate(&[p1.clone(), p2.clone()])
            .expect("rule-based prediction never fails");
        assert_eq!(got.scores, vec![0.0, 1.0]);
        assert_eq!(
            got.explanations,
            vec!["", "Simple PointGet or BatchPointGet is the best plan"]
        );

        p1.plan = batch_point_plan.to_owned();
        p2.plan = non_point_plan.to_owned();
        let got = p.perf_predicate(&[p1.clone(), p2.clone()]).unwrap();
        assert_eq!(got.scores, vec![1.0, 0.0]);
        assert_eq!(
            got.explanations,
            vec!["Simple PointGet or BatchPointGet is the best plan", ""]
        );

        // Test rule 2
        p1.plan = non_point_plan.to_owned();
        p2.plan = non_point_plan.to_owned();
        p2.scan_rows_per_return_row = 30.0;
        let got = p.perf_predicate(&[p1.clone(), p2.clone()]).unwrap();
        assert_eq!(got.scores, vec![1.0, 0.0]);
        assert_eq!(
            got.explanations,
            vec![
                "Plan's scan_rows_per_returned_row is 50% better than others'",
                ""
            ]
        );
        p2.scan_rows_per_return_row = 100.0;

        // Test rule 3
        p1.avg_latency = 30.0;
        p1.avg_scan_rows = 30.0;
        p1.latency_per_return_row = 30.0;
        let got = p.perf_predicate(&[p1.clone(), p2.clone()]).unwrap();
        assert_eq!(got.scores, vec![1.0, 0.0]);
        assert_eq!(
            got.explanations,
            vec![
                "Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'",
                ""
            ]
        );

        p1.avg_latency = 60.0;
        let got = p.perf_predicate(&[p1, p2]).unwrap();
        assert_eq!(got.scores, vec![0.0, 0.0]); // no recommendation
    }

    // Not from Go: the early-return arms Go's table never reaches, kept honest
    // against `binding_plan_evolution.go` lines 40-46 and 58-62.
    #[test]
    fn the_degenerate_inputs_short_circuit() {
        let p = RuleBasedPlanPerfPredictor;
        let got = p.perf_predicate(&[]).unwrap();
        assert!(got.scores.is_empty() && got.explanations.is_empty());

        let one = BindingPlanInfo::default();
        let got = p.perf_predicate(std::slice::from_ref(&one)).unwrap();
        assert_eq!(got.scores, vec![1.0]);
        assert_eq!(got.explanations, vec![""]);

        // Two plans with no execution info: rules 2 and 3 are skipped.
        let got = p.perf_predicate(&[one.clone(), one]).unwrap();
        assert_eq!(got.scores, vec![0.0, 0.0]);
    }

    // Not from Go: the LLM predictor is a stub upstream, so this only pins the
    // shape of its zeroed output.
    #[test]
    fn the_llm_predictor_scores_nothing_yet() {
        let plans = vec![BindingPlanInfo::default(); 3];
        let got = LlmBasedPlanPerfPredictor.perf_predicate(&plans).unwrap();
        assert_eq!(got.scores, vec![0.0; 3]);
        assert_eq!(got.explanations, vec![String::new(); 3]);
    }
}
