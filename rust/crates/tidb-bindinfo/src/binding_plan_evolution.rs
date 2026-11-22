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

//! Go `pkg/bindinfo/binding_plan_evolution.go`: the predictors that score
//! `EXPLAIN EXPLORE`'s plan candidates.

use tidb_driver_error::DriverError;

use crate::binding_auto::{is_simple_point_plan, BindingPlanInfo};

/// Go `PlanPerfPredictor`: scores the plan candidates and explains them; all
/// scores 0 means no plan is recommended.
pub trait PlanPerfPredictor {
    /// Go `PerfPredicate`. It may reorder `plans`, as Go's sorts the slice it
    /// is given.
    ///
    /// # Errors
    ///
    /// The predictor's failure.
    fn perf_predicate(
        &self,
        plans: &mut [BindingPlanInfo],
    ) -> Result<(Vec<f64>, Vec<String>), DriverError>;
}

/// Go `ruleBasedPlanPerfPredictor`: a plan hitting a rule scores 1, all
/// others 0.
/// 1. A simple PointGet or BatchPointGet plan is recommended.
/// 2. A plan whose `ScanRowsPerReturnedRow` is 50% better than the others'.
/// 3. A plan whose `Latency`, `ScanRows` and `LatencyPerReturnRow` are 50%
///    better than the others'.
pub struct RuleBasedPlanPerfPredictor;

impl PlanPerfPredictor for RuleBasedPlanPerfPredictor {
    fn perf_predicate(
        &self,
        plans: &mut [BindingPlanInfo],
    ) -> Result<(Vec<f64>, Vec<String>), DriverError> {
        let mut scores = vec![0.0; plans.len()];
        let mut explanations = vec![String::new(); plans.len()];
        if plans.is_empty() {
            return Ok((scores, explanations));
        }
        if plans.len() == 1 {
            scores[0] = 1.0;
            return Ok((scores, explanations));
        }

        // Rule 1.
        for (i, current) in plans.iter().enumerate() {
            if is_simple_point_plan(&current.plan) {
                scores[i] = 1.0;
                explanations[i] = "Simple PointGet or BatchPointGet is the best plan".to_owned();
                return Ok((scores, explanations));
            }
        }

        if plans.iter().any(|plan| plan.exec_times == 0) {
            // No execution info.
            return Ok((scores, explanations));
        }

        // Sort for rules 2 and 3: only the first binding can be their
        // candidate. Go `sort.Slice` with this less function.
        crate::binding_plan_evolution::go_sort_slice(plans, |left, right| {
            if left.scan_rows_per_return_row == right.scan_rows_per_return_row {
                return left.avg_latency < right.avg_latency
                    && left.avg_scan_rows < right.avg_scan_rows
                    && left.latency_per_return_row < right.latency_per_return_row;
            }
            left.scan_rows_per_return_row < right.scan_rows_per_return_row
        });

        // Rule 2.
        if plans[0].scan_rows_per_return_row < plans[1].scan_rows_per_return_row / 2.0 {
            scores[0] = 1.0;
            explanations[0] = "Plan's scan_rows_per_returned_row is 50% better than others'".to_owned();
            return Ok((scores, explanations));
        }

        // Rule 3.
        for i in 1..plans.len() {
            let hit_rule3 = plans[0].avg_latency <= plans[i].avg_latency / 2.0
                && plans[0].avg_scan_rows <= plans[i].avg_scan_rows / 2.0
                && plans[0].latency_per_return_row <= plans[i].latency_per_return_row / 2.0;
            if !hit_rule3 {
                break;
            }
            if i == plans.len() - 1 {
                scores[0] = 1.0;
                explanations[0] =
                    "Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'"
                        .to_owned();
                return Ok((scores, explanations));
            }
        }
        Ok((scores, explanations))
    }
}

/// Go `llmBasedPlanPerfPredictor`: scores nothing yet (Go's TODO).
pub struct LlmBasedPlanPerfPredictor;

impl PlanPerfPredictor for LlmBasedPlanPerfPredictor {
    fn perf_predicate(
        &self,
        plans: &mut [BindingPlanInfo],
    ) -> Result<(Vec<f64>, Vec<String>), DriverError> {
        Ok((vec![0.0; plans.len()], vec![String::new(); plans.len()]))
    }
}

/// Go `sort.Slice(x, less)`: the unstable pdqsort, whose order among equal
/// elements this reproduces.
pub(crate) fn go_sort_slice<T>(items: &mut [T], mut less: impl FnMut(&T, &T) -> bool) {
    tidb_goruntime::go_pdqsort::go_sort_func_by(items, |left, right| {
        if less(left, right) {
            std::cmp::Ordering::Less
        } else if less(right, left) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}

/// Go `binding_plan_evolution_test.go`.
#[cfg(test)]
mod tests {
    use super::*;

    const POINT_PLAN: &str = "       id  task    estRows operator info  actRows execution info  memory          disk
        Projection_4    root    1       plus(test.t.a, 1)->Column#3     0       time:173µs, open:24.9µs, close:8.92µs, loops:1, Concurrency:OFF                         380 Bytes       N/A
        └─Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A";
    const BATCH_POINT_PLAN: &str = "id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Projection_4            root    3.00    plus(test.t.a, 1)->Column#3                             0       time:218.3µs, open:14.5µs, close:9.79µs, loops:1, Concurrency:OFF                                                                 145 Bytes       N/A
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   ";
    const NON_POINT_PLAN: &str = "       id                      task            estRows operator info                           actRows execution info memory          disk
        TableReader_5           root            10000   data:TableFullScan_4                    0       time:456.3µs, open:141µs, close:6.79µs, loops:1, cop_task: {num: 1, max: 241.3µs, proc_keys: 0, copr_cache_hit_ratio: 0.00, build_task_duration: 91.5µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:203.9µs}}      182 Bytes       N/A
        └─TableFullScan_4       cop[tikv]       10000   table:t, keep order:false, stats:pseudo 0       tikv_task:{time:155.2µs, loops:0}                                                                                                                                                                                                         N/A             N/A ";

    fn plan(text: &str) -> BindingPlanInfo {
        BindingPlanInfo {
            plan: text.to_owned(),
            avg_latency: 100.0,
            exec_times: 100,
            avg_scan_rows: 100.0,
            avg_returned_rows: 100.0,
            latency_per_return_row: 100.0,
            scan_rows_per_return_row: 100.0,
            ..BindingPlanInfo::default()
        }
    }

    /// Go passes fresh slices of the same two pointers, so a sort inside the
    /// predictor does not carry over to the next call.
    fn predict(p1: &BindingPlanInfo, p2: &BindingPlanInfo) -> (Vec<f64>, Vec<String>) {
        RuleBasedPlanPerfPredictor
            .perf_predicate(&mut [p1.clone(), p2.clone()])
            .unwrap()
    }

    #[test]
    fn test_rule_based_plan_perf_predictor() {
        // Rule 1.
        let mut p1 = plan(NON_POINT_PLAN);
        let mut p2 = plan(POINT_PLAN);
        let (scores, explanations) = predict(&p1, &p2);
        assert_eq!(scores, vec![0.0, 1.0]);
        assert_eq!(explanations, vec!["".to_owned(), "Simple PointGet or BatchPointGet is the best plan".to_owned()]);

        p1.plan = BATCH_POINT_PLAN.to_owned();
        p2.plan = NON_POINT_PLAN.to_owned();
        let (scores, explanations) = predict(&p1, &p2);
        assert_eq!(scores, vec![1.0, 0.0]);
        assert_eq!(explanations, vec!["Simple PointGet or BatchPointGet is the best plan".to_owned(), "".to_owned()]);

        // Rule 2.
        p1.plan = NON_POINT_PLAN.to_owned();
        p2.plan = NON_POINT_PLAN.to_owned();
        p2.scan_rows_per_return_row = 30.0;
        let (scores, explanations) = predict(&p1, &p2);
        assert_eq!(scores, vec![1.0, 0.0]);
        assert_eq!(explanations, vec!["Plan's scan_rows_per_returned_row is 50% better than others'".to_owned(), "".to_owned()]);
        p2.scan_rows_per_return_row = 100.0;

        // Rule 3.
        p1.avg_latency = 30.0;
        p1.avg_scan_rows = 30.0;
        p1.latency_per_return_row = 30.0;
        let (scores, explanations) = predict(&p1, &p2);
        assert_eq!(scores, vec![1.0, 0.0]);
        assert_eq!(
            explanations,
            vec!["Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'".to_owned(), "".to_owned()]
        );

        p1.avg_latency = 60.0;
        let (scores, _) = predict(&p1, &p2);
        assert_eq!(scores, vec![0.0, 0.0]); // no recommendation
    }
}
