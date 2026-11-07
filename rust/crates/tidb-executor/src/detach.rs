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

//! Go `pkg/executor/detach.go`: whether an executor tree can keep running
//! after its session context moves on to another statement.
//!
//! Go asks each executor of the tree (`exec.Executor.Detach`), and the tree is
//! the one `executorBuilder.build` makes, one executor per root physical plan
//! node. This tier's builder lowers a reader's pushed-down plan into local
//! operators and folds Go's per-executor base behavior (killer checks,
//! runtime stats, index-usage reporting) into wrapper operators, so its
//! operator tree no longer has Go's shape. The answer is therefore computed
//! over the physical plan the tree was built from, where Go's per-executor
//! rules apply one-to-one:
//!
//! * `TableReaderExecutor`, `IndexReaderExecutor` and `IndexLookUpExecutor`
//!   detach (their contexts become static, `tableReaderExecutorContext.
//!   Detach` and friends); a reader has no child executor.
//! * `ProjectionExec` detaches when its `evaluatorSuit` requires no optional
//!   evaluation property, `SelectionExec` when none of its filters does.
//! * Every other executor keeps `BaseExecutorV2.Detach`'s `false`.
//!
//! Every operator this tier builds owns its statement state (the statement
//! context, memory trackers and snapshot handles are per statement, not read
//! back from the session), which is what the context conversion of Go's
//! `Detach` methods achieves; a `true` here hands the whole operator tree to
//! the detached record set unchanged.

use tidb_expr::evaluator::get_optional_eval_props_for_expr;
use tidb_planner_core_operator_physicalop::PhysicalPlan;

/// Go `Detach(originalExecutor)`: the executor and, recursively, every child
/// of it can be detached from the session context.
#[must_use]
pub fn detach(plan: &PhysicalPlan) -> bool {
    if !detach_executor(plan) {
        return false;
    }
    plan.children().iter().all(detach)
}

/// The `Detach` method of the executor Go's `executorBuilder.build` makes for
/// `plan`.
fn detach_executor(plan: &PhysicalPlan) -> bool {
    match plan {
        // Go `buildTableReader` builds an `MPPGather` instead when the
        // pushed plan is an exchange (`useMPPExecution`), and a
        // `TableDualExec` when dynamic partition pruning leaves no partition
        // (`len(partitions) == 0`); neither detaches.
        PhysicalPlan::TableReader(reader) => {
            !matches!(reader.table_plan.as_deref(), Some(PhysicalPlan::ExchangeSender(_)))
                && !reader.table_plan.as_deref().is_some_and(prunes_to_no_partition)
        }
        // Go `IndexReaderExecutor.Detach`, `IndexLookUpExecutor.Detach`.
        PhysicalPlan::IndexReader(_) | PhysicalPlan::IndexLookUpReader(_) => true,
        // Go `ProjectionExec.Detach`: "if it requires any optional property,
        // it should return false".
        PhysicalPlan::Projection(projection) => projection
            .exprs
            .iter()
            .all(|expr| get_optional_eval_props_for_expr(expr).is_empty()),
        // Go `SelectionExec.Detach`.
        PhysicalPlan::Selection(selection) => selection
            .conditions
            .iter()
            .all(|expr| get_optional_eval_props_for_expr(expr).is_empty()),
        // Go `BaseExecutorV2.Detach`.
        _ => false,
    }
}

/// Go `buildTableReader`'s `partitionPruning` over a dynamically pruned
/// partitioned table answering no partition: the reader's table scan reads a
/// restricted partition set that is empty.
fn prunes_to_no_partition(table_plan: &PhysicalPlan) -> bool {
    match table_plan {
        PhysicalPlan::TableScan(scan) => scan
            .dynamic_partition_access
            .as_ref()
            .is_some_and(|access| !access.all_partitions && access.partitions.is_empty()),
        other => other.children().iter().any(prunes_to_no_partition),
    }
}
