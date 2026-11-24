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

//! How physical reads are lowered and what work the driver hands down to
//! them.
//!
//! The shared planner owns ordinary access-path enumeration and selection.
//! Go's statement-level `TryFastPlan` is [`super::point_get_plan`]; this
//! module retains the prepared point and SELECT plans and the mechanical
//! offers to the selected reader: [`prune_scan_columns`],
//! [`negotiate_scan_filter`], and [`offer_scan_limit`].
//!
//! The offers are not commands: [`tidb_executor_internal_exec::table_access`] holds the
//! contract, every method of it is fail-closed, and the source alone decides.
//! The order matters and is fixed here -- pruning runs before the predicate
//! split so a pushed conjunct's `column_offset` is already in narrow space,
//! and the cap is offered last, after the residual `WHERE` is known, because
//! a residual filter above the source forbids one.
//!
pub use tidb_planner::plan_cache::{PreparedSelectExecution, PreparedSelectPlan, build_prepared_select_plan};
pub use tidb_planner::plan_cache_utils::{PlanCacheStmtInfo, PreparedPlanCacheEnvironment};
#[cfg(test)]
use super::*;










#[cfg(test)]
pub(crate) fn run_prepared_select_for_test(
    execution: &PreparedSelectExecution,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let ctx = ctx.clone().with_prepared_params(execution.parameters());
    if execution.schema_version() != catalog.schema_version()
        || !execution
            .plan()
            .current_database()
            .eq_ignore_ascii_case(current_database)
    {
        return Err(DriverError::unsupported(
            "prepared SELECT test execution saw a moved schema",
        ));
    }
    execution
        .with_plan(|statement, physical| {
            let tidb_ast::Stmt::Query(query) = statement else {
                unreachable!("a prepared query owns a query statement")
            };
            super::run_query_meta_stmt_with_physical(
                query,
                Some(physical),
                catalog,
                current_database,
                &ctx,
            )
        })
        .ok_or_else(|| {
            DriverError::unsupported(
                "prepared SELECT test generation changed before executor construction",
            )
        })?
}




