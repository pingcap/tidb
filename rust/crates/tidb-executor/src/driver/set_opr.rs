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

//! Set operations: `UNION`, `EXCEPT` and `INTERSECT`, Go's `buildSetOpr` over
//! materialized rows.
//!
//! # DOCUMENTED DIVERGENCE from Go's planner -- do not harvest
//!
//! This is a ROW FOLD over materialized terms, not Go's operator tree: Go
//! builds LogicalUnionAll/semi-join plans and never materializes. The plan
//! path's set operations are `tidb-planner`'s `plan_builder/set_opr.rs`,
//! built from `buildSetOpr` directly -- never from this file. This module
//! dies with the driver.
//!
//! Go plans the terms left to right and folds each into the accumulated
//! result, which is what [`run_set_opr_stmt`] does. The distinct forms
//! deduplicate and the `ALL` forms keep multiplicity, so the fold is stated
//! once over a row KEY -- the same codec encoding used for grouping elsewhere
//! ([`row_key`]) -- rather than per operator.
//!
//! A statement-level `ORDER BY`/`LIMIT` applies to the whole result rather
//! than to the last term, and its items name OUTPUT columns rather than any
//! term's source columns, which is why [`sort_rows_by_output`] sorts rows
//! instead of reshaping a plan.
//!
//! Row order is unspecified for the deduplicating forms -- TiDB returns them
//! in hash order -- so only `UNION ALL` and an explicit `ORDER BY` have an
//! order worth relying on.
//!
//! The OUTPUT column types are a property of every term, not of the first:
//! Go's `buildUnion` folds them pairwise through `unionJoinFieldType` and then
//! casts each branch to the merged type, so an INT branch united with a
//! DECIMAL one reads `1.0`. [`union_join_field_type`] is that merge and
//! [`cast_rows_to_columns`] is that cast.
//!
//! DEFERRED (documented): pushing the work into executors instead of
//! materializing each term.

use super::*;

/// Runs a set-operation statement: `UNION`, `EXCEPT` or `INTERSECT`.
///
/// Go plans the terms left to right and folds each into the accumulated
/// result (`buildSetOpr`), which is what this does over materialized rows.
/// The distinct forms deduplicate, the `ALL` forms keep multiplicity, and a
/// statement-level `ORDER BY`/`LIMIT` applies to the whole result rather than
/// to the last term.
///
/// Row order is unspecified for the deduplicating forms -- TiDB returns them
/// in hash order -- so only `UNION ALL` and an explicit `ORDER BY` have an
/// order worth relying on.
///
/// DEFERRED (documented): pushing the work into executors instead of
/// materializing each term.
pub fn run_set_opr_stmt(
    stmt: &tidb_ast::SetOprStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    super::run_physical_set_opr_stmt(stmt, catalog, current_db, ctx)
}

