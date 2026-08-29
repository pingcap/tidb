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
use tidb_chunk::chunk::Chunk;

/// Runtime executor for the common direct `UNION ALL` shape.
///
/// The ordinary set-operation path materializes every term because it also
/// owns type merging and duplicate folding.  A direct `UNION ALL` whose terms
/// already expose the same types has neither operation to perform, so Go's
/// union reader can stream the terms.  Keeping this as a separate, narrow
/// executor lets a parent global `COUNT` ask each branch for an exact count
/// without changing DISTINCT or mixed-type semantics.
pub(super) struct UnionAllExec {
    meta: ExecutorMeta,
    children: Vec<Box<dyn Executor>>,
    current: usize,
}

impl UnionAllExec {
    pub(super) fn new(meta: ExecutorMeta, children: Vec<Box<dyn Executor>>) -> Self {
        Self {
            meta,
            children,
            current: 0,
        }
    }
}

impl Executor for UnionAllExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.current = 0;
        for child in &mut self.children {
            child.open()?;
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while self.current < self.children.len() {
            self.children[self.current].next(req)?;
            if req.num_rows() > 0 {
                return Ok(());
            }
            self.current += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        for child in &mut self.children {
            child.close()?;
        }
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `preprocessor.checkSetOprSelectList`: every nested set-operation list is
/// checked, and every plain term except that list's last one must put its own
/// `ORDER BY`/`LIMIT` inside parentheses. `INTO` is incompatible with UNION
/// even when that term is parenthesized.
pub(super) fn validate_set_opr_usage(stmt: &tidb_ast::SetOprStmt) -> Result<(), DriverError> {
    for (index, term) in stmt.terms.iter().enumerate() {
        match &term.body {
            tidb_ast::SetOprTermBody::Nested(nested) => validate_set_opr_usage(nested)?,
            tidb_ast::SetOprTermBody::Select(select) if index + 1 < stmt.terms.len() => {
                if select.into_outfile.is_some() {
                    return Err(DriverError::WrongUsage {
                        first: "UNION",
                        second: "INTO",
                    });
                }
                if term.in_braces {
                    continue;
                }
                if select.limit.is_some() {
                    return Err(DriverError::WrongUsage {
                        first: "UNION",
                        second: "LIMIT",
                    });
                }
                if !select.order_by.is_empty() {
                    return Err(DriverError::WrongUsage {
                        first: "UNION",
                        second: "ORDER BY",
                    });
                }
            }
            tidb_ast::SetOprTermBody::Select(_) => {}
        }
    }
    Ok(())
}

/// Runs the set-operation part of Go's AST preprocessor over a complete
/// query. Go visits CTE definitions as child queries, so validating only the
/// outer `SetOprStmt` would incorrectly accept a malformed UNION inside a
/// WITH body.
pub(crate) fn validate_query_usage(query: &tidb_ast::QueryStmt) -> Result<(), DriverError> {
    let with = match query {
        tidb_ast::QueryStmt::Select(select) => select.with.as_ref(),
        tidb_ast::QueryStmt::SetOpr(set_opr) => {
            validate_set_opr_usage(set_opr)?;
            set_opr.with.as_ref()
        }
    };
    if let Some(with) = with {
        for cte in &with.ctes {
            validate_query_usage(&cte.query)?;
        }
    }
    Ok(())
}

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
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    super::run_physical_set_opr_stmt(stmt, catalog, current_db, ctx)
}

/// Plans a set operation and returns only its merged result-column metadata.
///
/// Go derives a scalar subquery's type from the query plan schema. In
/// particular, it does not execute every UNION branch merely to discover the
/// merged type. Keep that boundary separate from [`run_set_opr_stmt`], whose
/// materialized rows are required by ordinary execution.
pub(crate) fn plan_set_opr_meta_stmt(
    stmt: &tidb_ast::SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<(String, FieldType)>, DriverError> {
    validate_set_opr_usage(stmt)?;
    if stmt.with.is_some() {
        return Err(DriverError::unsupported(
            "plan-only metadata for a set operation with CTEs is not supported yet",
        ));
    }

    let mut terms = Vec::with_capacity(stmt.terms.len());
    for term in &stmt.terms {
        let columns = match &term.body {
            tidb_ast::SetOprTermBody::Select(select) => {
                plan_select_meta_stmt(select, catalog, current_db, ctx)?
            }
            tidb_ast::SetOprTermBody::Nested(nested) => {
                plan_set_opr_meta_stmt(nested, catalog, current_db, ctx)?
            }
        };
        if terms
            .first()
            .is_some_and(|first: &Vec<(String, FieldType)>| first.len() != columns.len())
        {
            return Err(DriverError::WrongNumberOfColumnsInSelect);
        }
        terms.push(columns);
    }

    let mut columns = terms
        .first()
        .cloned()
        .ok_or(DriverError::unsupported("an empty set operation"))?;
    for (index, (_, merged)) in columns.iter_mut().enumerate() {
        for term in &terms[1..] {
            *merged = union_join_field_type(merged, &term[index].1);
        }
    }
    Ok(columns)
}

/// The type one output column takes when two terms are united.
///
/// Port of Go `unionJoinFieldType`
/// (`pkg/planner/core/logical_plan_builder.go`). The merge itself is
/// `types.AggFieldType`, the same `fieldTypeMergeRules` lookup a control
/// function's branches go through; everything after it is the width, sign and
/// charset rules that decide how the merged value PRINTS.
///
/// A pure NULL branch carries no type and is skipped, so `SELECT NULL UNION
/// SELECT 1` is an integer column rather than a NULL one.
fn union_join_field_type(left: &FieldType, right: &FieldType) -> FieldType {
    if left.code() == FieldTypeCode::Null {
        return right.clone();
    }
    if right.code() == FieldTypeCode::Null {
        return left.clone();
    }
    let mut result = tidb_datatype::agg_field_type(&[left.clone(), right.clone()]);
    if result.code() == FieldTypeCode::NewDecimal {
        // A united DECIMAL is unsigned only when every branch is.
        result.and_flags(right.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED);
    } else {
        result.add_flags(left.flags() & right.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    result.set_decimal_under_limit(left.decimal().max(right.decimal()));
    // `flen - decimal` is the fraction before the point, so the widths are
    // merged on their INTEGRAL parts and the merged scale is added back.
    if left.flen() == tidb_datatype::UNSPECIFIED_LENGTH
        || right.flen() == tidb_datatype::UNSPECIFIED_LENGTH
    {
        result.set_flen_under_limit(tidb_datatype::UNSPECIFIED_LENGTH);
    } else {
        result.set_flen_under_limit(
            (left.flen() - left.decimal()).max(right.flen() - right.decimal()) + result.decimal(),
        );
    }
    // Go `types.TryToFixFlenOfDatetime`.
    if result.code() == FieldTypeCode::Datetime {
        /// Go `mysql.MaxDatetimeWidthNoFsp`.
        const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;
        let decimal = result.decimal();
        result.set_flen(MAX_DATETIME_WIDTH_NO_FSP + if decimal > 0 { decimal + 1 } else { 0 });
    }
    // A non-integer result that united an INTEGER branch is widened to the
    // full integer width, so the integer branch's digits still fit.
    if result.eval_type() != tidb_datatype::EvalType::Int
        && (left.eval_type() == tidb_datatype::EvalType::Int
            || right.eval_type() == tidb_datatype::EvalType::Int)
        && result.flen() < MAX_INT_WIDTH
        && result.flen() != tidb_datatype::UNSPECIFIED_LENGTH
    {
        result.set_flen(MAX_INT_WIDTH);
    }
    set_bin_flag_or_bin_str(right, &mut result);
    result
}

/// Go `mysql.MaxIntWidth`.
const MAX_INT_WIDTH: i64 = 20;

/// Go `expression.SetBinFlagOrBinStr` (`pkg/expression/builtin_string.go`):
/// carries an argument's binary-ness onto a result type.
fn set_bin_flag_or_bin_str(arg: &FieldType, result: &mut FieldType) {
    let non_enum_or_set = !matches!(arg.code(), FieldTypeCode::Enum | FieldTypeCode::Set);
    if arg.is_binary_string() {
        // Go `types.SetBinChsClnFlag`.
        result.set_charset_name("binary");
        result.set_collation_name("binary");
        result.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
    } else if arg.has_flag(tidb_datatype::FieldTypeFlags::BINARY)
        || (!arg.is_character_string() && non_enum_or_set)
    {
        result.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
    }
}
