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
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    // A CTE prefix belongs to the whole statement, so it is materialized once
    // and every term sees it.
    let with_catalog;
    let catalog = match &stmt.with {
        Some(with) => {
            with_catalog = materialize_ctes(with, catalog, current_db, ctx)?;
            &with_catalog
        }
        None => catalog,
    };

    // Every term is materialized BEFORE any is folded, because the output
    // column types are a property of ALL the terms: Go's `buildUnion` merges
    // them pairwise and only then builds the per-branch casts
    // (`buildProjection4Union`). Folding as each term arrived would have to
    // commit to a type before the later terms had been seen.
    let mut terms: Vec<SelectMeta> = Vec::with_capacity(stmt.terms.len());
    for term in &stmt.terms {
        let term_meta = run_set_opr_term(term, catalog, current_db, ctx)?;
        // Go raises ErrWrongNumberOfColumnsInSelect for a term whose width
        // differs.
        if let Some((first_columns, _)) = terms.first() {
            if term_meta.0.len() != first_columns.len() {
                return Err(DriverError::WrongNumberOfColumnsInSelect);
            }
        }
        terms.push(term_meta);
    }
    let (first_columns, _) = terms
        .first()
        .ok_or(DriverError::unsupported("an empty set operation"))?;

    // The merged type per output column. Names come from the FIRST term (Go
    // takes the union's schema names from it); only the TYPES are merged.
    let mut columns: Vec<(String, FieldType)> = first_columns.clone();
    for (index, (_, merged)) in columns.iter_mut().enumerate() {
        for (term_columns, _) in &terms[1..] {
            *merged = union_join_field_type(merged, &term_columns[index].1);
        }
    }

    // Each branch is then cast to the merged type, which is what makes an
    // INT branch united with a DECIMAL one read `1.0` rather than `1`.
    for (_, rows) in &mut terms {
        cast_rows_to_columns(rows, &columns);
    }

    let mut term_iter = terms.into_iter();
    let mut accumulated = term_iter.next().map(|(_, rows)| rows).unwrap_or_default();
    for (term, (_, term_rows)) in stmt.terms.iter().skip(1).zip(term_iter) {
        let Some(op) = term.op else {
            return Err(DriverError::unsupported(
                "a set-operation term after the first needs an operator",
            ));
        };
        accumulated = combine_set_opr(op, accumulated, term_rows)?;
    }

    // The statement-level ORDER BY and LIMIT apply to the folded result.
    if !stmt.order_by.is_empty() {
        sort_rows_by_output(&mut accumulated, &columns, &stmt.order_by)?;
    }
    if let Some(limit) = &stmt.limit {
        let count = eval_limit_bound(&limit.count)? as usize;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)? as usize,
            None => 0,
        };
        accumulated = accumulated.into_iter().skip(offset).take(count).collect();
    }
    Ok((columns, accumulated))
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

/// Casts every cell of every row into its output column's merged type, which
/// is Go's `buildProjection4Union` -- the cast it puts on each branch.
///
/// A cell that cannot be converted is LEFT AS IT WAS rather than turned into
/// an error: the merged type is display metadata for a value the branch
/// already produced legally, and a set operation is not the place a
/// conversion diagnostic belongs.
fn cast_rows_to_columns(rows: &mut [Vec<Datum>], columns: &[(String, FieldType)]) {
    for row in rows {
        for (value, (_, field_type)) in row.iter_mut().zip(columns) {
            if value.is_null() {
                continue;
            }
            if let Ok(converted) =
                value.convert_to(field_type, tidb_datatype::ConversionFlags::default())
            {
                *value = converted.value;
            }
        }
    }
}

/// One term of a set operation, which is a `SELECT` or a nested set operation.
fn run_set_opr_term(
    term: &tidb_ast::SetOprTerm,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    match &term.body {
        tidb_ast::SetOprTermBody::Select(select) => {
            run_select_stmt(select, catalog, current_db, ctx)
        }
        tidb_ast::SetOprTermBody::Nested(nested) => {
            run_set_opr_stmt(nested, catalog, current_db, ctx)
        }
    }
}

/// Folds one term into the accumulated rows.
pub(crate) fn combine_set_opr(
    op: tidb_ast::SetOp,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    use tidb_ast::SetOp;
    Ok(match op {
        SetOp::Union { all: true } => {
            let mut rows = left;
            rows.extend(right);
            rows
        }
        SetOp::Union { all: false } => {
            let mut rows = left;
            rows.extend(right);
            dedup_rows(rows)?
        }
        SetOp::Except { all } => {
            let mut remaining = row_counts(&right)?;
            let mut rows = Vec::new();
            for row in left {
                let key = row_key(&row)?;
                match remaining.get_mut(&key) {
                    // EXCEPT ALL removes one occurrence per matching right row.
                    Some(count) if *count > 0 && all => *count -= 1,
                    Some(count) if *count > 0 => {}
                    _ => rows.push(row),
                }
            }
            if all {
                rows
            } else {
                dedup_rows(rows)?
            }
        }
        SetOp::Intersect { all } => {
            let mut available = row_counts(&right)?;
            let mut rows = Vec::new();
            for row in left {
                let key = row_key(&row)?;
                if let Some(count) = available.get_mut(&key) {
                    if *count > 0 {
                        if all {
                            *count -= 1;
                        }
                        rows.push(row);
                    }
                }
            }
            if all {
                rows
            } else {
                dedup_rows(rows)?
            }
        }
    })
}

/// The key a row is compared by, which is the codec encoding its datums use
/// for grouping elsewhere.
pub(crate) fn row_key(row: &[Datum]) -> Result<Vec<u8>, DriverError> {
    let mut key = Vec::new();
    for value in row {
        key.extend_from_slice(
            &value
                .to_hash_key()
                .map_err(|_| DriverError::unsupported("this datum kind cannot be deduplicated"))?,
        );
        key.push(0xff);
    }
    Ok(key)
}

/// How many times each row appears.
fn row_counts(rows: &[Vec<Datum>]) -> Result<HashMap<Vec<u8>, usize>, DriverError> {
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    for row in rows {
        *counts.entry(row_key(row)?).or_insert(0) += 1;
    }
    Ok(counts)
}

/// Keeps the first occurrence of each distinct row.
fn dedup_rows(rows: Vec<Vec<Datum>>) -> Result<Vec<Vec<Datum>>, DriverError> {
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if seen.insert(row_key(&row)?) {
            out.push(row);
        }
    }
    Ok(out)
}

/// Sorts the folded rows by a statement-level `ORDER BY`, whose items name
/// output columns rather than any term's source columns.
pub(crate) fn sort_rows_by_output(
    rows: &mut [Vec<Datum>],
    columns: &[(String, FieldType)],
    order_by: &[tidb_ast::OrderItem],
) -> Result<(), DriverError> {
    let mut keys = Vec::with_capacity(order_by.len());
    for item in order_by {
        let index = match &item.expr {
            tidb_ast::Expr::Column(path) => {
                let name = path
                    .last()
                    .ok_or(DriverError::unsupported("empty ORDER BY column"))?;
                columns
                    .iter()
                    .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                    .ok_or(DriverError::unsupported(
                        "a set operation's ORDER BY must name an output column",
                    ))?
            }
            // MySQL also allows ordering by output position.
            tidb_ast::Expr::Int(text) => {
                let position: usize = text
                    .parse()
                    .map_err(|_| DriverError::unsupported("bad ORDER BY position"))?;
                if position == 0 || position > columns.len() {
                    return Err(DriverError::unsupported("ORDER BY position out of range"));
                }
                position - 1
            }
            _ => {
                return Err(DriverError::unsupported(
                    "a set operation's ORDER BY must name an output column",
                ))
            }
        };
        keys.push((index, item.desc));
    }
    let mut failure = None;
    rows.sort_by(|left, right| {
        for (index, desc) in &keys {
            let ordering = match tidb_expr::compare_datums(&left[*index], &right[*index]) {
                Ok(ordering) => ordering,
                Err(error) => {
                    failure = Some(error);
                    std::cmp::Ordering::Equal
                }
            };
            if ordering != std::cmp::Ordering::Equal {
                return if *desc { ordering.reverse() } else { ordering };
            }
        }
        std::cmp::Ordering::Equal
    });
    match failure {
        Some(error) => Err(DriverError::Exec(ExecError::Eval(error))),
        None => Ok(()),
    }
}
