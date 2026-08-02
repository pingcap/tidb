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

//! An index key part that is an EXPRESSION rather than a column:
//! `CREATE INDEX idx ON t((a + 1))`.
//!
//! Mirrors Go `pkg/ddl/create_table.go`'s `BuildHiddenColumnInfo` and
//! `precheckBuildHiddenColumnInfo`, plus the `illegalFunctionChecker` half of
//! `pkg/ddl/generated_column.go`.
//!
//! # There is no expression-index machinery, because Go has none either
//!
//! TiDB does not store expressions in index metadata. It rewrites
//! `((a + 1))` into a HIDDEN VIRTUAL GENERATED COLUMN named
//! `_V$_<index name>_<part>` holding `a + 1`, and indexes THAT column. Every
//! later step -- writing index entries, maintaining them across `UPDATE` and
//! `DELETE`, reading them back, `ADMIN CHECK TABLE` -- is the ordinary
//! generated-column path in [`crate::generated_column`], which already writes
//! index entries from the materialized row. So this module builds a column
//! and nothing else; there is deliberately no second code path to keep in
//! step with the first.
//!
//! What that DOES cost is one invariant, enforced in [`crate::kv_table`]: a
//! hidden column exists in the row and in index offsets but must appear in no
//! user-visible column enumeration -- not `SELECT *`, not an `INSERT`'s
//! arity, not `SHOW CREATE TABLE`, `DESC`, or `information_schema.COLUMNS`.
//! `KvTable` keeps hidden columns as a contiguous TAIL and records their
//! count, so "the visible columns" is a prefix slice and a visible offset IS
//! a physical offset. That removes the mapping an interior hidden column
//! would have forced between the two numberings -- and a mapping that can be
//! forgotten at one call site is exactly how a hidden column leaks into an
//! answer.
//!
//! # Captured from Go (`difftests/gorun`), not assumed
//!
//! ```text
//! create table t (a int, b int); create index idx on t((a+1));
//! show create table t   ->  KEY `idx` ((`a` + 1))        -- not the column
//! select * from t       ->  1|2                          -- two columns
//! desc t                ->  a, b                         -- two rows
//! information_schema.columns for `te` -> a|1, z|2        -- hidden absent
//! insert into te values (5)  -> 1136 when te has 2 visible columns
//! show index from te    ->  Column_name NULL, Expression `a` + 1
//! admin check table t   ->  OK after INSERT, UPDATE, DELETE and REPLACE
//! alter table t drop index idx; show create table t -> hidden column gone
//! create table t2 (a int, index ((a+1))) -> KEY `expression_index`, then
//!                                           `expression_index_2`
//! ```
//!
//! and the refusals, each with the errno `gorun` reported:
//!
//! ```text
//! index ((a))                  3762  Expression index on a column is not supported.
//! index i ((rand()))           3758  ... contains a disallowed function
//! index i ((a+@@max_connections)) 3758
//! index i ((values(a)))        3758
//! index i ((abs(a)))           8200  Unsupported creating expression index containing
//!                                    unsafe functions without allow-expression-index
//! index i ((sum(a)))           1111  Invalid use of group function
//! index i (((a,a)))            3800  ... cannot refer to a row value
//! index i ((zz+1))             1054  Unknown column 'zz' in 'expression'
//! index i ((a+1)) on auto_inc  3754  ... cannot refer to an auto-increment column
//! a user column named `_V$_i_0` 1060 Duplicate column name
//! ```
//!
//! `index i ((lower(a)))` is ACCEPTED: Go admits a function call in an
//! expression index only from the `GAFunction4ExpressionIndex` whitelist
//! (`pkg/sessionctx/variable/varsutil.go`), everything else being 8200 while
//! `allow-expression-index` is off, which is the default.

use tidb_ast::{Expr, IndexPart};
use tidb_datatype::FieldType;

use crate::driver::DriverError;
use crate::generated_column::{GeneratedColumn, TableColumnResolver};

/// Go `pkg/ddl/executor.go`'s `expressionIndexPrefix`.
const HIDDEN_COLUMN_PREFIX: &str = "_V$";

/// Go `mysql.MaxColumnNameLength`.
const MAX_COLUMN_NAME_LENGTH: usize = 64;

/// The hidden column an expression key part is rewritten into.
pub struct HiddenIndexColumn {
    /// The generated name, `_V$_<index name>_<part index>`.
    pub name: String,
    /// The column's type, which is the expression's own result type.
    pub field_type: FieldType,
    /// The virtual generation that computes the indexed value.
    pub generated: GeneratedColumn,
}

/// The functions Go allows a FUNCTION CALL in an expression index to be:
/// `variable.GAFunction4ExpressionIndex`. Anything else is 8200 unless the
/// server was started with `allow-expression-index`, which this tier does not
/// model as settable.
const GA_FUNCTIONS: &[&str] = &[
    "lower",
    "upper",
    "md5",
    "reverse",
    "vitess_hash",
    "tidb_shard",
    "json_type",
    "json_extract",
    "json_unquote",
    "json_array",
    "json_object",
    "json_set",
    "json_insert",
    "json_replace",
    "json_remove",
    "json_contains",
    "json_contains_path",
    "json_valid",
    "json_array_append",
    "json_array_insert",
    "json_merge_patch",
    "json_merge_preserve",
    "json_pretty",
    "json_quote",
    "json_schema_valid",
    "json_search",
    "json_storage_size",
    "json_depth",
    "json_keys",
    "json_length",
];

/// Go `illegalFunctionChecker` reduced to the outcomes it can reach for
/// `typeIndex`, in the order `checkIllegalFn4Generated` reports them.
///
/// The scan is a WHITELIST over expression forms rather than Go's blacklist
/// over function names. That is not a shortcut: Go's blacklist is applied to
/// `ast.FuncCallExpr`, and every call that survives it still has to be on the
/// GA list, so the set Go accepts is `{non-call nodes} + {GA calls}`. Listing
/// that set directly is the same set with no second list to drift.
fn check_admissible(index_name: &str, expr: &Expr) -> Result<(), DriverError> {
    match expr {
        // Leaves and the operators Go never routes through a function check.
        Expr::Column(_)
        | Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::Null
        | Expr::Bool(_) => Ok(()),

        // Go: `*ast.AggregateFuncExpr` -> ErrInvalidGroupFuncUse (1111).
        Expr::Aggregate { .. } | Expr::GroupConcat { .. } => Err(DriverError::InvalidGroupFuncUse),
        // Go: `*ast.RowExpr` -> ErrFunctionalIndexRowValueIsNotAllowed (3800).
        Expr::Row(_) => Err(DriverError::FunctionalIndexRowValue(index_name.to_owned())),
        // Go: `*ast.WindowFuncExpr` -> ErrWindowInvalidWindowFuncUse (3593).
        Expr::Window { .. } => Err(DriverError::WindowInvalidWindowFuncUse(
            index_name.to_owned(),
        )),
        // Go: `*ast.SubqueryExpr, *ast.ValuesExpr, *ast.VariableExpr` ->
        // ErrFunctionalIndexFunctionIsNotAllowed (3758).
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::UserVar(_)
        | Expr::SysVar { .. }
        | Expr::Default(_) => Err(DriverError::FunctionalIndexFunctionNotAllowed(
            index_name.to_owned(),
        )),

        Expr::Paren(inner) | Expr::Unary(_, inner) => check_admissible(index_name, inner),
        Expr::Binary(_, left, right) => {
            check_admissible(index_name, left)?;
            check_admissible(index_name, right)
        }
        Expr::Is { expr, .. } | Expr::Collate { expr, .. } => check_admissible(index_name, expr),
        Expr::In { expr, list, .. } => {
            check_admissible(index_name, expr)?;
            list.iter()
                .try_for_each(|e| check_admissible(index_name, e))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            check_admissible(index_name, expr)?;
            check_admissible(index_name, low)?;
            check_admissible(index_name, high)
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            for expr in value
                .iter()
                .map(AsRef::as_ref)
                .chain(else_clause.iter().map(AsRef::as_ref))
            {
                check_admissible(index_name, expr)?;
            }
            for (condition, result) in when_clauses {
                check_admissible(index_name, condition)?;
                check_admissible(index_name, result)?;
            }
            Ok(())
        }
        // Go's `*ast.FuncCastExpr` is not a function call, so it never meets
        // the GA list. The ARRAY form is multi-valued indexing, a feature of
        // its own that this tier does not maintain, so it is refused here
        // rather than built as an ordinary scalar index -- which would index
        // the whole JSON document under a multi-valued index's name.
        Expr::Cast(cast) => {
            if cast.array {
                return Err(DriverError::unsupported(
                    "a multi-valued index (CAST(... AS ... ARRAY)) is not supported yet",
                ));
            }
            check_admissible(index_name, &cast.expr)
        }

        Expr::Func { name, args, .. } => check_function(index_name, name, args),
        Expr::GenericFuncCall { name, args, .. } => check_function(index_name, name, args),

        // Every remaining form -- `EXTRACT`, `POSITION`, `TRIM`, `INTERVAL`,
        // `MATCH ... AGAINST`, a charset introducer, ... -- is refused rather
        // than guessed at. Go reaches most of them as `FuncCallExpr` and
        // answers 8200, but not all, and an index built on a guess is an
        // index that disagrees with the rows it indexes.
        _ => Err(DriverError::unsupported(
            "this expression form is not supported in an expression index yet",
        )),
    }
}

/// The one rule a function call in an expression index must pass: Go's GA
/// whitelist. A call outside it is 8200 while `allow-expression-index` is
/// off, which is how every server this tier models is configured.
fn check_function(index_name: &str, name: &str, args: &[Expr]) -> Result<(), DriverError> {
    let lower = name.to_ascii_lowercase();
    // Go reaches `values(x)` as `*ast.ValuesExpr`, before any function check.
    if lower == "values" {
        return Err(DriverError::FunctionalIndexFunctionNotAllowed(
            index_name.to_owned(),
        ));
    }
    if !GA_FUNCTIONS.contains(&lower.as_str()) {
        return Err(DriverError::UnsafeFunctionInExpressionIndex);
    }
    args.iter()
        .try_for_each(|arg| check_admissible(index_name, arg))
}

/// Go `BuildHiddenColumnInfo`: turns an index's EXPRESSION key parts into
/// hidden virtual generated columns, one per part, leaving the ordinary
/// column parts alone.
///
/// `names`/`types` are the table's columns in physical order, so a built
/// expression indexes the same row the write and read paths pass around --
/// the property [`crate::generated_column`] relies on.
///
/// Returns one entry per expression part, paired with that part's position in
/// `parts` so the caller can put the hidden column's offset back in the right
/// key slot.
pub fn build_hidden_columns(
    index_name: &str,
    parts: &[IndexPart],
    names: &[String],
    types: &[FieldType],
) -> Result<Vec<(usize, HiddenIndexColumn)>, DriverError> {
    let mut built = Vec::new();
    for (position, part) in parts.iter().enumerate() {
        let IndexPart::Expr { expr, .. } = part else {
            continue;
        };
        let name = format!("{HIDDEN_COLUMN_PREFIX}_{index_name}_{position}");
        // Go `precheckBuildHiddenColumnInfo`: the generated name is subject
        // to the ordinary identifier length limit, reported against the
        // hidden column rather than the index.
        if name.chars().count() > MAX_COLUMN_NAME_LENGTH {
            return Err(DriverError::TooLongIdent("hidden column".to_owned()));
        }
        check_admissible(index_name, expr)?;
        // Go: the hidden column's name must not already be taken. Captured as
        // 1060 against a user column literally called `_V$_idxh_0`.
        if names.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
            return Err(DriverError::DuplicateColumnName(name));
        }

        let resolver = TableColumnResolver::new(names, types);
        let built_expr = match tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver) {
            Ok(built_expr) => built_expr,
            Err(_) => {
                return Err(match resolver.missing_name() {
                    // Go reports 1054 with the clause `expression`, not the
                    // `generated column function` a column generation uses.
                    Some(missing) => DriverError::UnknownColumnInClause {
                        column: missing,
                        clause: "expression".to_owned(),
                    },
                    None => DriverError::unsupported(
                        "this expression index's expression is not supported yet",
                    ),
                });
            }
        };
        if let Some(missing) = resolver.missing_name() {
            return Err(DriverError::UnknownColumnInClause {
                column: missing,
                clause: "expression".to_owned(),
            });
        }
        // Go `BuildHiddenColumnInfo`: an expression that IS a column is 3762,
        // checked on the BUILT expression so `((a))` and `(((a)))` are both
        // caught, exactly as Go's `expr.(*expression.Column)` is.
        if matches!(built_expr, tidb_expr::expression::Expression::Column(_)) {
            return Err(DriverError::FunctionalIndexOnField);
        }

        // Go takes the hidden column's type from `expr.GetType()`. Here the
        // scalar function has already coerced its own result into exactly this
        // type before returning it (`ScalarFunction::coerce_to_ret_type`), so
        // the cast `materialize` then applies is a no-op rather than a second,
        // disagreeing conversion.
        //
        // An expression with no static type has nothing to encode the index
        // entry as, so it is refused rather than given a guessed one: an
        // index whose key type disagrees with the value it stores reads back
        // the wrong rows and `ADMIN CHECK TABLE` would call it consistent.
        let Some(field_type) = built_expr.static_type().cloned() else {
            return Err(DriverError::unsupported(
                "an expression index over an expression with no static type is not supported yet",
            ));
        };
        built.push((
            position,
            HiddenIndexColumn {
                name,
                field_type,
                generated: GeneratedColumn {
                    expr_text: expr.restore_with_flags(hidden_restore_flags()),
                    // Go sets `GeneratedStored: false`: the value is
                    // recomputed on every read, and only the INDEX stores it.
                    stored: false,
                    dependencies: resolver.dependency_names(),
                    expr: built_expr,
                },
            },
        ));
    }
    Ok(built)
}

/// Go restores an expression index's key part with the same flag set a
/// generated column uses, which is why `SHOW CREATE TABLE` prints
/// `` KEY `idx` ((`a` + 1)) ``.
fn hidden_restore_flags() -> tidb_ast::RestoreFlags {
    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_ast::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES
        | tidb_ast::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME
}

/// Whether an index has at least one expression key part, which is what makes
/// the caller take the hidden-column path at all.
#[must_use]
pub fn has_expression_part(parts: &[IndexPart]) -> bool {
    parts.iter().any(|p| matches!(p, IndexPart::Expr { .. }))
}
