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

//! A column whose value comes from an EXPRESSION rather than from the row
//! bytes: `GENERATED ALWAYS AS (<expr>) VIRTUAL | STORED`.
//!
//! Mirrors Go `pkg/ddl/generated_column.go` (`verifyColumnGeneration`,
//! `findDependedColumnNames`, `checkDependedColExist`) for the DDL-time
//! validation, and `pkg/table/column.go` +
//! `pkg/expression/column.go`'s `GeneratedExpr` for what the value IS.
//!
//! # The one rule, and why there is no second one
//!
//! A generated column is an ordinary column whose value source is an
//! expression instead of the stored bytes. Everything that follows is that
//! single statement applied consistently, NOT a family of special cases:
//!
//! * The value is recomputed from the row whenever the row is written
//!   ([`materialize`]). Recomputation is IDEMPOTENT -- an expression may only
//!   read non-generated columns and generated columns defined EARLIER, so a
//!   left-to-right pass always sees final values -- which is why every write
//!   path may call it without asking whether someone already did. That is
//!   what keeps a `STORED` column from going stale after an `UPDATE` to its
//!   dependency: the update writes a freshly computed row, never a patched
//!   one. "Whenever the row is written" is stronger than it sounds -- the
//!   recomputation has to happen before anything READS the new row, not
//!   merely before the bytes are encoded, because the referential operators
//!   run over the staged row. An `UPDATE` that recomputed only inside the
//!   encoder left the foreign-key layer comparing a generated value against
//!   its own stale copy, so a referenced key looked unchanged and the
//!   `ON UPDATE CASCADE` that should have repointed the children never fired.
//! * `STORED` writes that value into the row bytes; `VIRTUAL` does not
//!   ([`is_virtual`] drives the encoder's skip list, exactly as the handle
//!   columns are skipped). On the way back a virtual column is therefore
//!   filled the same way it was written -- by evaluating -- so the read and
//!   the write cannot disagree.
//! * An index over a generated column is built from the materialized row, so
//!   it stores the computed value with no index-specific code at all. An index
//!   BACKFILL is therefore a write too, and evaluates at the write level: a
//!   row the expression cannot compute fails the `ALTER TABLE` rather than
//!   being indexed under a value that does not exist.
//! * The expression is evaluated under the SQL MODE of the statement that
//!   writes the row, exactly as every other expression of that statement is
//!   ([`materialize`]'s `ctx`). There is no separate rule for generated
//!   columns and no special case per condition: `100/a` over `a = 0` fails the
//!   write with 1365 under `ERROR_FOR_DIVISION_BY_ZERO` and stores NULL
//!   without it, because that is what `errctx.ErrGroupDividedByZero` says for
//!   an `INSERT`/`UPDATE`/`DELETE`. Evaluating with no context at all made
//!   that flag unreachable, which turned every such write into a silently
//!   stored NULL. A READ evaluates at the query level instead -- also not a
//!   special case, just the level Go's `SELECT` carries -- so restoring a
//!   virtual column warns and reads NULL.
//!
//! # NOT MODELLED (measured, documented, and NOT half-done)
//!
//! The CAST of the computed value into the column's declared type still uses
//! a fixed flag set rather than the statement's, so the STRICT half of the
//! mode does not reach it. Captured from real TiDB under the default mode,
//! against what this tier answers today:
//!
//! | write | TiDB | here |
//! | --- | --- | --- |
//! | `b DATE AS (a) STORED`, `a = '0000-00-00'` | 1292 | stored |
//! | `b DATE AS (a) STORED`, `a = 'not-a-date'` | 1292 | 1064 |
//! | `b TINYINT AS (a) STORED`, `a = 1000` | 1264 | clamped to 127 |
//! | `b INT AS (a) STORED`, `a = '12abc'` | 1366 | truncated to 12 |
//! | `b INT AS (a+1) STORED NOT NULL`, `a = NULL` | 1048 | stored NULL |
//!
//! Every row above was RE-CAPTURED and still holds. What has changed is the
//! reason it holds, so read the old one with care: this was once "the
//! ORDINARY write path is equally lax, so fixing it here alone would give
//! generated columns a strictness ordinary columns do not have". The ordinary
//! path has since been fixed, and the asymmetry now runs the other way.
//! `INSERT INTO t(a INT NOT NULL) VALUES (NULL)` is 1048 and
//! `INSERT INTO t(a INT UNSIGNED) VALUES (-5)` overflows, in both SQL modes,
//! while the same values reaching the same columns THROUGH a generated
//! expression are still stored.
//!
//! The reason this half stayed behind is a signature, not a decision to defer
//! twice: [`materialize`] takes a `tidb_expr::Columns`, the EXPRESSION
//! context, and the write-level rules live on `StmtContext` --
//! `write_conversion_flags` (Go `GetTypeFlagsForInsert`), the warning buffer,
//! and the per-statement `bad_null::NullLevel`. Go has no such split: its
//! `table.CastValue` takes the session and every caller of it is a write. The
//! honest unit here is therefore to route generation's cast through
//! `driver::dml::cast_value_for_column` and `bad_null::handle_bad_null`, which
//! means giving `materialize` the statement context -- and its callers are not
//! all writes (a virtual column is filled on READ, and an index backfill is a
//! write at DDL level), so each call site has to choose its level rather than
//! inherit one. That is why it is still one named seam and not a patch here.
//!
//! # What Go refuses, and this refuses with it
//!
//! `verifyColumnGeneration`: a dependency that is not a column of the table
//! is `ErrBadField` (1054, `Unknown column 'z' in 'generated column
//! function'`), and a dependency on a generated column defined at or after
//! this one is `ErrGeneratedColumnNonPrior` (3107). A dependency on a
//! NON-generated column defined later is ACCEPTED -- captured from Go, where
//! `create table t (a int, b int as (c+1), c int)` succeeds.
//!
//! Not modelled (refused loudly rather than accepted wrongly): an expression
//! this tier's rewriter cannot build, which covers Go's 3102
//! `ErrGeneratedColumnFunctionIsNotAllowed` cases (`rand()`, `@x`,
//! `@@max_connections`, a subquery) along with functions simply not ported
//! yet.
//!
//! # The substitution rule, and why its obstacle turned out not to exist
//!
//! Go rewrites a predicate like `a+1=3` into the indexed virtual generated
//! column that stores `a+1`, so the index can serve the query
//! (`pkg/planner/core/rule_generate_column_substitute.go`). The predicate
//! half of that rule is now ported, in
//! [`crate::generated_column_substitute`].
//!
//! This section used to record an obstacle, and it was the wrong one, so read
//! it as a correction rather than a status: the two expressions to compare
//! DO live in different column namespaces -- a [`GeneratedColumn::expr`]'s
//! `Column` nodes index [`GeneratedColumn::dependencies`], the expression's
//! OWN name list, while a `WHERE` condition's columns index the query -- and
//! the conclusion drawn from that, that an explicit mapping between the two
//! had to be built and kept honest across pruning and derived tables, was
//! wrong.
//!
//! The two never have to meet in a positional namespace at all. The consumer
//! of the rewrite is the access-path choice, and that consumes the `WHERE` as
//! an `tidb_ast::Expr` with column NAMES -- not as a resolved `Expression`.
//! The table side already carries [`GeneratedColumn::expr_text`], the
//! canonically restored text of the same AST. Reducing both sides with the
//! one flag set they are already stored under ([`generated_restore_flags`])
//! gives a single equality with no mapping and no second comparison mode. The
//! compiled [`GeneratedColumn::expr`] is not consulted by the rule at all.

use std::cell::{Cell, RefCell};

use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::ColumnResolver;

/// What makes a column generated: Go `ColumnInfo.GeneratedExprString` /
/// `GeneratedStored`, plus the built expression Go keeps as `GeneratedExpr`.
#[derive(Clone, Debug)]
pub struct GeneratedColumn {
    /// Go `ColumnInfo.GeneratedExprString`: the expression as `SHOW CREATE
    /// TABLE` prints it back, in Go's own restored spelling (`` `a` + 1 ``).
    pub expr_text: String,
    /// Go `ColumnInfo.GeneratedStored`: false for `VIRTUAL`, which is also
    /// what an omitted keyword means.
    pub stored: bool,
    /// The evaluable form, whose `Column` nodes index [`Self::dependencies`]
    /// -- NOT the table's columns.
    ///
    /// This namespace is what makes an `ALTER TABLE` that moves columns
    /// harmless: the dependency list is fixed at DDL time and never
    /// reorders, so there is no offset here for a mutator to forget to
    /// remap. The table offsets are derived from the NAMES at evaluation
    /// time ([`dependency_offsets`]), which is also Go's shape -- its
    /// `Dependences` is a name set and its evaluator binds by schema lookup.
    pub expr: Expression,
    /// Go `ColumnInfo.Dependences`: the NAMES of the columns the expression
    /// reads, in first-seen order -- which is also the namespace
    /// [`Self::expr`]'s `Column` nodes index. A projected scan must decode
    /// these even when the query never named them, or the expression would
    /// evaluate over holes.
    pub dependencies: Vec<String>,
    /// Go `ColumnInfo.GeneratedExpr`, the AST Go keeps beside the text.
    ///
    /// Go does NOT keep a compiled expression on the table: every statement
    /// that touches the column REWRITES this AST in its OWN context
    /// (`logical_plan_builder.go`'s `b.rewrite(ctx,
    /// columns[i].GeneratedExpr.Clone(), ds, nil, true)`), so a fold that
    /// reads the session `time_zone` follows the READING session rather than
    /// the session that ran the `CREATE TABLE`. [`Self::expr`] is a CACHE of
    /// that rewrite for one zone; this is what the cache is rebuilt from when
    /// another zone asks.
    pub source: tidb_ast::Expr,
    /// The session `time_zone` [`Self::expr`] was built in -- the one zone
    /// the cache is valid for.
    pub build_zone: tidb_datatype::SessionTimeZone,
    /// Whether building [`Self::expr`] CONSULTED the zone -- reported by the
    /// resolver that was asked rather than guessed from the expression's
    /// shape, so a new zone-reading fold in the rewriter is covered the day
    /// it lands without anything here changing.
    ///
    /// It is what keeps `b INT AS (a + 1)` on the fast path: an expression
    /// that never asked for a zone builds identically in every session, so
    /// its cache is valid everywhere.
    pub zone_sensitive: bool,
}

impl GeneratedColumn {
    /// The evaluable expression for a statement running in `zone`.
    ///
    /// Borrows the cached build when it is the right one, which is every
    /// zone-independent expression and every statement whose zone matches the
    /// DDL's, and otherwise re-runs the rewrite Go re-runs per statement.
    fn expression_in<S: GeneratedColumnSlot>(
        &self,
        columns: &[S],
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<std::borrow::Cow<'_, Expression>, ()> {
        if !self.zone_sensitive || *zone == self.build_zone {
            return Ok(std::borrow::Cow::Borrowed(&self.expr));
        }
        let names: Vec<String> = columns
            .iter()
            .map(|column| column.column_name().to_owned())
            .collect();
        let types: Vec<FieldType> = columns
            .iter()
            .map(|column| column.column_type().clone())
            .collect();
        let resolver = TableColumnResolver::new(&names, &types, zone.clone());
        // The dependency namespace is derived from the EXPRESSION's first-seen
        // order, so re-resolving the same AST reproduces the same one and the
        // rebuilt `Column` indexes stay the ones `dependencies` names.
        tidb_expr::rewriter::rewrite_expr_resolved(&self.source, &resolver)
            .map(std::borrow::Cow::Owned)
            .map_err(|_| ())
    }
}

/// A column description reduced to what generation needs: the two facts
/// [`materialize`] reads per column.
///
/// This exists so the write/read paths can hand over `KvColumn`s without
/// `generated_column` depending on the table module, and so the unit tests
/// here can drive the same code the table drives.
pub trait GeneratedColumnSlot {
    /// The column's generation, if it has one.
    fn generation(&self) -> Option<&GeneratedColumn>;
    /// The column's own type, which the computed value is cast into.
    fn column_type(&self) -> &FieldType;
    /// The column's name, which is what a dependency names it by.
    fn column_name(&self) -> &str;
}

/// The offset of `name` in `columns`, matched as MySQL matches column names.
pub fn offset_of<S: GeneratedColumnSlot>(columns: &[S], name: &str) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.column_name().eq_ignore_ascii_case(name))
}

/// The offsets `dependencies` name in `columns`, or the first name that is
/// not there.
///
/// This is the ONLY place a name-keyed dependency becomes an offset, and it
/// reads the CURRENT column list every time, so an `ALTER TABLE` that
/// reorders columns has nothing to invalidate.
///
/// # Errors
///
/// The first dependency name `columns` does not define.
pub fn dependency_offsets<S: GeneratedColumnSlot>(
    columns: &[S],
    dependencies: &[String],
) -> Result<Vec<usize>, String> {
    dependencies
        .iter()
        .map(|name| offset_of(columns, name).ok_or_else(|| name.clone()))
        .collect()
}

/// Whether a column's value is NOT written into the row bytes: a generated
/// column that is not `STORED`.
///
/// Go's row encoder builds its column list from `tbl.Cols()` filtered by
/// `!col.IsVirtualGenerated()`; this is that predicate.
pub fn is_virtual(slot: &impl GeneratedColumnSlot) -> bool {
    slot.generation().is_some_and(|g| !g.stored)
}

/// A failure while computing a generated column's value.
#[derive(Clone, Debug)]
pub struct GenerationError {
    /// The column whose expression failed.
    pub column: String,
    /// The evaluation failure, rendered.
    pub detail: String,
    /// The evaluation failure itself, when the EXPRESSION is what failed
    /// rather than the cast into the column's declared type. It carries the
    /// MySQL code the statement has to report -- 1365 for a zero divisor
    /// under `ERROR_FOR_DIVISION_BY_ZERO` -- which a rendered string does not.
    pub eval: Option<tidb_expr::EvalError>,
}

/// Go `table.FillVirtualColumnValue` + the generated-column half of
/// `addRecord`: recomputes every generated column of `row` from the row
/// itself, left to right, casting each result into the column's own type.
///
/// `only_virtual` selects the READ path, which restores the columns the bytes
/// never held; the write path passes `false` and recomputes all of them.
///
/// Left-to-right is the whole of the dependency order: DDL has already
/// refused a generated column that reads a generated column defined at or
/// after it (3107), so by the time offset `i` is evaluated every generated
/// input it may legally name is final.
pub fn materialize<S: GeneratedColumnSlot>(
    columns: &[S],
    row: &mut [Datum],
    only_virtual: bool,
    ctx: &impl tidb_expr::Columns,
) -> Result<(), GenerationError> {
    materialize_with_conversion_flags(
        columns,
        row,
        only_virtual,
        ctx,
        tidb_datatype::DEFAULT_STATEMENT_FLAGS,
    )
}

pub(crate) fn materialize_with_conversion_flags<S: GeneratedColumnSlot>(
    columns: &[S],
    row: &mut [Datum],
    only_virtual: bool,
    ctx: &impl tidb_expr::Columns,
    conversion_flags: tidb_datatype::ConversionFlags,
) -> Result<(), GenerationError> {
    if !columns.iter().any(|c| c.generation().is_some()) {
        return Ok(());
    }
    for (offset, column) in columns.iter().enumerate() {
        let Some(generated) = column.generation() else {
            continue;
        };
        if only_virtual && generated.stored {
            continue;
        }
        // Go rewrites the stored AST in the CURRENT statement's context, so a
        // `TIMESTAMP 'lit'` inside the expression folds in the reading
        // session's zone. Captured from real TiDB, one table created under
        // `+05:00` and read from three sessions:
        //
        // ```text
        // v datetime as (timestamp '2020-01-01 10:00:00+00:00') virtual
        //   read at +05:00 -> 2020-01-01 15:00:00
        //   read at +00:00 -> 2020-01-01 10:00:00
        //   read at -08:00 -> 2020-01-01 02:00:00
        // ```
        //
        // The STORED twin of that column answers 15:00:00 from all three,
        // because a stored value is computed once by the WRITING session --
        // which is this same rule, applied at the moment the write's own
        // context is the current one, not a second rule.
        let expr = generated
            .expression_in(columns, &ctx.time_zone())
            .map_err(|()| GenerationError {
                column: columns[offset].column_name().to_owned(),
                detail: "the generated expression does not build in this session".to_owned(),
                eval: None,
            })?;
        let value = eval_over_dependencies(&expr, &generated.dependencies, columns, row, ctx)
            .map_err(|error| GenerationError {
                column: columns[offset].column_name().to_owned(),
                detail: format!("{error:?}"),
                eval: Some(error),
            })?;
        // Go casts the generated value into the column's declared type before
        // it is stored or returned (`table.CastValue`), which is what makes
        // `b INT AS (a / 2)` an integer rather than the decimal the division
        // produced.
        let value = if value.is_null() {
            Datum::Null
        } else {
            match value.convert_to_in(column.column_type(), conversion_flags, &ctx.time_zone()) {
                Ok(converted) => converted.value,
                Err(error) => {
                    return Err(GenerationError {
                        column: columns[offset].column_name().to_owned(),
                        detail: format!("{error:?}"),
                        eval: None,
                    })
                }
            }
        };
        row[offset] = value;
    }
    Ok(())
}

/// Evaluates an expression whose `Column` nodes index `dependencies` against
/// a row of the TABLE, gathering the named columns' values into the
/// expression's own namespace.
///
/// The gather is where a name becomes an offset, and it happens per
/// evaluation over the CURRENT column list. That is the whole of the
/// invalidation story: no offset survives an `ALTER TABLE` because none is
/// stored. It also picks up the current TYPES, so a `MODIFY COLUMN` that
/// rewrites a dependency's type is seen without a rebuild either.
///
/// The gathered row is `dependencies.len()` wide rather than the table's
/// width, so this is also strictly less work than evaluating over a full row
/// -- the cost of name-keying is the lookups, not the chunk.
///
/// # Errors
///
/// [`tidb_expr::EvalError`] from the expression, or `Unsupported` when a
/// dependency names a column the table no longer defines -- which DDL
/// refuses to create (3837 / 1054), so it is a corruption report, not a
/// user-reachable path.
pub(crate) fn eval_over_dependencies<S: GeneratedColumnSlot>(
    expr: &Expression,
    dependencies: &[String],
    columns: &[S],
    row: &[Datum],
    ctx: &impl tidb_expr::Columns,
) -> Result<Datum, tidb_expr::EvalError> {
    let mut types = Vec::with_capacity(dependencies.len());
    let mut values = Vec::with_capacity(dependencies.len());
    for name in dependencies {
        let offset = offset_of(columns, name).ok_or(tidb_expr::EvalError::Unsupported(
            "a generated or partition expression names a column the table no longer defines",
        ))?;
        types.push(columns[offset].column_type().clone());
        values.push(row.get(offset).cloned().unwrap_or(Datum::Null));
    }
    eval_over_row(expr, &types, &values, ctx)
}

/// Evaluates one expression against a row of datums by materializing the row
/// into the single-row chunk [`Expression::eval`] reads.
///
/// `ctx` is the STATEMENT's evaluation context, not a placeholder: a
/// generated expression is evaluated under the same SQL mode as any other
/// expression of the statement that writes the row, which is what decides
/// whether `100/0` is an error or a NULL.
pub(crate) fn eval_over_row(
    expr: &Expression,
    types: &[FieldType],
    row: &[Datum],
    ctx: &impl tidb_expr::Columns,
) -> Result<Datum, tidb_expr::EvalError> {
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(types, 1);
    for (index, value) in row.iter().enumerate() {
        chunk.append_datum(index, value);
    }
    // A row shorter than the schema (a partially built INSERT row) still has
    // to present every column, or a reference to a trailing column would read
    // off the end.
    for index in row.len()..types.len() {
        chunk.append_datum(index, &Datum::Null);
    }
    expr.eval(ctx, chunk.get_row(0))
}

/// Why DDL refused a generated column.
#[derive(Clone, Debug)]
pub enum GeneratedDdlError {
    /// Go `ErrBadField` (1054): the expression names a column the table does
    /// not define. Reported as `Unknown column '<name>' in 'generated column
    /// function'`.
    UnknownDependency(String),
    /// Go `ErrGeneratedColumnNonPrior` (3107): the expression names a
    /// generated column defined at or after this one.
    NonPrior,
    /// Go `ErrUnsupportedOnGeneratedColumn` (3106) with the reason as its
    /// argument, e.g. `Defining a virtual generated column as primary key`.
    Unsupported(&'static str),
    /// An expression form this tier cannot build (Go accepts some of these
    /// and refuses others as 3102). Refused rather than mis-evaluated.
    Unbuildable(&'static str),
}

/// Go `columnDefToCol`'s generated arm plus `verifyColumnGeneration`: turns
/// the parsed column definitions into one `Option<GeneratedColumn>` per
/// column, refusing exactly what Go refuses.
///
/// `names` and `types` are the table's columns in definition order, so the
/// built expressions index the row the write and read paths use directly.
pub fn build_generated_columns(
    defs: &[tidb_ast::ColumnDef],
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<Option<GeneratedColumn>>, GeneratedDdlError> {
    // Which columns are generated has to be known before any expression is
    // validated, because the 3107 check asks about the column it READS.
    let generated_at: Vec<bool> = defs
        .iter()
        .map(|def| {
            def.options
                .iter()
                .any(|option| matches!(option, tidb_ast::ColumnOption::Generated { .. }))
        })
        .collect();

    let mut built = Vec::with_capacity(defs.len());
    for (position, def) in defs.iter().enumerate() {
        let Some(tidb_ast::ColumnOption::Generated {
            expression, stored, ..
        }) = def
            .options
            .iter()
            .find(|option| matches!(option, tidb_ast::ColumnOption::Generated { .. }))
        else {
            built.push(None);
            continue;
        };
        let resolver = TableColumnResolver::new(names, types, zone.clone());
        let expr = match tidb_expr::rewriter::rewrite_expr_resolved(expression, &resolver) {
            Ok(expr) => expr,
            Err(_) => {
                // An unresolved NAME is Go's 1054; anything else is a form
                // this tier does not build.
                return Err(match resolver.missing_name() {
                    Some(name) => GeneratedDdlError::UnknownDependency(name),
                    None => GeneratedDdlError::Unbuildable(
                        "this generated-column expression is not supported yet",
                    ),
                });
            }
        };
        // A rewrite can succeed while an unrelated branch of the tree failed
        // to resolve only if the resolver was never consulted, so the missing
        // name is still the authority on 1054.
        if let Some(name) = resolver.missing_name() {
            return Err(GeneratedDdlError::UnknownDependency(name));
        }
        let dependencies = resolver.dependencies();
        for dependency in &dependencies {
            // Go `verifyColumnGeneration`: a generated column may refer only
            // to generated columns occurring EARLIER. A later NON-generated
            // column is fine -- captured from Go, `create table t (a int, b
            // int as (c+1), c int)` is accepted.
            if generated_at[*dependency] && position <= *dependency {
                return Err(GeneratedDdlError::NonPrior);
            }
        }
        built.push(Some(GeneratedColumn {
            expr_text: expression.restore_with_flags(generated_restore_flags()),
            stored: *stored,
            expr,
            dependencies: resolver.dependency_names(),
            source: expression.clone(),
            build_zone: zone.clone(),
            zone_sensitive: resolver.zone_was_read(),
        }));
    }
    Ok(built)
}

/// Builds ONE generated column for `ALTER TABLE ... ADD COLUMN <name> <type>
/// AS (expr) VIRTUAL`, against the columns that will PRECEDE it.
///
/// `names`/`types` are the preceding columns in table order and
/// `generated_at` says which of them are themselves generated -- the same
/// three inputs [`build_generated_columns`] derives per position, except that
/// here every candidate really is earlier, so Go's
/// `verifyColumnGeneration` prior-order rule can only be satisfied and is
/// carried by construction rather than re-tested.
///
/// STORED is the caller's refusal, not this function's: Go answers 3106
/// `'Adding generated stored column through ALTER TABLE' is not supported for
/// generated columns.` before any expression is built (captured).
pub fn build_added_generated_column(
    expression: &tidb_ast::Expr,
    stored: bool,
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<GeneratedColumn, GeneratedDdlError> {
    let resolver = TableColumnResolver::new(names, types, zone.clone());
    let expr = match tidb_expr::rewriter::rewrite_expr_resolved(expression, &resolver) {
        Ok(expr) => expr,
        Err(_) => {
            return Err(match resolver.missing_name() {
                Some(name) => GeneratedDdlError::UnknownDependency(name),
                None => GeneratedDdlError::Unbuildable(
                    "this generated-column expression is not supported yet",
                ),
            })
        }
    };
    if let Some(name) = resolver.missing_name() {
        return Err(GeneratedDdlError::UnknownDependency(name));
    }
    Ok(GeneratedColumn {
        expr_text: expression.restore_with_flags(generated_restore_flags()),
        stored,
        expr,
        dependencies: resolver.dependency_names(),
        source: expression.clone(),
        build_zone: zone.clone(),
        zone_sensitive: resolver.zone_was_read(),
    })
}

/// The flag set Go restores `GeneratedExprString` with
/// (`pkg/ddl/add_column.go`): single-quoted strings, lowercase keywords,
/// back-quoted names, spaces around binary operations, and no schema or table
/// qualifier -- which is why `SHOW CREATE TABLE` prints `` (`a` + 1) ``.
///
/// This is also the canonical form the substitution rule compares under, on
/// BOTH sides -- see [`crate::generated_column_substitute`] for why that is
/// the whole of its namespace problem.
pub(crate) fn generated_restore_flags() -> tidb_ast::RestoreFlags {
    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_ast::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES
        | tidb_ast::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME
}

/// Resolves a generated expression's column references against the table's
/// own columns, recording what it saw.
///
/// Go finds a generated column's dependencies with a dedicated AST walk
/// (`FindColumnNamesInExpr`) and then builds the expression separately. Here
/// the ONE walk the rewriter already performs does both jobs: every name the
/// expression reads passes through [`ColumnResolver::resolve`], so the
/// recorded set cannot drift from the set the evaluator will actually read --
/// which is the failure mode a second, independent walk invites.
pub struct TableColumnResolver<'a> {
    names: &'a [String],
    types: &'a [FieldType],
    /// The DDL statement's session `time_zone` (Go threads its
    /// `BuildContext` down every expression-building DDL path), which the
    /// rewriter reads while folding temporal literals in the expression.
    zone: tidb_datatype::SessionTimeZone,
    /// The columns successfully resolved, in first-seen order, as (offset,
    /// name). This order IS the namespace the built expression indexes: a
    /// `Column` node's `index` is a position in this list, never a position
    /// in the table.
    seen: RefCell<Vec<(usize, String)>>,
    /// The first name that resolved to nothing, which is Go's 1054 argument.
    missing: RefCell<Option<String>>,
    /// Whether the rewriter ASKED for the zone while building against this
    /// resolver, which is the only honest answer to "does this expression
    /// fold differently in another session". Recorded here rather than
    /// derived from the AST so it cannot fall behind the rewriter.
    zone_read: Cell<bool>,
}

impl<'a> TableColumnResolver<'a> {
    /// A resolver over the table's columns, by name and declared type.
    pub fn new(
        names: &'a [String],
        types: &'a [FieldType],
        zone: tidb_datatype::SessionTimeZone,
    ) -> Self {
        Self {
            names,
            types,
            zone,
            seen: RefCell::new(Vec::new()),
            missing: RefCell::new(None),
            zone_read: Cell::new(false),
        }
    }

    /// The column offsets the expression referenced, in the table the
    /// expression was BUILT against. Only DDL-time checks may use these: they
    /// run while that table is still the current one. Anything stored has to
    /// keep [`Self::dependency_names`] instead.
    pub fn dependencies(&self) -> Vec<usize> {
        self.seen.borrow().iter().map(|(at, _)| *at).collect()
    }

    /// The column NAMES the expression referenced, in the order the built
    /// expression indexes them. This is what a `GeneratedColumn` or a
    /// `PartitionSpec` stores.
    pub fn dependency_names(&self) -> Vec<String> {
        self.seen
            .borrow()
            .iter()
            .map(|(_, name)| name.clone())
            .collect()
    }

    /// The first unresolvable name, if any.
    pub fn missing_name(&self) -> Option<String> {
        self.missing.borrow().clone()
    }

    /// Whether the build read the session zone, so the caller knows whether
    /// the expression it just built is valid in other sessions.
    pub fn zone_was_read(&self) -> bool {
        self.zone_read.get()
    }
}

impl ColumnResolver for TableColumnResolver<'_> {
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.zone_read.set(true);
        self.zone.clone()
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let offset = self
            .names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name));
        match offset {
            Some(offset) => {
                let mut seen = self.seen.borrow_mut();
                let index = match seen.iter().position(|(at, _)| *at == offset) {
                    Some(index) => index,
                    None => {
                        // The TABLE's spelling, not the reference's, so the
                        // later lookup is stable whatever case was written.
                        seen.push((offset, self.names[offset].clone()));
                        seen.len() - 1
                    }
                };
                Some((index, self.types[offset].clone(), index as i64))
            }
            None => {
                let mut missing = self.missing.borrow_mut();
                if missing.is_none() {
                    // Go quotes the name as written.
                    *missing = Some(name.clone());
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    struct Slot {
        name: String,
        generation: Option<GeneratedColumn>,
        field_type: FieldType,
    }

    impl GeneratedColumnSlot for Slot {
        fn generation(&self) -> Option<&GeneratedColumn> {
            self.generation.as_ref()
        }
        fn column_type(&self) -> &FieldType {
            &self.field_type
        }
        fn column_name(&self) -> &str {
            &self.name
        }
    }

    fn int_type() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// The generated-column expression of `create table t (x int as (<text>))`,
    /// i.e. the same AST node the DDL path reads.
    fn parse_generated_expr(text: &str) -> tidb_ast::Expr {
        let sql = format!("create table t (x int as ({text}))");
        let stmt = tidb_parser::parse(&sql).unwrap();
        let tidb_ast::Stmt::Ddl(ddl) = &stmt else {
            unreachable!("a CREATE TABLE parses as DDL")
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
            unreachable!("a CREATE TABLE parses as CreateTable")
        };
        for option in &create.columns[0].options {
            if let tidb_ast::ColumnOption::Generated { expression, .. } = option {
                return expression.clone();
            }
        }
        unreachable!("the column carries a generated option")
    }

    /// Builds `[a, b AS (a+1) <kind>, c AS (b+1) <kind>]`.
    fn chain(stored: bool) -> Vec<Slot> {
        let names = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let types = vec![int_type(), int_type(), int_type()];
        let build = |text: &str| {
            let resolver =
                TableColumnResolver::new(&names, &types, tidb_datatype::SessionTimeZone::utc());
            let expr = parse_generated_expr(text);
            let built = tidb_expr::rewriter::rewrite_expr_resolved(&expr, &resolver).unwrap();
            GeneratedColumn {
                expr_text: text.to_owned(),
                stored,
                expr: built,
                dependencies: resolver.dependency_names(),
                source: expr,
                build_zone: tidb_datatype::SessionTimeZone::utc(),
                zone_sensitive: resolver.zone_was_read(),
            }
        };
        vec![
            Slot {
                name: "a".to_owned(),
                generation: None,
                field_type: int_type(),
            },
            Slot {
                name: "b".to_owned(),
                generation: Some(build("a + 1")),
                field_type: int_type(),
            },
            Slot {
                name: "c".to_owned(),
                generation: Some(build("b + 1")),
                field_type: int_type(),
            },
        ]
    }

    #[test]
    fn a_chain_of_generated_columns_is_computed_left_to_right() {
        let columns = chain(true);
        let mut row = vec![Datum::Int(1), Datum::Null, Datum::Null];
        materialize(&columns, &mut row, false, &tidb_expr::NoColumns).unwrap();
        assert_eq!(row, vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]);
    }

    /// The property every write path depends on: recomputing an already
    /// computed row changes nothing, so no caller has to know whether some
    /// earlier caller already did it.
    #[test]
    fn materializing_twice_gives_the_same_row() {
        let columns = chain(true);
        let mut row = vec![Datum::Int(4), Datum::Null, Datum::Null];
        materialize(&columns, &mut row, false, &tidb_expr::NoColumns).unwrap();
        let once = row.clone();
        materialize(&columns, &mut row, false, &tidb_expr::NoColumns).unwrap();
        assert_eq!(row, once);
    }

    /// The staleness trap: a row whose dependency changed must not keep the
    /// value that was computed from the OLD dependency.
    #[test]
    fn a_changed_dependency_recomputes_the_whole_chain() {
        let columns = chain(true);
        let mut row = vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)];
        row[0] = Datum::Int(10);
        materialize(&columns, &mut row, false, &tidb_expr::NoColumns).unwrap();
        assert_eq!(row, vec![Datum::Int(10), Datum::Int(11), Datum::Int(12)]);
    }

    /// The read path restores only what the bytes did not hold.
    #[test]
    fn only_virtual_leaves_a_stored_column_as_decoded() {
        let columns = chain(true);
        let mut row = vec![Datum::Int(1), Datum::Int(99), Datum::Int(98)];
        materialize(&columns, &mut row, true, &tidb_expr::NoColumns).unwrap();
        assert_eq!(row, vec![Datum::Int(1), Datum::Int(99), Datum::Int(98)]);
    }

    #[test]
    fn only_virtual_recomputes_a_virtual_column() {
        let columns = chain(false);
        let mut row = vec![Datum::Int(1), Datum::Null, Datum::Null];
        materialize(&columns, &mut row, true, &tidb_expr::NoColumns).unwrap();
        assert_eq!(row, vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]);
    }

    #[test]
    fn an_unknown_dependency_is_reported_by_name() {
        let names = vec!["a".to_owned()];
        let types = vec![int_type()];
        let resolver =
            TableColumnResolver::new(&names, &types, tidb_datatype::SessionTimeZone::utc());
        let expr = parse_generated_expr("zz + 1");
        assert!(tidb_expr::rewriter::rewrite_expr_resolved(&expr, &resolver).is_err());
        assert_eq!(resolver.missing_name().as_deref(), Some("zz"));
    }

    #[test]
    fn dependencies_are_the_offsets_the_expression_reads() {
        let names = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let types = vec![int_type(), int_type(), int_type()];
        let resolver =
            TableColumnResolver::new(&names, &types, tidb_datatype::SessionTimeZone::utc());
        let expr = parse_generated_expr("c + a");
        tidb_expr::rewriter::rewrite_expr_resolved(&expr, &resolver).unwrap();
        assert_eq!(resolver.dependencies(), vec![2, 0]);
    }
}
