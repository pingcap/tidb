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

//! Go `pkg/table`'s generated columns: the expression a stored or virtual
//! column is computed from (`buildGeneratedExpr`, which `TableFromMeta` runs),
//! and its evaluation over a row being read or written
//! (`FillVirtualColumnValue`). Building and checking one from a DDL statement
//! is the DDL package's.

use std::cell::{Cell, RefCell};

use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::ColumnResolver;
use tidb_hack::GoCaseCompare;

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
    /// The implicit `LIKE` escape that built the cached expression.
    pub build_like_default_escape: u8,
    /// Whether building [`Self::expr`] CONSULTED the zone -- reported by the
    /// resolver that was asked rather than guessed from the expression's
    /// shape, so a new zone-reading fold in the rewriter is covered the day
    /// it lands without anything here changing.
    ///
    /// It is what keeps `b INT AS (a + 1)` on the fast path: an expression
    /// that never asked for a zone builds identically in every session, so
    /// its cache is valid everywhere.
    pub zone_sensitive: bool,
    /// Whether the cached rewrite consulted the omitted-escape policy.
    pub like_default_escape_sensitive: bool,
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
        like_default_escape: u8,
    ) -> Result<std::borrow::Cow<'_, Expression>, ()> {
        if (!self.zone_sensitive || *zone == self.build_zone)
            && (!self.like_default_escape_sensitive
                || like_default_escape == self.build_like_default_escape)
        {
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
        let resolver = TableColumnResolver::with_like_default_escape(
            &names,
            &types,
            zone.clone(),
            like_default_escape,
        );
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
        .position(|column| column.column_name().go_lower_eq(name))
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

/// Go FillVirtualColumnValue: cast with the caller's context, then apply
/// the virtual reader's unsigned clipping and NULL substitution rules.
pub fn fill_virtual_column_values<S: GeneratedColumnSlot>(
    columns: &[S],
    row: &mut [Datum],
    ctx: &dyn crate::context::MutateContext,
) -> Result<(), crate::column::ColumnError> {
    let flags = tidb_expr::Columns::type_flags(ctx);
    materialize_with(columns, row, true, ctx, |column, value| {
        cast_virtual_column_value(
            value,
            column.column_type(),
            column.column_name(),
            ctx,
            flags,
        )
        .map_err(crate::column::ColumnError::from)
    })
}

pub(crate) fn cast_virtual_column_value(
    value: Datum,
    field_type: &FieldType,
    name: &str,
    ctx: &dyn crate::context::MutateContext,
    flags: tidb_datatype::ConversionFlags,
) -> Result<Datum, crate::column::CastError> {
    let negative = match &value {
        Datum::Int(value) => *value < 0,
        Datum::Real(value) | Datum::Float32(value) => tidb_datatype::round_float(*value) < 0.0,
        Datum::Decimal(value) => value.is_negative(),
        _ => false,
    };
    let mut cast =
        crate::column::cast_table_value_with_flags(value, field_type, name, ctx, flags, true)?;
    let column_flags = field_type.flags();
    if (!cast.is_null()
        && column_flags & tidb_datatype::FieldTypeFlags::UNSIGNED != 0
        && flags.allow_negative_to_unsigned()
        && negative)
        || (cast.is_null()
            && column_flags
                & (tidb_datatype::FieldTypeFlags::NOT_NULL
                    | tidb_datatype::FieldTypeFlags::PREVENT_NULL_INSERT)
                != 0)
    {
        cast = crate::bad_null::zero_value(field_type);
    }
    Ok(cast)
}

/// Evaluates each expression, then applies its caller's conversion/NULL policy
/// before later generated dependencies can read that column. Mutation and row
/// decoding share dependency evaluation without sharing their error levels.
pub fn materialize_with<S: GeneratedColumnSlot, E: From<GenerationError>>(
    columns: &[S],
    row: &mut [Datum],
    only_virtual: bool,
    ctx: &dyn tidb_expr::Columns,
    mut convert: impl FnMut(&S, Datum) -> Result<Datum, E>,
) -> Result<(), E> {
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
            .expression_in(columns, &ctx.time_zone(), ctx.like_default_escape())
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
        let value = convert(column, value)?;
        row[offset] = value;
    }
    Ok(())
}

/// [`materialize_with`] over expressions the planner already built
/// (Go `Insert.GenCols`, `resolveGeneratedColumns`): each `Column` of a
/// planned expression indexes the table's complete stored row, so the row is
/// evaluated whole instead of gathered by dependency name.
pub fn materialize_planned<S: GeneratedColumnSlot, E: From<GenerationError>>(
    columns: &[S],
    planned: &[Option<Expression>],
    row: &mut [Datum],
    ctx: &dyn tidb_expr::Columns,
    mut convert: impl FnMut(&S, Datum) -> Result<Datum, E>,
) -> Result<(), E> {
    let types: Vec<FieldType> = columns
        .iter()
        .map(|column| column.column_type().clone())
        .collect();
    for (offset, column) in columns.iter().enumerate() {
        if column.generation().is_none() {
            continue;
        }
        let Some(expr) = planned.get(offset).and_then(Option::as_ref) else {
            return Err(GenerationError {
                column: column.column_name().to_owned(),
                detail: "the generated expression was not planned".to_owned(),
                eval: None,
            }
            .into());
        };
        let value = eval_over_row(expr, &types, row, ctx).map_err(|error| GenerationError {
            column: column.column_name().to_owned(),
            detail: format!("{error:?}"),
            eval: Some(error),
        })?;
        let value = convert(column, value)?;
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
pub fn eval_over_dependencies<S: GeneratedColumnSlot>(
    expr: &Expression,
    dependencies: &[String],
    columns: &[S],
    row: &[Datum],
    ctx: &dyn tidb_expr::Columns,
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
pub fn eval_over_row(
    expr: &Expression,
    types: &[FieldType],
    row: &[Datum],
    ctx: &dyn tidb_expr::Columns,
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
    /// Go `ErrGeneratedColumnRefAutoInc` (3109): the generated column reads
    /// the table's AUTO_INCREMENT column, which
    /// `tidb_enable_auto_increment_in_generated` did not allow.
    RefAutoInc(String),
    /// Go `checkIllegalFn4Generated` for `typeColumn`: a disallowed or
    /// unknown function (3102), an aggregate (1111), a row value (3764), a
    /// window function (3593), a wrong argument count (1582), or
    /// `CAST(... AS ... ARRAY)`, which belongs to functional indexes (1235).
    IllegalFunction(tidb_error::mysql::MysqlError),
    /// Go `ErrUnsupportedOnGeneratedColumn` (3106) with the reason as its
    /// argument, e.g. `Defining a virtual generated column as primary key`.
    Unsupported(&'static str),
    /// An expression form this tier cannot build (Go accepts some of these
    /// and refuses others as 3102). Refused rather than mis-evaluated.
    Unbuildable(&'static str),
    /// An error the expression builder raised with its own MySQL identity,
    /// which Go's `buildColumnAndConstraint` returns as it is: 1582 for a
    /// wrong argument count, 3146 for a JSON argument of the wrong type.
    Eval(tidb_expr::EvalError),
}

/// The generated-column error for a failed rewrite: an unresolved NAME is
/// Go's 1054, an error carrying its own identity propagates, and only a form
/// this tier does not build is [`GeneratedDdlError::Unbuildable`].
pub fn rewrite_error(error: tidb_expr::EvalError, missing_name: Option<String>) -> GeneratedDdlError {
    match (missing_name, error) {
        (Some(name), _) => GeneratedDdlError::UnknownDependency(name),
        (None, tidb_expr::EvalError::Unsupported(_)) => {
            GeneratedDdlError::Unbuildable("this generated-column expression is not supported yet")
        }
        (None, error) => GeneratedDdlError::Eval(error),
    }
}


/// Go `buildGeneratedExpr` (`pkg/table/tables/tables.go`), which
/// `TableFromMeta` runs over a stored column's `GeneratedExprString`. The
/// expression was admitted when its DDL ran, so it is only resolved against
/// the table's columns -- no `checkIllegalFn4Generated` -- and a
/// `CAST(... AS ... ARRAY)` builds (`WithAllowCastArray`), as the hidden
/// column of a multi-valued index needs.
pub fn build_generated_expr(
    expression: &tidb_ast::Expr,
    stored: bool,
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<GeneratedColumn, GeneratedDdlError> {
    resolve_generated_expr(expression, stored, names, types, zone, b'\\')
}

pub fn resolve_generated_expr(
    expression: &tidb_ast::Expr,
    stored: bool,
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> Result<GeneratedColumn, GeneratedDdlError> {
    let resolver = TableColumnResolver::with_like_default_escape(
        names,
        types,
        zone.clone(),
        like_default_escape,
    );
    let expr = tidb_expr::rewriter::rewrite_expr_resolved(expression, &resolver)
        .map_err(|error| rewrite_error(error, resolver.missing_name()))?;
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
        build_like_default_escape: like_default_escape,
        zone_sensitive: resolver.zone_was_read(),
        like_default_escape_sensitive: resolver.like_default_escape_was_read(),
    })
}

/// The flag set Go restores `GeneratedExprString` with
/// (`pkg/ddl/add_column.go`): single-quoted strings, lowercase keywords,
/// back-quoted names, spaces around binary operations, and no schema or table
/// qualifier -- which is why `SHOW CREATE TABLE` prints `` (`a` + 1) ``.
///
/// This is also the canonical form a generated-column substitution rule can
/// compare against once the wired planner implements `GcSubstituter`.
pub fn generated_restore_flags() -> tidb_parser_format::RestoreFlags {
    tidb_parser_format::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_parser_format::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_parser_format::RestoreFlags::NAME_BACK_QUOTES
        | tidb_parser_format::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | tidb_parser_format::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_parser_format::RestoreFlags::WITHOUT_TABLE_NAME
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
    like_default_escape: u8,
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
    like_default_escape_read: Cell<bool>,
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
            like_default_escape: b'\\',
            seen: RefCell::new(Vec::new()),
            missing: RefCell::new(None),
            zone_read: Cell::new(false),
            like_default_escape_read: Cell::new(false),
        }
    }

    /// A resolver over table columns using this statement's implicit `LIKE`
    /// escape for an omitted `ESCAPE` clause.
    pub fn with_like_default_escape(
        names: &'a [String],
        types: &'a [FieldType],
        zone: tidb_datatype::SessionTimeZone,
        like_default_escape: u8,
    ) -> Self {
        let mut resolver = Self::new(names, types, zone);
        resolver.like_default_escape = like_default_escape;
        resolver
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

    /// Whether the expression used the resolver's implicit `LIKE` escape.
    pub fn like_default_escape_was_read(&self) -> bool {
        self.like_default_escape_read.get()
    }
}

impl ColumnResolver for TableColumnResolver<'_> {
    /// Go builds a functional index's hidden column and rebuilds every
    /// generated column for DML with `allowBuildCastArray`
    /// (`BuildHiddenColumnInfo`, `resolveGeneratedColumns`); an ordinary
    /// generated column's DDL refuses the cast first
    /// ([`crate::expression_index::check_illegal_fn_4_generated`]).
    fn allow_build_cast_array(&self) -> bool {
        true
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.zone_read.set(true);
        self.zone.clone()
    }

    fn like_default_escape(&self) -> u8 {
        self.like_default_escape_read.set(true);
        self.like_default_escape
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let offset = self
            .names
            .iter()
            .position(|candidate| candidate.go_lower_eq(name));
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


impl From<GenerationError> for crate::column::ColumnError {
    fn from(error: GenerationError) -> Self {
        match error.eval {
            Some(error) => Self::Eval(error),
            None => Self::Parse(format!(
                "generated column '{}': {}",
                error.column, error.detail
            )),
        }
    }
}

impl From<GenerationError> for crate::kv_table::KvTableError {
    fn from(error: GenerationError) -> Self {
        Self::Generation {
            column: error.column,
            detail: error.detail,
            eval: error.eval,
        }
    }
}
