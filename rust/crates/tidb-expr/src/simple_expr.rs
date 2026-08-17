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

//! SEED of Go `pkg/expression`, covering the "build one expression against a
//! table, with limited context" surface that DDL, partition pruning and the
//! DDL coprocessor reach for -- and nothing else. `pkg/expression` is far too
//! large to complete in one unit; this module is explicitly a SEED with the
//! boundaries named below.
//!
//! Ported symbol groups, each with its Go home:
//!
//! - **Simple-expression building** (`simple_rewriter.go`, `expression.go`):
//!   [`BuildOptions`] with its complete option surface --
//!   [`BuildOptions::with_table_info`] (Go `WithTableInfo`),
//!   [`BuildOptions::with_input_schema_and_names`] (`WithInputSchemaAndNames`),
//!   [`BuildOptions::with_allow_cast_array`] (`WithAllowCastArray`),
//!   [`BuildOptions::with_cast_expr_to`] (`WithCastExprTo`) and
//!   [`BuildOptions::with_use_new_collate`] (`WithUseNewCollate`) -- plus
//!   [`build_simple_expr`] (Go `BuildSimpleExpr`, whose body lives in
//!   `pkg/planner/core/expression_rewriter.go:108` `buildSimpleExpr`),
//!   [`parse_simple_expr`] (`simple_rewriter.go:37`) and
//!   [`parse_simple_expr_with_table_info`] (`simple_rewriter.go:31`).
//! - **Name resolution** (`simple_rewriter.go:63`, `:92`): ALREADY PORTED in
//!   this crate as [`crate::find_field_name`] /
//!   [`crate::find_field_name_index_by_column`]; this module adds only the
//!   [`SchemaNameResolver`] that binds an `Expr::Column` path through them.
//! - **Condition composition** (`expression.go:824-848`):
//!   [`compose_cnf_condition`], [`compose_dnf_condition`] and their shared
//!   `compose_condition_with_binary_op`.
//! - **Column extraction** (`util.go:127`, `:140`, `:164`):
//!   [`extract_columns`], [`extract_cor_columns`] and
//!   [`extract_columns_from_expressions`].
//! - **Column-info conversion** (`expression.go:1109`, `:1115`):
//!   [`column_infos_to_columns_and_names`] and
//!   [`column_infos_to_columns_and_names_with_collate`], over the
//!   [`ColumnInfoSource`] view described below, plus the `ResolveIndices`
//!   walk they finish a virtual generated column with,
//!   [`resolve_indices_in_place`].
//!
//! # Boundaries (this is a seed, not the package)
//!
//! - `// boundary:` Go `model.TableInfo`/`model.ColumnInfo`. `tidb-expr` sits
//!   BELOW `tidb-model` in the workspace and must not depend on it, so the
//!   column-info conversions take a [`ColumnInfoSource`] view: six accessors
//!   naming exactly the `ColumnInfo` fields Go reads here. Callers that hold
//!   real `tidb_model::ColumnInfo` values implement it in one place. No model
//!   type is duplicated.
//! - `// boundary:` Go `generatedexpr.SimpleResolveName`. Go resolves a stored
//!   generated-column string's names against the `TableInfo` BEFORE building,
//!   because its rewriter needs `ColumnNameExpr.Refer`. This port resolves
//!   names through [`SchemaNameResolver`] during the single rewrite walk, so
//!   the pre-pass has no counterpart and the `tblInfo` argument of
//!   `WithInputSchemaAndNames` is not carried.
//! - `// boundary:` Go `DEFAULT(col)`. `buildSimpleExpr`'s `SourceTable` also
//!   feeds `DEFAULT(col)`, which needs `ColumnInfo.DefaultValue` /
//!   `DefaultIsExpr`. That leg is NOT ported; it stays available to callers
//!   through [`ColumnResolver::resolve_default`], which
//!   [`SchemaNameResolver`] forwards to its base context.
//! - `// boundary:` `WithAllowCastArray(true)`. The flag is stored and
//!   reported, but this crate's rewriter rejects every `CAST(.. AS .. ARRAY)`
//!   (`rewriter.rs`, "a CAST with the ARRAY modifier is not supported yet"),
//!   so the permissive leg cannot yet be exercised.
//! - `// boundary:` `WithUseNewCollate`. Stored and reported, but this crate
//!   derives collation from the process-wide
//!   `tidb_datatype::new_collation_enabled()` rather than a
//!   per-build flag, so the value does not yet steer derivation.
//! - `// boundary:` Go's `sqlexec.SQLParser` fast path in `ParseSimpleExpr`
//!   (reuse of the session's pooled parser). This port always calls
//!   [`tidb_parser::parse`]; the parse RESULT is identical, only the pooling
//!   is absent. Go's `AppendWarning(util.SyntaxWarn(..))` loop over parser
//!   warnings likewise has no warning sink here.
//! - `// boundary:` unknown-column diagnostics. Go raises
//!   `[planner:1054]Unknown column 'a' in 'expression'`; this crate's
//!   rewriter reports `EvalError::Unsupported("unresolved column
//!   reference")`, which is the shared spelling every existing resolver
//!   already produces. Changing it is an `EvalError` change, not a change
//!   here.
//! - `// boundary:` `NewFunctionInternal`'s constant folding. Go composes each
//!   CNF/DNF node through `NewFunctionInternal`, which folds. The composers
//!   here build the node and stop, because their callers compose predicates
//!   that were already folded when they were built.
//!
//! NOT ported from the extractor family (`util.go`), by name, so the omission
//! is greppable: `ExtractDependentColumns`, `ExtractColumnsMapFromExpressions`,
//! `ExtractColumnsMapFromExpressionsWithReusedMap`,
//! `ExtractAllColumnsFromExpressionsInUsedSlices`,
//! `ExtractAllColumnsFromExpressions`, `ExtractColumnsSetFromExpressions`,
//! `ExtractColumnsAndCorColumnsFromExpressions`, `ExtractColumnsFromColOpCol`,
//! `GetUniqueIDToColumnMap`/`PutUniqueIDToColumnMap`. Also not ported here:
//! `FlattenCNFConditions`/`FlattenDNFConditions` (the inverse of the
//! composers) and `TableInfo2SchemaAndNames` (needs `TableInfo.Indices`,
//! i.e. the model boundary above).

use std::collections::BTreeMap;

use tidb_ast::{CiString, Expr, SelectField, Stmt};
use tidb_datatype::{
    FieldName, FieldNameMetadata, FieldType, FieldTypeCode, FieldTypeFlags, IdentifierMetadata,
    QualifiedColumnName,
};

use crate::column::{Column, CorrelatedColumn};
use crate::constant_fold::ConstantFoldMode;
use crate::exprctx::PlanColumnIdAllocator;
use crate::expression::{Expression, ScalarFunction};
use crate::field_name::{find_field_name, NonUniqueFieldName};
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
use crate::schema::Schema;
use crate::EvalError;

/// Why a simple-expression build failed.
///
/// The four fixed messages are Go's own `errors.New` strings, byte for byte,
/// so a caller that surfaces them matches TiDB's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimpleExprError {
    /// Go `ParseSimpleExpr`: the expression string was empty.
    EmptyExpressionString,
    /// Go `buildSimpleExpr`: names were given without a schema.
    NamesWithoutSchema,
    /// Go `buildSimpleExpr`: schema and names disagree in length.
    SchemaNamesLengthMismatch,
    /// Go `errNonUniq` (1052), raised by the already-ported
    /// [`crate::find_field_name`]: the reference matches several visible
    /// fields.
    NonUniqueColumn(NonUniqueFieldName),
    /// Go `Column.ResolveIndices`: a bound column is absent from the schema.
    ColumnNotInSchema(i64),
    /// The parser rejected `select <expr>`.
    Parse(String),
    /// The rewrite could not build the expression in the ported domain.
    Build(EvalError),
}

impl std::fmt::Display for SimpleExprError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyExpressionString => {
                formatter.write_str("expression should not be an empty string")
            }
            Self::NamesWithoutSchema => formatter
                .write_str("InputSchema and InputNames should be specified at the same time"),
            Self::SchemaNamesLengthMismatch => {
                formatter.write_str("InputSchema and InputNames should be the same length")
            }
            Self::NonUniqueColumn(error) => write!(formatter, "{error}"),
            Self::ColumnNotInSchema(unique_id) => {
                write!(
                    formatter,
                    "Can't find column with UniqueID {unique_id} in schema"
                )
            }
            Self::Parse(message) => write!(formatter, "{message}"),
            Self::Build(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl std::error::Error for SimpleExprError {}

impl From<NonUniqueFieldName> for SimpleExprError {
    fn from(error: NonUniqueFieldName) -> Self {
        Self::NonUniqueColumn(error)
    }
}

impl From<EvalError> for SimpleExprError {
    fn from(error: EvalError) -> Self {
        Self::Build(error)
    }
}

/// The `model.ColumnInfo` fields `ColumnInfos2ColumnsAndNamesWithCollate`
/// reads, as a view.
///
/// boundary: `tidb-expr` may not depend on `tidb-model` (the model crate is
/// the higher layer). Rather than copy a `ColumnInfo` struct into this crate
/// -- a duplicate that would silently drift -- the conversion is generic over
/// this trait, and the one caller that owns real column metadata implements
/// it once.
pub trait ColumnInfoSource {
    /// Go `ColumnInfo.Name`.
    fn column_name(&self) -> &CiString;
    /// Go `ColumnInfo.ID`.
    fn column_id(&self) -> i64;
    /// Go `ColumnInfo.Offset`: the column's position in the TABLE, which
    /// becomes `Column.Index` before `ResolveIndices` remaps it.
    fn column_offset(&self) -> i64;
    /// Go `ColumnInfo.FieldType`.
    fn column_field_type(&self) -> &FieldType;
    /// Go `ColumnInfo.Hidden`.
    fn column_hidden(&self) -> bool {
        false
    }
    /// Go `ColumnInfo.GeneratedExprString`, but only when
    /// `ColumnInfo.IsVirtualGenerated()` holds -- the exact condition under
    /// which Go builds the expression. A STORED generated column returns
    /// `None`: its value is read from the row, never recomputed.
    fn virtual_generated_expr(&self) -> Option<&str> {
        None
    }
}

/// Go `BuildOptions` (`expression.go:55`): the optional settings a simple
/// build accepts.
///
/// Go applies variadic `BuildOption` closures; Rust uses consuming builder
/// methods, one per Go option, with the same names.
#[derive(Debug, Clone, Default)]
pub struct BuildOptions {
    /// Go `InputSchema`.
    pub input_schema: Option<Schema>,
    /// Go `InputNames`.
    pub input_names: Vec<FieldName>,
    /// Go `SourceTableDB`.
    pub source_table_db: IdentifierMetadata,
    /// Go `AllowCastArray`. See this module's boundary note: stored, but the
    /// permissive leg is not reachable yet.
    pub allow_cast_array: bool,
    /// Go `TargetFieldType`: when set, the built expression is wrapped in a
    /// cast to it.
    pub target_field_type: Option<FieldType>,
    /// Go `UseNewCollate`. See this module's boundary note.
    pub use_new_collate: bool,
}

impl BuildOptions {
    /// Go's zero `BuildOptions` with `UseNewCollate` seeded from the process
    /// collation mode, which is what `buildSimpleExpr` does before applying
    /// any option.
    #[must_use]
    pub fn new() -> Self {
        Self {
            use_new_collate: tidb_datatype::new_collation_enabled(),
            ..Self::default()
        }
    }

    /// Go `WithInputSchemaAndNames(schema, names, table)`.
    ///
    /// The `table` argument is dropped: it exists in Go only to reach
    /// `DEFAULT(col)` metadata, which this seed does not port (see the module
    /// boundary note).
    #[must_use]
    pub fn with_input_schema_and_names(mut self, schema: Schema, names: Vec<FieldName>) -> Self {
        self.input_schema = Some(schema);
        self.input_names = names;
        self
    }

    /// Go `WithTableInfo(db, tblInfo)`.
    ///
    /// Go stores the table and lets `buildSimpleExpr` call
    /// `ColumnInfos2ColumnsAndNames` if no schema was supplied. Because that
    /// conversion needs a column-id allocator and a build context, this port
    /// performs it here -- the observable result (a schema and names over the
    /// table's columns, qualified by `db`) is the same, and it fails at the
    /// point the caller can see why.
    pub fn with_table_info<C: ColumnInfoSource>(
        mut self,
        ctx: &dyn ColumnResolver,
        ids: &dyn PlanColumnIdAllocator,
        db: &str,
        table_name: &CiString,
        col_infos: &[C],
    ) -> Result<Self, SimpleExprError> {
        self.source_table_db = IdentifierMetadata::from_parts(db, CiString::new(db).lowercase());
        if self.input_schema.is_none() {
            let (columns, names) = column_infos_to_columns_and_names(
                ctx,
                ids,
                &self.source_table_db,
                table_name,
                col_infos,
            )?;
            self.input_schema = Some(Schema::new(columns));
            self.input_names = names;
        }
        Ok(self)
    }

    /// Go `WithAllowCastArray(allow)`.
    #[must_use]
    pub fn with_allow_cast_array(mut self, allow: bool) -> Self {
        self.allow_cast_array = allow;
        self
    }

    /// Go `WithCastExprTo(targetFt)`.
    #[must_use]
    pub fn with_cast_expr_to(mut self, target: FieldType) -> Self {
        self.target_field_type = Some(target);
        self
    }

    /// Go `WithUseNewCollate(useNewCollate)`.
    #[must_use]
    pub fn with_use_new_collate(mut self, use_new_collate: bool) -> Self {
        self.use_new_collate = use_new_collate;
        self
    }
}

/// Go `expressionRewriter`'s schema/name scope, as a [`ColumnResolver`].
///
/// It resolves an `Expr::Column` path through [`find_field_name`] (Go
/// `FindFieldName`) into the position of a [`FieldName`], then hands back the
/// schema column at that position UNCHANGED -- Go's `toColumn` returns
/// `schema.Columns[idx]` itself, so `ID`, `OrigName`, `IsHidden` and
/// `VirtualExpr` must survive the binding.
///
/// Every non-column decision (session zone, connection charset, fold mode,
/// `DEFAULT` resolution, ...) is forwarded to the base context, which is Go's
/// `ctx BuildContext`.
pub struct SchemaNameResolver<'a> {
    base: &'a dyn ColumnResolver,
    schema: &'a Schema,
    names: &'a [FieldName],
}

impl<'a> SchemaNameResolver<'a> {
    /// Binds `schema`/`names` as the column scope over the `base` context.
    #[must_use]
    pub fn new(base: &'a dyn ColumnResolver, schema: &'a Schema, names: &'a [FieldName]) -> Self {
        Self {
            base,
            schema,
            names,
        }
    }
}

impl ColumnResolver for SchemaNameResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let column = self.resolve_column(path)?;
        Some((
            usize::try_from(column.index).ok()?,
            column.ret_type.clone()?,
            column.unique_id,
        ))
    }

    /// narrowing: [`ColumnResolver::resolve_column`] answers `Option`, so an
    /// AMBIGUOUS reference (Go's 1052 from `FindFieldName`) collapses into the
    /// same "unresolved" answer as an unknown one. The distinction is
    /// preserved in [`SimpleExprError::NonUniqueColumn`] for callers that
    /// reach [`crate::find_field_name`] directly; carrying it through the
    /// rewrite would require an error channel the resolver trait does not
    /// have.
    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let index = find_field_name(self.names, &qualified_name_of(path)).ok()??;
        self.schema.columns.get(index).cloned()
    }

    fn resolve_default(&self, path: &[String]) -> Option<Expression> {
        self.base.resolve_default(path)
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.base.time_zone()
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.base.date_modes()
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.base.connection_charset_info()
    }

    fn tidb_info_len(&self) -> usize {
        self.base.tidb_info_len()
    }

    fn like_default_escape(&self) -> u8 {
        self.base.like_default_escape()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.base.no_unsigned_subtraction()
    }

    fn div_precision_increment(&self) -> u32 {
        self.base.div_precision_increment()
    }

    fn current_database(&self) -> Option<String> {
        self.base.current_database()
    }

    fn fold_mode(&self) -> ConstantFoldMode {
        self.base.fold_mode()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
        self.base.fold_constant(expression, mode);
    }
}

/// The `ast.ColumnName` a rewriter path denotes: `["db","t","a"]`,
/// `["t","a"]` or `["a"]`.
fn qualified_name_of(path: &[String]) -> QualifiedColumnName {
    let part = |offset: usize| -> IdentifierMetadata {
        path.len()
            .checked_sub(offset)
            .and_then(|at| path.get(at))
            .map(|raw| IdentifierMetadata::from_parts(raw, CiString::new(raw).lowercase()))
            .unwrap_or_default()
    };
    QualifiedColumnName {
        database: part(3),
        table: part(2),
        column: part(1),
    }
}

/// Go `ParseSimpleExpr` (`simple_rewriter.go:37`): parses `expr_str` as the
/// sole select field of `select <expr_str>` and builds it.
pub fn parse_simple_expr(
    ctx: &dyn ColumnResolver,
    expr_str: &str,
    options: &BuildOptions,
) -> Result<Expression, SimpleExprError> {
    if expr_str.is_empty() {
        // Go asserts in intest builds and returns this exact message
        // otherwise, because reaching it means a caller bug.
        return Err(SimpleExprError::EmptyExpressionString);
    }
    let node = parse_select_field_expr(expr_str)?;
    build_simple_expr(ctx, &node, options)
}

/// Go `ParseSimpleExprWithTableInfo` (`simple_rewriter.go:31`), kept because
/// Go keeps it: a deprecated shorthand for `ParseSimpleExpr` with
/// `WithTableInfo("", tableInfo)`.
pub fn parse_simple_expr_with_table_info<C: ColumnInfoSource>(
    ctx: &dyn ColumnResolver,
    ids: &dyn PlanColumnIdAllocator,
    expr_str: &str,
    table_name: &CiString,
    col_infos: &[C],
) -> Result<Expression, SimpleExprError> {
    let options = BuildOptions::new().with_table_info(ctx, ids, "", table_name, col_infos)?;
    parse_simple_expr(ctx, expr_str, &options)
}

/// The `select <expr>` trick both `ParseSimpleExpr` and
/// `generatedexpr.ParseExpression` use to reach the expression grammar.
fn parse_select_field_expr(expr_str: &str) -> Result<Expr, SimpleExprError> {
    let stmt = tidb_parser::parse(&format!("select {expr_str}"))
        .map_err(|error| SimpleExprError::Parse(error.message))?;
    let Stmt::Query(query) = stmt else {
        return Err(SimpleExprError::Parse("expected a query".to_owned()));
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        return Err(SimpleExprError::Parse("expected a SELECT".to_owned()));
    };
    match select.fields.fields().first() {
        Some(SelectField::Expr { expr, .. }) => Ok(expr.clone()),
        _ => Err(SimpleExprError::Parse(
            "expected an expression field".to_owned(),
        )),
    }
}

/// Go `BuildSimpleExpr` (`expression.go:126`, implemented by
/// `pkg/planner/core/expression_rewriter.go:108` `buildSimpleExpr`): builds an
/// expression from one AST node with limited context.
///
/// Subqueries, window and aggregate functions and the other planner-only
/// constructs Go lists are outside this crate's rewriter as well, so they fail
/// as `EvalError::Unsupported` rather than being silently accepted.
pub fn build_simple_expr(
    ctx: &dyn ColumnResolver,
    node: &Expr,
    options: &BuildOptions,
) -> Result<Expression, SimpleExprError> {
    if options.input_schema.is_none() && !options.input_names.is_empty() {
        return Err(SimpleExprError::NamesWithoutSchema);
    }
    if let Some(schema) = &options.input_schema {
        if schema.columns.len() != options.input_names.len() {
            return Err(SimpleExprError::SchemaNamesLengthMismatch);
        }
    }

    // Go falls back to an EMPTY schema when no scope was supplied, so an
    // unqualified name is "unknown column" rather than a panic.
    let empty = Schema::default();
    let schema = options.input_schema.as_ref().unwrap_or(&empty);
    let resolver = SchemaNameResolver::new(ctx, schema, &options.input_names);

    let expr = rewrite_expr_resolved(node, &resolver)?;
    match &options.target_field_type {
        Some(target) => Ok(build_cast_function(expr, target.clone())?),
        None => Ok(expr),
    }
}

/// Go `BuildCastFunction(ctx, expr, tp)` restricted to what
/// `WithCastExprTo` needs: wrap `expr` in the cast signature that produces
/// `target`.
///
/// Go picks the signature from `tp.EvalType()`; this port switches on the
/// type CODE first so that `YEAR`, `DATE`, `TIME` and `JSON` -- which share an
/// eval type with a wider class -- keep their own cast, exactly as Go's
/// per-type `castAs*` selection does. The result type is the caller's own
/// `target`, so its flen/decimal/charset drive evaluation.
pub(crate) fn build_cast_function(
    expr: Expression,
    target: FieldType,
) -> Result<Expression, EvalError> {
    let unsigned = target.flags() & FieldTypeFlags::UNSIGNED != 0;
    let name = match target.code() {
        FieldTypeCode::Year => "cast_year",
        FieldTypeCode::Date | FieldTypeCode::NewDate => "cast_date",
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => "cast_datetime",
        FieldTypeCode::Duration => "cast_time",
        FieldTypeCode::NewDecimal => "cast_decimal",
        FieldTypeCode::Float | FieldTypeCode::Double => "cast_double",
        FieldTypeCode::Json => "cast_json",
        FieldTypeCode::VectorFloat32 => "cast_vector",
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Bit => {
            if unsigned {
                "cast_unsigned"
            } else {
                "cast_signed"
            }
        }
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::Enum
        | FieldTypeCode::Set => {
            if target.charset_name() == "binary" {
                "cast_binary"
            } else {
                "cast_char"
            }
        }
        _ => return Err(EvalError::Unsupported("this cast target is not ported")),
    };
    Ok(Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        target,
        vec![expr],
    )))
}

/// Go `composeConditionWithBinaryOp` (`expression.go:825`): folds
/// `conditions` into a BALANCED binary tree, which is what keeps the
/// coprocessor's protobuf encoder/decoder shallow.
///
/// `None` is Go's nil for an empty slice; a single condition is returned
/// untouched.
fn compose_condition_with_binary_op(
    mut conditions: Vec<Expression>,
    func_name: &str,
) -> Option<Expression> {
    match conditions.len() {
        0 => None,
        1 => conditions.pop(),
        length => {
            let right = conditions.split_off(length / 2);
            let left = compose_condition_with_binary_op(conditions, func_name)?;
            let right = compose_condition_with_binary_op(right, func_name)?;
            let ret_type = crate::builtin_op::infer_op_type(func_name)
                .expect("`and`/`or` are in the logical-op result-type table");
            Some(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(func_name),
                ret_type,
                vec![left, right],
            )))
        }
    }
}

/// Go `ComposeCNFCondition` (`expression.go:842`): the conjunction of
/// `conditions` as a balanced `AND` tree.
#[must_use]
pub fn compose_cnf_condition(conditions: Vec<Expression>) -> Option<Expression> {
    compose_condition_with_binary_op(conditions, "and")
}

/// Go `ComposeDNFCondition` (`expression.go:847`): the disjunction of
/// `conditions` as a balanced `OR` tree.
#[must_use]
pub fn compose_dnf_condition(conditions: Vec<Expression>) -> Option<Expression> {
    compose_condition_with_binary_op(conditions, "or")
}

/// Go `extractColumns` (`util.go:263`): the private walk both public
/// extractors share.
fn extract_columns_into(
    result: &mut BTreeMap<i64, Column>,
    expr: &Expression,
    filter: Option<&dyn Fn(&Column) -> bool>,
) {
    match expr {
        Expression::Column(column) => {
            if filter.is_none_or(|keep| keep(column)) {
                result.insert(column.unique_id, column.clone());
            }
        }
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_columns_into(result, arg, filter);
            }
        }
        _ => {}
    }
}

/// Go `ExtractColumns` (`util.go:127`): every distinct `*Column` under
/// `expr`, deduplicated by `UniqueID` and sorted by it.
///
/// Go deduplicates through a map and then sorts, precisely because a map's
/// iteration order is not stable; a `BTreeMap` gives the same set in the same
/// order without the sort.
#[must_use]
pub fn extract_columns(expr: &Expression) -> Vec<Column> {
    let mut result = BTreeMap::new();
    extract_columns_into(&mut result, expr, None);
    result.into_values().collect()
}

/// Go `ExtractColumnsFromExpressions` (`util.go:164`): [`extract_columns`]
/// over a batch, with an optional filter applied while walking so a caller
/// never allocates the columns it would discard.
#[must_use]
pub fn extract_columns_from_expressions(
    exprs: &[Expression],
    filter: Option<&dyn Fn(&Column) -> bool>,
) -> Vec<Column> {
    if exprs.is_empty() {
        return Vec::new();
    }
    let mut result = BTreeMap::new();
    for expr in exprs {
        extract_columns_into(&mut result, expr, filter);
    }
    result.into_values().collect()
}

/// Go `ExtractCorColumns` (`util.go:140`): the correlated columns under
/// `expr`, in walk order and WITHOUT deduplication -- Go appends, so a column
/// referenced twice appears twice.
#[must_use]
pub fn extract_cor_columns(expr: &Expression) -> Vec<CorrelatedColumn> {
    let mut result = Vec::new();
    extract_cor_columns_into(&mut result, expr);
    result
}

fn extract_cor_columns_into(result: &mut Vec<CorrelatedColumn>, expr: &Expression) {
    match expr {
        Expression::CorrelatedColumn(column) => result.push(column.clone()),
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_cor_columns_into(result, arg);
            }
        }
        _ => {}
    }
}

/// Go `Expression.ResolveIndices(schema)` restricted to the node kinds a
/// simple expression can contain: rebinds every `Column`'s `Index` to its
/// POSITION in `schema`.
///
/// `ColumnInfos2ColumnsAndNamesWithCollate` needs this because a column's
/// `Index` starts as its offset in the TABLE, which is not its position in a
/// schema built over a subset of the table's columns.
pub fn resolve_indices_in_place(
    expr: &mut Expression,
    schema: &Schema,
) -> Result<(), SimpleExprError> {
    match expr {
        Expression::Column(column) => {
            let at = schema.column_index(column);
            if at < 0 {
                return Err(SimpleExprError::ColumnNotInSchema(column.unique_id));
            }
            column.index = at as i64;
            Ok(())
        }
        Expression::ScalarFunction(function) => {
            for arg in &mut function.args {
                resolve_indices_in_place(arg, schema)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Go `ColumnInfos2ColumnsAndNames` (`expression.go:1109`):
/// [`column_infos_to_columns_and_names_with_collate`] under the process-wide
/// collation mode.
pub fn column_infos_to_columns_and_names<C: ColumnInfoSource>(
    ctx: &dyn ColumnResolver,
    ids: &dyn PlanColumnIdAllocator,
    db_name: &IdentifierMetadata,
    tbl_name: &CiString,
    col_infos: &[C],
) -> Result<(Vec<Column>, Vec<FieldName>), SimpleExprError> {
    column_infos_to_columns_and_names_with_collate(
        ctx,
        ids,
        db_name,
        tbl_name,
        col_infos,
        tidb_datatype::new_collation_enabled(),
    )
}

/// Go `ColumnInfos2ColumnsAndNamesWithCollate` (`expression.go:1115`): turns
/// column metadata into planner columns plus their field names, then resolves
/// each VIRTUAL generated column's expression against the columns just built.
///
/// The two-pass shape is Go's and is load-bearing: a generated column may name
/// a column that appears after it, so every column must exist before any
/// expression is built.
pub fn column_infos_to_columns_and_names_with_collate<C: ColumnInfoSource>(
    ctx: &dyn ColumnResolver,
    ids: &dyn PlanColumnIdAllocator,
    db_name: &IdentifierMetadata,
    tbl_name: &CiString,
    col_infos: &[C],
    use_new_collate: bool,
) -> Result<(Vec<Column>, Vec<FieldName>), SimpleExprError> {
    let table = IdentifierMetadata::from_parts(tbl_name.original(), tbl_name.lowercase());
    let mut columns = Vec::with_capacity(col_infos.len());
    let mut names = Vec::with_capacity(col_infos.len());
    for col in col_infos {
        let column_name = IdentifierMetadata::from_parts(
            col.column_name().original(),
            col.column_name().lowercase(),
        );
        let name = FieldName::new(FieldNameMetadata {
            original_table: table.clone(),
            original_column: column_name.clone(),
            database: db_name.clone(),
            table: table.clone(),
            column: column_name,
        });
        let mut column = Column::new(ids.alloc_plan_column_id(), col.column_field_type().clone());
        column.id = col.column_id();
        column.index = col.column_offset();
        // Go reads `names[i].String()` -- the name built just above, before
        // any hidden-column suppression is applied to it.
        column.orig_name = name.display_name();
        column.is_hidden = col.column_hidden();
        columns.push(column);
        names.push(name);
    }

    let mock_schema = Schema::new(columns.clone());
    for (at, col) in col_infos.iter().enumerate() {
        let Some(generated) = col.virtual_generated_expr() else {
            continue;
        };
        // boundary: Go wraps `ctx` with `CtxWithHandleTruncateErrLevel(
        // errctx.LevelIgnore)` on the first virtual column so a generated
        // expression's truncation does not warn twice. This crate's
        // `ColumnResolver` has no truncate-level knob, so the warning
        // suppression has no counterpart; the built expression is the same.
        let node = parse_select_field_expr(generated)?;
        let options = BuildOptions::new()
            .with_input_schema_and_names(mock_schema.clone(), names.clone())
            .with_allow_cast_array(true)
            .with_use_new_collate(use_new_collate);
        let mut virtual_expr = build_simple_expr(ctx, &node, &options)?;
        resolve_indices_in_place(&mut virtual_expr, &mock_schema)?;
        columns[at].virtual_expr = Some(Box::new(virtual_expr));
    }
    Ok((columns, names))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::NoColumns;
    use crate::exprctx::SimplePlanColumnIdAllocator;
    use crate::rewriter::NoResolver;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::Datum;

    /// The `model.ColumnInfo` slice shape the Go tests build inline.
    struct TestColumnInfo {
        name: CiString,
        id: i64,
        offset: i64,
        field_type: FieldType,
        hidden: bool,
        virtual_generated: Option<String>,
    }

    impl TestColumnInfo {
        fn new(name: &str, offset: i64, code: FieldTypeCode) -> Self {
            Self {
                name: CiString::new(name),
                id: offset + 1,
                offset,
                field_type: FieldType::new(code),
                hidden: false,
                virtual_generated: None,
            }
        }

        fn generated(mut self, expr: &str) -> Self {
            self.virtual_generated = Some(expr.to_owned());
            self
        }
    }

    impl ColumnInfoSource for TestColumnInfo {
        fn column_name(&self) -> &CiString {
            &self.name
        }
        fn column_id(&self) -> i64 {
            self.id
        }
        fn column_offset(&self) -> i64 {
            self.offset
        }
        fn column_field_type(&self) -> &FieldType {
            &self.field_type
        }
        fn column_hidden(&self) -> bool {
            self.hidden
        }
        fn virtual_generated_expr(&self) -> Option<&str> {
            self.virtual_generated.as_deref()
        }
    }

    /// Go `TestBuildExpression`'s table: `id string`, `a bigint`, `b bigint`.
    fn test_table() -> Vec<TestColumnInfo> {
        vec![
            TestColumnInfo::new("id", 0, FieldTypeCode::String),
            TestColumnInfo::new("a", 1, FieldTypeCode::LongLong),
            TestColumnInfo::new("b", 2, FieldTypeCode::LongLong),
        ]
    }

    fn eval_row(expr: &Expression, types: &[FieldType], values: &[Datum]) -> Datum {
        let mut chunk = Chunk::new_with_capacity(types, 1);
        for (index, value) in values.iter().enumerate() {
            chunk.append_datum(index, value);
        }
        expr.eval(&NoColumns, chunk.get_row(0)).expect("eval")
    }

    fn row_types() -> Vec<FieldType> {
        vec![
            FieldType::new(FieldTypeCode::String),
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ]
    }

    /// Source: `pkg/planner/core/expression_test.go::TestBuildExpression`, the
    /// `WithTableInfo` / `ParseSimpleExpr` leg. Go builds `(1+a)*(3+b)` twice
    /// -- once from an AST node, once from the string -- asserts the two are
    /// `Equal`, and evaluates both on `("", 1, 2)` and `("", 3, 4)`.
    #[test]
    fn build_expression_with_table_info() {
        let table = test_table();
        let name = CiString::new("t");

        let ids = SimplePlanColumnIdAllocator::new(0);
        let options = BuildOptions::new()
            .with_table_info(&NoResolver, &ids, "", &name, &table)
            .expect("options");
        let node = parse_select_field_expr("(1+a)*(3+b)").expect("parses");
        let expr = build_simple_expr(&NoResolver, &node, &options).expect("builds");

        let ids2 = SimplePlanColumnIdAllocator::new(0);
        let options2 = BuildOptions::new()
            .with_table_info(&NoResolver, &ids2, "", &name, &table)
            .expect("options");
        let expr2 = parse_simple_expr(&NoResolver, "(1+a)*(3+b)", &options2).expect("builds");
        // Go asserts `expr.Equal(evalCtx, expr2)`. `Expression::equal` in this
        // crate is still the Go-`Column`-only spelling (a `ScalarFunction`
        // always answers false), so structural identity is pinned through the
        // hash code, which IS defined over function name plus argument codes.
        let (Expression::ScalarFunction(mut left), Expression::ScalarFunction(mut right)) =
            (expr.clone(), expr2.clone())
        else {
            panic!("`(1+a)*(3+b)` builds a scalar function")
        };
        assert_eq!(left.hash_code(), right.hash_code());

        let types = row_types();
        for (a, b, want) in [(1_i64, 2_i64, 10_i64), (3, 4, 28)] {
            let values = [Datum::Bytes(Vec::new()), Datum::Int(a), Datum::Int(b)];
            assert_eq!(eval_row(&expr, &types, &values), Datum::Int(want));
            assert_eq!(eval_row(&expr2, &types, &values), Datum::Int(want));
        }
    }

    /// Source: `TestBuildExpression`, the `WithInputSchemaAndNames` leg.
    #[test]
    fn build_expression_with_input_schema_and_names() {
        let table = test_table();
        let ids = SimplePlanColumnIdAllocator::new(0);
        let (columns, names) = column_infos_to_columns_and_names(
            &NoResolver,
            &ids,
            &IdentifierMetadata::default(),
            &CiString::new("t"),
            &table,
        )
        .expect("converts");

        let options = BuildOptions::new().with_input_schema_and_names(Schema::new(columns), names);
        let expr = parse_simple_expr(&NoResolver, "(1+a)*(3+b)", &options).expect("builds");
        let values = [Datum::Bytes(Vec::new()), Datum::Int(1), Datum::Int(2)];
        assert_eq!(eval_row(&expr, &row_types(), &values), Datum::Int(10));
    }

    /// Source: `TestBuildExpression`'s "build expression without enough
    /// columns" leg.
    ///
    /// Go reports `[planner:1054]Unknown column 'a' in 'expression'`. This
    /// crate's rewriter has one shared spelling for an unbound name (see the
    /// module's unknown-column boundary), so the assertion pins that spelling
    /// and the Go text is recorded here rather than weakened away.
    #[test]
    fn build_expression_without_enough_columns() {
        let options = BuildOptions::new();
        assert_eq!(
            parse_simple_expr(&NoResolver, "1+a", &options).unwrap_err(),
            SimpleExprError::Build(EvalError::Unsupported("unresolved column reference"))
        );

        let table = test_table();
        let ids = SimplePlanColumnIdAllocator::new(0);
        let options = BuildOptions::new()
            .with_table_info(&NoResolver, &ids, "", &CiString::new("t"), &table)
            .expect("options");
        assert_eq!(
            parse_simple_expr(&NoResolver, "(1+a)*(3+b+c)", &options).unwrap_err(),
            SimpleExprError::Build(EvalError::Unsupported("unresolved column reference"))
        );
    }

    /// Source: `TestBuildExpression`'s `WithCastExprTo` leg: `1+2+3` is
    /// `bigint` on its own and `varchar` (evaluating to the string `6`) once
    /// the option is applied.
    #[test]
    fn build_expression_with_cast_expr_to() {
        let options = BuildOptions::new();
        let expr = parse_simple_expr(&NoResolver, "1+2+3", &options).expect("builds");
        assert_eq!(
            expr.static_type().expect("typed").code(),
            FieldTypeCode::LongLong
        );

        let mut target = FieldType::new(FieldTypeCode::Varchar);
        target.set_charset_name("utf8mb4");
        target.set_collation_name("utf8mb4_bin");
        let options = BuildOptions::new().with_cast_expr_to(target);
        let expr = parse_simple_expr(&NoResolver, "1+2+3", &options).expect("builds");
        assert_eq!(
            expr.static_type().expect("typed").code(),
            FieldTypeCode::Varchar
        );
        let chunk = Chunk::new_empty(&[]);
        assert_eq!(
            expr.eval(&NoColumns, chunk.get_row(0)).expect("eval"),
            Datum::new_string(*b"6")
        );
    }

    /// Go `ParseSimpleExpr`'s empty-string guard.
    #[test]
    fn parse_simple_expr_rejects_an_empty_string() {
        assert_eq!(
            parse_simple_expr(&NoResolver, "", &BuildOptions::new()).unwrap_err(),
            SimpleExprError::EmptyExpressionString
        );
        assert_eq!(
            SimpleExprError::EmptyExpressionString.to_string(),
            "expression should not be an empty string"
        );
    }

    /// Go `buildSimpleExpr`'s two option-consistency checks, with their exact
    /// messages.
    #[test]
    fn build_simple_expr_validates_schema_and_names() {
        let node = parse_select_field_expr("1").expect("parses");

        let mut options = BuildOptions::new();
        options.input_names = vec![FieldName::default()];
        assert_eq!(
            build_simple_expr(&NoResolver, &node, &options).unwrap_err(),
            SimpleExprError::NamesWithoutSchema
        );
        assert_eq!(
            SimpleExprError::NamesWithoutSchema.to_string(),
            "InputSchema and InputNames should be specified at the same time"
        );

        let options = BuildOptions::new().with_input_schema_and_names(
            Schema::new(vec![Column::new(
                1,
                FieldType::new(FieldTypeCode::LongLong),
            )]),
            Vec::new(),
        );
        assert_eq!(
            build_simple_expr(&NoResolver, &node, &options).unwrap_err(),
            SimpleExprError::SchemaNamesLengthMismatch
        );
        assert_eq!(
            SimpleExprError::SchemaNamesLengthMismatch.to_string(),
            "InputSchema and InputNames should be the same length"
        );
    }

    fn int_column(unique_id: i64) -> Expression {
        let mut column = Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
        column.index = unique_id;
        Expression::Column(column)
    }

    fn eq(left: Expression, right: Expression) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![left, right],
        ))
    }

    /// Source: `pkg/expression/util_test.go::BenchmarkExtractColumns`'s
    /// fixture -- five conditions over columns 0..3 plus a constant -- run
    /// through `ComposeCNFCondition` and then `ExtractColumns`, which is
    /// exactly what the benchmark pins.
    #[test]
    fn compose_cnf_condition_and_extract_columns() {
        let long_long = || FieldType::new(FieldTypeCode::LongLong);
        let one = Expression::Constant(crate::constant::Constant::new(Datum::Int(1), long_long()));
        let conditions = vec![
            eq(int_column(0), int_column(1)),
            eq(int_column(1), int_column(2)),
            eq(int_column(2), int_column(3)),
            eq(int_column(3), one.clone()),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("or"),
                long_long(),
                vec![one, int_column(0)],
            )),
        ];
        let expr = compose_cnf_condition(conditions).expect("five conditions compose");

        let columns = extract_columns(&expr);
        assert_eq!(
            columns.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );

        // `ExtractColumnsFromExpressions` over the same leaves, with Go's
        // optional filter applied during the walk.
        let odd = |column: &Column| column.unique_id % 2 == 1;
        let filtered = extract_columns_from_expressions(std::slice::from_ref(&expr), Some(&odd));
        assert_eq!(
            filtered.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(extract_columns_from_expressions(&[], None).is_empty());
    }

    /// `composeConditionWithBinaryOp` builds a BALANCED tree, not a right
    /// spine: four items become `and(and(1,2), and(3,4))`.
    #[test]
    fn compose_condition_is_balanced() {
        let items: Vec<Expression> = (0..4).map(int_column).collect();
        let expr = compose_cnf_condition(items).expect("composes");
        let Expression::ScalarFunction(root) = &expr else {
            panic!("expected a scalar function")
        };
        assert_eq!(root.func_name.lowercase(), "and");
        for (side, ids) in root.args.iter().zip([[0_i64, 1], [2, 3]]) {
            let Expression::ScalarFunction(branch) = side else {
                panic!("expected a balanced inner AND")
            };
            assert_eq!(branch.func_name.lowercase(), "and");
            assert_eq!(
                branch
                    .args
                    .iter()
                    .map(|arg| arg.as_column().expect("column").unique_id)
                    .collect::<Vec<_>>(),
                ids.to_vec()
            );
        }

        // Go returns nil for none and the item itself for one.
        assert!(compose_cnf_condition(Vec::new()).is_none());
        assert!(compose_dnf_condition(Vec::new()).is_none());
        let single = compose_dnf_condition(vec![int_column(7)]).expect("one item");
        assert_eq!(single.as_column().expect("column").unique_id, 7);

        let pair = compose_dnf_condition(vec![int_column(0), int_column(1)]).expect("composes");
        let Expression::ScalarFunction(root) = &pair else {
            panic!("expected a scalar function")
        };
        assert_eq!(root.func_name.lowercase(), "or");
    }

    /// Go `ExtractCorColumns` appends without deduplicating, so a correlated
    /// column named twice appears twice.
    #[test]
    fn extract_cor_columns_keeps_duplicates() {
        let correlated = CorrelatedColumn {
            column: Column::new(9, FieldType::new(FieldTypeCode::LongLong)),
            data: None,
        };
        let expr = eq(
            Expression::CorrelatedColumn(correlated.clone()),
            Expression::CorrelatedColumn(correlated),
        );
        let found = extract_cor_columns(&expr);
        assert_eq!(found.len(), 2);
        assert!(found.iter().all(|c| c.column.unique_id == 9));
        assert!(extract_columns(&expr).is_empty());
    }

    fn field(db: &str, table: &str, column: &str) -> FieldName {
        FieldName::new(FieldNameMetadata {
            original_table: IdentifierMetadata::new(table),
            original_column: IdentifierMetadata::new(column),
            database: IdentifierMetadata::new(db),
            table: IdentifierMetadata::new(table),
            column: IdentifierMetadata::new(column),
        })
    }

    /// [`qualified_name_of`] turns a rewriter path into the `ast.ColumnName`
    /// the already-ported [`crate::find_field_name`] expects, and
    /// [`SchemaNameResolver`] hands back the SCHEMA column itself so its
    /// `ID`/`OrigName` survive the binding.
    ///
    /// (`FindFieldName`'s own source cases live in
    /// `tests/field_name_resolution_source.rs`; they are not re-asserted here.)
    #[test]
    fn schema_name_resolver_binds_qualified_paths() {
        let names = vec![field("test", "t1", "a"), field("test", "t2", "a")];
        let mut schema = Schema::new(vec![
            Column::new(11, FieldType::new(FieldTypeCode::LongLong)),
            Column::new(12, FieldType::new(FieldTypeCode::LongLong)),
        ]);
        schema.columns[0].id = 101;
        schema.columns[0].orig_name = "test.t1.a".to_owned();
        schema.columns[1].index = 5;
        let resolver = SchemaNameResolver::new(&NoResolver, &schema, &names);
        let path = |parts: &[&str]| -> Vec<String> {
            parts.iter().map(|part| (*part).to_owned()).collect()
        };

        let bound = resolver
            .resolve_column(&path(&["test", "t1", "a"]))
            .expect("qualified path resolves");
        assert_eq!(bound.unique_id, 11);
        assert_eq!(bound.id, 101);
        assert_eq!(bound.orig_name, "test.t1.a");

        // The schema column's own `Index` is what Go's `toColumn` returns,
        // not the name-slice position.
        let bound = resolver
            .resolve_column(&path(&["t2", "a"]))
            .expect("table-qualified path resolves");
        assert_eq!(bound.unique_id, 12);
        assert_eq!(bound.index, 5);
        assert_eq!(resolver.resolve(&path(&["t2", "a"])).expect("triple").2, 12);

        assert!(resolver.resolve_column(&path(&["b"])).is_none());
        // Ambiguous (`a` matches both visible fields) reports as unresolved
        // through the resolver, whose Go 1052 spelling is
        // `NonUniqueFieldName`; the error itself is asserted at the
        // `find_field_name` level.
        assert!(resolver.resolve_column(&path(&["a"])).is_none());
        assert!(find_field_name(&names, &qualified_name_of(&path(&["a"]))).is_err());
    }

    /// `ColumnInfos2ColumnsAndNamesWithCollate`: ids come from the allocator,
    /// `OrigName` is the field name's `String()`, `Index` starts as the
    /// table offset, and a virtual generated column gets a resolved
    /// expression.
    #[test]
    fn column_infos_to_columns_and_names_builds_virtual_expressions() {
        let table = vec![
            TestColumnInfo::new("a", 0, FieldTypeCode::LongLong),
            TestColumnInfo::new("b", 1, FieldTypeCode::LongLong),
            TestColumnInfo::new("c", 2, FieldTypeCode::LongLong).generated("a + b"),
        ];
        let ids = SimplePlanColumnIdAllocator::new(0);
        let (columns, names) = column_infos_to_columns_and_names_with_collate(
            &NoResolver,
            &ids,
            &IdentifierMetadata::new("test"),
            &CiString::new("t"),
            &table,
            true,
        )
        .expect("converts");

        assert_eq!(
            columns.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            columns.iter().map(|c| c.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            columns.iter().map(|c| c.index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            columns
                .iter()
                .map(|c| c.orig_name.as_str())
                .collect::<Vec<_>>(),
            vec!["test.t.a", "test.t.b", "test.t.c"]
        );
        assert_eq!(names[2].display_name(), "test.t.c");

        assert!(columns[0].virtual_expr.is_none());
        let generated = columns[2].virtual_expr.as_ref().expect("built");
        let referenced = extract_columns(generated);
        assert_eq!(
            referenced.iter().map(|c| c.unique_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            referenced.iter().map(|c| c.index).collect::<Vec<_>>(),
            vec![0, 1]
        );

        let types = vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        assert_eq!(
            eval_row(generated, &types, &[Datum::Int(4), Datum::Int(5)]),
            Datum::Int(9)
        );
    }

    /// Regression: a column BENEATH a function keeps the schema column's
    /// identity, not just its `(index, type, unique id)`.
    ///
    /// The rewriter wraps its resolver in a fold-mode child scope before
    /// building a function's arguments; when that wrapper did not forward
    /// [`ColumnResolver::resolve_column`], every column argument came back
    /// with `ID`/`OrigName`/`VirtualExpr` cleared, which silently made a
    /// virtual generated column look like an ordinary one to
    /// `pkg/ddl/copr`'s `GetCondition`.
    #[test]
    fn a_column_under_a_function_keeps_its_identity() {
        let table = vec![
            TestColumnInfo::new("a", 0, FieldTypeCode::LongLong),
            TestColumnInfo::new("b", 1, FieldTypeCode::LongLong),
            TestColumnInfo::new("c", 2, FieldTypeCode::LongLong).generated("a + b"),
        ];
        let ids = SimplePlanColumnIdAllocator::new(0);
        let (columns, names) = column_infos_to_columns_and_names(
            &NoResolver,
            &ids,
            &IdentifierMetadata::new("test"),
            &CiString::new("t"),
            &table,
        )
        .expect("converts");

        let options = BuildOptions::new().with_input_schema_and_names(Schema::new(columns), names);
        let expr = parse_simple_expr(&NoResolver, "c > 1 and a > 0", &options).expect("builds");
        let referenced = extract_columns(&expr);
        assert_eq!(
            referenced.iter().map(|c| c.id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            referenced
                .iter()
                .map(|c| c.orig_name.as_str())
                .collect::<Vec<_>>(),
            vec!["test.t.a", "test.t.c"]
        );
        // `c` is generated, so its expression must survive the binding.
        assert!(referenced[0].virtual_expr.is_none());
        assert!(referenced[1].virtual_expr.is_some());
    }

    /// `resolve_indices_in_place` is what makes a SUBSET schema work: the
    /// column's `Index` is its table offset until the schema it will be
    /// evaluated against remaps it.
    #[test]
    fn resolve_indices_remaps_a_subset_schema() {
        let mut column = Column::new(42, FieldType::new(FieldTypeCode::LongLong));
        column.index = 7;
        let schema = Schema::new(vec![
            Column::new(41, FieldType::new(FieldTypeCode::LongLong)),
            column.clone(),
        ]);
        let mut expr = eq(Expression::Column(column), int_column(41));
        // `int_column(41)` shares the schema's first unique id.
        resolve_indices_in_place(&mut expr, &schema).expect("resolves");
        let Expression::ScalarFunction(function) = &expr else {
            panic!("expected a scalar function")
        };
        assert_eq!(function.args[0].as_column().expect("column").index, 1);
        assert_eq!(function.args[1].as_column().expect("column").index, 0);

        let mut missing = int_column(99);
        assert_eq!(
            resolve_indices_in_place(&mut missing, &schema),
            Err(SimpleExprError::ColumnNotInSchema(99))
        );
    }
}
