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
//! * The value is computed from the row by the statement that writes it
//!   (the mutation caller of `materialize_with`). An expression may only
//!   read non-generated columns and generated columns defined EARLIER, so a
//!   left-to-right pass sees the final converted values. That is
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
//!   restored by evaluation in the reading statement's context.
//! * An index over a generated column is built from the materialized row, so
//!   it stores the computed value with no index-specific code at all. An index
//!   backfill uses the row decoder's separate evaluation and conversion
//!   contract rather than an INSERT statement's NULL/error policy.
//! * The expression is evaluated under the SQL MODE of the statement that
//!   writes the row, exactly as every other expression of that statement is
//!   (the mutation caller's `ctx`). There is no separate rule for generated
//!   columns and no special case per condition: `100/a` over `a = 0` fails the
//!   write with 1365 under `ERROR_FOR_DIVISION_BY_ZERO` and stores NULL
//!   without it, because that is what `errctx.ErrGroupDividedByZero` says for
//!   an `INSERT`/`UPDATE`/`DELETE`. Evaluating with no context at all made
//!   that flag unreachable, which turned every such write into a silently
//!   stored NULL. A READ evaluates at the query level instead -- also not a
//!   special case, just the level Go's `SELECT` carries -- so restoring a
//!   virtual column warns and reads NULL.
//!
//! Mutation policy belongs to the statement owner. INSERT, UPDATE and ODKU
//! use the shared column-cast/error/NULL handlers before storage and constraints
//! consume the row. Table insert/update do not evaluate it again: warnings and
//! expression evaluation are observable effects, even when values are unchanged.
//! Row decoding and ANALYZE use the shared raw column cast with their own
//! type flags. Virtual readers additionally clip unsigned values and replace
//! constrained NULLs; general row decoding keeps Go's separate contract.
//! Shared column casting also retains documented datatype/error-identity gaps.
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
//! # Planner substitution
//!
//! Go rewrites a predicate like `a+1=3` into the indexed virtual generated
//! column that stores `a+1`, so the index can serve the query
//! (`pkg/planner/core/rule_generate_column_substitute.go`). The catalog bridge
//! retains each virtual generated column's AST; `PlanBuilder` resolves that
//! AST against the complete newborn `DataSource` schema and attaches the
//! resulting expression to the planner column. The ordinary
//! `GcSubstituter` rule then performs Go's typed expression comparison and
//! schema-presence checks. Execution does not carry a second substitution
//! path.


use tidb_terror::MysqlError;

use tidb_datatype::FieldType;

/// Go `expression.IllegalFunctions4GeneratedColumns`.
const DISALLOWED_FUNCTIONS: &[&str] = &[
    "benchmark",
    "connection_id",
    "curdate",
    "current_date",
    "current_resource_group",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "curtime",
    "database",
    "found_rows",
    "get_lock",
    "getvar",
    "is_free_lock",
    "is_used_lock",
    "json_merge",
    "last_insert_id",
    "load_file",
    "localtime",
    "localtimestamp",
    "name_const",
    "now",
    "rand",
    "random_bytes",
    "release_all_locks",
    "release_lock",
    "row_count",
    "row",
    "schema",
    "session_user",
    "setvar",
    "sleep",
    "sysdate",
    "system_user",
    "tidb_bounded_staleness",
    "tidb_current_tso",
    "tidb_is_ddl_owner",
    "tidb_row_checksum",
    "tidb_version",
    "unix_timestamp",
    "user",
    "utc_date",
    "utc_time",
    "utc_timestamp",
    "uuid",
    "uuid_v4",
    "uuid_v7",
    "uuid_short",
    "values",
    "version",
];

pub fn is_disallowed_generated_function(name: &str) -> bool {
    DISALLOWED_FUNCTIONS
        .iter()
        .any(|blocked| name.eq_ignore_ascii_case(blocked))
}


use tidb_table::generated_column::*;

/// Go `findDependedColumnNames` over `FindColumnNamesInExpr`: whether the
/// column is generated, and the `Name.L` of every column its expression
/// names, in first-seen order. A qualifier naming another database is 1102,
/// another table 1103.
pub fn find_depended_column_names(
    schema: &str,
    table: &str,
    def: &tidb_ast::ColumnDef,
) -> Result<(bool, Vec<String>), MysqlError> {
    #[derive(Default)]
    struct ColumnNames(Vec<Vec<String>>);
    impl tidb_ast::Visitor for ColumnNames {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_mut::<tidb_ast::Expr>() {
                self.0.push(path.clone());
                return true;
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let Some(tidb_ast::ColumnOption::Generated { expression, .. }) = def
        .options
        .iter()
        .find(|option| matches!(option, tidb_ast::ColumnOption::Generated { .. }))
    else {
        return Ok((false, Vec::new()));
    };
    let mut names = ColumnNames::default();
    tidb_ast::Visitable::accept(&mut expression.clone(), &mut names);
    let (schema, table) = (
        tidb_hack::go_to_lower(schema),
        tidb_hack::go_to_lower(table),
    );
    let mut dependences = Vec::new();
    for path in names.0 {
        let (db, tbl, column) = match path.as_slice() {
            [column] => (None, None, column),
            [tbl, column] => (None, Some(tbl), column),
            [db, tbl, column, ..] => (Some(db), Some(tbl), column),
            [] => continue,
        };
        if let Some(db) = db.filter(|db| !db.is_empty() && !schema.is_empty()) {
            if tidb_hack::go_to_lower(db) != schema {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_WRONG_DB_NAME
                        .generate_with_stack_by_args(&[db.into()]),
                ));
            }
        }
        if let Some(tbl) = tbl.filter(|tbl| !tbl.is_empty() && !table.is_empty()) {
            if tidb_hack::go_to_lower(tbl) != table {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_WRONG_TABLE_NAME
                        .generate_with_stack_by_args(&[tbl.into()]),
                ));
            }
        }
        let column = tidb_hack::go_to_lower(column);
        if !dependences.contains(&column) {
            dependences.push(column);
        }
    }
    Ok((true, dependences))
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
    build_generated_columns_with_like_default_escape(
        defs,
        names,
        types,
        zone,
        b'\\',
        false,
        tidb_mysql::DefaultCharset,
    )
}

/// Statement-aware generated-column builder. The implicit `LIKE` escape is
/// captured with the DDL statement and retained by its expression cache;
/// `connection_charset` is the DDL session's `character_set_connection`,
/// which Go's parser gave every plain string literal of the expression and
/// which its restored text therefore writes as the `_charset` introducer.
pub fn build_generated_columns_with_like_default_escape(
    defs: &[tidb_ast::ColumnDef],
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
    auto_increment_in_generated: bool,
    connection_charset: &str,
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
    let mut dependencies_at = Vec::new();
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
        crate::expression_index::check_illegal_fn_4_generated(
            &tidb_hack::go_to_lower(&def.name),
            crate::expression_index::GenType::Column,
            expression,
        )
        .map_err(GeneratedDdlError::IllegalFunction)?;
        let resolver = TableColumnResolver::with_like_default_escape(
            names,
            types,
            zone.clone(),
            like_default_escape,
        );
        let expr = tidb_expr::rewriter::rewrite_expr_resolved(expression, &resolver)
            .map_err(|error| rewrite_error(error, resolver.missing_name()))?;
        // A rewrite can succeed while an unrelated branch of the tree failed
        // to resolve only if the resolver was never consulted, so the missing
        // name is still the authority on 1054.
        if let Some(name) = resolver.missing_name() {
            return Err(GeneratedDdlError::UnknownDependency(name));
        }
        dependencies_at.push((position, resolver.dependencies()));
        built.push(Some(GeneratedColumn {
            expr_text: expression.restore_with_flags_and_string_charset(
                generated_restore_flags(),
                connection_charset,
            ),
            stored: *stored,
            expr,
            dependencies: resolver.dependency_names(),
            source: expression.clone(),
            build_zone: zone.clone(),
            build_like_default_escape: like_default_escape,
            zone_sensitive: resolver.zone_was_read(),
            like_default_escape_sensitive: resolver.like_default_escape_was_read(),
        }));
    }
    // Go `checkGeneratedColumn`: once every column is read, a generated
    // column may not read the AUTO_INCREMENT column unless
    // `tidb_enable_auto_increment_in_generated` allows it ...
    let auto_increment = defs.iter().rposition(|def| {
        def.options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement))
    });
    if let Some(auto_increment) = auto_increment.filter(|_| !auto_increment_in_generated) {
        if let Some((position, _)) = dependencies_at
            .iter()
            .find(|(_, dependencies)| dependencies.contains(&auto_increment))
        {
            return Err(GeneratedDdlError::RefAutoInc(tidb_hack::go_to_lower(
                &defs[*position].name,
            )));
        }
    }
    // ... and `verifyColumnGeneration`: a generated column may refer only to
    // generated columns occurring EARLIER. A later NON-generated column is
    // fine -- captured from Go, `create table t (a int, b int as (c+1), c
    // int)` is accepted.
    for (position, dependencies) in &dependencies_at {
        if dependencies
            .iter()
            .any(|dependency| generated_at[*dependency] && *position <= *dependency)
        {
            return Err(GeneratedDdlError::NonPrior);
        }
    }
    Ok(built)
}

/// Builds ONE generated column for `ALTER TABLE ... ADD COLUMN <name> <type>
/// AS (expr) VIRTUAL`, against the supplied existing-column namespace.
///
/// `names`/`types` are normally the table's complete current columns. The
/// caller performs Go's position-sensitive `verifyColumnGenerationSingle`
/// check after this build, because resolving against the full namespace is
/// what preserves Go's 1054-before-3107 validation order. Unit callers that
/// intentionally provide only preceding columns still get the same builder
/// behavior and can rely on construction to make prior-order valid.
///
/// STORED is the caller's refusal, not this function's: Go answers 3106
/// `'Adding generated stored column through ALTER TABLE' is not supported for
/// generated columns.` before any expression is built (captured).
pub fn build_added_generated_column(
    name: &str,
    expression: &tidb_ast::Expr,
    stored: bool,
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<GeneratedColumn, GeneratedDdlError> {
    build_added_generated_column_with_like_default_escape(
        name,
        expression,
        stored,
        names,
        types,
        zone,
        b'\\',
        tidb_mysql::DefaultCharset,
    )
}

/// Statement-aware form of [`build_added_generated_column`];
/// `connection_charset` as for
/// [`build_generated_columns_with_like_default_escape`].
#[allow(clippy::too_many_arguments)]
pub fn build_added_generated_column_with_like_default_escape(
    name: &str,
    expression: &tidb_ast::Expr,
    stored: bool,
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
    connection_charset: &str,
) -> Result<GeneratedColumn, GeneratedDdlError> {
    crate::expression_index::check_illegal_fn_4_generated(
        &tidb_hack::go_to_lower(name),
        crate::expression_index::GenType::Column,
        expression,
    )
    .map_err(GeneratedDdlError::IllegalFunction)?;
    let mut generated =
        resolve_generated_expr(expression, stored, names, types, zone, like_default_escape)?;
    generated.expr_text = expression
        .restore_with_flags_and_string_charset(generated_restore_flags(), connection_charset);
    Ok(generated)
}

