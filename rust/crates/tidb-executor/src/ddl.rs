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

//! `CREATE TABLE` execution: builds a `tidb_model::TableInfo` from the parsed
//! statement and registers a TiKV-byte-backed table for it -- tying the
//! transcreated metadata structs into the runnable path.
//!
//! This is the metadata slice of Go's `pkg/ddl` `buildTableInfo` /
//! `buildColumnAndConstraint`: column types map through `str_to_type` (Go
//! `types.StrToType`) with flen/decimal from the type arguments and the
//! unsigned flag, and charset/collation through [`field_type_of`]'s
//! transcreation of Go `ResolveCharsetCollation` (see its doc for the exact
//! precedence). DEFERRED (documented): `CREATE TABLE ... LIKE`, and the
//! schema-version/DDL-job machinery (the driver applies metadata directly; the
//! DDL job queue is a separate tier). Constraints/indexes and the column
//! options are no longer deferred; see [`crate::column_default`] for what a
//! `DEFAULT` may be.
//!
//! # `PARTITION BY` is REFUSED here, not ignored
//!
//! This builder used to skip `CreateTableStmt::partitioning` entirely, so a
//! partitioned `CREATE TABLE` SUCCEEDED and produced an UNPARTITIONED table --
//! a wrong table rather than a missing feature, since `SHOW CREATE TABLE`
//! printed no `PARTITION BY` clause, pruning had nothing to prune, and
//! `SELECT ... PARTITION (p0)` could not mean what it said. It now refuses
//! through [`table_partition::refuse_table_partitioning`], which is what the
//! sibling metadata builder `tidb_exec::table_info_build` already did; the two
//! no longer disagree about the same statement.
//!
//! Refusing was itself a cascade decision, taken deliberately: it moved
//! `table not found in catalog` up sharply, because those tables now honestly
//! do not exist. What it bought is that 1,171 statements stopped being
//! compared against a table that was never partitioned -- `table/partition`
//! and `planner/core/partition_pruner` went to ZERO divergences and onboarded.
//! See [`table_partition`]'s module doc for Go's captured `SHOW CREATE TABLE`
//! text and the eleven definitions TiDB rejects at CREATE, which are pinned so
//! that real partitioning cannot land without its validation.
//!
//! # Where a resolved collation is, and is NOT, consulted
//!
//! DDL-time resolution is complete: every string column carries its real
//! charset and collation, and every metadata surface (`SHOW FULL COLUMNS`,
//! `SHOW CREATE TABLE`, `SHOW TABLE STATUS`, `information_schema.columns`)
//! reports them, so a `VARBINARY` is distinguishable from a `VARCHAR`. The
//! write path validates a column's bytes against its charset, so a non-UTF-8
//! string is rejected by a `utf8mb4` column and accepted by a binary one.
//!
//! Expression-level derivation is complete too: `tidb_expr::collation_derive`
//! transcreates Go's `CheckAndDeriveCollationFromExprs`/`deriveCollation`, so a
//! comparison's collation is aggregated from its operands by coercibility
//! (EXPLICIT `COLLATE` > IMPLICIT column > SYSCONST > COERCIBLE literal >
//! NUMERIC > IGNORABLE `NULL`) and stamped on the function's result type, which
//! is where the comparer, `LIKE`, `IN`, `INSTR`/`LOCATE`/`STRCMP`, and the
//! sort/group key comparers all read it from. The previously documented
//! byte-wise divergences are GRADUATED and covered by
//! `tidb-session`'s `tests_collation`: `_ci` `=`/`<>`/`<`/`IN`/`BETWEEN`/
//! `LIKE`/`ORDER BY`/`GROUP BY`, `binary`'s NO PAD against `utf8mb4_bin`'s PAD
//! SPACE, the collation-aware string builtins, and the exact 1267/1271
//! "Illegal mix of collations" and 1253 charset-mismatch texts.
//!
//! The non-Unicode charsets are GRADUATED too, and covered by
//! `tidb-session`'s `tests_charset`. A `gbk`/`gb18030` column validates its
//! writes against the charset (1366 for an unrepresentable character),
//! defaults to `gbk_chinese_ci`/`gb18030_chinese_ci` and orders by those
//! weights, and transcodes at exactly the boundary Go transcodes at -- the
//! implicit `to_binary` wrap on a binary-aware function's argument, which is
//! why `HEX`/`LENGTH`/`ASCII`/`CAST(... AS BINARY)` report the GBK form while
//! the stored bytes stay UTF-8. `CONVERT(x USING cs)` retags and
//! `?`-replaces. `latin1` and `ascii` need no transcode at all (Go's
//! `isLegacyCharset`); see `tidb_expr::convert_charset` for the whole seam
//! and its captured evidence.
//!
//! DIVERGENCE (documented, captured, and NOT yet fixed) in what remains:
//!
//! * A `_charset'literal'` introducer (`_gbk'...'`) is a 1064 PARSE error
//!   here; TiDB answers 1115 "Unsupported character introducer: 'gbk'" from
//!   its own parser, so both refuse the form but under different codes. An
//!   unknown collation spelled in `COLLATE` is likewise 1064 here against
//!   TiDB's `ddl:1273 Unknown collation: '...'`.
//! * Go's `from_binary` half of `HandleBinaryLiteral` (a binary argument
//!   flowing into a non-binary string result) is not wired: it needs the
//!   DERIVED result charset of the whole function, and no captured case
//!   reaches it.
//! * `deriveCollation`'s `DATE_FORMAT`/`TIME_FORMAT`, `CASE` (Go's own comment
//!   marks its aggregation incorrect), `FIELD`, and `CAST`-to-string arms fall
//!   into the default arm here, which gives them the connection collation
//!   rather than an aggregated one.
//! * A comparison between a string and a NUMBER promotes to REAL and consults
//!   no collation (matching Go), so no collation-mismatch error can arise
//!   there even when TiDB's own planner would have rewritten the expression.

mod alter_metadata;
mod alter_table;
mod column_types;
mod indexes;
mod table_constraints;
mod table_lifecycle;
mod table_partition;

pub use alter_table::run_alter_table_in;

use alter_table::normalize_column_default;
use column_types::{field_type_of, table_charset_of, NOT_NULL_FLAG};
pub use indexes::{run_create_index_in, run_drop_index_in};
use table_constraints::{
    is_int_column, primary_key_column, table_foreign_keys, table_indexes, AUTO_INCREMENT_FLAG,
    PRI_KEY_FLAG,
};

use indexes::{index_part_names, is_visible};
pub use table_lifecycle::{run_drop_table_in, run_rename_table_in, run_truncate_table_in};

use crate::driver::{Catalog, DriverError};
use crate::kv_table::{FkAction, KvColumn, KvForeignKey, KvIndex, KvTable, TableCharset};
use crate::SchemaErrorKind;
use tidb_ast::CiString;
use tidb_ast::{ColumnDef, ColumnTypeArg, DdlStmt, Stmt};
use tidb_datatype::FieldTypeCode;
use tidb_model::column::ColumnInfo;
use tidb_model::table_info::TableInfo;

/// The `AUTO_INCREMENT [=] n` table option's value, if the list carries one.
///
/// Go reads the same option at CREATE (seeding the allocator) and at ALTER
/// (rebasing it). `FORCE AUTO_INCREMENT`, which lets Go move the counter
/// DOWN, is refused rather than silently treated as the plain form.
fn auto_increment_option(options: &[tidb_ast::TableOption]) -> Result<Option<i64>, DriverError> {
    let mut seed = None;
    for option in options {
        match option {
            tidb_ast::TableOption::AutoIncrement(value) => {
                // Go's parser holds this option in `opt.UintValue` (a `uint64`)
                // and every reader converts with `int64(opt.UintValue)`, so a
                // value above `i64::MAX` becomes negative rather than being
                // rejected -- see `rebase_auto_increment` for what that means.
                seed = Some(value.parse::<u64>().map_err(|_| {
                    DriverError::Unsupported("AUTO_INCREMENT= needs an integer value")
                })? as i64);
            }
            tidb_ast::TableOption::ForceAutoIncrement(_) => {
                return Err(DriverError::Unsupported(
                    "FORCE AUTO_INCREMENT is not supported yet",
                ));
            }
            _ => {}
        }
    }
    Ok(seed)
}

/// Parses and executes a `CREATE TABLE`, building a [`TableInfo`] and
/// registering a TiKV-byte-backed table in `catalog`. Returns whether a table
/// was created (`false` only for `IF NOT EXISTS` over an existing name).
pub fn run_create_table_on(sql: &str, catalog: &mut Catalog) -> Result<bool, DriverError> {
    // A stock session has `tidb_enable_check_constraint` OFF, which is the
    // only mode this tier models; see [`run_create_table_in`].
    run_create_table_in(sql, catalog, tidb_executor_default_database(), true, false)
}

/// How many `CHECK` constraints a `CREATE TABLE` writes, counting both the
/// table-level `[CONSTRAINT name] CHECK (expr)` form and the form written
/// inline on a column.
///
/// Go emits ONE `tidb_enable_check_constraint is off` warning per constraint
/// it discards (captured: a table with two of them produces two warnings), so
/// the session needs the count, not a boolean. It lives here so "what counts
/// as a CHECK constraint" has a single definition shared with the executor's
/// own discard path.
#[must_use]
pub fn check_constraint_count(create: &tidb_ast::CreateTableStmt) -> usize {
    let table_level = create
        .table_constraints
        .iter()
        .filter(|constraint| matches!(constraint, tidb_ast::TableConstraint::Check(_)))
        .count();
    let column_level = create
        .columns
        .iter()
        .flat_map(|column| &column.options)
        .filter(|option| matches!(option, tidb_ast::ColumnOption::Check(_)))
        .count();
    table_level + column_level
}

/// The default schema an unqualified `CREATE TABLE` lands in.
fn tidb_executor_default_database() -> &'static str {
    crate::driver::DEFAULT_DATABASE
}

/// [`run_create_table_on`] creating the table in `current_db`.
///
/// `enable_check_constraint` is `@@global.tidb_enable_check_constraint`, and it
/// decides what a `CHECK` constraint MEANS rather than merely whether it is
/// enforced. Captured from real TiDB (`gorun`, plus `SHOW WARNINGS` through
/// testkit) with the variable at its OFF default:
///
/// * `create table ck (a int, check (a > 0))` succeeds, warning 1105
///   `tidb_enable_check_constraint is off` once per constraint;
/// * `SHOW CREATE TABLE ck` restores `CREATE TABLE \`ck\` (\n  \`a\` int(11)
///   DEFAULT NULL\n) ...` -- with NO `CONSTRAINT ... CHECK` clause, and
///   `information_schema.check_constraints` is empty. The constraint is
///   DISCARDED at DDL time, not stored-but-unenforced;
/// * `insert into ck values (-1)` therefore succeeds.
///
/// So discarding is the faithful behaviour, and storing the constraint would
/// be the divergence: `SHOW CREATE TABLE` would grow a clause TiDB does not
/// print. With the variable ON, TiDB stores the constraint (auto-named
/// `<table>_chk_<N>`), prints it, and enforces it with error 3819; none of
/// that is modelled here, so this refuses rather than silently discarding.
pub fn run_create_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    foreign_key_checks: bool,
    enable_check_constraint: bool,
) -> Result<bool, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let create = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::CreateTable(create) => create,
            _ => {
                return Err(DriverError::Unsupported(
                    "only CREATE TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::Unsupported(
                "only CREATE TABLE is supported here",
            ))
        }
    };

    if enable_check_constraint && check_constraint_count(create) > 0 {
        return Err(DriverError::Unsupported(
            "CHECK constraints are only modelled with tidb_enable_check_constraint off",
        ));
    }

    if create.like_table.is_some() {
        return Err(DriverError::Unsupported("CREATE TABLE LIKE is deferred"));
    }
    // `DROP TEMPORARY TABLE` is already refused here; creating one and
    // storing it as an ORDINARY table is the same gap on the other side, and
    // the more dangerous half: the table then outlives its session, is
    // visible to every other one, and answers statements TiDB refuses on a
    // temporary table outright -- `ADMIN CHECK TABLE` among them (Go's 8006,
    // `preprocessor.checkAdminCheckTableGrammar`). Refusing at CREATE keeps
    // the TEMPORARY keyword from being silently dropped.
    if create.temporary != tidb_ast::CreateTableTemporary::None {
        return Err(DriverError::Unsupported(
            "temporary tables are not supported yet",
        ));
    }
    // Before any metadata is built: this tier stores no `PartitionInfo`, so a
    // partitioned table would silently become an ordinary one. See
    // [`table_partition`]'s module doc for the captured Go answer.
    table_partition::refuse_table_partitioning(create)?;
    if create.columns.is_empty() {
        return Err(DriverError::Unsupported("a table needs columns"));
    }

    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name);
    if catalog.contains_in(&database, name) {
        if create.if_not_exists {
            return Ok(false);
        }
        // Go `infoschema.ErrTableExists` (1050) prints the db-qualified name:
        // "Table 'test.t1' already exists".
        return Err(DriverError::Schema(crate::SchemaErrorKind::TableExists(
            format!("{database}.{name}"),
        )));
    }

    // Build the ColumnInfos (ids 1..n, offsets in definition order).
    let table_charset = table_charset_of(&create.table_options)?;
    let mut columns = Vec::with_capacity(create.columns.len());
    for (i, def) in create.columns.iter().enumerate() {
        let field_type = field_type_of(def, table_charset)?;
        let mut col = ColumnInfo::new((i + 1) as i64, &def.name, field_type);
        col.offset = i as i32;
        columns.push(col);
    }

    // The primary key, written either inline on a column or as a table
    // constraint.
    for (i, def) in create.columns.iter().enumerate() {
        if def
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull))
        {
            columns[i].add_flag(NOT_NULL_FLAG);
        }
    }

    // Go rejects a second auto column with ErrWrongAutoKey (1075) and a
    // non-integer one with "Incorrect column specifier"; captured from real
    // TiDB, which -- unlike MySQL -- does NOT require the column to be a key.
    let mut auto_increment_offset = None;
    for (i, def) in create.columns.iter().enumerate() {
        if !def
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement))
        {
            continue;
        }
        if auto_increment_offset.is_some() {
            return Err(DriverError::WrongAutoKey);
        }
        if !is_int_column(&columns[i]) {
            return Err(DriverError::WrongColumnSpecifier(def.name.clone()));
        }
        // An auto-increment column is implicitly NOT NULL and carries Go's
        // AutoIncrementFlag.
        columns[i].add_flag(NOT_NULL_FLAG | AUTO_INCREMENT_FLAG);
        auto_increment_offset = Some(i);
    }

    let primary_key = primary_key_column(create)?;
    let pk_offsets: Vec<usize> = match &primary_key {
        Some(names) => {
            let mut offsets = Vec::with_capacity(names.len());
            for name in names {
                offsets.push(
                    columns
                        .iter()
                        .position(|col| col.name.original().eq_ignore_ascii_case(name))
                        .ok_or(DriverError::Unsupported(
                            "the primary key names a column the table does not define",
                        ))?,
                );
            }
            offsets
        }
        None => Vec::new(),
    };
    let pk_offset = if pk_offsets.len() == 1 {
        Some(pk_offsets[0])
    } else {
        None
    };
    // Go `isSingleIntPK` + `ShouldBuildClusteredIndex`: a single-column
    // integer primary key becomes the row handle rather than a separate
    // index, which is what `TableInfo.PKIsHandle` records.
    let pk_is_handle = pk_offset.is_some_and(|offset| is_int_column(&columns[offset]));
    // Go `ShouldBuildClusteredIndex` under the default clustered-index mode:
    // a primary key that is not a single integer column becomes a clustered
    // COMMON handle, whose encoding is the row key.
    let common_handle_offsets: Vec<usize> = if pk_is_handle || pk_offsets.is_empty() {
        Vec::new()
    } else {
        pk_offsets.clone()
    };
    for offset in &pk_offsets {
        // Go `checkIndexColumn` reaches a primary key too, and a clustered
        // primary key never becomes an entry in `table_indexes` (its
        // encoding IS the row key), so the JSON refusal has to be repeated
        // on this path rather than being left to the index builder.
        if columns[*offset].field_type.code() == FieldTypeCode::Json {
            return Err(DriverError::JsonUsedInKey(
                columns[*offset].name.original().to_owned(),
            ));
        }
        // A primary key column is implicitly NOT NULL, as in MySQL, and Go
        // marks it PRI (mysql.NotNullFlag, mysql.PriKeyFlag).
        columns[*offset].add_flag(NOT_NULL_FLAG | PRI_KEY_FLAG);
    }

    // Go evaluates a constant DEFAULT at DDL time and stores the value on the
    // ColumnInfo; a NOT NULL column with no DEFAULT keeps NoDefaultValueFlag,
    // which is the `None` case here.
    let mut defaults: Vec<Option<crate::column_default::ColumnDefault>> =
        Vec::with_capacity(create.columns.len());
    for def in &create.columns {
        let mut default_value = None;
        for option in &def.options {
            match option {
                tidb_ast::ColumnOption::Default(expr) => {
                    let field_type = columns[defaults.len()].field_type.clone();
                    // Go `SetDefaultValue`: a FUNCTION-CALL default takes the
                    // whitelist route and never the constant folder, which is
                    // why `DEFAULT (abs(1))` is 3770 in TiDB despite folding.
                    let built = crate::column_default::build(expr, &field_type, |expr| {
                        let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
                            expr,
                            &tidb_expr::rewriter::NoResolver,
                        )
                        .map_err(|_| {
                            crate::column_default::DefaultError::Unsupported(
                                "a DEFAULT this node cannot evaluate",
                            )
                        })?;
                        // Go `EvalSimpleAst`: the expression is EVALUATED,
                        // not merely required to be a literal already, which
                        // is what settles `DEFAULT (1 + 1)` to 2.
                        let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
                        dual.set_num_virtual_rows(1);
                        rewritten
                            .eval(&tidb_expr::NoColumns, dual.get_row(0))
                            .map_err(|_| {
                                crate::column_default::DefaultError::Unsupported(
                                    "a DEFAULT this node cannot evaluate",
                                )
                            })
                    })
                    .map_err(|error| column_default_error(error, &def.name))?;
                    // Go normalizes and checks a SETTLED default against the
                    // column's own type at DDL time; a computed one is cast
                    // per row instead, exactly as Go's `CastColumnValue` does.
                    default_value =
                        Some(match built {
                            crate::column_default::ColumnDefault::Value(value) => {
                                crate::column_default::ColumnDefault::Value(
                                    normalize_column_default(value, &field_type, &def.name)?,
                                )
                            }
                            computed => computed,
                        });
                }
                // AUTO_INCREMENT is its own value source, handled below.
                tidb_ast::ColumnOption::AutoIncrement => {}
                // A generated column's value source is its expression, built
                // below once every column's name and type is known.
                tidb_ast::ColumnOption::Generated { .. } => {}
                _ => {}
            }
        }
        defaults.push(default_value);
    }

    let info = TableInfo {
        id: catalog.allocate_table_id(),
        name: CiString::new(name),
        columns,
        pk_is_handle,
        ..TableInfo::default()
    };

    // The generated columns, built against the table's own final column list
    // so their expressions index the stored row directly.
    let column_names: Vec<String> = info
        .columns
        .iter()
        .map(|c| c.name.original().to_owned())
        .collect();
    let column_types: Vec<tidb_datatype::FieldType> =
        info.columns.iter().map(|c| c.field_type.clone()).collect();
    let generated = crate::generated_column::build_generated_columns(
        &create.columns,
        &column_names,
        &column_types,
    )
    .map_err(generated_column_error)?;
    // Go `ErrUnsupportedOnGeneratedColumn`: a VIRTUAL generated column cannot
    // be the primary key, because the key would have no stored value to be.
    // A STORED one can. Captured: `create table t (a int, b int as (a+1),
    // primary key(b))` is 3106.
    for offset in &pk_offsets {
        if generated[*offset]
            .as_ref()
            .is_some_and(|generated| !generated.stored)
        {
            return Err(DriverError::UnsupportedOnGeneratedColumn(
                "Defining a virtual generated column as primary key".to_owned(),
            ));
        }
    }

    let kv_columns: Vec<KvColumn> = info
        .columns
        .iter()
        .map(|c| KvColumn {
            name: c.name.original().to_owned(),
            id: c.id,
            field_type: c.field_type.clone(),
            generated: generated[c.offset as usize].clone(),
            default_value: defaults[c.offset as usize].clone(),
            // A column present at CREATE TABLE has no pre-existing rows.
            origin_default: None,
        })
        .collect();
    let table = KvTable::new(info.id, kv_columns);
    let mut table = table;
    table.set_name(name);
    table.set_charset(table_charset);
    if pk_is_handle {
        if let Some(offset) = pk_offset {
            table.set_pk_handle_offset(offset);
        }
    } else if !common_handle_offsets.is_empty() {
        table.set_common_handle_offsets(common_handle_offsets.clone());
    }
    let clustered = pk_is_handle || !common_handle_offsets.is_empty();
    if let Some(offset) = auto_increment_offset {
        table.set_auto_increment_offset(offset);
        // Go `handleAutoIncID`: CREATE seeds the allocator only when the
        // option is `> 1` -- a SIGNED comparison on `int64(opt.UintValue)`,
        // so `AUTO_INCREMENT = 18446744073709551615` (and any value above
        // `i64::MAX`) seeds nothing and the first row lands on 1 even for a
        // `BIGINT UNSIGNED` column. Only ALTER rebases in the column's own
        // domain; captured from Go, the two really do disagree here.
        if let Some(seed) = auto_increment_option(&create.table_options)? {
            if seed > 1 {
                table.rebase_auto_increment(seed);
            }
        }
    }
    let (indexes, hidden_columns) = table_indexes(create, &info.columns, clustered)?;
    for hidden in hidden_columns {
        // Go `checkExpressionIndexAutoIncrement`: an expression index may not
        // read an AUTO_INCREMENT column. Captured as 3754 naming the index,
        // which is the index whose part built this column.
        if let Some(auto) = auto_increment_offset {
            if hidden.generated.dependencies.contains(&auto) {
                return Err(DriverError::ExpressionIndexCanNotRefer(
                    indexes
                        .iter()
                        .find(|index| {
                            index
                                .column_offsets
                                .iter()
                                .any(|offset| *offset >= info.columns.len())
                        })
                        .map_or_else(String::new, |index| index.name.clone()),
                ));
            }
        }
        table.add_hidden_column(KvColumn {
            name: hidden.name,
            id: table.next_column_id(),
            field_type: hidden.field_type,
            generated: Some(hidden.generated),
            default_value: None,
            origin_default: None,
        });
    }
    for index in indexes {
        table.add_index(index);
    }
    for foreign_key in table_foreign_keys(
        create,
        &info.columns,
        catalog,
        &database,
        foreign_key_checks,
    )? {
        // Go `addForeignKeyIndex`: a foreign key needs an index on its
        // referencing columns, and TiDB adds one named after the constraint
        // UNLESS an existing key -- the clustered primary key included --
        // already has those columns as a PREFIX. Captured: a child whose FK
        // column is its primary key, and one with `KEY kk (pid, k)`, get no
        // extra index, while one with `KEY kk (k, pid)` does.
        let covered = |offsets: &[usize]| offsets.starts_with(&foreign_key.cols);
        let clustered: &[usize] = if pk_is_handle {
            pk_offsets.as_slice()
        } else {
            common_handle_offsets.as_slice()
        };
        if !covered(clustered)
            && !table
                .indexes()
                .iter()
                .any(|index| covered(&index.column_offsets))
        {
            let id = table.next_index_id();
            table.add_index(KvIndex {
                id,
                name: foreign_key.name.clone(),
                unique: false,
                column_offsets: foreign_key.cols.clone(),
                visible: true,
            });
        }
        table.add_foreign_key(foreign_key);
    }
    catalog.register_kv_in(&database, name, table);
    Ok(true)
}

/// Names a column-default DDL refusal the way Go's own error does. Go's 3770
/// message names both the column and the function, and only the caller knows
/// the column.
fn column_default_error(error: crate::column_default::DefaultError, column: &str) -> DriverError {
    use crate::column_default::DefaultError;
    match error {
        DefaultError::FunctionNotAllowed(function) => {
            DriverError::DefaultFunctionNotAllowed(column.to_owned(), function)
        }
        DefaultError::Unsupported(reason) => DriverError::Unsupported(reason),
    }
}

/// Names a generated-column DDL refusal the way Go's own error does.
fn generated_column_error(error: crate::generated_column::GeneratedDdlError) -> DriverError {
    use crate::generated_column::GeneratedDdlError;
    match error {
        GeneratedDdlError::UnknownDependency(name) => DriverError::UnknownColumnInClause {
            column: name,
            clause: "generated column function".to_owned(),
        },
        GeneratedDdlError::NonPrior => DriverError::GeneratedColumnNonPrior,
        GeneratedDdlError::Unsupported(reason) => {
            DriverError::UnsupportedOnGeneratedColumn(reason.to_owned())
        }
        GeneratedDdlError::Unbuildable(reason) => DriverError::Unsupported(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{run_insert_on, run_select_on};
    use tidb_datatype::Datum;
    use tidb_datatype::{Charset, Collation};

    /// CREATE TABLE -> INSERT -> SELECT, all from SQL strings, with rows as
    /// real TiKV-format bytes and metadata as tidb-model structs.
    #[test]
    fn create_insert_select_from_sql() {
        let mut catalog = Catalog::default();
        assert!(run_create_table_on(
            "CREATE TABLE t (a BIGINT, b BIGINT UNSIGNED, s VARCHAR(10))",
            &mut catalog
        )
        .unwrap());

        assert_eq!(
            run_insert_on(
                "INSERT INTO t (a, s) VALUES (7, 'x')",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        let rows = run_select_on(
            "SELECT a, s FROM t",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Datum::Int(7));
        match &rows[0][1] {
            Datum::Bytes(b) => assert_eq!(b.as_slice(), b"x"),
            Datum::String(s) => assert_eq!(s.bytes(), b"x"),
            other => panic!("unexpected string datum {other:?}"),
        }
    }

    /// An index's visibility is stated by the DDL and read by the planner off
    /// `KvIndex::visible` -- one fact, two places. Every statement that
    /// creates an index resolves it at the single point that builds the
    /// `KvIndex`, so an `INVISIBLE` key is maintained (it is in `indexes()`)
    /// and hidden from every access path (it is not in `plan_indexes()`),
    /// which is Go's rule. Hardcoding `visible: true` -- as all three sites
    /// did -- made the planner choose an index Go never chooses.
    #[test]
    fn an_index_declared_invisible_is_maintained_but_never_planned() {
        let mut catalog = Catalog::default();
        run_create_table_on(
            "CREATE TABLE t (a INT, b INT, KEY idx_a (a) INVISIBLE, KEY idx_b (b))",
            &mut catalog,
        )
        .unwrap();
        run_create_index_in(
            "CREATE INDEX idx_c ON t (a) INVISIBLE",
            &mut catalog,
            crate::driver::DEFAULT_DATABASE,
        )
        .unwrap();
        run_alter_table_in(
            "ALTER TABLE t ADD INDEX idx_d (b) INVISIBLE",
            &mut catalog,
            crate::driver::DEFAULT_DATABASE,
        )
        .unwrap();

        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t") else {
            panic!("expected a kv table");
        };
        let maintained: Vec<&str> = kv.indexes().iter().map(|i| i.name.as_str()).collect();
        let planned: Vec<&str> = kv.plan_indexes().map(|i| i.name.as_str()).collect();
        assert_eq!(
            maintained,
            vec!["idx_a", "idx_b", "idx_c", "idx_d"],
            "every declared index is maintained by writes"
        );
        assert_eq!(
            planned,
            vec!["idx_b"],
            "only the visible one is an access path"
        );
    }

    #[test]
    fn if_not_exists_and_duplicates() {
        let mut catalog = Catalog::default();
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).unwrap());
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).is_err());
        assert!(
            !run_create_table_on("CREATE TABLE IF NOT EXISTS t (a INT)", &mut catalog).unwrap()
        );
    }

    /// A single-column integer primary key sets Go's `PKIsHandle`; a
    /// non-integer one does not (`isIntCol`).
    #[test]
    fn primary_key_sets_the_handle_flag_only_for_an_integer_column() {
        let mut catalog = Catalog::default();
        run_create_table_on("CREATE TABLE t (a INT, PRIMARY KEY (a))", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE s (a VARCHAR(4) PRIMARY KEY)", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE h (a INT)", &mut catalog).unwrap();

        let handle_offset = |name: &str| match catalog.get_table_for_test(name) {
            Some(crate::TableEntry::Kv(kv)) => kv.pk_handle_offset(),
            _ => panic!("expected a kv table"),
        };
        assert_eq!(handle_offset("t"), Some(0));
        assert_eq!(handle_offset("s"), None, "a string PK is not a handle");
        assert_eq!(handle_offset("h"), None, "no PK, no handle column");
    }

    /// The `(charset, collation, flen)` a column resolves to, for the
    /// charset/collation captures below.
    fn resolved(catalog: &Catalog, table: &str, column: &str) -> (String, String, i64) {
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test(table) else {
            panic!("expected a kv table");
        };
        let column = kv
            .columns
            .iter()
            .find(|c| c.name == column)
            .expect("column exists");
        (
            column.field_type.charset_name().to_owned(),
            column.field_type.collation_name().to_owned(),
            column.field_type.flen(),
        )
    }

    /// Captured from TiDB (`SHOW FULL COLUMNS` + `information_schema.columns`
    /// over one table carrying every string form): the BINARY/VARBINARY/BLOB
    /// family is charset `binary`, the CHAR/VARCHAR/TEXT family takes the
    /// table's default (`utf8mb4`/`utf8mb4_bin` -- NOT `utf8mb4_0900_ai_ci`),
    /// an explicit COLLATE wins, and the `BINARY` column attribute picks the
    /// charset's `_bin` collation.
    #[test]
    fn ddl_resolves_charset_and_collation_per_column() {
        let mut catalog = Catalog::default();
        run_create_table_on(
            "CREATE TABLE t1 (\
                 c_varchar VARCHAR(10), c_char CHAR(10), \
                 c_varbinary VARBINARY(10), c_binary BINARY(3), \
                 c_blob BLOB, c_text TEXT, c_tinytext TINYTEXT, c_longblob LONGBLOB, \
                 c_vc_cs VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, \
                 c_vc_bin VARCHAR(10) BINARY, \
                 c_enum ENUM('a','B'), c_set SET('a','B'), c_int INT)",
            &mut catalog,
        )
        .unwrap();

        let case = |column: &str| resolved(&catalog, "t1", column);
        assert_eq!(
            case("c_varchar"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 10)
        );
        assert_eq!(case("c_char"), ("utf8mb4".into(), "utf8mb4_bin".into(), 10));
        assert_eq!(case("c_varbinary"), ("binary".into(), "binary".into(), 10));
        assert_eq!(case("c_binary"), ("binary".into(), "binary".into(), 3));
        // The BLOB/TEXT family carries its type's fixed capacity as flen.
        assert_eq!(case("c_blob"), ("binary".into(), "binary".into(), 65535));
        assert_eq!(
            case("c_text"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 65535)
        );
        assert_eq!(
            case("c_tinytext"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 255)
        );
        assert_eq!(
            case("c_longblob"),
            ("binary".into(), "binary".into(), 4_294_967_295)
        );
        assert_eq!(
            case("c_vc_cs"),
            ("utf8mb4".into(), "utf8mb4_general_ci".into(), 10)
        );
        // `VARCHAR(10) BINARY` is the charset's `_bin` collation, NOT charset
        // `binary`: it still reports utf8mb4 (captured).
        assert_eq!(
            case("c_vc_bin"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 10)
        );
        // ENUM/SET take the table charset, and their flen is the display
        // length Go derives from the members.
        assert_eq!(case("c_enum"), ("utf8mb4".into(), "utf8mb4_bin".into(), 1));
        assert_eq!(case("c_set"), ("utf8mb4".into(), "utf8mb4_bin".into(), 3));

        // A binary-charset string column has no charset for `HasCharset`,
        // which is what makes SHOW/information_schema report NULL for it.
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t1") else {
            panic!("expected a kv table");
        };
        let has_charset = |column: &str| {
            kv.columns
                .iter()
                .find(|c| c.name == column)
                .unwrap()
                .field_type
                .has_charset()
        };
        assert!(has_charset("c_varchar") && has_charset("c_enum"));
        assert!(!has_charset("c_varbinary") && !has_charset("c_blob"));
        assert!(!has_charset("c_int"));
    }

    /// Captured from TiDB over `... DEFAULT CHARSET=latin1`: a column with no
    /// clause takes the table's charset AND collation, an explicit
    /// `CHARACTER SET utf8mb4` takes that charset's default collation, and
    /// `CHARACTER SET binary` turns a VARCHAR into a `varbinary`.
    #[test]
    fn ddl_column_charset_falls_back_to_the_table_default() {
        let mut catalog = Catalog::default();
        run_create_table_on(
            "CREATE TABLE t2 (a VARCHAR(10), b VARCHAR(10) CHARACTER SET utf8mb4, \
                 c VARCHAR(10) CHARACTER SET latin1, d VARCHAR(10) CHARACTER SET binary) \
                 DEFAULT CHARSET=latin1",
            &mut catalog,
        )
        .unwrap();
        let case = |column: &str| resolved(&catalog, "t2", column);
        assert_eq!(case("a"), ("latin1".into(), "latin1_bin".into(), 10));
        assert_eq!(case("b"), ("utf8mb4".into(), "utf8mb4_bin".into(), 10));
        assert_eq!(case("c"), ("latin1".into(), "latin1_bin".into(), 10));
        assert_eq!(case("d"), ("binary".into(), "binary".into(), 10));

        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t2") else {
            panic!("expected a kv table");
        };
        assert_eq!(
            kv.charset(),
            TableCharset {
                charset: Charset::Latin1,
                collation: Collation::Latin1Bin,
            }
        );
    }

    /// A `COLLATE` alone determines the charset, and a `COLLATE` that does not
    /// belong to the written `CHARACTER SET` is rejected rather than silently
    /// producing a contradictory field type.
    #[test]
    fn ddl_collate_alone_picks_the_charset_and_a_mismatched_pair_is_rejected() {
        let mut catalog = Catalog::default();
        run_create_table_on(
            "CREATE TABLE t (a VARCHAR(10) COLLATE latin1_bin)",
            &mut catalog,
        )
        .unwrap();
        assert_eq!(
            resolved(&catalog, "t", "a"),
            ("latin1".into(), "latin1_bin".into(), 10)
        );
        assert!(run_create_table_on(
            "CREATE TABLE bad (a VARCHAR(10) CHARACTER SET latin1 COLLATE utf8mb4_bin)",
            &mut catalog,
        )
        .is_err());
    }

    /// Captured from TiDB: `INSERT INTO tb(b BINARY(3)) VALUES ('ab')` reads
    /// back as `0x616200` with `LENGTH` 3 -- a fixed-width binary column is
    /// zero-padded to its flen -- while `VARBINARY(3)` keeps the two bytes.
    #[test]
    fn binary_column_zero_pads_to_its_length() {
        let mut catalog = Catalog::default();
        run_create_table_on(
            "CREATE TABLE tb (b BINARY(3), vb VARBINARY(3))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO tb VALUES ('ab','ab')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let rows = run_select_on(
            "SELECT b, vb FROM tb",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let bytes = |value: &Datum| match value {
            Datum::Bytes(b) => b.clone(),
            Datum::String(s) => s.bytes().to_vec(),
            other => panic!("unexpected string datum {other:?}"),
        };
        assert_eq!(bytes(&rows[0][0]), vec![b'a', b'b', 0]);
        assert_eq!(bytes(&rows[0][1]), vec![b'a', b'b']);
    }
}
