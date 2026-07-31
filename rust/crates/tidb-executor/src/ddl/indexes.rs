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

//! Adding and removing a key on an existing table: `CREATE INDEX` and
//! `DROP INDEX`, plus the `ALTER TABLE ... ADD/DROP INDEX` actions that reach
//! the same code.
//!
//! Inside: [`run_create_index_in`] and [`run_drop_index_in`], the statement
//! entry points; [`add_index_to_table`] and [`drop_index_from_table`], the
//! actions the `ALTER TABLE` dispatcher shares with them; and the two AST
//! readers `is_visible` (Go `!IndexInfo.Invisible`) and `index_part_names`,
//! which reject the index shapes this tier does not model rather than
//! silently creating a plain index.
//!
//! Mirrors Go `pkg/ddl/index.go` (`CreateIndex`, `dropIndex`): creating a
//! unique index backfills and re-checks the existing rows, so a collision
//! leaves the table WITHOUT the index, which is the behaviour the doc
//! comments record as captured from TiDB.

use super::{Catalog, DdlStmt, DriverError, KvColumn, KvIndex, Stmt};

/// Runs a `CREATE INDEX`, backfilling the existing rows.
///
/// Captured from TiDB: a duplicate index name is 1061, and a unique index
/// whose existing rows already collide is 1062 naming `table.index`, leaving
/// the table without the index.
pub fn run_create_index_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only CREATE INDEX is supported here",
        ));
    };
    let DdlStmt::CreateIndex(create) = &**ddl else {
        return Err(DriverError::Unsupported(
            "only CREATE INDEX is supported here",
        ));
    };
    let (database, table_name) = crate::driver::split_table_path_pub(&create.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    let unique = match create.kind {
        tidb_ast::IndexKind::Ordinary => false,
        tidb_ast::IndexKind::Unique => true,
        _ => {
            return Err(DriverError::Unsupported(
                "SPATIAL, FULLTEXT, VECTOR and COLUMNAR indexes are not supported yet",
            ))
        }
    };
    reject_partial_index(&create.options)?;
    add_index_to_table(
        catalog,
        &database,
        &table_name,
        IndexSpec {
            name: &create.name,
            unique,
            parts: &create.parts,
            visible: is_visible(&create.options),
        },
        ctx,
    )
}

/// Go `IndexInfo.Invisible`, read off the statement that declares the index.
///
/// The visibility a statement writes and the `visible` flag the planner reads
/// are the same fact in two places, so it is resolved here, at the single
/// point where an `IndexOptions` becomes a `KvIndex` -- a `CREATE INDEX`, an
/// `ALTER TABLE ... ADD INDEX` and a `CREATE TABLE` key all pass through it.
/// A key with no visibility clause is visible, which is Go's default.
pub(crate) fn is_visible(options: &tidb_ast::IndexOptions) -> bool {
    options.visibility != Some(tidb_ast::IndexVisibility::Invisible)
}

/// Refuses a PARTIAL index -- `KEY idx(a) WHERE a > 0` -- which this tier
/// parses but does not maintain.
///
/// Creating it anyway would build a FULL index under a partial index's name:
/// every row would get an entry, including the rows the condition excludes.
/// That is wrong in both directions at once. A plan that trusted the
/// condition would read entries for rows that should not be there, and
/// `ADMIN CHECK TABLE` -- which Go refuses outright on a partial index unless
/// `tidb_enable_fast_table_check=ON` (8273) -- would call the table
/// consistent, because the index really is consistent with the wrong
/// definition. Refusing at CREATE keeps the condition from being silently
/// dropped.
pub(crate) fn reject_partial_index(options: &tidb_ast::IndexOptions) -> Result<(), DriverError> {
    if options.condition.is_some() {
        return Err(DriverError::Unsupported(
            "a partial index (KEY ... WHERE) is not supported yet",
        ));
    }
    Ok(())
}

/// The column names an index's key parts name, rejecting the forms this tier
/// does not maintain rather than creating an index that ignores them.
pub(crate) fn index_part_names(parts: &[tidb_ast::IndexPart]) -> Result<Vec<String>, DriverError> {
    let mut names = Vec::with_capacity(parts.len());
    for part in parts {
        let tidb_ast::IndexPart::Column {
            name, prefix_len, ..
        } = part
        else {
            return Err(DriverError::Unsupported(
                "an expression index is not supported yet",
            ));
        };
        if prefix_len.is_some() {
            return Err(DriverError::Unsupported(
                "a prefix-length index is not supported yet",
            ));
        }
        names.push(name.clone());
    }
    Ok(names)
}

/// The index a statement asks for, read off either spelling of the request:
/// `CREATE INDEX` and `ALTER TABLE ... ADD INDEX` name the same four facts in
/// different AST nodes.
pub(crate) struct IndexSpec<'a> {
    /// The index's name.
    pub name: &'a str,
    /// Go `IndexInfo.Unique`.
    pub unique: bool,
    /// The key parts, each a column or an expression.
    pub parts: &'a [tidb_ast::IndexPart],
    /// Go `!IndexInfo.Invisible`.
    pub visible: bool,
}

/// Adds one index to a table, shared by `CREATE INDEX` and
/// `ALTER TABLE ... ADD INDEX`.
///
/// An EXPRESSION key part is rewritten into a hidden generated column
/// appended to the table, exactly as `CREATE TABLE` does it -- see
/// [`crate::expression_index`]. Nothing about index maintenance changes:
/// entries are written from the materialized row, so the hidden column's
/// value is indexed by the generated-column path that already exists.
pub(crate) fn add_index_to_table(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index: IndexSpec<'_>,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let IndexSpec {
        name: index_name,
        unique,
        parts,
        visible,
    } = index;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    // A hidden column is built against the VISIBLE columns, so an expression
    // can never name an earlier index's hidden column.
    let names: Vec<String> = table
        .visible_columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let types: Vec<tidb_datatype::FieldType> = table
        .visible_columns()
        .iter()
        .map(|c| c.field_type.clone())
        .collect();
    let hidden = crate::expression_index::build_hidden_columns(index_name, parts, &names, &types)?;
    // Go `checkExpressionIndexAutoIncrement` (3754).
    if let Some(auto) = table.auto_increment_offset() {
        if hidden
            .iter()
            .any(|(_, column)| column.generated.dependencies.contains(&auto))
        {
            return Err(DriverError::ExpressionIndexCanNotRefer(
                index_name.to_owned(),
            ));
        }
    }
    let mut offsets = Vec::with_capacity(parts.len());
    let mut built = hidden.into_iter();
    let mut pending = Vec::new();
    for part in parts {
        match part {
            tidb_ast::IndexPart::Column {
                name, prefix_len, ..
            } => {
                if prefix_len.is_some() {
                    return Err(DriverError::Unsupported(
                        "a prefix-length index is not supported yet",
                    ));
                }
                offsets.push(
                    table
                        .columns
                        .iter()
                        .position(|candidate| candidate.name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| DriverError::UnknownColumnInAlter(name.clone()))?,
                );
            }
            tidb_ast::IndexPart::Expr { .. } => {
                let (_, column) = built.next().expect("one hidden column per expression part");
                offsets.push(table.columns.len() + pending.len());
                pending.push(column);
            }
        }
    }
    let added = pending.len();
    for column in pending {
        table.add_hidden_column(KvColumn {
            name: column.name,
            id: table.next_column_id(),
            field_type: column.field_type,
            generated: Some(column.generated),
            default_value: None,
            origin_default: None,
        });
    }
    let id = table.next_index_id();
    let result = table
        .create_index(
            KvIndex {
                id,
                name: index_name.to_owned(),
                unique,
                column_offsets: offsets,
                visible,
            },
            ctx,
        )
        .map_err(|e| match e {
            crate::kv_table::KvTableError::DuplicateKeyName(name) => {
                DriverError::DuplicateKeyName(name)
            }
            // The backfill computes each entry's value from the row, so a
            // generated column that cannot be evaluated fails the DDL with
            // its own code -- Go answers 1365 for
            // `ALTER TABLE t ADD INDEX ((100/a))` over a row with `a = 0`.
            crate::kv_table::KvTableError::Generation {
                eval: Some(eval), ..
            } => DriverError::Exec(crate::ExecError::Eval(eval)),
            crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                DriverError::DuplicateEntry { value, key }
            }
            other => DriverError::Parse(format!("index creation failed: {other:?}")),
        });
    // A failed `CREATE INDEX` must leave no trace. The hidden columns have to
    // be in place BEFORE the index is built (its entries are computed from
    // the materialized row), so a failure takes them back off rather than
    // leaving a column no statement can name and no index uses.
    if result.is_err() {
        for _ in 0..added {
            table.drop_column(table.columns.len() - 1);
        }
    }
    result
}

/// Runs a `DROP INDEX`.
///
/// Captured from TiDB: dropping one that does not exist is 1091 with its own
/// message, "index nosuch doesn't exist".
pub fn run_drop_index_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only DROP INDEX is supported here",
        ));
    };
    let DdlStmt::DropIndex(drop) = &**ddl else {
        return Err(DriverError::Unsupported(
            "only DROP INDEX is supported here",
        ));
    };
    let (database, table_name) = crate::driver::split_table_path_pub(&drop.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    drop_index_from_table(catalog, &database, &table_name, &drop.name, drop.if_exists)
}

/// Removes one index, shared by `DROP INDEX` and `ALTER TABLE ... DROP INDEX`.
pub(crate) fn drop_index_from_table(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index_name: &str,
    if_exists: bool,
) -> Result<(), DriverError> {
    // Go `ddl.checkIndexNeededInForeignKey`, before anything is removed: an
    // index a live constraint relies on is 1553, on either side.
    crate::foreign_key::check_index_needed(catalog, database, table_name, index_name)?;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    // The hidden columns this index owns go with it. Captured from Go:
    // `alter table te drop index idxe` leaves `SHOW CREATE TABLE` printing
    // only the declared columns, and an `INSERT` then takes their arity.
    // Collected before the drop, while the index still names its offsets.
    let hidden: Vec<usize> = table
        .indexes()
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .map(|index| {
            let mut offsets: Vec<usize> = index
                .column_offsets
                .iter()
                .copied()
                .filter(|offset| table.is_hidden(*offset))
                .collect();
            // Highest first, so each removal leaves the rest addressable.
            offsets.sort_unstable_by(|a, b| b.cmp(a));
            offsets
        })
        .unwrap_or_default();
    let dropped = table
        .drop_index(index_name)
        .map_err(|e| DriverError::Parse(format!("index drop failed: {e:?}")))?;
    if !dropped && !if_exists {
        return Err(DriverError::UnknownIndex(index_name.to_owned()));
    }
    for offset in hidden {
        table.drop_column(offset);
    }
    Ok(())
}
