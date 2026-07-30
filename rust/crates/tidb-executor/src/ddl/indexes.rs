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

use super::{Catalog, DdlStmt, DriverError, KvIndex, Stmt};

/// Runs a `CREATE INDEX`, backfilling the existing rows.
///
/// Captured from TiDB: a duplicate index name is 1061, and a unique index
/// whose existing rows already collide is 1062 naming `table.index`, leaving
/// the table without the index.
pub fn run_create_index_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
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
    let columns: Vec<String> = index_part_names(&create.parts)?;
    add_index_to_table(
        catalog,
        &database,
        &table_name,
        &create.name,
        unique,
        &columns,
        is_visible(&create.options),
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

/// Adds one index to a table, shared by `CREATE INDEX` and
/// `ALTER TABLE ... ADD INDEX`.
pub(crate) fn add_index_to_table(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index_name: &str,
    unique: bool,
    columns: &[String],
    visible: bool,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let mut offsets = Vec::with_capacity(columns.len());
    for column in columns {
        offsets.push(
            table
                .columns
                .iter()
                .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| DriverError::UnknownColumnInAlter(column.clone()))?,
        );
    }
    let id = table.next_index_id();
    table
        .create_index(KvIndex {
            id,
            name: index_name.to_owned(),
            unique,
            column_offsets: offsets,
            visible,
        })
        .map_err(|e| match e {
            crate::kv_table::KvTableError::DuplicateKeyName(name) => {
                DriverError::DuplicateKeyName(name)
            }
            crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                DriverError::DuplicateEntry { value, key }
            }
            other => DriverError::Parse(format!("index creation failed: {other:?}")),
        })
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
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let dropped = table
        .drop_index(index_name)
        .map_err(|e| DriverError::Parse(format!("index drop failed: {e:?}")))?;
    if !dropped && !if_exists {
        return Err(DriverError::UnknownIndex(index_name.to_owned()));
    }
    Ok(())
}
