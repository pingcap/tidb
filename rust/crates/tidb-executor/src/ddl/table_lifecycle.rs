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

//! The statements that move or remove a whole table: `RENAME TABLE`,
//! `TRUNCATE TABLE` and `DROP TABLE`.
//!
//! Inside: [`run_rename_table_in`], which relocates each pair in written
//! order and may cross schemas; [`run_truncate_table_in`], which empties the
//! rows and index entries and restarts the auto-increment counter while
//! keeping the definition; and [`run_drop_table_in`], which drops the names
//! it finds and errors on the first it does not, so a partial list still
//! removes tables. Each function's doc records the captured TiDB error code.
//!
//! Mirrors Go `pkg/ddl/table.go` (`RenameTable`, `TruncateTable`,
//! `DropTable`). The definition-changing statements live in the sibling
//! modules: columns in `alter_table`, keys in `indexes`, and `CREATE TABLE`
//! in the parent.

use super::{Catalog, DdlStmt, DriverError, Stmt};

/// Runs a `RENAME TABLE`, moving each pair in written order.
///
/// Captured from TiDB: renaming onto a name that already exists is 1050, and
/// renaming a table that does not exist is 1146. A rename may move the table
/// to another schema, since both sides carry a full path.
pub fn run_rename_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    // The session's scanner `sql_mode`: this entry RE-PARSES text the session
    // already parsed, so without it a double-quoted name would mean one thing
    // to the session and another here.
    sql_mode: tidb_parser::SqlMode,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only RENAME TABLE is supported here",
        ));
    };
    let pairs: Vec<(Vec<String>, Vec<String>)> = match &**ddl {
        DdlStmt::RenameTable(rename) => rename.pairs.clone(),
        // `ALTER TABLE x RENAME TO y` is the same operation.
        DdlStmt::AlterTable(alter) => {
            let mut pairs = Vec::new();
            for action in &alter.actions {
                if let tidb_ast::AlterTableAction::RenameTable { new_name } = action {
                    pairs.push((alter.name.clone(), new_name.clone()));
                }
            }
            pairs
        }
        _ => {
            return Err(DriverError::Unsupported(
                "only RENAME TABLE is supported here",
            ))
        }
    };

    for (from, to) in &pairs {
        let (from_db, from_name) = crate::driver::split_table_path_pub(from, current_db)?;
        let (from_db, from_name) = (from_db.to_owned(), from_name.to_owned());
        let (to_db, to_name) = crate::driver::split_table_path_pub(to, current_db)?;
        let (to_db, to_name) = (to_db.to_owned(), to_name.to_owned());

        if catalog.table_in(&from_db, &from_name).is_none() {
            return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                format!("{from_db}.{from_name}"),
            )));
        }
        if catalog.table_in(&to_db, &to_name).is_some() {
            return Err(DriverError::Schema(crate::SchemaErrorKind::TableExists(
                format!("{to_db}.{to_name}"),
            )));
        }
        // A foreign key names the referenced table, so moving one side would
        // leave the constraint pointing at a name that no longer resolves.
        if crate::foreign_key::participates(catalog, &from_db, &from_name) {
            return Err(DriverError::Unsupported(
                "renaming a table involved in a FOREIGN KEY is not supported yet",
            ));
        }
        catalog.rename_table(&from_db, &from_name, &to_db, &to_name);
    }
    Ok(())
}

/// Runs a `TRUNCATE TABLE`, emptying it while keeping its definition.
///
/// Captured from TiDB: the rows and index entries go, the schema and indexes
/// stay, the auto-increment counter restarts, and truncating a table that
/// does not exist is 1146.
pub fn run_truncate_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    // The session's scanner `sql_mode`: this entry RE-PARSES text the session
    // already parsed, so without it a double-quoted name would mean one thing
    // to the session and another here.
    sql_mode: tidb_parser::SqlMode,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only TRUNCATE TABLE is supported here",
        ));
    };
    let DdlStmt::TruncateTable(truncate) = &**ddl else {
        return Err(DriverError::Unsupported(
            "only TRUNCATE TABLE is supported here",
        ));
    };
    let (database, name) = crate::driver::split_table_path_pub(truncate, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(&database, &name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        )));
    };
    // TRUNCATE starts the counter over, and on a shared counter that is a
    // write like any other: a failure here must not be reported as a
    // successful truncate whose next insert then collides.
    table
        .truncate()
        .map_err(|error| DriverError::AutoIdUnavailable(error.0))?;
    Ok(())
}

/// Runs a `DROP TABLE`, removing every named table that exists.
///
/// Go drops the tables it finds and reports `ErrBadTable` for the first name
/// it does not, rather than validating the whole list first: captured from
/// TiDB, `drop table d1, nosuch` removes `d1` AND errors. `IF EXISTS`
/// suppresses the error, leaving the drops it did perform.
///
/// DEFERRED (documented): `TEMPORARY` tables, which this executor never
/// models, are rejected rather than silently dropping a permanent table of
/// the same name.
pub fn run_drop_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    // The session's scanner `sql_mode`: this entry RE-PARSES text the session
    // already parsed, so without it a double-quoted name would mean one thing
    // to the session and another here.
    sql_mode: tidb_parser::SqlMode,
    foreign_key_checks: bool,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let drop = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::DropTable(drop) => drop,
            _ => {
                return Err(DriverError::Unsupported(
                    "only DROP TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::Unsupported(
                "only DROP TABLE is supported here",
            ))
        }
    };
    if drop.temporary != tidb_ast::DropTemporary::None {
        return Err(DriverError::Unsupported(
            "temporary tables are not supported yet",
        ));
    }

    // Go `checkDropTableHasForeignKeyReferredInOwner` runs over the WHOLE
    // statement before anything is dropped, so a parent and its child dropped
    // together succeed regardless of the order they are listed in, while a
    // parent alone fails without dropping any of the named tables.
    if foreign_key_checks {
        let mut dropping = Vec::with_capacity(drop.names.len());
        for path in &drop.names {
            let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
            dropping.push((database.to_owned(), name.to_owned()));
        }
        crate::foreign_key::check_drop_tables(catalog, &dropping)?;
    }

    let mut missing: Option<String> = None;
    for path in &drop.names {
        let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        // A view is not a table: `DROP TABLE v` reports the name as unknown
        // rather than dropping the view (Go's own captured behaviour).
        let dropped =
            !catalog.is_view_in(&database, &name) && catalog.drop_table_in(&database, &name);
        if !dropped && missing.is_none() {
            // Go reports the first missing name, after the drops it performed.
            missing = Some(format!("{database}.{name}"));
        }
    }
    match missing {
        Some(name) if !drop.if_exists => {
            Err(DriverError::Schema(crate::SchemaErrorKind::BadTable(name)))
        }
        _ => Ok(()),
    }
}
