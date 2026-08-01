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
//! Inside: [`run_rename_table_in`], which validates every pair in written
//! order and then moves them all or none, and may cross schemas;
//! [`run_truncate_table_in`], which empties the
//! rows and index entries and restarts the auto-increment counter while
//! keeping the definition; and [`run_drop_table_in`], which drops the names
//! it finds and reports the ones it does not, so a partial list still
//! removes tables. Each function's doc records the captured TiDB error code.
//!
//! Mirrors Go `pkg/ddl/table.go` (`RenameTable`, `TruncateTable`,
//! `DropTable`). The definition-changing statements live in the sibling
//! modules: columns in `alter_table`, keys in `indexes`, and `CREATE TABLE`
//! in the parent.

use super::{Catalog, DdlStmt, DriverError, Stmt};

/// Runs a `RENAME TABLE`, validating each pair in written order and then
/// moving them all or none.
///
/// Captured from TiDB: renaming onto a name that already exists is 1050
/// (which is also what renaming a table ONTO ITSELF reports), renaming a
/// table that does not exist is 1146, and naming a destination schema that
/// does not exist is 1025 with the source left in place. A rename may move
/// the table to another schema, since both sides carry a full path.
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
        return Err(DriverError::unsupported(
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
            return Err(DriverError::unsupported(
                "only RENAME TABLE is supported here",
            ))
        }
    };

    // Every pair is checked before ANY pair is moved. Go builds the same
    // separation in `ExtractTblInfos`, which validates each pair against a
    // `tables` overlay of the renames staged so far and only then runs the
    // DDL job; captured, `RENAME TABLE c TO c2, nope TO q` leaves `c` named
    // `c` -- the first pair is not applied. Staging is also why a chain like
    // `a TO tmp, b TO a` succeeds: `a` is free by the time pair two is read.
    let mut staged: Vec<Rename> = Vec::new();
    for (from, to) in &pairs {
        let (from_db, from_name) = crate::driver::split_table_path_pub(from, current_db)?;
        let (from_db, from_name) = (from_db.to_lowercase(), from_name.to_lowercase());
        let (to_db, to_name) = crate::driver::split_table_path_pub(to, current_db)?;
        let (to_db, to_name) = (to_db.to_lowercase(), to_name.to_lowercase());

        if !staged_table_exists(catalog, &staged, &from_db, &from_name) {
            return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                format!("{from_db}.{from_name}"),
            )));
        }
        // Go checks the destination SCHEMA before the destination table, and
        // reports a missing one as 1025 rather than moving anything.
        if !catalog.has_database(&to_db) {
            return Err(DriverError::Schema(
                crate::SchemaErrorKind::RenameTargetDatabaseMissing {
                    from: format!("{from_db}.{from_name}"),
                    to: format!("{to_db}.{to_name}"),
                    database: to_db,
                },
            ));
        }
        if staged_table_exists(catalog, &staged, &to_db, &to_name) {
            return Err(DriverError::Schema(crate::SchemaErrorKind::TableExists(
                format!("{to_db}.{to_name}"),
            )));
        }
        // A foreign key names the referenced table, so moving one side would
        // leave the constraint pointing at a name that no longer resolves.
        if crate::foreign_key::participates(catalog, &from_db, &from_name) {
            return Err(DriverError::unsupported(
                "renaming a table involved in a FOREIGN KEY is not supported yet",
            ));
        }
        staged.push(Rename {
            from_db,
            from_name,
            to_db,
            to_name,
        });
    }

    for rename in &staged {
        catalog.rename_table(
            &rename.from_db,
            &rename.from_name,
            &rename.to_db,
            &rename.to_name,
        );
    }
    Ok(())
}

/// One validated pair, held back until every pair of the statement has passed.
/// All four names are lowercased, which is how the catalog keys them.
struct Rename {
    from_db: String,
    from_name: String,
    to_db: String,
    to_name: String,
}

/// Whether `database`.`name` would exist once `staged` had been applied.
///
/// Replaying the staged pairs in order is what makes the last word win: a
/// name vacated by an earlier pair reads as free, and one occupied by an
/// earlier pair reads as taken.
fn staged_table_exists(catalog: &Catalog, staged: &[Rename], database: &str, name: &str) -> bool {
    let mut exists = catalog.table_in(database, name).is_some();
    for rename in staged {
        if rename.from_db == database && rename.from_name == name {
            exists = false;
        }
        if rename.to_db == database && rename.to_name == name {
            exists = true;
        }
    }
    exists
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
        return Err(DriverError::unsupported(
            "only TRUNCATE TABLE is supported here",
        ));
    };
    let DdlStmt::TruncateTable(truncate) = &**ddl else {
        return Err(DriverError::unsupported(
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
/// Go drops the tables it finds and reports `ErrBadTable` for the names it
/// does not, rather than validating the whole list first: captured from TiDB,
/// `drop table d1, nosuch` removes `d1` AND errors.
///
/// Returns the names that were not there, which is not bookkeeping: under
/// `IF EXISTS` Go does not discard the error, it files it as a `Note` per
/// missing name (`pkg/ddl/executor.go` hands `StmtCtx.AppendNote` the same
/// `ErrBadTable`), so the caller needs the list rather than a bare `Ok`.
/// Without `IF EXISTS` the list becomes the error's own text and the return
/// is empty.
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
) -> Result<Vec<String>, DriverError> {
    let stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let drop = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::DropTable(drop) => drop,
            _ => {
                return Err(DriverError::unsupported(
                    "only DROP TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::unsupported(
                "only DROP TABLE is supported here",
            ))
        }
    };
    if drop.temporary != tidb_ast::DropTemporary::None {
        return Err(DriverError::unsupported(
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

    let mut missing = Vec::new();
    for path in &drop.names {
        let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        // A view is not a table: `DROP TABLE v` reports the name as unknown
        // rather than dropping the view (Go's own captured behaviour).
        let dropped =
            !catalog.is_view_in(&database, &name) && catalog.drop_table_in(&database, &name);
        if !dropped {
            missing.push(format!("{database}.{name}"));
        }
    }
    if !drop.if_exists && !missing.is_empty() {
        // Go accumulates the names it could not drop and reports them as ONE
        // `ErrBadTable` after the drops it did perform, so the message holds
        // the whole list: `drop table nosuchA, nosuchB` is captured from
        // `gorun` as `Unknown table 'test.nosuchA,test.nosuchB'`, not as the
        // first name alone.
        return Err(DriverError::Schema(crate::SchemaErrorKind::BadTable(
            missing.join(","),
        )));
    }
    Ok(missing)
}
