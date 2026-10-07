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
use tidb_hack::GoToLower;

/// The object kind relevant to Go's DROP TABLE / DROP VIEW admission.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DropObjectKind {
    Table,
    View,
    Other,
}

/// Classic Go `dropTableObject` admission, after resolving an existing object.
/// A non-table target of DROP TABLE is collected as missing; a wrong VIEW
/// target is an immediate error. Protected objects precede either distinction.
pub fn check_drop_object(
    schema: &str,
    name: &str,
    expected: DropObjectKind,
    actual: DropObjectKind,
    cached: bool,
) -> Result<bool, DriverError> {
    let schema_key = schema.go_to_lower();
    let name_key = name.go_to_lower();
    if schema_key == "workload_schema"
        || (schema_key == "mysql"
            && matches!(
                name_key.as_str(),
                "tidb" | "gc_delete_range" | "gc_delete_range_done"
            ))
    {
        return Err(DriverError::Mysql(crate::MysqlError::new(
            tidb_error::tidb::errcode::ErrForbiddenDDL,
            format!("Drop tidb system table '{schema_key}.{name_key}' is forbidden"),
        )));
    }
    match expected {
        DropObjectKind::View if actual != DropObjectKind::View => {
            Err(DriverError::Schema(crate::SchemaErrorKind::WrongObject {
                name: format!("{schema}.{name}"),
                expected: "VIEW",
            }))
        }
        DropObjectKind::Table if actual != DropObjectKind::Table => Ok(false),
        DropObjectKind::Table if cached => Err(DriverError::OperationOnCachedTable("Drop Table")),
        _ => Ok(true),
    }
}

/// A target error interrupts immediately; absent names are reported only after
/// all independently completed targets. Keep each caller's typed error intact.
pub enum DropObjectsError<E> {
    Target(E),
    Missing(Vec<String>),
}

/// Go `dropTableObject`'s ordered completion and deferred missing-name policy.
/// The callback must publish one target before returning true.
pub fn drop_objects<E>(
    names: &[(String, String)],
    if_exists: bool,
    mut drop_one: impl FnMut(&str, &str) -> Result<bool, E>,
) -> Result<Vec<String>, DropObjectsError<E>> {
    let mut missing = Vec::new();
    for (schema, name) in names {
        if !drop_one(schema, name).map_err(DropObjectsError::Target)? {
            missing.push(format!("{schema}.{name}"));
        }
    }
    if !if_exists && !missing.is_empty() {
        return Err(DropObjectsError::Missing(missing));
    }
    Ok(missing)
}

/// Shared FK preflight over the original persistent target set.
pub fn check_drop_table_references(
    catalog: &Catalog,
    names: &[(String, String)],
) -> Result<(), DriverError> {
    crate::foreign_key::check_drop_tables(catalog, names)
}

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
    let cache_operation = if pairs.len() == 1 {
        "Rename Table"
    } else {
        "Rename Tables"
    };
    for (from, to) in &pairs {
        let (from_db, from_name) = crate::driver::split_table_path_pub(from, current_db)?;
        let (from_db, from_name) = (from_db.go_to_lower(), from_name.go_to_lower());
        let (to_db, to_name) = crate::driver::split_table_path_pub(to, current_db)?;
        let (to_db, to_name) = (to_db.go_to_lower(), to_name.go_to_lower());

        super::refuse_local_temporary_table_ddl(catalog, &from_db, &from_name, "RENAME TABLE")?;
        if !staged_table_exists(catalog, &staged, &from_db, &from_name) {
            return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                format!("{from_db}.{from_name}"),
            )));
        }
        let source_cached = staged
            .iter()
            .rev()
            .find(|rename| rename.to_db == from_db && rename.to_name == from_name)
            .map_or_else(
                || {
                    matches!(
                        catalog.table_in(&from_db, &from_name),
                        Some(crate::TableEntry::Kv(table)) if table.is_cache_table()
                    )
                },
                |rename| rename.source_cached,
            );
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
        if source_cached {
            return Err(DriverError::OperationOnCachedTable(cache_operation));
        }
        staged.push(Rename {
            from_db,
            from_name,
            to_db,
            to_name,
            source_cached,
        });
    }

    for rename in &staged {
        crate::foreign_key::rewrite_table_references(
            catalog,
            &rename.from_db,
            &rename.from_name,
            &rename.to_db,
            &rename.to_name,
        );
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
    source_cached: bool,
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
    run_truncate_table_in_with_foreign_key_checks(sql, catalog, current_db, sql_mode, true)
}

/// Runs `TRUNCATE TABLE` with the issuing session's `foreign_key_checks`
/// switch. Go's owner check is skipped when the switch is OFF, and a
/// self-referencing constraint is ignored because truncation removes the
/// child and parent rows together.
pub fn run_truncate_table_in_with_foreign_key_checks(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    sql_mode: tidb_parser::SqlMode,
    foreign_key_checks: bool,
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
    if foreign_key_checks {
        if let Some(error) = crate::foreign_key::find_table_referred(
            catalog,
            &database,
            &name,
            &[(database.clone(), name.clone())],
        ) {
            return Err(error);
        }
    }
    if let Some(crate::TableEntry::Kv(table)) = catalog.table_in(&database, &name) {
        if table.temp_table_type() == tidb_model::TempTableType::LOCAL {
            let mut replacement = table.as_ref().clone();
            let id = catalog.allocate_local_temporary_table_id()?;
            replacement.recreate_local_temporary(id);
            *catalog
                .table_mut_in(&database, &name)
                .expect("resolved local table") =
                crate::TableEntry::Kv(std::sync::Arc::new(replacement));
            return Ok(());
        }
    }
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(&database, &name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        )));
    };
    if table.is_cache_table() {
        return Err(DriverError::OperationOnCachedTable("Truncate Table"));
    }
    // TRUNCATE starts the counter over, and on a shared counter that is a
    // write like any other: a failure here must not be reported as a
    // successful truncate whose next insert then collides.
    std::sync::Arc::make_mut(table)
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
/// A plain `DROP TABLE` drops a LOCAL temporary table too, and does it
/// without a DDL job: Go strips such names out of the statement first
/// (`pkg/executor/ddl.go:118`) and, when none are left, never opens a
/// transaction at all. Here the local temporary table is in the catalog for
/// the statement's duration (the session's overlay attached it), so removing
/// it is the same call -- and the session's detach is what makes it
/// permanent, and what puts back any permanent table the temporary one was
/// shadowing.
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
    run_drop_table_stmt_in(drop, catalog, current_db, foreign_key_checks)
}

/// Execute the already parsed DROP using one resolved temporary/persistent split.
pub fn run_drop_table_stmt_in(
    drop: &tidb_ast::DropTableStmt,
    catalog: &mut Catalog,
    current_db: &str,
    foreign_key_checks: bool,
) -> Result<Vec<String>, DriverError> {
    // The kind of table each written name currently resolves to, resolved
    // ONCE so the two temporary arms below and the ordinary drop all judge
    // the same catalog state.
    // `None` is "no such object"; a view or a sequence answers
    // `TempTableType::NONE`, because Go's `TableByName` finds those too and
    // judges them by the `TempTableType` their `TableInfo` carries.
    let kind_of =
        |catalog: &Catalog, database: &str, name: &str| match catalog.table_in(database, name) {
            Some(crate::TableEntry::Kv(table)) => Some(table.temp_table_type()),
            Some(_) => Some(tidb_model::TempTableType::NONE),
            None => None,
        };

    let (persistent, local) = split_drop_table_targets(drop, |path| {
        crate::driver::split_table_path_pub(path, current_db).is_ok_and(|(db, table)| {
            kind_of(catalog, db, table) == Some(tidb_model::TempTableType::LOCAL)
        })
    });
    match drop.temporary {
        tidb_ast::DropTemporary::None => {}
        // Go `DDLExec.Next` (`pkg/executor/ddl.go:129`): `DROP TEMPORARY
        // TABLE` matches LOCAL temporary tables only. Every other name --
        // a permanent table, a GLOBAL temporary table, a name that is not
        // there at all -- is collected and reported as ONE
        // `infoschema.ErrTableDropExists` (1051), and the statement drops
        // NOTHING: Go returns before `dropLocalTemporaryTables` runs, so a
        // local temporary table listed beside a bad name survives.
        tidb_ast::DropTemporary::Local => {
            let mut not_local = Vec::new();
            for path in &drop.names {
                let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
                if kind_of(catalog, database, name) != Some(tidb_model::TempTableType::LOCAL) {
                    not_local.push(format!("{database}.{name}"));
                }
            }
            if !not_local.is_empty() {
                if drop.if_exists {
                    // Go files the same error as a NOTE and returns success,
                    // which is what the caller's returned list becomes.
                    return Ok(vec![not_local.join(",")]);
                }
                return Err(DriverError::Schema(crate::SchemaErrorKind::BadTable(
                    not_local.join(","),
                )));
            }
            drop_local_table_targets(&local, catalog, current_db)?;
            return Ok(Vec::new());
        }
        // Go `checkDropTemporaryTableGrammar` (`preprocess.go:1122`): a name
        // that does not exist is left to the drop itself, but a name that
        // exists and is not a GLOBAL temporary table is 8007 -- checked over
        // the WHOLE list before anything is dropped, as a preprocessor pass
        // is. The drop that follows is the ordinary one.
        tidb_ast::DropTemporary::Global => {
            for path in &drop.names {
                let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
                match kind_of(catalog, database, name) {
                    None | Some(tidb_model::TempTableType::GLOBAL) => {}
                    Some(_) => return Err(DriverError::DropTableOnTemporaryTable),
                }
            }
        }
    }

    // Local targets have no durable FK/index owner. Execute the persistent
    // operation first; an error must leave every local target intact.
    let drop = &persistent;
    // Go `dropTableObject` checks references across the WHOLE
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

    let names = drop
        .names
        .iter()
        .map(|path| {
            crate::driver::split_table_path_pub(path, current_db)
                .map(|(schema, name)| (schema.to_owned(), name.to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let missing = drop_objects(&names, drop.if_exists, |schema, name| {
        let Some(entry) = catalog.table_in(schema, name) else {
            return Ok(false);
        };
        let actual = if entry.is_view() {
            DropObjectKind::View
        } else if entry.is_sequence() {
            DropObjectKind::Other
        } else {
            DropObjectKind::Table
        };
        let cached = matches!(entry, crate::TableEntry::Kv(table) if table.is_cache_table());
        if !check_drop_object(schema, name, DropObjectKind::Table, actual, cached)? {
            return Ok(false);
        }
        Ok(catalog.drop_table_in(schema, name))
    })
    .map_err(|error| match error {
        DropObjectsError::Target(error) => error,
        DropObjectsError::Missing(names) => {
            DriverError::Schema(crate::SchemaErrorKind::BadTable(names.join(",")))
        }
    })?;
    drop_local_table_targets(&local, catalog, current_db)?;
    Ok(missing)
}

/// Go DDLExec.Next removes local names backwards, preserving the persistent
/// order and dropping local names in reverse order after cluster success.
pub fn split_drop_table_targets(
    drop: &tidb_ast::DropTableStmt,
    mut is_local: impl FnMut(&[String]) -> bool,
) -> (tidb_ast::DropTableStmt, Vec<Vec<String>>) {
    let mut persistent = drop.clone();
    let mut local = Vec::new();
    for index in (0..persistent.names.len()).rev() {
        if is_local(&persistent.names[index]) {
            local.push(persistent.names.remove(index));
        }
    }
    (persistent, local)
}

fn drop_local_table_targets(
    names: &[Vec<String>],
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    for path in names {
        let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
        if !matches!(catalog.table_in(database, name), Some(crate::TableEntry::Kv(table))
            if table.temp_table_type() == tidb_model::TempTableType::LOCAL)
        {
            return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                format!("{database}.{name}"),
            )));
        }
        catalog.drop_table_in(database, name);
    }
    Ok(())
}
