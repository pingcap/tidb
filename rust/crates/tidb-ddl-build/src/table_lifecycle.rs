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

//! The checks of the statements that move or remove a whole table, Go
//! `pkg/ddl/executor.go`'s `dropTableObject` and `renameTable` admission.

use tidb_error::MysqlError;
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
) -> Result<bool, MysqlError> {
    let schema_key = schema.go_to_lower();
    let name_key = name.go_to_lower();
    if schema_key == "workload_schema"
        || (schema_key == "mysql"
            && matches!(
                name_key.as_str(),
                "tidb" | "gc_delete_range" | "gc_delete_range_done"
            ))
    {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_FORBIDDEN_DDL.generate_with_stack_by_args(&[format!(
                "Drop tidb system table '{}.{}'",
                schema_key, name_key
            )
            .into()]),
        ));
    }
    match expected {
        DropObjectKind::View if actual != DropObjectKind::View => {
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_WRONG_OBJECT.generate_with_stack_by_args(&[
                    schema.into(),
                    name.into(),
                    "VIEW".into(),
                ]),
            ))
        }
        DropObjectKind::Table if actual != DropObjectKind::Table => Ok(false),
        DropObjectKind::Table if cached => Err(tidb_error::MysqlError::from_terror(
            &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE
                .generate_with_stack_by_args(&["Drop Table".into()]),
        )),
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

/// Facts selected from the original catalog and the ordered rename overlay.
/// The source view flag comes from the original name, as Go ExtractTblInfos does.
pub struct RenameAdmission<'a> {
    /// Source database and table, preserving the submitting identifiers.
    pub source: (&'a str, &'a str),
    /// Destination database and table.
    pub target: (&'a str, &'a str),
    /// Source presence after earlier pairs in this statement.
    pub source_exists: bool,
    /// Destination database presence.
    pub target_schema_exists: bool,
    /// Destination presence after earlier pairs in this statement.
    pub target_exists: bool,
    /// Whether the original source name resolves to a view.
    pub source_is_view: bool,
    /// ALTER permits an identity rename and keeps missing-source precedence.
    pub is_alter: bool,
}

/// Go ExtractTblInfos admission in source order. False means ALTER's no-job
/// identity case; callers retain their existing staging and publication owner.
pub fn check_rename(admission: RenameAdmission<'_>) -> Result<bool, MysqlError> {
    let RenameAdmission {
        source: (from_db, from_name),
        target: (to_db, to_name),
        source_exists,
        target_schema_exists,
        target_exists,
        source_is_view,
        is_alter,
    } = admission;
    let occupied = || {
        tidb_error::MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_TABLE_EXISTS
                .generate_with_stack_by_args(&[(&format!("{to_db}.{to_name}")).into()]),
        )
    };
    if !source_exists {
        if !is_alter && target_exists {
            return Err(occupied());
        }
        return Err(tidb_error::MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[from_db.into(), from_name.into()]),
        ));
    }
    if is_alter
        && from_db.go_to_lower() == to_db.go_to_lower()
        && from_name.go_to_lower() == to_name.go_to_lower()
    {
        return Ok(false);
    }
    if source_is_view && from_db != to_db {
        return Err(MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_FORBID_SCHEMA_CHANGE
                .generate_with_stack_by_args(&[from_db.into(), to_db.into()]),
        ));
    }
    if !target_schema_exists {
        return Err(tidb_error::MysqlError::from_terror(
            &tidb_dbterror::ERR_ERROR_ON_RENAME.generate_with_stack_by_args(&[
                (&format!("{from_db}.{from_name}")).into(),
                (&format!("{to_db}.{to_name}")).into(),
                168.into(),
                format!("Database `{}` doesn't exist", (to_db)).into(),
            ]),
        ));
    }
    if target_exists {
        return Err(occupied());
    }
    if to_name.go_to_lower().chars().count() > 64 {
        return Err(tidb_error::MysqlError::from_terror(
            &tidb_dbterror::ERR_TOO_LONG_IDENT
                .generate_with_stack_by_args(&[to_name.to_string().into()]),
        ));
    }
    Ok(true)
}

/// Go's FK rename helper does no reference adjustment for an unchanged table
/// name, including a schema-only move. Keep this independent of SQL FK checks.
pub fn rename_changes_fk_reference(from_table: &str, to_table: &str) -> bool {
    tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst)
        && from_table.go_to_lower() != to_table.go_to_lower()
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

/// Go CreateTableWithInfo's shared view-name admission, including its worker
/// recheck against the publication snapshot. `existing` describes the durable
/// object only; true means a view and false means another object kind.
pub fn check_view_target(
    database: &str,
    name: &str,
    or_replace: bool,
    existing: Option<bool>,
) -> Result<(), MysqlError> {
    match existing {
        Some(_) if !or_replace => Err(tidb_error::MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_TABLE_EXISTS
                .generate_with_stack_by_args(&[(&format!("{database}.{name}")).into()]),
        )),
        Some(false) => Err(tidb_error::MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_WRONG_OBJECT.generate_with_stack_by_args(&[
                database.into(),
                name.into(),
                "VIEW".into(),
            ]),
        )),
        _ => Ok(()),
    }
}
