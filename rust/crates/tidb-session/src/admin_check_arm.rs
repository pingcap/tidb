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

//! `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX`: the `AdminStmt::AdminCheck` arm
//! of [`crate::Session::dispatch_admin_stmt`].
//!
//! The check itself lives in `tidb_executor::admin_check` (which mirrors Go
//! `pkg/executor/check_table_index.go`); this file is only the statement
//! seam -- name resolution, the output SHAPE, and the refusals.
//!
//! # The shapes, from TiDB's own recording
//!
//! From `tests/integrationtest/r/util/admin.result` and
//! `tests/integrationtest/r/executor/admin.result`, which are TiDB's oracle
//! and are only ever read here:
//!
//! - `admin check table t;` -- NO output at all: Go's `CheckTable` plan has
//!   an EMPTY schema, so the server sends an OK packet, not a zero-column
//!   result set (see [`admin_check_passed`]). Producing nothing is the whole
//!   success contract, which is exactly why the check underneath must be
//!   real: an `Ok` returned without reading a byte is indistinguishable from
//!   a passing check, and would be a success return that is not one.
//! - `admin check index t idx;` -- also no output; the same consistency
//!   check, narrowed to one index.
//! - `admin check index t idx (2, 4);` -- a RESULT SET: the indexed columns
//!   followed by `extra_handle`, in index order, for the rows whose handle
//!   falls in a half-open interval.
//!
//! # What this refuses, by name
//!
//! A table whose rows are not stored as index-bearing bytes has no index
//! entries to check against, so `ADMIN CHECK` over one is REFUSED rather than
//! answered OK. Same for the multi-table `ADMIN CHECK TABLE t1, t2` form,
//! which Go's own planner rejects.

use crate::*;

impl Session {
    pub(crate) fn admin_check_stmt(
        &mut self,
        check: &tidb_ast::AdminCheckStmt,
    ) -> Result<StmtOutput, DriverError> {
        match check {
            tidb_ast::AdminCheckStmt::Table { tables } => {
                let [path] = tables.as_slice() else {
                    // Go's parser accepts the list and its planner then
                    // refuses it; refusing here keeps the same outcome.
                    return Err(DriverError::Unsupported(
                        "ADMIN CHECK TABLE checks one table at a time",
                    ));
                };
                let (database, name) = self.split_table_path(path)?;
                self.run_admin_check(&database, &name, None)?;
                Ok(admin_check_passed())
            }
            tidb_ast::AdminCheckStmt::Index {
                table,
                index,
                handle_ranges,
            } => {
                let (database, name) = self.split_table_path(table)?;
                if handle_ranges.is_empty() {
                    self.run_admin_check(&database, &name, Some(index))?;
                    return Ok(admin_check_passed());
                }
                let index = index.clone();
                let ranges = handle_ranges.clone();
                let zone = self.statement_context(false).session_zone();
                let (names, rows) = self.with_catalog_mut(|catalog| {
                    let table = admin_check_table_mut(catalog, &database, &name)?;
                    tidb_executor::admin_check::check_index_ranges(table, &index, &ranges, &zone)
                        .map_err(admin_check_error)
                })?;
                let text = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let columns = names.into_iter().map(|name| (name, text.clone())).collect();
                Ok(StmtOutput::Rows { columns, rows })
            }
        }
    }

    /// Resolves a written table path against the current database.
    fn split_table_path(&self, path: &[String]) -> Result<(String, String), DriverError> {
        match path {
            [name] => Ok((self.require_current_database()?.to_owned(), name.clone())),
            [database, name] => Ok((database.clone(), name.clone())),
            _ => Err(DriverError::Unsupported("empty table name")),
        }
    }

    fn run_admin_check(
        &mut self,
        database: &str,
        name: &str,
        only_index: Option<&str>,
    ) -> Result<usize, DriverError> {
        let (database, name) = (database.to_owned(), name.to_owned());
        let only_index = only_index.map(str::to_owned);
        let zone = self.statement_context(false).session_zone();
        self.with_catalog_mut(|catalog| {
            let table = admin_check_table_mut(catalog, &database, &name)?;
            tidb_executor::admin_check::check_table(table, only_index.as_deref(), &zone)
                .map_err(admin_check_error)
        })
    }
}

/// What a passing `ADMIN CHECK TABLE`/`CHECK INDEX` answers.
///
/// NOT an empty result set: Go's `CheckTable` plan is a
/// `SimpleSchemaProducer` that never calls `SetSchema`, so its schema is
/// EMPTY and the server replies with an OK packet rather than a zero-column
/// result set. `r/util/admin.result` records the statement with nothing
/// under it, which a zero-column result set would render as a blank header
/// line -- one line where TiDB wrote none.
fn admin_check_passed() -> StmtOutput {
    StmtOutput::Affected(0)
}

/// The storage-backed table `ADMIN CHECK` needs, or the refusal that names
/// why it is not checkable.
fn admin_check_table_mut<'a>(
    catalog: &'a mut tidb_executor::Catalog,
    database: &str,
    name: &str,
) -> Result<&'a mut tidb_executor::kv_table::KvTable, DriverError> {
    let Some(entry) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
            "{database}.{name}"
        ))));
    };
    match entry {
        tidb_executor::TableEntry::Kv(table) => Ok(table),
        // A view has no rows of its own and a sequence has no index; a
        // `MemTable` has rows but stores no index entries, so "consistent"
        // would be a statement about nothing.
        _ => Err(DriverError::Unsupported(
            "ADMIN CHECK needs a storage-backed table: this table stores no index entries to \
             check its rows against",
        )),
    }
}

/// Maps the check's own outcome onto the errors TiDB reports.
fn admin_check_error(error: tidb_executor::admin_check::AdminCheckError) -> DriverError {
    use tidb_executor::admin_check::AdminCheckError;
    match error {
        AdminCheckError::CountMismatch {
            table_count,
            index,
            index_count,
        } => DriverError::AdminCheckTable(format!(
            "table count {table_count} != index({index}) count {index_count}"
        )),
        AdminCheckError::Inconsistent {
            table,
            index,
            handle,
            index_values,
            record_values,
        } => DriverError::DataInconsistent(format!(
            "data inconsistency in table: {table}, index: {index}, handle: {handle}, \
             index-values:{index_values} != record-values:{record_values}"
        )),
        AdminCheckError::UnknownIndex { index, table } => {
            DriverError::UnknownIndex(format!("{index} on {table}"))
        }
        AdminCheckError::NotStored(detail) | AdminCheckError::Decode(detail) => {
            DriverError::UnsupportedKind(detail)
        }
    }
}
