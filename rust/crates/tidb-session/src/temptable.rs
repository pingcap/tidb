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

//! Go `pkg/table/temptable/ddl.go`: `TemporaryTableDDL` over the session
//! (`temporaryTableDDL{sctx}`), and the session's LOCAL temporary tables it
//! keeps (`SessionVars.LocalTemporaryTables`, an `infoschema.SessionTables`).
//!
//! It stays in the session crate because its state is the session's: the
//! table list lives on [`Session`] and a new table's id comes from the
//! session's catalog allocator.

use tidb_model::db::DBInfo;
use tidb_hack::go_to_lower;

use crate::{DriverError, Session};

/// Go `temptable.TemporaryTableDDL`: the three DDL operations on the
/// session's LOCAL temporary tables.
pub(crate) struct TemporaryTableDdl<'a> {
    sctx: &'a mut Session,
}

impl Session {
    /// Go `temptable.GetTemporaryTableDDL(sctx)`.
    pub(crate) fn temporary_table_ddl(&mut self) -> TemporaryTableDdl<'_> {
        TemporaryTableDdl { sctx: self }
    }

    /// Go `SessionTables.TableByName` over `SessionVars.LocalTemporaryTables`:
    /// the tables are keyed by the lowercased schema and table names
    /// (`schemaMap[schema.L].tables[table.L]`), so a lookup lowercases the
    /// names it is given and compares them with the keys. The table keeps
    /// the name it was created with (`tbl.Meta().Name.O`).
    pub(crate) fn local_temporary_table(
        &self,
        schema: &str,
        name: &str,
    ) -> Option<&tidb_table::kv_table::KvTable> {
        self.local_temporary_position(schema, name)
            .map(|position| &self.local_temporary_tables[position].2)
    }

    /// [`Self::local_temporary_table`] for `true`.
    pub(crate) fn is_local_temporary_table(&self, schema: &str, name: &str) -> bool {
        self.local_temporary_position(schema, name).is_some()
    }

    /// [`Self::is_local_temporary_table`] for a written name path, an
    /// unqualified one in the current database.
    pub(crate) fn is_local_temporary_table_path(&self, path: &[String]) -> bool {
        match path {
            [table] => self.is_local_temporary_table(&self.vars.current_db, table),
            [schema, table] => self.is_local_temporary_table(schema, table),
            _ => false,
        }
    }

    fn local_temporary_position(&self, schema: &str, name: &str) -> Option<usize> {
        let (schema, name) = (go_to_lower(schema), go_to_lower(name));
        self.local_temporary_tables
            .iter()
            .position(|(db, table, _)| *db == schema && *table == name)
    }
}

/// Go `checkLocalTemporaryExistsAndReturn`'s `ErrTableNotExists`.
fn local_temporary_not_exists(schema: &str, name: &str) -> DriverError {
    DriverError::from(
        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
            .generate_with_stack_by_args(&[(&schema).into(), (&name).into()]),
    )
}

impl TemporaryTableDdl<'_> {
    /// Go `CreateLocalTemporaryTable`: the table, in its database
    /// (`info.DBID = db.ID`) under a real table id (Go `GenGlobalID` in
    /// `newTemporaryTableFromTableInfo`), joins the session's LOCAL temporary
    /// tables under `db.Name.L` and `info.Name.L` (`SessionTables.AddTable`).
    pub(crate) fn create_local_temporary_table(
        &mut self,
        db: &DBInfo,
        mut info: tidb_model::TableInfo,
    ) -> Result<(), DriverError> {
        info.db_id = db.id;
        let id = self.sctx.gen_global_id()?;
        let name = go_to_lower(info.name.original());
        let table = tidb_table::table_from_meta::new_temporary_table_from_table_info(info, id)
            .map_err(DriverError::unsupported)?;
        self.sctx
            .local_temporary_tables
            .push((db.name.lowercase().to_owned(), name, table));
        Ok(())
    }

    /// Go `DropLocalTemporaryTable`: the table leaves the session, and its
    /// rows with it.
    pub(crate) fn drop_local_temporary_table(
        &mut self,
        schema: &str,
        name: &str,
    ) -> Result<(), DriverError> {
        let position = self
            .sctx
            .local_temporary_position(schema, name)
            .ok_or_else(|| local_temporary_not_exists(schema, name))?;
        self.sctx.local_temporary_tables.remove(position);
        Ok(())
    }

    /// Go `TruncateLocalTemporaryTable`: the same metadata under a new table
    /// id, with no rows and fresh allocators.
    pub(crate) fn truncate_local_temporary_table(
        &mut self,
        schema: &str,
        name: &str,
    ) -> Result<(), DriverError> {
        let position = self
            .sctx
            .local_temporary_position(schema, name)
            .ok_or_else(|| local_temporary_not_exists(schema, name))?;
        let id = self.sctx.gen_global_id()?;
        self.sctx.local_temporary_tables[position]
            .2
            .recreate_local_temporary(id);
        Ok(())
    }
}
