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

//! Go `pkg/table/temptable` (`ddl.go`): `TemporaryTableDDL` over the
//! session that runs it -- creating, dropping and truncating the session's LOCAL
//! temporary tables (Go `SessionVars.LocalTemporaryTables`).

use tidb_driver_error::DriverError;
use tidb_hack::go_to_lower;
use tidb_infoschema::session_tables::{self, SessionTables};
use tidb_model::db::DBInfo;

/// What the DDL reads from the session that runs it (Go's `sctx`).
pub trait TemporaryTableContext {
    /// Go `SessionVars.LocalTemporaryTables`.
    fn local_temporary_tables(&mut self) -> &mut SessionTables;

    /// Go `GenGlobalID`: a real table id for a new or truncated table.
    fn gen_global_id(&mut self) -> Result<i64, DriverError>;
}

/// Go `temptable.TemporaryTableDDL`: the three DDL operations on the
/// session's LOCAL temporary tables.
pub struct TemporaryTableDdl<'a, C: TemporaryTableContext + ?Sized> {
    sctx: &'a mut C,
}

/// Go `temptable.GetTemporaryTableDDL(sctx)`.
pub fn get_temporary_table_ddl<C: TemporaryTableContext + ?Sized>(
    sctx: &mut C,
) -> TemporaryTableDdl<'_, C> {
    TemporaryTableDdl { sctx }
}

/// Go `checkLocalTemporaryExistsAndReturn`'s `ErrTableNotExists`.
fn local_temporary_not_exists(schema: &str, name: &str) -> DriverError {
    DriverError::from(
        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
            .generate_with_stack_by_args(&[(&schema).into(), (&name).into()]),
    )
}

impl<C: TemporaryTableContext + ?Sized> TemporaryTableDdl<'_, C> {
    /// Go `CreateLocalTemporaryTable`: the table, in its database
    /// (`info.DBID = db.ID`) under a real table id (Go `GenGlobalID` in
    /// `newTemporaryTableFromTableInfo`), joins the session's LOCAL temporary
    /// tables under `db.Name.L` and `info.Name.L` (`SessionTables.AddTable`).
    pub fn create_local_temporary_table(
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
            .local_temporary_tables()
            .push((db.name.lowercase().to_owned(), name, table));
        Ok(())
    }

    /// Go `DropLocalTemporaryTable`: the table leaves the session, and its
    /// rows with it.
    pub fn drop_local_temporary_table(&mut self, schema: &str, name: &str) -> Result<(), DriverError> {
        let tables = self.sctx.local_temporary_tables();
        let position = session_tables::position(tables, schema, name)
            .ok_or_else(|| local_temporary_not_exists(schema, name))?;
        tables.remove(position);
        Ok(())
    }

    /// Go `TruncateLocalTemporaryTable`: the same metadata under a new table
    /// id, with no rows and fresh allocators.
    pub fn truncate_local_temporary_table(
        &mut self,
        schema: &str,
        name: &str,
    ) -> Result<(), DriverError> {
        let position = session_tables::position(self.sctx.local_temporary_tables(), schema, name)
            .ok_or_else(|| local_temporary_not_exists(schema, name))?;
        let id = self.sctx.gen_global_id()?;
        self.sctx.local_temporary_tables()[position]
            .2
            .recreate_local_temporary(id);
        Ok(())
    }
}
