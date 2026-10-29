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

//! The session as Go `temptable.TemporaryTableDDL`'s `sctx`: its LOCAL
//! temporary tables (`SessionVars.LocalTemporaryTables`, an
//! `infoschema.SessionTables`) and its catalog's id allocator.

use tidb_infoschema::session_tables::{self, SessionTables};

use crate::{DriverError, Session};

impl tidb_temptable::TemporaryTableContext for Session {
    fn local_temporary_tables(&mut self) -> &mut SessionTables {
        &mut self.local_temporary_tables
    }

    fn gen_global_id(&mut self) -> Result<i64, DriverError> {
        Session::gen_global_id(self)
    }
}

impl Session {
    /// Go `temptable.GetTemporaryTableDDL(sctx)`.
    pub(crate) fn temporary_table_ddl(&mut self) -> tidb_temptable::TemporaryTableDdl<'_, Self> {
        tidb_temptable::get_temporary_table_ddl(self)
    }

    /// Go `SessionTables.TableByName` over `SessionVars.LocalTemporaryTables`.
    pub(crate) fn local_temporary_table(
        &self,
        schema: &str,
        name: &str,
    ) -> Option<&tidb_table::kv_table::KvTable> {
        session_tables::table_by_name(&self.local_temporary_tables, schema, name)
    }

    /// [`Self::local_temporary_table`] for `true`.
    pub(crate) fn is_local_temporary_table(&self, schema: &str, name: &str) -> bool {
        session_tables::position(&self.local_temporary_tables, schema, name).is_some()
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
}
