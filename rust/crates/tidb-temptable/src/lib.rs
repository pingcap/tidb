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
use std::sync::{Arc, Mutex};

use tidb_kv::storage::StorageError;
use tidb_model::db::DBInfo;
use tidb_tablecodec::table_key::encode_table_prefix;
use tidb_txnkv::Key;
use tidb_variable::temporary_table_data::TemporaryTableData;

pub mod interceptor;

/// What the DDL reads from the session that runs it (Go's `sctx`).
pub trait TemporaryTableContext {
    /// Go `SessionVars.LocalTemporaryTables`.
    fn local_temporary_tables(&mut self) -> &mut SessionTables;

    /// Go `GenGlobalID`: a real table id for a new or truncated table.
    fn gen_global_id(&mut self) -> Result<i64, DriverError>;

    /// Go `SessionVars.TemporaryTableData`.
    fn temporary_table_data(&mut self) -> &mut Option<SessionData>;
}

/// The session's `TemporaryTableData`, shared with the snapshot interceptor
/// that reads it.
pub type SessionData = Arc<Mutex<TemporaryTableData>>;

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
        ensure_session_data(self.sctx);
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
        let (_, _, table) = tables.remove(position);
        self.clear_temporary_table_records(table.table_id())
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
        let table = &mut self.sctx.local_temporary_tables()[position].2;
        let old_id = table.table_id();
        table.truncate_local_temporary_table(id);
        self.clear_temporary_table_records(old_id)
    }

    /// Go `clearTemporaryTableRecords`: every key of table `tbl_id` leaves
    /// the session data.
    fn clear_temporary_table_records(&mut self, tbl_id: i64) -> Result<(), DriverError> {
        let Some(session_data) = self.sctx.temporary_table_data().as_ref() else {
            return Ok(());
        };
        let mut session_data = session_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tbl_prefix = Key::from_bytes(encode_table_prefix(tbl_id));
        let end_key = Key::from_bytes(encode_table_prefix(tbl_id + 1));
        let storage_error = |error: StorageError| DriverError::unsupported(format!("{error:?}"));
        let mut keys = Vec::new();
        let mut iter = session_data
            .iter(Some(&tbl_prefix), Some(&end_key))
            .map_err(storage_error)?;
        while iter.valid() {
            let key = iter.key().clone();
            if !key.as_bytes().starts_with(tbl_prefix.as_bytes()) {
                break;
            }
            keys.push(key);
            iter.next().map_err(storage_error)?;
        }
        iter.close();
        for key in keys {
            session_data.delete_table_key(tbl_id, key).map_err(storage_error)?;
        }
        Ok(())
    }
}

/// Go `ensureSessionData`.
fn ensure_session_data<C: TemporaryTableContext + ?Sized>(sctx: &mut C) {
    sctx.temporary_table_data()
        .get_or_insert_with(|| Arc::new(Mutex::new(TemporaryTableData::new())));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Sctx {
        tables: SessionTables,
        next_id: i64,
        data: Option<SessionData>,
    }

    impl TemporaryTableContext for Sctx {
        fn local_temporary_tables(&mut self) -> &mut SessionTables {
            &mut self.tables
        }

        fn gen_global_id(&mut self) -> Result<i64, DriverError> {
            self.next_id += 1;
            Ok(self.next_id)
        }

        fn temporary_table_data(&mut self) -> &mut Option<SessionData> {
            &mut self.data
        }
    }

    fn row_key(tbl_id: i64) -> Key {
        Key::from_bytes([encode_table_prefix(tbl_id), b"_r1".to_vec()].concat())
    }

    fn stored(sctx: &Sctx, tbl_id: i64) -> Vec<u8> {
        sctx.data
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get(&row_key(tbl_id))
            .unwrap_or_default()
    }

    /// Go `ddl_test.go`'s create/drop/truncate over the session data: a
    /// created table makes the session data, and dropping or truncating a
    /// table clears its records and only its records.
    #[test]
    fn drop_and_truncate_clear_only_their_tables_records() {
        let mut sctx = Sctx::default();
        let db = DBInfo {
            id: 1,
            name: tidb_ast::CiString::new("test".to_owned()),
            ..DBInfo::default()
        };
        for name in ["t1", "t2"] {
            let info = tidb_model::TableInfo {
                name: tidb_ast::CiString::new(name.to_owned()),
                temp_table_type: tidb_model::TempTableType::LOCAL,
                columns: tidb_model::GoSharedPointerSlice::from_handles(vec![Some(
                    tidb_model::GoShared::new(tidb_model::ColumnInfo {
                        id: 1,
                        name: tidb_ast::CiString::new("a".to_owned()),
                        field_type: tidb_datatype::FieldType::new(
                            tidb_datatype::FieldTypeCode::Long,
                        ),
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::ColumnInfo::default()
                    }),
                )]),
                ..tidb_model::TableInfo::default()
            };
            get_temporary_table_ddl(&mut sctx)
                .create_local_temporary_table(&db, info)
                .unwrap();
        }
        let (t1, t2) = (sctx.tables[0].2.table_id(), sctx.tables[1].2.table_id());
        for id in [t1, t2] {
            sctx.data
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .set_table_key(id, row_key(id), b"row".to_vec())
                .unwrap();
        }
        get_temporary_table_ddl(&mut sctx)
            .truncate_local_temporary_table("test", "t1")
            .unwrap();
        assert!(stored(&sctx, t1).is_empty());
        assert_eq!(stored(&sctx, t2), b"row");
        assert_ne!(sctx.tables[0].2.table_id(), t1, "TRUNCATE takes a new id");
        get_temporary_table_ddl(&mut sctx)
            .drop_local_temporary_table("test", "t2")
            .unwrap();
        assert!(stored(&sctx, t2).is_empty());
    }
}
