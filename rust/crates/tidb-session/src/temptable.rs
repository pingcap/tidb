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
//! `infoschema.SessionTables`) and its catalog's id allocator; and the
//! primitives `DDLExec`'s cases over them read
//! ([`tidb_executor::ddl_exec::TemporaryTableDdlExec`]).

use tidb_infoschema::session_tables::{self, SessionTables};

use crate::{DriverError, Session};

/// What a session's temporary tables read beyond their metadata: Go's
/// `sctx.Txn()` (the store every table writes through), and the
/// temporary tables of the statement's infoschema that the snapshot
/// interceptor and the commit's `temporaryTableKVFilter` consult.
#[derive(Clone, Debug, Default)]
pub(crate) struct SessionTemporaryTables {
    /// The session transaction's store; `None` for a session no node opened,
    /// whose temporary tables keep rows of their own.
    pub(crate) txn_store: Option<Box<dyn tidb_kv::storage::TableStorage>>,
    /// The temporary tables by id and kind.
    pub(crate) kinds: tidb_temptable::interceptor::TemporaryTableKinds,
    /// The same ids, for the commit's filter.
    pub(crate) ids: tidb_store_driver_txn::session_commit_protocol::TemporaryTableIds,
}

impl SessionTemporaryTables {
    /// Publishes the temporary tables the statement's infoschema holds.
    pub(crate) fn publish(&self, kinds: std::collections::HashMap<i64, tidb_model::TempTableType>) {
        let ids = kinds.keys().copied().collect();
        *self
            .ids
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ids;
        *self
            .kinds
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = kinds;
    }
}

/// Binds an empty snapshot under `session`'s temporary-table interceptor
/// when `slot` has none, for an in-process read of a temporary table (its
/// rows are never in the store; Go builds a dummy `TableReader` for it).
/// Answers whether it bound one, for [`finish_temporary_read_snapshot`].
pub(crate) fn bind_temporary_read_snapshot(
    slot: &crate::statement_lifecycle::SnapshotSlot,
    session: &Session,
) -> bool {
    let unbound = !slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_bound();
    if unbound {
        drop(crate::statement_lifecycle::bind_snapshot(
            slot,
            session,
            Box::new(tidb_temptable::interceptor::EmptySnapshot),
        ));
    }
    unbound
}

/// Unbinds what [`bind_temporary_read_snapshot`] bound.
pub(crate) fn finish_temporary_read_snapshot(
    slot: &crate::statement_lifecycle::SnapshotSlot,
    bound_here: bool,
) {
    if bound_here {
        drop(crate::statement_lifecycle::finish_snapshot(slot));
    }
}

impl tidb_temptable::TemporaryTableContext for Session {
    fn local_temporary_tables(&mut self) -> &mut SessionTables {
        &mut self.local_temporary_tables
    }

    fn gen_global_id(&mut self) -> Result<i64, DriverError> {
        Session::gen_global_id(self)
    }

    fn temporary_table_data(&mut self) -> &mut Option<tidb_temptable::SessionData> {
        &mut self.temporary_table_data
    }
}

impl Session {
    /// Go `sctx.Txn()` for the running statement: a stale read's historical
    /// store while one is active, the session transaction's otherwise.
    pub(crate) fn transaction_store(&self) -> Option<Box<dyn tidb_kv::storage::TableStorage>> {
        if let Some((_, read)) = &self.statement_snapshot {
            return Some(read.store.clone());
        }
        if let Some((_, read)) = self.txn_ctx.as_ref().and_then(|ctx| ctx.stale.as_ref()) {
            return Some(read.store.clone());
        }
        self.temporary.txn_store.clone()
    }

    /// Binds the session's transaction store (Go `sctx.Txn()`), which every
    /// table of the session, temporary ones included, reads and writes
    /// through.
    pub fn set_transaction_store(&mut self, store: Box<dyn tidb_kv::storage::TableStorage>) {
        self.temporary.txn_store = Some(store);
    }

    /// Go `temptable.SessionSnapshotInterceptor(sctx, is)` over `snapshot`.
    pub(crate) fn session_snapshot_interceptor(
        &self,
        snapshot: Box<dyn tidb_kv::cluster_storage::ClusterSnapshot>,
    ) -> Box<dyn tidb_kv::cluster_storage::ClusterSnapshot> {
        match tidb_temptable::interceptor::session_snapshot_interceptor(
            snapshot,
            std::sync::Arc::clone(&self.temporary.kinds),
            self.temporary_table_data.clone(),
        ) {
            Ok(interceptor) => Box::new(interceptor),
            Err(snapshot) => snapshot,
        }
    }

    /// The temporary tables the commit's `temporaryTableKVFilter` drops.
    pub(crate) fn temporary_table_ids(
        &self,
    ) -> tidb_store_driver_txn::session_commit_protocol::TemporaryTableIds {
        self.temporary.ids.clone()
    }

    /// Go `commitTxnWithTemporaryData`, before `txn.Commit`: every key a
    /// MODIFIED local temporary table holds in the transaction's buffer goes
    /// into the session data under one stage (an empty value deletes).
    /// [`Self::finish_temporary_data_commit`] releases the stage after a
    /// successful commit and cleans it up otherwise.
    pub(crate) fn stage_temporary_data_commit(
        &self,
        buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
    ) -> Result<
        Option<(tidb_variable::temporary_table_data::SessionData, tidb_kv::storage::StagingHandle)>,
        tidb_kv::storage::StorageError,
    > {
        let modified: Vec<i64> = self
            .txn_temporary_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(_, table)| table.get_modified())
            .map(|(id, _)| *id)
            .collect();
        if modified.is_empty() {
            return Ok(None);
        }
        let Some(session_data) = self.temporary_table_data.clone() else {
            return Ok(None);
        };
        let local: Vec<i64> = modified
            .into_iter()
            .filter(|id| {
                self.local_temporary_tables
                    .iter()
                    .any(|(_, _, table)| table.table_id() == *id)
            })
            .collect();
        if local.is_empty() {
            return Ok(None);
        }
        let mut data = session_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stage = data.staging();
        for tbl_id in local {
            let tbl_prefix =
                tidb_txnkv::Key::from_bytes(tidb_tablecodec::table_key::encode_table_prefix(tbl_id));
            let end_key = tidb_txnkv::Key::from_bytes(
                tidb_tablecodec::table_key::encode_table_prefix(tbl_id + 1),
            );
            let pairs = buffer.snapshot_of(&[(tbl_prefix.clone(), end_key.clone())]);
            for (key, value) in pairs.entries(&tbl_prefix, &end_key, false) {
                let written = if value.is_empty() {
                    data.delete_table_key(tbl_id, key)
                } else {
                    data.set_table_key(tbl_id, key, value)
                };
                if let Err(error) = written {
                    data.cleanup(stage);
                    return Err(error);
                }
            }
        }
        drop(data);
        Ok(Some((session_data, stage)))
    }

    /// The second half of Go `commitTxnWithTemporaryData`.
    pub(crate) fn finish_temporary_data_commit(
        staged: Option<(tidb_variable::temporary_table_data::SessionData, tidb_kv::storage::StagingHandle)>,
        committed: bool,
    ) {
        let Some((session_data, stage)) = staged else {
            return;
        };
        let mut data = session_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if committed {
            data.release(stage);
        } else {
            data.cleanup(stage);
        }
    }

    /// Go `temptable.GetTemporaryTableDDL(sctx)`.
    pub(crate) fn temporary_table_ddl(&mut self) -> tidb_temptable::TemporaryTableDdl<'_, Self> {
        tidb_temptable::get_temporary_table_ddl(self)
    }

    /// Go `SessionTables.TableByName` over `SessionVars.LocalTemporaryTables`.
    pub(crate) fn local_temporary_table(
        &self,
        schema: &str,
        name: &str,
    ) -> Option<&tidb_table::tables::TableCommon> {
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

impl tidb_executor::ddl_exec::TemporaryTableDdlExec for Session {
    fn has_local_temporary_table(&self, schema: &str, name: &str) -> bool {
        Session::is_local_temporary_table(self, schema, name)
    }

    fn session_info_schema(
        &self,
    ) -> Option<std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema>> {
        self.info_schema.clone()
    }

    fn append_suppressed(&mut self, error: DriverError) {
        Session::append_suppressed(self, error);
    }

    fn drain_eval_warnings(&mut self, ctx: &tidb_sessionctx::StmtContext) {
        Session::drain_eval_warnings(self, ctx);
    }
}
