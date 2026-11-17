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

//! What a BRIE task uses of this node (Go `tidbGlue`'s store, domain and
//! sessions): the TiKV store with snapshot reads and the coprocessor
//! client, the statistics and PD label rules of the domain, and internal
//! sessions from the domain's pool.

use std::sync::Arc;
use std::time::Duration;

use tidb_br::errors::{errorf, Result};
use tidb_domain_core::domain::Domain;
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, TikvTxn};
use tidb_txnkv::driver::tikv_store::TikvStore;

/// Each snapshot read's deadline.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// The BRIE node over this server's TiKV store.
pub(crate) struct TikvBrieNode {
    storage: Arc<BrStorage>,
    domain: Arc<Domain>,
}

impl TikvBrieNode {
    pub(crate) fn new(
        store: TikvStore,
        opener: RealOptimisticTransactionOpener,
        client: Arc<dyn tidb_txnkv::Client>,
        domain: Arc<Domain>,
    ) -> Self {
        Self {
            storage: Arc::new(BrStorage {
                store,
                opener: Arc::new(opener),
                client,
            }),
            domain,
        }
    }
}

impl tidb_executor::brie::BrieNode for TikvBrieNode {
    fn storage(&self) -> std::result::Result<Arc<dyn tidb_br::glue::Storage>, String> {
        Ok(Arc::clone(&self.storage) as Arc<dyn tidb_br::glue::Storage>)
    }

    fn domain(&self) -> std::result::Result<Arc<dyn tidb_br::glue::Domain>, String> {
        Ok(Arc::new(BrDomain(Arc::clone(&self.domain))))
    }

    fn create_session(&self) -> std::result::Result<Box<dyn tidb_br::glue::Session>, String> {
        Ok(Box::new(BrSession(Arc::clone(&self.domain))))
    }
}

/// Go `kv.Storage` / `tikv.Storage` of this node.
struct BrStorage {
    store: TikvStore,
    opener: Arc<RealOptimisticTransactionOpener>,
    client: Arc<dyn tidb_txnkv::Client>,
}

/// Go `GetSnapshot(kv.NewVersion(ts))`: a read-only transaction at `ts`.
struct BrSnapshot(Option<TikvTxn<tikv_client::PdRpcClient>>);

impl BrSnapshot {
    fn txn(&mut self) -> &mut TikvTxn<tikv_client::PdRpcClient> {
        self.0.as_mut().expect("the snapshot is open until dropped")
    }
}

impl Drop for BrSnapshot {
    fn drop(&mut self) {
        if let Some(txn) = self.0.take() {
            let _ = txn.finish_without_writes();
        }
    }
}

impl tidb_br::glue::KvSnapshot for BrSnapshot {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let call = tidb_txnkv::UnaryCallContext::with_timeout(READ_TIMEOUT);
        self.txn()
            .snapshot_get(key, &call)
            .map(|r| r.value)
            .map_err(|e| errorf(e.to_string()))
    }

    fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let call = tidb_txnkv::UnaryCallContext::with_timeout(READ_TIMEOUT);
        self.txn()
            .snapshot_scan(start, end, limit, &call)
            .map_err(|e| errorf(e.to_string()))
    }
}

/// A snapshot that could not be opened: every read answers the error.
struct FailedSnapshot(String);

impl tidb_br::glue::KvSnapshot for FailedSnapshot {
    fn get(&mut self, _: &[u8]) -> Result<Option<Vec<u8>>> {
        Err(errorf(self.0.clone()))
    }

    fn scan(&mut self, _: &[u8], _: &[u8], _: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(errorf(self.0.clone()))
    }
}

impl tidb_br::glue::Storage for BrStorage {
    fn tikv(&self) -> &TikvStore {
        &self.store
    }

    fn get_snapshot(&self, ts: u64) -> Box<dyn tidb_br::glue::KvSnapshot> {
        match self.opener.begin_read_only_at(ts) {
            Ok(txn) => Box::new(BrSnapshot(Some(txn))),
            Err(e) => Box::new(FailedSnapshot(e.to_string())),
        }
    }

    fn current_version(&self) -> Result<u64> {
        self.store.get_timestamp_async().wait().map_err(errorf)
    }

    fn get_client(&self) -> Arc<dyn tidb_txnkv::Client> {
        Arc::clone(&self.client)
    }

    fn keyspace_id(&self) -> u32 {
        match self.store.store().keyspace() {
            tikv_client::request::Keyspace::Enable { keyspace_id } => keyspace_id,
            _ => tidb_br::gc::NULLSPACE_ID,
        }
    }

    /// Go `codecV2.GetKeyspace`: `'x'` and the 3-byte keyspace ID.
    fn keyspace_prefix(&self) -> Vec<u8> {
        match self.store.store().keyspace() {
            tikv_client::request::Keyspace::Enable { keyspace_id: id } => {
                vec![b'x', (id >> 16) as u8, (id >> 8) as u8, id as u8]
            }
            _ => Vec::new(),
        }
    }

    /// Go `GetCodec().GetAPIVersion()`: V2 with a keyspace, else V1.
    fn api_version(&self) -> i32 {
        match self.store.store().keyspace() {
            tikv_client::request::Keyspace::Enable { .. } => {
                tidb_proto::kvrpcpb::ApiVersion::V2 as i32
            }
            _ => tidb_proto::kvrpcpb::ApiVersion::V1 as i32,
        }
    }
}

/// Go `*domain.Domain`, as BR reads it.
struct BrDomain(Arc<Domain>);

impl tidb_br::glue::Domain for BrDomain {
    fn persist_stats_by_snapshot(
        &self,
        db_name: &str,
        table: &tidb_model::TableInfo,
        snapshot: u64,
        persist: &mut dyn FnMut(Option<Vec<u8>>, i64) -> Result<()>,
    ) -> Result<()> {
        let mut failed = None;
        let res = self
            .0
            .persist_stats_by_snapshot(db_name, table, snapshot, &mut |json, id| {
                let bytes = match json
                    .map(|j| tidb_model::serde_helpers::to_go_json(&j))
                    .transpose()
                {
                    Ok(b) => b,
                    Err(e) => return Err(e.to_string()),
                };
                persist(bytes, id).map_err(|e| {
                    let text = e.to_string();
                    failed = Some(e);
                    text
                })
            });
        match (res, failed) {
            (Ok(()), _) => Ok(()),
            (Err(_), Some(e)) => Err(e),
            (Err(e), None) => Err(errorf(e)),
        }
    }

    fn get_label_rules(&self, rule_ids: &[String]) -> Result<Vec<tidb_br::glue::LabelRule>> {
        let rules = tidb_domain_infosync::label_manager::label_rule_manager()
            .get_label_rules(rule_ids)
            .map_err(errorf)?;
        Ok(rules
            .into_iter()
            .map(|(id, rule)| {
                let labels = rule
                    .labels
                    .snapshot()
                    .iter()
                    .map(|l| (l.key.clone(), l.value.clone()))
                    .collect();
                (id, labels)
            })
            .collect())
    }
}

/// Go `tidbGlueSession`: each statement on a session of the domain's
/// internal pool, under `kv.InternalTxnBR`.
struct BrSession(Arc<Domain>);

impl BrSession {
    fn exec(
        &self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>> {
        self.0
            .advanced_sys_session_pool()
            .with_session(|lease| {
                lease.with_session_context(|context| {
                    tidb_domain_core::sys_session::exec_sql(
                        context,
                        tidb_txnkv::INTERNAL_TXN_BR,
                        sql,
                        args,
                    )
                    .map_err(tidb_syssession::SysSessionError::new)
                })
            })
            .map_err(|e| errorf(e.to_string()))
    }
}

fn datum_text(d: &tidb_datatype::Datum) -> String {
    match d {
        tidb_datatype::Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        tidb_datatype::Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    }
}

/// DDL restore runs through the session's DDL executor, which the restore
/// task brings.
fn restore_ddl_unsupported() -> tidb_br::errors::Error {
    errorf("restoring schemas is not supported by this node yet")
}

impl tidb_br::glue::Session for BrSession {
    fn execute(&self, _ctx: &tidb_gocontext::Context, sql: &str) -> Result<()> {
        self.exec(sql, &[]).map(|_| ())
    }

    fn execute_internal(
        &self,
        _ctx: &tidb_gocontext::Context,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<()> {
        self.exec(sql, args).map(|_| ())
    }

    fn create_database_on_exist_error(
        &self,
        _: &tidb_gocontext::Context,
        _: &tidb_model::DBInfo,
    ) -> Result<()> {
        Err(restore_ddl_unsupported())
    }

    fn create_table(
        &self,
        _: &tidb_gocontext::Context,
        _: &str,
        _: &tidb_model::TableInfo,
        _: tidb_br::glue::CreateTableConfig,
    ) -> Result<()> {
        Err(restore_ddl_unsupported())
    }

    fn create_tables(
        &self,
        _: &tidb_gocontext::Context,
        _: &[(String, Vec<tidb_model::TableInfo>)],
        _: tidb_br::glue::CreateTableConfig,
    ) -> Option<Result<()>> {
        Some(Err(restore_ddl_unsupported()))
    }

    fn create_placement_policy(
        &self,
        _: &tidb_gocontext::Context,
        _: &tidb_model::PolicyInfo,
    ) -> Result<()> {
        Err(restore_ddl_unsupported())
    }

    /// Go `GlobalVarsAccessor.GetTiDBTableValue`: the `mysql.tidb` row,
    /// an error when there is none.
    fn get_global_variable(&self, name: &str) -> Result<String> {
        let rows = self.exec(
            "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME=%?",
            &[tidb_sqlescape::SqlArg::String(name.as_bytes())],
        )?;
        let value = rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next());
        value
            .map(|d| datum_text(&d))
            .ok_or_else(|| errorf("Result is empty"))
    }

    fn get_global_sys_var(&self, name: &str) -> Result<String> {
        self.0
            .global_vars
            .get(name)
            .map_err(|e| errorf(format!("{e:?}")))
    }

    /// Go `ddl.GetAllDDLJobs`: `mysql.tidb_ddl_job` by job ID.
    fn get_all_ddl_jobs(&self, _: &tidb_gocontext::Context) -> Result<Vec<tidb_model::Job>> {
        self.exec(
            "select job_meta from mysql.tidb_ddl_job where 1 order by job_id",
            &[],
        )?
        .iter()
        .map(|row| {
            let mut job = tidb_model::Job::default();
            job.decode(row[0].go_bytes())
                .map_err(|e| errorf(e.to_string()))?;
            Ok(job)
        })
        .collect()
    }

    fn alter_table_mode(
        &self,
        _: &tidb_gocontext::Context,
        _: i64,
        _: i64,
        _: tidb_model::TableMode,
    ) -> Result<()> {
        Err(restore_ddl_unsupported())
    }
}
