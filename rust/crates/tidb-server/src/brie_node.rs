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
//! sessions from the domain's pool, which also run a restore's DDL.

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
        Ok(Box::new(BrSession::new(&self.domain)?))
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

    /// Go `kv.RunInNewTxn` with `meta.NewMutator` under `InternalTxnBR`.
    fn run_in_new_meta_txn(
        &self,
        f: &mut dyn FnMut(&dyn tidb_br::glue::GlobalIdAllocator) -> Result<()>,
    ) -> Result<()> {
        let store = self
            .0
            .meta_store
            .get()
            .ok_or_else(|| errorf("store is not initialized"))?;
        let mut failed = None;
        store
            .run_in_new_txn(
                &tidb_gocontext::Context::background(),
                tidb_txnkv::INTERNAL_TXN_BR,
                true,
                &mut |txn| {
                    let m = MetaIds(tidb_meta::transaction::Mutator::new(txn));
                    f(&m).map_err(|e| {
                        let text = e.to_string();
                        failed = Some(e);
                        tidb_meta::MetaError::Storage(text)
                    })
                },
            )
            .map_err(|e| failed.take().unwrap_or_else(|| errorf(e)))
    }

    /// Go `Domain.InfoSchema()`: the latest loaded schema.
    fn info_schema(&self) -> Result<Arc<dyn tidb_br::glue::InfoSchema>> {
        Ok(Arc::new(BrInfoSchema(self.0.catalog.get_latest())))
    }

    /// Go `LoadStatsFromJSONNoUpdate`, through the node's `LOAD STATS`
    /// writer (which also refreshes this node's statistics cache).
    fn load_stats_from_json_no_update(
        &self,
        _ctx: &tidb_gocontext::Context,
        json_table: &tidb_stats_util::JsonTable,
    ) -> Result<()> {
        self.load_stats(json_table)
    }

    /// Go `LoadStatsFromJSONConcurrently`: the partition tasks as one dump
    /// of `table`, each named by its partition (the table's own ID being
    /// its global statistics).
    fn load_partition_stats_from_json(
        &self,
        _ctx: &tidb_gocontext::Context,
        table: &tidb_model::TableInfo,
        tasks: Vec<(i64, tidb_stats_util::JsonTable)>,
    ) -> Result<()> {
        let is = self.0.catalog.get_latest();
        let db_name = is
            .stored_schemas()
            .iter()
            .map(|db| db.read())
            .find(|db| db.id == table.db_id)
            .map(|db| db.name.original().to_owned())
            .ok_or_else(|| errorf(format!("the database of table {} is not found", table.name)))?;
        let definitions = table
            .get_partition_info()
            .map(|p| p.read().definitions.snapshot())
            .unwrap_or_default();
        if definitions.is_empty() {
            for (_, mut json) in tasks {
                json.database_name.clone_from(&db_name);
                json.table_name = table.name.original().to_owned();
                self.load_stats(&json)?;
            }
            return Ok(());
        }
        let mut partitions = std::collections::BTreeMap::new();
        for (id, json) in tasks {
            let name = if id == table.id {
                tidb_stats_util::TIDB_GLOBAL_STATS.to_owned()
            } else {
                definitions
                    .iter()
                    .find(|d| d.id == id)
                    .map(|d| d.name.lowercase().to_owned())
                    .ok_or_else(|| {
                        errorf(format!(
                            "partition {id} of table {} is not found",
                            table.name
                        ))
                    })?
            };
            partitions.insert(name, Some(json));
        }
        self.load_stats(&tidb_stats_util::JsonTable {
            database_name: db_name,
            table_name: table.name.original().to_owned(),
            partitions: Some(partitions),
            ..Default::default()
        })
    }

    /// Go `StatsHandle().SaveMetaToStorage(false, updates...)`.
    fn save_meta_to_storage(&self, updates: &[tidb_br::glue::MetaUpdate]) -> Result<()> {
        let updates: Vec<_> = updates
            .iter()
            .map(|u| tidb_stats_handle_storage::save::MetaUpdate {
                physical_id: u.physical_id,
                count: u.count,
                modify_count: u.modify_count,
            })
            .collect();
        let pool = self.0.advanced_sys_session_pool();
        tidb_stats_handle_storage::stats_read_writer::save_meta_to_storage(
            &tidb_domain_core::sys_session::StatsPool {
                session_pool: Some(&pool),
            },
            false,
            &updates,
        )
        .map(|_| ())
        .map_err(sql_error)
    }

    /// Go `NotifyUpdateAllUsersPrivilege`.
    fn notify_update_all_users_privilege(&self) -> Result<()> {
        self.0
            .accounts
            .notify_all_privileges_changed()
            .map_err(errorf)
    }

    /// Go `GetEtcdClient()`; BR's keys are not keyspace-prefixed.
    fn etcd_client(&self) -> Option<Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>> {
        self.0
            .unprefixed_etcd_client()
            .or_else(|| self.0.etcd_client())
            .map(|client| {
                Arc::new(tidb_ddl::etcd_ops::EtcdClientOps::new(client))
                    as Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>
            })
    }

    /// Go `infosync.UpdateLabelRules`.
    fn update_label_rules(&self, patch: &tidb_ddl_label::LabelRulePatch) -> Result<()> {
        tidb_domain_infosync::label_manager::label_rule_manager()
            .update_label_rules(patch)
            .map_err(errorf)
    }
}

impl BrDomain {
    fn load_stats(&self, json: &tidb_stats_util::JsonTable) -> Result<()> {
        let historical = self
            .0
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .is_ok_and(|v| v.eq_ignore_ascii_case("ON") || v == "1");
        let pool = self.0.advanced_sys_session_pool();
        tidb_session::domain::cluster_session_node::node_services(&self.0)
            .analyze()
            .load_stats(
                json,
                historical,
                &tidb_domain_core::sys_session::StatsPool {
                    session_pool: Some(&pool),
                },
            )
            .map(|_| ())
            .map_err(sql_error)
    }
}

/// A server error as BR sees it: its code kept (Go's `terror` identity, so
/// the restore can tell `ErrInvalidDDLJob`).
fn sql_error(e: tidb_store_driver_error::sql_query_error::SqlQueryError) -> tidb_br::errors::Error {
    Arc::new(tidb_terror::TerrorError::compatible(
        tidb_terror::TerrorCode::new(e.code as isize),
        e.message,
    ))
}

/// Go `meta.Mutator`'s global ID counter.
struct MetaIds<'a>(
    tidb_meta::transaction::Mutator<&'a mut dyn tidb_meta::transaction::MetaTransaction>,
);

impl tidb_br::glue::GlobalIdAllocator for MetaIds<'_> {
    fn get_global_id(&self) -> Result<i64> {
        self.0.global_id().map_err(|e| errorf(e.to_string()))
    }

    fn advance_global_ids(&self, n: i32) -> Result<i64> {
        self.0
            .advance_global_ids(n)
            .map_err(|e| errorf(e.to_string()))
    }
}

/// Go `infoschema.InfoSchema` over this node's latest schema.
struct BrInfoSchema(Arc<tidb_infoschema::SessionExtendedInfoSchema>);

impl tidb_br::glue::InfoSchema for BrInfoSchema {
    fn schema_by_name(&self, db: &str) -> Option<tidb_model::DBInfo> {
        self.0.schema_by_name(db).map(|d| d.read().clone())
    }

    fn table_by_name(&self, db: &str, table: &str) -> Option<tidb_model::TableInfo> {
        self.0
            .table_info_by_name(db, table)
            .map(|(_, t)| t.read().clone())
    }

    fn schema_table_infos(&self, db: &str) -> Vec<tidb_model::TableInfo> {
        self.0
            .schema_table_infos(db)
            .unwrap_or_default()
            .iter()
            .map(|t| t.read().clone())
            .collect()
    }

    fn all_schemas(&self) -> Vec<tidb_model::DBInfo> {
        self.0
            .stored_schemas()
            .iter()
            .map(|d| d.read().clone())
            .collect()
    }
}

/// Go `tidbGlueSession`: one internal session of the domain's pool (Go
/// `CreateSession`), every statement on it under `kv.InternalTxnBR`, so a
/// transaction and `USE` carry across statements. It is closed, not
/// returned, when dropped (Go `Close`).
struct BrSession {
    domain: Arc<Domain>,
    pool: Arc<tidb_domain_core::sys_session::DomainSysSessionPool>,
    session: tidb_syssession::Session<dyn tidb_domain_core::sys_session::DomainSysSession>,
}

impl BrSession {
    fn new(domain: &Arc<Domain>) -> std::result::Result<Self, String> {
        let pool = domain.advanced_sys_session_pool();
        let session = pool.get().map_err(|e| e.to_string())?;
        Ok(Self {
            domain: Arc::clone(domain),
            pool,
            session,
        })
    }

    fn exec(
        &self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>> {
        self.session
            .with_session_context(|context| {
                tidb_domain_core::sys_session::exec_sql(
                    context,
                    tidb_txnkv::INTERNAL_TXN_BR,
                    sql,
                    args,
                )
                .map_err(tidb_syssession::SysSessionError::new)
            })
            .map_err(|e: tidb_syssession::SysSessionError| errorf(e.to_string()))
    }

    /// The session's variables as a DDL statement captures them.
    fn ddl_vars(&self) -> Result<tidb_sessionctx::StmtContext> {
        self.session
            .with_session_context(|context| {
                context
                    .ddl_statement_context()
                    .map_err(tidb_syssession::SysSessionError::new)
            })
            .map_err(|e: tidb_syssession::SysSessionError| errorf(e.to_string()))
    }
}

impl Drop for BrSession {
    fn drop(&mut self) {
        self.session.avoid_reuse();
        self.pool.put(&self.session);
    }
}

fn datum_text(d: &tidb_datatype::Datum) -> String {
    match d {
        tidb_datatype::Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        tidb_datatype::Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    }
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
        schema: &tidb_model::DBInfo,
    ) -> Result<()> {
        tidb_executor::brie_utils::brie_create_database(
            self.domain.ddl.as_ref(),
            self.ddl_vars()?,
            schema,
            "",
        )
        .map_err(sql_error)
    }

    fn create_table(
        &self,
        _: &tidb_gocontext::Context,
        db_name: &str,
        table: &tidb_model::TableInfo,
        cfg: tidb_br::glue::CreateTableConfig,
    ) -> Result<()> {
        // `BRIECreateTable` always ignores an existing table.
        tidb_executor::brie_utils::brie_create_table(
            self.domain.ddl.as_ref(),
            self.ddl_vars()?,
            db_name,
            table.clone(),
            "",
            cfg.id_allocated,
        )
        .map_err(sql_error)
    }

    fn create_tables(
        &self,
        _: &tidb_gocontext::Context,
        tables: &[(String, Vec<tidb_model::TableInfo>)],
        cfg: tidb_br::glue::CreateTableConfig,
    ) -> Option<Result<()>> {
        Some(self.ddl_vars().and_then(|vars| {
            tidb_executor::brie_utils::brie_create_tables(
                self.domain.ddl.as_ref(),
                vars,
                tables,
                "",
                cfg.id_allocated,
            )
            .map_err(sql_error)
        }))
    }

    fn create_placement_policy(
        &self,
        _: &tidb_gocontext::Context,
        policy: &tidb_model::PolicyInfo,
    ) -> Result<()> {
        tidb_executor::brie_utils::brie_create_placement_policy(
            self.domain.ddl.as_ref(),
            self.ddl_vars()?,
            policy,
        )
        .map_err(sql_error)
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
        self.domain
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
        schema_id: i64,
        table_id: i64,
        mode: tidb_model::TableMode,
    ) -> Result<()> {
        tidb_executor::brie_utils::brie_alter_table_mode(
            self.domain.ddl.as_ref(),
            self.ddl_vars()?,
            schema_id,
            table_id,
            mode,
        )
        .map_err(sql_error)
    }

    /// Go `ExecRestrictedSQL` with `ExecOptionUseCurSession`: on this
    /// session, as the registry's transactions need.
    fn exec_restricted_sql(
        &self,
        _: &tidb_gocontext::Context,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>> {
        self.exec(sql, args)
    }
}
