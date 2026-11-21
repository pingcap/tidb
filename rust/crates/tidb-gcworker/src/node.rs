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

//! The production collaborators of Go's GC worker on a node against TiKV and
//! PD -- its sessions (Go `session.CreateSession`), placement rules and store
//! client -- and its start at boot (Go `store/driver` -> `gcworker.NewGCWorker`
//! + `Start`), which the server's main runs.
//!
//! The store's keyspace metadata selects Go's full or unified-GC procedure.

use tidb_session::domain::cluster_session_node::DomainSessions as _;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::seams::{GcError, GcResult, GcRuleClient, GcSession, GcSessionFactory};
use tidb_ddl::ddl_delete_range::DelRangeTask;
use crate::tikv_store::TikvStoreClient;
use crate::{GcWorker, GcWorkerDeps, GcWorkerHandle};
use tidb_sqlescape::SqlArg;

use tidb_session::domain::cluster_session_node::SystemSqlExecutor;
use tidb_domain_core::domain::Domain;
use tidb_session::domain::session_api::SessionContext;

/// How long one internal request to PD or TiKV may take before the worker
/// gives up on it.
const GC_RPC_TIMEOUT: Duration = Duration::from_secs(60);

/// A started GC worker. Dropping it stops the worker (Go `GCWorker.Close`).
pub struct NodeGcWorker {
    _handle: GcWorkerHandle,
}

/// Go `gcworker.NewGCWorker(store, pdClient)` and `Start` for this node, over
/// the node's one store.
pub fn start_gc_worker(
    factory: &Arc<Domain>,
    store: &tidb_txnkv::driver::tikv_store::TikvStore,
    opener: Arc<tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener>,
) -> Result<NodeGcWorker, String> {
    let worker = GcWorker::new(gc_worker_deps(factory, store, opener)).map_err(|error| error.to_string())?;
    Ok(NodeGcWorker {
        _handle: worker.start(),
    })
}

/// The store, PD and sessions this node's GC worker (and Go's test API's
/// `RunResolveLocks`) runs over.
pub fn gc_worker_deps<S: tidb_store_driver_txn::transaction::StoreClient + 'static>(
    factory: &Arc<Domain>,
    store: &tidb_txnkv::driver::tikv_store::TikvStore<S>,
    opener: Arc<tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>>,
) -> GcWorkerDeps {
    let client = Arc::new(TikvStoreClient::new(
        Arc::clone(store.runtime()),
        store.store().clone(),
        store.store().keyspace_meta().cloned(),
    ));
    let pd_http_client = store.pd_http_client();
    GcWorkerDeps {
        store: client.clone(),
        pd: client.clone(),
        kv: client,
        sessions: Arc::new(NodeGcSessions::new(
            factory,
            Arc::new(move |job_id| {
                tidb_ddl::real_tikv_ddl::load_history_persisted_ddl_job(
                    Arc::clone(&opener),
                    job_id,
                    GC_RPC_TIMEOUT,
                )
                .map_err(|error| error.to_string())
            }),
        )),
        rules: Arc::new(NodeGcRules { pd_http_client }),
        // Go `extworkload.GetManagerFromStore(w.store)`.
        gcv2: tidb_extworkload::get_manager_from_store()
            .map(|manager| Arc::new(ExtWorkloadGcV2(manager)) as Arc<dyn crate::seams::GcV2Manager>),
    }
}

/// Go `extworkload.Manager` as the GC worker's `notifyGCV2AfterGC` uses it.
struct ExtWorkloadGcV2(Arc<dyn tidb_extworkload::Manager>);

impl crate::seams::GcV2Manager for ExtWorkloadGcV2 {
    fn role(&self) -> tidb_config::external_workload::ExternalWorkloadRole {
        self.0.role()
    }

    fn meta(&self) -> Option<tikv_client::proto::keyspacepb::KeyspaceMeta> {
        Some(self.0.meta().clone())
    }

    fn register_gcv2(&self, safe_point: u64, gc_life_time: std::time::Duration) -> crate::seams::GcResult<()> {
        self.0.register_gcv2(safe_point, gc_life_time).map_err(gc_error)
    }

    fn recycle_gcv2(&self, safe_point: u64) -> crate::seams::GcResult<()> {
        self.0.recycle_gcv2(safe_point).map_err(gc_error)
    }
}

fn gc_error(error: impl std::fmt::Display) -> GcError {
    GcError::new(error.to_string())
}

/// Internal connection ids of the GC worker's sessions, apart from the
/// workload repository's (`1 << 62`).
const GC_CONNECTION_ID_BASE: u64 = (1_u64 << 62) | (1_u64 << 60);

/// Go `ddl.GetHistoryJobByID`: the finished job, read from the meta keys.
type HistoryJobLoader = Arc<dyn Fn(i64) -> Result<Option<tidb_model::Job>, String> + Send + Sync>;

/// Go `createSession(store)`: internal sessions without privilege checks.
pub struct NodeGcSessions {
    factory: Weak<Domain>,
    next_connection_id: AtomicU64,
    history_job: HistoryJobLoader,
}

impl NodeGcSessions {
    pub fn new(factory: &Arc<Domain>, history_job: HistoryJobLoader) -> Self {
        Self {
            factory: Arc::downgrade(factory),
            next_connection_id: AtomicU64::new(GC_CONNECTION_ID_BASE),
            history_job,
        }
    }
}

impl GcSessionFactory for NodeGcSessions {
    fn create_session(&self) -> Box<dyn GcSession + '_> {
        let session = self
            .factory
            .upgrade()
            .ok_or_else(|| GcError::new("cluster session factory is stopped"))
            .and_then(|factory| {
                factory
                    .open_session(SessionContext {
                        connection_id: self.next_connection_id.fetch_add(1, Ordering::Relaxed),
                        peer_addr: tidb_session::domain::peer_address::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
                        identity: tidb_session::domain::configured_user_store::AuthenticatedIdentity::internal(),
                        client_capability: 0,
                        tls_state: None,
                        compression_algorithm: tidb_mysql::consts::CompressionNone,
                        compression_level: 0,
                        cancellation: tidb_session::domain::session_api::ConnectionCancellation::default(),
                        close: tidb_session::domain::session_api::ConnectionClose::default(),
                    })
                    .map(SystemSqlExecutor::new)
                    .map_err(|error| GcError::new(error.message))
            });
        Box::new(NodeGcSession {
            session,
            history_job: &self.history_job,
        })
    }
}

struct NodeGcSession<'a> {
    session: GcResult<SystemSqlExecutor>,
    history_job: &'a HistoryJobLoader,
}

fn mysql_error(error: tidb_terror::MysqlError) -> GcError {
    GcError::new(error.message)
}

/// Go `kv.WithInternalSourceType(ctx, kv.InternalTxnGC)`, the context the
/// worker's statements run under.
fn gc_context() -> impl tidb_sqlexec::ExecutionContext {
    tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_GC,
    )
}

fn escaped(sql: &str, arguments: &[SqlArg<'_>]) -> GcResult<String> {
    tidb_sqlexec::sql_with_params(sql, arguments)
        .map(std::borrow::Cow::into_owned)
        .map_err(gc_error)
}

fn datum_string(value: &tidb_datatype::Datum) -> GcResult<String> {
    match value {
        tidb_datatype::Datum::Bytes(bytes) | tidb_datatype::Datum::Raw(bytes) => {
            String::from_utf8(bytes.clone()).map_err(gc_error)
        }
        tidb_datatype::Datum::String(value) => value.as_utf8().map(str::to_owned).map_err(gc_error),
        other => Err(GcError::new(format!("unexpected string value {other:?}"))),
    }
}

impl NodeGcSession<'_> {
    /// Go `se.GetSQLExecutor()`.
    fn sql_executor(&self) -> GcResult<&SystemSqlExecutor> {
        self.session.as_ref().map_err(Clone::clone)
    }

    /// Go `ExecuteInternal` of a read: every row.
    fn query(&mut self, sql: &str) -> GcResult<Vec<Vec<tidb_datatype::Datum>>> {
        tidb_sqlexec::execute_sql(&gc_context(), self.sql_executor()?, sql, &[])
            .map_err(tidb_store_driver_error::sql_query_error::mysql_error_of)
            .map_err(mysql_error)
    }

    /// Go `ExecuteInternal` of a write.
    fn write(&mut self, sql: &str) -> GcResult<()> {
        self.query(sql).map(|_rows| ())
    }

    fn control(&mut self, statement: &str) -> GcResult<()> {
        self.query(statement).map(|_rows| ())
    }
}

/// Go `getRaftKvVersionSQL`.
const GET_RAFT_KV_VERSION_SQL: &str = "select `value` from information_schema.cluster_config where type = 'tikv' and `key` = 'storage.engine'";

impl GcSession for NodeGcSession<'_> {
    fn begin(&mut self) -> GcResult<()> {
        self.control("BEGIN")
    }

    fn commit(&mut self) -> GcResult<()> {
        self.control("COMMIT")
    }

    fn rollback(&mut self) {
        let _ = self.control("ROLLBACK");
    }

    fn load_value(&mut self, key: &str) -> GcResult<Option<String>> {
        let sql = escaped(
            "SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name=%? FOR UPDATE",
            &[key.into()],
        )?;
        let rows = self.query(&sql)?;
        match rows.first().and_then(|row| row.first()) {
            None => Ok(None),
            Some(value) => datum_string(value).map(Some),
        }
    }

    fn save_value(&mut self, key: &str, value: &str, comment: &str) -> GcResult<()> {
        let sql = escaped(
            "INSERT HIGH_PRIORITY INTO mysql.tidb VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE variable_value = %?, comment = %?",
            &[
                key.into(),
                value.into(),
                comment.into(),
                value.into(),
                comment.into(),
            ],
        )?;
        self.write(&sql)
    }

    /// Go `util.LoadDeleteRanges` / `util.LoadDoneDeleteRanges`.
    fn load_delete_ranges(&mut self, done: bool, safe_point: u64) -> GcResult<Vec<DelRangeTask>> {
        let load = if done {
            tidb_ddl::ddl_delete_range::load_done_delete_ranges
        } else {
            tidb_ddl::ddl_delete_range::load_delete_ranges
        };
        load(&gc_context(), self.sql_executor()?, safe_point).map_err(mysql_error)
    }

    /// Go `util.CompleteDeleteRange`.
    fn complete_delete_range(&mut self, range: &DelRangeTask, record_done: bool) -> GcResult<()> {
        tidb_ddl::ddl_delete_range::complete_delete_range(self.sql_executor()?, range, record_done)
            .map_err(mysql_error)
    }

    /// Go `util.DeleteDoneRecord`.
    fn delete_done_record(&mut self, range: &DelRangeTask) -> GcResult<()> {
        tidb_ddl::ddl_delete_range::delete_done_record(self.sql_executor()?, range).map_err(mysql_error)
    }

    /// Go `util.IsRaftKv2`: every TiKV runs the same engine.
    fn is_raft_kv2(&mut self) -> GcResult<bool> {
        let rows = self.query(GET_RAFT_KV_VERSION_SQL)?;
        match rows.first().and_then(|row| row.first()) {
            None => Ok(false),
            Some(value) => Ok(datum_string(value)? == "raft-kv2"),
        }
    }

    /// Go `ddl.GetHistoryJobByID`.
    fn history_job(&mut self, job_id: i64) -> GcResult<Option<tidb_model::Job>> {
        (self.history_job)(job_id).map_err(GcError::new)
    }
}

/// `infosync`'s placement and label rule calls, over the store's PD HTTP
/// client.
struct NodeGcRules {
    pd_http_client: Option<tikv_client::pd_http::Client>,
}

impl NodeGcRules {
    fn pd_http_client(&self) -> GcResult<&tikv_client::pd_http::Client> {
        self.pd_http_client
            .as_ref()
            .ok_or_else(|| GcError::new("pd http client unavailable"))
    }
}

impl GcRuleClient for NodeGcRules {
    /// Go `infosync.DeleteTiFlashPlacementRules`.
    fn delete_tiflash_placement_rules(&self, physical_table_ids: &[i64]) -> GcResult<()> {
        tidb_domain_infosync::tiflash_manager::delete_tiflash_placement_rules(physical_table_ids)
            .map_err(GcError::new)
    }

    fn put_rule_bundles(&self, bundles: Vec<tidb_placement::Bundle>) -> GcResult<()> {
        tidb_ddl::placement_delivery::put_rule_bundles(
            self.pd_http_client()?,
            &bundles,
            GC_RPC_TIMEOUT,
        )
        .map_err(gc_error)
    }

    fn get_label_rules(
        &self,
        rule_ids: &[String],
    ) -> GcResult<HashMap<String, tidb_ddl_label::Rule>> {
        tidb_domain_infosync::label_manager::label_rule_manager()
            .get_label_rules(rule_ids)
            .map(|rules| rules.into_iter().collect())
            .map_err(GcError::new)
    }

    fn delete_label_rules(&self, rule_ids: Vec<String>) -> GcResult<()> {
        tidb_domain_infosync::label_manager::label_rule_manager()
            .update_label_rules(&tidb_ddl_label::new_rule_patch(
                Vec::new(),
                rule_ids,
            ))
            .map_err(GcError::new)
    }
}

#[cfg(test)]
mod playground_tests {
    /// Counts the keys of the table `GC_ACC_TABLE_ID` (its record and index
    /// keys, `t{id}` .. `t{id+1}`) on the cluster at `GC_ACC_PD`, read at a
    /// fresh timestamp. Run by hand against a playground: after `DROP TABLE` the rows are
    /// still there (Go deletes nothing until GC), and after the GC worker
    /// destroyed the table's delete range there is none, at any timestamp.
    #[test]
    #[ignore = "needs a TiKV playground: GC_ACC_PD and GC_ACC_TABLE_ID"]
    fn playground_table_keys() {
        use tikv_client::TimestampExt;

        let pd = std::env::var("GC_ACC_PD").expect("GC_ACC_PD");
        let table_id: i64 = std::env::var("GC_ACC_TABLE_ID")
            .expect("GC_ACC_TABLE_ID")
            .parse()
            .expect("a table id");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let count = runtime.block_on(async {
            let store = tikv_client::tikv::KvStore::new_with_config(
                vec![pd],
                tikv_client::Config::default(),
            )
            .await
            .expect("store");
            let timestamp = store.current_timestamp().await.expect("timestamp");
            let mut snapshot = store.snapshot(
                tikv_client::proto::pdpb::Timestamp::from_version(timestamp.version()),
                tikv_client::TransactionOptions::new_optimistic().read_only(),
            );
            let start = tidb_tablecodec::table_key::encode_table_prefix(table_id);
            let end = tidb_tablecodec::table_key::encode_table_prefix(table_id + 1);
            snapshot
                .scan(start..end, 10_000)
                .await
                .expect("scan")
                .count()
        });
        println!("GC_ACC table {table_id} keys: {count}");
    }

    /// Resolves locks over the whole key space through the client, as the GC
    /// worker's runner does, after `GC_ACC_IDLE_SECS` of idleness.
    #[test]
    #[ignore = "needs a TiKV playground: GC_ACC_PD"]
    fn playground_resolve_locks() {
        use tikv_client::TimestampExt;

        let pd = std::env::var("GC_ACC_PD").expect("GC_ACC_PD");
        let idle: u64 = std::env::var("GC_ACC_IDLE_SECS")
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let store = tikv_client::tikv::KvStore::new_with_config(
                vec![pd],
                tikv_client::Config::default(),
            )
            .await
            .expect("store");
            tokio::time::sleep(std::time::Duration::from_secs(idle)).await;
            let timestamp = store.current_timestamp().await.expect("timestamp");
            let (stat, result) = store
                .resolve_locks_for_range(
                    tikv_client::async_util::Cancellation::default(),
                    &tikv_client::proto::pdpb::Timestamp::from_version(timestamp.version() - 1),
                    (Vec::new(), Vec::new()),
                    1024,
                )
                .await;
            println!("GC_ACC resolve after {idle}s idle: {stat:?} {result:?}");
        });
    }
}

