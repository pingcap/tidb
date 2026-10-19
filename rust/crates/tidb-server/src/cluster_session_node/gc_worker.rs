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

//! The production collaborators of Go's GC worker (`pkg/store/gcworker`,
//! `tidb_gcworker`) on a node against TiKV and PD, and its start at boot
//! (Go `Domain.Start` -> `gcworker.NewGCWorker` + `Start`).
//!
//! The node serves the null keyspace, so the worker runs Go's full procedure
//! (leader lease, safe points, lock resolution, delete ranges) rather than
//! the unified-GC keyspace path.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tidb_gcworker::seams::{
    AdvanceGcSafePointResult, AdvanceTxnSafePointResult, DelRangeTask, GcError, GcKvClient,
    GcPdClient, GcResult, GcRuleClient, GcSession, GcSessionFactory, GcStore, ResolveLocksRunner,
};
use tidb_gcworker::{GcWorker, GcWorkerDeps, GcWorkerHandle};
use tidb_util::sqlescape::{escape_sql, SqlArg};
use tikv_client::async_util::Cancellation;
use tikv_client::proto::keyspacepb::KeyspaceMeta;
use tikv_client::proto::{kvrpcpb, metapb, pdpb};
use tikv_client::transaction::range_task::{RangeTaskHandler, Runner, TaskStat};
use tikv_client::{PdClient, TimestampExt};

use super::{ClusterServerSession, ClusterSessionFactory, SessionContext};
use crate::resultset_source::ResultSetSource;
use crate::sql_node::{QuerySession, QuerySessionFactory, SqlQueryError};
use tidb_txnkv::pd_capability::PdCapability;

/// How long one internal request to PD or TiKV may take before the worker
/// gives up on it.
const GC_RPC_TIMEOUT: Duration = Duration::from_secs(60);

/// A started GC worker and the runtime its TiKV and PD calls run on. Dropping
/// it stops the worker (Go `GCWorker.Close`).
pub(crate) struct NodeGcWorker {
    // Field order is drop order: the worker thread joins before the runtime
    // its calls block on goes away.
    _handle: GcWorkerHandle,
    _runtime: Arc<tokio::runtime::Runtime>,
}

/// Go `gcworker.NewGCWorker(store, pdClient)` and `Start` for this node.
pub(crate) fn start_gc_worker(
    factory: &Arc<ClusterSessionFactory>,
    pd_endpoints: &[String],
    security: &tidb_pd_client::ClusterSecurity,
    pd: tidb_pd_client::PdClient,
    opener: Arc<tidb_exec::real_tikv_read::RealOptimisticTransactionOpener>,
) -> Result<NodeGcWorker, String> {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("gc-worker-rpc")
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?,
    );
    let mut config = tikv_client::Config::default();
    if security.is_tls_enabled() {
        config = config.with_security(
            security.ca_path(),
            security.cert_path(),
            security.key_path(),
        );
    }
    let store = runtime
        .block_on(tikv_client::tikv::KvStore::new_with_config(
            pd_endpoints.to_vec(),
            config,
        ))
        .map_err(|error| error.to_string())?;
    let client = Arc::new(NodeGcClient {
        runtime: Arc::clone(&runtime),
        store,
        pd,
    });
    let http_endpoint = opener.pd().http_endpoint();
    let worker = GcWorker::new(GcWorkerDeps {
        store: client.clone(),
        pd: client.clone(),
        kv: client,
        sessions: Arc::new(NodeGcSessions::new(
            factory,
            Arc::new(move |job_id| {
                tidb_exec::real_tikv_ddl::load_history_persisted_ddl_job(
                    Arc::clone(&opener),
                    job_id,
                    GC_RPC_TIMEOUT,
                )
                .map_err(|error| error.to_string())
            }),
        )),
        rules: Arc::new(NodeGcRules { http_endpoint }),
        gcv2: None,
    })
    .map_err(|error| error.to_string())?;
    Ok(NodeGcWorker {
        _handle: worker.start(),
        _runtime: runtime,
    })
}

/// The store, PD and TiKV halves: client-go's `tikv.Storage` and pd-client,
/// over the client's `KvStore`, and the node's PD client for `GetAllStores`.
struct NodeGcClient {
    runtime: Arc<tokio::runtime::Runtime>,
    store: tikv_client::tikv::KvStore,
    pd: tidb_pd_client::PdClient,
}

fn gc_error(error: impl std::fmt::Display) -> GcError {
    GcError::new(error.to_string())
}

impl GcStore for NodeGcClient {
    fn current_version(&self) -> GcResult<u64> {
        self.runtime
            .block_on(self.store.current_timestamp())
            .map(|timestamp| timestamp.version())
            .map_err(gc_error)
    }

    fn keyspace_meta(&self) -> Option<KeyspaceMeta> {
        // The node serves the null keyspace.
        None
    }
}

impl GcPdClient for NodeGcClient {
    fn get_all_stores(&self) -> GcResult<Vec<metapb::Store>> {
        self.pd.all_store_metadata().map_err(gc_error)
    }

    fn update_gc_safe_point(&self, safe_point: u64) -> GcResult<u64> {
        self.runtime
            .block_on(self.store.pd_client().update_safepoint_value(safe_point))
            .map_err(gc_error)
    }

    fn update_service_gc_safe_point(
        &self,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
    ) -> GcResult<u64> {
        self.runtime
            .block_on(
                self.store
                    .pd_client()
                    .update_service_gc_safe_point(service_id, ttl, safe_point),
            )
            .map_err(gc_error)
    }

    fn advance_txn_safe_point(&self, target: u64) -> GcResult<AdvanceTxnSafePointResult> {
        let response: pdpb::AdvanceTxnSafePointResponse = self
            .runtime
            .block_on(
                self.store
                    .pd_client()
                    .advance_transaction_safe_point(target),
            )
            .map_err(gc_error)?;
        Ok(AdvanceTxnSafePointResult {
            old_txn_safe_point: response.old_txn_safe_point,
            target,
            new_txn_safe_point: response.new_txn_safe_point,
            blocker_description: response.blocker_description,
        })
    }

    fn advance_gc_safe_point(&self, target: u64) -> GcResult<AdvanceGcSafePointResult> {
        let response: pdpb::AdvanceGcSafePointResponse = self
            .runtime
            .block_on(self.store.pd_client().advance_gc_safe_point(target))
            .map_err(gc_error)?;
        Ok(AdvanceGcSafePointResult {
            old_gc_safe_point: response.old_gc_safe_point,
            target,
            new_gc_safe_point: response.new_gc_safe_point,
        })
    }

    fn get_all_keyspaces(&self, start_id: u32, limit: u32) -> GcResult<Vec<KeyspaceMeta>> {
        self.runtime
            .block_on(self.store.pd_client().get_all_keyspaces(start_id, limit))
            .map_err(gc_error)
    }
}

/// client-go `tikv.ResolveLocksForRange` as a range-task handler.
#[derive(Clone)]
struct ResolveLocksForRange {
    client: tikv_client::TransactionClient,
    max_version: pdpb::Timestamp,
    scan_limit: u32,
}

#[async_trait::async_trait]
impl RangeTaskHandler for ResolveLocksForRange {
    async fn handle(
        &self,
        cancellation: Cancellation,
        range: (Vec<u8>, Vec<u8>),
    ) -> (TaskStat, tikv_client::Result<()>) {
        self.client
            .resolve_locks_for_range(cancellation, &self.max_version, range, self.scan_limit)
            .await
    }
}

struct NodeResolveLocksRunner<'a> {
    runtime: &'a tokio::runtime::Runtime,
    runner: Runner<tikv_client::PdRpcClient, ResolveLocksForRange>,
}

impl ResolveLocksRunner for NodeResolveLocksRunner<'_> {
    fn run_on_range(&self, start: &[u8], end: &[u8]) -> GcResult<()> {
        self.runtime
            .block_on(self.runner.run_on_range(start.to_vec(), end.to_vec()))
            .map_err(gc_error)
    }

    fn completed_regions(&self) -> usize {
        self.runner.completed_regions()
    }
}

impl GcKvClient for NodeGcClient {
    fn unsafe_destroy_range(
        &self,
        address: &str,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> GcResult<Option<String>> {
        let request = kvrpcpb::UnsafeDestroyRangeRequest {
            context: Some(kvrpcpb::Context {
                disk_full_opt: kvrpcpb::DiskFullOpt::AllowedOnAlmostFull as i32,
                ..kvrpcpb::Context::default()
            }),
            start_key: start.to_vec(),
            end_key: end.to_vec(),
        };
        let response = self
            .runtime
            .block_on(self.store.send_request(address, &request, timeout))
            .map_err(gc_error)?;
        Ok(response
            .downcast::<kvrpcpb::UnsafeDestroyRangeResponse>()
            .ok()
            .map(|response| response.error))
    }

    fn delete_range_task(&self, start: &[u8], end: &[u8], concurrency: usize) -> GcResult<()> {
        self.runtime
            .block_on(
                self.store
                    .delete_range_with_concurrency((start.to_vec(), end.to_vec()), concurrency),
            )
            .map(|_| ())
            .map_err(gc_error)
    }

    fn resolve_locks_runner(
        &self,
        name: &str,
        concurrency: usize,
        max_version: u64,
        scan_limit: u32,
    ) -> Box<dyn ResolveLocksRunner + '_> {
        let handler = ResolveLocksForRange {
            client: (*self.store).clone(),
            max_version: pdpb::Timestamp::from_version(max_version),
            scan_limit,
        };
        Box::new(NodeResolveLocksRunner {
            runtime: &self.runtime,
            runner: Runner::new(name, self.store.pd_client(), concurrency.max(1), handler),
        })
    }

    fn gc_region(&self, _region_key: &[u8], _safe_point: u64) -> GcResult<Option<String>> {
        // Go `doGCForRegion` has no production caller; only its store-backed
        // test sends the region-level `CmdGC`.
        Err(GcError::new(
            "doGCForRegion is not sent by the production GC worker",
        ))
    }
}

/// Internal connection ids of the GC worker's sessions, apart from the
/// workload repository's (`1 << 62`).
const GC_CONNECTION_ID_BASE: u64 = (1_u64 << 62) | (1_u64 << 60);

/// Go `ddl.GetHistoryJobByID`: the finished job, read from the meta keys.
type HistoryJobLoader = Arc<dyn Fn(i64) -> Result<Option<tidb_model::Job>, String> + Send + Sync>;

/// Go `createSession(store)`: internal sessions without privilege checks.
pub(crate) struct NodeGcSessions {
    factory: Weak<ClusterSessionFactory>,
    next_connection_id: AtomicU64,
    history_job: HistoryJobLoader,
}

impl NodeGcSessions {
    pub(crate) fn new(factory: &Arc<ClusterSessionFactory>, history_job: HistoryJobLoader) -> Self {
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
                        peer_addr: "127.0.0.1:0".parse().expect("loopback socket address"),
                        identity: crate::configured_user_store::AuthenticatedIdentity::internal(),
                        client_found_rows: false,
                        secure_transport: false,
                        tls_status: None,
                        cancellation: crate::sql_node::ConnectionCancellation::default(),
                        close: crate::sql_node::ConnectionClose::default(),
                    })
                    .map_err(|error| GcError::new(error.message))
            });
        Box::new(NodeGcSession {
            session,
            history_job: &self.history_job,
        })
    }
}

struct NodeGcSession<'a> {
    session: GcResult<ClusterServerSession>,
    history_job: &'a HistoryJobLoader,
}

fn sql_error(error: SqlQueryError) -> GcError {
    GcError::new(error.message)
}

fn escaped(sql: &str, arguments: &[SqlArg<'_>]) -> GcResult<String> {
    let bytes = escape_sql(sql, arguments).map_err(gc_error)?;
    String::from_utf8(bytes).map_err(gc_error)
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

fn datum_i64(value: &tidb_datatype::Datum) -> GcResult<i64> {
    match value {
        tidb_datatype::Datum::Int(value) => Ok(*value),
        tidb_datatype::Datum::UInt(value) => i64::try_from(*value).map_err(gc_error),
        other => Err(GcError::new(format!("unexpected integer value {other:?}"))),
    }
}

impl NodeGcSession<'_> {
    fn session(&mut self) -> GcResult<&mut ClusterServerSession> {
        self.session.as_mut().map_err(|error| error.clone())
    }

    /// Go `ExecuteInternal` of a read: every row.
    fn query(&mut self, sql: &str) -> GcResult<Vec<Vec<tidb_datatype::Datum>>> {
        let session = self.session()?;
        let mut result = session.execute(sql).map_err(sql_error)?;
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(256).map_err(gc_error)?;
            if batch.is_empty() {
                break;
            }
            rows.extend(batch);
        }
        source.finish().map_err(gc_error)?;
        source.close().map_err(gc_error)?;
        Ok(rows)
    }

    /// Go `ExecuteInternal` of a write.
    fn write(&mut self, sql: &str) -> GcResult<()> {
        self.session()?.execute_write(sql).map_err(sql_error)?;
        Ok(())
    }

    fn control(&mut self, statement: &str) -> GcResult<()> {
        self.session()?
            .control_transaction(statement)
            .map_err(sql_error)?;
        Ok(())
    }
}

/// Go `pkg/ddl/util`'s `loadDeleteRangeSQL`.
const LOAD_DELETE_RANGE_SQL: &str =
    "SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM mysql.%n WHERE ts < %?";
/// Go `recordDoneDeletedRangeSQL`.
const RECORD_DONE_DELETED_RANGE_SQL: &str = "INSERT IGNORE INTO mysql.gc_delete_range_done SELECT * FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?";
/// Go `completeDeleteRangeSQL`.
const COMPLETE_DELETE_RANGE_SQL: &str =
    "DELETE FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?";
/// Go `deleteDoneRecordSQL`.
const DELETE_DONE_RECORD_SQL: &str =
    "DELETE FROM mysql.gc_delete_range_done WHERE job_id = %? AND element_id = %?";
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

    /// Go `loadDeleteRangesFromTable`.
    fn load_delete_ranges(&mut self, done: bool, safe_point: u64) -> GcResult<Vec<DelRangeTask>> {
        let table = if done {
            "gc_delete_range_done"
        } else {
            "gc_delete_range"
        };
        let sql = escaped(LOAD_DELETE_RANGE_SQL, &[table.into(), safe_point.into()])?;
        let decode_hex = |text: String| -> GcResult<Vec<u8>> {
            (0..text.len())
                .step_by(2)
                .map(|index| {
                    text.get(index..index + 2)
                        .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                        .ok_or_else(|| GcError::new(format!("encoding/hex: invalid key {text}")))
                })
                .collect()
        };
        self.query(&sql)?
            .into_iter()
            .map(|row| {
                Ok(DelRangeTask {
                    job_id: datum_i64(&row[0])?,
                    element_id: datum_i64(&row[1])?,
                    start_key: decode_hex(datum_string(&row[2])?)?,
                    end_key: decode_hex(datum_string(&row[3])?)?,
                })
            })
            .collect()
    }

    /// Go `util.CompleteDeleteRange`.
    fn complete_delete_range(&mut self, range: &DelRangeTask, record_done: bool) -> GcResult<()> {
        self.begin()?;
        let arguments = [range.job_id.into(), range.element_id.into()];
        if record_done {
            self.write(&escaped(RECORD_DONE_DELETED_RANGE_SQL, &arguments)?)?;
        }
        self.write(&escaped(COMPLETE_DELETE_RANGE_SQL, &arguments)?)?;
        self.commit()
    }

    /// Go `util.DeleteDoneRecord`.
    fn delete_done_record(&mut self, range: &DelRangeTask) -> GcResult<()> {
        self.write(&escaped(
            DELETE_DONE_RECORD_SQL,
            &[range.job_id.into(), range.element_id.into()],
        )?)
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

/// `infosync`'s placement and label rule calls.
struct NodeGcRules {
    http_endpoint: Option<String>,
}

impl NodeGcRules {
    fn endpoint(&self) -> GcResult<&str> {
        self.http_endpoint
            .as_deref()
            .ok_or_else(|| GcError::new("pd http client unavailable"))
    }
}

impl GcRuleClient for NodeGcRules {
    fn delete_tiflash_placement_rules(&self, physical_table_ids: &[i64]) -> GcResult<()> {
        tidb_exec::placement_delivery::delete_tiflash_placement_rules(
            self.endpoint()?,
            physical_table_ids,
            GC_RPC_TIMEOUT,
        )
        .map_err(gc_error)
    }

    fn put_rule_bundles(&self, bundles: Vec<tidb_placement::Bundle>) -> GcResult<()> {
        tidb_exec::placement_delivery::put_rule_bundles(self.endpoint()?, &bundles, GC_RPC_TIMEOUT)
            .map_err(gc_error)
    }

    fn get_label_rules(
        &self,
        rule_ids: &[String],
    ) -> GcResult<HashMap<String, tidb_executor::ddl_label::Rule>> {
        tidb_executor::label_manager::label_rule_manager()
            .get_label_rules(rule_ids)
            .map(|rules| rules.into_iter().collect())
            .map_err(GcError::new)
    }

    fn delete_label_rules(&self, rule_ids: Vec<String>) -> GcResult<()> {
        tidb_executor::label_manager::label_rule_manager()
            .update_label_rules(&tidb_executor::ddl_label::new_rule_patch(
                Vec::new(),
                rule_ids,
            ))
            .map_err(GcError::new)
    }
}
