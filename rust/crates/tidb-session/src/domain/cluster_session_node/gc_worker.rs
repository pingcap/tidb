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
    DelRangeTask, GcError, GcResult, GcRuleClient, GcSession, GcSessionFactory,
};
use tidb_gcworker::tikv_store::TikvStoreClient;
use tidb_gcworker::{GcWorker, GcWorkerDeps, GcWorkerHandle};
use tidb_util::sqlescape::{escape_sql, SqlArg};

use super::{ClusterServerSession, Domain};
use crate::Session;
use crate::domain::session_api::SessionContext;
use crate::domain::session_api::{QuerySessionFactory, SqlQueryError};

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
    opener: Arc<tidb_cluster::real_tikv_read::RealOptimisticTransactionOpener>,
) -> Result<NodeGcWorker, String> {
    // The node serves the null keyspace.
    let client = Arc::new(TikvStoreClient::new(
        Arc::clone(store.runtime()),
        store.store().clone(),
        None,
    ));
    let pd_http_client = store.pd_http_client();
    let worker = GcWorker::new(GcWorkerDeps {
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
        gcv2: None,
    })
    .map_err(|error| error.to_string())?;
    Ok(NodeGcWorker {
        _handle: worker.start(),
    })
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
                        peer_addr: crate::domain::client_socket::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
                        identity: crate::domain::configured_user_store::AuthenticatedIdentity::internal(),
                        client_found_rows: false,
                        tls_state: None,
                        compression_algorithm: tidb_mysql::consts::CompressionNone,
                        compression_level: 0,
                        cancellation: crate::domain::session_api::ConnectionCancellation::default(),
                        close: crate::domain::session_api::ConnectionClose::default(),
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
    session: GcResult<Session>,
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
    fn session(&mut self) -> GcResult<ClusterServerSession<'_>> {
        self.session
            .as_mut()
            .map(Session::node_conn)
            .map_err(|error| error.clone())
    }

    /// Go `ExecuteInternal` of a read: every row.
    fn query(&mut self, sql: &str) -> GcResult<Vec<Vec<tidb_datatype::Datum>>> {
        self.session()?.execute_internal(sql).map_err(sql_error)
    }

    /// Go `ExecuteInternal` of a write.
    fn write(&mut self, sql: &str) -> GcResult<()> {
        self.session()?.execute_internal(sql).map_err(sql_error)?;
        Ok(())
    }

    fn control(&mut self, statement: &str) -> GcResult<()> {
        self.session()?
            .execute_internal(statement)
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
    fn delete_tiflash_placement_rules(&self, physical_table_ids: &[i64]) -> GcResult<()> {
        tidb_ddl::placement_delivery::delete_tiflash_placement_rules(
            self.pd_http_client()?,
            physical_table_ids,
            GC_RPC_TIMEOUT,
        )
        .map_err(gc_error)
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
        tidb_domain::label_manager::label_rule_manager()
            .get_label_rules(rule_ids)
            .map(|rules| rules.into_iter().collect())
            .map_err(GcError::new)
    }

    fn delete_label_rules(&self, rule_ids: Vec<String>) -> GcResult<()> {
        tidb_domain::label_manager::label_rule_manager()
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
    /// fresh timestamp. Run by hand against a playground (see
    /// `rust/docs/gcworker-execplan.md`): after `DROP TABLE` the rows are
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
            let start = tidb_codec::table_key::encode_table_prefix(table_id);
            let end = tidb_codec::table_key::encode_table_prefix(table_id + 1);
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

