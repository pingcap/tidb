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

//! Server adapter for one process-owned real-PD/TiKV read authority.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_distsql::{CancelHandle, DirectUnaryTransportEvidenceHandle, PublishedDispatchEvidence};
use tidb_exec::distsql_recordset::DistSqlRecordSet;
use tidb_exec::real_tikv_read::{
    PdTimestampSource, ProductionReadProcessAuthority, ProductionReadSessionFactory,
    ProductionReadTransport, ReadProcessShutdownError, ReadProcessShutdownStage,
    RealTiKvReadSession, RealTiKvReadSessionOpener,
};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

use crate::configured_user_store::{ConfiguredUserStore, ConfiguredUserStoreError};
use crate::node_config::{ConfiguredReadColumnKind, NodeConfig};
use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    ActiveQueryCancellation, ConcurrentSqlNode, QueryCancellationLease, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlNodeError, SqlQueryError,
};

const PRODUCTION_CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

impl ActiveQueryCancellation for CancelHandle {
    fn cancel(&self) {
        CancelHandle::cancel(self);
    }
}

/// Cloneable session opener shared by the fixed connection workers.
pub struct RealTiKvSessionFactory {
    opener: RealTiKvReadSessionOpener<ProductionReadSessionFactory, PdTimestampSource>,
    query_activity: Arc<QueryActivity>,
    read_authority_id: u64,
}

impl RealTiKvSessionFactory {
    /// Connects the unique process owner and derives its cloneable opener.
    pub fn connect(
        config: &NodeConfig,
    ) -> Result<(Self, ProductionReadProcessAuthority), SqlQueryError> {
        let table = configured_table(config);
        let authority = ProductionReadProcessAuthority::connect(
            config.pd_endpoints.clone(),
            PRODUCTION_CONTROL_PLANE_TIMEOUT,
            table,
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let factory = Self {
            opener: authority.opener(),
            query_activity: Arc::new(QueryActivity::default()),
            read_authority_id: authority.read_authority_id(),
        };
        Ok((factory, authority))
    }

    /// Returns the PD cluster identity validated during process bootstrap.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.opener.cluster_id()
    }

    /// Stable executor process-authority identity.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.opener.authority_id()
    }

    /// Stable maintained read-authority identity.
    #[must_use]
    pub const fn read_authority_id(&self) -> u64 {
        self.read_authority_id
    }
}

impl QuerySessionFactory for RealTiKvSessionFactory {
    type Session = RealTiKvServerSession;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        let inner = self
            .opener
            .open_session()
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(RealTiKvServerSession {
            inner,
            context,
            query_activity: Arc::clone(&self.query_activity),
            next_query_id: 1,
        })
    }
}

/// Worker-local server session around the executor session.
pub struct RealTiKvServerSession {
    inner: RealTiKvReadSession<ProductionReadTransport, PdTimestampSource>,
    context: SessionContext,
    query_activity: Arc<QueryActivity>,
    next_query_id: u64,
}

#[derive(Default)]
struct QueryActivity {
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl QueryActivity {
    fn begin(self: &Arc<Self>, connection_id: u64, query_id: u64) -> QueryActivityLease {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::AcqRel);
        eprintln!(
            "{{\"event\":\"query_activity\",\"phase\":\"begin\",\"connection_id\":{connection_id},\"query_id\":{query_id},\"active\":{active},\"max_active\":{}}}",
            self.max_active.load(Ordering::Acquire)
        );
        QueryActivityLease {
            activity: Arc::clone(self),
            connection_id,
            query_id,
        }
    }
}

struct QueryActivityLease {
    activity: Arc<QueryActivity>,
    connection_id: u64,
    query_id: u64,
}

impl Drop for QueryActivityLease {
    fn drop(&mut self) {
        let previous = self.activity.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "query activity count underflow");
        eprintln!(
            "{{\"event\":\"query_activity\",\"phase\":\"end\",\"connection_id\":{},\"query_id\":{},\"active\":{},\"max_active\":{}}}",
            self.connection_id,
            self.query_id,
            previous - 1,
            self.activity.max_active.load(Ordering::Acquire)
        );
    }
}

impl QuerySession for RealTiKvServerSession {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        let query_id = self.next_query_id;
        self.next_query_id = self
            .next_query_id
            .checked_add(1)
            .ok_or_else(|| SqlQueryError::unknown("query identity space exhausted"))?;
        let query_activity = self
            .query_activity
            .begin(self.context.connection_id, query_id);
        let cancellation = Arc::new(CancelHandle::default());
        let cancellation_lease = self.context.cancellation.install(cancellation.clone());
        let query = self
            .inner
            .execute_with_cancellation(sql, cancellation)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let snapshot_ts = query.snapshot_ts();
        let table_id = query.table_id();
        let cluster_id = self.inner.cluster_id();
        let identity = query.session_identity();
        let evidence = self.inner.transport_evidence_handle();
        let connection_id = self.context.connection_id;
        let authority_id = identity.authority_id();
        let session_id = identity.session_id();
        evidence
            .set_publication_observer(move |published| {
                emit_query_transport_publication(
                    connection_id,
                    query_id,
                    authority_id,
                    session_id,
                    published,
                );
            })
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        eprintln!(
            "{{\"event\":\"query_snapshot\",\"connection_id\":{},\"query_id\":{query_id},\"authority_id\":{},\"session_id\":{},\"cluster_id\":{cluster_id},\"snapshot_ts\":{snapshot_ts},\"table_id\":{table_id},\"user\":{:?},\"host\":{:?}}}",
            connection_id,
            authority_id,
            session_id,
            self.context.identity.username(),
            self.context.identity.host(),
        );
        Ok(QueryResult::new(Box::new(ObservedResultSet {
            inner: query.into_record_set(),
            evidence,
            connection_id,
            query_id,
            authority_id,
            session_id,
            emitted: false,
            _cancellation_lease: cancellation_lease,
            _query_activity: query_activity,
        })))
    }
}

fn emit_query_transport_publication(
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    published: &PublishedDispatchEvidence,
) {
    let publication = &published.publication;
    let forwarded_host = publication
        .forwarded_host()
        .map_or_else(|| "null".to_owned(), |host| format!("{host:?}"));
    eprintln!(
        "{{\"event\":\"query_transport_published\",\"connection_id\":{connection_id},\"query_id\":{query_id},\"authority_id\":{authority_id},\"session_id\":{session_id},\"region_id\":{},\"physical_address\":{:?},\"physical_channel_version\":{},\"stream_generation\":{},\"forwarded_host\":{forwarded_host}}}",
        published.region_id,
        publication.physical_address(),
        publication.physical_channel_version(),
        publication.batch_stream_generation(),
    );
}

struct ObservedResultSet {
    inner: DistSqlRecordSet,
    evidence: DirectUnaryTransportEvidenceHandle,
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    emitted: bool,
    _cancellation_lease: QueryCancellationLease,
    _query_activity: QueryActivityLease,
}

impl ObservedResultSet {
    fn emit_evidence(&mut self) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        let evidence = self.evidence.snapshot();
        let located_regions = evidence
            .located_region_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let dispatched_regions = evidence
            .dispatched_region_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        eprintln!(
            "{{\"event\":\"query_transport\",\"connection_id\":{},\"query_id\":{},\"authority_id\":{},\"session_id\":{},\"located_region_ids\":[{located_regions}],\"dispatched_region_ids\":[{dispatched_regions}],\"batch_attempts\":{},\"unary_attempts\":{}}}",
            self.connection_id,
            self.query_id,
            self.authority_id,
            self.session_id,
            evidence.batch_attempts,
            evidence.unary_attempts
        );
    }
}

impl ResultSetSource for ObservedResultSet {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner
            .next_batch(max_rows)
            .map_err(|error| error.to_string())
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        Ok(self.inner.columns().to_vec())
    }

    fn finish(&mut self) -> Result<(), String> {
        let result = self.inner.finish().map_err(|error| error.to_string());
        self.emit_evidence();
        result
    }

    fn close(&mut self) -> Result<(), String> {
        let result = self.inner.close().map_err(|error| error.to_string());
        self.emit_evidence();
        result
    }
}

fn configured_table(config: &NodeConfig) -> ConfiguredTable {
    let columns: Vec<_> = config
        .read_table
        .columns
        .iter()
        .map(|column| match column.kind {
            ConfiguredReadColumnKind::ClusteredPrimaryKey => {
                ConfiguredColumn::clustered_primary_key(&column.name, column.id)
            }
            ConfiguredReadColumnKind::StoredNotNull => {
                ConfiguredColumn::stored_not_null(&column.name, column.id)
            }
        })
        .collect();
    ConfiguredTable::new(
        &config.read_table.database,
        &config.read_table.table,
        config.read_table.table_id,
        columns,
    )
}

/// Starts the bounded concurrent production Rust SQL node.
pub fn run_configured_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let users = Arc::new(
        ConfiguredUserStore::load(&config.auth_file).map_err(RunConfiguredNodeError::Auth)?,
    );
    let (factory, authority) =
        RealTiKvSessionFactory::connect(&config).map_err(RunConfiguredNodeError::Engine)?;
    let factory = Arc::new(factory);
    let cluster_id = factory.cluster_id();
    let authority_id = factory.authority_id();
    let read_authority_id = factory.read_authority_id();
    run_with_process_shutdown(factory, authority, move |factory| {
        let node =
            ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users)).map_err(|error| {
                emit_connections_startup_failure(&error);
                RunConfiguredNodeError::Node(error)
            })?;
        let address = node.local_addr().map_err(|error| {
            emit_connections_startup_failure(&error);
            RunConfiguredNodeError::Node(error)
        })?;
        let shutdown_grace_ms = node.shutdown_grace_ms();
        let shutdown = node.shutdown_handle();
        ctrlc::set_handler(move || shutdown.shutdown()).map_err(|error| {
            emit_connections_startup_failure(&error);
            RunConfiguredNodeError::Signal(error)
        })?;
        let column_descriptors = config
            .read_table
            .columns
            .iter()
            .map(|column| {
                format!(
                    "{}:{}:{}",
                    column.name,
                    column.id,
                    column.kind.descriptor_name()
                )
            })
            .collect::<Vec<_>>();
        eprintln!(
            "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"pd_endpoints\":{},\"cluster_id\":{cluster_id},\"authority_id\":{authority_id},\"read_authority_id\":{read_authority_id},\"database\":{:?},\"table\":{:?},\"table_id\":{},\"column_count\":{},\"columns\":{:?},\"max_connections\":{},\"account_count\":{},\"shutdown_grace_ms\":{shutdown_grace_ms}}}",
            config.pd_endpoints.len(),
            config.read_table.database,
            config.read_table.table,
            config.read_table.table_id,
            column_descriptors.len(),
            column_descriptors,
            config.max_connections,
            users.len(),
        );
        node.run().map_err(RunConfiguredNodeError::Node)
    })
}

fn emit_connections_startup_failure(error: &impl std::fmt::Display) {
    eprintln!(
        "{{\"event\":\"process_shutdown_stage\",\"stage\":\"connections\",\"outcome\":\"error\",\"active\":0,\"accepted\":0,\"completed\":0,\"failed\":0,\"forced_connections\":0,\"error\":{:?}}}",
        error.to_string()
    );
}

/// Fallible unique process owner consumed after every server run path.
pub trait ProcessReadAuthority {
    /// Stops RegionCache, TiKV transport, and PD in dependency order.
    fn shutdown_process(&mut self) -> Result<(), ReadProcessShutdownError>;
}

impl ProcessReadAuthority for ProductionReadProcessAuthority {
    fn shutdown_process(&mut self) -> Result<(), ReadProcessShutdownError> {
        self.shutdown()
    }
}

/// Runs one node closure, drops every opener, then always shuts its authority.
pub fn run_with_process_shutdown<F, A, R>(
    factory: F,
    authority: A,
    run: R,
) -> Result<(), RunConfiguredNodeError>
where
    A: ProcessReadAuthority,
    R: FnOnce(F) -> Result<(), RunConfiguredNodeError>,
{
    run_with_process_shutdown_and_final(factory, authority, run, || {
        eprintln!("{{\"event\":\"sql_node_stopped\",\"outcome\":\"success\"}}");
    })
}

fn run_with_process_shutdown_and_final<F, A, R, S>(
    factory: F,
    mut authority: A,
    run: R,
    on_success: S,
) -> Result<(), RunConfiguredNodeError>
where
    A: ProcessReadAuthority,
    R: FnOnce(F) -> Result<(), RunConfiguredNodeError>,
    S: FnOnce(),
{
    let run_result = run(factory);
    let shutdown_result = authority.shutdown_process();
    emit_process_shutdown_events(&shutdown_result);
    match (run_result, shutdown_result) {
        (Ok(()), Ok(())) => {
            on_success();
            Ok(())
        }
        (Err(run), Ok(())) => Err(run),
        (Ok(()), Err(authority)) => Err(RunConfiguredNodeError::Authority(authority)),
        (Err(run), Err(authority)) => Err(RunConfiguredNodeError::Combined {
            run: Box::new(run),
            authority,
        }),
    }
}

fn emit_process_shutdown_events(result: &Result<(), ReadProcessShutdownError>) {
    if matches!(
        result,
        Err(ReadProcessShutdownError::ActiveSessions { .. })
            | Err(ReadProcessShutdownError::AdmissionPoisoned)
    ) {
        let error = result.as_ref().expect_err("matched shutdown error");
        eprintln!(
            "{{\"event\":\"process_shutdown_rejected\",\"error\":{:?}}}",
            error.to_string()
        );
        return;
    }
    for stage in [
        ReadProcessShutdownStage::RegionCache,
        ReadProcessShutdownStage::TikvTransport,
        ReadProcessShutdownStage::Pd,
    ] {
        let failure = match result {
            Err(ReadProcessShutdownError::StageFailures(failures)) => {
                failures.iter().find(|failure| failure.stage == stage)
            }
            _ => None,
        };
        let stage_name = match stage {
            ReadProcessShutdownStage::RegionCache => "region_cache",
            ReadProcessShutdownStage::TikvTransport => "tikv_transport",
            ReadProcessShutdownStage::Pd => "pd",
        };
        match failure {
            Some(failure) => eprintln!(
                "{{\"event\":\"process_shutdown_stage\",\"stage\":\"{stage_name}\",\"outcome\":\"error\",\"error\":{:?}}}",
                failure.message
            ),
            None => eprintln!(
                "{{\"event\":\"process_shutdown_stage\",\"stage\":\"{stage_name}\",\"outcome\":\"success\"}}"
            ),
        }
    }
}

/// Startup/runtime failure from the fully composed node.
#[derive(Debug)]
pub enum RunConfiguredNodeError {
    /// The required immutable account catalog was rejected.
    Auth(ConfiguredUserStoreError),
    /// The process SIGINT/SIGTERM handler could not be installed.
    Signal(ctrlc::Error),
    /// Production query-authority construction failed.
    Engine(SqlQueryError),
    /// Listener or connection runtime failed.
    Node(SqlNodeError),
    /// Process authority shutdown failed after the node drained.
    Authority(ReadProcessShutdownError),
    /// Both node execution and process authority shutdown failed.
    Combined {
        /// Node startup, admission, or drain failure.
        run: Box<RunConfiguredNodeError>,
        /// Ordered process authority shutdown failure.
        authority: ReadProcessShutdownError,
    },
}

impl std::fmt::Display for RunConfiguredNodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auth(error) => write!(formatter, "cannot load authentication catalog: {error}"),
            Self::Signal(error) => write!(formatter, "cannot install shutdown handler: {error}"),
            Self::Engine(error) => {
                write!(
                    formatter,
                    "cannot construct read authority: {}",
                    error.message
                )
            }
            Self::Node(error) => error.fmt(formatter),
            Self::Authority(error) => write!(formatter, "read authority shutdown failed: {error}"),
            Self::Combined { run, authority } => write!(
                formatter,
                "node failed: {run}; read authority shutdown also failed: {authority}"
            ),
        }
    }
}

impl std::error::Error for RunConfiguredNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Auth(error) => Some(error),
            Self::Signal(error) => Some(error),
            Self::Node(error) => Some(error),
            Self::Authority(error)
            | Self::Combined {
                authority: error, ..
            } => Some(error),
            Self::Engine(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FactoryEvent(Arc<Mutex<Vec<&'static str>>>);

    impl Drop for FactoryEvent {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("factory_drop");
        }
    }

    struct AuthorityEvent {
        events: Arc<Mutex<Vec<&'static str>>>,
        result: Result<(), ReadProcessShutdownError>,
    }

    impl ProcessReadAuthority for AuthorityEvent {
        fn shutdown_process(&mut self) -> Result<(), ReadProcessShutdownError> {
            self.events.lock().unwrap().push("authority_shutdown");
            self.result.clone()
        }
    }

    #[test]
    fn final_success_event_runs_only_after_factory_drop_and_authority_shutdown() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let final_events = Arc::clone(&events);
        run_with_process_shutdown_and_final(
            FactoryEvent(Arc::clone(&events)),
            AuthorityEvent {
                events: Arc::clone(&events),
                result: Ok(()),
            },
            |factory| {
                drop(factory);
                Ok(())
            },
            move || final_events.lock().unwrap().push("sql_node_stopped"),
        )
        .unwrap();

        assert_eq!(
            *events.lock().unwrap(),
            ["factory_drop", "authority_shutdown", "sql_node_stopped"]
        );
    }

    #[test]
    fn authority_failure_suppresses_final_success_event() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let final_events = Arc::clone(&events);
        let result = run_with_process_shutdown_and_final(
            FactoryEvent(Arc::clone(&events)),
            AuthorityEvent {
                events: Arc::clone(&events),
                result: Err(ReadProcessShutdownError::AdmissionPoisoned),
            },
            |factory| {
                drop(factory);
                Ok(())
            },
            move || final_events.lock().unwrap().push("sql_node_stopped"),
        );

        assert!(matches!(result, Err(RunConfiguredNodeError::Authority(_))));
        assert_eq!(
            *events.lock().unwrap(),
            ["factory_drop", "authority_shutdown"]
        );
    }
}
