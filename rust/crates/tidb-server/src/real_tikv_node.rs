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

use tidb_distsql::{CancelHandle, DirectUnaryTransportEvidenceHandle};
use tidb_exec::distsql_recordset::DistSqlRecordSet;
use tidb_exec::real_tikv_read::{
    PdTimestampSource, ProductionReadAuthority, ProductionReadTransport, RealTiKvReadSession,
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

/// Process-owned factory shared by the fixed connection workers.
pub struct RealTiKvSessionFactory {
    authority: ProductionReadAuthority,
    query_activity: Arc<QueryActivity>,
}

impl RealTiKvSessionFactory {
    /// Connects all process authorities exactly once from validated config.
    pub fn connect(config: &NodeConfig) -> Result<Self, SqlQueryError> {
        let table = configured_table(config);
        let authority = ProductionReadAuthority::connect(
            config.pd_endpoints.clone(),
            PRODUCTION_CONTROL_PLANE_TIMEOUT,
            table,
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(Self {
            authority,
            query_activity: Arc::new(QueryActivity::default()),
        })
    }

    /// Returns the PD cluster identity validated during process bootstrap.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.authority.cluster_id()
    }

    /// Stable executor process-authority identity.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority.authority_id()
    }

    /// Stable maintained read-authority identity.
    #[must_use]
    pub const fn read_authority_id(&self) -> u64 {
        self.authority.read_authority_id()
    }
}

impl QuerySessionFactory for RealTiKvSessionFactory {
    type Session = RealTiKvServerSession;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        let inner = self
            .authority
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
        eprintln!(
            "{{\"event\":\"query_snapshot\",\"connection_id\":{},\"query_id\":{query_id},\"authority_id\":{},\"session_id\":{},\"cluster_id\":{cluster_id},\"snapshot_ts\":{snapshot_ts},\"table_id\":{table_id},\"user\":{:?},\"host\":{:?}}}",
            self.context.connection_id,
            identity.authority_id(),
            identity.session_id(),
            self.context.identity.username(),
            self.context.identity.host(),
        );
        Ok(QueryResult::new(Box::new(ObservedResultSet {
            inner: query.into_record_set(),
            evidence,
            connection_id: self.context.connection_id,
            query_id,
            authority_id: identity.authority_id(),
            session_id: identity.session_id(),
            emitted: false,
            _cancellation_lease: cancellation_lease,
            _query_activity: query_activity,
        })))
    }
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
    let factory =
        Arc::new(RealTiKvSessionFactory::connect(&config).map_err(RunConfiguredNodeError::Engine)?);
    let cluster_id = factory.cluster_id();
    let authority_id = factory.authority_id();
    let read_authority_id = factory.read_authority_id();
    let node = ConcurrentSqlNode::bind(&config, Arc::clone(&factory), Arc::clone(&users))
        .map_err(RunConfiguredNodeError::Node)?;
    let address = node.local_addr().map_err(RunConfiguredNodeError::Node)?;
    let shutdown = node.shutdown_handle();
    ctrlc::set_handler(move || shutdown.shutdown()).map_err(RunConfiguredNodeError::Signal)?;
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
        "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"pd_endpoints\":{},\"cluster_id\":{cluster_id},\"authority_id\":{authority_id},\"read_authority_id\":{read_authority_id},\"database\":{:?},\"table\":{:?},\"table_id\":{},\"column_count\":{},\"columns\":{:?},\"max_connections\":{},\"account_count\":{}}}",
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
        }
    }
}

impl std::error::Error for RunConfiguredNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Auth(error) => Some(error),
            Self::Signal(error) => Some(error),
            Self::Node(error) => Some(error),
            Self::Engine(_) => None,
        }
    }
}
