// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! First deployable read-only SQL-to-real-TiKV execution path.
//!
//! This module composes existing source-shaped owners. SQL admission and
//! physical scan selection stay in `tidb-planner`; DAG construction stays in
//! [`crate::dag_request`]; ranges and request metadata stay in
//! `tidb-distsql`; PD, region selection, BatchCommands-first dispatch, retry,
//! and lock recovery stay in the production transport. A process authority
//! owns the shared cluster capabilities while [`RealTiKvReadSession`] keeps
//! one connection's transport, cancellation, and lazy result state local to
//! the worker that admitted it.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::region::RegionCache;
use tidb_distsql::{
    CancelHandle, DatumRange, DirectUnaryQueryTransport, DirectUnaryRuntimeConfig,
    DirectUnaryTransportEvidence, DirectUnaryTransportEvidenceHandle, EncodeType, ExecutorKind,
    ExecutorShape, InjectedQueryRuntime, QueryResultContext, QueryTransport, RequestBuilder,
    RequestEnvelope, SelectInput, TimestampSource, WarningCollector,
};
use tidb_pd_client::{PdClient, PdClientError};
use tidb_planner::read_only_scan::{
    ConfiguredColumnKind, ConfiguredTable, ReadOnlyScanError, ReadOnlyScanPlan,
};
use tidb_protocol::{ColumnInfo, BINARY_DEFAULT_COLLATION_ID};
use tidb_txnkv::{rpc::TonicCoprocessorClient, PdRegionLoader};

use crate::dag_request::{
    construct_dag_req, DagRequestBuildError, DagRequestContext, TiKvScanPlan,
};
use crate::distsql_recordset::DistSqlRecordSet;

/// Concrete retained production transport used by the first SQL node.
pub type ProductionReadTransport =
    DirectUnaryQueryTransport<TonicCoprocessorClient, PdRegionLoader>;

static NEXT_READ_AUTHORITY_ID: AtomicU64 = AtomicU64::new(1);

fn next_read_authority_id() -> u64 {
    let id = NEXT_READ_AUTHORITY_ID.fetch_add(1, Ordering::Relaxed);
    assert_ne!(id, 0, "read authority identity space exhausted");
    id
}

/// Builds one worker-local query transport from process-owned shared handles.
///
/// An implementation may retain the unique process lifecycle owners together
/// with their cloneable request/cache handles, but it must give a session only
/// the handles. In particular, opening a session must not connect another PD
/// client, create a region cache, start a maintenance worker, or start a tonic
/// runtime. The returned transport deliberately has no `Send` bound because
/// the fixed server worker creates and consumes it on one thread.
///
/// Campaign 21's production implementation owns one
/// `TonicCoprocessorRuntime` and one
/// `tidb_txnkv::SharedReadAuthority<TonicCoprocessorClient, PdRegionLoader>`.
/// This method calls `DirectUnaryQueryTransport::from_read_authority`; it does
/// not call either authority's startup constructor.
pub trait RealTiKvSessionTransportFactory: Send + Sync {
    /// Worker-local transport type created for one admitted connection.
    type Transport: QueryTransport;

    /// Creates query-local state over the already-running process authority.
    fn open_session_transport(&self) -> Result<Self::Transport, String>;
}

/// Stable process/session identity carried into query evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealTiKvReadSessionIdentity {
    authority_id: u64,
    session_id: u64,
}

impl RealTiKvReadSessionIdentity {
    /// Identifies the process authority that minted this session.
    #[must_use]
    pub const fn authority_id(self) -> u64 {
        self.authority_id
    }

    /// Identifies this connection-local session within the authority.
    #[must_use]
    pub const fn session_id(self) -> u64 {
        self.session_id
    }
}

/// Process authority that opens worker-local real-TiKV sessions.
///
/// `F` owns the sole process transport/cache authorities and opens sessions
/// from their handles. `S` is the cloneable PD timestamp capability.
/// Consequently this type is `Send + Sync` whenever those two process
/// capabilities are `Send + Sync`, while `F::Transport` may remain
/// thread-local.
pub struct RealTiKvReadAuthority<F, S> {
    table: Arc<ConfiguredTable>,
    transport_factory: F,
    timestamp_source: S,
    cluster_id: u64,
    authority_id: u64,
    next_session_id: AtomicU64,
}

impl<F, S> RealTiKvReadAuthority<F, S> {
    /// Retains already-bootstrapped process capabilities.
    ///
    /// Construction is intentionally generic until the lower DistSQL layer
    /// supplies its cloneable production handle. This executor boundary must
    /// not guess at, or recreate, transport lifecycle ownership.
    #[must_use]
    pub fn new(
        table: ConfiguredTable,
        transport_factory: F,
        timestamp_source: S,
        cluster_id: u64,
    ) -> Self {
        Self {
            table: Arc::new(table),
            transport_factory,
            timestamp_source,
            cluster_id,
            authority_id: next_read_authority_id(),
            next_session_id: AtomicU64::new(1),
        }
    }

    /// Returns the real PD cluster identity, or zero for an injected authority.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Returns the stable identity shared by every session from this authority.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }

    /// Returns the exact configured table admitted by every opened session.
    #[must_use]
    pub fn configured_table(&self) -> &ConfiguredTable {
        self.table.as_ref()
    }
}

impl<F, S> RealTiKvReadAuthority<F, S>
where
    F: RealTiKvSessionTransportFactory,
    F::Transport: QueryTransport,
    <F::Transport as QueryTransport>::Response: 'static,
    S: TimestampSource + Clone,
{
    /// Opens one connection-local session inside the calling server worker.
    pub fn open_session(&self) -> Result<RealTiKvReadSession<F::Transport, S>, RealTiKvReadError> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        if session_id == 0 {
            return Err(RealTiKvReadError::Transport(
                "read session identity space exhausted".to_owned(),
            ));
        }
        let transport = self
            .transport_factory
            .open_session_transport()
            .map_err(RealTiKvReadError::Transport)?;
        Ok(RealTiKvReadSession::from_authority(
            Arc::clone(&self.table),
            transport,
            self.timestamp_source.clone(),
            self.cluster_id,
            RealTiKvReadSessionIdentity {
                authority_id: self.authority_id,
                session_id,
            },
        ))
    }
}

/// A clone of the sole PD worker used as TiDB's timestamp-oracle capability.
#[derive(Clone)]
pub struct PdTimestampSource {
    client: PdClient,
}

impl fmt::Debug for PdTimestampSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PdTimestampSource")
            .field("cluster_id", &self.client.cluster_id())
            .field("leader", &self.client.member_set().leader_url)
            .finish()
    }
}

impl PdTimestampSource {
    /// Shares the already-bootstrapped PD worker; this opens no runtime.
    #[must_use]
    pub const fn new(client: PdClient) -> Self {
        Self { client }
    }
}

impl TimestampSource for PdTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.client
            .get_timestamp()
            .map_err(|error| error.to_string())
    }
}

/// One admitted query and its lazy response owner.
pub struct RealTiKvQuery {
    record_set: DistSqlRecordSet,
    snapshot_ts: u64,
    table_id: i64,
    session_identity: RealTiKvReadSessionIdentity,
    cancellation: Arc<CancelHandle>,
}

impl RealTiKvQuery {
    /// Returns the real PD timestamp placed in the TiKV request.
    #[must_use]
    pub const fn snapshot_ts(&self) -> u64 {
        self.snapshot_ts
    }

    /// Returns the configured physical table identity.
    #[must_use]
    pub const fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Returns the process/session identity attached to this query evidence.
    #[must_use]
    pub const fn session_identity(&self) -> RealTiKvReadSessionIdentity {
        self.session_identity
    }

    /// Cancels only this query and its transport-owned request children.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns whether this query-local cancellation has fired.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Transfers the sole lazy response owner to the MySQL connection.
    #[must_use]
    pub fn into_record_set(self) -> DistSqlRecordSet {
        self.record_set
    }
}

/// Fail-closed construction or execution error.
#[derive(Debug)]
pub enum RealTiKvReadError {
    /// PD bootstrap or timestamp acquisition failed.
    Pd(PdClientError),
    /// TiKV transport construction failed.
    Transport(String),
    /// SQL is outside the visible milestone grammar/catalog boundary.
    Plan(ReadOnlyScanError),
    /// Existing physical scan-to-DAG lowering rejected the plan.
    Dag(DagRequestBuildError),
    /// Existing request construction rejected a range or envelope.
    Request(String),
    /// Existing lazy DistSQL send boundary failed.
    Query(String),
}

impl fmt::Display for RealTiKvReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pd(error) => write!(formatter, "PD read control plane failed: {error}"),
            Self::Transport(message) => write!(formatter, "TiKV transport failed: {message}"),
            Self::Plan(error) => write!(formatter, "read-only SQL rejected: {error}"),
            Self::Dag(error) => write!(formatter, "DAG lowering failed: {error}"),
            Self::Request(message) => write!(formatter, "DistSQL request failed: {message}"),
            Self::Query(message) => write!(formatter, "TiKV query failed: {message}"),
        }
    }
}

impl std::error::Error for RealTiKvReadError {}

impl From<PdClientError> for RealTiKvReadError {
    fn from(error: PdClientError) -> Self {
        Self::Pd(error)
    }
}

impl From<ReadOnlyScanError> for RealTiKvReadError {
    fn from(error: ReadOnlyScanError) -> Self {
        Self::Plan(error)
    }
}

impl From<DagRequestBuildError> for RealTiKvReadError {
    fn from(error: DagRequestBuildError) -> Self {
        Self::Dag(error)
    }
}

/// One worker-local read session retaining query and lazy response state.
pub struct RealTiKvReadSession<T = ProductionReadTransport, S = PdTimestampSource> {
    table: Arc<ConfiguredTable>,
    transport: T,
    timestamp_source: S,
    cluster_id: u64,
    identity: RealTiKvReadSessionIdentity,
    last_snapshot_ts: Option<u64>,
}

/// Compatibility name for callers migrating to the authority/session split.
///
/// New server code must own [`RealTiKvReadAuthority`] and call
/// [`RealTiKvReadAuthority::open_session`] inside a fixed worker.
pub type RealTiKvReadEngine<T = ProductionReadTransport, S = PdTimestampSource> =
    RealTiKvReadSession<T, S>;

impl RealTiKvReadSession<ProductionReadTransport, PdTimestampSource> {
    /// Legacy single-session bootstrap retained only until server integration.
    ///
    /// Concurrent server code must not call this per connection: it creates
    /// PD, cache-maintenance, and tonic owners. Production Campaign 21 wiring
    /// instead constructs those once below this layer and supplies a
    /// [`RealTiKvSessionTransportFactory`] to [`RealTiKvReadAuthority`].
    pub fn connect<I, E>(
        pd_endpoints: I,
        timeout: Duration,
        table: ConfiguredTable,
    ) -> Result<Self, RealTiKvReadError>
    where
        I: IntoIterator<Item = E>,
        E: Into<String>,
    {
        let pd = PdClient::connect_seeds(pd_endpoints, timeout)?;
        let cluster_id = pd.cluster_id();
        let timestamp_source = PdTimestampSource::new(pd.clone());
        let lock_timestamp_source = timestamp_source.clone();
        let loader = PdRegionLoader::from_client(pd);
        let cache = RegionCache::new(loader);
        let client = TonicCoprocessorClient::new()
            .map_err(|error| RealTiKvReadError::Transport(error.to_string()))?;
        let config = DirectUnaryRuntimeConfig {
            default_timeout: timeout,
            ..DirectUnaryRuntimeConfig::default()
        };
        let transport =
            DirectUnaryQueryTransport::new_production(client, cache, config, lock_timestamp_source)
                .map_err(|error| RealTiKvReadError::Transport(error.to_string()))?;
        let mut session = Self::new(table, transport, timestamp_source);
        session.cluster_id = cluster_id;
        session.identity = RealTiKvReadSessionIdentity {
            authority_id: next_read_authority_id(),
            session_id: 1,
        };
        Ok(session)
    }

    /// Returns real region and physical transport observations for the most
    /// recently bound query.
    #[must_use]
    pub fn transport_evidence(&self) -> DirectUnaryTransportEvidence {
        self.transport.evidence()
    }

    /// Returns a read-only handle that can observe the lazy physical attempt.
    #[must_use]
    pub fn transport_evidence_handle(&self) -> DirectUnaryTransportEvidenceHandle {
        self.transport.evidence_handle()
    }
}

impl<T, S> RealTiKvReadSession<T, S>
where
    T: QueryTransport,
    T::Response: 'static,
    S: TimestampSource,
{
    /// Injects an already-built transport and timestamp source for focused
    /// tests without changing production ownership.
    #[must_use]
    pub fn new(table: ConfiguredTable, transport: T, timestamp_source: S) -> Self {
        Self {
            table: Arc::new(table),
            transport,
            timestamp_source,
            cluster_id: 0,
            identity: RealTiKvReadSessionIdentity {
                authority_id: 0,
                session_id: 0,
            },
            last_snapshot_ts: None,
        }
    }

    fn from_authority(
        table: Arc<ConfiguredTable>,
        transport: T,
        timestamp_source: S,
        cluster_id: u64,
        identity: RealTiKvReadSessionIdentity,
    ) -> Self {
        Self {
            table,
            transport,
            timestamp_source,
            cluster_id,
            identity,
            last_snapshot_ts: None,
        }
    }

    /// Returns the real PD cluster identity, or zero for an injected test engine.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Returns the exact configured table admitted by the planner.
    #[must_use]
    pub fn configured_table(&self) -> &ConfiguredTable {
        self.table.as_ref()
    }

    /// Returns this worker-local session's process/session identity.
    #[must_use]
    pub const fn identity(&self) -> RealTiKvReadSessionIdentity {
        self.identity
    }

    /// Returns the most recent real snapshot accepted for a statement.
    #[must_use]
    pub const fn last_snapshot_ts(&self) -> Option<u64> {
        self.last_snapshot_ts
    }

    /// Parses, lowers, builds, and starts one lazy real-TiKV query.
    pub fn execute(&mut self, sql: &str) -> Result<RealTiKvQuery, RealTiKvReadError> {
        let plan = ReadOnlyScanPlan::lower(sql, self.table.as_ref())?;
        let snapshot_ts = self
            .timestamp_source
            .current_ts()
            .map_err(RealTiKvReadError::Query)?;
        if snapshot_ts == 0 {
            return Err(RealTiKvReadError::Query(
                "PD returned a zero snapshot timestamp".to_owned(),
            ));
        }

        let dag = construct_dag_req(
            &DagRequestContext::new("UTC", 0, 0, EncodeType::Default),
            &[TiKvScanPlan::Table(plan.table_scan())],
        )?;
        let dag_data = dag.encode_to_vec();
        let table_id = plan.table_id();
        let mut builder = RequestBuilder::new();
        builder
            .set_start_ts(snapshot_ts)
            // The retained response runtime publishes one logical region at
            // a time and deliberately has no unordered merge authority.
            // SQL without ORDER BY permits this stronger deterministic order.
            .set_keep_order(true)
            .set_table_ranges(
                table_id,
                &[DatumRange::inclusive(
                    vec![Datum::Int(i64::MIN)],
                    vec![Datum::Int(i64::MAX)],
                )],
            )
            .set_dag_request(
                RequestEnvelope::new(vec![ExecutorShape::new(ExecutorKind::TableScan)]),
                dag_data,
            );
        let cancellation = Arc::new(CancelHandle::default());
        let request = builder
            .build_transport_request(Arc::clone(&cancellation))
            .map_err(|error| RealTiKvReadError::Request(format!("{error:?}")))?;

        let field_types = plan
            .projected_columns()
            .iter()
            .map(|_| FieldType::new(FieldTypeCode::LongLong))
            .collect::<Vec<_>>();
        let protocol_columns = plan
            .projected_columns()
            .iter()
            .map(|column| ColumnInfo {
                schema: self.table.schema().to_owned(),
                table: self.table.table().to_owned(),
                org_table: self.table.table().to_owned(),
                name: column.output_name().to_owned(),
                org_name: column.source_name().to_owned(),
                column_length: 20,
                charset: BINARY_DEFAULT_COLLATION_ID,
                flag: match column.kind() {
                    ConfiguredColumnKind::ClusteredPrimaryKey => 0x0003,
                    ConfiguredColumnKind::StoredNotNull => 0x0001,
                },
                decimal: 0,
                type_code: FieldTypeCode::LongLong.mysql_type(),
                default_value: None,
            })
            .collect();

        let mut runtime = InjectedQueryRuntime::new(&mut self.transport);
        let result = runtime
            .select_with_runtime_stats(
                &request,
                SelectInput::default(),
                QueryResultContext::new(field_types, WarningCollector::new()),
                vec![0],
                0,
                true,
            )
            .map_err(|error| RealTiKvReadError::Query(error.to_string()))?;
        let record_set =
            DistSqlRecordSet::new(result.into_select_iter(Vec::new()), protocol_columns);
        self.last_snapshot_ts = Some(snapshot_ts);
        Ok(RealTiKvQuery {
            record_set,
            snapshot_ts,
            table_id,
            session_identity: self.identity,
            cancellation,
        })
    }
}
