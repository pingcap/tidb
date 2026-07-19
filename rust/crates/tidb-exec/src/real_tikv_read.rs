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
use std::sync::{Arc, Mutex};
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
use tidb_txnkv::{
    rpc::TonicCoprocessorClient, DirectUnaryClient, PdRegionLoader, SharedReadAuthority,
    SharedReadOpener,
};

use crate::dag_request::{
    construct_read_only_dag_req, DagRequestBuildError, DagRequestContext, TiKvScanPlan,
};
use crate::distsql_recordset::DistSqlRecordSet;

/// Concrete retained production transport used by the first SQL node.
pub type ProductionReadTransport =
    DirectUnaryQueryTransport<TonicCoprocessorClient, PdRegionLoader>;

/// Cloneable production session opener over process-owned capabilities.
///
/// This value contains no worker join authority. The unique process authority
/// retains RegionCache, TiKV transport, and PD lifecycle owners separately.
pub struct ProductionReadSessionFactory {
    read_opener: SharedReadOpener<TonicCoprocessorClient, PdRegionLoader>,
    default_timeout: Duration,
    lock_timestamp_source: PdTimestampSource,
}

impl RealTiKvSessionTransportFactory for ProductionReadSessionFactory {
    type Transport = ProductionReadTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        DirectUnaryQueryTransport::from_read_authority(
            &self.read_opener,
            DirectUnaryRuntimeConfig {
                default_timeout: self.default_timeout,
                ..DirectUnaryRuntimeConfig::default()
            },
            self.lock_timestamp_source.clone(),
        )
        .map_err(|error| error.to_string())
    }
}

/// Backward-compatible name for the unique production process authority.
pub type ProductionReadAuthority = ProductionReadProcessAuthority;

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
/// `F` opens sessions from process-owned transport/cache handles. `S` is the
/// cloneable PD timestamp capability.
/// Consequently this type is `Send + Sync` whenever those two process
/// capabilities are `Send + Sync`, while `F::Transport` may remain
/// thread-local.
pub struct RealTiKvReadSessionOpener<F, S> {
    table: Arc<ConfiguredTable>,
    transport_factory: Arc<F>,
    timestamp_source: S,
    cluster_id: u64,
    authority_id: u64,
    leases: Arc<ReadSessionLeases>,
}

/// Compatibility alias for callers that only need a cloneable session opener.
pub type RealTiKvReadAuthority<F, S> = RealTiKvReadSessionOpener<F, S>;

struct ReadSessionLeases {
    admission: Mutex<ReadSessionAdmission>,
}

struct ReadSessionAdmission {
    accepting: bool,
    active: usize,
    next_session_id: u64,
}

struct ReadSessionLease {
    leases: Arc<ReadSessionLeases>,
}

impl Drop for ReadSessionLease {
    fn drop(&mut self) {
        let mut admission = match self.leases.admission.lock() {
            Ok(admission) => admission,
            Err(poisoned) => poisoned.into_inner(),
        };
        admission.active = admission
            .active
            .checked_sub(1)
            .expect("read session lease count underflow");
    }
}

/// Unique authority that closes session admission before process shutdown.
pub struct ReadSessionAdmissionOwner {
    leases: Arc<ReadSessionLeases>,
}

impl ReadSessionAdmissionOwner {
    /// Linearizes admission closure with the zero-active-session check.
    pub fn close_admission(&self) -> Result<(), ReadProcessShutdownError> {
        let mut admission = self
            .leases
            .admission
            .lock()
            .map_err(|_| ReadProcessShutdownError::AdmissionPoisoned)?;
        admission.accepting = false;
        if admission.active == 0 {
            return Ok(());
        }
        let active = admission.active;
        admission.accepting = true;
        Err(ReadProcessShutdownError::ActiveSessions { active })
    }
}

impl<F, S: Clone> Clone for RealTiKvReadSessionOpener<F, S> {
    fn clone(&self) -> Self {
        Self {
            table: Arc::clone(&self.table),
            transport_factory: Arc::clone(&self.transport_factory),
            timestamp_source: self.timestamp_source.clone(),
            cluster_id: self.cluster_id,
            authority_id: self.authority_id,
            leases: Arc::clone(&self.leases),
        }
    }
}

impl<F, S> RealTiKvReadSessionOpener<F, S> {
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
        Self::new_with_admission_owner(table, transport_factory, timestamp_source, cluster_id).0
    }

    /// Retains a cloneable opener plus one non-cloneable admission authority.
    #[must_use]
    pub fn new_with_admission_owner(
        table: ConfiguredTable,
        transport_factory: F,
        timestamp_source: S,
        cluster_id: u64,
    ) -> (Self, ReadSessionAdmissionOwner) {
        let leases = Arc::new(ReadSessionLeases {
            admission: Mutex::new(ReadSessionAdmission {
                accepting: true,
                active: 0,
                next_session_id: 1,
            }),
        });
        let opener = Self {
            table: Arc::new(table),
            transport_factory: Arc::new(transport_factory),
            timestamp_source,
            cluster_id,
            authority_id: next_read_authority_id(),
            leases: Arc::clone(&leases),
        };
        (opener, ReadSessionAdmissionOwner { leases })
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

    /// Number of connection-local sessions that have not drained yet.
    #[must_use]
    pub fn active_sessions(&self) -> usize {
        match self.leases.admission.lock() {
            Ok(admission) => admission.active,
            Err(poisoned) => poisoned.into_inner().active,
        }
    }
}

impl<F, S> RealTiKvReadSessionOpener<F, S>
where
    F: RealTiKvSessionTransportFactory,
    F::Transport: QueryTransport,
    <F::Transport as QueryTransport>::Response: 'static,
    S: TimestampSource + Clone,
{
    /// Opens one connection-local session inside the calling server worker.
    pub fn open_session(&self) -> Result<RealTiKvReadSession<F::Transport, S>, RealTiKvReadError> {
        let (session_id, lease) = {
            let mut admission = self.leases.admission.lock().map_err(|_| {
                RealTiKvReadError::Transport("read session admission lock is poisoned".to_owned())
            })?;
            if !admission.accepting {
                return Err(RealTiKvReadError::Transport(
                    "read session admission is closed".to_owned(),
                ));
            }
            let session_id = admission.next_session_id;
            if session_id == 0 {
                return Err(RealTiKvReadError::Transport(
                    "read session identity space exhausted".to_owned(),
                ));
            }
            admission.next_session_id = admission.next_session_id.wrapping_add(1);
            admission.active = admission.active.checked_add(1).ok_or_else(|| {
                RealTiKvReadError::Transport("read session lease space exhausted".to_owned())
            })?;
            (
                session_id,
                ReadSessionLease {
                    leases: Arc::clone(&self.leases),
                },
            )
        };
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
            Some(lease),
        ))
    }
}

/// Unique lifecycle owner for production PD, RegionCache, and TiKV transport.
pub struct ProductionReadProcessAuthority {
    opener: ProductionOpener,
    admission: ReadSessionAdmissionOwner,
    lifecycle: ProductionReadLifecycle,
}

enum ProductionOpener {
    Open(RealTiKvReadSessionOpener<ProductionReadSessionFactory, PdTimestampSource>),
    Closed,
}

struct ProductionReadLifecycle {
    region_cache: ProductionRegionLifecycle,
    transport: ProductionTransportLifecycle,
    pd: ProductionPdLifecycle,
}

enum ProductionRegionLifecycle {
    Running(SharedReadAuthority<TonicCoprocessorClient, PdRegionLoader>),
    Closed,
}

enum ProductionTransportLifecycle {
    Running(TonicCoprocessorClient),
    Closed,
}

enum ProductionPdLifecycle {
    Running(PdClient),
    Closed,
}

/// One stage in the dependency-ordered production read shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadProcessShutdownStage {
    /// Stop and join RegionCache maintenance before its dependencies.
    RegionCache,
    /// Stop and join the TiKV transport after RegionCache.
    TikvTransport,
    /// Stop and join PD only after every dependent worker has stopped.
    Pd,
}

/// One stage-specific failure retained without skipping later stages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadProcessShutdownFailure {
    /// Stage that returned the failure.
    pub stage: ReadProcessShutdownStage,
    /// Typed stage error rendered at the composition boundary.
    pub message: String,
}

/// Rejection or aggregate failure from explicit process shutdown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadProcessShutdownError {
    /// Connection-local sessions must drain before any process stage stops.
    ActiveSessions {
        /// Number of live session leases observed by the unique authority.
        active: usize,
    },
    /// The internal admission lock was poisoned before shutdown linearized.
    AdmissionPoisoned,
    /// Every stage was attempted in order; these stages failed.
    StageFailures(Vec<ReadProcessShutdownFailure>),
}

impl fmt::Display for ReadProcessShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ActiveSessions { active } => {
                write!(
                    formatter,
                    "cannot shut down with {active} active read sessions"
                )
            }
            Self::AdmissionPoisoned => {
                formatter.write_str("read session admission lock is poisoned")
            }
            Self::StageFailures(failures) => {
                formatter.write_str("read process shutdown failed")?;
                for failure in failures {
                    write!(formatter, "; {:?}: {}", failure.stage, failure.message)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ReadProcessShutdownError {}

/// Fallible process lifecycle used by production and deterministic order tests.
pub trait ReadProcessShutdownStages {
    /// Stops and joins RegionCache maintenance.
    fn shutdown_region_cache(&mut self) -> Result<(), String>;
    /// Stops and joins the TiKV transport.
    fn shutdown_tikv_transport(&mut self) -> Result<(), String>;
    /// Stops and joins PD.
    fn shutdown_pd(&mut self) -> Result<(), String>;
}

/// Attempts every process stage in dependency order and aggregates failures.
pub fn shutdown_read_process(
    active_sessions: usize,
    stages: &mut impl ReadProcessShutdownStages,
) -> Result<(), ReadProcessShutdownError> {
    if active_sessions != 0 {
        return Err(ReadProcessShutdownError::ActiveSessions {
            active: active_sessions,
        });
    }
    let mut failures = Vec::new();
    for (stage, result) in [
        (
            ReadProcessShutdownStage::RegionCache,
            stages.shutdown_region_cache(),
        ),
        (
            ReadProcessShutdownStage::TikvTransport,
            stages.shutdown_tikv_transport(),
        ),
        (ReadProcessShutdownStage::Pd, stages.shutdown_pd()),
    ] {
        if let Err(message) = result {
            failures.push(ReadProcessShutdownFailure { stage, message });
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(ReadProcessShutdownError::StageFailures(failures))
    }
}

impl ReadProcessShutdownStages for ProductionReadLifecycle {
    fn shutdown_region_cache(&mut self) -> Result<(), String> {
        let authority =
            std::mem::replace(&mut self.region_cache, ProductionRegionLifecycle::Closed);
        match authority {
            ProductionRegionLifecycle::Running(authority) => {
                authority.shutdown().map_err(|error| error.to_string())
            }
            ProductionRegionLifecycle::Closed => Ok(()),
        }
    }

    fn shutdown_tikv_transport(&mut self) -> Result<(), String> {
        let transport =
            std::mem::replace(&mut self.transport, ProductionTransportLifecycle::Closed);
        match transport {
            ProductionTransportLifecycle::Running(mut transport) => {
                transport.close().map_err(|error| error.to_string())
            }
            ProductionTransportLifecycle::Closed => Ok(()),
        }
    }

    fn shutdown_pd(&mut self) -> Result<(), String> {
        let pd = std::mem::replace(&mut self.pd, ProductionPdLifecycle::Closed);
        match pd {
            ProductionPdLifecycle::Running(pd) => pd.shutdown().map_err(|error| error.to_string()),
            ProductionPdLifecycle::Closed => Ok(()),
        }
    }
}

impl ProductionReadProcessAuthority {
    /// Bootstraps PD, region maintenance, and tonic exactly once per process.
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
        let loader = PdRegionLoader::from_client(pd.clone());
        let cache = RegionCache::new(loader);
        let transport_owner = TonicCoprocessorClient::new()
            .map_err(|error| RealTiKvReadError::Transport(error.to_string()))?;
        debug_assert!(transport_owner.is_transport_owner());
        let read_authority =
            SharedReadAuthority::start_with_store_liveness(transport_owner.clone(), cache)
                .map_err(|error| RealTiKvReadError::Transport(error.to_string()))?;
        let factory = ProductionReadSessionFactory {
            read_opener: read_authority.opener(),
            default_timeout: timeout,
            lock_timestamp_source: timestamp_source.clone(),
        };
        let (opener, admission) = RealTiKvReadSessionOpener::new_with_admission_owner(
            table,
            factory,
            timestamp_source,
            cluster_id,
        );
        Ok(Self {
            opener: ProductionOpener::Open(opener),
            admission,
            lifecycle: ProductionReadLifecycle {
                region_cache: ProductionRegionLifecycle::Running(read_authority),
                transport: ProductionTransportLifecycle::Running(transport_owner),
                pd: ProductionPdLifecycle::Running(pd),
            },
        })
    }

    /// Cloneable session-opening capability without process shutdown authority.
    #[must_use]
    pub fn opener(
        &self,
    ) -> RealTiKvReadSessionOpener<ProductionReadSessionFactory, PdTimestampSource> {
        self.opener_ref().clone()
    }

    /// Opens one connection-local production session.
    pub fn open_session(
        &self,
    ) -> Result<RealTiKvReadSession<ProductionReadTransport, PdTimestampSource>, RealTiKvReadError>
    {
        self.opener_ref().open_session()
    }

    /// Real PD cluster identity retained by this process.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.opener_ref().cluster_id()
    }

    /// Stable executor process-authority identity.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.opener_ref().authority_id()
    }

    /// Stable identity of the sole maintained read authority.
    #[must_use]
    pub const fn read_authority_id(&self) -> u64 {
        match &self.lifecycle.region_cache {
            ProductionRegionLifecycle::Running(authority) => authority.authority_id(),
            ProductionRegionLifecycle::Closed => 0,
        }
    }

    /// Whether the retained transport value is the unique worker owner.
    #[must_use]
    pub const fn owns_transport_worker(&self) -> bool {
        match &self.lifecycle.transport {
            ProductionTransportLifecycle::Running(transport) => transport.is_transport_owner(),
            ProductionTransportLifecycle::Closed => false,
        }
    }

    /// Rejects active sessions, then always attempts RegionCache, TiKV, and PD.
    pub fn shutdown(&mut self) -> Result<(), ReadProcessShutdownError> {
        self.admission.close_admission()?;
        let opener = std::mem::replace(&mut self.opener, ProductionOpener::Closed);
        drop(opener);
        shutdown_read_process(0, &mut self.lifecycle)
    }

    const fn opener_ref(
        &self,
    ) -> &RealTiKvReadSessionOpener<ProductionReadSessionFactory, PdTimestampSource> {
        match &self.opener {
            ProductionOpener::Open(opener) => opener,
            ProductionOpener::Closed => panic!("production read authority is closed"),
        }
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
    plan_evidence: RealTiKvQueryPlanEvidence,
    cancellation: Arc<CancelHandle>,
}

/// One physical executor kind frozen before a real query is published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealTiKvPlanExecutorKind {
    /// The configured table scan at executor-list position zero.
    TableScan,
    /// A TiKV Selection containing the planner's resolved predicates.
    Selection,
}

impl RealTiKvPlanExecutorKind {
    /// Returns the stable source-facing executor name used by live evidence.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TableScan => "TableScan",
            Self::Selection => "Selection",
        }
    }

    const fn request_envelope_kind(self) -> ExecutorKind {
        match self {
            Self::TableScan => ExecutorKind::TableScan,
            // Selection is deliberately `Other` in the request-builder's
            // concurrency-only shape model. The immutable query evidence and
            // encoded DAG retain its exact physical identity.
            Self::Selection => ExecutorKind::Other,
        }
    }
}

/// Immutable physical-plan evidence attached to an admitted real query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealTiKvQueryPlanEvidence {
    executor_kinds: Vec<RealTiKvPlanExecutorKind>,
    predicate_count: usize,
    output_offsets: Vec<u32>,
}

impl RealTiKvQueryPlanEvidence {
    fn from_plan(plan: &ReadOnlyScanPlan) -> Self {
        let predicate_count = plan
            .selection()
            .map_or(0, |selection| selection.conditions().len());
        let mut executor_kinds = vec![RealTiKvPlanExecutorKind::TableScan];
        if predicate_count != 0 {
            executor_kinds.push(RealTiKvPlanExecutorKind::Selection);
        }
        Self {
            executor_kinds,
            predicate_count,
            output_offsets: plan.projection_output_offsets().to_vec(),
        }
    }

    /// Returns executor kinds in exact TiKV list-DAG order.
    #[must_use]
    pub fn executor_kinds(&self) -> &[RealTiKvPlanExecutorKind] {
        &self.executor_kinds
    }

    /// Returns the number of flattened Selection conditions.
    #[must_use]
    pub const fn predicate_count(&self) -> usize {
        self.predicate_count
    }

    /// Returns the final reader projection over the scan input.
    #[must_use]
    pub fn output_offsets(&self) -> &[u32] {
        &self.output_offsets
    }

    fn request_envelope(&self) -> RequestEnvelope {
        RequestEnvelope::new(
            self.executor_kinds
                .iter()
                .map(|kind| ExecutorShape::new(kind.request_envelope_kind()))
                .collect(),
        )
    }
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

    /// Returns immutable physical-plan evidence before response completion.
    #[must_use]
    pub const fn plan_evidence(&self) -> &RealTiKvQueryPlanEvidence {
        &self.plan_evidence
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
    _lease: Option<ReadSessionLease>,
}

impl RealTiKvReadSession<ProductionReadTransport, PdTimestampSource> {
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
            _lease: None,
        }
    }

    fn from_authority(
        table: Arc<ConfiguredTable>,
        transport: T,
        timestamp_source: S,
        cluster_id: u64,
        identity: RealTiKvReadSessionIdentity,
        lease: Option<ReadSessionLease>,
    ) -> Self {
        Self {
            table,
            transport,
            timestamp_source,
            cluster_id,
            identity,
            last_snapshot_ts: None,
            _lease: lease,
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
        self.execute_with_cancellation(sql, Arc::new(CancelHandle::default()))
    }

    /// Parses, lowers, builds, and starts one lazy real-TiKV query with the
    /// caller's connection-visible cancellation authority.
    pub fn execute_with_cancellation(
        &mut self,
        sql: &str,
        cancellation: Arc<CancelHandle>,
    ) -> Result<RealTiKvQuery, RealTiKvReadError> {
        let plan = ReadOnlyScanPlan::lower(sql, self.table.as_ref())?;
        let plan_evidence = RealTiKvQueryPlanEvidence::from_plan(&plan);
        let snapshot_ts = self
            .timestamp_source
            .current_ts()
            .map_err(RealTiKvReadError::Query)?;
        if snapshot_ts == 0 {
            return Err(RealTiKvReadError::Query(
                "PD returned a zero snapshot timestamp".to_owned(),
            ));
        }

        let dag = construct_read_only_dag_req(
            &DagRequestContext::new("UTC", 0, 0, EncodeType::Default),
            TiKvScanPlan::Table(plan.table_scan()),
            plan.selection(),
            plan.projection_output_offsets(),
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
            .set_dag_request(plan_evidence.request_envelope(), dag_data);
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
            plan_evidence,
            cancellation,
        })
    }
}
