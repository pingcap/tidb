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
use tidb_exec::real_tikv_dml::{commit_configured_write, prepare_configured_write};
use tidb_exec::real_tikv_read::{
    prepare_configured_point_read, PdTimestampSource, ProductionReadProcessAuthority,
    ProductionReadSessionFactory, ProductionReadTransport, ReadProcessShutdownError,
    ReadProcessShutdownStage, RealOptimisticTransactionOpener, RealTiKvQuery, RealTiKvReadError,
    RealTiKvReadSession, RealTiKvReadSessionOpener,
};
use tidb_planner::aggregation_descriptor::AggregateKind;
use tidb_planner::prepared_dml::PreparedBindValue;
use tidb_planner::read_only_scan::{
    configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredIndex, ConfiguredTable,
    PreparedAggregate, PreparedAggregateKind, ReadOnlyScanPlan,
};
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_protocol::ColumnInfo;

use crate::aggregate_result_set::AggregateResultSetSource;
use crate::configured_user_store::{ConfiguredUserStore, ConfiguredUserStoreError};
use crate::distinct_result_set::DistinctResultSetSource;
use crate::node_config::{ConfiguredReadColumnKind, ConfiguredReadTable, NodeConfig};
use crate::resultset_source::ResultSetSource;
use crate::session_transaction::SessionTransaction;
use crate::sorting_result_set::SortingResultSetSource;
use crate::sql_node::{
    ActiveQueryCancellation, ConcurrentSqlNode, PreparedPointRead, PreparedWrite,
    QueryCancellationLease, QueryResult, QuerySession, QuerySessionFactory, SessionContext,
    SqlNodeError, SqlQueryError, WriteOutcome,
};

const PRODUCTION_CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// `MYSQL_TYPE_NEWDECIMAL`, the result type of `SUM` over an integer column.
const MYSQL_TYPE_NEWDECIMAL: u8 = 246;
/// The binary charset/collation id (`mysql.CharsetNameToID("binary")`), applied
/// to a numeric aggregate result by Go `SetBinChsClnFlag`.
const BINARY_CHARSET_ID: u16 = 63;
/// `mysql.BinaryFlag`, set on a numeric aggregate's result column.
const BINARY_FLAG: u16 = 128;

/// Builds the result column metadata for a prepared aggregate.
///
/// A `SUM` collapses the scan to one row whose type is the aggregate's own, not
/// the summed column's: a `DECIMAL` on the binary charset, nullable (an empty
/// group is `NULL`), with the flen Go `typeInfer4Sum` assigns. The metadata is
/// used both for the prepare response and, because the binary row encoder
/// dispatches each cell on its column type, for the execute-time cell encoding.
fn aggregate_result_columns(aggregate: &PreparedAggregate) -> Vec<ColumnInfo> {
    let type_code = match aggregate.kind() {
        PreparedAggregateKind::Sum => MYSQL_TYPE_NEWDECIMAL,
    };
    vec![ColumnInfo {
        name: aggregate.output_name().to_owned(),
        column_length: aggregate.result_column_length(),
        charset: BINARY_CHARSET_ID,
        flag: BINARY_FLAG,
        decimal: aggregate.result_decimals(),
        type_code,
        ..ColumnInfo::default()
    }]
}

impl ActiveQueryCancellation for CancelHandle {
    fn cancel(&self) {
        CancelHandle::cancel(self);
    }
}

/// Cloneable session opener shared by the fixed connection workers.
pub struct RealTiKvSessionFactory {
    opener: RealTiKvReadSessionOpener<ProductionReadSessionFactory, PdTimestampSource>,
    transaction_opener: RealOptimisticTransactionOpener,
    query_activity: Arc<QueryActivity>,
    read_authority_id: u64,
}

impl RealTiKvSessionFactory {
    /// Connects the unique process owner and derives its cloneable opener.
    pub fn connect(
        config: &NodeConfig,
    ) -> Result<(Self, ProductionReadProcessAuthority), SqlQueryError> {
        let catalog = configured_catalog(config)?;
        let [table] = catalog.tables() else {
            return Err(SqlQueryError::unknown(
                "multiple configured tables require the multi-relation dispatcher",
            ));
        };
        let authority = ProductionReadProcessAuthority::connect(
            config.pd_endpoints.clone(),
            PRODUCTION_CONTROL_PLANE_TIMEOUT,
            table.clone(),
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let factory = Self {
            opener: authority.opener(),
            transaction_opener: authority.transaction_opener(),
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
            transaction_opener: self.transaction_opener.clone(),
            context,
            query_activity: Arc::clone(&self.query_activity),
            next_query_id: 1,
            transaction: SessionTransaction::new(),
        })
    }
}

/// Worker-local server session around the executor session.
pub struct RealTiKvServerSession {
    inner: RealTiKvReadSession<ProductionReadTransport, PdTimestampSource>,
    transaction_opener: RealOptimisticTransactionOpener,
    context: SessionContext,
    query_activity: Arc<QueryActivity>,
    next_query_id: u64,
    /// The session's explicit-transaction state, pinning one read snapshot for
    /// the duration of a `BEGIN`/`COMMIT` transaction.
    transaction: SessionTransaction,
}

#[derive(Default)]
pub(crate) struct QueryActivity {
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl QueryActivity {
    pub(crate) fn begin(self: &Arc<Self>, connection_id: u64, query_id: u64) -> QueryActivityLease {
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

pub(crate) struct QueryActivityLease {
    activity: Arc<QueryActivity>,
    connection_id: u64,
    query_id: u64,
}

pub(crate) fn install_remote_publication_observer<E>(
    snapshot_ts: Option<u64>,
    install: impl FnOnce() -> Result<(), E>,
) -> Result<(), E> {
    if snapshot_ts.is_some() {
        install()?;
    }
    Ok(())
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

impl RealTiKvServerSession {
    /// Starts one read at the snapshot its session transaction requires.
    ///
    /// Inside an explicit transaction every read runs at the one pinned
    /// transaction snapshot, so the transaction is snapshot-consistent; a
    /// contradiction plan is empty at any snapshot and keeps the plain path.
    /// Outside a transaction each read takes its own fresh snapshot exactly as
    /// before.
    fn read_at_transaction_snapshot(
        &mut self,
        plan: ReadOnlyScanPlan,
        cancellation: Arc<CancelHandle>,
    ) -> Result<RealTiKvQuery, RealTiKvReadError> {
        if plan.is_contradiction() {
            return self
                .inner
                .execute_lowered_plan_with_cancellation(plan, cancellation);
        }
        let snapshot = {
            let inner = &self.inner;
            self.transaction.read_snapshot(|| inner.acquire_snapshot_ts())?
        };
        match snapshot {
            Some(pinned) => self
                .inner
                .execute_plan_at_snapshot(plan, pinned, cancellation),
            None => self
                .inner
                .execute_lowered_plan_with_cancellation(plan, cancellation),
        }
    }
}

impl QuerySession for RealTiKvServerSession {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        // Text statements inside an explicit transaction are a later slice: the
        // read-only transaction path pins snapshots for the prepared statements
        // sysbench uses, and a text data statement here fails closed rather than
        // silently running at a fresh, non-transactional snapshot.
        if self.transaction.is_active() {
            return Err(SqlQueryError::unknown(
                "COM_QUERY statements inside an explicit transaction are not yet supported",
            ));
        }
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
        let cluster_id = self.inner.cluster_id();
        let evidence = self.inner.transport_evidence_handle();
        let query = self
            .inner
            .execute_with_cancellation(sql, cancellation)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        observe_real_tikv_query(
            &self.context,
            query,
            query_id,
            cancellation_lease,
            query_activity,
            cluster_id,
            evidence,
        )
    }

    fn prepare_point_read(&mut self, sql: &str) -> Result<PreparedPointRead, SqlQueryError> {
        let catalog = ConfiguredCatalog::new([self.inner.configured_table().clone()])
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let template = prepare_configured_point_read(sql, &catalog)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        // An aggregate's result column is its own type (a DECIMAL for SUM), not
        // the summed scan column's, so it bypasses the scan-derived metadata.
        let result_columns = if let Some(aggregate) = template.aggregate() {
            aggregate_result_columns(aggregate)
        } else {
            // Bind placeholder handles only to resolve the result-column
            // metadata; a range template needs one placeholder per marker.
            let metadata_plan = template
                .bind(&vec![0; template.parameter_count()])
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            self.inner
                .protocol_columns_for_plan(&metadata_plan)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?
        };
        Ok(PreparedPointRead::new(template, result_columns))
    }

    fn execute_prepared_point_read<'a>(
        &'a mut self,
        statement: &PreparedPointRead,
        parameters: &[i64],
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        let plan = statement
            .template()
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
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
        let cluster_id = self.inner.cluster_id();
        let evidence = self.inner.transport_evidence_handle();
        let query = self
            .read_at_transaction_snapshot(plan, cancellation)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let result = observe_real_tikv_query(
            &self.context,
            query,
            query_id,
            cancellation_lease,
            query_activity,
            cluster_id,
            evidence,
        )?;
        // A SUM has no GROUP BY, so it collapses the whole scan to one row; wrap
        // the observed scan so the fold runs outside the storage-facing observer.
        // It is mutually exclusive with ORDER BY / DISTINCT (the planner rejects
        // those combinations), so it is handled before them.
        let template = statement.template();
        if let Some(aggregate) = template.aggregate() {
            let columns = statement.result_columns().to_vec();
            let kind = match aggregate.kind() {
                PreparedAggregateKind::Sum => AggregateKind::Sum,
            };
            return Ok(QueryResult::new(Box::new(AggregateResultSetSource::new(
                result.into_source(),
                kind,
                aggregate.source_offset(),
                columns,
            ))));
        }
        // ORDER BY (a SQL-layer sort over the projected output rows) and DISTINCT
        // (a whole-tuple dedup) are executor stages layered over the observed
        // scan stream. Compose sort inside dedup so `DISTINCT ... ORDER BY`
        // returns distinct rows already in sorted order; an unordered,
        // non-distinct read keeps the observed source untouched.
        let order_by = template.order_by();
        let distinct = template.is_distinct();
        if order_by.is_empty() && !distinct {
            return Ok(result);
        }
        let output_width = statement.result_columns().len();
        let mut source = result.into_source();
        if !order_by.is_empty() {
            source = Box::new(SortingResultSetSource::new(
                source,
                order_by.to_vec(),
                output_width,
            ));
        }
        if distinct {
            source = Box::new(DistinctResultSetSource::new(source));
        }
        Ok(QueryResult::new(source))
    }

    fn prepare_write(&mut self, sql: &str) -> Result<PreparedWrite, SqlQueryError> {
        let catalog = ConfiguredCatalog::new([self.inner.configured_table().clone()])
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let template = prepare_configured_write(sql, &catalog)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(PreparedWrite::new(template))
    }

    fn execute_prepared_write(
        &mut self,
        statement: &PreparedWrite,
        parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        // A write commits its own single-statement transaction. Buffering writes
        // into the session's open transaction and committing them together at
        // COMMIT is a later slice, so a write inside an explicit transaction
        // fails closed rather than auto-committing behind the transaction's back.
        if self.transaction.is_active() {
            return Err(SqlQueryError::unknown(
                "writes inside an explicit transaction are not yet supported",
            ));
        }
        let bound = statement
            .template()
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let report = commit_configured_write(
            &self.transaction_opener,
            &bound,
            PRODUCTION_CONTROL_PLANE_TIMEOUT,
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(WriteOutcome {
            affected_rows: report.affected_rows,
        })
    }

    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        match classify_transaction_control(sql) {
            None => Ok(None),
            Some(TransactionControl::Begin) => {
                self.transaction.begin();
                Ok(Some(true))
            }
            Some(TransactionControl::End) => {
                self.transaction.end();
                Ok(Some(false))
            }
            Some(TransactionControl::Unsupported(feature)) => Err(SqlQueryError::unknown(format!(
                "{feature} is not supported by the read-only Rust SQL node"
            ))),
        }
    }
}

pub(crate) fn observe_real_tikv_query<'a>(
    context: &SessionContext,
    query: RealTiKvQuery,
    query_id: u64,
    cancellation_lease: QueryCancellationLease,
    query_activity: QueryActivityLease,
    cluster_id: u64,
    evidence: DirectUnaryTransportEvidenceHandle,
) -> Result<QueryResult<'a>, SqlQueryError> {
    let snapshot_ts = query.snapshot_ts();
    let snapshot_ts_json =
        snapshot_ts.map_or_else(|| "null".to_owned(), |timestamp| timestamp.to_string());
    let table_id = query.table_id();
    let identity = query.session_identity();
    let executor_kinds = query
        .plan_evidence()
        .executor_kinds()
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
    let predicate_count = query.plan_evidence().predicate_count();
    let output_offsets = query.plan_evidence().output_offsets().to_vec();
    let handle_range_count = query.plan_evidence().handle_range_count();
    let handle_ranges = query
        .plan_evidence()
        .handle_ranges()
        .iter()
        .map(|range| {
            format!(
                "{{\"low\":{},\"high\":{},\"low_exclude\":{},\"high_exclude\":{}}}",
                range.low(),
                range.high(),
                range.low_exclude(),
                range.high_exclude(),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let connection_id = context.connection_id;
    let authority_id = identity.authority_id();
    let session_id = identity.session_id();
    install_remote_publication_observer(snapshot_ts, || {
        evidence.set_publication_observer(move |published| {
            emit_query_transport_publication(
                connection_id,
                query_id,
                authority_id,
                session_id,
                published,
            );
        })
    })
    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    eprintln!(
        "{{\"event\":\"query_snapshot\",\"connection_id\":{},\"query_id\":{query_id},\"authority_id\":{},\"session_id\":{},\"cluster_id\":{cluster_id},\"snapshot_ts\":{snapshot_ts_json},\"table_id\":{table_id},\"executor_kinds\":{executor_kinds:?},\"predicate_count\":{predicate_count},\"output_offsets\":{output_offsets:?},\"handle_range_count\":{handle_range_count},\"handle_ranges\":[{handle_ranges}],\"user\":{:?},\"host\":{:?}}}",
        connection_id,
        authority_id,
        session_id,
        context.identity.username(),
        context.identity.host(),
    );
    Ok(QueryResult::new(Box::new(ObservedResultSet {
        inner: query.into_record_set(),
        evidence,
        connection_id,
        query_id,
        authority_id,
        session_id,
        emitted: false,
        _completion: QueryCompletion::new(cancellation_lease, query_activity),
    })))
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
    _completion: QueryCompletion,
}

/// Keeps cancellation registration and activity accounting alive until one
/// query result is finished or dropped. Multi-relation sessions reuse this
/// guard instead of creating a second lifecycle authority.
pub(crate) struct QueryCompletion {
    _cancellation_lease: QueryCancellationLease,
    _query_activity: QueryActivityLease,
}

impl QueryCompletion {
    pub(crate) const fn new(
        cancellation_lease: QueryCancellationLease,
        query_activity: QueryActivityLease,
    ) -> Self {
        Self {
            _cancellation_lease: cancellation_lease,
            _query_activity: query_activity,
        }
    }
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

pub(crate) fn configured_table(table: &ConfiguredReadTable) -> ConfiguredTable {
    let columns: Vec<_> = table
        .columns
        .iter()
        .map(|column| match column.kind {
            ConfiguredReadColumnKind::ClusteredPrimaryKey => {
                ConfiguredColumn::clustered_primary_key(&column.name, column.id)
            }
            ConfiguredReadColumnKind::StoredNotNull => {
                ConfiguredColumn::stored_not_null(&column.name, column.id)
            }
            ConfiguredReadColumnKind::StoredIntNotNull => {
                ConfiguredColumn::stored_int_not_null(&column.name, column.id)
            }
            ConfiguredReadColumnKind::StoredCharNotNull { max_length } => {
                ConfiguredColumn::stored_char_not_null(&column.name, column.id, max_length)
            }
        })
        .collect();
    // Every declared index is a non-unique single-column index; the write path
    // maintains its entries and fails closed on any shape it does not model.
    let indexes = table
        .indexes
        .iter()
        .map(|index| ConfiguredIndex::non_unique(index.index_id, index.column_id));
    ConfiguredTable::new(&table.database, &table.table, table.table_id, columns).with_indexes(indexes)
}

/// Converts the one canonical startup list into the planner's immutable
/// catalog, preserving source order and shared identity validation.
pub(crate) fn configured_catalog(config: &NodeConfig) -> Result<ConfiguredCatalog, SqlQueryError> {
    ConfiguredCatalog::new(config.read_tables.iter().map(configured_table))
        .map_err(|error| SqlQueryError::unknown(error.to_string()))
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
        let table_descriptors = config
            .read_tables
            .iter()
            .map(|table| {
                let columns = table
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
                format!(
                    "{{\"database\":{:?},\"table\":{:?},\"table_id\":{},\"columns\":{:?}}}",
                    table.database, table.table, table.table_id, columns
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        eprintln!(
            "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"pd_endpoints\":{},\"cluster_id\":{cluster_id},\"authority_id\":{authority_id},\"read_authority_id\":{read_authority_id},\"tables\":[{table_descriptors}],\"max_connections\":{},\"account_count\":{},\"shutdown_grace_ms\":{shutdown_grace_ms}}}",
            config.pd_endpoints.len(),
            config.max_connections,
            users.len(),
        );
        node.run().map_err(RunConfiguredNodeError::Node)
    })
}

pub(crate) fn emit_connections_startup_failure(error: &impl std::fmt::Display) {
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
    use std::cell::Cell;
    use std::sync::Mutex;

    #[test]
    fn contradiction_then_remote_query_leaves_no_stale_publication_observer() {
        let installed = Cell::new(false);
        let install_calls = Cell::new(0);
        let install_once = || {
            install_calls.set(install_calls.get() + 1);
            if installed.replace(true) {
                Err("a publication observer is already installed for this query")
            } else {
                Ok(())
            }
        };

        install_remote_publication_observer(None, install_once)
            .expect("a local contradiction has no physical publication to observe");
        assert!(!installed.get());
        assert_eq!(install_calls.get(), 0);

        install_remote_publication_observer(Some(42), install_once)
            .expect("the next remote query must own the sole observer slot");
        assert!(installed.get());
        assert_eq!(install_calls.get(), 1);
    }

    #[test]
    fn canonical_configuration_conversion_preserves_order_and_rejects_unwired_multi_dispatch() {
        let config = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--read-table",
            "campaign25",
            "orders",
            "42",
            "2",
            "id:1:clustered-pk",
            "account_id:2:stored-not-null",
            "--read-table",
            "campaign25",
            "accounts",
            "43",
            "2",
            "id:1:clustered-pk",
            "balance:2:stored-not-null",
            "--auth-file",
            "/tmp/campaign25-users.tsv",
        ])
        .unwrap();
        let catalog = configured_catalog(&config).unwrap();
        assert_eq!(
            catalog
                .tables()
                .iter()
                .map(|table| (table.schema(), table.table(), table.table_id()))
                .collect::<Vec<_>>(),
            [("campaign25", "orders", 42), ("campaign25", "accounts", 43)]
        );

        let error = RealTiKvSessionFactory::connect(&config)
            .err()
            .expect("F0 must fail before attempting PD/TiKV multi-table dispatch");
        assert_eq!(
            error.message,
            "multiple configured tables require the multi-relation dispatcher"
        );
    }

    #[test]
    fn a_declared_index_maps_into_the_planner_catalog_table() {
        let config = NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--read-table",
            "sbtest",
            "sbtest1",
            "900",
            "2",
            "id:1:clustered-pk",
            "k:2:stored-int-not-null",
            "1",
            "k_idx:5:2",
            "--auth-file",
            "/tmp/campaign30-users.tsv",
        ])
        .unwrap();
        let catalog = configured_catalog(&config).unwrap();
        let indexes = catalog.tables()[0].indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_id(), 5);
        assert_eq!(indexes[0].column_id(), 2);
        assert!(!indexes[0].is_unique(), "declared indexes are non-unique");
    }

    #[test]
    fn shared_query_completion_owns_activity_and_cancellation_leases() {
        let activity = Arc::new(QueryActivity::default());
        let cancellation = crate::sql_node::ConnectionCancellation::default();
        let cancellation_lease = cancellation.install(Arc::new(CancelHandle::default()));
        let activity_lease = activity.begin(7, 11);
        assert_eq!(activity.active.load(Ordering::Acquire), 1);

        let completion = QueryCompletion::new(cancellation_lease, activity_lease);
        assert_eq!(activity.active.load(Ordering::Acquire), 1);
        drop(completion);
        assert_eq!(activity.active.load(Ordering::Acquire), 0);
    }

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
