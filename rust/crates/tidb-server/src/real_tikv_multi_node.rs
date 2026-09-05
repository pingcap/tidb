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

//! The two-relation production server adapter.
//!
//! This is deliberately a sibling of the single-table adapter rather than a
//! second listener or process owner. One `ProductionReadProcessAuthority`
//! supplies both table-bound transports; every statement then reaches the
//! planner and `ConfiguredInnerJoinRecordSet` before a result can escape.

use std::sync::Arc;
use std::time::Duration;

use tidb_distsql::CancelHandle;
use tidb_exec::cluster_catalog::LoadedTableRefusal;
use tidb_exec::{
    configured_inner_join::ConfiguredInnerJoinRecordSet,
    configured_ordered_query::{
        ConfiguredOrderedQueryRecordSet, PreparedConfiguredOrderedQueryTail,
    },
    real_tikv_read::{
        prepare_configured_point_read, PdTimestampSource, ProductionReadProcessAuthority,
        ProductionReadSessionFactory, ProductionReadTransport, RealTiKvMultiReadSession,
        RealTiKvReadSessionOpener,
    },
};
use tidb_planner::{
    configured_join_plan::ConfiguredJoinPlan, configured_order_limit::ConfiguredOrderedJoinPlan,
    prepared_dml::PreparedBindValue,
};
use tidb_session::process::{ProcessGuard, ProcessRegistry};

use crate::cluster_privileges::PrivilegeReloader;
use crate::configured_user_store::ConfiguredUserStore;
use crate::node_config::NodeConfig;
use crate::real_tikv_node::{
    aggregate_result_columns, aggregate_result_field_types, complete_real_tikv_query,
    configured_catalog, default_cursor_memory, emit_connections_startup_failure,
    execute_cluster_ddl, lightweight_ddl_statement_context, parse_set_time_zone,
    point_read_result_field_types, prepared_bind_sql_error, read_error_sql_error,
    refusal_aware_error, refusal_aware_prepared_plan_error, run_with_process_shutdown,
    served_table_descriptor, shape_prepared_point_read_result, time_zone_sql_error,
    RealTiKvSessionTimeZone, RunConfiguredNodeError, CURSOR_INIT_CHUNK_SIZE, CURSOR_MAX_CHUNK_SIZE,
};
use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    configured_write_error, ActiveQueryCancellation, ConcurrentSqlNode, ConnectionKillTarget,
    PreparedPointRead, PreparedWrite, QueryResult, QuerySession, QuerySessionFactory,
    SessionContext, SqlQueryError, WriteOutcome,
};
use tidb_exec::real_tikv_dml::{
    commit_configured_write, prepare_configured_write, ConfiguredWriteWarning,
};
use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;

const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Cloneable worker-session opener for the exactly-two-table SQL-node path.
pub struct RealTiKvMultiSessionFactory {
    opener: RealTiKvReadSessionOpener<ProductionReadSessionFactory, PdTimestampSource>,
    transaction_opener: RealOptimisticTransactionOpener,
    tables: [tidb_planner::read_only_scan::ConfiguredTable; 2],
    read_authority_id: u64,
    max_topn_rows: usize,
    /// Loaded tables the cluster really has, at the same schema-version
    /// snapshot as `tables`, that this node could not decode. Empty for the
    /// command-line-only two-table shape, which has no cluster catalog to
    /// refuse anything from.
    table_refusals: Arc<Vec<LoadedTableRefusal>>,
    /// The etcd client a catalog change committed here announces itself
    /// through, so peers reload without waiting out their lease.
    schema_notifier: Option<Arc<tidb_pd_client::EtcdClient>>,
    spill_storage: Option<Arc<tidb_util::spill_storage::SpillStorage>>,
    mem_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    processes: ProcessRegistry,
}

impl RealTiKvMultiSessionFactory {
    /// Connects the one process authority and retains the two immutable table shapes.
    pub fn connect(
        config: &NodeConfig,
    ) -> Result<(Self, ProductionReadProcessAuthority), SqlQueryError> {
        let catalog = configured_catalog(config)?;
        let tables = catalog.tables();
        let [left, right] = tables else {
            return Err(SqlQueryError::unknown(
                "multi-relation dispatcher requires exactly two configured tables",
            ));
        };
        let authority = ProductionReadProcessAuthority::connect(
            config.pd_endpoints.clone(),
            CONTROL_PLANE_TIMEOUT,
            left.clone(),
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let factory = Self::from_authority(
            &authority,
            [left.clone(), right.clone()],
            config.max_topn_rows,
            Vec::new(),
            crate::real_tikv_node::connect_schema_notifier(config),
        );
        Ok((factory, authority))
    }

    /// Builds the cloneable two-table session opener over an authority that
    /// has already bootstrapped PD, RegionCache, and the TiKV transport.
    ///
    /// Used both by [`Self::connect`] (the command-line two-table shape, which
    /// has no cluster catalog and so no refusals) and by the catalog-loaded
    /// dispatcher, which resolves `tables` and `table_refusals` from the
    /// cluster's own stored schema before this node is known to be the
    /// two-table surface.
    pub(crate) fn from_authority(
        authority: &ProductionReadProcessAuthority,
        tables: [tidb_planner::read_only_scan::ConfiguredTable; 2],
        max_topn_rows: usize,
        table_refusals: Vec<LoadedTableRefusal>,
        schema_notifier: Option<Arc<tidb_pd_client::EtcdClient>>,
    ) -> Self {
        Self {
            opener: authority.opener(),
            transaction_opener: authority.transaction_opener(),
            tables,
            read_authority_id: authority.read_authority_id(),
            max_topn_rows,
            table_refusals: Arc::new(table_refusals),
            schema_notifier,
            spill_storage: None,
            mem_arbitrator: None,
            processes: ProcessRegistry::default(),
        }
    }

    pub(crate) fn with_spill_storage(
        mut self,
        spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    ) -> Self {
        self.spill_storage = Some(spill_storage);
        self
    }

    /// Installs the process memory authority inherited by cursor statements.
    #[must_use]
    pub fn with_mem_arbitrator(
        mut self,
        arbitrator: Arc<tidb_util::memory::MemArbitrator>,
    ) -> Self {
        self.mem_arbitrator = Some(arbitrator);
        self
    }

    /// The two tables this node serves, whether described on the command line
    /// or loaded from the cluster's own catalog.
    #[must_use]
    pub fn served_tables(&self) -> &[tidb_planner::read_only_scan::ConfiguredTable; 2] {
        &self.tables
    }

    /// Loaded tables this node refuses to serve, with their exact reasons.
    #[must_use]
    pub fn table_refusals(&self) -> &[LoadedTableRefusal] {
        &self.table_refusals
    }

    /// Returns the PD cluster identity validated during process bootstrap.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.opener.cluster_id()
    }

    /// Returns the stable process authority shared by both table readers.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.opener.authority_id()
    }

    /// Returns the maintained transport authority identity.
    #[must_use]
    pub const fn read_authority_id(&self) -> u64 {
        self.read_authority_id
    }

    /// Returns the process-wide admission bound for one configured TopN heap.
    #[must_use]
    pub const fn max_topn_rows(&self) -> usize {
        self.max_topn_rows
    }
}

impl QuerySessionFactory for RealTiKvMultiSessionFactory {
    type Session = RealTiKvMultiServerSession;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        let reader = self
            .opener
            .open_multi_session(self.tables.clone())
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let cursor_memory = default_cursor_memory(
            context.connection_id,
            self.spill_storage.as_ref(),
            self.mem_arbitrator.as_ref(),
        );
        let process = self.processes.register(
            context.connection_id,
            context.identity.username().to_owned(),
            context.peer_addr.to_string(),
            String::new(),
            Some(Arc::new(ConnectionKillTarget::new(
                context.cancellation.clone(),
                context.close.clone(),
            ))),
        );
        process.set_trackers(
            Arc::clone(cursor_memory.session_tracker()),
            Arc::clone(cursor_memory.session_disk_tracker()),
        );
        Ok(RealTiKvMultiServerSession {
            reader,
            transaction_opener: self.transaction_opener.clone(),
            table_refusals: Arc::clone(&self.table_refusals),
            schema_notifier: self.schema_notifier.clone(),
            context,
            max_topn_rows: self.max_topn_rows,
            time_zone: RealTiKvSessionTimeZone::default(),
            cursor_memory,
            statement_warnings: Vec::new(),
            write_sli: tidb_util::sli::TxnWriteThroughputSli::default(),
            last_affected_rows: 0,
            _process: process,
        })
    }

    fn session_manager(&self) -> Option<Arc<dyn tidb_util::memoryusagealarm::SessionManager>> {
        Some(Arc::new(self.processes.clone()))
    }
}

/// One authenticated worker session over the shared two-table read authority.
pub struct RealTiKvMultiServerSession {
    reader: RealTiKvMultiReadSession<ProductionReadTransport, PdTimestampSource>,
    transaction_opener: RealOptimisticTransactionOpener,
    /// Loaded-but-unservable tables, consulted when a statement names one.
    table_refusals: Arc<Vec<LoadedTableRefusal>>,
    /// The factory's etcd client, so a catalog change this session commits
    /// announces itself the way Go's DDL owner does.
    schema_notifier: Option<Arc<tidb_pd_client::EtcdClient>>,
    context: SessionContext,
    max_topn_rows: usize,
    /// This session's `time_zone`, threaded into both relations' DAG reads
    /// and into `TIMESTAMP` write literals, mirroring the single-table
    /// session's own `time_zone` field (`RealTiKvServerSession`).
    time_zone: RealTiKvSessionTimeZone,
    cursor_memory: tidb_executor::SessionMemory,
    statement_warnings: Vec<ConfiguredWriteWarning>,
    write_sli: tidb_util::sli::TxnWriteThroughputSli,
    last_affected_rows: u64,
    _process: ProcessGuard,
}

impl QuerySession for RealTiKvMultiServerSession {
    fn finish_execute_stmt(&mut self, cost: std::time::Duration) {
        let cost = i64::try_from(cost.as_nanos()).unwrap_or(i64::MAX);
        self.write_sli
            .finish_execute_stmt(cost, self.last_affected_rows, false);
    }

    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        self.last_affected_rows = 0;
        let process_statement = self._process.statement_started(sql, "", "autocommit");
        self.statement_warnings.clear();
        let cancellation = Arc::new(CancelHandle::default());
        let cancellation_registration: Arc<dyn ActiveQueryCancellation> = cancellation.clone();
        let cancellation_lease = self.context.cancellation.install(cancellation_registration);
        let catalog = configured_catalog_from_tables(&self.reader)?;
        let route = prepare_configured_query(sql, &catalog, self.max_topn_rows)
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.message))?;
        if let ConfiguredQueryRoute::LocalEmpty { plan, .. } = route {
            let inner = ConfiguredOrderedQueryRecordSet::local_empty(
                &plan,
                self.reader.configured_tables(),
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            return Ok(QueryResult::new(Box::new(OrderedMultiJoinResultSet {
                inner,
                _cancellation_lease: cancellation_lease,
            }))
            .with_process_statement(process_statement));
        }
        let ConfiguredQueryRoute::Join { plan, tail, .. } = route else {
            unreachable!("local empty route returned above")
        };
        let join = self
            .reader
            .execute_configured_inner_join_with_cancellation(plan, cancellation)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let inner = match tail {
            Some(tail) => tail
                .attach(join)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?,
            None => {
                return self
                    .finish_unordered_query(join, cancellation_lease)
                    .map(|result| result.with_process_statement(process_statement));
            }
        };
        Ok(QueryResult::new(Box::new(OrderedMultiJoinResultSet {
            inner,
            _cancellation_lease: cancellation_lease,
        }))
        .with_process_statement(process_statement))
    }

    fn prepare_point_read(&mut self, sql: &str) -> Result<PreparedPointRead, SqlQueryError> {
        let catalog = configured_catalog_from_tables(&self.reader)?;
        let template = prepare_configured_point_read(sql, &catalog)
            .map_err(|error| refusal_aware_prepared_plan_error(&self.table_refusals, error))?;
        let (result_columns, result_field_types) = if let Some(aggregate) = template.aggregate() {
            (
                aggregate_result_columns(aggregate),
                aggregate_result_field_types(aggregate),
            )
        } else {
            // Bind placeholder handles only to resolve the result-column
            // metadata; a range template needs one placeholder per marker.
            let metadata_plan = template
                .bind(&vec![0; template.parameter_count()])
                .map_err(|error| prepared_bind_sql_error(&error))?;
            let result_field_types = point_read_result_field_types(&metadata_plan);
            let result_columns = self
                .reader
                .protocol_columns_for_point_read_plan(&metadata_plan)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            (result_columns, result_field_types)
        };
        PreparedPointRead::new(sql.to_owned(), template, result_columns, result_field_types)
    }

    fn execute_prepared_point_read<'a>(
        &'a mut self,
        statement: &PreparedPointRead,
        parameters: &[i64],
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        self.last_affected_rows = 0;
        let process_statement = self
            ._process
            .statement_started(statement.sql(), "", "autocommit");
        let field_types = statement.result_field_types().to_vec();
        let authority = tidb_session::ResultMaterializationAuthority::new(
            self.cursor_memory.statement(),
            CURSOR_INIT_CHUNK_SIZE,
            CURSOR_MAX_CHUNK_SIZE,
        );
        let plan = statement
            .template()
            .bind(parameters)
            .map_err(|error| prepared_bind_sql_error(&error))?;
        let configured = self
            .reader
            .readers()
            .iter()
            .any(|reader| reader.configured_table().table_id() == plan.table_id());
        if !configured {
            return Err(SqlQueryError::unknown(
                "prepared point-read plan does not belong to a configured relation",
            ));
        }
        let cancellation = Arc::new(CancelHandle::default());
        let cancellation_registration: Arc<dyn ActiveQueryCancellation> = cancellation.clone();
        let cancellation_lease = self.context.cancellation.install(cancellation_registration);
        let query = self
            .reader
            .execute_point_read_plan_with_cancellation(plan, cancellation)
            .map_err(read_error_sql_error)?;
        let result = complete_real_tikv_query(query, cancellation_lease);
        Ok(shape_prepared_point_read_result(result, statement)
            .with_cursor_materialization(field_types, authority)
            .with_process_statement(process_statement))
    }

    fn prepare_write(&mut self, sql: &str) -> Result<PreparedWrite, SqlQueryError> {
        let catalog = configured_catalog_from_tables(&self.reader)?;
        let template = prepare_configured_write(sql, &catalog)
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.to_string()))?;
        Ok(PreparedWrite::new(sql.to_owned(), template))
    }

    fn execute_prepared_write(
        &mut self,
        statement: &PreparedWrite,
        parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        let _process_statement = self
            ._process
            .statement_started(statement.sql(), "", "autocommit");
        self.statement_warnings.clear();
        let bound = statement
            .template()
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let session_tz = self.time_zone.zone();
        let report = commit_configured_write(
            &self.transaction_opener,
            &bound,
            CONTROL_PLANE_TIMEOUT,
            &session_tz,
        )
        .map_err(|error| configured_write_error(&error))?
        .with_client_found_rows(self.context.client_found_rows);
        if report.write_size > 0 {
            self.write_sli
                .add_txn_write_size(report.write_size, report.write_keys);
        }
        if report.processed_keys > 0 && report.affected_rows > 0 {
            self.write_sli.add_read_keys(report.processed_keys);
        }
        self.last_affected_rows = report.affected_rows;
        self.statement_warnings = report.warnings;
        Ok(WriteOutcome {
            affected_rows: report.affected_rows,
            // This node has no auto-increment allocator.
            last_insert_id: 0,
        })
    }

    fn warning_count(&self) -> u16 {
        u16::try_from(self.statement_warnings.len()).unwrap_or(u16::MAX)
    }

    fn warning_codes(&self) -> Vec<u16> {
        self.statement_warnings
            .iter()
            .map(|warning| warning.code)
            .collect()
    }

    /// A catalog change is the only text-protocol OK-packet statement this
    /// multi-relation surface answers: DML over a joined pair is not lowered
    /// here, so anything else falls through to the query path unchanged.
    ///
    /// The default schema is the first served table's, which is the same
    /// relation the command line named first.
    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        self.last_affected_rows = 0;
        let _process_statement = self._process.statement_started(sql, "", "autocommit");
        // `SET time_zone` updates this session's own zone rather than reaching
        // storage: every subsequent read from either relation and every
        // `TIMESTAMP` write literal consult it from here on, mirroring
        // `RealTiKvServerSession::execute_write`.
        if let Some(value) = parse_set_time_zone(sql) {
            let parsed =
                RealTiKvSessionTimeZone::parse(value.trim()).map_err(time_zone_sql_error)?;
            self.reader.set_time_zone(&parsed.zone());
            self.time_zone = parsed;
            return Ok(Some(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        let default_schema = self.reader.configured_tables()[0].schema().to_owned();
        // This surface has no explicit-transaction state of its own, so a
        // catalog change here is always its own autocommit transaction.
        let ddl_context = lightweight_ddl_statement_context(&self.time_zone);
        execute_cluster_ddl(
            &self.transaction_opener,
            sql,
            &default_schema,
            &ddl_context,
            false,
            self.schema_notifier.as_ref(),
        )
    }
}

impl RealTiKvMultiServerSession {
    fn finish_unordered_query<'a>(
        &'a self,
        join: ConfiguredInnerJoinRecordSet,
        cancellation_lease: crate::sql_node::QueryCancellationLease,
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        Ok(QueryResult::new(Box::new(MultiJoinResultSet {
            inner: join,
            _cancellation_lease: cancellation_lease,
        })))
    }
}

fn configured_catalog_from_tables(
    reader: &RealTiKvMultiReadSession<ProductionReadTransport, PdTimestampSource>,
) -> Result<tidb_planner::read_only_scan::configured_catalog::ConfiguredCatalog, SqlQueryError> {
    tidb_planner::read_only_scan::configured_catalog::ConfiguredCatalog::new(
        reader.configured_tables().into_iter().cloned(),
    )
    .map_err(|error| SqlQueryError::unknown(error.to_string()))
}

enum ConfiguredQueryRoute {
    /// LIMIT 0 has ordinary metadata but must not open a TiKV query.
    LocalEmpty { plan: ConfiguredJoinPlan },
    /// The terminal tail, if present, was fully admitted before reader execution.
    Join {
        plan: ConfiguredJoinPlan,
        tail: Option<PreparedConfiguredOrderedQueryTail>,
    },
}

fn prepare_configured_query(
    sql: &str,
    catalog: &tidb_planner::read_only_scan::configured_catalog::ConfiguredCatalog,
    max_topn_rows: usize,
) -> Result<ConfiguredQueryRoute, SqlQueryError> {
    let plan = ConfiguredOrderedJoinPlan::lower(sql, catalog)
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    if plan.is_empty() {
        return Ok(ConfiguredQueryRoute::LocalEmpty {
            plan: plan.metadata_join().clone(),
        });
    }
    let join = plan
        .join()
        .expect("nonempty configured ordered plan retains its join")
        .clone();
    let tail = plan
        .order_limit()
        .cloned()
        .map(|tail| {
            PreparedConfiguredOrderedQueryTail::prepare(
                tail,
                join.full_schema().len(),
                max_topn_rows,
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))
        })
        .transpose()?;
    Ok(ConfiguredQueryRoute::Join { plan: join, tail })
}

struct MultiJoinResultSet {
    inner: ConfiguredInnerJoinRecordSet,
    _cancellation_lease: crate::sql_node::QueryCancellationLease,
}

impl ResultSetSource for MultiJoinResultSet {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner
            .next_batch(max_rows)
            .map_err(|error| error.to_string())
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        Ok(self.inner.columns().to_vec())
    }

    fn finish(&mut self) -> Result<(), String> {
        self.inner.finish().map_err(|error| error.to_string())
    }

    fn close(&mut self) -> Result<(), String> {
        self.inner.close().map_err(|error| error.to_string())
    }
}

/// Result-set wrapper for ordered, limited, and planner-known-empty joins.
///
/// The connection writer still owns the same finish/close lifecycle; this
/// wrapper changes only the terminal pull operator beneath it.
struct OrderedMultiJoinResultSet {
    inner: ConfiguredOrderedQueryRecordSet,
    _cancellation_lease: crate::sql_node::QueryCancellationLease,
}

impl ResultSetSource for OrderedMultiJoinResultSet {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner
            .next_batch(max_rows)
            .map_err(|error| error.to_string())
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        Ok(self.inner.columns().to_vec())
    }

    fn finish(&mut self) -> Result<(), String> {
        self.inner.finish().map_err(|error| error.to_string())
    }

    fn close(&mut self) -> Result<(), String> {
        self.inner.close().map_err(|error| error.to_string())
    }
}

/// Starts the existing listener/lifecycle against the two-relation factory.
pub fn run_configured_multi_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let spill_storage = crate::open_spill_storage(&config)?;
    let memory_arbitrator = crate::MemoryArbitratorAuthority::open(&config)?;
    run_configured_multi_node_with_spill(config, spill_storage, memory_arbitrator.arbitrator())
}

pub(crate) fn run_configured_multi_node_with_spill(
    config: NodeConfig,
    spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(), RunConfiguredNodeError> {
    let users = crate::real_tikv_node::configured_account_store(&config)?;
    let (factory, authority) =
        RealTiKvMultiSessionFactory::connect(&config).map_err(RunConfiguredNodeError::Engine)?;
    let users = Arc::new(users);
    run_bound_multi_node(
        config,
        factory,
        authority,
        users,
        spill_storage,
        memory_arbitrator,
        None,
    )
}

/// Starts the same listener/lifecycle over an already-connected two-table
/// factory and process authority.
///
/// Used both by [`run_configured_multi_node`] (which connects for itself from
/// the command-line two-table shape) and by the catalog-loaded dispatcher in
/// the crate root, which must connect once to read the cluster catalog before
/// it can even know two of its loaded tables are servable.
pub(crate) fn run_bound_multi_node(
    config: NodeConfig,
    factory: RealTiKvMultiSessionFactory,
    authority: ProductionReadProcessAuthority,
    users: Arc<ConfiguredUserStore>,
    spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    privilege_reloader: Option<PrivilegeReloader>,
) -> Result<(), RunConfiguredNodeError> {
    let sysvar_reloader =
        crate::real_tikv_node::prepare_cluster_sysvar_runtime(&config, &users, &authority);
    let factory = factory.with_spill_storage(spill_storage);
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = Arc::new(factory);
    let cluster_id = factory.cluster_id();
    let authority_id = factory.authority_id();
    let read_authority_id = factory.read_authority_id();
    let served_tables = factory.served_tables().clone();
    let refused_descriptors = factory
        .table_refusals()
        .iter()
        .map(|refusal| {
            format!(
                "{{\"table\":{:?},\"reason\":{:?}}}",
                refusal.name, refusal.reason
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    run_with_process_shutdown(factory, authority, move |factory| {
        // Held for exactly the node's run: the reload thread it owns is
        // stopped by `Drop` when this closure returns, whether the node
        // exited normally or by error.
        let privilege_reloader = privilege_reloader;
        let sysvar_reloader = sysvar_reloader?;
        let sysvar_watcher =
            crate::real_tikv_node::spawn_sysvar_watch(&config, sysvar_reloader.as_ref());
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
        let table_descriptors = served_tables
            .iter()
            .map(served_table_descriptor)
            .collect::<Vec<_>>()
            .join(",");
        eprintln!(
            "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"pd_endpoints\":{},\"cluster_id\":{cluster_id},\"authority_id\":{authority_id},\"read_authority_id\":{read_authority_id},\"tables\":[{table_descriptors}],\"refused_tables\":[{refused_descriptors}],\"max_connections\":{},\"account_count\":{},\"shutdown_grace_ms\":{shutdown_grace_ms}}}",
            config.pd_endpoints.len(),
            config.max_connections,
            users.len(),
        );
        let result = node.run().map_err(RunConfiguredNodeError::Node);
        drop(sysvar_watcher);
        crate::real_tikv_node::emit_privilege_reload_stats(privilege_reloader.as_ref());
        crate::real_tikv_node::emit_sysvar_reload_stats(sysvar_reloader.as_ref());
        drop(sysvar_reloader);
        result
    })
}

#[cfg(test)]
mod tests {
    use super::{configured_catalog, prepare_configured_query, ConfiguredQueryRoute, NodeConfig};

    fn config() -> NodeConfig {
        NodeConfig::parse([
            "tidb-server",
            "--path",
            "127.0.0.1:2379",
            "--read-table",
            "campaign26",
            "accounts",
            "101",
            "2",
            "id:1:clustered-pk",
            "balance:2:stored-not-null",
            "--read-table",
            "campaign26",
            "orders",
            "202",
            "3",
            "id:7:clustered-pk",
            "account_id:23:stored-not-null",
            "amount:31:stored-not-null",
            "--auth-file",
            "/tmp/campaign26-users.tsv",
        ])
        .expect("valid two-relation node config")
    }

    #[test]
    fn ordered_route_preflights_without_a_reader_and_keeps_limit_zero_local() {
        let config = config();
        let catalog = configured_catalog(&config).expect("catalog is local configuration only");
        let sql = "SELECT a.balance FROM accounts a JOIN orders o ON a.id=o.account_id \
                   ORDER BY o.amount DESC LIMIT 3";

        let rejected = prepare_configured_query(sql, &catalog, 2)
            .err()
            .expect("TopN capacity must reject before a reader can execute");
        assert_eq!(rejected.message, "configured TopN end 3 exceeds capacity 2");

        assert!(matches!(
            prepare_configured_query(sql, &catalog, 3).expect("admitted TopN"),
            ConfiguredQueryRoute::Join { tail: Some(_), .. }
        ));
        assert!(matches!(
            prepare_configured_query(
                "SELECT a.balance FROM accounts a JOIN orders o ON a.id=o.account_id \
                 ORDER BY o.amount LIMIT 0",
                &catalog,
                1,
            )
            .expect("LIMIT 0 is local"),
            ConfiguredQueryRoute::LocalEmpty { .. }
        ));
    }
}
