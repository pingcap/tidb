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

use tidb_distsql::{CancelHandle, DirectUnaryTransportEvidenceHandle, PublishedDispatchEvidence};
use tidb_exec::cluster_catalog::LoadedTableRefusal;
use tidb_exec::{
    configured_inner_join::ConfiguredInnerJoinRecordSet,
    configured_ordered_query::{
        ConfiguredOrderedQueryEvidence, ConfiguredOrderedQueryRecordSet,
        PreparedConfiguredOrderedQueryTail,
    },
    real_tikv_read::{
        prepare_configured_point_read, PdTimestampSource, ProductionReadProcessAuthority,
        ProductionReadSessionFactory, ProductionReadTransport, RealTiKvMultiReadSession,
        RealTiKvReadSessionOpener,
    },
};
use tidb_planner::{
    configured_join_plan::ConfiguredJoinPlan,
    configured_order_limit::{ConfiguredOrderLimit, ConfiguredOrderedJoinPlan},
    configured_order_limit_contract::ConfiguredLimitWindow,
    prepared_dml::PreparedBindValue,
};

use crate::cluster_privileges::PrivilegeReloader;
use crate::configured_user_store::ConfiguredUserStore;
use crate::node_config::NodeConfig;
use crate::real_tikv_node::{
    configured_catalog, emit_connections_startup_failure, execute_cluster_ddl,
    install_remote_publication_observer, observe_real_tikv_query, refusal_aware_error,
    run_with_process_shutdown, served_table_descriptor, QueryActivity, QueryCompletion,
    RunConfiguredNodeError,
};
use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    ActiveQueryCancellation, ConcurrentSqlNode, PreparedPointRead, PreparedWrite, QueryResult,
    QuerySession, QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};
use tidb_exec::real_tikv_dml::{commit_configured_write, prepare_configured_write};
use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;

const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Cloneable worker-session opener for the exactly-two-table SQL-node path.
pub struct RealTiKvMultiSessionFactory {
    opener: RealTiKvReadSessionOpener<ProductionReadSessionFactory, PdTimestampSource>,
    transaction_opener: RealOptimisticTransactionOpener,
    tables: [tidb_planner::read_only_scan::ConfiguredTable; 2],
    activity: Arc<QueryActivity>,
    read_authority_id: u64,
    max_topn_rows: usize,
    /// Loaded tables the cluster really has, at the same schema-version
    /// snapshot as `tables`, that this node could not decode. Empty for the
    /// command-line-only two-table shape, which has no cluster catalog to
    /// refuse anything from.
    table_refusals: Arc<Vec<LoadedTableRefusal>>,
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
    ) -> Self {
        Self {
            opener: authority.opener(),
            transaction_opener: authority.transaction_opener(),
            tables,
            activity: Arc::new(QueryActivity::default()),
            read_authority_id: authority.read_authority_id(),
            max_topn_rows,
            table_refusals: Arc::new(table_refusals),
        }
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
        Ok(RealTiKvMultiServerSession {
            reader,
            transaction_opener: self.transaction_opener.clone(),
            table_refusals: Arc::clone(&self.table_refusals),
            context,
            activity: Arc::clone(&self.activity),
            next_query_id: 1,
            max_topn_rows: self.max_topn_rows,
        })
    }
}

/// One authenticated worker session over the shared two-table read authority.
pub struct RealTiKvMultiServerSession {
    reader: RealTiKvMultiReadSession<ProductionReadTransport, PdTimestampSource>,
    transaction_opener: RealOptimisticTransactionOpener,
    /// Loaded-but-unservable tables, consulted when a statement names one.
    table_refusals: Arc<Vec<LoadedTableRefusal>>,
    context: SessionContext,
    activity: Arc<QueryActivity>,
    next_query_id: u64,
    max_topn_rows: usize,
}

impl QuerySession for RealTiKvMultiServerSession {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        let query_id = self.next_query_id;
        self.next_query_id = self
            .next_query_id
            .checked_add(1)
            .ok_or_else(|| SqlQueryError::unknown("query identity space exhausted"))?;
        let activity = self.activity.begin(self.context.connection_id, query_id);
        let cancellation = Arc::new(CancelHandle::default());
        let cancellation_registration: Arc<dyn ActiveQueryCancellation> = cancellation.clone();
        let cancellation_lease = self.context.cancellation.install(cancellation_registration);
        let catalog = configured_catalog_from_tables(&self.reader)?;
        let route = prepare_configured_query(sql, &catalog, self.max_topn_rows)
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.message))?;
        if let Some((receipt, input_required)) = route.ordered_plan() {
            emit_ordered_query_plan(
                self.context.connection_id,
                query_id,
                receipt,
                input_required,
            );
        }
        if let ConfiguredQueryRoute::LocalEmpty { plan, .. } = route {
            let inner = ConfiguredOrderedQueryRecordSet::local_empty(
                &plan,
                self.reader.configured_tables(),
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            return Ok(QueryResult::new(Box::new(OrderedMultiJoinResultSet {
                inner,
                evidence: None,
                _completion: QueryCompletion::new(cancellation_lease, activity),
            })));
        }
        let ConfiguredQueryRoute::Join { plan, tail, .. } = route else {
            unreachable!("local empty route returned above")
        };
        let scans = [plan.left_scan().clone(), plan.right_scan().clone()];
        let table_ids = scans.each_ref().map(|scan| scan.table_id());
        let equality_offsets = plan
            .equality()
            .map(|equality| (equality.left().full_index(), equality.right().full_index()));
        let join = self
            .reader
            .execute_configured_inner_join_with_cancellation(plan, cancellation)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let snapshot_ts = join.snapshot_ts();
        let inner = match tail {
            Some(tail) => tail
                .attach(join)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?,
            None => {
                return self.finish_unordered_query(
                    join,
                    scans,
                    table_ids,
                    equality_offsets,
                    query_id,
                    cancellation_lease,
                    activity,
                );
            }
        };
        let identity = self.reader.readers()[0].identity();
        let evidence = std::array::from_fn(|relation| {
            self.reader.readers()[relation].transport_evidence_handle()
        });
        let connection_id = self.context.connection_id;
        let authority_id = identity.authority_id();
        let session_id = identity.session_id();
        install_remote_publication_observer(snapshot_ts, || {
            for (relation, handle) in evidence.iter().enumerate() {
                let table_id = table_ids[relation];
                handle
                    .set_publication_observer(move |published| {
                        emit_multi_query_transport_publication(
                            connection_id,
                            query_id,
                            authority_id,
                            session_id,
                            relation,
                            table_id,
                            published,
                        );
                    })
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            }
            Ok::<(), SqlQueryError>(())
        })?;
        emit_multi_query_snapshot(MultiQuerySnapshot {
            connection_id,
            query_id,
            authority_id,
            session_id,
            cluster_id: self.reader.readers()[0].cluster_id(),
            snapshot_ts,
            table_ids,
            scans: scans.each_ref(),
            equality_offsets,
            user: self.context.identity.username(),
            host: self.context.identity.host(),
        });
        Ok(QueryResult::new(Box::new(OrderedMultiJoinResultSet {
            inner,
            evidence: Some(MultiJoinEvidence {
                evidence,
                connection_id,
                query_id,
                authority_id,
                session_id,
                table_ids,
                emitted: false,
            }),
            _completion: QueryCompletion::new(cancellation_lease, activity),
        })))
    }

    fn prepare_point_read(&mut self, sql: &str) -> Result<PreparedPointRead, SqlQueryError> {
        let catalog = configured_catalog_from_tables(&self.reader)?;
        let template = prepare_configured_point_read(sql, &catalog)
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.to_string()))?;
        // Bind placeholder handles only to resolve the result-column metadata;
        // a range template needs one placeholder per marker.
        let metadata_plan = template
            .bind(&vec![0; template.parameter_count()])
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let result_columns = self
            .reader
            .protocol_columns_for_point_read_plan(&metadata_plan)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
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
        let physical_reader = self
            .reader
            .readers()
            .iter()
            .find(|reader| reader.configured_table().table_id() == plan.table_id())
            .ok_or_else(|| {
                SqlQueryError::unknown(
                    "prepared point-read plan does not belong to a configured relation",
                )
            })?;
        let cluster_id = physical_reader.cluster_id();
        let evidence = physical_reader.transport_evidence_handle();
        let query_id = self.next_query_id;
        self.next_query_id = self
            .next_query_id
            .checked_add(1)
            .ok_or_else(|| SqlQueryError::unknown("query identity space exhausted"))?;
        let activity = self.activity.begin(self.context.connection_id, query_id);
        let cancellation = Arc::new(CancelHandle::default());
        let cancellation_registration: Arc<dyn ActiveQueryCancellation> = cancellation.clone();
        let cancellation_lease = self.context.cancellation.install(cancellation_registration);
        let query = self
            .reader
            .execute_point_read_plan_with_cancellation(plan, cancellation)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        observe_real_tikv_query(
            &self.context,
            query,
            query_id,
            cancellation_lease,
            activity,
            cluster_id,
            evidence,
        )
    }

    fn prepare_write(&mut self, sql: &str) -> Result<PreparedWrite, SqlQueryError> {
        let catalog = configured_catalog_from_tables(&self.reader)?;
        let template = prepare_configured_write(sql, &catalog)
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.to_string()))?;
        Ok(PreparedWrite::new(template))
    }

    fn execute_prepared_write(
        &mut self,
        statement: &PreparedWrite,
        parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        let bound = statement
            .template()
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let report =
            commit_configured_write(&self.transaction_opener, &bound, CONTROL_PLANE_TIMEOUT)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(WriteOutcome {
            affected_rows: report.affected_rows,
            // This node has no auto-increment allocator.
            last_insert_id: 0,
        })
    }

    /// A catalog change is the only text-protocol OK-packet statement this
    /// multi-relation surface answers: DML over a joined pair is not lowered
    /// here, so anything else falls through to the query path unchanged.
    ///
    /// The default schema is the first served table's, which is the same
    /// relation the command line named first.
    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        let default_schema = self.reader.configured_tables()[0].schema().to_owned();
        // This surface has no explicit-transaction state of its own, so a
        // catalog change here is always its own autocommit transaction.
        execute_cluster_ddl(&self.transaction_opener, sql, &default_schema, false)
    }
}

impl RealTiKvMultiServerSession {
    #[allow(clippy::too_many_arguments)]
    fn finish_unordered_query<'a>(
        &'a self,
        join: ConfiguredInnerJoinRecordSet,
        scans: [tidb_planner::read_only_scan::ReadOnlyScanPlan; 2],
        table_ids: [i64; 2],
        equality_offsets: Option<(usize, usize)>,
        query_id: u64,
        cancellation_lease: crate::sql_node::QueryCancellationLease,
        activity: crate::real_tikv_node::QueryActivityLease,
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        let identity = self.reader.readers()[0].identity();
        let evidence = std::array::from_fn(|relation| {
            self.reader.readers()[relation].transport_evidence_handle()
        });
        let connection_id = self.context.connection_id;
        let authority_id = identity.authority_id();
        let session_id = identity.session_id();
        let snapshot_ts = join.snapshot_ts();
        install_remote_publication_observer(snapshot_ts, || {
            for (relation, handle) in evidence.iter().enumerate() {
                let table_id = table_ids[relation];
                handle
                    .set_publication_observer(move |published| {
                        emit_multi_query_transport_publication(
                            connection_id,
                            query_id,
                            authority_id,
                            session_id,
                            relation,
                            table_id,
                            published,
                        );
                    })
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            }
            Ok::<(), SqlQueryError>(())
        })?;
        emit_multi_query_snapshot(MultiQuerySnapshot {
            connection_id,
            query_id,
            authority_id,
            session_id,
            cluster_id: self.reader.readers()[0].cluster_id(),
            snapshot_ts,
            table_ids,
            scans: scans.each_ref(),
            equality_offsets,
            user: self.context.identity.username(),
            host: self.context.identity.host(),
        });
        Ok(QueryResult::new(Box::new(MultiJoinResultSet {
            inner: join,
            evidence,
            connection_id,
            query_id,
            authority_id,
            session_id,
            table_ids,
            emitted: false,
            _completion: QueryCompletion::new(cancellation_lease, activity),
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
    LocalEmpty {
        plan: ConfiguredJoinPlan,
        ordered_plan: OrderedQueryPlanReceipt,
    },
    /// The terminal tail, if present, was fully admitted before reader execution.
    Join {
        plan: ConfiguredJoinPlan,
        tail: Option<PreparedConfiguredOrderedQueryTail>,
        ordered_plan: Option<OrderedQueryPlanReceipt>,
    },
}

impl ConfiguredQueryRoute {
    /// Returns the immutable tail receipt and whether this query opens input.
    ///
    /// Keeping this alongside the prepared tail makes the planner's typed
    /// offsets and checked window observable before any PD/TiKV side effect.
    fn ordered_plan(&self) -> Option<(&OrderedQueryPlanReceipt, bool)> {
        match self {
            Self::LocalEmpty { ordered_plan, .. } => Some((ordered_plan, false)),
            Self::Join {
                ordered_plan: Some(ordered_plan),
                ..
            } => Some((ordered_plan, true)),
            Self::Join {
                ordered_plan: None, ..
            } => None,
        }
    }
}

/// Planner-resolved terminal metadata retained solely for the pre-I/O receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderedQueryPlanReceipt {
    mode: OrderedQueryPlanMode,
    limit: ConfiguredLimitWindow,
    /// The process admission cap. It is binding for TopN and retained on
    /// LIMIT-only receipts so every ordered-plan record has one policy value.
    capacity: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OrderedQueryPlanMode {
    Limit,
    TopN(Vec<OrderedQueryPlanKey>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrderedQueryPlanKey {
    full_schema_offset: usize,
    descending: bool,
}

impl OrderedQueryPlanReceipt {
    fn from_tail(tail: &ConfiguredOrderLimit, max_topn_rows: usize) -> Self {
        match tail {
            ConfiguredOrderLimit::Limit(limit) => Self {
                mode: OrderedQueryPlanMode::Limit,
                limit: *limit,
                capacity: max_topn_rows,
            },
            ConfiguredOrderLimit::TopN(spec) => Self {
                mode: OrderedQueryPlanMode::TopN(
                    spec.order_keys()
                        .iter()
                        .map(|key| OrderedQueryPlanKey {
                            full_schema_offset: key.full_offset(),
                            descending: key.direction().is_descending(),
                        })
                        .collect(),
                ),
                limit: spec.limit(),
                capacity: max_topn_rows,
            },
        }
    }
}

fn prepare_configured_query(
    sql: &str,
    catalog: &tidb_planner::read_only_scan::configured_catalog::ConfiguredCatalog,
    max_topn_rows: usize,
) -> Result<ConfiguredQueryRoute, SqlQueryError> {
    let plan = ConfiguredOrderedJoinPlan::lower(sql, catalog)
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    if plan.is_empty() {
        let ordered_plan = plan
            .order_limit()
            .map(|tail| OrderedQueryPlanReceipt::from_tail(tail, max_topn_rows))
            .expect("planner-known empty configured query has a typed LIMIT tail");
        return Ok(ConfiguredQueryRoute::LocalEmpty {
            plan: plan.metadata_join().clone(),
            ordered_plan,
        });
    }
    let join = plan
        .join()
        .expect("nonempty configured ordered plan retains its join")
        .clone();
    let ordered_plan = plan
        .order_limit()
        .map(|tail| OrderedQueryPlanReceipt::from_tail(tail, max_topn_rows));
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
    Ok(ConfiguredQueryRoute::Join {
        plan: join,
        tail,
        ordered_plan,
    })
}

fn emit_ordered_query_plan(
    connection_id: u64,
    query_id: u64,
    receipt: &OrderedQueryPlanReceipt,
    input_required: bool,
) {
    eprintln!(
        "{}",
        ordered_query_plan_json(connection_id, query_id, receipt, input_required)
    );
}

fn ordered_query_plan_json(
    connection_id: u64,
    query_id: u64,
    receipt: &OrderedQueryPlanReceipt,
    input_required: bool,
) -> String {
    let (mode, order_keys) = match &receipt.mode {
        OrderedQueryPlanMode::Limit => ("limit", String::new()),
        OrderedQueryPlanMode::TopN(keys) => (
            "topn",
            keys.iter()
                .map(|key| {
                    let direction = if key.descending { "desc" } else { "asc" };
                    format!(
                        "{{\"full_schema_offset\":{},\"direction\":\"{direction}\"}}",
                        key.full_schema_offset
                    )
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
    };
    format!(
        "{{\"event\":\"query_ordered_plan\",\"connection_id\":{connection_id},\"query_id\":{query_id},\"mode\":\"{mode}\",\"order_keys\":[{order_keys}],\"limit_offset\":{},\"limit_count\":{},\"limit_end_exclusive\":{},\"capacity\":{capacity},\"input_required\":{input_required}}}",
        receipt.limit.offset(),
        receipt.limit.count(),
        receipt.limit.end_exclusive(),
        capacity = receipt.capacity,
    )
}

struct MultiJoinResultSet {
    inner: ConfiguredInnerJoinRecordSet,
    evidence: [DirectUnaryTransportEvidenceHandle; 2],
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    table_ids: [i64; 2],
    emitted: bool,
    _completion: QueryCompletion,
}

impl MultiJoinResultSet {
    fn emit_evidence(&mut self) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        for (relation, handle) in self.evidence.iter().enumerate() {
            let evidence = handle.snapshot();
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
                "{{\"event\":\"query_multi_transport\",\"connection_id\":{},\"query_id\":{},\"authority_id\":{},\"session_id\":{},\"relation\":{relation},\"table_id\":{},\"located_region_ids\":[{located_regions}],\"dispatched_region_ids\":[{dispatched_regions}],\"batch_attempts\":{},\"unary_attempts\":{}}}",
                self.connection_id,
                self.query_id,
                self.authority_id,
                self.session_id,
                self.table_ids[relation],
                evidence.batch_attempts,
                evidence.unary_attempts,
            );
        }
    }
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

struct MultiJoinEvidence {
    evidence: [DirectUnaryTransportEvidenceHandle; 2],
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    table_ids: [i64; 2],
    emitted: bool,
}

/// Result-set wrapper for ordered, limited, and planner-known-empty joins.
///
/// The connection writer still owns the same finish/close lifecycle; this
/// wrapper changes only the terminal pull operator beneath it.
struct OrderedMultiJoinResultSet {
    inner: ConfiguredOrderedQueryRecordSet,
    evidence: Option<MultiJoinEvidence>,
    _completion: QueryCompletion,
}

impl OrderedMultiJoinResultSet {
    fn emit_evidence(&mut self) {
        let Some(evidence) = &mut self.evidence else {
            return;
        };
        if evidence.emitted {
            return;
        }
        evidence.emitted = true;
        for (relation, handle) in evidence.evidence.iter().enumerate() {
            let transport = handle.snapshot();
            let located_regions = transport
                .located_region_ids
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let dispatched_regions = transport
                .dispatched_region_ids
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            eprintln!(
                "{{\"event\":\"query_multi_transport\",\"connection_id\":{},\"query_id\":{},\"authority_id\":{},\"session_id\":{},\"relation\":{relation},\"table_id\":{},\"located_region_ids\":[{located_regions}],\"dispatched_region_ids\":[{dispatched_regions}],\"batch_attempts\":{},\"unary_attempts\":{}}}",
                evidence.connection_id,
                evidence.query_id,
                evidence.authority_id,
                evidence.session_id,
                evidence.table_ids[relation],
                transport.batch_attempts,
                transport.unary_attempts,
            );
        }
        if let Some(accounting) = self.inner.completed_evidence() {
            match accounting {
                ConfiguredOrderedQueryEvidence::TopN(topn) => eprintln!(
                    "{{\"event\":\"query_ordered_topn\",\"connection_id\":{},\"query_id\":{},\"capacity\":{},\"high_water_candidates\":{},\"rows_consumed\":{},\"rows_emitted\":{}}}",
                    evidence.connection_id,
                    evidence.query_id,
                    topn.capacity(),
                    topn.high_water_candidates(),
                    topn.rows_consumed(),
                    topn.rows_emitted(),
                ),
                ConfiguredOrderedQueryEvidence::Limit(limit) => eprintln!(
                    "{{\"event\":\"query_ordered_limit\",\"connection_id\":{},\"query_id\":{},\"rows_requested\":{},\"rows_skipped\":{},\"rows_emitted\":{},\"source_closed\":{}}}",
                    evidence.connection_id,
                    evidence.query_id,
                    limit.rows_requested(),
                    limit.rows_skipped(),
                    limit.rows_emitted(),
                    limit.source_closed(),
                ),
            }
        }
    }
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

struct MultiQuerySnapshot<'a> {
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    cluster_id: u64,
    snapshot_ts: Option<u64>,
    table_ids: [i64; 2],
    scans: [&'a tidb_planner::read_only_scan::ReadOnlyScanPlan; 2],
    equality_offsets: Option<(usize, usize)>,
    user: &'a str,
    host: &'a str,
}

fn emit_multi_query_snapshot(snapshot: MultiQuerySnapshot<'_>) {
    let snapshot_ts = snapshot
        .snapshot_ts
        .map_or_else(|| "null".to_owned(), |timestamp| timestamp.to_string());
    let relations = snapshot.scans.map(scan_evidence_json).join(",");
    let join_equality = snapshot.equality_offsets.map_or_else(
        || "null".to_owned(),
        |(left, right)| format!("{{\"left_full_offset\":{left},\"right_full_offset\":{right}}}"),
    );
    eprintln!(
        "{{\"event\":\"query_multi_snapshot\",\"connection_id\":{},\"query_id\":{},\"authority_id\":{},\"session_id\":{},\"cluster_id\":{},\"snapshot_ts\":{snapshot_ts},\"relations\":[{relations}],\"join_equality\":{join_equality},\"user\":{:?},\"host\":{:?}}}",
        snapshot.connection_id,
        snapshot.query_id,
        snapshot.authority_id,
        snapshot.session_id,
        snapshot.cluster_id,
        snapshot.user,
        snapshot.host,
    );
    debug_assert_eq!(
        snapshot.table_ids,
        snapshot
            .scans
            .map(tidb_planner::read_only_scan::ReadOnlyScanPlan::table_id)
    );
}

fn scan_evidence_json(scan: &tidb_planner::read_only_scan::ReadOnlyScanPlan) -> String {
    let predicate_count = scan
        .selection()
        .map_or(0, |selection| selection.conditions().len());
    let executor_kinds = if predicate_count == 0 {
        "[\"TableScan\"]"
    } else {
        "[\"TableScan\",\"Selection\"]"
    };
    let offsets = scan
        .projection_output_offsets()
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let ranges = scan
        .handle_ranges()
        .iter()
        .map(|range| {
            format!(
                "{{\"low\":{},\"high\":{},\"low_exclude\":false,\"high_exclude\":false}}",
                range.start(),
                range.end()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"table_id\":{},\"executor_kinds\":{executor_kinds},\"predicate_count\":{predicate_count},\"output_offsets\":[{offsets}],\"handle_range_count\":{},\"handle_ranges\":[{ranges}]}}",
        scan.table_id(),
        scan.handle_ranges().len(),
    )
}

fn emit_multi_query_transport_publication(
    connection_id: u64,
    query_id: u64,
    authority_id: u64,
    session_id: u64,
    relation: usize,
    table_id: i64,
    published: &PublishedDispatchEvidence,
) {
    let publication = &published.publication;
    let forwarded_host = publication
        .forwarded_host()
        .map_or_else(|| "null".to_owned(), |host| format!("{host:?}"));
    eprintln!(
        "{{\"event\":\"query_multi_transport_published\",\"connection_id\":{connection_id},\"query_id\":{query_id},\"authority_id\":{authority_id},\"session_id\":{session_id},\"relation\":{relation},\"table_id\":{table_id},\"region_id\":{},\"physical_address\":{:?},\"physical_channel_version\":{},\"stream_generation\":{},\"forwarded_host\":{forwarded_host}}}",
        published.region_id,
        publication.physical_address(),
        publication.physical_channel_version(),
        publication.batch_stream_generation(),
    );
}

/// Starts the existing listener/lifecycle against the two-relation factory.
pub fn run_configured_multi_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let users = Arc::new(
        ConfiguredUserStore::load(&config.auth_file).map_err(RunConfiguredNodeError::Auth)?,
    );
    let (factory, authority) =
        RealTiKvMultiSessionFactory::connect(&config).map_err(RunConfiguredNodeError::Engine)?;
    run_bound_multi_node(config, factory, authority, users, None)
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
    privilege_reloader: Option<PrivilegeReloader>,
) -> Result<(), RunConfiguredNodeError> {
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
        crate::real_tikv_node::emit_privilege_reload_stats(privilege_reloader.as_ref());
        result
    })
}

#[cfg(test)]
mod tests {
    use super::{
        configured_catalog, ordered_query_plan_json, prepare_configured_query,
        ConfiguredQueryRoute, NodeConfig,
    };

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

    #[test]
    fn ordered_plan_receipt_retains_topn_metadata_before_input_and_for_local_limit_zero() {
        let config = config();
        let catalog = configured_catalog(&config).expect("catalog is local configuration only");
        let topn = prepare_configured_query(
            "SELECT a.balance FROM accounts a JOIN orders o ON a.id=o.account_id \
             ORDER BY o.amount DESC, a.id ASC LIMIT 1,2",
            &catalog,
            7,
        )
        .expect("admitted TopN");
        let (receipt, input_required) = topn
            .ordered_plan()
            .expect("typed ordered route retains a receipt");
        assert!(input_required);
        assert_eq!(
            ordered_query_plan_json(9, 12, receipt, input_required),
            "{\"event\":\"query_ordered_plan\",\"connection_id\":9,\"query_id\":12,\"mode\":\"topn\",\"order_keys\":[{\"full_schema_offset\":4,\"direction\":\"desc\"},{\"full_schema_offset\":0,\"direction\":\"asc\"}],\"limit_offset\":1,\"limit_count\":2,\"limit_end_exclusive\":3,\"capacity\":7,\"input_required\":true}"
        );

        let local = prepare_configured_query(
            "SELECT a.balance FROM accounts a JOIN orders o ON a.id=o.account_id \
             ORDER BY o.amount LIMIT 0",
            &catalog,
            7,
        )
        .expect("LIMIT 0 is local");
        let (receipt, input_required) = local
            .ordered_plan()
            .expect("local LIMIT 0 retains its typed receipt");
        assert!(!input_required);
        assert_eq!(
            ordered_query_plan_json(9, 13, receipt, input_required),
            "{\"event\":\"query_ordered_plan\",\"connection_id\":9,\"query_id\":13,\"mode\":\"topn\",\"order_keys\":[{\"full_schema_offset\":4,\"direction\":\"asc\"}],\"limit_offset\":0,\"limit_count\":0,\"limit_end_exclusive\":0,\"capacity\":7,\"input_required\":false}"
        );
    }
}
