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
use tidb_exec::catalog_reload::ReloadedCatalog;
use tidb_exec::catalog_watch::{
    CatalogReloadError, CatalogReloadPass, CatalogReloadStats, CatalogReloader, SharedCatalog,
};
use tidb_exec::cluster_catalog::{configure_loaded_table, ClusterCatalog, LoadedTableRefusal};
use tidb_exec::distsql_recordset::DistSqlRecordSet;
use tidb_exec::multi_statement_transaction::{
    MultiStatementTransaction, StagedRowOverlay, TransactionStatementError,
};
use tidb_exec::real_tikv_catalog::{load_catalog_from_cluster, reload_catalog_from_cluster};
use tidb_exec::real_tikv_ddl::{
    commit_cluster_ddl, prepare_cluster_ddl, ClusterDdlReport, SchemaVersionNotifier,
};
use tidb_exec::real_tikv_dml::{
    commit_configured_write, prepare_configured_write, prepare_text_write,
};
use tidb_exec::real_tikv_read::{
    prepare_configured_point_read, PdTimestampSource, ProductionReadProcessAuthority,
    ProductionReadSessionFactory, ProductionReadTransport, ReadProcessShutdownError,
    ReadProcessShutdownStage, RealOptimisticTransactionOpener, RealTiKvQuery, RealTiKvReadSession,
    RealTiKvReadSessionOpener,
};
use tidb_pd_client::{
    EtcdClient, EtcdWatchStats, EtcdWatcher, DDL_GLOBAL_SCHEMA_VERSION_KEY, PRIVILEGE_UPDATE_KEY,
    SYSVAR_UPDATE_KEY,
};
use tidb_planner::aggregation_descriptor::AggregateKind;
use tidb_planner::prepared_dml::{ConfiguredPreparedWriteTemplate, PreparedBindValue};
use tidb_planner::read_only_scan::{
    configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredColumnKind, ConfiguredIndex,
    ConfiguredScalarType, ConfiguredTable, PreparedAggregate, PreparedAggregateKind,
    ReadOnlyScanPlan,
};
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_protocol::ColumnInfo;

use crate::aggregate_result_set::AggregateResultSetSource;
use crate::cluster_privileges::PrivilegeReloader;
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
use crate::transaction_overlay_result_set::{OverlayHandleSource, TransactionOverlayResultSet};

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
    /// Tables the cluster really has, that this node loaded and cannot serve.
    /// They are not hidden: a query naming one gets the exact reason back.
    table_refusals: Arc<Vec<LoadedTableRefusal>>,
    /// The cluster catalog this node keeps current, present only for a node
    /// that read its schema from the cluster rather than the command line.
    ///
    /// The catalog is republished whole by [`Self::reloader`]; a reader takes
    /// one `Arc` and keeps it, so no query ever sees a half-updated catalog.
    catalog: Option<Arc<SharedCatalog>>,
    /// The etcd watch that wakes `reloader` the moment any node publishes a
    /// new schema version. Declared before `reloader` so it is dropped first:
    /// a watch may not outlive the thread it nudges.
    watcher: Option<EtcdWatcher>,
    /// The lease-cadence reload thread, stopped and joined when this factory
    /// is dropped. `None` for a command-line-described node, which has no
    /// cluster catalog to follow.
    reloader: Option<CatalogReloader>,
    /// The etcd client this node announces its own catalog changes through.
    /// `None` when etcd could not be reached at startup, which leaves peers
    /// on their own lease tick rather than failing DDL.
    schema_notifier: Option<Arc<EtcdClient>>,
}

impl RealTiKvSessionFactory {
    /// Connects the unique process owner and derives its cloneable opener.
    pub fn connect(
        config: &NodeConfig,
    ) -> Result<(Self, ProductionReadProcessAuthority), SqlQueryError> {
        // A shape this node could never dispatch is rejected before PD, a
        // region cache, or a transport is started. Only a `--load-table` node
        // has to reach the cluster to know its own catalog.
        if config.read_tables.len() > 1
            || (config.load_tables.is_empty() && config.read_tables.len() != 1)
        {
            return Err(SqlQueryError::unknown(
                "multiple configured tables require the multi-relation dispatcher",
            ));
        }
        let mut refusals = Vec::new();
        let mut loaded = None;
        let authority = ProductionReadProcessAuthority::connect_with_catalog(
            config.pd_endpoints.clone(),
            PRODUCTION_CONTROL_PLANE_TIMEOUT,
            |transaction_opener| {
                served_table(config, transaction_opener, &mut refusals, &mut loaded)
            },
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let schema_notifier = connect_schema_notifier(config);
        let (catalog, watcher, reloader) = match loaded {
            Some(catalog) => {
                let (catalog, reloader) = spawn_catalog_reloader(
                    catalog,
                    authority.transaction_opener(),
                    config.schema_lease,
                )
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
                let watcher = spawn_schema_version_watch(config, &reloader);
                (Some(catalog), watcher, Some(reloader))
            }
            None => (None, None, None),
        };
        let factory = Self::from_authority_with_catalog(
            &authority,
            refusals,
            catalog,
            watcher,
            reloader,
            schema_notifier,
        );
        Ok((factory, authority))
    }

    /// Builds the cloneable single-table session opener over an authority that
    /// has already bootstrapped PD, RegionCache, and the TiKV transport, and
    /// already chose its served table. Both [`Self::connect`] and the
    /// catalog-loaded dispatcher (which picks the single-table surface only
    /// after reading the cluster's own catalog) call this with their own
    /// catalog/watcher/reloader, `None` when the served table came only from
    /// `--read-table` with no cluster catalog to follow.
    fn from_authority_with_catalog(
        authority: &ProductionReadProcessAuthority,
        table_refusals: Vec<LoadedTableRefusal>,
        catalog: Option<Arc<SharedCatalog>>,
        watcher: Option<EtcdWatcher>,
        reloader: Option<CatalogReloader>,
        schema_notifier: Option<Arc<EtcdClient>>,
    ) -> Self {
        Self {
            opener: authority.opener(),
            transaction_opener: authority.transaction_opener(),
            query_activity: Arc::new(QueryActivity::default()),
            read_authority_id: authority.read_authority_id(),
            table_refusals: Arc::new(table_refusals),
            catalog,
            watcher,
            reloader,
            schema_notifier,
        }
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

    /// The exact table this node serves, whether described on the command line
    /// or loaded from the cluster's own catalog.
    #[must_use]
    pub fn served_table(&self) -> &ConfiguredTable {
        self.opener.configured_table()
    }

    /// Loaded tables this node refuses to serve, with their exact reasons.
    #[must_use]
    pub fn table_refusals(&self) -> &[LoadedTableRefusal] {
        &self.table_refusals
    }

    /// The cluster schema version this node has followed to, `None` for a node
    /// whose table was described on the command line.
    #[must_use]
    pub fn followed_schema_version(&self) -> Option<i64> {
        self.catalog
            .as_ref()
            .map(|catalog| catalog.load().schema_version)
    }

    /// What the reload thread has done so far, `None` when there is no thread.
    #[must_use]
    pub fn catalog_reload_stats(&self) -> Option<CatalogReloadStats> {
        self.reloader.as_ref().map(CatalogReloader::stats)
    }

    /// What the schema-version watch has seen, `None` when there is no watch.
    #[must_use]
    pub fn schema_watch_stats(&self) -> Option<EtcdWatchStats> {
        self.watcher.as_ref().map(EtcdWatcher::stats)
    }
}

/// Connects the best-effort etcd client this node announces its DDL through.
///
/// A failure here is a warning, never a startup error: the announcement only
/// makes peers reload *sooner*, and Go itself carries on when the PUT fails
/// (`pkg/ddl/job_worker.go` logs "update latest schema version failed" and
/// continues outside MDL). Refusing to start because etcd was unreachable
/// would trade an availability property for a latency one.
pub(crate) fn connect_schema_notifier(config: &NodeConfig) -> Option<Arc<EtcdClient>> {
    match EtcdClient::connect(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
    ) {
        Ok(client) => Some(Arc::new(client)),
        Err(error) => {
            emit_warning("schema_version_notifier_unavailable", &error.to_string());
            None
        }
    }
}

/// Starts the etcd watch that wakes `reloader` as soon as any node publishes a
/// new schema version.
///
/// Like the notifier, a failure is a warning: the `lease/2` tick still keeps
/// this node current, only less promptly. That is exactly the relationship Go
/// has between its watch channel and its ticker in `Syncer.SyncLoop`.
pub(crate) fn spawn_schema_version_watch(
    config: &NodeConfig,
    reloader: &CatalogReloader,
) -> Option<EtcdWatcher> {
    let waker = reloader.waker();
    match EtcdWatcher::spawn(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        DDL_GLOBAL_SCHEMA_VERSION_KEY,
        move |event| {
            eprintln!(
                "{{\"event\":\"schema_version_watch_fired\",\"mod_revision\":{},\"value\":{}}}",
                event.mod_revision,
                json_string(&String::from_utf8_lossy(&event.value))
            );
            waker.nudge();
        },
    ) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            emit_warning("schema_version_watch_unavailable", &error.to_string());
            None
        }
    }
}

/// Starts the watch on the key TiDB announces account changes under, so this
/// node's privilege reloader runs within a round trip of a Go TiDB's `GRANT`
/// instead of waiting out its interval.
///
/// This is the same division [`spawn_schema_version_watch`] keeps, on the
/// other key: the watch is an optimisation, the reloader's own tick is the
/// guarantee, and a node whose etcd is unreachable simply loses the promptness.
pub(crate) fn spawn_privilege_watch(
    config: &NodeConfig,
    reloader: Option<&PrivilegeReloader>,
) -> Option<EtcdWatcher> {
    let waker = reloader?.waker();
    match EtcdWatcher::spawn(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        PRIVILEGE_UPDATE_KEY,
        move |event| {
            eprintln!(
                "{{\"event\":\"privilege_watch_fired\",\"mod_revision\":{},\"value\":{}}}",
                event.mod_revision,
                json_string(&String::from_utf8_lossy(&event.value))
            );
            waker.nudge();
        },
    ) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            emit_warning("privilege_watch_unavailable", &error.to_string());
            None
        }
    }
}

/// Starts the watch on the key TiDB announces `SET GLOBAL` changes under, so
/// this node's sysvar reloader runs within a round trip of a Go TiDB's
/// `SET GLOBAL` instead of waiting out its interval.
///
/// Mirrors [`spawn_privilege_watch`] exactly, on the sysvar key.
pub(crate) fn spawn_sysvar_watch(
    config: &NodeConfig,
    reloader: Option<&crate::cluster_sysvar_seam::SysvarReloader>,
) -> Option<EtcdWatcher> {
    let waker = reloader?.waker();
    match EtcdWatcher::spawn(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        SYSVAR_UPDATE_KEY,
        move |event| {
            eprintln!(
                "{{\"event\":\"sysvar_watch_fired\",\"mod_revision\":{}}}",
                event.mod_revision
            );
            waker.nudge();
        },
    ) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            emit_warning("sysvar_watch_unavailable", &error.to_string());
            None
        }
    }
}

fn emit_warning(event: &str, detail: &str) {
    eprintln!(
        "{{\"event\":\"{event}\",\"level\":\"warning\",\"error\":{}}}",
        json_string(detail)
    );
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"unprintable\"".to_owned())
}

/// Publishes the startup catalog and starts following the cluster's schema.
///
/// Go's domain reloads at `schemaLease / 2` so a node is never more than one
/// lease behind; the same halving is applied here to the configured lease.
///
/// A failed pass is not fatal and does not stop the thread: the previously
/// published catalog stays in force and the next tick tries again, which is
/// what Go's reload loop does with a failed `Reload`.
pub(crate) fn spawn_catalog_reloader(
    startup: ClusterCatalog,
    transaction_opener: RealOptimisticTransactionOpener,
    schema_lease: Duration,
) -> Result<(Arc<SharedCatalog>, CatalogReloader), CatalogReloadError> {
    let catalog = Arc::new(SharedCatalog::new(startup));
    let reloader = CatalogReloader::spawn(
        Arc::clone(&catalog),
        schema_lease / 2,
        Box::new(move |current| {
            match reload_catalog_from_cluster(
                &transaction_opener,
                PRODUCTION_CONTROL_PLANE_TIMEOUT,
                current,
            ) {
                Ok(ReloadedCatalog::Unchanged { .. }) => Ok(CatalogReloadPass::Unchanged),
                Ok(ReloadedCatalog::Diffs { catalog, .. }) => Ok(CatalogReloadPass::Diffs(catalog)),
                Ok(ReloadedCatalog::Full { catalog, reason }) => {
                    eprintln!(
                        "{{\"event\":\"catalog_full_reload\",\"schema_version\":{},\"reason\":\"{reason}\"}}",
                        catalog.schema_version
                    );
                    Ok(CatalogReloadPass::Full(catalog))
                }
                Err(error) => Err(error.to_string()),
            }
        }),
    )?;
    Ok((catalog, reloader))
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
            table_refusals: Arc::clone(&self.table_refusals),
            schema_notifier: self.schema_notifier.clone(),
            context,
            query_activity: Arc::clone(&self.query_activity),
            next_query_id: 1,
            transaction: SessionTransaction::new(),
            time_zone: RealTiKvSessionTimeZone::default(),
        })
    }
}

/// Worker-local server session around the executor session.
pub struct RealTiKvServerSession {
    inner: RealTiKvReadSession<ProductionReadTransport, PdTimestampSource>,
    transaction_opener: RealOptimisticTransactionOpener,
    /// Loaded-but-unservable tables, consulted when a statement names one.
    table_refusals: Arc<Vec<LoadedTableRefusal>>,
    /// The factory's etcd client, so a catalog change this session commits
    /// announces itself the way Go's DDL owner does.
    schema_notifier: Option<Arc<EtcdClient>>,
    context: SessionContext,
    query_activity: Arc<QueryActivity>,
    next_query_id: u64,
    /// The session's explicit-transaction state, pinning one read snapshot for
    /// the duration of a `BEGIN`/`COMMIT` transaction.
    transaction: SessionTransaction,
    /// This session's `time_zone`, as `(display name, seconds east of UTC)`.
    /// Threaded into every read's DAG request (`TimeZoneName`/`TimeZoneOffset`)
    /// and every write's `TIMESTAMP` literal-to-UTC conversion, so both sides
    /// of the round trip use the same session-visible zone Go's
    /// `SessionVars.Location()` would. `SET time_zone` updates it in place;
    /// a fresh session starts at `UTC`/`0`, matching Go's connection default.
    time_zone: RealTiKvSessionTimeZone,
}

/// A real-TiKV session's `time_zone` value: a display name for the DAG
/// request's `TimeZoneName` field, and the same zone's offset in seconds east
/// of UTC. Only fixed offsets and the bare `UTC`/`SYSTEM` spellings are
/// supported — this node carries no IANA timezone database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RealTiKvSessionTimeZone {
    pub(crate) name: String,
    pub(crate) offset_secs: i32,
}

impl Default for RealTiKvSessionTimeZone {
    fn default() -> Self {
        Self {
            name: "UTC".to_owned(),
            offset_secs: 0,
        }
    }
}

impl RealTiKvSessionTimeZone {
    /// Parses `SET time_zone = <value>`'s source-observable subset: `SYSTEM`,
    /// `UTC`, and fixed `+HH:MM`/`-HH:MM` offsets. Named IANA zones are
    /// refused rather than silently approximated, matching this node's
    /// generally-UTC-only temporal seed.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("SYSTEM") || value.eq_ignore_ascii_case("UTC") {
            return Some(Self {
                name: value.to_owned(),
                offset_secs: 0,
            });
        }
        let offset_secs = parse_fixed_tz_offset(value)?;
        Some(Self {
            name: value.to_owned(),
            offset_secs,
        })
    }
}

/// Parses a fixed UTC offset (`+HH:MM`/`-HH:MM`, e.g. `'+05:00'`, `'-08:00'`)
/// into whole seconds east of UTC.
fn parse_fixed_tz_offset(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hh: i32 = s.get(1..3)?.parse().ok()?;
    let mm: i32 = s.get(4..6)?.parse().ok()?;
    Some(sign * (hh * 3600 + mm * 60))
}

/// Recognizes `SET [SESSION] time_zone = <value>` / `SET @@time_zone = <value>`
/// (case-insensitively; `GLOBAL` is left unmatched, so it falls through to this
/// node's ordinary unsupported-statement handling rather than silently
/// changing session state) and returns the unquoted, un-lowercased value text.
pub(crate) fn parse_set_time_zone(sql: &str) -> Option<&str> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let lower = trimmed.to_ascii_lowercase();
    let mut rest = lower.strip_prefix("set")?.trim_start();
    rest = rest.strip_prefix("session").map_or(rest, str::trim_start);
    rest = rest
        .strip_prefix("@@session.")
        .or_else(|| rest.strip_prefix("@@"))
        .map_or(rest, str::trim_start);
    let rest = rest.strip_prefix("time_zone")?.trim_start();
    let rest = rest.strip_prefix('=')?;
    let value_lower = rest.trim();
    if value_lower.is_empty() {
        return None;
    }
    // `lower` is ASCII-only wherever it overlaps `trimmed`'s SQL keywords, so
    // the byte offset of the value's start is identical in both strings;
    // slicing `trimmed` at that offset recovers the value's original case.
    let start = trimmed.len() - value_lower.len();
    let value = trimmed[start..].trim();
    let unquoted = if value.len() >= 2
        && ((value.starts_with('\'') && value.ends_with('\''))
            || (value.starts_with('"') && value.ends_with('"')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    };
    Some(unquoted)
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
    /// Opens this session's explicit transaction on first use, if one is open.
    fn transaction_for_statement(
        &mut self,
    ) -> Result<Option<&mut MultiStatementTransaction>, SqlQueryError> {
        // Cloned before the borrow because opening the transaction needs the
        // session's own opener and table while the transaction state is
        // mutably borrowed.
        let opener = self.transaction_opener.clone();
        let table = self.inner.configured_table().clone();
        self.transaction
            .opened_or_begin(|mode| {
                MultiStatementTransaction::begin(
                    &opener,
                    mode,
                    crate::session_transaction::session_fair_locking(),
                    crate::session_transaction::session_commit_protocol(),
                    table,
                    PRODUCTION_CONTROL_PLANE_TIMEOUT,
                )
            })
            .map_err(|error| SqlQueryError::unknown(error.to_string()))
    }

    /// Reports one statement failure, ending the transaction when the failure
    /// ended it.
    ///
    /// A pessimistic lock failure (3572 / 1205 / 1213) costs only the statement:
    /// the transaction stays open and the client may run the next statement in
    /// it. Anything that ended the transaction leaves the session in autocommit
    /// rather than pointing at a coordinator that has already terminated.
    fn report(&mut self, error: &TransactionStatementError) -> SqlQueryError {
        if !error.keeps_transaction_open() {
            self.transaction.abandon();
        }
        Self::transaction_error(self, error)
    }

    /// Renders a transaction failure for the client, leaving the session state
    /// alone — used where the transaction is already known to be over, so that
    /// `COMMIT`'s own failure still returns the session to autocommit.
    fn transaction_error(&self, error: &TransactionStatementError) -> SqlQueryError {
        let sql_error = error.sql_error();
        SqlQueryError::new(sql_error.code, sql_error.state, sql_error.message.clone())
    }

    /// Runs one already-lowered read, inside or outside an explicit transaction.
    ///
    /// Inside a transaction the read runs at the transaction's own `start_ts`,
    /// so every statement in it observes one consistent snapshot; a
    /// `SELECT ... FOR UPDATE` locks its rows first; and a transaction that has
    /// staged writes reads them back through the union-scan overlay. Outside a
    /// transaction each read takes its own fresh snapshot exactly as before.
    fn execute_read<'a>(
        &'a mut self,
        plan: ReadOnlyScanPlan,
        cancellation: Arc<CancelHandle>,
        query_id: u64,
        cancellation_lease: QueryCancellationLease,
        query_activity: QueryActivityLease,
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        // A contradiction returns no rows at any snapshot and locks nothing, so
        // it never opens a transaction or touches storage.
        if plan.is_contradiction() {
            let query = self
                .inner
                .execute_lowered_plan_with_cancellation(plan, cancellation)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            return self.observe(query, query_id, cancellation_lease, query_activity);
        }
        let lock = plan.lock();
        let handles = point_handles(&plan);
        let overlay = if self.transaction_for_statement()?.is_some() {
            if let Some(lock) = lock {
                // Lowering admits `FOR UPDATE` only over point handles, so this
                // key set is the complete one the statement locks.
                let locked = self
                    .transaction
                    .opened_mut()
                    .expect("the statement's transaction was just opened")
                    .lock_handles(&handles, lock.wait);
                if let Err(error) = locked {
                    return Err(self.report(&error));
                }
            }
            let transaction = self
                .transaction
                .opened()
                .expect("the statement's transaction was just opened");
            resolve_overlay(transaction, &plan, &handles)?
        } else {
            if lock.is_some() {
                return Err(SqlQueryError::unknown(
                    "a locking read requires an explicit transaction; an autocommit \
                     statement releases its locks before the client can use them",
                ));
            }
            None
        };
        let snapshot = self
            .transaction
            .opened()
            .map(MultiStatementTransaction::start_ts);
        let query = match snapshot {
            Some(start_ts) => self
                .inner
                .execute_plan_at_snapshot(plan, start_ts, cancellation),
            None => self
                .inner
                .execute_lowered_plan_with_cancellation(plan, cancellation),
        }
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let result = self.observe(query, query_id, cancellation_lease, query_activity)?;
        let Some((rows, handles)) = overlay else {
            return Ok(result);
        };
        Ok(QueryResult::new(Box::new(
            TransactionOverlayResultSet::new(result.into_source(), rows, handles),
        )))
    }

    fn observe<'a>(
        &'a mut self,
        query: RealTiKvQuery,
        query_id: u64,
        cancellation_lease: QueryCancellationLease,
        query_activity: QueryActivityLease,
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        let cluster_id = self.inner.cluster_id();
        let evidence = self.inner.transport_evidence_handle();
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

    /// Binds one write template and applies it, whichever protocol carried it.
    ///
    /// Inside an explicit transaction the write is buffered into it and
    /// published only by COMMIT; outside one it commits its own
    /// single-statement transaction. A text statement supplies no bind values
    /// because its template already carries them, so both protocols reach
    /// storage through this one seam.
    fn commit_bound_write(
        &mut self,
        template: &ConfiguredPreparedWriteTemplate,
        parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        let bound = template
            .bind(parameters)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let tz_offset_secs = self.time_zone.offset_secs;
        let buffered = self
            .transaction_for_statement()?
            .map(|transaction| transaction.execute_write(&bound, tz_offset_secs));
        let report = match buffered {
            Some(Ok(report)) => report,
            Some(Err(error)) => return Err(self.report(&error)),
            None => commit_configured_write(
                &self.transaction_opener,
                &bound,
                PRODUCTION_CONTROL_PLANE_TIMEOUT,
                tz_offset_secs,
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?,
        };
        Ok(WriteOutcome {
            affected_rows: report.affected_rows,
            // This node has no auto-increment allocator.
            last_insert_id: 0,
        })
    }

    /// Allocates this session's next query identity and its activity lease.
    fn begin_query(&mut self) -> Result<(u64, QueryActivityLease), SqlQueryError> {
        let query_id = self.next_query_id;
        self.next_query_id = self
            .next_query_id
            .checked_add(1)
            .ok_or_else(|| SqlQueryError::unknown("query identity space exhausted"))?;
        let activity = self
            .query_activity
            .begin(self.context.connection_id, query_id);
        Ok((query_id, activity))
    }
}

/// The exact clustered handles a plan's point ranges name, empty when any range
/// covers more than one handle.
fn point_handles(plan: &ReadOnlyScanPlan) -> Vec<i64> {
    let ranges = plan.handle_ranges();
    if ranges.iter().all(|range| range.start() == range.end()) {
        ranges.iter().map(|range| range.start()).collect()
    } else {
        Vec::new()
    }
}

/// Builds the read-your-own-writes overlay a read inside a transaction needs,
/// or `None` when the transaction has staged nothing this read could observe.
///
/// A read whose staged rows exist but whose rows cannot be identified — because
/// neither the clustered key nor a single point handle pins them down — or whose
/// predicate TiKV evaluates for us and this node cannot evaluate over a staged
/// row, is refused. Returning the snapshot's pre-transaction rows instead would
/// silently break read-your-own-writes.
fn resolve_overlay(
    transaction: &MultiStatementTransaction,
    plan: &ReadOnlyScanPlan,
    point_handles: &[i64],
) -> Result<Option<(StagedRowOverlay, OverlayHandleSource)>, SqlQueryError> {
    if !transaction.has_staged_writes() {
        return Ok(None);
    }
    let rows = transaction
        .read_overlay(plan.projected_columns(), plan.handle_ranges())
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    if rows.is_empty() {
        return Ok(None);
    }
    if plan.selection().is_some() {
        return Err(SqlQueryError::unknown(
            "a read inside a transaction that wrote rows cannot apply a pushed-down \
             predicate to its own uncommitted rows yet",
        ));
    }
    let projected_key = plan
        .projected_columns()
        .iter()
        .position(|column| column.kind() == ConfiguredColumnKind::ClusteredPrimaryKey);
    let handles = match (projected_key, point_handles) {
        (Some(offset), _) => OverlayHandleSource::ProjectedAt(offset),
        (None, [handle]) => OverlayHandleSource::SinglePoint(*handle),
        (None, _) => {
            return Err(SqlQueryError::unknown(
                "a read inside a transaction that wrote rows must project the clustered \
                 primary key, or name exactly one row, so its own writes can be matched",
            ));
        }
    };
    Ok(Some((rows, handles)))
}

/// Resolves every table this node serves: every command-line `--read-table`
/// in order, followed by every `--load-table` whose cluster-stored schema
/// this node can decode, in command-line order.
///
/// A loaded table this node cannot decode is not silently dropped: it is
/// recorded in `refusals` so a statement naming it is answered with the exact
/// column and type that blocked it. The cluster catalog is read once, at one
/// snapshot, even when multiple tables are loaded from it.
pub(crate) fn served_tables(
    config: &NodeConfig,
    transaction_opener: &RealOptimisticTransactionOpener,
    refusals: &mut Vec<LoadedTableRefusal>,
    loaded: &mut Option<ClusterCatalog>,
) -> Result<Vec<ConfiguredTable>, String> {
    let mut tables: Vec<ConfiguredTable> =
        config.read_tables.iter().map(configured_table).collect();
    if !config.load_tables.is_empty() {
        let catalog =
            load_catalog_from_cluster(transaction_opener, PRODUCTION_CONTROL_PLANE_TIMEOUT)
                .map_err(|error| error.to_string())?;
        for wanted in &config.load_tables {
            let Some((database, stored)) = catalog.find_table(&wanted.database, &wanted.table)
            else {
                return Err(format!(
                    "table {}.{} is not in the cluster catalog at schema version {}",
                    wanted.database, wanted.table, catalog.schema_version
                ));
            };
            match configure_loaded_table(database.name.original(), stored) {
                Ok(table) => tables.push(table),
                Err(refusal) => refusals.push(refusal),
            }
        }
        *loaded = Some(catalog);
    }
    Ok(tables)
}

/// Chooses the one table this node serves, reading any `--load-table` schema
/// from the cluster's own catalog.
///
/// Kept for the single-table-only connect path: a shape that resolves to more
/// than one servable table is rejected here with the same message the
/// pre-flight guard in [`RealTiKvSessionFactory::connect`] gives for a
/// command-line shape it can already see is wrong.
fn served_table(
    config: &NodeConfig,
    transaction_opener: &RealOptimisticTransactionOpener,
    refusals: &mut Vec<LoadedTableRefusal>,
    loaded: &mut Option<ClusterCatalog>,
) -> Result<ConfiguredTable, String> {
    let mut tables = served_tables(config, transaction_opener, refusals, loaded)?;
    match tables.len() {
        1 => Ok(tables.remove(0)),
        0 => Err(match refusals.first() {
            Some(refusal) => refusal.to_string(),
            None => "no table is configured or loaded".to_owned(),
        }),
        _ => Err("multiple configured tables require the multi-relation dispatcher".to_owned()),
    }
}

/// Runs one statement as a catalog change, if that is what it is.
///
/// `Ok(None)` means the statement is not a DDL this node owns, so the caller
/// continues down its ordinary write/query path — including when the text does
/// not parse at all, which the query path reports with its own message.
///
/// A catalog change is never part of a client's explicit transaction: it opens,
/// publishes, and commits its own, exactly as a real TiDB's DDL job does
/// outside the user's transaction. `affected_rows` is zero, which is what MySQL
/// answers for DDL.
///
/// MySQL and TiDB answer a DDL inside an open transaction by implicitly
/// committing that transaction first. This node does not implement that
/// implicit commit, so `in_transaction` makes it refuse instead: silently
/// committing the catalog change while leaving the client's own transaction
/// open would give a durability answer neither MySQL nor this node means.
pub(crate) fn execute_cluster_ddl(
    opener: &RealOptimisticTransactionOpener,
    sql: &str,
    default_schema: &str,
    in_transaction: bool,
    schema_notifier: Option<&Arc<EtcdClient>>,
) -> Result<Option<WriteOutcome>, SqlQueryError> {
    let Some(statement) = prepare_cluster_ddl(sql, default_schema)
        .map_err(|error| SqlQueryError::unknown(error.reason))?
    else {
        return Ok(None);
    };
    if in_transaction {
        return Err(SqlQueryError::unknown(
            "a catalog change inside an explicit transaction would implicitly commit it, \
             which this node does not implement; COMMIT or ROLLBACK first",
        ));
    }
    let notifier = schema_notifier.map(|client| Arc::as_ref(client) as &dyn SchemaVersionNotifier);
    let report = commit_cluster_ddl(
        opener,
        &statement,
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        notifier,
    )
    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    match report {
        ClusterDdlReport::Applied {
            schema_version,
            created_id,
        } => eprintln!(
            "{{\"event\":\"catalog_change\",\"outcome\":\"applied\",\"schema_version\":{schema_version},\"created_id\":{}}}",
            created_id.map_or_else(|| "null".to_owned(), |id| id.to_string())
        ),
        ClusterDdlReport::AlreadySatisfied { detail } => eprintln!(
            "{{\"event\":\"catalog_change\",\"outcome\":\"already_satisfied\",\"detail\":{detail:?}}}"
        ),
    }
    Ok(Some(WriteOutcome {
        affected_rows: 0,
        last_insert_id: 0,
    }))
}

/// Reports a statement that named a loaded-but-unservable table with the exact
/// reason, instead of the generic unknown-table failure.
pub(crate) fn refusal_aware_error(
    refusals: &[LoadedTableRefusal],
    message: String,
) -> SqlQueryError {
    let lowered = message.to_lowercase();
    for refusal in refusals {
        if lowered.contains(&refusal.name.to_lowercase()) {
            return SqlQueryError::unknown(refusal.to_string());
        }
        if let Some((_, table)) = refusal.name.split_once('.') {
            if lowered.contains(&format!("table: {}", table.to_lowercase())) {
                return SqlQueryError::unknown(refusal.to_string());
            }
        }
    }
    SqlQueryError::unknown(message)
}

impl QuerySession for RealTiKvServerSession {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        let (query_id, query_activity) = self.begin_query()?;
        let cancellation = Arc::new(CancelHandle::default());
        let cancellation_lease = self.context.cancellation.install(cancellation.clone());
        // Text and prepared reads share one lowering and one execution seam, so
        // a transaction's snapshot, locks, and read-your-own-writes overlay
        // apply identically to both.
        let plan = ReadOnlyScanPlan::lower(sql, self.inner.configured_table())
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.to_string()))?;
        self.execute_read(
            plan,
            cancellation,
            query_id,
            cancellation_lease,
            query_activity,
        )
    }

    fn prepare_point_read(&mut self, sql: &str) -> Result<PreparedPointRead, SqlQueryError> {
        let catalog = ConfiguredCatalog::new([self.inner.configured_table().clone()])
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let template = prepare_configured_point_read(sql, &catalog)
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.to_string()))?;
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
        let (query_id, query_activity) = self.begin_query()?;
        let cancellation = Arc::new(CancelHandle::default());
        let cancellation_lease = self.context.cancellation.install(cancellation.clone());
        let result = self.execute_read(
            plan,
            cancellation,
            query_id,
            cancellation_lease,
            query_activity,
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
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.to_string()))?;
        Ok(PreparedWrite::new(template))
    }

    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        // `SET time_zone` updates this session's own zone rather than reaching
        // storage at all: every read's DAG request and every write's
        // `TIMESTAMP` literal conversion consult it from here on.
        if let Some(value) = parse_set_time_zone(sql) {
            let parsed = RealTiKvSessionTimeZone::parse(value.trim()).ok_or_else(|| {
                SqlQueryError::unknown(format!("unsupported SET time_zone value: {value}"))
            })?;
            self.inner
                .set_time_zone(parsed.name.clone(), parsed.offset_secs);
            self.time_zone = parsed;
            return Ok(Some(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        // A catalog change is answered before the DML lowering, because it is
        // not a write against the served table at all: it commits its own
        // transaction against the `m` meta namespace.
        if let Some(outcome) = execute_cluster_ddl(
            &self.transaction_opener,
            sql,
            self.inner.configured_table().schema(),
            self.transaction.is_active(),
            self.schema_notifier.as_ref(),
        )? {
            return Ok(Some(outcome));
        }
        let catalog = ConfiguredCatalog::new([self.inner.configured_table().clone()])
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let template = prepare_text_write(sql, &catalog)
            .map_err(|error| refusal_aware_error(&self.table_refusals, error.to_string()))?;
        // Not a write statement at all: the caller runs it as an ordinary query.
        let Some(template) = template else {
            return Ok(None);
        };
        // A text statement carries its own values, so binding supplies none; the
        // bound write, and everything downstream of it, is the prepared path's.
        self.commit_bound_write(&template, &[]).map(Some)
    }

    fn execute_prepared_write(
        &mut self,
        statement: &PreparedWrite,
        parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        self.commit_bound_write(statement.template(), parameters)
    }

    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        match classify_transaction_control(sql) {
            None => Ok(None),
            Some(TransactionControl::Begin { mode }) => {
                // A BEGIN that implicitly commits a previous transaction reports
                // that commit's failure: the client must not be told the new
                // transaction started while the old one's writes were lost.
                self.transaction
                    .begin(mode)
                    .map_err(|error| self.transaction_error(&error))?;
                Ok(Some(true))
            }
            Some(control @ (TransactionControl::Commit | TransactionControl::Rollback)) => {
                let commit = control == TransactionControl::Commit;
                self.transaction
                    .end(commit)
                    .map_err(|error| self.transaction_error(&error))?;
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
    ConfiguredTable::new(&table.database, &table.table, table.table_id, columns)
        .with_indexes(indexes)
}

/// Converts the one canonical startup list into the planner's immutable
/// catalog, preserving source order and shared identity validation.
pub(crate) fn configured_catalog(config: &NodeConfig) -> Result<ConfiguredCatalog, SqlQueryError> {
    ConfiguredCatalog::new(config.read_tables.iter().map(configured_table))
        .map_err(|error| SqlQueryError::unknown(error.to_string()))
}

/// Renders the served table for the readiness event.
///
/// The served table is the truth published here, because with `--load-table`
/// the schema comes from the cluster rather than from the command line, and it
/// can carry scalar types (`BIGINT UNSIGNED`, `DOUBLE`) the command-line
/// descriptor grammar cannot even express.
pub(crate) fn served_table_descriptor(table: &ConfiguredTable) -> String {
    let columns = table
        .columns()
        .iter()
        .map(|column| {
            let kind = match (column.kind(), column.scalar_type()) {
                (ConfiguredColumnKind::ClusteredPrimaryKey, _) => "clustered-pk".to_owned(),
                (_, ConfiguredScalarType::BigInt) => "stored-not-null".to_owned(),
                (_, ConfiguredScalarType::Int) => "stored-int-not-null".to_owned(),
                (_, ConfiguredScalarType::UnsignedBigInt) => {
                    "stored-unsigned-bigint-not-null".to_owned()
                }
                (_, ConfiguredScalarType::Double) => "stored-double-not-null".to_owned(),
                (_, ConfiguredScalarType::Char { max_length }) => {
                    format!("stored-char-not-null:{max_length}")
                }
                (_, ConfiguredScalarType::Varchar { max_length, binary }) => {
                    format!("stored-varchar-not-null:{max_length}:{binary}")
                }
                (_, ConfiguredScalarType::Decimal { precision, scale }) => {
                    format!("stored-decimal-not-null:{precision}:{scale}")
                }
                (_, ConfiguredScalarType::Date) => "stored-date-not-null".to_owned(),
                (_, ConfiguredScalarType::Datetime { fsp }) => {
                    format!("stored-datetime-not-null:{fsp}")
                }
                (_, ConfiguredScalarType::Timestamp { fsp }) => {
                    format!("stored-timestamp-not-null:{fsp}")
                }
                (_, ConfiguredScalarType::Duration { fsp }) => {
                    format!("stored-duration-not-null:{fsp}")
                }
            };
            format!("{}:{}:{}", column.name(), column.id(), kind)
        })
        .collect::<Vec<_>>();
    format!(
        "{{\"database\":{:?},\"table\":{:?},\"table_id\":{},\"columns\":{:?}}}",
        table.schema(),
        table.table(),
        table.table_id(),
        columns
    )
}

/// Starts the bounded concurrent production Rust SQL node.
pub fn run_configured_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let users = Arc::new(
        ConfiguredUserStore::load(&config.auth_file).map_err(RunConfiguredNodeError::Auth)?,
    );
    let (factory, authority) =
        RealTiKvSessionFactory::connect(&config).map_err(RunConfiguredNodeError::Engine)?;
    run_bound_node(config, factory, authority, users, None)
}

/// Starts the same listener/lifecycle over an already-connected factory and
/// process authority.
///
/// Used both by [`run_configured_node`] (which connects for itself) and by the
/// catalog-loaded dispatcher in the crate root, which must connect once to
/// read the cluster catalog before it can even know this is the single-table
/// surface it should serve.
pub(crate) fn run_bound_node(
    config: NodeConfig,
    factory: RealTiKvSessionFactory,
    authority: ProductionReadProcessAuthority,
    users: Arc<ConfiguredUserStore>,
    privilege_reloader: Option<PrivilegeReloader>,
) -> Result<(), RunConfiguredNodeError> {
    let factory = Arc::new(factory);
    let cluster_id = factory.cluster_id();
    let authority_id = factory.authority_id();
    let read_authority_id = factory.read_authority_id();
    let served_table = factory.served_table().clone();
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
        let table_descriptors = served_table_descriptor(&served_table);
        eprintln!(
            "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"pd_endpoints\":{},\"cluster_id\":{cluster_id},\"authority_id\":{authority_id},\"read_authority_id\":{read_authority_id},\"tables\":[{table_descriptors}],\"refused_tables\":[{refused_descriptors}],\"max_connections\":{},\"account_count\":{},\"shutdown_grace_ms\":{shutdown_grace_ms}}}",
            config.pd_endpoints.len(),
            config.max_connections,
            users.len(),
        );
        let result = node.run().map_err(RunConfiguredNodeError::Node);
        emit_privilege_reload_stats(privilege_reloader.as_ref());
        result
    })
}

/// Logs what the privilege reload thread did over the node's whole run,
/// right before it is dropped (and stopped) along with the node.
pub(crate) fn emit_privilege_reload_stats(reloader: Option<&PrivilegeReloader>) {
    if let Some(reloader) = reloader {
        let stats = reloader.stats();
        eprintln!(
            "{{\"event\":\"privilege_reload_stats\",\"passes\":{},\"reloads\":{},\"failures\":{}}}",
            stats.passes, stats.reloads, stats.failures
        );
    }
}

/// Builds this node's live account table from whichever source the
/// command line named, plus the live-refresh reload thread when
/// `--load-privileges` is set.
///
/// `--load-privileges` reads the cluster's own `mysql.*` through the
/// already-connected authority, so the accounts this node admits are exactly
/// the ones a Go TiDB wrote there. It is refused against a keyspace no TiDB
/// ever bootstrapped: an empty account table would accept nobody, and
/// reporting that as a successful load would hide the real cause.
///
/// The returned [`PrivilegeReloader`], when present, must be kept alive for
/// the node's whole run: dropping it stops the reload thread, so a caller
/// that let it go out of scope early would silently fall back to the
/// one-shot startup snapshot.
pub(crate) fn node_accounts(
    config: &NodeConfig,
    authority: &ProductionReadProcessAuthority,
) -> Result<(Arc<ConfiguredUserStore>, Option<PrivilegeReloader>), RunConfiguredNodeError> {
    if !config.load_privileges {
        return Ok((
            Arc::new(
                ConfiguredUserStore::load(&config.auth_file)
                    .map_err(RunConfiguredNodeError::Auth)?,
            ),
            None,
        ));
    }
    let accounts = tidb_exec::real_tikv_privileges::load_accounts_from_cluster(
        &authority.transaction_opener(),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    if !accounts.bootstrap.already_bootstrapped() {
        return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            format!(
                "--load-privileges needs a cluster whose mysql.* a TiDB bootstrapped, but {}",
                accounts.bootstrap
            ),
        )));
    }
    let loaded = crate::cluster_privileges::registry_from_cluster(&accounts.privileges);
    let skipped = loaded
        .skipped
        .iter()
        .map(|skip| {
            format!(
                "{{\"source\":{:?},\"account\":{:?},\"privilege\":{:?}}}",
                skip.source, skip.account, skip.privilege
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "{{\"event\":\"privileges_loaded\",\"bootstrap\":{:?},\"accounts\":{},\"db_grants\":{},\"table_grants\":{},\"column_grants\":{},\"dynamic_grants\":{},\"role_edges\":{},\"sysvars\":{},\"skipped\":[{skipped}]}}",
        accounts.bootstrap.to_string(),
        loaded.account_count,
        accounts.privileges.db_grants.len(),
        accounts.privileges.table_grants.len(),
        accounts.privileges.column_grants.len(),
        accounts.privileges.dynamic_grants.len(),
        accounts.privileges.role_edges.len(),
        accounts.sysvars.len(),
    );
    let users = ConfiguredUserStore::from_accounts(loaded.registry);
    // `--load-privileges` implies loading `mysql.global_variables` too,
    // rather than adding a second flag for it: both read the same
    // already-bootstrapped `mysql.*` at the same snapshot
    // (`load_accounts_from_cluster` took both in one transaction), and a
    // node that trusts the cluster for its accounts has no principled reason
    // to keep trusting only its own sysvar defaults. `load_from_cluster`
    // writes straight into the store's shared `GlobalSysvars`, so every
    // session this node opens sees the loaded overrides as their defaults --
    // this is a one-shot load, not a write-through: a `SET GLOBAL` a Go node
    // runs after this point is invisible here until this node restarts, and a
    // `SET GLOBAL` run through this node's own wide session updates only this
    // in-memory table, not `mysql.global_variables` itself.
    users.global_vars().load_from_cluster(accounts.sysvars);
    let users = Arc::new(users);
    // Ticks at the same `schema_lease / 2` cadence as the catalog reloader,
    // so a node is never more than one lease behind the cluster's accounts
    // either. A failed spawn (only a zero lease can cause it, and the parser
    // already rejects that) is reported rather than silently leaving the
    // node on its startup snapshot forever.
    let reloader = PrivilegeReloader::spawn(
        users.accounts(),
        authority.transaction_opener(),
        config.schema_lease / 2,
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    Ok((users, Some(reloader)))
}

/// Every servable-table outcome a `--load-table` node can settle on, once its
/// one cluster-catalog snapshot is in hand.
pub(crate) enum LoadedCatalogAuthority {
    /// Exactly one servable table: the same single-reader surface a
    /// command-line one-table node serves.
    Single(Box<RealTiKvSessionFactory>, ProductionReadProcessAuthority),
    /// Exactly two servable tables: the same connected-join surface a
    /// command-line two-table node serves.
    Multi(
        Box<crate::real_tikv_multi_node::RealTiKvMultiSessionFactory>,
        ProductionReadProcessAuthority,
    ),
}

/// Connects the one process authority for a `--load-table` node, then routes
/// on however many of its command-line and loaded tables turned out to be
/// servable: one table keeps the single-reader surface, two reach the
/// connected-join surface, and any other count is a startup error.
///
/// The route cannot be decided before this connects, because a loaded table's
/// schema — and therefore whether it is servable at all — is only known after
/// reading the cluster's own catalog, which this same authority connection
/// performs.
pub(crate) fn connect_loaded_catalog_authority(
    config: &NodeConfig,
) -> Result<LoadedCatalogAuthority, SqlQueryError> {
    let schema_notifier = connect_schema_notifier(config);
    let mut refusals = Vec::new();
    let mut resolved: Option<Vec<ConfiguredTable>> = None;
    // Captured out of the closure so the `Single` route below can hand the
    // one loaded snapshot to `spawn_catalog_reloader`, the same way
    // `RealTiKvSessionFactory::connect` does. The `Multi` route still has no
    // `SharedCatalog`/reloader concept of its own (its dispatcher is built
    // over a static `ConfiguredCatalog`, not `ClusterCatalog`), so for that
    // route this snapshot is still read once and dropped -- documented, not
    // silently gapped.
    let mut loaded = None;
    let authority = ProductionReadProcessAuthority::connect_with_catalog(
        config.pd_endpoints.clone(),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        |transaction_opener| {
            let tables = served_tables(config, transaction_opener, &mut refusals, &mut loaded)?;
            let primary = tables
                .first()
                .cloned()
                .ok_or_else(|| match refusals.first() {
                    Some(refusal) => refusal.to_string(),
                    None => "no table is configured or loaded".to_owned(),
                })?;
            resolved = Some(tables);
            Ok(primary)
        },
    )
    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    let tables = resolved.expect("choose_table ran exactly once and set resolved");
    match <[ConfiguredTable; 2]>::try_from(tables) {
        Ok([left, right]) => Ok(LoadedCatalogAuthority::Multi(
            Box::new(
                crate::real_tikv_multi_node::RealTiKvMultiSessionFactory::from_authority(
                    &authority,
                    [left, right],
                    config.max_topn_rows,
                    refusals,
                    schema_notifier,
                ),
            ),
            authority,
        )),
        Err(tables) if tables.len() == 1 => {
            // Threads the loaded snapshot into a `SharedCatalog` + reloader
            // exactly as `RealTiKvSessionFactory::connect` does, so a
            // `--load-table` node with one servable table follows the
            // cluster's schema instead of running forever on its startup
            // read. `loaded` is `None` only when every table came from
            // `--read-table` (no `--load-table` was given at all).
            let (catalog, watcher, reloader) = match loaded {
                Some(catalog) => {
                    let (catalog, reloader) = spawn_catalog_reloader(
                        catalog,
                        authority.transaction_opener(),
                        config.schema_lease,
                    )
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
                    let watcher = spawn_schema_version_watch(config, &reloader);
                    (Some(catalog), watcher, Some(reloader))
                }
                None => (None, None, None),
            };
            Ok(LoadedCatalogAuthority::Single(
                Box::new(RealTiKvSessionFactory::from_authority_with_catalog(
                    &authority,
                    refusals,
                    catalog,
                    watcher,
                    reloader,
                    schema_notifier,
                )),
                authority,
            ))
        }
        Err(tables) => Err(SqlQueryError::unknown(format!(
            "configured SQL node requires one or two servable tables, got {}",
            tables.len()
        ))),
    }
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
    fn parse_set_time_zone_recognizes_the_source_observable_forms() {
        assert_eq!(
            parse_set_time_zone("SET time_zone='+05:00'"),
            Some("+05:00")
        );
        assert_eq!(
            parse_set_time_zone("set time_zone = '-08:00';"),
            Some("-08:00")
        );
        assert_eq!(
            parse_set_time_zone("SET SESSION time_zone = 'UTC'"),
            Some("UTC")
        );
        assert_eq!(
            parse_set_time_zone("SET @@time_zone = 'SYSTEM'"),
            Some("SYSTEM")
        );
        assert_eq!(
            parse_set_time_zone("SET @@session.time_zone = '+00:00'"),
            Some("+00:00")
        );
        // Unquoted and case-preserved inside the value.
        assert_eq!(parse_set_time_zone("SET time_zone=SYSTEM"), Some("SYSTEM"));
        // Not a `time_zone` assignment at all.
        assert_eq!(parse_set_time_zone("SET autocommit = 0"), None);
        assert_eq!(parse_set_time_zone("SELECT 1"), None);
    }

    #[test]
    fn real_tikv_session_time_zone_parses_fixed_offsets_and_refuses_named_zones() {
        assert_eq!(
            RealTiKvSessionTimeZone::parse("+05:00"),
            Some(RealTiKvSessionTimeZone {
                name: "+05:00".to_owned(),
                offset_secs: 18_000,
            })
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("-12:00"),
            Some(RealTiKvSessionTimeZone {
                name: "-12:00".to_owned(),
                offset_secs: -43_200,
            })
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("UTC"),
            Some(RealTiKvSessionTimeZone {
                name: "UTC".to_owned(),
                offset_secs: 0,
            })
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("SYSTEM"),
            Some(RealTiKvSessionTimeZone {
                name: "SYSTEM".to_owned(),
                offset_secs: 0,
            })
        );
        // No IANA timezone database is threaded in, so a named zone is
        // refused rather than silently approximated.
        assert_eq!(RealTiKvSessionTimeZone::parse("Asia/Shanghai"), None);
        assert_eq!(RealTiKvSessionTimeZone::parse("not-a-zone"), None);
    }

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
