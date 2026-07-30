//! How this node keeps following the cluster it reads: the reload threads that
//! re-read the schema and the statistics, and the etcd watches that make a
//! peer's change arrive in a round trip instead of an interval.
//!
//! Mirrors Go's `domain.Domain` reload loop plus `Syncer.SyncLoop`
//! (`pkg/ddl/syncer`): the ticker is the *guarantee* and the watch is only an
//! optimisation, so every failure here is a warning and never a startup
//! error -- refusing to start because etcd was unreachable would trade an
//! availability property for a latency one.

use super::*;

/// Connects the best-effort etcd client this node announces its DDL through.
///
/// A failure here is a warning, never a startup error: the announcement only
/// makes peers reload *sooner*, and Go itself carries on when the PUT fails
/// (`pkg/ddl/job_worker.go` logs "update latest schema version failed" and
/// continues outside MDL). Refusing to start because etcd was unreachable
/// would trade an availability property for a latency one.
pub(crate) fn connect_schema_notifier(config: &NodeConfig) -> Option<Arc<EtcdClient>> {
    match EtcdClient::connect_with_security(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        Arc::new(config.cluster_security.clone()),
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
    match EtcdWatcher::spawn_with_security(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        Arc::new(config.cluster_security.clone()),
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
    match EtcdWatcher::spawn_with_security(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        Arc::new(config.cluster_security.clone()),
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
    match EtcdWatcher::spawn_with_security(
        config.pd_endpoints.iter().map(String::as_str),
        PRODUCTION_CONTROL_PLANE_TIMEOUT,
        Arc::new(config.cluster_security.clone()),
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

/// Every table's `(table_id, column_types)` a loaded catalog holds -- exactly
/// the argument [`load_stats_snapshot_from_cluster`] needs to boot-load
/// statistics for every table this node serves.
fn stats_targets(
    catalog: &ClusterCatalog,
) -> Vec<(
    i64,
    std::collections::BTreeMap<i64, tidb_datatype::FieldType>,
)> {
    catalog
        .databases
        .iter()
        .flat_map(|database| database.tables.iter())
        .map(|table| {
            (
                table.id,
                tidb_exec::cluster_stats_load::column_types_of(table),
            )
        })
        .collect()
}

/// Boot-loads every table a loaded catalog holds and starts following the
/// cluster's `mysql.stats_*` for them.
///
/// This is a one-shot load over every loaded table rather than Go's lazy,
/// per-column, async sync-load (`pkg/statistics/handle/syncload`, driven by
/// `collect_column_stats_usage` at plan time with its own worker pool and
/// priority channels): this node's loaded catalog is small (one process's
/// worth of served tables, not a whole cluster's schema), so reading every
/// table's statistics once at boot is a bounded cost, and it keeps the supply
/// line -- plumbing only, no estimation logic -- decoupled from the planner's
/// per-column load-on-demand path that a future estimator unit will add.
/// Documented simplification, not a silent gap: a table analyzed for the
/// first time after boot is picked up by [`StatsReloader`]'s tick, just not
/// as promptly as Go's synchronous on-demand load would.
///
/// Ticks at the same cadence [`spawn_catalog_reloader`] uses (`schema_lease`,
/// not halved -- see the [`tidb_exec::stats_watch`] module doc for why there
/// is no watch to keep prompt the way the catalog's `lease/2` tick is backed
/// by an etcd watch: Go's own stats refresh has no such key either).
pub(crate) fn spawn_node_stats(
    catalog: &ClusterCatalog,
    authority: &ProductionReadProcessAuthority,
    schema_lease: Duration,
    timeout: Duration,
) -> Result<(Arc<SharedStats>, StatsReloader), StatsReloadError> {
    let targets = stats_targets(catalog);
    let snapshot =
        load_stats_snapshot_from_cluster(&authority.transaction_opener(), timeout, &targets)
            .map_err(|error| StatsReloadError::Spawn(std::io::Error::other(error.to_string())))?;
    let receipt = tidb_exec::stats_watch::receipt_of(&snapshot);
    eprintln!(
        "{{\"event\":\"stats_loaded\",\"loaded\":{},\"pseudo\":{}}}",
        receipt.loaded, receipt.pseudo
    );
    let shared = Arc::new(SharedStats::new(snapshot));
    let opener = authority.transaction_opener();
    let reloader = StatsReloader::spawn(
        Arc::clone(&shared),
        schema_lease,
        Box::new(move || {
            load_stats_snapshot_from_cluster(&opener, timeout, &targets)
                .map_err(|error| error.to_string())
        }),
    )?;
    Ok((shared, reloader))
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
