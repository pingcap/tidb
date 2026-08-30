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
/// the argument [`load_stats_snapshot_and_loader`] needs to boot-load
/// statistics for every table this node serves.
fn stats_targets(catalog: &ClusterCatalog) -> Vec<tidb_exec::real_tikv_stats::StatsTarget> {
    catalog
        .databases
        .iter()
        .flat_map(|database| database.tables.iter())
        .map(|table| tidb_exec::real_tikv_stats::StatsTarget {
            table: table.clone(),
            column_types: tidb_exec::cluster_stats_load::column_types_of(table),
        })
        .collect()
}

/// Resolves the reload target set from the catalog currently published by the
/// schema follower. Go's statistics handle does the equivalent on every
/// cache update: a DDL must not leave a newly added table, or a changed column
/// type, outside later `mysql.stats_*` reads.
fn current_stats_targets(catalog: &SharedCatalog) -> Vec<tidb_exec::real_tikv_stats::StatsTarget> {
    let current = catalog.load();
    stats_targets(&current)
}

/// Boot-loads Go's lite statistics state for every table a loaded catalog
/// holds and starts following the cluster's `mysql.stats_*` for them.
///
/// Like pinned Go `InitStatsLite`, startup loads `stats_meta` plus histogram
/// existence/load-state metadata. Buckets, TopN, and CMSketch remain evicted
/// and planning requests individual items through the shared sync/async load
/// path. Reloads use the same lite shape as Go `StatsCacheImpl.Update` with
/// `loadAll=false`.
///
/// Uses Go's independent `Performance.StatsLease`: negative disables both
/// initialization and reload, zero falls back to three seconds, and a
/// positive value is the exact tick. Stats have no schema-watch nudge; Go's
/// own refresh is tick-only too.
pub(crate) fn spawn_node_stats<C, L, P>(
    catalog: Arc<SharedCatalog>,
    opener: tidb_txnkv::transaction::RealOptimisticTransactionOpener<C, L, P>,
    stats_lease: crate::node_config::StatsLease,
    timeout: Duration,
) -> Result<(Arc<SharedStats>, StatsReloader), StatsReloadError>
where
    C: tidb_txnkv::transaction::StoreWriteClient,
    L: tidb_txnkv::transaction::StoreWriteLoader,
    P: tidb_txnkv::transaction::StorePdCapability,
{
    let Some(reload_interval) = stats_lease.reload_interval() else {
        let shared = Arc::new(
            SharedStats::new(Default::default())
                .map_err(|error| StatsReloadError::Spawn(std::io::Error::other(error)))?,
        );
        return Ok((shared, StatsReloader::disabled()));
    };
    let shared = Arc::new(
        SharedStats::new(Default::default())
            .map_err(|error| StatsReloadError::Spawn(std::io::Error::other(error)))?,
    );
    // The read closure needs its own handle to compare against what is
    // published; the caller keeps the original for queries.
    let published = Arc::clone(&shared);
    let mut loader = None;
    let reloader = StatsReloader::spawn_with_initial_pass(
        Arc::clone(&shared),
        reload_interval,
        Box::new(move || {
            let shared = &published;
            let targets = current_stats_targets(&catalog);
            if loader.is_none() {
                let (snapshot, located) =
                    load_stats_snapshot_and_loader(&opener, timeout, &targets)
                        .map_err(|error| error.to_string())?;
                loader = Some(located);
                let receipt = tidb_exec::stats_watch::receipt_of(&snapshot);
                eprintln!(
                    "{{\"event\":\"stats_loaded\",\"loaded\":{},\"pseudo\":{}}}",
                    receipt.loaded, receipt.pseudo
                );
                return Ok(Some(snapshot));
            }
            // Go `Handle.Update`'s tick (`pkg/statistics/handle/update.go`):
            // ONE scan of `mysql.stats_meta` decides. Every version equal to
            // what is published -- and the tracked set unchanged -- means the
            // expensive per-table reads (histograms, buckets, top-n, the
            // catalog they are located through) stay untouched this pass; a
            // moved or new version falls back to the lite snapshot load.
            let ids: Vec<i64> = targets.iter().map(|target| target.table.id).collect();
            match load_stats_meta_versions(
                &opener,
                timeout,
                loader.as_ref().expect("initial pass located stats tables"),
                &ids,
            ) {
                Ok(versions) => {
                    if stats_snapshot_unchanged_since(shared.load().as_ref(), &versions, &targets) {
                        Ok(None)
                    } else {
                        let current = shared.load();
                        refresh_stats_snapshot_from_cluster(
                            &opener,
                            timeout,
                            &targets,
                            current.as_ref(),
                        )
                        .map(Some)
                        .map_err(|error| error.to_string())
                    }
                }
                Err(error) => Err(error.to_string()),
            }
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
pub(crate) fn spawn_catalog_reloader<C, L, P>(
    startup: ClusterCatalog,
    transaction_opener: RealOptimisticTransactionOpener<C, L, P>,
    schema_lease: Duration,
) -> Result<(Arc<SharedCatalog>, CatalogReloader), CatalogReloadError>
where
    C: tidb_txnkv::transaction::StoreWriteClient,
    L: tidb_txnkv::transaction::StoreWriteLoader,
    P: tidb_txnkv::transaction::StorePdCapability,
{
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

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_exec::cluster_catalog::LoadedDatabase;
    use tidb_model::column::ColumnInfo;
    use tidb_model::db::DBInfo;
    use tidb_model::table_info::TableInfo;

    fn catalog(table_id: i64, column_id: i64) -> ClusterCatalog {
        let column = ColumnInfo::new(
            column_id,
            "value",
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        ClusterCatalog {
            schema_version: table_id,
            databases: vec![LoadedDatabase {
                info: DBInfo::default(),
                tables: vec![TableInfo {
                    id: table_id,
                    columns: vec![column].into(),
                    ..TableInfo::default()
                }],
            }],
        }
    }

    #[test]
    fn statistics_reload_targets_follow_the_current_catalog() {
        let shared = SharedCatalog::new(catalog(11, 101));
        assert_eq!(
            current_stats_targets(&shared)
                .into_iter()
                .map(|target| {
                    (
                        target.table.id,
                        target.column_types.into_keys().collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(11, vec![101])]
        );

        // A DDL publication must affect the next stats pass. The old path
        // captured the first target vector and would have kept reading table
        // 11 with its old type map forever.
        shared.store(catalog(22, 202));
        assert_eq!(
            current_stats_targets(&shared)
                .into_iter()
                .map(|target| {
                    (
                        target.table.id,
                        target.column_types.into_keys().collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(22, vec![202])]
        );
    }
}
