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
        .flat_map(tidb_exec::real_tikv_stats::StatsTarget::for_table)
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

/// Go starts `loadStatsWorker` for a zero lease using its three-second
/// fallback, but returns from `UpdateTableStatsLoop` before starting
/// `asyncLoadHistogram`. Only a positive lease owns that second ticker.
fn async_stats_load_interval(stats_lease: crate::node_config::StatsLease) -> Option<Duration> {
    match stats_lease {
        crate::node_config::StatsLease::Positive(interval) => Some(interval),
        crate::node_config::StatsLease::Disabled | crate::node_config::StatsLease::Zero => None,
    }
}

/// Boot-loads the statistics startup shape selected by Go's performance
/// configuration and starts following the cluster's `mysql.stats_*`.
///
/// `lite-init-stats` selects metadata-only `InitStatsLite`; otherwise
/// `InitStats` fully loads indexes and leaves columns evicted. `skip-init-stats`
/// skips only the immediate pass, so the ordinary periodic updater remains
/// able to populate the cache. Reloads use Go `StatsCacheImpl.Update`.
///
/// Uses Go's independent `Performance.StatsLease`: negative disables all
/// loading, zero gives only the ordinary loader its three-second fallback,
/// and a positive value is the exact tick for both ordinary and asynchronous
/// loading. Stats have no schema-watch nudge; Go's own refresh is tick-only
/// too.
pub(crate) fn spawn_node_stats<C, L, P>(
    catalog: Arc<SharedCatalog>,
    opener: tidb_txnkv::transaction::RealOptimisticTransactionOpener<C, L, P>,
    stats_lease: crate::node_config::StatsLease,
    timeout: Duration,
) -> Result<(Arc<SharedStats>, StatsReloader, AsyncStatsLoader), StatsReloadError>
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
        return Ok((
            shared,
            StatsReloader::disabled(),
            AsyncStatsLoader::disabled(),
        ));
    };
    let shared = Arc::new(
        SharedStats::new(Default::default())
            .map_err(|error| StatsReloadError::Spawn(std::io::Error::other(error)))?,
    );
    // The read closure needs its own handle to compare against what is
    // published; the caller keeps the original for queries.
    let published = Arc::clone(&shared);
    let performance = tidb_config::config_tree::config::get_global_config().performance;
    let initial_mode = if performance.lite_init_stats {
        InitialStatsLoad::Lite
    } else {
        InitialStatsLoad::IndexFull
    };
    let mut first_pass = true;
    let skip_initial = performance.skip_init_stats;
    // Resolving a system-table view is not a startup gate in Go. Keep a view
    // when the boot catalog has one, and let ordinary leased passes retry the
    // lookup after bootstrap or a transient catalog failure.
    let loader = Arc::new(std::sync::RwLock::new(
        tidb_exec::cluster_stats_load::ClusterStatsLoader::locate(&catalog.load()).ok(),
    ));
    let (async_loader, async_init) = if let Some(async_interval) =
        async_stats_load_interval(stats_lease)
    {
        let async_shared = Arc::clone(&shared);
        let async_catalog = Arc::clone(&catalog);
        let async_opener = opener.clone();
        let async_item_loader = Arc::clone(&loader);
        let (loader, init) = AsyncStatsLoader::spawn_waiting_for_init(
            async_interval,
            Box::new(move || {
                let Some(loader) = async_item_loader
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                else {
                    return;
                };
                let targets = current_stats_targets(&async_catalog);
                if let Err(error) = tidb_exec::real_tikv_stats::load_needed_histograms_from_cluster(
                    &async_opener,
                    timeout,
                    &targets,
                    &async_shared,
                    &loader,
                ) {
                    emit_warning("async_stats_load_failed", &error.to_string());
                }
            }),
        )?;
        (loader, Some(init))
    } else {
        (AsyncStatsLoader::disabled(), None)
    };
    let update_async_init = async_init;
    let update_loader = Arc::clone(&loader);
    let update_opener = opener.clone();
    let update_catalog = Arc::clone(&catalog);
    let reloader = StatsReloader::spawn_with_initial_pass(
        Arc::clone(&shared),
        reload_interval,
        Box::new(move || {
            let shared = &published;
            let targets = current_stats_targets(&update_catalog);
            if first_pass {
                first_pass = false;
                let result = if skip_initial {
                    Ok(StatsReloadReadResult::Unchanged)
                } else {
                    match load_stats_snapshot_and_loader(
                        &update_opener,
                        timeout,
                        &targets,
                        &[],
                        initial_mode,
                    ) {
                        Ok((snapshot, located)) => {
                            *update_loader
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(located);
                            let receipt = tidb_exec::stats_watch::receipt_of(&snapshot);
                            eprintln!(
                                "{{\"event\":\"stats_loaded\",\"loaded\":{},\"pseudo\":{}}}",
                                receipt.loaded, receipt.pseudo
                            );
                            Ok(StatsReloadReadResult::Publish(snapshot))
                        }
                        Err(error) => Err(error.to_string()),
                    }
                };
                // Go closes InitStatsDone when initialization succeeds, is
                // skipped, or returns an error, so the async ticker can
                // consume a tick that became pending during initialization.
                if let Some(init) = &update_async_init {
                    init.finish();
                }
                return result;
            }
            let located = update_loader
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .map(Ok)
                .unwrap_or_else(|| {
                    tidb_exec::cluster_stats_load::ClusterStatsLoader::locate(
                        &update_catalog.load(),
                    )
                    .map_err(|error| error.to_string())
                })?;
            *update_loader
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(located.clone());
            match update_stats_cache_from_cluster(
                &update_opener,
                timeout,
                &targets,
                shared,
                &located,
                stats_lease.slow_save_interval(),
                stats_lease == crate::node_config::StatsLease::Zero,
                Vec::new(),
            ) {
                Ok(true) => Ok(StatsReloadReadResult::Updated),
                Ok(false) => Ok(StatsReloadReadResult::Unchanged),
                Err(error) => Err(error.to_string()),
            }
        }),
    )?;
    Ok((shared, reloader, async_loader))
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
                        target.physical_id,
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
                        target.physical_id,
                        target.column_types.into_keys().collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(22, vec![202])]
        );
    }

    #[test]
    fn asynchronous_statistics_loading_requires_a_positive_lease() {
        assert_eq!(
            async_stats_load_interval(crate::node_config::StatsLease::Disabled),
            None
        );
        assert_eq!(
            async_stats_load_interval(crate::node_config::StatsLease::Zero),
            None
        );
        assert_eq!(
            async_stats_load_interval(crate::node_config::StatsLease::Positive(
                Duration::from_secs(7)
            )),
            Some(Duration::from_secs(7))
        );
    }
}
