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
use tidb_infoschema_issyncer::real_tikv_catalog::note_reload;
use tidb_infoschema_isvalidator::{SchemaValidator, Validator as _};

/// Connects the node's one etcd client (Go `domain.NewDomain`'s
/// `etcdClient`, from `store.EtcdAddrs()`).
///
/// A failure here is a warning, never a startup error: Go's domain runs with
/// `etcdCli == nil` too, its announcements only make peers reload *sooner*,
/// and it carries on when a PUT fails (`pkg/ddl/job_worker.go` logs "update
/// latest schema version failed" and continues outside MDL). The consumers
/// that cannot run without etcd refuse on their own.
pub fn connect_etcd_client(config: &NodeConfig) -> Option<Arc<EtcdClient>> {
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

/// Starts the watch on the key TiDB announces account changes under, so this
/// node's privilege reloader runs within a round trip of a Go TiDB's `GRANT`
/// instead of waiting out its interval.
///
/// This is the same division as the global-version watch in
/// [`tidb_domain_core::schema_sync`]: the watch is an optimisation,
/// the reloader's own tick is the guarantee, and a node whose etcd is
/// unreachable simply loses the promptness.
pub fn spawn_privilege_watch(
    etcd: Option<&EtcdClient>,
    reloader: Option<&PrivilegeReloader>,
) -> Option<EtcdWatcher> {
    let waker = reloader?.waker();
    match etcd?.watch_key(PRIVILEGE_UPDATE_KEY, 0, move |event| {
        // Not log content (Go logs no watch receipt):
        // rust/scripts/run-realtikv-convergence.sh reads this JSON line.
        eprintln!(
            "{{\"event\":\"privilege_watch_fired\",\"mod_revision\":{},\"value\":{}}}",
            event.mod_revision,
            json_string(&String::from_utf8_lossy(&event.value))
        );
        waker.nudge();
    }) {
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
pub fn spawn_sysvar_watch(
    etcd: Option<&EtcdClient>,
    reloader: Option<&tidb_domain_core::cluster_sysvar_seam::SysvarReloader>,
) -> Option<EtcdWatcher> {
    let waker = reloader?.waker();
    match etcd?.watch_key(SYSVAR_UPDATE_KEY, 0, move |_event| {
        // Go `LoadSysVarCacheLoop`, woken by the watch.
        tidb_logutil::bg_logger()
            .debug("Rebuilding sysvar cache from etcd watch event.", &[]);
        waker.nudge();
    }) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            emit_warning("sysvar_watch_unavailable", &error.to_string());
            None
        }
    }
}

// Not log content: a degraded state Go does not have (its etcd connect
// failure stops startup; its watches cannot fail to start), kept on stderr.
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
fn stats_targets(catalog: &ClusterCatalog) -> Vec<tidb_stats_handle_syncload::real_tikv_stats::StatsTarget> {
    catalog
        .databases
        .iter()
        .flat_map(|database| database.tables.iter())
        .flat_map(tidb_stats_handle_syncload::real_tikv_stats::StatsTarget::for_table)
        .collect()
}

/// Resolves the reload target set from the catalog currently published by the
/// schema follower. Go's statistics handle does the equivalent on every
/// cache update: a DDL must not leave a newly added table, or a changed column
/// type, outside later `mysql.stats_*` reads.
fn current_stats_targets(catalog: &SharedCatalog) -> Vec<tidb_stats_handle_syncload::real_tikv_stats::StatsTarget> {
    let current = catalog.load();
    stats_targets(&current)
}

/// Go starts `loadStatsWorker` for a zero lease using its three-second
/// fallback, but returns from `UpdateTableStatsLoop` before starting
/// `asyncLoadHistogram`. Only a positive lease owns that second ticker.
fn async_stats_load_interval(stats_lease: tidb_domain_core::node_config::StatsLease) -> Option<Duration> {
    match stats_lease {
        tidb_domain_core::node_config::StatsLease::Positive(interval) => Some(interval),
        tidb_domain_core::node_config::StatsLease::Disabled | tidb_domain_core::node_config::StatsLease::Zero => None,
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
pub fn spawn_node_stats<S>(
    catalog: Arc<SharedCatalog>,
    opener: tidb_txnkv::transaction::RealOptimisticTransactionOpener<S>,
    stats_lease: tidb_domain_core::node_config::StatsLease,
    timeout: Duration,
) -> Result<(Arc<SharedStats>, StatsReloader, AsyncStatsLoader), StatsReloadError>
where
    S: StoreClient,
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
    let config = tidb_config::config_tree::config::get_global_config();
    let performance = &config.performance;
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
        tidb_stats_handle_storage::cluster_stats_load::ClusterStatsLoader::locate(&catalog.load()).ok(),
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
                if let Err(error) = tidb_stats_handle_syncload::real_tikv_stats::load_needed_histograms_from_cluster(
                    &async_opener,
                    timeout,
                    &targets,
                    &async_shared,
                    &loader,
                ) {
                    // Go `Domain.asyncLoadHistogram`.
                    tidb_stats_handle_logutil::stats_err_verbose_sample_logger()
                        .warn("Load histograms failed", &[crate::domain::zap_error(error)]);
                }
            }),
        )?;
        (loader, Some(init))
    } else {
        (AsyncStatsLoader::disabled(), None)
    };
    let update_loader = Arc::clone(&loader);
    let update_opener = opener.clone();
    let update_catalog = Arc::clone(&catalog);
    let reloader = StatsReloader::spawn_with_initial_pass_notify(
        Arc::clone(&shared),
        reload_interval,
        Box::new(move || {
            let shared = &published;
            let targets = current_stats_targets(&update_catalog);
            if first_pass {
                first_pass = false;
                // Go `Domain.initStats`' log lines.
                let stats_logger = tidb_stats_handle_logutil::stats_logger();
                let result = if skip_initial {
                    stats_logger.info(
                        "Skipping initial stats due to skip-grant-table being set",
                        &[],
                    );
                    Ok(StatsReloadReadResult::Unchanged)
                } else {
                    let started = std::time::Instant::now();
                    let init_fields = |started: std::time::Instant| {
                        vec![
                            tidb_log::Field::new(
                                "isLiteInitStats",
                                tidb_log::Value::Bool(matches!(
                                    initial_mode,
                                    InitialStatsLoad::Lite
                                )),
                            ),
                            tidb_log::Field::new(
                                "duration",
                                tidb_log::Value::Duration(
                                    i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX),
                                ),
                            ),
                        ]
                    };
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
                            stats_logger.info("Init stats succeed", &init_fields(started));
                            Ok(StatsReloadReadResult::Publish(snapshot))
                        }
                        Err(error) => {
                            let mut fields = init_fields(started);
                            fields.push(crate::domain::zap_error(&error));
                            stats_logger.error("Init stats failed", &fields);
                            Err(error.to_string())
                        }
                    }
                };
                return result;
            }
            let located = update_loader
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .map(Ok)
                .unwrap_or_else(|| {
                    tidb_stats_handle_storage::cluster_stats_load::ClusterStatsLoader::locate(
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
                Vec::new(),
            ) {
                Ok(true) => Ok(StatsReloadReadResult::Updated),
                Ok(false) => Ok(StatsReloadReadResult::Unchanged),
                Err(error) => Err(error.to_string()),
            }
        }),
        async_init,
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
///
/// `startup_lease_grant_ts` is the timestamp `startup` was read at
/// ([`tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster_at`]).
pub fn spawn_catalog_reloader<S>(
    startup: ClusterCatalog,
    startup_lease_grant_ts: u64,
    transaction_opener: RealOptimisticTransactionOpener<S>,
    schema_lease: Duration,
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(Arc<SharedCatalog>, CatalogReloader, Arc<SchemaValidator>), CatalogReloadError>
where
    S: StoreClient,
{
    if schema_lease.is_zero() {
        return Err(CatalogReloadError::ZeroInterval);
    }
    // Go `isvalidator.New(do.schemaLease)` (`domain.go:612`): one validator
    // per node, its lease the schema lease.
    let validator = Arc::new(SchemaValidator::new(schema_lease));
    // Go `Domain.Init` runs `isSyncer.Reload()` (`domain.go:779`) before the
    // reload loop starts and before any session exists. That startup load is
    // a full one, so `Reload` resets the validator and renews its lease at
    // the load's own timestamp (`issyncer/syncer.go:455-458`, `:494`). Left
    // unrenewed until the loop's first tick, `schemaLease / 2` later, every
    // commit in that window was `ResultUnknown` until its retries ran out:
    // 8027 on the first COMMIT after boot.
    validator.reset();
    validator.update(startup_lease_grant_ts, 0, startup.schema_version, None);
    let catalog = Arc::new(SharedCatalog::new(startup));
    let pass_validator = Arc::clone(&validator);
    let reloader = CatalogReloader::spawn(
        Arc::clone(&catalog),
        schema_lease / 2,
        Box::new(move |current| {
            match reload_catalog_from_cluster(
                &transaction_opener,
                PRODUCTION_CONTROL_PLANE_TIMEOUT,
                current,
                &row_ids,
            ) {
                Ok(at) => Ok(note_reload(&pass_validator, current.schema_version, at)),
                Err(error) => Err(error.to_string()),
            }
        }),
    )?;
    Ok((catalog, reloader, validator))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_infoschema::cluster_catalog::LoadedDatabase;
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
            resource_groups: Vec::new(),
            policies: Vec::new(),
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
            async_stats_load_interval(tidb_domain_core::node_config::StatsLease::Disabled),
            None
        );
        assert_eq!(
            async_stats_load_interval(tidb_domain_core::node_config::StatsLease::Zero),
            None
        );
        assert_eq!(
            async_stats_load_interval(tidb_domain_core::node_config::StatsLease::Positive(
                Duration::from_secs(7)
            )),
            Some(Duration::from_secs(7))
        );
    }
}
