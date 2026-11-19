// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/domain/crossks`: the target runtime over its own native store.

use super::cluster_session_node::{new_cross_ks_session_pool, CrossKsSessionResources};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_domain_core::cluster_transactions::{ClusterTransactions, RealClusterTransactions};
use tidb_domain_serverinfo::serverinfo_syncer::{
    SyncIntervals, Syncer as ServerInfoSyncer, SyncerRunner,
};
use tidb_infoschema_issyncer::catalog_watch::{InfoCache, Syncer as InfoSchemaSyncer};
use tidb_infoschema_issyncer::real_tikv_catalog::{
    note_reload, ReloadedCatalogAt, TransactionMetaSnapshot,
};
use tidb_infoschema_isvalidator::{SchemaValidator, Validator as _};
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

/// Go `SessionManager` owns the target pool and all of its background loops.
pub struct SessionManager<S: StoreClient> {
    resources: Arc<CrossKsSessionResources>,
    pool: Arc<tidb_syssession::AdvancedSessionPool<dyn tidb_dxf_storage::SessionContext>>,
    opener: RealOptimisticTransactionOpener<S>,
    /// Go `SessionManager.ddlClient`.
    pub ddl_client: super::cross_ks_ddl::DdlClient<S>,
    workers: Mutex<Option<Workers>>,
}

struct Workers {
    server_info: Option<SyncerRunner>,
    info_schema: Option<InfoSchemaSyncer>,
    schema_version: Option<tidb_ddl::schema_sync::SchemaSyncAck>,
    min_job_stop: Option<std::sync::mpsc::Sender<()>>,
    min_job_thread: Option<std::thread::JoinHandle<()>>,
}

impl Workers {
    /// Go `SessionManager.close`: cancel and join runtime workers.
    fn stop(&mut self) {
        drop(self.min_job_stop.take());
        if let Some(thread) = self.min_job_thread.take() {
            let _ = thread.join();
        }
        drop(self.info_schema.take());
        if let Some(schema) = &mut self.schema_version {
            schema.stop_workers();
        }
        drop(self.server_info.take());
        drop(self.schema_version.take());
    }
}
impl Drop for Workers {
    /// Go `createSessionManager` deferred cleanup and `SessionManager.close`.
    fn drop(&mut self) {
        self.stop();
    }
}

/// Go `createSessionManager`'s failed-initialization cleanup.
struct InitializationGuard<S: StoreClient> {
    opener: RealOptimisticTransactionOpener<S>,
    server_info: Arc<ServerInfoSyncer>,
    etcd: Option<Arc<tidb_pd_client::EtcdClient>>,
    registered: bool,
    armed: bool,
}
impl<S: StoreClient> Drop for InitializationGuard<S> {
    /// Go `createSessionManager` error cleanup.
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.registered {
            self.server_info.remove_server_info();
            self.server_info.revoke_session();
        }
        if let Some(etcd) = &self.etcd {
            etcd.close();
        }
        let store = self.opener.store().store().clone();
        let _ = self.opener.store().block_on(store.close());
    }
}

impl<S: StoreClient + Send + 'static> SessionManager<S> {
    /// Go `Manager.createSessionManager`: the caller opens the target store,
    /// supplies its scoped etcd handle and its store-owned KV/MPP clients.
    pub fn create_session_manager(
        current_keyspace: &str,
        opener: RealOptimisticTransactionOpener<S>,
        etcd: Option<Arc<tidb_pd_client::EtcdClient>>,
        info: tidb_domain_serverinfo::serverinfo::ServerInfo,
        schema_lease: Duration,
        kv_client: Option<Arc<dyn tidb_txnkv::Client>>,
        mpp_client: Option<Arc<dyn tidb_txnkv::MppClient>>,
    ) -> Result<Arc<Self>, String> {
        let keyspace = opener.store().get_keyspace().to_owned();
        if current_keyspace == keyspace {
            return Err("cannot create session for the same keyspace".into());
        }
        let server_info = Arc::new(ServerInfoSyncer::new_cross_ks_syncer(
            info,
            etcd.as_ref().map(|client| {
                Arc::new(tidb_ddl::etcd_ops::EtcdClientOps::new(client.clone()))
                    as Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>
            }),
            &keyspace,
        ));
        let mut guard = InitializationGuard {
            opener: opener.clone(),
            server_info: server_info.clone(),
            etcd: etcd.clone(),
            registered: false,
            armed: true,
        };
        server_info.new_session_and_store_server_info()?;
        guard.registered = true;
        let mut workers = Workers {
            server_info: None,
            info_schema: None,
            schema_version: Some(tidb_ddl::schema_sync::SchemaSyncAck::initialize(
                etcd.clone(),
                server_info.get_local_server_info().static_info.id,
                server_info.clone(),
            )?),
            min_job_stop: None,
            min_job_thread: None,
        };
        let server_state: Arc<dyn tidb_ddl_serverstate::Syncer> = match &etcd {
            Some(client) => Arc::new(tidb_ddl_serverstate::EtcdSyncer::new(
                client.clone(),
                tidb_ddl_serverstate::SERVER_GLOBAL_STATE.as_bytes(),
            )),
            None => Arc::new(tidb_ddl_serverstate::MemSyncer::new()),
        };
        server_state
            .get_global_state(&tidb_timeutil::SleepContext::background())
            .map_err(|error| error.to_string())?;
        let timeout = Duration::from_secs(3); // Go Loader.LoadWithTS's TiKVClientReadTimeout.
        let transaction_timeout = tikv_client::tikv::READ_TIMEOUT_SHORT; // Go client.ReadTimeoutShort.
        let mut txn = tidb_txnkv::kv_storage::Storage::begin_read_only(&opener)
            .map_err(|error| error.to_string())?;
        let startup_ts = txn.start_ts();
        let loaded = tidb_infoschema_issyncer::cross_ks::load_info_schema(
            &mut TransactionMetaSnapshot::new(txn.as_mut(), timeout),
        )
        .map_err(|error| error.to_string())?;
        txn.finish_without_writes()
            .map_err(|error| error.to_string())?;
        let auto_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds> = Arc::new(
            tidb_domain_core::cluster_auto_id_seam::ClusterTableAutoIds::new(
                opener.clone(),
                transaction_timeout,
            ),
        );
        let catalog = Arc::new(InfoCache::new_for_cross_ks(loaded));
        catalog.install_schema_builder(Box::new(move |loaded, previous| {
            tidb_domain_core::cluster_session::cluster_session_catalog_for_cross_ks(
                loaded,
                auto_ids.as_ref(),
                previous,
            )
            .catalog
        }));
        let validator = Arc::new(SchemaValidator::new(schema_lease));
        validator.set_mdl_enabled(tidb_vardef::is_mdl_enabled(
            tidb_config_kerneltype::is_next_gen(),
        ));
        validator.update(
            startup_ts,
            0,
            catalog.get_latest().schema_meta_version(),
            None,
        );
        let pins = Arc::new(tidb_ddl::schema_sync::SchemaPinRegistry::default());
        let kv_client = Some(kv_client.unwrap_or_else(|| {
            Arc::new(tidb_copr::cop_client::CopClient::new(Arc::new(
                TargetReadSessionFactory {
                    store: Arc::new(opener.store().clone()),
                },
            )))
        }));
        let mpp_client = Some(mpp_client.unwrap_or_else(|| {
            let cache = catalog.clone();
            Arc::new(tidb_copr::mpp_client::MPPClient::new(
                opener.store().clone(),
                move || cache.get_latest().schema_meta_version(),
            ))
        }));
        let resources = Arc::new(CrossKsSessionResources {
            keyspace,
            coordinator: Arc::default(),
            transactions: Arc::new(RealClusterTransactions::new(
                opener.clone(),
                transaction_timeout,
            )) as Arc<dyn ClusterTransactions>,
            meta_store: Arc::new(tidb_store_driver_txn::meta_txn::MetaStore::new(
                Arc::new(opener.clone()),
                transaction_timeout,
            )),
            info_cache: catalog.clone(),
            schema_validator: validator.clone(),
            schema_pins: pins.clone(),
            kv_client,
            mpp_client,
            // Go cross-KS sessions have no Domain/global-variable cache and
            // begin with NewSessionVars defaults. Do not publish target globals.
            global_vars: tidb_domain::global_sysvars::GlobalSysvars::from_cluster_rows(Vec::new()),
        });
        let pool = new_cross_ks_session_pool(current_keyspace.to_owned(), resources.clone());
        let reload_opener = opener.clone();
        let reload_validator = validator.clone();
        let reloader = InfoSchemaSyncer::spawn(
            catalog.clone(),
            schema_lease / 2,
            Box::new(move |current| {
                let mut txn = tidb_txnkv::kv_storage::Storage::begin_read_only(&reload_opener)
                    .map_err(|error| error.to_string())?;
                let ts = txn.start_ts();
                let reloaded = tidb_infoschema_issyncer::catalog_reload::reload_cross_ks_catalog(
                    &mut TransactionMetaSnapshot::new(txn.as_mut(), timeout),
                    current,
                )
                .map_err(|error| error.to_string())?;
                txn.finish_without_writes()
                    .map_err(|error| error.to_string())?;
                Ok(note_reload(
                    &reload_validator,
                    current.schema_meta_version(),
                    ReloadedCatalogAt {
                        lease_grant_ts: ts,
                        reloaded,
                    },
                ))
            }),
        )
        .map_err(|error| error.to_string())?;
        let min_job_id = Arc::new(tidb_ddl_systable::MinJobIdRefresher::new());
        let ddl_pool = Arc::new(crate::ddl_session_pool::CrossKsDdlSessionPool::new(
            current_keyspace.to_owned(),
            resources.clone(),
        ));
        workers.schema_version = Some(workers.schema_version.take().unwrap().start(
            catalog.clone(),
            Arc::clone(&ddl_pool) as _,
            pins,
            min_job_id.clone(),
            validator,
            reloader.waker(),
            reloader.stats_source(),
            tidb_ddl::schema_sync::MDL_CHECK_LOOK_DURATION,
        )?);
        workers.info_schema = Some(reloader);
        let ddl_client = super::cross_ks_ddl::DdlClient {
            opener: opener.clone(),
            meta_store: resources.meta_store.clone(),
            pool: pool.clone(),
            server_state,
            min_job_id: min_job_id.clone(),
            catalog: catalog.clone(),
            etcd,
            ddl_pool,
        };
        let mut run_min_job_id = true;
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/domain/crossks/skipMinJobIDRefresher",
            &mut run_min_job_id,
        );
        if run_min_job_id {
            let (stop, stopped) = std::sync::mpsc::channel();
            workers.min_job_stop = Some(stop);
            let min_job_pool = pool.clone();
            workers.min_job_thread=Some(std::thread::Builder::new().name("cross-ks-min-job-id".into()).spawn(move || {
            min_job_id.start(&stopped,|previous| {
                let session=min_job_pool.get().map_err(|error|error.to_string())?;
                let result=session.with_session_context(|context| -> tidb_syssession::Result<i64> {
                    let sql=format!("select min(job_id) from mysql.tidb_ddl_job where job_id >= {previous}");
                    let rows=tidb_domain_core::sys_session::exec_sql(context,"ddl",&sql,&[]).map_err(tidb_syssession::SysSessionError::new)?;
                    Ok(rows.first().and_then(|row|row.first()).map_or(0,|value|match value {tidb_datatype::Datum::Int(value)=>*value,tidb_datatype::Datum::UInt(value)=>*value as i64,_=>0}))
                });
                min_job_pool.put(&session);
                result.map_err(|error|error.to_string())
            });
        }).map_err(|error|error.to_string())?);
        }
        workers.server_info = Some(SyncerRunner::start_cross_ks(
            server_info,
            SyncIntervals::default(),
        )?);
        guard.armed = false;
        Ok(Arc::new(Self {
            resources,
            pool,
            opener,
            ddl_client,
            workers: Mutex::new(Some(workers)),
        }))
    }

    /// Go `SessionManager.InfoCache`.
    pub fn info_cache(&self) -> Arc<InfoCache> {
        self.resources.info_cache.clone()
    }
    /// Go `SessionManager.Coordinator`.
    pub fn coordinator(&self) -> Arc<super::cluster_session_node::SchemaCoordinator> {
        self.resources.coordinator.clone()
    }

    /// Go `SessionManager.close`: pool, loops, registration, schema lease, store.
    pub fn close(&self) {
        let mut workers = self.workers.lock().unwrap();
        let Some(mut active) = workers.take() else {
            return;
        };
        self.pool.close();
        active.stop();
        if let Some(etcd) = &self.ddl_client.etcd {
            etcd.close();
        }
        let mut need_close_store = self.resources.keyspace != "SYSTEM";
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
            &mut need_close_store,
        );
        if need_close_store {
            let store = self.opener.store().store().clone();
            let _ = self.opener.store().block_on(store.close());
        }
    }
}
impl<S: StoreClient + Send + 'static>
    tidb_domain_sqlsvrapi::Runtime<dyn tidb_dxf_storage::SessionContext> for SessionManager<S>
{
    /// Go `SessionManager.Store`.
    fn store(&self) -> Arc<dyn tidb_txnkv::kv_storage::Storage> {
        self.resources.transactions.kv_storage()
    }
    /// Go `SessionManager.Store().GetKeyspace`.
    fn store_keyspace(&self) -> String {
        self.resources.keyspace.clone()
    }
    /// Go `SessionManager.alterTableMode`.
    fn alter_table_mode(
        &self,
        ctx: &tidb_gocontext::Context,
        target: &tidb_model::AlterTableModeTarget,
    ) -> Result<(), String> {
        self.ddl_client.alter_table_mode(ctx, target)
    }
    /// Go `SessionManager.SysSessionPool`.
    fn sys_session_pool(
        &self,
    ) -> Arc<dyn tidb_syssession::Pool<dyn tidb_dxf_storage::SessionContext>> {
        self.pool.clone()
    }
}
impl<S: StoreClient + Send + 'static>
    tidb_domain_crossks::SessionManager<dyn tidb_dxf_storage::SessionContext>
    for SessionManager<S>
{
    /// Go `SessionManager.InfoCache`.
    fn info_cache(&self) -> Arc<InfoCache> {
        Self::info_cache(self)
    }
    /// Go `SessionManager.close`.
    fn close(&self) {
        Self::close(self);
    }
}
impl<S: StoreClient> Drop for SessionManager<S> {
    /// Go `SessionManager.close` when the final runtime owner releases it.
    fn drop(&mut self) {
        self.pool.close();
        if let Some(mut workers) = self.workers.get_mut().unwrap().take() {
            workers.stop();
            if let Some(etcd) = &self.ddl_client.etcd {
                etcd.close();
            }
            let mut need_close_store = self.resources.keyspace != "SYSTEM";
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
                &mut need_close_store,
            );
            if need_close_store {
                let store = self.opener.store().store().clone();
                let _ = self.opener.store().block_on(store.close());
            }
        }
    }
}

/// Go `store.GetClient`: coprocessor requests use the target's own store.
struct TargetReadSessionFactory<S: StoreClient> {
    store: Arc<tidb_txnkv::driver::tikv_store::TikvStore<S>>,
}
impl<S: StoreClient> tidb_copr::cop_client::RealTiKvSessionTransportFactory
    for TargetReadSessionFactory<S>
{
    type Transport =
        tidb_copr::cop_paging::NativeCopTransport<tidb_txnkv::driver::tikv_store::TikvStore<S>>;
    /// Go `store.GetClient` for the target keyspace.
    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(tidb_copr::cop_paging::NativeCopTransport::new(
            self.store.clone(),
            tidb_copr::cop_paging::CopRuntimeConfig::default(),
        ))
    }
    /// Go `store.GetClient().Send` for batched target coprocessor requests.
    fn send_batch(
        &self,
        request: &tidb_txnkv::TransportRequest,
    ) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
        tidb_copr::batch_coprocessor::send_batch(&self.store, request)
    }
}

/// Go `getOrCreateStore`: SYSTEM is retained outside individual runtimes.
struct StoreFactory {
    config: tidb_domain_core::node_config::NodeConfig,
    current_keyspace: String,
    system: Mutex<Option<tidb_txnkv::driver::tikv_store::TikvStore>>,
}
impl StoreFactory {
    /// Go `getOrCreateStore`.
    fn get_or_create_store(
        &self,
        target: &str,
    ) -> Result<tidb_txnkv::driver::tikv_store::TikvStore, String> {
        let open = || {
            let mut config =
                tidb_txnkv::driver::tikv_store::cluster_security(&self.config.cluster_security);
            if !target.is_empty() {
                config = config.with_keyspace(target);
            }
            tidb_txnkv::driver::tikv_store::TikvStore::connect(
                self.config.pd_endpoints.clone(),
                config,
            )
        };
        if target != "SYSTEM" {
            return open();
        }
        let mut system = self.system.lock().unwrap();
        if let Some(store) = system.as_ref() {
            return Ok(store.clone());
        }
        let store = open()?;
        *system = Some(store.clone());
        Ok(store)
    }
    /// Go `Manager.createSessionManager` opens the target's native resources.
    fn create(
        &self,
        target: &str,
    ) -> Result<
        Arc<dyn tidb_domain_crossks::SessionManager<dyn tidb_dxf_storage::SessionContext>>,
        String,
    > {
        let store = self.get_or_create_store(target)?;
        let prepared = (|| {
            let mut client = tidb_pd_client::EtcdClient::connect_with_security(
                self.config.pd_endpoints.clone(),
                Duration::from_secs(5),
                Arc::new(self.config.cluster_security.clone()),
            )
            .map_err(|error| error.to_string())?;
            client.set_etcd_cli_by_namespace(&tidb_keyspace::make_keyspace_etcd_namespace(&store));
            let opener = RealOptimisticTransactionOpener::from_capabilities(
                store.clone(),
                Duration::from_secs(60),
            )
            .map_err(|error| error.to_string())?;
            Ok::<_, String>((client, opener))
        })();
        let (client, opener) = prepared.map_err(|error| {
            let native = store.store().clone();
            let _ = store.block_on(native.close());
            error
        })?;
        let info = tidb_domain_serverinfo::serverinfo_syncer::server_info_from_config(
            &tidb_domain_serverinfo::serverinfo_syncer::new_node_id(),
            &self.config.global_config,
            tidb_versioninfo::TIDB_GIT_HASH,
            Some(Arc::new(|| 0)),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        );
        SessionManager::create_session_manager(
            &self.current_keyspace,
            opener,
            Some(Arc::new(client)),
            info,
            self.config.schema_lease,
            None,
            None,
        )
        .map(|manager| {
            manager
                as Arc<
                    dyn tidb_domain_crossks::SessionManager<dyn tidb_dxf_storage::SessionContext>,
                >
        })
    }
}
impl Drop for StoreFactory {
    /// Go `store.CloseAll` releases the retained SYSTEM store at shutdown.
    fn drop(&mut self) {
        if let Some(store) = self.system.get_mut().unwrap().take() {
            let native = store.store().clone();
            let _ = store.block_on(native.close());
        }
    }
}

/// Go `Domain.Init` / `getCrossKSSessionFactory` for production TiKV.
pub fn install_cross_ks_runtime_factory(
    domain: &Arc<tidb_domain_core::domain::Domain>,
    config: &tidb_domain_core::node_config::NodeConfig,
) -> Result<(), String> {
    let stores = Arc::new(StoreFactory {
        config: config.clone(),
        current_keyspace: domain.store_keyspace().to_owned(),
        system: Mutex::new(None),
    });
    domain.install_cross_ks_factory(Arc::new(move |target| stores.create(target)))
}

#[cfg(test)]
mod live_tests {
    use super::*;

    /// Go `TestManager` and `TestDomainAcquireKSRuntimeHandle`, over the
    /// production store factory and an already bootstrapped real TiKV keyspace.
    #[test]
    #[ignore = "requires O15_PD_ENDPOINT and O15_TARGET_KEYSPACE in an isolated cluster"]
    fn test_manager_real_tikv() {
        if tidb_config_kerneltype::is_classic() {
            return;
        }
        let endpoint = std::env::var("O15_PD_ENDPOINT").expect("PD fixture endpoint");
        let target = std::env::var("O15_TARGET_KEYSPACE").expect("bootstrapped target keyspace");
        let config = tidb_domain_core::node_config::NodeConfig::with_defaults(
            |config| {
                config.store = tidb_config::store::StoreType("tikv".into());
                config.path = endpoint.clone();
                config.keyspace_name = "SYSTEM".into();
            },
            tidb_domain_core::node_config::NodeOptions::default(),
        )
        .unwrap();
        let factory = StoreFactory {
            config,
            current_keyspace: "SYSTEM".into(),
            system: Mutex::new(None),
        };
        let observer =
            tidb_pd_client::EtcdClient::connect([endpoint.as_str()], Duration::from_secs(5))
                .unwrap();
        let registrations = || {
            observer
                .get_prefix(b"/keyspaces/tidb/")
                .unwrap()
                .into_iter()
                .filter(|(key, value)| {
                    String::from_utf8_lossy(key).contains("/tidb/server/info/")
                        && serde_json::from_slice::<serde_json::Value>(value)
                            .ok()
                            .is_some_and(|value| {
                                value["assumed_keyspace"].as_str() == Some(target.as_str())
                            })
                })
                .map(|(key, _)| key)
                .collect::<std::collections::BTreeSet<_>>()
        };
        let before = registrations();
        let manager = Arc::new(tidb_domain_crossks::Manager::<
            dyn tidb_dxf_storage::SessionContext,
        >::new("SYSTEM".into()));
        let handle = manager
            .acquire(&target, "o15-live", |target| factory.create(target))
            .unwrap();
        assert_eq!(handle.store_keyspace(), target);
        let created: Vec<_> = registrations().difference(&before).cloned().collect();
        assert_eq!(created.len(), 1);
        let second = manager
            .acquire(&target, "o15-live-second", |_| {
                panic!("runtime must be reused")
            })
            .unwrap();
        second.release();
        let pool = handle.sys_session_pool();
        let session = pool.get().unwrap();
        let marker = format!(
            "o15_crossks_{}",
            tidb_domain_serverinfo::serverinfo_syncer::new_node_id()
        );
        session.with_session_context(|context| -> tidb_syssession::Result<()> {
            let exec = |sql: &str| tidb_domain_core::sys_session::exec_sql(context, "ddl", sql, &[])
                .map_err(tidb_syssession::SysSessionError::new);
            exec(&format!("insert into mysql.tidb(variable_name,variable_value,comment) values('{marker}','target','o15 fixture')"))?;
            let rows = exec(&format!("select variable_value from mysql.tidb where variable_name='{marker}'"))?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].sql_string().unwrap(), "target");
            exec(&format!("delete from mysql.tidb where variable_name='{marker}'"))?;
            assert!(exec("select * from test.t_named").is_err(), "user tables must be absent from the cross-KS schema");
            assert!(exec("create table test.o15_disallowed(id int)").unwrap_err().to_string()
                .contains("DDL is not supported in cross keyspace session"));
            Ok(())
        }).unwrap();
        pool.put(&session);
        handle.release();
        manager.close();
        let after = registrations();
        assert!(
            created.iter().all(|key| !after.contains(key)),
            "virtual registration must be removed on close"
        );
        assert!(
            pool.get().is_err(),
            "closed target pool must reject acquisition"
        );
    }
}
