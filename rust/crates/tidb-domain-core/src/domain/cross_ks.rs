// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `Domain.crossKSSessMgr`, acquisition and the SYSTEM-only GC loop.
use super::Domain;
use std::sync::{Arc, Mutex};
use tidb_domain_crossks::{Manager, SessionManager};
use tidb_domain_sqlsvrapi::KsRuntimeHandle;
use tidb_dxf_storage::SessionContext;
use tidb_gocontext::{CancelFunc, Context};

/// Go `Manager.createSessionManager` supplied by the store/session composition.
pub type SessionManagerFactory =
    Arc<dyn Fn(&str) -> Result<Arc<dyn SessionManager<dyn SessionContext>>, String> + Send + Sync>;

/// Go `Domain.crossKSSessMgr` and the worker joined by `Domain.Close`.
pub(super) struct CrossKsRuntimes {
    manager: Arc<Manager<dyn SessionContext>>,
    factory: SessionManagerFactory,
    cancel: CancelFunc,
    gc: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl CrossKsRuntimes {
    /// Go `Domain.Init` and `Domain.Start`: install the manager and SYSTEM GC loop.
    fn new(keyspace: &str, factory: SessionManagerFactory) -> Result<Self, String> {
        let manager = Arc::new(Manager::new(keyspace.to_owned()));
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let gc = if tidb_config_kerneltype::is_next_gen() && keyspace == "SYSTEM" {
            let owner = manager.clone();
            Some(
                std::thread::Builder::new()
                    .name("cross-ks-runtime-gc".into())
                    .spawn(move || owner.run_system_ks_gc_loop(&ctx))
                    .map_err(|error| error.to_string())?,
            )
        } else {
            None
        };
        Ok(Self {
            manager,
            factory,
            cancel,
            gc: Mutex::new(gc),
        })
    }
}
impl Drop for CrossKsRuntimes {
    /// Go `Domain.Close`: cancel the GC loop before closing all target runtimes.
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(thread) = self.gc.get_mut().unwrap().take() {
            let _ = thread.join();
        }
        self.manager.close();
    }
}

impl Domain {
    /// Go `Domain.Init` installs the target-store/session factory.
    pub fn install_cross_ks_factory(&self, factory: SessionManagerFactory) -> Result<(), String> {
        self.cross_ks_runtimes
            .set(CrossKsRuntimes::new(self.store_keyspace(), factory)?)
            .map_err(|_| "cross keyspace session factory already initialized".to_owned())
    }

    /// Go `Domain.AcquireKSRuntime`.
    pub fn acquire_ks_runtime(
        &self,
        target: &str,
        holder: &str,
    ) -> Result<Arc<dyn KsRuntimeHandle<dyn SessionContext>>, String> {
        if holder.is_empty() {
            return Err("cross keyspace runtime holderID must not be empty".into());
        }
        self.validate_cross_ks(target)?;
        let runtimes = self
            .cross_ks_runtimes
            .get()
            .ok_or_else(|| "cross keyspace session factory is not initialized".to_owned())?;
        runtimes
            .manager
            .acquire(target, holder, |ks| (runtimes.factory)(ks))
    }

    /// Go `Domain.GetKSStore`: the retained runtime's target transaction store.
    pub fn get_ks_store(
        &self,
        target: &str,
    ) -> Result<Arc<dyn tidb_txnkv::kv_storage::Storage>, String> {
        self.validate_cross_ks(target)?;
        let runtimes = self
            .cross_ks_runtimes
            .get()
            .ok_or_else(|| "cross keyspace session factory is not initialized".to_owned())?;
        Ok(runtimes
            .manager
            .get_or_create(target, |ks| (runtimes.factory)(ks))?
            .store())
    }

    /// Go `Domain.GetKSInfoCache`: the retained runtime's target catalog cache.
    pub fn get_ks_info_cache(
        &self,
        target: &str,
    ) -> Result<Arc<tidb_infoschema_issyncer::catalog_watch::InfoCache>, String> {
        self.validate_cross_ks(target)?;
        let runtimes = self
            .cross_ks_runtimes
            .get()
            .ok_or_else(|| "cross keyspace session factory is not initialized".to_owned())?;
        Ok(runtimes
            .manager
            .get_or_create(target, |ks| (runtimes.factory)(ks))?
            .info_cache())
    }

    /// Go `Domain.GetKSSessPool`: retained raw runtime, without an idle deadline.
    pub fn get_ks_sess_pool(
        &self,
        target: &str,
    ) -> Result<Arc<dyn tidb_syssession::Pool<dyn SessionContext>>, String> {
        self.validate_cross_ks(target)?;
        let runtimes = self
            .cross_ks_runtimes
            .get()
            .ok_or_else(|| "cross keyspace session factory is not initialized".to_owned())?;
        Ok(runtimes
            .manager
            .get_or_create(target, |ks| (runtimes.factory)(ks))?
            .sys_session_pool())
    }

    /// Go `Domain.loadSysKSInfoSchema`: creation loads the SYSTEM catalog.
    pub fn load_sys_ks_info_schema(&self) -> Result<(), String> {
        self.get_ks_store(tidb_keyspace::SYSTEM).map(|_| ())
    }

    /// Go `Domain.GetCrossKSMgr`.
    pub fn get_cross_ks_mgr(&self) -> Option<Arc<Manager<dyn SessionContext>>> {
        self.cross_ks_runtimes
            .get()
            .map(|runtimes| runtimes.manager.clone())
    }

    /// Go `Manager.validateTargetKS` before any store/session construction.
    fn validate_cross_ks(&self, target: &str) -> Result<(), String> {
        if tidb_config_kerneltype::is_classic() || self.store_keyspace() == target {
            return Err(
                "cross keyspace is not available in classic kernel or current keyspace".into(),
            );
        }
        Ok(())
    }
}
