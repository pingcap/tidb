// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `session.createCrossKSSession` and `createSessionWithOpt` with a nil Domain.
use super::*;

/// Go `createSessionWithOpt`'s target store, infoschema and schema validator.
pub struct CrossKsSessionResources {
    /// Go `store.GetKeyspace`.
    pub keyspace: String,
    /// Go `SessionManager.coordinator`.
    pub coordinator: Arc<SchemaCoordinator>,
    /// Go `store` transaction interface.
    pub transactions: Arc<dyn ClusterTransactions>,
    /// Go `meta.NewReader`/`NewMutator` over the target store.
    pub meta_store: Arc<dyn tidb_meta::MetaStore>,
    /// Go `infoCache`, populated by `issyncer.NewCrossKSSyncer`.
    pub info_cache: Arc<InfoCache>,
    /// Go `schemaValidator` for the target keyspace.
    pub schema_validator: Arc<SchemaValidator>,
    /// Go `schemaCoordinator`'s related-table tracking.
    pub schema_pins: Arc<tidb_ddl::schema_sync::SchemaPinRegistry>,
    /// Go `store.GetClient`.
    pub kv_client: Option<Arc<dyn tidb_txnkv::Client>>,
    /// Go `store.GetMPPClient`.
    pub mpp_client: Option<Arc<dyn tidb_txnkv::MppClient>>,
    /// Go `session.GlobalVarsAccessor` over target-keyspace globals.
    pub global_vars: GlobalSysvars,
}

/// Go `createCrossKSSession`: target sessions do not retain a Domain.
pub fn create_cross_ks_session(
    current_keyspace: &str,
    target: Arc<CrossKsSessionResources>,
    connection_id: u64,
) -> Result<Session, SqlQueryError> {
    let transactions = target.transactions.clone();
    create_cross_ks_session_over(
        current_keyspace,
        target,
        connection_id,
        transactions,
        MutationBuffer::new(),
    )
}

/// [`create_cross_ks_session`] over the given transactions and buffer (Go:
/// a target session whose `kv.Transaction` is a DDL step's).
pub(crate) fn create_cross_ks_session_over(
    current_keyspace: &str,
    target: Arc<CrossKsSessionResources>,
    connection_id: u64,
    transactions: Arc<dyn ClusterTransactions>,
    buffer: MutationBuffer,
) -> Result<Session, SqlQueryError> {
    if current_keyspace == target.keyspace {
        return Err(SqlQueryError::unknown(
            "cannot create session for the same keyspace",
        ));
    }
    let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
    let handle: Arc<Mutex<dyn ClusterSnapshot>> = slot.clone();
    let mut storage = ClusterTableStorage::new(buffer.clone(), handle);
    if let Some(client) = &target.kv_client {
        storage = storage.with_client(client.clone());
    }
    if let Some(client) = &target.mpp_client {
        storage = storage.with_mpp_client(client.clone());
    }
    storage = storage.with_kv_storage(Arc::new(
        tidb_domain_core::cluster_transactions::ClusterKvStorage(transactions.clone()),
    ));
    let loaded = target.info_cache.get_latest();
    let validator = target.schema_validator.clone();
    let node = Box::new(NodeConn {
        domain: None,
        transactions: transactions.clone(),
        catalog: target.info_cache.clone(),
        cross_ks_store: Some(target.clone()),
        txn: TxnManager::new(
            transactions.clone(),
            Arc::new(NodeSchemaPins(target.schema_pins.clone())),
            connection_id,
            buffer.clone(),
        ),
        slot: slot.clone(),
        follower: catalog_follower::CatalogFollower::new(
            target.info_cache.clone(),
            loaded.schema_meta_version(),
            loaded.skipped_tables().to_vec(),
        ),
        activate_statement_txn: false,
        commit_hooks: Box::new(move |session, schema_version| {
            Box::new(NodeCommitHooks {
                bindings: Arc::default(),
                schema_validator: Some(validator.clone()),
                schema_version,
                catalog: session.shared_catalog(),
            })
        }),
        buffer,
        connection_id,
        ok_packet: WriteOutcome::default(),
        parked_local_infile: None,
        store: None,
    });
    let mut session = Session::over_catalog(crate::SharedCatalog::new((*loaded).clone()), node);
    session.cross_ks = true;
    session.set_transaction_store(Box::new(storage));
    session.set_global_id_generator(Arc::new(ClusterGlobalIds {
        transactions: transactions.clone(),
        global_vars: target.global_vars.clone(),
    }));
    session.set_restricted_sql(true);
    session.enable_privilege_bypass();
    session.set_connection_id(connection_id);
    session.set_info_schema(loaded.clone());
    session.set_plan_cache_schema_version(loaded.schema_meta_version() as u64);
    session.set_global_vars_accessor(Arc::new(target.global_vars.clone()));
    session.set_statement_txn_activator(Arc::new(move || {
        slot.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .start_ts();
    }));
    session.set_mdl_related_table_sink(Arc::new(schema_sync::ConnectionMdlSink::new(
        target.schema_pins.clone(),
        connection_id,
    )));
    session.set_min_safe_ts_provider(Arc::new(move |scope| transactions.min_safe_ts(scope)));
    let catalogs = target.info_cache.clone();
    session.set_cluster_schema_version_source(Arc::new(move || {
        catalogs.get_latest().schema_meta_version()
    }));
    Ok(session)
}

/// Go `schemaCoordinator`: internal sessions currently borrowed from the pool.
#[derive(Default)]
pub struct SchemaCoordinator {
    sessions: Mutex<std::collections::HashSet<u64>>,
}
impl SchemaCoordinator {
    /// Go `schemaCoordinator.StoreInternalSession`.
    pub fn store_internal_session(&self, id: u64) {
        self.sessions.lock().unwrap().insert(id);
    }
    /// Go `schemaCoordinator.DeleteInternalSession`.
    pub fn delete_internal_session(&self, id: u64) {
        self.sessions.lock().unwrap().remove(&id);
    }
    /// Go `schemaCoordinator.ContainsInternalSession`.
    pub fn contains_internal_session(&self, id: u64) -> bool {
        self.sessions.lock().unwrap().contains(&id)
    }
    /// Go `schemaCoordinator.InternalSessionCount`.
    pub fn internal_session_count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

/// Go `ksSessFactoryGetter` and `crossKSSessPoolSize` over domain-free sessions.
pub fn new_cross_ks_session_pool(
    current_keyspace: String,
    target: Arc<CrossKsSessionResources>,
) -> Arc<tidb_syssession::AdvancedSessionPool<dyn tidb_dxf_storage::SessionContext>> {
    let ids = std::sync::atomic::AtomicU64::new(1);
    Arc::new(tidb_syssession::AdvancedSessionPool::new(5, move || {
        let id = ids.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let session = create_cross_ks_session(&current_keyspace, target.clone(), id)
            .map_err(|error| tidb_syssession::SysSessionError::new(error.message))?;
        Ok(Arc::new(super::sessions::ClusterStatsSessionContext::new(
            session,
            target.global_vars.clone(),
        )) as Arc<dyn tidb_dxf_storage::SessionContext>)
    }))
}
