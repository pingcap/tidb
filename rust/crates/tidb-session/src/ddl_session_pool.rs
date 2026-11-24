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

//! The node's DDL session pool.
//!
//! Go's `pkg/ddl` takes its sessions from the pool the domain hands it: a
//! system session for a job the domain submits itself, and the sessions its
//! data checks (`checkModifyColumnData`, `checkForNullValue`,
//! `checkForeignKeyConstrain`, `verifyRemainRecordsForCheckConstraint`,
//! `checkExchangePartitionRecordValidation`) run their restricted SQL in; the
//! DDL package never imports the executor. The worker here holds a
//! [`tidb_ddl_build::session_pool::DdlSessionPool`] the same way, and this is
//! the implementation the node hands it: built before the node's domain (the
//! DDL is one of the domain's inputs) and bound to it once the domain exists,
//! as Go's domain hands DDL its session pool from the session factory.

use std::sync::{Arc, OnceLock, Weak};

use tidb_domain_core::cluster_transactions::ClusterTransactions;
use tidb_domain_core::domain::Domain;

use crate::domain::cluster_session_node::DomainSessions;

use tidb_ddl_build::session_pool::DdlSessionPool;
use tidb_terror::MysqlError;
use tidb_sessionctx::StmtContext;

/// The node's DDL session pool: system sessions of the node's domain.
#[derive(Default)]
pub struct NodeDdlSessionPool {
    domain: OnceLock<Weak<Domain>>,
    sess_pool: OnceLock<Arc<tidb_ddl_session::DynPool>>,
    /// The node's store is Go's `mockStorage` (unistore), not TiKV.
    mock_storage: bool,
    /// Go `GetDomain(store)`: retain the domain for store-owned sessions.
    store_domain: Option<StoreDomain>,
}

impl std::fmt::Debug for NodeDdlSessionPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeDdlSessionPool")
            .field("bound", &self.domain.get().is_some())
            .finish()
    }
}

impl NodeDdlSessionPool {
    /// The pool of a node over Go's `mockStorage` (unistore).
    #[must_use]
    pub fn mock_storage() -> Self {
        Self {
            mock_storage: true,
            ..Self::default()
        }
    }

    /// Go `CreateSession(store)` / `GetDomain(store)`: system sessions for
    /// DDL work over a store before a serving node owns its domain. The
    /// domain runs the ordinary SQL executor; its DDL owner is not campaigned.
    pub fn for_store<S: tidb_store_driver_txn::transaction::StoreClient>(
        opener: tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
        timeout: std::time::Duration,
        row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
        mock_storage: bool,
        schema_lease: std::time::Duration,
    ) -> Result<Arc<Self>, String> {
        Self::for_store_with_mode(opener, timeout, row_ids, mock_storage, schema_lease, false)
    }

    /// Go `Domain.Start(ddl.Bootstrap)`: the bootstrap owns the ordinary
    /// DDL scheduler and its system sessions before the serving node starts.
    pub(crate) fn for_bootstrap<S: tidb_store_driver_txn::transaction::StoreClient>(
        opener: tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
        timeout: std::time::Duration,
    ) -> Result<Arc<Self>, String> {
        let lease = tidb_domain_core::node_config::parse_file_schema_lease(
            &tidb_config::config_tree::config::get_global_config().lease,
        ).map_err(|error| error.to_string())?;
        let row_ids = Arc::new(tidb_domain_core::cluster_auto_id_seam::ClusterTableAutoIds::new(opener.clone(), timeout));
        Self::for_store_with_mode(
            opener, timeout, row_ids, false,
            lease, true,
        )
    }

    /// Go `initGlobalVarFromSystemDB`'s `domap.GetOrCreateWithFilter(store,
    /// systemDBFilter{})`: a separate domain over the store, neither started
    /// nor campaigning for DDL, whose sessions read `mysql.tidb` before the
    /// serving domain exists. (Go's filter loads only the system schemas;
    /// this one loads the whole catalog.)
    pub(crate) fn for_global_var_init<S: tidb_store_driver_txn::transaction::StoreClient>(
        opener: tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
        timeout: std::time::Duration,
    ) -> Result<Arc<Self>, String> {
        let lease = tidb_domain_core::node_config::parse_file_schema_lease(
            &tidb_config::config_tree::config::get_global_config().lease,
        ).map_err(|error| error.to_string())?;
        let row_ids = Arc::new(tidb_domain_core::cluster_auto_id_seam::ClusterTableAutoIds::new(opener.clone(), timeout));
        Self::for_store_with_mode(opener, timeout, row_ids, false, lease, false)
    }

    fn for_store_with_mode<S: tidb_store_driver_txn::transaction::StoreClient>(
        opener: tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
        timeout: std::time::Duration,
        row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
        mock_storage: bool,
        schema_lease: std::time::Duration,
        bootstrap: bool,
    ) -> Result<Arc<Self>, String> {
        use tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster_at;
        use tidb_domain_core::cluster_transactions::RealClusterTransactions;
        // Go `NewDomain` / `Domain.Init`: the domain owns one validator,
        // grants its initial lease on reload, and shares it with DDL and
        // sessions. A DDL barrier's zero wait is not a domain schema lease.
        let (startup, at) = load_catalog_from_cluster_at(&opener, timeout)
            .map_err(|e| e.to_string())?;
        let (catalog, reloader, schema_validator) =
            crate::domain::real_tikv_node::spawn_catalog_reloader(
                startup, at, opener.clone(), schema_lease, Arc::clone(&row_ids),
            ).map_err(|e| e.to_string())?;
        // This internal-session domain reads variables without publishing
        // process-wide defaults over the owning DDL's current settings.
        let globals = tidb_domain::global_sysvars::GlobalSysvars::from_cluster_rows([]);
        let privileges = tidb_privilege::PrivilegeRegistry::default();
        let stats = Arc::new(tidb_stats_handle_syncload::stats_watch::SharedStats::new(Default::default())
            .map_err(|e| e.to_string())?);
        let worker_pool = Arc::new(Self { mock_storage, ..Self::default() });
        let mut info = tidb_domain_serverinfo::serverinfo::ServerInfo::default();
        info.static_info.id = tidb_domain_serverinfo::serverinfo_syncer::new_node_id();
        info.static_info.version_info.version = tidb_mysql::runtime_versions().tidb_release_version;
        let server_info = Arc::new(tidb_domain_serverinfo::serverinfo_syncer::Syncer::new(info, None));
        let ddl = Arc::new(tidb_ddl::worker::RealClusterDdl::new_with_min_job_id_refresher(
            opener.clone(), Arc::clone(&catalog), timeout, None, server_info, None,
            Arc::clone(&schema_validator),
            bootstrap, Arc::new(tidb_ddl_systable::MinJobIdRefresher::new()),
            worker_pool.clone(), Arc::clone(&row_ids),
            if bootstrap { tidb_ddl::StartMode::Bootstrap } else { tidb_ddl::StartMode::Normal },
        )?);
        let domain = Domain::new(
            Arc::new(RealClusterTransactions::new(opener.clone(), timeout)), ddl,
            Arc::new(tidb_domain_core::cluster_account_seam::RealClusterAccountWriter::new(
                privileges.clone(), None,
            )),
            Arc::new(crate::cluster_sysvar_writer::RealClusterSysvarWriter::new(
                Arc::new(opener.clone()), globals.clone(), timeout, None, Default::default(), worker_pool.clone(),
            )),
            catalog, privileges, globals, Arc::clone(&stats), row_ids,
            crate::domain::cluster_session_node::open_pool_session,
        ).with_schema_validator(schema_validator)
        .with_kv_client(Arc::new(tidb_copr::cop_client::CopClient::new(Arc::new(
            StoreReadSessionFactory { store: Arc::new(opener.store().clone()), timeout },
        )))).shared();
        crate::domain::cluster_session_node::NodeServices::new(Arc::new(
            tidb_executor::cluster_analyze_seam::RealClusterAnalyze::new(
                Arc::new(opener), stats, timeout, std::time::Duration::ZERO,
            ),
        )).bind(&domain);
        worker_pool.bind(&domain);
        // The DDL's own pool retains only a Weak domain pointer, avoiding
        // a domain -> DDL -> pool -> domain cycle.
        let pool = Arc::new(Self { mock_storage, store_domain: Some(StoreDomain { domain: Arc::clone(&domain), _reloader: reloader }), ..Self::default() });
        pool.bind(&domain);
        Ok(pool)
    }

    /// Go `mustExecute` for bootstrap DDL, on the domain's ordinary owner.
    pub(crate) fn execute_bootstrap(
        &self,
        statement: &tidb_ast::Stmt,
        view: Option<&tidb_model::TableInfo>,
        context: tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<(), String> {
        execute_bootstrap_ddl(
            &self.store_domain.as_ref().expect("store-owned domain").domain,
            statement,
            view,
            context,
        )
    }

    /// Binds the pool to the domain whose sessions it hands out.
    pub fn bind(&self, domain: &Arc<Domain>) {
        if self.domain.set(Arc::downgrade(domain)).is_err() {
            panic!("a node's DDL session pool is bound once");
        }
        let _ = self.sess_pool.set(Arc::new(tidb_ddl_session::Pool::new(Box::new(
            NodeDdlResourcePool {
                domain: Arc::downgrade(domain),
            },
        ))));
    }

    /// The domain [`Self::for_bootstrap`] built and owns.
    pub(crate) fn store_domain(&self) -> Option<Arc<Domain>> {
        self.store_domain.as_ref().map(|owned| Arc::clone(&owned.domain))
    }
}

/// Go `mustExecute` for one bootstrap DDL statement, on `domain`'s DDL.
pub(crate) fn execute_bootstrap_ddl(
    domain: &Domain,
    statement: &tidb_ast::Stmt,
    view: Option<&tidb_model::TableInfo>,
    context: tidb_ddl::cluster_ddl::DdlContext,
) -> Result<(), String> {
    let ddl = &domain.ddl;
    let tidb_ast::Stmt::Ddl(ddl_statement) = statement else {
        return Err("bootstrap statement is not DDL".into());
    };
    let result = match &**ddl_statement {
        tidb_ast::DdlStmt::CreateView(create) => {
            let (schema, name) =
                tidb_ddl::cluster_ddl::split_name(&create.name, "mysql", "view")
                    .map_err(|error| error.to_string())?;
            ddl.create_view(
                &schema,
                &name,
                create.or_replace,
                view.expect("bootstrap view metadata"),
                context.as_ref(),
            )
        }
        tidb_ast::DdlStmt::AlterResourceGroup(_) => {
            let (group, _) = crate::mysql_bootstrap::bootstrap_default_resource_group()
                .map_err(|error| error.to_string())?;
            ddl.do_ddl_job_wrapper(
                context.as_ref(),
                tidb_ddl::cluster_ddl::resource_group::alter_resource_group(
                    &group,
                    context.as_ref(),
                ),
            )
        }
        _ => ddl.execute_ddl(statement, "mysql", &context),
    };
    result.map(drop).map_err(|error| error.to_string())
}

/// Go `GetDomain(store)`'s owned domain for this pool's lifetime.
struct StoreDomain {
    domain: Arc<Domain>,
    _reloader: tidb_infoschema_issyncer::catalog_watch::Syncer,
}

impl Drop for StoreDomain {
    /// Go `Domain.Close` / `DDL.Stop`: retire the store-owned domain before
    /// releasing it. Its scheduler session retains the domain while running,
    /// so dropping the last external Arc alone cannot retire that owner.
    fn drop(&mut self) {
        if let Some(owner) = self.domain.ddl.owner_manager() {
            owner.campaign_cancel();
        }
    }
}

/// Go `kv.Storage.GetClient`: the same store serves the pool's SQL scans.
struct StoreReadSessionFactory<S: tidb_store_driver_txn::transaction::StoreClient> {
    store: Arc<tidb_txnkv::driver::tikv_store::TikvStore<S>>,
    timeout: std::time::Duration,
}

impl<S: tidb_store_driver_txn::transaction::StoreClient>
    tidb_copr::cop_client::RealTiKvSessionTransportFactory for StoreReadSessionFactory<S>
{
    type Transport = tidb_copr::cop_paging::NativeCopTransport<tidb_txnkv::driver::tikv_store::TikvStore<S>>;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(tidb_copr::cop_paging::NativeCopTransport::new(
            Arc::clone(&self.store), tidb_copr::cop_paging::CopRuntimeConfig {
                default_timeout: self.timeout,
                ..Default::default()
            },
        ))
    }

    fn send_batch(&self, request: &tidb_txnkv::TransportRequest) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
        tidb_copr::batch_coprocessor::send_batch(&self.store, request)
    }
}

/// Go's DDL callers run the pool's sessions under the DDL context
/// (`kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)`), whose source a
/// restricted session adopts (`setRequestSource`).
fn set_ddl_request_source(session: &mut crate::Session) {
    tidb_txnkv::INTERNAL_TXN_DDL.clone_into(&mut session.vars.request_source_type);
}

/// Go `ddl.WithResourcePool(sysCtxPool)`: the domain's system sessions as
/// the resource pool `sess.NewSessionPool` wraps.
struct NodeDdlResourcePool {
    domain: Weak<Domain>,
}

impl tidb_ddl_session::ResourcePool<dyn tidb_ddl_session::SessionContext> for NodeDdlResourcePool {
    fn get(&self) -> tidb_ddl_session::Result<Arc<dyn tidb_ddl_session::SessionContext>> {
        let domain = self
            .domain
            .upgrade()
            .ok_or_else(|| tidb_ddl_session::Error::new("the DDL session pool has no running node"))?;
        let mut session = domain
            .open_system_session()
            .map_err(|error| tidb_ddl_session::Error::new(error.message))?;
        set_ddl_request_source(&mut session);
        Ok(Arc::new(
            crate::domain::cluster_session_node::ClusterStatsSessionContext::new(
                session,
                domain.global_vars.clone(),
            ),
        ))
    }

    /// The session ends with its handle; the node opens one per `Get`.
    fn put(&self, _resource: Option<Arc<dyn tidb_ddl_session::SessionContext>>) {}

    fn close(&self) {}

    fn destroy_mode(&self) -> tidb_ddl_session::DestroyMode {
        tidb_ddl_session::DestroyMode::ResourcePool
    }
}

impl DdlSessionPool for NodeDdlSessionPool {
    fn sess_pool(&self) -> Option<Arc<tidb_ddl_session::DynPool>> {
        self.sess_pool.get().cloned()
    }

    fn ddl_event_ch(
        &self,
    ) -> Option<crossbeam_channel::Sender<tidb_ddl_notifier::SchemaChangeEvent>> {
        self.domain
            .get()
            .and_then(Weak::upgrade)
            .map(|domain| domain.ddl_event_sender())
    }

    /// Go `session.GetAdvisoryLock`: each lock is an internal session of
    /// the node's domain, as a user session's are.
    fn advisory_lock_service(
        &self,
    ) -> Option<Arc<dyn tidb_sessionctx::advisory_lock_state::AdvisoryLockService>> {
        let domain = self.domain.get()?.clone();
        Some(Arc::new(crate::domain::cluster_session_node::advisory_locks::SessionAdvisoryLockService { domain }))
    }

    /// A fresh statement of a default session.
    fn system_session(&self) -> Box<dyn tidb_ddl_build::session_context::DdlSessionContext> {
        Box::new(StmtContext::for_query())
    }

    /// Each statement on a fresh system session of the node, in its own
    /// autocommit transaction (Go `withRestrictedSQLExecutor` without
    /// `ExecOptionUseCurSession`).
    fn restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
        Arc::new(crate::domain::cluster_session_node::NodeRestrictedSql::new(
            self.domain.get().cloned().unwrap_or_default(),
        ))
    }

    /// A fresh system session of the node under the DDL's request source.
    fn get(&self) -> Result<Box<dyn tidb_ddl_build::session_pool::DdlSysSession>, MysqlError> {
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("the DDL session pool has no running node"))?;
        let mut session = domain.open_system_session().map_err(sql_to_mysql)?;
        set_ddl_request_source(&mut session);
        Ok(Box::new(crate::domain::cluster_session_node::SystemSqlExecutor::new(session)))
    }

    fn current_version(&self) -> Result<u64, MysqlError> {
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("the DDL session pool has no running node"))?;
        domain.transactions.fresh_timestamp().map_err(MysqlError::unknown)
    }

    fn min_safe_ts(&self) -> u64 {
        self.domain
            .get()
            .and_then(Weak::upgrade)
            .map_or(0, |domain| domain.transactions.min_safe_ts("global"))
    }

    fn store_name(&self) -> &'static str {
        if self.mock_storage {
            "mock-storage"
        } else {
            "TiKV"
        }
    }

    fn tiflash_store_count(&self) -> Result<u64, MysqlError> {
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("the DDL session pool has no running node"))?;
        domain
            .ddl
            .tiflash_store_count()
            .map_err(|error| MysqlError::from_parts(error.code, error.state, error.message))
    }

    fn open_sql_session(&self) -> Result<tidb_ddl_session::DdlSession, MysqlError> {
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("the DDL session pool has no running node"))?;
        let mut session = domain.open_system_session().map_err(sql_to_mysql)?;
        set_ddl_request_source(&mut session);
        Ok(ddl_session(session, domain.global_vars.clone()))
    }

    fn open_step_session(
        &self,
        start_ts: u64,
        buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
    ) -> Result<tidb_ddl_session::DdlSession, MysqlError> {
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("the DDL session pool has no running node"))?;
        let transactions: Arc<dyn ClusterTransactions> = Arc::new(StepTransactions {
            inner: Arc::clone(&domain.transactions),
            start_ts,
        });
        let mut session = domain
            .open_system_session_over(transactions, buffer.clone())
            .map_err(sql_to_mysql)?;
        // Go `sess.Begin`: `sessiontxn.NewTxn` enters an optimistic
        // transaction.
        session
            .node_conn()
            .new_txn(tidb_ast::TransactionMode::Optimistic)
            .map_err(sql_to_mysql)?;
        Ok(step_session(session, domain.global_vars.clone()))
    }
}

/// Go `ddlClient`'s `SubmitOptions.SessPool`: `sess.NewSessionPool` over the
/// target keyspace's system session pool (crossks `SessionManager`). Its
/// sessions are domain-free target sessions (Go `createCrossKSSession`).
pub struct CrossKsDdlSessionPool {
    resources: CrossKsDdlResourcePool,
    sess_pool: Arc<tidb_ddl_session::DynPool>,
}

impl CrossKsDdlSessionPool {
    /// Go `sess.NewSessionPool(SessionManager.sysSessPool)`.
    pub fn new(
        current_keyspace: String,
        target: Arc<crate::domain::cluster_session_node::CrossKsSessionResources>,
    ) -> Self {
        let resources = CrossKsDdlResourcePool { current_keyspace, target };
        let sess_pool = Arc::new(tidb_ddl_session::Pool::new(Box::new(resources.clone())
            as Box<dyn tidb_ddl_session::ResourcePool<dyn tidb_ddl_session::SessionContext>>));
        Self { resources, sess_pool }
    }
}

/// The target keyspace's system sessions, one per `Get`.
#[derive(Clone)]
struct CrossKsDdlResourcePool {
    current_keyspace: String,
    target: Arc<crate::domain::cluster_session_node::CrossKsSessionResources>,
}

impl CrossKsDdlResourcePool {
    /// Go `createCrossKSSession` under the DDL's request source.
    fn open(
        &self,
        transactions: Arc<dyn ClusterTransactions>,
        buffer: tidb_store_driver_txn::cluster_storage::MutationBuffer,
    ) -> Result<crate::Session, MysqlError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        // Apart from client connections and the advanced pool's range.
        static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1_u64 << 61);
        let mut session = crate::domain::cluster_session_node::create_cross_ks_session_over(
            &self.current_keyspace,
            Arc::clone(&self.target),
            NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            transactions,
            buffer,
        )
        .map_err(sql_to_mysql)?;
        set_ddl_request_source(&mut session);
        Ok(session)
    }

    fn open_fresh(&self) -> Result<crate::Session, MysqlError> {
        self.open(
            Arc::clone(&self.target.transactions),
            tidb_store_driver_txn::cluster_storage::MutationBuffer::new(),
        )
    }
}

impl tidb_ddl_session::ResourcePool<dyn tidb_ddl_session::SessionContext>
    for CrossKsDdlResourcePool
{
    fn get(&self) -> tidb_ddl_session::Result<Arc<dyn tidb_ddl_session::SessionContext>> {
        let session = self
            .open_fresh()
            .map_err(|error| tidb_ddl_session::Error::new(error.to_string()))?;
        Ok(Arc::new(
            crate::domain::cluster_session_node::ClusterStatsSessionContext::new(
                session,
                self.target.global_vars.clone(),
            ),
        ))
    }

    /// The session ends with its handle; the pool opens one per `Get`.
    fn put(&self, _resource: Option<Arc<dyn tidb_ddl_session::SessionContext>>) {}

    fn close(&self) {}

    fn destroy_mode(&self) -> tidb_ddl_session::DestroyMode {
        tidb_ddl_session::DestroyMode::ResourcePool
    }
}

/// Go `withRestrictedSQLExecutor` on a target session: each statement on a
/// fresh target session in its own autocommit transaction.
struct CrossKsRestrictedSql(CrossKsDdlResourcePool);

impl CrossKsRestrictedSql {
    fn executor(&self) -> Result<crate::domain::cluster_session_node::SystemSqlExecutor, tidb_sqlexec::SqlExecError> {
        let session = self.0.open_fresh().map_err(|error| {
            tidb_domain_core::sys_session::stats_session_error(error.to_string())
        })?;
        Ok(crate::domain::cluster_session_node::SystemSqlExecutor::new(session))
    }
}

impl tidb_sqlexec::RestrictedSqlExecutor for CrossKsRestrictedSql {
    fn parse_with_params(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<tidb_ast::Stmt, tidb_sqlexec::SqlExecError> {
        tidb_sqlexec::RestrictedSqlExecutor::parse_with_params(&self.executor()?, context, sql, arguments)
    }

    fn exec_restricted_stmt(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
        options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> Result<(Vec<Vec<tidb_datatype::Datum>>, Vec<tidb_resolve::ResultFieldRef>), tidb_sqlexec::SqlExecError>
    {
        tidb_sqlexec::RestrictedSqlExecutor::exec_restricted_stmt(&self.executor()?, context, statement, options)
    }

    fn exec_restricted_sql(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        options: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<(Vec<Vec<tidb_datatype::Datum>>, Vec<tidb_resolve::ResultFieldRef>), tidb_sqlexec::SqlExecError>
    {
        tidb_sqlexec::RestrictedSqlExecutor::exec_restricted_sql(&self.executor()?, context, options, sql, arguments)
    }
}

impl DdlSessionPool for CrossKsDdlSessionPool {
    fn sess_pool(&self) -> Option<Arc<tidb_ddl_session::DynPool>> {
        Some(Arc::clone(&self.sess_pool))
    }

    /// A fresh statement of a default session.
    fn system_session(&self) -> Box<dyn tidb_ddl_build::session_context::DdlSessionContext> {
        Box::new(StmtContext::for_query())
    }

    fn restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
        Arc::new(CrossKsRestrictedSql(self.resources.clone()))
    }

    fn get(&self) -> Result<Box<dyn tidb_ddl_build::session_pool::DdlSysSession>, MysqlError> {
        Ok(Box::new(crate::domain::cluster_session_node::SystemSqlExecutor::new(
            self.resources.open_fresh()?,
        )))
    }

    fn current_version(&self) -> Result<u64, MysqlError> {
        self.resources.target.transactions.fresh_timestamp().map_err(MysqlError::unknown)
    }

    fn min_safe_ts(&self) -> u64 {
        self.resources.target.transactions.min_safe_ts("global")
    }

    fn open_sql_session(&self) -> Result<tidb_ddl_session::DdlSession, MysqlError> {
        Ok(ddl_session(self.resources.open_fresh()?, self.resources.target.global_vars.clone()))
    }

    fn open_step_session(
        &self,
        start_ts: u64,
        buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
    ) -> Result<tidb_ddl_session::DdlSession, MysqlError> {
        let transactions: Arc<dyn ClusterTransactions> = Arc::new(StepTransactions {
            inner: Arc::clone(&self.resources.target.transactions),
            start_ts,
        });
        let mut session = self.resources.open(transactions, buffer.clone())?;
        // Go `sess.Begin`: `sessiontxn.NewTxn` enters an optimistic transaction.
        session
            .node_conn()
            .new_txn(tidb_ast::TransactionMode::Optimistic)
            .map_err(sql_to_mysql)?;
        Ok(step_session(session, self.resources.target.global_vars.clone()))
    }
}

fn sql_to_mysql(error: tidb_store_driver_error::sql_query_error::SqlQueryError) -> MysqlError {
    MysqlError::from_parts(error.code, error.state, error.message).with_identity_opt(error.identity)
}

/// Go `sess.NewSession(se)` over a pooled system session.
pub(crate) fn ddl_session(
    session: crate::Session,
    global_vars: crate::GlobalSysvars,
) -> tidb_ddl_session::DdlSession {
    tidb_ddl_session::Session::new(Arc::new(
        crate::domain::cluster_session_node::ClusterStatsSessionContext::new(session, global_vars),
    ))
}

/// A node system session serving as one DDL step's `w.sess`; dropping the
/// last handle ends it and leaves its writes in the step's buffer.
fn step_session(
    session: crate::Session,
    global_vars: crate::GlobalSysvars,
) -> tidb_ddl_session::DdlSession {
    tidb_ddl_session::Session::new(Arc::new(
        crate::domain::cluster_session_node::ClusterStatsSessionContext::new_step(session, global_vars),
    ))
}

/// Go `w.sess`'s transaction as the DDL job step owns it: every read is at
/// the step's start timestamp, and nothing commits on its own -- the
/// session's statements stage into the step's buffer, which the step commits
/// with its meta writes. Anything that would commit outside the step is
/// refused.
struct StepTransactions {
    inner: Arc<dyn ClusterTransactions>,
    start_ts: u64,
}

fn outside_the_step() -> String {
    "a DDL step session commits only with its step".to_owned()
}

impl ClusterTransactions for StepTransactions {
    /// Go `session.GetStore`: a DDL step retains its session's store.
    fn kv_storage(&self) -> Arc<dyn tidb_txnkv::kv_storage::Storage> { self.inner.kv_storage() }
    /// Go `session.GetStore().GetCodec().GetKeyspaceID`.
    fn keyspace_id(&self) -> u32 { self.inner.keyspace_id() }
    fn fresh_timestamp(&self) -> Result<u64, String> {
        self.inner.fresh_timestamp()
    }

    fn external_timestamp(&self, value: Option<u64>) -> Result<u64, String> {
        self.inner.external_timestamp(value)
    }

    fn prepare_snapshot(
        &self,
        resource_group: &str,
        low_resolution: bool,
    ) -> Result<Box<dyn tidb_domain_core::cluster_transactions::PendingClusterSnapshot>, String> {
        self.inner.prepare_snapshot(resource_group, low_resolution)
    }

    fn open_snapshot(
        &self,
        resource_group: &str,
    ) -> Result<Box<dyn tidb_kv::cluster_storage::ClusterSnapshot>, String> {
        self.inner.open_snapshot_at(self.start_ts, resource_group)
    }

    fn open_snapshot_at(
        &self,
        read_ts: u64,
        resource_group: &str,
    ) -> Result<Box<dyn tidb_kv::cluster_storage::ClusterSnapshot>, String> {
        self.inner.open_snapshot_at(read_ts, resource_group)
    }

    fn open_max_ts_snapshot(
        &self,
        resource_group: &str,
    ) -> Result<Box<dyn tidb_kv::cluster_storage::ClusterSnapshot>, String> {
        self.inner.open_max_ts_snapshot(resource_group)
    }

    fn prepare_autocommit_txn(
        &self,
        _resource_group: &str,
        _pessimistic: bool,
        _fair_locking: bool,
        _low_resolution: bool,
        _options: tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions,
    ) -> Result<Box<dyn tidb_domain_core::cluster_transactions::PendingClusterTransaction>, String> {
        Err(outside_the_step())
    }

    fn commit(
        &self,
        _buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
        _read_ts: Option<u64>,
        _resource_group: &str,
        _options: tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions,
    ) -> Result<
        Option<tidb_domain_core::cluster_transactions::CommittedTxnInfo>,
        tidb_store_driver_error::sql_query_error::SqlQueryError,
    > {
        Err(tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(outside_the_step()))
    }

    fn begin(
        &self,
        _pessimistic: bool,
        _fair_locking: bool,
        _resource_group: &str,
        _options: tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions,
    ) -> Result<Box<dyn tidb_domain_core::cluster_transactions::OpenClusterTransaction>, String> {
        Ok(Box::new(StepTransaction {
            inner: Arc::clone(&self.inner),
            start_ts: self.start_ts,
            opened_at: std::time::Instant::now(),
            statements: std::sync::atomic::AtomicU64::new(0),
        }))
    }

}

/// The step's transaction as one session statement sees it.
struct StepTransaction {
    inner: Arc<dyn ClusterTransactions>,
    start_ts: u64,
    opened_at: std::time::Instant,
    statements: std::sync::atomic::AtomicU64,
}

impl tidb_domain_core::cluster_transactions::OpenClusterTransaction for StepTransaction {
    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    fn opened_at(&self) -> std::time::Instant {
        self.opened_at
    }

    fn statement_count(&self) -> u64 {
        self.statements.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn note_statement(&self) {
        self.statements.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn set_resource_group_name(&self, _name: &str) -> Result<(), String> {
        Ok(())
    }

    fn set_statement_assertion_level(
        &self,
        _level: tidb_proto::KvrpcAssertionLevel,
    ) -> Result<(), String> {
        Ok(())
    }

    fn snapshot(&self) -> Result<Box<dyn tidb_kv::cluster_storage::ClusterSnapshot>, String> {
        self.inner.open_snapshot_at(self.start_ts, "")
    }

    /// The writes stay in the step's buffer; the step commits them.
    fn commit(
        self: Box<Self>,
        _buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
    ) -> Result<
        Option<tidb_domain_core::cluster_transactions::CommittedTxnInfo>,
        tidb_store_driver_error::sql_query_error::SqlQueryError,
    > {
        Ok(None)
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
        Ok(())
    }
}

/// Recorded catalog fixtures using Go testkit's mock-store SQL sessions.
pub mod recorded;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tidb_infoschema_isvalidator::Validator;

    /// Go `TestDomainSchemaLease` (`zzprobe/y01_domain_lease`): the domain
    /// owns a positive, initially granted lease shared by its SQL sessions.
    #[test]
    fn test_domain_schema_lease() {
        let (_pd, opener) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        let pool = NodeDdlSessionPool::for_store(
            opener, Duration::from_secs(20),
            Arc::new(tidb_table::table_from_meta::LocalTableAutoIds::default()),
            true, Duration::from_millis(500),
        ).unwrap();
        let domain = &pool.store_domain.as_ref().unwrap().domain;
        let validator = domain.schema_validator.as_ref().expect("Go domain owns the validator");
        assert_eq!(validator.lease(), Duration::from_millis(500));
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
        assert!(!validator.is_lease_expired(i64::try_from(now.as_nanos()).unwrap()));
        let session = pool.open_sql_session().unwrap();
        let rows = tidb_ddl_build::session_pool::execute(&session, "select 1", "test", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], tidb_datatype::Datum::Int(1));
    }

    /// Go `TestExecuteInternalRestoresLabels` (`zzprobe/y01_internal_labels`):
    /// internal SQL restores its caller's labels on success and error.
    #[test]
    fn test_execute_internal_restores_labels() {
        let (_pd, opener) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        let pool = NodeDdlSessionPool::for_store(
            opener, Duration::from_secs(20),
            Arc::new(tidb_table::table_from_meta::LocalTableAutoIds::default()),
            true, Duration::from_millis(500),
        ).unwrap();
        let session = pool.open_sql_session().unwrap();
        tidb_topsql_state::enable_top_sql();
        let labels = tidb_cpuprofile::labels::with_labels(None, &[("outer", "ddl".to_owned())]);
        tidb_cpuprofile::labels::set_goroutine_labels(Some(labels.clone()));
        assert!(tidb_ddl_build::session_pool::execute(&session, "select 1", "test", &[]).is_ok());
        assert_eq!(tidb_cpuprofile::labels::goroutine_labels(), Some(labels.clone()));
        assert!(tidb_ddl_build::session_pool::execute(&session, "select * from test.missing_table", "test", &[]).is_err());
        assert_eq!(tidb_cpuprofile::labels::goroutine_labels(), Some(labels));
        tidb_topsql_state::disable_top_sql();
        tidb_cpuprofile::labels::set_goroutine_labels(None);
    }
}

#[cfg(test)]
mod keyspace_tests {
    use super::*;

    /// Go `session.NewSession` retains `GetStore` and its codec; zzprobe/o15_named_sql.
    #[test]
    fn test_named_keyspace_ddl_step_store() {
        if !tidb_config_kerneltype::is_next_gen() { return; }
        let domain = crate::domain::mock_store::MockStoreDomain::create().unwrap();
        let inner = domain.domain().transactions.clone();
        let step = StepTransactions { inner: inner.clone(), start_ts: 1 };
        assert_eq!(step.keyspace_id(), 16777214);
        assert!(Arc::ptr_eq(&step.kv_storage(), &inner.kv_storage()));
    }
}
