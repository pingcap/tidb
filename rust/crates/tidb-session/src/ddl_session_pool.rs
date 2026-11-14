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

    fn open_sql_session(
        &self,
    ) -> Result<Box<dyn tidb_ddl_build::session_pool::StepSqlSession>, MysqlError> {
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("the DDL session pool has no running node"))?;
        let mut session = domain.open_system_session().map_err(sql_to_mysql)?;
        set_ddl_request_source(&mut session);
        Ok(Box::new(NodeSqlSession::new(session)))
    }

    fn has_step_sessions(&self) -> bool {
        self.domain.get().and_then(Weak::upgrade).is_some()
    }

    fn open_step_session(
        &self,
        start_ts: u64,
        buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
    ) -> Result<Box<dyn tidb_ddl_build::session_pool::StepSqlSession>, MysqlError> {
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
        Ok(Box::new(NodeStepSession(NodeSqlSession::new(session))))
    }
}

fn sql_to_mysql(error: tidb_store_driver_error::sql_query_error::SqlQueryError) -> MysqlError {
    MysqlError::from_parts(error.code, error.state, error.message).with_identity_opt(error.identity)
}

/// A node system session the DDL runs SQL on (Go `sess.NewSession(se)`
/// over a pooled session).
pub(crate) struct NodeSqlSession(crate::domain::cluster_session_node::SystemSqlExecutor);

impl NodeSqlSession {
    pub(crate) fn new(session: crate::Session) -> Self {
        Self(crate::domain::cluster_session_node::SystemSqlExecutor::new(session))
    }
}

impl tidb_ddl_build::session_pool::StepSqlSession for NodeSqlSession {
    fn get_sql_executor(&self) -> &dyn tidb_sqlexec::SqlExecutor {
        &self.0
    }

    fn begin(&mut self) -> Result<(), MysqlError> {
        self.0
            .with_session(|session| session.node_conn().new_txn(tidb_ast::TransactionMode::Optimistic))
            .map_err(sql_to_mysql)
    }

    fn begin_pessimistic(&mut self) -> Result<(), MysqlError> {
        self.0
            .with_session(|session| session.node_conn().new_txn(tidb_ast::TransactionMode::Pessimistic))
            .map_err(sql_to_mysql)
    }

    fn commit(&mut self) -> Result<(), MysqlError> {
        self.0.with_session(|session| session.node_conn().commit_txn()).map_err(sql_to_mysql)
    }

    fn rollback(&mut self) {
        // Go `RollbackTxn` returns nothing.
        let _ = self.0.with_session(|session| session.node_conn().rollback_txn());
    }
}

/// A node system session serving as one DDL step's `w.sess`.
struct NodeStepSession(NodeSqlSession);

impl tidb_ddl_build::session_pool::StepSqlSession for NodeStepSession {
    fn get_sql_executor(&self) -> &dyn tidb_sqlexec::SqlExecutor {
        self.0.get_sql_executor()
    }

    fn begin(&mut self) -> Result<(), MysqlError> {
        self.0.begin()
    }

    fn begin_pessimistic(&mut self) -> Result<(), MysqlError> {
        self.0.begin_pessimistic()
    }

    fn commit(&mut self) -> Result<(), MysqlError> {
        self.0.commit()
    }

    fn rollback(&mut self) {
        self.0.rollback();
    }
}

impl Drop for NodeStepSession {
    /// The session ends; its writes stay in the step's buffer, which the step
    /// commits (the transaction's own commit publishes nothing) -- closing
    /// would otherwise reset the buffer.
    fn drop(&mut self) {
        self.0 .0.with_session(|session| {
            let mut conn = session.node_conn();
            let _ = conn.commit_txn();
            conn.release_step_buffer();
        });
    }
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

    fn prepare_autocommit_write(
        &self,
        _resource_group: &str,
        _pessimistic: bool,
        _fair_locking: bool,
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
