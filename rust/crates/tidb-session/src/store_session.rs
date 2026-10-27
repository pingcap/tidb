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

//! A [`Session`] that runs its statements over a store by itself.
//!
//! Go's `session` is one object: the session variables, the `LazyTxn` it
//! drives through `sessiontxn.TxnManager`, and the statement loop of
//! `ExecStmt`. Here the first of those is [`Session`]; a [`StoreSession`] is
//! that session together with the rest -- its [`TxnManager`], the snapshot
//! slot every table of its catalog reads through, and the follower that keeps
//! the catalog current -- so that the server driving it hands in only what is
//! genuinely its own: the catalog source and the commit hooks.

use tidb_ast::Stmt;
use tidb_cluster::sql_query_error::SqlQueryError;
use tidb_executor::access_path::StatementReadShape;
use tidb_executor::cluster_storage::BufferCheckpoint;
use tidb_executor::DriverError;

use crate::statement_lifecycle::{
    finish_snapshot, CatalogFollowing, SnapshotSlot, StatementLifecycle,
};
use crate::txn::driver_error_of;
use crate::txn_manager::{CommitHooks, CommitTxnRegions, TxnManager};
use crate::{Session, StmtResult, TxnManagerOps};

/// A driver error as the client sees it: Go's code, SQLSTATE and message, with
/// the error's own identity and its typed cause (Go `errors.Cause`) carried
/// across.
#[must_use]
pub fn sql_query_error(error: tidb_executor::DriverError) -> SqlQueryError {
    let mapped = error.clone().to_mysql_error();
    SqlQueryError::new(mapped.code, mapped.state, mapped.message)
        .with_identity(mapped.identity)
        .with_cause(error)
}

/// Builds what a commit reads off the server, for the schema version the
/// statement planned against.
pub type CommitHooksFactory = Box<dyn Fn(&Session, i64) -> Box<dyn CommitHooks> + Send>;

/// See the module documentation.
pub struct StoreSession<F: CatalogFollowing> {
    /// The session variables and the in-process statement machinery.
    pub session: Session,
    /// Go `sessiontxn.TxnManager` with its `LazyTxn`.
    pub txn: TxnManager,
    /// The slot every table of `session` reads through; rebound per statement.
    pub slot: SnapshotSlot,
    /// The catalog the session follows.
    pub follower: F,
    /// Whether the running write is one whose transaction Go's executor
    /// builder activates before it runs (`updateForUpdateTS` ->
    /// `GetStmtForUpdateTS`).
    pub activate_statement_txn: bool,
    commit_hooks: CommitHooksFactory,
}

impl<F: CatalogFollowing> StoreSession<F> {
    /// A session over its transaction manager, slot and catalog follower.
    pub fn new(
        session: Session,
        txn: TxnManager,
        slot: SnapshotSlot,
        follower: F,
        commit_hooks: CommitHooksFactory,
    ) -> Self {
        Self {
            session,
            txn,
            slot,
            follower,
            activate_statement_txn: false,
            commit_hooks,
        }
    }

    /// What a commit reads off the server, for the schema version the session
    /// is following now.
    #[must_use]
    pub fn commit_hooks(&self) -> Box<dyn CommitHooks> {
        (self.commit_hooks)(&self.session, self.follower.schema_version())
    }

    /// The borrowed context one statement runs in.
    pub fn lifecycle(&mut self) -> StatementLifecycle<'_> {
        let hooks = self.commit_hooks();
        StatementLifecycle {
            session: &mut self.session,
            txn: &mut self.txn,
            slot: &self.slot,
            follower: &mut self.follower,
            hooks,
            activate_statement_txn: self.activate_statement_txn,
        }
    }

    /// The `SimpleExec` transaction statements (`BEGIN`, `COMMIT`,
    /// `ROLLBACK`, `SAVEPOINT`, `RELEASE SAVEPOINT`) over this session's
    /// transaction manager: [`Session::control_transaction_stmt`] with the
    /// store half through [`StoreTxnOps`].
    pub fn control_transaction_stmt(&mut self, stmt: &Stmt) -> Result<Option<bool>, SqlQueryError> {
        let hooks = self.commit_hooks();
        let mut ops = StoreTxnOps {
            txn: &mut self.txn,
            slot: &self.slot,
            follower: &mut self.follower,
            hooks,
        };
        self.session
            .control_transaction_stmt(stmt, &mut ops)
            .map_err(sql_query_error)
    }

    /// Go `sessiontxn.NewTxnInStmt` for the executor of the running
    /// statement: [`Session::new_txn_in_stmt`] over this session's
    /// transaction manager, when a transaction is open.
    pub fn new_txn_in_stmt(&mut self) -> Result<(), SqlQueryError> {
        if self.txn.explicit.is_none() && !self.session.in_transaction() {
            return Ok(());
        }
        let hooks = self.commit_hooks();
        let mut ops = StoreTxnOps {
            txn: &mut self.txn,
            slot: &self.slot,
            follower: &mut self.follower,
            hooks,
        };
        self.session
            .new_txn_in_stmt(&mut ops)
            .map_err(sql_query_error)
    }

    /// Rebinds the session's tables to the latest catalog, unless a
    /// transaction has pinned the one it started with.
    pub fn rebuild_catalog_if_stale(&mut self) {
        self.follower.rebuild_if_stale(&mut self.session, &self.txn);
    }

    /// Go `PrepareTxnCtx` for the statement about to run: follows the catalog,
    /// then enters the `autocommit = 0` transaction (pending: it takes a
    /// timestamp at the first statement that activates it).
    pub fn prepare_statement_context(
        &mut self,
        resource_group: &str,
        read_only: bool,
    ) -> Result<(), SqlQueryError> {
        self.rebuild_catalog_if_stale();
        if read_only && self.txn.explicit.is_none() && !self.session.in_transaction() {
            let vars = self.session.vars();
            if vars.snapshot_ts() != 0
                || vars
                    .get_system("tidb_read_staleness")
                    .is_ok_and(|v| v.parse::<i64>().is_ok_and(|v| v != 0))
            {
                return Ok(());
            }
        }
        self.txn.enter_if_autocommit_off(
            &self.session,
            resource_group,
            self.follower.schema_version(),
        );
        // Go `SetOptionsOnTxnActive` reads the session values when the
        // transaction activates, so a pending one follows each statement's.
        if let Some(transaction) = &self.txn.explicit {
            transaction
                .set_activation_options(TxnManager::statement_transaction_options(&self.session));
        }
        Ok(())
    }

    /// Runs one client statement after refreshing the catalog and opening any
    /// implicit transaction. A pessimistic statement's locking point reads
    /// lock inside execution with returned row values (Go
    /// `PointGetExecutor.getAndLock`, `InitReturnValues` /
    /// `SetPessimisticLockCache`), through `TableStorage::lock_keys_for_read`.
    pub fn with_prelocked_statement<T>(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
        is_dml: bool,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.prepare_statement_context(
            resource_group,
            shape != StatementReadShape::AutocommitWrite,
        )?;
        self.lifecycle()
            .with_bound_statement(shape, resource_group, true, is_dml, run)
    }

    /// [`Self::with_prelocked_statement`] for work the CLIENT did not ask to
    /// run: the PREPARE probe, which executes a query only to learn its result
    /// columns.
    ///
    /// The one thing it must not do is open the transaction `autocommit = 0`
    /// implies. Go's `PrepareStmt` calls `PrepareTxnCtx` too
    /// (`pkg/session/session.go:3171`), but with a nil statement and through
    /// `EnterNewTxnBeforeStmt`, which leaves the transaction *pending*: no
    /// timestamp is spent and the first statement that really reads is what
    /// takes one. Captured: `@@tidb_current_ts` is `0` after a PREPARE under
    /// `autocommit = 0`, and a read issued after another session's commit sees
    /// the NEW value. An already-open transaction is read through as usual.
    pub fn probe_statement<T>(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.lifecycle()
            .with_bound_statement(shape, resource_group, false, false, run)
    }

    /// Whether Go's executor builder activates `stmt`'s transaction before
    /// the statement runs: `buildInsert` (INSERT and REPLACE), `buildUpdate`
    /// and `buildDelete` call `updateForUpdateTS`, whose
    /// `GetStmtForUpdateTS` activates it. A text `EXECUTE` builds the
    /// statement it names.
    #[must_use]
    pub fn activates_txn_at_build(&self, stmt: &Stmt) -> bool {
        let stmt = match stmt {
            Stmt::Session(session) => match session.as_ref() {
                tidb_ast::SessionStmt::Execute { name, .. } => {
                    match self.session.prepared_statement_node(name) {
                        Some(prepared) => prepared,
                        None => return false,
                    }
                }
                _ => return false,
            },
            stmt => stmt,
        };
        matches!(
            stmt,
            Stmt::Dml(dml) if matches!(
                dml.as_ref(),
                tidb_ast::DmlStmt::Insert(_)
                    | tidb_ast::DmlStmt::Update(_)
                    | tidb_ast::DmlStmt::Delete(_)
            )
        )
    }

    /// Runs an ordinary write statement (INSERT, UPDATE, DELETE and the like)
    /// through the statement lifecycle and answers its affected-row count and
    /// last insert id.
    pub fn run_ordinary_write(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<(u64, u64), SqlQueryError> {
        let owned = sql.to_owned();
        let resource_group = self.session.statement_resource_group(stmt).into_owned();
        // A write declares nothing. Its read-before-write reaches the snapshot
        // as the same `get` a point-get SELECT issues, which is exactly why the
        // declaration is made from the statement rather than from the read.
        //
        // Go's pessimistic point write folds its row read INTO its lock inside
        // execution (`PointGetExecutor.getAndLock`,
        // `pkg/executor/point_get.go:612-624`): the executor locks the keys its
        // plan names through `TableStorage::lock_keys_for_read`, so nothing is
        // classified or locked here before the statement compiles.
        self.activate_statement_txn = self.activates_txn_at_build(stmt);
        let attempt = self.with_prelocked_statement(
            StatementReadShape::Unknown,
            &resource_group,
            true,
            move |session| match session
                .run_parsed(stmt.clone(), &owned)
                .map_err(sql_query_error)?
            {
                StmtResult::Affected(count) => Ok(count),
                StmtResult::Done(_) => Ok(0),
                StmtResult::Rows(_) => Err(SqlQueryError::unknown(
                    "a write statement unexpectedly produced rows",
                )),
            },
        );
        self.activate_statement_txn = false;
        let affected_rows = attempt?;
        self.session.add_statement_read_keys(affected_rows);
        Ok((affected_rows, self.session.statement_insert_id()))
    }
}

/// Go `sessiontxn.GetTxnManager(sctx)` as the `SimpleExec` transaction arms
/// reach it: the store session's [`TxnManager`] with the snapshot slot, the
/// catalog follower and the commit hooks it needs.
struct StoreTxnOps<'a, F: CatalogFollowing> {
    txn: &'a mut TxnManager,
    slot: &'a SnapshotSlot,
    follower: &'a mut F,
    hooks: Box<dyn CommitHooks>,
}

impl<F: CatalogFollowing> TxnManagerOps for StoreTxnOps<'_, F> {
    fn commit_txn(&mut self, session: &mut Session) -> Result<(), DriverError> {
        let _commit_txn = CommitTxnRegions::begin(true);
        finish_snapshot(self.slot).map_err(driver_error_of)?;
        self.txn
            .commit(session, self.hooks.as_ref())
            .map_err(driver_error_of)
    }

    fn rollback_txn(&mut self, session: &mut Session) -> Result<(), DriverError> {
        finish_snapshot(self.slot).map_err(driver_error_of)?;
        self.txn.rollback(session).map_err(driver_error_of)
    }

    fn enter_new_txn_with_begin(&mut self, session: &mut Session) -> Result<(), DriverError> {
        // The refresh runs before the transaction pins its schema view: Go
        // activates a transaction with the LATEST schema at start, on a
        // connection of any age.
        self.follower.rebuild(session);
        let resource_group = session.current_resource_group().to_owned();
        self.txn
            .open(session, &resource_group, self.follower.schema_version())
            .map_err(driver_error_of)
    }

    fn savepoint_checkpoint(
        &mut self,
        session: &mut Session,
    ) -> Result<Option<BufferCheckpoint>, DriverError> {
        let resource_group = session.current_resource_group().to_owned();
        self.txn
            .savepoint_checkpoint(session, &resource_group, self.follower.schema_version())
            .map_err(driver_error_of)
    }

    fn rollback_to_checkpoint(&mut self, checkpoint: &BufferCheckpoint) {
        self.txn.rollback_to_checkpoint(checkpoint);
    }
}
