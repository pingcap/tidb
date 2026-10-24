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

use crate::statement_lifecycle::{CatalogFollowing, SnapshotSlot, StatementLifecycle};
use crate::txn_manager::{CommitHooks, TxnManager};
use crate::Session;

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
}
