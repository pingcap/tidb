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

//! Go `pkg/resourcegroup/runaway`: the runaway checker each statement of a
//! resource group with runaway settings or active watches carries, the
//! manager's watch list, the `mysql.tidb_runaway_queries` /
//! `mysql.tidb_runaway_watch` flushers and the watch syncer.

#![allow(missing_docs)]

pub mod checker;
mod flusher;
pub mod manager;
pub mod record;
mod syncer;
mod ttlcache;

pub use checker::Checker;
pub use manager::{ExitSignal, Manager, MANUAL_SOURCE, MAX_WAIT_DURATION};
pub use record::{null_time, QuarantineRecord, Record};


/// Go `*rmclient.ResourceGroupsController`'s `GetResourceGroup`.
pub trait ResourceGroupsController: Send + Sync {
    /// The group's meta, `None` when it does not exist.
    ///
    /// # Errors
    /// Whatever the resource manager reports.
    fn get_resource_group(
        &self,
        name: &str,
    ) -> Result<Option<tidb_proto::resource_manager::ResourceGroup>, String>;
}

/// Go `sessionctx.Context` as the runaway records drive a pooled session.
pub trait SqlSession {
    /// Go `GetSQLExecutor()`.
    fn get_sql_executor(&self) -> std::sync::Arc<dyn tidb_sqlexec::SqlExecutor>;

    /// Go `GetRestrictedSQLExecutor()`.
    fn get_restricted_sql_executor(&self) -> std::sync::Arc<dyn tidb_sqlexec::RestrictedSqlExecutor>;
}

/// Go `util.SessionPool`: `body` runs on a session got from the pool, which
/// is put back afterwards.
pub trait SessionPool: Send + Sync {
    /// # Errors
    /// The pool's `Get` error.
    fn with_session(&self, body: &mut dyn FnMut(&dyn SqlSession)) -> Result<(), String>;
}

/// Go `*infoschema.InfoCache`'s latest schema, as the syncer and the record
/// GC read it.
pub trait InfoCache: Send + Sync {
    /// Go `is.TableExists(schema, table)`.
    fn table_exists(&self, schema: &str, table: &str) -> bool;

    /// Go `is.TableByName(ctx, schema, table)`'s meta, `None` when the
    /// table or the schema is missing.
    fn table_by_name(&self, schema: &str, table: &str) -> Option<tidb_model::GoShared<tidb_model::TableInfo>>;
}

/// Go `ddl.DDL.OwnerManager().IsOwner()`.
pub trait DdlOwner: Send + Sync {
    fn is_owner(&self) -> bool;
}

#[cfg(test)]
mod tests;
