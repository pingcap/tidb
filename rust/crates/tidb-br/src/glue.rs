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

//! Go `br/pkg/glue/glue.go`: what BR needs from the TiDB it runs in. Inside
//! TiDB (BRIE statements) the executor implements it (Go `tidbGlue` in
//! `pkg/executor/brie.go`).
//!
//! Go's `Glue.GetDomain` answers TiDB's `*domain.Domain` and
//! `Session.GetSessionCtx` a `sessionctx.Context`; BR reaches them for a
//! few operations, which [`Domain`] and [`Session`] name directly.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_model::{DBInfo, PolicyInfo, TableInfo};
use tidb_sqlescape::SqlArg;

use crate::errors::Result;

/// Go `GlueClient`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GlueClient {
    /// Go `ClientCLP`: the `br` command line.
    Clp,
    /// Go `ClientSql`: a BRIE statement.
    Sql,
}

/// Go `kv.Storage` and `tikv.Storage` of the cluster BR works on.
#[derive(Clone)]
pub struct Storage {
    /// Go `tikv.Storage`.
    pub tikv: tidb_txnkv::driver::tikv_store::TikvStore,
    /// Go `kv.Storage`.
    pub kv: Arc<dyn tidb_txnkv::kv_storage::Storage>,
    /// Go `GetCodec().GetKeyspaceID()`: the keyspace ID, `NullspaceID`
    /// without one.
    pub keyspace_id: u32,
    /// Go `GetCodec().GetKeyspace()`: the keyspace's key prefix, empty
    /// without one.
    pub keyspace_prefix: Vec<u8>,
    /// Go `GetCodec().GetAPIVersion()`.
    pub api_version: i32,
}

/// Go `*domain.Domain`, the parts BR reads.
pub trait Domain: Send + Sync {}

/// Go `Glue`.
pub trait Glue: Send + Sync {
    /// Go `GetDomain`.
    ///
    /// # Errors
    /// The domain cannot be loaded.
    fn get_domain(&self) -> Result<Arc<dyn Domain>>;
    /// Go `CreateSession`.
    ///
    /// # Errors
    /// A session cannot be created.
    fn create_session(&self) -> Result<Box<dyn Session>>;
    /// Go `Open`: the cluster's storage.
    ///
    /// # Errors
    /// The storage cannot be opened.
    fn open(&self) -> Result<Storage>;
    /// Go `OwnsStorage`.
    fn owns_storage(&self) -> bool;
    /// Go `StartProgress`.
    fn start_progress(&self, ctx: &Context, cmd_name: &str, total: i64, redirect_log: bool) -> Arc<dyn Progress>;
    /// Go `Record`.
    fn record(&self, name: &str, value: u64);
    /// Go `GetVersion`.
    fn get_version(&self) -> String;
    /// Go `UseOneShotSession`.
    ///
    /// # Errors
    /// A session cannot be created, or `f`'s error.
    fn use_one_shot_session(&self, close_domain: bool, f: &mut dyn FnMut(&dyn Session) -> Result<()>) -> Result<()>;
    /// Go `GetClient`.
    fn get_client(&self) -> GlueClient;
}

/// Go `ddl.CreateTableOption`s BR passes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CreateTableConfig {
    /// Go `ddl.WithOnExist(ddl.OnExistIgnore)`.
    pub on_exist_ignore: bool,
    /// Go `ddl.WithIDAllocated(true)`: the table IDs are preallocated.
    pub id_allocated: bool,
}

/// Go `Session`.
pub trait Session: Send {
    /// Go `Execute`.
    ///
    /// # Errors
    /// The statement's error.
    fn execute(&self, ctx: &Context, sql: &str) -> Result<()>;
    /// Go `ExecuteInternal`: `sql` with `%?` arguments.
    ///
    /// # Errors
    /// The statement's error.
    fn execute_internal(&self, ctx: &Context, sql: &str, args: &[SqlArg<'_>]) -> Result<()>;
    /// Go `CreateDatabaseOnExistError`.
    ///
    /// # Errors
    /// The DDL error, including an existing database.
    fn create_database_on_exist_error(&self, ctx: &Context, schema: &DBInfo) -> Result<()>;
    /// Go `CreateTable`.
    ///
    /// # Errors
    /// The DDL error.
    fn create_table(&self, ctx: &Context, db_name: &str, table: &TableInfo, cfg: CreateTableConfig) -> Result<()>;
    /// Go `BatchCreateTableSession.CreateTables`: `None` when the session
    /// cannot create tables in batches.
    ///
    /// # Errors
    /// The DDL error.
    fn create_tables(
        &self,
        ctx: &Context,
        tables: &[(String, Vec<TableInfo>)],
        cfg: CreateTableConfig,
    ) -> Option<Result<()>>;
    /// Go `CreatePlacementPolicy`.
    ///
    /// # Errors
    /// The DDL error.
    fn create_placement_policy(&self, ctx: &Context, policy: &PolicyInfo) -> Result<()>;
    /// Go `GetGlobalVariable`: from `mysql.tidb`.
    ///
    /// # Errors
    /// The read error.
    fn get_global_variable(&self, name: &str) -> Result<String>;
    /// Go `GetGlobalSysVar`.
    ///
    /// # Errors
    /// An unknown variable, or the read error.
    fn get_global_sys_var(&self, name: &str) -> Result<String>;
    /// Go `AlterTableMode`.
    ///
    /// # Errors
    /// The DDL error.
    fn alter_table_mode(&self, ctx: &Context, schema_id: i64, table_id: i64, mode: tidb_model::TableMode) -> Result<()>;
}

/// Go `Progress`.
pub trait Progress: Send + Sync {
    /// Go `Inc`.
    fn inc(&self);
    /// Go `IncBy`.
    fn inc_by(&self, cnt: i64);
    /// Go `GetCurrent`.
    fn get_current(&self) -> i64;
    /// Go `Close`.
    fn close(&self);
}

/// Go `WithProgress`.
///
/// # Errors
/// `cc`'s error.
pub fn with_progress<T>(
    ctx: &Context,
    g: &dyn Glue,
    cmd_name: &str,
    total: i64,
    redirect_log: bool,
    cc: impl FnOnce(&Arc<dyn Progress>) -> Result<T>,
) -> Result<T> {
    let p = g.start_progress(ctx, cmd_name, total, redirect_log);
    let res = cc(&p);
    p.close();
    res
}
