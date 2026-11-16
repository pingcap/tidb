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

/// Go `kv.Snapshot`: reads of the cluster at one timestamp.
pub trait KvSnapshot: Send {
    /// Go `Get`: `None` for a missing key.
    ///
    /// # Errors
    /// The read error.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Go `Iter` over `[start, end)`, up to `limit` pairs.
    ///
    /// # Errors
    /// The read error.
    fn scan(&mut self, start: &[u8], end: &[u8], limit: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// Go `kv.Storage` and `tikv.Storage` of the cluster BR works on.
pub trait Storage: Send + Sync {
    /// Go `tikv.Storage`: the TiKV store.
    fn tikv(&self) -> &tidb_txnkv::driver::tikv_store::TikvStore;
    /// Go `GetSnapshot(kv.NewVersion(ts))`.
    fn get_snapshot(&self, ts: u64) -> Box<dyn KvSnapshot>;
    /// Go `CurrentVersion(kv.GlobalTxnScope)`.
    ///
    /// # Errors
    /// PD's error.
    fn current_version(&self) -> Result<u64>;
    /// Go `GetClient`: the coprocessor client.
    fn get_client(&self) -> Arc<dyn tidb_txnkv::Client>;
    /// Go `GetCodec().GetKeyspaceID()`: `NullspaceID` without a keyspace.
    fn keyspace_id(&self) -> u32;
    /// Go `GetCodec().GetKeyspace()`: the keyspace's key prefix, empty
    /// without one.
    fn keyspace_prefix(&self) -> Vec<u8>;
    /// Go `GetCodec().GetAPIVersion()`.
    fn api_version(&self) -> i32;
    /// Go `GetCodec().EncodeRange`: the range in the store's key space.
    fn encode_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let prefix = self.keyspace_prefix();
        if prefix.is_empty() {
            return (start.to_vec(), end.to_vec());
        }
        let encode = |k: &[u8]| [prefix.as_slice(), k].concat();
        let end = if end.is_empty() {
            // The keyspace's range ends at the next keyspace prefix.
            let mut next = prefix.clone();
            for i in (0..next.len()).rev() {
                if next[i] < 0xff {
                    next[i] += 1;
                    next.truncate(i + 1);
                    break;
                }
            }
            next
        } else {
            encode(end)
        };
        (encode(start), end)
    }
}

/// A PD region label rule: its ID and `(key, value)` labels.
pub type LabelRule = (String, Vec<(String, String)>);

/// Go `*domain.Domain`, the parts BR reads.
pub trait Domain: Send + Sync {
    /// Go `statsHandle.PersistStatsBySnapshot`: each physical table's
    /// statistics at `snapshot`, as Go `util.JSONTable` JSON (`None` for Go's
    /// nil table), handed to `persist` with the physical table ID.
    ///
    /// # Errors
    /// The read error, or `persist`'s.
    fn persist_stats_by_snapshot(
        &self,
        db_name: &str,
        table: &TableInfo,
        snapshot: u64,
        persist: &mut dyn FnMut(Option<Vec<u8>>, i64) -> Result<()>,
    ) -> Result<()>;
    /// Go `infosync.GetLabelRules`: the PD region label rules with the IDs,
    /// as `(rule ID, [(key, value)])`.
    ///
    /// # Errors
    /// PD's error.
    fn get_label_rules(&self, rule_ids: &[String]) -> Result<Vec<LabelRule>>;
}
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
    fn open(&self) -> Result<Arc<dyn Storage>>;
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
    /// Go `ddl.GetAllDDLJobs(se.GetSessionCtx())`: the queued DDL jobs.
    ///
    /// # Errors
    /// The read error.
    fn get_all_ddl_jobs(&self, ctx: &Context) -> Result<Vec<tidb_model::Job>>;
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
