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

/// Go `prealloctableid.Allocator`: `meta.Mutator`'s global ID counter.
pub trait GlobalIdAllocator {
    /// Go `GetGlobalID`.
    ///
    /// # Errors
    /// The read error.
    fn get_global_id(&self) -> Result<i64>;
    /// Go `AdvanceGlobalIDs(n)`: the counter before the advance.
    ///
    /// # Errors
    /// The write error.
    fn advance_global_ids(&self, n: i32) -> Result<i64>;
}

/// Go `infoschema.InfoSchema`, the reads restore does. Names compare
/// case-insensitively, as Go's.
pub trait InfoSchema: Send + Sync {
    /// Go `SchemaByName`.
    fn schema_by_name(&self, db: &str) -> Option<DBInfo>;
    /// Go `TableByName(...).Meta()`: `None` for a missing table.
    fn table_by_name(&self, db: &str, table: &str) -> Option<TableInfo>;
    /// Go `SchemaTableInfos`.
    fn schema_table_infos(&self, db: &str) -> Vec<TableInfo>;
    /// Go `AllSchemas`.
    fn all_schemas(&self) -> Vec<DBInfo>;
    /// Go `TableExists`.
    fn table_exists(&self, db: &str, table: &str) -> bool {
        self.table_by_name(db, table).is_some()
    }
}

/// Go `statstypes.MetaUpdate`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetaUpdate {
    /// Go `PhysicalID`.
    pub physical_id: i64,
    /// Go `Count`.
    pub count: i64,
    /// Go `ModifyCount`.
    pub modify_count: i64,
}

/// Go `GetTiFlashProgressStores`'s `(tiFlashStores, tikvStores)`.
pub type TiFlashProgressStores = (
    Vec<tikv_client::pd_http::StoreInfo>,
    Vec<tikv_client::pd_http::StoreInfo>,
);

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
    /// Go `kv.RunInNewTxn(ctx, dom.Store(), true, func(txn) {
    /// f(meta.NewMutator(txn)) })` under `kv.InternalTxnBR`: the global ID
    /// allocator restore preallocates table IDs from.
    ///
    /// # Errors
    /// The transaction's or `f`'s error.
    fn run_in_new_meta_txn(
        &self,
        _f: &mut dyn FnMut(&dyn GlobalIdAllocator) -> Result<()>,
    ) -> Result<()> {
        Err(crate::errors::errorf(
            "RunInNewTxn is not supported by this domain",
        ))
    }
    /// Go `InfoSchema()`: the latest schema.
    ///
    /// # Errors
    /// The schema cannot be loaded.
    fn info_schema(&self) -> Result<Arc<dyn InfoSchema>> {
        Err(crate::errors::errorf(
            "InfoSchema is not supported by this domain",
        ))
    }
    /// Go `StatsHandle().LoadStatsFromJSONNoUpdate(ctx, InfoSchema(),
    /// jsonTbl, 0)`: a table's statistics, its `database_name` /
    /// `table_name` naming the target table.
    ///
    /// # Errors
    /// The load error.
    fn load_stats_from_json_no_update(
        &self,
        _ctx: &Context,
        _json_table: &tidb_stats_util::JsonTable,
    ) -> Result<()> {
        Err(crate::errors::errorf(
            "LoadStatsFromJSONNoUpdate is not supported by this domain",
        ))
    }
    /// Go `StatsHandle().LoadStatsFromJSONConcurrently(ctx, table, tasks, 0)`:
    /// each `(physical ID, statistics)` partition task into `table`.
    ///
    /// # Errors
    /// The load error.
    fn load_partition_stats_from_json(
        &self,
        _ctx: &Context,
        _table: &TableInfo,
        _tasks: Vec<(i64, tidb_stats_util::JsonTable)>,
    ) -> Result<()> {
        Err(crate::errors::errorf(
            "LoadStatsFromJSONConcurrently is not supported by this domain",
        ))
    }
    /// Go `StatsHandle().SaveMetaToStorage(false, updates...)`.
    ///
    /// # Errors
    /// The write error.
    fn save_meta_to_storage(&self, _updates: &[MetaUpdate]) -> Result<()> {
        Err(crate::errors::errorf(
            "SaveMetaToStorage is not supported by this domain",
        ))
    }
    /// Go `NotifyUpdateAllUsersPrivilege`.
    ///
    /// # Errors
    /// The notification error.
    fn notify_update_all_users_privilege(&self) -> Result<()> {
        Err(crate::errors::errorf(
            "NotifyUpdateAllUsersPrivilege is not supported by this domain",
        ))
    }
    /// Go `GetEtcdClient()`: PD's etcd, `None` without one.
    fn etcd_client(&self) -> Option<Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>> {
        None
    }
    /// Go `Store().GetCodec()`, as the region label rules encode ranges.
    fn label_codec(&self) -> Box<dyn tidb_ddl_label::LabelCodec> {
        Box::new(tidb_ddl_label::CodecV1)
    }
    /// Go `infosync.UpdateLabelRules`.
    ///
    /// # Errors
    /// PD's error.
    fn update_label_rules(&self, _patch: &tidb_ddl_label::LabelRulePatch) -> Result<()> {
        Err(crate::errors::errorf(
            "UpdateLabelRules is not supported by this domain",
        ))
    }
    /// Go `infosync.GetTiFlashProgressStores`: the TiFlash write stores and
    /// the TiKV stores.
    ///
    /// # Errors
    /// PD's error.
    fn tiflash_progress_stores(&self) -> Result<TiFlashProgressStores> {
        Err(crate::errors::errorf(
            "GetTiFlashProgressStores is not supported by this domain",
        ))
    }
    /// Go `infosync.MustGetTiFlashProgress(ctx, id, replicaCount, tiflash,
    /// tikv)`: the replica sync progress of a physical table, in `[0, 1]`.
    ///
    /// # Errors
    /// PD's or a store's error.
    fn tiflash_progress(
        &self,
        _ctx: &Context,
        _physical_id: i64,
        _replica_count: u64,
        _stores: &TiFlashProgressStores,
    ) -> Result<f64> {
        Err(crate::errors::errorf(
            "MustGetTiFlashProgress is not supported by this domain",
        ))
    }
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
    /// Go `GetSessionCtx().GetRestrictedSQLExecutor().ExecRestrictedSQL`:
    /// the rows of `sql` with `%?` / `%n` arguments, under
    /// `kv.InternalTxnBR`.
    ///
    /// # Errors
    /// The statement's error.
    fn exec_restricted_sql(
        &self,
        _ctx: &Context,
        _sql: &str,
        _args: &[SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>> {
        Err(crate::errors::errorf(
            "ExecRestrictedSQL is not supported by this session",
        ))
    }
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
