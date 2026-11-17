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

//! An in-memory cluster behind the restore glue: sessions create schema
//! objects in it, the domain reads them back. Go's restore tests run a mock
//! TiDB for the same purpose.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use tidb_datatype::Datum;
use tidb_gocontext::Context;
use tidb_model::{DBInfo, PolicyInfo, TableInfo};
use tidb_sqlescape::SqlArg;

use crate::errors::{self, Result};
use crate::glue::{
    CreateTableConfig, Domain, GlobalIdAllocator, Glue, GlueClient, InfoSchema, LabelRule,
    MetaUpdate, Progress, Session, Storage,
};

/// What the cluster holds and what was done to it.
#[derive(Default)]
pub struct ClusterState {
    /// Databases by lowercase name, with their tables.
    pub dbs: BTreeMap<String, (DBInfo, Vec<TableInfo>)>,
    /// The global ID counter.
    pub global_id: i64,
    /// Created placement policies.
    pub policies: Vec<PolicyInfo>,
    /// `Execute` / `ExecuteInternal` statements.
    pub stmts: Vec<String>,
    /// Each batch create: `(db, [table])`.
    pub batch_calls: Vec<Vec<(String, Vec<String>)>>,
    /// Each single create: `(db, table, cfg)`.
    pub single_calls: Vec<(String, String, CreateTableConfig)>,
    /// Sessions refuse batch create (Go's non-`BatchCreateTableSession`).
    pub no_batch: bool,
    /// The next batch create fails with this.
    pub batch_error: Option<errors::Error>,
    /// Statements containing the key fail with the error.
    pub fail_sql: Vec<(String, errors::Error)>,
    /// Rows for `ExecRestrictedSQL` by SQL substring.
    pub rows: Vec<(String, Vec<Vec<Datum>>)>,
    /// `LoadStatsFromJSONNoUpdate` inputs.
    pub stats_loaded: Vec<tidb_stats_util::JsonTable>,
    /// `LoadStatsFromJSONConcurrently` inputs: `(table, [physical ID])`.
    pub partition_stats_loaded: Vec<(String, Vec<i64>)>,
    /// `SaveMetaToStorage` batches.
    pub meta_updates: Vec<Vec<MetaUpdate>>,
    /// `UpdateLabelRules` set-rule IDs per call.
    pub label_patches: Vec<Vec<String>>,
    /// `NotifyUpdateAllUsersPrivilege` calls.
    pub privilege_notified: usize,
    /// TiFlash progress by physical ID; missing is 1.
    pub tiflash_progress: HashMap<i64, f64>,
}

/// A cluster shared by its sessions and domain.
#[derive(Clone, Default)]
pub struct MockCluster(pub Arc<Mutex<ClusterState>>);

impl MockCluster {
    /// The state, locked.
    pub fn state(&self) -> std::sync::MutexGuard<'_, ClusterState> {
        self.0.lock().unwrap()
    }

    /// Adds a database with tables.
    pub fn add_db(&self, db: DBInfo, tables: Vec<TableInfo>) {
        self.state()
            .dbs
            .insert(db.name.lowercase().to_owned(), (db, tables));
    }

    /// The table, if created.
    pub fn table(&self, db: &str, table: &str) -> Option<TableInfo> {
        SchemaSnapshot(self.state().dbs.clone()).table_by_name(db, table)
    }

    fn check_fail(&self, sql: &str) -> Result<()> {
        let st = self.state();
        match st.fail_sql.iter().find(|(k, _)| sql.contains(k.as_str())) {
            Some((_, e)) => Err(Arc::clone(e)),
            None => Ok(()),
        }
    }

    fn create_table_in(&self, db: &str, mut info: TableInfo, cfg: CreateTableConfig) -> Result<()> {
        let mut st = self.state();
        if !cfg.id_allocated {
            st.global_id += 1;
            info.id = st.global_id;
        }
        let Some((_, tables)) = st.dbs.get_mut(&db.to_lowercase()) else {
            return Err(Arc::new(tidb_terror::TerrorError::compatible(
                tidb_terror::TerrorCode::new(1049),
                format!("Unknown database '{db}'"),
            )));
        };
        if tables
            .iter()
            .any(|t| t.name.lowercase() == info.name.lowercase())
        {
            if cfg.on_exist_ignore {
                return Ok(());
            }
            return Err(Arc::new(tidb_terror::TerrorError::compatible(
                tidb_terror::TerrorCode::new(1050),
                format!("Table '{}' already exists", info.name.original()),
            )));
        }
        tables.push(info);
        Ok(())
    }

    /// A session over the cluster.
    #[must_use]
    pub fn session(&self) -> Box<dyn Session> {
        Box::new(MockSession(self.clone()))
    }
}

/// An info schema snapshot.
pub struct SchemaSnapshot(pub BTreeMap<String, (DBInfo, Vec<TableInfo>)>);

impl InfoSchema for SchemaSnapshot {
    fn schema_by_name(&self, db: &str) -> Option<DBInfo> {
        self.0.get(&db.to_lowercase()).map(|(d, _)| d.clone())
    }
    fn table_by_name(&self, db: &str, table: &str) -> Option<TableInfo> {
        let (_, tables) = self.0.get(&db.to_lowercase())?;
        tables
            .iter()
            .find(|t| t.name.lowercase() == table.to_lowercase())
            .cloned()
    }
    fn schema_table_infos(&self, db: &str) -> Vec<TableInfo> {
        self.0
            .get(&db.to_lowercase())
            .map(|(_, t)| t.clone())
            .unwrap_or_default()
    }
    fn all_schemas(&self) -> Vec<DBInfo> {
        self.0.values().map(|(d, _)| d.clone()).collect()
    }
}

/// A session over a [`MockCluster`].
pub struct MockSession(pub MockCluster);

impl Session for MockSession {
    fn execute(&self, _: &Context, sql: &str) -> Result<()> {
        self.0.check_fail(sql)?;
        self.0.state().stmts.push(sql.to_owned());
        Ok(())
    }
    fn execute_internal(&self, _: &Context, sql: &str, args: &[SqlArg<'_>]) -> Result<()> {
        let sql = tidb_sqlescape::escape_sql_text(sql, args)
            .map_err(|e| errors::errorf(e.to_string()))?;
        self.0.check_fail(&sql)?;
        self.0.state().stmts.push(sql);
        Ok(())
    }
    fn exec_restricted_sql(
        &self,
        _: &Context,
        sql: &str,
        args: &[SqlArg<'_>],
    ) -> Result<Vec<Vec<Datum>>> {
        let sql = tidb_sqlescape::escape_sql_text(sql, args)
            .map_err(|e| errors::errorf(e.to_string()))?;
        self.0.check_fail(&sql)?;
        let mut st = self.0.state();
        st.stmts.push(sql.clone());
        Ok(st
            .rows
            .iter()
            .find(|(k, _)| sql.contains(k.as_str()))
            .map(|(_, r)| r.clone())
            .unwrap_or_default())
    }
    fn create_database_on_exist_error(&self, _: &Context, schema: &DBInfo) -> Result<()> {
        let mut st = self.0.state();
        let name = schema.name.lowercase().to_owned();
        if st.dbs.contains_key(&name) {
            return Err(Arc::new(tidb_terror::TerrorError::compatible(
                tidb_terror::TerrorCode::new(1007),
                format!(
                    "Can't create database '{}'; database exists",
                    schema.name.original()
                ),
            )));
        }
        let mut db = schema.clone();
        st.global_id += 1;
        db.id = st.global_id;
        st.dbs.insert(name, (db, Vec::new()));
        Ok(())
    }
    /// Go's BRIE glue (`BRIECreateTable`) always adds `OnExistIgnore`.
    fn create_table(
        &self,
        _: &Context,
        db_name: &str,
        table: &TableInfo,
        cfg: CreateTableConfig,
    ) -> Result<()> {
        self.0.state().single_calls.push((
            db_name.to_owned(),
            table.name.original().to_owned(),
            cfg,
        ));
        self.0.create_table_in(
            db_name,
            table.clone(),
            CreateTableConfig {
                on_exist_ignore: true,
                ..cfg
            },
        )
    }
    fn create_tables(
        &self,
        _: &Context,
        tables: &[(String, Vec<TableInfo>)],
        cfg: CreateTableConfig,
    ) -> Option<Result<()>> {
        {
            let mut st = self.0.state();
            if st.no_batch {
                return None;
            }
            st.batch_calls.push(
                tables
                    .iter()
                    .map(|(db, ts)| {
                        (
                            db.clone(),
                            ts.iter().map(|t| t.name.original().to_owned()).collect(),
                        )
                    })
                    .collect(),
            );
            if let Some(e) = st.batch_error.take() {
                return Some(Err(e));
            }
        }
        for (db, ts) in tables {
            for t in ts {
                let cfg = CreateTableConfig {
                    on_exist_ignore: true,
                    ..cfg
                };
                if let Err(e) = self.0.create_table_in(db, t.clone(), cfg) {
                    return Some(Err(e));
                }
            }
        }
        Some(Ok(()))
    }
    fn create_placement_policy(&self, _: &Context, policy: &PolicyInfo) -> Result<()> {
        self.0.state().policies.push(policy.clone());
        Ok(())
    }
    fn get_global_variable(&self, _: &str) -> Result<String> {
        Ok(String::new())
    }
    fn get_global_sys_var(&self, _: &str) -> Result<String> {
        Ok(String::new())
    }
    fn get_all_ddl_jobs(&self, _: &Context) -> Result<Vec<tidb_model::Job>> {
        Ok(Vec::new())
    }
    fn alter_table_mode(
        &self,
        _: &Context,
        _: i64,
        _: i64,
        _: tidb_model::TableMode,
    ) -> Result<()> {
        Ok(())
    }
}

/// The global ID counter of a [`MockCluster`].
struct MockMutator<'a>(&'a MockCluster);

impl GlobalIdAllocator for MockMutator<'_> {
    fn get_global_id(&self) -> Result<i64> {
        Ok(self.0.state().global_id)
    }
    fn advance_global_ids(&self, n: i32) -> Result<i64> {
        let mut st = self.0.state();
        let old = st.global_id;
        st.global_id += i64::from(n);
        Ok(old)
    }
}

impl Domain for MockCluster {
    fn persist_stats_by_snapshot(
        &self,
        _: &str,
        _: &TableInfo,
        _: u64,
        _: &mut dyn FnMut(Option<Vec<u8>>, i64) -> Result<()>,
    ) -> Result<()> {
        unimplemented!()
    }
    fn get_label_rules(&self, _: &[String]) -> Result<Vec<LabelRule>> {
        Ok(Vec::new())
    }
    fn run_in_new_meta_txn(
        &self,
        f: &mut dyn FnMut(&dyn GlobalIdAllocator) -> Result<()>,
    ) -> Result<()> {
        f(&MockMutator(self))
    }
    fn info_schema(&self) -> Result<Arc<dyn InfoSchema>> {
        Ok(Arc::new(SchemaSnapshot(self.state().dbs.clone())))
    }
    fn load_stats_from_json_no_update(
        &self,
        _: &Context,
        json_table: &tidb_stats_util::JsonTable,
    ) -> Result<()> {
        self.state().stats_loaded.push(json_table.clone());
        Ok(())
    }
    fn load_partition_stats_from_json(
        &self,
        _: &Context,
        table: &TableInfo,
        tasks: Vec<(i64, tidb_stats_util::JsonTable)>,
    ) -> Result<()> {
        self.state().partition_stats_loaded.push((
            table.name.original().to_owned(),
            tasks.iter().map(|t| t.0).collect(),
        ));
        Ok(())
    }
    fn save_meta_to_storage(&self, updates: &[MetaUpdate]) -> Result<()> {
        self.state().meta_updates.push(updates.to_vec());
        Ok(())
    }
    fn notify_update_all_users_privilege(&self) -> Result<()> {
        self.state().privilege_notified += 1;
        Ok(())
    }
    fn update_label_rules(&self, patch: &tidb_ddl_label::LabelRulePatch) -> Result<()> {
        self.state()
            .label_patches
            .push(patch.set_rules.map_visible(|r| r.id.clone()));
        Ok(())
    }
    fn tiflash_progress(&self, _: &Context, physical_id: i64, _: u64) -> Result<f64> {
        Ok(self
            .state()
            .tiflash_progress
            .get(&physical_id)
            .copied()
            .unwrap_or(1.0))
    }
}

struct NoProgress;

impl Progress for NoProgress {
    fn inc(&self) {}
    fn inc_by(&self, _: i64) {}
    fn get_current(&self) -> i64 {
        0
    }
    fn close(&self) {}
}

impl Glue for MockCluster {
    fn get_domain(&self) -> Result<Arc<dyn Domain>> {
        Ok(Arc::new(self.clone()))
    }
    fn create_session(&self) -> Result<Box<dyn Session>> {
        Ok(self.session())
    }
    fn open(&self) -> Result<Arc<dyn Storage>> {
        unimplemented!()
    }
    fn owns_storage(&self) -> bool {
        false
    }
    fn start_progress(&self, _: &Context, _: &str, _: i64, _: bool) -> Arc<dyn Progress> {
        Arc::new(NoProgress)
    }
    fn record(&self, _: &str, _: u64) {}
    fn get_version(&self) -> String {
        String::new()
    }
    fn use_one_shot_session(
        &self,
        _: bool,
        f: &mut dyn FnMut(&dyn Session) -> Result<()>,
    ) -> Result<()> {
        f(self.session().as_ref())
    }
    fn get_client(&self) -> GlueClient {
        GlueClient::Sql
    }
}
