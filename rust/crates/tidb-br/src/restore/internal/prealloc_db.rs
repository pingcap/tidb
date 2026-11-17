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

//! Go `br/pkg/restore/internal/prealloc_db`: the session restore runs its
//! DDL through, with the preallocated table IDs registered.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tidb_ast::CiString;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::{ActionType, DBInfo, Job, PolicyInfo, TableInfo};

use super::prealloc_table_id::PreallocIDs;
use crate::errors::{self, Result};
use crate::glue::{CreateTableConfig, Glue, Session};
use crate::metautil;
use crate::restore::misc::UniqueTableName;
use crate::utils::misc::{enclose_name, need_auto_id, sql_error_code};

/// Go's `*sync.Map` of placement policies by lowercase name; each one is
/// created the first time a database or table refers to it.
pub type PolicyMap = Mutex<HashMap<String, PolicyInfo>>;

fn log_str(k: &'static str, v: impl ToString) -> Field {
    Field::new(k, Value::Str(v.to_string()))
}

/// Go `DB`: one session. Go's is not thread-safe; the mutex lets a pool of
/// them be shared across DDL workers.
pub struct DB {
    se: Mutex<Box<dyn Session>>,
    prealloced_ids: Mutex<Option<Arc<PreallocIDs>>>,
}

/// Go `NewDB`: a session with `sql_mode` cleared and, for a policy mode,
/// `tidb_placement_mode` set; whether the target supports placement
/// policies.
///
/// # Errors
/// The session cannot be created or set up.
pub fn new_db(g: &dyn Glue, policy_mode: &str) -> Result<(DB, bool)> {
    let se = g.create_session()?;
    let ctx = Context::background();
    se.execute(&ctx, "set @@sql_mode=''")?;
    let mut support_policy = false;
    if !policy_mode.is_empty() {
        match se.execute(&ctx, &format!("set @@tidb_placement_mode='{policy_mode}';")) {
            Err(err) => {
                if sql_error_code(err.as_ref())
                    != Some(tidb_errno::errcode::ErrUnknownSystemVariable as isize)
                {
                    return Err(err);
                }
                tidb_log::warn(
                    "target tidb not support tidb_placement_mode, ignore create policies",
                    &[log_str("error", &err)],
                );
            }
            Ok(()) => {
                tidb_log::debug(
                    "set tidb_placement_mode success",
                    &[log_str("mode", policy_mode)],
                );
                support_policy = true;
            }
        }
    }
    Ok((
        DB {
            se: Mutex::new(se),
            prealloced_ids: Mutex::new(None),
        },
        support_policy,
    ))
}

/// Go `infoschema.ErrDatabaseExists.Equal(err)`.
fn is_database_exists(err: &errors::Error) -> bool {
    sql_error_code(err.as_ref()) == Some(tidb_errno::errcode::ErrDBCreateExists as isize)
}

impl DB {
    /// A DB over an existing session, as `NewDB` without its setup.
    #[must_use]
    pub fn from_session(se: Box<dyn Session>) -> Self {
        Self {
            se: Mutex::new(se),
            prealloced_ids: Mutex::new(None),
        }
    }

    /// Go `Session`: `f` with the session.
    pub fn with_session<T>(&self, f: impl FnOnce(&dyn Session) -> T) -> T {
        f(self.se.lock().unwrap().as_ref())
    }

    /// Go `RegisterPreallocatedIDs`.
    pub fn register_preallocated_ids(&self, ids: Arc<PreallocIDs>) {
        *self.prealloced_ids.lock().unwrap() = Some(ids);
    }

    /// Go `ExecDDL`: a create-schema or create-table job through the DDL
    /// interface, any other job by its query.
    ///
    /// # Errors
    /// The DDL error, except an existing database.
    pub fn exec_ddl(&self, ctx: &Context, ddl_job: &Job) -> Result<()> {
        let (table_info, db_info) = ddl_job
            .binlog_info
            .as_ref()
            .map(|b| {
                let b = b.read();
                (
                    b.table_info.as_ref().map(|t| t.read().clone()),
                    b.db_info.as_ref().map(|d| d.read().clone()),
                )
            })
            .unwrap_or_default();
        let db_name = || {
            db_info
                .as_ref()
                .map(|d| d.name.to_string())
                .unwrap_or_default()
        };
        let se = self.se.lock().unwrap();
        if ddl_job.type_ == ActionType::ACTION_CREATE_SCHEMA {
            let info = db_info.clone().unwrap_or_default();
            if let Err(err) = se.create_database_on_exist_error(ctx, &info) {
                if !is_database_exists(&err) {
                    tidb_log::error(
                        "create database failed",
                        &[log_str("db", db_name()), log_str("error", &err)],
                    );
                    return Err(err);
                }
            }
            return Ok(());
        }
        if ddl_job.type_ == ActionType::ACTION_CREATE_TABLE {
            let info = table_info
                .as_ref()
                .map(TableInfo::clone_like_go)
                .unwrap_or_default();
            let res = se.create_table(
                ctx,
                &ddl_job.schema_name.to_string(),
                &info,
                CreateTableConfig::default(),
            );
            if let Err(err) = &res {
                tidb_log::error(
                    "create table failed",
                    &[
                        log_str("db", db_name()),
                        log_str("table", &info.name),
                        log_str("error", err),
                    ],
                );
            }
            return res;
        }
        let query = ddl_job.query.to_string();
        if query.is_empty() {
            tidb_log::warn(
                "query of ddl job is empty, ignore it",
                &[
                    log_str("type", ddl_job.type_),
                    log_str("db", &ddl_job.schema_name),
                ],
            );
            return Ok(());
        }
        if table_info.is_some() {
            let switch_db_sql = format!("use {};", enclose_name(&ddl_job.schema_name.to_string()));
            if let Err(err) = se.execute(ctx, &switch_db_sql) {
                tidb_log::error(
                    "switch db failed",
                    &[
                        log_str("query", &switch_db_sql),
                        log_str("db", &ddl_job.schema_name),
                        log_str("error", &err),
                    ],
                );
                return Err(err);
            }
        }
        let res = se.execute(ctx, &query);
        if let Err(err) = &res {
            let version = ddl_job
                .binlog_info
                .as_ref()
                .map_or(0, |b| b.read().schema_version);
            tidb_log::error(
                "execute ddl query failed",
                &[
                    log_str("query", &query),
                    log_str("db", &ddl_job.schema_name),
                    Field::new("historySchemaVersion", Value::I64(version)),
                    log_str("error", err),
                ],
            );
        }
        res
    }

    /// Go `CreatePlacementPolicy`.
    ///
    /// # Errors
    /// The DDL error.
    pub fn create_placement_policy(&self, ctx: &Context, policy: &PolicyInfo) -> Result<()> {
        self.se
            .lock()
            .unwrap()
            .create_placement_policy(ctx, policy)?;
        tidb_log::info(
            "create placement policy succeed",
            &[log_str("name", &policy.name)],
        );
        Ok(())
    }

    /// Go `CreateDatabase`: whether the database already existed.
    ///
    /// # Errors
    /// The DDL error, except an existing database.
    pub fn create_database(
        &self,
        ctx: &Context,
        schema: &DBInfo,
        support_policy: bool,
        policy_map: Option<&PolicyMap>,
    ) -> Result<bool> {
        let mut schema_clone = schema.clone_like_go();
        tidb_log::info("create database", &[log_str("name", &schema_clone.name)]);
        if !support_policy {
            tidb_log::info(
                "set placementPolicyRef to nil when target tidb not support policy",
                &[log_str("database", &schema_clone.name)],
            );
            schema_clone.placement_policy_ref = None;
        }
        if let Some(policy_ref) = &schema_clone.placement_policy_ref {
            let name = policy_ref.read().name.clone();
            self.ensure_placement_policy(ctx, &name, policy_map)?;
        }
        match self
            .se
            .lock()
            .unwrap()
            .create_database_on_exist_error(ctx, &schema_clone)
        {
            Ok(()) => Ok(false),
            Err(err) if is_database_exists(&err) => Ok(true),
            Err(err) => {
                tidb_log::error(
                    "create database failed",
                    &[log_str("db", &schema_clone.name), log_str("error", &err)],
                );
                Err(err)
            }
        }
    }
}

impl DB {
    fn execute_logged(&self, ctx: &Context, sql: &str, table: &metautil::Table) -> Result<()> {
        let res = self.se.lock().unwrap().execute(ctx, sql);
        if let Err(err) = &res {
            tidb_log::error(
                "restore meta sql failed",
                &[
                    log_str("query", sql),
                    log_str("db", &table.db.name),
                    log_str("table", &table.info().name),
                    log_str("error", err),
                ],
            );
        }
        res
    }

    /// Go `restoreSequence`: the sequence's value, and for a cycling one its
    /// round pushed past zero first.
    fn restore_sequence(&self, ctx: &Context, table: &metautil::Table) -> Result<()> {
        let info = table.info();
        if !info.is_sequence() {
            return Ok(());
        }
        let name = format!(
            "{}.{}",
            enclose_name(table.db.name.original()),
            enclose_name(info.name.original())
        );
        let set_val = |v: i64| format!("do setval({name}, {v});");
        let seq = info
            .sequence
            .as_ref()
            .map(|s| s.read().clone())
            .unwrap_or_default();
        if seq.cycle {
            // TiDB keeps a sequence's pace across the cluster, so its cycle
            // round is restored by setting the bound and taking one value
            // (https://github.com/pingcap/br/pull/242#issuecomment-631307978).
            let bound = if seq.increment < 0 {
                seq.min_value
            } else {
                seq.max_value
            };
            self.execute_logged(ctx, &set_val(bound), table)?;
            self.execute_logged(ctx, &format!("do nextval({name});"), table)?;
        }
        self.execute_logged(ctx, &set_val(info.auto_inc_id), table)
    }

    /// Go `CreateTablePostRestore`: a sequence's value, or for a table an
    /// incremental restore found existing, its auto ID base.
    ///
    /// # Errors
    /// The statement's error.
    pub fn create_table_post_restore(
        &self,
        ctx: &Context,
        table: &metautil::Table,
        to_be_corrected_tables: &HashSet<UniqueTableName>,
    ) -> Result<()> {
        let info = table.info();
        if info.is_view() {
            return Ok(());
        }
        if info.is_sequence() {
            return self.restore_sequence(ctx, table);
        }
        let key = UniqueTableName {
            db: table.db.name.to_string(),
            table: info.name.to_string(),
        };
        if !to_be_corrected_tables.contains(&key) {
            return Ok(());
        }
        let name = format!(
            "{}.{}",
            enclose_name(table.db.name.original()),
            enclose_name(info.name.original())
        );
        let sql = if need_auto_id(info) {
            format!("alter table {name} auto_increment = {};", info.auto_inc_id)
        } else if info.contains_auto_random_bits() {
            format!(
                "alter table {name} auto_random_base = {}",
                info.auto_rand_id
            )
        } else {
            tidb_log::info(
                "table exists in incremental ddl jobs, but don't need to be altered",
                &[log_str("db", &table.db.name), log_str("table", &info.name)],
            );
            return Ok(());
        };
        self.execute_logged(ctx, &sql, table)
    }

    /// The target table as the DDL creates it: placement cleared or its
    /// policies created, TTL disabled, IDs preallocated.
    fn target_info(
        &self,
        ctx: &Context,
        table: &metautil::Table,
        support_policy: bool,
        policy_map: Option<&PolicyMap>,
    ) -> Result<TableInfo> {
        let mut target = table.info().clone_like_go();
        if support_policy {
            self.ensure_table_placement_policies(ctx, &target, policy_map)?;
        } else {
            tidb_log::info(
                "set placementPolicyRef to nil when target tidb not support policy",
                &[
                    log_str("table", &target.name),
                    log_str("db", &table.db.name),
                ],
            );
            target.clear_placement();
        }
        if let Some(ttl) = &target.ttl_info {
            ttl.write().enable = false;
        }
        let ids = self
            .prealloced_ids
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| errors::errorf("preallocedIDs is nil"))?;
        ids.rewrite_table_info(&target)
    }

    /// Go `CreateTables`: one batch create per call, by database; then each
    /// table's post-restore statements. A session without batch create does
    /// nothing, as Go's failed `BatchCreateTableSession` assertion.
    ///
    /// # Errors
    /// The DDL error; `ErrInvalidDDLJob` asks the caller to fall back to
    /// [`Self::create_table`].
    pub fn create_tables(
        &self,
        ctx: &Context,
        tables: &[metautil::Table],
        ddl_tables: &HashSet<UniqueTableName>,
        support_policy: bool,
        policy_map: Option<&PolicyMap>,
    ) -> Result<()> {
        if self.prealloced_ids.lock().unwrap().is_none() {
            return Err(errors::errorf("preallocedIDs is nil"));
        }
        // Go's map keeps no order; databases are batched in first-seen order.
        let mut cloned: Vec<(String, Vec<TableInfo>)> = Vec::new();
        for table in tables {
            let info = self.target_info(ctx, table, support_policy, policy_map)?;
            let db = table.db.name.lowercase();
            match cloned.iter_mut().find(|(name, _)| name == db) {
                Some((_, infos)) => infos.push(info),
                None => cloned.push((db.to_owned(), vec![info])),
            }
        }
        if !cloned.is_empty() {
            let cfg = CreateTableConfig {
                id_allocated: true,
                ..Default::default()
            };
            match self.se.lock().unwrap().create_tables(ctx, &cloned, cfg) {
                None => return Ok(()),
                Some(res) => res?,
            }
        }
        for table in tables {
            self.create_table_post_restore(ctx, table, ddl_tables)?;
        }
        Ok(())
    }

    /// Go `CreateTable`.
    ///
    /// # Errors
    /// The DDL error.
    pub fn create_table(
        &self,
        ctx: &Context,
        table: &metautil::Table,
        ddl_tables: &HashSet<UniqueTableName>,
        support_policy: bool,
        policy_map: Option<&PolicyMap>,
    ) -> Result<()> {
        let info = self.target_info(ctx, table, support_policy, policy_map)?;
        let cfg = CreateTableConfig {
            id_allocated: true,
            ..Default::default()
        };
        let res = self
            .se
            .lock()
            .unwrap()
            .create_table(ctx, table.db.name.original(), &info, cfg);
        if let Err(err) = res {
            tidb_log::error(
                "create table failed",
                &[
                    log_str("db", &table.db.name),
                    log_str("table", &table.info().name),
                    log_str("error", &err),
                ],
            );
            return Err(err);
        }
        self.create_table_post_restore(ctx, table, ddl_tables)
    }

    /// Go `ensurePlacementPolicy`: the policy created the first time it is
    /// referred to.
    fn ensure_placement_policy(
        &self,
        ctx: &Context,
        policy_name: &CiString,
        policies: Option<&PolicyMap>,
    ) -> Result<()> {
        let Some(policies) = policies else {
            return Ok(());
        };
        let policy = policies.lock().unwrap().remove(policy_name.lowercase());
        match policy {
            Some(policy) => self.create_placement_policy(ctx, &policy),
            None => Ok(()),
        }
    }

    /// Go `ensureTablePlacementPolicies`.
    fn ensure_table_placement_policies(
        &self,
        ctx: &Context,
        info: &TableInfo,
        policies: Option<&PolicyMap>,
    ) -> Result<()> {
        if let Some(r) = &info.placement_policy_ref {
            let name = r.read().name.clone();
            self.ensure_placement_policy(ctx, &name, policies)?;
        }
        if let Some(part) = &info.partition {
            let refs: Vec<CiString> = part
                .read()
                .definitions
                .map_visible(|d| {
                    d.placement_policy_ref
                        .as_ref()
                        .map(|r| r.read().name.clone())
                })
                .into_iter()
                .flatten()
                .collect();
            for name in refs {
                self.ensure_placement_policy(ctx, &name, policies)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
