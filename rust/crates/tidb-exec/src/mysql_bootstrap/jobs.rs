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

//! The Classic kernel's bootstrap: Go `bootstrapSessionImpl` without
//! `bootstrapSchemas` (`kerneltype.IsClassic()`).
//!
//! Go creates the `mysql` database with an ordinary id (`InitDDLTables`,
//! `Mutator.CreateMySQLDatabaseIfNotExists`: the first `GenGlobalID`), writes
//! the DDL-owned tables straight into meta at their reserved ids, and then
//! runs `doDDLWorks` as ordinary DDL: `CREATE DATABASE sys`, one `CREATE
//! TABLE` per `mysql.*` table, the two views, `CREATE DATABASE test` and
//! `ALTER RESOURCE GROUP default`. Each statement is a DDL job, so each draws
//! its object id and then its job id from `NextGlobalID`, publishes its own
//! schema version and diff, and ends as a `mysql.tidb_ddl_history` row.
//!
//! This planner runs those statements through the same job planners a live
//! owner uses ([`tidb_ddl::cluster_ddl`]'s submission, `plan_insert_attempt` and
//! `plan_persisted_ddl_job_step`), against an overlay of the bootstrap's own
//! writes, and returns the net write set. The one difference from Go is the
//! transaction boundary: Go commits each job on its own and this node
//! publishes the whole bootstrap in one transaction (see
//! [`super::BootstrapWrite`]); the committed bytes are the same.

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::CiString;
use tidb_meta::{key, value};
use tidb_model::db::DBInfo;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_model::JobVersion;
use tidb_txnkv::transaction::{BufferMutation, BufferMutationOp};

use super::{
    bootstrap_default_resource_group, build_bootstrap_table, build_bootstrap_view, encode_error,
    read_schema_cache_size, rows, schema_cache_size_kv_key, BootstrapEnvironment, BootstrapError,
    BootstrapLayout, BootstrapSplits, BootstrapWrite, BOOTSTRAP_VIEWS, SYSTEM_DB,
    SYSTEM_DB_CHARSET, SYSTEM_DB_COLLATION, SYS_DB,
};
use tidb_cluster::cluster_catalog::{load_cluster_catalog, ClusterCatalogError, MetaPairs, MetaSnapshot};
use tidb_ddl::cluster_ddl::add_index::AddIndexSubmission;
use tidb_ddl::cluster_ddl::{
    lower_ddl_with_context, plan_persisted_ddl_job_step, DdlJobSchemaState, DdlStatement,
    PersistedDdlJobPlan, ResourceGroupUpdate,
};
use tidb_ddl::ddl_job_submit::{plan_insert_attempt, JobSpec};
use tidb_ddl::real_tikv_ddl::prepare_persisted_job_submission;
use tidb_metadef::system::SYSTEM_DATABASE_ID;
use tidb_metadef::{BOOTSTRAP_TABLES, DDL_TABLE_VERSION_TABLES};

/// The tables Go's `versionedBootstrapSchemas` of this tree does not create:
/// Go master's materialized-view version, which `BOOTSTRAP_TABLES` also
/// carries for the next-gen layout.
const MATERIALIZED_VIEW_TABLES: [&str; 5] = [
    "tidb_mview_refresh_info",
    "tidb_mlog_purge_info",
    "tidb_mview_refresh_hist",
    "tidb_mview_refresh_alert",
    "tidb_mlog_purge_hist",
];

/// The writes a planner has made on top of a snapshot, readable through the
/// snapshot interface, so one job's planners see the previous job's commit.
struct Overlay<'a, S: MetaSnapshot> {
    base: &'a mut S,
    /// `None` is a deletion.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// While set, a catalog scan sees only these table keys: every other
    /// table meta key is withheld from scans (not from point reads), so a job
    /// planner that loads the whole catalog parses the few tables it needs
    /// instead of everything the bootstrap has created so far. See
    /// [`Overlay::withhold_tables_but`].
    visible_tables: Option<BTreeSet<Vec<u8>>>,
}

impl<'a, S: MetaSnapshot> Overlay<'a, S> {
    fn new(base: &'a mut S) -> Self {
        Self {
            base,
            writes: BTreeMap::new(),
            visible_tables: None,
        }
    }

    /// Withholds every table meta key written after this call from catalog
    /// scans. The bootstrap's DDL jobs create tables that share nothing with
    /// each other (no foreign key, no name, no view body names another), so a
    /// job plans the same write set whether or not it sees its predecessors;
    /// the catalog load every planner step makes is what costs, and it grows
    /// with each table. `hide_created_tables_changes_nothing` pins the claim.
    fn withhold_tables_but(&mut self, visible: BTreeSet<Vec<u8>>) {
        self.visible_tables = Some(visible);
    }

    /// Ends [`Self::withhold_tables_but`].
    fn show_all(&mut self) {
        self.visible_tables = None;
    }

    fn table_keys(&self) -> BTreeSet<Vec<u8>> {
        self.writes
            .iter()
            .filter(|(raw_key, stored)| stored.is_some() && is_table_meta_key(raw_key))
            .map(|(raw_key, _)| raw_key.clone())
            .collect()
    }

    fn withheld(&self, raw_key: &[u8]) -> bool {
        self.visible_tables
            .as_ref()
            .is_some_and(|visible| is_table_meta_key(raw_key) && !visible.contains(raw_key))
    }

    fn apply(&mut self, mutations: &[BufferMutation]) {
        for mutation in mutations {
            match mutation.kind() {
                BufferMutationOp::Set => {
                    self.writes
                        .insert(mutation.key().to_vec(), Some(mutation.value().to_vec()));
                }
                BufferMutationOp::Delete => {
                    self.writes.insert(mutation.key().to_vec(), None);
                }
                BufferMutationOp::Lock => {}
            }
        }
    }

    /// The net write set: the last write of every key, minus the deletions of
    /// keys the base never held.
    fn net_mutations(&mut self) -> Result<Vec<BufferMutation>, BootstrapError> {
        let mut net = Vec::with_capacity(self.writes.len());
        for (raw_key, stored) in &self.writes {
            match stored {
                Some(stored) => net.push(BufferMutation::set(raw_key.clone(), stored.clone())?),
                None => {
                    if self.base.get(raw_key)?.is_some() {
                        net.push(BufferMutation::delete(raw_key.clone())?);
                    }
                }
            }
        }
        Ok(net)
    }
}

impl<S: MetaSnapshot> MetaSnapshot for Overlay<'_, S> {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        match self.writes.get(raw_key) {
            Some(stored) => Ok(stored.clone()),
            None => self.base.get(raw_key),
        }
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> =
            self.base.scan_prefix(prefix)?.into_iter().collect();
        for (raw_key, stored) in self.writes.range(prefix.to_vec()..) {
            if !raw_key.starts_with(prefix) {
                break;
            }
            if self.withheld(raw_key) {
                continue;
            }
            match stored {
                Some(stored) => {
                    merged.insert(raw_key.clone(), stored.clone());
                }
                None => {
                    merged.remove(raw_key);
                }
            }
        }
        Ok(merged.into_iter().collect())
    }
}

/// A `DB:<id>` hash field `Table:<id>`: one table's stored `TableInfo`.
fn is_table_meta_key(raw_key: &[u8]) -> bool {
    tidb_meta::structure::decode_hash_data_key(raw_key)
        .is_ok_and(|(hash, field)| hash.starts_with(b"DB:") && field.starts_with(b"Table:"))
}

/// Go's TSO, as far as this planner needs one: every submission and every
/// worker transaction gets a later timestamp than the one before it.
struct Clock(u64);

impl Clock {
    fn tick(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}

/// What one bootstrap DDL statement leaves for the caller.
#[derive(Default)]
struct JobOutcome {
    /// The last schema diff the job published, if it published one.
    diff: Option<tidb_model::schema_diff::SchemaDiff>,
    /// The resource-manager write the job made before its version.
    resource_group_update: Option<ResourceGroupUpdate>,
}

/// Go's bootstrap session: no SQL mode (so a TEXT column's `''` default is a
/// warning, not an error), `tidb_enable_clustered_index` pinned to INT_ONLY
/// "to be compatible with old TiDB versions" (`BootstrapSession`), and the
/// statement text the job carries is the statement as written.
fn bootstrap_context(sql: &str) -> tidb_executor::StmtContext {
    crate::table_info_build::default_ddl_statement_context()
        .with_strict(false)
        .with_ddl_sql_mode(0)
        .with_clustered_index_def_mode(tidb_vardef::modes::ClusteredIndexDefMode::INT_ONLY)
        .with_ddl_query(sql)
}

/// Runs one statement as the DDL job Go's executor submits and the owner
/// steps to history, against the overlay.
fn run_ddl_job<S: MetaSnapshot>(
    overlay: &mut Overlay<'_, S>,
    clock: &mut Clock,
    sql: &str,
    statement: &DdlStatement,
) -> Result<JobOutcome, BootstrapError> {
    let job_error = |detail: String| BootstrapError::Job {
        statement: sql.lines().next().unwrap_or(sql).trim().to_owned(),
        detail,
    };
    let context = bootstrap_context(sql);
    // `Domain.Init` registered this server, the only one, before the first
    // job, so the job version detection settles on version 2.
    let submission = prepare_persisted_job_submission(
        overlay,
        statement,
        &context,
        &[],
        clock.tick(),
        false,
        0,
        0,
        // The bootstrap's statements set no TiFlash replica.
        &|| Ok(0),
        &mut |_, _| Ok(false),
    )
    .map_err(|error| job_error(error.to_string()))?;
    let Some(AddIndexSubmission::Job(mut spec, _)) = submission else {
        return Err(job_error("the statement queued no DDL job".to_owned()));
    };
    spec.job.version = JobVersion::V2;
    let catalog = load_cluster_catalog(overlay)?;
    let mut before_insert = |_: &[JobSpec]| Option::<fn()>::None;
    let (mutations, _) = plan_insert_attempt(
        overlay,
        &catalog,
        std::slice::from_mut(&mut spec),
        &mut before_insert,
    )
    .map_err(|error| job_error(error.to_string()))?;
    overlay.apply(&mutations);
    let job_id = spec.job.id;

    let mut outcome = JobOutcome::default();
    loop {
        let plan = plan_persisted_ddl_job_step(
            overlay,
            job_id,
            clock.tick(),
            DdlJobSchemaState::new(true),
            &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT,
            None,
        )
        .map_err(|error| job_error(error.to_string()))?;
        let PersistedDdlJobPlan::Step(step) = plan else {
            return Err(job_error(
                "the job needs a worker the bootstrap does not run".to_owned(),
            ));
        };
        if let Some(error) = &step.run_error {
            return Err(job_error(error.to_string()));
        }
        let write = step.write;
        // The side effects the owner makes beside the commit have no object
        // in the bootstrap: system tables carry no placement, label rule or
        // affinity group, and the bootstrap's statements create no index.
        if !write.label_rules.is_empty()
            || !write.placement_bundles.is_empty()
            || write.check_constraint_validation.is_some()
        {
            return Err(job_error(
                "the job carries a side effect the bootstrap cannot publish".to_owned(),
            ));
        }
        overlay.apply(&write.mutations);
        if write.schema_version != 0 {
            outcome.diff = Some(write.diff);
        }
        if write.resource_group_update.is_some() {
            outcome.resource_group_update = write.resource_group_update;
        }
        if step.terminal {
            return Ok(outcome);
        }
    }
}

/// [`run_ddl_job`], keeping the newest diff and resource-manager write.
fn run<S: MetaSnapshot>(
    overlay: &mut Overlay<'_, S>,
    clock: &mut Clock,
    last: &mut JobOutcome,
    sql: &str,
    statement: &DdlStatement,
) -> Result<(), BootstrapError> {
    let outcome = run_ddl_job(overlay, clock, sql, statement)?;
    if outcome.diff.is_some() {
        last.diff = outcome.diff;
    }
    if outcome.resource_group_update.is_some() {
        last.resource_group_update = outcome.resource_group_update;
    }
    Ok(())
}

fn parse_statement(sql: &str, default_schema: &str) -> Result<DdlStatement, BootstrapError> {
    let parsed = tidb_parser::parse(sql).map_err(|error| BootstrapError::Job {
        statement: sql.lines().next().unwrap_or(sql).trim().to_owned(),
        detail: format!("does not parse: {error:?}"),
    })?;
    lower_ddl_with_context(
        &parsed,
        default_schema,
        &(std::sync::Arc::new(bootstrap_context(sql)) as tidb_ddl::cluster_ddl::DdlContext),
    )
        .map_err(|error| BootstrapError::Job {
            statement: sql.lines().next().unwrap_or(sql).trim().to_owned(),
            detail: error.to_string(),
        })?
        .ok_or_else(|| BootstrapError::Job {
            statement: sql.lines().next().unwrap_or(sql).trim().to_owned(),
            detail: "is not a catalog change".to_owned(),
        })
}

/// Refuses a keyspace that already carries the `mysql` database, found by
/// name: under the classic layout no id is reserved for it.
fn refuse_if_present<S: MetaSnapshot>(snapshot: &mut S) -> Result<(), BootstrapError> {
    for (raw_key, stored) in snapshot.scan_prefix(&key::databases_kv_prefix())? {
        let (_, field) = tidb_meta::structure::decode_hash_data_key(&raw_key)
            .map_err(|error| BootstrapError::Encode(format!("DBs hash key: {error}")))?;
        if !key::has_prefix(key::DB_PREFIX, &field) {
            continue;
        }
        let database = value::parse_db_info(&stored)
            .map_err(|error| BootstrapError::Encode(format!("DBInfo: {error}")))?;
        if database.name.lowercase() == SYSTEM_DB {
            return Err(BootstrapError::AlreadyPresent {
                object: format!("the `{SYSTEM_DB}` database"),
            });
        }
    }
    Ok(())
}

/// Plans the bootstrap in `layout`.
pub(super) fn plan<S: MetaSnapshot>(
    snapshot: &mut S,
    start_ts: u64,
    environment: &BootstrapEnvironment,
    layout: BootstrapLayout,
) -> Result<BootstrapWrite, BootstrapError> {
    plan_with(snapshot, start_ts, environment, layout, true)
}

/// A `TableInfo` written into meta, as `createAndSplitTables` does.
fn write_table<S: MetaSnapshot>(
    overlay: &mut Overlay<'_, S>,
    database_id: i64,
    info: &TableInfo,
) -> Result<(), BootstrapError> {
    overlay.apply(&[BufferMutation::set(
        key::table_kv_key(database_id, info.id),
        value::serialize_table_info(info).map_err(encode_error)?,
    )?]);
    Ok(())
}

fn write_database<S: MetaSnapshot>(
    overlay: &mut Overlay<'_, S>,
    id: i64,
    name: &str,
) -> Result<(), BootstrapError> {
    let database = DBInfo {
        id,
        name: CiString::new(name),
        charset: SYSTEM_DB_CHARSET.to_owned(),
        collate: SYSTEM_DB_COLLATION.to_owned(),
        state: SchemaState::PUBLIC,
        ..DBInfo::default()
    };
    overlay.apply(&[BufferMutation::set(
        key::database_kv_key(id),
        value::serialize_db_info(&database).map_err(encode_error)?,
    )?]);
    Ok(())
}

/// [`plan`], choosing whether each job's planners see the tables the earlier
/// jobs created (`false`) or only the DDL-owned ones (`true`; classic layout
/// only, whose 53 table jobs are what the withholding saves).
fn plan_with<S: MetaSnapshot>(
    snapshot: &mut S,
    start_ts: u64,
    environment: &BootstrapEnvironment,
    layout: BootstrapLayout,
    withhold_created_tables: bool,
) -> Result<BootstrapWrite, BootstrapError> {
    let classic = layout == BootstrapLayout::Classic;
    refuse_if_present(snapshot)?;
    let mut overlay = Overlay::new(snapshot);
    let mut clock = Clock(start_ts);

    // Go `bootstrap`'s first step, `InitMDLVariableForBootstrap`.
    overlay.apply(&[BufferMutation::set(
        key::metadata_lock_kv_key(),
        b"1".to_vec(),
    )?]);

    // `bootstrapSchemas` (next-gen) and `InitDDLTables`: the `mysql` database
    // is `SystemDatabaseID` under the next-gen kernel and the next global id
    // under the classic one.
    let database_id = if classic {
        let current = match overlay.get(&key::next_global_id_kv_key())? {
            Some(stored) => value::parse_int_value(&stored)
                .map_err(|error| BootstrapError::Encode(format!("NextGlobalID: {error}")))?,
            // Go's `Inc` treats a missing key as zero.
            None => 0,
        };
        overlay.apply(&[BufferMutation::set(
            key::next_global_id_kv_key(),
            value::encode_int_value(current + 1),
        )?]);
        current + 1
    } else {
        SYSTEM_DATABASE_ID
    };
    write_database(&mut overlay, database_id, SYSTEM_DB)?;

    let mut created_tables = Vec::new();
    let mut global_batches = Vec::new();
    if !classic {
        // `bootstrapSchemas`: `sys` and every `mysql.*` table at its reserved
        // id, cut in one split, built under the metabuild default
        // (`tidb_enable_clustered_index` ON: `mysql.user`, `mysql.tidb`,
        // `mysql.global_variables` and most others have a common handle).
        write_database(&mut overlay, tidb_metadef::system::SYS_DATABASE_ID, SYS_DB)?;
        let mut batch = Vec::new();
        for table in BOOTSTRAP_TABLES
            .iter()
            .filter(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name))
        {
            let info = build_bootstrap_table(table, start_ts)?;
            write_table(&mut overlay, database_id, &info)?;
            batch.push(info.id);
            created_tables.push(info);
        }
        global_batches.push(batch);
    }

    // The DDL-owned tables keep their reserved ids under both kernels and
    // are written straight into meta, without a job or a schema version.
    for version in DDL_TABLE_VERSION_TABLES
        .iter()
        .filter(|version| environment.ddl_table_version < version.version)
    {
        let mut group = Vec::with_capacity(version.tables.len());
        for table in version.tables {
            // Go `InitDDLTables` builds under the metabuild default
            // (`tidb_enable_clustered_index` ON).
            let info = build_bootstrap_table(table, start_ts)?;
            write_table(&mut overlay, database_id, &info)?;
            group.push(info.id);
            created_tables.push(info);
        }
        global_batches.push(group);
    }
    let ddl_table_version = DDL_TABLE_VERSION_TABLES
        .last()
        .map_or(environment.ddl_table_version, |version| {
            environment.ddl_table_version.max(version.version)
        });
    if ddl_table_version > environment.ddl_table_version {
        overlay.apply(&[BufferMutation::set(
            key::ddl_table_version_kv_key(),
            value::encode_int_value(ddl_table_version),
        )?]);
    }

    // Go `doDDLWorks`.
    if classic && withhold_created_tables {
        let visible = overlay.table_keys();
        overlay.withhold_tables_but(visible);
    }
    let mut last = JobOutcome::default();
    let mut table_names = Vec::new();
    if classic {
        // `systemDatabases`: `mysql` exists, so its `IF NOT EXISTS` queues no
        // job; `sys` does. Then one `CREATE TABLE` per `mysql.*` table.
        let create_sys = format!("CREATE DATABASE IF NOT EXISTS `{SYS_DB}`");
        run(
            &mut overlay,
            &mut clock,
            &mut last,
            &create_sys,
            &parse_statement(&create_sys, SYSTEM_DB)?,
        )?;
        for table in BOOTSTRAP_TABLES
            .iter()
            .filter(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name))
        {
            run(
                &mut overlay,
                &mut clock,
                &mut last,
                table.create_sql,
                &parse_statement(table.create_sql, SYSTEM_DB)?,
            )?;
            table_names.push(table.name);
        }
    }
    // The views Go creates after `insertBuiltinBindInfoRow`, whose DML is
    // seeded with the other rows below.
    for view in BOOTSTRAP_VIEWS {
        let statement = DdlStatement::CreateView {
            schema: view.schema.to_owned(),
            name: view.name.to_owned(),
            or_replace: true,
            info: Box::new(build_bootstrap_view(view, 0, 0)),
        };
        run(
            &mut overlay,
            &mut clock,
            &mut last,
            view.create_sql,
            &statement,
        )?;
    }
    let create_test = "CREATE DATABASE IF NOT EXISTS test";
    run(
        &mut overlay,
        &mut clock,
        &mut last,
        create_test,
        &parse_statement(create_test, SYSTEM_DB)?,
    )?;
    // `ALTER RESOURCE GROUP default BACKGROUND=(TASK_TYPES='stats')`.
    let (default_group, _) = bootstrap_default_resource_group()?;
    let alter_group = format!(
        "ALTER RESOURCE GROUP `default` BACKGROUND=(TASK_TYPES='{}')",
        tidb_txnkv::INTERNAL_TXN_STATS
    );
    run(
        &mut overlay,
        &mut clock,
        &mut last,
        &alter_group,
        &DdlStatement::AlterResourceGroup {
            group: Box::new(default_group),
        },
    )?;
    let diff = last
        .diff
        .ok_or_else(|| BootstrapError::Encode("the bootstrap published no schema".to_owned()))?;
    let resource_group_update = match last.resource_group_update {
        Some(update) => update,
        None => bootstrap_default_resource_group()?.1,
    };

    // The tables the jobs created, as the catalog now holds them.
    overlay.show_all();
    let mut table_ids = Vec::with_capacity(table_names.len());
    if classic {
        let stored = tidb_cluster::cluster_catalog::load_database_tables(&mut overlay, database_id)?;
        for name in &table_names {
            let info = stored
                .iter()
                .find(|table| table.name.lowercase() == *name)
                .ok_or_else(|| BootstrapError::Encode(format!("mysql.{name} was not created")))?;
            table_ids.push(info.id);
            created_tables.push(info.clone());
        }
    }

    // Go `doDMLWorks`.
    let seed_environment = BootstrapEnvironment {
        ddl_table_version,
        ..environment.clone()
    };
    let mut seeded = Vec::new();
    rows::seed(database_id, &created_tables, &seed_environment, &mut seeded)?;
    overlay.apply(&seeded);

    // Go `InitTiDBSchemaCacheSize`.
    if read_schema_cache_size(&mut overlay)?.is_none() {
        let schema_cache_size = u64::try_from(tidb_vardef::defaults::DEF_TIDB_SCHEMA_CACHE_SIZE)
            .expect("the source schema-cache default is non-negative");
        overlay.apply(&[BufferMutation::set(
            schema_cache_size_kv_key(),
            schema_cache_size.to_string().as_bytes(),
        )?]);
    }

    let schema_version = diff.version;
    let mutations = overlay.net_mutations()?;
    Ok(BootstrapWrite {
        splits: BootstrapSplits {
            global_batches,
            table_ids,
        },
        mutations,
        schema_version,
        diff,
        created_tables,
        resource_group_update,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Time, TimeType};

    #[derive(Default)]
    struct MetaStore;

    impl MetaSnapshot for MetaStore {
        fn get(&mut self, _: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(&mut self, _: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    fn environment() -> BootstrapEnvironment {
        BootstrapEnvironment {
            system_tz: "UTC".to_owned(),
            new_collation_enabled: true,
            cluster_id: 1,
            current_timestamp: Time::from_date_checked(
                2026,
                10,
                21,
                0,
                0,
                0,
                0,
                TimeType::Timestamp,
                0,
            )
            .unwrap(),
            ddl_table_version: 0,
        }
    }

    /// Withholding the created tables from each job's catalog scans is an
    /// optimisation, not a behaviour: both ways plan the same bytes.
    #[test]
    fn hide_created_tables_changes_nothing() {
        let start_ts = 467_996_279_696_261_139;
        let hidden = plan_with(
            &mut MetaStore,
            start_ts,
            &environment(),
            BootstrapLayout::Classic,
            true,
        )
        .unwrap();
        let shown = plan_with(
            &mut MetaStore,
            start_ts,
            &environment(),
            BootstrapLayout::Classic,
            false,
        )
        .unwrap();
        // A history row carries the process-wide history sequence number, which
        // the second plan continues from: compare those rows by count only.
        let history_prefix =
            tidb_codec::table_key::gen_table_prefix(tidb_metadef::system::TI_DBDDLHISTORY_TABLE_ID);
        let is_history = |raw_key: &[u8]| {
            raw_key.starts_with(&history_prefix)
                || tidb_meta::structure::decode_hash_data_key(raw_key)
                    .is_ok_and(|(hash, _)| hash == b"DDLJobHistory")
        };
        let bytes = |write: &BootstrapWrite| {
            write
                .mutations
                .iter()
                .filter(|mutation| !is_history(mutation.key()))
                .map(|mutation| {
                    (
                        mutation.key().to_vec(),
                        format!("{:?}", mutation.kind()),
                        mutation.value().to_vec(),
                    )
                })
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(hidden.mutations.len(), shown.mutations.len());
        assert!(bytes(&hidden) == bytes(&shown));
    }
}
