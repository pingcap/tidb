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

//! Source-backed tests for the catalog writer.
//!
//! The `GO_*` fixtures below are not hand-written: they are the exact
//! `TableInfo` JSON a real TiDB v8.5.6 stored for the same `CREATE TABLE`
//! text, read back from that server's own `/schema/<db>/<table>` status
//! endpoint (which re-marshals the stored struct). The decisive property is
//! that what this node writes carries the same values, because a Go server
//! must be able to load and serve it.

use std::collections::BTreeMap;

use tidb_datatype::Datum;
use tidb_ddl_notifier::SchemaChangeEvent;
use tidb_exec::cluster_catalog::{
    load_cluster_catalog, prefix_scan_end, ClusterCatalogError, MetaPairs, MetaSnapshot,
};
use tidb_exec::cluster_ddl::{
    lower_ddl, lower_ddl_with_context, plan_ddl, plan_ddl_with_collation,
    plan_persisted_ddl_job_failure, plan_persisted_ddl_job_step,
    prepare_check_constraint_job_submission, prepare_materialized_view_job_submission,
    AlterColumnAction, DdlPlan, DdlPlanError, DdlStatement, IndexBackfillOperation, MdlInfoUpdate,
    PersistedDdlJobFailure, PersistedDdlJobPlan, PersistedDdlJobStep,
};

use tidb_exec::cluster_ddl::DdlJobSchemaState;
use tidb_exec::ddl_history_table::DdlHistoryTable;
use tidb_exec::ddl_job_submit::{finish_insert_attempt, plan_insert_attempt};
use tidb_exec::ddl_job_table::DdlJobTable;
use tidb_exec::ddl_systable::{SystemTableManager, SystemTableManagerError};
use tidb_exec::mysql_system_tables::{SystemRow, SystemTableView};
use tidb_exec::real_tikv_ddl::{prepare_cluster_ddl_with_context, DdlWarningLevel};
use tidb_exec::system_row_write::store_clustered_row;
use tidb_exec::table_info_build::{build_table_info, ClusteredIndexDefMode};
use tidb_meta::{key, value};
use tidb_model::{
    ActionType, DBInfo, GoAnyView, GoShared, Job, JobArgsValue, JobState, JobVersion,
    MaterializedViewLogInfo, SchemaState, TimeZoneLocation,
};
use tidb_txnkv::transaction::{BufferMutation, BufferMutationOp};

fn plan_worker_step(
    store: &mut MetaStore,
    job_id: i64,
    ts: u64,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    match plan_persisted_ddl_job_step(store, job_id, ts, DdlJobSchemaState::new(true), &|| {
        tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT
    })? {
        PersistedDdlJobPlan::Step(step) => Ok(step),
        PersistedDdlJobPlan::Paused => panic!("paused job has no worker transaction"),
        PersistedDdlJobPlan::SchemaSync { .. } => panic!("a previous MDL barrier is still pending"),
    }
}

fn finish_worker_job(store: &mut MetaStore, job_id: i64, ts: u64) {
    let step = plan_worker_step(store, job_id, ts).unwrap();
    assert!(
        step.terminal,
        "only the post-sync history transaction removes the job"
    );
    assert_eq!(
        step.write.schema_version, 0,
        "history does not publish another schema"
    );
    apply(store, &step.write);
}

#[test]
fn on_update_current_timestamp_uses_go_exact_function_and_fsp_rules() {
    let cases = [
        (
            "CREATE TABLE t (a TIMESTAMP ON UPDATE CURRENT_TIMESTAMP())",
            true,
        ),
        (
            "CREATE TABLE t (a TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3))",
            true,
        ),
        // TiDB's parser canonicalizes the accepted aliases to the
        // CURRENT_TIMESTAMP function node before validation.
        ("CREATE TABLE t (a TIMESTAMP ON UPDATE NOW())", true),
        ("CREATE TABLE t (a TIMESTAMP ON UPDATE LOCALTIME())", true),
        (
            "CREATE TABLE t (a TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP)",
            false,
        ),
        (
            "CREATE TABLE t (a TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(3))",
            false,
        ),
    ];

    for (sql, accepted) in cases {
        let statement = tidb_parser::parse(sql).expect("source DDL parses");
        let tidb_ast::Stmt::Ddl(ddl) = statement else {
            panic!("source statement is not DDL: {sql}")
        };
        let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
            panic!("source statement is not CREATE TABLE: {sql}")
        };
        let on_update = create.columns[0]
            .options
            .iter()
            .find_map(|option| match option {
                tidb_ast::ColumnOption::OnUpdate(expr) => Some(expr),
                _ => None,
            })
            .expect("source column has ON UPDATE");
        let field_type = tidb_executor::ddl::column_field_type::build_field_type(
            &create.columns[0].name,
            &create.columns[0].ty,
            "utf8mb4",
            "utf8mb4_bin",
        )
        .expect("source column type builds");
        assert_eq!(
            tidb_expr::is_valid_current_timestamp_expr(on_update, Some(&field_type)),
            accepted,
            "source SQL: {sql}"
        );
        let result = build_table_info(create, "utf8mb4", "utf8mb4_bin", ClusteredIndexDefMode::On);
        assert_eq!(result.is_ok(), accepted, "source SQL: {sql}");
    }
}

/// A mutable snapshot of stored meta bytes: reads observe it, and a test may
/// apply a planned write set to it to model the transaction having committed.
#[derive(Clone, Default)]
pub(crate) struct MetaStore {
    pub(crate) pairs: BTreeMap<Vec<u8>, Vec<u8>>,
    last_range: Option<(Vec<u8>, Vec<u8>)>,
}

impl MetaStore {
    fn put(&mut self, raw_key: Vec<u8>, raw_value: impl Into<Vec<u8>>) {
        self.pairs.insert(raw_key, raw_value.into());
    }
}

impl MetaSnapshot for MetaStore {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let end = prefix_scan_end(prefix).expect("finite scan end");
        Ok(self
            .pairs
            .range(prefix.to_vec()..end)
            .map(|(stored_key, stored_value)| (stored_key.clone(), stored_value.clone()))
            .collect())
    }

    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        self.last_range = Some((start.to_vec(), end.to_vec()));
        Ok(self
            .pairs
            .range(start.to_vec()..end.to_vec())
            .map(|(stored_key, stored_value)| (stored_key.clone(), stored_value.clone()))
            .collect())
    }
}

/// One database, `u6` with id 112, at schema version 60 and max used id 116 —
/// the shape the ground-truth cluster was actually in.
pub(crate) fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    store.put(key::next_global_id_kv_key(), b"116".to_vec());
    store.put(key::schema_version_kv_key(), b"60".to_vec());
    store.put(
        key::database_kv_key(112),
        br#"{"id":112,"db_name":{"O":"u6","L":"u6"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#.to_vec(),
    );
    let mysql_id = tidb_metadef::system::SYSTEM_DATABASE_ID;
    let mysql = DBInfo {
        id: mysql_id,
        name: tidb_ast::CiString::new("mysql"),
        charset: "utf8mb4".to_owned(),
        collate: "utf8mb4_bin".to_owned(),
        state: SchemaState::PUBLIC,
        ..DBInfo::default()
    };
    store.put(
        key::database_kv_key(mysql_id),
        value::serialize_db_info(&mysql).expect("the mysql database encodes"),
    );
    for system_table_name in [
        tidb_metadef::system_tables_def::NOTIFIER_TABLE_NAME,
        "tidb_ddl_job",
        "tidb_ddl_history",
        "tidb_mdl_info",
        "tidb_mlog_purge_info",
        "tidb_mview_refresh_info",
    ] {
        let system_table = tidb_metadef::DDL_TABLE_VERSION_TABLES
            .iter()
            .flat_map(|version| version.tables)
            .chain(tidb_metadef::BOOTSTRAP_TABLES.iter())
            .find(|table| table.name == system_table_name)
            .unwrap_or_else(|| panic!("the pinned bootstrap defines {system_table_name}"));
        let parsed = tidb_parser::parse(system_table.create_sql)
            .unwrap_or_else(|error| panic!("the {system_table_name} DDL parses: {error:?}"));
        let tidb_ast::Stmt::Ddl(ddl) = parsed else {
            panic!("the {system_table_name} definition is DDL")
        };
        let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
            panic!("the {system_table_name} definition creates a table")
        };
        let mut info = build_table_info(
            create,
            "utf8mb4",
            "utf8mb4_bin",
            ClusteredIndexDefMode::IntOnly,
        )
        .unwrap_or_else(|error| panic!("the {system_table_name} TableInfo builds: {error:?}"));
        info.id = system_table.id;
        store.put(
            key::table_kv_key(mysql_id, system_table.id),
            value::serialize_table_info(&info).unwrap_or_else(|error| {
                panic!("the {system_table_name} TableInfo encodes: {error}")
            }),
        );
    }
    store
}

struct PlannedCheckSubmission {
    job: Job,
    mutations: Vec<BufferMutation>,
}

fn plan_check_constraint_job_submission(
    store: &mut MetaStore,
    statement: &DdlStatement,
    start_ts: u64,
) -> Result<Option<PlannedCheckSubmission>, DdlPlanError> {
    let Some(mut spec) =
        prepare_check_constraint_job_submission(store, statement, start_ts, false, 0)?
    else {
        return Ok(None);
    };
    let catalog = load_cluster_catalog(store)?;
    let mut before_insert_with_assigned_ids =
        |_: &[tidb_exec::ddl_job_submit::JobSpec]| Option::<fn()>::None;
    let (mutations, _) = plan_insert_attempt(
        store,
        &catalog,
        std::slice::from_mut(&mut spec),
        &mut before_insert_with_assigned_ids,
    )?;
    Ok(Some(PlannedCheckSubmission {
        job: spec.job,
        mutations,
    }))
}

pub(crate) fn statement(sql: &str) -> DdlStatement {
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    lower_ddl(&parsed, "u6")
        .unwrap_or_else(|error| panic!("the fixture SQL is admitted: {sql}: {error:?}"))
        .expect("the fixture SQL is a catalog change")
}

fn stored_default_bytes(sql: &str) -> Vec<u8> {
    let DdlStatement::CreateTable { build, .. } = statement(sql) else {
        panic!("the fixture is not CREATE TABLE: {sql}");
    };
    let column = build
        .template()
        .columns
        .get(0)
        .expect("the fixture declares one non-null column");
    let column = column.read();
    match column.default_value.view() {
        Some(GoAnyView::String(bytes)) => bytes.as_bytes().to_vec(),
        other => panic!("the fixture stored a non-string default: {other:?}"),
    }
}

fn refusal_with_code(sql: &str) -> (u16, String) {
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    let error =
        lower_ddl(&parsed, "u6").expect_err("this shape must be refused before any mutation");
    (error.code, error.reason)
}

fn refusal(sql: &str) -> String {
    refusal_with_code(sql).1
}

#[test]
fn active_ddl_jobs_use_the_go_job_table_lifecycle() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrap catalog loads");
    let table = DdlJobTable::locate(&catalog).expect("the Go active-job table exists");
    assert!(
        table.table().indices.is_empty(),
        "the pinned integer primary key is the record handle"
    );

    let mut job = Job::default();
    job.id = 117;
    job.type_ = ActionType::ACTION_ADD_CHECK_CONSTRAINT;
    job.schema_id = 112;
    job.table_id = 116;
    job.state = JobState::QUEUEING;
    job.version = JobVersion::V1;
    job.start_ts = 1_001;
    let mut insert = Vec::new();
    table
        .append_insert(&mut job, false, "112", "116", false, &mut insert)
        .expect("Go job submission row encodes");
    assert_eq!(insert.len(), 1);
    assert_eq!(insert[0].kind(), BufferMutationOp::Set);
    apply_mutations(&mut store, &insert);

    let mut active = table.load(&mut store).expect("the owner scans jobs");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].job.id, 117);
    assert_eq!(active[0].job.type_, job.type_);
    assert_eq!(active[0].job.raw_args.as_ref().unwrap().get(), "null");
    assert_eq!(active[0].schema_ids, "112");
    assert_eq!(active[0].table_ids, "116");
    assert!(!active[0].reorg);
    assert!(!active[0].processing);

    active[0].job.state = JobState::RUNNING;
    active[0].job.schema_state = SchemaState::WRITE_ONLY;
    active[0].job.last_schema_version = 61;
    let mut update = Vec::new();
    table
        .append_update(&mut active[0], false, &mut update)
        .expect("Go worker job update encodes");
    assert_eq!(update.len(), 1);
    assert_eq!(update[0].kind(), BufferMutationOp::Set);
    apply_mutations(&mut store, &update);

    let active = table
        .load(&mut store)
        .expect("the new owner resumes the job");
    assert_eq!(active[0].job.state, JobState::RUNNING);
    assert_eq!(active[0].job.schema_state, SchemaState::WRITE_ONLY);
    assert_eq!(active[0].job.last_schema_version, 61);
    assert_eq!(active[0].job.raw_args.as_ref().unwrap().get(), "null");

    let mut delete = Vec::new();
    table
        .append_delete(&active[0], &mut delete)
        .expect("Go terminal deletion encodes");
    assert_eq!(delete.len(), 1);
    assert_eq!(delete[0].kind(), BufferMutationOp::Delete);
    apply_mutations(&mut store, &delete);
    assert!(table.load(&mut store).unwrap().is_empty());
}

#[test]
fn flashback_admission_reads_only_the_go_query_columns() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrap catalog loads");
    let table = DdlJobTable::locate(&catalog).expect("the Go active-job table exists");

    let mut ordinary = Job::default();
    ordinary.id = 117;
    ordinary.type_ = ActionType::ACTION_CREATE_TABLE;
    let mut insert = Vec::new();
    table
        .append_insert(&mut ordinary, false, "112", "116", false, &mut insert)
        .expect("the ordinary row encodes");
    apply_mutations(&mut store, &insert);

    let view = SystemTableView::project(
        "mysql.tidb_ddl_job",
        table.table(),
        &[
            "job_id",
            "reorg",
            "schema_ids",
            "table_ids",
            "job_meta",
            "type",
            "processing",
        ],
    );
    let row = SystemRow::parse(&view, insert[0].key(), insert[0].value())
        .expect("the inserted row decodes");
    let mut corrupt = row.into_values();
    let existing = corrupt.clone();
    let job_meta_id = table
        .table()
        .cols()
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "job_meta")
        .expect("the job table has job_meta")
        .read()
        .id;
    corrupt.insert(job_meta_id, Datum::Bytes(b"{".to_vec()));
    let rewrite = store_clustered_row(table.table(), Some(&existing), &corrupt)
        .expect("the malformed fixture row encodes");
    apply_mutations(&mut store, &rewrite);

    assert!(
        table.load(&mut store).is_err(),
        "the full scheduler load decodes job_meta"
    );
    assert!(
        !table
            .has_flashback_cluster_job(&mut store, 0)
            .expect("Go's admission query does not read job_meta"),
        "an unrelated malformed job is not a flashback job"
    );

    let mut flashback = Job::default();
    flashback.id = 118;
    flashback.type_ = ActionType::ACTION_FLASHBACK_CLUSTER;
    let mut insert = Vec::new();
    table
        .append_insert(&mut flashback, false, "0", "0", false, &mut insert)
        .expect("the flashback row encodes");
    apply_mutations(&mut store, &insert);
    assert!(table
        .has_flashback_cluster_job(&mut store, 118)
        .expect("the source-shaped query reads the action column"));
    assert!(!table
        .has_flashback_cluster_job(&mut store, 119)
        .expect("the source-shaped query honors its minimum job ID"));
    let jobs = table
        .load_from(&mut store, 118)
        .expect("the scheduler lower bound skips older malformed metadata");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job.id, 118);
    let view = SystemTableView::project("mysql.tidb_ddl_job", table.table(), &["job_id"]);
    let expected_start = view
        .record_prefix(&[Datum::Int(118)])
        .expect("the integer clustered handle encodes");
    let expected_end = prefix_scan_end(&view.record_prefix(&[]).unwrap()).unwrap();
    assert_eq!(store.last_range, Some((expected_start, expected_end)));
}

#[test]
fn ddl_systable_manager_matches_go_queries() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrap catalog loads");
    let manager = SystemTableManager::new(&catalog);
    assert!(matches!(
        manager.get_job_by_id(&mut store, 9_999),
        Err(SystemTableManagerError::NotFound)
    ));
    assert_eq!(manager.get_min_job_id(&mut store, 0).unwrap(), 0);
    assert!(!manager.has_flashback_cluster_job(&mut store, 0).unwrap());

    let table = DdlJobTable::locate(&catalog).expect("the active-job table exists");
    let mut job = Job::default();
    job.id = 9_999;
    job.type_ = ActionType::ACTION_CREATE_SCHEMA;
    let mut mutations = Vec::new();
    table
        .append_insert(&mut job, false, "1", "1", false, &mut mutations)
        .expect("the job row encodes");
    let expected_bytes = job.encode(true).expect("the exact job bytes encode");
    apply_mutations(&mut store, &mutations);
    let loaded = manager
        .get_job_by_id(&mut store, 9_999)
        .expect("the inserted job is found");
    assert_eq!(loaded.bytes.snapshot(), expected_bytes);
    assert_eq!(loaded.job.unwrap().read().id, 9_999);

    assert!(matches!(
        manager.get_mdl_version(&mut store, 9_999),
        Err(SystemTableManagerError::NotFound)
    ));
    let (_, mdl_table) = catalog
        .find_table("mysql", "tidb_mdl_info")
        .expect("the MDL table exists");
    let mdl = MdlInfoUpdate {
        omit_owner_id: false,
        table: Box::new(mdl_table.clone_like_go()),
        table_ids: "1".to_owned(),
    };
    let mut mutations = Vec::new();
    mdl.append_mutations(9_999, 123, "owner", &mut mutations)
        .expect("the MDL row encodes");
    apply_mutations(&mut store, &mutations);
    assert_eq!(
        manager
            .get_mdl_version(&mut store, 9_999)
            .expect("the MDL version is found"),
        123
    );

    assert_eq!(manager.get_min_job_id(&mut store, 0).unwrap(), 9_999);
    assert_eq!(manager.get_min_job_id(&mut store, 9_999).unwrap(), 9_999);
    assert_eq!(manager.get_min_job_id(&mut store, 10_000).unwrap(), 0);
    assert!(!manager.has_flashback_cluster_job(&mut store, 0).unwrap());

    let mut flashback = Job::default();
    flashback.id = 10_000;
    flashback.type_ = ActionType::ACTION_FLASHBACK_CLUSTER;
    let mut mutations = Vec::new();
    table
        .append_insert(&mut flashback, false, "0", "0", false, &mut mutations)
        .expect("the flashback row encodes");
    apply_mutations(&mut store, &mutations);
    assert!(manager
        .has_flashback_cluster_job(&mut store, 10_000)
        .unwrap());
    assert!(!manager
        .has_flashback_cluster_job(&mut store, 10_001)
        .unwrap());
}

#[test]
fn ddl_sql_history_uses_go_insert_ignore_semantics() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrap catalog loads");
    let history = DdlHistoryTable::locate(&catalog).expect("the Go history table exists");
    let mut job = Job::default();
    job.id = 117;
    job.type_ = ActionType::ACTION_ADD_CHECK_CONSTRAINT;
    job.schema_id = 112;
    job.table_id = 116;
    job.schema_name = "u6".into();
    job.table_name = "t".into();
    job.state = JobState::QUEUEING;
    job.start_ts = 1_001;
    let first = job.encode(true).expect("the submitted job encodes");
    let mut insert = Vec::new();
    history
        .append_insert_ignore(&mut store, &job, &first, &mut insert)
        .expect("the first INSERT IGNORE plans");
    assert_eq!(insert.len(), 1);
    assert_eq!(insert[0].kind(), BufferMutationOp::Set);
    apply_mutations(&mut store, &insert);

    job.state = JobState::DONE;
    let second = job.encode(true).expect("the terminal job encodes");
    let mut duplicate = Vec::new();
    history
        .append_insert_ignore(&mut store, &job, &second, &mut duplicate)
        .expect("a duplicate INSERT IGNORE succeeds");
    assert!(
        duplicate.is_empty(),
        "the existing SQL history row is preserved"
    );
    let stored = history.load(&mut store).expect("SQL history scans");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].state, JobState::QUEUEING);
}

#[test]
fn persisted_catalog_actions_share_sync_and_history_lifecycle() {
    use tidb_model::{
        BatchCreateTableArgs, CreateSchemaArgs, CreateTableArgs, GoField, HistoryInfo,
        RenameTableArgs, RenameTablesArgs, TableInfo,
    };
    fn execute(store: &mut MetaStore, job: &mut Job, ids: &str, expected_phases: usize) {
        let catalog = load_cluster_catalog(store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        // Go retains earlier retry diagnostics on the job. A later successful
        // action must finish normally, not treat this as a fresh cancellation.
        job.error = Some(GoShared::new(tidb_error::terror::TerrorError::compatible(
            tidb_error::terror::TerrorCode::new(1105),
            "earlier retry failed",
        )));
        job.error_count = 2;
        let mut mutations = Vec::new();
        queue
            .append_insert(job, false, "112", ids, false, &mut mutations)
            .unwrap();
        apply_mutations(store, &mutations);
        let mut phases = 0;
        loop {
            let step = plan_worker_step(store, job.id, 10_000 + phases as u64).unwrap();
            apply(store, &step.write);
            if step.terminal {
                break;
            }
            phases += 1;
            assert!(phases <= expected_phases, "action failed to reach DONE");
            assert!(!store
                .pairs
                .contains_key(&key::ddl_job_history_kv_key(job.id)));
            assert_eq!(
                queue.load_by_id(store, job.id).unwrap().unwrap().table_ids,
                ids
            );
            let mdl = step
                .write
                .mdl_info_update
                .as_ref()
                .expect("schema publication registers MDL");
            assert_eq!(mdl.table_ids, ids);
            let mut registration = Vec::new();
            mdl.append_mutations(
                job.id,
                step.write.schema_version,
                "first-owner",
                &mut registration,
            )
            .unwrap();
            apply_mutations(store, &registration);
            match plan_persisted_ddl_job_step(
                store,
                job.id,
                11_000,
                DdlJobSchemaState::new(true),
                &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT,
            )
            .unwrap()
            {
                PersistedDdlJobPlan::SchemaSync { version, .. } => {
                    assert_eq!(version, step.write.schema_version)
                }
                _ => panic!("a restarted owner must recover MDL before advancing"),
            }
            let mut cleanup = Vec::new();
            mdl.append_delete_mutations(store, job.id, "replacement-owner", &mut cleanup)
                .unwrap();
            assert!(
                cleanup.is_empty(),
                "an owner must not delete a different owner's MDL row"
            );
            mdl.append_delete_mutations(store, job.id, "first-owner", &mut cleanup)
                .unwrap();
            apply_mutations(store, &cleanup);
        }
        assert_eq!(phases, expected_phases);
        assert!(queue.load_by_id(store, job.id).unwrap().is_none());
        if matches!(
            job.type_,
            ActionType::ACTION_DROP_SCHEMA | ActionType::ACTION_DROP_TABLE
        ) {
            let mut completed = Job::default();
            completed
                .decode(
                    store
                        .pairs
                        .get(&key::ddl_job_history_kv_key(job.id))
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(
                completed.raw_args, job.raw_args,
                "lifecycle steps preserve unmodified submission args"
            );
        }

        let history = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(store)
            .unwrap();
        assert_eq!(
            history
                .iter()
                .find(|entry| entry.id == job.id)
                .unwrap()
                .state,
            JobState::SYNCED
        );
        let completed = history.iter().find(|entry| entry.id == job.id).unwrap();
        assert_eq!(completed.error_count, 2);
        assert_eq!(
            completed.error.as_ref().unwrap().read().message(),
            "earlier retry failed"
        );
        if job.type_ == ActionType::ACTION_CREATE_TABLES {
            let entry = history.iter().find(|entry| entry.id == job.id).unwrap();
            let info = entry.binlog_info.as_ref().unwrap().read();
            let tables = info
                .multiple_table_infos
                .iter_deref()
                .map(|table| {
                    let table = table.read();
                    assert_eq!(table.state, SchemaState::PUBLIC);
                    table.id.to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                tables.join(","),
                ids,
                "batch history retains every published table"
            );
            assert!(
                info.table_info.is_none(),
                "Go SetTableInfos does not select a single table"
            );
        }
    }
    let mut store = bootstrapped();
    let mut job = Job::default();
    job.id = 200;
    job.schema_id = 112;
    job.schema_name = "u6".into();
    job.version = JobVersion::V2;
    job.state = JobState::QUEUEING;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.type_ = ActionType::ACTION_CREATE_SCHEMA;
    job.schema_id = 300;
    job.fill_args(Some(GoShared::new(CreateSchemaArgs {
        db_info: GoField::new(Some(GoShared::new(DBInfo {
            id: 300,
            name: tidb_ast::CiString::new("destination"),
            ..Default::default()
        }))),
    })));
    execute(&mut store, &mut job, "0", 1);

    job.id += 1;
    job.schema_id = 112;
    job.table_id = 301;
    job.type_ = ActionType::ACTION_CREATE_TABLE;
    let table_args = |id, name: &str| CreateTableArgs {
        table_info: GoField::new(Some(GoShared::new(TableInfo {
            id,
            name: tidb_ast::CiString::new(name),
            ..Default::default()
        }))),
        ..Default::default()
    };
    job.fill_args(Some(GoShared::new(table_args(301, "first"))));
    execute(&mut store, &mut job, "301", 1);

    job.id += 1;
    job.type_ = ActionType::ACTION_CREATE_TABLES;
    job.fill_args(Some(GoShared::new(BatchCreateTableArgs {
        tables: GoField::new(vec![table_args(302, "second"), table_args(303, "third")].into()),
    })));
    execute(&mut store, &mut job, "302,303", 1);
    job.id += 1;
    job.fill_args(Some(GoShared::new(BatchCreateTableArgs::default())));
    execute(&mut store, &mut job, "", 1);

    job.id += 1;
    job.type_ = ActionType::ACTION_RENAME_TABLES;
    job.fill_args(Some(GoShared::new(RenameTablesArgs {
        rename_table_infos: GoField::new(
            vec![RenameTableArgs {
                old_schema_id: 112,
                new_schema_id: 300,
                table_id: 301,
                old_schema_name: tidb_ast::CiString::new("u6"),
                old_table_name: tidb_ast::CiString::new("first"),
                new_table_name: tidb_ast::CiString::new("renamed"),
                ..Default::default()
            }]
            .into(),
        ),
    })));
    execute(&mut store, &mut job, "301", 2);

    job.id += 1;
    job.type_ = ActionType::ACTION_DROP_TABLE;
    job.schema_id = 300;
    job.fill_v2_arg(serde_json::from_str("{}").unwrap());
    execute(&mut store, &mut job, "301", 3);

    job.id += 1;
    job.type_ = ActionType::ACTION_DROP_SCHEMA;
    job.fill_args(Some(GoShared::new(tidb_model::DropSchemaArgs::default())));
    execute(&mut store, &mut job, "0", 3);
}

#[test]
fn persisted_cancellation_precedes_forward_action() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let queue = DdlJobTable::locate(&catalog).unwrap();
    for (offset, action) in [
        ActionType::ACTION_CREATE_SCHEMA,
        ActionType::ACTION_CREATE_TABLE,
        ActionType::ACTION_CREATE_TABLES,
        ActionType::ACTION_RENAME_TABLES,
    ]
    .into_iter()
    .enumerate()
    {
        let mut job = Job::default();
        job.id = 900 + offset as i64;
        job.schema_id = 112;
        job.type_ = action;
        job.state = JobState::CANCELLING;
        job.version = JobVersion::V2;
        // Conversion must not decode forward CREATE/RENAME arguments.
        job.fill_v2_arg(serde_json::from_str("123").unwrap());
        job.error_count = 2;
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "112", "0", true, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let raw = queue
            .load_by_id(&mut store, job.id)
            .unwrap()
            .unwrap()
            .job
            .raw_args;
        let step = plan_worker_step(&mut store, job.id, 10_000).unwrap();
        assert!(step.terminal);
        assert_eq!(step.write.schema_version, 0);
        apply(&mut store, &step.write);
        let history = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap();
        let job = history.iter().find(|j| j.id == job.id).unwrap();
        assert_eq!(job.state, JobState::CANCELLED);
        assert_eq!(job.error_count, 3);
        assert_eq!(
            job.error.as_ref().unwrap().read().code().value(),
            tidb_error::tidb::errcode::ErrCancelledDDLJob as isize,
            "{action}"
        );
        assert_eq!(job.raw_args, raw);
        assert_eq!(
            job.error.as_ref().unwrap().read().rfc_code(),
            "ddl:8214",
            "cancellation retains dbterror.ErrCancelledDDLJob identity"
        );
    }
}

#[test]
fn persisted_cancellation_distinguishes_errors_with_the_same_number() {
    use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
    for (original, expected_rfc, expected_message) in [
        (
            TerrorError::synthesize(
                TerrorClass::Schema,
                TerrorCode::new(8214),
                "original failure from another class",
            ),
            "schema:8214",
            "DDL job rollback, error msg: original failure from another class",
        ),
        // Old Rust rows have no RFC identity. Cancellation must not invent a
        // class-zero RFC prefix when retaining their original diagnostic.
        (
            TerrorError::compatible(TerrorCode::new(1061), "legacy duplicate"),
            "",
            "DDL job rollback, error msg: legacy duplicate",
        ),
        (
            tidb_util::dbterror::ERR_CANCELLED_DDL_JOB.generate("normal cancellation"),
            "ddl:8214",
            "normal cancellation",
        ),
    ] {
        let mut store = bootstrapped();
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 990;
        job.schema_id = 112;
        job.type_ = ActionType::ACTION_CREATE_SCHEMA;
        job.version = JobVersion::V2;
        job.state = JobState::CANCELLING;
        job.error = Some(GoShared::new(original));
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "112", "0", true, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let step = plan_worker_step(&mut store, job.id, 10_000).unwrap();
        assert!(step.terminal);
        apply(&mut store, &step.write);
        let history = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap();
        let saved = history.iter().find(|j| j.id == job.id).unwrap();
        let error = saved.error.as_ref().unwrap().read();
        assert_eq!(error.rfc_code(), expected_rfc);
        assert_eq!(error.message(), expected_message);
    }
}

#[test]
fn persisted_action_panic_preserves_checkpoint_and_discards_unfinished_metadata() {
    for limit in [5, 0] {
        let mut store = bootstrapped();
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 990;
        job.schema_id = 991;
        job.type_ = ActionType::ACTION_CREATE_SCHEMA;
        job.version = JobVersion::V2;
        job.last_schema_version = 55;
        // Go's FinishDBJob also panics on a missing BinlogInfo. This invokes
        // the real action after it has constructed, but not published, writes.
        job.binlog_info = None;
        job.fill_args(Some(GoShared::new(tidb_model::CreateSchemaArgs {
            db_info: tidb_model::GoField::new(Some(GoShared::new(DBInfo {
                id: 991,
                name: tidb_ast::CiString::new("panic_database"),
                ..Default::default()
            }))),
        })));
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "991", "0", true, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let raw = queue
            .load_by_id(&mut store, job.id)
            .unwrap()
            .unwrap()
            .job
            .raw_args;
        let PersistedDdlJobPlan::Step(step) = plan_persisted_ddl_job_step(
            &mut store,
            job.id,
            10_000,
            DdlJobSchemaState::new(true),
            &|| limit,
        )
        .unwrap() else {
            panic!("panic recovery must checkpoint")
        };
        assert_eq!(step.terminal, limit == 0);
        assert!(step.run_error.is_none(), "a panic is not countForError");
        assert_eq!(step.write.schema_version, 0);
        assert!(step.write.mdl_info_update.is_none());
        apply(&mut store, &step.write);
        let after = load_cluster_catalog(&mut store).unwrap();
        assert_eq!(after.schema_version, catalog.schema_version);
        assert!(!after.databases.iter().any(|db| db.info.id == 991));
        if limit > 0 {
            let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
            assert_eq!(active.job.state, JobState::CANCELLING);
            assert_eq!(active.job.error_count, 1);
            assert!(active.job.error.is_none());
            assert_eq!(active.job.last_schema_version, 55);
            assert_eq!(active.job.raw_args, raw);
            let done = plan_worker_step(&mut store, job.id, 20_000).unwrap();
            assert!(done.terminal);
            apply(&mut store, &done.write);
        }
        let history = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap();
        let history = history.iter().find(|j| j.id == job.id).unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(history.error_count, if limit == 0 { 1 } else { 2 });
        assert_eq!(history.raw_args, raw);
        let error = history.error.as_ref().unwrap().read();
        if limit == 0 {
            assert_eq!(error.class(), tidb_error::terror::TerrorClass::Ddl);
            assert_eq!(error.code(), tidb_error::terror::CODE_UNKNOWN);
            assert_eq!(
                error.message(),
                "panic in handling DDL logic and error count beyond the limitation 0, cancelled"
            );
        } else {
            assert_eq!(
                error.code().value(),
                tidb_error::tidb::errcode::ErrCancelledDDLJob as isize
            );
        }
    }
}

#[test]
fn persisted_panic_during_cancellation_or_rollback_keeps_go_budget_and_prior_error() {
    for initial_state in [JobState::CANCELLING, JobState::ROLLINGBACK] {
        let mut store = bootstrapped();
        let create = plan(&mut store, "CREATE TABLE panic_check (a INT)", 1_000);
        let table_id = create.created_id.unwrap();
        apply(&mut store, &create);
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let mut table = catalog
            .find_table("u6", "panic_check")
            .unwrap()
            .1
            .clone_like_go();
        let constraint = tidb_model::table::ConstraintInfo {
            name: tidb_ast::CiString::new("c"),
            state: SchemaState::WRITE_ONLY,
            enforced: true,
            ..Default::default()
        };
        // A nil metadata pointer produces a real action panic; there is no
        // test-only production switch or synthetic error-return replacement.
        table.constraints = tidb_model::GoSharedPointerSlice::from_handles(vec![
            None,
            Some(GoShared::new(constraint.clone())),
        ]);
        store.put(
            key::table_kv_key(112, table_id),
            value::serialize_table_info(&table).unwrap(),
        );
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 990;
        job.schema_id = 112;
        job.table_id = table_id;
        job.type_ = ActionType::ACTION_ADD_CHECK_CONSTRAINT;
        job.state = initial_state;
        job.version = JobVersion::V2;
        job.error_count = 2;
        job.error = Some(GoShared::new(tidb_error::terror::TerrorError::compatible(
            tidb_error::terror::TerrorCode::new(3819),
            "original validation failure",
        )));
        job.fill_args(Some(GoShared::new(tidb_model::AddCheckConstraintArgs {
            constraint: tidb_model::GoField::new(Some(GoShared::new(constraint))),
        })));
        let mut mutations = Vec::new();
        queue
            .append_insert(
                &mut job,
                false,
                "112",
                &table_id.to_string(),
                true,
                &mut mutations,
            )
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let raw = queue
            .load_by_id(&mut store, job.id)
            .unwrap()
            .unwrap()
            .job
            .raw_args;
        let PersistedDdlJobPlan::Step(step) = plan_persisted_ddl_job_step(
            &mut store,
            job.id,
            10_000,
            DdlJobSchemaState::new(true),
            &|| 3,
        )
        .unwrap() else {
            panic!("panic must checkpoint")
        };
        assert_eq!(step.terminal, initial_state == JobState::ROLLINGBACK);
        assert!(step.run_error.is_none());
        assert_eq!(step.write.schema_version, 0);
        apply(&mut store, &step.write);
        if initial_state == JobState::CANCELLING {
            let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
            assert_eq!(active.job.state, JobState::CANCELLING);
            assert_eq!(active.job.error_count, 3);
            assert_eq!(
                active.job.error.unwrap().read().message(),
                "original validation failure"
            );
            assert_eq!(active.job.raw_args, raw);
            let PersistedDdlJobPlan::Step(step) = plan_persisted_ddl_job_step(
                &mut store,
                job.id,
                20_000,
                DdlJobSchemaState::new(true),
                &|| 3,
            )
            .unwrap() else {
                panic!("exhausted panic budget must terminate")
            };
            assert!(step.terminal);
            apply(&mut store, &step.write);
        }
        let history = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap();
        let history = history.iter().find(|j| j.id == job.id).unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(history.raw_args, raw);
        let error = history.error.as_ref().unwrap().read();
        if initial_state == JobState::ROLLINGBACK {
            assert_eq!(history.error_count, 3);
            assert_eq!(error.message(), "original validation failure");
        } else {
            assert_eq!(history.error_count, 4);
            assert_eq!(error.code(), tidb_error::terror::CODE_UNKNOWN);
            assert_eq!(
                error.message(),
                "panic in handling DDL logic and error count beyond the limitation 3, cancelled"
            );
        }
    }
}

#[test]
fn persisted_errors_keep_the_same_codes_as_direct_ddl() {
    let cases = [
        (
            DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(1061)
                    .generate("schema duplicate"),
            ),
            1061,
        ),
        (
            DdlPlanError::from(tidb_util::dbterror::ERR_DUP_KEY_NAME.generate("DDL duplicate")),
            1061,
        ),
        (DdlPlanError::UnknownDatabase("db".into()), 1049),
        (DdlPlanError::DatabaseExists("db".into()), 1007),
        (
            DdlPlanError::UnknownTable {
                schema: "db".into(),
                table: "t".into(),
            },
            1051,
        ),
        (DdlPlanError::UnknownTables(vec!["db.t".into()]), 1051),
        (
            DdlPlanError::TableNotExists {
                schema: "db".into(),
                table: "t".into(),
            },
            1146,
        ),
        (
            DdlPlanError::TableExists {
                schema: "db".into(),
                table: "t".into(),
            },
            1050,
        ),
        (DdlPlanError::DuplicateKeyName("idx".into()), 1061),
        (DdlPlanError::DuplicateColumnName("col".into()), 1060),
        (
            DdlPlanError::UnknownIndexColumn {
                column: "col".into(),
                index: "idx".into(),
            },
            1072,
        ),
        (
            DdlPlanError::KeyNotExists {
                index: "idx".into(),
                table: "t".into(),
            },
            1176,
        ),
        (DdlPlanError::CantDropFieldOrKey("col".into()), 1091),
        (DdlPlanError::UnknownIndex("idx".into()), 1091),
        (
            DdlPlanError::UnknownColumn {
                column: "col".into(),
                table: "t".into(),
            },
            1054,
        ),
        (
            DdlPlanError::InvalidAutoRandom("invalid range".into()),
            8216,
        ),
        (DdlPlanError::AutoIdReadFailed, 1467),
        (
            DdlPlanError::Admission(tidb_exec::cluster_ddl::DdlAdmissionError::with_code(
                3819,
                "invalid row",
            )),
            3819,
        ),
        (DdlPlanError::Unsupported("unsupported action".into()), 1105),
        (DdlPlanError::Encode("invalid metadata".into()), -1),
        (
            DdlPlanError::Catalog(ClusterCatalogError::Snapshot("read failed".into())),
            -1,
        ),
        (
            DdlPlanError::Mutations(tidb_txnkv::transaction::MutationSetError::EmptyKey),
            -1,
        ),
        (DdlPlanError::GlobalIdExhausted { wanted: i64::MAX }, -1),
    ];
    for (error, code) in cases {
        let mut store = bootstrapped();
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 990;
        job.type_ = ActionType::ACTION_CREATE_SCHEMA;
        job.version = JobVersion::V2;
        job.state = JobState::RUNNING;
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "991", "0", true, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let message = error.to_job_error().message().to_owned();
        let expected_rfc = match &error {
            DdlPlanError::Source(source) => {
                assert_eq!(error.to_string(), format!("[{}]{message}", source.rfc_code()));
                source.rfc_code().to_owned()
            }
            _ if code == -1 => "ddl:-1".to_owned(),
            _ => String::new(),
        };
        let direct = error.to_sql_error();
        assert_eq!(direct.code, if code == -1 { 1105 } else { code as u16 });
        assert_eq!(direct.message, message);
        let step = plan_persisted_ddl_job_failure(
            &mut store,
            job.id,
            10_000,
            PersistedDdlJobFailure::Error(error),
            &|| 5,
        )
        .unwrap();
        apply(&mut store, &step.write);
        let mut active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
        assert_eq!(active.job.error_count, 1);
        let saved = active.job.error.as_ref().unwrap().read();
        assert_eq!(saved.code().value(), code, "{message}");
        assert_eq!(saved.message(), message);
        assert_eq!(saved.rfc_code(), expected_rfc);
        if code == -1 {
            assert_eq!(saved.class(), tidb_error::terror::TerrorClass::Ddl);
        }
        assert_eq!(
            tidb_exec::cluster_ddl::ddl_job_error_to_sql_error(&saved),
            direct
        );
        drop(saved);

        // Finalization and a fresh history decode must retain the same error;
        // no statement-local value supplies the submitting connection's result.
        active.job.state = JobState::CANCELLED;
        mutations.clear();
        queue
            .append_update(&mut active, false, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let finished = plan_worker_step(&mut store, job.id, 20_000).unwrap();
        assert!(finished.terminal);
        apply(&mut store, &finished.write);
        let history = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap();
        let history = history.iter().find(|stored| stored.id == job.id).unwrap();
        assert_eq!(history.error_count, 1);
        let restored = history.error.as_ref().unwrap().read();
        assert_eq!(restored.rfc_code(), expected_rfc);
        assert_eq!(restored.message(), message);
        assert_eq!(
            tidb_exec::cluster_ddl::ddl_job_error_to_sql_error(&restored),
            direct
        );
    }
}

#[test]
fn persisted_check_failure_uses_source_identity_for_rollback() {
    use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
    for (error, state, rfc) in [
        (
            DdlPlanError::from(
                tidb_util::dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED.generate("violation"),
            ),
            JobState::ROLLINGBACK,
            "ddl:3819",
        ),
        (
            DdlPlanError::from(TerrorError::synthesize(
                TerrorClass::Schema,
                TerrorCode::new(3819),
                "another class",
            )),
            JobState::RUNNING,
            "schema:3819",
        ),
        (
            DdlPlanError::Encode("plain validation failure".into()),
            JobState::RUNNING,
            "ddl:-1",
        ),
        (
            DdlPlanError::Admission(tidb_exec::cluster_ddl::DdlAdmissionError::with_code(
                3819,
                "number without source identity",
            )),
            JobState::RUNNING,
            "",
        ),
    ] {
        let mut store = bootstrapped();
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 990;
        job.type_ = ActionType::ACTION_ADD_CHECK_CONSTRAINT;
        job.state = JobState::RUNNING;
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "112", "116", true, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let step = plan_persisted_ddl_job_failure(
            &mut store,
            job.id,
            10_000,
            PersistedDdlJobFailure::Error(error),
            &|| 512,
        )
        .unwrap();
        apply(&mut store, &step.write);
        let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
        assert_eq!(active.job.state, state, "{rfc}");
        assert_eq!(active.job.error_count, 1);
        assert_eq!(active.job.error.as_ref().unwrap().read().rfc_code(), rfc);
    }
}

#[test]
fn persisted_action_error_is_checkpointed_before_retry() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let queue = DdlJobTable::locate(&catalog).unwrap();
    let mut job = Job::default();
    job.id = 990;
    job.schema_id = 991;
    job.type_ = ActionType::ACTION_CREATE_SCHEMA;
    job.version = JobVersion::V2;
    // Existing planner rejects a missing db_info without cancelling; the
    // shared worker must still persist its error and untouched raw arguments.
    job.fill_v2_arg(serde_json::from_str("{}").unwrap());
    let mut mutations = Vec::new();
    queue
        .append_insert(&mut job, false, "991", "0", true, &mut mutations)
        .unwrap();
    apply_mutations(&mut store, &mutations);
    let raw = queue
        .load_by_id(&mut store, job.id)
        .unwrap()
        .unwrap()
        .job
        .raw_args;
    for count in 1..=2 {
        let step = plan_worker_step(&mut store, job.id, 10_000 + count).unwrap();
        assert!(!step.terminal);
        assert_eq!(step.write.schema_version, 0);
        apply(&mut store, &step.write);
        let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
        assert_eq!(active.job.error_count, count as i64);
        assert_eq!(active.job.state, JobState::RUNNING);
        assert_eq!(active.job.raw_args, raw);
        let error = active.job.error.as_ref().unwrap().read();
        assert_eq!(error.class(), tidb_error::terror::TerrorClass::Ddl);
        assert_eq!(error.code(), tidb_error::terror::CODE_UNKNOWN);
    }
    let PersistedDdlJobPlan::Step(step) = plan_persisted_ddl_job_step(
        &mut store,
        job.id,
        20_000,
        DdlJobSchemaState::new(true),
        &|| 2,
    )
    .unwrap() else {
        panic!("error must checkpoint")
    };
    assert!(step.run_error.is_some());
    apply(&mut store, &step.write);
    let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
    assert_eq!(active.job.error_count, 3);
    assert_eq!(active.job.state, JobState::CANCELLING);
    let original = active
        .job
        .error
        .as_ref()
        .unwrap()
        .read()
        .message()
        .to_owned();
    let PersistedDdlJobPlan::Step(step) = plan_persisted_ddl_job_step(
        &mut store,
        job.id,
        30_000,
        DdlJobSchemaState::new(true),
        &|| panic!("normal cancellation must not reload the limit"),
    )
    .unwrap() else {
        panic!("cancellation must reach history")
    };
    assert!(step.terminal);
    apply(&mut store, &step.write);
    let history = DdlHistoryTable::locate(&catalog)
        .unwrap()
        .load(&mut store)
        .unwrap();
    let history = history.iter().find(|j| j.id == job.id).unwrap();
    assert_eq!(history.error_count, 4);
    assert_eq!(history.state, JobState::CANCELLED);
    assert_eq!(
        history.error.as_ref().unwrap().read().message(),
        format!("DDL job rollback, error msg: {original}")
    );
    assert_eq!(history.raw_args, raw);
}

#[test]
fn persisted_cancel_uses_physical_drop_state() {
    for action in [
        ActionType::ACTION_DROP_SCHEMA,
        ActionType::ACTION_DROP_TABLE,
    ] {
        for state in [
            SchemaState::PUBLIC,
            SchemaState::WRITE_ONLY,
            SchemaState::DELETE_ONLY,
        ] {
            let mut store = bootstrapped();
            let create = plan(&mut store, "CREATE TABLE cancel_drop (a INT)", 1_000);
            let id = create.created_id.unwrap();
            apply(&mut store, &create);
            let catalog = load_cluster_catalog(&mut store).unwrap();
            let (db, table) = catalog.find_table("u6", "cancel_drop").unwrap();
            if action == ActionType::ACTION_DROP_SCHEMA {
                let mut db = db.clone();
                db.state = state;
                store.put(
                    key::database_kv_key(db.id),
                    value::serialize_db_info(&db).unwrap(),
                );
            } else {
                let mut table = table.clone_like_go();
                table.state = state;
                store.put(
                    key::table_kv_key(db.id, id),
                    value::serialize_table_info(&table).unwrap(),
                );
            }
            let queue = DdlJobTable::locate(&catalog).unwrap();
            let mut job = Job::default();
            job.id = 990;
            job.schema_id = 112;
            job.table_id = id;
            job.type_ = action;
            job.state = JobState::CANCELLING;
            // Stale job state must not override the actual schema object.
            job.schema_state = SchemaState::NONE;
            let mut mutations = Vec::new();
            queue
                .append_insert(
                    &mut job,
                    false,
                    "112",
                    &id.to_string(),
                    true,
                    &mut mutations,
                )
                .unwrap();
            apply_mutations(&mut store, &mutations);
            let step = plan_worker_step(&mut store, job.id, 10_000).unwrap();
            assert_eq!(step.terminal, state == SchemaState::PUBLIC);
            assert_eq!(step.write.schema_version, 0);
            apply(&mut store, &step.write);
            if !step.terminal {
                let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
                assert_eq!(active.job.state, JobState::RUNNING);
                assert_eq!(active.job.error_count, 0);
                assert!(active.job.error.is_none());
            }
            let after = load_cluster_catalog(&mut store).unwrap();
            assert_eq!(after.schema_version, catalog.schema_version);
            assert!(after.find_table("u6", "cancel_drop").is_some());
        }
    }
}

#[test]
fn persisted_check_cancellation_restores_metadata_before_history() {
    for action in [
        ActionType::ACTION_ADD_CHECK_CONSTRAINT,
        ActionType::ACTION_DROP_CHECK_CONSTRAINT,
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
    ] {
        for state in [
            SchemaState::NONE,
            SchemaState::PUBLIC,
            SchemaState::WRITE_ONLY,
            SchemaState::WRITE_REORGANIZATION,
        ] {
            let mut store = bootstrapped();
            let create = plan(&mut store, "CREATE TABLE cancel_check (a INT)", 1_000);
            let table_id = create.created_id.unwrap();
            apply(&mut store, &create);
            let catalog = load_cluster_catalog(&mut store).unwrap();
            let mut table = catalog
                .find_table("u6", "cancel_check")
                .unwrap()
                .1
                .clone_like_go();
            let constraint = tidb_model::table::ConstraintInfo {
                name: tidb_ast::CiString::new("c_positive"),
                expr_string: "`a` > 0".into(),
                state,
                enforced: true,
                ..Default::default()
            };
            if state != SchemaState::NONE {
                table.constraints = vec![constraint.clone()].into();
            }
            store.put(
                key::table_kv_key(112, table_id),
                value::serialize_table_info(&table).unwrap(),
            );
            let queue = DdlJobTable::locate(&catalog).unwrap();
            let mut job = Job::default();
            job.id = 990;
            job.schema_id = 112;
            job.table_id = table_id;
            job.type_ = action;
            job.version = JobVersion::V2;
            job.state = JobState::CANCELLING;
            job.schema_state = state;
            if action == ActionType::ACTION_ADD_CHECK_CONSTRAINT {
                let mut submitted = constraint;
                submitted.state = SchemaState::WRITE_ONLY;
                job.fill_args(Some(GoShared::new(tidb_model::AddCheckConstraintArgs {
                    constraint: tidb_model::GoField::new(Some(GoShared::new(submitted))),
                })));
            } else {
                job.fill_args(Some(GoShared::new(tidb_model::CheckConstraintArgs {
                    constraint_name: tidb_model::GoField::new(tidb_ast::CiString::new(
                        "c_positive",
                    )),
                    enforced: tidb_model::GoField::new(true),
                })));
            }
            let mut mutations = Vec::new();
            queue
                .append_insert(
                    &mut job,
                    false,
                    "112",
                    &table_id.to_string(),
                    true,
                    &mut mutations,
                )
                .unwrap();
            apply_mutations(&mut store, &mutations);
            let raw = queue
                .load_by_id(&mut store, job.id)
                .unwrap()
                .unwrap()
                .job
                .raw_args;
            let step = plan_worker_step(&mut store, job.id, 10_000).unwrap();
            apply(&mut store, &step.write);
            if state == SchemaState::PUBLIC || state == SchemaState::NONE {
                assert!(step.terminal);
                assert_eq!(step.write.schema_version, 0);
            } else if action == ActionType::ACTION_DROP_CHECK_CONSTRAINT {
                assert!(!step.terminal);
                assert_eq!(step.write.schema_version, 0);
                assert_eq!(
                    queue
                        .load_by_id(&mut store, job.id)
                        .unwrap()
                        .unwrap()
                        .job
                        .state,
                    JobState::RUNNING
                );
                if state == SchemaState::WRITE_REORGANIZATION {
                    let PersistedDdlJobPlan::Step(retry) = plan_persisted_ddl_job_step(
                        &mut store,
                        job.id,
                        20_000,
                        DdlJobSchemaState::new(true),
                        &|| 0,
                    )
                    .unwrap() else {
                        panic!("invalid forward state must checkpoint")
                    };
                    assert!(retry.run_error.is_some());
                    apply(&mut store, &retry.write);
                    let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
                    assert_eq!(active.job.error_count, 1);
                    assert_eq!(
                        active.job.state,
                        JobState::RUNNING,
                        "a non-rollbackable action must not cross into CANCELLING"
                    );
                }
            } else {
                assert!(!step.terminal);
                assert_eq!(step.write.schema_version, catalog.schema_version + 1);
                let active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
                assert_eq!(active.job.state, JobState::CANCELLING);
                assert_eq!(active.job.raw_args, raw);
                let after = load_cluster_catalog(&mut store).unwrap();
                let table = after.find_table("u6", "cancel_check").unwrap().1;
                if action == ActionType::ACTION_ADD_CHECK_CONSTRAINT {
                    assert!(table.constraints.is_empty());
                } else {
                    assert!(!table.constraints.get(0).unwrap().read().enforced);
                    assert_eq!(
                        table.constraints.get(0).unwrap().read().state,
                        SchemaState::PUBLIC
                    );
                }
                let mdl = step.write.mdl_info_update.unwrap();
                let mut mutations = Vec::new();
                mdl.append_mutations(job.id, step.write.schema_version, "owner", &mut mutations)
                    .unwrap();
                apply_mutations(&mut store, &mutations);
                assert!(matches!(
                    plan_persisted_ddl_job_step(
                        &mut store,
                        job.id,
                        20_000,
                        DdlJobSchemaState::new(true),
                        &|| 512
                    )
                    .unwrap(),
                    PersistedDdlJobPlan::SchemaSync { .. }
                ));
                let PersistedDdlJobPlan::Step(done) = plan_persisted_ddl_job_step(
                    &mut store,
                    job.id,
                    30_000,
                    DdlJobSchemaState {
                        synced_version: Some(step.write.schema_version),
                        ..DdlJobSchemaState::new(true)
                    },
                    &|| 512,
                )
                .unwrap() else {
                    panic!("cancel after sync")
                };
                assert!(done.terminal);
                apply(&mut store, &done.write);
                let history = DdlHistoryTable::locate(&after)
                    .unwrap()
                    .load(&mut store)
                    .unwrap();
                let history = history.iter().find(|j| j.id == job.id).unwrap();
                assert_eq!(history.state, JobState::CANCELLED);
                assert_eq!(
                    history.error.as_ref().unwrap().read().code().value(),
                    tidb_error::tidb::errcode::ErrCancelledDDLJob as isize
                );
            }
        }
    }
}

#[test]
fn persisted_check_source_state_errors() {
    for (action, code, message) in [
        (
            ActionType::ACTION_ADD_CHECK_CONSTRAINT,
            8210,
            "Invalid constraint state: delete only",
        ),
        (
            ActionType::ACTION_DROP_CHECK_CONSTRAINT,
            8204,
            "Invalid DDL job%!(EXTRA string=constraint, model.SchemaState=public)",
        ),
        (ActionType::ACTION_ALTER_CHECK_CONSTRAINT, 0, ""),
    ] {
        // Each action starts from the same independently persisted metadata.
        let mut store = bootstrapped();
        let create = plan(&mut store, "CREATE TABLE checked_table (a INT)", 1_000);
        let table_id = create.created_id.unwrap();
        apply(&mut store, &create);
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let mut table = catalog
            .find_table("u6", "checked_table")
            .unwrap()
            .1
            .clone_like_go();
        table.constraints = vec![tidb_model::table::ConstraintInfo {
            name: tidb_ast::CiString::new("MixedCheck"),
            expr_string: "`a` > 0".into(),
            state: SchemaState::DELETE_ONLY,
            enforced: true,
            ..Default::default()
        }]
        .into();
        store.put(
            key::table_kv_key(112, table_id),
            value::serialize_table_info(&table).unwrap(),
        );
        let mut job = Job::default();
        job.id = 991;
        job.schema_id = 112;
        job.table_id = table_id;
        job.table_name = "checked_table".into();
        job.type_ = action;
        job.state = JobState::RUNNING;
        job.version = JobVersion::V2;
        if action == ActionType::ACTION_ADD_CHECK_CONSTRAINT {
            let mut submitted = table.constraints.get(0).unwrap().read().clone();
            // Go skips the cross-table name check after first publication.
            submitted.state = SchemaState::WRITE_ONLY;
            job.fill_args(Some(GoShared::new(tidb_model::AddCheckConstraintArgs {
                constraint: tidb_model::GoField::new(Some(GoShared::new(submitted))),
            })));
        } else {
            job.fill_args(Some(GoShared::new(tidb_model::CheckConstraintArgs {
                constraint_name: tidb_model::GoField::new(tidb_ast::CiString::new("MixedCheck")),
                enforced: tidb_model::GoField::new(true),
            })));
        }
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut mutations = Vec::new();
        queue
            .append_insert(
                &mut job,
                false,
                "112",
                &table_id.to_string(),
                true,
                &mut mutations,
            )
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let before_table = store.pairs[&key::table_kv_key(112, table_id)].clone();
        // A successful Go action re-encodes decoded args even without a
        // schema mutation. Failed actions must keep the original raw args.
        let mut active = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
        let mut raw: serde_json::Value =
            serde_json::from_str(&active.job.raw_args.as_ref().unwrap().get()).unwrap();
        raw["unknown"] = serde_json::json!("retained only on error");
        active.job.raw_args =
            Some(tidb_model::PersistedRawJson::from_string(raw.to_string()).unwrap());
        mutations.clear();
        queue
            .append_update(&mut active, false, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let step = plan_worker_step(&mut store, job.id, 2_000).unwrap();
        assert!(!step.terminal, "{action}");
        assert_eq!(step.write.schema_version, 0, "{action}");
        if code != 0 {
            let DdlPlanError::Source(error) = step.run_error.as_ref().unwrap() else {
                panic!("{action}: expected the Go source error");
            };
            assert_eq!(error.code().value(), code, "{action}");
            assert_eq!(error.message(), message, "{action}");
            assert!(
                error.stack().is_some(),
                "{action}: GenWithStackByArgs captures a stack"
            );
        } else {
            assert!(
                step.run_error.is_none(),
                "ALTER has no default switch error"
            );
        }
        apply(&mut store, &step.write);
        assert_eq!(
            store.pairs[&key::table_kv_key(112, table_id)],
            before_table,
            "{action}"
        );
        let reloaded = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
        assert_eq!(reloaded.job.error_count, i64::from(code != 0), "{action}");
        assert_eq!(reloaded.job.state, JobState::RUNNING, "{action}");
        let raw: serde_json::Value =
            serde_json::from_str(&reloaded.job.raw_args.as_ref().unwrap().get()).unwrap();
        assert_eq!(
            raw.get("unknown").is_some(),
            code != 0,
            "{action}: raw-arg ownership"
        );
        if code != 0 {
            let persisted = reloaded.job.error.as_ref().unwrap().read();
            assert_eq!(persisted.message(), message);
            assert_eq!(persisted.rfc_code(), format!("ddl:{code}"));
        } else {
            assert!(reloaded.job.error.is_none());
        }
    }
}

#[test]
fn persisted_check_source_missing_constraint_keeps_original_name() {
    for action in [
        ActionType::ACTION_DROP_CHECK_CONSTRAINT,
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
    ] {
        let mut store = bootstrapped();
        let create = plan(&mut store, "CREATE TABLE checked_table (a INT)", 1_000);
        let table_id = create.created_id.unwrap();
        apply(&mut store, &create);
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let mut job = Job::default();
        job.id = 992;
        job.schema_id = 112;
        job.table_id = table_id;
        job.table_name = "checked_table".into();
        job.type_ = action;
        job.state = JobState::RUNNING;
        job.version = JobVersion::V2;
        job.fill_args(Some(GoShared::new(tidb_model::CheckConstraintArgs {
            constraint_name: tidb_model::GoField::new(tidb_ast::CiString::new("MissingCheck")),
            enforced: tidb_model::GoField::new(true),
        })));
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut mutations = Vec::new();
        queue
            .append_insert(
                &mut job,
                false,
                "112",
                &table_id.to_string(),
                true,
                &mut mutations,
            )
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let step = plan_worker_step(&mut store, job.id, 2_000).unwrap();
        assert!(step.terminal);
        apply(&mut store, &step.write);
        let history = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap();
        let history = history.iter().find(|j| j.id == job.id).unwrap();
        let error = history.error.as_ref().unwrap().read();
        assert_eq!(error.rfc_code(), "ddl:3940");
        assert_eq!(error.message(), "Constraint 'MissingCheck' does not exist.");
        let sql = tidb_exec::cluster_ddl::ddl_job_error_to_sql_error(&error);
        assert_eq!(sql.message, error.message());
    }
}

#[test]
fn persisted_check_lookup_failures_cancel_before_retry() {
    for action in [
        ActionType::ACTION_ADD_CHECK_CONSTRAINT,
        ActionType::ACTION_DROP_CHECK_CONSTRAINT,
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
    ] {
        for invalid in ["missing", "missingdb", "renamed", "nonpublic", "constraint"] {
            if invalid == "constraint" && action == ActionType::ACTION_ADD_CHECK_CONSTRAINT {
                continue;
            }
            let mut store = bootstrapped();
            let create = plan(&mut store, "CREATE TABLE checked_table (a INT)", 1_000);
            let id = create.created_id.unwrap();
            apply(&mut store, &create);
            let catalog = load_cluster_catalog(&mut store).unwrap();
            if invalid == "nonpublic" {
                let mut table = catalog
                    .find_table("u6", "checked_table")
                    .unwrap()
                    .1
                    .clone_like_go();
                table.state = SchemaState::WRITE_ONLY;
                store.put(
                    key::table_kv_key(112, id),
                    value::serialize_table_info(&table).unwrap(),
                );
            }
            let mut job = Job::default();
            job.id = 990;
            job.schema_id = if invalid == "missingdb" { 998 } else { 112 };
            job.table_id = if invalid == "missing" { 999 } else { id };
            job.table_name = if invalid == "renamed" {
                "old_name"
            } else {
                "checked_table"
            }
            .into();
            job.type_ = action;
            job.state = JobState::RUNNING;
            if invalid == "constraint" {
                job.version = JobVersion::V2;
                job.fill_args(Some(GoShared::new(tidb_model::CheckConstraintArgs {
                    constraint_name: tidb_model::GoField::new(tidb_ast::CiString::new("absent")),
                    enforced: tidb_model::GoField::new(true),
                })));
            }
            let queue = DdlJobTable::locate(&catalog).unwrap();
            let mut mutations = Vec::new();
            let table_ids = job.table_id.to_string();
            queue
                .append_insert(&mut job, false, "112", &table_ids, true, &mut mutations)
                .unwrap();
            apply_mutations(&mut store, &mutations);
            let step = plan_worker_step(&mut store, job.id, 10_000).unwrap();
            assert!(
                step.terminal,
                "{action} {invalid}: object validation must cancel"
            );
            assert_eq!(step.write.schema_version, 0);
            apply(&mut store, &step.write);
            let history = DdlHistoryTable::locate(&catalog)
                .unwrap()
                .load(&mut store)
                .unwrap();
            let history = history.iter().find(|j| j.id == job.id).unwrap();
            assert_eq!(history.error_count, 1);
            assert_eq!(history.state, JobState::CANCELLED);
            assert_eq!(
                history.error.as_ref().unwrap().read().rfc_code(),
                match invalid {
                    "nonpublic" => "ddl:8210",
                    "missingdb" => "schema:1049",
                    "constraint" => "ddl:3940",
                    _ => "schema:1146",
                }
            );
            assert_eq!(
                history.error.as_ref().unwrap().read().code().value(),
                match invalid {
                    "nonpublic" => tidb_error::tidb::errcode::ErrInvalidDDLState as isize,
                    "missingdb" => tidb_error::tidb::errcode::ErrBadDB as isize,
                    "constraint" => tidb_error::tidb::errcode::ErrConstraintNotFound as isize,
                    _ => tidb_error::tidb::errcode::ErrNoSuchTable as isize,
                }
            );
        }
    }
}

#[test]
fn persisted_pausing_jobs_checkpoint_only_control_state() {
    for action in [
        ActionType::ACTION_ADD_CHECK_CONSTRAINT,
        ActionType::ACTION_DROP_CHECK_CONSTRAINT,
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
        ActionType::ACTION_CREATE_SCHEMA,
        ActionType::ACTION_CREATE_TABLE,
        ActionType::ACTION_CREATE_TABLES,
        ActionType::ACTION_RENAME_TABLES,
        ActionType::ACTION_DROP_SCHEMA,
        ActionType::ACTION_DROP_TABLE,
    ] {
        let mut store = bootstrapped();
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 900;
        job.schema_id = 112;
        job.table_id = 901;
        job.schema_name = "u6".into();
        job.type_ = action;
        job.state = JobState::PAUSING;
        job.schema_state = SchemaState::WRITE_ONLY;
        job.version = JobVersion::V2;
        // These args cannot run an action: Go pauses before decoding them.
        job.fill_v2_arg(serde_json::from_str(r#"{"preserve":"unknown action fields"}"#).unwrap());
        job.error = Some(GoShared::new(tidb_error::terror::TerrorError::compatible(
            tidb_error::terror::TerrorCode::new(1105),
            "earlier retry failed",
        )));
        job.error_count = 2;
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, true, "112", "901,902", true, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let original = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
        let mdl = MdlInfoUpdate {
            table: Box::new(
                catalog
                    .find_table("mysql", "tidb_mdl_info")
                    .unwrap()
                    .1
                    .clone_like_go(),
            ),
            table_ids: original.table_ids.clone(),
            omit_owner_id: false,
        };
        let version = catalog.schema_version;
        let mut registration = Vec::new();
        mdl.append_mutations(job.id, version, "owner", &mut registration)
            .unwrap();
        apply_mutations(&mut store, &registration);
        let before = store.pairs.clone();
        assert!(
            matches!(
                plan_persisted_ddl_job_step(&mut store, job.id, 9_000, DdlJobSchemaState::new(true), &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT).unwrap(),
                PersistedDdlJobPlan::SchemaSync { version: actual, .. } if actual == version
            ),
            "{action}: acknowledge the previous publication before pausing"
        );
        assert_eq!(store.pairs, before);
        let PersistedDdlJobPlan::Step(step) = plan_persisted_ddl_job_step(
            &mut store,
            job.id,
            10_000,
            DdlJobSchemaState {
                synced_version: Some(version),
                ..DdlJobSchemaState::new(true)
            },
            &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT,
        )
        .unwrap() else {
            panic!("{action}: an acknowledged pause must checkpoint")
        };
        assert!(!step.terminal, "{action}: pause must retain the active job");
        assert_eq!(step.write.schema_version, 0, "{action}");
        assert!(step.write.mdl_info_update.is_none());
        assert!(step.write.backfill.is_empty());
        assert!(step.write.check_constraint_validation.is_none());
        assert!(step.write.placement_bundles.is_empty());
        assert_eq!(store.pairs, before, "planning must not mutate storage");
        apply(&mut store, &step.write);
        let paused = queue.load_by_id(&mut store, job.id).unwrap().unwrap();
        assert_eq!(paused.job.state, JobState::PAUSED, "{action}");
        assert_eq!(paused.job.real_start_ts, 10_000);
        assert_eq!(paused.job.schema_state, job.schema_state);
        assert_eq!(paused.job.raw_args, original.job.raw_args);
        assert_eq!(paused.job.error_count, 2);
        assert_eq!(
            paused.job.error.as_ref().unwrap().read().message(),
            "earlier retry failed"
        );
        assert_eq!(paused.table_ids, "901,902");
        // Compare the complete write set with Go's job-only checkpoint. No
        // catalog, history, ID allocation or scheduling field may be changed.
        let mut expected = original;
        expected.job.state = JobState::PAUSED;
        expected.job.real_start_ts = 10_000;
        let mut checkpoint = Vec::new();
        queue
            .append_update(&mut expected, false, &mut checkpoint)
            .unwrap();
        let mut expected_store = MetaStore {
            pairs: before,
            ..Default::default()
        };
        apply_mutations(&mut expected_store, &checkpoint);
        assert_eq!(store.pairs, expected_store.pairs, "{action}");
        let before = store.pairs.clone();
        assert!(matches!(
            plan_persisted_ddl_job_step(
                &mut store,
                job.id,
                20_000,
                DdlJobSchemaState {
                    synced_version: Some(version),
                    ..DdlJobSchemaState::new(true)
                },
                &|| { tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT }
            )
            .unwrap(),
            PersistedDdlJobPlan::Paused
        ));
        assert_eq!(
            store.pairs, before,
            "{action}: repeated pause has no writes"
        );
        assert!(
            matches!(
                plan_persisted_ddl_job_step(&mut store, job.id, 30_000, DdlJobSchemaState::new(true), &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT).unwrap(),
                PersistedDdlJobPlan::SchemaSync { version: actual, .. } if actual == version
            ),
            "{action}: replacement owner must recover an uncleaned MDL row"
        );
        let mut cleanup = Vec::new();
        mdl.append_delete_mutations(&mut store, job.id, "owner", &mut cleanup)
            .unwrap();
        apply_mutations(&mut store, &cleanup);
        let before = store.pairs.clone();
        assert!(matches!(
            plan_persisted_ddl_job_step(
                &mut store,
                job.id,
                40_000,
                DdlJobSchemaState::new(true),
                &|| { tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT }
            )
            .unwrap(),
            PersistedDdlJobPlan::Paused
        ));
        assert_eq!(store.pairs, before);
    }
}

#[test]
fn persisted_actions_cancel_through_shared_worker_without_schema_changes() {
    use tidb_model::{
        BatchCreateTableArgs, CreateSchemaArgs, CreateTableArgs, GoField, HistoryInfo, TableInfo,
    };
    for action in [
        ActionType::ACTION_CREATE_SCHEMA,
        ActionType::ACTION_CREATE_TABLE,
        ActionType::ACTION_CREATE_TABLES,
        ActionType::ACTION_DROP_SCHEMA,
        ActionType::ACTION_DROP_TABLE,
    ] {
        let mut store = bootstrapped();
        let create = plan(
            &mut store,
            "CREATE TABLE existing (id INT PRIMARY KEY)",
            1_000,
        );
        apply(&mut store, &create);
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 900;
        job.schema_id = 112;
        job.schema_name = "u6".into();
        job.table_id = 901;
        job.version = JobVersion::V2;
        job.type_ = action;
        job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
        job.error_count = 2;
        let table = |id, name: &str| CreateTableArgs {
            table_info: GoField::new(Some(GoShared::new(TableInfo {
                id,
                name: tidb_ast::CiString::new(name),
                ..Default::default()
            }))),
            ..Default::default()
        };
        let expected_code = match action {
            ActionType::ACTION_CREATE_SCHEMA => {
                job.fill_args(Some(GoShared::new(CreateSchemaArgs {
                    db_info: GoField::new(Some(GoShared::new(DBInfo {
                        name: tidb_ast::CiString::new("u6"),
                        ..Default::default()
                    }))),
                })));
                tidb_error::tidb::errcode::ErrDBCreateExists
            }
            ActionType::ACTION_CREATE_TABLE => {
                job.fill_args(Some(GoShared::new(table(901, "existing"))));
                tidb_error::tidb::errcode::ErrTableExists
            }
            ActionType::ACTION_CREATE_TABLES => {
                job.fill_args(Some(GoShared::new(BatchCreateTableArgs {
                    tables: GoField::new(
                        vec![table(902, "must_not_publish"), table(901, "existing")].into(),
                    ),
                })));
                tidb_error::tidb::errcode::ErrTableExists
            }
            ActionType::ACTION_DROP_SCHEMA => {
                job.schema_id = 999;
                job.fill_args(Some(GoShared::new(tidb_model::DropSchemaArgs::default())));
                tidb_error::tidb::errcode::ErrDBDropExists
            }
            _ => {
                job.fill_v2_arg(serde_json::from_str("{}").unwrap());
                tidb_error::tidb::errcode::ErrNoSuchTable
            }
        };
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "112", "901", false, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let raw_args = queue
            .load_by_id(&mut store, 900)
            .unwrap()
            .unwrap()
            .job
            .raw_args
            .clone();
        let before = store.pairs.clone();
        let mut broken = store.clone();
        let (db, history) = catalog.find_table("mysql", "tidb_ddl_history").unwrap();
        broken.pairs.remove(&key::table_kv_key(db.id, history.id));
        let broken_before = broken.pairs.clone();
        assert!(plan_worker_step(&mut broken, 900, 10_000).is_err());
        assert_eq!(
            broken.pairs, broken_before,
            "a failed finalizer must keep cancellation retryable"
        );
        let step = plan_worker_step(&mut store, 900, 10_000).unwrap();
        assert!(
            step.terminal,
            "{action}: Go finalizes cancellation in this transaction"
        );
        assert_eq!(step.write.schema_version, 0);
        assert!(step.write.mdl_info_update.is_none());
        assert_eq!(store.pairs, before);
        apply(&mut store, &step.write);
        assert!(queue.load_by_id(&mut store, 900).unwrap().is_none());
        let finished = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap()
            .into_iter()
            .find(|job| job.id == 900)
            .unwrap();
        assert_eq!(finished.state, JobState::CANCELLED);
        assert_eq!(finished.error_count, 3);
        assert_eq!(
            finished.error.as_ref().unwrap().read().code().value(),
            expected_code as isize
        );
        assert_eq!(finished.raw_args, raw_args);
        assert_eq!(
            finished.binlog_info.as_ref().unwrap().read().finished_ts,
            10_000
        );
        let after = load_cluster_catalog(&mut store).unwrap();
        assert_eq!(after.schema_version, catalog.schema_version);
        assert!(after.find_table("u6", "must_not_publish").is_none());
        assert!(after.find_table("u6", "existing").is_some());
    }
}

#[test]
fn persisted_action_decode_errors_use_shared_cancellation() {
    for action in [
        ActionType::ACTION_CREATE_SCHEMA,
        ActionType::ACTION_CREATE_TABLE,
        ActionType::ACTION_CREATE_TABLES,
        ActionType::ACTION_RENAME_TABLES,
        ActionType::ACTION_ADD_CHECK_CONSTRAINT,
        ActionType::ACTION_DROP_CHECK_CONSTRAINT,
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
    ] {
        let mut store = bootstrapped();
        let create = plan(
            &mut store,
            "CREATE TABLE decode_error (id INT PRIMARY KEY)",
            1_000,
        );
        apply(&mut store, &create);
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 900;
        job.schema_id = 112;
        job.table_id = create.created_id.unwrap();
        job.type_ = action;
        job.version = JobVersion::V2;
        job.binlog_info = Some(GoShared::new(tidb_model::HistoryInfo::default()));
        job.fill_v2_arg(serde_json::from_str("123").unwrap());
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "112", "116", false, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let raw_args = job.raw_args.clone();
        let step = plan_worker_step(&mut store, 900, 10_000).unwrap();
        assert!(
            step.terminal,
            "{action} decode failure must finalize cancellation"
        );
        assert_eq!(step.write.schema_version, 0);
        apply(&mut store, &step.write);
        assert!(queue.load_by_id(&mut store, 900).unwrap().is_none());
        let finished = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap()
            .into_iter()
            .find(|job| job.id == 900)
            .unwrap();
        assert_eq!(finished.state, JobState::CANCELLED);
        assert_eq!(finished.error_count, 1);
        let error = finished.error.as_ref().unwrap().read();
        assert_eq!(error.code(), tidb_error::terror::CODE_UNKNOWN);
        assert_eq!(error.rfc_code(), "ddl:-1");
        assert_eq!(
            tidb_exec::cluster_ddl::ddl_job_error_to_sql_error(&error).code,
            1105
        );
        assert_eq!(finished.raw_args, raw_args);
        assert_eq!(
            load_cluster_catalog(&mut store).unwrap().schema_version,
            catalog.schema_version
        );
    }
}

#[test]
fn history_failure_keeps_completed_job_recoverable() {
    for action in [
        ActionType::ACTION_CREATE_SCHEMA,
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG,
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
    ] {
        for state in [JobState::DONE, JobState::ROLLBACK_DONE, JobState::CANCELLED] {
            let mut store = bootstrapped();
            let catalog = load_cluster_catalog(&mut store).unwrap();
            let queue = DdlJobTable::locate(&catalog).unwrap();
            let mut job = Job::default();
            job.id = 900;
            job.type_ = action;
            job.state = state;
            let mut mutations = Vec::new();
            queue
                .append_insert(&mut job, false, "0", "0", false, &mut mutations)
                .unwrap();
            apply_mutations(&mut store, &mutations);
            // The old action writers discarded this error and removed the queue row.
            let (db, table) = catalog.find_table("mysql", "tidb_ddl_history").unwrap();
            store.pairs.remove(&key::table_kv_key(db.id, table.id));
            let result = match action {
                ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG => {
                    plan_mview_log_step(&mut store, 900, 10_000)
                }
                ActionType::ACTION_CREATE_MATERIALIZED_VIEW => {
                    plan_mview_step(&mut store, 900, 10_000, None)
                }
                _ => plan_worker_step(&mut store, 900, 10_000),
            };
            assert!(result.is_err());
            assert_eq!(
                queue
                    .load_by_id(&mut store, 900)
                    .unwrap()
                    .unwrap()
                    .job
                    .state,
                state
            );
            assert!(!store.pairs.contains_key(&key::ddl_job_history_kv_key(900)));
        }
    }
}
#[test]
fn completed_check_schema_stays_active_until_schema_sync() {
    let mut store = bootstrapped();
    let create = plan(&mut store, "CREATE TABLE sync_check (a INT)", 2_000);
    apply(&mut store, &create);
    let parsed =
        tidb_parser::parse("ALTER TABLE sync_check ADD CONSTRAINT c CHECK (a > 0)").unwrap();
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .unwrap()
        .unwrap();
    let submission = plan_check_constraint_job_submission(&mut store, &statement, 2_001)
        .unwrap()
        .unwrap();
    let job_id = submission.job.id;
    apply_mutations(&mut store, &submission.mutations);
    for start_ts in 2_002..=2_004 {
        let step = plan_worker_step(&mut store, job_id, start_ts).unwrap();
        assert!(
            !step.terminal,
            "schema publication must not delete the active job before synchronization"
        );
        apply(&mut store, &step.write);
    }
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let jobs = DdlJobTable::locate(&catalog)
        .unwrap()
        .load(&mut store)
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job.state, JobState::DONE);
    assert!(!store
        .pairs
        .contains_key(&key::ddl_job_history_kv_key(job_id)));
    // The completed CREATE remains in history while this CHECK still waits
    // for schema synchronization; only the CHECK must be absent.
    let history = DdlHistoryTable::locate(&catalog).unwrap().load(&mut store).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].type_, ActionType::ACTION_CREATE_TABLE);
    assert_ne!(history[0].id, job_id);
}

#[test]
fn check_job_submission_precedes_every_schema_transition() {
    let mut store = bootstrapped();
    let create = plan(&mut store, "CREATE TABLE queued_check (a INT)", 2_000);
    let table_id = create.created_id.expect("CREATE allocated the table ID");
    apply(&mut store, &create);
    let schema_version_before = store
        .pairs
        .get(&key::schema_version_kv_key())
        .cloned()
        .expect("schema version exists");
    let table_before = store
        .pairs
        .get(&key::table_kv_key(112, table_id))
        .cloned()
        .expect("table metadata exists");

    let sql_mode = (1_i64 << 2) | (1_i64 << 24);
    let context = tidb_executor::StmtContext::for_query()
        .with_enable_check_constraint(true)
        .with_ddl_sql_mode(sql_mode)
        .with_connection_id(Some(77))
        .with_ddl_job_context(9, 2, "ddl-alias", vec![1, 2, 3])
        .with_ddl_query("ALTER TABLE queued_check ADD CONSTRAINT c_positive CHECK (a > 0)");
    let parsed =
        tidb_parser::parse("ALTER TABLE queued_check ADD CONSTRAINT c_positive CHECK (a > 0)")
            .expect("CHECK DDL parses");
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("CHECK DDL is admitted")
        .expect("CHECK DDL owns a catalog route");
    let submission = plan_check_constraint_job_submission(&mut store, &statement, 2_001)
        .expect("Go submission plans")
        .expect("ADD CHECK uses the job table");

    assert_eq!(submission.mutations.len(), 2);
    assert_eq!(submission.mutations[0].kind(), BufferMutationOp::Set);
    assert_eq!(submission.mutations[0].key(), key::next_global_id_kv_key());
    assert_eq!(submission.mutations[1].kind(), BufferMutationOp::Set);
    assert!(submission
        .mutations
        .iter()
        .all(|mutation| mutation.key() != key::schema_version_kv_key()));
    assert!(submission
        .mutations
        .iter()
        .all(|mutation| mutation.key() != key::table_kv_key(112, table_id)));
    assert_eq!(submission.job.state, JobState::QUEUEING);
    assert_eq!(submission.job.schema_state, SchemaState::NONE);
    assert_eq!(
        submission.job.type_,
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
    );
    assert_eq!(submission.job.sql_mode, sql_mode);
    assert_eq!(submission.job.cdc_write_source, 9);
    assert_eq!(submission.job.priority, 2);
    let trace = submission
        .job
        .trace_info
        .as_ref()
        .expect("Go submission captures trace identity")
        .read();
    assert_eq!(trace.connection_id, 77);
    assert_eq!(trace.session_alias.to_utf8_lossy_go(), "ddl-alias");
    assert_eq!(trace.trace_id.snapshot(), vec![1, 2, 3]);
    assert_eq!(
        submission.job.query.to_utf8_lossy_go(),
        "ALTER TABLE queued_check ADD CONSTRAINT c_positive CHECK (a > 0)"
    );
    assert_eq!(submission.job.start_ts, 2_001);

    apply_mutations(&mut store, &submission.mutations);
    assert_eq!(
        store.pairs.get(&key::schema_version_kv_key()),
        Some(&schema_version_before),
        "submission does not spend a schema version"
    );
    assert_eq!(
        store.pairs.get(&key::table_kv_key(112, table_id)),
        Some(&table_before),
        "submission does not publish candidate table metadata"
    );

    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads after submission");
    let table = DdlJobTable::locate(&catalog).expect("active-job table exists");
    let mut active = table.load(&mut store).expect("owner sees submitted job");
    assert_eq!(active.len(), 1);
    let args = tidb_model::get_add_check_constraint_args(&mut active[0].job)
        .expect("Go args decode")
        .expect("ADD has args");
    let constraint = args
        .read()
        .constraint
        .get()
        .expect("ADD carries constraint metadata");
    let constraint = constraint.read();
    assert_eq!(constraint.id, 0, "the worker allocates the constraint ID");
    assert_eq!(constraint.state, SchemaState::NONE);
    assert_eq!(constraint.name.original(), "c_positive");
    drop(constraint);
    let job_id = active[0].job.id;
    drop(active);

    let write_only = plan_worker_step(&mut store, job_id, 2_002)
        .expect("a fresh owner runs the first persisted step");
    assert!(!write_only.terminal);
    assert_eq!(write_only.write.schema_version, 62);
    apply(&mut store, &write_only.write);
    let table_info: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&key::table_kv_key(112, table_id))
            .expect("WriteOnly metadata exists"),
    )
    .expect("WriteOnly table decodes");
    let constraint = table_info.constraints.iter_deref().last().unwrap();
    assert_eq!(constraint.read().state, SchemaState::WRITE_ONLY);
    assert_eq!(constraint.read().id, 1);
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads after first step");
    let table = DdlJobTable::locate(&catalog).expect("active-job table exists");
    let mut active = table.load(&mut store).expect("next owner reloads the job");
    assert_eq!(active[0].job.state, JobState::RUNNING);
    assert_eq!(active[0].job.schema_state, SchemaState::WRITE_ONLY);
    assert_eq!(active[0].job.last_schema_version, 62);
    let args = tidb_model::get_add_check_constraint_args(&mut active[0].job)
        .expect("updated args decode")
        .expect("ADD args remain present");
    assert_eq!(
        args.read().constraint.get().unwrap().read().id,
        1,
        "the worker's allocated ID is durable in job_meta"
    );

    let reorganization = plan_worker_step(&mut store, job_id, 2_003)
        .expect("another fresh owner runs the second persisted step");
    assert!(!reorganization.terminal);
    apply(&mut store, &reorganization.write);
    let table_info: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&key::table_kv_key(112, table_id))
            .expect("WriteReorganization metadata exists"),
    )
    .expect("WriteReorganization table decodes");
    assert_eq!(
        table_info
            .constraints
            .iter_deref()
            .last()
            .unwrap()
            .read()
            .state,
        SchemaState::WRITE_REORGANIZATION
    );

    let public = plan_worker_step(&mut store, job_id, 2_004)
        .expect("a final fresh owner validates and finishes the job");
    assert!(!public.terminal);
    assert!(public.write.check_constraint_validation.is_some());
    apply(&mut store, &public.write);
    finish_worker_job(&mut store, job_id, 2_005);
    let table_info: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&key::table_kv_key(112, table_id))
            .expect("Public metadata exists"),
    )
    .expect("Public table decodes");
    assert_eq!(
        table_info
            .constraints
            .iter_deref()
            .last()
            .unwrap()
            .read()
            .state,
        SchemaState::PUBLIC
    );
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads after finish");
    let table = DdlJobTable::locate(&catalog).expect("active-job table exists");
    assert!(
        table.load(&mut store).expect("active jobs scan").is_empty(),
        "terminal handling removes the active row"
    );
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let history_jobs = history.load(&mut store).expect("SQL history scans");
    assert_eq!(history_jobs.len(), 2, "both CREATE and CHECK retain history");
    assert_eq!(history_jobs.iter().filter(|job| job.type_ == ActionType::ACTION_CREATE_TABLE).count(), 1);
    let finished = history_jobs.iter().find(|job| job.id == job_id).expect("CHECK history exists");
    assert_eq!(finished.state, JobState::SYNCED);
    assert_eq!(
        finished
            .binlog_info
            .as_ref()
            .expect("history keeps BinlogInfo")
            .read()
            .finished_ts,
        2_005
    );
    let encoded = store
        .pairs
        .get(&key::ddl_job_history_kv_key(job_id))
        .expect("the meta DDLJobHistory hash is written atomically");
    let mut meta_history = Job::default();
    meta_history
        .decode(encoded)
        .expect("meta history job decodes");
    assert_eq!(meta_history.id, job_id);
    assert_eq!(meta_history.state, JobState::SYNCED);
}

#[test]
fn failed_job_insert_attempt_cleans_up_assigned_id_registration_before_retry() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let mut store = bootstrapped();
    let create = plan(&mut store, "CREATE TABLE retry_cleanup (a INT)", 2_100);
    apply(&mut store, &create);
    let parsed =
        tidb_parser::parse("ALTER TABLE retry_cleanup ADD CONSTRAINT c_positive CHECK (a > 0)")
            .expect("CHECK DDL parses");
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("CHECK DDL is admitted")
        .expect("CHECK DDL owns a catalog route");
    let mut spec = prepare_check_constraint_job_submission(&mut store, &statement, 2_101, false, 0)
        .expect("Go submission preflight succeeds")
        .expect("ADD CHECK creates a job spec");
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");

    let assigned_ids = Rc::new(RefCell::new(Vec::new()));
    let cleanup_ids = Rc::new(RefCell::new(Vec::new()));
    let mut before_insert_with_assigned_ids = {
        let assigned_ids = Rc::clone(&assigned_ids);
        let cleanup_ids = Rc::clone(&cleanup_ids);
        move |specs: &[tidb_exec::ddl_job_submit::JobSpec]| {
            assert_eq!(specs.len(), 1);
            let id = specs[0].job.id;
            assert_ne!(id, 0);
            assigned_ids.borrow_mut().push(id);
            let cleanup_ids = Rc::clone(&cleanup_ids);
            Some(move || cleanup_ids.borrow_mut().push(id))
        }
    };

    let (_, cleanup) = plan_insert_attempt(
        &mut store,
        &catalog,
        std::slice::from_mut(&mut spec),
        &mut before_insert_with_assigned_ids,
    )
    .expect("the first insertion attempt plans");
    assert_eq!(
        finish_insert_attempt::<(), _, _>(Err("retryable"), cleanup),
        Err("retryable")
    );

    let (_, cleanup) = plan_insert_attempt(
        &mut store,
        &catalog,
        std::slice::from_mut(&mut spec),
        &mut before_insert_with_assigned_ids,
    )
    .expect("the retry insertion attempt plans");
    finish_insert_attempt::<_, &str, _>(Ok(()), cleanup).expect("the retry commits");

    let assigned_ids = assigned_ids.borrow();
    let cleanup_ids = cleanup_ids.borrow();
    assert_eq!(assigned_ids.len(), 2);
    assert_eq!(cleanup_ids.as_slice(), &assigned_ids[..1]);
    assert_eq!(assigned_ids[1], spec.job.id);
}

#[test]
fn check_job_submission_observes_the_global_upgrading_state() {
    let mut store = bootstrapped();
    let create = plan(&mut store, "CREATE TABLE upgrading_submit (a INT)", 2_200);
    apply(&mut store, &create);
    let parsed =
        tidb_parser::parse("ALTER TABLE upgrading_submit ADD CONSTRAINT c_positive CHECK (a > 0)")
            .expect("CHECK DDL parses");
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("CHECK DDL is admitted")
        .expect("CHECK DDL owns a catalog route");

    let spec = prepare_check_constraint_job_submission(&mut store, &statement, 2_201, true, 0)
        .expect("Go submission preflight succeeds")
        .expect("ADD CHECK creates a job spec");
    assert_eq!(spec.job.state, tidb_model::JobState::PAUSING);
    assert_eq!(
        spec.job.admin_operator,
        tidb_model::AdminCommandOperator::BY_SYSTEM
    );
}

#[test]
fn persisted_add_check_rolls_back_after_owner_restart() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE queued_check_rollback (a INT)",
        3_000,
    );
    let table_id = create.created_id.expect("CREATE allocated the table ID");
    apply(&mut store, &create);

    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let parsed = tidb_parser::parse(
        "ALTER TABLE queued_check_rollback ADD CONSTRAINT c_positive CHECK (a > 0)",
    )
    .expect("CHECK DDL parses");
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("CHECK DDL is admitted")
        .expect("CHECK DDL owns a catalog route");
    let submission = plan_check_constraint_job_submission(&mut store, &statement, 3_001)
        .expect("Go submission plans")
        .expect("ADD CHECK uses the job table");
    let job_id = submission.job.id;
    apply_mutations(&mut store, &submission.mutations);

    let write_only = plan_worker_step(&mut store, job_id, 3_002)
        .expect("the first owner publishes WriteOnly");
    apply(&mut store, &write_only.write);
    let reorganization = plan_worker_step(&mut store, job_id, 3_003)
        .expect("a restarted owner publishes WriteReorganization");
    apply(&mut store, &reorganization.write);

    let validation_message = "Check constraint 'c_positive' is violated.";
    let rollingback = plan_persisted_ddl_job_failure(
        &mut store,
        job_id,
        10_000,
        PersistedDdlJobFailure::Error(DdlPlanError::from(
            tidb_util::dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED.generate(validation_message),
        )),
        &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT,
    )
    .expect("countForError persists Running to Rollingback");
    apply(&mut store, &rollingback.write);
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads after error");
    let table = DdlJobTable::locate(&catalog).expect("active-job table exists");
    let mut active = table.load(&mut store).expect("new owner sees Rollingback");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].job.state, JobState::ROLLINGBACK);
    assert_eq!(active[0].job.error_count, 1);
    let rollback_args = tidb_model::get_add_check_constraint_args(&mut active[0].job)
        .expect("rollback args decode")
        .expect("rollback keeps ADD args");
    assert_eq!(
        rollback_args
            .read()
            .constraint
            .get()
            .expect("rollback keeps constraint")
            .read()
            .name
            .original(),
        "c_positive"
    );
    assert_eq!(
        active[0]
            .job
            .error
            .as_ref()
            .expect("countForError stores the error")
            .read()
            .message(),
        validation_message
    );

    let rollback = plan_worker_step(&mut store, job_id, 3_004)
        .expect("another owner performs action-specific rollback");
    assert!(!rollback.terminal);
    assert!(rollback.write.check_constraint_validation.is_none());
    apply(&mut store, &rollback.write);
    finish_worker_job(&mut store, job_id, 3_005);

    let table_info: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&key::table_kv_key(112, table_id))
            .expect("rolled-back table metadata exists"),
    )
    .expect("rolled-back table decodes");
    assert!(
        table_info.constraints.is_empty(),
        "rollback left constraints: {:?}",
        table_info
            .constraints
            .iter_deref()
            .map(|constraint| {
                let constraint = constraint.read();
                (constraint.name.to_string(), constraint.state)
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        table_info.max_constraint_id, 1,
        "Go does not reuse the allocated constraint ID after rollback"
    );
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads after rollback");
    let active_table = DdlJobTable::locate(&catalog).expect("active-job table exists");
    assert!(active_table
        .load(&mut store)
        .expect("active jobs scan")
        .is_empty());
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let history_jobs = history.load(&mut store).expect("history scans");
    let history_job = history_jobs
        .iter()
        .find(|job| job.id == job_id)
        .expect("rollback job is retained in history");
    assert_eq!(history_job.state, JobState::ROLLBACK_DONE);
    assert_eq!(history_job.error_count, 1);
    assert_eq!(
        history_job
            .error
            .as_ref()
            .expect("history retains the validation error")
            .read()
            .message(),
        validation_message
    );
}

#[test]
fn persisted_drop_and_alter_check_jobs_resume_and_finish_like_go() {
    let mut store = bootstrapped();
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let lower = |sql: &str| {
        let parsed = tidb_parser::parse(sql).expect("CHECK DDL parses");
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect("CHECK DDL is admitted")
            .expect("CHECK DDL owns a catalog route")
    };

    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE queued_check_actions (a INT, \
             CONSTRAINT c_drop CHECK (a > 0), \
             CONSTRAINT c_toggle CHECK (a < 100) NOT ENFORCED)",
        ),
        4_000,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    let table_id = create.created_id.expect("CREATE allocated the table ID");
    apply(&mut store, &create);

    let drop_submission = plan_check_constraint_job_submission(
        &mut store,
        &lower("ALTER TABLE queued_check_actions DROP CONSTRAINT c_drop"),
        4_001,
    )
    .expect("DROP submission plans")
    .expect("DROP CHECK uses the job table");
    let drop_job_id = drop_submission.job.id;
    apply_mutations(&mut store, &drop_submission.mutations);
    let drop_write_only = plan_worker_step(&mut store, drop_job_id, 4_002)
        .expect("the first owner publishes DROP WriteOnly");
    assert!(!drop_write_only.terminal);
    apply(&mut store, &drop_write_only.write);
    let table = committed_table(&store, table_id);
    assert_eq!(
        table
            .constraints
            .iter_deref()
            .find(|constraint| constraint.read().name.lowercase() == "c_drop")
            .expect("DROP target remains during WriteOnly")
            .read()
            .state,
        SchemaState::WRITE_ONLY
    );
    let drop_done = plan_worker_step(&mut store, drop_job_id, 4_003)
        .expect("a restarted owner removes the constraint");
    assert!(!drop_done.terminal);
    apply(&mut store, &drop_done.write);
    finish_worker_job(&mut store, drop_job_id, 4_004);
    let table = committed_table(&store, table_id);
    assert!(table
        .constraints
        .iter_deref()
        .all(|constraint| constraint.read().name.lowercase() != "c_drop"));

    let enable_submission = plan_check_constraint_job_submission(
        &mut store,
        &lower("ALTER TABLE queued_check_actions ALTER CONSTRAINT c_toggle ENFORCED"),
        4_004,
    )
    .expect("ENABLE submission plans")
    .expect("ALTER CHECK uses the job table");
    let enable_job_id = enable_submission.job.id;
    apply_mutations(&mut store, &enable_submission.mutations);
    let reorganization = plan_worker_step(&mut store, enable_job_id, 4_005)
        .expect("ENABLE publishes WriteReorganization");
    assert!(!reorganization.terminal);
    apply(&mut store, &reorganization.write);
    let table = committed_table(&store, table_id);
    let toggle = table
        .constraints
        .iter_deref()
        .find(|constraint| constraint.read().name.lowercase() == "c_toggle")
        .expect("ALTER target exists");
    assert!(toggle.read().enforced);
    assert_eq!(toggle.read().state, SchemaState::WRITE_REORGANIZATION);

    let write_only = plan_worker_step(&mut store, enable_job_id, 4_006)
        .expect("a restarted owner publishes ENABLE WriteOnly");
    assert!(!write_only.terminal);
    apply(&mut store, &write_only.write);
    assert_eq!(
        committed_table(&store, table_id)
            .constraints
            .iter_deref()
            .find(|constraint| constraint.read().name.lowercase() == "c_toggle")
            .unwrap()
            .read()
            .state,
        SchemaState::WRITE_ONLY
    );

    let enable_done = plan_worker_step(&mut store, enable_job_id, 4_007)
        .expect("a final owner validates and finishes ENABLE");
    assert!(!enable_done.terminal);
    assert!(enable_done.write.check_constraint_validation.is_some());
    apply(&mut store, &enable_done.write);
    finish_worker_job(&mut store, enable_job_id, 4_008);
    let table = committed_table(&store, table_id);
    let toggle = table
        .constraints
        .iter_deref()
        .find(|constraint| constraint.read().name.lowercase() == "c_toggle")
        .unwrap();
    assert!(toggle.read().enforced);
    assert_eq!(toggle.read().state, SchemaState::PUBLIC);

    let disable_submission = plan_check_constraint_job_submission(
        &mut store,
        &lower("ALTER TABLE queued_check_actions ALTER CONSTRAINT c_toggle NOT ENFORCED"),
        4_008,
    )
    .expect("DISABLE submission plans")
    .expect("ALTER CHECK uses the job table");
    let disable_job_id = disable_submission.job.id;
    apply_mutations(&mut store, &disable_submission.mutations);
    let disable_done = plan_worker_step(&mut store, disable_job_id, 4_009)
        .expect("DISABLE finishes in one owner step");
    assert!(!disable_done.terminal);
    assert!(disable_done.write.check_constraint_validation.is_none());
    apply(&mut store, &disable_done.write);
    finish_worker_job(&mut store, disable_job_id, 4_010);
    let table = committed_table(&store, table_id);
    let toggle = table
        .constraints
        .iter_deref()
        .find(|constraint| constraint.read().name.lowercase() == "c_toggle")
        .unwrap();
    assert!(!toggle.read().enforced);
    assert_eq!(toggle.read().state, SchemaState::PUBLIC);

    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads after jobs finish");
    let active = DdlJobTable::locate(&catalog).expect("active-job table exists");
    assert!(active
        .load(&mut store)
        .expect("active jobs scan")
        .is_empty());
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let jobs = history.load(&mut store).expect("history scans");
    for (job_id, action, schema_state) in [
        (
            drop_job_id,
            ActionType::ACTION_DROP_CHECK_CONSTRAINT,
            SchemaState::NONE,
        ),
        (
            enable_job_id,
            ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
            SchemaState::PUBLIC,
        ),
        (
            disable_job_id,
            ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
            SchemaState::PUBLIC,
        ),
    ] {
        let job = jobs
            .iter()
            .find(|job| job.id == job_id)
            .expect("every terminal job is retained in history");
        assert_eq!(job.type_, action);
        assert_eq!(job.state, JobState::SYNCED);
        assert_eq!(job.schema_state, schema_state);
    }
}

#[test]
fn persisted_alter_check_validation_rolls_back_to_not_enforced() {
    let mut store = bootstrapped();
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let lower = |sql: &str| {
        let parsed = tidb_parser::parse(sql).expect("CHECK DDL parses");
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect("CHECK DDL is admitted")
            .expect("CHECK DDL owns a catalog route")
    };
    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE queued_alter_rollback \
             (a INT, CONSTRAINT c_positive CHECK (a > 0) NOT ENFORCED)",
        ),
        5_000,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    let table_id = create.created_id.expect("CREATE allocated the table ID");
    apply(&mut store, &create);

    let submission = plan_check_constraint_job_submission(
        &mut store,
        &lower("ALTER TABLE queued_alter_rollback ALTER CONSTRAINT c_positive ENFORCED"),
        5_001,
    )
    .expect("ENABLE submission plans")
    .expect("ALTER CHECK uses the job table");
    let job_id = submission.job.id;
    apply_mutations(&mut store, &submission.mutations);
    let reorganization = plan_worker_step(&mut store, job_id, 5_002)
        .expect("ENABLE publishes WriteReorganization");
    apply(&mut store, &reorganization.write);
    let write_only = plan_worker_step(&mut store, job_id, 5_003)
        .expect("ENABLE publishes WriteOnly");
    apply(&mut store, &write_only.write);

    let validation_message = "Check constraint 'c_positive' is violated.";
    let rollingback = plan_persisted_ddl_job_failure(
        &mut store,
        job_id,
        10_000,
        PersistedDdlJobFailure::Error(DdlPlanError::from(
            tidb_util::dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED.generate(validation_message),
        )),
        &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT,
    )
    .expect("countForError persists ALTER Rollingback");
    apply(&mut store, &rollingback.write);
    let rollback = plan_worker_step(&mut store, job_id, 5_004)
        .expect("a restarted owner restores the old constraint");
    assert!(!rollback.terminal);
    apply(&mut store, &rollback.write);
    finish_worker_job(&mut store, job_id, 5_005);

    let table = committed_table(&store, table_id);
    let constraint = table.constraints.iter_deref().next().unwrap();
    assert!(!constraint.read().enforced);
    assert_eq!(constraint.read().state, SchemaState::PUBLIC);
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads after rollback");
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let jobs = history.load(&mut store).expect("history scans");
    let job = jobs
        .iter()
        .find(|job| job.id == job_id)
        .expect("rollback job is retained in history");
    assert_eq!(job.state, JobState::ROLLBACK_DONE);
    assert_eq!(job.error_count, 1);
    assert_eq!(
        job.error
            .as_ref()
            .expect("history retains the validation error")
            .read()
            .message(),
        validation_message
    );
}

fn committed_table(store: &MetaStore, table_id: i64) -> tidb_model::TableInfo {
    serde_json::from_slice(
        store
            .pairs
            .get(&key::table_kv_key(112, table_id))
            .expect("committed table metadata exists"),
    )
    .expect("committed table metadata decodes")
}

pub(crate) fn plan(
    store: &mut MetaStore,
    sql: &str,
    start_ts: u64,
) -> tidb_exec::cluster_ddl::DdlWrite {
    match plan_ddl(store, &statement(sql), start_ts).expect("the fixture plans") {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => {
            panic!("expected a write, got already-satisfied: {detail}")
        }
    }
}

/// Applies a planned write set, modelling its transaction having committed.
fn apply_mutations(store: &mut MetaStore, mutations: &[BufferMutation]) {
    for mutation in mutations {
        match mutation.kind() {
            BufferMutationOp::Set => {
                store
                    .pairs
                    .insert(mutation.key().to_vec(), mutation.value().to_vec());
            }
            BufferMutationOp::Delete => {
                store.pairs.remove(mutation.key());
            }
            BufferMutationOp::Lock => {}
        }
    }
}

pub(crate) fn apply(store: &mut MetaStore, write: &tidb_exec::cluster_ddl::DdlWrite) {
    apply_mutations(store, &write.mutations);
}

pub(crate) fn stored_value<'write>(
    write: &'write tidb_exec::cluster_ddl::DdlWrite,
    raw_key: &[u8],
) -> &'write [u8] {
    write
        .mutations
        .iter()
        .find(|mutation| mutation.key() == raw_key)
        .unwrap_or_else(|| panic!("the write set carries this key"))
        .value()
}

/// Asserts that every field the Go server stored is present with the same value
/// in what this node writes.
///
/// Equality of the whole object is deliberately NOT asserted: this node's
/// `TableInfo` follows master and carries fields v8.5.6 has never heard of, and
/// Go's `encoding/json` ignores unknown fields on unmarshal. What must not drift
/// is any field Go DOES read.
fn assert_carries(go: &str, ours: &[u8], ignored: &[&str]) {
    let go: serde_json::Value = serde_json::from_str(go).expect("the Go fixture is JSON");
    let ours: serde_json::Value = serde_json::from_slice(ours).expect("what we wrote is JSON");
    let (go, ours) = (
        go.as_object().expect("a Go object"),
        ours.as_object().expect("our object"),
    );
    for (field, expected) in go {
        if ignored.contains(&field.as_str()) {
            continue;
        }
        assert_eq!(
            ours.get(field),
            Some(expected),
            "stored field `{field}` differs from what TiDB v8.5.6 wrote"
        );
    }
}

/// `CREATE TABLE u6.minimal (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)`
/// as TiDB v8.5.6 stored it.
const GO_MINIMAL: &str = r#"{"id":116,"name":{"O":"minimal","L":"minimal"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"id","L":"id"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4099,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"v","L":"v"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4097,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":2,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":467996279696261139,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":5,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"revision":0}"#;

/// The same for every column type this node admits.
const GO_SHAPES: &str = r#"{"id":114,"name":{"O":"shapes","L":"shapes"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"id","L":"id"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4099,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"amount","L":"amount"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4097,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":3,"name":{"O":"big","L":"big"},"offset":2,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4129,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":4,"name":{"O":"ratio","L":"ratio"},"offset":3,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":5,"Flag":4097,"Flen":22,"Decimal":-1,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":5,"name":{"O":"tag","L":"tag"},"offset":4,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":4097,"Flen":8,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":6,"name":{"O":"name","L":"name"},"offset":5,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":4097,"Flen":32,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":7,"name":{"O":"price","L":"price"},"offset":6,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":246,"Flag":4097,"Flen":10,"Decimal":2,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":7,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":467996279683416098,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":5,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"revision":0}"#;

/// The Go server's own database object for `u6`.
const GO_DATABASE: &str = r#"{"id":112,"db_name":{"O":"u6","L":"u6"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#;

#[test]
fn a_created_table_is_stored_exactly_as_the_go_server_stores_it() {
    let mut store = bootstrapped();
    // The Go fixture's own table id was 116 and this store's max used id is
    // 115 short of that, so the allocation reproduces the same id: the fixture
    // can then be compared field for field, id included.
    store.put(key::next_global_id_kv_key(), b"115".to_vec());
    let write = plan(
        &mut store,
        "CREATE TABLE u6.minimal (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)",
        467_996_279_696_261_139,
    );
    assert_eq!(write.created_id, Some(116));
    assert_carries(
        GO_MINIMAL,
        stored_value(&write, &key::table_kv_key(112, 116)),
        &[],
    );
    assert_eq!(
        stored_table(&write, 116)["index_info"],
        serde_json::Value::Null,
        "a clustered integer primary key builds no IndexInfo, and Go persists the builder's nil slice"
    );
}

#[test]
fn every_admitted_column_type_is_stored_with_the_go_servers_field_type() {
    let mut store = bootstrapped();
    store.put(key::next_global_id_kv_key(), b"113".to_vec());
    let write = plan(
        &mut store,
        "CREATE TABLE u6.shapes (
           id BIGINT PRIMARY KEY CLUSTERED,
           amount BIGINT NOT NULL,
           big BIGINT UNSIGNED NOT NULL,
           ratio DOUBLE NOT NULL,
           tag CHAR(8) NOT NULL,
           name VARCHAR(32) NOT NULL,
           price DECIMAL(10,2) NOT NULL
         )",
        467_996_279_683_416_098,
    );
    assert_eq!(write.created_id, Some(114));
    assert_carries(
        GO_SHAPES,
        stored_value(&write, &key::table_kv_key(112, 114)),
        &[],
    );
}

#[test]
fn table_build_uses_the_loaded_database_defaults() {
    let mut store = bootstrapped();
    store.put(
        key::database_kv_key(112),
        br#"{"id":112,"db_name":{"O":"u6","L":"u6"},"charset":"utf8","collate":"utf8_general_ci","Deprecated":{},"state":5,"policy_ref_info":null}"#.to_vec(),
    );
    store.put(key::next_global_id_kv_key(), b"116".to_vec());

    let write = plan(
        &mut store,
        "CREATE TABLE u6.inherited (name VARCHAR(32) NOT NULL)",
        467_996_279_700_000_000,
    );
    let table = stored_table(&write, 117);
    assert_eq!(table["charset"], "utf8");
    assert_eq!(table["collate"], "utf8_general_ci");
    assert_eq!(table["cols"][0]["type"]["Charset"], "utf8");
    assert_eq!(table["cols"][0]["type"]["Collate"], "utf8_general_ci");
}

#[test]
fn a_table_constraint_primary_key_is_the_same_clustered_handle_as_an_inline_one() {
    let template = |sql: &str| {
        let DdlStatement::CreateTable { build, .. } = statement(sql) else {
            panic!("a CREATE TABLE");
        };
        build.template().clone()
    };
    let inline = template("CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)");
    let constraint =
        template("CREATE TABLE u6.t (id BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id))");
    assert_eq!(
        value::serialize_table_info(&inline).unwrap(),
        value::serialize_table_info(&constraint).unwrap()
    );
    assert!(inline.pk_is_handle);
    // The clustered handle IS the row key, so it is recorded in the flag and
    // in the column's own PriKeyFlag, and gets no IndexInfo of its own.
    assert!(inline.indices.is_empty());
    assert!(inline
        .columns
        .get(0)
        .expect("the fixture declares id")
        .read()
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY));
    assert!(!inline
        .columns
        .get(1)
        .expect("the fixture declares v")
        .read()
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY));
}

#[test]
fn created_database_metadata_follows_go() {
    for (sql, charset, collate) in [
        ("CREATE DATABASE u6", "utf8mb4", "utf8mb4_bin"),
        (
            "CREATE DATABASE u6 CHARACTER SET utf8 COLLATE utf8_general_ci",
            "utf8",
            "utf8_general_ci",
        ),
    ] {
        let mut store = bootstrapped();
        store.pairs.remove(&key::database_kv_key(112));
        store.put(key::next_global_id_kv_key(), b"111".to_vec());
        let write = plan(&mut store, sql, 1);
        assert_eq!(write.created_id, Some(112));
        let encoded = stored_value(&write, &key::database_kv_key(112));
        let database = value::parse_db_info(encoded).expect("stored metadata decodes");
        assert_eq!(database.charset, charset);
        assert_eq!(database.collate, collate);
        if charset == "utf8mb4" {
            assert_carries(GO_DATABASE, encoded, &[]);
        }
    }

    for (sql, expected_charset, expected_collate) in [
        ("CREATE DATABASE c CHARACTER SET utf8", "utf8", "utf8_bin"),
        (
            "CREATE DATABASE c COLLATE utf8_general_ci",
            "utf8",
            "utf8_general_ci",
        ),
    ] {
        let DdlStatement::CreateDatabase {
            charset, collate, ..
        } = statement(sql)
        else {
            panic!("the fixture is CREATE DATABASE")
        };
        assert_eq!(charset, expected_charset, "{sql}");
        assert_eq!(collate, expected_collate, "{sql}");
    }

    assert!(
        refusal("CREATE DATABASE c CHARACTER SET utf8 COLLATE utf8mb4_bin")
            .contains("is not valid for CHARACTER SET")
    );
}

#[test]
fn every_catalog_change_writes_the_schema_version_and_its_diff() {
    // The version key in the write set is the whole concurrency story: it is
    // written from the value this snapshot read, so a competing DDL becomes a
    // TiKV write conflict instead of an interleaved half-change.
    for sql in [
        "CREATE DATABASE fresh",
        "DROP DATABASE u6",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    ] {
        let mut store = bootstrapped();
        let write = plan(&mut store, sql, 7);
        assert_eq!(write.schema_version, 61, "{sql}");
        assert_eq!(write.diff.version, 61, "{sql}");
        assert_eq!(
            stored_value(&write, &key::schema_version_kv_key()),
            b"61",
            "{sql}"
        );
        let stored_diff =
            value::parse_schema_diff(stored_value(&write, &key::schema_diff_kv_key(61)))
                .expect("the stored diff decodes")
                .expect("the stored diff is not empty");
        // The reloader (ours and a real TiDB's domain) reads exactly this back.
        assert_eq!(stored_diff, write.diff, "{sql}");
    }
}

#[test]
fn the_allocated_id_advances_the_global_counter_by_exactly_what_it_took() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        7,
    );
    // Go `getRequiredGIDCount` reserves one ID for the table and one for its
    // DDL job. `assignGIDsForJobs` consumes the object ID first and the job ID
    // last, so the one allocation batch moves the max used ID from 116 to 118.
    assert_eq!(write.created_id, Some(117));
    assert_eq!(write.ddl_job_id, 118);
    assert_eq!(stored_value(&write, &key::next_global_id_kv_key()), b"118");
}

#[test]
fn create_table_stages_the_go_notifier_row_in_the_catalog_transaction() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.notified (id BIGINT PRIMARY KEY)",
        7,
    );
    let catalog = load_cluster_catalog(&mut store).expect("the fixture catalog loads");
    let (_, notifier) = catalog
        .find_table(
            "mysql",
            tidb_metadef::system_tables_def::NOTIFIER_TABLE_NAME,
        )
        .expect("the notifier table is bootstrapped");
    let record_prefix = tidb_codec::gen_table_record_prefix(notifier.id);
    let record = write
        .mutations
        .iter()
        .find(|mutation| {
            mutation.assertion() == tidb_txnkv::AssertionOp::AssertNotExist
                && mutation.key().starts_with(&record_prefix)
        })
        .expect("the DDL transaction inserts one notifier record");
    let field_types = notifier
        .cols()
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.id, column.field_type.clone())
        })
        .collect();
    let values = tidb_tablecodec::decode_table_row_to_map(record.value(), &field_types, None)
        .expect("the ordinary TiDB row decoder reads the notifier record");
    let value = |name: &str| {
        let id = notifier
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .expect("the named notifier column exists")
            .read()
            .id;
        values.get(&id).expect("the notifier row stores the column")
    };
    assert_eq!(value("ddl_job_id"), &Datum::Int(write.ddl_job_id));
    assert_eq!(value("sub_job_id"), &Datum::Int(-1));
    assert_eq!(value("processed_by_flag"), &Datum::UInt(0));
    let encoded_event = match value("schema_change") {
        Datum::Bytes(bytes) => bytes.as_slice(),
        Datum::String(text) => text.bytes(),
        other => panic!("schema_change is stored as bytes, got {other:?}"),
    };
    let event: SchemaChangeEvent =
        serde_json::from_slice(encoded_event).expect("the Go-shaped event JSON decodes");
    assert_eq!(event.create_table_info().id, write.created_id.unwrap());
    assert_eq!(event.create_table_info().name.original(), "notified");
    assert!(write.mutations.iter().any(|mutation| {
        mutation.key()
            == key::auto_table_id_kv_key(tidb_metadef::system::SYSTEM_DATABASE_ID, notifier.id)
    }));
}

#[test]
fn clustered_notifier_events_use_the_composite_primary_key() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let (_, notifier) = catalog.find_table("mysql", "tidb_ddl_notifier").unwrap();
    let mut notifier = notifier.clone_like_go();
    notifier.is_common_handle = true;
    notifier.common_handle_version = 1;
    store.put(
        key::table_kv_key(tidb_metadef::system::SYSTEM_DATABASE_ID, notifier.id),
        value::serialize_table_info(&notifier).unwrap(),
    );
    let write = plan(
        &mut store,
        "CREATE TABLE u6.clustered_notified (id BIGINT PRIMARY KEY)",
        7,
    );
    let prefix = tidb_codec::gen_table_record_prefix(notifier.id);
    let record = write
        .mutations
        .iter()
        .find(|mutation| mutation.key().starts_with(&prefix))
        .unwrap();
    // Go's common handle stores signed integers as datum flag 3 followed by
    // the comparable eight-byte integer, once per primary-key component.
    let mut expected = prefix;
    expected.push(3);
    expected.extend_from_slice(&((write.ddl_job_id as u64) ^ (1_u64 << 63)).to_be_bytes());
    expected.push(3);
    expected.extend_from_slice(&(((-1_i64) as u64) ^ (1_u64 << 63)).to_be_bytes());
    assert_eq!(record.key(), expected);
    let allocator =
        key::auto_table_id_kv_key(tidb_metadef::system::SYSTEM_DATABASE_ID, notifier.id);
    assert!(write
        .mutations
        .iter()
        .all(|mutation| mutation.key() != allocator));
}

#[test]
fn system_database_ddl_stages_no_notifier_event() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE mysql.not_notified (id BIGINT PRIMARY KEY)",
        7,
    );
    let notifier_prefix =
        tidb_codec::table_key::gen_table_prefix(tidb_metadef::system::TI_DBDDLNOTIFIER_TABLE_ID);
    assert!(write
        .mutations
        .iter()
        .all(|mutation| !mutation.key().starts_with(&notifier_prefix)));
}

fn notifier_events(
    store: &mut MetaStore,
    write: &tidb_exec::cluster_ddl::DdlWrite,
) -> Vec<(i64, SchemaChangeEvent)> {
    let catalog = load_cluster_catalog(store).expect("the fixture catalog loads");
    let (_, notifier) = catalog
        .find_table(
            "mysql",
            tidb_metadef::system_tables_def::NOTIFIER_TABLE_NAME,
        )
        .expect("the notifier table is bootstrapped");
    let record_prefix = tidb_codec::gen_table_record_prefix(notifier.id);
    let field_types: BTreeMap<_, _> = notifier
        .cols()
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.id, column.field_type.clone())
        })
        .collect();
    let column_id = |name: &str| {
        notifier
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .expect("the named notifier column exists")
            .read()
            .id
    };
    let sub_job_id = column_id("sub_job_id");
    let schema_change = column_id("schema_change");
    let mut records: Vec<_> = write
        .mutations
        .iter()
        .filter(|mutation| {
            mutation.assertion() == tidb_txnkv::AssertionOp::AssertNotExist
                && mutation.key().starts_with(&record_prefix)
        })
        .collect();
    records.sort_by_key(|mutation| mutation.key());
    records
        .into_iter()
        .map(|record| {
            let values =
                tidb_tablecodec::decode_table_row_to_map(record.value(), &field_types, None)
                    .expect("the ordinary TiDB row decoder reads the notifier record");
            let Datum::Int(sub_job_id) = values[&sub_job_id] else {
                panic!("sub_job_id is an integer")
            };
            let encoded = match &values[&schema_change] {
                Datum::Bytes(bytes) => bytes.as_slice(),
                Datum::String(text) => text.bytes(),
                other => panic!("schema_change is stored as bytes, got {other:?}"),
            };
            let event = serde_json::from_slice(encoded).expect("the Go-shaped event JSON decodes");
            (sub_job_id, event)
        })
        .collect()
}

#[test]
fn a_created_table_is_loadable_and_droppable_by_this_node() {
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.made (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        7,
    );
    apply(&mut store, &created);
    // The catalog reader finds what the writer wrote, at the version it wrote.
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the written catalog loads");
    assert_eq!(catalog.schema_version, 61);
    let (database, table) = catalog.find_table("u6", "made").expect("the created table");
    assert_eq!(database.id, 112);
    assert_eq!(table.id, 117);
    tidb_exec::cluster_catalog::configure_loaded_table(database.name.original(), table)
        .expect("a table this node created is one it can serve");

    // Its own auto-id allocator key was never written, so DROP removes exactly
    // the table key, and the next version is the one after the create's.
    let dropped = plan(&mut store, "DROP TABLE u6.made", 8);
    assert_eq!(dropped.schema_version, 62);
    let deleted: Vec<_> = dropped
        .mutations
        .iter()
        .filter(|mutation| mutation.kind() == BufferMutationOp::Delete)
        .map(|mutation| mutation.key().to_vec())
        .collect();
    assert_eq!(deleted, vec![key::table_kv_key(112, 117)]);
    apply(&mut store, &dropped);
    assert!(tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the catalog loads")
        .find_table("u6", "made")
        .is_none());
}

#[test]
fn dropping_a_database_removes_every_field_of_its_hash() {
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.made (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        7,
    );
    apply(&mut store, &created);
    // A table that has allocated row IDs also owns an allocator field, which
    // Go's `HClear` removes with the rest of the hash.
    store.put(key::auto_table_id_kv_key(112, 117), b"30000".to_vec());

    let dropped = plan(&mut store, "DROP DATABASE u6", 9);
    let mut deleted: Vec<_> = dropped
        .mutations
        .iter()
        .filter(|mutation| mutation.kind() == BufferMutationOp::Delete)
        .map(|mutation| mutation.key().to_vec())
        .collect();
    deleted.sort();
    let mut expected = vec![
        key::table_kv_key(112, 117),
        key::auto_table_id_kv_key(112, 117),
        key::database_kv_key(112),
    ];
    expected.sort();
    assert_eq!(deleted, expected);
    apply(&mut store, &dropped);
    let catalog =
        tidb_exec::cluster_catalog::load_cluster_catalog(&mut store).expect("the catalog loads");
    assert!(!catalog
        .databases
        .iter()
        .any(|database| database.info.id == 112));
    // DROP DATABASE u6 must leave the fixture's mysql database untouched.
    assert!(catalog
        .databases
        .iter()
        .any(|database| { database.info.id == tidb_metadef::system::SYSTEM_DATABASE_ID }));
}

#[test]
fn an_if_exists_clause_turns_a_missing_object_into_a_no_op_that_spends_no_version() {
    let mut store = bootstrapped();
    for sql in [
        "DROP TABLE IF EXISTS u6.absent",
        "DROP TABLE IF EXISTS absent_db.absent",
        "DROP DATABASE IF EXISTS absent_db",
    ] {
        let plan = plan_ddl(&mut store, &statement(sql), 7).expect("the fixture plans");
        assert!(
            matches!(plan, DdlPlan::AlreadySatisfied { .. }),
            "{sql} must publish nothing"
        );
    }
    let plan = plan_ddl(
        &mut store,
        &statement("CREATE DATABASE IF NOT EXISTS u6"),
        7,
    )
    .expect("the fixture plans");
    assert!(matches!(plan, DdlPlan::AlreadySatisfied { .. }));
}

#[test]
fn a_missing_object_without_if_exists_is_named_precisely() {
    let mut store = bootstrapped();
    let unknown_table = plan_ddl(&mut store, &statement("DROP TABLE u6.absent"), 7)
        .expect_err("a missing table is an error");
    assert!(matches!(unknown_table, DdlPlanError::UnknownTable { .. }));
    assert_eq!(unknown_table.to_string(), "Unknown table 'u6.absent'");

    let unknown_database = plan_ddl(
        &mut store,
        &statement("CREATE TABLE nowhere.t (id BIGINT PRIMARY KEY)"),
        7,
    )
    .expect_err("a missing database is an error");
    assert_eq!(unknown_database.to_string(), "Unknown database 'nowhere'");

    let existing = plan_ddl(&mut store, &statement("CREATE DATABASE U6"), 7)
        .expect_err("a duplicate database is an error, case-insensitively");
    assert_eq!(
        existing.to_string(),
        "Can't create database 'U6'; database exists"
    );
}

#[test]
fn the_shapes_a_bootstrap_needs_are_admitted_rather_than_refused() {
    // Each of these was refused before the CREATE surface grew to cover the
    // `mysql.*` bootstrap DDL. They must build now, and they must build into
    // exactly the metadata Go builds, which is what
    // `mysql_bootstrap_tableinfo_source` proves table by table.
    //
    // Admitting them does NOT mean this node can serve them: whether a stored
    // table is readable is `configure_loaded_table`'s single decision, taken at
    // LOAD time, and it still refuses everything but a clustered signed-BIGINT
    // handle. Writing the catalog a real TiDB writes and serving it are two
    // separate questions, and only one of them belongs in DDL admission.
    for sql in [
        // A nullable column.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT)",
        // No primary key at all.
        "CREATE TABLE u6.t (id BIGINT NOT NULL, v BIGINT NOT NULL)",
        // A non-integer and an unsigned clustered handle.
        "CREATE TABLE u6.t (id VARCHAR(8) PRIMARY KEY, v BIGINT NOT NULL)",
        "CREATE TABLE u6.t (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT NOT NULL)",
        // A non-clustered and a composite primary key.
        "CREATE TABLE u6.t (id BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id) NONCLUSTERED)",
        "CREATE TABLE u6.t (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
        // Secondary and unique indexes.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, KEY (v))",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL UNIQUE)",
        // The column types the bootstrap corpus needs.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v TIMESTAMP NOT NULL)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v ENUM('N','Y') NOT NULL DEFAULT 'N')",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v SET('a','b'))",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v JSON)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v LONGTEXT)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v SMALLINT UNSIGNED)",
        // Defaults, literal and CURRENT_TIMESTAMP.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL DEFAULT 3)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        // Table options.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) ENGINE=InnoDB",
        // sysbench's own `sbtest1` shape, which this path used to refuse.
        "CREATE TABLE u6.t (id INTEGER NOT NULL AUTO_INCREMENT, k INTEGER NOT NULL, \
         PRIMARY KEY (id))",
        "CREATE TABLE u6.t (id BIGINT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=100",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        assert!(
            lower_ddl(&parsed, "u6").is_ok(),
            "`{sql}` was refused: {}",
            refusal(sql)
        );
    }
}

#[test]
fn cluster_table_options_keep_their_go_error_identity() {
    for (sql, code, reason) in [
        (
            "CREATE TABLE u6.t (id BIGINT) UNION=(u6.other)",
            8232,
            "CREATE/ALTER table with union option is not supported",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT) INSERT_METHOD=FIRST",
            8233,
            "CREATE/ALTER table with insert method option is not supported",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT) ENGINE=imaginary",
            1286,
            "Unknown storage engine 'imaginary'",
        ),
    ] {
        let (actual_code, actual_reason) = refusal_with_code(sql);
        assert_eq!(actual_code, code, "{sql}");
        assert!(actual_reason.contains(reason), "{sql}: {actual_reason}");
    }
}

#[test]
fn parsed_binary_enum_default_reaches_the_shared_normalizer_losslessly() {
    let sql = "CREATE TABLE u6.t (a ENUM(0xff,0x15) CHARACTER SET binary DEFAULT 0xff)";
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    let tidb_ast::Stmt::Ddl(ddl) = &parsed else {
        panic!("the fixture is DDL");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
        panic!("the fixture is CREATE TABLE");
    };
    let column = &create.columns[0];
    let field_type = tidb_executor::ddl::column_field_type::build_field_type(
        &column.name,
        &column.ty,
        "binary",
        "binary",
    )
    .expect("the parsed binary ENUM type is buildable");
    assert_eq!(field_type.elem(0).as_bytes(), [0xff]);
    assert_eq!(field_type.elem(1).as_bytes(), [0x15]);

    let default = column
        .options
        .iter()
        .find_map(|option| match option {
            tidb_ast::ColumnOption::Default(expr) => Some(expr),
            _ => None,
        })
        .expect("the parsed column retains its DEFAULT");
    let value = tidb_expr::eval(default).expect("the literal folds");
    assert!(matches!(value, tidb_datatype::Datum::BinaryLiteral(_)));
    assert_eq!(value.go_bytes(), [0xff]);

    let value = tidb_executor::ddl::normalize_column_default(
        value,
        &field_type,
        &column.name,
        &tidb_datatype::SessionTimeZone::utc(),
    )
    .expect("the parsed raw member passes final strict validation");
    assert_eq!(value.sql_bytes().unwrap(), [0xff]);
}

#[test]
fn enum_and_set_integer_defaults_keep_their_literal_kind() {
    for (sql, expected) in [
        (
            "CREATE TABLE u6.t (a ENUM('2','3','4') DEFAULT 2)",
            b"3".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('a','c','d') DEFAULT 2)",
            b"c".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('2','3','4') DEFAULT '2')",
            b"2".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('9223372036854775808') DEFAULT 9223372036854775808)",
            b"9223372036854775808".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('first','second') DEFAULT TRUE)",
            b"first".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('2','x') DEFAULT 2)",
            b"x".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('2','x') DEFAULT '2')",
            b"2".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('9223372036854775808') DEFAULT 9223372036854775808)",
            b"9223372036854775808".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('1','4','10','21') DEFAULT 3)",
            b"1,4".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('1','4','10','21') DEFAULT 15)",
            b"1,4,10,21".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM(0xff,0x15) CHARACTER SET binary DEFAULT 0xff)",
            &[0xff],
        ),
        (
            "CREATE TABLE u6.t (a SET(0xff,0x15) CHARACTER SET binary DEFAULT 0x15)",
            &[0x15],
        ),
        (
            "CREATE TABLE u6.t (a ENUM(b'11111111',b'00010101') CHARACTER SET binary DEFAULT b'00010101')",
            &[0x15],
        ),
        (
            "CREATE TABLE u6.t (a VARBINARY(2) DEFAULT b'000000001')",
            &[0x00, 0x01],
        ),
        (
            "CREATE TABLE u6.t (a BIGINT DEFAULT 0x10)",
            b"16".as_slice(),
        ),
    ] {
        assert_eq!(stored_default_bytes(sql), expected, "{sql}");
    }

    for sql in [
        "CREATE TABLE u6.t (a ENUM('1','4','10') DEFAULT 0)",
        "CREATE TABLE u6.t (a ENUM('1','4','10') DEFAULT FALSE)",
        "CREATE TABLE u6.t (a ENUM('1','4','10') DEFAULT 4)",
        "CREATE TABLE u6.t (a SET('1','4','10') DEFAULT 0)",
        "CREATE TABLE u6.t (a SET('1','4','10') DEFAULT 8)",
    ] {
        assert_eq!(refusal(sql), "Invalid default value for 'a'", "{sql}");
    }
}

#[test]
fn defaults_are_validated_and_persisted_against_the_final_column_type() {
    // A non-key NULL default is checked only after later nullability options.
    assert_eq!(
        refusal_with_code("CREATE TABLE u6.t (a BIGINT DEFAULT NULL NOT NULL)"),
        (1067, "Invalid default value for 'a'".to_owned())
    );

    // Go's first `checkPriKeyConstraint` arm can see only an INLINE key. Its
    // DEFAULT NULL is 1067 and wins even when an explicit NULL also exists.
    for sql in [
        "CREATE TABLE u6.t (a BIGINT PRIMARY KEY DEFAULT NULL)",
        "CREATE TABLE u6.t (a BIGINT PRIMARY KEY NULL DEFAULT NULL)",
    ] {
        assert_eq!(
            refusal_with_code(sql),
            (1067, "Invalid default value for 'a'".to_owned()),
            "{sql}"
        );
    }

    // The table-level key is installed only after that precheck. An explicit
    // NULL is then 1171, even ahead of a non-NULL spelling that final default
    // validation would reject. Without explicit NULL, a NULL default is also
    // 1171, including when a separate NOT NULL option is present.
    for sql in [
        "CREATE TABLE u6.t (a BIGINT DEFAULT NULL, PRIMARY KEY (a))",
        "CREATE TABLE u6.t (a BIGINT NULL DEFAULT NULL, PRIMARY KEY (a))",
        "CREATE TABLE u6.t (a BIGINT NULL DEFAULT 'bad', PRIMARY KEY (a))",
        "CREATE TABLE u6.t (a BIGINT NOT NULL DEFAULT NULL, PRIMARY KEY (a))",
    ] {
        assert_eq!(
            refusal_with_code(sql),
            (
                1171,
                "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead"
                    .to_owned()
            ),
            "{sql}"
        );
    }

    // The settled spelling includes Go's fixed-BINARY padding, while the
    // model setter retains a BIT default's raw-byte shadow.
    assert_eq!(
        stored_default_bytes("CREATE TABLE u6.t (a BINARY(4) DEFAULT 0x61)"),
        b"a\0\0\0".as_slice()
    );
    let DdlStatement::CreateTable { build, .. } =
        statement("CREATE TABLE u6.t (a BIT(9) DEFAULT b'1')")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    let column = build
        .template()
        .columns
        .get(0)
        .expect("the fixture declares a BIT column");
    let column = column.read();
    assert_eq!(column.default_value_bit.snapshot(), vec![1]);
    assert_eq!(
        column
            .default_value
            .builtin_string()
            .map(|value| value.as_bytes()),
        Some(&[1][..])
    );

    // The non-expression clock marker stays on its computed-default path.
    assert_eq!(
        stored_default_bytes("CREATE TABLE u6.t (a TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"),
        b"CURRENT_TIMESTAMP".as_slice()
    );
}

#[test]
fn cluster_create_persists_a_literal_timestamp_in_utc_from_the_session_zone() {
    let context = tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
        .with_time_zone(tidb_datatype::SessionTimeZone::Fixed {
            name: "+08:00".to_owned(),
            offset_secs: 8 * 60 * 60,
        });
    let sql = "CREATE TABLE u6.t (a TIMESTAMP DEFAULT '2020-01-02 08:00:00')";
    let parsed = tidb_parser::parse_with_sql_mode(sql, context.sql_mode()).expect("parses");
    let DdlStatement::CreateTable { build, .. } = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("the timestamp default is admitted")
        .expect("the statement is cluster DDL")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    let column_handle = build.template().columns.get(0).expect("one column");
    let column = column_handle.read();
    assert_eq!(
        column
            .default_value
            .builtin_string()
            .map(|value| value.as_bytes()),
        Some(b"2020-01-02 00:00:00".as_slice())
    );
}

#[test]
fn cluster_create_folds_a_timestamp_expression_in_the_session_zone() {
    fn stored(zone: tidb_datatype::SessionTimeZone) -> Vec<u8> {
        let context = tidb_executor::StmtContext::for_query()
            .with_strict(true)
            .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
            .with_time_zone(zone);
        let sql =
            "CREATE TABLE u6.t (v VARCHAR(64) DEFAULT (TIMESTAMP '2024-01-01 14:00:00+05:00'))";
        let parsed = tidb_parser::parse_with_sql_mode(sql, context.sql_mode()).expect("parses");
        let DdlStatement::CreateTable { build, .. } =
            lower_ddl_with_context(&parsed, "u6", &context)
                .expect("the expression default is admitted")
                .expect("the statement is cluster DDL")
        else {
            panic!("the fixture is CREATE TABLE");
        };
        let column_handle = build.template().columns.get(0).expect("one column");
        let column = column_handle.read();
        match column.default_value.view() {
            Some(GoAnyView::String(bytes)) => bytes.as_bytes().to_vec(),
            other => panic!("the fixture stored a non-string default: {other:?}"),
        }
    }

    assert_eq!(
        stored(tidb_datatype::SessionTimeZone::Fixed {
            name: "+02:00".to_owned(),
            offset_secs: 2 * 60 * 60,
        }),
        b"2024-01-01 11:00:00"
    );
    assert_eq!(
        stored(tidb_datatype::SessionTimeZone::utc()),
        b"2024-01-01 09:00:00"
    );
}

#[test]
fn cluster_create_default_admission_uses_the_captured_date_modes() {
    let sql = "CREATE TABLE u6.t (a DATE DEFAULT '0000-00-00')";
    let strict_default = tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
        .with_time_zone(tidb_datatype::SessionTimeZone::utc());
    let parsed = tidb_parser::parse_with_sql_mode(sql, strict_default.sql_mode()).expect("parses");
    let error = lower_ddl_with_context(&parsed, "u6", &strict_default)
        .expect_err("NO_ZERO_DATE rejects the default");
    assert_eq!(
        (error.code, error.sql_state(), error.reason.as_str()),
        (1067, *b"42000", "Invalid default value for 'a'")
    );

    let permissive = tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::default())
        .with_time_zone(tidb_datatype::SessionTimeZone::utc());
    assert!(lower_ddl_with_context(&parsed, "u6", &permissive)
        .expect("zero dates are admitted when the mode bits allow them")
        .is_some());
}

#[test]
fn cluster_create_preserves_coded_default_errors() {
    for (sql, code, state, message) in [
        (
            "CREATE TABLE u6.t (a INT DEFAULT (ABS(1)))",
            3770,
            *b"HY000",
            "Default value expression of column 'a' contains a disallowed function: `abs`.",
        ),
        (
            "CREATE TABLE u6.t (ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP)",
            1067,
            *b"42000",
            "Invalid default value for 'ts'",
        ),
    ] {
        let parsed = tidb_parser::parse(sql).expect("parses");
        let error = lower_ddl(&parsed, "u6").expect_err("the default is refused");
        assert_eq!((error.code, error.sql_state()), (code, state), "{sql}");
        assert_eq!(error.reason, message, "{sql}");
    }
}

#[test]
fn a_statement_this_module_does_not_own_is_left_to_its_own_path() {
    for sql in ["SELECT 1", "INSERT INTO u6.t VALUES (1, 2)"] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        assert!(
            lower_ddl(&parsed, "u6").expect("no refusal").is_none(),
            "`{sql}` is not a catalog change this module owns"
        );
    }
}

/// Go workloadrepo creates RANGE tables and later uses ordinary ALTER TABLE
/// ADD/DROP PARTITION jobs. The cluster path must publish those same catalog
/// changes rather than falling through to a cache- or repository-only path.
#[test]
fn workload_repository_partition_changes_use_cluster_ddl() {
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.hist (ts DATETIME NOT NULL) \
         PARTITION BY RANGE(TO_DAYS(ts)) (\
           PARTITION p20260830 VALUES LESS THAN (TO_DAYS('2026-08-30')),\
           PARTITION p20260831 VALUES LESS THAN (TO_DAYS('2026-08-31')))",
        470_100_000,
    );
    let table_id = created.created_id.expect("CREATE TABLE allocates an id");
    apply(&mut store, &created);

    let added = plan(
        &mut store,
        "ALTER TABLE u6.hist ADD PARTITION (\
           PARTITION p20260901 VALUES LESS THAN (TO_DAYS('2026-09-01')))",
        470_100_001,
    );
    assert_eq!(
        added.diff.action_type,
        tidb_model::ActionType::ACTION_ADD_TABLE_PARTITION
    );
    let added_table = stored_table(&added, table_id);
    let definitions = added_table["partition"]["definitions"]
        .as_array()
        .expect("partition definitions are stored");
    assert_eq!(definitions.len(), 3);
    assert_eq!(definitions[2]["name"]["L"], "p20260901");
    assert!(definitions[2]["less_than"][0]
        .as_str()
        .expect("the folded bound is stored as text")
        .parse::<i64>()
        .is_ok());
    apply(&mut store, &added);

    let dropped = plan(
        &mut store,
        "ALTER TABLE u6.hist DROP PARTITION p20260830",
        470_100_002,
    );
    assert_eq!(
        dropped.diff.action_type,
        tidb_model::ActionType::ACTION_DROP_TABLE_PARTITION
    );
    let dropped_table = stored_table(&dropped, table_id);
    let names = dropped_table["partition"]["definitions"]
        .as_array()
        .expect("partition definitions are stored")
        .iter()
        .map(|definition| definition["name"]["L"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, ["p20260831", "p20260901"]);
}

#[test]
fn cluster_repartition_has_no_direct_metadata_plan() {
    for sql in [
        "ALTER TABLE u6.t PARTITION BY HASH(a) PARTITIONS 2",
        "ALTER TABLE u6.t PARTITION BY RANGE(a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    ] {
        let parsed = tidb_parser::parse(sql).expect("Go repartition syntax parses");
        // This is a containment boundary, not a Go parity claim: the complete
        // durable reorganization owner must precede a replacement live route.
        assert!(lower_ddl(&parsed, "u6").unwrap().is_none(), "{sql}");
    }
}

#[test]
fn cluster_partition_changes_do_not_require_prior_thread_metadata() {
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.t (a INT) PARTITION BY RANGE(a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
        470_100_020,
    );
    apply(&mut store, &created);
    for (sql, action) in [
        (
            "ALTER TABLE u6.t ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))",
            ActionType::ACTION_ADD_TABLE_PARTITION,
        ),
        (
            "ALTER TABLE u6.t DROP PARTITION p0",
            ActionType::ACTION_DROP_TABLE_PARTITION,
        ),
        (
            "ALTER TABLE u6.t TRUNCATE PARTITION p0",
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
        ),
    ] {
        let mut snapshot = store.clone();
        std::thread::spawn(move || {
            let write = plan(&mut snapshot, sql, 470_100_021);
            assert_eq!(write.diff.action_type, action);
        })
        .join()
        .expect("planning must depend only on its explicit snapshot, not thread history");
    }
}

/// Pinned Go `onExchangeTablePartition` swaps the standalone table's physical
/// ID with the named partition, preserves the logical partitioned-table ID,
/// raises all three allocators on both results to their pairwise maximum, and
/// publishes the original physical objects to the notifier in the same DDL
/// transaction. `WITHOUT VALIDATION` isolates those metadata obligations from
/// the row-routing check covered by the real-store execution regression.
#[test]
fn exchange_partition_swaps_ids_auto_ids_and_notifier_payload() {
    let mut store = bootstrapped();
    let partitioned = plan(
        &mut store,
        "CREATE TABLE u6.pt (id BIGINT PRIMARY KEY CLUSTERED) \
         PARTITION BY RANGE (id) (\
           PARTITION p0 VALUES LESS THAN (10),\
           PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        470_100_010,
    );
    let partitioned_id = partitioned.created_id.expect("partitioned table id");
    let old_partition_id = stored_table(&partitioned, partitioned_id)["partition"]["definitions"]
        [0]["id"]
        .as_i64()
        .expect("partition id");
    apply(&mut store, &partitioned);

    let standalone = plan(
        &mut store,
        "CREATE TABLE u6.nt (id BIGINT PRIMARY KEY CLUSTERED)",
        470_100_011,
    );
    let old_standalone_id = standalone.created_id.expect("standalone table id");
    apply(&mut store, &standalone);

    store.put(
        key::auto_table_id_kv_key(112, partitioned_id),
        b"11".to_vec(),
    );
    store.put(
        key::auto_table_id_kv_key(112, old_standalone_id),
        b"22".to_vec(),
    );
    store.put(
        key::auto_increment_id_kv_key(112, partitioned_id),
        b"33".to_vec(),
    );
    store.put(
        key::auto_increment_id_kv_key(112, old_standalone_id),
        b"44".to_vec(),
    );
    store.put(
        key::auto_random_table_id_kv_key(112, partitioned_id),
        b"55".to_vec(),
    );
    store.put(
        key::auto_random_table_id_kv_key(112, old_standalone_id),
        b"66".to_vec(),
    );

    let exchanged = plan(
        &mut store,
        "ALTER TABLE u6.pt EXCHANGE PARTITION p0 WITH TABLE u6.nt WITHOUT VALIDATION",
        470_100_012,
    );
    assert_eq!(
        exchanged.diff.action_type,
        tidb_model::ActionType::ACTION_EXCHANGE_TABLE_PARTITION
    );
    let new_partitioned = stored_table(&exchanged, partitioned_id);
    assert_eq!(
        new_partitioned["partition"]["definitions"][0]["id"],
        old_standalone_id
    );
    let new_standalone: serde_json::Value = serde_json::from_slice(stored_value(
        &exchanged,
        &key::table_kv_key(112, old_partition_id),
    ))
    .expect("new standalone metadata");
    assert_eq!(new_standalone["name"]["L"], "nt");
    assert_eq!(new_standalone["id"], old_partition_id);
    assert!(exchanged.mutations.iter().any(|mutation| {
        mutation.kind() == BufferMutationOp::Delete
            && mutation.key() == key::table_kv_key(112, old_standalone_id)
    }));
    for (key, expected) in [
        (
            key::auto_table_id_kv_key(112, partitioned_id),
            b"22".as_slice(),
        ),
        (
            key::auto_table_id_kv_key(112, old_partition_id),
            b"22".as_slice(),
        ),
        (
            key::auto_increment_id_kv_key(112, partitioned_id),
            b"44".as_slice(),
        ),
        (
            key::auto_increment_id_kv_key(112, old_partition_id),
            b"44".as_slice(),
        ),
        (
            key::auto_random_table_id_kv_key(112, partitioned_id),
            b"66".as_slice(),
        ),
        (
            key::auto_random_table_id_kv_key(112, old_partition_id),
            b"66".as_slice(),
        ),
    ] {
        assert_eq!(stored_value(&exchanged, &key), expected, "counter {key:?}");
    }

    let events = notifier_events(&mut store, &exchanged);
    assert_eq!(events.len(), 1);
    let (_, event) = &events[0];
    let (event_table, event_partition, event_standalone) = event.exchange_partition_info();
    assert_eq!(event_table.id, partitioned_id);
    assert_eq!(event_partition.definitions.get(0).id, old_partition_id);
    assert_eq!(event_standalone.id, old_standalone_id);
}

/// Pinned Go performs the `checkExchangePartition` and
/// `checkTableDefCompatible` admission checks before it creates the job, and
/// DEFAULT is `WITH VALIDATION`. Keep the errno/message distinctions here:
/// they are separate public contracts in Go, not interchangeable generic DDL
/// failures.
#[test]
fn exchange_partition_validation_default_and_admission_errors_match_go() {
    fn admission(error: DdlPlanError) -> (u16, String) {
        let DdlPlanError::Admission(error) = error else {
            panic!("expected a source DDL admission error, got {error:?}");
        };
        (error.code, error.reason)
    }

    let mut store = bootstrapped();
    for (sql, ts) in [
        (
            "CREATE TABLE u6.pt (id BIGINT PRIMARY KEY CLUSTERED) \
             PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN (MAXVALUE))",
            470_100_020,
        ),
        (
            "CREATE TABLE u6.nt (id BIGINT PRIMARY KEY CLUSTERED)",
            470_100_021,
        ),
        (
            "CREATE TABLE u6.mismatch (id INT PRIMARY KEY CLUSTERED)",
            470_100_022,
        ),
        (
            "CREATE TABLE u6.other_pt (id BIGINT PRIMARY KEY CLUSTERED) \
             PARTITION BY HASH (id) PARTITIONS 2",
            470_100_023,
        ),
    ] {
        let write = plan(&mut store, sql, ts);
        apply(&mut store, &write);
    }

    let validated = plan(
        &mut store,
        "ALTER TABLE u6.pt EXCHANGE PARTITION p0 WITH TABLE u6.nt",
        470_100_024,
    );
    assert!(
        validated.exchange_partition_validation.is_some(),
        "the grammar's DEFAULT is Go WITH VALIDATION"
    );
    assert_eq!(
        validated.warnings,
        vec![(
            DdlWarningLevel::Warning,
            1105,
            "after the exchange, please analyze related table of the exchange to update statistics"
                .to_owned()
        )]
    );

    let unvalidated = plan(
        &mut store,
        "ALTER TABLE u6.pt EXCHANGE PARTITION p0 WITH TABLE u6.nt WITHOUT VALIDATION",
        470_100_025,
    );
    assert!(unvalidated.exchange_partition_validation.is_none());

    let (code, message) = admission(
        plan_ddl(
            &mut store,
            &statement("ALTER TABLE u6.nt EXCHANGE PARTITION p0 WITH TABLE u6.mismatch"),
            470_100_026,
        )
        .expect_err("partition management on a normal table refuses"),
    );
    assert_eq!(code, 1505);
    assert_eq!(
        message,
        "Partition management on a not partitioned table is not possible"
    );

    let (code, message) = admission(
        plan_ddl(
            &mut store,
            &statement("ALTER TABLE u6.pt EXCHANGE PARTITION p0 WITH TABLE u6.other_pt"),
            470_100_027,
        )
        .expect_err("a partitioned exchange table refuses"),
    );
    assert_eq!(code, 1732);
    assert_eq!(
        message,
        "Table 'other_pt' is partitioned. It cannot be used in EXCHANGE PARTITION"
    );

    let (code, message) = admission(
        plan_ddl(
            &mut store,
            &statement("ALTER TABLE u6.pt EXCHANGE PARTITION absent WITH TABLE u6.nt"),
            470_100_028,
        )
        .expect_err("an unknown partition refuses"),
    );
    assert_eq!(code, 1735);
    assert_eq!(message, "Unknown partition 'absent' in table 'pt'");

    let (code, message) = admission(
        plan_ddl(
            &mut store,
            &statement("ALTER TABLE u6.pt EXCHANGE PARTITION p0 WITH TABLE u6.mismatch"),
            470_100_029,
        )
        .expect_err("different definitions refuse"),
    );
    assert_eq!(code, 1736);
    assert_eq!(message, "Tables have different definitions");

    let view = match plan_ddl(
        &mut store,
        &view_statement("u6", "exchange_view", false),
        470_100_030,
    )
    .expect("the view plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write: {detail}"),
    };
    apply(&mut store, &view);
    let (code, message) = admission(
        plan_ddl(
            &mut store,
            &statement("ALTER TABLE u6.pt EXCHANGE PARTITION p0 WITH TABLE u6.exchange_view"),
            470_100_031,
        )
        .expect_err("a view is Go ErrCheckNoSuchTable"),
    );
    assert_eq!(code, 1177);
    assert_eq!(message, "Can't open table");
}

#[test]
fn exchange_partition_builds_gos_four_way_label_rule_patch() {
    use tidb_executor::ddl_label::{CodecV1, Rule};

    let swap = tidb_exec::cluster_ddl::ExchangePartitionLabelSwap {
        partitioned_schema: "dbp".to_owned(),
        partitioned_table: "pt".to_owned(),
        partition: "p0".to_owned(),
        standalone_schema: "dbn".to_owned(),
        standalone_table: "nt".to_owned(),
        partition_id: 21,
        standalone_id: 11,
    };
    let codec = CodecV1;
    let [standalone_id, partition_id] = swap.rule_ids(&codec);
    let make_rule = |id: &str| Rule {
        id: id.to_owned(),
        labels: vec![tidb_executor::ddl_label::RegionLabel {
            key: "zone".to_owned(),
            value: "z1".to_owned(),
            ..Default::default()
        }]
        .into(),
        ..Rule::default()
    };

    let patch = swap.patch(
        &codec,
        &[make_rule(&standalone_id), make_rule(&partition_id)],
    );
    assert_eq!(patch.set_rules.len(), 2);
    assert!(patch.delete_rules.is_empty());
    let set_rules = patch.set_rules.snapshot();
    let partition_rule = set_rules
        .iter()
        .find(|rule| rule.id == partition_id)
        .expect("the standalone rule moved to the partition");
    let partition_labels = partition_rule.labels.snapshot();
    assert_eq!(partition_labels[1].value, "dbp");
    assert_eq!(partition_labels[2].value, "pt");
    assert_eq!(partition_labels[3].value, "p0");
    let standalone_rule = set_rules
        .iter()
        .find(|rule| rule.id == standalone_id)
        .expect("the partition rule moved to the standalone table");
    let standalone_labels = standalone_rule.labels.snapshot();
    assert_eq!(standalone_labels[1].value, "dbn");
    assert_eq!(standalone_labels[2].value, "nt");

    let only_partition = swap.patch(&codec, &[make_rule(&partition_id)]);
    assert_eq!(only_partition.set_rules.get(0).id, standalone_id);
    assert_eq!(
        only_partition.delete_rules.snapshot(),
        [partition_id.clone()]
    );

    let only_standalone = swap.patch(&codec, &[make_rule(&standalone_id)]);
    assert_eq!(only_standalone.set_rules.get(0).id, partition_id);
    assert_eq!(only_standalone.delete_rules.snapshot(), [standalone_id]);

    let neither = swap.patch(&codec, &[]);
    assert!(neither.set_rules.is_empty());
    assert!(neither.delete_rules.is_empty());
}

#[test]
fn exchange_partition_precomputes_inverse_placement_bundles_without_pd_reads() {
    let mut store = bootstrapped();
    for (sql, ts) in [
        (
            "CREATE PLACEMENT POLICY p PRIMARY_REGION='r1' REGIONS='r1'",
            470_100_032,
        ),
        (
            "CREATE TABLE u6.pt (id BIGINT PRIMARY KEY CLUSTERED) \
             PLACEMENT POLICY=p PARTITION BY RANGE (id) \
             (PARTITION p0 VALUES LESS THAN (10), \
              PARTITION p1 VALUES LESS THAN (MAXVALUE))",
            470_100_033,
        ),
        (
            "CREATE TABLE u6.nt (id BIGINT PRIMARY KEY CLUSTERED) PLACEMENT POLICY=p",
            470_100_034,
        ),
    ] {
        let write = plan(&mut store, sql, ts);
        apply(&mut store, &write);
    }

    let exchanged = plan(
        &mut store,
        "ALTER TABLE u6.pt EXCHANGE PARTITION p0 WITH TABLE u6.nt WITHOUT VALIDATION",
        470_100_035,
    );
    assert_eq!(exchanged.placement_bundles.len(), 3);
    assert_eq!(exchanged.placement_rollback_bundles.len(), 3);

    // With an inherited partition policy, Go writes an empty group for the
    // physical ID that stopped being a standalone table. The inverse set must
    // empty the opposite ID and restore the old table-level key ranges.
    let forward_empty = exchanged
        .placement_bundles
        .iter()
        .find(|bundle| bundle.rules.is_empty())
        .expect("the forward exchange clears the old standalone group");
    let rollback_empty = exchanged
        .placement_rollback_bundles
        .iter()
        .find(|bundle| bundle.rules.is_empty())
        .expect("rollback clears the old partition group");
    assert_ne!(forward_empty.id, rollback_empty.id);

    let forward_table = exchanged
        .placement_bundles
        .iter()
        .max_by_key(|bundle| bundle.rules.len())
        .expect("the forward table bundle exists");
    let rollback_table = exchanged
        .placement_rollback_bundles
        .iter()
        .find(|bundle| bundle.id == forward_table.id)
        .expect("rollback restores the same table group");
    assert_ne!(forward_table, rollback_table);
}

/// Go routes the single-action ALTER spelling through the same add/drop-index
/// job as standalone `CREATE INDEX`/`DROP INDEX`. The cluster catalog does the
/// same, so both spellings publish the metadata mutation and row backfill.
#[test]
fn alter_table_index_actions_share_the_catalog_backfill_path() {
    let mut store = bootstrapped();
    let table_id = table_with_two_columns(&mut store);

    let added = plan(
        &mut store,
        "ALTER TABLE u6.minimal ADD UNIQUE INDEX vi (v)",
        470_000_000,
    );
    assert_eq!(added.diff.action_type.0, 7, "ActionAddIndex");
    let backfill = added.backfill.first().expect("the index owes entries");
    assert!(matches!(backfill.operation, IndexBackfillOperation::Add(_)));
    assert!(backfill.index.read().unique);
    assert_eq!(
        stored_table(&added, table_id)["index_info"][0]["idx_name"]["O"],
        "vi"
    );
    apply(&mut store, &added);

    let dropped = plan(
        &mut store,
        "ALTER TABLE u6.minimal DROP INDEX vi",
        470_000_001,
    );
    assert_eq!(dropped.diff.action_type.0, 8, "ActionDropIndex");
    assert!(matches!(
        dropped
            .backfill
            .first()
            .expect("entries are removed")
            .operation,
        IndexBackfillOperation::Drop
    ));

    // Go merges multiple add-index sub-jobs. Both entry walks remain ordered
    // in the one catalog transaction.
    let multiple = plan(
        &mut store,
        "ALTER TABLE u6.minimal ADD INDEX i1 (v), ADD INDEX i2 (v)",
        470_000_002,
    );
    assert_eq!(multiple.backfill.len(), 2);
    assert_eq!(multiple.backfill[0].index.read().name.original(), "i1");
    assert_eq!(multiple.backfill[1].index.read().name.original(), "i2");
    let events = notifier_events(&mut store, &multiple);
    assert_eq!(events.len(), 1, "Go merges both indexes into one sub-job");
    assert_eq!(events[0].0, 0);
    let (_, indexes, analyzed) = events[0].1.add_index_info();
    assert_eq!(indexes.len(), 2);
    assert!(!analyzed);
}

/// Go's single-table rename job keeps the table ID and its auto-ID authority,
/// while moving the metadata field to the destination schema.  `ALTER TABLE
/// ... RENAME TO` uses that same job rather than a distinct local-only path.
#[test]
fn alter_table_rename_moves_catalog_metadata_without_reissuing_ids() {
    let mut store = bootstrapped();
    let archive = plan(&mut store, "CREATE DATABASE archive", 470_000_100);
    apply(&mut store, &archive);
    let created = plan(
        &mut store,
        "CREATE TABLE u6.made (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        470_000_101,
    );
    let table_id = created.created_id.expect("a table id");
    apply(&mut store, &created);

    let renamed = plan(
        &mut store,
        "ALTER TABLE u6.made RENAME TO archive.renamed",
        470_000_102,
    );
    assert_eq!(renamed.diff.action_type.0, 14, "ActionRenameTable");
    assert_eq!(renamed.diff.old_schema_id, 112);
    assert!(renamed
        .mutations
        .iter()
        .any(|mutation| mutation.kind() == BufferMutationOp::Delete
            && mutation.key() == key::table_kv_key(112, table_id)));
    apply(&mut store, &renamed);
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the renamed catalog loads");
    assert!(catalog.find_table("u6", "made").is_none());
    let (database, table) = catalog
        .find_table("archive", "renamed")
        .expect("the renamed table");
    assert_eq!(table.id, table_id);
    assert_eq!(table.auto_id_schema_id, 112);
    assert_ne!(database.id, 112);

    let returned = plan(
        &mut store,
        "ALTER TABLE archive.renamed RENAME TO u6.made_again",
        470_000_103,
    );
    apply(&mut store, &returned);
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the returned catalog loads");
    let (_, table) = catalog
        .find_table("u6", "made_again")
        .expect("the returned table");
    assert_eq!(table.id, table_id);
    assert_eq!(table.auto_id_schema_id, 0);

    let identity = tidb_parser::parse("ALTER TABLE u6.made_again RENAME TO u6.made_again")
        .expect("the identity spelling parses");
    let lowered = lower_ddl(&identity, "u6")
        .unwrap()
        .expect("identity rename still resolves its table");
    let planned = plan_ddl(&mut store, &lowered, 470_000_104).unwrap();
    let DdlPlan::AlreadySatisfied { warnings, .. } = planned else {
        panic!("identity ALTER rename must not stage catalog writes");
    };
    assert!(warnings.is_empty());

    for (sql, existing) in [
        ("RENAME TABLE u6.made_again TO u6.made_again", true),
        ("ALTER TABLE u6.missing RENAME TO u6.missing", false),
    ] {
        let statement = tidb_parser::parse(sql).unwrap();
        let lowered = lower_ddl(&statement, "u6").unwrap().unwrap();
        let error = plan_ddl(&mut store, &lowered, 470_000_104).unwrap_err();
        if existing {
            assert!(
                matches!(error, DdlPlanError::TableExists { .. }),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(error, DdlPlanError::TableNotExists { .. }),
                "{error:?}"
            );
        }
    }

    let pairs = tidb_parser::parse("RENAME TABLE u6.made_again TO u6.a, u6.a TO u6.b")
        .expect("the multi-pair spelling parses");
    let lowered = lower_ddl(&pairs, "u6")
        .expect("the full multi-table rename is admitted")
        .expect("a catalog change");
    let DdlStatement::RenameTables { pairs } = lowered else {
        panic!("the multi-pair spelling retains every pair");
    };
    assert_eq!(pairs.len(), 2);
    let renamed_twice = match plan_ddl(
        &mut store,
        &DdlStatement::RenameTables { pairs },
        470_000_104,
    )
    .expect("one atomic multi-table rename plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write, got {detail}"),
    };
    assert_eq!(renamed_twice.diff.action_type.0, 47, "ActionRenameTables");
    assert_eq!(renamed_twice.diff.affected_options.len(), 1);
    apply(&mut store, &renamed_twice);
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the multi-renamed catalog loads");
    assert!(catalog.find_table("u6", "made_again").is_none());
    assert!(catalog.find_table("u6", "a").is_none());
    let (_, table) = catalog
        .find_table("u6", "b")
        .expect("the second pair sees the first pair's namespace");
    assert_eq!(table.id, table_id);
}

#[test]
fn an_unqualified_name_resolves_against_the_sessions_default_schema() {
    let parsed = tidb_parser::parse("CREATE TABLE t (id BIGINT PRIMARY KEY)").expect("parses");
    let DdlStatement::CreateTable { schema, table, .. } = lower_ddl(&parsed, "campaign31")
        .expect("admitted")
        .expect("a catalog change")
    else {
        panic!("a CREATE TABLE");
    };
    assert_eq!((schema.as_str(), table.as_str()), ("campaign31", "t"));
}

/// The live bug this closes: `CREATE TABLE ... AUTO_INCREMENT` was accepted
/// and written, and the catalog loader then refused the very table the
/// statement had just created, so its creator answered `table not found in
/// catalog` to both `INSERT` and `SELECT` for sysbench's `sbtest1` shape.
/// That was replaced by an honest
/// refusal, and the refusal is now gone in turn: the counter has the meta-key
/// home Go gives it (`tidb_exec::cluster_auto_id`), so the shape is admitted
/// and served.
#[test]
fn create_table_with_auto_increment_is_admitted_now_the_counter_has_a_home() {
    let parsed = tidb_parser::parse(
        "CREATE TABLE sbtest1 (id INTEGER NOT NULL AUTO_INCREMENT, k INTEGER NOT NULL, \
         PRIMARY KEY (id))",
    )
    .expect("the fixture SQL parses");
    let DdlStatement::CreateTable { build, .. } = lower_ddl(&parsed, "sbtest")
        .expect("admitted")
        .expect("a catalog change")
    else {
        panic!("a CREATE TABLE");
    };
    assert!(
        build
            .template()
            .columns
            .get(0)
            .expect("the fixture declares id")
            .read()
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT),
        "the admitted template keeps the AUTO_INCREMENT flag the loader reads"
    );
    // Go `SepAutoInc`: without `AUTO_ID_CACHE 1` the ids come from the row-id
    // key, the SAME one `_tidb_rowid` uses, which is what a Go `tidb-server`
    // on this cluster reads. Picking `IID:` because the name matches would
    // give the two nodes separate counters for one column, with nothing to
    // detect it.
    assert!(!build.template().sep_auto_inc());
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_id_key_for(7, build.template()),
        tidb_meta::key::auto_table_id_kv_key(7, build.template().id),
    );
}

#[test]
fn create_table_with_auto_random_persists_its_allocator_format() {
    let parsed = tidb_parser::parse(
        "CREATE TABLE ar (id BIGINT UNSIGNED AUTO_RANDOM(5, 32) PRIMARY KEY, v INT) \
         AUTO_RANDOM_BASE=100",
    )
    .expect("the fixture SQL parses");
    let DdlStatement::CreateTable { build, .. } = lower_ddl(&parsed, "test")
        .expect("admitted")
        .expect("a catalog change")
    else {
        panic!("a CREATE TABLE");
    };
    assert_eq!(build.template().auto_random_bits, 5);
    assert_eq!(build.template().auto_random_range_bits, 32);
    assert_eq!(build.template().auto_rand_id, 100);
    assert!(build.template().is_auto_random_bit_col_unsigned());
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_random_id_key_for(7, build.template()),
        tidb_meta::key::auto_random_table_id_kv_key(7, build.template().id),
    );

    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE ar (id BIGINT UNSIGNED AUTO_RANDOM(5, 32) PRIMARY KEY, v INT) \
         AUTO_RANDOM_BASE=100",
        123,
    );
    assert_eq!(
        stored_value(
            &write,
            &tidb_meta::key::auto_random_table_id_kv_key(112, write.created_id.unwrap())
        ),
        b"99"
    );
}

#[test]
fn alter_auto_random_base_updates_table_info_and_the_tarid_counter_together() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE ar_alter (id BIGINT AUTO_RANDOM(5) PRIMARY KEY, v INT)",
        123,
    );
    apply(&mut store, &create);
    store.put(key::schema_version_kv_key(), b"61".to_vec());

    let alter = plan(&mut store, "ALTER TABLE ar_alter AUTO_RANDOM_BASE=500", 124);
    let table_id = create.created_id.unwrap();
    let table: tidb_model::TableInfo =
        serde_json::from_slice(stored_value(&alter, &key::table_kv_key(112, table_id))).unwrap();
    assert_eq!(table.auto_rand_id, 500);
    assert_eq!(
        stored_value(&alter, &key::auto_random_table_id_kv_key(112, table_id)),
        b"499"
    );
    assert_eq!(
        alter.diff.action_type,
        tidb_model::ActionType::ACTION_REBASE_AUTO_RANDOM_BASE
    );

    apply(&mut store, &alter);
    store.put(key::schema_version_kv_key(), b"62".to_vec());
    let lower = plan(&mut store, "ALTER TABLE ar_alter AUTO_RANDOM_BASE=10", 125);
    let lower_table: tidb_model::TableInfo =
        serde_json::from_slice(stored_value(&lower, &key::table_kv_key(112, table_id))).unwrap();
    assert_eq!(lower_table.auto_rand_id, 500);
    assert_eq!(
        stored_value(&lower, &key::auto_random_table_id_kv_key(112, table_id)),
        b"499"
    );

    apply(&mut store, &lower);
    store.put(key::schema_version_kv_key(), b"63".to_vec());
    let forced = plan(
        &mut store,
        "ALTER TABLE ar_alter FORCE AUTO_RANDOM_BASE=2",
        126,
    );
    let forced_table: tidb_model::TableInfo =
        serde_json::from_slice(stored_value(&forced, &key::table_kv_key(112, table_id))).unwrap();
    assert_eq!(forced_table.auto_rand_id, 2);
    assert_eq!(
        stored_value(&forced, &key::auto_random_table_id_kv_key(112, table_id)),
        b"1"
    );

    let forced_zero = statement("ALTER TABLE ar_alter FORCE AUTO_RANDOM_BASE=0");
    assert!(matches!(
        plan_ddl(&mut store, &forced_zero, 127).unwrap_err(),
        DdlPlanError::AutoIdReadFailed
    ));

    let plain_create = plan(
        &mut store,
        "CREATE TABLE not_random (id BIGINT PRIMARY KEY)",
        128,
    );
    apply(&mut store, &plain_create);
    store.put(key::schema_version_kv_key(), b"64".to_vec());
    let non_random = statement("ALTER TABLE not_random AUTO_RANDOM_BASE=10");
    assert!(matches!(
        plan_ddl(&mut store, &non_random, 129).unwrap_err(),
        DdlPlanError::InvalidAutoRandom(reason)
            if reason == "alter auto_random_base of a non auto_random table"
    ));
}

#[test]
fn alter_auto_id_cache_publishes_table_metadata_without_touching_the_counter() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE cached (id INT AUTO_INCREMENT PRIMARY KEY)",
        130,
    );
    apply(&mut store, &create);
    store.put(key::schema_version_kv_key(), b"61".to_vec());

    let alter = plan(&mut store, "ALTER TABLE cached AUTO_ID_CACHE=100", 131);
    let table_id = create.created_id.unwrap();
    let table: tidb_model::TableInfo =
        serde_json::from_slice(stored_value(&alter, &key::table_kv_key(112, table_id))).unwrap();
    assert_eq!(table.auto_id_cache, 100);
    assert_eq!(
        alter.diff.action_type,
        tidb_model::ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE
    );
    assert!(!alter
        .mutations
        .iter()
        .any(|mutation| mutation.key() == key::auto_table_id_kv_key(112, table_id)));

    apply(&mut store, &alter);
    store.put(key::schema_version_kv_key(), b"62".to_vec());
    assert!(matches!(
        plan_ddl(
            &mut store,
            &statement("ALTER TABLE cached AUTO_ID_CACHE=1"),
            132,
        )
        .unwrap_err(),
        DdlPlanError::Unsupported(reason)
            if reason == "Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different"
    ));
}

#[test]
fn modify_auto_random_bits_updates_table_info_and_the_tarid_counter_together() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE ar_bits_cluster (id BIGINT AUTO_RANDOM(5) PRIMARY KEY, v INT)",
        130,
    );
    apply(&mut store, &create);
    store.put(key::schema_version_kv_key(), b"61".to_vec());
    let table_id = create.created_id.unwrap();

    let alter = plan(
        &mut store,
        "ALTER TABLE ar_bits_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(8)",
        131,
    );
    let table: tidb_model::TableInfo =
        serde_json::from_slice(stored_value(&alter, &key::table_kv_key(112, table_id))).unwrap();
    assert_eq!(table.auto_random_bits, 8);
    assert_eq!(table.auto_random_range_bits, 64);
    assert_eq!(
        stored_value(&alter, &key::auto_random_table_id_kv_key(112, table_id)),
        b"1"
    );
    assert_eq!(
        alter.diff.action_type,
        tidb_model::ActionType::ACTION_MODIFY_COLUMN
    );
    let events = notifier_events(&mut store, &alter);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, -1);
    let (event_table, columns, analyzed) = events[0].1.modify_column_info();
    assert_eq!(event_table.auto_random_bits, 8);
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name.original(), "id");
    assert!(!analyzed);

    apply(&mut store, &alter);
    store.put(key::schema_version_kv_key(), b"62".to_vec());
    let decrease = statement("ALTER TABLE ar_bits_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(7)");
    assert!(matches!(
        plan_ddl(&mut store, &decrease, 132).unwrap_err(),
        DdlPlanError::InvalidAutoRandom(reason)
            if reason == "decreasing auto_random shard bits is not supported"
    ));
    let wrong_column =
        statement("ALTER TABLE ar_bits_cluster MODIFY COLUMN v BIGINT AUTO_RANDOM(9)");
    assert!(matches!(
        plan_ddl(&mut store, &wrong_column, 133).unwrap_err(),
        DdlPlanError::InvalidAutoRandom(reason)
            if reason == "auto_random can only be converted from auto_increment clustered primary key"
    ));

    let create_ai = plan(
        &mut store,
        "CREATE TABLE ai_bits_cluster (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)",
        134,
    );
    apply(&mut store, &create_ai);
    store.put(key::schema_version_kv_key(), b"63".to_vec());
    let ai_table_id = create_ai.created_id.unwrap();
    store.put(key::auto_table_id_kv_key(112, ai_table_id), b"100".to_vec());
    let converted = plan(
        &mut store,
        "ALTER TABLE ai_bits_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(5)",
        135,
    );
    let converted_table: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &converted,
        &key::table_kv_key(112, ai_table_id),
    ))
    .unwrap();
    assert_eq!(converted_table.auto_random_bits, 5);
    assert_eq!(
        converted_table.get_pk_col_info().unwrap().read().get_flag()
            & u64::from(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT),
        0
    );
    assert_eq!(
        stored_value(
            &converted,
            &key::auto_random_table_id_kv_key(112, ai_table_id)
        ),
        b"101"
    );
    assert!(converted.mutations.iter().any(|mutation| {
        mutation.kind() == BufferMutationOp::Delete
            && mutation.key() == key::auto_table_id_kv_key(112, ai_table_id)
    }));

    let create_separate = plan(
        &mut store,
        "CREATE TABLE ai_separate_cluster (id BIGINT AUTO_INCREMENT PRIMARY KEY)",
        136,
    );
    apply(&mut store, &create_separate);
    store.put(key::schema_version_kv_key(), b"64".to_vec());
    let separate_table_id = create_separate.created_id.unwrap();
    let separate_table_key = key::table_kv_key(112, separate_table_id);
    let mut separate_info: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&separate_table_key)
            .expect("the committed table metadata exists"),
    )
    .unwrap();
    separate_info.auto_id_cache = 1;
    store.put(
        separate_table_key,
        value::serialize_table_info(&separate_info).unwrap(),
    );
    store.put(
        key::auto_increment_id_kv_key(112, separate_table_id),
        b"100".to_vec(),
    );
    store.put(
        key::auto_table_id_kv_key(112, separate_table_id),
        b"40".to_vec(),
    );
    let separate = plan(
        &mut store,
        "ALTER TABLE ai_separate_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(5)",
        137,
    );
    assert_eq!(
        stored_value(
            &separate,
            &key::auto_increment_id_kv_key(112, separate_table_id)
        ),
        b"101"
    );
    assert_eq!(
        stored_value(
            &separate,
            &key::auto_random_table_id_kv_key(112, separate_table_id)
        ),
        b"40"
    );
    assert!(separate.mutations.iter().any(|mutation| {
        mutation.kind() == BufferMutationOp::Delete
            && mutation.key() == key::auto_table_id_kv_key(112, separate_table_id)
    }));
}

/// `AUTO_ID_CACHE 1` is Go's `SepAutoInc`, and only then does the counter move
/// to its own `IID:` key.
///
/// This node's own `CREATE TABLE` refuses the `AUTO_ID_CACHE` option (a
/// separate, pre-existing refusal), so the shape is built here the way it
/// really reaches this node: LOADED, from a table a Go `tidb-server` created.
/// The branch is not dead code — it is the case where reading `TID:` would
/// silently count in a key the owning Go node never touches.
#[test]
fn a_separate_allocator_table_counts_in_the_increment_key() {
    let mut template = tidb_model::table_info::TableInfo {
        id: 91,
        version: tidb_model::table_info::TABLE_INFO_VERSION5,
        ..tidb_model::table_info::TableInfo::default()
    };
    assert!(
        !template.sep_auto_inc(),
        "an ordinary table counts in the row-id key"
    );
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_id_key_for(7, &template),
        tidb_meta::key::auto_table_id_kv_key(7, 91),
    );

    template.auto_id_cache = 1;
    assert!(
        template.sep_auto_inc(),
        "AUTO_ID_CACHE 1 is Go's SepAutoInc"
    );
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_id_key_for(7, &template),
        tidb_meta::key::auto_increment_id_kv_key(7, 91),
    );
}

/// The stored table the index tests below are planned against.
fn table_with_two_columns(store: &mut MetaStore) -> i64 {
    let write = plan(
        store,
        "CREATE TABLE u6.minimal (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)",
        467_996_279_696_261_139,
    );
    apply(store, &write);
    write.created_id.expect("a table id")
}

#[test]
fn tiflash_batch_count_and_label_changes_preserve_available() {
    let mut store = bootstrapped();
    let id = table_with_two_columns(&mut store);
    let mut table = committed_table(&store, id);
    table.tiflash_replica = Some(GoShared::new(tidb_model::TiFlashReplicaInfo {
        count: 2,
        available: true,
        available_partition_ids: vec![991].into(),
        ..Default::default()
    }));
    store.put(
        key::table_kv_key(112, id),
        value::serialize_table_info(&table).unwrap(),
    );
    for sql in [
        "ALTER TABLE u6.minimal SET TIFLASH REPLICA 3 LOCATION LABELS 'zone'",
        "ALTER TABLE u6.minimal SET TIFLASH REPLICA 1",
    ] {
        let write = plan(&mut store, sql, 5000);
        apply(&mut store, &write);
        let current = committed_table(&store, id);
        let replica = current.tiflash_replica.as_ref().unwrap().read();
        assert!(
            replica.available,
            "Go preserves usability across count/label changes"
        );
        assert!(
            replica.available_partition_ids.is_empty(),
            "Go constructs a new record"
        );
    }
}

#[test]
fn tiflash_batch_partition_status_updates_containing_table() {
    let mut store = bootstrapped();
    let id = table_with_two_columns(&mut store);
    let mut table = committed_table(&store, id);
    table.partition = Some(GoShared::new(tidb_model::PartitionInfo {
        enable: true,
        definitions: vec![
            tidb_model::PartitionDefinition {
                id: 9001,
                ..Default::default()
            },
            tidb_model::PartitionDefinition {
                id: 9002,
                ..Default::default()
            },
        ]
        .into(),
        ..Default::default()
    }));
    table.tiflash_replica = Some(GoShared::new(tidb_model::TiFlashReplicaInfo {
        count: 2,
        ..Default::default()
    }));
    store.put(
        key::table_kv_key(112, id),
        value::serialize_table_info(&table).unwrap(),
    );
    for (physical, available, logical) in [
        (9001, true, false),
        (9002, true, true),
        (9001, false, false),
    ] {
        let DdlPlan::Write(write) = plan_ddl(
            &mut store,
            &DdlStatement::UpdateTiFlashReplicaStatus {
                table_id: physical,
                available,
            },
            5000,
        )
        .unwrap() else {
            panic!("status publishes a metadata change")
        };
        assert_eq!(write.diff.table_id, id);
        apply(&mut store, &write);
        let current = committed_table(&store, id);
        let replica = current.tiflash_replica.as_ref().unwrap().read();
        assert_eq!(replica.available, logical);
        assert_eq!(
            replica
                .available_partition_ids
                .iter()
                .any(|v| *v == physical),
            available
        );
        assert!(!store.pairs.contains_key(&key::table_kv_key(112, physical)));
    }
}

#[test]
fn tiflash_batch_restore_reset_and_zero_count_clear_the_record() {
    let mut store = bootstrapped();
    let id = table_with_two_columns(&mut store);
    let mut table = committed_table(&store, id);
    table.tiflash_replica = Some(GoShared::new(tidb_model::TiFlashReplicaInfo {
        count: 1,
        available: true,
        ..Default::default()
    }));
    store.put(
        key::table_kv_key(112, id),
        value::serialize_table_info(&table).unwrap(),
    );
    let mut statement = statement("ALTER TABLE u6.minimal SET TIFLASH REPLICA 1");
    let DdlStatement::SetTiFlashReplica {
        reset_available, ..
    } = &mut statement
    else {
        panic!("replica statement")
    };
    *reset_available = true;
    let DdlPlan::Write(write) = plan_ddl(&mut store, &statement, 5000).unwrap() else {
        panic!("reset writes")
    };
    apply(&mut store, &write);
    assert!(
        !committed_table(&store, id)
            .tiflash_replica
            .as_ref()
            .unwrap()
            .read()
            .available
    );
    let write = plan(
        &mut store,
        "ALTER TABLE u6.minimal SET TIFLASH REPLICA 0",
        5001,
    );
    apply(&mut store, &write);
    assert!(committed_table(&store, id).tiflash_replica.is_none());
}

fn tiflash_partition_fixture() -> (MetaStore, i64, Vec<i64>) {
    let mut store = bootstrapped();
    let create = plan(&mut store, "CREATE TABLE u6.tfparts (id BIGINT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN(MAXVALUE))", 5000);
    let id = create.created_id.unwrap();
    apply(&mut store, &create);
    let mut table = committed_table(&store, id);
    let ids: Vec<_> = table
        .partition
        .as_ref()
        .unwrap()
        .read()
        .definitions
        .snapshot()
        .iter()
        .map(|p| p.id)
        .collect();
    table.tiflash_replica = Some(GoShared::new(tidb_model::TiFlashReplicaInfo {
        count: 2,
        available: true,
        available_partition_ids: ids.clone().into(),
        ..Default::default()
    }));
    store.put(
        key::table_kv_key(112, id),
        value::serialize_table_info(&table).unwrap(),
    );
    (store, id, ids)
}

#[test]
fn tiflash_batch_drop_partition_removes_only_retired_availability_ids() {
    let (mut store, id, old_ids) = tiflash_partition_fixture();
    let write = plan(&mut store, "ALTER TABLE u6.tfparts DROP PARTITION p0", 5001);
    apply(&mut store, &write);
    let table = committed_table(&store, id);
    let replica = table.tiflash_replica.as_ref().unwrap().read();
    assert!(replica.available);
    assert_eq!(
        replica
            .available_partition_ids
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![old_ids[1]]
    );
}

#[test]
fn tiflash_batch_truncate_partition_resets_only_replaced_availability_ids() {
    let (mut store, id, old_ids) = tiflash_partition_fixture();
    let write = plan(
        &mut store,
        "ALTER TABLE u6.tfparts TRUNCATE PARTITION p0",
        5001,
    );
    apply(&mut store, &write);
    let table = committed_table(&store, id);
    let replica = table.tiflash_replica.as_ref().unwrap().read();
    assert!(!replica.available);
    assert_eq!(
        replica
            .available_partition_ids
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![old_ids[1]]
    );
}

#[test]
fn tiflash_batch_truncate_table_resets_all_availability() {
    let (mut store, _, _) = tiflash_partition_fixture();
    let write = plan(&mut store, "TRUNCATE TABLE u6.tfparts", 5001);
    apply(&mut store, &write);
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let table = catalog
        .databases
        .iter()
        .find(|db| db.info.name.lowercase() == "u6")
        .unwrap()
        .tables
        .iter()
        .find(|t| t.name.lowercase() == "tfparts")
        .unwrap();
    let replica = table.tiflash_replica.as_ref().unwrap().read();
    assert!(!replica.available);
    assert!(replica.available_partition_ids.is_empty());
    assert_eq!(replica.count, 2);
}

/// Reads back the `TableInfo` a write set stored for `table_id`.
pub(crate) fn stored_table(
    write: &tidb_exec::cluster_ddl::DdlWrite,
    table_id: i64,
) -> serde_json::Value {
    serde_json::from_slice(stored_value(write, &key::table_kv_key(112, table_id)))
        .expect("a stored TableInfo")
}

/// The index lands in `index_info` with the id `max_idx_id` names, and its
/// column offset is resolved against the STORED table rather than trusted from
/// the statement -- Go's `IndexColumn.Offset` is a position in `TableInfo.Cols`.
#[test]
fn create_index_stores_the_index_and_owes_a_backfill() {
    let mut store = bootstrapped();
    let table_id = table_with_two_columns(&mut store);
    let write = plan(&mut store, "CREATE INDEX vi ON u6.minimal (v)", 470_000_000);
    let stored = stored_table(&write, table_id);

    assert_eq!(stored["max_idx_id"], 1, "the first index of this table");
    let index = &stored["index_info"][0];
    assert_eq!(index["id"], 1);
    assert_eq!(index["idx_name"]["O"], "vi");
    assert_eq!(index["is_unique"], false);
    assert_eq!(index["is_primary"], false);
    // `state` 5 is Go's `StatePublic`.
    assert_eq!(index["state"], 5);
    assert_eq!(index["idx_cols"][0]["name"]["O"], "v");
    assert_eq!(
        index["idx_cols"][0]["offset"], 1,
        "`v` is the second column"
    );
    assert_eq!(index["idx_cols"][0]["length"], -1, "not a prefix index");
    assert_eq!(
        stored["update_timestamp"], 470_000_000_u64,
        "Go stamps the job transaction's own start timestamp"
    );

    // `ActionAddIndex`, so a peer's schema reload knows what changed.
    assert_eq!(write.diff.action_type.0, 7);
    assert_eq!(write.diff.table_id, table_id);

    // The half that keeps the index from being EMPTY. Losing it is the silent
    // wrong answer this whole path exists to avoid, which is why the publisher
    // that cannot perform it refuses outright rather than writing the meta half.
    let backfill = write.backfill.first().expect("entries are owed");
    assert!(matches!(backfill.operation, IndexBackfillOperation::Add(_)));
    {
        let index = backfill.index.read();
        assert_eq!(index.id, 1);
    }
    assert_eq!(
        backfill.table.indices.len(),
        0,
        "the walker gets the table as its stored ROWS have it: before the change"
    );
}

/// The Go c605 add-index path carries AUTO independently of manual split
/// arguments.  The catalog plan must retain that request instead of silently
/// dropping it while constructing `IndexInfo`.
#[test]
fn create_index_auto_pre_split_marker_reaches_catalog_write() {
    let mut store = bootstrapped();
    let table_id = table_with_two_columns(&mut store);
    let auto = plan(
        &mut store,
        "CREATE INDEX vi ON u6.minimal (v) PRE_SPLIT_REGIONS = AUTO",
        470_000_000,
    );
    assert!(auto.auto_pre_split);
    assert_eq!(auto.backfill.first().unwrap().index.read().id, 1);
    assert_eq!(
        stored_table(&auto, table_id)["index_info"][0]["idx_name"]["O"],
        "vi"
    );

    let manual = plan(
        &mut store,
        "CREATE INDEX manual ON u6.minimal (v) PRE_SPLIT_REGIONS = 4",
        470_000_001,
    );
    assert!(!manual.auto_pre_split);
}

/// Go captures `collate.NewCollationEnabled()` in `DDLReorgMeta` when the job
/// is built. The backfill must carry that snapshot rather than re-read the
/// runtime switch after the catalog mutation has already been planned.
#[test]
fn index_backfill_carries_the_planned_collation_mode() {
    for use_new_collation in [false, true] {
        let mut store = bootstrapped();
        table_with_two_columns(&mut store);
        let write = match plan_ddl_with_collation(
            &mut store,
            &statement("CREATE INDEX vi ON u6.minimal (v)"),
            470_000_000,
            use_new_collation,
        )
        .expect("the fixture plans")
        {
            DdlPlan::Write(write) => write,
            DdlPlan::AlreadySatisfied { detail, .. } => {
                panic!("expected a write, got already-satisfied: {detail}")
            }
        };
        assert_eq!(
            write
                .backfill
                .first()
                .expect("CREATE INDEX owes a backfill")
                .use_new_collation,
            use_new_collation
        );
    }
}

/// A second index of the same name is 1061, and `IF NOT EXISTS` makes it a
/// no-op that spends no schema version.
#[test]
fn a_duplicate_index_name_is_refused_and_if_not_exists_is_a_no_op() {
    let mut store = bootstrapped();
    table_with_two_columns(&mut store);
    let write = plan(&mut store, "CREATE INDEX vi ON u6.minimal (v)", 470_000_000);
    apply(&mut store, &write);

    let refused = plan_ddl(
        &mut store,
        &statement("CREATE INDEX vi ON u6.minimal (v)"),
        470_000_001,
    )
    .expect_err("a duplicate index name is an error");
    assert!(
        matches!(&refused, DdlPlanError::DuplicateKeyName(name) if name == "vi"),
        "{refused}"
    );
    assert_eq!(refused.to_string(), "Duplicate key name 'vi'");

    match plan_ddl(
        &mut store,
        &statement("CREATE INDEX IF NOT EXISTS vi ON u6.minimal (v)"),
        470_000_002,
    )
    .expect("IF NOT EXISTS plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("IF NOT EXISTS on an existing index must write nothing"),
    }
}

/// Every shape whose entries this node would not go on to maintain is refused
/// before a timestamp is spent: publishing one writes a `TableInfo` this node's
/// own catalog loader then drops, so the table would vanish from the very
/// connection that indexed it.
#[test]
fn index_shapes_this_node_cannot_maintain_are_refused_at_admission() {
    for (sql, expected) in [
        (
            "CREATE INDEX ei ON u6.minimal ((v + 1))",
            "an expression index",
        ),
        (
            "CREATE FULLTEXT INDEX fi ON u6.minimal (c)",
            "CREATE FULLTEXT INDEX",
        ),
    ] {
        let reason = refusal(sql);
        assert!(
            reason.contains(expected),
            "`{sql}` must be refused for {expected}, got: {reason}"
        );
    }
}

/// The index leaves the stored table, and its entries are named for removal in
/// the same transaction -- a stale entry reads as a row that is not there.
#[test]
fn drop_index_removes_it_and_owes_the_entry_removal() {
    let mut store = bootstrapped();
    let table_id = table_with_two_columns(&mut store);
    let created = plan(&mut store, "CREATE INDEX vi ON u6.minimal (v)", 470_000_000);
    apply(&mut store, &created);

    let write = plan(&mut store, "DROP INDEX vi ON u6.minimal", 470_000_003);
    let stored = stored_table(&write, table_id);
    assert!(
        stored["index_info"]
            .as_array()
            .is_none_or(|indexes| indexes.is_empty()),
        "the index is gone from the stored table: {}",
        stored["index_info"]
    );
    assert_eq!(
        stored["max_idx_id"], 1,
        "Go never lowers MaxIndexID, so a later index cannot reuse the id"
    );
    // `ActionDropIndex`.
    assert_eq!(write.diff.action_type.0, 8);
    let backfill = write.backfill.first().expect("entries are owed");
    assert!(matches!(backfill.operation, IndexBackfillOperation::Drop));
    {
        let index = backfill.index.read();
        assert_eq!(index.name.original(), "vi");
    }
    assert_eq!(
        backfill.table.indices.len(),
        1,
        "the walker gets the table with the index still on it, so it can key its entries"
    );

    apply(&mut store, &write);
    let refused = plan_ddl(
        &mut store,
        &statement("DROP INDEX nosuch ON u6.minimal"),
        470_000_004,
    )
    .expect_err("a missing index is an error");
    // Go `ErrCantDropFieldOrKey` (1091), not a message of this port's own.
    assert_eq!(
        refused.to_string(),
        "Can't DROP 'nosuch'; check that column/key exists"
    );
    match plan_ddl(
        &mut store,
        &statement("DROP INDEX IF EXISTS nosuch ON u6.minimal"),
        470_000_005,
    )
    .expect("IF EXISTS plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("IF EXISTS on a missing index must write nothing"),
    }
}

/// Go `preprocessor.checkAutoIncrementOp`: the allocator hands out integers,
/// so a non-numeric AUTO_INCREMENT column is refused.
///
/// The cluster tier used to refuse EVERY `AUTO_INCREMENT` table for its own
/// reason, which hid this. Captured from a Go `tidb-server` on the same
/// cluster: `id VARCHAR(10) NOT NULL AUTO_INCREMENT` answers
/// `ERROR 1105 (HY000): Incorrect column specifier for column 'id'`, while
/// without the check this node created the table and then failed every INSERT
/// with a decode error -- an unusable table reported as a success.
#[test]
fn a_non_numeric_auto_increment_column_is_refused_the_way_go_refuses_it() {
    for sql in [
        "CREATE TABLE t (id VARCHAR(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id DATETIME NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id DECIMAL(10,2) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        let refused = lower_ddl(&parsed, "u6").expect_err("this shape must be refused");
        assert_eq!(
            refused.reason, "Incorrect column specifier for column 'id'",
            "`{sql}`"
        );
    }
    // Go's list is WIDER than "integer": FLOAT and DOUBLE are in it, and a Go
    // tidb-server really does accept `id DOUBLE NOT NULL AUTO_INCREMENT`.
    for sql in [
        "CREATE TABLE t (id TINYINT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id MEDIUMINT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id FLOAT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id DOUBLE NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        assert!(lower_ddl(&parsed, "u6").is_ok(), "`{sql}` must be admitted");
    }
}

#[test]
fn add_column_appends_a_public_nullable_column_and_refuses_rewrites() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)",
        100,
    );
    apply(&mut store, &write);

    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN note VARCHAR(32)",
        200,
    );
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    assert_eq!(columns.len(), 3, "the new column is appended");
    let added = &columns[2];
    assert_eq!(added["name"]["O"], "note");
    // Go `AllocateColumnID`: past the existing max, never reused.
    assert_eq!(added["id"], 3);
    assert_eq!(added["offset"], 2);
    assert_eq!(
        added["state"], 5,
        "public immediately: no backfill was needed"
    );
    assert_eq!(stored["max_col_id"], 3);

    // A duplicate is MySQL's own message; IF NOT EXISTS is a no-op. Both are
    // plan-time answers: only the stored table knows its columns.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN note VARCHAR(8)"),
        300,
    )
    .expect_err("a duplicate column is refused")
    .to_string();
    assert!(error.contains("Duplicate column name 'note'"), "{error}");
    match plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN IF NOT EXISTS note VARCHAR(8)"),
        300,
    )
    .expect("the no-op plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("IF NOT EXISTS over an existing column must be a no-op"),
    }

    // Go `generateOriginDefaultValue`: a declared default becomes the origin
    // default existing rows report; NOT NULL without one stamps the type's
    // zero value. Neither rewrites a row.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN flag BIGINT DEFAULT 7",
        400,
    );
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let flag = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"]["O"] == "flag")
        .expect("the defaulted column is stored");
    assert_eq!(flag["origin_default"], "7");
    assert_eq!(flag["default"], "7");

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN zeroed BIGINT NOT NULL",
        500,
    );
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let zeroed = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"]["O"] == "zeroed")
        .expect("the NOT NULL column is stored");
    assert_eq!(zeroed["origin_default"], "0", "the type's zero value");
    assert_eq!(zeroed["default"], serde_json::Value::Null);

    // Go's clock arm: the DECLARED default stays the word for every later
    // INSERT, while the origin default is stamped ONCE at DDL time.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN ts DATETIME DEFAULT CURRENT_TIMESTAMP",
        600,
    );
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let ts = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"]["O"] == "ts")
        .expect("the clock column is stored");
    assert_eq!(ts["default"], "CURRENT_TIMESTAMP");
    let stamped = ts["origin_default"].as_str().expect("a stamped instant");
    assert!(
        stamped.len() == 19 && stamped.contains('-') && stamped.contains(':'),
        "the origin default is one wall-clock instant, got {stamped}"
    );
}

/// Go `onTruncateTable`: the schema survives under a FRESH table id, the old
/// id's rows become unreachable, and the auto-id counters restart because the
/// allocator keys travel with the id.
#[test]
fn truncate_reallocates_the_table_id_and_restarts_the_allocators() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let old_id = write.created_id.expect("CREATE TABLE allocates an id");
    // A used allocator, which the truncate must delete.
    store.put(key::auto_table_id_kv_key(112, old_id), b"30".to_vec());

    let write = plan(&mut store, "TRUNCATE TABLE u6.t", 200);
    let new_id = write.diff.table_id;
    assert_ne!(new_id, old_id, "the id is fresh, never reused");
    assert_eq!(write.diff.old_table_id, old_id);
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_TRUNCATE_TABLE
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, new_id)))
            .expect("the truncated table is stored");
    assert_eq!(stored["id"], new_id);
    assert!(
        write.mutations.iter().any(|mutation| mutation.key()
            == key::table_kv_key(112, old_id).as_slice()
            && matches!(mutation.kind(), BufferMutationOp::Delete)),
        "the old table key is deleted"
    );
    assert!(
        write.mutations.iter().any(|mutation| mutation.key()
            == key::auto_table_id_kv_key(112, old_id).as_slice()
            && matches!(mutation.kind(), BufferMutationOp::Delete)),
        "the observed allocator is deleted with the old id"
    );
}

/// Go `isDroppableColumn` + `onDropColumn`: offsets close over the gap, a
/// single-column secondary index goes with its column, and the three
/// refusals answer Go's exact messages.
#[test]
fn drop_column_shifts_offsets_and_takes_its_single_column_index() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT, c BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("an id");
    let write = plan(&mut store, "CREATE INDEX idx_a ON u6.t (a)", 150);
    apply(&mut store, &write);

    let write = plan(&mut store, "ALTER TABLE u6.t DROP COLUMN b", 200);
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("the altered table is stored");
    let columns = stored["cols"].as_array().expect("columns");
    let names: Vec<_> = columns
        .iter()
        .map(|c| c["name"]["O"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["id", "a", "c"]);
    let offsets: Vec<_> = columns
        .iter()
        .map(|c| c["offset"].as_i64().unwrap())
        .collect();
    assert_eq!(offsets, [0, 1, 2], "the gap closes");
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_DROP_COLUMN
    );

    // The single-column index on `a` goes with `a`.
    let write = plan(&mut store, "ALTER TABLE u6.t DROP COLUMN a", 300);
    assert_eq!(
        write.backfill.len(),
        1,
        "column removal must delete its index keys"
    );
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    // `[]`, not `null`: Go removes the entry from a non-nil slice, and an
    // emptied non-nil slice marshals as an empty array — unlike the builder's
    // untouched nil slice a fresh CREATE TABLE stores. Both states are pinned.
    assert_eq!(
        stored["index_info"],
        serde_json::json!([]),
        "listIndicesWithColumn drops idx_a with its column"
    );

    for (sql, message) in [
        (
            "ALTER TABLE u6.t DROP COLUMN id",
            "Unsupported drop integer primary key",
        ),
        (
            "ALTER TABLE u6.t DROP COLUMN missing",
            "Can't DROP 'missing'; check that column/key exists",
        ),
    ] {
        let error = plan_ddl(&mut store, &statement(sql), 400)
            .expect_err("refused")
            .to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
}

/// Go's one ActionMultiSchemaChange job: the sub-actions fold over ONE
/// evolving TableInfo inside one transaction, in SQL order, so a later action
/// sees the earlier one's change and the table lands whole or not at all.
#[test]
fn a_multi_action_alter_folds_over_one_evolving_table() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("an id");

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN c BIGINT DEFAULT 5, DROP COLUMN a, ADD COLUMN d BIGINT",
        200,
    );
    apply(&mut store, &write);
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let names: Vec<_> = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"]["O"].as_str().unwrap())
        .collect();
    // `c` appended after `b`, then `a` dropped closing the gap, then `d`
    // appended after the shift — SQL order, one evolving table.
    assert_eq!(names, ["id", "b", "c", "d"]);
    let offsets: Vec<_> = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["offset"].as_i64().unwrap())
        .collect();
    assert_eq!(offsets, [0, 1, 2, 3]);

    // One failing sub-action fails the whole bundle: nothing is staged.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN e BIGINT, DROP COLUMN missing"),
        300,
    )
    .expect_err("the failing drop fails the bundle")
    .to_string();
    assert!(error.contains("Can't DROP 'missing'"), "{error}");

    // Every sub-action a no-op is the statement already satisfied.
    match plan_ddl(
        &mut store,
        &statement(
            "ALTER TABLE u6.t ADD COLUMN IF NOT EXISTS c BIGINT, DROP COLUMN IF EXISTS missing",
        ),
        400,
    )
    .expect("the all-no-op bundle plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("an all-no-op bundle must publish nothing"),
    }
}

/// A bundle mixing column and index changes folds over the same
/// evolving table: the index resolves against an original column, the
/// backfill walks existing rows against the evolved columns, and multiple
/// index actions retain their SQL order in one catalog transaction.
#[test]
fn a_column_and_index_bundle_folds_and_backfills_together() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("an id");

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN c BIGINT DEFAULT 5, ADD INDEX idx_c (v)",
        200,
    );
    let events = notifier_events(&mut store, &write);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, 0);
    assert_eq!(
        events[0].1.action_type(),
        tidb_model::ActionType::ACTION_ADD_COLUMN
    );
    assert_eq!(events[1].0, 1);
    assert_eq!(
        events[1].1.action_type(),
        tidb_model::ActionType::ACTION_ADD_INDEX
    );
    apply(&mut store, &write);
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE
    );
    let backfill = write.backfill.first().expect("the index change backfills");
    assert!(matches!(backfill.operation, IndexBackfillOperation::Add(_)));
    assert_eq!(backfill.index.read().name.original(), "idx_c");
    assert_eq!(
        backfill
            .index
            .read()
            .columns
            .iter_deref()
            .next()
            .unwrap()
            .read()
            .offset,
        1,
        "the admitted original column retains its offset"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    assert_eq!(stored["index_info"].as_array().unwrap().len(), 1);

    // The drop direction: the backfill's table still CARRIES the index the
    // walk removes, while the stored table no longer names it.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN d BIGINT, DROP INDEX idx_c",
        300,
    );
    apply(&mut store, &write);
    let backfill = write.backfill.first().expect("the removal walks");
    assert!(matches!(backfill.operation, IndexBackfillOperation::Drop));
    assert!(
        backfill
            .table
            .indices
            .iter_deref()
            .any(|index| index.read().name.original() == "idx_c"),
        "the walk's table still carries the dropped index"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    assert_eq!(stored["index_info"].as_array().unwrap().len(), 0);

    // Go merges multiple add-index sub-jobs and executes their reorganization
    // together. The Rust transaction carries both ordered entry walks.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD INDEX i1 (v), ADD INDEX i2 (c)",
        400,
    );
    assert_eq!(write.backfill.len(), 2);
    assert_eq!(write.backfill[0].index.read().name.original(), "i1");
    assert_eq!(write.backfill[1].index.read().name.original(), "i2");
}

/// Go's type-compatibility decision distinguishes metadata changes from row
/// reorganization. This catalog planner only owns the former; refusing the
/// latter protects stored rows until the durable MODIFY worker is implemented.
#[test]
fn a_modify_column_reorganizes_exactly_where_go_says_it_must() {
    fn refuse(store: &mut MetaStore, sql: &str, start_ts: u64) -> String {
        plan_ddl(store, &statement(sql), start_ts)
            .expect_err("a reorganizing modify must be refused")
            .to_string()
    }

    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE widen (id BIGINT PRIMARY KEY, small INT, big BIGINT, \
         name VARCHAR(10), money DECIMAL(10,2))",
        200,
    );
    apply(&mut store, &create);

    // Integer widening is metadata only: Go compares the types' DEFAULT
    // display widths, so INT(11) -> BIGINT(20) grows and costs nothing.
    let widened = plan(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN small BIGINT",
        201,
    );
    assert!(!widened.mutations.is_empty(), "the widening is planned");

    // The reverse narrows, and carries Go's own reason verbatim.
    let narrowed = refuse(&mut store, "ALTER TABLE widen MODIFY COLUMN big INT", 202);
    assert!(
        narrowed.contains("length 11 is less than origin 20"),
        "{narrowed}"
    );

    // A string widening is free; a shortening is not.
    let longer = plan(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN name VARCHAR(40)",
        203,
    );
    assert!(
        !longer.mutations.is_empty(),
        "the longer varchar is planned"
    );
    let shorter = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN name VARCHAR(4)",
        204,
    );
    assert!(
        shorter.contains("length 4 is less than origin 10"),
        "{shorter}"
    );

    // Crossing families is never free.
    let crossed = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN small DATETIME",
        205,
    );
    assert!(crossed.contains("not match origin"), "{crossed}");

    // Go: char <-> varchar always reorganizes, in either direction.
    let recast = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN name CHAR(40)",
        206,
    );
    assert!(
        recast.contains("conversion between char and varchar string"),
        "{recast}"
    );

    // Sign is a rewrite of every stored row.
    let resigned = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN small INT UNSIGNED",
        207,
    );
    assert!(
        resigned.contains("can't change unsigned integer to signed or vice versa"),
        "{resigned}"
    );

    // A decimal must match exactly in flen, scale and sign.
    let rescaled = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN money DECIMAL(10,3)",
        208,
    );
    assert!(
        rescaled.contains("decimal change from decimal(10, 2) to decimal(10, 3)"),
        "{rescaled}"
    );
}

/// All prefix-index spellings retain their key-part lengths in published metadata.
#[test]
fn prefix_index_spellings_preserve_key_part_lengths() {
    let mut store = bootstrapped();
    for (name, sql, unique) in [
        (
            "pfx",
            "CREATE TABLE pfx(id BIGINT PRIMARY KEY,c VARCHAR(20),INDEX px(c(5)))",
            false,
        ),
        (
            "pfx_unique",
            "CREATE TABLE pfx_unique(id BIGINT PRIMARY KEY,c VARCHAR(20),UNIQUE KEY px(c(5)))",
            true,
        ),
    ] {
        let write = plan(&mut store, sql, 300);
        let stored = stored_table(&write, write.created_id.unwrap());
        assert_eq!(
            stored["index_info"][0]["idx_cols"][0]["length"], 5,
            "{name}: {stored}"
        );
        assert_eq!(stored["index_info"][0]["is_unique"], unique);
        apply(&mut store, &write);
    }
    let create = plan(
        &mut store,
        "CREATE TABLE pfx2(id BIGINT PRIMARY KEY,c VARCHAR(20))",
        301,
    );
    let id = create.created_id.unwrap();
    apply(&mut store, &create);
    for (sql, name) in [
        ("CREATE INDEX px ON pfx2(c(5))", "px"),
        ("ALTER TABLE pfx2 ADD INDEX ax(c(5))", "ax"),
    ] {
        let write = plan(&mut store, sql, 302);
        let stored = stored_table(&write, id);
        let index = stored["index_info"]
            .as_array()
            .unwrap()
            .iter()
            .find(|index| index["idx_name"]["O"] == name)
            .unwrap();
        assert_eq!(index["idx_cols"][0]["length"], 5);
        apply(&mut store, &write);
    }
}

/// A resolved view definition as the route would hand it over, for the
/// plan-arm pins below — the RESOLUTION itself is the session tier's
/// `resolve_view_definition`, tested beside `run_create_view_in`.
fn view_statement(schema: &str, name: &str, or_replace: bool) -> DdlStatement {
    let view = tidb_executor::ViewDef {
        name: name.to_owned(),
        columns: vec![(
            "a".to_owned(),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        )],
        select_sql: format!("SELECT `t`.`a` AS `a` FROM `{schema}`.`t`"),
        definer_user: "root".to_owned(),
        definer_host: "%".to_owned(),
        character_set_client: "utf8mb4".to_owned(),
        collation_connection: "utf8mb4_bin".to_owned(),
        algorithm: "UNDEFINED".to_owned(),
        security: "DEFINER".to_owned(),
        check_option: "CASCADED".to_owned(),
    };
    DdlStatement::CreateView {
        schema: schema.to_owned(),
        name: name.to_owned(),
        or_replace,
        info: Box::new(tidb_exec::cluster_ddl::build_view_table_info(name, &view)),
    }
}

#[test]
fn a_create_view_publishes_a_view_table_info() {
    // Go `onCreateView` (`ddl/create_table.go:371`): the finished TableInfo
    // goes to `createTableOrViewWithCheck` under ACTION_CREATE_VIEW; the
    // published metadata carries the view half, the resolved columns, and
    // the creator's client charset/collation in Charset/Collate — which is
    // where SHOW CREATE VIEW reads them back (`executor/show.go`).
    let mut store = bootstrapped();
    let create_t = plan(&mut store, "CREATE TABLE t (a BIGINT PRIMARY KEY)", 101);
    apply(&mut store, &create_t);

    // Recheck the actual publication snapshot: a table can appear after
    // session-side view resolution, and must never be deleted as a view.
    let error = plan_ddl(&mut store, &view_statement("u6", "t", true), 102)
        .expect_err("OR REPLACE cannot replace a base table");
    let DdlPlanError::Admission(error) = error else {
        panic!("expected WrongObject admission: {error:?}");
    };
    assert_eq!(error.code, 1347);

    let write = match plan_ddl(&mut store, &view_statement("u6", "v", false), 102)
        .expect("a fresh view plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write: {detail}"),
    };
    assert!(
        notifier_events(&mut store, &write).is_empty(),
        "Go onCreateView publishes no schema-change event"
    );
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_CREATE_VIEW
    );
    let put = write
        .mutations
        .iter()
        .find(|m| m.kind() == BufferMutationOp::Set && m.value().starts_with(b"{"))
        .expect("the view's TableInfo is written");
    let info: tidb_model::TableInfo =
        serde_json::from_slice(put.value()).expect("the published value is a TableInfo");
    let view = info
        .view
        .as_ref()
        .expect("the view half rides along")
        .read();
    assert_eq!(view.select_stmt, "SELECT `t`.`a` AS `a` FROM `u6`.`t`");
    assert_eq!(info.charset, "utf8mb4");
    assert_eq!(info.collate, "utf8mb4_bin");
    assert_eq!(info.columns.len(), 1);
    apply(&mut store, &write);

    // The same name again without OR REPLACE is Go's ErrTableExists.
    let error = plan_ddl(&mut store, &view_statement("u6", "v", false), 103)
        .expect_err("a duplicate view name refuses");
    assert!(format!("{error:?}").contains("TableExists"), "{error:?}");

    // OR REPLACE drops the old id and creates a fresh one.
    let replace = match plan_ddl(&mut store, &view_statement("u6", "v", true), 104)
        .expect("OR REPLACE plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write: {detail}"),
    };
    assert!(
        replace
            .mutations
            .iter()
            .any(|m| m.kind() == BufferMutationOp::Delete),
        "the old view's key is deleted"
    );
    assert_eq!(replace.diff.old_table_id, write.diff.table_id);
    assert_ne!(replace.diff.table_id, replace.diff.old_table_id);
    apply(&mut store, &replace);

    // DROP VIEW deletes it under ACTION_DROP_VIEW; a base table under the
    // same statement is Go's ErrWrongObject; a missing name without
    // IF EXISTS is Go's Unknown table.
    let drop = match plan_ddl(
        &mut store,
        &DdlStatement::DropView {
            names: vec![("u6".to_owned(), "v".to_owned())],
            if_exists: false,
        },
        105,
    )
    .expect("the drop plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write: {detail}"),
    };
    assert_eq!(
        drop.diff.action_type,
        tidb_model::ActionType::ACTION_DROP_VIEW
    );
    apply(&mut store, &drop);

    let wrong = plan_ddl(
        &mut store,
        &DdlStatement::DropView {
            names: vec![("u6".to_owned(), "t".to_owned())],
            if_exists: true,
        },
        106,
    )
    .expect_err("a base table refuses DROP VIEW even under IF EXISTS");
    assert_eq!(wrong.to_sql_error().code, 1347);

    let missing = plan_ddl(
        &mut store,
        &DdlStatement::DropView {
            names: vec![("u6".to_owned(), "gone".to_owned())],
            if_exists: false,
        },
        107,
    )
    .expect_err("a missing view without IF EXISTS refuses");
    let missing = missing.to_sql_error();
    assert_eq!(missing.code, 1051);
    assert_eq!(missing.message, "Unknown table 'u6.gone'");
}

#[test]
fn a_check_constraint_is_ignored_with_gos_warning() {
    // Go's DEFAULT (`tidb_enable_check_constraint` off): both CHECK
    // spellings — the column option (`ddl/add_column.go:577`) and the table
    // constraint (`ddl/create_table.go:1470`) — warn
    // `tidb_enable_check_constraint is off` and are IGNORED; the table
    // creates and enforces nothing. Probe 24 caught this node refusing
    // where every default-configured Go server accepts.
    let context = tidb_executor::StmtContext::for_query();
    let parsed =
        tidb_parser::parse("CREATE TABLE ck (v INT CHECK (v > 0), CONSTRAINT big CHECK (v < 100))")
            .expect("parses");
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("admitted")
        .expect("a catalog change");
    let DdlStatement::CreateTable { build, .. } = statement else {
        panic!("a CreateTable");
    };
    assert!(
        build.template().view.is_none() && build.template().columns.len() == 1,
        "one plain column, no constraint metadata"
    );
    assert_eq!(
        context.warning_count(),
        2,
        "one warning per ignored CHECK spelling"
    );
}

#[test]
fn enabled_create_check_constraints_persist_gos_metadata_and_errors() {
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let parsed = tidb_parser::parse(
        "CREATE TABLE ck (v INT CHECK (v > 0), CONSTRAINT big CHECK (v < 100) NOT ENFORCED)",
    )
    .expect("parses");
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("enabled CHECK DDL is admitted")
        .expect("a catalog change");
    let DdlStatement::CreateTable { build, .. } = statement else {
        panic!("a CreateTable");
    };
    let constraints = build
        .template()
        .constraints
        .iter_deref()
        .collect::<Vec<_>>();
    assert_eq!(constraints.len(), 2);
    let table_check = constraints[0].read();
    assert_eq!(table_check.id, 1);
    assert_eq!(table_check.name.original(), "big");
    assert_eq!(table_check.constraint_cols[0].original(), "v");
    assert_eq!(table_check.expr_string, "`v` < 100");
    assert!(!table_check.enforced);
    assert!(!table_check.in_column);
    assert_eq!(table_check.state, tidb_model::SchemaState::PUBLIC);
    drop(table_check);
    let column_check = constraints[1].read();
    assert_eq!(column_check.id, 2);
    assert_eq!(column_check.name.original(), "ck_chk_1");
    assert_eq!(column_check.expr_string, "`v` > 0");
    assert!(column_check.enforced);
    assert!(column_check.in_column);
    assert_eq!(context.warning_count(), 0);

    let refusal = |sql: &str| {
        let parsed = tidb_parser::parse(sql).expect("error fixture parses");
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect_err("enabled Go CHECK validation rejects the fixture")
    };
    let duplicate =
        refusal("CREATE TABLE ck (v INT, CONSTRAINT c CHECK(v > 0), CONSTRAINT c CHECK(v < 2))");
    assert_eq!(duplicate.code, 3822);
    assert_eq!(duplicate.reason, "Duplicate check constraint name 'c'.");
    let missing = refusal("CREATE TABLE ck (v INT, CONSTRAINT c CHECK(absent > 0))");
    assert_eq!(missing.code, 3820);
    assert_eq!(
        missing.reason,
        "Check constraint 'c' refers to non-existing column 'absent'."
    );
    let cross_column = refusal("CREATE TABLE ck (v INT CHECK(v < other), other INT)");
    assert_eq!(cross_column.code, 3813);
    assert_eq!(
        cross_column.reason,
        "Column check constraint 'ck_chk_1' references other column."
    );
    let auto_increment =
        refusal("CREATE TABLE ck (v INT AUTO_INCREMENT, CONSTRAINT c CHECK(v > 0))");
    assert_eq!(auto_increment.code, 3818);
    assert_eq!(
        auto_increment.reason,
        "Check constraint 'c' cannot refer to an auto-increment column."
    );
    let non_boolean = refusal("CREATE TABLE ck (v INT, CONSTRAINT c CHECK(v + 1))");
    assert_eq!(non_boolean.code, 3812);
    assert_eq!(
        non_boolean.reason,
        "An expression of non-boolean type specified to a check constraint 'c'."
    );
    let variable = refusal("CREATE TABLE ck (v INT, CONSTRAINT c CHECK(v > @limit))");
    assert_eq!(variable.code, 3816);
    assert_eq!(
        variable.reason,
        "An expression of a check constraint 'c' cannot refer to a user or system variable."
    );
    let function = refusal("CREATE TABLE ck (v INT, CONSTRAINT c CHECK(v < rand()))");
    assert_eq!(function.code, 3814);
    assert_eq!(
        function.reason,
        "An expression of a check constraint 'c' contains disallowed function: rand."
    );
}

#[test]
fn check_constraints_follow_go_for_column_dependencies_and_create_like() {
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let lower = |sql: &str| {
        let parsed = tidb_parser::parse(sql).expect("CHECK DDL parses");
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect("CHECK DDL is admitted")
            .expect("CHECK DDL owns a catalog route")
    };
    let mut store = bootstrapped();

    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE ck_dep (a INT, b INT, \
             CONSTRAINT c_pair CHECK (a < b) NOT ENFORCED, \
             CONSTRAINT c_one CHECK (b > 0) NOT ENFORCED)",
        ),
        1_100,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);

    let rename = plan_ddl(
        &mut store,
        &lower("ALTER TABLE ck_dep RENAME COLUMN a TO a"),
        1_101,
    )
    .expect_err("Go checks CHECK dependencies before its same-name no-op");
    let DdlPlanError::Admission(rename) = rename else {
        panic!("expected a coded DDL refusal, got {rename:?}")
    };
    assert_eq!(rename.code, 3959);
    assert_eq!(
        rename.reason,
        "Check constraint 'c_pair' uses column 'a', hence column cannot be dropped or renamed."
    );

    let drop_multi = plan_ddl(
        &mut store,
        &lower("ALTER TABLE ck_dep DROP COLUMN a"),
        1_102,
    )
    .expect_err("a multi-column CHECK blocks DROP COLUMN");
    let DdlPlanError::Admission(drop_multi) = drop_multi else {
        panic!("expected a coded DDL refusal, got {drop_multi:?}")
    };
    assert_eq!(drop_multi.code, 3959);

    let source_id = create.created_id.expect("CREATE allocated a table id");
    let source_key = key::table_kv_key(112, source_id);
    let mut source: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&source_key)
            .expect("the committed source table exists"),
    )
    .expect("the source table decodes");
    source.tiflash_replica = Some(GoShared::new(tidb_model::TiFlashReplicaInfo {
        count: 2,
        location_labels: vec!["zone".to_owned()].into(),
        available: true,
        available_partition_ids: vec![source_id].into(),
    }));
    source.ttl_info = Some(GoShared::new(tidb_model::TTLInfo {
        column_name: tidb_ast::CiString::new("a"),
        interval_expr_str: "1".to_owned(),
        interval_time_unit: 5,
        enable: true,
        job_interval: "1h".to_owned(),
    }));
    source.affinity = Some(GoShared::new(tidb_model::TableAffinityInfo {
        level: "table".to_owned(),
    }));
    source.auto_rand_id = 77;
    source.max_foreign_key_id = 9;
    store.put(
        source_key.clone(),
        value::serialize_table_info(&source).expect("the augmented source encodes"),
    );

    let clean_source = source.clone_like_go();
    let missing_source = plan_ddl(
        &mut store,
        &lower("CREATE TABLE missing_target.ck_missing LIKE missing_source.ck_dep"),
        1_102_0,
    )
    .expect_err("Go resolves a missing LIKE source before the target database");
    assert!(
        matches!(
            missing_source,
            DdlPlanError::TableNotExists { ref schema, ref table }
                if schema == "missing_source" && table == "ck_dep"
        ),
        "{missing_source:?}"
    );
    let expect_temporary_like_refusal = |store: &mut MetaStore,
                                         source: &tidb_model::TableInfo,
                                         target: &str,
                                         start_ts: u64,
                                         code: u16,
                                         reason: &str| {
        store.put(
            source_key.clone(),
            value::serialize_table_info(source).expect("the test source encodes"),
        );
        let error = plan_ddl(
            store,
            &lower(&format!(
                "CREATE GLOBAL TEMPORARY TABLE {target} LIKE ck_dep \
                     ON COMMIT DELETE ROWS"
            )),
            start_ts,
        )
        .expect_err("Go refuses this inherited temporary-table setting");
        let DdlPlanError::Admission(error) = error else {
            panic!("expected a coded DDL refusal, got {error:?}")
        };
        assert_eq!(error.code, code, "{target}");
        assert_eq!(error.reason, reason, "{target}");
    };

    let mut invalid_source = clean_source.clone_like_go();
    invalid_source.temp_table_type = tidb_model::TempTableType::GLOBAL;
    invalid_source.auto_random_bits = 3;
    expect_temporary_like_refusal(
        &mut store,
        &invalid_source,
        "missing_schema.ck_from_temp",
        1_102_1,
        8006,
        "`create table like` is unsupported on temporary tables.",
    );

    let mut invalid_source = clean_source.clone_like_go();
    invalid_source.auto_random_bits = 3;
    expect_temporary_like_refusal(
        &mut store,
        &invalid_source,
        "ck_auto_random",
        1_102_2,
        8006,
        "`auto_random` is unsupported on temporary tables.",
    );

    let mut invalid_source = clean_source.clone_like_go();
    invalid_source.pre_split_regions = 2;
    expect_temporary_like_refusal(
        &mut store,
        &invalid_source,
        "ck_pre_split",
        1_102_3,
        8006,
        "`pre split regions` is unsupported on temporary tables.",
    );

    let mut invalid_source = clean_source.clone_like_go();
    invalid_source.partition = Some(GoShared::new(tidb_model::PartitionInfo::default()));
    expect_temporary_like_refusal(
        &mut store,
        &invalid_source,
        "ck_partitioned",
        1_102_4,
        1562,
        "Cannot create temporary table with partitions",
    );

    let mut invalid_source = clean_source.clone_like_go();
    invalid_source.shard_row_id_bits = 4;
    expect_temporary_like_refusal(
        &mut store,
        &invalid_source,
        "ck_sharded",
        1_102_5,
        8006,
        "`shard_row_id_bits` is unsupported on temporary tables.",
    );

    let mut invalid_source = clean_source.clone_like_go();
    invalid_source.placement_policy_ref = Some(GoShared::new(tidb_model::PolicyRefInfo::default()));
    expect_temporary_like_refusal(
        &mut store,
        &invalid_source,
        "ck_placed",
        1_102_6,
        8006,
        "`placement` is unsupported on temporary tables.",
    );
    store.put(
        source_key.clone(),
        value::serialize_table_info(&clean_source).expect("the clean source encodes"),
    );

    let create_like = plan_ddl(
        &mut store,
        &lower("CREATE TABLE ck_copy LIKE ck_dep"),
        1_103,
    )
    .expect("CREATE LIKE plans");
    let DdlPlan::Write(create_like) = create_like else {
        panic!("CREATE LIKE writes metadata")
    };
    let copy_id = create_like
        .created_id
        .expect("CREATE LIKE allocates a table id");
    let copied: tidb_model::TableInfo =
        serde_json::from_slice(stored_value(&create_like, &key::table_kv_key(112, copy_id)))
            .expect("copied table decodes");
    let replica = copied
        .tiflash_replica
        .as_ref()
        .expect("ordinary CREATE LIKE preserves the TiFlash setting")
        .read();
    assert_eq!(replica.count, 2);
    assert_eq!(
        replica.location_labels.iter().cloned().collect::<Vec<_>>(),
        vec!["zone".to_owned()]
    );
    assert!(!replica.available);
    assert!(replica.available_partition_ids.is_empty());
    assert!(
        !replica.available_partition_ids.is_allocated(),
        "Go clears AvailablePartitionIDs to nil rather than an allocated empty slice"
    );
    assert!(copied.ttl_info.is_some());
    assert!(copied.affinity.is_some());
    assert_eq!(copied.temp_table_type, tidb_model::TempTableType::NONE);
    assert_eq!(copied.auto_rand_id, 77, "Go resets only AutoIncID");
    assert_eq!(
        copied.max_foreign_key_id, 9,
        "Go clears ForeignKeys without rewinding MaxForeignKeyID"
    );
    assert_eq!(copied.max_constraint_id, 2);
    let copied_constraints = copied
        .constraints
        .iter_deref()
        .map(|constraint| {
            let constraint = constraint.read();
            (
                constraint.id,
                constraint.name.original().to_owned(),
                constraint.table.original().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        copied_constraints,
        vec![
            (1, "ck_copy_chk_1".to_owned(), "ck_copy".to_owned()),
            (2, "ck_copy_chk_2".to_owned(), "ck_copy".to_owned()),
        ]
    );

    let temporary_like = plan_ddl(
        &mut store,
        &lower("CREATE GLOBAL TEMPORARY TABLE ck_temp LIKE ck_dep ON COMMIT DELETE ROWS"),
        1_104,
    )
    .expect("temporary CREATE LIKE plans");
    let DdlPlan::Write(temporary_like) = temporary_like else {
        panic!("temporary CREATE LIKE writes metadata")
    };
    let temporary_id = temporary_like
        .created_id
        .expect("temporary CREATE LIKE allocates a table id");
    let temporary: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &temporary_like,
        &key::table_kv_key(112, temporary_id),
    ))
    .expect("temporary copied table decodes");
    assert_eq!(temporary.temp_table_type, tidb_model::TempTableType::GLOBAL);
    assert!(temporary.tiflash_replica.is_none());
    assert!(temporary.ttl_info.is_none());
    assert!(temporary.affinity.is_none());
    assert_eq!(temporary.auto_rand_id, 77);
    assert_eq!(temporary.max_foreign_key_id, 9);

    let preserve = plan_ddl(
        &mut store,
        &lower(
            "CREATE GLOBAL TEMPORARY TABLE ck_preserve LIKE ck_dep \
             ON COMMIT PRESERVE ROWS",
        ),
        1_105,
    )
    .expect_err("Go refuses ON COMMIT PRESERVE ROWS after validating the LIKE source");
    let DdlPlanError::Admission(preserve) = preserve else {
        panic!("expected a coded DDL refusal, got {preserve:?}")
    };
    assert_eq!(preserve.code, 8200);
    assert_eq!(
        preserve.reason,
        "TiDB doesn't support ON COMMIT PRESERVE ROWS for now"
    );

    let drop_pair = lower("ALTER TABLE ck_dep DROP CONSTRAINT c_pair");
    let submission = plan_check_constraint_job_submission(&mut store, &drop_pair, 1_106)
        .expect("DROP CHECK submission plans")
        .expect("DROP CHECK uses the Go job table");
    let drop_job_id = submission.job.id;
    apply_mutations(&mut store, &submission.mutations);
    let write_only = plan_worker_step(&mut store, drop_job_id, 1_107)
        .expect("DROP CHECK publishes WriteOnly");
    assert!(!write_only.terminal);
    apply(&mut store, &write_only.write);
    let removed = plan_worker_step(&mut store, drop_job_id, 1_108)
        .expect("DROP CHECK removes the constraint");
    assert!(!removed.terminal);
    apply(&mut store, &removed.write);
    finish_worker_job(&mut store, drop_job_id, 1_109);

    let drop_single = plan_ddl(
        &mut store,
        &lower("ALTER TABLE ck_dep DROP COLUMN b"),
        1_109,
    )
    .expect("a single-column CHECK does not block DROP COLUMN");
    let DdlPlan::Write(drop_single) = drop_single else {
        panic!("DROP COLUMN writes metadata")
    };
    let dropped: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &drop_single,
        &key::table_kv_key(112, source_id),
    ))
    .expect("post-DROP table decodes");
    assert!(dropped.constraints.is_empty());
    assert_eq!(
        dropped.max_constraint_id, 2,
        "lazy removal does not rewind Go's constraint allocator"
    );
}

#[test]
fn inline_add_column_check_matches_gos_discard_and_off_warning() {
    let mut store = bootstrapped();
    let enabled = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let create = tidb_parser::parse("CREATE TABLE ck_inline (a INT)").expect("CREATE parses");
    let create = lower_ddl_with_context(&create, "u6", &enabled)
        .expect("CREATE is admitted")
        .expect("CREATE owns a catalog route");
    let create = plan_ddl(&mut store, &create, 1_200).expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    let table_id = create.created_id.expect("CREATE allocates a table id");
    apply(&mut store, &create);

    let add = tidb_parser::parse(
        "ALTER TABLE ck_inline ADD COLUMN b INT CONSTRAINT c_inline CHECK (b > 0)",
    )
    .expect("ADD COLUMN parses");
    let add = lower_ddl_with_context(&add, "u6", &enabled)
        .expect("Go admits an inline ADD COLUMN CHECK")
        .expect("ADD COLUMN owns a catalog route");
    let add = plan_ddl(&mut store, &add, 1_201).expect("ADD COLUMN plans");
    let DdlPlan::Write(add) = add else {
        panic!("ADD COLUMN writes metadata")
    };
    let added: tidb_model::TableInfo =
        serde_json::from_slice(stored_value(&add, &key::table_kv_key(112, table_id)))
            .expect("post-ADD table decodes");
    assert_eq!(added.columns.len(), 2);
    assert!(added.constraints.is_empty());
    assert_eq!(enabled.warning_count(), 0);

    let off = tidb_executor::StmtContext::for_query();
    let add_off =
        tidb_parser::parse("ALTER TABLE ck_inline ADD COLUMN c INT CONSTRAINT c_off CHECK (c > 0)")
            .expect("second ADD COLUMN parses");
    let add_off = lower_ddl_with_context(&add_off, "u6", &off)
        .expect("Go admits the OFF form")
        .expect("ADD COLUMN owns a catalog route");
    let _ = plan_ddl(&mut store, &add_off, 1_202).expect("OFF ADD COLUMN plans");
    assert_eq!(off.warning_count(), 1);
    assert_eq!(
        off.take_warnings()[0].2,
        "tidb_enable_check_constraint is off"
    );
}

#[test]
fn grouped_add_columns_admits_check_against_original_schema_then_refuses_job() {
    let mut store = bootstrapped();
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let lower = |sql: &str, context: &tidb_executor::StmtContext| {
        let parsed = tidb_parser::parse(sql).expect("grouped ADD parses");
        lower_ddl_with_context(&parsed, "u6", context)
            .expect("grouped ADD is admitted")
            .expect("grouped ADD owns a catalog route")
    };
    let create = lower("CREATE TABLE ck_grouped (a INT)", &context);
    let create = plan_ddl(&mut store, &create, 1_300).expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    let table_id = create.created_id.expect("CREATE allocates a table id");
    apply(&mut store, &create);

    let grouped = lower(
        "ALTER TABLE ck_grouped ADD COLUMN \
         (b INT, CONSTRAINT c_grouped CHECK (b > 0))",
        &context,
    );
    let DdlStatement::MultiSchemaChange { actions, .. } = &grouped else {
        panic!("Go expands grouped ADD into one multi-schema job")
    };
    assert!(matches!(actions[0], AlterColumnAction::Add { .. }));
    assert!(matches!(actions[1], AlterColumnAction::AddCheck { .. }));
    // Go CreateCheckConstraint resolves the original table, then
    // fillMultiSchemaInfo refuses supported expressions as unsupported jobs.
    let error = plan_ddl(&mut store, &grouped, 1_301).unwrap_err();
    let DdlPlanError::Admission(error) = error else {
        panic!("{error:?}")
    };
    assert_eq!(error.code, 1054);
    let grouped = lower(
        "ALTER TABLE ck_grouped ADD COLUMN (b INT, CONSTRAINT c_grouped CHECK(a > 0))",
        &context,
    );
    let error = plan_ddl(&mut store, &grouped, 1_301).unwrap_err();
    let DdlPlanError::Admission(error) = error else {
        panic!("{error:?}")
    };
    assert_eq!(error.code, 8200);
    assert_eq!(
        error.reason,
        "Unsupported multi schema change for add check constraint"
    );
    let unchanged: tidb_model::TableInfo =
        serde_json::from_slice(store.pairs.get(&key::table_kv_key(112, table_id)).unwrap())
            .unwrap();
    assert_eq!(unchanged.columns.len(), 1);
    assert_eq!(unchanged.constraints.len(), 0);

    let off = tidb_executor::StmtContext::for_query();
    let grouped_off = lower(
        "ALTER TABLE ck_grouped ADD COLUMN \
         (c INT, CONSTRAINT c_off CHECK (c > 0))",
        &off,
    );
    let DdlStatement::MultiSchemaChange { actions, .. } = &grouped_off else {
        panic!("the surviving ADD COLUMN still owns a multi-schema job")
    };
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], AlterColumnAction::Add { .. }));
    assert_eq!(off.warning_count(), 1);
}

/// Go `onAddColumn`'s write-reorganization step: the column is appended,
/// then `MoveColumnInfo` puts it where `FIRST`/`AFTER` asked -- renumbering
/// every offset it passed and re-pointing every INDEX column that addressed
/// one of them (`meta/model/table.go:434`).
///
/// The stored rows are untouched: a row's values are keyed by column id,
/// not by position, so what moves is only the descriptor readers resolve
/// names through. The column ID therefore stays at its allocation order
/// while the OFFSET follows the request.
#[test]
fn add_column_first_and_after_move_offsets_and_repoint_indexes() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT, KEY kb(b))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    // AFTER lands between `a` and `b`, so `b` shifts right by one and the
    // index on `b` must follow it.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN mid BIGINT AFTER a",
        200,
    );
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["id", "a", "mid", "b"]);
    for (position, column) in columns.iter().enumerate() {
        assert_eq!(column["offset"], position, "offsets are renumbered");
    }
    // The id keeps its allocation order even though the offset moved.
    assert_eq!(columns[2]["id"], 4, "mid was allocated after id/a/b");
    let index_column = &stored["index_info"][0]["idx_cols"][0];
    assert_eq!(index_column["name"]["O"], "b");
    assert_eq!(
        index_column["offset"], 3,
        "the index follows the column it names"
    );

    // FIRST pushes everything right by one, index included.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN head BIGINT FIRST",
        300,
    );
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["head", "id", "a", "mid", "b"]);
    for (position, column) in columns.iter().enumerate() {
        assert_eq!(column["offset"], position);
    }
    assert_eq!(
        stored["index_info"][0]["idx_cols"][0]["offset"], 4,
        "the index follows again"
    );

    // Go `LocateOffsetToMove`'s AFTER arm answers ErrColumnNotExists (1054)
    // for a column that is not there.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN late BIGINT AFTER nosuch"),
        400,
    )
    .expect_err("AFTER an unknown column is refused")
    .to_string();
    assert!(error.contains("Unknown column 'nosuch'"), "{error}");
}

/// Go `modify_column.go:704`: a MODIFY/CHANGE may also MOVE the column,
/// and the destination is located against the column's CURRENT offset --
/// unlike ADD COLUMN, which appends first and locates against that.
///
/// `MODIFY b AFTER b` names the column as its own anchor, which Go answers
/// as `ErrColumnNotExists` on that column rather than as a no-op
/// (`modify_column.go:700`).
#[test]
fn modify_column_moves_the_column_and_refuses_a_self_anchor() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT, KEY kb(b))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let write = plan(&mut store, "ALTER TABLE u6.t MODIFY b BIGINT FIRST", 200);
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["b", "id", "a"]);
    for (position, column) in columns.iter().enumerate() {
        assert_eq!(column["offset"], position);
    }
    assert_eq!(
        stored["index_info"][0]["idx_cols"][0]["offset"], 0,
        "the index follows the column it names"
    );

    // A CHANGE renames and moves in one statement.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t CHANGE a a2 BIGINT AFTER b",
        300,
    );
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let names: Vec<&str> = stored["cols"]
        .as_array()
        .expect("columns array")
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["b", "a2", "id"]);

    // Go's self-anchor rule, and its 1054 code.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t MODIFY b BIGINT AFTER b"),
        400,
    )
    .expect_err("a self-anchored MODIFY is refused");
    assert!(
        matches!(error, DdlPlanError::UnknownColumn { ref column, .. } if column == "b"),
        "{error:?}"
    );
    assert!(error.to_string().contains("Unknown column 'b'"), "{error}");
}

/// Go `onAlterIndexVisibility` (`ddl/index.go:720`): `ALTER TABLE ...
/// ALTER INDEX <i> VISIBLE|INVISIBLE` is metadata only -- the index is
/// still maintained by writes, it is only hidden from the optimizer.
///
/// Three of Go's rules ride with it: the index must exist AND be public,
/// else `ErrKeyNotExists` (1176, not DROP INDEX's 1091); a visibility that
/// already matches is an early return that spends no schema version; and
/// `setIndexVisibility` walks EVERY index of the matching name rather than
/// stopping at the first.
#[test]
fn alter_index_visibility_toggles_and_refuses_a_missing_index() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, KEY ia(a))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let invisible_of = |write: &tidb_exec::cluster_ddl::DdlWrite| {
        let stored = stored_table(write, table_id);
        stored["index_info"]
            .as_array()
            .expect("index array")
            .iter()
            .find(|index| index["idx_name"]["O"] == "ia")
            .expect("the index is there")["is_invisible"]
            .clone()
    };

    let write = plan(&mut store, "ALTER TABLE u6.t ALTER INDEX ia INVISIBLE", 200);
    apply(&mut store, &write);
    assert_eq!(invisible_of(&write), serde_json::json!(true));

    let write = plan(&mut store, "ALTER TABLE u6.t ALTER INDEX ia VISIBLE", 300);
    apply(&mut store, &write);
    assert_eq!(invisible_of(&write), serde_json::json!(false));

    // Go's early return: already visible, so the job finishes without
    // touching the table and no schema version is spent.
    match plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ALTER INDEX ia VISIBLE"),
        400,
    )
    .expect("an already-satisfied visibility plans")
    {
        DdlPlan::AlreadySatisfied { detail, .. } => {
            assert!(detail.contains("already visible"), "{detail}");
        }
        DdlPlan::Write(_) => panic!("a no-op visibility must publish nothing"),
    }

    // A missing index is Go's ErrKeyNotExists, not DROP INDEX's 1091.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ALTER INDEX nosuch INVISIBLE"),
        500,
    )
    .expect_err("a missing index is refused");
    assert!(
        matches!(error, DdlPlanError::KeyNotExists { ref index, .. } if index == "nosuch"),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("Key 'nosuch' doesn't exist in table 't'"),
        "{error}"
    );
}

/// `PARTITION BY` reaches the stored `TableInfo` rather than being refused,
/// and it carries Go's own stored shape: the restored expression, the
/// definitions in written order, and the `Enable` flag that makes
/// `GetPartitionInfo` return it at all.
#[test]
fn cluster_create_persists_a_partition_clause() {
    let DdlStatement::CreateTable { build, .. } =
        statement("CREATE TABLE u6.t (id BIGINT PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 2")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    let table = build.template();
    let partition = table
        .partition
        .as_ref()
        .expect("the clause reached the stored table")
        .read();
    assert_eq!(partition.partition_type, tidb_ast::PartitionType::HASH);
    assert!(
        partition.enable,
        "Go's GetPartitionInfo returns nil for metadata that is not enabled, \
         so a table stored with Enable false is not partitioned at all"
    );
    assert_eq!(partition.expr, "`id`");
    assert_eq!(partition.num, 2);
    let definitions = partition.definitions.snapshot();
    assert_eq!(
        definitions
            .iter()
            .map(|definition| definition.name.original().to_owned())
            .collect::<Vec<_>>(),
        vec!["p0".to_owned(), "p1".to_owned()]
    );
    // The builder leaves the physical ids for the writer: Go allocates them
    // at job submission, one per definition after the table's own.
    assert!(definitions.iter().all(|definition| definition.id == 0));
}

/// A RANGE clause stores its bounds as the TEXT Go stores, with `MAXVALUE`
/// kept as that literal word rather than folded into a number.
#[test]
fn cluster_create_persists_range_bounds_as_go_spells_them() {
    let DdlStatement::CreateTable { build, .. } = statement(
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY) PARTITION BY RANGE (id) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    ) else {
        panic!("the fixture is CREATE TABLE");
    };
    let table = build.template();
    let partition = table
        .partition
        .as_ref()
        .expect("the clause reached the stored table")
        .read();
    let bounds = partition
        .definitions
        .snapshot()
        .iter()
        .map(|definition| definition.less_than.snapshot())
        .collect::<Vec<_>>();
    assert_eq!(
        bounds,
        vec![vec!["10".to_owned()], vec!["MAXVALUE".to_owned()]]
    );
}

#[test]
fn cluster_create_persists_a_partition_comment() {
    // Go `buildPartitionDefinitionsInfo` stores the validated comment on the
    // definition (`ddl/partition.go:1576` for LIST, `:1670` for RANGE), and
    // `AppendPartitionDefs` prints it back from there. The comment was being
    // length-checked at CREATE and then dropped before it reached the stored
    // table, so a cluster round trip lost it -- invisible to the in-process
    // `SHOW CREATE TABLE`, which reads the routing spec rather than this.
    let DdlStatement::CreateTable { build, .. } = statement(
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY) PARTITION BY RANGE (id) \
         (PARTITION p0 VALUES LESS THAN (10) COMMENT 'first', \
          PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    ) else {
        panic!("the fixture is CREATE TABLE");
    };
    let table = build.template();
    let partition = table
        .partition
        .as_ref()
        .expect("the clause reached the stored table")
        .read();
    let comments = partition
        .definitions
        .snapshot()
        .iter()
        .map(|definition| definition.comment.clone())
        .collect::<Vec<_>>();
    assert_eq!(comments, vec!["first".to_owned(), String::new()]);
}

#[test]
fn cluster_create_table_resolves_a_placement_policy_by_id() {
    // Go `CreateTableWithInfo` resolves a table's `PLACEMENT POLICY = name`
    // against the infoschema and records a reference carrying the policy's
    // ID as well as its name. The id is the load-bearing half: placement
    // bundles resolve by id, so a name-only reference would describe
    // placement that never reaches the scheduler.
    //
    // The resolution happens at PLANNING time, not lowering, because the
    // lookup needs the same snapshot the rest of the statement plans
    // against.
    let DdlStatement::CreateTable { build, .. } =
        statement("CREATE TABLE u6.t (a INT) PLACEMENT POLICY = pol")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    // Lowering must NOT refuse it -- the option reaches the planner intact.
    assert!(
        build.template().placement_policy_ref.is_none(),
        "the reference is stamped by the planner, not the lowering step"
    );
}

#[test]
fn cluster_truncate_reassigns_partition_ids() {
    // Go `onTruncateTable` reassigns the PARTITION ids as well as the table's
    // (`ddl/table.go:510`), and its comment gives the reason: "all the old data
    // is encoded with the old partition ID, it can not be accessed anymore".
    //
    // A partitioned table's rows are keyed by the PARTITION's physical id, not
    // the table's. A truncate that changed only the table id would leave every
    // row exactly where it was and still addressable -- reporting success and
    // emptying nothing.
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.pt (a INT) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
        7,
    );
    apply(&mut store, &created);

    let before = partition_ids(&mut store, "pt");
    assert_eq!(before.len(), 2, "the fixture has two partitions");

    let truncated = plan(&mut store, "TRUNCATE TABLE u6.pt", 8);
    apply(&mut store, &truncated);

    let after = partition_ids(&mut store, "pt");
    assert_eq!(after.len(), 2, "the partition count is unchanged");
    for (old, new) in before.iter().zip(after.iter()) {
        assert_ne!(
            old, new,
            "every partition must take a NEW id, or its rows survive the truncate"
        );
    }
}

#[test]
fn cluster_truncate_partition_reassigns_only_selected_physical_ids() {
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.pt (a INT) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), \
          PARTITION p1 VALUES LESS THAN (20), \
          PARTITION p2 VALUES LESS THAN (30))",
        7,
    );
    apply(&mut store, &created);
    let before = partition_ids(&mut store, "pt");

    let truncated = plan(&mut store, "ALTER TABLE u6.pt TRUNCATE PARTITION p0, p2", 8);
    assert_eq!(
        truncated.diff.action_type,
        tidb_model::ActionType::ACTION_TRUNCATE_TABLE_PARTITION
    );
    apply(&mut store, &truncated);
    let after = partition_ids(&mut store, "pt");
    assert_ne!(after[0], before[0]);
    assert_eq!(after[1], before[1]);
    assert_ne!(after[2], before[2]);
}

/// The physical ids of one table's partitions, in definition order.
fn partition_ids(store: &mut MetaStore, table: &str) -> Vec<i64> {
    let catalog =
        tidb_exec::cluster_catalog::load_cluster_catalog(store).expect("the catalog loads");
    let (_, info) = catalog
        .find_table("u6", table)
        .unwrap_or_else(|| panic!("table {table} exists"));
    let partition = info.partition.as_ref().expect("a partitioned table");
    let definitions = partition.read().definitions.snapshot();
    definitions.iter().map(|definition| definition.id).collect()
}

/// Go `BuildTableInfoWithLike` (master `94a9cbedab`): a LIKE copy clears the
/// materialized-view metadata — a copy is never a view, log or base table of
/// one — while the source keeps its own.
#[test]
fn create_table_like_clears_materialized_view_metadata() {
    let context = tidb_executor::StmtContext::for_query();
    let lower = |sql: &str| {
        let parsed = tidb_parser::parse(sql).expect("LIKE DDL parses");
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect("LIKE DDL is admitted")
            .expect("LIKE DDL owns a catalog route")
    };
    let mut store = bootstrapped();

    let create = plan_ddl(
        &mut store,
        &lower("CREATE TABLE mv_base (id INT PRIMARY KEY AUTO_INCREMENT, k INT)"),
        1_200,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);
    let source_id = create.created_id.expect("CREATE allocated a table id");
    let source_key = key::table_kv_key(112, source_id);
    let mut source: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&source_key)
            .expect("the committed source table exists"),
    )
    .expect("the source table decodes");
    let mut purge_zone = tidb_model::TimeZoneLocation::default();
    purge_zone.name = "UTC".into();
    source.materialized_view_log = Some(GoShared::new(tidb_model::MaterializedViewLogInfo {
        base_table_id: source_id,
        columns: vec![tidb_ast::CiString::new("id")].into(),
        definition_sql_mode: 0,
        purge_schedule_time_zone: purge_zone,
        ..Default::default()
    }));
    store.put(
        source_key.clone(),
        value::serialize_table_info(&source).expect("the augmented source encodes"),
    );

    let like = plan_ddl(
        &mut store,
        &lower("CREATE TABLE mv_copy LIKE mv_base"),
        1_201,
    )
    .expect("LIKE plans");
    let DdlPlan::Write(like) = like else {
        panic!("LIKE writes metadata")
    };
    apply(&mut store, &like);
    let copy_id = like.created_id.expect("LIKE allocated a table id");
    let copy: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&key::table_kv_key(112, copy_id))
            .expect("the copy exists"),
    )
    .expect("the copy decodes");
    assert!(
        copy.materialized_view_log.is_none(),
        "Go clears the log metadata"
    );
    assert!(copy.materialized_view.is_none());
    assert!(copy.materialized_view_base.is_none());

    let reloaded_source: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&source_key)
            .expect("the source still exists"),
    )
    .expect("the source decodes");
    assert!(
        reloaded_source.materialized_view_log.is_some(),
        "the source keeps its own metadata"
    );
}

/// Go `CreateMaterializedView`/`CreateMaterializedViewLog` (master
/// `94a9cbedab`): the admission checks run in source order and carry Go's
/// exact refusals; a valid statement submits a queueing job whose typed
/// arguments carry the derived view table (its initial-build worker is the
/// still-unwired reorg batch).
#[test]
fn materialized_view_lowering_follows_go_admission_order() {
    use tidb_executor::StmtContext;
    let disabled = StmtContext::for_query();
    // The session tier installs the live MV-execution variable image on the
    // DDL context (tidb-session `m_view_execution_session_vars_image`);
    // this two-key image stands in for it and proves the envelope records
    // the LIVE values rather than the defaults.
    let mut enabled = StmtContext::for_query().with_enable_mview(true);
    enabled.set_session_vars_image(
        [
            (
                "tidb_mview_maintain_import_threads".to_owned(),
                "7".to_owned(),
            ),
            ("tidb_max_tiflash_threads".to_owned(), "16".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let try_lower = |context: &StmtContext, sql: &str, schema: &str| {
        let parsed = tidb_parser::parse(sql)
            .unwrap_or_else(|error| panic!("MV DDL does not parse ({sql}): {error:?}"));
        lower_ddl_with_context(&parsed, schema, context)
    };
    let lower_with = |context: &StmtContext, sql: &str, schema: &str| {
        try_lower(context, sql, schema)
            .expect("MV DDL admission outcome")
            .expect("MV DDL lowers to a statement")
    };
    let mut store = bootstrapped();
    // View statements route through the durable-job submission planner, the
    // same way Go's `DoDDLJobWrapper` owns them.
    let submit = |store: &mut MetaStore, sql: &str| {
        prepare_materialized_view_job_submission(
            store,
            &lower_with(&enabled, sql, "u6"),
            1_311,
            false,
            0,
        )
    };

    // Go `checkMaterializedViewEnabled` fires before everything else.
    let error = try_lower(
        &disabled,
        "CREATE MATERIALIZED VIEW mv (id, k) AS (SELECT id, k FROM mv_base GROUP BY id, k)",
        "u6",
    )
    .expect_err("the disabled flag refuses");
    assert_eq!(error.code, 8200);
    assert_eq!(
        error.reason,
        "Unsupported Materialized View is disabled, please set `tidb_mview_enable` to `ON` to enable it"
    );

    // Go `plannererrors.ErrNoDB` when no default schema exists.
    let error = try_lower(
        &enabled,
        "CREATE MATERIALIZED VIEW mv (id, k) AS (SELECT id, k FROM mv_base GROUP BY id, k)",
        "",
    )
    .expect_err("no database refuses");
    assert_eq!(error.code, tidb_error::mysql::errcode::ErrNoDB);

    // A real base table (no schedule, plain columns) for the plan phase.
    let create = plan_ddl(
        &mut store,
        &lower_with(
            &enabled,
            "CREATE TABLE mv_base (id INT PRIMARY KEY AUTO_INCREMENT, k INT)",
            "u6",
        ),
        1_300,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);
    let base_id = create.created_id.expect("base table id");

    // Go: the schema must exist at planning.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW nowhere.mv (id, k) AS (SELECT id, k FROM mv_base GROUP BY id, k)",
    )
    .expect_err("unknown schema refuses");
    assert!(matches!(error, DdlPlanError::UnknownDatabase(ref db) if db == "nowhere"));

    // Go `validateCommentLength`: the 1024-byte comment cap.
    let long_comment_sql = format!(
        "CREATE MATERIALIZED VIEW mv (id, k) COMMENT '{}' AS SELECT id, k FROM mv_base GROUP BY id, k",
        "x".repeat(1025)
    );
    let error = submit(&mut store, &long_comment_sql).expect_err("over-long comment refuses");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 8020);
    assert_eq!(
        admission.reason,
        "Comment for table 'mv' is too long (max = 1024)"
    );

    // Go: only a plain SELECT is accepted.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (c) AS (SELECT 1 UNION SELECT 2)",
    )
    .expect_err("set operations refuse");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 8200);
    assert_eq!(
        admission.reason,
        "Unsupported CREATE MATERIALIZED VIEW only supports SELECT statement"
    );

    // Go `extractSingleTableNameFromSelect`: comma joins refuse.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (a) AS (SELECT * FROM a, b GROUP BY a)",
    )
    .expect_err("multi-table refuses");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 8200);
    assert_eq!(
        admission.reason,
        "Unsupported CREATE MATERIALIZED VIEW only supports a single base table"
    );

    // Go: the base table must live in the same schema.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (id) AS (SELECT * FROM other.mv_base GROUP BY id)",
    )
    .expect_err("cross-schema base refuses");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 8200);
    assert_eq!(
        admission.reason,
        "Unsupported CREATE MATERIALIZED VIEW only supports base table in the same schema"
    );

    // Go: the base table must exist.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (id) AS (SELECT * FROM no_base GROUP BY id)",
    )
    .expect_err("missing base refuses");
    assert!(matches!(
        error,
        DdlPlanError::TableNotExists { ref table, .. } if table == "no_base"
    ));

    // Go: the `$mlog$` physical table must exist for the base.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (id, k) AS (SELECT id, k FROM mv_base GROUP BY id, k)",
    )
    .expect_err("missing mlog refuses");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(
        admission.reason,
        "materialized view log does not exist for base table u6.mv_base"
    );

    // The mlog exists: inject its TableInfo pointing at the base.
    let mut mlog = tidb_model::TableInfo::default();
    mlog.id = 9_001;
    mlog.name = tidb_ast::CiString::new("$mlog$mv_base");
    let mut mlog_meta = tidb_model::MaterializedViewLogInfo::default();
    mlog_meta.base_table_id = base_id;
    mlog_meta.columns = vec![tidb_ast::CiString::new("id"), tidb_ast::CiString::new("k")].into();
    mlog.materialized_view_log = Some(GoShared::new(mlog_meta));
    store.put(
        key::table_kv_key(112, 9_001),
        value::serialize_table_info(&mlog).expect("the mlog encodes"),
    );

    // Go `validateCreateMaterializedViewQuery`: GROUP BY is required.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (id, k) AS (SELECT id, k FROM mv_base)",
    )
    .expect_err("GROUP BY is required");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 8200);
    assert_eq!(
        admission.reason,
        "Unsupported CREATE MATERIALIZED VIEW requires GROUP BY clause"
    );

    // Go: WITH ROLLUP refuses.
    let error = submit(&mut store, "CREATE MATERIALIZED VIEW mv (id, cnt) AS (SELECT id, COUNT(k) FROM mv_base GROUP BY id WITH ROLLUP)")
        .expect_err("ROLLUP refuses");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 8200);
    assert_eq!(
        admission.reason,
        "Unsupported CREATE MATERIALIZED VIEW does not support GROUP BY WITH ROLLUP"
    );

    // Go's `mviewutil.CheckMaterializedViewSelect` clauses flow through with
    // their own messages: a locking clause is refused.
    let error = submit(&mut store, "CREATE MATERIALIZED VIEW mv (id, c) AS (SELECT id, COUNT(k) FROM mv_base GROUP BY id FOR UPDATE)")
        .expect_err("locking clauses refuse");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(
        admission.reason,
        "Unsupported CREATE MATERIALIZED VIEW does not support locking clauses"
    );

    // A declared column list that disagrees with the query output refuses.
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (a, b, c) AS (SELECT id, COUNT(1) FROM mv_base GROUP BY id)",
    )
    .expect_err("the column count is checked against the query output");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 1105);
    assert_eq!(
        admission.reason,
        "materialized view column count 3 does not match query output 2"
    );

    // The valid statement submits a queueing view job whose typed arguments
    // carry the derived view table: flag-stripped result columns, the
    // one-row-per-group PRIMARY KEY (every group key is NOT NULL), and the
    // MaterializedViewInfo metadata pointing back at the base.
    let mut spec = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW mv (id, c) AS (SELECT id, COUNT(1) FROM mv_base GROUP BY id)",
    )
    .expect("Go submission preflight succeeds")
    .expect("the view create owns a job spec");
    assert_eq!(spec.job.type_, ActionType::ACTION_CREATE_MATERIALIZED_VIEW);
    assert_eq!(spec.job.state, JobState::QUEUEING);
    assert_eq!(spec.job.table_name.to_utf8_lossy_go(), "mv");
    assert_eq!(spec.job.involving_schema_info.len(), 3);
    assert!(
        !spec.id_allocated,
        "the view table ID is assigned at insert"
    );
    assert!(spec.job.may_need_reorg(), "the initial build is reorg DDL");
    // Go `initMaterializedViewReorgMetaFromVariables` + the twelve
    // MV-execution session vars: the reorg metadata and the maintenance
    // variable snapshot ride the job envelope.
    let reorg = spec
        .job
        .reorg_meta
        .as_ref()
        .expect("the reorg metadata rides the job")
        .read();
    assert_eq!(
        reorg.get_concurrency(tidb_model::reorg::DDLReorgProcessDefaults::new(|| 0, || 0)),
        4,
        "Go `SetConcurrency(DefTiDBDDLReorgWorkerCount)`"
    );
    assert_eq!(
        reorg.get_batch_size(tidb_model::reorg::DDLReorgProcessDefaults::new(|| 0, || 0)),
        256,
        "Go `SetBatchSize(DefTiDBDDLReorgBatchSize)`"
    );
    assert_eq!(reorg.sql_mode, u64::try_from(spec.job.sql_mode).unwrap());
    assert_eq!(
        reorg
            .location
            .as_ref()
            .expect("the zone is recorded")
            .read()
            .name
            .to_utf8_lossy_go(),
        "UTC"
    );
    let job_vars = spec
        .job
        .session_vars
        .as_ref()
        .expect("the envelope carries the session vars")
        .read()
        .clone();
    drop(reorg);
    assert_eq!(
        job_vars.len(),
        3,
        "scatter region + the two image-supplied MV vars (the session tier always supplies the full twelve)"
    );
    assert_eq!(
        job_vars
            .get("tidb_mview_maintain_import_threads")
            .map(|v| v.to_utf8_lossy_go()),
        Some("7".into()),
        "the image's live value rides the envelope"
    );
    assert_eq!(
        job_vars
            .get("tidb_max_tiflash_threads")
            .map(|v| v.to_utf8_lossy_go()),
        Some("16".into())
    );
    assert_eq!(
        job_vars
            .get("tidb_scatter_region")
            .map(|v| v.to_utf8_lossy_go()),
        Some(String::new())
    );

    let JobArgsValue::CreateMaterializedView(Some(view_args)) = &spec.args else {
        panic!("the spec carries CreateMaterializedViewArgs")
    };
    let table_shared = view_args.read().table_info.get().expect("nil TableInfo");
    // The RwLock guards must drop before the insertion attempt, whose GID
    // assignment writes the same shared TableInfo.
    {
        let view = table_shared.read();
        assert_eq!(view.name.original(), "mv");
        assert_eq!(
            view.materialized_view
                .as_ref()
                .expect("the view metadata is set")
                .read()
                .base_table_ids
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![base_id]
        );
        assert_eq!(
            view.materialized_view
                .as_ref()
                .unwrap()
                .read()
                .refresh_method,
            "FAST"
        );
        let handles: Vec<_> = view.columns.iter_handles().into_iter().flatten().collect();
        let columns: Vec<_> = handles.iter().map(|column| column.read()).collect();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name.original(), "id");
        assert_eq!(
            columns[0].field_type.code(),
            tidb_datatype::FieldTypeCode::Long
        );
        assert_eq!(
            columns[0].field_type.flags() & tidb_datatype::FieldTypeFlags::PRI_KEY,
            0,
            "Go deletes the key flags on the derived column"
        );
        assert_eq!(columns[1].name.original(), "c");
        assert_eq!(
            columns[1].field_type.code(),
            tidb_datatype::FieldTypeCode::LongLong,
            "COUNT derives the Go bigint output type"
        );
        // One-row-per-group constraint: the only group key `id` is NOT NULL, so
        // Go builds a PRIMARY KEY over the declared column.
        assert!(view.indices.iter_deref().any(|index| index.read().primary));
    }

    // The view's own worker (the initial-build reorg phase) is not wired yet;
    // the submitted job stays queued rather than pretending to finish.
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let (mutations, cleanup) = plan_insert_attempt(
        &mut store,
        &catalog,
        std::slice::from_mut(&mut spec),
        &mut |_| Option::<fn()>::None,
    )
    .expect("the insertion attempt plans");
    apply_mutations(&mut store, &mutations);
    drop(cleanup);
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let job_table = DdlJobTable::locate(&catalog).expect("the job table exists");
    let queued = job_table
        .load(&mut store)
        .expect("the queue scans")
        .into_iter()
        .find(|active| active.job.id == spec.job.id)
        .expect("the view job stays queued for its worker batch");
    assert_eq!(
        queued.job.type_,
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW
    );
}

/// Go `CreateMaterializedViewLog` (master `94a9cbedab`): the base-shape
/// refusals, the derived `$mlog$` name collision, `BuildMaterializedViewLogTableInfo`'s
/// own refusals, and the submitted job envelope with its typed arguments.
#[test]
fn materialized_view_log_lowering_follows_go_admission_order() {
    use tidb_executor::StmtContext;
    let enabled = StmtContext::for_query().with_enable_mview(true);
    let lower = |sql: &str, schema: &str| {
        let parsed = tidb_parser::parse(sql).expect("MV LOG DDL parses");
        lower_ddl_with_context(&parsed, schema, &enabled)
            .expect("MV LOG DDL admission outcome")
            .expect("MV LOG DDL lowers to a statement")
    };
    let submit = |store: &mut MetaStore, sql: &str| {
        let statement = lower(sql, "u6");
        prepare_materialized_view_job_submission(store, &statement, 1_401, false, 0)
    };
    let mut store = bootstrapped();

    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE mv_base (id INT PRIMARY KEY AUTO_INCREMENT, k INT)",
            "u6",
        ),
        1_400,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);
    let base_id = create.created_id.expect("base table id");

    // The routing guard: a log create never plans through the ordinary
    // one-write planner -- Go submits it as a durable job.
    let error = plan_ddl(
        &mut store,
        &lower("CREATE MATERIALIZED VIEW LOG ON mv_base (id)", "u6"),
        1_401,
    )
    .expect_err("the one-write planner refuses the job route");
    assert!(matches!(error, DdlPlanError::Encode(ref message)
        if message == "materialized view log DDL must execute through mysql.tidb_ddl_job"));

    // Go: the base table must exist.
    let error = submit(&mut store, "CREATE MATERIALIZED VIEW LOG ON no_base (id)")
        .expect_err("missing base refuses");
    assert!(matches!(
        error,
        DdlPlanError::TableNotExists { ref table, .. } if table == "no_base"
    ));

    // Go: the derived `$mlog$` name collision reports `ErrTableExists`.
    // Inject a physical table occupying the derived name.
    let mut occupier = tidb_model::TableInfo::default();
    occupier.id = 9_002;
    occupier.name = tidb_ast::CiString::new("$mlog$mv_base");
    store.put(
        key::table_kv_key(112, 9_002),
        value::serialize_table_info(&occupier).expect("the occupier encodes"),
    );
    let error = submit(&mut store, "CREATE MATERIALIZED VIEW LOG ON mv_base (id)")
        .expect_err("the derived mlog name already exists");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, tidb_error::tidb::errcode::ErrTableExists);
    assert_eq!(admission.reason, "Table 'u6.$mlog$mv_base' already exists");
    store
        .pairs
        .remove(&key::table_kv_key(112, 9_002))
        .expect("the occupier is removed");

    // `BuildMaterializedViewLogTableInfo`'s refusals, in Go's order.
    for (sql, code, message) in [
        (
            "CREATE MATERIALIZED VIEW LOG ON mv_base (id, id)",
            tidb_error::tidb::errcode::ErrDupFieldName,
            "Duplicate column name 'id'",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON mv_base (id, _MLOG$_DML_TYPE)",
            tidb_error::tidb::errcode::ErrDupFieldName,
            "Duplicate column name '_MLOG$_DML_TYPE'",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON mv_base (no_col)",
            tidb_error::tidb::errcode::ErrBadField,
            "Unknown column 'no_col' in 'mv_base'",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON mv_base (id) PURGE IMMEDIATE",
            1105,
            "PURGE IMMEDIATE is not supported for CREATE MATERIALIZED VIEW LOG",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON mv_base (id) PURGE NEXT 'x'",
            1105,
            "Unsupported PURGE NEXT expression must return DATETIME/TIMESTAMP, but got var_string",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON mv_base (id) ALERT ROWS -3",
            1105,
            "invalid ALERT ROWS value: -3 (must be non-negative)",
        ),
    ] {
        let error = submit(&mut store, sql).expect_err(sql);
        let DdlPlanError::Admission(admission) = error else {
            panic!("expected a coded refusal for {sql}")
        };
        assert_eq!(admission.code, code, "{sql}");
        assert_eq!(admission.reason, message, "{sql}");
    }

    // A JSON base column refuses; the log cannot copy it.
    let json_create = plan_ddl(
        &mut store,
        &lower("CREATE TABLE json_base (id INT PRIMARY KEY, j JSON)", "u6"),
        1_406,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(json_create) = json_create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &json_create);
    let error = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW LOG ON json_base (id, j)",
    )
    .expect_err("JSON columns refuse");
    let DdlPlanError::Admission(admission) = error else {
        panic!("expected a coded refusal")
    };
    assert_eq!(admission.code, 1105);
    assert_eq!(
        admission.reason,
        "CREATE MATERIALIZED VIEW LOG does not support JSON column j"
    );

    // The valid statement submits a queueing job whose typed arguments carry
    // the built log table: copied base columns with the key/auto-increment
    // flags deleted, the two NOT NULL `_MLOG$_*` physical columns, and the
    // `MaterializedViewLogInfo` metadata pointing back at the base.
    let mut spec = submit(
        &mut store,
        "CREATE MATERIALIZED VIEW LOG ON mv_base (id, k) PURGE NEXT CAST('2027-01-01 00:00:00' AS DATETIME) ALERT ROWS 1000",
    )
    .expect("Go submission preflight succeeds")
    .expect("the log create owns a job spec");
    assert_eq!(
        spec.job.type_,
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
    );
    assert_eq!(spec.job.state, JobState::QUEUEING);
    assert_eq!(spec.job.schema_name.to_utf8_lossy_go(), "u6");
    assert_eq!(spec.job.table_name.to_utf8_lossy_go(), "$mlog$mv_base");
    assert_eq!(spec.job.involving_schema_info.len(), 2);
    assert_eq!(
        spec.job.get_system_var("tidb_scatter_region").as_ref(),
        Some(&"".into()),
        "Go stamps the scatter scope even at its default"
    );
    assert!(!spec.id_allocated, "the table ID is assigned at insertion");
    assert_eq!(spec.job.id, 0, "the job ID is assigned at insertion");

    let JobArgsValue::CreateMaterializedViewLog(Some(log_args)) = &spec.args else {
        panic!("the spec carries CreateMaterializedViewLogArgs")
    };
    let table_shared = log_args.read().table_info.get().expect("nil TableInfo");
    // The built log table, asserted inside a scoped block: the RwLock guards
    // must drop before the insertion attempt, whose GID assignment writes the
    // same shared TableInfo.
    {
        let table = table_shared.read();
        assert_eq!(table.name.original(), "$mlog$mv_base");
        assert_eq!(table.id, 0, "the build leaves the ID to the submission");
        {
            let log = table
                .materialized_view_log
                .as_ref()
                .expect("the log metadata is set")
                .read();
            assert_eq!(log.base_table_id, base_id);
            assert_eq!(log.purge_method, "DEFERRED");
            assert!(
                log.purge_next.contains("2027-01-01"),
                "the NEXT clause restores canonically: {}",
                log.purge_next
            );
            assert_eq!(log.log_accumulation_alert_rows, Some(1000));
            assert_eq!(
                log.columns
                    .iter()
                    .map(|c| c.original().to_owned())
                    .collect::<Vec<_>>(),
                vec!["id".to_owned(), "k".to_owned()],
            );
        }
        let handles: Vec<_> = table.columns.iter_handles().into_iter().flatten().collect();
        let columns: Vec<_> = handles.iter().map(|column| column.read()).collect();
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[0].name.original(), "id");
        assert_eq!(
            columns[0].field_type.code(),
            tidb_datatype::FieldTypeCode::Long
        );
        assert_eq!(
            columns[0].field_type.flags() & tidb_datatype::FieldTypeFlags::PRI_KEY,
            0,
            "Go deletes the key flags on the log copy"
        );
        assert_eq!(
            columns[0].field_type.flags() & tidb_datatype::FieldTypeFlags::AUTO_INCREMENT,
            0,
            "Go deletes the auto-increment flag on the log copy"
        );
        assert_ne!(
            columns[0].field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL,
            0,
            "NOT NULL travels with the copy"
        );
        assert_eq!(columns[1].name.original(), "k");
        assert_eq!(columns[2].name.original(), "_MLOG$_DML_TYPE");
        assert_eq!(
            columns[2].field_type.code(),
            tidb_datatype::FieldTypeCode::Varchar
        );
        assert_eq!(columns[2].field_type.flen(), 1);
        assert_eq!(columns[2].field_type.charset_name(), "utf8mb4");
        assert_ne!(
            columns[2].field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL,
            0,
            "Go sets NOT NULL on the DML-type column"
        );
        assert_eq!(columns[3].name.original(), "_MLOG$_OLD_NEW");
        assert_eq!(
            columns[3].field_type.code(),
            tidb_datatype::FieldTypeCode::Tiny
        );
        assert_eq!(columns[3].field_type.flen(), 4);
    }

    // The insertion attempt assigns the global IDs (job + table) and lands
    // the active job row atomically, exactly as Go's
    // `GenGIDAndInsertJobsWithRetry` commits.
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let (mutations, cleanup) = plan_insert_attempt(
        &mut store,
        &catalog,
        std::slice::from_mut(&mut spec),
        &mut |_| Option::<fn()>::None,
    )
    .expect("the insertion attempt plans");
    apply_mutations(&mut store, &mutations);
    drop(cleanup);

    assert_ne!(spec.job.id, 0, "the job ID is assigned");
    let assigned_table_id = table_shared.read().id;
    assert_eq!(
        spec.job.table_id, assigned_table_id,
        "Job.TableID follows the args"
    );
    assert_ne!(assigned_table_id, 0, "the args' TableInfo carries its ID");
    let job_table = DdlJobTable::locate(&catalog).expect("the job table exists");
    let active = job_table
        .load(&mut store)
        .expect("the active queue scans")
        .into_iter()
        .find(|active| active.job.id == spec.job.id)
        .expect("the submitted job row is active");
    assert_eq!(
        active.job.type_,
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
    );
}

fn seed_step(
    plan: Result<PersistedDdlJobPlan, DdlPlanError>,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    match plan? {
        PersistedDdlJobPlan::Step(step) => Ok(step),
        PersistedDdlJobPlan::Paused => panic!("paused job has no worker transaction"),
        PersistedDdlJobPlan::SchemaSync { .. } => panic!("seed step must acknowledge previous MDL"),
    }
}

fn plan_mview_log_step(
    store: &mut MetaStore,
    job_id: i64,
    ts: u64,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    seed_step(
        tidb_exec::cluster_ddl::plan_persisted_materialized_view_log_job_step(
            store,
            job_id,
            ts,
            DdlJobSchemaState::new(true),
        ),
    )
}

fn plan_mview_step(
    store: &mut MetaStore,
    job_id: i64,
    ts: u64,
    build: Option<tidb_exec::cluster_ddl::MviewBuildOutcome>,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    seed_step(
        tidb_exec::cluster_ddl::plan_persisted_materialized_view_create_job_step(
            store,
            job_id,
            ts,
            build,
            DdlJobSchemaState::new(true),
        ),
    )
}

// Model the worker's schema acknowledgement after committing an action. Each
// seed phase must recover the same durable queue scope on owner replacement.
fn acknowledge_mview_schema(store: &mut MetaStore, step: &PersistedDdlJobStep) {
    assert!(!step.terminal);
    let job_id = step.write.ddl_job_id;
    let catalog = load_cluster_catalog(store).unwrap();
    let queue = DdlJobTable::locate(&catalog).unwrap();
    let active = queue.load_by_id(store, job_id).unwrap().unwrap();
    assert!(active.job.real_start_ts > 0);
    assert!(!store
        .pairs
        .contains_key(&key::ddl_job_history_kv_key(job_id)));
    let mdl = step.write.mdl_info_update.as_ref().unwrap();
    assert_eq!(mdl.table_ids, active.table_ids);
    let mut mutations = Vec::new();
    mdl.append_mutations(job_id, step.write.schema_version, "owner", &mut mutations)
        .unwrap();
    apply_mutations(store, &mutations);
    let plan = match active.job.type_ {
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG => {
            tidb_exec::cluster_ddl::plan_persisted_materialized_view_log_job_step(
                store,
                job_id,
                20_000,
                DdlJobSchemaState::new(true),
            )
        }
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW => {
            tidb_exec::cluster_ddl::plan_persisted_materialized_view_create_job_step(
                store,
                job_id,
                20_000,
                None,
                DdlJobSchemaState::new(true),
            )
        }
        _ => panic!("not a materialized-view seed action"),
    }
    .unwrap();
    let PersistedDdlJobPlan::SchemaSync {
        version, mdl_info, ..
    } = plan
    else {
        panic!("replacement owner must recover the previous publication before advancing");
    };
    assert_eq!(version, step.write.schema_version);
    let mdl_info = mdl_info.unwrap();
    assert_eq!(mdl_info.table_ids, active.table_ids);
    assert!(!tidb_exec::cluster_ddl::supports_persisted_ddl_job(
        active.job.type_
    ));
    assert!(plan_persisted_ddl_job_step(
        store,
        job_id,
        20_000,
        DdlJobSchemaState::new(true),
        &|| { tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT }
    )
    .is_err());
    let mut cleanup = Vec::new();
    mdl_info
        .append_delete_mutations(store, job_id, "owner", &mut cleanup)
        .unwrap();
    apply_mutations(store, &cleanup);
}

fn finish_mview_job(store: &mut MetaStore, job_id: i64, ts: u64) {
    let catalog = load_cluster_catalog(store).unwrap();
    let queue = DdlJobTable::locate(&catalog).unwrap();
    let active = queue.load_by_id(store, job_id).unwrap().unwrap();
    assert!(matches!(
        active.job.state,
        JobState::DONE | JobState::ROLLBACK_DONE
    ));
    assert_eq!(
        active.job.binlog_info.as_ref().unwrap().read().finished_ts,
        0
    );
    let step = match active.job.type_ {
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG => plan_mview_log_step(store, job_id, ts),
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW => plan_mview_step(store, job_id, ts, None),
        _ => panic!("not a materialized-view seed action"),
    }
    .unwrap();
    assert!(step.terminal);
    assert_eq!(step.write.schema_version, 0);
    assert!(step.write.mdl_info_update.is_none());
    apply(store, &step.write);
    assert!(queue.load_by_id(store, job_id).unwrap().is_none());
    let finished = DdlHistoryTable::locate(&catalog)
        .unwrap()
        .load(store)
        .unwrap()
        .into_iter()
        .find(|job| job.id == job_id)
        .unwrap();
    assert_eq!(finished.raw_args, active.job.raw_args);
    assert_eq!(
        finished.binlog_info.as_ref().unwrap().read().finished_ts,
        ts
    );
}

#[test]
fn materialized_view_cancellation_persists_history_and_error() {
    for (action, args, expected_code) in [
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG,
            serde_json::json!({"table_info": {}}),
            tidb_error::tidb::errcode::ErrInvalidDDLJob,
        ),
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG,
            serde_json::json!({}),
            tidb_error::tidb::errcode::ErrInvalidDDLJob,
        ),
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
            serde_json::json!({"table_info": {}}),
            tidb_error::tidb::errcode::ErrInvalidDDLJob,
        ),
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
            serde_json::json!({}),
            tidb_error::tidb::errcode::ErrInvalidDDLJob,
        ),
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG,
            serde_json::json!({"table_info": 123}),
            1105,
        ),
        (
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
            serde_json::json!({"table_info": 123}),
            1105,
        ),
    ] {
        let mut store = bootstrapped();
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut job = Job::default();
        job.id = 900;
        job.version = JobVersion::V2;
        job.type_ = action;
        job.schema_id = 112;
        job.schema_name = "u6".into();
        job.binlog_info = Some(GoShared::new(tidb_model::HistoryInfo::default()));
        job.fill_v2_arg(serde_json::from_value(args).unwrap());
        let mut mutations = Vec::new();
        queue
            .append_insert(&mut job, false, "112", "0", true, &mut mutations)
            .unwrap();
        apply_mutations(&mut store, &mutations);
        let before = store.pairs.clone();
        let raw_args = queue
            .load_by_id(&mut store, 900)
            .unwrap()
            .unwrap()
            .job
            .raw_args
            .clone();
        // A wrong action entrypoint must not finalize this queue row. A failed
        // SQL history write must likewise leave the active row retryable.
        let wrong_entrypoint = if action == ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG {
            plan_mview_step(&mut store, 900, 10_000, None)
        } else {
            plan_mview_log_step(&mut store, 900, 10_000)
        };
        assert!(wrong_entrypoint.is_err());
        let mut broken = store.clone();
        let (db, history) = catalog.find_table("mysql", "tidb_ddl_history").unwrap();
        broken.pairs.remove(&key::table_kv_key(db.id, history.id));
        let broken_before = broken.pairs.clone();
        let result = if action == ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG {
            plan_mview_log_step(&mut broken, 900, 10_000)
        } else {
            plan_mview_step(&mut broken, 900, 10_000, None)
        };
        assert!(result.is_err());
        assert_eq!(broken.pairs, broken_before);
        assert!(queue.load_by_id(&mut broken, 900).unwrap().is_some());
        let step = if action == ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG {
            plan_mview_log_step(&mut store, 900, 10_000)
        } else {
            plan_mview_step(&mut store, 900, 10_000, None)
        }
        .expect("cancellation returns committable history mutations");
        assert!(step.terminal);
        assert_eq!(step.write.schema_version, 0);
        assert!(step.write.mdl_info_update.is_none());
        assert!(step.write.warnings.is_empty());
        assert_eq!(store.pairs, before, "planning does not publish changes");
        apply(&mut store, &step.write);
        assert!(queue.load_by_id(&mut store, 900).unwrap().is_none());
        let finished = DdlHistoryTable::locate(&catalog)
            .unwrap()
            .load(&mut store)
            .unwrap()
            .into_iter()
            .find(|job| job.id == 900)
            .unwrap();
        assert_eq!(finished.state, JobState::CANCELLED);
        assert_eq!(finished.error_count, 1);
        assert_eq!(
            finished.error.as_ref().unwrap().read().code(),
            tidb_error::terror::TerrorCode::new(expected_code as isize)
        );
        assert_eq!(finished.raw_args, raw_args);
        assert_eq!(
            finished.binlog_info.as_ref().unwrap().read().finished_ts,
            10_000
        );
        assert!(store.pairs.contains_key(&key::ddl_job_history_kv_key(900)));
        assert_eq!(
            load_cluster_catalog(&mut store).unwrap().schema_version,
            catalog.schema_version
        );
    }
}

/// Go `onCreateMaterializedViewLog` (master `94a9cbedab`): one owner step
/// turns the submitted job into the created `$mlog$` table, the base's
/// `MLogID` back-reference, the purge-schedule row, the schema-version bump
/// with its create-table event. History follows schema acknowledgement. The
/// rollback transition also waits before shared finalization.
#[test]
fn persisted_materialized_view_log_step_creates_the_log_and_rolls_back() {
    use tidb_executor::StmtContext;

    let enabled = StmtContext::for_query().with_enable_mview(true);
    let lower = |sql: &str, schema: &str| {
        let parsed = tidb_parser::parse(sql).expect("MV LOG DDL parses");
        lower_ddl_with_context(&parsed, schema, &enabled)
            .expect("MV LOG DDL admission outcome")
            .expect("MV LOG DDL lowers to a statement")
    };
    let mut store = bootstrapped();

    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE mv_base (id INT PRIMARY KEY AUTO_INCREMENT, k INT)",
            "u6",
        ),
        1_500,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);
    let base_id = create.created_id.expect("base table id");
    let base_schema_version = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        catalog.schema_version
    };

    // Submit the log create (batch 14's planner).
    let mut spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower("CREATE MATERIALIZED VIEW LOG ON mv_base (id, k)", "u6"),
        1_501,
        false,
        0,
    )
    .expect("submission preflight succeeds")
    .expect("the log create owns a job spec");
    let job_id = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let (mutations, cleanup) = plan_insert_attempt(
            &mut store,
            &catalog,
            std::slice::from_mut(&mut spec),
            &mut |_| Option::<fn()>::None,
        )
        .expect("the insertion attempt plans");
        apply_mutations(&mut store, &mutations);
        drop(cleanup);
        spec.job.id
    };
    assert_ne!(job_id, 0);

    // The owner step creates everything Go's single phase creates.
    let step = plan_mview_log_step(&mut store, job_id, 1_502).expect("the worker step plans");
    assert!(
        !step.terminal,
        "DONE must wait for schema sync before history"
    );
    assert_eq!(step.write.schema_version, base_schema_version + 1);
    assert_eq!(
        step.write.diff.action_type,
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
    );
    let mlog_id = step.write.created_id.expect("the step created the mlog");
    apply_mutations(&mut store, &step.write.mutations);
    if step.write.schema_version != 0 {
        acknowledge_mview_schema(&mut store, &step);
    }

    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.name.original() == "u6")
        .expect("u6 survives");
    let mlog = database
        .tables
        .iter()
        .find(|table| table.id == mlog_id)
        .expect("the mlog table exists after the step");
    assert_eq!(mlog.name.original(), "$mlog$mv_base");
    assert_eq!(mlog.state, SchemaState::PUBLIC);
    assert_eq!(
        mlog.materialized_view_log
            .as_ref()
            .expect("the log metadata is set")
            .read()
            .base_table_id,
        base_id
    );
    let base = database
        .tables
        .iter()
        .find(|table| table.id == base_id)
        .expect("the base survives");
    assert_eq!(
        base.materialized_view_base
            .as_ref()
            .expect("the base gains its log reference")
            .read()
            .mlog_id,
        mlog_id
    );

    // The purge-schedule row records the log ID with a NULL deadline: the
    // statement wrote no PURGE clause, which derives (None, true).
    let purge_table = tidb_exec::mlog_purge_info_table::MlogPurgeInfoTable::locate(&catalog)
        .expect("the purge table exists");
    let row = purge_table
        .find(&mut store, mlog_id)
        .expect("the purge table scans")
        .expect("the step recorded the schedule row");
    assert_eq!(row.next_purge_unix_seconds, None);

    // The shared history transaction follows acknowledgement and carries
    // both affected tables, with Go's DONE -> SYNCED transition.
    finish_mview_job(&mut store, job_id, 19_000);
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let job_table = DdlJobTable::locate(&catalog).expect("the job table exists");
    assert!(job_table
        .load(&mut store)
        .expect("the queue scans")
        .iter()
        .all(|active| active.job.id != job_id));
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let history_jobs = history.load(&mut store).expect("SQL history scans");
    let finished = history_jobs
        .iter()
        .find(|job| job.id == job_id)
        .expect("the finished job is in history");
    assert_eq!(finished.state, JobState::SYNCED);
    let binlog = finished
        .binlog_info
        .as_ref()
        .expect("history keeps BinlogInfo")
        .read();
    let finished_tables: Vec<_> = binlog
        .multiple_table_infos
        .iter_handles()
        .into_iter()
        .flatten()
        .map(|table| table.read().name.original().to_owned())
        .collect();
    assert_eq!(
        finished_tables,
        vec!["mv_base".to_owned(), "$mlog$mv_base".to_owned()]
    );

    // A second base's log create, rolled back before the phase committed:
    // nothing is dropped, the base keeps no log reference, and the job ends
    // ROLLBACK_DONE.
    let fresh = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE second_base (id INT PRIMARY KEY AUTO_INCREMENT)",
            "u6",
        ),
        1_503,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(fresh) = fresh else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &fresh);
    let mut spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower("CREATE MATERIALIZED VIEW LOG ON second_base (id)", "u6"),
        1_504,
        false,
        0,
    )
    .expect("submission preflight succeeds")
    .expect("the second log create owns a job spec");
    let rollback_job_id = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let (mutations, cleanup) = plan_insert_attempt(
            &mut store,
            &catalog,
            std::slice::from_mut(&mut spec),
            &mut |_| Option::<fn()>::None,
        )
        .expect("the insertion attempt plans");
        apply_mutations(&mut store, &mutations);
        drop(cleanup);
        spec.job.id
    };

    // Go enters the rollback through `job.State = Rollingback`, persisted by
    // an admin cancel. Rewrite the queued row's state directly.
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let job_table = DdlJobTable::locate(&catalog).expect("the job table exists");
    let mut active = job_table
        .load(&mut store)
        .expect("the queue scans")
        .into_iter()
        .find(|active| active.job.id == rollback_job_id)
        .expect("the second job is active");
    active.job.state = JobState::ROLLINGBACK;
    let mut rewrite = Vec::new();
    job_table
        .append_update(&mut active, false, &mut rewrite)
        .expect("the queued row updates");
    apply_mutations(&mut store, &rewrite);

    let step =
        plan_mview_log_step(&mut store, rollback_job_id, 1_505).expect("the rollback step plans");
    assert!(!step.terminal);
    apply_mutations(&mut store, &step.write.mutations);
    if step.write.schema_version != 0 {
        acknowledge_mview_schema(&mut store, &step);
    }
    finish_mview_job(&mut store, rollback_job_id, 19_000);

    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.name.original() == "u6")
        .expect("u6 survives");
    assert!(
        !database
            .tables
            .iter()
            .any(|table| table.name.original() == "$mlog$second_base"),
        "nothing was created, so nothing is dropped"
    );
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let finished = history
        .load(&mut store)
        .expect("SQL history scans")
        .into_iter()
        .find(|job| job.id == rollback_job_id)
        .expect("the rolled-back job is in history");
    assert_eq!(finished.state, JobState::ROLLBACK_DONE);
}

/// Go `onCreateMaterializedView` (master `94a9cbedab`), phase 1: the owner
/// step checks every base, lands the view table PUBLIC, records the view in
/// each base's `MViewIDs`, prewrites the refresh-info row, and transitions
/// the queued job to `Running`/`StateWriteReorganization` as a non-terminal
/// step. The initial-build phase is the recorded seam; the rollback
/// transition undoes everything phase 1 committed.
#[test]
fn persisted_materialized_view_create_step_runs_phase_one_and_rolls_back() {
    use tidb_executor::StmtContext;

    let enabled = StmtContext::for_query().with_enable_mview(true);
    let lower = |sql: &str, schema: &str| {
        let parsed = tidb_parser::parse(sql).expect("MV DDL parses");
        lower_ddl_with_context(&parsed, schema, &enabled)
            .expect("MV DDL admission outcome")
            .expect("MV DDL lowers to a statement")
    };
    let mut store = bootstrapped();

    // Base table + its log (batch 14/15 machinery).
    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE mv_base (id INT PRIMARY KEY AUTO_INCREMENT, k INT)",
            "u6",
        ),
        1_600,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);
    let base_id = create.created_id.expect("base table id");
    let mut log_submit = prepare_materialized_view_job_submission(
        &mut store,
        &lower("CREATE MATERIALIZED VIEW LOG ON mv_base (id, k)", "u6"),
        1_601,
        false,
        0,
    )
    .expect("log submission preflight succeeds")
    .expect("the log create owns a job spec");
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let (mutations, cleanup) = plan_insert_attempt(
        &mut store,
        &catalog,
        std::slice::from_mut(&mut log_submit),
        &mut |_| Option::<fn()>::None,
    )
    .expect("the log insertion plans");
    apply_mutations(&mut store, &mutations);
    drop(cleanup);
    let log_job_id = log_submit.job.id;
    let log_step =
        plan_mview_log_step(&mut store, log_job_id, 1_602).expect("the log worker step plans");
    apply_mutations(&mut store, &log_step.write.mutations);
    acknowledge_mview_schema(&mut store, &log_step);
    finish_mview_job(&mut store, log_step.write.ddl_job_id, 19_000);
    let mlog_id = log_step
        .write
        .created_id
        .expect("the log worker created the mlog");

    // Submit the view create (batch 16 machinery).
    let mut spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower(
            "CREATE MATERIALIZED VIEW mv (id, c) AS (SELECT id, COUNT(1) FROM mv_base GROUP BY id)",
            "u6",
        ),
        1_603,
        false,
        0,
    )
    .expect("view submission preflight succeeds")
    .expect("the view create owns a job spec");
    let view_job_id = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let (mutations, cleanup) = plan_insert_attempt(
            &mut store,
            &catalog,
            std::slice::from_mut(&mut spec),
            &mut |_| Option::<fn()>::None,
        )
        .expect("the view insertion plans");
        apply_mutations(&mut store, &mutations);
        drop(cleanup);
        spec.job.id
    };

    // Phase 1: the catalog gains the view, the base its back-reference, and
    // the job its WriteReorganization transition — non-terminal.
    let step = plan_mview_step(&mut store, view_job_id, 1_604, None).expect("phase 1 plans");
    assert!(!step.terminal, "phase 1 hands the job to the build phase");
    assert_eq!(
        step.write.diff.action_type,
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW
    );
    let view_id = step.write.created_id.expect("phase 1 created the view");
    apply_mutations(&mut store, &step.write.mutations);
    if step.write.schema_version != 0 {
        acknowledge_mview_schema(&mut store, &step);
    }

    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.name.original() == "u6")
        .expect("u6 survives");
    let view = database
        .tables
        .iter()
        .find(|table| table.id == view_id)
        .expect("the view table exists after phase 1");
    assert_eq!(view.name.original(), "mv");
    assert_eq!(view.state, SchemaState::PUBLIC);
    let view_meta = view
        .materialized_view
        .as_ref()
        .expect("the view metadata is set")
        .read();
    assert_eq!(
        view_meta.base_table_ids.iter().copied().collect::<Vec<_>>(),
        vec![base_id]
    );
    let base = database
        .tables
        .iter()
        .find(|table| table.id == base_id)
        .expect("the base survives");
    let base_meta = base
        .materialized_view_base
        .as_ref()
        .expect("the base gains its view reference")
        .read();
    assert_eq!(base_meta.mlog_id, mlog_id);
    assert_eq!(
        base_meta.mview_ids.iter().copied().collect::<Vec<_>>(),
        vec![view_id]
    );
    drop(base_meta);

    // The refresh-info prewrite row records the phase's read TSO.
    let refresh_table =
        tidb_exec::mview_refresh_info_table::MviewRefreshInfoTable::locate(&catalog)
            .expect("the refresh table exists");
    let row = refresh_table
        .find(&mut store, view_id)
        .expect("the refresh table scans")
        .expect("phase 1 prewrote the refresh row");
    assert_eq!(row.last_success_read_tso, Some(1_604));
    assert_eq!(row.next_refresh_unix_seconds, None);

    // The job is still queued, now Running at WriteReorganization.
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let job_table = DdlJobTable::locate(&catalog).expect("the job table exists");
    let active = job_table
        .load(&mut store)
        .expect("the queue scans")
        .into_iter()
        .find(|active| active.job.id == view_job_id)
        .expect("the view job stays queued for its build phase");
    assert_eq!(active.job.state, JobState::RUNNING);
    assert_eq!(active.job.schema_state, SchemaState::WRITE_REORGANIZATION);

    // The base carries the rows the build reads, stored as the record bytes
    // a real cluster writes: an Int-handle table keeps `id` in the key.
    let base_info = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let database = catalog
            .databases
            .iter()
            .find(|db| db.info.name.original() == "u6")
            .expect("u6 exists");
        database
            .tables
            .iter()
            .find(|t| t.id == base_id)
            .expect("the base table exists")
            .clone_like_go()
    };
    for (id, k) in [(1, 10), (2, 10), (3, 20)] {
        let mut values = tidb_exec::system_row_write::RowValues::new();
        values.insert(view_info_columns(&base_info, 0), Datum::Int(id));
        values.insert(view_info_columns(&base_info, 1), Datum::Int(k));
        let mutations =
            store_clustered_row(&base_info, None, &values).expect("the base row encodes");
        apply_mutations(&mut store, &mutations);
    }

    // Phase 2 with no caller-supplied outcome runs the pure-tier build
    // itself: the definition SELECT executes over the base rows just seeded
    // and the aggregated view rows land in the completion transaction —
    // one action tick, job DONE but still active until schema acknowledgement.
    let step = plan_mview_step(&mut store, view_job_id, 1_605, None)
        .expect("the build phase plans, builds, and completes");
    assert!(!step.terminal, "the built job waits for schema sync");
    apply_mutations(&mut store, &step.write.mutations);
    if step.write.schema_version != 0 {
        acknowledge_mview_schema(&mut store, &step);
    }
    let view_id = step.write.created_id.expect("the view id");

    // The build actually MOVED the rows: the view answers the aggregation
    // the definition computes over the seeded base rows.
    let view_info = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
        let database = catalog
            .databases
            .iter()
            .find(|db| db.info.name.original() == "u6")
            .expect("u6 exists");
        database
            .tables
            .iter()
            .find(|t| t.id == view_id)
            .expect("the view exists")
            .clone_like_go()
    };
    assert_eq!(
        read_view_rows(&mut store, &view_info),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(1)],
            vec![Datum::Int(3), Datum::Int(1)]
        ],
    );

    // The shared history transaction follows the successful schema wait.
    finish_mview_job(&mut store, view_job_id, 19_000);
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let finished = history
        .load(&mut store)
        .expect("SQL history scans")
        .into_iter()
        .find(|job| job.id == view_job_id)
        .expect("the finished job is in history");
    assert_eq!(finished.state, JobState::SYNCED);
    let finished_tables: Vec<_> = finished
        .binlog_info
        .as_ref()
        .expect("history keeps BinlogInfo")
        .read()
        .multiple_table_infos
        .iter_handles()
        .into_iter()
        .flatten()
        .map(|table| table.read().name.original().to_owned())
        .collect();
    assert_eq!(finished_tables, vec!["mv_base".to_owned(), "mv".to_owned()]);

    // Rollback: a second view on another base with its own log, whose build
    // never ran -- persist Go's Rollingback transition after phase 1, then
    // the step drops the phase-1 view, clears the base's view reference,
    // deletes the refresh row, and ends ROLLBACK_DONE.
    let second = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE second_base (id INT PRIMARY KEY AUTO_INCREMENT)",
            "u6",
        ),
        1_607,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(second) = second else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &second);
    let mut log_spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower("CREATE MATERIALIZED VIEW LOG ON second_base (id)", "u6"),
        1_608,
        false,
        0,
    )
    .expect("log submission preflight succeeds")
    .expect("the second log create owns a job spec");
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let (mutations, cleanup) = plan_insert_attempt(
        &mut store,
        &catalog,
        std::slice::from_mut(&mut log_spec),
        &mut |_| Option::<fn()>::None,
    )
    .expect("the log insertion plans");
    apply_mutations(&mut store, &mutations);
    drop(cleanup);
    let log_step = plan_mview_log_step(&mut store, log_spec.job.id, 1_609)
        .expect("the second log worker step plans");
    apply_mutations(&mut store, &log_step.write.mutations);
    acknowledge_mview_schema(&mut store, &log_step);
    finish_mview_job(&mut store, log_step.write.ddl_job_id, 19_000);

    let mut view_spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower(
            "CREATE MATERIALIZED VIEW mv2 (id, c) AS (SELECT id, COUNT(1) FROM second_base GROUP BY id)",
            "u6",
        ),
        1_610,
        false,
        0,
    )
    .expect("view submission preflight succeeds")
    .expect("the second view create owns a job spec");
    let view_job_id = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let (mutations, cleanup) = plan_insert_attempt(
            &mut store,
            &catalog,
            std::slice::from_mut(&mut view_spec),
            &mut |_| Option::<fn()>::None,
        )
        .expect("the view insertion plans");
        apply_mutations(&mut store, &mutations);
        drop(cleanup);
        view_spec.job.id
    };

    // Phase 1 only.
    let step = plan_mview_step(&mut store, view_job_id, 1_611, None).expect("phase 1 plans");
    assert!(!step.terminal);
    apply_mutations(&mut store, &step.write.mutations);
    if step.write.schema_version != 0 {
        acknowledge_mview_schema(&mut store, &step);
    }

    // Persist Rollingback, then the step undoes phase 1.
    let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
    let job_table = DdlJobTable::locate(&catalog).expect("the job table exists");
    let mut active = job_table
        .load(&mut store)
        .expect("the queue scans")
        .into_iter()
        .find(|active| active.job.id == view_job_id)
        .expect("the view job is still queued");
    active.job.state = JobState::ROLLINGBACK;
    let mut rewrite = Vec::new();
    job_table
        .append_update(&mut active, false, &mut rewrite)
        .expect("the queued row updates");
    apply_mutations(&mut store, &rewrite);

    let step =
        plan_mview_step(&mut store, view_job_id, 1_613, None).expect("the rollback step plans");
    assert!(!step.terminal);
    apply_mutations(&mut store, &step.write.mutations);
    if step.write.schema_version != 0 {
        acknowledge_mview_schema(&mut store, &step);
    }
    finish_mview_job(&mut store, view_job_id, 19_000);

    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.name.original() == "u6")
        .expect("u6 survives");
    assert!(
        !database
            .tables
            .iter()
            .any(|table| table.name.original() == "mv2"),
        "the rollback drops the created view"
    );
    let second_base = database
        .tables
        .iter()
        .find(|table| table.name.original() == "second_base")
        .expect("the second base survives");
    let base_meta = second_base
        .materialized_view_base
        .as_ref()
        .expect("the second base keeps its log reference")
        .read();
    assert!(
        base_meta.mview_ids.is_empty(),
        "the rollback removes the view from the base"
    );
    let refresh_table =
        tidb_exec::mview_refresh_info_table::MviewRefreshInfoTable::locate(&catalog)
            .expect("the refresh table exists");
    assert!(
        refresh_table
            .find(&mut store, view_job_id)
            .expect("the refresh table scans")
            .is_none(),
        "the rollback deletes the refresh row"
    );
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let finished = history
        .load(&mut store)
        .expect("SQL history scans")
        .into_iter()
        .find(|job| job.id == view_job_id)
        .expect("the rolled-back job is in history");
    assert_eq!(finished.state, JobState::ROLLBACK_DONE);
}

/// Go refuses to build over rows a crashed prior attempt left in the view
/// (`hasCreateMaterializedViewBuildRows` + the ErrInvalidDDLJob it raises):
/// the job moves to Rollingback and the NEXT tick drops the phase-1 view.
#[test]
fn persisted_materialized_view_build_refuses_residual_rows_then_rolls_back() {
    use tidb_executor::StmtContext;

    let enabled = StmtContext::for_query().with_enable_mview(true);
    let lower = |sql: &str, schema: &str| {
        let parsed = tidb_parser::parse(sql).expect("MV DDL parses");
        lower_ddl_with_context(&parsed, schema, &enabled)
            .expect("MV DDL admission outcome")
            .expect("MV DDL lowers to a statement")
    };
    let mut store = bootstrapped();

    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE mv_base (id INT PRIMARY KEY AUTO_INCREMENT, k INT)",
            "u6",
        ),
        1_700,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);
    let mut log_spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower("CREATE MATERIALIZED VIEW LOG ON mv_base (id, k)", "u6"),
        1_701,
        false,
        0,
    )
    .expect("log submission preflight succeeds")
    .expect("the log create owns a job spec");
    {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let (mutations, cleanup) = plan_insert_attempt(
            &mut store,
            &catalog,
            std::slice::from_mut(&mut log_spec),
            &mut |_| Option::<fn()>::None,
        )
        .expect("the log insertion plans");
        apply_mutations(&mut store, &mutations);
        drop(cleanup);
    }
    let log_step =
        plan_mview_log_step(&mut store, log_spec.job.id, 1_702).expect("the log worker step plans");
    apply_mutations(&mut store, &log_step.write.mutations);
    acknowledge_mview_schema(&mut store, &log_step);
    finish_mview_job(&mut store, log_step.write.ddl_job_id, 19_000);

    let mut spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower(
            "CREATE MATERIALIZED VIEW mv (id, c) AS (SELECT id, COUNT(1) FROM mv_base GROUP BY id)",
            "u6",
        ),
        1_703,
        false,
        0,
    )
    .expect("view submission preflight succeeds")
    .expect("the view create owns a job spec");
    let view_job_id = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let (mutations, cleanup) = plan_insert_attempt(
            &mut store,
            &catalog,
            std::slice::from_mut(&mut spec),
            &mut |_| Option::<fn()>::None,
        )
        .expect("the view insertion plans");
        apply_mutations(&mut store, &mutations);
        drop(cleanup);
        spec.job.id
    };
    let phase_one = plan_mview_step(&mut store, view_job_id, 1_704, None).expect("phase 1 plans");
    apply_mutations(&mut store, &phase_one.write.mutations);

    // Rows a crashed prior attempt left behind: the view's record range is
    // not empty when the build phase next ticks.
    let view_id = phase_one
        .write
        .created_id
        .expect("phase 1 created the view");
    let residual_view = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let database = catalog
            .databases
            .iter()
            .find(|db| db.info.name.original() == "u6")
            .expect("u6 exists");
        database
            .tables
            .iter()
            .find(|t| t.id == view_id)
            .expect("the view exists")
            .clone_like_go()
    };
    let mut values = tidb_exec::system_row_write::RowValues::new();
    values.insert(view_info_columns(&residual_view, 0), Datum::Int(7));
    values.insert(view_info_columns(&residual_view, 1), Datum::Int(1));
    let mutations =
        store_clustered_row(&residual_view, None, &values).expect("the residual row encodes");
    apply_mutations(&mut store, &mutations);

    // The build error must survive the Rollingback transition and history;
    // it is a job error, not a successful statement warning.
    let step = plan_mview_step(&mut store, view_job_id, 1_705, None)
        .expect("the refused tick plans the Rollingback transition");
    assert!(!step.terminal, "Rollingback is not terminal");
    assert!(step.write.warnings.is_empty());
    apply_mutations(&mut store, &step.write.mutations);
    if step.write.schema_version != 0 {
        acknowledge_mview_schema(&mut store, &step);
    }

    let catalog = load_cluster_catalog(&mut store).unwrap();
    let active = DdlJobTable::locate(&catalog)
        .unwrap()
        .load_by_id(&mut store, view_job_id)
        .unwrap()
        .unwrap();
    assert!(
        active.job.error.is_some(),
        "Go countForError persists the build error"
    );
    assert_eq!(active.job.error_count, 1);

    // The next tick runs the rollback: the phase-1 view drops and the job
    // ends ROLLBACK_DONE.
    let rollback =
        plan_mview_step(&mut store, view_job_id, 1_706, None).expect("the rollback tick plans");
    assert!(!rollback.terminal, "rollback waits for schema sync");
    apply_mutations(&mut store, &rollback.write.mutations);
    acknowledge_mview_schema(&mut store, &rollback);
    finish_mview_job(&mut store, view_job_id, 19_000);
    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let database = catalog
        .databases
        .iter()
        .find(|db| db.info.name.original() == "u6")
        .expect("u6 survives");
    assert!(
        !database.tables.iter().any(|table| table.id == view_id),
        "the rollback drops the phase-1 view"
    );
    let history = DdlHistoryTable::locate(&catalog).expect("history table exists");
    let finished = history
        .load(&mut store)
        .expect("SQL history scans")
        .into_iter()
        .find(|job| job.id == view_job_id)
        .expect("the rolled-back job is in history");
    assert_eq!(finished.state, JobState::ROLLBACK_DONE);
    assert_eq!(finished.error_count, 1);
    assert_eq!(
        finished.error.as_ref().unwrap().read().code().value(),
        tidb_error::tidb::errcode::ErrInvalidDDLJob as isize
    );
    assert!(finished
        .error
        .as_ref()
        .unwrap()
        .read()
        .to_string()
        .contains("residual build rows"));
}

/// Go master `94a9cbedab` parses `ALTER MATERIALIZED VIEW` and
/// `ALTER MATERIALIZED VIEW LOG` but neither its planner (`buildDDL` has no
/// case) nor its executor (`DDLExec.Next` has no case, `err` stays nil)
/// handles them: both statements SUCCEED as no-ops. The Rust route must
/// therefore own the statements and plan them as zero-write successes, not
/// refuse them.
#[test]
fn alter_materialized_view_succeeds_as_a_no_op_like_go() {
    let mut store = bootstrapped();
    for sql in [
        "ALTER MATERIALIZED VIEW u6.mv COMMENT 'x'",
        "ALTER MATERIALIZED VIEW u6.mv REFRESH NEXT now() + INTERVAL 1 HOUR",
        "ALTER MATERIALIZED VIEW LOG ON u6.mv_base PURGE IMMEDIATE",
        "ALTER MATERIALIZED VIEW LOG ON u6.mv_base ADD COLUMN (extra)",
        "DROP MATERIALIZED VIEW u6.mv",
        "DROP MATERIALIZED VIEW IF EXISTS u6.mv",
        "DROP MATERIALIZED VIEW LOG ON u6.mv_base",
        "DROP MATERIALIZED VIEW LOG IF EXISTS ON u6.mv_base",
    ] {
        let statement =
            prepare_cluster_ddl_with_context(sql, "u6", &tidb_executor::StmtContext::for_query())
                .expect("the statement lowers")
                .unwrap_or_else(|| panic!("the no-op route owns {sql}"));
        let plan = plan_ddl(&mut store, &statement, 1_801).expect("the no-op plans");
        let DdlPlan::AlreadySatisfied { warnings, .. } = plan else {
            panic!("{sql} plans as a zero-write success")
        };
        assert!(warnings.is_empty(), "Go appends no warning for the no-op");
    }
}

/// Looks up the column ID at the given offset in the table.
fn view_info_columns(table: &tidb_model::TableInfo, offset: usize) -> i64 {
    table
        .columns
        .iter_deref()
        .nth(offset)
        .map(|c| c.read().id)
        .expect("column exists at offset")
}

/// Reads a view's stored rows back through the driver: the same pre-load-
/// and-SELECT shape the build engine uses, so the assertion exercises the
/// real decode path rather than re-reading the test's own encodings.
fn read_view_rows(
    store: &mut MetaStore,
    view: &tidb_model::TableInfo,
) -> Vec<Vec<tidb_datatype::Datum>> {
    use tidb_executor::storage::{MemTableStorage, TableStorage};
    use tidb_executor::{Catalog, KvColumn, KvTable};
    let mut kv_columns: Vec<KvColumn> = Vec::new();
    for column in view.columns.iter_deref() {
        let column = column.read();
        kv_columns.push(KvColumn {
            name: column.name.original().to_owned(),
            id: column.id,
            field_type: column.field_type.clone(),
            column_info_version: column.version,
            default_value: None,
            origin_default: None,
            comment: column.comment.clone(),
            generated: None,
        });
    }
    let mut storage = MemTableStorage::new();
    let (start, end) = tidb_codec::table_key::get_table_handle_key_range(view.id);
    for (key, stored) in store
        .scan_range(&start, &end)
        .expect("the view record range scans")
    {
        storage
            .set(tidb_txnkv::Key::from_bytes(key), stored)
            .expect("the row preloads");
    }
    let mut kv_table = KvTable::with_storage(view.id, kv_columns, Box::new(storage));
    kv_table.set_name(view.name.original());
    if view.pk_is_handle {
        let offset = view
            .columns
            .iter_deref()
            .position(|column| {
                column.read().field_type.flags() & tidb_datatype::FieldTypeFlags::PRI_KEY != 0
            })
            .expect("the PK column exists");
        kv_table.set_pk_handle_offset(offset);
    } else if view.is_common_handle {
        let mut offsets = Vec::new();
        let primary = view
            .indices
            .iter_deref()
            .find(|index| index.read().primary)
            .expect("the view has its PRIMARY index");
        for part in primary.read().columns.iter_deref() {
            let name = part.read().name.lowercase().to_owned();
            offsets.push(
                view.columns
                    .iter_deref()
                    .position(|column| column.read().name.lowercase() == name.as_str())
                    .expect("the key column exists"),
            );
        }
        kv_table.set_common_handle_offsets(offsets);
        kv_table.set_common_handle_version(view.common_handle_version);
    }
    let mut catalog = Catalog::default();
    catalog.create_database("u6");
    catalog
        .register_kv_in("u6", view.name.original(), kv_table)
        .expect("the view registers");
    let context = tidb_executor::StmtContext::for_query();
    let sql = format!("SELECT * FROM {} ORDER BY 1", view.name.original());
    let (_, rows) = tidb_executor::run_select_meta_in(&sql, &catalog, "u6", &context)
        .expect("the view rows read");
    rows
}

/// Looks up the column ID at the given offset in the table.

/// Go `TestCreateMaterializedViewLogPreservesTextColumnTypes` (master
/// `94a9cbedab`): TEXT columns in the log copy keep their declared type but
/// a max-length BLOB (flen 65535) is normalized back to unspecified length.
#[test]
fn materialized_view_log_preserves_text_column_types() {
    use tidb_executor::StmtContext;
    let enabled = StmtContext::for_query().with_enable_mview(true);
    let lower = |sql: &str, schema: &str| {
        let parsed = tidb_parser::parse(sql).expect("MV LOG DDL parses");
        lower_ddl_with_context(&parsed, schema, &enabled)
            .expect("MV LOG DDL admission outcome")
            .expect("MV LOG DDL lowers to a statement")
    };
    let mut store = bootstrapped();

    // Base table with a TEXT column and a VARCHAR column.
    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE text_base (id INT PRIMARY KEY, txt TEXT, vc VARCHAR(100))",
            "u6",
        ),
        1_700,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);

    let mut spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower(
            "CREATE MATERIALIZED VIEW LOG ON text_base (id, txt, vc)",
            "u6",
        ),
        1_701,
        false,
        0,
    )
    .expect("submission preflight succeeds")
    .expect("the log create owns a job spec");

    let JobArgsValue::CreateMaterializedViewLog(Some(log_args)) = &spec.args else {
        panic!("the spec carries CreateMaterializedViewLogArgs")
    };
    let table_shared = log_args.read().table_info.get().expect("nil TableInfo");
    let table = table_shared.read();
    let handles: Vec<_> = table.columns.iter_handles().into_iter().flatten().collect();
    let cols: Vec<_> = handles.iter().map(|c| c.read()).collect();

    assert_eq!(cols.len(), 5, "3 declared + 2 physical _MLOG$_ columns");
    // TEXT column: flen is preserved (not max-length, so no normalization).
    assert_eq!(cols[1].name.original(), "txt");
    assert_eq!(
        cols[1].field_type.code(),
        tidb_datatype::FieldTypeCode::Blob,
        "TEXT maps to TypeBlob"
    );
    // VARCHAR column: preserved as-is.
    assert_eq!(cols[2].name.original(), "vc");
    assert_eq!(
        cols[2].field_type.code(),
        tidb_datatype::FieldTypeCode::Varchar
    );
    // The two _MLOG$_ physical columns are appended after the copies.
    assert_eq!(cols[3].name.original(), "_MLOG$_DML_TYPE");
    assert_eq!(cols[4].name.original(), "_MLOG$_OLD_NEW");
}

/// Go `deriveCreateMaterializedViewLogNextUnixSeconds` (master
/// `94a9cbedab`): a log WITH a purge schedule derives its deadline by
/// evaluating the schedule expression under the recorded schedule zone
/// through the owner's SQL — here the driver's FROM-less SELECT — and the
/// worker step records the derived unix seconds in the purge row.
#[test]
fn persisted_materialized_view_log_step_derives_the_purge_schedule() {
    use tidb_executor::StmtContext;

    let enabled = StmtContext::for_query().with_enable_mview(true);
    let lower = |sql: &str, schema: &str| {
        let parsed = tidb_parser::parse(sql).expect("MV DDL parses");
        lower_ddl_with_context(&parsed, schema, &enabled)
            .expect("MV DDL admission outcome")
            .expect("MV DDL lowers to a statement")
    };
    let mut store = bootstrapped();

    let create = plan_ddl(
        &mut store,
        &lower(
            "CREATE TABLE sched_base (id INT PRIMARY KEY AUTO_INCREMENT)",
            "u6",
        ),
        1_700,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);

    let mut spec = prepare_materialized_view_job_submission(
        &mut store,
        &lower(
            "CREATE MATERIALIZED VIEW LOG ON sched_base (id) PURGE NEXT CAST('2030-01-02 10:00:00' AS DATETIME)",
            "u6",
        ),
        1_701,
        false,
        0,
    )
    .expect("submission preflight succeeds")
    .expect("the scheduled log create owns a job spec");
    let job_id = {
        let catalog = load_cluster_catalog(&mut store).expect("catalog loads");
        let (mutations, cleanup) = plan_insert_attempt(
            &mut store,
            &catalog,
            std::slice::from_mut(&mut spec),
            &mut |_| Option::<fn()>::None,
        )
        .expect("the insertion attempt plans");
        apply_mutations(&mut store, &mutations);
        drop(cleanup);
        spec.job.id
    };

    // The derivation evaluates CAST('2030-01-02 10:00:00' AS DATETIME) under
    // the log's schedule zone (this context's zone is UTC) and persists the
    // unix seconds.
    let step = plan_mview_log_step(&mut store, job_id, 1_702)
        .expect("the worker step plans with the derived schedule");
    apply_mutations(&mut store, &step.write.mutations);
    let mlog_id = step.write.created_id.expect("the mlog id");

    let catalog = load_cluster_catalog(&mut store).expect("catalog reloads");
    let purge_table = tidb_exec::mlog_purge_info_table::MlogPurgeInfoTable::locate(&catalog)
        .expect("the purge table exists");
    let row = purge_table
        .find(&mut store, mlog_id)
        .expect("the purge table scans")
        .expect("the step recorded the schedule row");
    assert_eq!(
        row.next_purge_unix_seconds,
        Some(1_893_578_400),
        "2030-01-02 10:00:00 UTC"
    );
}

/// Go `validateCreateMaterializedViewQuery`'s clause refusals (master
/// `94a9cbedab`): HAVING, ORDER BY, LIMIT and DISTINCT each refuse with
/// Go's exact message, after the GROUP BY requirement.
#[test]
fn materialized_view_query_clause_refusals_follow_go() {
    let context = tidb_executor::StmtContext::for_query().with_enable_mview(true);
    let lower = |sql: &str| {
        let parsed = tidb_parser::parse(sql).expect("MV DDL parses");
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect("MV DDL admission outcome")
            .expect("MV DDL lowers to a statement")
    };
    let mut store = bootstrapped();
    let create = plan_ddl(
        &mut store,
        &lower("CREATE TABLE mv_base (id INT PRIMARY KEY AUTO_INCREMENT, k INT)"),
        1_500,
    )
    .expect("CREATE plans");
    let DdlPlan::Write(create) = create else {
        panic!("CREATE writes metadata")
    };
    apply(&mut store, &create);
    let mut mlog = tidb_model::TableInfo::default();
    mlog.id = 9_100;
    mlog.name = tidb_ast::CiString::new("$mlog$mv_base");
    let mut mlog_meta = tidb_model::MaterializedViewLogInfo::default();
    mlog_meta.base_table_id = create.created_id.expect("base id");
    mlog.materialized_view_log = Some(GoShared::new(mlog_meta));
    store.put(
        key::table_kv_key(112, 9_100),
        value::serialize_table_info(&mlog).expect("the mlog encodes"),
    );

    let cases = [
        (
            "CREATE MATERIALIZED VIEW mv (c) AS (SELECT id, COUNT(k) FROM mv_base GROUP BY id HAVING COUNT(k) > 1)",
            "Unsupported CREATE MATERIALIZED VIEW does not support HAVING clause",
        ),
        (
            "CREATE MATERIALIZED VIEW mv (c) AS (SELECT id, COUNT(k) FROM mv_base GROUP BY id ORDER BY id)",
            "Unsupported CREATE MATERIALIZED VIEW does not support ORDER BY clause",
        ),
        (
            "CREATE MATERIALIZED VIEW mv (c) AS (SELECT id, COUNT(k) FROM mv_base GROUP BY id LIMIT 10)",
            "Unsupported CREATE MATERIALIZED VIEW does not support LIMIT clause",
        ),
        (
            "CREATE MATERIALIZED VIEW mv (c) AS (SELECT DISTINCT id FROM mv_base GROUP BY id)",
            "Unsupported CREATE MATERIALIZED VIEW does not support SELECT DISTINCT",
        ),
    ];
    for (sql, want) in cases {
        let statement = lower(sql);
        let error =
            prepare_materialized_view_job_submission(&mut store, &statement, 1_501, false, 0)
                .expect_err("the clause refuses");
        let DdlPlanError::Admission(admission) = error else {
            panic!("expected a coded refusal for {sql}")
        };
        assert_eq!(admission.code, 8200);
        assert_eq!(admission.reason, want);
    }
}

/// Go BuildHiddenColumnInfo uses the expression type and shared index admission.
#[test]
fn json_result_batch_cluster_hidden_columns_use_the_shared_type_owner() {
    for (expression, code) in [
        ("JSON_SEARCH(j,'one','x')", 3753),
        ("JSON_PRETTY(j)", 3757),
        ("JSON_UNQUOTE(j)", 3757),
    ] {
        let sql = format!("CREATE TABLE t (j JSON, INDEX i (({expression})))");
        assert_eq!(refusal_with_code(&sql).0, code, "{expression}");
    }
    let DdlStatement::CreateTable { build, .. } =
        statement("CREATE TABLE t (v VARCHAR(10), INDEX i ((JSON_UNQUOTE(v))))")
    else {
        panic!("CREATE TABLE expected");
    };
    let column = build.template().columns.get(1).unwrap();
    let column = column.read();
    assert!(column.hidden);
    assert_eq!(
        column.field_type.code(),
        tidb_datatype::FieldTypeCode::VarString
    );
    assert_eq!(column.field_type.flen(), 10);
}

#[test]
fn check_job_admission_uses_alter_column_errors_and_constraint_name_scope() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE check_admission (a INT, INDEX ia(a))",
        3_001,
    );
    apply(&mut store, &create);
    let context = tidb_executor::StmtContext::for_query().with_enable_check_constraint(true);
    let lower = |sql: &str| {
        lower_ddl_with_context(&tidb_parser::parse(sql).unwrap(), "u6", &context)
            .unwrap()
            .unwrap()
    };
    let missing = lower("ALTER TABLE check_admission ADD CONSTRAINT cb CHECK(b>0)");
    let error =
        prepare_check_constraint_job_submission(&mut store, &missing, 3_002, false, 0).unwrap_err();
    let DdlPlanError::Admission(error) = error else {
        panic!("{error:?}")
    };
    assert_eq!(error.code, 1054);
    let shares_index_name = lower("ALTER TABLE check_admission ADD CONSTRAINT ia CHECK(a>0)");
    assert!(prepare_check_constraint_job_submission(
        &mut store,
        &shares_index_name,
        3_002,
        false,
        0
    )
    .unwrap()
    .is_some());
}

#[test]
fn grouped_drop_columns_remove_every_owned_index_range() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t(id INT PRIMARY KEY, a INT, b INT, KEY ia(a), KEY ib(b))",
        100,
    );
    apply(&mut store, &write);
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t DROP COLUMN a, DROP COLUMN b",
        200,
    );
    assert_eq!(write.backfill.len(), 2);
    for (work, name) in write.backfill.iter().zip(["ia", "ib"]) {
        assert!(matches!(work.operation, IndexBackfillOperation::Drop));
        assert_eq!(work.index.read().name.original(), name);
        assert!(work
            .table
            .indices
            .iter_deref()
            .any(|idx| idx.read().id == work.index.read().id));
    }
}

/// Go admits all specs before its shared MultiSchemaChange worker applies them.
#[test]
fn cluster_column_batch_retains_every_sibling_and_notification() {
    for sql in [
        "ALTER TABLE u6.t RENAME COLUMN a TO renamed, ADD COLUMN c INT DEFAULT 7, DROP COLUMN b",
        "ALTER TABLE u6.t ADD COLUMN c INT DEFAULT 7, CHANGE COLUMN a renamed BIGINT, DROP COLUMN b",
        "ALTER TABLE u6.t MODIFY COLUMN a BIGINT, ADD COLUMN c INT DEFAULT 7, RENAME COLUMN b TO renamed",
    ] {
        let mut store = bootstrapped();
        let create = plan(
            &mut store,
            "CREATE TABLE u6.t(id BIGINT PRIMARY KEY, a INT, b INT, KEY ia(a))",
            100,
        );
        let id = create.created_id.unwrap();
        apply(&mut store, &create);
        let write = plan(&mut store, sql, 200);
        assert_eq!(
            write.diff.action_type,
            ActionType::ACTION_MULTI_SCHEMA_CHANGE,
            "{sql}"
        );
        let stored = stored_table(&write, id);
        let names: Vec<_> = stored["cols"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"]["O"].as_str().unwrap())
            .collect();
        assert!(
            names.contains(&"c") && names.contains(&"renamed"),
            "{sql}: {names:?}"
        );
        let events = notifier_events(&mut store, &write);
        assert!(
            events
                .iter()
                .any(|event| event.1.action_type() == ActionType::ACTION_ADD_COLUMN),
            "{sql}"
        );
        assert!(
            events
                .iter()
                .any(|event| event.1.action_type() == ActionType::ACTION_MODIFY_COLUMN),
            "{sql}"
        );
        assert_eq!(
            stored["cols"][1]["id"], 2,
            "column identity survives siblings"
        );
    }
}

#[test]
fn cluster_column_batch_admits_original_names_before_execution() {
    for (sql, code) in [
        (
            "ALTER TABLE u6.t RENAME COLUMN a TO renamed, RENAME COLUMN renamed TO last_name",
            1054,
        ),
        ("ALTER TABLE u6.t ADD COLUMN c INT, ADD INDEX ic(c)", 1072),
        (
            "ALTER TABLE u6.t RENAME COLUMN a TO renamed, ADD INDEX ia(a)",
            8200,
        ),
        (
            "ALTER TABLE u6.t MODIFY COLUMN a BIGINT, DROP COLUMN a",
            8200,
        ),
        (
            "ALTER TABLE u6.t ADD COLUMN c INT AFTER a, DROP COLUMN a",
            8200,
        ),
    ] {
        let mut store = bootstrapped();
        let create = plan(
            &mut store,
            "CREATE TABLE u6.t(id BIGINT PRIMARY KEY,a INT,b INT)",
            100,
        );
        apply(&mut store, &create);
        let before = store.pairs.clone();
        let error = plan_ddl(&mut store, &statement(sql), 200).expect_err(sql);
        assert_eq!(error.to_sql_error().code, code, "{sql}: {error}");
        assert_eq!(store.pairs, before, "admission must not mutate storage");
    }
}

#[test]
fn cluster_column_batch_rejects_reorg_before_publishing_siblings() {
    for sql in [
        "ALTER TABLE u6.t MODIFY a INT, ADD COLUMN c INT",
        "ALTER TABLE u6.t CHANGE a renamed INT, ADD COLUMN c INT",
        "ALTER TABLE u6.t MODIFY s VARCHAR(2), DROP COLUMN a",
    ] {
        let mut store = bootstrapped();
        let create = plan(
            &mut store,
            "CREATE TABLE u6.t(id BIGINT PRIMARY KEY,a BIGINT,s VARCHAR(10))",
            100,
        );
        apply(&mut store, &create);
        let error = plan_ddl(&mut store, &statement(sql), 200).expect_err(sql);
        assert_eq!(error.to_sql_error().code, 8200, "{sql}: {error}");
    }
}

#[test]
fn cluster_column_batch_conditional_noops_keep_notes_and_jobs() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE u6.t(id BIGINT PRIMARY KEY,a INT,b INT)",
        100,
    );
    let id = create.created_id.unwrap();
    apply(&mut store, &create);
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN IF NOT EXISTS a INT, DROP COLUMN a, RENAME COLUMN b TO c",
        200,
    );
    let stored = stored_table(&write, id);
    assert_eq!(stored["cols"].as_array().unwrap().len(), 2);
    assert_eq!(stored["cols"][1]["name"]["O"], "c");
    assert!(write.warnings.iter().any(|warning| warning.1 == 1060));
    let noop = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t RENAME COLUMN a TO a, DROP COLUMN IF EXISTS missing"),
        300,
    )
    .unwrap();
    let DdlPlan::AlreadySatisfied { warnings, .. } = noop else {
        panic!("no jobs to publish")
    };
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].1, 1091);
}

#[test]
fn cluster_column_batch_positions_keep_index_identity() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE u6.t(id BIGINT PRIMARY KEY,a INT,b INT,KEY ia(a))",
        100,
    );
    let id = create.created_id.unwrap();
    apply(&mut store, &create);
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN c INT FIRST, CHANGE a renamed BIGINT AFTER b",
        200,
    );
    let stored = stored_table(&write, id);
    let names: Vec<_> = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"]["O"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["c", "id", "b", "renamed"]);
    assert_eq!(
        stored["index_info"][0]["idx_cols"][0]["name"]["O"],
        "renamed"
    );
    assert_eq!(stored["index_info"][0]["idx_cols"][0]["offset"], 3);
    assert_eq!(stored["cols"][3]["id"], 2);
}
