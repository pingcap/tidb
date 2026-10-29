// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The Classic kernel's bootstrap layout, against what a classic Go
//! `tidb-server --store=unistore` bootstrap leaves behind.
//!
//! The fixtures under `tests/data` are read off that server:
//! `mysql_bootstrap_classic_tableinfos.json` is the `TableInfo` of every
//! `mysql.*` and `sys.*` table from its `/schema/<db>` status endpoint, and
//! `mysql_bootstrap_classic_jobs.json` is the envelope of each of the 58 rows
//! `mysql.tidb_ddl_history` holds after bootstrap. The ids are the ones
//! `information_schema.tables` (`TIDB_TABLE_ID`) and `ADMIN SHOW DDL JOBS`
//! report: `mysql` is database 1, `sys` is 2, the 53 `mysql.*` tables are the
//! even ids 4..=108 and each table's job is the odd id after it, the two
//! views are 110 and 112, `test` is 114, the resource group job is 116 and the
//! first user object is 117; `ADMIN SHOW DDL` reports schema version 58, one
//! per job.

use std::collections::BTreeMap;

use serde_json::Value;
use tidb_datatype::{Time, TimeType};
use tidb_infoschema::cluster_catalog::{
    load_cluster_catalog, ClusterCatalogError, MetaPairs, MetaSnapshot,
};
use tidb_ddl::cluster_ddl::add_index::AddIndexSubmission;
use tidb_ddl::cluster_ddl::{
    lower_ddl_with_context, plan_persisted_ddl_job_step, DdlJobSchemaState, PersistedDdlJobPlan,
};
use tidb_ddl::ddl_history_table::DdlHistoryTable;
use tidb_ddl::ddl_job_submit::{plan_insert_attempt, JobSpec};
use tidb_session::mysql_bootstrap::{
    plan_mysql_bootstrap_with_layout, BootstrapEnvironment, BootstrapLayout, BootstrapWrite,
};
use tidb_ddl::real_tikv_ddl::prepare_persisted_job_submission;
use tidb_meta::key;
use tidb_metadef::BOOTSTRAP_TABLES;
use tidb_txnkv::transaction::{BufferMutation, BufferMutationOp};

const GO_TABLE_INFOS: &str = include_str!("data/mysql_bootstrap_classic_tableinfos.json");
const GO_JOBS: &str = include_str!("data/mysql_bootstrap_classic_jobs.json");

/// The tables of Go master's materialized-view version, which this tree's Go
/// does not create.
const MATERIALIZED_VIEW_TABLES: [&str; 5] = [
    "tidb_mview_refresh_info",
    "tidb_mlog_purge_info",
    "tidb_mview_refresh_hist",
    "tidb_mview_refresh_alert",
    "tidb_mlog_purge_hist",
];

fn environment() -> BootstrapEnvironment {
    BootstrapEnvironment {
        system_tz: "Asia/Shanghai".to_owned(),
        new_collation_enabled: true,
        cluster_id: 1,
        current_timestamp: Time::from_date_checked(
            2026,
            10,
            21,
            6,
            12,
            55,
            0,
            TimeType::Timestamp,
            0,
        )
        .expect("a fixed calendar date is a valid timestamp"),
        ddl_table_version: 0,
    }
}

#[derive(Default)]
struct MetaStore {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MetaSnapshot for MetaStore {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        Ok(self
            .pairs
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

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

fn bootstrapped() -> (MetaStore, BootstrapWrite) {
    bootstrapped_with(BootstrapLayout::Classic)
}

fn bootstrapped_with(layout: BootstrapLayout) -> (MetaStore, BootstrapWrite) {
    let mut store = MetaStore::default();
    let write = plan_mysql_bootstrap_with_layout(
        &mut store,
        467_996_279_696_261_139,
        &environment(),
        layout,
    )
    .expect("a fresh keyspace bootstraps");
    apply_mutations(&mut store, &write.mutations);
    (store, write)
}

fn int_key(store: &mut MetaStore, raw_key: Vec<u8>) -> i64 {
    let stored = store
        .get(&raw_key)
        .unwrap()
        .expect("the bootstrap wrote the counter");
    tidb_meta::value::parse_int_value(&stored).expect("an integer counter")
}

#[test]
fn ids_and_versions_are_the_classic_go_ones() {
    let (mut store, write) = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let database = |name: &str| {
        catalog
            .databases
            .iter()
            .find(|database| database.info.name.lowercase() == name)
            .unwrap_or_else(|| panic!("the bootstrap created {name}"))
    };
    assert_eq!(database("mysql").info.id, 1);
    assert_eq!(database("sys").info.id, 2);
    assert_eq!(database("test").info.id, 114);

    let mysql = database("mysql");
    let created = BOOTSTRAP_TABLES
        .iter()
        .filter(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name));
    for (position, table) in created.enumerate() {
        let stored = mysql
            .tables
            .iter()
            .find(|stored| stored.name.lowercase() == table.name)
            .unwrap_or_else(|| panic!("mysql.{} was created", table.name));
        assert_eq!(stored.id, 4 + 2 * position as i64, "mysql.{}", table.name);
    }
    let view = |db: &str, name: &str| {
        database(db)
            .tables
            .iter()
            .find(|table| table.name.lowercase() == name)
            .unwrap_or_else(|| panic!("{db}.{name} was created"))
            .id
    };
    assert_eq!(view("mysql", "tidb_mdl_view"), 110);
    assert_eq!(view("sys", "schema_unused_indexes"), 112);
    // The DDL-owned tables keep their reserved ids under both kernels.
    assert_eq!(view("mysql", "tidb_ddl_job"), 281_474_976_710_654);
    assert_eq!(view("mysql", "tidb_ddl_notifier"), 281_474_976_710_648);

    assert_eq!(catalog.schema_version, 58);
    assert_eq!(write.schema_version, 58);
    assert_eq!(int_key(&mut store, key::next_global_id_kv_key()), 116);
    // The five materialized-view tables are not Go's, in this layout.
    assert!(mysql
        .tables
        .iter()
        .all(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name.lowercase())));
    // `InitDDLTables` leaves no schema version of its own: version 1 is `sys`.
    let first_diff = store
        .get(&key::schema_diff_kv_key(1))
        .unwrap()
        .expect("version 1 has a diff");
    let first_diff = tidb_meta::value::parse_schema_diff(&first_diff)
        .unwrap()
        .expect("a stored diff");
    assert_eq!(first_diff.schema_id, 2);
}

/// Every bootstrap table lowers to the `TableInfo` the classic server
/// stores, but for the creation timestamp and for one residual shared with
/// every Rust-created view: a view's empty index list is `[]`, not Go's nil.
#[test]
fn table_infos_are_what_the_classic_go_server_stores() {
    let (_, write) = bootstrapped();
    let mut go: BTreeMap<String, Value> = serde_json::from_str(GO_TABLE_INFOS).unwrap();
    let mut seen = 0;
    for mutation in &write.mutations {
        let Ok((hash, field)) = tidb_meta::structure::decode_hash_data_key(mutation.key()) else {
            continue;
        };
        if !field.starts_with(b"Table:") || !hash.starts_with(b"DB:") {
            continue;
        }
        let mut info: Value = serde_json::from_slice(mutation.value()).unwrap();
        let database = match hash.as_slice() {
            b"DB:1" => "mysql",
            b"DB:2" => "sys",
            other => panic!("unexpected table under {}", String::from_utf8_lossy(other)),
        };
        let name = format!("{database}.{}", info["name"]["L"].as_str().unwrap());
        let mut expected = go
            .remove(&name)
            .unwrap_or_else(|| panic!("{name} is not a table of the classic Go bootstrap"));
        for value in [&mut info, &mut expected] {
            value["update_timestamp"] = Value::from(0);
            if value["index_info"] == Value::Array(Vec::new()) {
                value["index_info"] = Value::Null;
            }
        }
        assert_eq!(info, expected, "{name}");
        seen += 1;
    }
    assert_eq!(seen, 62);
    assert!(go.is_empty(), "never created: {:?}", go.keys());
}

/// One history row per job, carrying the envelope the classic server's does.
#[test]
fn history_rows_are_the_classic_go_jobs() {
    let (mut store, _) = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let history = DdlHistoryTable::locate(&catalog).unwrap();
    let jobs = history.load(&mut store).unwrap();
    let go: BTreeMap<String, Value> = serde_json::from_str(GO_JOBS).unwrap();
    assert_eq!(jobs.len(), go.len());
    let mut previous_sequence = None;
    for mut job in jobs {
        let expected = &go[&job.id.to_string()];
        let actual: Value = serde_json::from_slice(&job.encode(false).unwrap()).unwrap();
        for field in [
            "id",
            "type",
            "schema_id",
            "table_id",
            "schema_name",
            "table_name",
            "state",
            "schema_state",
            "version",
            "query",
            "sql_mode",
            "last_schema_version",
        ] {
            assert_eq!(actual[field], expected[field], "job {} `{field}`", job.id);
        }
        assert_eq!(
            actual["binlog"]["SchemaVersion"], expected["schema_version"],
            "job {}",
            job.id
        );
        assert_eq!(
            actual.get("involving_schema_info"),
            expected.get("involving_schema_info"),
            "job {}",
            job.id
        );
        // Go numbers the history from 1 in finishing order; Rust's counter is
        // the process's, so the order is what a bootstrap can promise.
        let sequence = actual["seq_num"].as_i64().unwrap();
        if let Some(previous) = previous_sequence {
            assert_eq!(sequence, previous + 1, "job {}", job.id);
        }
        previous_sequence = Some(sequence);
    }
}

/// Go leaves the job queue, the reorganization table, the MDL table and the
/// notifier empty once the bootstrap's jobs are done.
#[test]
fn no_job_state_outlives_the_bootstrap() {
    let (store, _) = bootstrapped();
    for (name, id) in [
        ("tidb_ddl_job", 281_474_976_710_654_i64),
        ("tidb_ddl_reorg", 281_474_976_710_653),
        ("tidb_mdl_info", 281_474_976_710_651),
        ("tidb_ddl_notifier", 281_474_976_710_648),
    ] {
        let prefix = tidb_codec::table_key::gen_table_prefix(id);
        assert!(
            store.pairs.keys().all(|key| !key.starts_with(&prefix)),
            "mysql.{name} must be empty"
        );
    }
}

/// The first statement after the bootstrap draws the ids Go's does: a table
/// 117 and its job 118.
#[test]
fn the_first_user_table_is_117() {
    let (mut store, _) = bootstrapped();
    let sql = "create table test.t1(a int primary key, b varchar(10))";
    let context = tidb_sessionctx::StmtContext::default().with_ddl_query(sql);
    let parsed = tidb_parser::parse(sql).unwrap();
    let statement = lower_ddl_with_context(&parsed, "test", &ddl_context(&context))
        .unwrap()
        .unwrap();
    let submission = prepare_persisted_job_submission(
        &mut store,
        &statement,
        &context,
        &[],
        467_996_279_696_262_000,
        false,
        0,
        0,
        // A store without TiFlash.
        &|| Ok(0),
        &mut |_, _| Ok(false),
    )
    .unwrap();
    let Some(AddIndexSubmission::Job(mut spec, _)) = submission else {
        panic!("CREATE TABLE queues a job");
    };
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let mut before_insert = |_: &[JobSpec]| Option::<fn()>::None;
    let (mutations, _) = plan_insert_attempt(
        &mut store,
        &catalog,
        std::slice::from_mut(&mut spec),
        &mut before_insert,
    )
    .unwrap();
    apply_mutations(&mut store, &mutations);
    assert_eq!(spec.job.id, 118);
    let mut version = 0;
    for tick in 1..=2 {
        let plan = plan_persisted_ddl_job_step(
            &mut store,
            118,
            467_996_279_696_262_000 + tick,
            DdlJobSchemaState::new(true),
            &|| tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT,
            None,
        )
        .unwrap();
        let PersistedDdlJobPlan::Step(step) = plan else {
            panic!("the job needs a worker step");
        };
        apply_mutations(&mut store, &step.write.mutations);
        version = version.max(step.write.schema_version);
    }
    assert_eq!(version, 59);
    let catalog = load_cluster_catalog(&mut store).unwrap();
    let (database, table) = catalog.find_table("test", "t1").expect("t1 was created");
    assert_eq!((database.id, table.id), (114, 117));
}

/// A node joining a bootstrapped cluster of either layout reads `mysql.*` by
/// name: the bootstrap marker, the system time zone, the seeded account and
/// the tables the DDL owner and the advisory locks use.
#[test]
fn a_keyspace_of_either_layout_is_served_by_name() {
    use tidb_privilege::cluster_privilege_load::{
        load_cluster_privileges, read_bootstrap_state, read_system_tz,
    };

    for layout in [BootstrapLayout::Classic, BootstrapLayout::NextGen] {
        let (mut store, _) = bootstrapped_with(layout);
        let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
        let state = read_bootstrap_state(&mut store, &catalog).expect("the marker reads back");
        assert!(state.already_bootstrapped(), "{layout:?}: {state}");
        assert_eq!(
            read_system_tz(&mut store, &catalog).expect("system_tz reads back"),
            "Asia/Shanghai"
        );
        let privileges =
            load_cluster_privileges(&mut store, &catalog).expect("the seed row reads back");
        let [root] = privileges.users.as_slice() else {
            panic!("a bootstrap seeds exactly one account");
        };
        assert_eq!((root.user.as_str(), root.host.as_str()), ("root", "%"));
        tidb_ddl::ddl_job_table::DdlJobTable::locate(&catalog).expect("mysql.tidb_ddl_job");
        DdlHistoryTable::locate(&catalog).expect("mysql.tidb_ddl_history");

        let advisory = tidb_syssession::advisory_locks::AdvisoryLockTable::locate(&mut store)
            .expect("mysql.advisory_locks locates by name");
        let table = tidb_syssession::advisory_locks::find_system_table(&mut store, "advisory_locks")
            .unwrap()
            .expect("mysql.advisory_locks");
        if layout == BootstrapLayout::Classic {
            assert_eq!(table.id, 66);
        } else {
            assert_eq!(table.id, tidb_metadef::system::ADVISORY_LOCKS_TABLE_ID);
        }
        let primary = table
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                index.primary.then_some(index.id)
            })
            .expect("the primary index");
        // A clustered primary key owns no index entry: the lock is the
        // record's key.
        let prefix = if table.is_common_handle {
            tidb_codec::gen_table_record_prefix(table.id)
        } else {
            tidb_codec::table_key::encode_index_seek_key(table.id, primary, &[])
        };
        assert!(advisory.key("rootlock").unwrap().starts_with(&prefix));
    }
}

/// A bootstrapped keyspace is refused, and so is one whose `mysql` database
/// survived a bootstrap that died halfway: found by name, as no id is
/// reserved for it.
#[test]
fn a_second_classic_bootstrap_is_refused() {
    use tidb_session::mysql_bootstrap::BootstrapError;

    let (mut store, _) = bootstrapped();
    let error =
        plan_mysql_bootstrap_with_layout(&mut store, 1, &environment(), BootstrapLayout::Classic)
            .expect_err("a bootstrapped keyspace is refused");
    assert!(
        matches!(error, BootstrapError::AlreadyPresent { .. }),
        "{error}"
    );

    let mut half = MetaStore::default();
    let database = tidb_model::DBInfo {
        id: 1,
        name: tidb_ast::CiString::new("mysql"),
        state: tidb_model::SchemaState::PUBLIC,
        ..tidb_model::DBInfo::default()
    };
    half.pairs.insert(
        key::database_kv_key(1),
        tidb_meta::value::serialize_db_info(&database).unwrap(),
    );
    let error =
        plan_mysql_bootstrap_with_layout(&mut half, 1, &environment(), BootstrapLayout::Classic)
            .expect_err("a half-bootstrapped keyspace is refused");
    assert!(
        matches!(error, BootstrapError::AlreadyPresent { .. }),
        "{error}"
    );
}

/// The shared session handle the DDL lowering keeps (Go's `sessionctx`).
fn ddl_context(context: &tidb_sessionctx::StmtContext) -> tidb_ddl::cluster_ddl::DdlContext {
    std::sync::Arc::new(context.clone())
}
