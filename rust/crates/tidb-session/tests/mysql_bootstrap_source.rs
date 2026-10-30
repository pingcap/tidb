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

//! A bootstrap is only real if this node's OWN readers can read it back.
//!
//! Every assertion here goes through the code paths that serve a live cluster —
//! [`tidb_infoschema::cluster_catalog::load_cluster_catalog`],
//! [`tidb_privilege::cluster_privilege_load::read_bootstrap_state`], and
//! [`tidb_privilege::cluster_privilege_load::load_cluster_privileges`] — rather than
//! inspecting the mutation list. Writing bytes only this test understands would
//! prove nothing.
//!
//! This file pins the NEXT-GEN layout (`BootstrapLayout::NextGen`: reserved
//! ids, the tables written straight into meta, four DDL jobs); the Classic
//! layout's ids, versions and history are `mysql_bootstrap_classic_source`'s.

use std::collections::BTreeMap;

use tidb_datatype::{Time, TimeType};
use tidb_infoschema::cluster_catalog::{
    load_cluster_catalog, ClusterCatalogError, MetaPairs, MetaSnapshot,
};
use tidb_privilege::cluster_privilege_load::{
    load_cluster_privileges, read_bootstrap_state, read_system_tz, ClusterBootstrapState,
};
use tidb_session::mysql_bootstrap::{
    plan_mysql_bootstrap_with_layout, BootstrapEnvironment, BootstrapError, BootstrapLayout,
    BootstrapWrite, BOOTSTRAP_VIEW_COUNT,
};
use tidb_meta::key;
use tidb_metadef::{BOOTSTRAP_TABLES, DDL_TABLE_VERSION_TABLES};
use tidb_txnkv::transaction::BufferMutationOp;

/// Go master's materialized-view tables, which this tree's Go does not create.
const MATERIALIZED_VIEW_TABLES: [&str; 5] = [
    "tidb_mview_refresh_info",
    "tidb_mlog_purge_info",
    "tidb_mview_refresh_hist",
    "tidb_mview_refresh_alert",
    "tidb_mlog_purge_hist",
];

/// The next-gen plan, which is the layout every test here is about.
fn plan_mysql_bootstrap(
    snapshot: &mut MetaStore,
    start_ts: u64,
    environment: &BootstrapEnvironment,
) -> Result<BootstrapWrite, BootstrapError> {
    plan_mysql_bootstrap_with_layout(snapshot, start_ts, environment, BootstrapLayout::NextGen)
}

/// One fixed environment, so every plan here is byte-for-byte reproducible.
fn environment() -> BootstrapEnvironment {
    BootstrapEnvironment {
        system_tz: "Asia/Shanghai".to_owned(),
        new_collation_enabled: true,
        cluster_id: 7_667_705_271_188_879_689,
        current_timestamp: Time::from_date_checked(
            2026,
            7,
            29,
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

/// Applies one planned bootstrap, the way a committed transaction would.
fn apply(store: &mut MetaStore, write: &BootstrapWrite) {
    for mutation in &write.mutations {
        match mutation.kind() {
            BufferMutationOp::Delete => {
                store.pairs.remove(mutation.key());
            }
            _ => {
                store
                    .pairs
                    .insert(mutation.key().to_vec(), mutation.value().to_vec());
            }
        }
    }
}

fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    let write = plan_mysql_bootstrap(&mut store, 467_996_279_696_261_139, &environment())
        .expect("a fresh keyspace bootstraps");
    apply(&mut store, &write);
    store
}

#[test]
fn a_bootstrapped_keyspace_loads_back_as_a_catalog_with_every_mysql_table() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "mysql")
        .expect("the bootstrap created the mysql database");
    // The five materialized-view tables are Go master's, not this tree's.
    let all_tables = DDL_TABLE_VERSION_TABLES
        .iter()
        .flat_map(|version| version.tables)
        .chain(
            BOOTSTRAP_TABLES
                .iter()
                .filter(|table| !MATERIALIZED_VIEW_TABLES.contains(&table.name)),
        );
    // Go bootstrap also creates the MDL view, represented by the catalog owner.
    assert!(database
        .tables
        .iter()
        .any(|table| table.name.lowercase() == "tidb_mdl_view" && table.view.is_some()));
    assert_eq!(database.tables.len(), all_tables.clone().count() + 1);
    for table in all_tables {
        assert!(
            database
                .tables
                .iter()
                .any(|stored| stored.name.lowercase() == table.name && stored.id == table.id),
            "mysql.{} is missing from the loaded catalog",
            table.name
        );
    }
}

#[test]
fn bootstrap_creates_all_ddl_tables_and_advances_their_version() {
    let mut store = bootstrapped();
    assert_eq!(
        tidb_session::mysql_bootstrap::read_ddl_table_version(&mut store)
            .expect("DDL table version reads"),
        4
    );
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    let state = read_bootstrap_state(&mut store, &catalog).expect("bootstrap state reads");
    assert!(state.already_bootstrapped());
    let mysql = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "mysql")
        .expect("mysql database");
    let notifier = mysql
        .tables
        .iter()
        .find(|table| table.name.lowercase() == "tidb_ddl_notifier")
        .expect("DDL notifier table");
    assert_eq!(notifier.id, tidb_metadef::system::TI_DBDDLNOTIFIER_TABLE_ID);
}

#[test]
fn the_bootstrap_marker_is_the_one_the_detector_reads() {
    // This is the whole point of writing `mysql.tidb`: the detector that
    // decides whether to bootstrap at all must see it as bootstrapped.
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    let state = read_bootstrap_state(&mut store, &catalog).expect("the marker reads back");
    assert!(state.already_bootstrapped(), "{state}");
    assert!(matches!(
        state,
        ClusterBootstrapState::Bootstrapped { version: Some(_) }
    ));
}

#[test]
fn the_seeded_system_timezone_is_the_one_startup_reads() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    assert_eq!(
        read_system_tz(&mut store, &catalog).expect("system_tz reads back"),
        "Asia/Shanghai"
    );
}

#[test]
fn the_seeded_root_account_loads_back_as_an_unlocked_superuser() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    let privileges =
        load_cluster_privileges(&mut store, &catalog).expect("the seed row reads back");
    let [root] = privileges.users.as_slice() else {
        panic!(
            "a bootstrap seeds exactly one account, got {:?}",
            privileges.users.len()
        );
    };
    assert_eq!(root.host, "%");
    assert_eq!(root.user, "root");
    // An EMPTY password, not a missing column: Go's own bootstrap stores the
    // empty string, and that is what makes a fresh cluster reachable.
    assert_eq!(root.authentication_string, "");
    assert_eq!(root.plugin, "mysql_native_password");
    assert!(!root.account_locked);
    assert!(
        root.privileges.contains(&"OPERATE VIEW"),
        "the bootstrap root row must include Go master's OPERATE VIEW grant"
    );
}

#[test]
fn a_second_bootstrap_is_refused_rather_than_layered_on_top() {
    let mut store = bootstrapped();
    let error = plan_mysql_bootstrap(&mut store, 1, &environment())
        .expect_err("a bootstrapped keyspace is refused");
    assert!(
        matches!(error, BootstrapError::AlreadyPresent { .. }),
        "{error}"
    );
    // A keyspace where only the DATABASE survived — a bootstrap that died
    // halfway — must be refused too, or the retry writes a second copy. The
    // database is found by name.
    let mut half = MetaStore::default();
    let database = tidb_model::DBInfo {
        id: tidb_metadef::system::SYSTEM_DATABASE_ID,
        name: tidb_ast::CiString::new("mysql"),
        state: tidb_model::SchemaState::PUBLIC,
        ..tidb_model::DBInfo::default()
    };
    half.pairs.insert(
        key::database_kv_key(tidb_metadef::system::SYSTEM_DATABASE_ID),
        tidb_meta::value::serialize_db_info(&database).unwrap(),
    );
    let error = plan_mysql_bootstrap(&mut half, 1, &environment())
        .expect_err("a half-bootstrapped keyspace is refused");
    assert!(
        matches!(error, BootstrapError::AlreadyPresent { .. }),
        "{error}"
    );
}

/// Go's next-gen bootstrap writes every table straight into meta, with no
/// schema version, and spends one version on each of its four DDL jobs: the
/// two views, `test` and the resource group.
#[test]
fn a_next_gen_bootstrap_spends_one_schema_version_per_ddl_job() {
    let mut store = MetaStore::default();
    store
        .pairs
        .insert(key::schema_version_kv_key(), b"60".to_vec());
    let write =
        plan_mysql_bootstrap(&mut store, 7, &environment()).expect("the keyspace bootstraps");
    assert_eq!(write.schema_version, 64);
    assert_eq!(write.diff.version, 64);
    assert_eq!(
        write.diff.action_type,
        tidb_model::action_type::ActionType::ACTION_ALTER_RESOURCE_GROUP
    );
    apply(&mut store, &write);
    assert_eq!(store.pairs[&key::schema_version_kv_key()], b"64");
    for version in 61..=64 {
        assert!(
            store.pairs.contains_key(&key::schema_diff_kv_key(version)),
            "version {version} has a diff"
        );
    }
    // The four jobs are the bootstrap's whole history.
    assert_eq!(BOOTSTRAP_VIEW_COUNT, 2);
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    let history = tidb_ddl::ddl_history_table::DdlHistoryTable::locate(&catalog).unwrap();
    assert_eq!(history.load(&mut store).unwrap().len(), 4);
    // Go draws an id per object and per job from 1: the views are 1 and 3,
    // `test` is 5, the jobs are 2, 4, 6 and 7.
    let (mysql, mdl_view) = catalog.find_table("mysql", "tidb_mdl_view").unwrap();
    assert_eq!(mysql.id, tidb_metadef::system::SYSTEM_DATABASE_ID);
    assert_eq!(mdl_view.id, 1);
    let (sys, sys_view) = catalog
        .find_table("sys", "schema_unused_indexes")
        .unwrap();
    assert_eq!(sys.id, tidb_metadef::system::SYS_DATABASE_ID);
    assert_eq!(sys_view.id, 3);
    let test = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "test")
        .unwrap();
    assert_eq!(test.info.id, 5);
    assert_eq!(store.pairs[&key::next_global_id_kv_key()], b"7");
}

/// Every next-gen table's stored `TableInfo` equals what the Go bootstrap
/// built with `-tags nextgen` stores, but for the creation timestamp and the
/// empty index list a Rust-created view stores as `[]`.
#[test]
fn next_gen_table_infos_are_what_go_stores() {
    const GO: &str = include_str!("data/mysql_bootstrap_nextgen_tableinfos.json");
    let mut go: std::collections::BTreeMap<String, serde_json::Value> =
        serde_json::from_str(GO).unwrap();
    let write = plan_mysql_bootstrap(&mut MetaStore::default(), 7, &environment())
        .expect("the keyspace bootstraps");
    let mut seen = 0;
    for mutation in &write.mutations {
        let Ok((hash, field)) = tidb_tablecodec::table_key::decode_meta_key(mutation.key()) else {
            continue;
        };
        if !field.starts_with(b"Table:") || !hash.starts_with(b"DB:") {
            continue;
        }
        let mut info: serde_json::Value = serde_json::from_slice(mutation.value()).unwrap();
        let database = match hash.as_slice() {
            h if h == format!("DB:{}", tidb_metadef::system::SYSTEM_DATABASE_ID).as_bytes() => {
                "mysql"
            }
            h if h == format!("DB:{}", tidb_metadef::system::SYS_DATABASE_ID).as_bytes() => "sys",
            other => panic!("unexpected table under {}", String::from_utf8_lossy(other)),
        };
        let name = format!("{database}.{}", info["name"]["L"].as_str().unwrap());
        let mut expected = go
            .remove(&name)
            .unwrap_or_else(|| panic!("{name} is not a table of the next-gen Go bootstrap"));
        for value in [&mut info, &mut expected] {
            value["update_timestamp"] = serde_json::Value::from(0);
            if value["index_info"] == serde_json::Value::Array(Vec::new()) {
                value["index_info"] = serde_json::Value::Null;
            }
        }
        assert_eq!(info, expected, "{name}");
        seen += 1;
    }
    assert_eq!(seen, 62);
    assert!(go.is_empty(), "never created: {:?}", go.keys());
}

/// The next-gen layout against a Go bootstrap built with `-tags nextgen`
/// (a throwaway `session` test over `mockstore`, whose meta it listed):
/// every table's id and clustered-handle flag, the database ids, the counters
/// and the four bootstrap jobs.
#[test]
fn the_next_gen_layout_is_what_go_creates() {
    const GO: &str = include_str!("data/mysql_bootstrap_nextgen_layout.json");
    let go: serde_json::Value = serde_json::from_str(GO).unwrap();
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    let mut seen = 0;
    for database in &catalog.databases {
        let name = database.info.name.lowercase().to_owned();
        if !["mysql", "sys", "test"].contains(&name.as_str()) {
            continue;
        }
        assert_eq!(
            go["databases"][&name].as_i64(),
            Some(database.info.id),
            "database {name}"
        );
        for table in &database.tables {
            let full = format!("{name}.{}", table.name.lowercase());
            let expected = &go["tables"][&full];
            assert_eq!(expected["id"].as_i64(), Some(table.id), "{full} id");
            assert_eq!(
                expected["common_handle"].as_bool(),
                Some(table.is_common_handle),
                "{full} common handle"
            );
            assert_eq!(
                expected["view"].as_bool(),
                Some(table.view.is_some()),
                "{full}"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, go["tables"].as_object().unwrap().len());
    assert_eq!(
        catalog.schema_version,
        go["head"]["schema_version"].as_i64().unwrap()
    );
    assert_eq!(
        store.pairs[&key::next_global_id_kv_key()],
        go["head"]["next_global_id"].to_string().into_bytes()
    );
    let history = tidb_ddl::ddl_history_table::DdlHistoryTable::locate(&catalog).unwrap();
    let jobs = history.load(&mut store).unwrap();
    let expected = go["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), expected.len());
    for (job, expected) in jobs.iter().zip(expected) {
        assert_eq!(expected["id"].as_i64(), Some(job.id));
        assert_eq!(
            expected["type"].as_str(),
            Some(job.type_.to_string().as_str())
        );
        assert_eq!(expected["schema_id"].as_i64(), Some(job.schema_id));
        assert_eq!(expected["table_id"].as_i64(), Some(job.table_id));
        assert_eq!(
            expected["schema_name"].as_str(),
            Some(job.schema_name.to_string().as_str())
        );
        assert_eq!(
            expected["table_name"].as_str(),
            Some(job.table_name.to_string().as_str())
        );
    }
}

#[test]
fn every_seeded_row_carries_the_index_entries_its_table_declares() {
    // `mysql.user` and `mysql.tidb` are both NON-clustered, so a row with no
    // index entry is a row half of TiDB cannot find. Nothing backfills them
    // later, so the bootstrap that writes the row must write them.
    let write = plan_mysql_bootstrap(&mut MetaStore::default(), 7, &environment())
        .expect("the keyspace bootstraps");
    let index_entries = write
        .mutations
        .iter()
        .filter(|mutation| tidb_tablecodec::table_key::is_index_key(mutation.key()))
        .count();
    let records = write
        .mutations
        .iter()
        .filter(|mutation| tidb_tablecodec::is_record_key(mutation.key()))
        .count();
    // One `mysql.user` row, six `mysql.tidb` rows and one
    // `mysql.global_variables` row per global-scope system variable in the
    // captured fixture. Their primary keys are common handles (the key IS the
    // record, no index entry), so only `mysql.user`'s `i_user` and the builtin
    // binding lock row's three indexes (sql_index, time_index, digest_index)
    // remain. The four DDL jobs file four `mysql.tidb_ddl_history` rows, whose
    // integer primary key is the handle.
    const GLOBAL_VARIABLES_FIXTURE: &str =
        include_str!("../src/mysql_bootstrap/global_variables_fixture.tsv");
    assert!(GLOBAL_VARIABLES_FIXTURE
        .lines()
        .any(|line| line == "tidb_workload_repository_snapshot_interval\t3600"));
    let global_variable_rows = GLOBAL_VARIABLES_FIXTURE
        .lines()
        .filter(|line| !line.is_empty())
        .count();
    assert_eq!(records, 8 + global_variable_rows + 4);
    assert_eq!(index_entries, 4);
}
