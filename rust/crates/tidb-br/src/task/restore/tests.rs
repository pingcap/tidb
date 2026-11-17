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

//! Go `br/pkg/task/restore_test.go`: the snapshot restore task's checks.

use tidb_ast::CiString;
use tidb_model::{ActionType, DBInfo, GoShared, Job, TableInfo, TiFlashReplicaInfo};
use tidb_proto::backup::File;

use super::*;
use crate::test_util::cluster::MockCluster;

const PB: u64 = 1 << 50;

fn job(type_: ActionType) -> Job {
    let mut j = Job::default();
    j.type_ = type_;
    j
}

/// A job on `schema_id` / `table_id` of `schema_name`, at its history.
fn job_on(
    type_: ActionType,
    schema_id: i64,
    table_id: i64,
    schema_name: &str,
    binlog: Option<GoShared<tidb_model::HistoryInfo>>,
) -> Job {
    let mut j = job(type_);
    j.schema_id = schema_id;
    j.table_id = table_id;
    j.schema_name = schema_name.into();
    j.binlog_info = binlog;
    j
}

fn types(jobs: &[Job]) -> Vec<ActionType> {
    jobs.iter().map(|j| j.type_).collect()
}

fn ddl_jobs() -> Vec<Job> {
    [
        ActionType::ACTION_SET_TI_FLASH_REPLICA,
        ActionType::ACTION_ADD_PRIMARY_KEY,
        ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS,
        ActionType::ACTION_CREATE_TABLE,
        ActionType::ACTION_LOCK_TABLE,
        ActionType::ACTION_ADD_INDEX,
        ActionType::ACTION_UNLOCK_TABLE,
        ActionType::ACTION_CREATE_SCHEMA,
        ActionType::ACTION_MODIFY_COLUMN,
    ]
    .into_iter()
    .map(job)
    .collect()
}

/// Go `TestFilterDDLJobByRules`.
#[test]
fn filter_ddl_job_by_block_list() {
    let jobs = filter_ddl_job_by_rules(ddl_jobs(), &[ddl_job_block_list_rule]);
    assert_eq!(
        types(&jobs),
        [
            ActionType::ACTION_ADD_PRIMARY_KEY,
            ActionType::ACTION_CREATE_TABLE,
            ActionType::ACTION_ADD_INDEX,
            ActionType::ACTION_CREATE_SCHEMA,
            ActionType::ACTION_MODIFY_COLUMN,
        ]
    );
}

/// Go `TestCheckDDLJobByRules`.
#[test]
fn check_ddl_job_by_incremental_rules() {
    let mut jobs = ddl_jobs();
    jobs.push(job(ActionType::ACTION_REORGANIZE_PARTITION));
    let filtered = filter_ddl_job_by_rules(
        jobs.clone(),
        &[ddl_job_log_incremental_compact_block_list_rule],
    );
    assert_eq!(
        types(&filtered),
        [
            ActionType::ACTION_SET_TI_FLASH_REPLICA,
            ActionType::ACTION_ADD_PRIMARY_KEY,
            ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS,
            ActionType::ACTION_CREATE_TABLE,
            ActionType::ACTION_LOCK_TABLE,
            ActionType::ACTION_UNLOCK_TABLE,
            ActionType::ACTION_CREATE_SCHEMA,
        ]
    );
    check_ddl_job_by_rules(
        &filtered,
        &[ddl_job_log_incremental_compact_block_list_rule],
    )
    .unwrap();
    check_ddl_job_by_rules(&jobs, &[ddl_job_log_incremental_compact_block_list_rule]).unwrap_err();
}

/// Go `TestTikvUsage`.
#[test]
fn tikv_usage() {
    let total: u64 = (1..=5).map(|n| n * PB).sum();
    assert_eq!(estimate_tikv_usage(total, 3, 6), 15 * PB * 3 / 6);
    assert_eq!(estimate_tikv_usage(total, 3, 0), 0);
}

fn sized_table(id: i64, replicas: u64, size: u64) -> metautil::Table {
    let mut t = metautil::Table::with_schema(
        DBInfo::default(),
        TableInfo {
            id,
            tiflash_replica: Some(GoShared::new(TiFlashReplicaInfo {
                count: replicas,
                ..Default::default()
            })),
            ..Default::default()
        },
    );
    t.files_of_physicals.insert(
        id,
        vec![File {
            size,
            ..Default::default()
        }],
    );
    t
}

/// Go `TestTiflashUsage`.
#[test]
fn tiflash_usage() {
    let tables = [
        sized_table(1, 0, PB),
        sized_table(2, 1, 2 * PB),
        sized_table(3, 2, 3 * PB),
    ];
    assert_eq!(estimate_tiflash_usage(&tables, 3), 8 * PB / 3);
}

/// Go `TestCheckTikvSpace`, and a store without room.
#[test]
fn check_tikv_space() {
    let mut store = tikv_client::pd_http::StoreInfo::default();
    store.store.id = 1;
    store.status.available = "500PB".into();
    check_store_space(400 * PB, &store).unwrap();
    let err = check_store_space(600 * PB, &store).unwrap_err();
    assert!(
        err.to_string()
            .contains("store 1 has no space left on device"),
        "{err}"
    );
    store.status.available = "garbage".into();
    check_store_space(1, &store).unwrap_err();
}

/// Go `TestHash`'s legacy storage identities: the fingerprints Go pins.
#[test]
fn hash_pins_legacy_storage_identity() {
    for (storage, hash) in [
        ("s3://bucket/prefix?endpoint=https%3A%2F%2FUSER%3APASS%40host%2F%3Fsig%3DPRIVATE", "fe3c78a5c211c56d07879b55a4fc8898c8c010914f23c57e9b5e414510044258"),
        ("s3://bucket/prefix?access-key=OLD&secret-access-key=OLD&session-token=OLD&unknown=PRIVATE#fragment", "233a506b0c6104b8e1a9f0b6b9037746b5feb666f660639f6377fdd506ddba00"),
        ("s3://bucket/prefix?access-key=NEW&secret-access-key=NEW&session-token=NEW&unknown=PRIVATE#fragment", "233a506b0c6104b8e1a9f0b6b9037746b5feb666f660639f6377fdd506ddba00"),
        ("ks3://bucket/prefix?Access_Key=OLD&region=us&region=eu&unknown=PRIVATE", "7c7fda3ac06086e5005499793032e3bb88b4023314a8388080e00a73f924b465"),
        ("oss://bucket/prefix?secret_access_key=OLD&unknown=PRIVATE", "4e2d4f12aa5f3f7fa100c1a51835795b10fb05e71b13638d2a51cd30d863848a"),
        ("azure://bucket/prefix?account-name=a&account-key=OLD&sas-token=OLD&encryption-key=OLD&endpoint=https%3A%2F%2Facct.example%2F%3Fsig%3DOLD&unknown=PRIVATE", "53c7b03446de70c03835d410bd1ef190d19cbf426f4fdc97694119969e540bd8"),
        ("azure://bucket/prefix?account-name=a&account-key=NEW&sas-token=NEW&encryption-key=NEW&endpoint=https%3A%2F%2Facct.example%2F%3Fsig%3DNEW&unknown=PRIVATE", "53c7b03446de70c03835d410bd1ef190d19cbf426f4fdc97694119969e540bd8"),
        ("azblob://bucket/prefix?sas-token=OLD&unknown=PRIVATE", "c393c116cfacdd72a17a362d6d77010bec115c1e8ed02c0239c84dcb8565b918"),
        ("gcs://USER:PASS@bucket/prefix?credentials-file=PRIVATE&endpoint=https://host#fragment", "04035e672cd2a23d49fbe665e32afd841137467cdce1aaec48e53988a49596f8"),
        ("gs://bucket/prefix?credentials-file=PRIVATE#fragment", "6b3cc728a592e0e19a3ea155bb91215ddc648ae5bf5f6f804986298e62d1a7bb"),
        ("local:///tmp/backup?token=PRIVATE#fragment", "0718bbd6d889bac22194be91cc6d839b2ee29eadc413e5473df5c1ac417c6186"),
        ("file:///tmp/backup?token=PRIVATE#fragment", "32c00f90ed7653acf8edb1c74330785dec41ae97a643cce2a6886d33b92eae84"),
        ("hdfs://USER:PASS@host/backup?token=PRIVATE#fragment", "5dad05b451f17eefcb775d59c6670ad23c3e3e9159f3efa8dd4376873320d467"),
        ("noop:///?token=PRIVATE#fragment", "3fc302be854edc6c384670a4fa657babeaa1039757f69b5560e7ed86c0a8a55a"),
        ("https://USER:PASS@host/backup?token=PRIVATE#fragment", "d8ef4d086065cf10124072fdb4b525312fd180d4c538b7ec07c1992333a2fdda"),
        ("/tmp/my files/100%off#name", "661e02efb345b11d525b1b74546af9304cecbf63d70b524b019dfdcc73825353"),
        ("s3://bucket/path%zz?access-key=PRIVATE", "8b0ebe72ce02bf2c4ac35186bbcd858128cdc0310f85bd96df344b450a53e4b5"),
        ("s3://bucket/prefix?sse=aws:kms&sse-kms-context=%7B%7D", "dc9ba73da5fb5694c1465c8d703e1f313363e13ba46ece58c334b2c7f945c386"),
        (r#"s3://bucket/prefix?sse=aws:kms&sse-kms-context={"a":"x&region=PRIVATE"}"#, "d08121c7e023c9f81050ea57d70db8afa8edaa999206b91d369c54b619733dda"),
        ("s3://bucket/prefix?unknown=PRIVATE%zz", "d08121c7e023c9f81050ea57d70db8afa8edaa999206b91d369c54b619733dda"),
        ("s3://bucket/prefix?", "0af7d308ff4fdd6ea242b8675c77c83d6610bd4f7aad4072626ee7f1185b7520"),
        ("gcs://bucket/prefix?", "14561c3b3a958e20ad879ffe3a4ea01c7f9af7b1448fc172b9360fb73de0d59c"),
    ] {
        let mut cfg = default_restore_config(super::super::common::default_config());
        cfg.config.storage = storage.to_owned();
        cfg.config.filter_str = vec!["source.*".to_owned()];
        cfg.upstream_cluster_id = 42;
        assert!(cfg.with_sys_table && cfg.fast_load_sys_tables && cfg.load_stats);
        assert_eq!(hex::encode(cfg.hash(FULL_RESTORE_CMD)), hash, "{storage}");
    }
}

/// A zero Go `RestoreConfig` but for its storage.
fn zero_config(storage: &str) -> RestoreConfig {
    let mut cfg = default_restore_config(super::super::common::default_config());
    cfg.config.storage = storage.to_owned();
    cfg.config.filter_str = Vec::new();
    cfg.with_sys_table = false;
    cfg.fast_load_sys_tables = false;
    cfg.load_stats = false;
    cfg
}

/// Go `TestHash`'s legacy storage equivalence: rotated credentials keep
/// the hash, anything else changes it.
#[test]
fn hash_legacy_storage_equivalence() {
    for (first, second, equal) in [
        (
            "s3://bucket/prefix?access-key=old",
            "s3://bucket/prefix?access-key=new",
            true,
        ),
        (
            "s3://bucket/prefix?access-key=old",
            "s3://bucket/prefix",
            false,
        ),
        (
            "s3://bucket/prefix?future-token=old",
            "s3://bucket/prefix?future-token=new",
            false,
        ),
        (
            "s3://bucket/prefix?endpoint=https://first",
            "s3://bucket/prefix?endpoint=https://second",
            false,
        ),
        (
            "gcs://bucket/prefix?credentials=old#fragment",
            "gcs://bucket/prefix?credentials=new#fragment",
            false,
        ),
        ("gcs://bucket/prefix", "gcs://bucket/prefix?", false),
        (
            "azure://bucket/prefix?endpoint=https://first",
            "azure://bucket/prefix?endpoint=https://second",
            true,
        ),
        ("s3://bucket/first%zz", "s3://bucket/second%zz", true),
    ] {
        let (a, b) = (
            zero_config(first).hash(FULL_RESTORE_CMD),
            zero_config(second).hash(FULL_RESTORE_CMD),
        );
        assert_eq!(a == b, equal, "{first} vs {second}");
    }
}

/// Go `TestHash`'s configuration cases without layouts or routes.
#[test]
fn hash_tracks_immutable_fields() {
    let base = || {
        let mut cfg = zero_config("default-storage");
        cfg.upstream_cluster_id = 1;
        cfg.config.explicit_filter = true;
        cfg.config.filter_str = vec!["filter1".into(), "filter2".into()];
        cfg.with_sys_table = true;
        cfg
    };
    let h = base().hash(FULL_RESTORE_CMD);
    let changed = |f: &dyn Fn(&mut RestoreConfig)| {
        let mut cfg = base();
        f(&mut cfg);
        cfg.hash(FULL_RESTORE_CMD) != h
    };
    assert!(!changed(&|_| {}));
    assert!(changed(&|c| c.upstream_cluster_id = 999));
    assert!(changed(&|c| c.config.storage = "new-storage".into()));
    assert!(!changed(&|c| c.config.explicit_filter = false));
    assert!(changed(&|c| c.config.filter_str.push("new-filter".into())));
    assert!(changed(&|c| c.config.filter_str.reverse()));
    assert!(changed(&|c| c.with_sys_table = false));
    assert_ne!(base().hash("Restore"), h, "the command is hashed");
}

fn db(name: &str, id: i64) -> DBInfo {
    DBInfo {
        id,
        name: CiString::new(name),
        ..Default::default()
    }
}

fn table(db: &DBInfo, id: i64, name: &str) -> metautil::Table {
    metautil::Table::with_schema(
        db.clone(),
        TableInfo {
            id,
            name: CiString::new(name),
            ..Default::default()
        },
    )
}

/// Go `VerifyDBAndTableInBackup`: the `restore db` / `restore table`
/// targets must be backed up.
#[test]
fn verify_db_and_table_in_backup_names() {
    let test = db("test", 1);
    let schemas = [metautil::Database::new(
        test.clone(),
        vec![table(&test, 2, "t")],
    )];
    let mut cfg = zero_config("");
    verify_db_and_table_in_backup(&schemas, &cfg).unwrap();
    cfg.config.schemas.insert("`test`".into());
    cfg.config.tables.insert("`test`.`t`".into());
    verify_db_and_table_in_backup(&schemas, &cfg).unwrap();
    cfg.config.tables.insert("`test`.`missing`".into());
    let err = verify_db_and_table_in_backup(&schemas, &cfg).unwrap_err();
    assert!(
        err.to_string()
            .contains("[table: `test`.`missing`] has not been backup"),
        "{err}"
    );
}

/// Go `FilterDDLJobs`: the jobs on a restored table under any of its IDs
/// or names, newest first; other databases' jobs are dropped.
#[test]
fn filter_ddl_jobs_follows_renames_and_truncates() {
    let test = db("test", 1);
    let binlog = |version: i64, db: Option<&DBInfo>, table: Option<TableInfo>| {
        Some(GoShared::new(tidb_model::HistoryInfo {
            schema_version: version,
            db_info: db.map(|d| GoShared::new(d.clone())),
            table_info: table.map(GoShared::new),
            ..Default::default()
        }))
    };
    let info = |id: i64, name: &str| TableInfo {
        id,
        name: CiString::new(name),
        ..Default::default()
    };
    let create_db = job_on(
        ActionType::ACTION_CREATE_SCHEMA,
        1,
        0,
        "",
        binlog(1, Some(&test), None),
    );
    let create = job_on(
        ActionType::ACTION_CREATE_TABLE,
        1,
        5,
        "test",
        binlog(2, None, Some(info(5, "t"))),
    );
    // A truncate gives the table a new ID.
    let truncate = job_on(
        ActionType::ACTION_TRUNCATE_TABLE,
        1,
        5,
        "test",
        binlog(3, None, Some(info(7, "t"))),
    );
    let add_index = job_on(
        ActionType::ACTION_ADD_INDEX,
        1,
        7,
        "test",
        binlog(4, None, Some(info(7, "t"))),
    );
    let other = job_on(
        ActionType::ACTION_CREATE_TABLE,
        9,
        10,
        "other",
        binlog(5, None, Some(info(10, "u"))),
    );
    let mut all = vec![create_db, create, truncate, add_index, other];
    let jobs = filter_ddl_jobs(&mut all, &[table(&test, 7, "t")]);
    assert_eq!(
        types(&jobs),
        [
            ActionType::ACTION_CREATE_SCHEMA,
            ActionType::ACTION_ADD_INDEX,
            ActionType::ACTION_TRUNCATE_TABLE,
            ActionType::ACTION_CREATE_TABLE
        ]
    );
}

/// Go `TestPreCheckTableClusterIndex`: an existing table with another
/// clustered-index choice is refused.
#[test]
fn pre_check_table_cluster_index_mismatch() {
    let c = MockCluster::default();
    let test = db("test", 1);
    c.add_db(
        test.clone(),
        vec![TableInfo {
            id: 2,
            name: CiString::new("t"),
            is_common_handle: false,
            ..Default::default()
        }],
    );
    let mut t = table(&test, 2, "t");
    pre_check_table_cluster_index(std::slice::from_ref(&t), &[], &c).unwrap();
    t.info.as_mut().unwrap().is_common_handle = true;
    let err = pre_check_table_cluster_index(&[t], &[], &c).unwrap_err();
    assert!(
        err.to_string().contains("Clustered index option mismatch"),
        "{err}"
    );
}

/// Go `checkTableExistence`.
#[test]
fn check_table_existence_names_existing() {
    let c = MockCluster::default();
    let test = db("test", 1);
    c.add_db(
        test.clone(),
        vec![TableInfo {
            id: 2,
            name: CiString::new("t"),
            ..Default::default()
        }],
    );
    check_table_existence(&c, &[table(&test, 3, "u")]).unwrap();
    let err = check_table_existence(&c, &[table(&test, 3, "u"), table(&test, 2, "t")]).unwrap_err();
    assert!(
        err.to_string().contains("table already exists: test.t"),
        "{err}"
    );
}
