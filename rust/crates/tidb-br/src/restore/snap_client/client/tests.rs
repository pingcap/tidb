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

//! Go `snap_client/client_test.go`: the schema side of `SnapClient` over the
//! in-memory cluster.

use std::sync::Arc;
use std::time::Duration;

use tidb_ast::CiString;
use tidb_gocontext::Context;
use tidb_model::{
    ActionType, DBInfo, GoShared, HistoryInfo, Job, PartitionDefinition, PartitionInfo, TableInfo,
};
use tidb_proto::backup::{BackupMeta, File, PlacementPolicy};

use super::*;
use crate::checkpoint::manager::{SnapshotMetaManager, TickDurationConfig};
use crate::checkpoint::restore::{
    CheckpointMetadataForSnapshotRestore, RestoreRunner, RestoreValueType,
};
use crate::test_util::cluster::MockCluster;
use crate::test_util::MockPdClient;

/// A coprocessor client that must not be used.
#[derive(Debug)]
pub(crate) struct NoKvClient;

impl tidb_txnkv::Client for NoKvClient {
    fn send(&self, _: &tidb_txnkv::TransportRequest) -> Box<dyn tidb_txnkv::Response + Send> {
        panic!("the test must not scan")
    }
    fn is_request_type_supported(&self, _: i64, _: i64) -> bool {
        false
    }
}

/// A client over the cluster, its sessions and domain set up.
pub(crate) fn client(c: &MockCluster) -> SnapClient {
    let rt = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
    let keepalive = Keepalive {
        time: Duration::from_secs(10),
        timeout: Duration::from_secs(3),
    };
    let mut rc = SnapClient::new(
        Arc::new(MockPdClient::default()),
        None,
        Default::default(),
        keepalive,
        rt,
    );
    rc.set_placement_policy_mode("strict");
    rc.init_connections(c).unwrap();
    rc
}

pub(crate) fn db_info(name: &str, id: i64) -> DBInfo {
    DBInfo {
        id,
        name: CiString::new(name),
        charset: "utf8mb4".into(),
        collate: "utf8mb4_bin".into(),
        ..Default::default()
    }
}

pub(crate) fn table(db: &DBInfo, id: i64, name: &str) -> metautil::Table {
    metautil::Table::with_schema(
        db.clone(),
        TableInfo {
            id,
            name: CiString::new(name),
            ..Default::default()
        },
    )
}

/// Go `TestCreateTables`: the tables are created with their backed-up IDs
/// when free, and the created tables carry the rewrite rules.
#[test]
fn create_tables_keeps_free_ids() {
    let ctx = Context::background();
    let c = MockCluster::default();
    c.state().global_id = 100;
    let mut rc = client(&c);
    let d = db_info("test", 2);
    rc.create_databases(&ctx, &[metautil::Database::new(d.clone(), Vec::new())])
        .unwrap();
    let tables = vec![table(&d, 200, "a"), table(&d, 50, "b")];
    assert!(!rc
        .alloc_table_ids(&ctx, &tables, false, false, None)
        .unwrap());
    let [start, end] = rc.get_pre_alloced_table_id_range().unwrap();
    assert_eq!(
        (start, end),
        (102, 202),
        "the range covers the kept ID and one rewritten"
    );
    let created = rc.create_tables(&ctx, &tables, 0).unwrap();
    let ids: HashMap<String, i64> = created
        .iter()
        .map(|ct| {
            (
                ct.table.as_ref().unwrap().name.original().to_owned(),
                ct.table.as_ref().unwrap().id,
            )
        })
        .collect();
    assert_eq!(ids["a"], 200);
    assert_eq!(ids["b"], 201);
    let b = created
        .iter()
        .find(|ct| ct.table.as_ref().unwrap().id == 201)
        .unwrap();
    assert_eq!(b.old_table.as_ref().unwrap().info().id, 50);
    assert!(b.rewrite_rule.as_ref().is_some_and(|r| !r.data.is_empty()));
    assert_eq!(b.target_db_name().original(), "test");
    // The pool created them one at a time: the batch size is 0.
    assert!(c.state().batch_calls.is_empty());
    assert_eq!(c.state().single_calls.len(), 2);
}

/// Go `TestCreateTablesBatch` with its fallback: `ErrInvalidDDLJob` from the
/// batch create falls back to one table at a time; another error fails.
#[test]
fn create_tables_batch_falls_back_on_invalid_ddl_job() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let mut rc = client(&c);
    rc.set_batch_ddl_size(2);
    let d = db_info("test", 2);
    rc.create_databases(&ctx, &[metautil::Database::new(d.clone(), Vec::new())])
        .unwrap();
    let tables: Vec<_> = (0..3)
        .map(|i| table(&d, 10 + i, &format!("t{i}")))
        .collect();
    rc.alloc_table_ids(&ctx, &tables, false, false, None)
        .unwrap();
    c.state().batch_error = Some(Arc::new(tidb_terror::TerrorError::compatible(
        tidb_terror::TerrorCode::new(tidb_errno::errcode::ErrInvalidDDLJob as isize),
        "invalid ddl job",
    )));
    let created = rc.create_tables(&ctx, &tables, 0).unwrap();
    assert_eq!(created.len(), 3);
    assert!(c.table("test", "t2").is_some());
    assert!(
        !c.state().batch_calls.is_empty(),
        "the batch create was tried"
    );

    let c2 = MockCluster::default();
    let mut rc2 = client(&c2);
    rc2.set_batch_ddl_size(2);
    rc2.create_databases(&ctx, &[metautil::Database::new(d.clone(), Vec::new())])
        .unwrap();
    rc2.alloc_table_ids(&ctx, &tables, false, false, None)
        .unwrap();
    c2.state().batch_error = Some(errors::errorf("boom"));
    let err = rc2.create_tables(&ctx, &tables, 0).err().unwrap();
    assert!(err.to_string().contains("boom"), "{err}");
}

/// The batches are ordered by schema then table ID (Go
/// `TestSortTablesBySchemaID`).
#[test]
fn sort_tables_by_schema_id_orders() {
    let (d1, d2) = (db_info("a", 2), db_info("b", 1));
    let tables = vec![
        table(&d1, 5, "x"),
        table(&d2, 9, "y"),
        table(&d1, 3, "z"),
        table(&d2, 1, "w"),
    ];
    let ids: Vec<i64> = sort_tables_by_schema_id(&tables)
        .iter()
        .map(|t| t.info().id)
        .collect();
    assert_eq!(ids, vec![1, 9, 3, 5]);
}

/// A clustered-index mismatch between the backup and the created table
/// fails (Go `ErrRestoreModeMismatch`).
#[test]
fn create_table_checks_clustered_index() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let mut rc = client(&c);
    let d = db_info("test", 2);
    rc.create_databases(&ctx, &[metautil::Database::new(d.clone(), Vec::new())])
        .unwrap();
    let mut t = table(&d, 7, "t");
    t.info = Some(TableInfo {
        is_common_handle: true,
        ..t.info().clone()
    });
    rc.alloc_table_ids(&ctx, std::slice::from_ref(&t), false, false, None)
        .unwrap();
    // The cluster creates the table without the clustered index.
    rc.enable_skip_create_sql();
    c.add_db(
        d.clone(),
        vec![TableInfo {
            id: 7,
            name: CiString::new("t"),
            ..Default::default()
        }],
    );
    let err = rc.create_tables(&ctx, &[t], 0).err().unwrap();
    assert!(
        err.to_string().contains("Clustered index option mismatch"),
        "{err}"
    );
}

/// Go `CreateDatabasesWithPlan`: an existing database is reused.
#[test]
fn create_databases_marks_reused() {
    let ctx = Context::background();
    let c = MockCluster::default();
    c.add_db(db_info("old", 1), Vec::new());
    let rc = client(&c);
    let mut plans: Vec<DatabaseRestorePlan> = ["old", "new"]
        .iter()
        .map(|n| DatabaseRestorePlan {
            source: metautil::Database::new(db_info(n, 9), Vec::new()),
            target: db_info(n, 9),
            reused: false,
        })
        .collect();
    rc.create_databases_with_plan(&ctx, &mut plans).unwrap();
    assert_eq!(
        plans.iter().map(|p| p.reused).collect::<Vec<_>>(),
        vec![true, false]
    );
}

/// Go `TestCheckTargetClusterFresh`: user tables, or a non-`test` user
/// database, make the cluster not fresh.
#[test]
fn ensure_no_user_tables() {
    let c = MockCluster::default();
    c.add_db(
        db_info("mysql", 1),
        vec![TableInfo {
            id: 2,
            name: CiString::new("user"),
            ..Default::default()
        }],
    );
    c.add_db(db_info("test", 3), Vec::new());
    let rc = client(&c);
    rc.ensure_no_user_tables().unwrap();
    c.add_db(
        db_info("user_db", 4),
        vec![TableInfo {
            id: 5,
            name: CiString::new("t"),
            ..Default::default()
        }],
    );
    let err = rc.ensure_no_user_tables().unwrap_err();
    assert!(
        err.to_string().contains("user db/tables: user_db.t"),
        "{err}"
    );
}

/// Go `ExecDDLs` runs the jobs by schema version.
#[test]
fn exec_ddls_by_schema_version() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let rc = client(&c);
    let job = |v: i64, q: &str| {
        let mut j = Job::default();
        j.type_ = ActionType::ACTION_ADD_INDEX;
        j.query = q.into();
        j.binlog_info = Some(GoShared::new(HistoryInfo {
            schema_version: v,
            ..Default::default()
        }));
        j
    };
    let mut jobs = vec![job(3, "c"), job(1, "a"), job(2, "b")];
    c.state().stmts.clear();
    rc.exec_ddls(&ctx, &mut jobs).unwrap();
    assert_eq!(c.state().stmts, ["a", "b", "c"]);
}

/// Go `GetPlacementPolicies` decodes the backed-up policies by name.
#[test]
fn placement_policies_from_backup_meta() {
    let c = MockCluster::default();
    let mut rc = client(&c);
    rc.backup_meta = BackupMeta {
        policies: vec![PlacementPolicy {
            info: br#"{"id":3,"name":{"O":"Five","L":"five"}}"#.to_vec(),
        }],
        ..Default::default()
    };
    let policies = rc.get_placement_policies().unwrap();
    assert_eq!(policies.lock().unwrap()["five"].id, 3);
    rc.create_policies(&Context::background(), &policies)
        .unwrap();
    assert_eq!(c.state().policies.len(), 1);
}

/// Go `IsIncremental` / `InitFullClusterRestore` / `NeedCheckFreshCluster`.
#[test]
fn full_cluster_restore_flags() {
    let c = MockCluster::default();
    let mut rc = client(&c);
    rc.backup_meta = BackupMeta {
        start_version: 0,
        end_version: 9,
        ..Default::default()
    };
    assert!(!rc.is_incremental());
    rc.init_full_cluster_restore(false, true, true);
    assert!(rc.is_full_cluster_restore());
    rc.init_full_cluster_restore(true, true, true);
    assert!(!rc.is_full_cluster_restore());
    assert!(rc.need_check_fresh_cluster(false, false));
    assert!(!rc.need_check_fresh_cluster(false, true));
    rc.backup_meta.start_version = 5;
    assert!(rc.is_incremental());
    assert!(!rc.need_check_fresh_cluster(false, false));
}

/// A checkpoint storage that keeps nothing.
struct NullStorage;

impl crate::checkpoint::CheckpointStorage for NullStorage {
    fn flush_checkpoint_data(&self, _: &Context, _: &[u8]) -> Result<()> {
        Ok(())
    }
    fn flush_checkpoint_checksum(&self, _: &Context, _: &[u8]) -> Result<()> {
        Ok(())
    }
    fn update_lock(&self, _: &Context) -> Result<()> {
        Ok(())
    }
    fn close(&self) {}
}

/// A meta manager over fixed checkpoint contents.
#[derive(Default)]
struct FakeManager {
    meta: Mutex<Option<CheckpointMetadataForSnapshotRestore>>,
    data: Vec<(i64, String)>,
    checksum: HashMap<i64, ChecksumItem>,
}

impl std::fmt::Display for FakeManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fake")
    }
}

impl SnapshotMetaManager for FakeManager {
    fn load_checkpoint_data(
        &self,
        _: &Context,
        f: &mut dyn FnMut(i64, RestoreValueType) -> Result<()>,
    ) -> Result<Duration> {
        for (id, key) in &self.data {
            f(
                *id,
                RestoreValueType {
                    range_key: key.clone(),
                    name: String::new(),
                },
            )?;
        }
        Ok(Duration::ZERO)
    }
    fn load_checkpoint_checksum(
        &self,
        _: &Context,
    ) -> Result<(HashMap<i64, ChecksumItem>, Duration)> {
        Ok((self.checksum.clone(), Duration::ZERO))
    }
    fn load_checkpoint_metadata(
        &self,
        _: &Context,
    ) -> Result<CheckpointMetadataForSnapshotRestore> {
        Ok(self.meta.lock().unwrap().clone().unwrap())
    }
    fn save_checkpoint_metadata(
        &self,
        _: &Context,
        meta: Option<&CheckpointMetadataForSnapshotRestore>,
    ) -> Result<()> {
        *self.meta.lock().unwrap() = meta.cloned();
        Ok(())
    }
    fn exists_checkpoint_metadata(&self, _: &Context) -> Result<bool> {
        Ok(self.meta.lock().unwrap().is_some())
    }
    fn remove_checkpoint_data(&self, _: &Context) -> Result<()> {
        Ok(())
    }
    fn start_checkpoint_runner(
        &self,
        ctx: &Context,
        _: TickDurationConfig,
    ) -> Result<Arc<RestoreRunner>> {
        let tick = Duration::from_secs(60);
        Ok(Arc::new(crate::checkpoint::CheckpointRunner::start(
            ctx,
            Arc::new(NullStorage),
            None,
            Arc::new(crate::checkpoint::restore::value_marshaler_for_restore),
            [tick, tick, Duration::ZERO, tick],
        )))
    }
    fn close(&self) {}
}

/// Go `TestInitCheckpoint`-like: the first run records the metadata with
/// the preallocated IDs; a rerun loads the ranges and checksums, and a
/// different backup or command is refused.
#[test]
fn init_checkpoint_first_run_then_resume() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let mut rc = client(&c);
    rc.backup_meta = BackupMeta {
        cluster_id: 7,
        end_version: 100,
        ..Default::default()
    };
    let d = db_info("test", 2);
    rc.alloc_table_ids(&ctx, &[table(&d, 30, "t")], false, false, None)
        .unwrap();
    let mgr = FakeManager::default();
    let cfg = ClusterConfig {
        rule_id: "r".into(),
        ..Default::default()
    };
    let init = rc
        .init_checkpoint(&ctx, &mgr, Some(&cfg), 55, 0, b"h", false)
        .unwrap();
    assert_eq!(init.new_restore_start_ts, 55);
    let meta = mgr.meta.lock().unwrap().clone().unwrap();
    assert_eq!(
        (
            meta.upstream_cluster_id,
            meta.restored_ts,
            meta.hash.as_slice()
        ),
        (7, 100, &b"h"[..])
    );
    assert_eq!(meta.schedulers_config.as_ref().unwrap().rule_id, "r");
    assert_eq!(meta.prealloc_ids, rc.create_prealloc_id_checkpoint());
    assert!(!meta.restore_uuid.is_nil());
    rc.wait_for_finish_checkpoint(&ctx, true);

    let mgr = FakeManager {
        meta: Mutex::new(Some(meta.clone())),
        data: vec![(30, "a".into()), (30, "b".into()), (31, "c".into())],
        checksum: [(
            30,
            ChecksumItem {
                table_id: 30,
                crc64xor: 1,
                total_kvs: 2,
                total_bytes: 3,
            },
        )]
        .into(),
    };
    let mut rc2 = client(&c);
    rc2.backup_meta = rc.backup_meta.clone();
    let init = rc2
        .init_checkpoint(&ctx, &mgr, None, 99, 0, b"h", true)
        .unwrap();
    assert_eq!(init.new_restore_start_ts, 55, "the first run's start");
    assert_eq!(init.checkpoint_set_with_table_id[&30].len(), 2);
    assert_eq!(init.checkpoint_cluster_config.unwrap().rule_id, "r");
    assert_eq!(rc2.checkpoint_checksum[&30].total_kvs, 2);
    assert_eq!(rc2.restore_uuid, meta.restore_uuid);
    rc2.wait_for_finish_checkpoint(&ctx, false);

    let mut rc3 = client(&c);
    rc3.backup_meta = rc.backup_meta.clone();
    let err = rc3
        .init_checkpoint(&ctx, &mgr, None, 99, 0, b"other", true)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("The hash of the current snapshot restore does not match"),
        "{err}"
    );
    rc3.backup_meta.cluster_id = 8;
    let err = rc3
        .init_checkpoint(&ctx, &mgr, None, 99, 0, b"h", true)
        .unwrap_err();
    assert!(
        err.to_string().contains("The upstream cluster id[8]"),
        "{err}"
    );
}

fn created_with_files(crc: u64, kvs: u64, bytes: u64) -> CreatedTable<metautil::Table> {
    let d = db_info("test", 2);
    let mut old = table(&d, 30, "t");
    old.files_of_physicals.insert(
        30,
        vec![File {
            crc64xor: crc,
            total_kvs: kvs,
            total_bytes: bytes,
            ..Default::default()
        }],
    );
    CreatedTable {
        table: Some(TableInfo {
            id: 40,
            name: CiString::new("t"),
            ..Default::default()
        }),
        old_table: Some(old),
        ..Default::default()
    }
}

/// Go `execAndValidateChecksum` with a checkpointed checksum: equal
/// passes, different fails with the details.
#[test]
fn exec_and_validate_checksum_from_checkpoint() {
    let ctx = Context::background();
    let c = MockCluster::default();
    let mut rc = client(&c);
    rc.checkpoint_checksum.insert(
        40,
        ChecksumItem {
            table_id: 40,
            crc64xor: 9,
            total_kvs: 2,
            total_bytes: 3,
        },
    );
    rc.exec_and_validate_checksum(&ctx, &created_with_files(9, 2, 3), &NoKvClient, 4)
        .unwrap();
    let err = rc
        .exec_and_validate_checksum(&ctx, &created_with_files(9, 5, 3), &NoKvClient, 4)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("checksum mismatch for table 'test.t' (ID: 40)")
            && err
                .to_string()
                .contains("totalKvs (expected: 5, actual: 2)"),
        "{err}"
    );
    // A backup without checksums is not scanned.
    rc.exec_and_validate_checksum(&ctx, &created_with_files(0, 0, 0), &NoKvClient, 4)
        .unwrap();
}

/// Go `setMergeOptionForTables`: a rule for each allowed table and
/// partition, by the new IDs.
#[test]
fn merge_option_label_rules() {
    let c = MockCluster::default();
    let rc = client(&c);
    let d = db_info("Test", 2);
    let part = |ids: &[(i64, &str)]| {
        let defs: Vec<PartitionDefinition> = ids
            .iter()
            .map(|(id, n)| PartitionDefinition {
                id: *id,
                name: CiString::new(*n),
                ..Default::default()
            })
            .collect();
        Some(GoShared::new(PartitionInfo {
            definitions: defs.into(),
            ..Default::default()
        }))
    };
    let mut old = table(&d, 30, "T");
    old.info = Some(TableInfo {
        partition: part(&[(31, "P0"), (32, "p1")]),
        ..old.info().clone()
    });
    old.is_merge_option_allowed = true;
    old.partition_merge_option_allowed.insert("P0".into(), true);
    let ct = CreatedTable {
        table: Some(TableInfo {
            id: 40,
            name: CiString::new("T"),
            partition: part(&[(41, "P0"), (42, "p1")]),
            ..Default::default()
        }),
        old_table: Some(old),
        ..Default::default()
    };
    rc.set_merge_option_for_tables(&[&ct]).unwrap();
    assert_eq!(
        c.state().label_patches,
        vec![vec![
            "schema/test/t".to_owned(),
            "schema/test/t/p0".to_owned()
        ]]
    );
}
