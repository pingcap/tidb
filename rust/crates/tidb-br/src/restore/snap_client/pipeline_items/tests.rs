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

//! Go `snap_client/pipeline_items_test.go`: the stage chain, statistics and
//! TiFlash wait over the in-memory cluster.

use std::sync::atomic::{AtomicUsize, Ordering};

use tidb_ast::CiString;
use tidb_model::{GoShared, PartitionDefinition, PartitionInfo, TableInfo, TiFlashReplicaInfo};

use super::*;
use crate::restore::snap_client::client::tests::{client, db_info, table, NoKvClient};
use crate::test_util::cluster::MockCluster;

fn created(id: i64, old: metautil::Table) -> Created {
    let name = old.info().name.clone();
    CreatedTable {
        table: Some(TableInfo {
            id,
            name,
            ..Default::default()
        }),
        old_table: Some(old),
        ..Default::default()
    }
}

fn pl_ctx(c: &MockCluster, dir: &std::path::Path) -> PipelineContext {
    PipelineContext {
        checksum: false,
        load_stats: false,
        load_stats_physical: false,
        load_sys_table_physical: false,
        wait_tiflash_ready: false,
        log_progress: false,
        checksum_concurrency: 4,
        stats_concurrency: 4,
        restore_ts: 0,
        kv_client: Arc::new(NoKvClient),
        ext_storage: Arc::new(tidb_objstore::LocalStorage::new(dir.to_str().unwrap()).unwrap()),
        glue: Arc::new(c.clone()),
    }
}

fn record_file(start_key: Vec<u8>, kvs: u64) -> File {
    File {
        start_key,
        total_kvs: kvs,
        ..Default::default()
    }
}

/// Every table passes each stage in order, and each end function runs
/// once its stage is done.
#[test]
fn stages_run_in_order() {
    let ctx = Context::background();
    let d = db_info("test", 2);
    let tables: Vec<Created> = (0..20)
        .map(|i| created(100 + i, table(&d, i, &format!("t{i}"))))
        .collect();
    let log = Mutex::new(Vec::<String>::new());
    let seen = [AtomicUsize::new(0), AtomicUsize::new(0)];
    let mut b = PipelineConcurrentBuilder::default();
    for (stage, counter) in seen.iter().enumerate() {
        let log = &log;
        b.register_pipeline_task(
            &format!("s{stage}"),
            3,
            move |_, t| {
                // The second stage sees only tables the first finished.
                log.lock()
                    .unwrap()
                    .push(format!("{stage}:{}", t.table.as_ref().unwrap().id));
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            move |_| {
                log.lock().unwrap().push(format!("end{stage}"));
                Ok(())
            },
        );
    }
    b.start_pipeline_task(&ctx, &tables).unwrap();
    drop(b);
    assert_eq!(seen[0].load(Ordering::SeqCst), 20);
    assert_eq!(seen[1].load(Ordering::SeqCst), 20);
    let log = log.into_inner().unwrap();
    for id in 100..120 {
        let first = log.iter().position(|l| *l == format!("0:{id}")).unwrap();
        let second = log.iter().position(|l| *l == format!("1:{id}")).unwrap();
        assert!(first < second);
    }
    let end0 = log.iter().position(|l| l == "end0").unwrap();
    assert!(log[..end0].iter().filter(|l| l.starts_with("0:")).count() == 20);
    assert_eq!(log.last().unwrap(), "end1");
}

/// The first stage error is returned, and no later end function runs.
#[test]
fn stage_error_cancels_the_pipeline() {
    let ctx = Context::background();
    let d = db_info("test", 2);
    let tables: Vec<Created> = (0..50)
        .map(|i| created(100 + i, table(&d, i, &format!("t{i}"))))
        .collect();
    let ended = AtomicUsize::new(0);
    let mut b = PipelineConcurrentBuilder::default();
    let ended = &ended;
    b.register_pipeline_task(
        "fail",
        2,
        |_, t| {
            if t.table.as_ref().unwrap().id == 110 {
                Err(errors::errorf("stage failed"))
            } else {
                Ok(())
            }
        },
        |_| Ok(()),
    );
    b.register_pipeline_task(
        "next",
        2,
        |_, _| Ok(()),
        |_| {
            ended.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );
    let err = b.start_pipeline_task(&ctx, &tables).unwrap_err();
    assert_eq!(err.to_string(), "stage failed");
    assert_eq!(ended.load(Ordering::SeqCst), 0);
}

/// Go `registerUpdateMetaAndLoadStats`: a table with statistics loads them
/// under its target names; one without gets its row counts, per
/// partition for a partitioned one, in one meta write at the end.
#[test]
fn load_stats_or_update_metas() {
    let ctx = Context::background();
    let dir = tempfile::tempdir().unwrap();
    let c = MockCluster::default();
    let rc = client(&c);
    let d = db_info("up", 2);
    let mut with_stats = table(&d, 10, "s");
    with_stats.stats = Some(tidb_stats_util::JsonTable {
        database_name: "up".into(),
        table_name: "s".into(),
        ..Default::default()
    });
    let mut ct_stats = created(110, with_stats);
    ct_stats.target_db = CiString::new("down");

    let mut plain = table(&d, 20, "p");
    plain.files_of_physicals.insert(
        20,
        vec![
            record_file(
                tidb_tablecodec::table_key::encode_row_key(20, &1_i64.to_be_bytes()),
                7,
            ),
            record_file(b"t\x80index".to_vec(), 99),
        ],
    );
    let defs = |ids: &[(i64, &str)]| {
        let v: Vec<PartitionDefinition> = ids
            .iter()
            .map(|(id, n)| PartitionDefinition {
                id: *id,
                name: CiString::new(*n),
                ..Default::default()
            })
            .collect();
        Some(GoShared::new(PartitionInfo {
            enable: true,
            definitions: v.into(),
            ..Default::default()
        }))
    };
    let mut parted = table(&d, 30, "q");
    parted.info = Some(TableInfo {
        partition: defs(&[(31, "p0"), (32, "p1")]),
        ..parted.info().clone()
    });
    parted.files_of_physicals.insert(
        31,
        vec![record_file(
            tidb_tablecodec::table_key::encode_row_key(31, &1_i64.to_be_bytes()),
            3,
        )],
    );
    parted.files_of_physicals.insert(
        32,
        vec![record_file(
            tidb_tablecodec::table_key::encode_row_key(32, &1_i64.to_be_bytes()),
            4,
        )],
    );
    let mut ct_parted = created(130, parted);
    ct_parted.table.as_mut().unwrap().partition = defs(&[(131, "p0"), (132, "p1")]);

    let mut ctx_pl = pl_ctx(&c, dir.path());
    ctx_pl.load_stats = true;
    rc.restore_pipeline(&ctx, &ctx_pl, &[ct_stats, created(120, plain), ct_parted])
        .unwrap();
    let st = c.state();
    assert_eq!(st.stats_loaded.len(), 1);
    assert_eq!(
        (
            st.stats_loaded[0].database_name.as_str(),
            st.stats_loaded[0].table_name.as_str()
        ),
        ("down", "s")
    );
    assert_eq!(st.meta_updates.len(), 1, "one write at the end");
    let mut got: Vec<(i64, i64)> = st.meta_updates[0]
        .iter()
        .map(|m| (m.physical_id, m.count))
        .collect();
    got.sort_unstable();
    assert_eq!(got, vec![(120, 7), (130, 7), (131, 3), (132, 4)]);
}

/// Go `RestoreStats`: the stats files written at backup load back by the
/// rewritten physical IDs.
#[test]
fn restore_stats_from_files() {
    let ctx = Context::background();
    let dir = tempfile::tempdir().unwrap();
    let storage: metautil::Storage =
        Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    let mut w = metautil::StatsWriter::new(Arc::clone(&storage), None);
    let json = |name: &str| {
        serde_json::to_vec(&tidb_stats_util::JsonTable {
            table_name: name.into(),
            ..Default::default()
        })
        .unwrap()
    };
    w.backup_stats(&ctx, Some(json("p0")), 31).unwrap();
    w.backup_stats(&ctx, Some(json("p1")), 32).unwrap();
    let indexes = w.backup_stats_done(&ctx).unwrap();
    let c = MockCluster::default();
    let new = TableInfo {
        id: 130,
        name: CiString::new("q"),
        ..Default::default()
    };
    let map = [(31, 131), (32, 132)].into_iter().collect();
    metautil::statsfile::restore_stats(&ctx, &storage, None, &c, &new, &indexes, &map).unwrap();
    assert_eq!(
        c.state().partition_stats_loaded,
        vec![("q".to_owned(), vec![131, 132])]
    );
    let err = metautil::statsfile::restore_stats(
        &ctx,
        &storage,
        None,
        &c,
        &new,
        &indexes,
        &[(31, 131)].into_iter().collect(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("old physical id: 32"), "{err}");
}

/// Go `registerWaitTiFlashReady`: a table without a replica passes; one
/// with a replica waits until its progress is 1.
#[test]
fn wait_tiflash_ready() {
    let ctx = Context::background();
    let dir = tempfile::tempdir().unwrap();
    let c = MockCluster::default();
    let rc = client(&c);
    let d = db_info("test", 2);
    let mut with_replica = created(110, table(&d, 10, "r"));
    with_replica.table.as_mut().unwrap().tiflash_replica =
        Some(GoShared::new(TiFlashReplicaInfo {
            count: 1,
            ..Default::default()
        }));
    let mut ctx_pl = pl_ctx(&c, dir.path());
    ctx_pl.wait_tiflash_ready = true;
    rc.restore_pipeline(
        &ctx,
        &ctx_pl,
        &[created(120, table(&d, 20, "n")), with_replica],
    )
    .unwrap();

    let mut ctx_pl = pl_ctx(&c, dir.path());
    ctx_pl.load_stats_physical = true;
    assert!(
        rc.restore_pipeline(&ctx, &ctx_pl, &[]).is_err(),
        "physical loads are not supported"
    );
}
