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

//! Cardinality fixtures that drain the process-global async statistics queue.
//! Isolate the domain lifecycle from other session tests' catalogs and queues.

use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use std::sync::Arc;
use tidb_session::{Session, StmtResult};

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    let StmtResult::Rows(rows) = session.run(sql).unwrap() else {
        panic!("no rows: {sql}")
    };
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell.to_bytes().unwrap()).unwrap())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

#[test]
fn subset_index_cardinality_after_async_statistics_load() {
    // Go `TestSubsetIdxCardinality`: ANALYZE leaves the histograms evicted
    // in the node's statistics cache, planning queues them, and the node's
    // loader (`h.LoadNeededHistograms`) brings them in.
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT,b INT,c INT,INDEX iabc(a,b,c))")
        .unwrap();
    session.run("INSERT INTO t VALUES (1,1,1),(1,1,1),(2,1,1),(2,1,1),(3,1,1),(3,1,1),(4,1,1),(4,1,1),(5,1,1),(5,1,1)").unwrap();
    session.run("INSERT INTO t SELECT a+5,a,a FROM t").unwrap();
    for i in 1..3 {
        session
            .run(&format!("INSERT INTO t SELECT a+10+{i},b+1,c FROM t"))
            .unwrap();
    }
    for _ in 0..3 {
        session.run("INSERT INTO t SELECT a,b,c FROM t").unwrap();
    }
    session.run("INSERT INTO t SELECT a,b+10,c FROM t").unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    let shared = session.shared_catalog();
    let (table_id, column_ids, index_id) = {
        let catalog = shared.lock().unwrap();
        let tidb_infoschema::TableEntry::Kv(table) = catalog.table_in("test", "t").unwrap() else {
            panic!("not KV")
        };
        let column_ids = table
            .columns()
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        (table.table_id(), column_ids, table.indexes()[0].id)
    };
    let queued = || {
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .into_iter()
            .any(|item| item.table_item_id.table_id == table_id)
    };
    session.run("SET tidb_stats_load_sync_wait=0").unwrap();
    rows(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT a,b FROM t WHERE a=1 AND b=1 AND c=1",
    );
    assert!(queued(), "planning must enqueue evicted statistics");
    {
        let catalog = shared.lock().unwrap();
        catalog.load_needed_histograms(&shared.stats_handle(), "default").unwrap();
        let loaded = catalog.table_statistics_in(&shared.stats_handle(), table_id).unwrap();
        for id in column_ids {
            assert!(loaded.hist_coll.get_column(id).unwrap().read().unwrap().is_full_load());
        }
        assert!(loaded.hist_coll.get_index(index_id).unwrap().read().unwrap().is_full_load());
    }
    assert!(!queued(), "domain loading must consume the demand");
    let output: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../pkg/planner/cardinality/testdata/cardinality_suite_out.json"
    ))
    .unwrap();
    let cases = output
        .as_array()
        .unwrap()
        .iter()
        .find(|section| section["Name"] == "TestSubsetIdxCardinality")
        .unwrap()["Cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 5);
    for case in cases {
        let sql = case["Query"].as_str().unwrap();
        let expected = case["Result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows(&mut session, sql), expected, "{sql}");
    }
    // Go asserts nothing about the queue after these replans: with sync load
    // off it queues every needed item again, loaded or not
    // (`CollectPredicateColumnsPoint`).
}

/// Storage bytes are in memory; bootstrap, DDL stats writes, delta writes,
/// startup loaders, cache publication and planner conversion are production code.
#[derive(Default)]
struct StatsStorage(std::collections::BTreeMap<Vec<u8>, Vec<u8>>);

impl tidb_infoschema::cluster_catalog::MetaSnapshot for StatsStorage {
    fn get(
        &mut self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
        Ok(self.0.get(key).cloned())
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<
        tidb_infoschema::cluster_catalog::MetaPairs,
        tidb_infoschema::cluster_catalog::ClusterCatalogError,
    > {
        Ok(self
            .0
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl StatsStorage {
    fn apply(&mut self, mutations: &[tidb_txnkv::transaction::BufferMutation]) {
        use tidb_txnkv::transaction::BufferMutationOp;
        for mutation in mutations {
            match mutation.kind() {
                BufferMutationOp::Lock => {}
                BufferMutationOp::Delete => {
                    self.0.remove(mutation.key());
                }
                _ => {
                    self.0
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
            }
        }
    }
}

struct CanonicalStatistics(Arc<tidb_stats_handle_syncload::stats_watch::SharedStats>);

impl tidb_stats_handle::StatisticsSource for CanonicalStatistics {
    fn table(&self, id: i64) -> Option<Arc<tidb_stats::Table>> {
        self.0
            .load()
            .get(&id)
            .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
            .cloned()
    }
}

/// Go's domain sync-load worker over the harness cache. The harness table
/// is never analyzed, so every load is `statsSyncLoad.handleOneItemTask`'s
/// `EmptyColumn` branch.
struct NeverAnalyzedLoader {
    cache: Arc<tidb_stats_handle_syncload::stats_watch::SharedStats>,
    table: tidb_model::TableInfo,
}

impl tidb_stats_handle_syncload::sync_load::StatisticsItemLoader for NeverAnalyzedLoader {
    fn load_items(
        &self,
        items: &[tidb_model::StatsLoadItem],
        _resource_group: &str,
    ) -> Result<Vec<(i64, Arc<tidb_stats::Table>)>, String> {
        for requested in items {
            let item = requested.table_item_id;
            if item.is_index {
                continue;
            }
            let Some(current) = self
                .cache
                .load()
                .get(&item.table_id)
                .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                .cloned()
            else {
                continue;
            };
            let (_, load_needed, analyzed) =
                current.column_load_needed(item.id, requested.full_load);
            assert!(!analyzed, "the harness table is never analyzed");
            if load_needed {
                self.cache.publish_empty_column(item.table_id, item.id, &self.table);
            }
        }
        Ok(Vec::new())
    }
}

/// Go TestBuiltinInEstWithoutStats, including the previously omitted
/// InitStatsLite / InitStats / existence-map lifecycle.
#[test]
fn builtin_in_estimate_survives_statistics_initialization() {
    use tidb_stats_handle_storage::cluster_stats_write::{insert_table_stats_statements, plan_insert_table_stats_statement};
    use tidb_stats_handle_usage::session_stats_collect::plan_stats_delta_statement;
    use tidb_stats_handle_usage::session_stats_collect::stats_delta_statements;
    use tidb_stats_handle_syncload::real_tikv_stats::{StatsTarget};
    use tidb_stats_handle::bootstrap::{load_initial_stats_snapshot_with_memory_limits, InitialStatsLoad};

    // The statistics storage is this test's own (`StatsStorage`), so the
    // session plans through a view over it instead of its node's cache.
    let mut session = Session::new();
    session.run("CREATE TABLE t(a INT,b INT)").unwrap();
    session
        .run("INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)")
        .unwrap();
    let shared_catalog = session.shared_catalog();
    let table_id = {
        let catalog = shared_catalog.lock().unwrap();
        let tidb_infoschema::TableEntry::Kv(table) = catalog.table_in("test", "t").unwrap() else {
            panic!("not KV")
        };
        table.table_id()
    };
    let parsed = tidb_parser::parse("CREATE TABLE t(a INT,b INT)").unwrap();
    let tidb_ast::Stmt::Ddl(ddl) = parsed else {
        panic!("not DDL")
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("not CREATE")
    };
    let mut table = tidb_ddl_build::table_info_build::build_table_info(
        create,
        "utf8mb4",
        "utf8mb4_bin",
    )
    .unwrap();
    table.id = table_id;
    let targets = StatsTarget::for_table(&table);
    let now = tidb_datatype::Time::from_date_checked(
        2026,
        9,
        25,
        0,
        0,
        0,
        0,
        tidb_datatype::TimeType::Timestamp,
        0,
    )
    .unwrap();
    let mut storage = StatsStorage::default();
    let bootstrap = tidb_session::mysql_bootstrap::plan_mysql_bootstrap(
        &mut storage,
        100,
        &tidb_session::mysql_bootstrap::BootstrapEnvironment {
            system_tz: "UTC".to_owned(),
            new_collation_enabled: true,
            cluster_id: 1,
            current_timestamp: now,
            ddl_table_version: 0,
        },
    )
    .unwrap();
    storage.apply(&bootstrap.mutations);
    let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut storage).unwrap();
    for statement in insert_table_stats_statements(&table, table_id) {
        let write =
            plan_insert_table_stats_statement(&mut storage, &catalog, &statement, 101, now, None)
                .unwrap();
        storage.apply(&write.mutations);
    }
    let updates = [tidb_stats_handle_usage::DeltaUpdate {
        table_id,
        delta: tidb_variable::table_delta::TableDelta {
            delta: 10,
            count: 10,
            init_time: None,
        },
        is_locked: false,
    }];
    for statement in stats_delta_statements(&updates) {
        let write =
            plan_stats_delta_statement(&mut storage, &catalog, &statement, 102, now, None).unwrap();
        storage.apply(&write.mutations);
    }
    let loader = tidb_stats_handle_storage::cluster_stats_load::ClusterStatsLoader::locate(&catalog).unwrap();
    let cache = Arc::new(tidb_stats_handle_syncload::stats_watch::SharedStats::new(Default::default()).unwrap());
    shared_catalog.stats_handle().set_statistics_view(Arc::new(
        tidb_stats_handle::StatisticsView::new(Arc::new(CanonicalStatistics(cache.clone()))),
    ));
    // Go's test domain runs the sync-load workers.
    shared_catalog.stats_handle().set_item_loader(
        Arc::new(NeverAnalyzedLoader {
            cache: cache.clone(),
            table: table.clone(),
        }),
        tidb_stats_handle_syncload::sync_load::StatisticsLoadWorkers::new(),
    );
    // Update after the delta, clear/init-lite, clear/init-full, then Update
    // again: retain Go's lifecycle order and the same planner-facing view.
    for mode in [
        None,
        Some(InitialStatsLoad::Lite),
        Some(InitialStatsLoad::IndexFull),
        None,
    ] {
        let snapshot = if let Some(mode) = mode {
            cache.store(Default::default());
            assert!(shared_catalog
                .lock()
                .unwrap()
                .table_statistics_in(&shared_catalog.stats_handle(), table_id)
                .is_none());
            load_initial_stats_snapshot_with_memory_limits(
                &mut storage,
                &loader,
                &targets,
                &[],
                mode,
                u64::MAX,
                0,
            )
            .unwrap()
        } else {
            let current = cache.load();
            let loaded = loader
                .load_statistics_table_for_update(
                    &mut storage,
                    table_id,
                    &table,
                    &targets[0].column_types,
                    current
                        .get(&table_id)
                        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                        .map(AsRef::as_ref),
                    std::time::Duration::from_secs(3),
                )
                .unwrap()
                .unwrap();
            std::collections::BTreeMap::from([(
                table_id,
                tidb_stats_handle_syncload::stats_watch::TableStatsState::Loaded(loaded),
            )])
        };
        cache.store(snapshot);
        let canonical = cache.load()[&table_id].loaded().unwrap().clone();
        let existence = canonical.existence_map.as_ref().unwrap().read().unwrap();
        assert!(!existence.is_empty(), "{mode:?}");
        for column in table.columns.iter_deref() {
            assert!(!existence.has_analyzed(column.read().id, false), "{mode:?}");
        }
        drop(existence);
        let stats = shared_catalog
            .lock()
            .unwrap()
            .table_statistics_in(&shared_catalog.stats_handle(), table_id)
            .unwrap();
        // Uninitialized: `GetStatsTable` plans from a pseudo copy, while the
        // cache object itself is not pseudo.
        assert!(!stats.is_initialized(), "{mode:?}");
        assert!(!stats.hist_coll.pseudo, "{mode:?}");
        let existence = stats.existence_map.as_ref().unwrap().read().unwrap();
        for id in [1, 2] {
            assert!(existence.has(id, false) && !existence.has_analyzed(id, false));
        }
        assert_eq!(existence.column_count(), 2);
        drop(existence);
        for column in ["a", "b"] {
            let sql = format!(
                "EXPLAIN FORMAT='brief' SELECT * FROM t WHERE {column} IN (1,2,3,4,5,6,7,8)"
            );
            assert_eq!(
                rows(&mut session, &sql),
                vec![
                    "TableReader 1.00 root  data:Selection".to_owned(),
                    format!(
                        "└─Selection 1.00 cop[tikv]  in(test.t.{column}, 1, 2, 3, 4, 5, 6, 7, 8)"
                    ),
                    "  └─TableFullScan 10.00 cop[tikv] table:t keep order:false, stats:pseudo"
                        .to_owned(),
                ],
                "{mode:?}: {sql}"
            );
        }
    }
}

/// Go TestRangeStepOverflow must survive both an evicted histogram and the
/// subsequent async load. The large datetime span exercises typed range
/// enumeration rather than a small integer-only surrogate.
#[test]
fn datetime_range_overflow_survives_async_statistics_load() {
    // ANALYZE leaves the histogram evicted in the node's statistics cache;
    // the node's loader brings it in once the query queued it.
    let mut session = Session::new();
    session.run("CREATE TABLE t(col DATETIME)").unwrap();
    session.run("INSERT INTO t VALUES('3580-05-26 07:16:48'),('4055-03-06 22:27:16'),('4862-01-26 07:16:54')").unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    session.run("SET tidb_stats_load_sync_wait=0").unwrap();
    let shared = session.shared_catalog();
    let (table_id, column_id) = {
        let catalog = shared.lock().unwrap();
        let tidb_infoschema::TableEntry::Kv(table) = catalog.table_in("test", "t").unwrap() else {
            panic!("not KV")
        };
        (table.table_id(), table.columns()[0].id)
    };
    let sql = "SELECT * FROM t WHERE col BETWEEN '8499-1-23 2:14:38' AND '9961-7-23 18:35:26'";
    assert!(rows(&mut session, sql).is_empty());
    let queued = || {
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .into_iter()
            .any(|item| item.table_item_id.table_id == table_id)
    };
    assert!(queued(), "the first query must request evicted statistics");
    {
        let catalog = shared.lock().unwrap();
        catalog.load_needed_histograms(&shared.stats_handle(), "default").unwrap();
        assert!(catalog
            .table_statistics_in(&shared.stats_handle(), table_id)
            .unwrap()
            .hist_coll
            .get_column(column_id)
            .unwrap()
            .read()
            .unwrap()
            .is_full_load());
    }
    assert!(!queued());
    assert!(rows(&mut session, sql).is_empty());
}
