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

// aggregate-test: standalone
//! Cardinality fixtures that drain the process-global async statistics queue.
//! Isolate the domain lifecycle from other session tests' catalogs and queues.

use std::sync::{Arc, Mutex};
use tidb_executor::access_cost::TableStatistics;
use tidb_executor::driver::{StatisticsItemLoader, StatisticsLoadWorkers};
use tidb_session::{Session, StmtResult};

/// Storage test double: only requested items become fully loaded. The real
/// Catalog owns queue consumption, cache publication and subsequent planning.
struct StoredStatistics {
    table_id: i64,
    analyzed: Arc<TableStatistics>,
    loaded: Mutex<TableStatistics>,
    requests: Mutex<Vec<tidb_model::StatsLoadItem>>,
}

impl StatisticsItemLoader for StoredStatistics {
    fn load_items(
        &self,
        items: &[tidb_model::StatsLoadItem],
        _: &str,
    ) -> Result<Vec<(i64, Arc<TableStatistics>)>, String> {
        let mut loaded = self.loaded.lock().unwrap();
        for request in items {
            let item = request.table_item_id;
            assert_eq!(item.table_id, self.table_id);
            assert!(request.full_load);
            self.requests.lock().unwrap().push(*request);
            if item.is_index {
                loaded
                    .indexes
                    .insert(item.id, self.analyzed.indexes[&item.id].clone());
                loaded
                    .index_load_status
                    .insert(item.id, tidb_stats::StatsLoadedStatus::full_load());
            } else {
                loaded
                    .columns
                    .insert(item.id, self.analyzed.columns[&item.id].clone());
                loaded
                    .column_load_status
                    .insert(item.id, tidb_stats::StatsLoadedStatus::full_load());
            }
        }
        Ok(vec![(self.table_id, Arc::new(loaded.clone()))])
    }
}

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
    let (table_id, column_ids, index_id, loader) = {
        let mut catalog = shared.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) = catalog.table_in("test", "t").unwrap() else {
            panic!("not KV")
        };
        let table_id = table.table_id;
        let column_ids = table
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        let index_id = table.indexes()[0].id;
        let analyzed = catalog.table_statistics(table_id).unwrap();
        let mut evicted = (*analyzed).clone();
        for column in evicted.columns.values_mut() {
            column.histogram.buckets.clear();
            column.topn = None;
            column.cms = None;
        }
        for index in evicted.indexes.values_mut() {
            index.histogram.buckets.clear();
            index.topn = None;
            index.cms = None;
        }
        for status in evicted.column_load_status.values_mut() {
            *status = tidb_stats::StatsLoadedStatus::all_evicted();
        }
        for status in evicted.index_load_status.values_mut() {
            *status = tidb_stats::StatsLoadedStatus::all_evicted();
        }
        let loader = Arc::new(StoredStatistics {
            table_id,
            analyzed,
            loaded: Mutex::new(evicted.clone()),
            requests: Mutex::new(Vec::new()),
        });
        catalog.set_table_statistics(table_id, Arc::new(evicted));
        catalog.set_statistics_item_loader(loader.clone(), StatisticsLoadWorkers::new());
        (table_id, column_ids, index_id, loader)
    };
    let queued = || {
        tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
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
        catalog.load_needed_histograms("default").unwrap();
        let loaded = catalog.table_statistics(table_id).unwrap();
        for id in column_ids {
            assert!(loaded.column_load_status[&id].is_full_load());
        }
        assert!(loaded.index_load_status[&index_id].is_full_load());
    }
    assert!(!queued(), "domain loading must consume the demand");
    assert_eq!(loader.requests.lock().unwrap().len(), 4);
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
    assert!(
        !queued(),
        "fully loaded replans must not enqueue statistics again"
    );
}

/// Storage bytes are in memory; bootstrap, DDL stats writes, delta writes,
/// startup loaders, cache publication and planner conversion are production code.
#[derive(Default)]
struct StatsStorage(std::collections::BTreeMap<Vec<u8>, Vec<u8>>);

impl tidb_exec::cluster_catalog::MetaSnapshot for StatsStorage {
    fn get(
        &mut self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, tidb_exec::cluster_catalog::ClusterCatalogError> {
        Ok(self.0.get(key).cloned())
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<
        tidb_exec::cluster_catalog::MetaPairs,
        tidb_exec::cluster_catalog::ClusterCatalogError,
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
    fn apply(&mut self, mutations: &[tidb_txnkv::transaction::OptimisticMutation]) {
        use tidb_txnkv::transaction::OptimisticMutationKind;
        for mutation in mutations {
            match mutation.kind() {
                OptimisticMutationKind::LockOnly => {}
                OptimisticMutationKind::MetaDelete
                | OptimisticMutationKind::Delete
                | OptimisticMutationKind::IndexDelete => {
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

struct CanonicalStatistics(Arc<tidb_exec::stats_watch::SharedStats>);

impl tidb_executor::driver::StatisticsSource for CanonicalStatistics {
    fn table(&self, id: i64) -> Option<Arc<tidb_stats::Table>> {
        self.0
            .load()
            .get(&id)
            .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
            .cloned()
    }
}

/// Go TestBuiltinInEstWithoutStats, including the previously omitted
/// InitStatsLite / InitStats / existence-map lifecycle.
#[test]
fn builtin_in_estimate_survives_statistics_initialization() {
    use tidb_exec::cluster_stats_write::{
        insert_table_stats_statements, plan_insert_table_stats_statement,
        plan_stats_delta_statement, stats_delta_statements,
    };
    use tidb_exec::real_tikv_stats::{
        InitialStatsLoad, StatsTarget, load_initial_stats_snapshot_with_memory_limits,
    };

    let mut session = Session::new();
    session.run("CREATE TABLE t(a INT,b INT)").unwrap();
    session
        .run("INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)")
        .unwrap();
    let shared_catalog = session.shared_catalog();
    let table_id = {
        let catalog = shared_catalog.lock().unwrap();
        let tidb_executor::TableEntry::Kv(table) = catalog.table_in("test", "t").unwrap() else {
            panic!("not KV")
        };
        table.table_id
    };
    let parsed = tidb_parser::parse("CREATE TABLE t(a INT,b INT)").unwrap();
    let tidb_ast::Stmt::Ddl(ddl) = parsed else {
        panic!("not DDL")
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("not CREATE")
    };
    let mut table = tidb_exec::table_info_build::build_table_info(
        create,
        "utf8mb4",
        "utf8mb4_bin",
        tidb_exec::table_info_build::ClusteredIndexDefMode::On,
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
    let bootstrap = tidb_exec::mysql_bootstrap::plan_mysql_bootstrap(
        &mut storage,
        100,
        &tidb_exec::mysql_bootstrap::BootstrapEnvironment {
            system_tz: "UTC".to_owned(),
            new_collation_enabled: true,
            cluster_id: 1,
            current_timestamp: now,
            ddl_table_version: 0,
        },
    )
    .unwrap();
    storage.apply(&bootstrap.mutations);
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut storage).unwrap();
    for statement in insert_table_stats_statements(&table, table_id) {
        let write = plan_insert_table_stats_statement(&mut storage, &catalog, &statement, 101, now)
            .unwrap();
        storage.apply(&write.mutations);
    }
    let updates = [tidb_stats_handle_usage::DeltaUpdate {
        table_id,
        delta: tidb_stats_handle_usage::TableDelta {
            delta: 10,
            count: 10,
            init_time: None,
        },
        is_locked: false,
    }];
    for statement in stats_delta_statements(&updates) {
        let write =
            plan_stats_delta_statement(&mut storage, &catalog, &statement, 102, now).unwrap();
        storage.apply(&write.mutations);
    }
    let loader = tidb_exec::cluster_stats_load::ClusterStatsLoader::locate(&catalog).unwrap();
    let cache = Arc::new(tidb_exec::stats_watch::SharedStats::new(Default::default()).unwrap());
    shared_catalog.lock().unwrap().set_statistics_view(Arc::new(
        tidb_executor::driver::StatisticsView::new(Arc::new(CanonicalStatistics(cache.clone()))),
    ));
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
            assert!(
                shared_catalog
                    .lock()
                    .unwrap()
                    .table_statistics(table_id)
                    .is_none()
            );
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
                        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                        .map(AsRef::as_ref),
                )
                .unwrap()
                .unwrap();
            std::collections::BTreeMap::from([(
                table_id,
                tidb_exec::stats_watch::TableStatsState::Loaded(loaded),
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
            .table_statistics(table_id)
            .unwrap();
        assert!(stats.pseudo, "{mode:?}");
        assert!(!stats.cache_pseudo, "{mode:?}");
        assert_eq!(
            stats.column_stats_existence,
            std::collections::BTreeMap::from([(1, false), (2, false)])
        );
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
