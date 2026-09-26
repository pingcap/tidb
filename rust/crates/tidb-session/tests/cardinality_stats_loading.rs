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
