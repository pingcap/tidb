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
            .map(|column| column.read().id)
            .collect::<Vec<_>>();
        let index_id = table.indexes()[0].read().id;
        (table.table_id(), column_ids, index_id)
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
        catalog.load_needed_histograms(&shared.stats_handle()).unwrap();
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
        let column_id = table.columns()[0].read().id;
        (table.table_id(), column_id)
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
        catalog.load_needed_histograms(&shared.stats_handle()).unwrap();
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
