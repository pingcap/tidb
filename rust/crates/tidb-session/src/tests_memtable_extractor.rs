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

//! Memory-table predicate extractors as `executor/explainfor.test` records
//! them: the claimed predicates leave the Selection, EXPLAIN prints them as
//! the scan's operator info, and the reader applies them.

use crate::tests_support::row_text;
use crate::Session;

fn explain(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(&format!("explain format='plan_tree' {sql}")))
        .into_iter()
        .map(|row| row.into_iter().skip(1).collect())
        .collect()
}

/// Go `ClusterTableExtractor` and `ClusterLogTableExtractor`: type and
/// instance become request filters, the log's time range a millisecond
/// window, and nothing is left for a Selection.
#[test]
fn cluster_table_predicates_become_the_scan_request() {
    let mut session = Session::new();
    assert_eq!(
        explain(
            &mut session,
            "select * from information_schema.cluster_config where type in ('tikv', 'tidb')"
        ),
        vec![vec![
            "root",
            "table:CLUSTER_CONFIG",
            "node_types:[\"tidb\",\"tikv\"]"
        ]]
    );
    assert_eq!(
        explain(
            &mut session,
            "select * from information_schema.cluster_config where type='tidb' and instance='192.168.1.7:2379'"
        ),
        vec![vec![
            "root",
            "table:CLUSTER_CONFIG",
            "node_types:[\"tidb\"], instances:[\"192.168.1.7:2379\"]"
        ]]
    );
    assert_eq!(
        explain(
            &mut session,
            "select * from information_schema.cluster_log where level in ('warn','error') and time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'"
        ),
        vec![vec![
            "root",
            "table:CLUSTER_LOG",
            "start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, log_levels:[\"error\",\"warn\"]"
        ]]
    );
}

/// Go `clusterLogRetriever.initialize`: the search needs both bounds and at
/// least one narrowing filter; a contradiction reads nothing at all.
#[test]
fn a_cluster_log_search_needs_bounds_and_a_filter() {
    let mut session = Session::new();
    let error_of = |session: &mut Session, sql: &str| {
        session
            .run(sql)
            .err()
            .unwrap_or_else(|| panic!("{sql} was accepted"))
            .to_string()
    };
    assert_eq!(
        error_of(&mut session, "select * from information_schema.cluster_log"),
        "denied to scan logs, please specified the start time, such as `time > '2020-01-01 00:00:00'`"
    );
    assert_eq!(
        error_of(
            &mut session,
            "select * from information_schema.cluster_log where time >= '2019-12-23 16:10:13'"
        ),
        "denied to scan logs, please specified the end time, such as `time < '2020-01-01 00:00:00'`"
    );
    assert_eq!(
        error_of(
            &mut session,
            "select * from information_schema.cluster_log where time >= '2019-12-23 16:10:13' and time < '2019-12-23 16:30:13'"
        ),
        "denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)"
    );
    assert_eq!(
        row_text(session.run(
            "select * from information_schema.cluster_log where time >= '2019-12-23 16:30:13' and time < '2019-12-23 16:10:13'"
        )),
        Vec::<Vec<String>>::new()
    );
}

/// Go `InfoSchemaBaseExtractor`: names are lower-cased and matched
/// case-insensitively, `LIKE` becomes a case-insensitive regexp, and EXPLAIN
/// prints both.
#[test]
fn information_schema_names_are_claimed_case_insensitively() {
    let mut session = Session::new();
    session.run("create table t(a int, key idx1(a))").unwrap();
    assert_eq!(
        explain(
            &mut session,
            "select table_name from information_schema.tables where table_schema='TEST' and table_name='T'"
        ),
        vec![vec![
            "root",
            "table:TABLES",
            "table_name:[\"t\"], table_schema:[\"test\"]"
        ]]
    );
    assert_eq!(
        row_text(session.run(
            "select table_name from information_schema.tables where table_schema='TEST' and table_name='T'"
        )),
        vec![vec!["t"]]
    );
    assert_eq!(
        row_text(session.run(
            "select table_name from information_schema.tables where table_schema='test' and upper(table_name)=upper('t')"
        )),
        vec![vec!["t"]]
    );
    assert_eq!(
        explain(
            &mut session,
            "select * from information_schema.tables where table_name like 'T%'"
        ),
        vec![vec!["root", "table:TABLES", "table_name_pattern:[t%]"]]
    );
    assert_eq!(
        row_text(session.run(
            "select index_name from information_schema.statistics where table_schema='test' and table_name like 'T' and index_name='IDX1'"
        )),
        vec![vec!["idx1"]]
    );
}

/// Go `MetricTableExtractor.GetMetricTablePromQL`: the quantile predicate
/// replaces the table's default quantile in the PromQL, and the step prints
/// as a Go duration.
#[test]
fn a_metric_table_scan_explains_its_prom_ql() {
    let mut session = Session::new();
    let rows = explain(
        &mut session,
        "select * from metrics_schema.tidb_query_duration where quantile = 0.99",
    );
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0][1], "table:tidb_query_duration");
    let info = &rows[0][2];
    assert!(
        info.starts_with(
            "PromQL:histogram_quantile(0.99, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance)), start_time:"
        ),
        "{info}"
    );
    assert!(info.ends_with(", step:1m0s"), "{info}");
}
