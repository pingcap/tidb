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

//! Go master `pkg/executor/test/distsqltest/distsql_test.go`.

use crate::testkit::TestKit;

/// Go `fmt.Sprint(tk.MustQuery(sql).Rows())`.
fn rows_text(tk: &mut TestKit, sql: &str) -> String {
    tk.must_query(sql).rows().iter().map(|row| row.join(" ")).collect::<Vec<_>>().join("\n")
}

fn rows_of(tk: &mut TestKit, sql: &str) -> Vec<String> {
    tk.must_query(sql).rows().iter().map(|row| row.join(" ")).collect()
}

/// The first `max_distsql_concurrency` an EXPLAIN ANALYZE reports.
fn max_distsql_concurrency(analyze: &str) -> usize {
    let at = analyze.find("max_distsql_concurrency: ").unwrap_or_else(|| panic!("no distsql concurrency: {analyze}"));
    let rest = &analyze[at + "max_distsql_concurrency: ".len()..];
    rest[..rest.find(|c: char| !c.is_ascii_digit()).unwrap()].parse().unwrap()
}

/// Go `TestIndexReaderLimitConcurrencyWithUniqueKeyPoints` (e48ac5fef19).
/// Go reads `kv.Request.Concurrency` through `CheckSelectRequestHook`; Rust
/// has no such hook, so the index is split into regions and the reader's
/// `max_distsql_concurrency` (the request concurrency capped by its cop
/// tasks) tells the minimal concurrency (1) from the default.
#[test]
fn test_index_reader_limit_concurrency_with_unique_key_points() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t (
		object_id bigint primary key clustered,
		listing_id bigint default null,
		key idx_lo (listing_id, object_id),
		unique key uk_l (listing_id))",
    );
    tk.must_exec(
        "create table t_no_uk (
		object_id bigint primary key clustered,
		listing_id bigint default null,
		key idx_lo (listing_id, object_id))",
    );
    let values: Vec<String> = (0..200).map(|i| format!("({i}, {})", i * 10)).collect();
    tk.must_exec(&format!("insert into t values {}", values.join(",")));
    tk.must_exec(&format!("insert into t_no_uk values {}", values.join(",")));
    for table in ["t", "t_no_uk"] {
        tk.must_exec(&format!("split table {table} index idx_lo between (0) and (2000) regions 8"));
    }
    let in_list = |n: usize| (0..n).map(|i| (i * 10).to_string()).collect::<Vec<_>>().join(",");
    let query = |table: &str, ids: &str| {
        format!(
            "select listing_id, object_id from {table} force index(idx_lo) where listing_id in ({ids}) \
             order by listing_id, object_id limit 100"
        )
    };

    // 35 unique points can never reach LIMIT 100, so the Scan -> Limit
    // request keeps the default concurrency.
    let q = query("t", &in_list(35));
    let plan = rows_text(&mut tk, &format!("explain format='brief' {q}"));
    assert!(plan.contains("IndexReader"), "{plan}");
    assert!(plan.lines().any(|line| line.contains("Limit") && line.contains("cop[tikv]") && line.contains("offset:0, count:100")), "{plan}");
    assert_eq!(rows_of(&mut tk, &q).len(), 35);
    let analyze = rows_text(&mut tk, &format!("explain analyze {q}"));
    assert!(max_distsql_concurrency(&analyze) > 1, "{analyze}");

    // More unique points than the limit: the limit may stop the scan early,
    // keep the minimal concurrency.
    let q = query("t", &in_list(101));
    assert_eq!(rows_of(&mut tk, &q).len(), 100);
    assert_eq!(max_distsql_concurrency(&rows_text(&mut tk, &format!("explain analyze {q}"))), 1);

    // Without a unique key on listing_id, a point may match many rows.
    let q = query("t_no_uk", &in_list(35));
    assert_eq!(rows_of(&mut tk, &q).len(), 35);
    assert_eq!(max_distsql_concurrency(&rows_text(&mut tk, &format!("explain analyze {q}"))), 1);

    // A unique index allows duplicated NULLs.
    let q = "select listing_id, object_id from t force index(idx_lo) where listing_id is null \
             order by listing_id, object_id limit 100";
    tk.must_query(q).check_empty();
}
