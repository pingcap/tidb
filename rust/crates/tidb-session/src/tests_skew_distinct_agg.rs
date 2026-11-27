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

#![cfg(test)]

use crate::tests_support::cell_text;
use crate::{Session, StmtResult};

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => rows
            .iter()
            .map(|row| row.iter().map(cell_text).collect::<Vec<_>>().join("|"))
            .collect(),
        other => panic!("expected rows from `{sql}`, got {other:?}"),
    }
}

#[test]
fn distinct_constant_arguments_reach_the_skew_rewrite() {
    let mut session = Session::new();
    for sql in [
        "create table t (a int, b int)",
        "insert into t values (1,10),(2,10),(3,20)",
        "set tidb_opt_skew_distinct_agg = on",
    ] {
        session.run(sql).unwrap();
    }

    let aggregate_count = match session
        .run("explain select b, count(distinct 1) from t group by b")
        .unwrap()
    {
        StmtResult::Rows(rows) => rows
            .iter()
            .filter(|row| cell_text(&row[0]).contains("HashAgg"))
            .count(),
        other => panic!("expected EXPLAIN rows, got {other:?}"),
    };
    // Live Go master (fdfadb96b2, unistore) with the skew rewrite ON shows
    // THREE HashAgg operators: cop partial dedup, root firstrow-dedup, and
    // the root final count — the skew rewrite splits the distinct into a
    // group-by stage and a final stage beside it. `2` was the pre-rewrite
    // capture.
    assert_eq!(aggregate_count, 3);

    assert_eq!(
        rows(
            &mut session,
            "select b, count(distinct 1) from t group by b order by b"
        ),
        ["10|1", "20|1"]
    );
    assert_eq!(
        rows(
            &mut session,
            "select b, sum(distinct 2) from t group by b order by b"
        ),
        ["10|2", "20|2"]
    );
}

/// Go `TestStreamAggJSONInputBufferReuse` (1de639d2b41): JSON_OBJECTAGG and
/// JSON_ARRAYAGG keep each opaque binary value of a group that spans
/// several Sort output chunks reused by StreamAgg.
#[test]
fn test_stream_agg_json_input_buffer_reuse() {
    use base64::Engine as _;
    let mut tk = crate::testkit::TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_init_chunk_size=32");
    tk.must_exec("set tidb_max_chunk_size=32");
    tk.must_exec("set tidb_streamagg_concurrency=1");
    // (column type, Go mysql type code, stored size)
    for (name, code, size) in [("blob", 252, 2), ("varbinary(8)", 15, 2), ("bit(16)", 16, 2), ("binary(8)", 254, 8)] {
        tk.must_exec("drop table if exists t_json_agg");
        tk.must_exec(&format!("create table t_json_agg(g int, k varchar(8), v {name})"));
        let mut values = Vec::new();
        let mut expected_objects = [std::collections::BTreeMap::new(), std::collections::BTreeMap::new()];
        let mut expected_arrays: [Vec<String>; 2] = [Vec::new(), Vec::new()];
        for i in 0..96usize {
            values.push(format!("({}, '{i:03}', x'{:04x}')", i % 2, i + 1));
            let mut buf = vec![0u8; size];
            buf[1] = (i + 1) as u8;
            let value = format!("base64:type{code}:{}", base64::engine::general_purpose::STANDARD.encode(&buf));
            expected_objects[i % 2].insert(format!("{i:03}"), value.clone());
            expected_arrays[i % 2].push(value);
        }
        tk.must_exec(&format!("insert into t_json_agg values {}", values.join(",")));
        // Each group spans multiple Sort output chunks reused by StreamAgg.
        let sql = "select /*+ stream_agg() */ g, json_objectagg(k, v), json_arrayagg(v) from t_json_agg group by g order by g";
        tk.must_have_plan(sql, "StreamAgg");
        tk.must_have_plan(sql, "Sort");
        let result = tk.must_query(sql);
        let rows = result.rows();
        assert_eq!(rows.len(), 2, "{name}");
        for (group, row) in rows.iter().enumerate() {
            let object: std::collections::BTreeMap<String, String> = serde_json::from_str(&row[1]).unwrap();
            let mut array: Vec<String> = serde_json::from_str(&row[2]).unwrap();
            assert_eq!(object, expected_objects[group], "{name} group {group}");
            array.sort();
            let mut expected = expected_arrays[group].clone();
            expected.sort();
            assert_eq!(array, expected, "{name} group {group}");
        }
    }
}
