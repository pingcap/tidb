// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Sessions over the REAL embedded store with the coprocessor wired.
//!
//! Every other module in this directory serves `cop_scans: None`, so a plan
//! that only misbehaves when base-table scans are answered by the pushdown
//! coprocessor -- `CopScanSource` over the in-process unistore transport --
//! never fails in-tree. This module builds the same stack `--store unistore
//! --cluster-session` boots and pins those plans.

use std::sync::Arc;

use tidb_datatype::Datum;

use super::node_fixture::{rows, session_context, ABC_HASH};
use crate::configured_user_store::ConfiguredUserStore;
use crate::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};
use crate::QuerySessionFactory;

fn cop_backed_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    let config = crate::node_config::NodeConfig::parse([
        "tidb-server",
        "--store",
        "unistore",
        "--cluster-session",
        "--port",
        "0",
        // Parse-time requirement only: the test passes its own user store
        // below, so the flag never has to name real rows.
        "--auth-file",
        "/dev/null",
    ])
    .expect("node config");
    let users = Arc::new(
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store"),
    );
    let stack = unistore_cluster_session_stack(&config, &users).expect("unistore stack");
    (stack, users)
}

fn displayed(rows: Vec<Vec<Datum>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|datum| match datum {
                    Datum::Int(v) => v.to_string(),
                    Datum::Decimal(d) => d.to_string(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

/// The probe-33 regression: a derived table whose inner SELECT plans as a
/// partial-aggregate push (root HashAgg over `TableReader(data:HashAgg)`)
/// must still answer the aggregate, not the bare scan rows. Go returns
/// `((1,30),(2,120))`; the broken handoff returned five raw rows.
#[test]
fn a_derived_aggregate_over_the_coprocessor_answers_its_output() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(7))
        .expect("session opens");
    rows(&mut session, "CREATE TABLE test.rep (g int, v int)");
    rows(
        &mut session,
        "INSERT INTO test.rep VALUES (1, 10), (1, 20), (2, 40), (2, 80), (1, 0)",
    );

    let inner = displayed(rows(
        &mut session,
        "SELECT g, sum(v) AS t FROM test.rep GROUP BY g ORDER BY g",
    ));
    assert_eq!(
        inner,
        [["1", "30"], ["2", "120"]],
        "the inner aggregate alone must already be right"
    );

    let derived = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, sum(v) AS t FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(
        derived,
        [["1", "30"], ["2", "120"]],
        "the derived consumer must see the aggregate, not the scan rows"
    );

    // The COUNT(*) shape panicked the worker thread on the live node
    // (chunk column index out of bounds); here a panic fails the test.
    let counted = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, count(*) AS c FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(counted, [["1", "3"], ["2", "2"]]);

    // The desc keep-order Limit rides the REVERSED region walk -- the
    // shape whose first live draft returned NOTHING because the reverse
    // scan's caller-swaps-the-bounds contract was missed.
    rows(&mut session, "CREATE TABLE test.walk (id bigint primary key, v int)");
    rows(
        &mut session,
        "INSERT INTO test.walk VALUES (1, 10), (2, 20), (3, 30), (5, 50), (100, 1)",
    );
    let descending = displayed(rows(
        &mut session,
        "SELECT id FROM test.walk WHERE id > 1 ORDER BY id DESC LIMIT 2",
    ));
    assert_eq!(
        descending,
        [["100"], ["5"]],
        "the desc keep-order Limit must answer the LARGEST ids over the region walk"
    );

    // The covering-index COUNT rides an [IndexScan, Aggregation] DAG:
    // the region decodes the indexed values out of the KEY and counts
    // them, Go's PhysicalIndexReader carrying the partial stage.
    rows(&mut session, "CREATE INDEX walk_v ON test.walk (v)");
    let counted_over_index = displayed(rows(
        &mut session,
        "SELECT count(v) FROM test.walk WHERE v > 5",
    ));
    assert_eq!(counted_over_index, [["4"]]);

    // The receipt that the partial stage ran AT THE REGION: the scanner's
    // request log names an aggregation executor in a served DAG. A refusal
    // would fall back to the local partial cursor -- same answer, but the
    // lowering this test pins would silently be dead.
    let stats = stack.cop_source.stats();
    assert!(
        stats
            .requests
            .iter()
            .any(|request| request.contains("HashAgg") || request.contains("StreamAgg")),
        "no served DAG carried an aggregation executor: {:?}",
        stats.requests
    );
    assert!(
        stats
            .requests
            .iter()
            .any(|request| request.contains("IndexScan")),
        "no served DAG carried the covering-index aggregate: {:?}",
        stats.requests
    );
}
