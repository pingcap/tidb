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

//! Pinned `executorBuilder.buildUnionScanExec/buildUnionScanFromReader`:
//! dirty-table physical plans must execute through their retained UnionScan
//! node, while point reads keep using the reader's own transaction overlay.

use tidb_datatype::Datum;
use tidb_executor::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

#[test]
fn dirty_table_union_scan_plan_executes_conditions_and_index_order() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE union_scan_build (\
            id BIGINT PRIMARY KEY, value BIGINT, INDEX value_idx(value))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO union_scan_build VALUES (3,30),(1,10),(2,20)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT id, value FROM union_scan_build USE INDEX(value_idx) \
             WHERE value >= 20 ORDER BY value",
            &catalog,
            &ctx,
        )
        .unwrap(),
        vec![
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(3), Datum::Int(30)],
        ]
    );

    assert_eq!(
        run_select_on(
            "SELECT value FROM union_scan_build WHERE id = 2",
            &catalog,
            &ctx,
        )
        .unwrap(),
        vec![vec![Datum::Int(20)]]
    );
}
