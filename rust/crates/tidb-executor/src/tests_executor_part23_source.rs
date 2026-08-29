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

//! Ports of `pkg/executor.part23`: deterministic items 1321--1380 of the
//! upstream executor test enumeration. The running rows use only the
//! in-memory catalog and executor seams owned by this crate. Rows requiring
//! session transactions, TiKV/PD, TiProxy, LOAD DATA, runtime memory
//! measurements, or Go-private helpers remain explicit parity gaps.

use crate::{
    run_create_table_on, run_insert_on, run_insert_reporting, run_select_on, Catalog, StmtContext,
};
use tidb_datatype::Datum;

fn query_ctx() -> StmtContext {
    StmtContext::for_query()
}

fn dml_ctx() -> StmtContext {
    StmtContext::for_dml(false, true, false)
}

fn int_value(datum: &Datum) -> i64 {
    match datum {
        Datum::Int(value) => *value,
        Datum::UInt(value) => i64::try_from(*value).expect("test value fits in i64"),
        other => panic!("expected integer datum, got {other:?}"),
    }
}

/// Go `pkg/executor/test/writetest/write_test.go:42::TestInsertIgnore`.
#[test]
fn insert_ignore_keeps_the_nonduplicate_rows_part23() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int primary key, c int unique key)",
        &mut catalog,
    )
    .expect("table creates");
    run_insert_on("insert into t values (1, 2)", &mut catalog, &dml_ctx()).expect("seed insert");
    run_insert_reporting(
        "insert ignore into t values (1, 3), (2, 3)",
        &mut catalog,
        "test",
        &dml_ctx(),
    )
    .expect("insert ignore succeeds");

    let rows = run_select_on("select id, c from t order by id", &catalog, &query_ctx())
        .expect("select succeeds");
    assert_eq!(rows.len(), 2);
    assert_eq!(int_value(&rows[0][0]), 1);
    assert_eq!(int_value(&rows[0][1]), 2);
    assert_eq!(int_value(&rows[1][0]), 2);
    assert_eq!(int_value(&rows[1][1]), 3);
}

/// Go `pkg/executor/utils_test.go:184::TestWorkerPool`.
#[test]
fn worker_pool_preserves_the_go_submission_contract_part23() {
    let values: Vec<usize> = (0..16).collect();
    let output = crate::worker_pool::map(
        values.iter().map(|value| {
            let value = *value;
            move || {
                if value % 3 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                value * 2
            }
        }),
        2,
    );
    assert_eq!(
        output,
        values.iter().map(|value| value * 2).collect::<Vec<_>>()
    );
}
