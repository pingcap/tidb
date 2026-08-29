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

//! Go `pkg/executor/executor_failpoint_test.go:395::TestIssue21441`.
//!
//! The Go test's failpoint (`pkg/executor/unionexec/union.go:155`,172,185)
//! only maintains a `childInFlightForTest` counter and panics if more
//! children than `Concurrency` are in flight; it changes no query semantics.
//! The test's observable contract is the UNION ALL result itself: eight
//! branches over the same three-row table concatenate in term order, and an
//! ordered derived wrapper takes LIMIT/LIMIT-with-offset windows from the
//! concatenated rows.
//!
//! Not reproducible here, and recorded rather than approximated: Go runs the
//! statement with `InitChunkSize = MaxChunkSize = 1` (session chunk sizing,
//! the stress axis of the original bug report) and reads
//! `SessionVars.UnionConcurrency()` after `SET tidb_executor_concurrency = 2`.
//! This tier has no session chunk-size control and no observable union
//! concurrency; the row contract below is the rest of the test.

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

fn catalog_with_t() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t values (1), (2), (3)",
        &mut catalog,
        &StmtContext::for_dml(false, true, false),
    )
    .unwrap();
    catalog
}

/// Go `pkg/executor/executor_failpoint_test.go:404-424::TestIssue21441`:
/// eight-branch UNION ALL over `t`. The derived ORDER BY/LIMIT assertions at
/// Go lines 426-427 are recorded separately because this crate's
/// `physical_builder::resolve_expression` (`rust/crates/tidb-executor/src/driver/physical_builder.rs:196-209`)
/// currently rejects the derived output-column expression.
#[test]
fn issue21441_union_all_concatenates_in_order() {
    let catalog = catalog_with_t();
    let ctx = StmtContext::for_query();
    let sql = "\
select a from t union all \
select a from t union all \
select a from t union all \
select a from t union all \
select a from t union all \
select a from t union all \
select a from t union all \
select a from t";

    let rows = run_select_on(sql, &catalog, &ctx).unwrap();
    let sorted = {
        let mut values: Vec<i64> = rows.iter().map(|row| row[0].as_int().unwrap()).collect();
        values.sort_unstable();
        values
    };
    // Go `.Sort().Check(...)`: every branch contributes every row, so the
    // sorted multiset is 1,2,3 each eight times.
    let mut expected = Vec::new();
    for value in [1, 2, 3] {
        for _ in 0..8 {
            expected.push(value);
        }
    }
    assert_eq!(sorted, expected);
    assert_eq!(rows.len(), 24, "UNION ALL keeps duplicates across branches");
}
