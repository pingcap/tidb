// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Ports of the single-session slice of `pkg/executor/insert_test.go:503
//! ::TestGlobalTempTableParallel`, plus its parallel-session gap.
//!
//! Go runs 8 threads, each inserting `value(0)` then `value(0), (0)` inside
//! a transaction on a `create global temporary table ... on commit delete
//! rows` table, and requires each session's `select max(id)` to see exactly
//! its own three rows with ids stepping 1..3. The auto-increment allocation
//! itself is per-statement batch state (`pkg/table/tables/tables.go`
//! `AddRecord` -> autoid allocator), which the Rust gateway models.

use crate::{run_create_table_on, run_insert_reporting, run_select_on, Catalog, StmtContext};

/// Go `insert_test.go:503::TestGlobalTempTableParallel`, single-session
/// slice: `insert temp_test value(0)` allocates id 1, the two-row
/// `value(0), (0)` allocates 2 and 3, so `select max(id)` is 3 (Go's
/// `maxID := strconv.Itoa(loops * 3)` at :522 with `loops = 1`).
#[test]
fn global_temporary_table_auto_increment_allocates_one_two_three() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_dml(false, true, false);

    run_create_table_on(
        "create global temporary table temp_test(id int primary key auto_increment) on commit delete rows",
        &mut catalog,
    )
    .unwrap();

    let (affected, first) =
        run_insert_reporting("insert temp_test value(0)", &mut catalog, "test", &ctx).unwrap();
    assert_eq!(affected, 1);
    let (affected, rest) =
        run_insert_reporting("insert temp_test value(0), (0)", &mut catalog, "test", &ctx).unwrap();
    assert_eq!(affected, 2);

    let max = run_select_on("select max(id) from temp_test", &catalog, &ctx).unwrap();
    assert_eq!(max.len(), 1);
    assert_eq!(
        max[0][0],
        tidb_datatype::Datum::Int(3),
        "ids allocate 1,2,3"
    );

    // The OK-packet insert id follows the FIRST allocated id of each
    // statement (1, then 2).
    assert_eq!(first, Some(1));
    assert_eq!(rest, Some(2));
}
