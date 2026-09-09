// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Ports of `pkg/executor/insert_test.go`'s AUTO_RANDOM id-allocation tests:
//! `TestAutoRandomID` (:307), `TestMultiAutoRandomID` (:347),
//! `TestAutoRandomIDAllowZero` (:389), plus the `TestAllocateContinuousRowID`
//! (:270) gap.
//!
//! The Rust statements run through `run_insert_reporting`, whose second
//! tuple element is Go's OK-packet insert id: the value `tk.Session()
//! .LastInsertID()` reads (mysql_insert_id). Go's tests additionally assert
//! `select last_insert_id()`; on this tier that SQL function evaluates to 0
//! (the session variable Go's `SetLastInsertID` feeds is not wired into the
//! expression surface), so the OK-packet value is the pinned observable, as
//! documented per test.

use crate::{
    run_alter_table_in, run_create_table_on, run_insert_reporting, run_select_on, Catalog,
    StmtContext,
};
use tidb_datatype::Datum;

fn dml_ctx() -> StmtContext {
    StmtContext::for_dml(false, true, false)
}

fn select_ints(catalog: &Catalog, sql: &str) -> Vec<i64> {
    run_select_on(sql, catalog, &StmtContext::for_query())
        .expect("select succeeds")
        .into_iter()
        .map(|row| match &row[0] {
            Datum::Int(value) => *value,
            Datum::UInt(value) => *value as i64,
            other => panic!("unexpected datum {other:?}"),
        })
        .collect()
}

/// Go `insert_test.go:307::TestAutoRandomID`. An `auto_random` clustered key
/// rewrites `NULL`, explicit `0`, and an omitted column into an allocated
/// positive id (`pkg/executor/insert_common.go:1043 adjustAutoRandomDatum`, zero-rewrite gate at :831), and
/// the OK-packet insert id is that first allocated value. The trailing
/// overflow arm pins `alter table ... auto_random_base` rejecting a base
/// above the incremental-bit maximum with Go's
/// `autoid.AutoRandomRebaseOverflow` text
/// (`pkg/meta/autoid/errors.go:61`, overflow value `1<<59`, max `1<<48-1`).
#[test]
fn auto_random_allocates_positive_ids_for_null_zero_and_omitted_and_caps_rebase() {
    let mut catalog = Catalog::default();
    let ctx = dml_ctx();

    run_create_table_on(
        "create table ar (id bigint key clustered auto_random, name char(10))",
        &mut catalog,
    )
    .unwrap();

    // insert_test.go:314-318: explicit NULL allocates.
    let (_, insert_id) = run_insert_reporting(
        "insert into ar(id) values (null)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let ids = select_ints(&catalog, "select id from ar");
    assert_eq!(ids.len(), 1);
    assert!(ids[0] > 0, "allocated id must be positive, got {}", ids[0]);
    assert_eq!(
        insert_id,
        Some(ids[0] as u64),
        "OK-packet id is the allocated value"
    );
    crate::run_delete_on("delete from ar", &mut catalog, &ctx).unwrap();

    // insert_test.go:319-329: explicit 0 is rewritten to an allocated value.
    let (_, insert_id) =
        run_insert_reporting("insert into ar(id) values (0)", &mut catalog, "test", &ctx).unwrap();
    let ids = select_ints(&catalog, "select id from ar");
    assert_eq!(ids.len(), 1);
    assert!(
        ids[0] > 0,
        "0 must be rewritten to a positive id, got {}",
        ids[0]
    );
    assert_eq!(insert_id, Some(ids[0] as u64));
    crate::run_delete_on("delete from ar", &mut catalog, &ctx).unwrap();

    // insert_test.go:330-340: an omitted auto_random column allocates.
    let (_, insert_id) = run_insert_reporting(
        "insert into ar(name) values ('a')",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let ids = select_ints(&catalog, "select id from ar");
    assert_eq!(ids.len(), 1);
    assert!(ids[0] > 0);
    assert_eq!(insert_id, Some(ids[0] as u64));

    // insert_test.go:342-344 (with the auto_random(15) table from :341):
    // rebasing above the incremental bits fails. `1 << (64-5)` overflows the
    // 48 incremental bits shard_exit leaves (`1<<(64-16)-1`).
    run_create_table_on(
        "create table ar2 (id bigint key clustered auto_random(15), name char(10))",
        &mut catalog,
    )
    .unwrap();
    let overflow = 1i64 << (64 - 5);
    let error = run_alter_table_in(
        &format!("alter table ar2 auto_random_base = {overflow}"),
        &mut catalog,
        "test",
        &StmtContext::for_query(),
    )
    .unwrap_err();
    let expected = format!(
        "alter auto_random_base to {overflow} overflows the incremental bits, max allowed base is {}",
        (1i64 << (64 - 16)) - 1
    );
    assert!(
        matches!(&error, crate::DriverError::InvalidAutoRandom(message) if *message == expected),
        "unexpected error: {error:?}"
    );
}

/// Go `insert_test.go:347::TestMultiAutoRandomID`: one statement inserting
/// three auto_random rows (`NULL`, `0`, or omitted) allocates three
/// CONSECUTIVE ids, and the OK-packet insert id is the FIRST of them
/// (`pkg/executor/insert_common.go:826` keeps one rebase across the batch).
#[test]
fn multi_auto_random_allocates_consecutive_ids_within_one_statement() {
    let mut catalog = Catalog::default();
    let ctx = dml_ctx();
    run_create_table_on(
        "create table ar (id bigint key clustered auto_random, name char(10))",
        &mut catalog,
    )
    .unwrap();

    for statement in [
        "insert into ar(id) values (null),(null),(null)",
        "insert into ar(id) values (0),(0),(0)",
        "insert into ar(name) values ('a'),('a'),('a')",
    ] {
        let (affected, insert_id) =
            run_insert_reporting(statement, &mut catalog, "test", &ctx).unwrap();
        assert_eq!(affected, 3, "{statement}");
        let mut ids = select_ints(&catalog, "select id from ar order by id");
        assert_eq!(ids.len(), 3, "{statement}");
        ids.sort_unstable();
        assert!(ids[0] > 0, "{statement}");
        assert_eq!(ids[1], ids[0] + 1, "{statement}");
        assert_eq!(ids[2], ids[0] + 2, "{statement}");
        assert_eq!(
            insert_id,
            Some(ids[0] as u64),
            "{statement}: first id reported"
        );
        crate::run_delete_on("delete from ar", &mut catalog, &ctx).unwrap();
    }
}

/// Go `insert_test.go:389::TestAutoRandomIDAllowZero`: with
/// `NO_AUTO_VALUE_ON_ZERO` in the sql_mode, an explicit `0` is STORED as 0
/// and reported as the OK-packet insert id, while `NULL` still allocates a
/// positive id (`pkg/executor/insert_common.go:831` gates the rewrite on
/// `mysql.ModeNoAutoValueOnZero`). The Rust session input is
/// `StmtContext::with_auto_increment_zero_explicit`, the port of Go's
/// `vars.InNoAutoValueOnZero`.
#[test]
fn auto_random_stores_explicit_zero_with_no_auto_value_on_zero() {
    let mut catalog = Catalog::default();
    let ctx = dml_ctx().with_auto_increment_zero_explicit(true);
    run_create_table_on(
        "create table ar (id bigint key clustered auto_random, name char(10))",
        &mut catalog,
    )
    .unwrap();

    // insert_test.go:400-411: 0 stays 0, and last_insert_id() answers 0 --
    // Go reaches that via SetLastInsertID(e.lastInsertID) with lastInsertID
    // still 0 (insert_common.go:1483). The gateway reports None (nothing
    // allocated), which leaves the session value at its initial 0.
    let (_, insert_id) =
        run_insert_reporting("insert into ar(id) values (0)", &mut catalog, "test", &ctx).unwrap();
    let ids = select_ints(&catalog, "select id from ar");
    assert_eq!(ids, [0]);
    assert_eq!(insert_id, None);
    crate::run_delete_on("delete from ar", &mut catalog, &ctx).unwrap();

    // insert_test.go:412-417: NULL still allocates a positive id.
    let (_, insert_id) = run_insert_reporting(
        "insert into ar(id) values (null)",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let ids = select_ints(&catalog, "select id from ar");
    assert_eq!(ids.len(), 1);
    assert!(ids[0] > 0);
    assert_eq!(insert_id, Some(ids[0] as u64));
}
