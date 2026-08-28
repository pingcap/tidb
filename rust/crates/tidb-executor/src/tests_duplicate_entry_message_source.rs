// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Ports of the statement-level slice of `pkg/executor/insert_test.go:437
//! ::TestDuplicateEntryMessage`, plus its transaction gap.
//!
//! Go loops over the three `EnableClusteredIndex` modes and, for each index
//! shape, requires the duplicate-key failure to print `Duplicate entry
//! '<value>' for key '<table>.<key>'` where `<value>` is the NEW row's
//! stored form joined with `-` across composite key columns
//! (`pkg/executor/insert_common.go` -> `table.duplicateEntryError`; the Rust
//! port is `KvTable::duplicate_entry_error` with the collation-aware index
//! entry encoding). The transaction arms are the gap test below.

use crate::{run_create_table_on, run_insert_reporting, Catalog, StmtContext};

fn strict() -> StmtContext {
    StmtContext::for_dml(false, true, false)
}

fn expect_duplicate(
    catalog: &mut Catalog,
    sql: &str,
    value: &str,
    key: &str,
) {
    let error = run_insert_reporting(sql, catalog, "test", &strict()).unwrap_err();
    assert!(
        matches!(
            &error,
            crate::DriverError::DuplicateEntry { value: v, key: k }
                if v == value && k == key
        ),
        "inserting {sql:?}: expected Duplicate entry '{value}' for key '{key}', got {error:?}"
    );
}

/// Go `insert_test.go:437::TestDuplicateEntryMessage`, the statement-level
/// arms (one representative per key shape; Go repeats each across the three
/// clustered-index session modes, which on this tier is a DDL-time table
/// property, not a session switch):
///
/// * unique `char(10)` under `utf8mb4_general_ci`: re-inserting `'12ak'`
///   over `'12Ak'` duplicates COLLATION-AWARELY and reports the new text
///   (`Duplicate entry '12ak' for key 't.b'`, :449);
/// * `datetime` primary key: the stored form `2020-01-01 00:00:00` is what
///   the message names (:458-461);
/// * `int` primary key (:465-468) and `datetime` unique (:471-474);
/// * composite `(datetime,int,varchar)` primary key under general_ci joins
///   the stored forms with `-` and reports the NEW row's text `'ASDD'`
///   (:477-480), likewise the composite unique key (:483-486);
/// * `insert ignore` under `utf8mb4_unicode_ci` turns the duplicate into a
///   1062 warning carrying the same message (:489-492);
/// * `bigint unsigned` primary key at `18446744073709551615` (:499-503).
#[test]
fn duplicate_entry_message_names_the_new_value_and_key_across_type_shapes() {
    let mut catalog = Catalog::default();

    // :444-449 unique char under general_ci, case-insensitive dup.
    run_create_table_on(
        "create table t(a int, b char(10), unique key(b)) collate utf8mb4_general_ci",
        &mut catalog,
    )
    .unwrap();
    run_insert_reporting("insert into t value (34, '12Ak')", &mut catalog, "test", &strict()).unwrap();
    expect_duplicate(&mut catalog, "insert into t value (34, '12Ak')", "12Ak", "t.b");
    expect_duplicate(&mut catalog, "insert into t value (34, '12ak')", "12ak", "t.b");

    // :456-461 datetime primary key.
    run_create_table_on("create table t2 (a datetime primary key)", &mut catalog).unwrap();
    run_insert_reporting("insert into t2 values ('2020-01-01')", &mut catalog, "test", &strict()).unwrap();
    expect_duplicate(
        &mut catalog,
        "insert into t2 values ('2020-01-01')",
        "2020-01-01 00:00:00",
        "t2.PRIMARY",
    );

    // :465-468 int primary key.
    run_create_table_on("create table t3 (a int primary key)", &mut catalog).unwrap();
    run_insert_reporting("insert into t3 value (1)", &mut catalog, "test", &strict()).unwrap();
    expect_duplicate(&mut catalog, "insert into t3 value (1)", "1", "t3.PRIMARY");

    // :471-474 datetime unique.
    run_create_table_on("create table t4 (a datetime unique)", &mut catalog).unwrap();
    run_insert_reporting("insert into t4 values ('2020-01-01')", &mut catalog, "test", &strict()).unwrap();
    expect_duplicate(
        &mut catalog,
        "insert into t4 values ('2020-01-01')",
        "2020-01-01 00:00:00",
        "t4.a",
    );

    // :477-486 composite keys under general_ci; the message shows the NEW
    // row's casing ('ASDD').
    run_create_table_on(
        "create table t5 (a datetime, b int, c varchar(10), primary key (a, b, c)) collate utf8mb4_general_ci",
        &mut catalog,
    )
    .unwrap();
    run_insert_reporting("insert into t5 values ('2020-01-01', 1, 'aSDd')", &mut catalog, "test", &strict())
        .unwrap();
    expect_duplicate(
        &mut catalog,
        "insert into t5 values ('2020-01-01', 1, 'ASDD')",
        "2020-01-01 00:00:00-1-ASDD",
        "t5.PRIMARY",
    );
    run_create_table_on(
        "create table t6 (a datetime, b int, c varchar(10), unique key (a, b, c)) collate utf8mb4_general_ci",
        &mut catalog,
    )
    .unwrap();
    run_insert_reporting("insert into t6 values ('2020-01-01', 1, 'aSDd')", &mut catalog, "test", &strict())
        .unwrap();
    expect_duplicate(
        &mut catalog,
        "insert into t6 values ('2020-01-01', 1, 'ASDD')",
        "2020-01-01 00:00:00-1-ASDD",
        "t6.a",
    );

    // :489-492 insert ignore under unicode_ci -> warning 1062.
    run_create_table_on(
        "create table t7 (a char(10) collate utf8mb4_unicode_ci, b char(20) collate utf8mb4_general_ci, c int(11), primary key (a, b, c), unique key (a))",
        &mut catalog,
    )
    .unwrap();
    let ignore = StmtContext::for_dml(false, true, true);
    run_insert_reporting("insert ignore into t7 values ('$', 'C', 10)", &mut catalog, "test", &ignore)
        .unwrap();
    run_insert_reporting("insert ignore into t7 values ('$', 'C', 10)", &mut catalog, "test", &ignore)
        .unwrap();
    let warnings = ignore.take_warnings();
    assert!(
        warnings.iter().any(|(_, code, message)| *code == 1062
            && message == "Duplicate entry '$-C-10' for key 't7.PRIMARY'"),
        "expected the 1062 duplicate warning, got {warnings:?}"
    );

    // :499-503 bigint unsigned maximum handle (issue 12420).
    run_create_table_on("create table t8(a bigint unsigned primary key)", &mut catalog).unwrap();
    run_insert_reporting("insert into t8 values(18446744073709551615)", &mut catalog, "test", &strict())
        .unwrap();
    expect_duplicate(
        &mut catalog,
        "insert into t8 values(18446744073709551615)",
        "18446744073709551615",
        "t8.PRIMARY",
    );
}

/// Go `insert_test.go:451-455/:462-465/:494-497`: inside
/// `begin optimistic`/`begin pessimistic` transactions a duplicate that
/// only surfaces at COMMIT (the write was masked by a later `delete` of the
/// conflicting key, or the pessimistic second write) fails the COMMIT with
/// `previous statement: <sql>: [kv:1062]Duplicate entry ...`, naming the
/// offending statement. The Rust gateway executes each statement committed
/// against the catalog as it runs; there is no buffered-transaction replay
/// that could re-raise a masked duplicate at commit time.
#[test]
#[ignore = "go-parity-gap: transaction-conflict duplicate reporting (previous statement: ... at COMMIT) needs the optimistic/pessimistic txn buffer + commit replay; the gateway commits statements immediately"]
fn duplicate_entry_txn_conflict_names_the_previous_statement() {}
