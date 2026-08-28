// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 license (see the License file at the crate root).

//! Gap tests for the remaining `pkg/executor/insert_test.go` write behaviors
//! this tier does not implement. Each entry records the Go behavior a future
//! port must satisfy and, where it was measured, how the current surface
//! diverges.

/// Go `insert_test.go:419::TestInsertRuntimeStat`: an
/// `InsertRuntimeStat` (`pkg/executor/insert_common.go:1509`, `String()`
/// :1518, `Clone()` :1580, `Merge()` :1599) built with
/// `CheckInsertTime: 2s`, `Prefetch: 1s` and a `BasicRuntimeStats` recording
/// 5s formats as
/// `prepare: 3s, check_insert: {total_time: 2s, mem_insert_time: 1s, prefetch: 1s}`;
/// after `Merge(Clone())` with another 5s+1 record it doubles to
/// `prepare: 6s, check_insert: {total_time: 4s, mem_insert_time: 2s, prefetch: 2s}`;
/// setting `FKCheckTime = 1s` appends `, fk_check: 1s`. No Rust struct ports
/// this stats surface (`Executor` has no runtime-stats attachment here).
#[test]
#[ignore = "go-parity-gap: InsertRuntimeStat (insert_common.go:1509) has no Rust counterpart; Executor carries no runtime-stats surface in this workspace"]
fn insert_runtime_stat_formats_prepare_check_and_prefetch() {}

/// Go `insert_test.go:534::TestInsertLockUnchangedKeys`: with
/// `tidb_lock_unchanged_keys` set, a REPLACE / insert-ignore /
/// ODKU-unchanged write in one transaction must keep the conflicting KEY
/// locked so a concurrent second `insert into t values (1)` blocks
/// (`pkg/executor/insert_common.go:1227/:1294/:1413` gate the lock on
/// `LockUnchangedKeys` + pessimistic txn). Needs two concurrent sessions, a
/// pessimistic transaction, and block-until-commit semantics; the gateway
/// commits statements immediately and has no lock manager.
#[test]
#[ignore = "go-parity-gap: pessimistic transactions, the lock manager, and tidb_lock_unchanged_keys are unported; the gateway has no concurrent-session blocking to observe"]
fn insert_lock_unchanged_keys_gates_concurrent_writers() {}

/// Go `insert_test.go:618::TestMySQLInsertID` (issue 55965): after an ODKU
/// redirect the OK-packet mysql_insert_id must be the UPDATED row's
/// auto-inc value (`insert ... (a,b) values (1,2) on duplicate key update
/// b = values(b)` reports Session().LastInsertID() == 1), it must be 0 when
/// nothing changed, an explicitly assigned pk reports that pk while
/// `LAST_INSERT_ID()` stays at the last AUTO-ALLOCATED value, and UPDATE
/// touches neither. Measured on this tier: the ODKU redirect publishes the
/// NEWLY allocated id (`Ok((2, Some(3)))` where Go pins 1; the no-change
/// arm reports `Ok((0, Some(4)))` where Go pins 0), the `last_insert_id()`
/// SQL function evaluates to 0, and no session id state survives an UPDATE
/// statement.
#[test]
#[ignore = "go-parity-gap: the OK-packet insert id publishes the newly allocated (not the updated-row) id on ODKU redirects, LAST_INSERT_ID() is not wired to the published value, and UPDATE leaves no session id state -- three measured divergences from insert_test.go:618"]
fn mysql_insert_id_semantics_for_odku_and_explicit_values() {}

/// Go `insert_test.go:710::TestInsertLargeRow`: (unistore-only, self-skipped
/// otherwise at :714) with `tidb_txn_entry_size_limit = 1<<23` a row of
/// 8388493 bytes must fail with the storage error `unistore lock entry too
/// big`. The Rust txn layer has `set_txn_entry_size_limit`
/// (`tidb-txnkv/src/kv_contract.rs:41`) but the session variable is not
/// reachable from the SQL surface, `REPEAT()` is not wired to this tier's
/// write path, and there is no unistore arena to exceed.
#[test]
#[ignore = "go-parity-gap: tidb_txn_entry_size_limit is not settable through the SQL surface and the unistore arena lock-entry error has no Rust counterpart"]
fn insert_large_row_hits_the_entry_size_limit() {}

/// Go `insert_test.go:725::TestInsertDuplicateToGeneratedColumns`: tables
/// with VIRTUAL generated columns feeding multi-valued indexes
/// (`unique key i1 ((cast(j1 as signed array)))`) and
/// `CURRENT_TIMESTAMP ON UPDATE` columns feeding generated chains survive
/// `insert ignore ... on duplicate key update` and `admin check table`
/// under both `tidb_enable_fast_table_check` modes. Multi-valued index
/// encoding, `admin check table`, and the fast-table-check flag have no
/// Rust surface (the gateway also has no session clock for
/// CURRENT_TIMESTAMP -- measured "no session clock (SET timestamp)").
#[test]
#[ignore = "go-parity-gap: multi-valued (cast(... as signed array)) indexes, admin check table, tidb_enable_fast_table_check, and the session clock for CURRENT_TIMESTAMP are unported"]
fn insert_duplicate_to_generated_columns_keeps_indexes_checkable() {}

/// Go `insert_test.go:761::TestInsertNullIntoNotNullGenerated`: a NOT NULL
/// virtual generated column (`GENERATED ALWAYS AS (concat(c1, c1))
/// VIRTUAL NOT NULL`) gates writes -- `insert ... c1 = null` must fail,
/// an `insert ignore ... on duplicate key update ... c1 = null` leaves the
/// row with `c2 = ''` (deletable only by `c2 = ''`, not `c2 is null`).
/// Measured on this tier the gate is absent: `insert into t3 set id = 2,
/// c1 = null` STORES `c2 = NULL`, and the ODKU arms then diverge (a dup-key
/// error where Go proceeds, an affected=2 where Go fails).
#[test]
#[ignore = "go-parity-gap: the NOT NULL constraint is not enforced on virtual generated column results (measured: c2 = NULL stored); the dependent ODKU/ignore/delete arms cannot hold without it"]
fn insert_null_into_not_null_generated_column_gates_the_write() {}
