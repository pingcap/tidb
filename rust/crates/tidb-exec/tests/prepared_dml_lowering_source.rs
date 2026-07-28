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

//! Mutation and affected-row lowering for bound configured writes.
//!
//! These are the executor contracts from `pkg/executor/insert.go` /
//! `pkg/executor/insert_common.go` (one affected row per added record) and
//! `pkg/executor/update.go` / `pkg/executor/write.go` (a row is affected only
//! when its value actually changes; an unchanged row adds an affected row only
//! under `ClientFoundRows`, which this bounded path never negotiates).
//!
//! Publication itself belongs to the real transaction and is proved on real
//! PD/TiKV by the dependent live slice; nothing here substitutes a mock
//! transport, mock catalog, or in-memory database for that proof.

use tidb_codec::{
    encode_configured_mixed_row, encode_configured_row,
    table_key::{encode_non_unique_index_key, non_unique_index_value},
    ConfiguredRowColumn, ConfiguredValue,
};
use tidb_datatype::Datum;
use tidb_exec::real_tikv_dml::{
    plan_configured_write, plan_delete, plan_insert, plan_update, planned_publication_bounds,
    prepare_text_write,
    ConfiguredWriteError, ConfiguredWritePlan, NoWriteReason, WritePlanningSnapshot,
};
use tidb_planner::{
    configured_catalog::ConfiguredCatalog,
    prepared_dml::{
        lower_prepared_write, ConfiguredAssignment, ConfiguredInsertRow, ConfiguredPreparedWrite,
        PreparedBindValue,
    },
    read_only_scan::{ConfiguredColumn, ConfiguredIndex, ConfiguredScalarType, ConfiguredTable},
};
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::OptimisticMutationKind;

/// Wraps signed integers as the planner's bind currency for these int-only cases.
fn int_binds(params: &[i64]) -> Vec<PreparedBindValue> {
    params.iter().copied().map(PreparedBindValue::Int).collect()
}

const TABLE_ID: i64 = 114;
const ID_COLUMN: i64 = 1;
const BALANCE_COLUMN: i64 = 2;
const BALANCE_INDEX: usize = 1;

fn table() -> ConfiguredTable {
    ConfiguredTable::new(
        "campaign28",
        "accounts",
        TABLE_ID,
        [
            ConfiguredColumn::clustered_primary_key("id", ID_COLUMN),
            ConfiguredColumn::stored_not_null("balance", BALANCE_COLUMN),
        ],
    )
}

fn bound_insert(sql: &str, params: &[i64]) -> ConfiguredPreparedWrite {
    let catalog = ConfiguredCatalog::new([table()]).expect("catalog must validate");
    lower_prepared_write(&tidb_parser::parse(sql).expect("SQL must parse"), &catalog)
        .expect("prepared write must lower")
        .bind(&int_binds(params))
        .expect("bind must succeed")
}

// A sysbench-shaped table: an INT clustered handle, one stored INT, and two
// CHAR columns — the columns a prepared sysbench INSERT binds.
fn mixed_table() -> ConfiguredTable {
    ConfiguredTable::new(
        "sbtest",
        "sbtest1",
        900,
        [
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_int_not_null("k", 2),
            ConfiguredColumn::stored_char_not_null("c", 3, 120),
            ConfiguredColumn::stored_char_not_null("pad", 4, 60),
        ],
    )
}

fn bound_mixed_insert(params: &[PreparedBindValue]) -> ConfiguredPreparedWrite {
    let catalog = ConfiguredCatalog::new([mixed_table()]).expect("catalog must validate");
    lower_prepared_write(
        &tidb_parser::parse("INSERT INTO sbtest.sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?)")
            .expect("SQL must parse"),
        &catalog,
    )
    .expect("prepared write must lower")
    .bind(params)
    .expect("bind must succeed")
}

#[test]
fn a_mixed_int_and_string_insert_routes_each_value_by_column_type() {
    let ConfiguredPreparedWrite::InsertRows { table, rows } = bound_mixed_insert(&[
        PreparedBindValue::Int(1),
        PreparedBindValue::Int(50),
        PreparedBindValue::Bytes(b"hello world".to_vec()),
        PreparedBindValue::Bytes(b"padding".to_vec()),
    ]) else {
        panic!("expected an INSERT command");
    };
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_insert(&table, &rows).expect("insert must plan")
    else {
        panic!("an INSERT always publishes");
    };
    assert_eq!(affected_rows, 1);
    // Stored columns route by type: `k` as an integer, `c`/`pad` as raw string
    // bytes, byte-identical to the mixed-row codec used directly.
    let (expected_key, expected_value) = encode_configured_mixed_row(
        900,
        1,
        &[
            (2, ConfiguredValue::Int(50)),
            (3, ConfiguredValue::Bytes(b"hello world".to_vec())),
            (4, ConfiguredValue::Bytes(b"padding".to_vec())),
        ],
    )
    .expect("mixed row must encode");
    assert_eq!(mutations[0].key(), expected_key);
    assert_eq!(mutations[0].value(), expected_value);
}

#[test]
fn a_string_bound_to_an_integer_column_is_rejected() {
    // `k` is an INT column; a string parameter there is a type error.
    let ConfiguredPreparedWrite::InsertRows { table, rows } = bound_mixed_insert(&[
        PreparedBindValue::Int(1),
        PreparedBindValue::Bytes(b"not an int".to_vec()),
        PreparedBindValue::Bytes(b"hello".to_vec()),
        PreparedBindValue::Bytes(b"pad".to_vec()),
    ]) else {
        panic!("expected an INSERT command");
    };
    assert!(matches!(
        plan_insert(&table, &rows),
        Err(ConfiguredWriteError::ColumnTypeMismatch { .. })
    ));
}

fn stored_row(balance: i64) -> Vec<u8> {
    let (_, value) = encode_configured_row(
        TABLE_ID,
        0,
        &[ConfiguredRowColumn::new(BALANCE_COLUMN, balance)],
    )
    .expect("stored row must encode");
    value
}

fn expected_row(handle: i64, balance: i64) -> (Vec<u8>, Vec<u8>) {
    encode_configured_row(
        TABLE_ID,
        handle,
        &[ConfiguredRowColumn::new(BALANCE_COLUMN, balance)],
    )
    .expect("expected row must encode")
}

// -----------------------------------------------------------------------------
// INSERT
// -----------------------------------------------------------------------------

#[test]
fn one_insert_row_becomes_one_not_exists_mutation_and_one_affected_row() {
    let ConfiguredPreparedWrite::InsertRows { table, rows } = bound_insert(
        "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)",
        &[10, 100],
    ) else {
        panic!("expected an INSERT command");
    };

    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_insert(&table, &rows).expect("insert must plan")
    else {
        panic!("an INSERT always publishes");
    };
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations.len(), 1);

    let (key, value) = expected_row(10, 100);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::Insert);
    assert_eq!(mutations[0].key(), key);
    assert_eq!(mutations[0].value(), value);
}

#[test]
fn every_inserted_row_counts_exactly_once() {
    let ConfiguredPreparedWrite::InsertRows { table, rows } = bound_insert(
        "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?), (?, ?)",
        &[10, 100, 11, 110],
    ) else {
        panic!("expected an INSERT command");
    };

    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_insert(&table, &rows).expect("insert must plan")
    else {
        panic!("an INSERT always publishes");
    };
    assert_eq!(affected_rows, 2);
    assert_eq!(mutations.len(), 2);
    assert_eq!(mutations[0].key(), expected_row(10, 100).0);
    assert_eq!(mutations[1].key(), expected_row(11, 110).0);
}

#[test]
fn the_written_column_order_never_changes_the_persisted_bytes() {
    let catalog_order = bound_insert(
        "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)",
        &[10, 100],
    );
    let written_order = bound_insert(
        "INSERT INTO campaign28.accounts (balance, id) VALUES (?, ?)",
        &[100, 10],
    );

    let plan = |write: ConfiguredPreparedWrite| {
        let ConfiguredPreparedWrite::InsertRows { table, rows } = write else {
            panic!("expected an INSERT command");
        };
        let ConfiguredWritePlan::Write { mutations, .. } =
            plan_insert(&table, &rows).expect("insert must plan")
        else {
            panic!("an INSERT always publishes");
        };
        (mutations[0].key().to_vec(), mutations[0].value().to_vec())
    };
    assert_eq!(plan(catalog_order), plan(written_order));
}

#[test]
fn one_statement_cannot_insert_the_same_handle_twice() {
    let ConfiguredPreparedWrite::InsertRows { table, rows } = bound_insert(
        "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?), (?, ?)",
        &[10, 100, 10, 110],
    ) else {
        panic!("expected an INSERT command");
    };
    assert_eq!(
        plan_insert(&table, &rows),
        Err(ConfiguredWriteError::DuplicateHandle(10))
    );
}

// -----------------------------------------------------------------------------
// UPDATE
// -----------------------------------------------------------------------------

#[test]
fn a_changed_row_publishes_one_exists_mutation_and_reports_one_row() {
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_update(
        &table(),
        10,
        BALANCE_INDEX,
        ConfiguredAssignment::Set(150),
        Some(&stored_row(100)),
    )
    .expect("update must plan")
    else {
        panic!("a changed row publishes");
    };
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations.len(), 1);

    let (key, value) = expected_row(10, 150);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
    assert_eq!(mutations[0].key(), key);
    assert_eq!(mutations[0].value(), value);
}

#[test]
fn an_unchanged_row_publishes_nothing_without_client_found_rows() {
    assert_eq!(
        plan_update(
            &table(),
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Set(100),
            Some(&stored_row(100)),
        ),
        Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::UnchangedRow,
        })
    );
    // Adding zero is the same unchanged row through the arithmetic shape.
    assert_eq!(
        plan_update(
            &table(),
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Add(0),
            Some(&stored_row(100)),
        ),
        Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::UnchangedRow,
        })
    );
}

#[test]
fn a_point_delete_publishes_one_exists_asserted_delete_mutation() {
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_delete(&table(), 10, Some(&stored_row(100))).expect("delete must plan")
    else {
        panic!("an existing row publishes a delete");
    };
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::Delete);
    // The delete targets the row record key (independent of the row value) and
    // carries no value.
    assert_eq!(mutations[0].key(), expected_row(10, 100).0);
    assert!(mutations[0].value().is_empty());
}

#[test]
fn a_delete_of_a_missing_row_publishes_nothing() {
    assert_eq!(
        plan_delete(&table(), 10, None),
        Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::MissingRow,
        })
    );
}

#[test]
fn a_missing_row_matches_nothing_and_publishes_nothing() {
    assert_eq!(
        plan_update(
            &table(),
            999,
            BALANCE_INDEX,
            ConfiguredAssignment::Set(150),
            None,
        ),
        Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::MissingRow,
        })
    );
}

#[test]
fn arithmetic_update_reads_the_snapshot_value_it_adds_to() {
    let ConfiguredWritePlan::Write { mutations, .. } = plan_update(
        &table(),
        10,
        BALANCE_INDEX,
        ConfiguredAssignment::Add(7),
        Some(&stored_row(150)),
    )
    .expect("update must plan") else {
        panic!("a changed row publishes");
    };
    assert_eq!(mutations[0].value(), expected_row(10, 157).1);
}

#[test]
fn signed_addition_fails_closed_exactly_where_go_overflows() {
    // Go: (a > 0 && b > MaxInt64-a) || (a < 0 && b < MinInt64-a)
    // -- pkg/expression/builtin_arithmetic.go, the signed/signed PLUS case.
    assert_eq!(
        plan_update(
            &table(),
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Add(1),
            Some(&stored_row(i64::MAX)),
        ),
        Err(ConfiguredWriteError::Overflow {
            column: "balance".to_owned(),
            current: i64::MAX,
            addend: 1,
        })
    );
    assert_eq!(
        plan_update(
            &table(),
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Add(-1),
            Some(&stored_row(i64::MIN)),
        ),
        Err(ConfiguredWriteError::Overflow {
            column: "balance".to_owned(),
            current: i64::MIN,
            addend: -1,
        })
    );

    // The exact boundary below the overflow stays admitted.
    let ConfiguredWritePlan::Write { mutations, .. } = plan_update(
        &table(),
        10,
        BALANCE_INDEX,
        ConfiguredAssignment::Add(1),
        Some(&stored_row(i64::MAX - 1)),
    )
    .expect("the boundary value must remain admitted") else {
        panic!("a changed row publishes");
    };
    assert_eq!(mutations[0].value(), expected_row(10, i64::MAX).1);
}

#[test]
fn a_row_missing_its_configured_column_fails_closed() {
    let foreign_row = encode_configured_row(
        TABLE_ID,
        0,
        &[ConfiguredRowColumn::new(BALANCE_COLUMN + 40, 100)],
    )
    .expect("row must encode")
    .1;

    assert!(matches!(
        plan_update(
            &table(),
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Set(150),
            Some(&foreign_row),
        ),
        Err(ConfiguredWriteError::RowRead(_))
    ));
    assert!(matches!(
        plan_update(
            &table(),
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Set(150),
            Some(b"not a row"),
        ),
        Err(ConfiguredWriteError::RowRead(_))
    ));
}

#[test]
fn an_update_rewrites_every_stored_column_not_just_the_assigned_one() {
    let wide = ConfiguredTable::new(
        "campaign28",
        "accounts",
        TABLE_ID,
        [
            ConfiguredColumn::clustered_primary_key("id", ID_COLUMN),
            ConfiguredColumn::stored_not_null("balance", BALANCE_COLUMN),
            ConfiguredColumn::stored_not_null("reserved", BALANCE_COLUMN + 1),
        ],
    );
    let stored = encode_configured_row(
        TABLE_ID,
        0,
        &[
            ConfiguredRowColumn::new(BALANCE_COLUMN, 100),
            ConfiguredRowColumn::new(BALANCE_COLUMN + 1, 42),
        ],
    )
    .expect("stored row must encode")
    .1;

    let ConfiguredWritePlan::Write { mutations, .. } = plan_update(
        &wide,
        10,
        BALANCE_INDEX,
        ConfiguredAssignment::Set(150),
        Some(&stored),
    )
    .expect("update must plan") else {
        panic!("a changed row publishes");
    };

    let expected = encode_configured_row(
        TABLE_ID,
        10,
        &[
            ConfiguredRowColumn::new(BALANCE_COLUMN, 150),
            ConfiguredRowColumn::new(BALANCE_COLUMN + 1, 42),
        ],
    )
    .expect("expected row must encode")
    .1;
    assert_eq!(
        mutations[0].value(),
        expected,
        "an untouched stored column must survive the rewrite unchanged"
    );
}

// -----------------------------------------------------------------------------
// Signed INT (int32) stored columns
// -----------------------------------------------------------------------------

/// A table whose stored `balance` column is a signed `INT`, matching sysbench's
/// `k INTEGER`.
fn int_table() -> ConfiguredTable {
    ConfiguredTable::new(
        "campaign28",
        "accounts",
        TABLE_ID,
        [
            ConfiguredColumn::clustered_primary_key("id", ID_COLUMN),
            ConfiguredColumn::stored_int_not_null("balance", BALANCE_COLUMN),
        ],
    )
}

/// Binds a one-row INSERT through the real planner against the `INT` catalog.
///
/// Binding itself never range-checks — it just packs signed values — so this
/// returns the bound rows and the resolved INT table for `plan_insert` to
/// validate, exactly as the server path does.
fn bound_int_insert(params: &[i64]) -> (ConfiguredTable, Vec<ConfiguredInsertRow>) {
    let catalog = ConfiguredCatalog::new([int_table()]).expect("catalog must validate");
    let write = lower_prepared_write(
        &tidb_parser::parse("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)")
            .expect("SQL must parse"),
        &catalog,
    )
    .expect("prepared write must lower")
    .bind(&int_binds(params))
    .expect("bind must succeed");
    let ConfiguredPreparedWrite::InsertRows { table, rows } = write else {
        panic!("expected an INSERT command");
    };
    (table, rows)
}

#[test]
fn an_int_column_persists_the_same_bytes_as_a_bigint_of_the_same_value() {
    // rowcodec stores the value's compact width, not the column type, so an INT
    // and a BIGINT holding 100 must produce byte-identical mutations.
    let ConfiguredPreparedWrite::InsertRows {
        table: bigint_table,
        rows: bigint_rows,
    } = bound_insert(
        "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)",
        &[10, 100],
    )
    else {
        panic!("expected an INSERT command");
    };
    let (int_table, int_rows) = bound_int_insert(&[10, 100]);

    let mutation_bytes = |table: &ConfiguredTable, rows: &[ConfiguredInsertRow]| {
        let ConfiguredWritePlan::Write { mutations, .. } =
            plan_insert(table, rows).expect("insert must plan")
        else {
            panic!("an INSERT always publishes");
        };
        (mutations[0].key().to_vec(), mutations[0].value().to_vec())
    };
    assert_eq!(
        mutation_bytes(&bigint_table, &bigint_rows),
        mutation_bytes(&int_table, &int_rows)
    );
}

#[test]
fn an_int_column_reports_int_scan_metadata_not_bigint() {
    // MYSQL_TYPE_LONG = 3 with display length 11, versus LONGLONG = 8 / 20.
    let table = int_table();
    let balance = &table.columns()[BALANCE_INDEX];
    assert_eq!(balance.scalar_type(), ConfiguredScalarType::Int);
    assert_eq!(
        balance.scalar_type().integer_range(),
        Some((i32::MIN as i64, i32::MAX as i64))
    );

    let id = &table.columns()[0];
    assert_eq!(id.scalar_type(), ConfiguredScalarType::BigInt);
}

#[test]
fn an_insert_outside_the_int_domain_fails_closed() {
    let (table, over) = bound_int_insert(&[10, i64::from(i32::MAX) + 1]);
    assert_eq!(
        plan_insert(&table, &over),
        Err(ConfiguredWriteError::ValueOutOfRange {
            column: "balance".to_owned(),
            value: i64::from(i32::MAX) + 1,
            scalar_type: ConfiguredScalarType::Int,
        })
    );
    let (table, under) = bound_int_insert(&[10, i64::from(i32::MIN) - 1]);
    assert!(matches!(
        plan_insert(&table, &under),
        Err(ConfiguredWriteError::ValueOutOfRange { .. })
    ));

    // The exact boundary is admitted.
    let (table, max) = bound_int_insert(&[10, i64::from(i32::MAX)]);
    assert!(matches!(
        plan_insert(&table, &max),
        Ok(ConfiguredWritePlan::Write { .. })
    ));
}

#[test]
fn an_int_arithmetic_update_overflows_at_the_i32_bound_not_the_i64_bound() {
    // current = i32::MAX, +1: the i64 addition does not wrap, but storing into
    // an INT column does — exactly Go's ConvertIntToInt overflow.
    let stored = stored_row(i64::from(i32::MAX));
    assert_eq!(
        plan_update(
            &int_table(),
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Add(1),
            Some(&stored),
        ),
        Err(ConfiguredWriteError::ValueOutOfRange {
            column: "balance".to_owned(),
            value: i64::from(i32::MAX) + 1,
            scalar_type: ConfiguredScalarType::Int,
        })
    );

    // The same +1 on a BIGINT column is admitted: its domain is i64.
    let bigint_stored = stored_row(i64::from(i32::MAX));
    let ConfiguredWritePlan::Write { mutations, .. } = plan_update(
        &table(),
        10,
        BALANCE_INDEX,
        ConfiguredAssignment::Add(1),
        Some(&bigint_stored),
    )
    .expect("a BIGINT admits values beyond the INT range") else {
        panic!("a changed row publishes");
    };
    assert_eq!(
        mutations[0].value(),
        expected_row(10, i64::from(i32::MAX) + 1).1
    );
}

#[test]
fn a_direct_int_update_outside_the_domain_fails_closed() {
    let table = int_table();
    let stored = stored_row(100);
    assert_eq!(
        plan_update(
            &table,
            10,
            BALANCE_INDEX,
            ConfiguredAssignment::Set(i64::from(i32::MAX) + 1),
            Some(&stored),
        ),
        Err(ConfiguredWriteError::ValueOutOfRange {
            column: "balance".to_owned(),
            value: i64::from(i32::MAX) + 1,
            scalar_type: ConfiguredScalarType::Int,
        })
    );
}

// -----------------------------------------------------------------------------
// Secondary index maintenance (a non-unique index on `balance`)
// -----------------------------------------------------------------------------

const BALANCE_INDEX_ID: i64 = 7;

fn indexed_table() -> ConfiguredTable {
    table().with_indexes([ConfiguredIndex::non_unique(
        BALANCE_INDEX_ID,
        BALANCE_COLUMN,
    )])
}

fn expected_index_entry(balance: i64, handle: i64) -> Vec<u8> {
    encode_non_unique_index_key(
        TABLE_ID,
        BALANCE_INDEX_ID,
        &[Datum::new_int(balance)],
        handle,
    )
    .expect("index key encodes")
}

#[test]
fn insert_adds_one_non_unique_index_entry_beside_the_record() {
    let catalog = ConfiguredCatalog::new([indexed_table()]).expect("catalog must validate");
    let ConfiguredPreparedWrite::InsertRows { table, rows } = lower_prepared_write(
        &tidb_parser::parse("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)")
            .expect("SQL must parse"),
        &catalog,
    )
    .expect("prepared write must lower")
    .bind(&int_binds(&[10, 100]))
    .expect("bind must succeed") else {
        panic!("expected an INSERT command");
    };
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_insert(&table, &rows).expect("insert must plan")
    else {
        panic!("an INSERT always publishes");
    };
    // One affected row, but two mutations committed together: the record and its
    // one index entry — so the index can never lag the row.
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations.len(), 2);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::Insert);
    assert_eq!(mutations[1].kind(), OptimisticMutationKind::IndexPut);
    assert_eq!(mutations[1].key(), expected_index_entry(100, 10));
    assert_eq!(mutations[1].value(), non_unique_index_value());
}

#[test]
fn delete_removes_the_index_entry_for_the_stored_value() {
    let ConfiguredWritePlan::Write { mutations, .. } =
        plan_delete(&indexed_table(), 10, Some(&stored_row(100))).expect("delete must plan")
    else {
        panic!("an existing row deletes");
    };
    assert_eq!(mutations.len(), 2);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::Delete);
    assert_eq!(mutations[1].kind(), OptimisticMutationKind::IndexDelete);
    assert_eq!(mutations[1].key(), expected_index_entry(100, 10));
    assert!(mutations[1].value().is_empty());
}

#[test]
fn a_missing_delete_touches_no_index() {
    assert_eq!(
        plan_delete(&indexed_table(), 10, None),
        Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::MissingRow,
        })
    );
}

#[test]
fn updating_the_indexed_column_moves_its_entry_from_old_to_new() {
    let ConfiguredWritePlan::Write { mutations, .. } = plan_update(
        &indexed_table(),
        10,
        BALANCE_INDEX,
        ConfiguredAssignment::Set(250),
        Some(&stored_row(100)),
    )
    .expect("update must plan") else {
        panic!("a changed row publishes");
    };
    // The record put, then the old index entry removed and the new one added.
    assert_eq!(mutations.len(), 3);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
    assert_eq!(mutations[1].kind(), OptimisticMutationKind::IndexDelete);
    assert_eq!(mutations[1].key(), expected_index_entry(100, 10));
    assert_eq!(mutations[2].kind(), OptimisticMutationKind::IndexPut);
    assert_eq!(mutations[2].key(), expected_index_entry(250, 10));
}

#[test]
fn updating_an_unindexed_column_leaves_the_index_alone() {
    // A table whose `a` is indexed but `b` is not: updating `b` writes only the
    // record, since `a`'s value — and therefore its index entry — is unchanged.
    let table = ConfiguredTable::new(
        "campaign28",
        "two",
        500,
        [
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_int_not_null("a", 2),
            ConfiguredColumn::stored_int_not_null("b", 3),
        ],
    )
    .with_indexes([ConfiguredIndex::non_unique(9, 2)]);
    let (_, stored) = encode_configured_row(
        500,
        0,
        &[
            ConfiguredRowColumn::new(2, 100),
            ConfiguredRowColumn::new(3, 200),
        ],
    )
    .expect("stored row encodes");
    let ConfiguredWritePlan::Write { mutations, .. } = plan_update(
        &table,
        10,
        2, // column index of `b`, the unindexed column
        ConfiguredAssignment::Set(999),
        Some(&stored),
    )
    .expect("update must plan") else {
        panic!("a changed row publishes");
    };
    assert_eq!(
        mutations.len(),
        1,
        "only the record mutation, no index change"
    );
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
}

// -----------------------------------------------------------------------------
// UPDATE over a CHAR-bearing row (sysbench `UPDATE sbtest SET k=k+1`)
// -----------------------------------------------------------------------------

fn stored_mixed_row(k: i64, c: &[u8], pad: &[u8]) -> Vec<u8> {
    let (_, value) = encode_configured_mixed_row(
        900,
        0,
        &[
            (2, ConfiguredValue::Int(k)),
            (3, ConfiguredValue::Bytes(c.to_vec())),
            (4, ConfiguredValue::Bytes(pad.to_vec())),
        ],
    )
    .expect("stored mixed row encodes");
    value
}

#[test]
fn updating_an_int_column_preserves_the_char_columns_of_a_mixed_row() {
    // sysbench `UPDATE sbtest SET k=k+1 WHERE id=?`: k increments while c and pad
    // survive the row rewrite as their raw bytes, never misread as integers.
    let stored = stored_mixed_row(50, b"hello world", b"padding");
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_update(
        &mixed_table(),
        1,
        1, // column index of `k`
        ConfiguredAssignment::Add(1),
        Some(&stored),
    )
    .expect("update must plan")
    else {
        panic!("a changed row publishes");
    };
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations.len(), 1, "no index configured on this table");
    // k became 51; c and pad are byte-identical to the stored row.
    let (expected_key, expected_value) = encode_configured_mixed_row(
        900,
        1,
        &[
            (2, ConfiguredValue::Int(51)),
            (3, ConfiguredValue::Bytes(b"hello world".to_vec())),
            (4, ConfiguredValue::Bytes(b"padding".to_vec())),
        ],
    )
    .expect("expected row encodes");
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
    assert_eq!(mutations[0].key(), expected_key);
    assert_eq!(mutations[0].value(), expected_value);
}

#[test]
fn a_set_that_would_overflow_the_int_column_still_fails_on_a_mixed_row() {
    // The column-domain check runs before the CHAR columns are carried forward,
    // so an out-of-range INT assignment fails closed exactly as on an all-int row.
    let stored = stored_mixed_row(i64::from(i32::MAX), b"c", b"p");
    assert!(matches!(
        plan_update(
            &mixed_table(),
            1,
            1,
            ConfiguredAssignment::Add(1),
            Some(&stored),
        ),
        Err(ConfiguredWriteError::ValueOutOfRange { .. })
    ));
}

fn char_update_template(sql: &str) -> tidb_planner::prepared_dml::ConfiguredPreparedWriteTemplate {
    let catalog = ConfiguredCatalog::new([mixed_table()]).expect("catalog validates");
    lower_prepared_write(&tidb_parser::parse(sql).expect("SQL must parse"), &catalog)
        .expect("lowering admits the assignment")
}

#[test]
fn setting_a_char_column_binds_a_string_as_a_bytes_assignment() {
    // sysbench `UPDATE sbtest SET c=? WHERE id=?`: the string parameter binds
    // into the CHAR column as a raw-bytes assignment.
    let template = char_update_template("UPDATE sbtest.sbtest1 SET c=? WHERE id=?");
    let bound = template
        .bind(&[
            PreparedBindValue::Bytes(b"replacement".to_vec()),
            PreparedBindValue::Int(1),
        ])
        .expect("a string binds to a CHAR column");
    let ConfiguredPreparedWrite::UpdatePoint { assignment, .. } = bound else {
        panic!("expected an UPDATE command");
    };
    assert_eq!(
        assignment,
        ConfiguredAssignment::SetBytes(b"replacement".to_vec())
    );
}

#[test]
fn a_char_and_an_int_assignment_reject_the_wrong_parameter_kind() {
    // An integer into a CHAR column, or a string into an INT column, is a type
    // error at bind — each column takes only its own kind.
    let char_template = char_update_template("UPDATE sbtest.sbtest1 SET c=? WHERE id=?");
    assert!(char_template
        .bind(&[PreparedBindValue::Int(7), PreparedBindValue::Int(1)])
        .is_err());

    let int_template = char_update_template("UPDATE sbtest.sbtest1 SET k=? WHERE id=?");
    assert!(int_template
        .bind(&[
            PreparedBindValue::Bytes(b"not an int".to_vec()),
            PreparedBindValue::Int(1),
        ])
        .is_err());
}

#[test]
fn setting_a_char_column_replaces_only_its_bytes_and_keeps_the_int_columns() {
    // `UPDATE sbtest SET c=? WHERE id=?`: c takes the new bytes while k and pad
    // are carried forward unchanged.
    let stored = stored_mixed_row(50, b"old value", b"padding");
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan_update(
        &mixed_table(),
        1,
        2, // column index of `c`
        ConfiguredAssignment::SetBytes(b"new value".to_vec()),
        Some(&stored),
    )
    .expect("update must plan")
    else {
        panic!("a changed row publishes");
    };
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations.len(), 1);
    let (_, expected_value) = encode_configured_mixed_row(
        900,
        1,
        &[
            (2, ConfiguredValue::Int(50)),
            (3, ConfiguredValue::Bytes(b"new value".to_vec())),
            (4, ConfiguredValue::Bytes(b"padding".to_vec())),
        ],
    )
    .expect("row encodes");
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
    assert_eq!(mutations[0].value(), expected_value);
}

#[test]
fn setting_a_char_column_to_its_current_value_writes_nothing() {
    let stored = stored_mixed_row(50, b"same", b"pad");
    assert_eq!(
        plan_update(
            &mixed_table(),
            1,
            2,
            ConfiguredAssignment::SetBytes(b"same".to_vec()),
            Some(&stored),
        ),
        Ok(ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::UnchangedRow,
        })
    );
}

#[test]
fn updating_k_on_an_indexed_mixed_table_moves_the_index_and_keeps_char_columns() {
    // The full sysbench shape: `UPDATE sbtest SET k=k+1 WHERE id=?` on a table
    // with CHAR columns AND a non-unique index on k. The row rewrite preserves
    // c/pad, and the k index entry moves from the old value to the new.
    let table = mixed_table().with_indexes([ConfiguredIndex::non_unique(7, 2)]);
    let stored = stored_mixed_row(50, b"hi", b"pad");
    let ConfiguredWritePlan::Write { mutations, .. } =
        plan_update(&table, 1, 1, ConfiguredAssignment::Add(1), Some(&stored))
            .expect("update must plan")
    else {
        panic!("a changed row publishes");
    };
    assert_eq!(mutations.len(), 3);
    let (_, expected_value) = encode_configured_mixed_row(
        900,
        1,
        &[
            (2, ConfiguredValue::Int(51)),
            (3, ConfiguredValue::Bytes(b"hi".to_vec())),
            (4, ConfiguredValue::Bytes(b"pad".to_vec())),
        ],
    )
    .expect("row encodes");
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
    assert_eq!(mutations[0].value(), expected_value);
    assert_eq!(mutations[1].kind(), OptimisticMutationKind::IndexDelete);
    assert_eq!(
        mutations[1].key(),
        encode_non_unique_index_key(900, 7, &[Datum::new_int(50)], 1).unwrap()
    );
    assert_eq!(mutations[2].kind(), OptimisticMutationKind::IndexPut);
    assert_eq!(
        mutations[2].key(),
        encode_non_unique_index_key(900, 7, &[Datum::new_int(51)], 1).unwrap()
    );
}

#[test]
fn arithmetic_on_a_char_column_is_rejected_at_lowering() {
    // `SET c = c + ?` is integer arithmetic; a CHAR column has no such
    // assignment, so it fails at prepare rather than binding a nonsense value.
    let catalog = ConfiguredCatalog::new([mixed_table()]).expect("catalog validates");
    assert!(lower_prepared_write(
        &tidb_parser::parse("UPDATE sbtest.sbtest1 SET c=c+? WHERE id=?").expect("SQL must parse"),
        &catalog,
    )
    .is_err());
}

#[test]
fn the_update_byte_budget_covers_a_max_length_char_row() {
    // An UPDATE rebuilds the whole row, so its pre-open byte budget must cover a
    // maximally-sized CHAR row (c CHAR(120), pad CHAR(60), at four utf8mb4 bytes
    // per character) or the coordinator rejects the commit as TransactionTooLarge.
    let write = ConfiguredPreparedWrite::UpdatePoint {
        table: mixed_table(),
        handle: 1,
        column_index: 1,
        assignment: ConfiguredAssignment::Add(1),
    };
    let (_, planned_bytes) = planned_publication_bounds(&write).expect("bounds compute");
    let (key, value) = encode_configured_mixed_row(
        900,
        1,
        &[
            (2, ConfiguredValue::Int(i64::MAX)),
            (3, ConfiguredValue::Bytes(vec![b'x'; 120 * 4])),
            (4, ConfiguredValue::Bytes(vec![b'y'; 60 * 4])),
        ],
    )
    .expect("a maximal CHAR row encodes");
    assert!(
        planned_bytes >= key.len() + value.len(),
        "planned byte budget {planned_bytes} must cover the {}-byte max row",
        key.len() + value.len()
    );
}

// -----------------------------------------------------------------------------
// CHAR(N) length enforcement (Go types.ErrDataTooLong, strict sql_mode)
// -----------------------------------------------------------------------------

#[test]
fn an_insert_string_longer_than_its_char_column_is_data_too_long() {
    // c is CHAR(120); a 121-character value overflows with a non-space character.
    let ConfiguredPreparedWrite::InsertRows { table, rows } = bound_mixed_insert(&[
        PreparedBindValue::Int(1),
        PreparedBindValue::Int(50),
        PreparedBindValue::Bytes(vec![b'a'; 121]),
        PreparedBindValue::Bytes(b"pad".to_vec()),
    ]) else {
        panic!("expected an INSERT command");
    };
    assert!(matches!(
        plan_insert(&table, &rows),
        Err(ConfiguredWriteError::DataTooLong {
            max_length: 120,
            char_length: 121,
            ..
        })
    ));
}

#[test]
fn an_insert_string_exactly_at_the_char_limit_is_admitted() {
    let ConfiguredPreparedWrite::InsertRows { table, rows } = bound_mixed_insert(&[
        PreparedBindValue::Int(1),
        PreparedBindValue::Int(50),
        PreparedBindValue::Bytes(vec![b'a'; 120]),
        PreparedBindValue::Bytes(b"pad".to_vec()),
    ]) else {
        panic!("expected an INSERT command");
    };
    assert!(plan_insert(&table, &rows).is_ok());
}

#[test]
fn updating_a_char_column_beyond_its_length_is_data_too_long() {
    let stored = stored_mixed_row(50, b"old", b"pad");
    assert!(matches!(
        plan_update(
            &mixed_table(),
            1,
            2, // column index of `c` (CHAR(120))
            ConfiguredAssignment::SetBytes(vec![b'z'; 121]),
            Some(&stored),
        ),
        Err(ConfiguredWriteError::DataTooLong {
            max_length: 120,
            char_length: 121,
            ..
        })
    ));
}

#[test]
fn updating_a_char_column_with_trailing_space_overflow_truncates_to_the_limit() {
    // 118 'x' + 4 trailing spaces = 122 characters into CHAR(120): the overflow
    // is all whitespace, so it truncates to the first 120 characters (118 'x'
    // and 2 spaces) rather than erroring.
    let stored = stored_mixed_row(50, b"old", b"pad");
    let mut value = vec![b'x'; 118];
    value.extend_from_slice(b"    ");
    let ConfiguredWritePlan::Write { mutations, .. } = plan_update(
        &mixed_table(),
        1,
        2,
        ConfiguredAssignment::SetBytes(value),
        Some(&stored),
    )
    .expect("a whitespace overflow truncates and publishes") else {
        panic!("a changed row publishes");
    };
    let mut expected_c = vec![b'x'; 118];
    expected_c.extend_from_slice(b"  ");
    let (_, expected_value) = encode_configured_mixed_row(
        900,
        1,
        &[
            (2, ConfiguredValue::Int(50)),
            (3, ConfiguredValue::Bytes(expected_c)),
            (4, ConfiguredValue::Bytes(b"pad".to_vec())),
        ],
    )
    .expect("row encodes");
    assert_eq!(mutations[0].value(), expected_value);
}

/// A snapshot that returns one canned observed value and records the keys
/// planning reads, so `plan_configured_write` can be exercised without a live
/// coordinator.
struct MockSnapshot {
    observed: Option<Vec<u8>>,
    reads: Vec<Vec<u8>>,
}

impl WritePlanningSnapshot for MockSnapshot {
    fn read_at_snapshot(
        &mut self,
        key: &[u8],
        _call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, ConfiguredWriteError> {
        self.reads.push(key.to_vec());
        Ok(self.observed.clone())
    }
}

fn call() -> UnaryCallContext {
    UnaryCallContext::with_timeout(std::time::Duration::from_secs(1))
}

/// Lowers and binds any single write on [`table`], the split's test currency.
fn bound_write(sql: &str, params: &[i64]) -> ConfiguredPreparedWrite {
    let catalog = ConfiguredCatalog::new([table()]).expect("catalog must validate");
    lower_prepared_write(&tidb_parser::parse(sql).expect("SQL must parse"), &catalog)
        .expect("prepared write must lower")
        .bind(&int_binds(params))
        .expect("bind must succeed")
}

#[test]
fn plan_configured_write_plans_an_insert_without_reading() {
    // An INSERT enforces absence with the prewrite NotExist assertion, so
    // planning reads no row and produces the insert mutation without committing.
    let write = bound_insert(
        "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)",
        &[10, 100],
    );
    let mut snapshot = MockSnapshot {
        observed: None,
        reads: Vec::new(),
    };
    let plan = plan_configured_write(&mut snapshot, &write, &call()).expect("insert plans");
    assert!(snapshot.reads.is_empty(), "an INSERT reads no row");
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan
    else {
        panic!("an INSERT plans a write");
    };
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::Insert);
}

#[test]
fn plan_configured_write_reads_the_row_then_plans_an_update() {
    // A point UPDATE reads its own row at the snapshot, then plans the rewrite.
    let write = bound_write(
        "UPDATE campaign28.accounts SET balance = ? WHERE id = ?",
        &[500, 10],
    );
    let mut snapshot = MockSnapshot {
        observed: Some(stored_row(100)),
        reads: Vec::new(),
    };
    let plan = plan_configured_write(&mut snapshot, &write, &call()).expect("update plans");
    assert_eq!(
        snapshot.reads.len(),
        1,
        "the UPDATE reads exactly its own row"
    );
    assert!(
        matches!(plan, ConfiguredWritePlan::Write { affected_rows, .. } if affected_rows == 1),
        "a changed row plans exactly one write"
    );
}

#[test]
fn plan_configured_write_reads_then_reports_no_write_for_a_missing_delete() {
    // The row read comes back empty (no value at start_ts): the DELETE affects
    // nothing, exactly as plan_delete reports MissingRow.
    let write = bound_write("DELETE FROM campaign28.accounts WHERE id = ?", &[10]);
    let mut snapshot = MockSnapshot {
        observed: None,
        reads: Vec::new(),
    };
    let plan = plan_configured_write(&mut snapshot, &write, &call()).expect("delete plans");
    assert_eq!(snapshot.reads.len(), 1, "the DELETE reads its own row");
    assert_eq!(
        plan,
        ConfiguredWritePlan::NoWrite {
            reason: NoWriteReason::MissingRow
        }
    );
}

#[test]
fn plan_configured_write_reads_then_plans_a_present_delete() {
    // A row present at start_ts plans a single delete mutation.
    let write = bound_write("DELETE FROM campaign28.accounts WHERE id = ?", &[10]);
    let mut snapshot = MockSnapshot {
        observed: Some(stored_row(100)),
        reads: Vec::new(),
    };
    let plan = plan_configured_write(&mut snapshot, &write, &call()).expect("delete plans");
    assert_eq!(snapshot.reads.len(), 1);
    let ConfiguredWritePlan::Write {
        mutations,
        affected_rows,
    } = plan
    else {
        panic!("a present row plans a delete");
    };
    assert_eq!(affected_rows, 1);
    assert_eq!(mutations[0].kind(), OptimisticMutationKind::Delete);
}

/// Lowers one text-protocol statement against the same configured catalog.
fn text_write(sql: &str) -> Option<ConfiguredPreparedWrite> {
    let catalog = ConfiguredCatalog::new([table()]).expect("catalog must validate");
    prepare_text_write(sql, &catalog)
        .expect("text write must admit")
        .map(|template| template.bind(&[]).expect("a text template binds no values"))
}

#[test]
fn a_text_dml_statement_lowers_to_the_same_bound_write_a_prepared_one_does() {
    for (text, prepared, params) in [
        (
            "INSERT INTO campaign28.accounts (id, balance) VALUES (10, 100)",
            "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)",
            vec![10, 100],
        ),
        (
            "UPDATE campaign28.accounts SET balance = 150 WHERE id = 10",
            "UPDATE campaign28.accounts SET balance = ? WHERE id = ?",
            vec![150, 10],
        ),
        (
            "DELETE FROM campaign28.accounts WHERE id = 10",
            "DELETE FROM campaign28.accounts WHERE id = ?",
            vec![10],
        ),
    ] {
        assert_eq!(
            text_write(text).expect("a DML statement is a write"),
            bound_write(prepared, &params),
            "text and prepared must lower {text} identically"
        );
    }
}

#[test]
fn a_statement_that_is_not_dml_is_left_to_the_read_path() {
    // A query, a non-DML statement, and text that does not parse are all `None`:
    // the caller runs them as ordinary queries so the read path owns the answer
    // (including the parse error).
    for sql in [
        "SELECT id, balance FROM campaign28.accounts WHERE id = 10",
        "SELECT balance FROM campaign28.accounts WHERE id = 10 FOR UPDATE",
        "SHOW TABLES",
        "NOT SQL AT ALL",
    ] {
        assert!(
            text_write(sql).is_none(),
            "{sql} is not a write and must fall through to the read path"
        );
    }
}

#[test]
fn a_text_dml_statement_outside_the_write_boundary_is_refused_not_ignored() {
    let catalog = ConfiguredCatalog::new([table()]).expect("catalog must validate");
    let error = prepare_text_write(
        "UPDATE campaign28.accounts SET balance = 1 WHERE balance = 2",
        &catalog,
    )
    .expect_err("a non-point UPDATE is refused rather than run as a query");
    assert!(
        matches!(error, ConfiguredWriteError::Plan(_)),
        "expected a plan refusal, found {error}"
    );
}
