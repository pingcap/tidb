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

//! `ORDER BY` resolution for configured prepared reads.
//!
//! An `ORDER BY` without a `LIMIT` is a SQL-layer sort over the projected
//! output rows. The template resolves each order item to a projected output
//! offset, its direction, and the column's scalar type (which selects the
//! executor's signed-integer versus `utf8mb4_bin` comparison), and fails closed
//! on anything the narrow read cannot honor over already-projected rows.

use tidb_planner::{
    configured_catalog::ConfiguredCatalog,
    configured_order_limit_contract::ConfiguredOrderDirection,
    read_only_scan::{
        lower_prepared_point_read, ConfiguredColumn, ConfiguredScalarType, ConfiguredTable,
        PreparedAggregateKind, PreparedPlanError, ReadOnlyScanError, UnsupportedReadOnlyFeature,
    },
};

fn catalog() -> ConfiguredCatalog {
    // The sysbench `sbtest` shape: a clustered BIGINT handle, one stored INT,
    // and two `utf8mb4_bin` CHAR columns.
    ConfiguredCatalog::new([ConfiguredTable::new(
        "sbtest",
        "sbtest1",
        100,
        [
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_int_not_null("k", 2),
            ConfiguredColumn::stored_char_not_null("c", 3, 120),
            ConfiguredColumn::stored_char_not_null("pad", 4, 60),
        ],
    )])
    .expect("test catalog is valid")
}

fn select(sql: &str) -> tidb_ast::SelectStmt {
    let tidb_ast::Stmt::Query(query) = tidb_parser::parse(sql).expect("SQL must parse") else {
        panic!("expected query statement");
    };
    let tidb_ast::QueryStmt::Select(select) = *query else {
        panic!("expected SELECT");
    };
    *select
}

fn order_by(sql: &str) -> Vec<(usize, ConfiguredOrderDirection, ConfiguredScalarType)> {
    lower_prepared_point_read(&select(sql), &catalog())
        .expect("prepared read must lower")
        .order_by()
        .iter()
        .map(|key| (key.output_offset(), key.direction(), key.scalar_type()))
        .collect()
}

fn ordering_rejection(sql: &str) -> PreparedPlanError {
    lower_prepared_point_read(&select(sql), &catalog())
        .expect_err("statement must fail to lower")
}

fn is_distinct(sql: &str) -> bool {
    lower_prepared_point_read(&select(sql), &catalog())
        .expect("prepared read must lower")
        .is_distinct()
}

#[test]
fn range_read_orders_by_the_projected_char_column() {
    // sysbench read 4: `SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c`.
    assert_eq!(
        order_by("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c"),
        vec![(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 120 }
        )],
    );
}

#[test]
fn descending_order_on_the_clustered_handle_resolves_to_bigint() {
    assert_eq!(
        order_by("SELECT id, c FROM sbtest1 WHERE id = ? ORDER BY id DESC"),
        vec![(0, ConfiguredOrderDirection::Descending, ConfiguredScalarType::BigInt)],
    );
}

#[test]
fn order_offset_follows_projection_order_not_catalog_order() {
    // `k` is the second projected column, so its output offset is 1 even though
    // it is the second catalog column and `c` sorts by string.
    assert_eq!(
        order_by("SELECT c, k FROM sbtest1 WHERE id = ? ORDER BY k"),
        vec![(1, ConfiguredOrderDirection::Ascending, ConfiguredScalarType::Int)],
    );
}

#[test]
fn multiple_order_keys_retain_source_order() {
    assert_eq!(
        order_by("SELECT k, c FROM sbtest1 WHERE id = ? ORDER BY c DESC, k"),
        vec![
            (
                1,
                ConfiguredOrderDirection::Descending,
                ConfiguredScalarType::Char { max_length: 120 }
            ),
            (0, ConfiguredOrderDirection::Ascending, ConfiguredScalarType::Int),
        ],
    );
}

#[test]
fn an_unordered_read_carries_no_order_keys() {
    assert!(order_by("SELECT c FROM sbtest1 WHERE id = ?").is_empty());
}

#[test]
fn ordering_by_an_unprojected_column_fails_closed() {
    // The narrow read sorts already-projected rows, so a key absent from the
    // SELECT list cannot be honored without augmenting the projection.
    assert!(matches!(
        ordering_rejection("SELECT c FROM sbtest1 WHERE id = ? ORDER BY k"),
        PreparedPlanError::ReadOnly(ReadOnlyScanError::Unsupported(
            UnsupportedReadOnlyFeature::Ordering
        )),
    ));
}

#[test]
fn ordering_by_a_positional_ordinal_fails_closed() {
    assert!(matches!(
        ordering_rejection("SELECT c FROM sbtest1 WHERE id = ? ORDER BY 1"),
        PreparedPlanError::ReadOnly(ReadOnlyScanError::Unsupported(
            UnsupportedReadOnlyFeature::Ordering
        )),
    ));
}

#[test]
fn ordering_by_an_expression_fails_closed() {
    assert!(matches!(
        ordering_rejection("SELECT k FROM sbtest1 WHERE id = ? ORDER BY k + 1"),
        PreparedPlanError::ReadOnly(ReadOnlyScanError::Unsupported(
            UnsupportedReadOnlyFeature::Ordering
        )),
    ));
}

#[test]
fn distinct_ordered_read_carries_both_the_distinct_flag_and_the_order_key() {
    // sysbench read 5: `SELECT DISTINCT c ... ORDER BY c`.
    assert!(is_distinct(
        "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c"
    ));
    assert_eq!(
        order_by("SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c"),
        vec![(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 120 }
        )],
    );
}

#[test]
fn distinct_without_order_is_admitted_and_order_free() {
    // DISTINCT dedups by whole-tuple identity, independent of any sort, so an
    // unordered DISTINCT read is valid (its output order is unspecified).
    assert!(is_distinct("SELECT DISTINCT c FROM sbtest1 WHERE id = ?"));
    assert!(order_by("SELECT DISTINCT c FROM sbtest1 WHERE id = ?").is_empty());
}

#[test]
fn a_non_distinct_read_carries_no_distinct_flag() {
    assert!(!is_distinct("SELECT c FROM sbtest1 WHERE id = ? ORDER BY c"));
}

/// `(kind, source_offset, output_name, result_flen, result_decimals)`.
fn aggregate(sql: &str) -> Option<(PreparedAggregateKind, usize, String, u32, u8)> {
    lower_prepared_point_read(&select(sql), &catalog())
        .expect("prepared read must lower")
        .aggregate()
        .map(|aggregate| {
            (
                aggregate.kind(),
                aggregate.source_offset(),
                aggregate.output_name().to_owned(),
                aggregate.result_column_length(),
                aggregate.result_decimals(),
            )
        })
}

#[test]
fn sum_over_an_int_column_is_a_decimal_of_flen_plus_twenty_one() {
    // sysbench read 3: `SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?`.
    // k is INT (flen 11), so per Go typeInfer4Sum the result is DECIMAL(32, 0).
    assert_eq!(
        aggregate("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?"),
        Some((PreparedAggregateKind::Sum, 0, "SUM(k)".to_owned(), 32, 0)),
    );
}

#[test]
fn sum_over_the_bigint_handle_uses_its_wider_flen() {
    // id is BIGINT (flen 20), so the result is DECIMAL(41, 0).
    assert_eq!(
        aggregate("SELECT SUM(id) FROM sbtest1 WHERE id = ?"),
        Some((PreparedAggregateKind::Sum, 0, "SUM(id)".to_owned(), 41, 0)),
    );
}

#[test]
fn an_aliased_sum_takes_the_alias_as_its_output_name() {
    assert_eq!(
        aggregate("SELECT SUM(k) AS total FROM sbtest1 WHERE id = ?"),
        Some((PreparedAggregateKind::Sum, 0, "total".to_owned(), 32, 0)),
    );
}

#[test]
fn a_plain_read_carries_no_aggregate() {
    assert!(aggregate("SELECT k FROM sbtest1 WHERE id = ?").is_none());
}

#[test]
fn unsupported_aggregate_shapes_fail_closed() {
    // Every shape the single-SUM(integer) fold cannot honor is a hard reject,
    // never a silently wrong single row.
    for sql in [
        "SELECT SUM(DISTINCT k) FROM sbtest1 WHERE id = ?", // DISTINCT fold
        "SELECT SUM(c) FROM sbtest1 WHERE id = ?",          // string arg -> DOUBLE
        "SELECT COUNT(k) FROM sbtest1 WHERE id = ?",        // a different function
        "SELECT AVG(k) FROM sbtest1 WHERE id = ?",          // a different function
        "SELECT SUM(k), c FROM sbtest1 WHERE id = ?",       // aggregate + column
        "SELECT SUM(k + 1) FROM sbtest1 WHERE id = ?",      // non-column argument
        "SELECT SUM(k) FROM sbtest1 WHERE id = ? ORDER BY k", // aggregate + ORDER BY
        "SELECT DISTINCT SUM(k) FROM sbtest1 WHERE id = ?", // aggregate + DISTINCT
    ] {
        assert!(
            matches!(
                lower_prepared_point_read(&select(sql), &catalog()),
                Err(PreparedPlanError::ReadOnly(ReadOnlyScanError::Unsupported(
                    UnsupportedReadOnlyFeature::Aggregate
                )))
            ),
            "expected {sql} to fail closed as an unsupported aggregate",
        );
    }
}
