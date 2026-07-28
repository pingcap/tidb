// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Source-derived admission tests for the first direct table-read lowering.

use tidb_planner::{
    access_path::ResolvedTableScanKind,
    physical_selection::{ComparisonOp, ComparisonOperand},
    read_only_scan::{
        BoundBigIntComparison, ConfiguredColumn, ConfiguredColumnKind, ConfiguredScalarType,
        ConfiguredTable, ReadOnlyScanError, ReadOnlyScanPlan, UnsupportedReadOnlyFeature,
    },
};

fn table() -> ConfiguredTable {
    ConfiguredTable::new(
        "test",
        "accounts",
        42,
        [
            ConfiguredColumn::clustered_primary_key("id", 7),
            ConfiguredColumn::stored_not_null("balance", 9),
        ],
    )
}

fn unsupported(sql: &str, feature: UnsupportedReadOnlyFeature) {
    assert_eq!(
        ReadOnlyScanPlan::lower(sql, &table()),
        Err(ReadOnlyScanError::Unsupported(feature))
    );
}

#[test]
fn direct_projection_preserves_alias_source_identity_and_scan_order() {
    let plan = ReadOnlyScanPlan::lower(
        "SELECT accounts.balance AS amount, id FROM test.accounts",
        &table(),
    )
    .expect("configured direct columns must lower in projection order");

    assert_eq!(plan.table_id(), 42);
    assert_eq!(
        plan.table_scan().scan_kind(),
        Some(ResolvedTableScanKind::Full)
    );
    assert_eq!(
        plan.table_scan().explain_id().as_deref(),
        Some("TableFullScan_1")
    );
    let [balance, id] = plan.projected_columns() else {
        panic!("the two direct projections must remain distinct");
    };
    assert_eq!(balance.output_name(), "amount");
    assert_eq!(balance.source_name(), "balance");
    assert_eq!(balance.kind(), ConfiguredColumnKind::StoredNotNull);
    assert_eq!(balance.scan_column().column_id, 9);
    assert_eq!(balance.scan_column().flag, 1);
    assert!(!balance.scan_column().pk_handle);

    assert_eq!(id.output_name(), "id");
    assert_eq!(id.source_name(), "id");
    assert_eq!(id.kind(), ConfiguredColumnKind::ClusteredPrimaryKey);
    assert_eq!(id.scan_column().column_id, 7);
    assert_eq!(id.scan_column().flag, 3);
    assert!(id.scan_column().pk_handle);

    assert_eq!(
        plan.table_scan().pushdown().columns,
        [balance.scan_column().clone(), id.scan_column().clone()]
    );
    for column in plan.projected_columns() {
        assert_eq!(column.scan_column().tp, 8);
        assert_eq!(column.scan_column().collation, 63);
        assert_eq!(column.scan_column().column_len, 20);
        assert_eq!(column.scan_column().decimal, 0);
        assert!(!column.scan_column().array);
    }
}

#[test]
fn a_char_column_reports_string_scan_metadata() {
    // A CHAR(120) column projects with the string type code (TypeString = 254),
    // its declared length, and the negated utf8mb4_bin collation id that TiDB's
    // new-collation coprocessor convention uses (RewriteNewCollationIDIfNeeded).
    // The sign follows the Go source; it is not yet exercised against real TiKV
    // because the string read path that would send it is not wired.
    let table = ConfiguredTable::new(
        "test",
        "accounts",
        42,
        [
            ConfiguredColumn::clustered_primary_key("id", 7),
            ConfiguredColumn::stored_char_not_null("c", 9, 120),
        ],
    );
    let plan = ReadOnlyScanPlan::lower("SELECT c FROM test.accounts", &table)
        .expect("a CHAR projection lowers");
    let [c] = plan.projected_columns() else {
        panic!("one projected column");
    };
    assert_eq!(c.source_name(), "c");
    assert_eq!(c.scan_column().column_id, 9);
    assert_eq!(c.scan_column().tp, 254);
    assert_eq!(c.scan_column().collation, -46);
    assert_eq!(c.scan_column().column_len, 120);
    assert_eq!(c.scan_column().flag, 1);
    assert!(!c.scan_column().pk_handle);
}

#[test]
fn table_alias_is_the_only_visible_two_part_qualifier() {
    let plan = ReadOnlyScanPlan::lower("SELECT a.balance, a.id FROM accounts AS a", &table())
        .expect("configured table aliases remain direct one-table reads");
    assert_eq!(plan.projected_columns()[0].source_name(), "balance");
    assert_eq!(
        ReadOnlyScanPlan::lower("SELECT accounts.id FROM accounts AS a", &table()),
        Err(ReadOnlyScanError::UnknownColumn("accounts.id".to_owned()))
    );
}

#[test]
fn bound_relation_reuses_sql_lowering_for_all_columns_ranges_and_residuals() {
    let table = table();
    let structured = ReadOnlyScanPlan::lower_bound_relation(
        &table,
        &[0, 1],
        &[
            BoundBigIntComparison::LiteralLeft {
                value: -5,
                op: ComparisonOp::Le,
                column_index: 0,
            },
            BoundBigIntComparison::ColumnLeft {
                column_index: 1,
                op: ComparisonOp::Gt,
                value: 10,
            },
        ],
    )
    .expect("bound local predicates must enter the canonical scan lowering");
    let sql = ReadOnlyScanPlan::lower(
        "SELECT id, balance FROM accounts WHERE -5 <= id AND balance > 10",
        &table,
    )
    .unwrap();

    assert_eq!(structured, sql);
    assert_eq!(structured.projection_output_offsets(), [0, 1]);
    assert_eq!(
        structured
            .projected_columns()
            .iter()
            .map(|column| column.source_name())
            .collect::<Vec<_>>(),
        ["id", "balance"]
    );
    assert_eq!(structured.handle_ranges().len(), 1);
    assert_eq!(structured.handle_ranges()[0].start(), -5);
    assert_eq!(structured.handle_ranges()[0].end(), i64::MAX);
    let conditions = structured.selection().unwrap().conditions();
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].op(), ComparisonOp::Gt);
    assert_eq!(conditions[0].lhs(), ComparisonOperand::InputOffset(1));
    assert_eq!(conditions[0].rhs(), ComparisonOperand::Int(10));
}

#[test]
fn bound_relation_rejects_projection_and_predicate_offsets() {
    let table = table();
    let invalid = ReadOnlyScanError::InvalidColumnIndex {
        index: 2,
        column_count: 2,
    };
    assert_eq!(
        ReadOnlyScanPlan::lower_bound_relation(&table, &[0, 2], &[]),
        Err(invalid.clone())
    );
    assert_eq!(
        ReadOnlyScanPlan::lower_bound_relation(
            &table,
            &[0],
            &[BoundBigIntComparison::ColumnLeft {
                column_index: 2,
                op: ComparisonOp::Eq,
                value: 1,
            }],
        ),
        Err(invalid)
    );
}

#[test]
fn unsupported_plan_shapes_fail_before_physical_lowering() {
    // The Campaign 19 milestone acquires these Go plan-builder cases as
    // explicit fail-closed admissions, not partial execution semantics.
    unsupported(
        "UPDATE accounts SET id = 2",
        UnsupportedReadOnlyFeature::WriteOrNonQueryStatement,
    );
    let filtered = ReadOnlyScanPlan::lower("SELECT id FROM accounts WHERE id = 1", &table())
        .expect("bounded signed-BIGINT handle predicates now lower to ranges");
    assert!(filtered.selection().is_none());
    assert_eq!(filtered.handle_ranges().len(), 1);
    assert_eq!(filtered.handle_ranges()[0].start(), 1);
    assert_eq!(filtered.handle_ranges()[0].end(), 1);
    assert_eq!(filtered.projection_output_offsets(), [0]);
    unsupported(
        "SELECT accounts.id FROM accounts JOIN other ON accounts.id = other.id",
        UnsupportedReadOnlyFeature::Join,
    );
    unsupported(
        "SELECT COUNT(id) FROM accounts",
        UnsupportedReadOnlyFeature::Aggregate,
    );
    // A supported SUM shape still fails closed on the literal COM_QUERY path,
    // which folds no aggregate — only the prepared read executor does.
    unsupported(
        "SELECT SUM(id) FROM accounts",
        UnsupportedReadOnlyFeature::Aggregate,
    );
    unsupported(
        "SELECT (SELECT id FROM accounts) FROM accounts",
        UnsupportedReadOnlyFeature::Subquery,
    );
    unsupported(
        "SELECT id FROM accounts PARTITION(p0)",
        UnsupportedReadOnlyFeature::Partition,
    );
    unsupported(
        "SELECT id FROM accounts ORDER BY id",
        UnsupportedReadOnlyFeature::Ordering,
    );
    // DISTINCT is a SQL-layer dedup the literal COM_QUERY path does not run, so
    // it fails closed here just like ORDER BY (both are applied only by the
    // prepared read executor).
    unsupported(
        "SELECT DISTINCT id FROM accounts",
        UnsupportedReadOnlyFeature::SelectModifier,
    );
    unsupported(
        "SELECT id FROM accounts LIMIT 1",
        UnsupportedReadOnlyFeature::Limit,
    );
    unsupported(
        "SELECT * FROM accounts",
        UnsupportedReadOnlyFeature::Wildcard,
    );
    unsupported(
        "SELECT id + 1 FROM accounts",
        UnsupportedReadOnlyFeature::ProjectionExpression,
    );
}

#[test]
fn unknown_catalog_names_and_invalid_configuration_fail_explicitly() {
    assert_eq!(
        ReadOnlyScanPlan::lower("SELECT id FROM missing", &table()),
        Err(ReadOnlyScanError::UnknownTable("missing".to_owned()))
    );
    assert_eq!(
        ReadOnlyScanPlan::lower("SELECT missing FROM accounts", &table()),
        Err(ReadOnlyScanError::UnknownColumn("missing".to_owned()))
    );
    assert_eq!(
        ReadOnlyScanPlan::lower(
            "SELECT id FROM accounts",
            &ConfiguredTable::new(
                "test",
                "accounts",
                0,
                [ConfiguredColumn::clustered_primary_key("id", 7)],
            ),
        ),
        Err(ReadOnlyScanError::InvalidConfiguration(
            "table ID must be positive"
        ))
    );
}

#[test]
fn invalid_column_catalogs_fail_before_sql_lowering() {
    let invalid = [
        (
            ConfiguredTable::new(
                "test",
                "accounts",
                42,
                [ConfiguredColumn::clustered_primary_key("", 7)],
            ),
            "column names must be nonempty",
        ),
        (
            ConfiguredTable::new(
                "test",
                "accounts",
                42,
                [ConfiguredColumn::clustered_primary_key("id", 0)],
            ),
            "column IDs must be positive",
        ),
        (
            ConfiguredTable::new(
                "test",
                "accounts",
                42,
                [
                    ConfiguredColumn::clustered_primary_key("id", 7),
                    ConfiguredColumn::stored_not_null("ID", 9),
                ],
            ),
            "column names must be unique",
        ),
        (
            ConfiguredTable::new(
                "test",
                "accounts",
                42,
                [
                    ConfiguredColumn::clustered_primary_key("id", 7),
                    ConfiguredColumn::stored_not_null("balance", 7),
                ],
            ),
            "column IDs must be unique",
        ),
        (
            ConfiguredTable::new(
                "test",
                "accounts",
                42,
                [ConfiguredColumn::stored_not_null("balance", 9)],
            ),
            "exactly one clustered primary key is required",
        ),
        (
            ConfiguredTable::new(
                "test",
                "accounts",
                42,
                [
                    ConfiguredColumn::clustered_primary_key("id", 7),
                    ConfiguredColumn::clustered_primary_key("other_id", 9),
                ],
            ),
            "exactly one clustered primary key is required",
        ),
    ];

    for (table, reason) in invalid {
        assert_eq!(
            ReadOnlyScanPlan::lower("SELECT id FROM accounts", &table),
            Err(ReadOnlyScanError::InvalidConfiguration(reason))
        );
    }
}

#[test]
fn result_column_metadata_follows_go_convert_column_info() {
    // Client-facing result metadata, per Go column.ConvertColumnInfo /
    // mysql.CharsetNameToID: the charset id is POSITIVE (distinct from the
    // negated coprocessor scan collation), and a string length is scaled by the
    // charset max byte width (utf8mb4 = 4).
    let char120 = ConfiguredScalarType::Char { max_length: 120 };
    assert_eq!(char120.result_type_code(), 254); // TypeString
    assert_eq!(char120.result_charset_id(), 46); // utf8mb4 default collation, positive
    assert_eq!(char120.result_column_length(), 480); // 120 * 4

    // Integer columns carry the binary charset (63). BIGINT is LONGLONG, INT is
    // LONG — each type-faithful, with a matching binary result cell.
    assert_eq!(ConfiguredScalarType::BigInt.result_type_code(), 8); // TypeLonglong
    assert_eq!(ConfiguredScalarType::BigInt.result_column_length(), 20);
    assert_eq!(ConfiguredScalarType::Int.result_type_code(), 3); // TypeLong
    assert_eq!(ConfiguredScalarType::Int.result_column_length(), 11);
    for integer in [ConfiguredScalarType::BigInt, ConfiguredScalarType::Int] {
        assert_eq!(integer.result_charset_id(), 63); // binary
    }

    // `BIGINT UNSIGNED` and `DOUBLE` widen the read path beyond signed
    // integers; both round-trip through the same binary/client metadata shape
    // as the existing integer types, with UNSIGNED carrying the client
    // unsigned flag bit.
    assert_eq!(
        ConfiguredScalarType::UnsignedBigInt.result_type_code(),
        8 // TypeLonglong
    );
    assert_eq!(ConfiguredScalarType::UnsignedBigInt.result_charset_id(), 63);
    assert!(ConfiguredScalarType::UnsignedBigInt.is_unsigned());
    assert!(!ConfiguredScalarType::BigInt.is_unsigned());
    assert_eq!(ConfiguredScalarType::Double.result_type_code(), 5); // TypeDouble
    assert_eq!(ConfiguredScalarType::Double.result_charset_id(), 63);
    assert!(!ConfiguredScalarType::Double.is_unsigned());
}

#[test]
fn chunk_field_type_drives_real_tikv_coprocessor_chunk_decode_per_column() {
    // `RealTiKvReadSession::execute_plan` derives its coprocessor response
    // `FieldType`s from this method instead of assuming every projected
    // column is a signed `BIGINT`; a wrong `FieldTypeCode` here corrupts or
    // fails the decode of any non-`BIGINT` configured column.
    use tidb_datatype::{Collation, FieldTypeCode};

    let bigint = ConfiguredScalarType::BigInt.chunk_field_type();
    assert_eq!(bigint.code(), FieldTypeCode::LongLong);
    assert!(!bigint.is_unsigned());

    let unsigned = ConfiguredScalarType::UnsignedBigInt.chunk_field_type();
    assert_eq!(unsigned.code(), FieldTypeCode::LongLong);
    assert!(unsigned.is_unsigned());

    let int = ConfiguredScalarType::Int.chunk_field_type();
    assert_eq!(int.code(), FieldTypeCode::Long);

    let double = ConfiguredScalarType::Double.chunk_field_type();
    assert_eq!(double.code(), FieldTypeCode::Double);

    let char_field = ConfiguredScalarType::Char { max_length: 30 }.chunk_field_type();
    assert_eq!(char_field.code(), FieldTypeCode::String);
    assert_eq!(char_field.collation(), Collation::Utf8Mb4Bin);
    assert_eq!(char_field.flen(), 30);
}
