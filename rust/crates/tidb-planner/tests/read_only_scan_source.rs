// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Source-derived admission tests for the first direct table-read lowering.

use tidb_planner::{
    access_path::ResolvedTableScanKind,
    read_only_scan::{
        ConfiguredColumn, ConfiguredColumnKind, ConfiguredTable, ReadOnlyScanError,
        ReadOnlyScanPlan, UnsupportedReadOnlyFeature,
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
