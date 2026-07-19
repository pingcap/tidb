// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Source-derived admission tests for the first direct table-read lowering.

use tidb_planner::{
    access_path::ResolvedTableScanKind,
    read_only_scan::{
        ConfiguredTable, ReadOnlyScanError, ReadOnlyScanPlan, UnsupportedReadOnlyFeature,
    },
};

fn table() -> ConfiguredTable {
    ConfiguredTable::new("test", "accounts", 42, "id", 7)
}

fn unsupported(sql: &str, feature: UnsupportedReadOnlyFeature) {
    assert_eq!(
        ReadOnlyScanPlan::lower(sql, &table()),
        Err(ReadOnlyScanError::Unsupported(feature))
    );
}

#[test]
fn direct_clustered_pk_projection_lowers_to_existing_table_reader_scan() {
    let plan = ReadOnlyScanPlan::lower(
        "SELECT accounts.id AS account_id, id FROM test.accounts",
        &table(),
    )
    .expect("one configured direct projection must lower");

    assert_eq!(plan.table_id(), 42);
    assert_eq!(plan.projected_column_names(), ["account_id", "id"]);
    assert_eq!(
        plan.table_scan().scan_kind(),
        Some(ResolvedTableScanKind::Full)
    );
    assert_eq!(
        plan.table_scan().explain_id().as_deref(),
        Some("TableFullScan_1")
    );
    assert_eq!(plan.projected_columns().len(), 2);
    for column in plan.projected_columns() {
        assert_eq!(column.column_id, 7);
        assert_eq!(column.tp, 8);
        assert_eq!(column.collation, 63);
        assert_eq!(column.column_len, 20);
        assert_eq!(column.flag, 2);
        assert!(column.pk_handle);
        assert!(!column.array);
    }
}

#[test]
fn table_alias_is_the_only_visible_two_part_qualifier() {
    ReadOnlyScanPlan::lower("SELECT a.id FROM accounts AS a", &table())
        .expect("configured table aliases remain direct one-table reads");
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
    unsupported(
        "SELECT id FROM accounts WHERE id = 1",
        UnsupportedReadOnlyFeature::Predicate,
    );
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
        ReadOnlyScanPlan::lower("SELECT balance FROM accounts", &table()),
        Err(ReadOnlyScanError::UnknownColumn("balance".to_owned()))
    );
    assert_eq!(
        ReadOnlyScanPlan::lower(
            "SELECT id FROM accounts",
            &ConfiguredTable::new("test", "accounts", 0, "id", 7),
        ),
        Err(ReadOnlyScanError::InvalidConfiguration(
            "table ID must be positive"
        ))
    );
}
