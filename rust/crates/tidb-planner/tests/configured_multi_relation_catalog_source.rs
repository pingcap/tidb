// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Source-derived checks for the bounded immutable configured catalog.

use tidb_planner::read_only_scan::{
    configured_catalog::{ConfiguredCatalog, ConfiguredCatalogError, ConfiguredTableLookupError},
    ConfiguredColumn, ConfiguredTable, ReadOnlyScanError, ReadOnlyScanPlan,
};

fn table(schema: &str, name: &str, table_id: i64, column_id: i64) -> ConfiguredTable {
    ConfiguredTable::new(
        schema,
        name,
        table_id,
        [ConfiguredColumn::clustered_primary_key("ID", column_id)],
    )
}

#[test]
fn source_order_and_stable_id_and_name_lookups_are_preserved() {
    let catalog = ConfiguredCatalog::new([
        table("Sales", "Accounts", 42, 1),
        table("Sales", "Profiles", 84, 2),
    ])
    .expect("distinct configured identities must build");

    assert_eq!(catalog.tables()[0].table_id(), 42);
    assert_eq!(catalog.tables()[1].table_id(), 84);
    assert_eq!(catalog.table_by_id(84), Some(&catalog.tables()[1]));
    assert_eq!(
        catalog.table_by_name("sales", "ACCOUNTS"),
        Some(&catalog.tables()[0])
    );
    assert_eq!(
        catalog.resolve_table(None, "profiles"),
        Ok(&catalog.tables()[1])
    );
}

#[test]
fn configured_identifiers_share_unicode_case_folding_with_table_validation() {
    let catalog = ConfiguredCatalog::new([ConfiguredTable::new(
        "ÜSER",
        "RÉSUMÉ",
        42,
        [ConfiguredColumn::clustered_primary_key("KÜNDENID", 7)],
    )])
    .expect("Unicode configured identifiers must remain resolvable");

    assert!(catalog.table_by_name("üser", "résumé").is_some());
    assert!(
        ReadOnlyScanPlan::lower("SELECT kündenid FROM üser.résumé", &catalog.tables()[0]).is_ok()
    );
    assert_eq!(
        ConfiguredTable::new(
            "test",
            "t",
            43,
            [
                ConfiguredColumn::clustered_primary_key("KÜNDENID", 1),
                ConfiguredColumn::stored_not_null("kündenid", 2),
            ],
        )
        .validate(),
        Err(ReadOnlyScanError::InvalidConfiguration(
            "column names must be unique"
        ))
    );
}

#[test]
fn duplicate_folded_names_and_physical_ids_fail_at_construction() {
    assert_eq!(
        ConfiguredCatalog::new([
            table("Sales", "Accounts", 42, 1),
            table("sales", "ACCOUNTS", 84, 2),
        ]),
        Err(ConfiguredCatalogError::DuplicateTableName {
            schema: "sales".to_owned(),
            table: "ACCOUNTS".to_owned(),
        })
    );
    assert_eq!(
        ConfiguredCatalog::new([
            table("sales", "accounts", 42, 1),
            table("archive", "accounts", 42, 2),
        ]),
        Err(ConfiguredCatalogError::DuplicateTableId(42))
    );
}

#[test]
fn unqualified_names_are_ambiguous_across_schemas_but_qualified_names_are_not() {
    let catalog = ConfiguredCatalog::new([
        table("sales", "accounts", 42, 1),
        table("archive", "accounts", 84, 2),
    ])
    .expect("the same table name in distinct schemas is legal");

    assert_eq!(
        catalog.resolve_table(None, "ACCOUNTS"),
        Err(ConfiguredTableLookupError::AmbiguousTable(
            "ACCOUNTS".to_owned()
        ))
    );
    assert_eq!(
        catalog.resolve_table(Some("archive"), "accounts"),
        Ok(&catalog.tables()[1])
    );
    assert_eq!(
        catalog.resolve_table(Some("missing"), "accounts"),
        Err(ConfiguredTableLookupError::UnknownTable(
            "missing.accounts".to_owned()
        ))
    );
}

#[test]
fn invalid_single_table_shape_uses_the_shared_validator() {
    assert_eq!(
        ConfiguredCatalog::new([ConfiguredTable::new(
            "test",
            "accounts",
            0,
            [ConfiguredColumn::clustered_primary_key("id", 1)],
        )]),
        Err(ConfiguredCatalogError::InvalidTable {
            index: 0,
            error: ReadOnlyScanError::InvalidConfiguration("table ID must be positive"),
        })
    );
}
