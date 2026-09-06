//! Go `checkAutoIncrement` (`pkg/planner/core/preprocess.go:803-837`) and the
//! column-option rules around it. Note Go collects the isKey flag but never
//! consults it: a lone `auto_increment` WITHOUT a key is accepted, exactly
//! like MySQL 8.

use tidb_executor::{run_create_table_on, Catalog};

fn create_error(sql: &str) -> String {
    let mut catalog = Catalog::default();
    match run_create_table_on(sql, &mut catalog) {
        Ok(_) => "ACCEPTED (should have been rejected)".to_string(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn two_auto_increment_columns_are_rejected_with_1075() {
    assert_eq!(
        create_error("create table t (a int auto_increment, b int auto_increment)"),
        "Incorrect table definition; there can be only one auto column and it must be defined as a key"
    );
}

#[test]
fn auto_increment_on_decimal_is_rejected_with_1063() {
    assert_eq!(
        create_error("create table t (a decimal(5,2) auto_increment)"),
        "Incorrect column specifier for column 'a'"
    );
}

#[test]
fn auto_increment_on_datetime_is_rejected_with_1063() {
    assert_eq!(
        create_error("create table t (a datetime auto_increment)"),
        "Incorrect column specifier for column 'a'"
    );
}

#[test]
fn lone_auto_increment_without_key_is_accepted() {
    // Go collects the isKey flag but never enforces it (MySQL 8 semantics).
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int auto_increment)", &mut catalog)
        .expect("a lone auto_increment without a key creates fine in Go");
}

#[test]
fn duplicate_column_and_double_primary_key_are_rejected() {
    assert_eq!(
        create_error("create table t (a int, a int)"),
        "Duplicate column name 'a'"
    );
    assert_eq!(
        create_error("create table t (a int primary key, b int primary key)"),
        "Multiple primary key defined"
    );
}
