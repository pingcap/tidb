//! Generated-column and charset-name rules from Go's preprocess
//! (`preprocess.go:1203` virtual-generated PK; parser charset validation).

use tidb_executor::{run_create_table_on, Catalog};

fn create_error(sql: &str) -> String {
    let mut catalog = Catalog::default();
    match run_create_table_on(sql, &mut catalog) {
        Ok(_) => "ACCEPTED (should have been rejected)".to_string(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn virtual_generated_primary_key_is_rejected() {
    assert!(create_error(
        "create table t (b int, a int generated always as (b+1) virtual, primary key(a))"
    )
    .contains("'Defining a virtual generated column as primary key'"));
}

#[test]
fn stored_generated_primary_key_is_accepted() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (b int, a int generated always as (b+1) stored, primary key(a))",
        &mut catalog,
    )
    .expect("stored generated PK creates fine");
}

#[test]
fn index_on_generated_column_is_accepted() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int generated always as (a+1), key(a))",
        &mut catalog,
    )
    .expect("index on generated column creates fine");
}

#[test]
fn unknown_charset_on_column_is_rejected_with_1115() {
    assert!(create_error("create table t (a varchar(10) charset foo)")
        .contains("[parser:1115]Unknown character set: 'foo'"));
}

#[test]
fn unknown_table_charset_is_rejected() {
    assert!(create_error("create table t (a int) charset=foo").contains("unknown character set"));
}
