//! DEFAULT/ON UPDATE column-option rules: Go `checkColumnOptions` /
//! `isInvalidDefaultValue` / the CURRENT_TIMESTAMP expression-default check.

use tidb_executor::{run_create_table_on, Catalog};

fn create_error(sql: &str) -> String {
    let mut catalog = Catalog::default();
    match run_create_table_on(sql, &mut catalog) {
        Ok(_) => "ACCEPTED (should have been rejected)".to_string(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn literal_default_on_int_is_rejected() {
    assert_eq!(create_error("create table t (a int default 'abc')"), "Invalid default value for 'a'");
}

#[test]
fn on_update_on_non_timestamp_is_rejected() {
    assert_eq!(
        create_error("create table t (a int on update current_timestamp)"),
        "Invalid ON UPDATE clause for 'a' column"
    );
}

#[test]
fn timestamp_defaults_on_datetime_are_accepted() {
    let mut catalog = Catalog::default();
    for sql in [
        "create table t (a datetime default current_timestamp)",
        "create table t (a datetime default current_timestamp on update current_timestamp)",
    ] {
        run_create_table_on(sql, &mut catalog).expect(sql);
    }
}

#[test]
fn current_timestamp_expression_default_on_int_is_rejected() {
    assert!(
        create_error("create table t (a int default (now()))")
            .contains("DEFAULT CURRENT_TIMESTAMP on a column that is not TIMESTAMP or DATETIME")
    );
}

#[test]
fn plain_expression_default_is_accepted() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int default (1+2))", &mut catalog)
        .expect("expression default (1+2) on int creates fine");
}
