//! Go `checkUnsupportedTableOptions` (`pkg/planner/core/preprocess.go:1513`)
//! and `checkTableEngine` (`:1549`, with `mysqlValidTableEngineNames`).

use tidb_executor::{run_create_table_on, Catalog};

fn create_error(sql: &str) -> String {
    let mut catalog = Catalog::default();
    match run_create_table_on(sql, &mut catalog) {
        Ok(_) => "ACCEPTED (should have been rejected)".to_string(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn known_engine_is_accepted() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int) engine = innodb", &mut catalog)
        .expect("innodb is in mysqlValidTableEngineNames");
}

#[test]
fn unknown_engine_is_rejected() {
    assert_eq!(
        create_error("create table t (a int) engine = fizzbuzz"),
        "Unknown storage engine 'fizzbuzz'"
    );
}

#[test]
fn union_option_is_rejected_with_8232() {
    assert_eq!(
        create_error("create table t (a int) union = (t2)"),
        "CREATE/ALTER table with union option is not supported"
    );
}

#[test]
fn insert_method_option_is_rejected_with_8233() {
    assert_eq!(
        create_error("create table t (a int) insert_method = first"),
        "CREATE/ALTER table with insert method option is not supported"
    );
}
