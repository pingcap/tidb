//! Go `hasDefaultValue`'s BLOB/TEXT/JSON and VECTOR literal-default refusals
//! (`pkg/ddl/add_column.go:1215-1237`).

use tidb_executor::{run_create_table_on, Catalog};

fn create_error(sql: &str) -> String {
    let mut catalog = Catalog::default();
    match run_create_table_on(sql, &mut catalog) {
        Ok(_) => "ACCEPTED (should have been rejected)".to_string(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn blob_text_json_literal_defaults_are_rejected_with_1102() {
    for sql in [
        "create table t (a blob default 'x')",
        "create table t (a text default 'x')",
        "create table t (a json default '{}')",
    ] {
        assert_eq!(
            create_error(sql),
            "BLOB/TEXT/JSON column 'a' can't have a default value",
            "{sql}"
        );
    }
}

#[test]
fn vector_literal_default_is_rejected_with_the_plain_error() {
    assert_eq!(
        create_error("create table t (a vector(3) default '[1,2,3]')"),
        "VECTOR column 'a' can't have a literal default. Use expression default instead: ((VEC_FROM_TEXT('...')))"
    );
}
