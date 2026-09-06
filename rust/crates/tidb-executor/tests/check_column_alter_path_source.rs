//! Go runs `checkColumn` over ALTER TABLE's `spec.NewColumns` too
//! (`pkg/planner/core/preprocess.go:1444`), so the width/size checks proven
//! for CREATE TABLE in `check_column_display_width_source.rs` must hold on the
//! ALTER path as well — MODIFY and ADD both flow through the same
//! `check_column_attributes` gate.

use tidb_executor::{run_alter_table_in, run_create_table_on, Catalog, StmtContext};

fn alter_error(sql: &str) -> String {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::default();
    run_create_table_on("create table t (a int)", &mut catalog).expect("base table");
    match run_alter_table_in(sql, &mut catalog, "test", &ctx) {
        Ok(_) => "ACCEPTED (should have been rejected)".to_string(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn alter_modify_to_bit65_is_rejected_with_1439() {
    assert_eq!(
        alter_error("alter table t modify column c bit(65)"),
        "Display width out of range for column 'c' (max = 64)"
    );
}

#[test]
fn alter_add_char300_is_rejected_with_1074() {
    assert_eq!(
        alter_error("alter table t add column d char(300)"),
        "Column length too big for column 'd' (max = 255); use BLOB or TEXT instead"
    );
}
