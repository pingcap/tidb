//! In-statement duplicate keys and REPLACE with an AUTO_INCREMENT key: IGNORE
//! keeps the FIRST row and skips the later duplicate while later rows still
//! insert; a plain insert fails; REPLACE on an auto-inc key reports 2
//! affected rows (delete + insert).

use crate::support::testkit::TestKit;

#[test]
fn ignore_keeps_first_and_skips_in_statement_duplicate() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, v int)");

    tk.must_exec("insert ignore into t values (1, 10), (1, 11), (2, 20)");
    // The duplicate is skipped, the rest insert.
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select a, v from t order by a").check(["1 10", "2 20"]);
}

#[test]
fn plain_insert_fails_on_in_statement_duplicate() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, v int)");
    // The in-statement duplicate must fail.
    tk.must_contain_err_msg("insert into t values (1, 10), (1, 11)", "Duplicate entry");
}

#[test]
fn replace_on_auto_inc_key_counts_two() {
    let mut tk = TestKit::new();
    tk.must_exec("create table u (id int auto_increment primary key, v int)");
    tk.must_exec("insert into u (v) values (1)");
    tk.must_exec("replace into u values (1, 100)");
    // Delete + insert.
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select id, v from u").check(["1 100"]);
}
