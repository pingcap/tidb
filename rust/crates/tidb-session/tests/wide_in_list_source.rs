//! A 1000-element IN list: every odd id matches (500 of 1000 rows), so the
//! wide operand list evaluates correctly end to end.

use tidb_session::Session;

use crate::support::tagged_integer_rows as rows;

#[test]
fn wide_in_list_matches_half() {
    let mut session = Session::new();
    session.run("create table t (id int primary key)").unwrap();
    let values: Vec<String> = (1..=1000).map(|i| format!("({i})")).collect();
    session
        .run(&format!("insert into t values {}", values.join(", ")))
        .unwrap();

    let targets: Vec<String> = (1..=1000).step_by(2).map(|i| i.to_string()).collect();
    let sql = format!("select count(*) from t where id in ({})", targets.join(", "));
    assert_eq!(rows(&mut session, &sql), "i:500");
}
