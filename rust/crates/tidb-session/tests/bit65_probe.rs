use tidb_session::Session;

#[test]
fn probe_panic_debug() {
    let mut session = Session::new();
    session.run("create table g (v int)").unwrap();
    session.run("insert into g values (1), (2), (3)").unwrap();
    let _ = session.run("select v % 3 from g group by v % 3");
}
