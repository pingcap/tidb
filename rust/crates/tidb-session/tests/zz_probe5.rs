use tidb_session::Session;
#[test]
fn probe5() {
    let mut session = Session::new();
    session.run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4").unwrap();
    session.run("INSERT INTO h VALUES (1,1),(2,2),(4,4)").unwrap();
    session.run("SELECT a FROM h PARTITION (p0) ORDER BY a").unwrap();
    session.run("SELECT a FROM h PARTITION (p0, p1) ORDER BY a").unwrap();
    session.run("SELECT count(*) FROM h").unwrap();
    let err = session.run("SELECT a FROM h PARTITION (nosuch)").unwrap_err();
    println!("ERR-AFTER-SEQUENCE: {err:?}");
}
