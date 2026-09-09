//! UUID() renders the 36-character 8-4-4-4-12 hyphenated form, values are
//! unique across calls, the version nibble is 1 (time-based), and the
//! final node block is stable within a session.

use tidb_session::Session;

fn uuid(session: &mut Session) -> String {
    match session.run("select uuid()").unwrap() {
        tidb_session::StmtResult::Rows(rows) => match &rows[0][0] {
            tidb_datatype::Datum::String(v) => String::from_utf8_lossy(v.bytes()).into_owned(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn uuid_shape_uniqueness_and_node() {
    let mut session = Session::new();

    let u1 = uuid(&mut session);
    let u2 = uuid(&mut session);

    // 8-4-4-4-12 with hyphens.
    assert_eq!(u1.len(), 36);
    let groups: Vec<usize> = u1.split('-').map(|g| g.len()).collect();
    assert_eq!(groups, vec![8, 4, 4, 4, 12], "{u1}");

    // Uniqueness within the session.
    assert_ne!(u1, u2);

    // Time-based variant: the version nibble is 1.
    assert!(u1.split('-').nth(2).unwrap().starts_with('1'), "{u1}");

    // The node block is stable across calls in one session.
    assert_eq!(
        u1.rsplit('-').next().unwrap(),
        u2.rsplit('-').next().unwrap()
    );
}
