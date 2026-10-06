//! Set-membership functions and the JSON aggregate: FIELD returns the
//! 1-based index, ELT the nth element, FIND_IN_SET the position within a
//! comma list, and JSON_OBJECTAGG builds a document from grouped rows.

use tidb_session::Session;

use crate::support::try_tagged_rows_60 as try_sql;

#[test]
fn membership_positions() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select field('b', 'a', 'b', 'c')"),
        "i:2"
    );
    // A missing member answers 0.
    assert_eq!(try_sql(&mut session, "select field('z', 'a', 'b', 'c')"), "i:0");
    assert_eq!(try_sql(&mut session, "select elt(2, 'a', 'b')"), "s:b");
    assert_eq!(try_sql(&mut session, "select find_in_set('b', 'a,b,c')"), "i:2");
}
