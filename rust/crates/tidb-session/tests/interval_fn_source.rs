//! The INTERVAL(N, N1, N2, ...) comparison function: returns the index of
//! the first pivot strictly greater than N (binary-search semantics), -1
//! when N is NULL.

use tidb_session::Session;

use crate::support::try_tagged_integer_rows_60 as try_sql;

#[test]
fn interval_index_semantics() {
    let mut session = Session::new();

    // 5 exceeds every pivot: the index past the last one.
    assert_eq!(try_sql(&mut session, "select interval(5, 1, 2, 3)"), "i:3");
    // 2 sits between pivots 1 and 3.
    assert_eq!(try_sql(&mut session, "select interval(2, 1, 3)"), "i:1");
    // STRICTLY greater: 2 < 2 is false, so the pivot 2 does not stop the scan.
    assert_eq!(try_sql(&mut session, "select interval(2, 1, 2, 3)"), "i:2");
    // NULL N answers -1.
    assert_eq!(try_sql(&mut session, "select interval(null, 1, 2)"), "i:-1");
}
