//! COALESCE returns its first non-NULL argument (all-NULL -> NULL) and
//! NULLIF(a, b) yields NULL when equal, a otherwise; the two compose
//! (`coalesce(nullif(null, 1), 5)` = 5).

use tidb_session::Session;

use crate::support::debug_rows_with_sql as rows;

#[test]
fn null_handling_functions() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select coalesce(null, null, 3), coalesce(1, 2)"),
        "Int(3)|Int(1)"
    );
    assert_eq!(rows(&mut session, "select coalesce(null, null)"), "Null");
    assert_eq!(rows(&mut session, "select nullif(1, 1), nullif(1, 2)"), "Null|Int(1)");
    assert_eq!(rows(&mut session, "select coalesce(nullif(null, 1), 5)"), "Int(5)");
}
