//! LOWER/UPPER: ASCII round-trips, accented Latin-1 letters fold (É -> é,
//! é -> É), the Greek final-sigma hazard folds with the simple mapping
//! (Σ -> σ, not ς), and NULL propagates.

use tidb_session::Session;

use crate::support::try_tagged_string_rows_60 as try_sql;

#[test]
fn case_mapping_rules() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select lower('AbC'), upper('AbC')"),
        "s:abc|s:ABC"
    );
    assert_eq!(try_sql(&mut session, "select lower('ÉÀ'), upper('éà')"), "s:éà|s:ÉÀ");
    assert_eq!(try_sql(&mut session, "select lower(null)"), "Null");
    // NOT the word-final sigma a full Unicode fold would produce.
    assert_eq!(try_sql(&mut session, "select lower('Σ')"), "s:σ");
}
