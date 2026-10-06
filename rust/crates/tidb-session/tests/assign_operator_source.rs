//! The `:=` assignment operator inside SELECT evaluates to the assigned
//! value and updates the user variable for later statements — including
//! chained arithmetic reading a previous assignment.

use tidb_session::Session;

use crate::support::try_tagged_integer_rows_60 as try_sql;

#[test]
fn assignment_operator_updates_and_yields() {
    let mut session = Session::new();

    // The assignment itself evaluates to the value.
    assert_eq!(try_sql(&mut session, "select @a := 5"), "i:5");
    // The variable persists across statements.
    assert_eq!(try_sql(&mut session, "select @a"), "i:5");
    // A later assignment can read the earlier one.
    assert_eq!(try_sql(&mut session, "select @b := @a + 1"), "i:6");
    assert_eq!(try_sql(&mut session, "select @b"), "i:6");
}
