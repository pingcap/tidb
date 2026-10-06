//! STRCMP's three-way result (-1/0/1) and the NULL-safe <=> operator:
//! NULL <=> NULL is TRUE and anything <=> NULL is FALSE — where the plain =
//! operator yields NULL for both.

use tidb_session::Session;

use crate::support::try_tagged_integer_rows_60 as try_sql;

#[test]
fn three_way_and_null_safe() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select strcmp('a', 'b'), strcmp('b', 'a'), strcmp('a', 'a')"),
        "i:-1|i:1|i:0"
    );

    // <=> treats NULL as an ordinary comparable value.
    assert_eq!(try_sql(&mut session, "select 1 <=> 1, 1 <=> null, null <=> null"), "i:1|i:0|i:1");

    // Plain = yields NULL with a NULL operand.
    assert_eq!(try_sql(&mut session, "select 1 = null, null = null"), "Null|Null");
}
