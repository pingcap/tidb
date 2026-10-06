//! DIV and % semantics: integer division truncates toward zero, modulo
//! takes the dividend's sign, either by zero yields NULL (with a warning),
//! and a decimal operand truncates the same way (`7.5 div 2` = 3).

use tidb_session::Session;

use crate::support::debug_rows_with_sql as rows;

#[test]
fn div_and_modulo_sign_and_zero_rules() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select 7 div 2, -7 div 2, 7 div -2"),
        "Int(3)|Int(-3)|Int(-3)"
    );
    assert_eq!(
        rows(&mut session, "select 7 % 3, -7 % 3, 7 % -3"),
        "Int(1)|Int(-1)|Int(1)"
    );
    assert_eq!(rows(&mut session, "select 7 div 0, 7 % 0"), "Null|Null");
    assert_eq!(rows(&mut session, "select 7.5 div 2"), "Int(3)");
}
