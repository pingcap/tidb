//! LIKE escape semantics: the default escape is backslash (`\%` matches a
//! literal %, `%` matches any run), a custom ESCAPE character works
//! (`ESCAPE '='`), and `\_` matches a literal underscore while `_` alone
//! matches any single character.

use tidb_session::Session;

use crate::support::first_debug as first;

#[test]
fn like_escape_forms() {
    let mut session = Session::new();

    // Default escape: \% is literal, % is a wildcard.
    assert_eq!(first(&mut session, r"select 'a%b' like 'a\%b'"), "Int(1)");
    assert_eq!(first(&mut session, r"select 'axb' like 'a%b'"), "Int(1)");

    // Custom ESCAPE character.
    assert_eq!(first(&mut session, r"select 'a%b' like 'a=%b' escape '='"), "Int(1)");

    // Underscore: wildcard vs escaped literal.
    assert_eq!(first(&mut session, r"select 'axb' like 'a_b'"), "Int(1)");
    assert_eq!(first(&mut session, r"select 'a_b' like 'a\_b'"), "Int(1)");
}
