//! GROUP_CONCAT composes its own ORDER BY (values sorted inside each
//! group), DISTINCT dedupes before the ordering, and SEPARATOR replaces
//! the default comma — including a multi-character separator.

use tidb_session::Session;

use crate::support::tagged_rows_with_sql as rows;

#[test]
fn order_distinct_and_separator() {
    let mut session = Session::new();
    session.run("create table t (g int, v int)").unwrap();
    session
        .run("insert into t values (1, 3), (1, 1), (1, 3), (2, 2)")
        .unwrap();

    // Inner ORDER BY sorts each group's values.
    assert_eq!(
        rows(&mut session, "select g, group_concat(v order by v) from t group by g order by g"),
        "i:1|s:1,3,3;i:2|s:2"
    );

    // DISTINCT dedupes first: the duplicate 3 collapses.
    assert_eq!(
        rows(&mut session, "select g, group_concat(distinct v order by v) from t group by g order by g"),
        "i:1|s:1,3;i:2|s:2"
    );

    // A custom separator, emitted verbatim.
    assert_eq!(
        rows(&mut session, "select group_concat(v order by v separator '; ') from t where g = 1"),
        "s:1; 3; 3"
    );
}
