//! Collation effects on grouping and mixing: GROUP BY on a CI column folds
//! case into one group, and mixing EXPLICIT collations of different
//! charsets/collations in one comparison fails with Go's 1267 ("Illegal
//! mix of collations").

use tidb_session::Session;

use crate::support::quoted_string_integer_rows as rows;

fn setup(session: &mut Session) {
    session
        .run("create table t (s varchar(8)) charset utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    session.run("insert into t values ('Apple'), ('apple'), ('b')").unwrap();
}

#[test]
fn ci_grouping_and_collation_mix_refusal() {
    let mut session = Session::new();
    setup(&mut session);

    // GROUP BY on a CI column folds 'Apple' and 'apple' into one group.
    assert_eq!(
        rows(&mut session, "select s, count(*) from t group by s order by s"),
        "'Apple'|2;'b'|1"
    );

    // Mixing explicit collations of one charset but different rules: 1267.
    let error = session
        .run("select 'x' collate utf8mb4_bin = 'x' collate utf8mb4_general_ci")
        .expect_err("the collations conflict");
    assert!(
        error
            .to_string()
            .contains("Illegal mix of collations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation '='"),
        "{error}"
    );
}
