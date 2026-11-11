//! Go `builtin_fts.go`, `builtin_like.go`, `builtin_ilike.go` and
//! `builtin_grouping.go`'s function classes through SQL. Results, errors,
//! warnings and plans are captured from Go with `zzprobe/sqlprobe`.

use crate::tests_support::*;
use crate::*;

const MATCH_OUTSIDE_INDEX: (u16, &str) =
    (1105, "cannot use 'MATCH ... AGAINST' outside of fulltext index");
const NON_STRING_COLUMN: (u16, &str) = (
    1235,
    "This version of TiDB doesn't yet support 'Doesn't support match search on a non-string \
     column without fulltext index'",
);
const NON_STRING_CONSTANT: (u16, &str) = (
    1235,
    "This version of TiDB doesn't yet support 'match against a non-string constant'",
);
const NON_CONSTANT_STRING: (u16, &str) = (
    1235,
    "This version of TiDB doesn't yet support 'match against a non-constant string'",
);
const NOT_A_COLUMN: (u16, &str) =
    (1235, "This version of TiDB doesn't yet support 'not matching a column'");
const MODIFIER: (u16, &str) = (
    1235,
    "This version of TiDB doesn't yet support 'MATCH...AGAINST with this modifier on the native \
     FTS path (modifier is not carried through pushdown to TiFlash)'",
);
const STARTER_ONLY: (u16, &str) = (
    1235,
    "This version of TiDB doesn't yet support 'FTS_MATCH_WORD() is only supported in starter \
     deployment mode'",
);

fn ft_session() -> Session {
    let mut session = Session::new();
    session.run("create table ft(a varchar(20), b int, c text)").unwrap();
    session
}

fn rows(text: &[&[&str]]) -> Vec<Vec<String>> {
    text.iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect()
}

fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).unwrap_err().to_mysql_error();
    (error.code, error.message)
}

/// The plan as the probe prints it: each row's cells joined by one space.
fn plan_of(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(&format!("explain format='brief' {sql}")))
        .iter()
        .map(|row| row.join(" "))
        .collect()
}

/// `MATCH ... AGAINST` builds whatever the table holds and fails only when a
/// row is evaluated: Go's `builtinFtsMysqlMatchAgainstSig.evalReal`.
#[test]
fn match_against_builds_and_fails_only_at_evaluation() {
    let mut session = ft_session();
    for sql in [
        "select match(a) against('x') from ft",
        "select match(a, c) against('x') from ft",
    ] {
        assert_eq!(row_text(session.run(sql)), rows(&[]), "{sql}");
    }
    let not_pushed = vec![(
        1105,
        "Scalar function 'match_against'(signature: FTSMatchExpression, return type: double) is \
         not supported to push down to tikv now."
            .to_owned(),
    )];
    assert_eq!(
        plan_of(&mut session, "select match(a) against('x') from ft"),
        [
            "Projection 10000.00 root  match_against(x, test.ft.a)->Column#6",
            "└─TableReader 10000.00 root  data:TableFullScan",
            "  └─TableFullScan 10000.00 cop[tikv] table:ft keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(warnings_of(&session), not_pushed);
    assert_eq!(
        plan_of(&mut session, "select match(a, c) against(NULL) from ft"),
        [
            "Projection 10000.00 root  match_against(<nil>, test.ft.a, test.ft.c)->Column#6",
            "└─TableReader 10000.00 root  data:TableFullScan",
            "  └─TableFullScan 10000.00 cop[tikv] table:ft keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(warnings_of(&session), not_pushed);

    session.run("insert into ft values('x', 1, 'y')").unwrap();
    for (sql, expected) in [
        ("select match(a) against(NULL) from ft", rows(&[&["NULL"]])),
        ("select match(a, c) against(NULL) from ft", rows(&[&["NULL"]])),
        ("select 1 from ft where match(a) against(NULL)", rows(&[])),
        ("select match(a) against(NULL) is null from ft", rows(&[&["1"]])),
        ("select match_against(NULL, a) from ft", rows(&[&["NULL"]])),
    ] {
        assert_eq!(row_text(session.run(sql)), expected, "{sql}");
    }
    for (sql, (code, message)) in [
        ("select match(a) against('x') from ft", MATCH_OUTSIDE_INDEX),
        ("select match(a) against('x' in natural language mode) from ft", MATCH_OUTSIDE_INDEX),
        ("select 1 from ft where match(a) against('x')", MATCH_OUTSIDE_INDEX),
        ("select match(a) against('x') is null from ft", MATCH_OUTSIDE_INDEX),
        ("select match_against('x', a) from ft", MATCH_OUTSIDE_INDEX),
        ("select match(b) against('x') from ft", NON_STRING_COLUMN),
        ("select match(b) against(NULL) from ft", NON_STRING_COLUMN),
        ("select match(a) against(1) from ft", NON_STRING_CONSTANT),
        ("select match(a) against(concat('x', a)) from ft", NON_CONSTANT_STRING),
        ("select match_against(a, a) from ft", NON_CONSTANT_STRING),
        ("select match_against('x', 'y') from ft", NOT_A_COLUMN),
        ("select match(a) against('x' in boolean mode) from ft", MODIFIER),
        ("select match(a) against('x' with query expansion) from ft", MODIFIER),
        (
            "select match_against('x') from ft",
            (1582, "Incorrect parameter count in the call to native function 'match_against'"),
        ),
        ("select fts_match_word('x', a) from ft", STARTER_ONLY),
        ("select fts_match_word(a, a) from ft", STARTER_ONLY),
        (
            "select fts_match_word('x') from ft",
            (1582, "Incorrect parameter count in the call to native function 'fts_match_word'"),
        ),
    ] {
        assert_eq!(error_of(&mut session, sql), (code, message.to_owned()), "{sql}");
    }
}

#[test]
fn like_and_ilike_cast_their_arguments_to_strings() {
    let mut session = ft_session();
    session.run("insert into ft values('x', 1, 'y')").unwrap();
    for (sql, expected) in [
        (
            "select a like 'x', a ilike 'X', b like 1, b ilike '1', a like b, c like 'Y', \
             c ilike 'Y' from ft",
            rows(&[&["1", "1", "1", "1", "0", "0", "1"]]),
        ),
        (
            "select a like 'x' escape '|', 'a_' like 'a|_' escape '|', NULL like 'x', \
             'x' like NULL from ft",
            rows(&[&["1", "1", "NULL", "NULL"]]),
        ),
        ("select a not like 'x', a not ilike 'X' from ft", rows(&[&["0", "0"]])),
        ("select `like`(a, 'x', 92), `ilike`(a, 'X', 92) from ft", rows(&[&["1", "1"]])),
    ] {
        assert_eq!(row_text(session.run(sql)), expected, "{sql}");
    }
    assert_eq!(
        error_of(&mut session, "select `like`(a, 'x') from ft"),
        (1582, "Incorrect parameter count in the call to native function 'like'".to_owned())
    );
    // Go `patternLikeOrIlikeToExpression` takes one value on each side.
    for sql in [
        "select (1, 2) like 'a'",
        "select 'a' like (1, 2)",
        "select (a, b) like 'a' from ft",
        "select a not like (1, 2) from ft",
        "select (1, 2) ilike 'a'",
    ] {
        assert_eq!(
            error_of(&mut session, sql),
            (1241, "Operand should contain 1 column(s)".to_owned()),
            "{sql}"
        );
    }

    // The probe's testkit prints an integer's display width (`bigint(1)`); a
    // server, which deprecates it, prints `bigint`.
    let ilike_not_pushed = vec![(
        1105,
        "Scalar function 'ilike'(signature: IlikeSig, return type: bigint) is not supported to \
         push down to tikv now."
            .to_owned(),
    )];
    for (sql, plan, warnings) in [
        (
            "select * from ft where b like 1",
            [
                "TableReader 1000.00 root  data:Selection",
                "└─Selection 1000.00 cop[tikv]  like(cast(test.ft.b, var_string(20)), \"1\", 92)",
                "  └─TableFullScan 10000.00 cop[tikv] table:ft keep order:false, stats:pseudo",
            ],
            vec![],
        ),
        (
            "select * from ft where b ilike '1'",
            [
                "Selection 1000.00 root  ilike(cast(test.ft.b, var_string(20)), \"1\", 92)",
                "└─TableReader 10000.00 root  data:TableFullScan",
                "  └─TableFullScan 10000.00 cop[tikv] table:ft keep order:false, stats:pseudo",
            ],
            ilike_not_pushed.clone(),
        ),
        (
            "select * from ft where a not like 'x%'",
            [
                "TableReader 9000.00 root  data:Selection",
                "└─Selection 9000.00 cop[tikv]  not(like(test.ft.a, \"x%\", 92))",
                "  └─TableFullScan 10000.00 cop[tikv] table:ft keep order:false, stats:pseudo",
            ],
            vec![],
        ),
        (
            "select * from ft where c ilike a",
            [
                "Selection 1000.00 root  ilike(test.ft.c, test.ft.a, 92)",
                "└─TableReader 10000.00 root  data:TableFullScan",
                "  └─TableFullScan 10000.00 cop[tikv] table:ft keep order:false, stats:pseudo",
            ],
            ilike_not_pushed.clone(),
        ),
    ] {
        assert_eq!(plan_of(&mut session, sql), plan, "{sql}");
        assert_eq!(warnings_of(&session), warnings, "{sql}");
    }
}

#[test]
fn grouping_reads_the_expand_operators_gid() {
    let mut session = ft_session();
    session.run("insert into ft values('x', 1, 'y')").unwrap();
    for (sql, expected) in [
        (
            "select a, grouping(a) from ft group by a with rollup",
            rows(&[&["NULL", "1"], &["x", "0"]]),
        ),
        (
            "select a, b, grouping(a), grouping(b), grouping(a, b) from ft group by a, b \
             with rollup",
            rows(&[
                &["NULL", "NULL", "1", "1", "3"],
                &["x", "1", "0", "0", "0"],
                &["x", "NULL", "0", "1", "1"],
            ]),
        ),
        (
            "select grouping(a, b, a) from ft group by a, b with rollup",
            rows(&[&["0"], &["2"], &["7"]]),
        ),
    ] {
        // A hash aggregate's row order is its map's.
        let mut actual = row_text(session.run(sql));
        actual.sort();
        assert_eq!(actual, expected, "{sql}");
    }
    assert_eq!(
        error_of(&mut session, "select grouping(a) from ft"),
        (1111, "Invalid use of group function".to_owned())
    );
    assert_eq!(
        error_of(&mut session, "select grouping(b + 1) from ft group by a, b with rollup"),
        (3602, "Argument #0 of GROUPING function is not in GROUP BY".to_owned())
    );

    assert_eq!(
        plan_of(&mut session, "select a, grouping(a) from ft group by a with rollup"),
        [
            "Projection 8000.00 root  Column#6->Column#9, grouping(gid)->Column#10",
            "└─HashAgg 8000.00 root  group by:Column#6, gid, funcs:firstrow(Column#6)->Column#6, \
             funcs:firstrow(gid)->gid",
            "  └─Expand 10000.00 root  level-projection:[<nil>->Column#6, 0->gid],[Column#6, \
             1->gid]; schema: [Column#6,gid]",
            "    └─TableReader 10000.00 root  data:TableFullScan",
            "      └─TableFullScan 10000.00 cop[tikv] table:ft keep order:false, stats:pseudo",
        ]
    );
    // The probe's testkit prints `bigint(20)`; a server prints `bigint`.
    assert_eq!(
        warnings_of(&session),
        [(
            1105,
            "Scalar function 'grouping'(signature: GroupingSig, return type: bigint) is not \
             supported to push down to tikv now."
                .to_owned()
        )]
    );
}

const SEARCH_TERM: (u16, &str) = (
    1235,
    "This version of TiDB doesn't yet support 'MATCH...AGAINST search term 'a*' is not supported \
     in the LIKE fallback'",
);
const SKIP_BAKED_SEARCH: &str = "skip prepared plan-cache: MATCH...AGAINST LIKE fallback bakes a \
                                 mutable search string into plan constants";

/// The `planner/core/fulltext_search` table under
/// `tidb_opt_enable_alternative_logical_plans`, where Go's `fts-like-fallback`
/// round runs.
fn articles_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "set @@tidb_opt_enable_alternative_logical_plans=ON",
        "create table articles (id int primary key, title varchar(200), n int)",
        "insert into articles values (1, 'MySQL Tutorial', 1), (2, 'Optimizing MySQL', 2), \
         (3, 'PostgreSQL', 3)",
        "create table sink (id int)",
        "set @q = 'MySQL'",
        "set @p = 'Post'",
        "set @n = NULL",
        "set @i = 1",
        "set @b = 'a*'",
        "set @lo = 0",
        "set @hi = 2",
    ] {
        session.run(sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
    session
}

fn sorted(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let mut rows = row_text(session.run(sql));
    rows.sort();
    rows
}

fn skip_baked_search() -> Vec<Vec<String>> {
    rows(&[&["Warning", "1105", SKIP_BAKED_SEARCH]])
}

/// Go `matchAgainstToLike` bakes the search string into the plan's constants:
/// a literal's plan is cached, a parameter's answers one execution only
/// (`SetSkipPlanCache`), NULL included, and whatever is not a string constant
/// is the first round's class to refuse.
#[test]
fn a_prepared_like_fallback_bakes_its_parameter_and_skips_the_plan_cache() {
    let mut session = articles_session();
    for sql in [
        "prepare st_lit from 'select id from articles where match(title) against(''MySQL'')'",
        "prepare st_param from 'select id from articles where match(title) against(?)'",
        "prepare st_two from 'select id from articles where id > ? and match(title) against(?) \
         and id < ?'",
        "prepare st_not from 'select id from articles where not match(title) against(?)'",
        "prepare st_sub from 'select id from articles where id in (select id from articles \
         where match(title) against(?))'",
        "prepare st_on from 'select a.id from articles a join articles b on a.id = b.id and \
         match(a.title) against(?)'",
    ] {
        session.run(sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }

    assert_eq!(sorted(&mut session, "execute st_lit"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("show warnings")), rows(&[]));
    assert_eq!(sorted(&mut session, "execute st_lit"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("select @@last_plan_from_cache")), rows(&[&["1"]]));

    assert_eq!(sorted(&mut session, "execute st_param using @q"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("show warnings")), skip_baked_search());
    assert_eq!(sorted(&mut session, "execute st_param using @q"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("select @@last_plan_from_cache")), rows(&[&["0"]]));
    assert_eq!(sorted(&mut session, "execute st_param using @p"), rows(&[&["3"]]));

    assert_eq!(sorted(&mut session, "execute st_param using @n"), rows(&[]));
    assert_eq!(row_text(session.run("show warnings")), skip_baked_search());
    assert_eq!(sorted(&mut session, "execute st_param using @q"), rows(&[&["1"], &["2"]]));

    assert_eq!(error_of(&mut session, "execute st_param using @i"), own(NON_STRING_CONSTANT));
    assert_eq!(error_of(&mut session, "execute st_param using @b"), own(SEARCH_TERM));

    assert_eq!(sorted(&mut session, "execute st_two using @lo, @q, @hi"), rows(&[&["1"]]));
    assert_eq!(row_text(session.run("show warnings")), skip_baked_search());
    assert_eq!(sorted(&mut session, "execute st_two using @lo, @q, @hi"), rows(&[&["1"]]));
    assert_eq!(row_text(session.run("select @@last_plan_from_cache")), rows(&[&["0"]]));

    assert_eq!(sorted(&mut session, "execute st_not using @q"), rows(&[&["3"]]));
    assert_eq!(sorted(&mut session, "execute st_not using @n"), rows(&[]));

    assert_eq!(sorted(&mut session, "execute st_sub using @q"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("show warnings")), skip_baked_search());
    assert_eq!(sorted(&mut session, "execute st_on using @q"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("show warnings")), skip_baked_search());

    // A scalar position is not the fallback's: the builtin evaluates.
    session
        .run("prepare st_scalar from 'select id, match(title) against(?) from articles'")
        .unwrap();
    assert_eq!(
        error_of(&mut session, "execute st_scalar using @q"),
        own(MATCH_OUTSIDE_INDEX)
    );
    // Go's class refuses the column while PREPARE builds its plan over the
    // NULL marker.
    assert_eq!(
        error_of(&mut session, "prepare st_n from 'select id from articles where match(n) against(?)'"),
        own(NON_STRING_COLUMN)
    );
}

fn own(error: (u16, &str)) -> (u16, String) {
    (error.0, error.1.to_owned())
}

/// Go's plan-cache key does not hold `tidb_opt_enable_alternative_logical_plans`:
/// the variable decides the plan a miss builds, and a cached plan answers
/// whatever the variable says by then.
#[test]
fn the_like_fallback_round_is_the_plan_cache_misss_alone() {
    let mut session = articles_session();
    for sql in [
        "set @@tidb_opt_enable_alternative_logical_plans=OFF",
        "prepare st_param from 'select id from articles where match(title) against(?)'",
        "prepare st_lit from 'select id from articles where match(title) against(''MySQL'')'",
    ] {
        session.run(sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
    // Off, the miss builds the builtin, and that plan is cached.
    assert_eq!(error_of(&mut session, "execute st_param using @q"), own(MATCH_OUTSIDE_INDEX));
    assert_eq!(error_of(&mut session, "execute st_param using @q"), own(MATCH_OUTSIDE_INDEX));
    assert_eq!(row_text(session.run("select @@last_plan_from_cache")), rows(&[&["1"]]));
    session.run("set @@tidb_opt_enable_alternative_logical_plans=ON").unwrap();
    assert_eq!(error_of(&mut session, "execute st_param using @q"), own(MATCH_OUTSIDE_INDEX));

    // On, the miss builds the fallback, and a literal's plan is cached.
    assert_eq!(sorted(&mut session, "execute st_lit"), rows(&[&["1"], &["2"]]));
    session.run("set @@tidb_opt_enable_alternative_logical_plans=OFF").unwrap();
    assert_eq!(sorted(&mut session, "execute st_lit"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("select @@last_plan_from_cache")), rows(&[&["1"]]));
}

/// Go `optimize` runs its alternative rounds only for a logical plan: a query
/// gets the LIKE fallback, DML, `SET` and `DO` keep the native builtin.
#[test]
fn only_a_query_gets_the_like_fallback_round() {
    let mut session = articles_session();
    for sql in [
        "select id from articles where match(title) against('MySQL') union select id from \
         articles where match(title) against('Post')",
    ] {
        assert_eq!(sorted(&mut session, sql), rows(&[&["1"], &["2"], &["3"]]), "{sql}");
    }
    for sql in [
        "with c as (select id from articles where match(title) against('MySQL')) select * from c",
        "select id from articles where match(title) against('MySQL') for update",
    ] {
        assert_eq!(sorted(&mut session, sql), rows(&[&["1"], &["2"]]), "{sql}");
        assert_eq!(row_text(session.run("show warnings")), rows(&[]), "{sql}");
    }
    let selection = |session: &mut Session, sql: &str| {
        plan_of(session, sql)
            .into_iter()
            .find(|row| row.contains("Selection"))
            .unwrap_or_else(|| panic!("{sql}: no Selection"))
    };
    assert_eq!(
        selection(&mut session, "select id from articles where match(title) against('MySQL')"),
        "└─Selection 8000.00 root  ifnull(ilike(test.articles.title, \"%MySQL%\", 92), 0)"
    );
    session
        .run(
            "prepare st_exp from 'explain format=''brief'' select id from articles where \
             match(title) against(?)'",
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("execute st_exp using @q"))
            .iter()
            .map(|row| row.join(" "))
            .find(|row| row.contains("Selection")),
        Some(
            "└─Selection 8000.00 root  ifnull(ilike(test.articles.title, \"%MySQL%\", 92), 0)"
                .to_owned()
        )
    );
    assert_eq!(
        selection(&mut session, "delete from articles where match(title) against('MySQL')"),
        "  └─Selection 1000.00 root  match_against(\"MySQL\", test.articles.title)"
    );

    for sql in [
        "delete from articles where match(title) against('Post')",
        "insert into articles select id + 10, title, n from articles where match(title) \
         against('MySQL')",
        "set @x = (select count(*) from articles where match(title) against('MySQL'))",
    ] {
        assert_eq!(error_of(&mut session, sql), own(MATCH_OUTSIDE_INDEX), "{sql}");
    }
    for (prepare, execute) in [
        ("prepare st_del from 'delete from articles where match(title) against(?)'", "execute st_del using @q"),
        (
            "prepare st_upd from 'update articles set n = n + 1 where match(title) against(?)'",
            "execute st_upd using @q",
        ),
        (
            "prepare st_ins from 'insert into sink select id from articles where match(title) \
             against(?)'",
            "execute st_ins using @q",
        ),
    ] {
        session.run(prepare).unwrap_or_else(|err| panic!("{prepare}: {err:?}"));
        assert_eq!(error_of(&mut session, execute), own(MATCH_OUTSIDE_INDEX), "{execute}");
    }
    assert_eq!(row_text(session.run("select @x")), rows(&[&["NULL"]]));
    assert_eq!(
        row_text(session.run("select count(*), sum(n) from articles")),
        rows(&[&["3", "6"]])
    );
    assert_eq!(row_text(session.run("select count(*) from sink")), rows(&[&["0"]]));

    // A search that is not a string constant is the first round's refusal,
    // and the class reads the search before the column.
    for sql in [
        "select id from articles where match(title) against(1)",
        "select id from articles where match(title) against(1.5)",
        "select id from articles where match(n) against(1)",
    ] {
        assert_eq!(error_of(&mut session, sql), own(NON_STRING_CONSTANT), "{sql}");
    }
}

/// Go `cardinality.Selectivity`'s `ast.FTSMysqlMatchAgainst` arm: the native
/// builtin is estimated as the ILIKE it stands for -- a string match (0.1), a
/// constant for `AGAINST(NULL)` and the empty search (0), and the string
/// match again when the substitution fails (multi-column).
#[test]
fn a_native_match_against_is_estimated_as_its_ilike() {
    let mut session = Session::new();
    session
        .run("create table articles (id int primary key, title varchar(200), body varchar(200), n int)")
        .unwrap();
    let selection = |session: &mut Session, sql: &str| {
        plan_of(session, sql)
            .into_iter()
            .find(|row| row.contains("Selection"))
            .unwrap_or_else(|| panic!("{sql}: no Selection"))
    };
    for (sql, row) in [
        (
            "delete from articles where match(title) against('MySQL')",
            "  └─Selection 1000.00 root  match_against(\"MySQL\", test.articles.title)",
        ),
        (
            "delete from articles where match(title) against(NULL)",
            "  └─Selection 1.00 root  match_against(NULL, test.articles.title)",
        ),
        (
            "delete from articles where match(title) against('')",
            "  └─Selection 1.00 root  match_against(\"\", test.articles.title)",
        ),
        (
            "delete from articles where match(title, body) against('MySQL')",
            "  └─Selection 1000.00 root  match_against(\"MySQL\", test.articles.title, \
             test.articles.body)",
        ),
        (
            "delete from articles where not match(title) against('MySQL')",
            "  └─Selection 8000.00 root  not(istrue_with_null(match_against(\"MySQL\", \
             test.articles.title)))",
        ),
        (
            "select id from articles where match(title) against('MySQL')",
            "└─Selection 1000.00 root  match_against(\"MySQL\", test.articles.title)",
        ),
    ] {
        assert_eq!(selection(&mut session, sql), row, "{sql}");
    }
}

/// Go `matchAgainstToLike` runs in the expression rewriter, over the search
/// expression the rewriter built: whatever folds to a string constant there
/// (a function of literals, a user variable) is the pattern.
#[test]
fn the_like_fallback_reads_the_search_string_the_rewriter_folded() {
    let mut session = articles_session();
    for (sql, want) in [
        ("select id from articles where match(title) against(concat('My','SQL'))", &[&["1"][..], &["2"]][..]),
        ("select id from articles where match(title) against(@p)", &[&["3"]]),
        ("select id from articles where match(title) against(concat(@p,'gre'))", &[&["3"]]),
        ("select id from articles where match(title) against(lower('POST'))", &[&["3"]]),
        ("select id from articles where match(title) against(@n)", &[]),
        ("select id from articles where match(title) against(@undefined)", &[]),
    ] {
        assert_eq!(sorted(&mut session, sql), rows(want), "{sql}");
    }
    for (sql, want) in [
        ("select id from articles where match(title) against(@i)", NON_STRING_CONSTANT),
        ("select id from articles where match(title) against(1+1)", NON_STRING_CONSTANT),
        ("select id from articles where match(title) against(title)", NON_CONSTANT_STRING),
        ("select id from articles where match(title) against(rand())", NON_CONSTANT_STRING),
        (
            "select id from articles where match(title) against(concat('My', cast(id as char)))",
            NON_CONSTANT_STRING,
        ),
        ("select id from articles where match(n) against(concat('a','b'))", NON_STRING_COLUMN),
        ("select id from articles where match(n) against(@q)", NON_STRING_COLUMN),
    ] {
        assert_eq!(error_of(&mut session, sql), own(want), "{sql}");
    }
    for (sql, pattern) in [
        ("select id from articles where match(title) against(concat('My','SQL'))", "%MySQL%"),
        ("select id from articles where match(title) against(@p)", "%Post%"),
    ] {
        assert_eq!(
            plan_of(&mut session, sql).into_iter().find(|row| row.contains("Selection")),
            Some(format!(
                "└─Selection 8000.00 root  ifnull(ilike(test.articles.title, \"{pattern}\", 92), 0)"
            )),
            "{sql}"
        );
    }
}

/// Go evaluates an uncorrelated scalar or `EXISTS` subquery while the first
/// round builds, with the native builtin: the statement fails before any
/// alternative round. A subquery that stays in the plan (`IN`, a correlated
/// `EXISTS`, a derived table) is rebuilt by the fallback round.
#[test]
fn a_subquery_the_first_round_evaluates_keeps_the_native_builtin() {
    let mut session = articles_session();
    for sql in [
        "select (select count(*) from articles where match(title) against('MySQL'))",
        "select id from articles where exists (select 1 from articles where match(title) \
         against('MySQL'))",
        "select id from articles where id > (select count(*) from articles where match(title) \
         against('MySQL'))",
        "do (select count(*) from articles where match(title) against('MySQL'))",
    ] {
        assert_eq!(error_of(&mut session, sql), own(MATCH_OUTSIDE_INDEX), "{sql}");
    }
    for sql in [
        "select id from articles where id in (select id from articles where match(title) \
         against('MySQL'))",
        "select id from articles a where exists (select 1 from articles b where b.id = a.id and \
         match(b.title) against('MySQL'))",
        "select id from (select * from articles where match(title) against('MySQL')) d",
    ] {
        assert_eq!(sorted(&mut session, sql), rows(&[&["1"], &["2"]]), "{sql}");
    }
}

/// A view's body is built by the round that reads the view: its `WHERE` is a
/// direct boolean context, its select list is not.
#[test]
fn a_views_match_against_falls_back_in_the_round_that_reads_it() {
    let mut session = articles_session();
    for sql in [
        "create view av as select id from articles where match(title) against('MySQL')",
        "create view av2 as select id, match(title) against('MySQL') m from articles",
        "prepare pv from 'select * from av order by id'",
    ] {
        session.run(sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
    assert_eq!(sorted(&mut session, "select * from av"), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("show warnings")), rows(&[]));
    assert_eq!(
        sorted(&mut session, "select id from av where id in (select id from av)"),
        rows(&[&["1"], &["2"]])
    );
    assert_eq!(error_of(&mut session, "select * from av2"), own(MATCH_OUTSIDE_INDEX));
    assert_eq!(row_text(session.run("execute pv")), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("execute pv")), rows(&[&["1"], &["2"]]));
    assert_eq!(row_text(session.run("select @@last_plan_from_cache")), rows(&[&["1"]]));
    assert_eq!(
        plan_of(&mut session, "select * from av").into_iter().find(|row| row.contains("Selection")),
        Some(
            "└─Selection 8000.00 root  ifnull(ilike(test.articles.title, \"%MySQL%\", 92), 0)"
                .to_owned()
        )
    );
}

/// A parameter inside the search expression is folded and baked by each
/// execution's fallback round.
#[test]
fn an_execution_bakes_the_search_string_its_parameter_folds_to() {
    let mut session = articles_session();
    session
        .run(
            "prepare pc from 'select id from articles where match(title) \
             against(concat(?, ''SQL'')) order by id'",
        )
        .unwrap();
    session.run("set @m = 'My'").unwrap();
    for _ in 0..2 {
        assert_eq!(row_text(session.run("execute pc using @m")), rows(&[&["1"], &["2"]]));
        assert_eq!(row_text(session.run("show warnings")), skip_baked_search());
    }
    session.run("execute pc using @m").unwrap();
    assert_eq!(row_text(session.run("select @@last_plan_from_cache")), rows(&[&["0"]]));
}

/// Go `GetPlanFromPlanCache`: a miss whose `generateNewPlan` fails returns
/// the optimizer's error as the statement's, and the EXECUTE's one statement
/// context keeps the warning the fallback left before it failed (captured:
/// `execute pb using @b` with `@b = 'a*'` answers 1235 and SHOW WARNINGS
/// lists the skip-plan-cache warning before the error).
#[test]
fn a_failed_plan_cache_miss_keeps_the_warnings_of_its_optimization() {
    let mut session = articles_session();
    session
        .run("prepare pb from 'select id from articles where match(title) against(?) order by id'")
        .unwrap();
    session.run("set @b = 'a*'").unwrap();
    let error = session.run("execute pb using @b").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1235, "{}", error.message);
    let mut want = skip_baked_search();
    want.extend(rows(&[&[
        "Error",
        "1235",
        "This version of TiDB doesn't yet support 'MATCH...AGAINST search term 'a*' is not \
         supported in the LIKE fallback'",
    ]]));
    assert_eq!(row_text(session.run("show warnings")), want);
}

/// Go's UPDATE and DELETE never reach the LIKE fallback: the prepared write's
/// `MATCH` is refused when the execution plans it, with no skip-plan-cache
/// warning and no row changed (captured over `articles`).
#[test]
fn a_prepared_write_refuses_its_match_at_execution_like_go() {
    let mut session = articles_session();
    session.run("set @b = 'a*'").unwrap();
    session.run("set @g = 'My'").unwrap();
    let refused = "cannot use 'MATCH ... AGAINST' outside of fulltext index";
    for (prepare, executions) in [
        (
            "prepare pu from 'update articles set n = n + 1 where match(title) against(?)'",
            ["execute pu using @b", "execute pu using @g"],
        ),
        (
            "prepare pd from 'delete from articles where match(title) against(?)'",
            ["execute pd using @b", "execute pd using @g"],
        ),
    ] {
        session.run(prepare).unwrap();
        for execute in executions {
            let error = session.run(execute).unwrap_err().to_mysql_error();
            assert_eq!((error.code, error.message.as_str()), (1105, refused), "{execute}");
            assert_eq!(
                row_text(session.run("show warnings")),
                rows(&[&["Error", "1105", refused]]),
                "{execute}"
            );
        }
    }
    assert_eq!(
        row_text(session.run("select id, n from articles order by id")),
        rows(&[&["1", "1"], &["2", "2"], &["3", "3"]])
    );
}

/// The same miss over the binary protocol (`COM_STMT_EXECUTE`): Go's
/// `ExecutePreparedStmt` is the one `ExecuteStmt`, so the query fails with the
/// optimizer's error and its statement context keeps the skip warning; the
/// write is refused as the text EXECUTE's is.
#[test]
fn a_failed_plan_cache_miss_over_the_binary_protocol_keeps_its_warnings() {
    use tidb_param::PreparedValue;

    let mut session = articles_session();
    let query = crate::tests_support::prepare_binary(
        &mut session,
        "select id from articles where match(title) against(?) order by id",
    );
    let failed = session
        .node_conn()
        .execute_general(&query, &[PreparedValue::String(b"a*".to_vec())])
        .is_err();
    assert!(failed, "the unsupported search term fails the EXECUTE");
    let mut want = skip_baked_search();
    want.extend(rows(&[&[
        "Error",
        "1235",
        "This version of TiDB doesn't yet support 'MATCH...AGAINST search term 'a*' is not \
         supported in the LIKE fallback'",
    ]]));
    assert_eq!(row_text(session.run("show warnings")), want);

    let write = crate::tests_support::prepare_binary(
        &mut session,
        "update articles set n = n + 1 where match(title) against(?)",
    );
    let error = session
        .node_conn()
        .execute_general(&write, &[PreparedValue::String(b"My".to_vec())])
        .err()
        .expect("a write's MATCH is refused");
    assert_eq!(
        (error.code, error.message.as_str()),
        (1105, "cannot use 'MATCH ... AGAINST' outside of fulltext index")
    );
    assert_eq!(
        row_text(session.run("select id, n from articles order by id")),
        rows(&[&["1", "1"], &["2", "2"], &["3", "3"]])
    );
}
