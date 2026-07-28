#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// Every `ALGORITHM` a `CREATE VIEW` may write round-trips through
/// `SHOW CREATE VIEW`, and NONE of them changes what the view returns.
///
/// Captured from Go's mock store: `MERGE` and `TEMPTABLE` print back
/// exactly as written, an omitted clause prints `UNDEFINED`, and all
/// three read the same rows -- on this tier the algorithm is recorded
/// text, because the merge-vs-materialize choice it names is a plan
/// shape, not a result.
#[test]
fn view_algorithm_round_trips_through_show_create_view() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    for (written, printed) in [
        ("", "UNDEFINED"),
        ("ALGORITHM=UNDEFINED ", "UNDEFINED"),
        ("ALGORITHM=MERGE ", "MERGE"),
        ("ALGORITHM=TEMPTABLE ", "TEMPTABLE"),
    ] {
        session.run("DROP VIEW IF EXISTS vv").unwrap();
        session
            .run(&format!("CREATE {written}VIEW vv AS SELECT a, b FROM t"))
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vv");
        assert_eq!(
            rows[0][1],
            format!(
                "CREATE ALGORITHM={printed} DEFINER=``@`` SQL SECURITY DEFINER VIEW `vv` \
                     (`a`, `b`) AS SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t`"
            )
        );
        let (_, rows) = query_text(&mut session, "SELECT * FROM vv");
        assert_eq!(rows, [["1", "10"], ["2", "20"], ["3", "30"]]);
    }

    // `CREATE OR REPLACE` rewrites the recorded algorithm along with the
    // body, so the replacement's own clause is what prints afterwards.
    session.run("DROP VIEW IF EXISTS vv").unwrap();
    session
        .run("CREATE ALGORITHM=MERGE VIEW vv AS SELECT a FROM t")
        .unwrap();
    session
        .run("CREATE OR REPLACE ALGORITHM=TEMPTABLE VIEW vv AS SELECT b FROM t")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vv");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=TEMPTABLE DEFINER=``@`` SQL SECURITY DEFINER VIEW `vv` (`b`) \
             AS SELECT `b` AS `b` FROM `test`.`t`"
    );

    // The algorithm and an explicit column list are recorded together.
    session
        .run("CREATE ALGORITHM=MERGE VIEW vw2 (p, q) AS SELECT a, b FROM t")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vw2");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=MERGE DEFINER=``@`` SQL SECURITY DEFINER VIEW `vw2` (`p`, `q`) \
             AS SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t`"
    );
}

/// A `LATERAL` derived table really is re-evaluated per outer row: the
/// captured counts differ per group, which no uncorrelated single run
/// could produce.
#[test]
fn lateral_derived_table_varies_per_outer_row() {
    let mut session = lateral_session();

    let (names, rows) = query_text(
        &mut session,
        "SELECT t.a, x.cnt FROM t, LATERAL (SELECT COUNT(*) AS cnt FROM s WHERE s.k = t.a) x",
    );
    assert_eq!(names, ["a", "cnt"]);
    assert_eq!(rows, [["1", "2"], ["2", "1"], ["3", "3"]]);

    // The inner relation may be several rows tall: each one is
    // concatenated onto its own outer row.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t, LATERAL (SELECT v FROM s WHERE s.k = t.a) x",
    );
    assert_eq!(
        rows,
        [
            ["1", "100"],
            ["1", "101"],
            ["2", "200"],
            ["3", "300"],
            ["3", "301"],
            ["3", "302"],
        ]
    );

    // A `LIMIT` inside the subquery applies per outer row, not once.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t, LATERAL (SELECT v FROM s WHERE s.k = t.a \
             ORDER BY v DESC LIMIT 2) x",
    );
    assert_eq!(
        rows,
        [
            ["1", "101"],
            ["1", "100"],
            ["2", "200"],
            ["3", "302"],
            ["3", "301"],
        ]
    );

    // The join is INNER (Go's `buildLateralJoin` always builds
    // `InnerJoin`), so an outer row whose inner relation is empty is
    // dropped -- captured: `a = 9` matches no `s` row and disappears.
    session.run("INSERT INTO t VALUES (9, 90)").unwrap();
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a FROM t, LATERAL (SELECT v FROM s WHERE s.k = t.a) x",
    );
    assert_eq!(rows.len(), 6);
    assert!(!rows.iter().any(|row| row[0] == "9"));
    // An inner relation that always has a row keeps every outer row.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a FROM t, LATERAL (SELECT 1) x WHERE t.a = 9",
    );
    assert_eq!(rows, [["9"]]);
}

/// The join shapes `buildLateralJoin` accepts and refuses.
#[test]
fn lateral_derived_table_join_shapes() {
    let mut session = lateral_session();

    // A `LATERAL` may correlate with SEVERAL preceding tables at once.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, u.z, x.n FROM t, u, LATERAL (SELECT t.a + u.z AS n) x ORDER BY t.a, u.z",
    );
    assert_eq!(
        rows,
        [
            ["1", "7", "8"],
            ["1", "8", "9"],
            ["2", "7", "9"],
            ["2", "8", "10"],
            ["3", "7", "10"],
            ["3", "8", "11"],
        ]
    );

    // One `LATERAL` may correlate with a preceding `LATERAL`.
    let (_, rows) = query_text(
        &mut session,
        "SELECT * FROM t, LATERAL (SELECT t.a) x, LATERAL (SELECT x.a + 1 AS y) z",
    );
    assert_eq!(
        rows,
        [
            ["1", "10", "1", "2"],
            ["2", "20", "2", "3"],
            ["3", "30", "3", "4"],
        ]
    );

    // CROSS/INNER JOIN read the same as the comma syntax, and an `ON`
    // condition filters the Apply's rows.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t CROSS JOIN LATERAL (SELECT v FROM s WHERE s.k = t.a) x",
    );
    assert_eq!(rows.len(), 6);
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t JOIN LATERAL (SELECT v FROM s WHERE s.k = t.a) x \
             ON x.v > 200 ORDER BY x.v",
    );
    assert_eq!(rows, [["3", "300"], ["3", "301"], ["3", "302"]]);

    // Captured: [planner:3809]. Go rejects outer joins with LATERAL.
    assert!(matches!(
        session
            .run("SELECT t.a FROM t LEFT JOIN LATERAL (SELECT v FROM s WHERE s.k = t.a) x ON TRUE"),
        Err(DriverError::InvalidLateralJoin(
            "LEFT JOIN is not supported with LATERAL"
        ))
    ));
    assert!(matches!(
        session.run(
            "SELECT t.a FROM t RIGHT JOIN LATERAL (SELECT v FROM s WHERE s.k = t.a) x ON TRUE"
        ),
        Err(DriverError::InvalidLateralJoin(
            "RIGHT JOIN is not supported with LATERAL"
        ))
    ));

    // A leftmost `LATERAL` has nothing to correlate with and reads as an
    // ordinary derived table (captured: it runs).
    let (_, rows) = query_text(&mut session, "SELECT * FROM LATERAL (SELECT 1) x");
    assert_eq!(rows, [["1"]]);
}

/// A `LATERAL` derived table whose body is a set operation (`UNION`/
/// `UNION ALL`), which the correlated-column collector previously did not
/// walk (see `collect_correlated_columns_query` in `driver.rs`). Every case
/// captured from Go's mock store.
#[test]
fn lateral_derived_table_over_set_operation() {
    let mut session = lateral_session();

    // A correlated column referenced in both arms, varying per outer row.
    let (names, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t, LATERAL \
             (SELECT k AS v FROM s WHERE s.k = t.a \
              UNION \
              SELECT v FROM s WHERE s.v = t.a * 100) x \
             ORDER BY t.a, x.v",
    );
    assert_eq!(names, ["a", "v"]);
    assert_eq!(
        rows,
        [
            ["1", "1"],
            ["1", "100"],
            ["2", "2"],
            ["2", "200"],
            ["3", "3"],
            ["3", "300"],
        ]
    );

    // UNION ALL keeps duplicates the lateral subquery itself produces;
    // plain UNION (no ALL) dedups them -- both per outer row.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t, LATERAL (SELECT t.a AS v UNION ALL SELECT t.a AS v) x \
             ORDER BY t.a",
    );
    assert_eq!(
        rows,
        [
            ["1", "1"],
            ["1", "1"],
            ["2", "2"],
            ["2", "2"],
            ["3", "3"],
            ["3", "3"],
        ]
    );
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t, LATERAL (SELECT t.a AS v UNION SELECT t.a AS v) x \
             ORDER BY t.a",
    );
    assert_eq!(rows, [["1", "1"], ["2", "2"], ["3", "3"]]);

    // The set operation's own ORDER BY/LIMIT applies to the folded result,
    // not to either arm alone, and still runs once per outer row.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t, LATERAL \
             (SELECT v FROM s WHERE s.k = t.a \
              UNION \
              SELECT v FROM s WHERE v > 0 \
              ORDER BY v DESC LIMIT 1) x \
             ORDER BY t.a",
    );
    assert_eq!(rows, [["1", "302"], ["2", "302"], ["3", "302"]]);

    // Correlation in only one arm: the other is a constant relation
    // unioned in on every outer row.
    let (_, rows) = query_text(
        &mut session,
        "SELECT t.a, x.v FROM t, LATERAL (SELECT t.a AS v UNION SELECT k AS v FROM s) x \
             ORDER BY t.a, x.v",
    );
    assert_eq!(
        rows,
        [
            ["1", "1"],
            ["1", "2"],
            ["1", "3"],
            ["2", "1"],
            ["2", "2"],
            ["2", "3"],
            ["3", "1"],
            ["3", "2"],
            ["3", "3"],
        ]
    );
}

/// The alias column list, which the grammar allows ONLY on a `LATERAL`
/// derived table -- a plain `(SELECT ...) x(c1)` is a parse error, so the
/// parser is the whole story there and only the lateral form reaches
/// execution.
#[test]
fn lateral_alias_column_list_renames_positionally() {
    let mut session = lateral_session();

    let (names, rows) = query_text(
        &mut session,
        "SELECT x.c FROM t, LATERAL (SELECT COUNT(*) FROM s WHERE s.k = t.a) x(c)",
    );
    assert_eq!(names, ["c"]);
    assert_eq!(rows, [["2"], ["1"], ["3"]]);

    // Captured: a width disagreement is [ddl:1353], the same error a
    // `CREATE VIEW` column list mismatch reports.
    assert!(matches!(
        session.run("SELECT * FROM t, LATERAL (SELECT 1, 2) x(c)"),
        Err(DriverError::ViewWrongList)
    ));
    assert!(matches!(
        session.run("SELECT * FROM t, LATERAL (SELECT 1) x(c1, c2)"),
        Err(DriverError::ViewWrongList)
    ));

    // A plain derived table's alias column list never parses.
    assert!(session
        .run("SELECT * FROM (SELECT a, b FROM t) x(c1, c2)")
        .is_err());
}

/// Without `LATERAL`, a derived table cannot see a sibling `FROM` entry:
/// captured as [planner:1054], reported against the clause the reference
/// sits in.
#[test]
fn non_lateral_derived_table_cannot_see_a_sibling() {
    let mut session = lateral_session();

    assert!(session
        .run("SELECT t.a FROM t, (SELECT COUNT(*) AS cnt FROM s WHERE s.k = t.a) x")
        .is_err());
    assert!(session.run("SELECT * FROM t, (SELECT t.a AS q) x").is_err());
}

/// Reading through a view: the plain form, a pushed-down predicate, an
/// explicit column list, a view of a view, and a view joined to a table.
/// Every result captured from upstream Go on a mock store.
#[test]
fn views_are_read_as_their_query() {
    let mut session = view_session();

    // Captured: header [a b], rows 1/10, 2/20, 3/30.
    let (names, rows) = query_text(&mut session, "SELECT * FROM v");
    assert_eq!(names, ["a", "b"]);
    assert_eq!(rows, [["1", "10"], ["2", "20"], ["3", "30"]]);

    // The outer WHERE filters the view's rows.
    let (_, rows) = query_text(&mut session, "SELECT * FROM v WHERE a > 1");
    assert_eq!(rows, [["2", "20"], ["3", "30"]]);
    let (_, rows) = query_text(&mut session, "SELECT a FROM v ORDER BY a DESC");
    assert_eq!(rows, [["3"], ["2"], ["1"]]);

    // The column list renames the body's output, so `a2` is the only name
    // that resolves.
    let (names, rows) = query_text(&mut session, "SELECT * FROM v2");
    assert_eq!(names, ["a2"]);
    assert_eq!(rows, [["1"], ["2"], ["3"]]);
    let (_, rows) = query_text(&mut session, "SELECT a2 FROM v2 WHERE a2 = 2");
    assert_eq!(rows, [["2"]]);

    // A view over a view.
    let (names, rows) = query_text(&mut session, "SELECT * FROM v3");
    assert_eq!(names, ["a", "b"]);
    assert_eq!(rows, [["2", "20"], ["3", "30"]]);

    // A view joined to a base table.
    let (names, rows) = query_text(&mut session, "SELECT v.a, s.c FROM v JOIN s ON v.a = s.a");
    assert_eq!(names, ["a", "c"]);
    assert_eq!(rows, [["1", "x"], ["2", "y"]]);
}

/// `SHOW CREATE VIEW` and `SHOW CREATE TABLE` over a view, asserted
/// against the exact captured text.
#[test]
fn show_create_view_prints_the_stored_definition() {
    let mut session = view_session();

    let (names, rows) = query_text(&mut session, "SHOW CREATE VIEW v");
    assert_eq!(
        names,
        [
            "View",
            "Create View",
            "character_set_client",
            "collation_connection"
        ]
    );
    assert_eq!(
        rows,
        [[
            "v",
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v` \
                 (`a`, `b`) AS SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t`",
            "utf8mb4",
            "utf8mb4_bin",
        ]]
    );

    // The explicit column list is what the header prints; the body keeps
    // the names it was written with.
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW v2");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v2` (`a2`) \
             AS SELECT `a` AS `a` FROM `test`.`t`"
    );

    // A view of a view stores its body's columns fully qualified.
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW v3");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v3` \
             (`a`, `b`) AS SELECT `test`.`v`.`a` AS `a`,`test`.`v`.`b` AS `b` \
             FROM `test`.`v` WHERE `b`>10"
    );

    // SHOW CREATE TABLE over a view prints the view form, header and all.
    let (table_names, table_rows) = query_text(&mut session, "SHOW CREATE TABLE v");
    let (view_names, view_rows) = query_text(&mut session, "SHOW CREATE VIEW v");
    assert_eq!(table_names, view_names);
    assert_eq!(table_rows, view_rows);

    // Captured: [executor:1347]'test.t' is not VIEW.
    assert!(matches!(
        session.run("SHOW CREATE VIEW t"),
        Err(DriverError::Schema(SchemaErrorKind::NotView(ref name))) if name == "test.t"
    ));

    // An aliased body keeps the alias, both in the FROM and in the
    // column references. Captured from Go.
    session
        .run("CREATE VIEW valias AS SELECT x.a FROM t AS x")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW valias");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `valias` (`a`) \
             AS SELECT `x`.`a` AS `a` FROM `test`.`t` AS `x`"
    );

    // A FROM-less body, whose single column is named after its text.
    session.run("CREATE VIEW vlit AS SELECT 1").unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vlit");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vlit` (`1`) \
             AS SELECT 1 AS `1`"
    );
    let (names, rows) = query_text(&mut session, "SELECT * FROM vlit");
    assert_eq!(names, ["1"]);
    assert_eq!(rows, [["1"]]);
}

/// Which statements may name a view, and which report the other kind.
#[test]
fn view_and_table_statements_do_not_cross() {
    let mut session = view_session();

    // Captured: [ddl:1347]'test.t' is not VIEW.
    assert!(matches!(
        session.run("DROP VIEW t"),
        Err(DriverError::Schema(SchemaErrorKind::NotView(ref name))) if name == "test.t"
    ));
    // The refusal really did not drop the table.
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM t")),
        vec![vec!["3".to_owned()]]
    );

    // Captured: [schema:1051]Unknown table 'test.v' -- DROP TABLE does not
    // see a view at all.
    assert!(matches!(
        session.run("DROP TABLE v"),
        Err(DriverError::Schema(SchemaErrorKind::BadTable(ref name))) if name == "test.v"
    ));
    assert_eq!(row_text(session.run("SELECT COUNT(*) FROM v")).len(), 1);

    // Captured: [schema:1050]Table 'test.v' already exists.
    assert!(matches!(
        session.run("CREATE VIEW v AS SELECT 1"),
        Err(DriverError::Schema(SchemaErrorKind::TableExists(ref name))) if name == "test.v"
    ));
    // OR REPLACE overwrites it instead.
    session
        .run("CREATE OR REPLACE VIEW v AS SELECT a AS a, b AS b FROM t")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SELECT * FROM v");
    assert_eq!(rows, [["1", "10"], ["2", "20"], ["3", "30"]]);

    // Captured: [schema:1051]Unknown table 'test.nosuch', suppressed by
    // IF EXISTS.
    session.run("DROP VIEW IF EXISTS nosuch").unwrap();
    assert!(matches!(
        session.run("DROP VIEW nosuch"),
        Err(DriverError::Schema(SchemaErrorKind::BadTable(ref name))) if name == "test.nosuch"
    ));

    // Captured: [ddl:1353], the column list and the select list disagree.
    assert!(matches!(
        session.run("CREATE VIEW vbad(x, y) AS SELECT a FROM t"),
        Err(DriverError::ViewWrongList)
    ));

    // Captured: a view is hidden from its own replacement's body, so
    // `SELECT ... FROM v` inside `CREATE OR REPLACE VIEW v` is
    // [planner:1146]Table 'test.v' doesn't exist -- which is also why no
    // directly recursive view can be built.
    assert!(matches!(
        session.run("CREATE OR REPLACE VIEW v AS SELECT * FROM v"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(ref name))) if name == "test.v"
    ));

    // A comma-separated DROP VIEW drops them all.
    session.run("DROP VIEW v, v2, v3").unwrap();
    let (_, rows) = query_text(&mut session, "SHOW TABLES");
    assert_eq!(rows, [["s"], ["t"]]);
}

/// Writes through a view, which this tier refuses with Go's own messages.
#[test]
fn writes_through_a_view_are_refused() {
    let mut session = view_session();

    // Captured: "insert into view v is not supported now" -- a plain Go
    // error, so it carries no error class.
    assert!(matches!(
        session.run("INSERT INTO v VALUES (1, 2)"),
        Err(DriverError::InsertIntoViewUnsupported(ref name)) if name == "v"
    ));
    // Captured: [planner:1288]The target table v of the UPDATE is not
    // updatable.
    assert!(matches!(
        session.run("UPDATE v SET a = 1"),
        Err(DriverError::TableNotUpdatable(ref name)) if name == "v"
    ));
    // Captured: "delete view v is not supported now".
    assert!(matches!(
        session.run("DELETE FROM v"),
        Err(DriverError::DeleteViewUnsupported(ref name)) if name == "v"
    ));
    // None of the refusals touched the base table.
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM t")),
        vec![vec!["3".to_owned()]]
    );
}

/// A view whose base table is dropped: the definition survives, reading
/// it does not.
#[test]
fn a_view_over_a_dropped_table_is_invalid() {
    let mut session = Session::new();
    session.run("CREATE TABLE base (x BIGINT)").unwrap();
    session.run("CREATE VIEW vb AS SELECT x FROM base").unwrap();
    assert_eq!(
        row_text(session.run("SELECT * FROM vb")),
        Vec::<Vec<String>>::new()
    );

    session.run("DROP TABLE base").unwrap();
    // Captured: [planner:1356]View 'test.vb' references invalid table(s)
    // or column(s) or function(s) or definer/invoker of view lack rights
    // to use them.
    assert!(matches!(
        session.run("SELECT * FROM vb"),
        Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(ref name))) if name == "test.vb"
    ));
    // SHOW CREATE VIEW still answers from the stored definition.
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vb");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vb` (`x`) \
             AS SELECT `x` AS `x` FROM `test`.`base`"
    );
}

/// Where a view shows up in the metadata statements.
#[test]
fn views_appear_in_the_metadata_statements() {
    let mut session = view_session();

    // SHOW TABLES lists views beside tables, in one sorted list.
    let (names, rows) = query_text(&mut session, "SHOW TABLES");
    assert_eq!(names, ["Tables_in_test"]);
    assert_eq!(rows, [["s"], ["t"], ["v"], ["v2"], ["v3"]]);

    // SHOW FULL TABLES adds the kind.
    let (names, rows) = query_text(&mut session, "SHOW FULL TABLES");
    assert_eq!(names, ["Tables_in_test", "Table_type"]);
    assert_eq!(
        rows,
        [
            ["s", "BASE TABLE"],
            ["t", "BASE TABLE"],
            ["v", "VIEW"],
            ["v2", "VIEW"],
            ["v3", "VIEW"],
        ]
    );

    // information_schema.tables reports the same kinds.
    let (_, rows) = query_text(
        &mut session,
        "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = 'test' ORDER BY table_name",
    );
    assert_eq!(
        rows,
        [
            ["s", "BASE TABLE"],
            ["t", "BASE TABLE"],
            ["v", "VIEW"],
            ["v2", "VIEW"],
            ["v3", "VIEW"],
        ]
    );

    // information_schema.views: the captured header, and the stored
    // definition as VIEW_DEFINITION.
    let (names, rows) = query_text(
        &mut session,
        "SELECT * FROM information_schema.views WHERE table_schema = 'test'",
    );
    assert_eq!(
        names,
        [
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "VIEW_DEFINITION",
            "CHECK_OPTION",
            "IS_UPDATABLE",
            "DEFINER",
            "SECURITY_TYPE",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
        ]
    );
    assert_eq!(
        rows[0],
        [
            "def",
            "test",
            "v",
            "SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t`",
            "CASCADED",
            "NO",
            "@",
            "DEFINER",
            "utf8mb4",
            "utf8mb4_bin",
        ]
    );
    assert_eq!(
        rows[2][3],
        "SELECT `test`.`v`.`a` AS `a`,`test`.`v`.`b` AS `b` FROM `test`.`v` WHERE `b`>10"
    );

    // DESCRIBE reports the view's own columns, with no key, default or
    // extra -- captured from Go, where a view's columns carry none.
    let (names, rows) = query_text(&mut session, "DESCRIBE v");
    assert_eq!(names, ["Field", "Type", "Null", "Key", "Default", "Extra"]);
    assert_eq!(
        rows,
        [
            ["a", "bigint(20)", "YES", "", "<nil>", ""],
            ["b", "bigint(20)", "YES", "", "<nil>", ""],
        ]
    );
}

/// A view body that is a set operation, asserted against the captured
/// `SHOW CREATE VIEW` text and the rows the view reads.
#[test]
fn a_view_body_may_be_a_set_operation() {
    let mut session = view_session();

    session
        .run("CREATE VIEW vu AS SELECT a FROM t UNION SELECT a FROM s")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vu");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vu` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION SELECT `a` AS `a` FROM `test`.`s`"
    );
    let (names, rows) = query_text(&mut session, "SELECT * FROM vu ORDER BY a");
    assert_eq!(names, ["a"]);
    assert_eq!(rows, [["1"], ["2"], ["3"]]);

    // A statement-level ORDER BY belongs to the whole set operation and
    // is stored with it.
    session
        .run("CREATE VIEW vua AS SELECT a FROM t UNION ALL SELECT a FROM s ORDER BY 1")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vua");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vua` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION ALL SELECT `a` AS `a` FROM `test`.`s` \
             ORDER BY 1"
    );
    let (_, rows) = query_text(&mut session, "SELECT * FROM vua");
    assert_eq!(rows, [["1"], ["1"], ["2"], ["2"], ["3"]]);

    // A nested term keeps its parentheses, and a statement-level LIMIT
    // its place after the last term.
    session
        .run("CREATE VIEW vun AS SELECT a FROM t UNION (SELECT a FROM s UNION ALL SELECT a FROM s)")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vun");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vun` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION (SELECT `a` AS `a` FROM `test`.`s` \
             UNION ALL SELECT `a` AS `a` FROM `test`.`s`)"
    );
    session
        .run("CREATE VIEW vus AS SELECT a FROM t UNION SELECT a FROM s LIMIT 2")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vus");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vus` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION SELECT `a` AS `a` FROM `test`.`s` LIMIT 2"
    );

    // The explicit column list renames the set operation's output; the
    // body keeps the first term's own field names.
    session
        .run("CREATE VIEW vuc(z) AS SELECT a FROM t UNION SELECT a FROM s")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vuc");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vuc` (`z`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION SELECT `a` AS `a` FROM `test`.`s`"
    );
    let (names, _) = query_text(&mut session, "SELECT * FROM vuc");
    assert_eq!(names, ["z"]);

    // Captured: [planner:1222]The used SELECT statements have a different
    // number of columns.
    assert!(matches!(
        session.run("CREATE VIEW vubad AS SELECT a FROM t UNION SELECT a, c FROM s"),
        Err(DriverError::WrongNumberOfColumnsInSelect)
    ));
}

/// A view body containing a derived table, and the derived tables a plain
/// `SELECT` may write -- the same code path either way.
#[test]
fn a_view_body_may_contain_a_derived_table() {
    let mut session = view_session();

    session
        .run("CREATE VIEW vd AS SELECT * FROM (SELECT a, b FROM t WHERE b > 10) x")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vd");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vd` \
             (`a`, `b`) AS SELECT `x`.`a` AS `a`,`x`.`b` AS `b` FROM (SELECT `a` AS `a`,\
             `b` AS `b` FROM `test`.`t` WHERE `b`>10) AS `x`"
    );
    let (_, rows) = query_text(&mut session, "SELECT * FROM vd");
    assert_eq!(rows, [["2", "20"], ["3", "30"]]);

    // A derived table joined to a base table: the derived side is named
    // by its alias, the base side stays schema-qualified.
    session
        .run("CREATE VIEW vd2 AS SELECT x.a FROM (SELECT a FROM t) AS x JOIN s ON x.a = s.a")
        .unwrap();
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vd2");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vd2` (`a`) \
             AS SELECT `x`.`a` AS `a` FROM (SELECT `a` AS `a` FROM `test`.`t`) AS `x` \
             JOIN `test`.`s` ON `x`.`a`=`s`.`a`"
    );
    let (_, rows) = query_text(&mut session, "SELECT * FROM vd2");
    assert_eq!(rows, [["1"], ["2"]]);

    // Captured: [ddl:1248]Every derived table must have its own alias --
    // in a view body and in a plain SELECT alike.
    assert!(matches!(
        session.run("CREATE VIEW vnd AS SELECT * FROM (SELECT a FROM t)"),
        Err(DriverError::DerivedMustHaveAlias)
    ));
    assert!(matches!(
        session.run("SELECT * FROM (SELECT a FROM t)"),
        Err(DriverError::DerivedMustHaveAlias)
    ));

    // Captured: [planner:1060]Duplicate column name 'a' -- a derived
    // table is a named relation, so its columns must be unique.
    assert!(matches!(
        session.run("SELECT * FROM (SELECT * FROM t JOIN s ON t.a = s.a) q"),
        Err(DriverError::DuplicateColumnName(ref name)) if name == "a"
    ));

    // Plain derived tables: the alias is the only qualifier they answer
    // to, an expression field keeps its written name, and a set
    // operation may sit inside one.
    let (names, rows) = query_text(&mut session, "SELECT * FROM (SELECT a FROM t) x");
    assert_eq!(names, ["a"]);
    assert_eq!(rows, [["1"], ["2"], ["3"]]);
    let (_, rows) = query_text(
        &mut session,
        "SELECT x.a FROM (SELECT a, b FROM t) x WHERE x.b > 10",
    );
    assert_eq!(rows, [["2"], ["3"]]);
    let (_, rows) = query_text(&mut session, "SELECT * FROM (SELECT a + 1 FROM t) x");
    assert_eq!(rows, [["2"], ["3"], ["4"]]);
    let (_, rows) = query_text(
        &mut session,
        "SELECT * FROM (SELECT a FROM t UNION SELECT a FROM s) u ORDER BY a",
    );
    assert_eq!(rows, [["1"], ["2"], ["3"]]);
    // Captured: [planner:1054]Unknown column 't.a' in 'field list' -- the
    // subquery's own tables are not visible outside it.
    assert!(session.run("SELECT t.a FROM (SELECT a FROM t) x").is_err());
}

/// `WITH CHECK OPTION`: stored and reported, never printed, and never
/// reached -- writes through a view are refused before it would apply.
#[test]
fn a_view_check_option_is_stored_and_reported() {
    let mut session = view_session();
    session
        .run("CREATE VIEW vc AS SELECT a, b FROM t WHERE b > 10 WITH CHECK OPTION")
        .unwrap();
    session
        .run("CREATE VIEW vcl AS SELECT a, b FROM t WHERE b > 10 WITH LOCAL CHECK OPTION")
        .unwrap();
    session
        .run("CREATE VIEW vcc AS SELECT a, b FROM t WHERE b > 10 WITH CASCADED CHECK OPTION")
        .unwrap();

    // Captured: SHOW CREATE VIEW prints no check option at all, whichever
    // form was written.
    for view in ["vc", "vcl", "vcc"] {
        let (_, rows) = query_text(&mut session, &format!("SHOW CREATE VIEW {view}"));
        assert_eq!(
            rows[0][1],
            format!(
                "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `{view}` \
                     (`a`, `b`) AS SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t` WHERE `b`>10"
            )
        );
    }

    // information_schema.views is where it surfaces: LOCAL when written,
    // CASCADED otherwise -- including for a view with no check option at
    // all, which Go still records as CASCADED.
    let (_, rows) = query_text(
        &mut session,
        "SELECT table_name, check_option, is_updatable FROM information_schema.views \
             WHERE table_schema = 'test' AND table_name IN ('v', 'vc', 'vcl', 'vcc') \
             ORDER BY table_name",
    );
    assert_eq!(
        rows,
        [
            ["v", "CASCADED", "NO"],
            ["vc", "CASCADED", "NO"],
            ["vcc", "CASCADED", "NO"],
            ["vcl", "LOCAL", "NO"],
        ]
    );

    // The check would apply to a write, and a write never gets that far.
    assert!(matches!(
        session.run("INSERT INTO vc VALUES (4, 5)"),
        Err(DriverError::InsertIntoViewUnsupported(ref name)) if name == "vc"
    ));
}

/// `information_schema.columns` and `SHOW TABLE STATUS` for a view.
#[test]
fn a_view_reports_its_columns_and_status() {
    let mut session = view_session();

    // Captured: a view's columns carry no default, no key and no extra,
    // are nullable, and report the same PRIVILEGES string a base table's
    // columns do.
    let (_, rows) = query_text(
        &mut session,
        "SELECT table_name, column_name, ordinal_position, column_default, is_nullable, \
             data_type, character_maximum_length, numeric_precision, column_type, column_key, \
             extra, privileges FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 'v' ORDER BY ordinal_position",
    );
    assert_eq!(
        rows,
        [
            [
                "v",
                "a",
                "1",
                "<nil>",
                "YES",
                "bigint",
                "<nil>",
                "19",
                "bigint(20)",
                "",
                "",
                "select,insert,update,references",
            ],
            [
                "v",
                "b",
                "2",
                "<nil>",
                "YES",
                "bigint",
                "<nil>",
                "19",
                "bigint(20)",
                "",
                "",
                "select,insert,update,references",
            ],
        ]
    );

    // Captured: SHOW TABLE STATUS answers a view with NULLs and the
    // literal VIEW as its comment; a base table's row keeps its storage
    // metadata.
    let (names, rows) = query_text(&mut session, "SHOW TABLE STATUS LIKE 'v'");
    assert_eq!(names[0], "Name");
    assert_eq!(names[names.len() - 1], "Comment");
    assert_eq!(
        rows,
        [[
            "v", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>",
            "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "", "<nil>", "VIEW",
        ]]
    );
    let (_, rows) = query_text(&mut session, "SHOW TABLE STATUS LIKE 't'");
    assert_eq!(rows[0][1], "InnoDB");
    assert_eq!(rows[0][rows[0].len() - 1], "");
}

/// A view's column types are its base tables' types *now*, not the ones
/// they had at `CREATE VIEW`.
#[test]
fn a_view_column_type_follows_the_base_column() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE bt (x BIGINT, y VARCHAR(10))")
        .unwrap();
    session.run("INSERT INTO bt VALUES (1, 'aa')").unwrap();
    session
        .run("CREATE VIEW vb AS SELECT x, y FROM bt")
        .unwrap();

    let columns_query = "SELECT column_name, data_type, column_type \
                             FROM information_schema.columns \
                             WHERE table_schema = 'test' AND table_name = 'vb' \
                             ORDER BY ordinal_position";
    let (_, rows) = query_text(&mut session, "DESCRIBE vb");
    assert_eq!(rows[0][1], "bigint(20)");
    assert_eq!(rows[1][1], "varchar(10)");
    let (_, rows) = query_text(&mut session, columns_query);
    assert_eq!(
        rows,
        [
            ["x", "bigint", "bigint(20)"],
            ["y", "varchar", "varchar(10)"],
        ]
    );

    // Captured: altering the base columns shows through immediately, with
    // no touch to the view -- Go re-plans the body for every answer.
    session
        .run("ALTER TABLE bt MODIFY COLUMN y VARCHAR(64)")
        .unwrap();
    session
        .run("ALTER TABLE bt MODIFY COLUMN x VARCHAR(32)")
        .unwrap();
    let (_, rows) = query_text(&mut session, "DESCRIBE vb");
    assert_eq!(rows[0][1], "varchar(32)");
    assert_eq!(rows[1][1], "varchar(64)");
    let (_, rows) = query_text(&mut session, columns_query);
    assert_eq!(
        rows,
        [
            ["x", "varchar", "varchar(32)"],
            ["y", "varchar", "varchar(64)"],
        ]
    );
    let (_, rows) = query_text(&mut session, "SELECT * FROM vb");
    assert_eq!(rows, [["1", "aa"]]);
    // The stored definition never changed.
    let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vb");
    assert_eq!(
        rows[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vb` (`x`, `y`) \
             AS SELECT `x` AS `x`,`y` AS `y` FROM `test`.`bt`"
    );

    // Dropping a base column breaks the view: the read is ErrViewInvalid,
    // DESCRIBE fails with the body's own error, and the view drops out of
    // information_schema.columns entirely -- all captured.
    session.run("ALTER TABLE bt DROP COLUMN y").unwrap();
    assert!(matches!(
        session.run("SELECT * FROM vb"),
        Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(ref name))) if name == "test.vb"
    ));
    assert!(session.run("DESCRIBE vb").is_err());
    let (_, rows) = query_text(&mut session, columns_query);
    assert_eq!(rows, Vec::<Vec<String>>::new());
    // information_schema.views still answers from the stored definition.
    let (_, rows) = query_text(
        &mut session,
        "SELECT view_definition FROM information_schema.views \
             WHERE table_schema = 'test' AND table_name = 'vb'",
    );
    assert_eq!(rows, [["SELECT `x` AS `x`,`y` AS `y` FROM `test`.`bt`"]]);
}
