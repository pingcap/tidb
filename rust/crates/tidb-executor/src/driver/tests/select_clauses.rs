//! The clauses a `SELECT` carries, over constants and over a table: the
//! expression list, `WHERE`, `ORDER BY` and `LIMIT`.
//!
//! These hold the plumbing still -- that each clause reaches its executor and
//! composes with the others -- rather than any one operator's semantics. Go's
//! counterpart surface is `pkg/executor`'s `LimitExec`/`SortExec` wiring.

use super::*;

#[test]
fn select_constant_arithmetic() {
    assert_eq!(
        run_select("SELECT 1 + 1").unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select("SELECT 1 + 1, 2 * 3").unwrap(),
        vec![vec![Datum::Int(2), Datum::Int(6)]]
    );
    assert_eq!(
        run_select("SELECT 2 * 3 - 1").unwrap(),
        vec![vec![Datum::Int(5)]]
    );
}

#[test]
fn select_with_where() {
    // A true predicate keeps the row.
    assert_eq!(
        run_select("SELECT 42 WHERE 1 = 1").unwrap(),
        vec![vec![Datum::Int(42)]]
    );
    // A false predicate yields no rows.
    assert_eq!(
        run_select("SELECT 42 WHERE 1 = 0").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
}

#[test]
fn an_explicit_case_insensitive_collation_controls_comparison() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE collated (a VARCHAR(10) COLLATE utf8mb4_bin) \
         PARTITION BY RANGE COLUMNS(a) (\
           PARTITION p_upper VALUES LESS THAN ('a'), \
           PARTITION p_lower VALUES LESS THAN (MAXVALUE))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO collated VALUES ('AA'), ('aa'), ('AAA'), ('aaa')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let values = |sql: &str| {
        let mut values: Vec<String> = run_select_on(sql, &catalog, &ctx)
            .unwrap()
            .iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect();
        values.sort();
        values
    };
    assert_eq!(
        values("SELECT a FROM collated WHERE a = 'AA' COLLATE utf8mb4_general_ci"),
        ["AA", "aa"],
    );
    assert_eq!(
        values("SELECT a FROM collated WHERE a IN ('AAA' COLLATE utf8mb4_general_ci, 'aa')",),
        ["AA", "AAA", "aa", "aaa"],
    );
}

/// Row-valued `IN` is the predicate shape go-tpc uses to lock stock rows.
/// Go lowers each tuple equality column by column, preserving SQL's
/// three-valued NULL behavior, then joins the candidates with `OR`.
#[test]
fn row_in_matches_go_tuple_and_null_semantics() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE stock (id INT PRIMARY KEY, w_id INT, i_id INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO stock VALUES \
         (1, 1, 1), (2, 2, 2), (3, 1, 3), (4, 1, NULL), \
         (5, 2, NULL), (6, NULL, 2), (7, NULL, NULL), (8, 3, NULL)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT id FROM stock \
             WHERE (w_id, i_id) IN ((1, 1), (2, 2)) \
             ORDER BY id FOR UPDATE",
            &catalog,
            &ctx,
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]],
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM stock \
             WHERE (w_id, i_id) NOT IN ((1, 1), (2, 2)) \
             ORDER BY id",
            &catalog,
            &ctx,
        )
        .unwrap(),
        vec![vec![Datum::Int(3)], vec![Datum::Int(8)]],
    );
}

#[test]
fn row_in_rejects_a_different_column_count_like_go() {
    let error = run_select("SELECT (1, 2) IN ((1, 2, 3))")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.state, error.message.as_str()),
        (1241, *b"21000", "Operand should contain 2 column(s)",),
    );
}

#[test]
fn limit_and_order_by_wire_up() {
    // LIMIT truncates / zeroes the single row.
    assert_eq!(
        run_select("SELECT 42 LIMIT 1").unwrap(),
        vec![vec![Datum::Int(42)]]
    );
    assert_eq!(
        run_select("SELECT 42 LIMIT 0").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select("SELECT 42 LIMIT 1, 1").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // ORDER BY over the single dual row passes through the sort.
    assert_eq!(
        run_select("SELECT 42 ORDER BY 1 DESC").unwrap(),
        vec![vec![Datum::Int(42)]]
    );
}

#[test]
fn select_from_table_order_limit() {
    let catalog = test_catalog();
    // ORDER BY a column that is not projected (sort runs below projection).
    assert_eq!(
        run_select_on(
            "SELECT a FROM t ORDER BY b",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(3)],
            vec![Datum::Int(2)],
            vec![Datum::Int(1)]
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM t ORDER BY b DESC LIMIT 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
}

/// `HAVING` on the NON-aggregate path: the clause is a filter, and it may name
/// only what the select list projects.
///
/// Go builds it as a `LogicalSelection` above the select list's `Projection`
/// (`buildSelect`), and `resolveHavingAndOrderBy` resolves every name against
/// the select FIELDS first -- with no `GROUP BY` items to match,
/// `resolveFieldsFirst` stays true and the `havingClause` branch of
/// `resolveFromPlan` returns `-1` for every name the select list lacks.
///
/// Before this, the driver DROPPED the clause on this path: the rows below
/// came back unfiltered and the 1054s came back as rows.
///
/// Captured from real TiDB on `ht(a, b)` = (1,10),(2,20):
///
/// ```text
/// select a, b from ht having b > 15    -- 2|20
/// select a from ht having a > 1        -- 2
/// select a from ht having b > 0        -- [planner:1054] Unknown column 'b' in 'having clause'
/// select a from ht having ht.b > 0     -- [planner:1054] Unknown column 'ht.b' in 'having clause'
/// select a from ht having b is null    -- [planner:1054] Unknown column 'b' in 'having clause'
/// select a from ht having b > 0 and a > 0
///                                      -- [planner:1054] Unknown column 'b' in 'having clause'
/// select b as a from ht having a > 15  -- 20   (the ALIAS wins over ht.a)
/// select b as a from ht having ht.a>1  -- [planner:1054] Unknown column 'ht.a'
/// select a+1 as a from ht having a > 2 -- 3
/// select a as z, b from ht having z>1  -- 2|20
/// select 1 as one from ht having one=1 -- 1;1
/// select a from ht t1 having t1.a > 1  -- 2
/// select a from ht t1 having ht.a > 1  -- [planner:1054] Unknown column 'ht.a'
/// select * from ht having b > 15       -- 2|20
/// select a from ht having 1            -- 1;2
/// select a from ht having 0            -- (no rows)
/// select a from ht having null         -- (no rows)
/// select a, b from ht having b>15 limit 1        -- 2|20
/// select a, b from ht having a>0 order by b desc -- 2|20 1|10
/// ```
#[test]
fn plain_having_filters_and_sees_only_the_select_list() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE ht (a INT, b INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO ht VALUES (1, 10), (2, 20)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let run = |sql: &str| run_select_on(sql, &catalog, &crate::StmtContext::for_query());

    // The clause FILTERS -- this is the whole point, and every row-set below
    // was two rows before.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING b > 15").unwrap(),
        vec![vec![Datum::Int(2), Datum::Int(20)]]
    );
    assert_eq!(
        run("SELECT a FROM ht HAVING a > 1").unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    assert_eq!(
        run("SELECT * FROM ht HAVING b > 15").unwrap(),
        vec![vec![Datum::Int(2), Datum::Int(20)]]
    );
    assert_eq!(
        run("SELECT a FROM ht HAVING 1").unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    assert_eq!(
        run("SELECT a FROM ht HAVING 0").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run("SELECT a FROM ht HAVING NULL").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // ... below the LIMIT and below the sort, which is Go's
    // `Selection -> Sort -> Limit`.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING b > 15 LIMIT 1").unwrap(),
        vec![vec![Datum::Int(2), Datum::Int(20)]]
    );
    assert_eq!(
        run("SELECT a, b FROM ht HAVING a > 0 ORDER BY b DESC").unwrap(),
        vec![
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(1), Datum::Int(10)]
        ]
    );

    // An ALIAS shadows a real column of the same name, and an aliased
    // expression is reachable by its alias.
    assert_eq!(
        run("SELECT b AS a FROM ht HAVING a > 15").unwrap(),
        vec![vec![Datum::Int(20)]]
    );
    assert_eq!(
        run("SELECT a+1 AS a FROM ht HAVING a > 2").unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    assert_eq!(
        run("SELECT a AS z, b FROM ht HAVING z > 1").unwrap(),
        vec![vec![Datum::Int(2), Datum::Int(20)]]
    );
    assert_eq!(
        run("SELECT 1 AS one FROM ht HAVING one = 1").unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(1)]]
    );
    // A FROM alias qualifies the name; the base table's own name no longer does.
    assert_eq!(
        run("SELECT a FROM ht t1 HAVING t1.a > 1").unwrap(),
        vec![vec![Datum::Int(2)]]
    );

    // ... and a source column the select list does NOT project is 1054, in
    // every spelling and every sql_mode.
    for (sql, name) in [
        ("SELECT a FROM ht HAVING b > 0", "b"),
        ("SELECT a FROM ht HAVING ht.b > 0", "ht.b"),
        ("SELECT a FROM ht HAVING b IS NULL", "b"),
        ("SELECT a FROM ht HAVING b > 0 AND a > 0", "b"),
        ("SELECT b AS a FROM ht HAVING ht.a > 1", "ht.a"),
        ("SELECT a FROM ht t1 HAVING ht.a > 1", "ht.a"),
    ] {
        match run(sql) {
            Err(DriverError::UnknownColumnInClause { column, clause }) => assert_eq!(
                (column.as_str(), clause.as_str()),
                (name, "having clause"),
                "{sql}"
            ),
            other => panic!("expected 1054 for `{sql}`, got {other:?}"),
        }
    }
}

/// A correlated subquery in a NON-aggregate `HAVING`: an EMPTY one is NULL,
/// so the group it belongs to is dropped -- and the names it correlates to
/// answer to the PROJECTION, not to the source row.
///
/// This is the `HAVING` site of the family #290's apply-deselect fixed in the
/// select list. Before this the clause was dropped entirely and the first
/// query below answered BOTH rows.
///
/// The projection's names are what Go's `FieldName` carries: a field written
/// `b AS bb` has `ColName = bb` and `OrigTblName = ht`, which is why `bb`
/// resolves and `ht.b` does not -- while the select-FIELD rule one function up
/// still matches `ht.b` by the field's written name. Both captured.
///
/// Captured from real TiDB on `ht(a, b)` = (1,10),(2,20) and `hs(x, y)` = (10,5):
///
/// ```text
/// select a, b from ht having (select y from hs where hs.x = ht.b) > 0     -- 1|10
/// select a, b from ht having (select y from hs where hs.x = ht.b) = 5     -- 1|10
/// select a, b from ht having (select y from hs where hs.x = ht.b) <> 5    -- (no rows)
/// select a, b from ht having (select y from hs where hs.x = ht.b) > 100   -- (no rows)
/// select a, b from ht having (select y from hs where hs.x = ht.b) is null -- 2|20
/// select a, b from ht having (select y from hs where hs.x = ht.b) is not null -- 1|10
/// select a, b from ht having (select count(*) from hs where hs.x = ht.b) > 0  -- 1|10
/// select a, b from ht having exists (select 1 from hs where hs.x = ht.b)      -- 1|10
/// select a, b from ht having not exists (select 1 from hs where hs.x = ht.b)  -- 2|20
/// select a, b from ht having ht.b in (select x from hs)                       -- 1|10
/// select a, b from ht having (select y from hs where hs.x = b) > 0            -- 1|10
/// select a, b from ht having (select y from hs where hs.x = ht.a) > 0    -- (no rows)
/// select a from ht having (select count(*) from hs) > 0                  -- 1;2
/// select b as bb from ht having (select y from hs where hs.x = bb) > 0    -- 10
/// select a from ht having (select y from hs where hs.x = ht.b) > 0
///   -- [planner:1054] Unknown column 'ht.b' in 'having clause'
/// select a from ht having (select y from hs where hs.x = b) > 0
///   -- [planner:1054] Unknown column 'b' in 'having clause'
/// select a from ht having exists (select 1 from hs where hs.x = ht.b)
///   -- [planner:1054] Unknown column 'ht.b' in 'having clause'
/// select b as bb from ht having (select y from hs where hs.x = ht.b) > 0
///   -- [planner:1054] Unknown column 'ht.b' in 'having clause'
/// ```
#[test]
fn an_empty_correlated_having_subquery_is_null_and_drops_its_row() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE ht (a INT, b INT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE hs (x INT, y INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO ht VALUES (1, 10), (2, 20)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hs VALUES (10, 5)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let run = |sql: &str| run_select_on(sql, &catalog, &crate::StmtContext::for_query());
    let first = vec![vec![Datum::Int(1), Datum::Int(10)]];
    let second = vec![vec![Datum::Int(2), Datum::Int(20)]];
    let none = Vec::<Vec<Datum>>::new();

    // `ht.b = 20` matches no `hs` row, so the subquery is NULL and `NULL > 0`
    // is not true. The row is DROPPED -- it was kept before.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0").unwrap(),
        first
    );
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) = 5").unwrap(),
        first
    );
    // ... and NULL is not FALSE either: `<> 5` drops both rows, the non-empty
    // one because 5 <> 5 is false and the empty one because NULL is unknown.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) <> 5").unwrap(),
        none
    );
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 100").unwrap(),
        none
    );
    // IS NULL / IS NOT NULL name the same value from the other side.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) IS NULL").unwrap(),
        second
    );
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) IS NOT NULL").unwrap(),
        first
    );
    // An aggregate INSIDE the subquery counts rows rather than yielding NULL,
    // so the empty side is `0` and still fails `> 0`.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT count(*) FROM hs WHERE hs.x = ht.b) > 0").unwrap(),
        first
    );
    // EXISTS / NOT EXISTS / IN partition the same two rows.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)").unwrap(),
        first
    );
    assert_eq!(
        run("SELECT a, b FROM ht HAVING NOT EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)").unwrap(),
        second
    );
    assert_eq!(
        run("SELECT a, b FROM ht HAVING ht.b IN (SELECT x FROM hs)").unwrap(),
        first
    );
    // The correlation may be written unqualified, and may name a column that
    // matches nothing at all.
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = b) > 0").unwrap(),
        first
    );
    assert_eq!(
        run("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.a) > 0").unwrap(),
        none
    );
    // An UNcorrelated subquery is a constant and keeps every row.
    assert_eq!(
        run("SELECT a FROM ht HAVING (SELECT count(*) FROM hs) > 0").unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // The correlation resolves against the PROJECTION, so the underlying
    // column's name is NOT what it answers to.
    //
    // DEFERRED, the other side of that same rule: a correlation to the ALIAS
    // (`hs.x = bb`, which TiDB answers `10`) is refused rather than answered
    // -- see `bind_having_correlations`. It is an error, not a wrong row set.
    assert!(run("SELECT b AS bb FROM ht HAVING (SELECT y FROM hs WHERE hs.x = bb) > 0").is_err());
    for (sql, name) in [
        (
            "SELECT a FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
            "ht.b",
        ),
        (
            "SELECT a FROM ht HAVING (SELECT y FROM hs WHERE hs.x = b) > 0",
            "b",
        ),
        (
            "SELECT a FROM ht HAVING EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)",
            "ht.b",
        ),
        (
            "SELECT b AS bb FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
            "ht.b",
        ),
    ] {
        match run(sql) {
            Err(DriverError::UnknownColumnInClause { column, clause }) => assert_eq!(
                (column.as_str(), clause.as_str()),
                (name, "having clause"),
                "{sql}"
            ),
            other => panic!("expected 1054 for `{sql}`, got {other:?}"),
        }
    }
}

/// Go's `findBestTask4LogicalDataSource` routes `ds.SampleInfo != nil` to
/// `convertToSampleTable`; it never treats the clause as an ordinary scan.
/// Rust parses the same syntax but has no TiKV region-sampling model, so the
/// honest boundary is a refusal. The control query proves the table remains a
/// normal readable table and that the error belongs to `TABLESAMPLE` itself.
#[test]
fn a_table_sample_clause_is_refused_rather_than_answered_in_full() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE smp (a BIGINT PRIMARY KEY)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO smp VALUES (1), (2), (3)", &mut catalog, &ctx).unwrap();

    for sql in [
        "SELECT a FROM smp TABLESAMPLE REGIONS()",
        "SELECT a FROM smp TABLESAMPLE BERNOULLI (10 PERCENT)",
        "SELECT a FROM smp TABLESAMPLE SYSTEM (2 ROWS) REPEATABLE(7)",
    ] {
        assert!(
            matches!(
                run_select_on(sql, &catalog, &ctx),
                Err(DriverError::Unsupported(_))
            ),
            "{sql} must refuse rather than answer the whole table",
        );
    }

    assert_eq!(
        run_select_on("SELECT a FROM smp", &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ],
    );
}

/// A name no schema resolves answers Go's `ErrBadField` with the RESOLVING
/// clause's name (`clauseMsg`, `planbuilder.go:132`) — probe round 29 caught
/// the field-list and where-clause paths answering a generic 1105
/// "unresolved column reference" instead.
///
/// Captured from real TiDB on `uc(a)`:
///
/// ```text
/// select no_col from uc          -- [planner:1054] Unknown column 'no_col' in 'field list'
/// select a from uc where nc = 1  -- [planner:1054] Unknown column 'nc' in 'where clause'
/// select uc.nc from uc           -- [planner:1054] Unknown column 'uc.nc' in 'field list'
/// ```
#[test]
fn an_unknown_column_names_its_clause() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE uc (a INT)", &mut catalog).unwrap();
    let run = |sql: &str| run_select_on(sql, &catalog, &crate::StmtContext::for_query());

    for (sql, column, clause) in [
        ("SELECT no_col FROM uc", "no_col", "field list"),
        ("SELECT a FROM uc WHERE nc = 1", "nc", "where clause"),
        ("SELECT uc.nc FROM uc", "uc.nc", "field list"),
    ] {
        let wire = run(sql).unwrap_err().to_mysql_error();
        assert_eq!(wire.code, 1054, "{sql}: {}", wire.message);
        assert_eq!(
            wire.message,
            format!("Unknown column '{column}' in '{clause}'"),
            "{sql}"
        );
    }
}
