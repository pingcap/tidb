//! Aggregate queries: the aggregate functions, `GROUP BY`, `HAVING`,
//! aggregate `ORDER BY`, and `SELECT DISTINCT`.
//!
//! Mirrors Go `pkg/executor/aggregate`'s hash-aggregate surface, including
//! the distinct path a `DISTINCT` select takes through the same operator.

use super::*;

#[test]
fn aggregate_selects() {
    let catalog = test_catalog();
    // Global aggregates: rows (1,30),(2,20),(3,10).
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*), SUM(a) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        // SUM is a DECIMAL in MySQL even over a BIGINT column.
        vec![vec![
            Datum::Int(3),
            Datum::Decimal(tidb_datatype::Decimal::from_int(6))
        ]]
    );
    // GROUP BY with a carried key column, WHERE below the agg.
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(1)],
        ]
    );
    // Empty-input rules through SQL: global agg over no rows -> one row.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(a) FROM t WHERE a > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(0)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM t WHERE a > 100 GROUP BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // MIN/MAX over the shared datum ordering.
    assert_eq!(
        run_select_on(
            "SELECT MIN(a), MAX(b) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1), Datum::Int(30)]]
    );
    // AVG over integers is DECIMAL, scaled by div_precision_increment.
    assert_eq!(
        run_select_on(
            "SELECT AVG(a) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
            "2.0000"
        ))]]
    );
    // DISTINCT folds repeated inputs once per group: a is 1,2,3 while the
    // constant 1 collapses to a single counted value.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(DISTINCT a), COUNT(DISTINCT 1) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3), Datum::Int(1)]]
    );
    // An all-NULL / empty group is NULL for MIN/MAX and AVG, as in Go.
    assert_eq!(
        run_select_on(
            "SELECT MIN(a), MAX(a), AVG(a) FROM t WHERE a > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Null, Datum::Null, Datum::Null]]
    );
}

#[test]
fn float_sum_and_avg_use_the_real_domain() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE f (v FLOAT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO f VALUES (1.25), (2.5), (NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT SUM(v), AVG(v) FROM f",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![Datum::Real(3.75), Datum::Real(1.875)]],
    );
}

/// HAVING filters aggregate output rows, ORDER BY sorts them, and an
/// aggregate that appears only in those clauses is computed as a hidden
/// column and trimmed from the result (Go's resolveHavingAndOrderBy plus
/// the final projection).
#[test]
fn aggregate_having_and_order_by() {
    let mut catalog = test_catalog();
    crate::run_create_table_on("CREATE TABLE g (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // HAVING over an aggregate that IS in the select list.
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(2)],
        ]
    );
    // HAVING over an aggregate that is NOT selected: one output column.
    assert_eq!(
        run_select_on(
            "SELECT a FROM g GROUP BY a HAVING SUM(b) > 15",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // ORDER BY an aggregate that is not selected, descending.
    assert_eq!(
        run_select_on(
            "SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(3)],
            vec![Datum::Int(2)]
        ]
    );
    // HAVING and ORDER BY together, with LIMIT applied after both.
    assert_eq!(
        run_select_on(
            "SELECT a, SUM(b) FROM g GROUP BY a HAVING COUNT(*) > 1 ORDER BY SUM(b) LIMIT 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![
            Datum::Int(3),
            Datum::Decimal(tidb_datatype::Decimal::from_int(15))
        ]]
    );
    // ORDER BY a selected alias.
    assert_eq!(
        run_select_on(
            "SELECT a, SUM(b) AS total FROM g GROUP BY a ORDER BY total",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(5))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(15))
            ],
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(30))
            ],
        ]
    );
    // A grouped column that is not selected is still visible to HAVING
    // and ORDER BY (Go carries it as a hidden FIRST_ROW column).
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // A global aggregate's HAVING filters the single group.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM g HAVING COUNT(*) > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
}

/// SELECT DISTINCT deduplicates the projected rows, which Go builds as an
/// aggregation grouping by every projected column with FIRST_ROW
/// aggregates. The plain path silently returned duplicates before.
#[test]
fn select_distinct() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE d2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO d2 VALUES (1, 1), (1, 2), (1, 1), (2, 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // Every projected column takes part, so (1,1) collapses but (1,2)
    // stays.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a, b FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2)],
        ]
    );
    // Without DISTINCT every row survives.
    assert_eq!(
        run_select_on(
            "SELECT a FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        4
    );

    // DISTINCT applies to the projected expression, not the source rows.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a + b FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(2)],
            vec![Datum::Int(3)],
            vec![Datum::Int(4)]
        ]
    );

    // The dedup emits groups in first-seen order, so a sort below it still
    // orders the surviving rows.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 ORDER BY a DESC",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(1)]]
    );
    // LIMIT applies after the dedup.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 LIMIT 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // A WHERE below it still filters.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 WHERE b = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // Over an aggregate result, DISTINCT deduplicates the output rows.
    crate::run_create_table_on("CREATE TABLE g3 (k BIGINT, v BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g3 VALUES (1, 5), (2, 5), (3, 9)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT SUM(v) FROM g3 GROUP BY k",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(5))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(9))],
        ]
    );
}

/// `BIT_AND`/`BIT_OR`/`BIT_XOR` return BIGINT **UNSIGNED**.
///
/// Go `aggregation/base_func.go`'s `typeInfer4BitFuncs` adds
/// `mysql.UnsignedFlag`, and `func_bitfuncs.go` appends the fold with
/// `AppendUint64`. Captured from TiDB over `v BIGINT`:
///
/// ```text
/// values (null)      -> 18446744073709551615 | 0                    | 0
/// values (-1),(-1)   -> 18446744073709551615 | 18446744073709551615 | 0
/// values (3),(5),(null) -> 1 | 7 | 6
/// desc of a view over them -> bigint(21) unsigned NO
/// ```
///
/// The all-NULL and all-`-1` rows are the ones that separate the signed
/// reading from Go's: as a signed BIGINT they would print `-1`.
#[test]
fn bit_aggregates_are_unsigned() {
    use tidb_datatype::FieldTypeCode;

    fn catalog_with(rows: Vec<Vec<Datum>>) -> Catalog {
        let mut catalog = Catalog::default();
        catalog.register(
            "b",
            MemTable {
                columns: vec![("v".to_owned(), FieldType::new(FieldTypeCode::LongLong))],
                rows,
            },
        );
        catalog
    }

    // `Datum`'s equality is NUMERIC across the integer kinds, so `Int(-1)`
    // and `UInt(u64::MAX)` compare equal. The whole finding is about which
    // KIND the fold lands in, so the assertion has to read the variant.
    fn unsigned(value: &Datum) -> u64 {
        match value {
            Datum::UInt(bits) => *bits,
            other => panic!("expected an unsigned datum, got {other:?}"),
        }
    }

    let query = "SELECT BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM b";
    let cases: Vec<(Vec<Vec<Datum>>, [u64; 3])> = vec![
        (vec![vec![Datum::Null]], [u64::MAX, 0, 0]),
        (
            vec![vec![Datum::Int(-1)], vec![Datum::Int(-1)]],
            [u64::MAX, u64::MAX, 0],
        ),
        (
            vec![vec![Datum::Int(3)], vec![Datum::Int(5)], vec![Datum::Null]],
            [1, 7, 6],
        ),
    ];
    for (rows, expected) in cases {
        let out =
            run_select_on(query, &catalog_with(rows), &crate::StmtContext::for_query()).unwrap();
        assert_eq!(out.len(), 1);
        let folds: Vec<u64> = out[0].iter().map(unsigned).collect();
        assert_eq!(folds, expected.to_vec());
    }

    // The inferred column type carries `UnsignedFlag` too, which is what
    // makes a view over one describe as `bigint(21) unsigned NO`.
    for name in ["BIT_AND", "BIT_OR", "BIT_XOR"] {
        let (_, field_type) = crate::driver::agg_build::agg_kind_and_type(name, &[]).unwrap();
        assert_ne!(
            field_type.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED,
            0,
            "{name} must infer an UNSIGNED column"
        );
    }
}

/// Aggregates read the COLLATION of what they aggregate, not raw bytes.
///
/// Go builds a `collate.GetCollator(RetTp.GetCollate())` per aggregate
/// (`aggfuncs/builder.go:460-468` for MIN/MAX, one per `byItem` for
/// `GROUP_CONCAT`'s own ORDER BY) and keys the DISTINCT value set with the
/// same collator. Captured from TiDB on a `utf8mb4_general_ci` column:
///
/// ```text
/// values ('a'),('B'),('A')
///   max(s), min(s)                                  -> B | a
///   count(distinct s), (distinct concat(s,'')),
///                      (distinct upper(s))          -> 2 | 2 | 2
/// values ('B'),('a')
///   group_concat(s order by s), (... order by s desc) -> a,B | B,a
/// values ('b'),('A'),('a'),('B')
///   group_concat(distinct s)                        -> b,A
/// ```
///
/// The ORDER BY probe deliberately uses a pair with NO collation tie:
/// `'B','a'` sorts `a,B` under the collation and `B,a` under bytes. Go
/// resolves ties with an unstable `sort.Sort` over its top-N heap
/// (`func_group_concat.go:470`), so the relative order of two
/// collation-equal values is not a behavior to pin.
///
/// The two COMPUTED-argument DISTINCT counts are the ones a bare column
/// cannot catch: a column datum carries its own collation, but a string
/// builtin mints its result with the default `utf8mb4_bin`, so the key has
/// to come from the argument EXPRESSION's derived collation.
#[test]
fn aggregates_read_the_arguments_collation() {
    fn catalog_with(values: &[&str]) -> Catalog {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE g (s VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci)",
            &mut catalog,
        )
        .unwrap();
        let list = values
            .iter()
            .map(|value| format!("('{value}')"))
            .collect::<Vec<_>>()
            .join(", ");
        run_insert_on(
            &format!("INSERT INTO g VALUES {list}"),
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        catalog
    }

    let catalog = catalog_with(&["a", "B", "A"]);
    let row = |sql: &str| {
        run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .remove(0)
    };

    // MIN/MAX under the case-insensitive collation: the binary answers would
    // be max='a', min='A'.
    assert_eq!(
        row("SELECT MAX(s), MIN(s) FROM g")
            .iter()
            .map(datum_text_for_test)
            .collect::<Vec<_>>(),
        vec!["B".to_owned(), "a".to_owned()]
    );
    // DISTINCT over a bare column AND over two computed string expressions.
    assert_eq!(
        row("SELECT COUNT(DISTINCT s), COUNT(DISTINCT CONCAT(s, '')), COUNT(DISTINCT UPPER(s)) FROM g"),
        vec![Datum::Int(2), Datum::Int(2), Datum::Int(2)]
    );

    // GROUP_CONCAT's own ORDER BY sorts under the byItem's collation: over
    // 'B','a' the byte order is the REVERSE of the collation order.
    let catalog = catalog_with(&["B", "a"]);
    let row = |sql: &str| {
        run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .remove(0)
    };
    assert_eq!(
        datum_text_for_test(&row("SELECT GROUP_CONCAT(s ORDER BY s) FROM g")[0]),
        "a,B"
    );
    assert_eq!(
        datum_text_for_test(&row("SELECT GROUP_CONCAT(s ORDER BY s DESC) FROM g")[0]),
        "B,a"
    );

    let catalog = catalog_with(&["b", "A", "a", "B"]);
    let row = |sql: &str| {
        run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .remove(0)
    };
    assert_eq!(
        datum_text_for_test(&row("SELECT GROUP_CONCAT(DISTINCT s) FROM g")[0]),
        "b,A"
    );
}
