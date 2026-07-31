//! Which predicates a table scan hands to the storage layer, and which it
//! keeps for itself.
//!
//! The point of these tests is the BOUNDARY: a conjunct TiKV can evaluate is
//! pushed, anything else stays above the scan, and the answer is identical
//! either way. Also covers the unknown-table rejection that happens before
//! any scan is built. Mirrors Go `pkg/planner/core`'s predicate pushdown into
//! `pkg/executor`'s table reader.

use super::*;

#[test]
fn unknown_table_is_rejected() {
    assert!(matches!(
        run_select("SELECT a FROM missing"),
        Err(DriverError::Unsupported(_))
    ));
}

/// The split rule itself, at the shape boundary: a column-versus-constant
/// comparison moves into the scan and everything else stays above it.
#[test]
fn the_scan_takes_comparisons_against_constants_and_nothing_else() {
    use tidb_datatype::FieldTypeCode;
    let scope = FromScope {
        tables: vec![FromTable {
            name: "t".to_owned(),
            database: None,
            columns: vec![
                ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ],
            offset: 0,
            func_deps: Default::default(),
        }],
        ..FromScope::default()
    };
    let split = |sql: &str| {
        let stmt = tidb_parser::parse(sql).expect("a select");
        let Stmt::Query(query) = &stmt else {
            panic!("a select");
        };
        let QueryStmt::Select(statement) = &**query else {
            panic!("a select");
        };
        let where_clause = statement.where_clause.clone().expect("a where clause");
        let (pushed, residual) =
            split_scan_predicates(&where_clause, &ScopeResolver { scope: &scope });
        (
            pushed
                .predicates()
                .iter()
                .map(|predicate| match predicate {
                    ScanPredicate::Compare(c) => {
                        (c.column_offset, c.op, c.literal.clone(), c.column_on_left)
                    }
                    other => panic!("only comparisons are described here: {other:?}"),
                })
                .collect::<Vec<_>>(),
            residual.map(|expr| expr.restore()),
        )
    };

    // Either operand order pushes, and the order is preserved.
    assert_eq!(
        split("SELECT 1 FROM t WHERE a > 5"),
        (vec![(0, ScanComparisonOp::Gt, Datum::Int(5), true)], None)
    );
    assert_eq!(
        split("SELECT 1 FROM t WHERE 5 < a"),
        (vec![(0, ScanComparisonOp::Lt, Datum::Int(5), false)], None)
    );
    // A qualified name resolves to the same column.
    assert_eq!(
        split("SELECT 1 FROM t WHERE t.b = 1").0,
        vec![(1, ScanComparisonOp::Eq, Datum::Int(1), true)]
    );
    // Mixed: the comparison pushes, the arithmetic does not.
    let (pushed, residual) = split("SELECT 1 FROM t WHERE a > 5 AND b + 1 < 10");
    assert_eq!(pushed, vec![(0, ScanComparisonOp::Gt, Datum::Int(5), true)]);
    assert!(residual.is_some(), "the arithmetic conjunct stays above");
    // Shapes that push nothing: a column-to-column comparison, a NULL
    // constant, `IS TRUE` (a different Go function with its own NULL rule),
    // NULL-safe equality (a different signature), an empty or NULL-carrying
    // `IN` list, and a disjunction one branch of which is not describable.
    for sql in [
        "SELECT 1 FROM t WHERE a > b",
        "SELECT 1 FROM t WHERE a = NULL",
        "SELECT 1 FROM t WHERE a IS TRUE",
        "SELECT 1 FROM t WHERE a <=> 5",
        "SELECT 1 FROM t WHERE a IN (1, NULL)",
        "SELECT 1 FROM t WHERE a > 5 OR b + 1 < 10",
        "SELECT 1 FROM t WHERE NOT (a > b)",
    ] {
        let (pushed, residual) = split(sql);
        assert!(pushed.is_empty(), "{sql} must not push");
        assert!(residual.is_some(), "{sql} keeps its whole predicate");
    }
}

/// The composed shapes the coprocessor's whitelist admits: `OR`, `NOT`,
/// `IS [NOT] NULL` and `[NOT] IN`. Each becomes one described conjunct and
/// leaves no residual, because the whole conjunct moved into the scan.
#[test]
fn the_scan_takes_the_composed_predicates_tikv_evaluates() {
    use tidb_datatype::FieldTypeCode;
    let scope = FromScope {
        tables: vec![FromTable {
            name: "t".to_owned(),
            database: None,
            columns: vec![
                ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ],
            offset: 0,
            func_deps: Default::default(),
        }],
        ..FromScope::default()
    };
    let split = |sql: &str| {
        let stmt = tidb_parser::parse(sql).expect("a select");
        let Stmt::Query(query) = &stmt else {
            panic!("a select");
        };
        let QueryStmt::Select(statement) = &**query else {
            panic!("a select");
        };
        let where_clause = statement.where_clause.clone().expect("a where clause");
        let (pushed, residual) =
            split_scan_predicates(&where_clause, &ScopeResolver { scope: &scope });
        (pushed.predicates().to_vec(), residual)
    };

    let (pushed, residual) = split("SELECT 1 FROM t WHERE a IS NULL");
    assert!(residual.is_none(), "the whole conjunct moved into the scan");
    assert!(matches!(
        pushed.as_slice(),
        [ScanPredicate::IsNull {
            column_offset: 0,
            negated: false,
            ..
        }]
    ));
    let (pushed, _) = split("SELECT 1 FROM t WHERE b IS NOT NULL");
    assert!(matches!(
        pushed.as_slice(),
        [ScanPredicate::IsNull {
            column_offset: 1,
            negated: true,
            ..
        }]
    ));

    let (pushed, residual) = split("SELECT 1 FROM t WHERE a IN (1, 3, 5)");
    assert!(residual.is_none());
    let [ScanPredicate::In {
        column_offset,
        literals,
        negated,
        ..
    }] = pushed.as_slice()
    else {
        panic!("one `IN` description: {pushed:?}");
    };
    assert_eq!((*column_offset, *negated), (0, false));
    assert_eq!(
        literals,
        &[Datum::Int(1), Datum::Int(3), Datum::Int(5)],
        "the list keeps its source order and values"
    );
    let (pushed, _) = split("SELECT 1 FROM t WHERE a NOT IN (2)");
    assert!(matches!(
        pushed.as_slice(),
        [ScanPredicate::In { negated: true, .. }]
    ));

    // An `OR` chain flattens: `a = 1 OR a = 2 OR b IS NULL` is one conjunct
    // with three branches, not a nest of two-branch disjunctions.
    let (pushed, residual) = split("SELECT 1 FROM t WHERE a = 1 OR a = 2 OR b IS NULL");
    assert!(residual.is_none());
    let [ScanPredicate::Or(branches)] = pushed.as_slice() else {
        panic!("one `OR` description: {pushed:?}");
    };
    assert_eq!(branches.len(), 3);
    assert!(matches!(branches[2], ScanPredicate::IsNull { .. }));

    // `NOT` wraps whatever it negates, and an `AND` inside an `OR` branch is
    // *not* describable, so that whole disjunction stays above the scan.
    let (pushed, _) = split("SELECT 1 FROM t WHERE NOT a = 1");
    assert!(matches!(pushed.as_slice(), [ScanPredicate::Not(_)]));
    let (pushed, residual) = split("SELECT 1 FROM t WHERE (a = 1 AND b = 2) OR a = 3");
    assert!(pushed.is_empty(), "an AND inside an OR is not described");
    assert!(residual.is_some());

    // Mixed with a residual: the describable conjunct moves, the rest stays.
    let (pushed, residual) = split("SELECT 1 FROM t WHERE a IN (1, 2) AND b + 1 < 10");
    assert_eq!(pushed.len(), 1);
    assert!(residual.is_some());
}
