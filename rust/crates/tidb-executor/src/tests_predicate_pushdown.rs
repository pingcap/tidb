// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The tests of `tidb_executor_internal_exec::predicate_pushdown` that drive
//! the executor, and the condition builders the scan tests share.

/// Builders of the retained physical conditions the scan tests offer.
pub(crate) mod fixtures {
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::expression::{Expression, ScalarFunction};

    /// A scan-output column at `offset`.
    pub(crate) fn column(offset: usize, field_type: FieldType) -> Expression {
        column_with_id(offset, offset as i64 + 1, field_type)
    }

    /// A scan-output column at `offset` with the given plan column id, which
    /// survives a remap of the offset.
    pub(crate) fn column_with_id(
        offset: usize,
        unique_id: i64,
        field_type: FieldType,
    ) -> Expression {
        let mut column = tidb_expr::column::Column::new(unique_id, field_type);
        column.index = offset as i64;
        Expression::Column(column)
    }

    /// A strict literal.
    pub(crate) fn literal(value: Datum, field_type: FieldType) -> Expression {
        Expression::Constant(tidb_expr::constant::Constant::new(value, field_type))
    }

    /// A call of the builtin `name`.
    pub(crate) fn call(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ))
    }

    /// `column OP literal` (or `literal OP column`), `name` one of `eq`,
    /// `ne`, `lt`, `le`, `gt`, `ge`.
    pub(crate) fn compare(
        offset: usize,
        column_type: FieldType,
        name: &str,
        value: Datum,
        literal_type: FieldType,
        column_on_left: bool,
    ) -> Expression {
        let column = column(offset, column_type);
        let literal = literal(value, literal_type);
        call(
            name,
            if column_on_left {
                vec![column, literal]
            } else {
                vec![literal, column]
            },
        )
    }

    /// `column IS NULL`, or `column IS NOT NULL` when `negated`.
    pub(crate) fn is_null(offset: usize, field_type: FieldType, negated: bool) -> Expression {
        let test = call("isnull", vec![column(offset, field_type)]);
        if negated {
            call("not", vec![test])
        } else {
            test
        }
    }

    /// The scan-output offsets `expression` reads, in traversal order.
    pub(crate) fn column_offsets(expression: &Expression) -> Vec<usize> {
        tidb_expr::simple_expr::extract_columns(expression)
            .into_iter()
            .map(|column| usize::try_from(column.index).expect("a scan offset"))
            .collect()
    }
}

mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_txnkv::Key;

    use tidb_kv::cluster_storage::{ClusterSnapshot, SnapshotPairs};
    use tidb_store_driver_txn::cluster_storage::{ClusterTableStorage, MutationBuffer};
    use crate::driver::run_select_on;
    use tidb_infoschema::Catalog;
    use tidb_table::kv_table::KvTable;
    use tidb_table::kv_table::KvColumn;
    use tidb_kv::storage::StorageError;

    /// A snapshot over a fixed map: the committed half of a cluster read.
    #[derive(Debug, Default)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for MockSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: long(),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    /// Publishes everything the buffer stages into the snapshot and empties
    /// it: what COMMIT does to the two halves of a cluster read.
    fn commit(buffer: &MutationBuffer, snapshot: &Arc<Mutex<MockSnapshot>>) {
        let mut snapshot = snapshot.lock().unwrap();
        for (key, value) in buffer.snapshot() {
            match value {
                Some(value) => snapshot.data.insert(key.as_bytes().to_vec(), value),
                None => snapshot.data.remove(key.as_bytes()),
            };
        }
        buffer.reset();
    }

    /// Go `PushDownExprs` consults `mysql.expr_pushdown_blacklist` for every
    /// Selection offered to a scan. A later offer's conditions joined to an
    /// earlier one keep that verdict: a blacklisted function stays a local
    /// condition and never reaches the coprocessor request.
    #[test]
    fn a_later_selection_offer_keeps_the_pushdown_blacklist() {
        use tidb_expr::expression::Expression;
        let gt = |column_id: i64, index: i64, value: i64| {
            let mut column = tidb_expr::column::Column::new(column_id, long());
            column.index = index;
            Expression::ScalarFunction(tidb_expr::expression::ScalarFunction::new(
                tidb_ast::CiString::new("gt"),
                long(),
                vec![
                    Expression::Column(column),
                    Expression::Constant(tidb_expr::constant::Constant::new(
                        Datum::Int(value),
                        long(),
                    )),
                ],
            ))
        };
        let mut blacklist = tidb_expr::infer_pushdown::ExprPushDownBlacklist::new();
        blacklist.insert(
            "gt".to_owned(),
            tidb_expr::infer_pushdown::blacklist_store_mask("tikv"),
        );
        let blocked = crate::StmtContext::for_query()
            .with_pushdown_blacklists((Arc::new(blacklist), Arc::default()));
        let open = crate::StmtContext::for_query();
        let mut first = tidb_executor_internal_exec::predicate_pushdown::PushedScanFilter::from_physical_conditions(vec![gt(1, 0, 5)], &open);
        assert!(first.fully_pushable());
        let later = tidb_executor_internal_exec::predicate_pushdown::PushedScanFilter::from_physical_conditions(vec![gt(2, 1, 6)], &blocked);
        assert!(!later.fully_pushable(), "the blacklist refuses `gt`");
        first.conjoin(&later);
        assert_eq!(first.filters().len(), 2, "both conditions still filter");
        assert_eq!(
            first.conditions().len(),
            2,
            "the request builder is offered every condition, never a subset"
        );
        assert!(
            !first.fully_pushable(),
            "one refused condition keeps the whole Selection local"
        );
    }

    /// A pushed predicate must filter the transaction's own staged rows too.
    ///
    /// Over `ClusterTableStorage` a scan produces the snapshot merged with the
    /// session's staged mutation buffer. Pushing a conjunct into the scan
    /// removes it from the `Selection` above, so the scan becomes the only
    /// place it is applied: a staged row that fails it must not be returned,
    /// and a staged row that satisfies it must not be lost -- including when
    /// a staged UPDATE moves a row across the predicate's boundary and when a
    /// staged DELETE removes a row the committed half still holds.
    #[test]
    fn a_pushed_predicate_filters_staged_rows_as_well_as_committed_ones() {
        let snapshot = Arc::new(Mutex::new(MockSnapshot::default()));
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let buffer = MutationBuffer::new();
        let mut table = KvTable::with_storage(
            91,
            vec![column("a", 1), column("b", 2)],
            Box::new(ClusterTableStorage::new(buffer.clone(), handle)),
        );

        // Committed half: one row above the predicate, one below.
        let committed_low = table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_exprctx::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(9), Datum::Int(90)], &tidb_exprctx::NoColumns)
            .unwrap();
        let committed_moved = table
            .insert_row(&[Datum::Int(2), Datum::Int(20)], &tidb_exprctx::NoColumns)
            .unwrap();
        commit(&buffer, &snapshot);
        assert!(buffer.is_empty(), "nothing is staged after the commit");

        // Staged half, all inside one open transaction:
        //   * an INSERT that satisfies `a > 5`,
        //   * an INSERT that does not,
        //   * an UPDATE that lifts a committed row across the boundary,
        //   * a DELETE of a committed row that satisfies it.
        table
            .insert_row(&[Datum::Int(7), Datum::Int(70)], &tidb_exprctx::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(3), Datum::Int(30)], &tidb_exprctx::NoColumns)
            .unwrap();
        table
            .update_row_with_context(
                &committed_moved,
                &[Datum::Int(8), Datum::Int(80)],
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        table
            .delete_row_with_context(
                &committed_low,
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        assert!(!buffer.is_empty(), "the writes are staged, not committed");

        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
        assert_eq!(
            run_select_on("SELECT a, b FROM t WHERE a > 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(7), Datum::Int(70)],
                vec![Datum::Int(8), Datum::Int(80)],
                vec![Datum::Int(9), Datum::Int(90)],
            ],
            "staged inserts and updates are filtered by the pushed predicate, \
             not waved through it"
        );
        // The residual half of a split predicate still runs above the scan.
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM t WHERE a > 5 AND b + 1 < 80 ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![vec![Datum::Int(7), Datum::Int(70)]]
        );
        // A staged row the predicate excludes is not reachable by any spelling.
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a = 3", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(3)]],
            "and it is still there when the predicate selects it"
        );
        // The composed descriptions carry the same obligation, because they are
        // removed from the `Selection` in exactly the same way: a staged row
        // must be tested by the pushed `IN` and the pushed `OR` too.
        assert_eq!(
            run_select_on(
                "SELECT a FROM t WHERE a IN (3, 7, 8) ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(8)],
            ],
            "the staged insert, the staged update's new value, and nothing else"
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM t WHERE a = 1 OR a = 8 ORDER BY a",
                &catalog,
                &ctx
            )
            .unwrap(),
            vec![vec![Datum::Int(8)]],
            "the staged DELETE removed the row `a = 1` matched, and the staged \
             UPDATE created the row `a = 8` matches"
        );
        // A pushed BUILTIN carries the same obligation, and widening what may
        // be pushed widens what the obligation covers -- so it is re-proved
        // here rather than assumed. `MOD(a, 4)` is 0 (false) for `a = 8`, the
        // value a staged UPDATE created, and truthy for the staged INSERTs; a
        // source that filtered the snapshot half only would return `a = 8`.
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE mod(a, 4) ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(9)],
            ],
            "the staged UPDATE's new value `a = 8` fails the pushed builtin and \
             must not be returned, while the staged inserts that pass it are"
        );
    }

    /// The whole predicate must survive when nothing is pushed, and the split
    /// must not change any answer a `Selection` alone produced.
    #[test]
    fn splitting_a_where_does_not_change_its_result() {
        let mut table = KvTable::new(92, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [(1, 10), (5, 50), (7, 70), (9, 90)] {
            table
                .insert_row(&[Datum::Int(a), Datum::Int(b)], &tidb_exprctx::NoColumns)
                .unwrap();
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let cases: [(&str, Vec<Vec<Datum>>); 5] = [
            (
                "SELECT a FROM t WHERE a > 5",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
            (
                "SELECT a FROM t WHERE 5 < a",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
            (
                "SELECT a FROM t WHERE a > 5 AND b < 80",
                vec![vec![Datum::Int(7)]],
            ),
            // A disjunction now pushes as one conjunct, and must answer the
            // same rows it did as a residual `Selection`.
            (
                "SELECT a FROM t WHERE a = 1 OR a = 9",
                vec![vec![Datum::Int(1)], vec![Datum::Int(9)]],
            ),
            // Mixed: `a > 1` pushes, the arithmetic stays above.
            (
                "SELECT a FROM t WHERE a > 1 AND b + 1 > 60",
                vec![vec![Datum::Int(7)], vec![Datum::Int(9)]],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// The composed predicates, over a table with NULLs, against the answer
    /// MySQL's three-valued logic gives.
    ///
    /// This is the pushed form and the local form agreeing: each `WHERE` below
    /// moves into the scan as one described conjunct, so the scan is the only
    /// place it is applied, and the expected rows are the ones the same
    /// predicate produces as a `Selection` -- including the cases where
    /// UNKNOWN, not FALSE, is the reason a row is absent.
    #[test]
    fn the_composed_predicates_keep_mysqls_three_valued_answers() {
        let mut table = KvTable::new(93, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [
            (Datum::Int(1), Datum::Int(10)),
            (Datum::Int(2), Datum::Null),
            (Datum::Null, Datum::Int(30)),
            (Datum::Int(4), Datum::Int(40)),
        ] {
            table.insert_row(&[a, b], &tidb_exprctx::NoColumns).unwrap();
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let int = |value: i64| vec![Datum::Int(value)];
        let cases: [(&str, Vec<Vec<Datum>>); 10] = [
            ("SELECT a FROM t WHERE a IS NULL", vec![vec![Datum::Null]]),
            (
                "SELECT a FROM t WHERE a IS NOT NULL",
                vec![int(1), int(2), int(4)],
            ),
            (
                "SELECT a FROM t WHERE b IS NULL",
                // The row whose `b` is NULL, whatever its `a` is.
                vec![int(2)],
            ),
            ("SELECT a FROM t WHERE a IN (1, 4)", vec![int(1), int(4)]),
            // `NOT IN` is UNKNOWN for the NULL row, so it is absent -- and it
            // would be present if `NOT IN` were pushed as a plain negation of
            // membership over a NULL-blind test.
            ("SELECT a FROM t WHERE a NOT IN (1, 4)", vec![int(2)]),
            ("SELECT a FROM t WHERE a = 1 OR a = 4", vec![int(1), int(4)]),
            // `OR` over a branch that is UNKNOWN for the NULL row: TRUE wins.
            (
                "SELECT a FROM t WHERE a = 1 OR a IS NULL",
                vec![int(1), vec![Datum::Null]],
            ),
            ("SELECT a FROM t WHERE NOT a = 1", vec![int(2), int(4)]),
            (
                "SELECT a FROM t WHERE a > -1 AND a < 3",
                vec![int(1), int(2)],
            ),
            // A pushed disjunction beside a residual conjunct: both apply.
            (
                "SELECT a FROM t WHERE (a = 1 OR a = 4) AND b + 1 > 20",
                vec![int(4)],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// A pushed builtin call answers the rows the same call answers as a
    /// `Selection`, including where the answer is NULL rather than false.
    ///
    /// This is the local half of the newly pushed math family: the conjunct is
    /// removed from the `Selection` above the scan, so the scan is the only
    /// place it is applied, and every expectation below is the row set MySQL's
    /// three-valued logic gives for the same predicate.
    #[test]
    fn a_pushed_math_builtin_answers_what_the_same_selection_answered() {
        let mut table = KvTable::new(94, vec![column("a", 1), column("b", 2)]);
        for (a, b) in [
            (Datum::Int(0), Datum::Int(3)),
            (Datum::Int(1), Datum::Int(0)),
            (Datum::Int(2), Datum::Int(7)),
            (Datum::Int(7), Datum::Int(7)),
            (Datum::Null, Datum::Int(1)),
        ] {
            table.insert_row(&[a, b], &tidb_exprctx::NoColumns).unwrap();
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        let int = |value: i64| vec![Datum::Int(value)];
        let cases: [(&str, Vec<Vec<Datum>>); 8] = [
            // `SIN(0)` is 0, which is false; the NULL row is UNKNOWN.
            ("SELECT a FROM t WHERE sin(a)", vec![int(1), int(2), int(7)]),
            // `MOD(a, 3)` is 0 for a = 0 and a = 3, and NULL for the NULL row.
            (
                "SELECT a FROM t WHERE mod(a, 3)",
                vec![int(1), int(2), int(7)],
            ),
            // A zero divisor is NULL in MySQL, so no row qualifies -- and it
            // must not be an error either, on the scan or above it.
            ("SELECT a FROM t WHERE mod(a, 0)", vec![]),
            // `ACOS` of anything outside [-1, 1] is NULL, not an error, and
            // `ACOS(1)` is exactly 0, which is false -- so only `a = 0`
            // (`ACOS(0)` = pi/2) qualifies.
            ("SELECT a FROM t WHERE acos(a)", vec![int(0)]),
            // `ROUND` over an integer column keeps the integer domain.
            (
                "SELECT a FROM t WHERE round(a)",
                vec![int(1), int(2), int(7)],
            ),
            // `PI()` is a nonzero constant, so it selects every row -- the NULL
            // one included, since the predicate does not read the column.
            (
                "SELECT a FROM t WHERE pi()",
                vec![int(0), int(1), int(2), int(7), vec![Datum::Null]],
            ),
            // `POW(a, 2)` is 0 only for a = 0.
            (
                "SELECT a FROM t WHERE pow(a, 2)",
                vec![int(1), int(2), int(7)],
            ),
            // A pushed builtin beside a pushed comparison and a residual
            // conjunct: all three still apply.
            (
                "SELECT a FROM t WHERE sin(a) AND a > 1 AND b + 1 > 7",
                vec![int(2), int(7)],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    /// A call the catalog does not hold stays above the scan, and so does one
    /// whose argument is not describable -- neither may quietly become a
    /// pushed conjunct, because the store would then evaluate what only this
    /// engine evaluates.
    #[test]
    fn a_call_outside_the_catalog_stays_above_the_scan() {
        let mut table = KvTable::new(95, vec![column("a", 1), column("b", 2)]);
        for a in [1_i64, 2, 3] {
            table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_exprctx::NoColumns)
                .unwrap();
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("t", table);
        let ctx = crate::StmtContext::for_query();
        // `TAN` is deliberately absent from Go's TiKV whitelist (the comment
        // cites Rust's LLVM math precision), `ABS` is on it but has no catalog
        // row yet, and `SIN(a + 1)` has an argument the description cannot
        // carry. All three must still answer correctly.
        for (sql, expected) in [
            ("SELECT a FROM t WHERE tan(a)", vec![1_i64, 2, 3]),
            ("SELECT a FROM t WHERE abs(a)", vec![1, 2, 3]),
            ("SELECT a FROM t WHERE sin(a + 1)", vec![1, 2, 3]),
            ("SELECT a FROM t WHERE mod(a, b)", vec![1, 2, 3]),
        ] {
            assert_eq!(
                run_select_on(sql, &catalog, &ctx).unwrap(),
                expected
                    .into_iter()
                    .map(|a| vec![Datum::Int(a)])
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        }
    }

    /// The planner splits a Selection per conjunct (Go `PushDownExprs`); the
    /// request builder then converts the planner's pushed list whole. The two
    /// must agree: a condition the planner admits for TiKV is one `ExprToPB`
    /// converts, or the executor would silently keep a Selection the plan says
    /// the coprocessor evaluates.
    #[test]
    fn the_converter_expresses_every_condition_the_planner_admits() {
        use tidb_datatype::{FieldType, FieldTypeCode};
        let int = FieldType::new(FieldTypeCode::LongLong);
        let text = FieldType::new(FieldTypeCode::Varchar).with_flen(10);
        let columns = vec![
            ("a".to_owned(), int.clone()),
            ("b".to_owned(), int.clone()),
            ("c".to_owned(), int.clone()),
            ("s".to_owned(), text),
            (
                "u".to_owned(),
                FieldType::new(FieldTypeCode::LongLong)
                    .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED),
            ),
            (
                "d".to_owned(),
                FieldType::new(FieldTypeCode::Datetime).with_decimal(0),
            ),
        ];
        let context = crate::StmtContext::for_query();
        let resolver = crate::driver::TableResolver {
            database: Some("test"),
            table_name: "t",
            columns: &columns,
            constant_context: context.clone(),
            zone: context.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            clause_message: "expression",
            subquery_constants: &[],
        };
        const PREDICATES: &[&str] = &[
            "a > 5",
            "5 < a",
            "a = b",
            "a <> c",
            "a IS NULL",
            "a IS NOT NULL",
            "a IN (1, 2, 3)",
            "a NOT IN (1, 2)",
            "s = 'x'",
            "s IN ('x', 'y')",
            "s LIKE 'x%'",
            "s NOT LIKE '%y'",
            "a BETWEEN 1 AND 9",
            "a > 1 AND b < 3",
            "a = 1 OR b = 2",
            "NOT (a > 1)",
            "a + b > 3",
            "b * 2 = c",
            "a - 1 = 0",
            "ABS(a - b) = 1",
            "COALESCE(b, 0) > 0",
            "IF(b > 0, a, c) > 2",
            "CASE WHEN b > 0 THEN a > 1 ELSE FALSE END",
            "a IN (b, 1)",
            "b IS TRUE",
            "b IS NOT TRUE",
            "u > 2",
            "u >= 9223372036854775808",
            "d > '2024-01-01'",
            "a = '5'",
            "a > 1.5",
            "s > 'u' AND a > 1",
            "UPPER(s) = 'X'",
            "LENGTH(s) > 2",
            "SUBSTRING(s, 1, 2) = 'ab'",
            "a % 3 = 0",
            "a DIV 2 = 1",
            "IFNULL(a, 0) = 0",
            "LEAST(a, b) > 1",
            "a IN (1, 2) AND s LIKE 'a%'",
        ];
        let mut refused = Vec::new();
        for sql in PREDICATES {
            let select = tidb_parser::parse(&format!("SELECT {sql}")).expect("parses");
            let tidb_ast::Stmt::Query(query) = select else {
                panic!("not a query");
            };
            let tidb_ast::QueryStmt::Select(select) = &*query else {
                panic!("not a select");
            };
            let Some(tidb_ast::SelectField::Expr { expr, .. }) = select.fields.fields().first()
            else {
                panic!("no field");
            };
            let Ok(mut condition) = tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver)
            else {
                continue;
            };
            tidb_expr::scalar_function::into_coprocessor_form(&mut condition);
            if !tidb_planner::pushdown::can_exprs_push_down_tikv(std::slice::from_ref(&condition)) {
                continue;
            }
            if tidb_executor_internal_exec::predicate_pushdown::describe_request_condition(&condition, Some(&context)).is_err() {
                refused.push(*sql);
            }
        }
        // The planner's admission is Go's name whitelist; the catalog holds
        // the signatures converted so far. These are the admitted shapes it
        // does not hold yet: their Selection is evaluated locally, whole (never
        // a subset), while EXPLAIN keeps Go's cop Selection. The list may only
        // shrink -- a new entry is a planner/converter disagreement.
        const NOT_CONVERTED_YET: &[&str] = &[];
        assert_eq!(
            refused, NOT_CONVERTED_YET,
            "the planner admits for TiKV what ExprToPB refuses"
        );
    }
}
