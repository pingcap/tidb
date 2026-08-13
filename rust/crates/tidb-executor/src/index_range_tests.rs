#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_trace::range_text;

    /// The `range:` cell EXPLAIN would print for a derived range list, which
    /// is the exact text the Go corpus below was captured from.
    fn render(ranges: &[IndexRange]) -> String {
        ranges.iter().map(range_text).collect::<Vec<_>>().join(", ")
    }

    fn columns(names: &[&str]) -> Vec<RangeColumn> {
        names
            .iter()
            // The corpus table is `t(a int, b int, c int, s varchar(255))`;
            // the field type matters now that `convertPointInPlace` casts
            // every endpoint into the column's own type.
            .map(|name| {
                let code = if *name == "s" {
                    tidb_datatype::FieldTypeCode::VarString
                } else {
                    tidb_datatype::FieldTypeCode::LongLong
                };
                RangeColumn::whole((*name).to_owned(), FieldType::new(code))
            })
            .collect()
    }

    fn derive(index: &[&str], where_sql: &str) -> String {
        derive_with_columns(&columns(index), where_sql)
    }

    /// [`derive`] with the index columns' real field types supplied, which is
    /// what the unsigned/overflow corpus needs: Go's `convertPointInPlace`
    /// converts every range endpoint to the indexed column's type before
    /// building, and that conversion is the whole subject of those rows.
    pub(super) fn derive_typed(index: &[(&str, FieldType)], where_sql: &str) -> String {
        let typed: Vec<RangeColumn> = index
            .iter()
            .map(|(name, ft)| RangeColumn::whole((*name).to_owned(), ft.clone()))
            .collect();
        derive_with_columns(&typed, where_sql)
    }

    /// [`derive`] over key parts that declare a PREFIX length, which is what
    /// the prefix corpus needs: Go cuts every endpoint to that length before
    /// building the range.
    fn derive_prefixed(index: &[(&str, FieldType, i64)], where_sql: &str) -> String {
        let typed: Vec<RangeColumn> = index
            .iter()
            .map(|(name, ft, prefix_len)| RangeColumn {
                name: (*name).to_owned(),
                field_type: ft.clone(),
                prefix_len: *prefix_len,
            })
            .collect();
        derive_with_columns(&typed, where_sql)
    }

    fn derive_with_columns(index: &[RangeColumn], where_sql: &str) -> String {
        let sql = format!("SELECT * FROM t WHERE {where_sql}");
        let stmt = tidb_parser::parse(&sql).expect("the corpus SQL parses");
        let tidb_ast::Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        let where_clause = select
            .where_clause
            .as_ref()
            .expect("the corpus has a WHERE");
        match detach_cond_and_build_range_for_index(
            index,
            where_clause,
            &tidb_datatype::SessionTimeZone::utc(),
        ) {
            Some(built) => render(&built.ranges),
            None => "<no range>".to_owned(),
        }
    }

    /// Every `range:` cell Go's EXPLAIN prints for these `WHERE` shapes,
    /// captured with `pkg/executor/zz_dump_ranges_test.go` against a mock
    /// store, with the index forced by `USE INDEX` so the text measures range
    /// derivation rather than stats-less plan choice.
    ///
    /// `""` is Go proving the conditions contradictory (it plans a TableDual
    /// and prints no scan at all), which this derivation reports as an empty
    /// range list.
    const GO_CORPUS: &[(&[&str], &str, &str)] = &[
        // single-column comparisons
        (&["a"], "a = 1", "[1,1]"),
        (&["a"], "a > 1", "(1,+inf]"),
        (&["a"], "a >= 1", "[1,+inf]"),
        (&["a"], "a < 1", "[-inf,1)"),
        (&["a"], "a <= 1", "[-inf,1]"),
        (&["a"], "a <> 1", "[-inf,1), (1,+inf]"),
        (&["a"], "not (a < 5)", "[5,+inf]"),
        (&["a"], "1 = a", "[1,1]"),
        (&["a"], "1 < a", "(1,+inf]"),
        (&["a"], "a > 1 and a < 10", "(1,10)"),
        (&["a"], "a >= 1 and a <= 10", "[1,10]"),
        (&["a"], "a > 1 and a > 5", "(5,+inf]"),
        (&["a"], "a > 10 and a < 1", ""),
        (&["a"], "a = 1 and a = 2", ""),
        (&["a"], "a is null", "[NULL,NULL]"),
        // BETWEEN
        (&["a"], "a between 1 and 10", "[1,10]"),
        (&["a"], "a not between 1 and 10", "[-inf,1), (10,+inf]"),
        (&["a"], "a between 10 and 1", ""),
        // IN lists
        (&["a"], "a in (1)", "[1,1]"),
        (&["a"], "a in (1, 2, 3)", "[1,1], [2,2], [3,3]"),
        (&["a"], "a in (3, 1, 2, 1)", "[1,1], [2,2], [3,3]"),
        (&["a"], "a not in (1, 2)", "(NULL,1), (2,+inf]"),
        (&["a"], "a in (1, null)", "[1,1]"),
        // composite index prefixes
        (&["a", "b"], "a = 1 and b = 2", "[1 2,1 2]"),
        (&["a", "b"], "a = 1 and b > 2", "(1 2,1 +inf]"),
        (&["a", "b"], "a = 1 and b >= 2 and b < 8", "[1 2,1 8)"),
        (&["a", "b"], "a = 1 and b between 2 and 8", "[1 2,1 8]"),
        (&["a", "b"], "a = 1 and b in (2, 3)", "[1 2,1 2], [1 3,1 3]"),
        (&["a", "b"], "a in (1, 2) and b = 3", "[1 3,1 3], [2 3,2 3]"),
        (&["a", "b"], "a > 1 and b = 2", "(1,+inf]"),
        (&["a", "b"], "a = 1", "[1,1]"),
        (
            &["a", "b", "c"],
            "a = 1 and b = 2 and c = 3",
            "[1 2 3,1 2 3]",
        ),
        (
            &["a", "b", "c"],
            "a = 1 and b = 2 and c > 3",
            "(1 2 3,1 2 +inf]",
        ),
        (&["a", "b", "c"], "a = 1 and c = 3", "[1,1]"),
        (
            &["a", "b", "c"],
            "a = 1 and b in (2, 3) and c = 4",
            "[1 2 4,1 2 4], [1 3 4,1 3 4]",
        ),
        // DNF / OR
        (&["a"], "a = 1 or a = 2", "[1,2]"),
        (&["a"], "a < 1 or a > 10", "[-inf,1), (10,+inf]"),
        (
            &["a", "b"],
            "(a = 1 and b = 2) or (a = 3 and b = 4)",
            "[1 2,1 2], [3 4,3 4]",
        ),
        (&["a"], "a = 1 or a in (2, 3)", "[1,3]"),
        (&["a"], "a > 5 or a > 1", "(1,+inf]"),
        (&["a"], "a in (1,2) or a = 5", "[1,2], [5,5]"),
        // LIKE
        (&["s"], "s like 'abc%'", "[\"abc\",\"abd\")"),
        (&["s"], "s like 'abc'", "[\"abc\",\"abc\"]"),
        (&["s"], "s like 'ab_c%'", "[\"ab\",\"ac\")"),
        (&["s"], "s = 'abc'", "[\"abc\",\"abc\"]"),
        (&["s"], "s > 'abc'", "(\"abc\",+inf]"),
        // conditions that leave a residual behind
        (&["a", "b", "c"], "a = 1 and c + 1 = 3", "[1,1]"),
        (&["s"], "s like 'x%' and a > 1", "[\"x\",\"y\")"),
    ];

    /// The differential: every derived range must render byte-for-byte as the
    /// text Go's own EXPLAIN prints for the same shape.
    #[test]
    fn derived_ranges_match_gos_explain_range_cell() {
        let mut mismatches = Vec::new();
        for (index, where_sql, expected) in GO_CORPUS {
            let got = derive(index, where_sql);
            if got != *expected {
                mismatches.push(format!(
                    "{:<10} {:<40} go={:<28} rust={}",
                    index.join(","),
                    where_sql,
                    expected,
                    got
                ));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} corpus shapes diverge from Go:\n{}",
            mismatches.len(),
            GO_CORPUS.len(),
            mismatches.join("\n")
        );
    }

    /// The corpus capture shows Go printing no `range:` cell for these, and
    /// this derivation likewise finds no access condition, so the read stays
    /// a full scan.
    #[test]
    fn shapes_go_plans_without_an_index_range_scan() {
        // No condition on the leading column: nothing to bound the scan.
        assert_eq!(derive(&["a", "b"], "b = 2"), "<no range>");
        // One OR branch constrains a column the index does not lead with, so
        // the disjunction as a whole bounds nothing.
        assert_eq!(derive(&["a", "b"], "a = 1 or b = 2"), "<no range>");
        // Conditions that span the whole index bound nothing, so they stay
        // filters rather than turning a full scan into a full range scan.
        assert_eq!(derive(&["a"], "a is not null"), "<no range>");
        assert_eq!(derive(&["s"], "s like '%abc'"), "<no range>");
        assert_eq!(derive(&["s"], "s like '%'"), "<no range>");
    }

    /// A negative bound is `unaryminus(literal)` in the AST, not a literal, so
    /// before constant folding reached [`constant_value`] every one of these
    /// derived no range at all and the read fell back to a full scan. Go folds
    /// the constant long before the ranger runs, so it has always ranged them.
    #[test]
    fn negative_bounds_derive_ranges() {
        assert_eq!(derive(&["a"], "a >= -2147483648"), "[-2147483648,+inf]");
        assert_eq!(derive(&["a"], "a < -1"), "[-inf,-1)");
        assert_eq!(derive(&["a"], "a > -100"), "(-100,+inf]");
        assert_eq!(derive(&["a"], "a < -1 and a < 1"), "[-inf,-1)");
    }

    /// Go `points.go` `buildFromScalarFunc`'s `ast.LogicOr` arm together with
    /// `detacher.go` `allEqOrIn`: an `OR` over ONE index column is that
    /// column's equality slot, so the walk keeps going to the column after it.
    ///
    /// Every expectation here is the `range:` cell
    /// `tests/integrationtest/r/util/ranger.result` records for the same
    /// statement over `t(a int, b int, c int, key(a, b, c))`.
    #[test]
    fn a_disjunction_on_one_index_column_still_pins_it() {
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and (b = 1 or b = 2) and c > 1"),
            "(1 1 1,1 1 +inf], (1 2 1,1 2 +inf]"
        );
        assert_eq!(
            derive(
                &["a", "b", "c"],
                "a = 1 and (b = 1 or b in (2, 3)) and c > 1"
            ),
            "(1 1 1,1 1 +inf], (1 2 1,1 2 +inf], (1 3 1,1 3 +inf]"
        );
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and (b is null or b = 2) and c > 1"),
            "(1 NULL 1,1 NULL +inf], (1 2 1,1 2 +inf]"
        );
        // A second condition on the same column intersects with the union, so
        // a disjunction that cannot hold is an EMPTY range list and not the
        // surviving branch alone.
        assert_eq!(
            derive(
                &["a", "b", "c"],
                "a = 1 and (b = 1 or b = 2) and b = 3 and c > 1"
            ),
            ""
        );
    }

    /// The CONTROL for the test above, and the reason `allEqOrIn` alone is not
    /// the rule: Go's `ExtractEqAndInCondition` asks `extractValueInfo` for
    /// the condition's constant right after `allEqOrIn` accepts it, and clears
    /// the access slot when that constant is NULL.
    ///
    /// So a BARE `IS NULL` does NOT advance the walk -- `c > 1` never reaches
    /// the range -- while the very same `IS NULL` inside a disjunction does.
    /// Reporting `IS NULL` as an equality everywhere passes the test above and
    /// breaks these two, which is exactly how the divergence was found.
    #[test]
    fn a_bare_is_null_does_not_advance_the_walk() {
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and b is null and c > 1"),
            "[1 NULL,1 NULL]"
        );
        assert_eq!(
            derive(
                &["a", "b", "c"],
                "a = 1 and b is null and b is null and c > 1"
            ),
            "[1 NULL,1 NULL]"
        );
        // A bare equality still advances it, so the rule above is about the
        // NULL constant and not about losing the third column generally.
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and b = 2 and c > 1"),
            "(1 2 1,1 2 +inf]"
        );
    }

    fn ft(code: tidb_datatype::FieldTypeCode) -> FieldType {
        FieldType::new(code)
    }

    fn unsigned(code: tidb_datatype::FieldTypeCode) -> FieldType {
        FieldType::new(code).with_unsigned(true)
    }

    /// Go `TestIndexRangeForUnsignedAndOverflow`
    /// (`pkg/util/ranger/ranger_test.go:314`), all 19 rows, against the table
    ///
    /// ```sql
    /// create table t(
    ///   a smallint(5) unsigned, decimal_unsigned decimal unsigned,
    ///   float_unsigned float unsigned, double_unsigned double unsigned,
    ///   col_int bigint, col_float float, ...)
    /// ```
    ///
    /// `resultStr` is Go's `fmt.Sprintf("%v", res.Ranges)`; the expectation
    /// below is the same list in this crate's `range:`-cell rendering (outer
    /// brackets dropped, ranges joined by `", "`), and `""` is Go's empty
    /// range list.
    ///
    /// Every row here turns on `convertPointInPlace`: Go converts each range
    /// endpoint to the indexed column's type before building, so `a >= -2147483648`
    /// on an UNSIGNED column collapses to `[0,+inf]` rather than keeping the
    /// negative bound. Rust now reproduces 13 rows; the six remaining rows
    /// stay `#[ignore]`d below with Go's answer asserted as the next unit's
    /// specification.
    /// One index column of a corpus row: name, type code, and whether the
    /// column is UNSIGNED.
    type IndexColumnSpec = (&'static str, tidb_datatype::FieldTypeCode, bool);

    const GO_UNSIGNED_AND_OVERFLOW: &[(&[IndexColumnSpec], &str, &str)] = &[
        // (index columns as (name, type code, unsigned), expr, Go's ranges)
        (
            &[
                ("a", tidb_datatype::FieldTypeCode::Short, true),
                ("col_int", tidb_datatype::FieldTypeCode::LongLong, false),
            ],
            "a = 1 and a = 2",
            "",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (0, 1, 2)",
            "(NULL,0), (2,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (-1, 1, 2)",
            "(NULL,1), (2,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (-2, -1, 1, 2)",
            "(NULL,1), (2,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (111)",
            "[-inf,111), (111,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (1, 2, 9223372036854775810)",
            "(NULL,1), (2,9223372036854775810), (9223372036854775810,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a >= -2147483648",
            "[0,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a > -2147483648",
            "[0,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a != -2147483648",
            "[0,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a < -1 or a < 1",
            "[-inf,1)",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a < -1 and a < 1",
            "",
        ),
        (
            &[(
                "decimal_unsigned",
                tidb_datatype::FieldTypeCode::NewDecimal,
                true,
            )],
            "decimal_unsigned > -100",
            "[0,+inf]",
        ),
        (
            &[("float_unsigned", tidb_datatype::FieldTypeCode::Float, true)],
            "float_unsigned > -100",
            "[0,+inf]",
        ),
        (
            &[(
                "double_unsigned",
                tidb_datatype::FieldTypeCode::Double,
                true,
            )],
            "double_unsigned > -100",
            "[0,+inf]",
        ),
        (
            &[("col_int", tidb_datatype::FieldTypeCode::LongLong, false)],
            "col_int != 9223372036854775808",
            "[-inf,+inf]",
        ),
        (
            &[("col_int", tidb_datatype::FieldTypeCode::LongLong, false)],
            "col_int > 9223372036854775808",
            "",
        ),
        (
            &[("col_int", tidb_datatype::FieldTypeCode::LongLong, false)],
            "col_int < 9223372036854775808",
            "[-inf,+inf]",
        ),
        (
            &[("col_float", tidb_datatype::FieldTypeCode::Float, false)],
            "col_float > 1000000000000000000000000000000000000000",
            "",
        ),
        (
            &[("col_float", tidb_datatype::FieldTypeCode::Float, false)],
            "col_float < -1000000000000000000000000000000000000000",
            "",
        ),
    ];

    fn derive_unsigned_row(index: &[IndexColumnSpec], where_sql: &str) -> String {
        let cols: Vec<(&str, FieldType)> = index
            .iter()
            .map(|(name, code, uns)| (*name, if *uns { unsigned(*code) } else { ft(*code) }))
            .collect();
        derive_typed(&cols, where_sql)
    }

    /// How many of [`GO_UNSIGNED_AND_OVERFLOW`]'s rows this derivation
    /// reproduces today. A ratchet, not a pass: the full table is asserted by
    /// the `#[ignore]`d test below, which names every row that is still wrong.
    #[test]
    fn unsigned_and_overflow_rows_that_already_match_go() {
        let mut matched = 0;
        let mut diverged = Vec::new();
        for (index, where_sql, expected) in GO_UNSIGNED_AND_OVERFLOW {
            let got = derive_unsigned_row(index, where_sql);
            if got == *expected {
                matched += 1;
            } else {
                diverged.push(format!("  {where_sql:<50} go={expected:<40} rust={got}"));
            }
        }
        // This assertion is a ratchet, not a pass: it records how many of Go's
        // 19 rows this derivation reproduces today. It must never fall.
        assert!(
            matched >= 13,
            "only {matched} of {} Go rows match; diverging rows:\n{}",
            GO_UNSIGNED_AND_OVERFLOW.len(),
            diverged.join("\n")
        );
    }

    /// The full Go table asserted verbatim. This fails until endpoint type
    /// conversion (`convertPointInPlace`) lands, and the failure message names
    /// every row that is still wrong -- that list is the work item.
    #[test]
    #[ignore = "6 of 19 rows still need Go's handleUnsignedCol signedness clamping and expression-level RefineCompareArgs for out-of-domain constants"]
    fn unsigned_and_overflow_ranges_match_go() {
        let mut mismatches = Vec::new();
        for (index, where_sql, expected) in GO_UNSIGNED_AND_OVERFLOW {
            let got = derive_unsigned_row(index, where_sql);
            if got != *expected {
                mismatches.push(format!("  {where_sql:<50} go={expected:<40} rust={got}"));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} Go rows diverge:\n{}",
            mismatches.len(),
            GO_UNSIGNED_AND_OVERFLOW.len(),
            mismatches.join("\n")
        );
    }

    /// Go `TestPrefixIndexRangeScan`
    /// (`pkg/util/ranger/ranger_test.go:1037`), both range rows verbatim.
    ///
    /// The original test also executes the queries against a mock store. The
    /// executor's prefix-index read tests cover that residual-filter half;
    /// this test owns the ranger boundary itself. In both rows every value
    /// that reaches a declared two-character prefix loses start exclusivity,
    /// so the scan remains a superset that the unchanged `WHERE` can filter.
    #[test]
    fn prefix_index_range_scan_matches_go() {
        let string_type = || ft(tidb_datatype::FieldTypeCode::VarString);
        for (index, where_sql, expected) in [
            (vec![("a", string_type(), 2)], "a > 'aa'", "[\"aa\",+inf]"),
            (
                vec![("a", string_type(), 2), ("b", string_type(), 2)],
                "a = 'aaa' and b > 'bb' and b < 'cc'",
                "[\"aa\" \"bb\",\"aa\" \"cc\")",
            ),
        ] {
            assert_eq!(derive_prefixed(&index, where_sql), expected, "{where_sql}");
        }
    }

    /// Go `TestPrefixIndexRange` (`pkg/util/ranger/ranger_test.go:2342`), all
    /// 10 rows, against
    ///
    /// ```sql
    /// create table t(a varchar(50), b varchar(50), c text(50), d varbinary(50),
    ///   index idx_a(a(2)), index idx_ab(a(2), b(2)),
    ///   index idx_c(c(2)), index idx_d(d(2)))
    /// ```
    ///
    /// with `tidb_opt_prefix_index_single_scan = 1`.
    ///
    /// The declared `(2)` reaches the builder now, and 4 of these 10 rows
    /// pass. The other 6 diverge for a reason that has nothing to do with the
    /// prefix: this crate does not turn `IS NOT NULL` (or an `isnull(...)`
    /// call) into an ACCESS condition at all -- `points_for_condition` drops
    /// any condition whose points span the full range -- so Go's
    /// `[-inf,+inf]` and `[NULL,+inf]` come back as "no range". That is the
    /// same gap the module doc lists, and closing it is a different unit, so
    /// the row set stays recorded rather than half-asserted.
    const GO_PREFIX_INDEX_RANGE: &[(&[&str], &str, &str)] = &[
        (&["a"], "a is null", "[NULL,NULL]"),
        // accessConds is empty here: Go detaches nothing and falls back to the
        // full range, which this crate reports as "<no range>".
        (&["a"], "isnull(a) or a in (1,2,3,4)", "[NULL,+inf]"),
        (&["a"], "isnull(a) and a in (1,2,3,4)", "[NULL,NULL]"),
        (&["a"], "a is not null", "[-inf,+inf]"),
        (
            &["a", "b"],
            "a = 'a' and b is null",
            "[\"a\" NULL,\"a\" NULL]",
        ),
        (
            &["a", "b"],
            "a = 'a' and b is not null",
            "[\"a\" -inf,\"a\" +inf]",
        ),
        (&["c"], "c is null", "[NULL,NULL]"),
        (&["c"], "c is not null", "[-inf,+inf]"),
        (&["d"], "d is null", "[NULL,NULL]"),
        (&["d"], "d is not null", "[-inf,+inf]"),
    ];

    #[test]
    #[ignore = "6 of 10 rows need IS NOT NULL as an access condition, which this crate does not build; Go's answers are recorded above as that unit's spec"]
    fn prefix_index_ranges_match_go() {
        let mut mismatches = Vec::new();
        for (index, where_sql, expected) in GO_PREFIX_INDEX_RANGE {
            let cols: Vec<(&str, FieldType, i64)> = index
                .iter()
                .map(|name| (*name, ft(tidb_datatype::FieldTypeCode::VarString), 2))
                .collect();
            let got = derive_prefixed(&cols, where_sql);
            if got != *expected {
                mismatches.push(format!("  {where_sql:<40} go={expected:<28} rust={got}"));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} Go rows diverge:\n{}",
            mismatches.len(),
            GO_PREFIX_INDEX_RANGE.len(),
            mismatches.join("\n")
        );
    }

    /// The endpoint CUT itself, which `GO_PREFIX_INDEX_RANGE` does not
    /// exercise: its rows are all NULL boundaries and one two-character
    /// equality, so none of them is long enough to be cut.
    ///
    /// The `range:` cells are captured from real TiDB's `EXPLAIN` over
    /// `t(a varchar(20), b int, key idx(a(3)))`; the rows that are full
    /// scans there (Go's cost model declines the double read) were captured
    /// with the index forced, since range DERIVATION is what is measured.
    ///
    /// Every one of these is a SUPERSET of the qualifying rows, which is the
    /// property that makes the residual `WHERE` above the scan sufficient. An
    /// uncut `["abcdef","abcdef"]` would be a subset and would lose rows.
    const GO_PREFIX_CUT_RANGE: &[(&str, &str)] = &[
        // Cut to the prefix; a point stays a point.
        ("a = 'abcdef'", "[\"abc\",\"abc\"]"),
        // Shorter than the prefix: nothing to cut, and the point is exact.
        ("a = 'ab'", "[\"ab\",\"ab\"]"),
        // Cut START endpoints lose their exclusiveness, so `> 'abcdef'` still
        // reads the `'abc'` entries that stand for it.
        ("a > 'abcdef'", "[\"abc\",+inf]"),
        ("a >= 'abcdef'", "[\"abc\",+inf]"),
        // A value that REACHES the prefix without being cut loses it too.
        ("a > 'abc'", "[\"abc\",+inf]"),
        // An END endpoint that was cut becomes inclusive.
        ("a < 'abcdef'", "[-inf,\"abc\"]"),
        ("a <= 'abcdef'", "[-inf,\"abc\"]"),
        // A value shorter than the prefix keeps its exclusiveness on both
        // sides: no entry hides behind it.
        ("a > 'ab'", "(\"ab\",+inf]"),
        ("a < 'ab'", "[-inf,\"ab\")"),
        // An IN list cuts each point, and two values sharing the prefix
        // collapse onto one range.
        (
            "a in ('abcdef', 'zzz')",
            "[\"abc\",\"abc\"], [\"zzz\",\"zzz\"]",
        ),
        ("a in ('abcdef', 'abcxyz')", "[\"abc\",\"abc\"]"),
        // A LIKE whose literal prefix outruns the key part is cut the same
        // way, and the derived upper bound goes with it.
        ("a like 'abcd%'", "[\"abc\",\"abd\")"),
        ("a like 'ab%'", "[\"ab\",\"ac\")"),
    ];

    #[test]
    fn a_cut_endpoint_widens_the_range_it_bounds() {
        let mut mismatches = Vec::new();
        for (where_sql, expected) in GO_PREFIX_CUT_RANGE {
            let got = derive_prefixed(
                &[("a", ft(tidb_datatype::FieldTypeCode::VarString), 3)],
                where_sql,
            );
            if got != *expected {
                mismatches.push(format!("  {where_sql:<30} want={expected:<32} got={got}"));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} rows diverge:\n{}",
            mismatches.len(),
            GO_PREFIX_CUT_RANGE.len(),
            mismatches.join("\n")
        );
    }

    /// The control for the row above: the SAME conditions over a key part
    /// with no declared length keep their exclusiveness and their whole
    /// values. Without this, a cut applied unconditionally would pass the
    /// corpus above and quietly widen every ordinary index range.
    #[test]
    fn a_whole_key_part_is_never_cut() {
        for (where_sql, expected) in [
            ("a = 'abcdef'", "[\"abcdef\",\"abcdef\"]"),
            ("a > 'abcdef'", "(\"abcdef\",+inf]"),
            ("a < 'abcdef'", "[-inf,\"abcdef\")"),
            (
                "a in ('abcdef', 'abcxyz')",
                "[\"abcdef\",\"abcdef\"], [\"abcxyz\",\"abcxyz\"]",
            ),
        ] {
            assert_eq!(
                derive_typed(
                    &[("a", ft(tidb_datatype::FieldTypeCode::VarString))],
                    where_sql
                ),
                expected,
                "{where_sql}"
            );
        }
    }

    /// `where_is_unsatisfiable` fires exactly on Go's `unsatisfiable`: an
    /// equality contradicted by another BINARY comparison on the same column,
    /// on ANY column of the table -- and never on the shapes Go leaves a
    /// filter (a range pair with no equality, an equality vs an `IN`, a lone
    /// disjunction).
    #[test]
    fn where_is_unsatisfiable_matches_go_predicate_simplification() {
        // The table is `t(a int, b int, c int)`; the contradiction may be on
        // any of them, indexed or not.
        let table: Vec<(String, FieldType)> = ["a", "b", "c"]
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )
            })
            .collect();
        let unsat = |where_sql: &str| {
            let sql = format!("SELECT * FROM t WHERE {where_sql}");
            let stmt = tidb_parser::parse(&sql).expect("parses");
            let tidb_ast::Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            let where_clause = select.where_clause.as_ref().expect("has a WHERE");
            where_is_unsatisfiable(&table, where_clause, &tidb_datatype::SessionTimeZone::utc())
        };

        // Contradictory: an equality no other comparison on the column admits.
        // The column need not be the leading one, nor indexed at all -- this is
        // the whole point of the index-independent dual.
        assert!(unsat("b = 1 and b = 2"), "two conflicting equalities on b");
        assert!(unsat("a = 2 and a = 3"), "the partition-key shape");
        assert!(unsat("a = 1 and a > 2"), "equality below a lower bound");
        assert!(unsat("a = 1 and a < 1"), "equality at an excluded bound");
        assert!(unsat("a = 1 and a <> 1"), "equality against its own <>");
        assert!(
            unsat("c > 0 and b = 1 and b = 2"),
            "a satisfiable conjunct on another column does not hide it"
        );

        // Satisfiable / not proven false by Go's rule:
        assert!(!unsat("a = 1 and a = 1"), "the same equality twice");
        assert!(!unsat("a = 1 and a < 2"), "equality inside the bound");
        assert!(
            !unsat("b > 10 and b < 1"),
            "no equality: Go keeps it a filter, not a dual"
        );
        assert!(
            !unsat("b = 1 and b in (2, 3)"),
            "the other side is an IN, not a binary comparison"
        );
        assert!(
            !unsat("(b = 1 and b = 2) or c = 5"),
            "a lone top-level OR is a disjunction"
        );
        assert!(!unsat("a = 1"), "a single equality constrains nothing else");
        assert!(
            !unsat("a = b"),
            "a column-to-column equality is not a point"
        );
    }
}
