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

//! `pkg/util/ranger/ranger_test.go`'s case tables, TRANSCREATED over this
//! port's own pipeline: Go builds each condition through a mock session's
//! planner (`session.Parse` → logical plan → `PushDownNot` →
//! `DetachCondsForColumn` → `BuildTableRange`); here the same SQL text
//! parses through `tidb-parser`, rewrites through `tidb-expr`'s rewriter
//! over a fixture resolver, and runs the same detach/build calls. Each
//! case pins Go's `resultStr` verbatim (the RANGE strings; Go's
//! `accessConds`/`filterConds` strings pin its expression FORMATTER, which
//! is not this package's surface).

#![cfg(test)]

use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::expr_util::builder::RealFunctionBuilder;
use tidb_expr::expr_util::normal_form::split_cnf_items;
use tidb_expr::expr_util::push_not::push_down_not;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};

use super::detacher::detach_conds_for_column;
use super::ranger::build_table_range;
use super::types::Range;

/// The mock table `t(a bigint, b bigint)` Go's harness creates, with `a`
/// as the int-handle primary key where the case table needs one.
struct TestTable {
    a: Column,
    b: Column,
}

impl TestTable {
    fn new() -> Self {
        let mut pk_type = FieldType::new(FieldTypeCode::LongLong);
        pk_type.set_flags(pk_type.flags() | FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL);
        Self {
            a: Column::new(1, pk_type),
            b: Column::new(2, FieldType::new(FieldTypeCode::LongLong)),
        }
    }
}

impl ColumnResolver for TestTable {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        match name.to_ascii_lowercase().as_str() {
            "a" => Some((0, self.a.ret_type.clone().expect("typed"), 1)),
            "b" => Some((1, self.b.ret_type.clone().expect("typed"), 2)),
            _ => None,
        }
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let name = path.last()?;
        match name.to_ascii_lowercase().as_str() {
            "a" => Some(self.a.clone()),
            "b" => Some(self.b.clone()),
            _ => None,
        }
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        tidb_expr::SessionTimeZone::utc()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        // The production resolvers fold through the same entry (Go folds
        // during expression rewrite); without it `-1` stays a unary tree
        // and the checker refuses it.
        tidb_expr::fold_constant_in_mode(expression, &tidb_expr::NoColumns, mode);
    }
}

/// Go's `fmt.Sprintf("%v", result)` over `Ranges`.
fn ranges_to_go_string(ranges: &[Range]) -> String {
    let inner: Vec<String> = ranges.iter().map(Range::to_display_string).collect();
    format!("[{}]", inner.join(" "))
}

/// The Go harness's pipeline over one WHERE text.
fn build_ranges_for(expr_text: &str) -> Result<String, String> {
    let table = TestTable::new();
    let sql = format!("select * from t where {expr_text}");
    let stmt = tidb_parser::parse(&sql).map_err(|error| format!("parse: {error:?}"))?;
    let tidb_ast::Stmt::Query(query) = stmt else {
        return Err("not a query".to_owned());
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        return Err("not a select".to_owned());
    };
    let where_clause = select.where_clause.ok_or("no where")?;
    let rewritten = rewrite_expr_resolved(&where_clause, &table)
        .map_err(|error| format!("rewrite: {error:?}"))?;
    // Go: selection.Conditions are the CNF items; each runs PushDownNot.
    let ctx = tidb_expr::NoColumns;
    let builder = RealFunctionBuilder::new(&ctx);
    let conds: Vec<Expression> = split_cnf_items(&rewritten)
        .iter()
        .map(|cond| push_down_not(cond, &builder))
        .collect();
    let (access, _filters) = detach_conds_for_column(&conds, &table.a, true);
    let result = build_table_range(
        &access,
        table.a.ret_type.as_ref().expect("typed"),
        0,
    )
    .map_err(|error| format!("build: {error:?}"))?;
    Ok(ranges_to_go_string(&result.ranges))
}

/// Go `TestTableRange` (`ranger_test.go:45`), the full case table, each
/// `resultStr` verbatim.
#[test]
fn table_ranges_match_gos_case_table() {
    let cases: &[(&str, &str)] = &[
        ("a = 1", "[[1,1]]"),
        ("1 = a", "[[1,1]]"),
        ("a != 1", "[[-inf,1) (1,+inf]]"),
        ("1 != a", "[[-inf,1) (1,+inf]]"),
        ("a > 1", "[(1,+inf]]"),
        ("1 < a", "[(1,+inf]]"),
        ("a >= 1", "[[1,+inf]]"),
        ("1 <= a", "[[1,+inf]]"),
        ("a < 1", "[[-inf,1)]"),
        ("1 > a", "[[-inf,1)]"),
        ("a <= 1", "[[-inf,1]]"),
        ("1 >= test.t.a", "[[-inf,1]]"),
        ("(a)", "[[-inf,0) (0,+inf]]"),
        ("a in (1, 3, NULL, 2)", "[[1,1] [2,2] [3,3]]"),
        ("a IN (8,8,81,45)", "[[8,8] [45,45] [81,81]]"),
        ("a between 1 and 2", "[[1,2]]"),
        ("a not between 1 and 2", "[[-inf,1) (2,+inf]]"),
        ("a between 2 and 1", "[]"),
        ("a not between 2 and 1", "[[-inf,+inf]]"),
        ("a IS NULL", "[]"),
        ("a IS NOT NULL", "[[-inf,+inf]]"),
        ("a IS TRUE", "[[-inf,0) (0,+inf]]"),
        ("a IS NOT TRUE", "[[0,0]]"),
        ("a IS FALSE", "[[0,0]]"),
        ("a IS NOT FALSE", "[[-inf,0) (0,+inf]]"),
        (
            "a = 1 or a = 3 or a = 4 or (a > 1 and (a = -1 or a = 5))",
            "[[1,1] [3,3] [4,4] [5,5]]",
        ),
        ("(a = 1 and b = 1) or (a = 2 and b = 2)", "[[1,1] [2,2]]"),
        (
            "a = 1 or a = 3 or a = 4 or (b > 1 and (a = -1 or a = 5))",
            "[[-1,-1] [1,1] [3,3] [4,4] [5,5]]",
        ),
        (
            "a in (1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)",
            "[[1,1] [2,2] [3,3] [4,4]]",
        ),
        ("a not in (1, 2, 3)", "[[-inf,1) (3,+inf]]"),
        ("a > 9223372036854775807", "[]"),
        ("a >= 9223372036854775807", "[[9223372036854775807,+inf]]"),
        (
            "a < -9223372036854775807",
            "[[-inf,-9223372036854775807)]",
        ),
        ("isnull(a) or a in (1, 2, 3)", "[[1,1] [2,2] [3,3]]"),
        ("isnull(a) and a in (1, 2, 3)", "[]"),
    ];
    let mut failures = Vec::new();
    for (expr, expected) in cases {
        match build_ranges_for(expr) {
            Ok(got) if got == *expected => {}
            Ok(got) => failures.push(format!("{expr}: got {got}, want {expected}")),
            Err(error) => failures.push(format!("{expr}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Go `TestColumnRange`'s mock table:
/// `t(a int, b double, c float(3,2), d varchar(3), e bigint unsigned)`.
struct ColumnRangeTable {
    columns: Vec<Column>,
}

impl ColumnRangeTable {
    fn new() -> Self {
        let a = Column::new(1, FieldType::new(FieldTypeCode::Long));
        let b = Column::new(2, FieldType::new(FieldTypeCode::Double));
        let c = Column::new(3, {
            let mut ft = FieldType::new(FieldTypeCode::Float);
            ft.set_flen(3);
            ft.set_decimal(2);
            ft
        });
        let d = Column::new(4, {
            let mut ft = FieldType::new(FieldTypeCode::Varchar);
            ft.set_flen(3);
            ft.set_charset_name("utf8mb4");
            ft.set_collation_name("utf8mb4_bin");
            ft.set_collation(tidb_datatype::Collation::Utf8Mb4Bin);
            ft
        });
        let e = Column::new(5, {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            ft.set_flags(ft.flags() | FieldTypeFlags::UNSIGNED);
            ft
        });
        Self {
            columns: vec![a, b, c, d, e],
        }
    }

    fn by_name(&self, name: &str) -> Option<&Column> {
        let index = match name {
            "a" => 0,
            "b" => 1,
            "c" => 2,
            "d" => 3,
            "e" => 4,
            _ => return None,
        };
        self.columns.get(index)
    }
}

impl ColumnResolver for ColumnRangeTable {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let column = self.by_name(&name.to_ascii_lowercase())?;
        Some((
            (column.unique_id - 1) as usize,
            column.ret_type.clone().expect("typed"),
            column.unique_id,
        ))
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let name = path.last()?;
        self.by_name(&name.to_ascii_lowercase()).cloned()
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        tidb_expr::SessionTimeZone::utc()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        tidb_expr::fold_constant_in_mode(expression, &tidb_expr::NoColumns, mode);
    }
}

/// Go `TestColumnRange`'s pipeline: `ExtractAccessConditionsForColumn` +
/// `BuildColumnRange` with the case's prefix length.
fn build_column_ranges_for(
    expr_text: &str,
    col_pos: usize,
    length: i64,
) -> Result<String, String> {
    let table = ColumnRangeTable::new();
    let sql = format!("select * from t where {expr_text}");
    let stmt = tidb_parser::parse(&sql).map_err(|error| format!("parse: {error:?}"))?;
    let tidb_ast::Stmt::Query(query) = stmt else {
        return Err("not a query".to_owned());
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        return Err("not a select".to_owned());
    };
    let where_clause = select.where_clause.ok_or("no where")?;
    let rewritten = rewrite_expr_resolved(&where_clause, &table)
        .map_err(|error| format!("rewrite: {error:?}"))?;
    let ctx = tidb_expr::NoColumns;
    let builder = RealFunctionBuilder::new(&ctx);
    let conds: Vec<Expression> = split_cnf_items(&rewritten)
        .iter()
        .map(|cond| push_down_not(cond, &builder))
        .collect();
    let col = &table.columns[col_pos];
    let access =
        super::detacher::extract_access_conditions_for_column(&conds, col, true);
    let result = super::ranger::build_column_range(
        &access,
        col.ret_type.as_ref().expect("typed"),
        length,
        0,
    )
    .map_err(|error| format!("build: {error:?}"))?;
    Ok(ranges_to_go_string(&result.ranges))
}

/// Go `TestColumnRange` (`ranger_test.go:512`), the full case table.
#[test]
fn column_ranges_match_gos_case_table() {
    const UNSPECIFIED: i64 = super::checker::UNSPECIFIED_LENGTH;
    let cases: &[(usize, &str, &str, i64)] = &[
        (0, "(a = 2 or a = 2) and (a = 2 or a = 2)", "[[2,2]]", UNSPECIFIED),
        (0, "(a = 2 or a = 1) and (a = 3 or a = 4)", "[]", UNSPECIFIED),
        (0, "a = 1 and b > 1", "[[1,1]]", UNSPECIFIED),
        (1, "b > 1", "[(1,+inf]]", UNSPECIFIED),
        (0, "1 = a", "[[1,1]]", UNSPECIFIED),
        (0, "a != 1", "[[-inf,1) (1,+inf]]", UNSPECIFIED),
        (0, "1 != a", "[[-inf,1) (1,+inf]]", UNSPECIFIED),
        (0, "a > 1", "[(1,+inf]]", UNSPECIFIED),
        (0, "1 < a", "[(1,+inf]]", UNSPECIFIED),
        (0, "a >= 1", "[[1,+inf]]", UNSPECIFIED),
        (0, "1 <= a", "[[1,+inf]]", UNSPECIFIED),
        (0, "a < 1", "[[-inf,1)]", UNSPECIFIED),
        (0, "1 > a", "[[-inf,1)]", UNSPECIFIED),
        (0, "a <= 1", "[[-inf,1]]", UNSPECIFIED),
        (0, "1 >= a", "[[-inf,1]]", UNSPECIFIED),
        (0, "(a)", "[[-inf,0) (0,+inf]]", UNSPECIFIED),
        (0, "a in (1, 3, NULL, 2)", "[[1,1] [2,2] [3,3]]", UNSPECIFIED),
        (0, "a IN (8,8,81,45)", "[[8,8] [45,45] [81,81]]", UNSPECIFIED),
        (0, "a between 1 and 2", "[[1,2]]", UNSPECIFIED),
        (0, "a not between 1 and 2", "[[-inf,1) (2,+inf]]", UNSPECIFIED),
        (0, "a between 2 and 1", "[]", UNSPECIFIED),
        (0, "a not between 2 and 1", "[[-inf,+inf]]", UNSPECIFIED),
        (0, "a IS NULL", "[[NULL,NULL]]", UNSPECIFIED),
        (0, "a IS NOT NULL", "[[-inf,+inf]]", UNSPECIFIED),
        (0, "a IS TRUE", "[[-inf,0) (0,+inf]]", UNSPECIFIED),
        (0, "a IS NOT TRUE", "[[NULL,NULL] [0,0]]", UNSPECIFIED),
        (0, "a IS FALSE", "[[0,0]]", UNSPECIFIED),
        (0, "a IS NOT FALSE", "[[NULL,0) (0,+inf]]", UNSPECIFIED),
        (1, "b in (1, '2.1')", "[[1,1] [2.1,2.1]]", UNSPECIFIED),
        (0, "a > 9223372036854775807", "[(9223372036854775807,+inf]]", UNSPECIFIED),
        (2, "c > 111.11111111", "[[111.111115,+inf]]", UNSPECIFIED),
        (3, "d > 'aaaaaaaaaaaaaa'", "[(\"aaaaaaaaaaaaaa\",+inf]]", UNSPECIFIED),
        (4, "e > 18446744073709500000", "[(18446744073709500000,+inf]]", UNSPECIFIED),
        (4, "e > -2147483648", "[[0,+inf]]", UNSPECIFIED),
        (3, "d = 'aab' or d = 'aac'", "[[\"a\",\"a\"]]", 1),
        (0, "a in (1, 2, 3)", "[[1,1] [2,2] [3,3]]", UNSPECIFIED),
    ];
    let mut failures = Vec::new();
    for (col_pos, expr, expected, length) in cases {
        match build_column_ranges_for(expr, *col_pos, *length) {
            Ok(got) if got == *expected => {}
            Ok(got) => failures.push(format!("{expr}: got {got}, want {expected}")),
            Err(error) => failures.push(format!("{expr}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Go `TestIndexRangeForUnsignedAndOverflow`'s mock table (issue 6661).
struct UnsignedTable {
    columns: Vec<Column>,
}

impl UnsignedTable {
    fn new() -> Self {
        let unsigned = |code: FieldTypeCode, unique_id: i64| {
            let mut ft = FieldType::new(code);
            ft.set_flags(ft.flags() | FieldTypeFlags::UNSIGNED);
            Column::new(unique_id, ft)
        };
        Self {
            columns: vec![
                unsigned(FieldTypeCode::Short, 1),        // a smallint(5) unsigned
                unsigned(FieldTypeCode::NewDecimal, 2),   // decimal unsigned
                unsigned(FieldTypeCode::Float, 3),        // float unsigned
                unsigned(FieldTypeCode::Double, 4),       // double unsigned
                Column::new(5, FieldType::new(FieldTypeCode::LongLong)), // col_int
                Column::new(6, FieldType::new(FieldTypeCode::Float)),    // col_float
            ],
        }
    }

    fn name_to_offset(name: &str) -> Option<usize> {
        Some(match name {
            "a" => 0,
            "decimal_unsigned" => 1,
            "float_unsigned" => 2,
            "double_unsigned" => 3,
            "col_int" => 4,
            "col_float" => 5,
            _ => return None,
        })
    }

    /// The index list, by `indexPos`: six single-column indexes then
    /// `idx_int_bigint(a, col_int)`.
    fn index_columns(&self, index_pos: usize) -> Vec<Column> {
        match index_pos {
            0..=5 => vec![self.columns[index_pos].clone()],
            6 => vec![self.columns[0].clone(), self.columns[4].clone()],
            _ => Vec::new(),
        }
    }
}

impl ColumnResolver for UnsignedTable {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let offset = Self::name_to_offset(&name.to_ascii_lowercase())?;
        let column = &self.columns[offset];
        Some((
            offset,
            column.ret_type.clone().expect("typed"),
            column.unique_id,
        ))
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let name = path.last()?;
        let offset = Self::name_to_offset(&name.to_ascii_lowercase())?;
        Some(self.columns[offset].clone())
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        tidb_expr::SessionTimeZone::utc()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        tidb_expr::fold_constant_in_mode(expression, &tidb_expr::NoColumns, mode);
    }
}

/// A stand-in for the two `pkg/planner/core/expression_rewriter.go`
/// stages Go runs BEFORE ranger and this harness's pipeline lacks (they
/// belong to the planner-rewriter port track):
/// * `inToExpression`: a single-member `IN` becomes `EQ`;
/// * the implicit comparison cast: a DECIMAL constant compared against a
///   REAL column arrives at ranger as a DOUBLE.
fn planner_rewriter_stage(expr: &Expression) -> Expression {
    match expr {
        Expression::ScalarFunction(sf) => {
            let name = sf.func_name.lowercase();
            if name == "in" && sf.args.len() == 2 {
                let mut eq = sf.clone();
                eq.func_name = tidb_ast::CiString::new("eq");
                eq.args = sf.args.iter().map(planner_rewriter_stage).collect();
                return Expression::ScalarFunction(eq);
            }
            let mut rewritten = sf.clone();
            rewritten.args = sf.args.iter().map(planner_rewriter_stage).collect();
            if matches!(name, "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "nulleq") {
                let real_column = rewritten.args.iter().any(|arg| {
                    matches!(arg, Expression::Column(col)
                        if col.ret_type.as_ref().is_some_and(|ft| {
                            ft.eval_type() == tidb_datatype::EvalType::Real
                        }))
                });
                if real_column {
                    for arg in &mut rewritten.args {
                        if let Expression::Constant(c) = arg {
                            if let Datum::Decimal(d) = &c.value {
                                c.value = Datum::Real(d.to_f64());
                            }
                        }
                    }
                }
            }
            Expression::ScalarFunction(rewritten)
        }
        other => other.clone(),
    }
}

/// Go's per-case pipeline: `DetachCondAndBuildRangeForIndex` over the
/// chosen index's columns.
fn build_index_ranges_for(expr_text: &str, index_pos: usize) -> Result<String, String> {
    let table = UnsignedTable::new();
    let sql = format!("select * from t where {expr_text}");
    let stmt = tidb_parser::parse(&sql).map_err(|error| format!("parse: {error:?}"))?;
    let tidb_ast::Stmt::Query(query) = stmt else {
        return Err("not a query".to_owned());
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        return Err("not a select".to_owned());
    };
    let where_clause = select.where_clause.ok_or("no where")?;
    let rewritten = rewrite_expr_resolved(&where_clause, &table)
        .map_err(|error| format!("rewrite: {error:?}"))?;
    let ctx = tidb_expr::NoColumns;
    let builder = RealFunctionBuilder::new(&ctx);
    let conds: Vec<Expression> = split_cnf_items(&rewritten)
        .iter()
        .map(planner_rewriter_stage)
        .map(|cond| push_down_not(&cond, &builder))
        .collect();
    let index_cols = table.index_columns(index_pos);
    let lengths = vec![super::checker::UNSPECIFIED_LENGTH; index_cols.len()];
    let result = super::detacher::detach_cond_and_build_range_for_index(
        &conds,
        &index_cols,
        &lengths,
        0,
    )
    .map_err(|error| format!("detach: {error:?}"))?;
    Ok(ranges_to_go_string(&result.ranges))
}

/// Go `TestIndexRangeForUnsignedAndOverflow` (`ranger_test.go:314`), the
/// full case table.
#[test]
fn unsigned_and_overflow_index_ranges_match_go() {
    let cases: &[(usize, &str, &str)] = &[
        (6, "a = 1 and a = 2", "[]"),
        (0, "a not in (0, 1, 2)", "[(NULL,0) (2,+inf]]"),
        (0, "a not in (-1, 1, 2)", "[(NULL,1) (2,+inf]]"),
        (0, "a not in (-2, -1, 1, 2)", "[(NULL,1) (2,+inf]]"),
        (0, "a not in (111)", "[[-inf,111) (111,+inf]]"),
        (
            0,
            "a not in (1, 2, 9223372036854775810)",
            "[(NULL,1) (2,9223372036854775810) (9223372036854775810,+inf]]",
        ),
        (0, "a >= -2147483648", "[[0,+inf]]"),
        (0, "a > -2147483648", "[[0,+inf]]"),
        (0, "a != -2147483648", "[[0,+inf]]"),
        (0, "a < -1 or a < 1", "[[-inf,1)]"),
        (0, "a < -1 and a < 1", "[]"),
        (1, "decimal_unsigned > -100", "[[0,+inf]]"),
        (2, "float_unsigned > -100", "[[0,+inf]]"),
        (3, "double_unsigned > -100", "[[0,+inf]]"),
        (4, "col_int != 9223372036854775808", "[[-inf,+inf]]"),
        (4, "col_int > 9223372036854775808", "[]"),
        (4, "col_int < 9223372036854775808", "[[-inf,+inf]]"),
        (
            5,
            "col_float > 1000000000000000000000000000000000000000",
            "[]",
        ),
        (
            5,
            "col_float < -1000000000000000000000000000000000000000",
            "[]",
        ),
    ];
    let mut failures = Vec::new();
    for (index_pos, expr, expected) in cases {
        match build_index_ranges_for(expr, *index_pos) {
            Ok(got) if got == *expected => {}
            Ok(got) => failures.push(format!("{expr}: got {got}, want {expected}")),
            Err(error) => failures.push(format!("{expr}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Go `TestIndexRangeForYear`'s table: `t(a year(4), key(a))`.
struct YearTable {
    a: Column,
}

impl YearTable {
    fn new() -> Self {
        Self {
            a: Column::new(1, FieldType::new(FieldTypeCode::Year)),
        }
    }
}

impl ColumnResolver for YearTable {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        if name.eq_ignore_ascii_case("a") {
            return Some((0, self.a.ret_type.clone().expect("typed"), 1));
        }
        None
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let name = path.last()?;
        if name.eq_ignore_ascii_case("a") {
            return Some(self.a.clone());
        }
        None
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        tidb_expr::SessionTimeZone::utc()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        tidb_expr::fold_constant_in_mode(expression, &tidb_expr::NoColumns, mode);
    }
}

/// Go `TestIndexRangeForYear` (`ranger_test.go:876`, issue 20101): the
/// two-digit year adjustment, the out-of-range clamps, and the not-in
/// inversions over converted members.
#[test]
fn year_index_ranges_match_go() {
    let table = YearTable::new();
    let cases: &[(&str, &str)] = &[
        ("a not in (0, 1, 2)", "[(NULL,0) (0,2001) (2002,+inf]]"),
        ("a not in (-1, 1, 2)", "[(NULL,2001) (2002,+inf]]"),
        ("a not in (1, 2, 70)", "[(NULL,1970) (1970,2001) (2002,+inf]]"),
        ("a = 1 or a = 2 or a = 70", "[[1970,1970] [2001,2002]]"),
        ("a not in (99)", "[[-inf,1999) (1999,+inf]]"),
        ("a not in (1, 2, 15698)", "[(NULL,2001) (2002,+inf]]"),
        ("a >= -1000", "[[0,+inf]]"),
        ("a > -1000", "[[0,+inf]]"),
        ("a != 1", "[[-inf,2001) (2001,+inf]]"),
        ("a != 2156", "[[-inf,+inf]]"),
        ("a < 99 or a > 01", "[[-inf,1999) (2001,+inf]]"),
        ("a >= 70 and a <= 69", "[[1970,2069]]"),
    ];
    let mut failures = Vec::new();
    for (expr, expected) in cases {
        let got = (|| -> Result<String, String> {
            let sql = format!("select * from t where {expr}");
            let stmt =
                tidb_parser::parse(&sql).map_err(|error| format!("parse: {error:?}"))?;
            let tidb_ast::Stmt::Query(query) = stmt else {
                return Err("not a query".to_owned());
            };
            let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
                return Err("not a select".to_owned());
            };
            let where_clause = select.where_clause.ok_or("no where")?;
            let rewritten = rewrite_expr_resolved(&where_clause, &table)
                .map_err(|error| format!("rewrite: {error:?}"))?;
            let ctx = tidb_expr::NoColumns;
            let builder = RealFunctionBuilder::new(&ctx);
            let conds: Vec<Expression> = split_cnf_items(&rewritten)
                .iter()
                .map(planner_rewriter_stage)
                .map(|cond| push_down_not(&cond, &builder))
                .collect();
            let cols = [table.a.clone()];
            let lengths = [super::checker::UNSPECIFIED_LENGTH];
            let result = super::detacher::detach_cond_and_build_range_for_index(
                &conds, &cols, &lengths, 0,
            )
            .map_err(|error| format!("detach: {error:?}"))?;
            Ok(ranges_to_go_string(&result.ranges))
        })();
        match got {
            Ok(got) if got == *expected => {}
            Ok(got) => failures.push(format!("{expr}: got {got}, want {expected}")),
            Err(error) => failures.push(format!("{expr}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
