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
            // Go's `inToExpression` casts every list member to the column's
            // type and DEDUPS the converted members, first-seen order kept.
            if name == "in" {
                if let Some(Expression::Column(column)) = rewritten.args.first() {
                    let eval_type = column
                        .ret_type
                        .as_ref()
                        .map(tidb_datatype::FieldType::eval_type);
                    let mut seen = Vec::new();
                    let mut members = vec![rewritten.args[0].clone()];
                    for member in &rewritten.args[1..] {
                        let mut member = member.clone();
                        if let Expression::Constant(constant) = &mut member {
                            constant.value = convert_in_member(
                                &constant.value,
                                eval_type,
                            );
                        }
                        let key = match &member {
                            Expression::Constant(constant) => format!("{:?}", constant.value),
                            other => format!("{other:?}"),
                        };
                        if seen.contains(&key) {
                            continue;
                        }
                        seen.push(key);
                        members.push(member);
                    }
                    rewritten.args = members;
                }
            }
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

/// One IN-list member as Go's rewriter would deliver it: converted to the
/// column's evaluation type (string digits to INT/REAL, decimal to the
/// column family), or kept as written when no conversion applies.
fn convert_in_member(
    value: &Datum,
    eval_type: Option<tidb_datatype::EvalType>,
) -> Datum {
    use tidb_datatype::EvalType;
    match (eval_type, value) {
        (Some(EvalType::Int), Datum::String(text)) => {
            match String::from_utf8_lossy(text.bytes()).trim().parse::<i64>() {
                Ok(parsed) => Datum::Int(parsed),
                Err(_) => value.clone(),
            }
        }
        (Some(EvalType::Int), Datum::Decimal(decimal)) => {
            let real = decimal.to_f64();
            if real.fract() == 0.0 {
                Datum::Int(real as i64)
            } else {
                value.clone()
            }
        }
        (Some(EvalType::Real), Datum::String(text)) => {
            match String::from_utf8_lossy(text.bytes()).trim().parse::<f64>() {
                Ok(parsed) => Datum::Real(parsed),
                Err(_) => value.clone(),
            }
        }
        (Some(EvalType::Real), Datum::Decimal(decimal)) => Datum::Real(decimal.to_f64()),
        _ => value.clone(),
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

/// `create table t (a varchar(50), b varchar(50), index idx_a(a(2)),
/// index idx_ab(a(2), b(2)))` -- `TestPrefixIndexRangeScan`'s fixture.
struct PrefixScanTable {
    a: Column,
    b: Column,
}

impl PrefixScanTable {
    fn new() -> Self {
        let varchar = |unique_id: i64| {
            let mut ft = FieldType::new(FieldTypeCode::Varchar);
            ft.set_flen(50);
            ft.set_charset_name("utf8mb4");
            ft.set_collation_name("utf8mb4_bin");
            ft.set_collation(tidb_datatype::Collation::Utf8Mb4Bin);
            Column::new(unique_id, ft)
        };
        Self {
            a: varchar(1),
            b: varchar(2),
        }
    }
}

impl ColumnResolver for PrefixScanTable {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        if name.eq_ignore_ascii_case("a") {
            return Some((0, self.a.ret_type.clone().expect("typed"), 1));
        }
        if name.eq_ignore_ascii_case("b") {
            return Some((1, self.b.ret_type.clone().expect("typed"), 2));
        }
        None
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let name = path.last()?;
        if name.eq_ignore_ascii_case("a") {
            return Some(self.a.clone());
        }
        if name.eq_ignore_ascii_case("b") {
            return Some(self.b.clone());
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

/// Go `expression.StringifyExpressionsWithCtx` narrowed to the shapes the
/// ranger tables print: `[eq(test.t.a, aaa) gt(test.t.b, bb)]` -- columns
/// as their catalog `OrigName`, constants as their raw datum text, list
/// entries joined by one space.
fn stringify_conds(conds: &[Expression], column_name: &dyn Fn(i64) -> String) -> String {
    fn one(expr: &Expression, column_name: &dyn Fn(i64) -> String) -> String {
        match expr {
            Expression::Column(column) => column_name(column.unique_id),
            Expression::Constant(constant) => cond_datum(&constant.value),
            Expression::ScalarFunction(function) => {
                let arguments: Vec<String> = function
                    .args
                    .iter()
                    .map(|argument| one(argument, column_name))
                    .collect();
                format!("{}({})", function.func_name.lowercase(), arguments.join(", "))
            }
            other => format!("{other:?}"),
        }
    }
    let items: Vec<String> = conds.iter().map(|cond| one(cond, column_name)).collect();
    format!("[{}]", items.join(" "))
}

/// A condition constant's text: Go's `%v` of the datum -- strings RAW
/// (unquoted, escapes kept), numbers as digits.
fn cond_datum(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Decimal(value) => value.to_string(),
        Datum::Real(value) => super::types::go_g_float(*value),
        other => format!("{other:?}"),
    }
}

/// Go `TestPrefixIndexRangeScan` (`ranger_test.go:1037`): a prefix index
/// CUTS the range at the prefix length and RETAINS the whole predicate as
/// a filter -- both directions of `DetachCondAndBuildRangeForIndex`'s
/// answer, pinned as strings.
#[test]
fn prefix_index_range_scan_matches_go() {
    let table = PrefixScanTable::new();
    let column_name = |unique_id: i64| -> String {
        match unique_id {
            1 => "test.t.a".to_owned(),
            2 => "test.t.b".to_owned(),
            other => format!("Column#{other}"),
        }
    };
    // (indexPos, expr, accessConds, filterConds, resultStr); index 0 is
    // idx_a(a(2)), index 1 is idx_ab(a(2), b(2)).
    let cases: &[(usize, &str, &str, &str, &str)] = &[
        (
            0,
            "a > 'aa'",
            "[gt(test.t.a, aa)]",
            "[gt(test.t.a, aa)]",
            "[[\"aa\",+inf]]",
        ),
        (
            1,
            "a = 'aaa' and b > 'bb' and b < 'cc'",
            "[eq(test.t.a, aaa) gt(test.t.b, bb) lt(test.t.b, cc)]",
            "[eq(test.t.a, aaa) gt(test.t.b, bb) lt(test.t.b, cc)]",
            "[[\"aa\" \"bb\",\"aa\" \"cc\")]",
        ),
    ];
    let mut failures = Vec::new();
    for (index_pos, expr, want_access, want_filter, want_ranges) in cases {
        let got = (|| -> Result<(String, String, String), String> {
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
            let (cols, lengths): (Vec<Column>, Vec<i64>) = match index_pos {
                0 => (vec![table.a.clone()], vec![2]),
                _ => (vec![table.a.clone(), table.b.clone()], vec![2, 2]),
            };
            let result = super::detacher::detach_cond_and_build_range_for_index(
                &conds, &cols, &lengths, 0,
            )
            .map_err(|error| format!("detach: {error:?}"))?;
            Ok((
                stringify_conds(&result.access_conds, &column_name),
                stringify_conds(&result.remained_conds, &column_name),
                ranges_to_go_string(&result.ranges),
            ))
        })();
        match got {
            Ok((access, filter, ranges)) => {
                if access != *want_access {
                    failures.push(format!("{expr}: access {access}, want {want_access}"));
                }
                if filter != *want_filter {
                    failures.push(format!("{expr}: filter {filter}, want {want_filter}"));
                }
                if ranges != *want_ranges {
                    failures.push(format!("{expr}: ranges {ranges}, want {want_ranges}"));
                }
            }
            Err(error) => failures.push(format!("{expr}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// `TestIndexRange`'s eight-column table (`ranger_test.go:1107`):
/// `a varchar(50), b int, c double, d varchar(10), e binary(10),
/// f varchar(10) collate utf8mb4_general_ci, g enum('A','B','C') collate
/// utf8mb4_general_ci, h varchar(10) collate utf8_bin`, with the eight
/// indexes the cases address by position.
struct IndexRangeTable {
    columns: Vec<Column>,
}

impl IndexRangeTable {
    fn new() -> Self {
        let varchar = |unique_id: i64, flen: i64, collation_name: &str,
                        collation: tidb_datatype::Collation| {
            let mut ft = FieldType::new(FieldTypeCode::Varchar);
            ft.set_flen(flen);
            ft.set_charset_name(if collation_name.starts_with("utf8_") {
                "utf8"
            } else {
                "utf8mb4"
            });
            ft.set_collation_name(collation_name);
            ft.set_collation(collation);
            Column::new(unique_id, ft)
        };
        let a = varchar(1, 50, "utf8mb4_bin", tidb_datatype::Collation::Utf8Mb4Bin);
        let b = Column::new(2, FieldType::new(FieldTypeCode::LongLong));
        let c = Column::new(3, FieldType::new(FieldTypeCode::Double));
        let d = varchar(4, 10, "utf8mb4_bin", tidb_datatype::Collation::Utf8Mb4Bin);
        let e = Column::new(5, {
            // `binary(10)`: the CHAR family with the binary charset.
            let mut ft = FieldType::new(FieldTypeCode::String);
            ft.set_flen(10);
            ft.set_charset_name("binary");
            ft.set_collation_name("binary");
            ft.set_collation(tidb_datatype::Collation::Binary);
            ft.set_flags(ft.flags() | FieldTypeFlags::BINARY);
            ft
        });
        let f = varchar(
            6,
            10,
            "utf8mb4_general_ci",
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
        );
        let g = Column::new(7, {
            let mut ft = FieldType::new(FieldTypeCode::Enum);
            ft.set_elems(vec!["A".into(), "B".into(), "C".into()]);
            ft.set_charset_name("utf8mb4");
            ft.set_collation_name("utf8mb4_general_ci");
            ft.set_collation(tidb_datatype::Collation::Utf8Mb4GeneralCi);
            ft
        });
        let h = varchar(8, 10, "utf8_bin", tidb_datatype::Collation::Utf8Bin);
        Self {
            columns: vec![a, b, c, d, e, f, g, h],
        }
    }

    fn name_to_offset(name: &str) -> Option<usize> {
        Some(match name {
            "a" => 0,
            "b" => 1,
            "c" => 2,
            "d" => 3,
            "e" => 4,
            "f" => 5,
            "g" => 6,
            "h" => 7,
            _ => return None,
        })
    }

    /// The `(columns, prefix lengths)` of the index at `indexPos`.
    fn index(&self, index_pos: usize) -> (Vec<Column>, Vec<i64>) {
        let unspec = super::checker::UNSPECIFIED_LENGTH;
        let col = |offset: usize| self.columns[offset].clone();
        match index_pos {
            // idx_ab(a(50), b): a's declared length EQUALS its flen, which
            // Go normalizes to a FULL column (no prefix cut, no union pass).
            0 => (vec![col(0), col(1)], vec![unspec, unspec]),
            1 => (vec![col(2), col(0)], vec![unspec, unspec]), // idx_cb(c, a)
            2 => (vec![col(3)], vec![2]),                  // idx_d(d(2))
            3 => (vec![col(4)], vec![2]),                  // idx_e(e(2))
            4 => (vec![col(5)], vec![unspec]),             // idx_f(f)
            5 => (vec![col(3), col(4)], vec![2, unspec]),  // idx_de(d(2), e)
            6 => (vec![col(6)], vec![unspec]),             // idx_g(g)
            _ => (vec![col(7)], vec![3]),                  // idx_h(h(3))
        }
    }
}

impl ColumnResolver for IndexRangeTable {
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

/// Go `TestIndexRange` (`ranger_test.go:1107`), the full case table:
/// LIKE lowering, IN dedup/merge, DNF detachment, prefix cuts through
/// multi-byte runes, `utf8mb4_general_ci` sort keys, an enum key, and the
/// CAST-AS-BINARY compare over a CI column.
#[test]
fn index_ranges_match_go() {
    let table = IndexRangeTable::new();
    let column_name = |unique_id: i64| -> String {
        let name = ["a", "b", "c", "d", "e", "f", "g", "h"]
            .get((unique_id - 1) as usize)
            .copied()
            .unwrap_or("?");
        format!("test.t.{name}")
    };
    let cases: &[(usize, &str, &str, &str, &str)] = &[
        (0, r"a LIKE 'abc%'", r"[like(test.t.a, abc%, 92)]", r"[like(test.t.a, abc%, 92)]", r#"[["abc","abd")]"#),
        (0, r"a LIKE 'abc_'", r"[like(test.t.a, abc_, 92)]", r"[like(test.t.a, abc_, 92)]", r#"[["abc","abd")]"#),
        (0, r"a LIKE 'abc'", r"[like(test.t.a, abc, 92)]", r"[like(test.t.a, abc, 92)]", r#"[["abc","abc"]]"#),
        (0, r#"a LIKE "ab\_c""#, r"[like(test.t.a, ab\_c, 92)]", r"[like(test.t.a, ab\_c, 92)]", r#"[["ab_c","ab_c"]]"#),
        (0, r"a LIKE '%'", r"[]", r"[like(test.t.a, %, 92)]", r"[[NULL,+inf]]"),
        (0, r"a LIKE '\%a'", r"[like(test.t.a, \%a, 92)]", r"[like(test.t.a, \%a, 92)]", r#"[["%a","%a"]]"#),
        (0, r#"a LIKE "\\""#, r"[like(test.t.a, \, 92)]", r"[like(test.t.a, \, 92)]", r#"[["\\","\\"]]"#),
        (0, r#"a LIKE "\\\\a%""#, r"[like(test.t.a, \\a%, 92)]", r"[like(test.t.a, \\a%, 92)]", r#"[["\\a","\\b")]"#),
        (0, r"a > NULL", r"[gt(test.t.a, <nil>)]", r"[]", r"[]"),
        (0, r"a = 'a' and b in (1, 2, 3)", r"[eq(test.t.a, a) in(test.t.b, 1, 2, 3)]", r"[]", r#"[["a" 1,"a" 1] ["a" 2,"a" 2] ["a" 3,"a" 3]]"#),
        (0, r"a = 'a' and b not in (1, 2, 3)", r"[eq(test.t.a, a) not(in(test.t.b, 1, 2, 3))]", r"[]", r#"[("a" NULL,"a" 1) ("a" 3,"a" +inf]]"#),
        (0, r"a in ('a') and b in ('1', 2.0, NULL)", r"[eq(test.t.a, a) in(test.t.b, 1, 2, <nil>)]", r"[]", r#"[["a" 1,"a" 1] ["a" 2,"a" 2]]"#),
        (1, r"c in ('1.1', 1, 1.1) and a in ('1', 'a', NULL)", r"[in(test.t.c, 1.1, 1) in(test.t.a, 1, a, <nil>)]", r"[]", r#"[[1 "1",1 "1"] [1 "a",1 "a"] [1.1 "1",1.1 "1"] [1.1 "a",1.1 "a"]]"#),
        (1, r"c in (1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)", r"[in(test.t.c, 1, 2, 3, 4)]", r"[]", r"[[1,1] [2,2] [3,3] [4,4]]"),
        (1, r"c not in (1, 2, 3)", r"[not(in(test.t.c, 1, 2, 3))]", r"[]", r"[(NULL,1) (1,2) (2,3) (3,+inf]]"),
        (1, r"c in (1, 2) and c in (1, 3)", r"[eq(test.t.c, 1)]", r"[]", r"[[1,1]]"),
        (1, r"c = 1 and c = 2", r"[]", r"[]", r"[]"),
        (0, r"a in (NULL)", r"[eq(test.t.a, <nil>)]", r"[]", r"[]"),
        (0, r"a not in (NULL, '1', '2', '3')", r"[not(in(test.t.a, <nil>, 1, 2, 3))]", r"[]", r"[]"),
        (0, r"not (a not in (NULL, '1', '2', '3') and a > '2')", r"[or(in(test.t.a, <nil>, 1, 2, 3), le(test.t.a, 2))]", r"[]", r#"[[-inf,"2"] ["3","3"]]"#),
        (0, r"not (a not in (NULL) and a > '2')", r"[or(eq(test.t.a, <nil>), le(test.t.a, 2))]", r"[]", r#"[[-inf,"2"]]"#),
        (0, r"not (a not in (NULL) or a > '2')", r"[and(eq(test.t.a, <nil>), le(test.t.a, 2))]", r"[]", r"[]"),
        (0, r"(a > 'b' and a < 'bbb') or (a < 'cb' and a > 'a')", r"[or(and(gt(test.t.a, b), lt(test.t.a, bbb)), and(lt(test.t.a, cb), gt(test.t.a, a)))]", r"[]", r#"[("a","cb")]"#),
        (0, r"(a > 'a' and a < 'b') or (a >= 'b' and a < 'c')", r"[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]", r"[]", r#"[("a","c")]"#),
        (0, r"(a > 'a' and a < 'b' and b < 1) or (a >= 'b' and a < 'c')", r"[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]", r"[or(and(and(gt(test.t.a, a), lt(test.t.a, b)), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]", r#"[("a","c")]"#),
        (0, r"(a in ('a', 'b') and b < 1) or (a >= 'b' and a < 'c')", r"[or(and(in(test.t.a, a, b), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]", r"[]", r#"[["a" -inf,"a" 1) ["b","c")]"#),
        (0, r"(a > 'a') or (c > 1)", r"[]", r"[or(gt(test.t.a, a), gt(test.t.c, 1))]", r"[[NULL,+inf]]"),
        (2, r#"d = "你好啊""#, r"[eq(test.t.d, 你好啊)]", r"[eq(test.t.d, 你好啊)]", r#"[["你好","你好"]]"#),
        (3, r#"e = "你好啊""#, r"[eq(test.t.e, 你好啊)]", r"[eq(test.t.e, 你好啊)]", r#"[["\xe4\xbd","\xe4\xbd"]]"#),
        (2, r#"d in ("你好啊", "再见")"#, r"[in(test.t.d, 你好啊, 再见)]", r"[in(test.t.d, 你好啊, 再见)]", r#"[["你好","你好"] ["再见","再见"]]"#),
        (2, r#"d not in ("你好啊")"#, r"[]", r"[ne(test.t.d, 你好啊)]", r"[[NULL,+inf]]"),
        (2, r#"d < "你好" || d > "你好""#, r"[or(lt(test.t.d, 你好), gt(test.t.d, 你好))]", r"[or(lt(test.t.d, 你好), gt(test.t.d, 你好))]", r"[[-inf,+inf]]"),
        (2, r#"not(d < "你好" || d > "你好")"#, r"[and(ge(test.t.d, 你好), le(test.t.d, 你好))]", r"[and(ge(test.t.d, 你好), le(test.t.d, 你好))]", r#"[["你好","你好"]]"#),
        (4, r"f >= 'a' and f <= 'B'", r"[ge(test.t.f, a) le(test.t.f, B)]", r"[]", r#"[["\x00A","\x00B"]]"#),
        (4, r"f in ('a', 'B')", r"[in(test.t.f, a, B)]", r"[]", r#"[["\x00A","\x00A"] ["\x00B","\x00B"]]"#),
        (4, r"f = 'a' and f = 'B' collate utf8mb4_bin", r"[eq(test.t.f, a)]", r"[eq(test.t.f, B)]", r#"[["\x00A","\x00A"]]"#),
        (4, r"f like '@%' collate utf8mb4_bin", r"[]", r"[like(test.t.f, @%, 92)]", r"[[NULL,+inf]]"),
        (5, r"d in ('aab', 'aac') and e = 'a'", r"[in(test.t.d, aab, aac) eq(test.t.e, a)]", r"[in(test.t.d, aab, aac)]", r#"[["aa" "a","aa" "a"]]"#),
        (6, r"g = 'a'", r"[eq(test.t.g, a)]", r"[]", r#"[["A","A"]]"#),
        (7, r"h LIKE 'ÿÿ%'", r"[like(test.t.h, ÿÿ%, 92)]", r"[like(test.t.h, ÿÿ%, 92)]", r#"[["ÿÿ","ÿ\xc3\xc0")]"#),
        (4, r"f = cast('a' as binary)", r"[eq(test.t.f, a)]", r"[eq(test.t.f, a)]", r#"[["\x00A","\x00A"]]"#),
        (4, r"f in (cast('a' as binary), cast('B' as binary))", r"[in(test.t.f, a, B)]", r"[in(test.t.f, a, B)]", r#"[["\x00A","\x00A"] ["\x00B","\x00B"]]"#),
    ];
    let mut failures = Vec::new();
    for (index_pos, expr, want_access, want_filter, want_ranges) in cases {
        let got = (|| -> Result<(String, String, String), String> {
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
            // Go's PushDownNot builds through NewFunction, which DERIVES
            // collation; this crate's `new_function` deliberately defers
            // that, so the rewriter's tree walk runs here instead.
            let conds: Vec<Expression> = split_cnf_items(&rewritten)
                .iter()
                .map(planner_rewriter_stage)
                .map(|cond| push_down_not(&cond, &builder))
                .map(|mut cond| {
                    tidb_expr::rewriter::derive_tree_collation(&mut cond)
                        .map(|()| cond)
                        .map_err(|error| format!("collation: {error:?}"))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let (cols, lengths) = table.index(*index_pos);
            let result = super::detacher::detach_cond_and_build_range_for_index(
                &conds, &cols, &lengths, 0,
            )
            .map_err(|error| format!("detach: {error:?}"))?;
            Ok((
                stringify_conds(&result.access_conds, &column_name),
                stringify_conds(&result.remained_conds, &column_name),
                ranges_to_go_string(&result.ranges),
            ))
        })();
        match got {
            Ok((access, filter, ranges)) => {
                if access != *want_access {
                    failures.push(format!("{expr}: access {access}, want {want_access}"));
                }
                if filter != *want_filter {
                    failures.push(format!("{expr}: filter {filter}, want {want_filter}"));
                }
                if ranges != *want_ranges {
                    failures.push(format!("{expr}: ranges {ranges}, want {want_ranges}"));
                }
            }
            Err(error) => failures.push(format!("{expr}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

/// Go `TestBinCollationRangeForIndex` (`ranger_test.go:2636`): the
/// `DetachSimpleCondAndBuildRangeForIndex` entry over a CI column compared
/// against `CAST(... AS BINARY)` -- the approximate EQ range is built from
/// the sort key AND the predicate stays a filter (the `shouldReserve` leg
/// of the non-DNF branch).
#[test]
fn bin_collation_simple_detach_matches_go() {
    let table = IndexRangeTable::new();
    let column_name = |unique_id: i64| -> String {
        let name = ["a", "b", "c", "d", "e", "f", "g", "h"]
            .get((unique_id - 1) as usize)
            .copied()
            .unwrap_or("?");
        format!("test.t.{name}")
    };
    let sql = "select * from t where f = cast('abc' as binary)";
    let stmt = tidb_parser::parse(sql).expect("parses");
    let tidb_ast::Stmt::Query(query) = stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("not a select");
    };
    let rewritten = rewrite_expr_resolved(&select.where_clause.expect("where"), &table)
        .expect("rewrites");
    let conds = split_cnf_items(&rewritten);
    let cols = vec![table.columns[5].clone()];
    let lengths = vec![super::checker::UNSPECIFIED_LENGTH];
    let (ranges, access_conds, remained_conds) =
        super::detacher::detach_simple_cond_and_build_range_for_index(&conds, &cols, &lengths, 0)
            .expect("detaches");
    assert_eq!(
        stringify_conds(&access_conds, &column_name),
        "[eq(test.t.f, abc)]"
    );
    assert_eq!(
        stringify_conds(&remained_conds, &column_name),
        "[eq(test.t.f, abc)]"
    );
    assert_eq!(
        ranges_to_go_string(&ranges),
        "[[\"\\x00A\\x00B\\x00C\",\"\\x00A\\x00B\\x00C\"]]"
    );
}

/// `TestIssue40997`'s table: `dt char(8) utf8mb4_bin NOT NULL, db_id
/// bigint NOT NULL, tbl_id bigint NOT NULL`, unique index `(dt, db_id,
/// tbl_id)`.
struct Issue40997Table {
    columns: Vec<Column>,
}

impl Issue40997Table {
    fn new() -> Self {
        let dt = Column::new(1, {
            let mut ft = FieldType::new(FieldTypeCode::String);
            ft.set_flen(8);
            ft.set_charset_name("utf8mb4");
            ft.set_collation_name("utf8mb4_bin");
            ft.set_collation(tidb_datatype::Collation::Utf8Mb4Bin);
            ft.set_flags(ft.flags() | FieldTypeFlags::NOT_NULL);
            ft
        });
        let bigint = |unique_id: i64| {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            ft.set_flags(ft.flags() | FieldTypeFlags::NOT_NULL);
            Column::new(unique_id, ft)
        };
        Self {
            columns: vec![dt, bigint(2), bigint(3)],
        }
    }
}

impl ColumnResolver for Issue40997Table {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let offset = match name.to_ascii_lowercase().as_str() {
            "dt" => 0,
            "db_id" => 1,
            "tbl_id" => 2,
            _ => return None,
        };
        let column = &self.columns[offset];
        Some((
            offset,
            column.ret_type.clone().expect("typed"),
            column.unique_id,
        ))
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let name = path.last()?;
        let offset = match name.to_ascii_lowercase().as_str() {
            "dt" => 0,
            "db_id" => 1,
            "tbl_id" => 2,
            _ => return None,
        };
        Some(self.columns[offset].clone())
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        tidb_expr::SessionTimeZone::utc()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        tidb_expr::fold_constant_in_mode(expression, &tidb_expr::NoColumns, mode);
    }
}

/// Go `TestIssue40997` (`ranger_test.go:2473`), the ranger-observable
/// core: the three-disjunct DNF detaches into exactly the two ranges the
/// captured EXPLAIN prints -- the third disjunct's `db_id > '62812' AND
/// db_id < '62813'` is an EMPTY integer interval and drops.
#[test]
fn issue_40997_dnf_ranges_match_go() {
    let table = Issue40997Table::new();
    let sql = "select * from t where (dt = '20210112' and db_id = '62812' and tbl_id > '228892694') \
               or (dt = '20210112' and db_id = '62813' and tbl_id <= '226785696') \
               or (dt = '20210112' and db_id > '62812' and db_id < '62813')";
    let stmt = tidb_parser::parse(sql).expect("parses");
    let tidb_ast::Stmt::Query(query) = stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("not a select");
    };
    let rewritten = rewrite_expr_resolved(&select.where_clause.expect("where"), &table)
        .expect("rewrites");
    let conds = split_cnf_items(&rewritten);
    let lengths = vec![super::checker::UNSPECIFIED_LENGTH; 3];
    let result = super::detacher::detach_cond_and_build_range_for_index(
        &conds,
        &table.columns,
        &lengths,
        0,
    )
    .expect("detaches");
    assert_eq!(
        ranges_to_go_string(&result.ranges),
        "[(\"20210112\" 62812 228892694,\"20210112\" 62812 +inf] [\"20210112\" 62813 -inf,\"20210112\" 62813 226785696]]"
    );
}

/// `TestMinAccessCondsForDNFCond`'s table: `a int, b int, c int, d int`
/// with `ia(a), ib(b), ic(c), iabc(a,b,c), iab(a,b)` at positions 0-4.
struct MinAccessTable {
    columns: Vec<Column>,
}

impl MinAccessTable {
    fn new() -> Self {
        let int_col =
            |unique_id: i64| Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
        Self {
            columns: (1..=4).map(int_col).collect(),
        }
    }

    fn index(&self, index_pos: usize) -> Vec<Column> {
        let col = |offset: usize| self.columns[offset].clone();
        match index_pos {
            0 => vec![col(0)],
            1 => vec![col(1)],
            2 => vec![col(2)],
            3 => vec![col(0), col(1), col(2)],
            _ => vec![col(0), col(1)],
        }
    }
}

impl ColumnResolver for MinAccessTable {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let offset = match name.to_ascii_lowercase().as_str() {
            "a" => 0,
            "b" => 1,
            "c" => 2,
            "d" => 3,
            _ => return None,
        };
        let column = &self.columns[offset];
        Some((
            offset,
            column.ret_type.clone().expect("typed"),
            column.unique_id,
        ))
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let name = path.last()?;
        let offset = match name.to_ascii_lowercase().as_str() {
            "a" => 0,
            "b" => 1,
            "c" => 2,
            "d" => 3,
            _ => return None,
        };
        Some(self.columns[offset].clone())
    }

    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        tidb_expr::SessionTimeZone::utc()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        tidb_expr::fold_constant_in_mode(expression, &tidb_expr::NoColumns, mode);
    }
}

/// Go `TestMinAccessCondsForDNFCond` (`ranger_test.go:2532`): how many
/// access conditions the WEAKEST disjunct of a detached DNF contributes --
/// the fix-44389 cost signal -- alongside the detached access conds.
#[test]
fn min_access_conds_for_dnf_match_go() {
    let table = MinAccessTable::new();
    let column_name = |unique_id: i64| -> String {
        let name = ["a", "b", "c", "d"]
            .get((unique_id - 1) as usize)
            .copied()
            .unwrap_or("?");
        format!("test.t.{name}")
    };
    let cases: &[(usize, &str, &str, i64)] = &[
        (0, "a = 1", "[eq(test.t.a, 1)]", 0),
        (
            0,
            "a = 1 or a = 2 or a = 3",
            "[or(eq(test.t.a, 1), or(eq(test.t.a, 2), eq(test.t.a, 3)))]",
            1,
        ),
        (0, "a = 1 or b = 2 or c = 3", "[]", 0),
        (
            0,
            "(a=1 and b=2) or (a=3 and b=4) or (a=5 and b=6 and c=7)",
            "[or(eq(test.t.a, 1), or(eq(test.t.a, 3), eq(test.t.a, 5)))]",
            1,
        ),
        (
            1,
            "(a=1 and b=2) or (a=3 and b=4) or (a=5 and b=6 and c=7)",
            "[or(eq(test.t.b, 2), or(eq(test.t.b, 4), eq(test.t.b, 6)))]",
            1,
        ),
        (
            2,
            "(a=1 and b=2) or (a=3 and b=4) or (a=5 and b=6 and c=7)",
            "[]",
            0,
        ),
        (
            3,
            "(a=1 and b=2) or (a=3 and b=4) or (a=5 and b=6 and c=7)",
            "[or(and(eq(test.t.a, 1), eq(test.t.b, 2)), or(and(eq(test.t.a, 3), eq(test.t.b, 4)), and(eq(test.t.a, 5), and(eq(test.t.b, 6), eq(test.t.c, 7)))))]",
            2,
        ),
        (
            4,
            "(a=1 and b=2) or (a=3 and b=4) or (a=5 and b=6 and c=7)",
            "[or(and(eq(test.t.a, 1), eq(test.t.b, 2)), or(and(eq(test.t.a, 3), eq(test.t.b, 4)), and(eq(test.t.a, 5), eq(test.t.b, 6))))]",
            2,
        ),
        (
            3,
            "(a=1) or (a=3 and b=4) or (a=5 and b=6 and c=7)",
            "[or(eq(test.t.a, 1), or(and(eq(test.t.a, 3), eq(test.t.b, 4)), and(eq(test.t.a, 5), and(eq(test.t.b, 6), eq(test.t.c, 7)))))]",
            1,
        ),
    ];
    let mut failures = Vec::new();
    for (index_pos, expr, want_access, want_min) in cases {
        let got = (|| -> Result<(String, i64), String> {
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
            let conds = split_cnf_items(&rewritten);
            let cols = table.index(*index_pos);
            let lengths = vec![super::checker::UNSPECIFIED_LENGTH; cols.len()];
            let result = super::detacher::detach_cond_and_build_range_for_index(
                &conds, &cols, &lengths, 0,
            )
            .map_err(|error| format!("detach: {error:?}"))?;
            Ok((
                stringify_conds(&result.access_conds, &column_name),
                result.min_access_conds_for_dnf_cond,
            ))
        })();
        match got {
            Ok((access, min)) => {
                if access != *want_access {
                    failures.push(format!("{expr}@{index_pos}: access {access}, want {want_access}"));
                }
                if min != *want_min {
                    failures.push(format!("{expr}@{index_pos}: min {min}, want {want_min}"));
                }
            }
            Err(error) => failures.push(format!("{expr}@{index_pos}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

/// Go `TestShardIndexFuncSuites` (`ranger_test.go:1752`): the shard-index
/// GC-column family's unit surface -- `IsValidShardIndex`,
/// `ExtractColumnsFromExpr`, `NeedAddColumn4InCond`/`4EqCond`,
/// `NeedAddGcColumn4ShardIndex`, and `AddExpr4EqAndInCondition`'s three
/// rewrite shapes with the pinned `tidb_shard` hash values 214 and 122.
#[test]
fn shard_index_func_suites_match_go() {
    use super::detacher::{
        add_expr4_eq_and_in_condition, extract_columns_from_expr, is_valid_shard_index,
        need_add_column4_eq_cond, need_add_column4_in_cond, need_add_gc_column4_shard_index,
    };

    let longlong = || FieldType::new(FieldTypeCode::LongLong);
    let column = |unique_id: i64| Column::new(unique_id, longlong());
    let ctx = tidb_expr::NoColumns;
    let build = |name: &str, args: Vec<Expression>| {
        tidb_expr::new_function::new_function(&ctx, name, longlong(), args).expect("builds")
    };

    let col0 = column(0);
    let col1 = column(1);
    // col2 is the GC column: VirtualExpr = tidb_shard(col0).
    let mut col2 = column(2);
    col2.virtual_expr = Some(Box::new(build(
        "tidb_shard",
        vec![Expression::Column(col0.clone())],
    )));
    // col3's virtual expression is abs(col0) -- NOT a shard prefix.
    let mut col3 = column(3);
    col3.virtual_expr = Some(Box::new(build("abs", vec![Expression::Column(col0.clone())])));
    let col4 = column(4);

    // ---- IsValidShardIndex ----
    assert!(!is_valid_shard_index(&[]));
    assert!(!is_valid_shard_index(&[col2.clone()]));
    assert!(!is_valid_shard_index(&[col0.clone(), col1.clone()]));
    assert!(!is_valid_shard_index(&[col2.clone(), col1.clone()]));
    assert!(!is_valid_shard_index(&[col3.clone(), col0.clone()]));
    assert!(is_valid_shard_index(&[col2.clone(), col0.clone()]));

    // ---- ExtractColumnsFromExpr ----
    let con1 = Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::Int(1),
        longlong(),
    ));
    let con5 = Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::Int(5),
        longlong(),
    ));
    let expr_eq = build("eq", vec![Expression::Column(col0.clone()), con1.clone()]);
    let expr_in = build(
        "in",
        vec![Expression::Column(col0.clone()), con1.clone(), con5.clone()],
    );
    assert_eq!(extract_columns_from_expr(None).len(), 0);
    let Expression::ScalarFunction(eq_function) = &expr_eq else {
        panic!("eq is a scalar function");
    };
    assert_eq!(extract_columns_from_expr(Some(eq_function)).len(), 1);
    // (col0 = 1 and abs(col0)-col3 > 1) or (col4 < 5 and 5): three distinct
    // columns underneath.
    let expr_gt = build("gt", vec![Expression::Column(col3.clone()), con1.clone()]);
    let and_expr1 = build("and", vec![expr_eq.clone(), expr_gt]);
    let expr_lt = build("lt", vec![Expression::Column(col4.clone()), con5.clone()]);
    let and_expr2 = build("and", vec![expr_lt, con5.clone()]);
    let or_expr2 = build("or", vec![and_expr1, and_expr2]);
    let Expression::ScalarFunction(or_function) = &or_expr2 else {
        panic!("or is a scalar function");
    };
    assert_eq!(extract_columns_from_expr(Some(or_function)).len(), 3);

    // ---- NeedAddColumn4InCond ----
    let Expression::ScalarFunction(in_function) = &expr_in else {
        panic!("in is a scalar function");
    };
    let shard_cols = vec![col2.clone(), col0.clone()];
    let access: Vec<Option<Expression>> = vec![None, Some(expr_in.clone())];
    assert!(need_add_column4_in_cond(&shard_cols, &access, Some(in_function)));
    assert!(!need_add_column4_in_cond(&[], &access, Some(in_function)));
    assert!(!need_add_column4_in_cond(&shard_cols, &[], Some(in_function)));
    assert!(!need_add_column4_in_cond(&shard_cols, &access, None));
    // col1 in (1, 5): not the shard function's column.
    let expr_in2 = build(
        "in",
        vec![Expression::Column(col1.clone()), con1.clone(), con5.clone()],
    );
    let Expression::ScalarFunction(in2_function) = &expr_in2 else {
        panic!("in is a scalar function");
    };
    let access2: Vec<Option<Expression>> = vec![None, Some(expr_in2.clone())];
    assert!(!need_add_column4_in_cond(&shard_cols, &access2, Some(in2_function)));
    // col1 in (1, col1): a non-constant member.
    let expr_in3 = build(
        "in",
        vec![
            Expression::Column(col1.clone()),
            con1.clone(),
            Expression::Column(col1.clone()),
        ],
    );
    let in3_function = match &expr_in3 {
        Expression::ScalarFunction(function) => function.clone(),
        _ => panic!("in is a scalar function"),
    };
    let access3: Vec<Option<Expression>> = vec![None, Some(expr_in3)];
    let in3_function = &in3_function;
    assert!(!need_add_column4_in_cond(&shard_cols, &access3, Some(in3_function)));

    // ---- NeedAddColumn4EqCond / NeedAddGcColumn4ShardIndex ----
    let eq_access: Vec<Option<Expression>> = vec![None, Some(expr_eq.clone())];
    assert!(!need_add_column4_eq_cond(&shard_cols, &eq_access, &[]));
    assert!(!need_add_gc_column4_shard_index(&shard_cols, &[], &[]));

    // ---- AddExpr4EqAndInCondition ----
    let column_name = |unique_id: i64| format!("Column#{unique_id}");
    let expr_in4 = build("in", vec![Expression::Column(col0.clone()), con1.clone()]);
    let cases: &[(&Expression, &str)] = &[
        (&expr_eq, "[eq(Column#2, 214) eq(Column#0, 1)]"),
        (&expr_in4, "[and(eq(Column#2, 214), eq(Column#0, 1))]"),
        (
            &expr_in,
            "[or(and(eq(Column#2, 214), eq(Column#0, 1)), and(eq(Column#2, 122), eq(Column#0, 5)))]",
        ),
    ];
    for (input, want) in cases {
        let rewritten =
            add_expr4_eq_and_in_condition(std::slice::from_ref(*input), &shard_cols)
                .expect("rewrites");
        assert_eq!(&stringify_conds(&rewritten, &column_name), want);
    }
}

/// Go `TestRangeFallbackForDetachCondAndBuildRangeForIndex`
/// (`ranger_test.go:1917`), the quota ladder: each step's quota is one
/// byte under the previous result's `MemUsage`, and the detachment backs
/// off one column at a time -- full three-column ranges, then the eq/in
/// prefix pairs, then single points, then the full range with EVERY
/// condition remained. (Go's slice-aliasing sub-tests are not ported:
/// `Vec` cannot alias its peers by construction.)
#[test]
fn range_fallback_ladder_matches_go() {
    let table = MinAccessTable::new();
    let column_name = |unique_id: i64| -> String {
        let name = ["a", "b", "c", "d"]
            .get((unique_id - 1) as usize)
            .copied()
            .unwrap_or("?");
        format!("test.t1.{name}")
    };
    let sql = "select * from t1 where a in (10,20,30) and b in (40,50,60) and c >= 70 and c <= 80";
    let stmt = tidb_parser::parse(sql).expect("parses");
    let tidb_ast::Stmt::Query(query) = stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("not a select");
    };
    let rewritten = rewrite_expr_resolved(&select.where_clause.expect("where"), &table)
        .expect("rewrites");
    let conds = split_cnf_items(&rewritten);
    assert_eq!(conds.len(), 4);
    let cols: Vec<Column> = [0, 1, 2].iter().map(|o| table.columns[*o].clone()).collect();
    let lengths = vec![super::checker::UNSPECIFIED_LENGTH; 3];

    let detach = |quota: i64| {
        super::detacher::detach_cond_and_build_range_for_index(&conds, &cols, &lengths, quota)
            .expect("detaches")
    };
    let check = |result: &super::detacher::DetachRangeResult,
                 want_access: &str,
                 want_remained: &str,
                 want_ranges: &str| {
        assert_eq!(stringify_conds(&result.access_conds, &column_name), want_access);
        assert_eq!(
            stringify_conds(&result.remained_conds, &column_name),
            want_remained
        );
        assert_eq!(ranges_to_go_string(&result.ranges), want_ranges);
    };

    let full = detach(0);
    check(
        &full,
        "[in(test.t1.a, 10, 20, 30) in(test.t1.b, 40, 50, 60) ge(test.t1.c, 70) le(test.t1.c, 80)]",
        "[]",
        "[[10 40 70,10 40 80] [10 50 70,10 50 80] [10 60 70,10 60 80] [20 40 70,20 40 80] [20 50 70,20 50 80] [20 60 70,20 60 80] [30 40 70,30 40 80] [30 50 70,30 50 80] [30 60 70,30 60 80]]",
    );

    let quota = super::types::ranges_mem_usage(&full.ranges) - 1;
    let two = detach(quota);
    check(
        &two,
        "[in(test.t1.a, 10, 20, 30) in(test.t1.b, 40, 50, 60)]",
        "[ge(test.t1.c, 70) le(test.t1.c, 80)]",
        "[[10 40,10 40] [10 50,10 50] [10 60,10 60] [20 40,20 40] [20 50,20 50] [20 60,20 60] [30 40,30 40] [30 50,30 50] [30 60,30 60]]",
    );

    let quota = super::types::ranges_mem_usage(&two.ranges) - 1;
    let one = detach(quota);
    check(
        &one,
        "[in(test.t1.a, 10, 20, 30)]",
        "[in(test.t1.b, 40, 50, 60) ge(test.t1.c, 70) le(test.t1.c, 80)]",
        "[[10,10] [20,20] [30,30]]",
    );

    let quota = super::types::ranges_mem_usage(&one.ranges) - 1;
    let none = detach(quota);
    check(
        &none,
        "[]",
        "[ge(test.t1.c, 70) le(test.t1.c, 80) in(test.t1.b, 40, 50, 60) in(test.t1.a, 10, 20, 30)]",
        "[[NULL,+inf]]",
    );
}

/// Go `TestRangeFallbackForBuildTableRange` (`ranger_test.go:2246`) and
/// `TestRangeFallbackForBuildColumnRange` (`:2282`): under quota the
/// table path answers `[[-inf,+inf]]` and the column path `[[NULL,+inf]]`
/// (a NOT NULL column still `[[-inf,+inf]]`), with the IN moved whole to
/// the remained side.
#[test]
fn build_range_fallbacks_match_go() {
    // ---- BuildTableRange over `a int primary key` ----
    let table = MinAccessTable::new();
    let column_name = |unique_id: i64| -> String {
        let name = ["a", "b", "c", "d"]
            .get((unique_id - 1) as usize)
            .copied()
            .unwrap_or("?");
        format!("test.t.{name}")
    };
    let parse_conds = |sql: &str| -> Vec<Expression> {
        let stmt = tidb_parser::parse(sql).expect("parses");
        let tidb_ast::Stmt::Query(query) = stmt else {
            panic!("not a query");
        };
        let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
            panic!("not a select");
        };
        let rewritten = rewrite_expr_resolved(&select.where_clause.expect("where"), &table)
            .expect("rewrites");
        split_cnf_items(&rewritten)
    };
    let conds = parse_conds("select * from t where a in (10,20,30,40,50)");
    let (conds, filters) =
        super::detacher::detach_conds_for_column(&conds, &table.columns[0], false);
    assert_eq!(conds.len(), 1);
    assert_eq!(filters.len(), 0);
    let tp = table.columns[0].ret_type.clone().expect("typed");
    let result = super::ranger::build_table_range(&conds, &tp, 0).expect("builds");
    assert_eq!(
        ranges_to_go_string(&result.ranges),
        "[[10,10] [20,20] [30,30] [40,40] [50,50]]"
    );
    assert_eq!(
        stringify_conds(&result.access_conds, &column_name),
        "[in(test.t.a, 10, 20, 30, 40, 50)]"
    );
    assert_eq!(stringify_conds(&result.remained_conds, &column_name), "[]");
    let quota = super::types::ranges_mem_usage(&result.ranges) - 1;
    let fallback = super::ranger::build_table_range(&conds, &tp, quota).expect("builds");
    assert_eq!(ranges_to_go_string(&fallback.ranges), "[[-inf,+inf]]");
    assert_eq!(stringify_conds(&fallback.access_conds, &column_name), "[]");
    assert_eq!(
        stringify_conds(&fallback.remained_conds, &column_name),
        "[in(test.t.a, 10, 20, 30, 40, 50)]"
    );

    // ---- BuildColumnRange over `a varchar(20)` ----
    let string_table = PrefixScanTable::new();
    let string_name = |unique_id: i64| -> String {
        match unique_id {
            1 => "test.t.a".to_owned(),
            2 => "test.t.b".to_owned(),
            other => format!("Column#{other}"),
        }
    };
    let parse_string_conds = |sql: &str| -> Vec<Expression> {
        let stmt = tidb_parser::parse(sql).expect("parses");
        let tidb_ast::Stmt::Query(query) = stmt else {
            panic!("not a query");
        };
        let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
            panic!("not a select");
        };
        let rewritten =
            rewrite_expr_resolved(&select.where_clause.expect("where"), &string_table)
                .expect("rewrites");
        split_cnf_items(&rewritten)
    };
    let conds = parse_string_conds("select * from t where a in ('aaa','bbb','ccc','ddd','eee')");
    let (conds, filters) =
        super::detacher::detach_conds_for_column(&conds, &string_table.a, false);
    assert_eq!(conds.len(), 1);
    assert_eq!(filters.len(), 0);
    let tp = string_table.a.ret_type.clone().expect("typed");
    let result = super::ranger::build_column_range(
        &conds,
        &tp,
        super::checker::UNSPECIFIED_LENGTH,
        0,
    )
    .expect("builds");
    assert_eq!(
        ranges_to_go_string(&result.ranges),
        "[[\"aaa\",\"aaa\"] [\"bbb\",\"bbb\"] [\"ccc\",\"ccc\"] [\"ddd\",\"ddd\"] [\"eee\",\"eee\"]]"
    );
    let quota = super::types::ranges_mem_usage(&result.ranges) - 1;
    let fallback = super::ranger::build_column_range(
        &conds,
        &tp,
        super::checker::UNSPECIFIED_LENGTH,
        quota,
    )
    .expect("builds");
    assert_eq!(ranges_to_go_string(&fallback.ranges), "[[NULL,+inf]]");
    assert_eq!(stringify_conds(&fallback.access_conds, &string_name), "[]");
    assert_eq!(
        stringify_conds(&fallback.remained_conds, &string_name),
        "[in(test.t.a, aaa, bbb, ccc, ddd, eee)]"
    );
}

/// The `AppendRanges2PointRanges` sub-case of the fallback test: two
/// point ranges fanned out over two tail ranges give four, in point-major
/// order, and a zero quota never falls back.
#[test]
fn append_ranges_to_point_ranges_matches_go() {
    use super::types::{Range, Ranges};
    let int_range = |low: &[i64], high: &[i64]| Range {
        low_val: low.iter().map(|v| Datum::Int(*v)).collect(),
        high_val: high.iter().map(|v| Datum::Int(*v)).collect(),
        collators: vec![tidb_datatype::Collation::Binary; low.len()],
        low_exclude: false,
        high_exclude: false,
    };
    let point_ranges: Ranges = vec![int_range(&[10], &[10]), int_range(&[20], &[20])];
    let tail_ranges: Ranges = vec![
        int_range(&[40, 70], &[40, 80]),
        int_range(&[50, 70], &[50, 80]),
    ];
    let (appended, fallback) =
        super::ranger::append_ranges_to_point_ranges(point_ranges, &tail_ranges, 0);
    assert!(!fallback);
    assert_eq!(
        ranges_to_go_string(&appended),
        "[[10 40 70,10 40 80] [10 50 70,10 50 80] [20 40 70,20 40 80] [20 50 70,20 50 80]]"
    );
}
