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
