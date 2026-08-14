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

//! Source-contract cases from `pkg/expression/builtin_info.go::builtinBenchmarkSig`.

use std::cell::Cell;

use tidb_datatype::Datum;
use tidb_expr::{rewriter::rewrite_expr, Columns, ErrorLevel, EvalError, NoColumns};

fn parse_expr(sql_expr: &str) -> tidb_ast::Expr {
    let statement = tidb_parser::parse(&format!("SELECT {sql_expr}")).expect("parses");
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("expected a query")
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        panic!("expected a SELECT")
    };
    let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
        panic!("expected an expression field")
    };
    expr.clone()
}

fn eval_built(sql_expr: &str, ctx: &impl Columns) -> Result<Datum, EvalError> {
    let expression = rewrite_expr(&parse_expr(sql_expr))?;
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    expression.eval(ctx, chunk.get_row(0))
}

#[derive(Default)]
struct CountingContext {
    column_reads: Cell<usize>,
    last_insert_writes: Cell<usize>,
    division_is_error: bool,
}

impl Columns for CountingContext {
    fn get(&self, path: &[String]) -> Option<Datum> {
        (path.last().is_some_and(|name| name == "c")).then(|| {
            self.column_reads.set(self.column_reads.get() + 1);
            Datum::Int(7)
        })
    }

    fn division_by_zero_level(&self) -> ErrorLevel {
        if self.division_is_error {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        }
    }

    fn set_last_insert_id(&self, _value: u64) {
        self.last_insert_writes
            .set(self.last_insert_writes.get() + 1);
    }
}

#[test]
fn benchmark_repeats_the_expression_and_returns_zero() {
    let ctx = CountingContext::default();
    assert_eq!(
        tidb_expr::eval_in(&parse_expr("benchmark(3, c)"), &ctx),
        Ok(Datum::Int(0))
    );
    assert_eq!(ctx.column_reads.get(), 3);

    ctx.column_reads.set(0);
    assert_eq!(
        tidb_expr::eval_in(&parse_expr("benchmark('2', c)"), &ctx),
        Ok(Datum::Int(0))
    );
    assert_eq!(ctx.column_reads.get(), 2);

    ctx.column_reads.set(0);
    assert_eq!(
        tidb_expr::eval_in(&parse_expr("benchmark(c, null)"), &ctx),
        Ok(Datum::Int(0))
    );
    assert_eq!(
        ctx.column_reads.get(),
        1,
        "the loop count is evaluated once"
    );

    assert_eq!(
        eval_built("benchmark(3, last_insert_id(7))", &ctx),
        Ok(Datum::Int(0))
    );
    assert_eq!(ctx.last_insert_writes.get(), 3);
}

#[test]
fn benchmark_preserves_count_and_error_short_circuits() {
    assert_eq!(eval_built("benchmark(-3, 1)", &NoColumns), Ok(Datum::Null));
    assert_eq!(
        eval_built("benchmark(null, 1)", &NoColumns),
        Ok(Datum::Null)
    );
    assert_eq!(
        eval_built("benchmark(2, null)", &NoColumns),
        Ok(Datum::Int(0))
    );
    assert!(
        tidb_expr::eval_in(&parse_expr("benchmark(0, length('a', 'b'))"), &NoColumns).is_err(),
        "the inner expression is built even when the loop count is zero"
    );
    assert!(eval_built("benchmark(0, length('a', 'b'))", &NoColumns).is_err());

    let ctx = CountingContext {
        division_is_error: true,
        ..Default::default()
    };
    assert_eq!(eval_built("benchmark(0, 1 / 0)", &ctx), Ok(Datum::Int(0)));
    assert_eq!(
        eval_built("benchmark(1, 1 / 0)", &ctx),
        Err(EvalError::DivisionByZero)
    );
    assert_eq!(
        eval_built("benchmark(1, vec_from_text('[1]'))", &NoColumns),
        Err(EvalError::Unsupported(
            "VectorFloat32 is not supported for BENCHMARK()"
        ))
    );
}
