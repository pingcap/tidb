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

use tidb_datatype::Datum;
use tidb_expr::rewriter::rewrite_expr;
use tidb_exprctx::Columns;
use tidb_exprctx::ErrorLevel;
use tidb_exprctx::EvalError;
use tidb_exprctx::NoColumns;

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
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty_chunk(&[]);
    chunk.set_num_virtual_rows(1);
    expression.eval(ctx, chunk.get_row(0))
}

/// Go `SessionVars` whose `SetLastInsertID` counts its calls.
#[derive(Clone, Default)]
struct CountingVars(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl tidb_expropt::SessionVars for CountingVars {
    fn user(&self) -> Option<std::sync::Arc<tidb_parser::auth::UserIdentity>> {
        None
    }
    fn active_roles(&self) -> Vec<std::sync::Arc<tidb_parser::auth::RoleIdentity>> {
        Vec::new()
    }
    fn connection_id(&self) -> u64 {
        0
    }
    fn last_found_rows(&self) -> u64 {
        0
    }
    fn prev_affected_rows(&self) -> i64 {
        0
    }
    fn prev_last_insert_id(&self) -> u64 {
        0
    }
    fn set_last_insert_id(&self, _: u64) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn hint_resource_group_name(&self) -> String {
        String::new()
    }
}

impl tidb_expropt::SessionVarsProvider for CountingVars {
    fn get_session_vars(&self) -> std::sync::Arc<dyn tidb_expropt::SessionVars> {
        std::sync::Arc::new(self.clone())
    }
}

#[derive(Default)]
struct CountingContext {
    last_insert_writes: CountingVars,
    division_is_error: bool,
}

impl CountingContext {
    fn writes(&self) -> usize {
        self.last_insert_writes.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Columns for CountingContext {
    fn get(&self, _path: &[String]) -> Option<Datum> {
        None
    }

    fn division_by_zero_level(&self) -> ErrorLevel {
        if self.division_is_error {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        }
    }

    fn get_optional_prop_provider(
        &self,
        key: tidb_exprctx::OptionalEvalPropKey,
    ) -> Option<std::sync::Arc<dyn tidb_exprctx::DynOptionalEvalPropProvider>> {
        (key == tidb_exprctx::OptionalEvalPropKey::SessionVars).then(|| {
            std::sync::Arc::new(tidb_expropt::new_session_vars_provider(std::sync::Arc::new(self.last_insert_writes.clone())))
                as std::sync::Arc<dyn tidb_exprctx::DynOptionalEvalPropProvider>
        })
    }
}

#[test]
fn benchmark_repeats_the_expression_and_returns_zero() {
    let ctx = CountingContext::default();
    assert_eq!(
        eval_built("benchmark(3, last_insert_id(7))", &ctx),
        Ok(Datum::Int(0))
    );
    assert_eq!(ctx.writes(), 3);

    assert_eq!(
        eval_built("benchmark(last_insert_id(2), null)", &ctx),
        Ok(Datum::Int(0))
    );
    assert_eq!(
        ctx.writes(),
        4,
        "the loop count is evaluated once"
    );
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
