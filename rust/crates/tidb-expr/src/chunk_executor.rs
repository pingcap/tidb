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

//! Go `chunk_executor.go`: vector kernel admission and typed result allocation.

use crate::expression::Expression;
use tidb_chunk::column::Column as ChunkColumn;
use tidb_datatype::EvalType;

thread_local! {
    static VEC_EVAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Ends a [`enter_vec_eval`] scope, restoring the mode it replaced.
#[must_use = "the scope ends when the guard is dropped"]
pub(crate) struct VecEvalScope(bool);

impl Drop for VecEvalScope {
    fn drop(&mut self) {
        VEC_EVAL.with(|flag| flag.set(self.0));
    }
}

/// Go `EvalExpr`'s choice for one expression: `VecEval*` when vectorization
/// is enabled and the expression is `Vectorized()`, `Eval*` row by row
/// otherwise. Shared datatype helpers read this mode where Go has
/// distinct scalar and vector semantics.
pub(crate) fn enter_vec_eval(vectorized: bool) -> VecEvalScope {
    VecEvalScope(VEC_EVAL.with(|flag| flag.replace(vectorized)))
}

/// Whether this thread is evaluating an expression as Go's `VecEval*` would.
pub(crate) fn in_vec_eval() -> bool {
    VEC_EVAL.with(std::cell::Cell::get)
}

/// Go `Expression.Vectorized`: a column or constant is, a scalar function
/// when its signature is and so are all of its arguments
/// (`isChildrenVectorized`).
pub(crate) fn vectorized(expression: &Expression) -> bool {
    match expression {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => true,
        Expression::Constant(constant) => constant
            .deferred_expr
            .as_deref()
            .is_none_or(|deferred| vectorized(deferred)),
        Expression::ScalarFunction(function) => {
            crate::scalar_function::vec_eval::has_kernel(function)
                && function.args.iter().all(|arg| vectorized(arg))
        }
        // The evaluator has no vectorized path for an unevaluated subquery.
        Expression::ScalarSubQuery(_) => false,
    }
}

/// `result.Resize*(n, false)` for a fixed-width evaluation type, or
/// `result.Reserve*(n)` for a variable-width one: Go `genVecFromConstExpr`
/// and builtins with their own result buffers.
pub(crate) fn allocate(eval_type: EvalType, n: usize, result: &mut ChunkColumn) {
    match eval_type {
        EvalType::Int => result.resize_int64(n, false),
        EvalType::Real => result.resize_float64(n, false),
        EvalType::Decimal => result.resize_decimal(n, false),
        EvalType::Datetime | EvalType::Timestamp => result.resize_time(n, false),
        EvalType::Duration => result.resize_go_duration(n, false),
        EvalType::String => result.reserve_string(n),
        EvalType::Json => result.reserve_json(n),
        EvalType::VectorFloat32 => result.reserve_vector_float32(n),
    }
}
