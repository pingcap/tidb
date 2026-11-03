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

//! Go `pkg/planner/core/scalar_subq_expression.go`'s `ScalarSubQueryExpr`:
//! the placeholder plain `EXPLAIN` puts where an uncorrelated scalar or
//! `EXISTS` subquery would have been folded into a constant, when
//! `tidb_opt_enable_non_eval_scalar_subquery` is on.
//!
//! Go keeps the type in the planner package because its evaluation context
//! runs the subquery's executors. The expression enum lives here, so the
//! node does too; the planner-side half is the [`ScalarSubqueryEval`] trait
//! object it carries.

use std::sync::{Arc, OnceLock};

use tidb_datatype::{Datum, FieldType};
use tidb_exprctx::EvalError;

use crate::constant::Constant;
use crate::expression::SCALAR_SUB_Q_FLAG;
use tidb_codec::encode_int;

/// Go `ScalarSubqueryEvalCtx`'s evaluation half (`getColVal`): it runs the
/// optimized subquery once and hands out its first row by output column id.
pub trait ScalarSubqueryEval: std::fmt::Debug + Send + Sync {
    /// Go `ScalarSubqueryEvalCtx.getColVal`.
    ///
    /// # Errors
    ///
    /// The subquery's own evaluation error, or Go's "Could not found" error
    /// for an id the context does not output.
    fn get_col_val(&self, col_id: i64) -> Result<Datum, EvalError>;
}

/// Go `ScalarSubQueryExpr`.
///
/// Go embeds `expression.Constant`, and every method it does not override
/// (`StringWithCtx`, the collation accessors, `ExplainNormalizedInfo4InList`)
/// is the embedded constant's: the value it prints is NULL until the
/// subquery has been evaluated.
#[derive(Clone, Debug)]
pub struct ScalarSubQueryExpr {
    /// Go `scalarSubqueryColID`.
    pub scalar_subquery_col_id: i64,
    /// Go `evalCtx`.
    pub eval_ctx: Arc<dyn ScalarSubqueryEval>,
    /// Go `evaled` / `evalErr` and the value `selfEvaluate` stores.
    evaluated: OnceLock<Result<Datum, EvalError>>,
    /// Lazily-filled `HashCode` cache (Go `hashcode`).
    hashcode: Vec<u8>,
    /// Go's embedded `expression.Constant`.
    pub constant: Constant,
}

impl ScalarSubQueryExpr {
    /// Go `&ScalarSubQueryExpr{scalarSubqueryColID, evalCtx}` with `RetType`
    /// set; the caller sets the coercibility on [`Self::constant`].
    #[must_use]
    pub fn new(
        scalar_subquery_col_id: i64,
        eval_ctx: Arc<dyn ScalarSubqueryEval>,
        ret_type: FieldType,
    ) -> Self {
        Self {
            scalar_subquery_col_id,
            eval_ctx,
            evaluated: OnceLock::new(),
            hashcode: Vec::new(),
            constant: {
                let mut constant = Constant::default();
                constant.ret_type = Some(ret_type);
                constant
            },
        }
    }

    /// Go `ScalarSubQueryExpr.Eval`: the first call runs `selfEvaluate`,
    /// later calls return what it stored.
    ///
    /// # Errors
    ///
    /// The subquery's evaluation error, again on every later call.
    pub fn eval(&self) -> Result<Datum, EvalError> {
        self.evaluated
            .get_or_init(|| self.eval_ctx.get_col_val(self.scalar_subquery_col_id))
            .clone()
    }

    /// Go `ScalarSubQueryExpr.evaled`.
    #[must_use]
    pub fn evaled(&self) -> bool {
        matches!(self.evaluated.get(), Some(Ok(_)))
    }

    /// The embedded `Constant` as Go's promoted methods see it: the evaluated
    /// value once `selfEvaluate` succeeded, `NewNull()` once it failed.
    #[must_use]
    pub fn embedded_constant(&self) -> Constant {
        match self.evaluated.get() {
            Some(Ok(value)) => {
                let mut constant = self.constant.clone();
                constant.value = value.clone();
                constant
            }
            Some(Err(_)) => Constant::new_null(),
            None => self.constant.clone(),
        }
    }

    /// Go `ScalarSubQueryExpr.GetType`.
    #[must_use]
    pub fn ret_type(&self) -> Option<&FieldType> {
        self.constant.ret_type.as_ref()
    }

    /// Go `ScalarSubQueryExpr.String` / `ExplainInfo`.
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!("ScalarQueryCol#{}", self.scalar_subquery_col_id)
    }

    /// Go `ScalarSubQueryExpr.Equal` / `Equals`: the output column id alone.
    #[must_use]
    pub fn equal(&self, other: &Self) -> bool {
        self.scalar_subquery_col_id == other.scalar_subquery_col_id
    }

    /// Go `ScalarSubQueryExpr.HashCode`: `[ScalarSubQFlag, EncodeInt(id)]`,
    /// cached on first call.
    pub fn hash_code(&mut self) -> &[u8] {
        if self.hashcode.is_empty() {
            self.hashcode.reserve(9);
            self.hashcode.push(SCALAR_SUB_Q_FLAG);
            encode_int(&mut self.hashcode, self.scalar_subquery_col_id);
        }
        &self.hashcode
    }

    /// Go `ScalarSubQueryExpr.MemoryUsage`: the embedded constant once
    /// evaluated, nothing before.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        if self.evaled() {
            self.embedded_constant().memory_usage()
        } else {
            0
        }
    }
}

/// Go `errors.Errorf(message)`: no registered code, so the client reads 1105.
pub(crate) fn unregistered_error(message: impl Into<String>) -> EvalError {
    EvalError::Conversion(tidb_terror::TerrorError::compatible(
        tidb_terror::TerrorCode::new(1105),
        message,
    ))
}

/// Go `ScalarSubQueryExpr.VecEval*`.
pub(crate) fn vec_eval_error() -> EvalError {
    unregistered_error("ScalarSubQueryExpr doesn't implement the vec eval yet")
}
