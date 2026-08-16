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

//! Go `pkg/expression/expropt/sqlexec.go`.

use std::any::Any;
use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// boundary: Go `expropt.SQLExecutor`, itself the narrowed subset of
/// `pkg/util/sqlexec.RestrictedSQLExecutor` that expression code may call.
///
/// Go's single method is
/// `ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias, sql string, args ...any)
/// ([]chunk.Row, []*resolve.ResultField, error)`. Neither
/// `sqlexec.OptionFuncAlias` nor `planner/core/resolve.ResultField` is ported,
/// and the Rust counterpart of `chunk.Row` is lifetime-bound to the chunk that
/// owns it, so the method is not reproduced. expropt itself only passes the
/// executor through from provider to caller.
pub trait SqlExecutor: Any + Send + Sync {}

/// Go `SQLExecutorPropProvider`: `func() (SQLExecutor, error)`.
pub struct SqlExecutorPropProvider(
    #[allow(clippy::type_complexity)]
    Box<dyn Fn() -> Result<Arc<dyn SqlExecutor>, ExprOptError> + Send + Sync>,
);

impl SqlExecutorPropProvider {
    /// Wraps a function as the provider.
    #[must_use]
    pub fn new(
        provide: impl Fn() -> Result<Arc<dyn SqlExecutor>, ExprOptError> + Send + Sync + 'static,
    ) -> Self {
        SqlExecutorPropProvider(Box::new(provide))
    }

    /// Calls the provider, Go's `p()`.
    pub fn call(&self) -> Result<Arc<dyn SqlExecutor>, ExprOptError> {
        (self.0)()
    }
}

impl OptionalEvalPropProvider for SqlExecutorPropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::SqlExecutor.desc()
    }
}

/// Go `SQLExecutorPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SqlExecutorPropReader;

impl RequireOptionalEvalProps for SqlExecutorPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::SqlExecutor.as_prop_key_set()
    }
}

impl SqlExecutorPropReader {
    /// Go `SQLExecutorPropReader.GetSQLExecutor`: the provider's own error is
    /// returned unchanged.
    pub fn get_sql_executor(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<dyn SqlExecutor>, ExprOptError> {
        let provider =
            get_prop_provider::<SqlExecutorPropProvider>(ctx, OptionalEvalPropKey::SqlExecutor)?;
        provider.call()
    }
}
