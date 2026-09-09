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

//! Go `pkg/expression/expropt/sessioncontext.go`.

use std::any::Any;
use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// boundary: Go `sessionctx.Context`'s three methods used by `EMBED_TEXT`.
///
/// `context.Context` and Go's `any` are represented as opaque, thread-safe
/// values. `Option` preserves Go's nil interface result for trace/domain
/// values; session variables are required by the caller and use the same
/// opaque [`super::SessionVars`] handle as the session-variable provider.
pub trait SessionContext: Any + Send + Sync {
    /// Go `Context.GetTraceCtx`.
    fn get_trace_ctx(&self) -> Option<Arc<dyn Any + Send + Sync>>;

    /// Go `Context.GetSessionVars`.
    fn get_session_vars(&self) -> Arc<dyn super::SessionVars>;

    /// Go `ValueStoreContext.GetDomain` as exposed by `sessionctx.Context`.
    fn get_domain(&self) -> Option<Arc<dyn Any + Send + Sync>>;
}

/// Go `SessionContextPropProvider`: a provider returning the current session.
///
/// The closure form keeps the Go function-provider semantics, including a
/// nil result in tests or contexts that have no session attached.
pub struct SessionContextPropProvider(
    Box<dyn Fn() -> Option<Arc<dyn SessionContext>> + Send + Sync>,
);

impl SessionContextPropProvider {
    /// Wraps a session-context provider closure.
    #[must_use]
    pub fn new(
        provide: impl Fn() -> Option<Arc<dyn SessionContext>> + Send + Sync + 'static,
    ) -> Self {
        SessionContextPropProvider(Box::new(provide))
    }

    /// Creates a provider for one non-null session context, matching Go's
    /// `NewSessionContextPropProvider` constructor and its assertion.
    #[must_use]
    pub fn from_context(context: Arc<dyn SessionContext>) -> Self {
        Self::new(move || Some(Arc::clone(&context)))
    }

    /// Calls the provider, Go's `p()`.
    #[must_use]
    pub fn call(&self) -> Option<Arc<dyn SessionContext>> {
        (self.0)()
    }
}

impl OptionalEvalPropProvider for SessionContextPropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::SessionContext.desc()
    }
}

/// Go `SessionContextPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionContextPropReader;

impl RequireOptionalEvalProps for SessionContextPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::SessionContext.as_prop_key_set()
    }
}

impl SessionContextPropReader {
    /// Go `SessionContextPropReader.GetSessionContext`.
    pub fn get_session_context(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Option<Arc<dyn SessionContext>>, ExprOptError> {
        let provider = get_prop_provider::<SessionContextPropProvider>(
            ctx,
            OptionalEvalPropKey::SessionContext,
        )?;
        Ok(provider.call())
    }
}
