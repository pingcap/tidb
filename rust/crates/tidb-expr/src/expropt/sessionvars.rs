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

//! Go `pkg/expression/expropt/sessionvars.go`.

use std::any::Any;
use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// boundary: Go `pkg/sessionctx/variable.SessionVars` — the session variable
/// bundle, ported in `tidb-session`, a crate that sits above this one. expropt
/// only carries the value from provider to reader, so an opaque marker trait
/// stands in and keeps the layering intact.
pub trait SessionVars: Any + Send + Sync {}

/// boundary: Go `pkg/sessionctx/variable.SessionVarsProvider` — anything that
/// can hand back the session variables.
pub trait SessionVarsProvider: Send + Sync {
    /// Go `SessionVarsProvider.GetSessionVars`.
    fn get_session_vars(&self) -> Arc<dyn SessionVars>;
}

/// Go `SessionVarsPropProvider`.
pub struct SessionVarsPropProvider {
    vars: Arc<dyn SessionVarsProvider>,
}

/// Go `NewSessionVarsProvider`.
///
/// Go asserts the argument is non-nil; `Arc` is non-nullable, so the assertion
/// holds by construction.
#[must_use]
pub fn new_session_vars_provider(
    provider: Arc<dyn SessionVarsProvider>,
) -> SessionVarsPropProvider {
    SessionVarsPropProvider { vars: provider }
}

impl SessionVarsPropProvider {
    /// The wrapped provider, Go's unexported `vars` field.
    #[must_use]
    pub fn vars(&self) -> &Arc<dyn SessionVarsProvider> {
        &self.vars
    }
}

impl OptionalEvalPropProvider for SessionVarsPropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::SessionVars.desc()
    }
}

/// Go `SessionVarsPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionVarsPropReader;

impl RequireOptionalEvalProps for SessionVarsPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::SessionVars.as_prop_key_set()
    }
}

impl SessionVarsPropReader {
    /// Go `SessionVarsPropReader.GetSessionVars`.
    ///
    /// Go additionally runs `exprctx.AssertLocationWithSessionVars` under the
    /// `intest` build tag; that helper lives in the not-yet-ported half of
    /// `exprctx`, and it is an assertion only, with no release-build effect.
    pub fn get_session_vars(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<dyn SessionVars>, ExprOptError> {
        let provider =
            get_prop_provider::<SessionVarsPropProvider>(ctx, OptionalEvalPropKey::SessionVars)?;
        Ok(provider.vars.get_session_vars())
    }
}
