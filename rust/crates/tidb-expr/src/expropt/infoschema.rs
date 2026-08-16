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

//! Go `pkg/expression/expropt/infoschema.go`.

use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};
use crate::metabuild::MetaOnlyInfoSchema;

/// Go `InfoSchemaPropProvider`: `func(isDomain bool) infoschema.MetaOnlyInfoSchema`.
///
/// The `MetaOnlyInfoSchema` boundary trait is the one `metabuild` already
/// carries for Go `pkg/infoschema/context.MetaOnlyInfoSchema`.
pub struct InfoSchemaPropProvider(
    #[allow(clippy::type_complexity)]
    Box<dyn Fn(bool) -> Arc<dyn MetaOnlyInfoSchema + Send + Sync> + Send + Sync>,
);

impl InfoSchemaPropProvider {
    /// Wraps a function as the provider.
    #[must_use]
    pub fn new(
        provide: impl Fn(bool) -> Arc<dyn MetaOnlyInfoSchema + Send + Sync> + Send + Sync + 'static,
    ) -> Self {
        InfoSchemaPropProvider(Box::new(provide))
    }

    /// Calls the provider, Go's `p(isDomain)`.
    #[must_use]
    pub fn call(&self, is_domain: bool) -> Arc<dyn MetaOnlyInfoSchema + Send + Sync> {
        (self.0)(is_domain)
    }
}

impl OptionalEvalPropProvider for InfoSchemaPropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::InfoSchema.desc()
    }
}

/// Go `InfoSchemaPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InfoSchemaPropReader;

impl RequireOptionalEvalProps for InfoSchemaPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::InfoSchema.as_prop_key_set()
    }
}

impl InfoSchemaPropReader {
    /// Go `InfoSchemaPropReader.GetSessionInfoSchema`: the session's schema,
    /// which is `p(false)`.
    pub fn get_session_info_schema(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<dyn MetaOnlyInfoSchema + Send + Sync>, ExprOptError> {
        Ok(self.get_provider(ctx)?.call(false))
    }

    /// Go `InfoSchemaPropReader.GetLatestInfoSchema`: the domain's schema,
    /// which is `p(true)`.
    pub fn get_latest_info_schema(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<dyn MetaOnlyInfoSchema + Send + Sync>, ExprOptError> {
        Ok(self.get_provider(ctx)?.call(true))
    }

    fn get_provider(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<InfoSchemaPropProvider>, ExprOptError> {
        get_prop_provider::<InfoSchemaPropProvider>(ctx, OptionalEvalPropKey::InfoSchema)
    }
}
