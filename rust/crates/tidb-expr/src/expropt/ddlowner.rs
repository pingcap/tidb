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

//! Go `pkg/expression/expropt/ddlowner.go`.

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// Go `DDLOwnerInfoProvider`: `func() bool`, reporting whether this node is
/// the DDL owner.
pub struct DdlOwnerInfoProvider(Box<dyn Fn() -> bool + Send + Sync>);

impl DdlOwnerInfoProvider {
    /// Wraps a function as the provider.
    #[must_use]
    pub fn new(provide: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        DdlOwnerInfoProvider(Box::new(provide))
    }

    /// Calls the provider, Go's `p()`.
    #[must_use]
    pub fn call(&self) -> bool {
        (self.0)()
    }
}

impl OptionalEvalPropProvider for DdlOwnerInfoProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::DdlOwnerInfo.desc()
    }
}

/// Go `DDLOwnerPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DdlOwnerPropReader;

impl RequireOptionalEvalProps for DdlOwnerPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::DdlOwnerInfo.as_prop_key_set()
    }
}

impl DdlOwnerPropReader {
    /// Go `DDLOwnerPropReader.IsDDLOwner`.
    pub fn is_ddl_owner(self, ctx: &dyn EvalPropContext) -> Result<bool, ExprOptError> {
        let provider =
            get_prop_provider::<DdlOwnerInfoProvider>(ctx, OptionalEvalPropKey::DdlOwnerInfo)?;
        Ok(provider.call())
    }
}
