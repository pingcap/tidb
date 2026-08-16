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

//! Go `pkg/expression/expropt/priv.go`. The file is named `privilege.rs`
//! because `priv` is a reserved Rust keyword.

use std::any::Any;
use std::sync::Arc;

use tidb_mysql::privilege::PrivilegeType;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// Go `PrivilegeChecker`: the privilege checks expression code performs.
pub trait PrivilegeChecker: Any + Send + Sync {
    /// Go `RequestVerification`.
    fn request_verification(
        &self,
        db: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool;

    /// Go `RequestDynamicVerification`, for a DYNAMIC privilege.
    fn request_dynamic_verification(&self, priv_name: &str, grantable: bool) -> bool;
}

/// Go `PrivilegeCheckerProvider`: `func() PrivilegeChecker`.
pub struct PrivilegeCheckerProvider(Box<dyn Fn() -> Arc<dyn PrivilegeChecker> + Send + Sync>);

impl PrivilegeCheckerProvider {
    /// Wraps a function as the provider.
    #[must_use]
    pub fn new(provide: impl Fn() -> Arc<dyn PrivilegeChecker> + Send + Sync + 'static) -> Self {
        PrivilegeCheckerProvider(Box::new(provide))
    }

    /// Calls the provider, Go's `p()`.
    #[must_use]
    pub fn call(&self) -> Arc<dyn PrivilegeChecker> {
        (self.0)()
    }
}

impl OptionalEvalPropProvider for PrivilegeCheckerProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::PrivilegeChecker.desc()
    }
}

/// Go `PrivilegeCheckerPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrivilegeCheckerPropReader;

impl RequireOptionalEvalProps for PrivilegeCheckerPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::PrivilegeChecker.as_prop_key_set()
    }
}

impl PrivilegeCheckerPropReader {
    /// Go `PrivilegeCheckerPropReader.GetPrivilegeChecker`.
    pub fn get_privilege_checker(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<dyn PrivilegeChecker>, ExprOptError> {
        let provider = get_prop_provider::<PrivilegeCheckerProvider>(
            ctx,
            OptionalEvalPropKey::PrivilegeChecker,
        )?;
        Ok(provider.call())
    }
}
