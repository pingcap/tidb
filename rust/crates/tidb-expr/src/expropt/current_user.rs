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

//! Go `pkg/expression/expropt/current_user.go`.

use std::sync::Arc;

use tidb_parser::auth::{RoleIdentity, UserIdentity};

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropProvider};

/// The identity pair Go's `CurrentUserPropProvider` returns:
/// `(*auth.UserIdentity, []*auth.RoleIdentity)`. Go's pointers become `Arc`s so
/// the provider and its callers observe the same values.
pub type CurrentUserAndRoles = (Option<Arc<UserIdentity>>, Vec<Arc<RoleIdentity>>);

/// Go `CurrentUserPropProvider`: a function providing the current user and
/// the active roles.
pub struct CurrentUserPropProvider(Box<dyn Fn() -> CurrentUserAndRoles + Send + Sync>);

impl CurrentUserPropProvider {
    /// Wraps a function as the provider, Go's `CurrentUserPropProvider(fn)`
    /// conversion.
    #[must_use]
    pub fn new(provide: impl Fn() -> CurrentUserAndRoles + Send + Sync + 'static) -> Self {
        CurrentUserPropProvider(Box::new(provide))
    }

    /// Calls the provider, Go's `p()`.
    #[must_use]
    pub fn call(&self) -> CurrentUserAndRoles {
        (self.0)()
    }
}

impl OptionalEvalPropProvider for CurrentUserPropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::CurrentUser.desc()
    }
}

/// Go `CurrentUserPropReader`: reads `OptPropCurrentUser` out of an
/// `EvalContext`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CurrentUserPropReader;

impl RequireOptionalEvalProps for CurrentUserPropReader {
    fn required_optional_eval_props(&self) -> crate::exprctx::OptionalEvalPropKeySet {
        OptionalEvalPropKey::CurrentUser.as_prop_key_set()
    }
}

impl CurrentUserPropReader {
    /// Go `CurrentUserPropReader.CurrentUser`.
    pub fn current_user(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Option<Arc<UserIdentity>>, ExprOptError> {
        Ok(self.get_provider(ctx)?.call().0)
    }

    /// Go `CurrentUserPropReader.ActiveRoles`.
    pub fn active_roles(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Vec<Arc<RoleIdentity>>, ExprOptError> {
        Ok(self.get_provider(ctx)?.call().1)
    }

    /// Go `CurrentUserPropReader.getProvider`.
    fn get_provider(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<CurrentUserPropProvider>, ExprOptError> {
        get_prop_provider::<CurrentUserPropProvider>(ctx, OptionalEvalPropKey::CurrentUser)
    }
}
