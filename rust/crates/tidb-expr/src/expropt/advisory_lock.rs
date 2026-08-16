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

//! Go `pkg/expression/expropt/advisory_lock.go`.

use std::any::Any;
use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use crate::exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// Go `AdvisoryLockContext`: the advisory-lock operations behind
/// `GET_LOCK()`, `IS_USED_LOCK()`, `RELEASE_LOCK()` and
/// `RELEASE_ALL_LOCKS()`.
pub trait AdvisoryLockContext: Any + Send + Sync {
    /// Go `GetAdvisoryLock`, taking the lock name and a timeout in seconds.
    fn get_advisory_lock(&self, name: &str, timeout: i64) -> Result<(), ExprOptError>;

    /// Go `IsUsedAdvisoryLock`: the holder's connection ID, or 0.
    fn is_used_advisory_lock(&self, name: &str) -> u64;

    /// Go `ReleaseAdvisoryLock`: whether a held lock was released.
    fn release_advisory_lock(&self, name: &str) -> bool;

    /// Go `ReleaseAllAdvisoryLocks`: how many locks this session released.
    fn release_all_advisory_locks(&self) -> i64;
}

/// Go `AdvisoryLockPropProvider`, which embeds an `AdvisoryLockContext` and so
/// is one itself; the reader hands the provider back as the context.
pub struct AdvisoryLockPropProvider {
    lock_ctx: Arc<dyn AdvisoryLockContext>,
}

impl AdvisoryLockPropProvider {
    /// Go `NewAdvisoryLockPropProvider`.
    ///
    /// Go asserts the argument is non-nil; `Arc` is non-nullable, so the
    /// assertion holds by construction.
    #[must_use]
    pub fn new(ctx: Arc<dyn AdvisoryLockContext>) -> Self {
        AdvisoryLockPropProvider { lock_ctx: ctx }
    }

    /// The embedded `AdvisoryLockContext`.
    #[must_use]
    pub fn advisory_lock_context(&self) -> &Arc<dyn AdvisoryLockContext> {
        &self.lock_ctx
    }
}

/// The embedded-field promotion Go gets for free.
impl AdvisoryLockContext for AdvisoryLockPropProvider {
    fn get_advisory_lock(&self, name: &str, timeout: i64) -> Result<(), ExprOptError> {
        self.lock_ctx.get_advisory_lock(name, timeout)
    }

    fn is_used_advisory_lock(&self, name: &str) -> u64 {
        self.lock_ctx.is_used_advisory_lock(name)
    }

    fn release_advisory_lock(&self, name: &str) -> bool {
        self.lock_ctx.release_advisory_lock(name)
    }

    fn release_all_advisory_locks(&self) -> i64 {
        self.lock_ctx.release_all_advisory_locks()
    }
}

impl OptionalEvalPropProvider for AdvisoryLockPropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::AdvisoryLock.desc()
    }
}

/// Go `AdvisoryLockPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdvisoryLockPropReader;

impl RequireOptionalEvalProps for AdvisoryLockPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::AdvisoryLock.as_prop_key_set()
    }
}

impl AdvisoryLockPropReader {
    /// Go `AdvisoryLockPropReader.AdvisoryLockCtx`: the provider itself, which
    /// is the lock context.
    pub fn advisory_lock_ctx(
        self,
        ctx: &dyn EvalPropContext,
    ) -> Result<Arc<AdvisoryLockPropProvider>, ExprOptError> {
        get_prop_provider::<AdvisoryLockPropProvider>(ctx, OptionalEvalPropKey::AdvisoryLock)
    }
}
