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

//! Go `pkg/expression/expropt` lands as a complete package: the provider half
//! of the optional-evaluation-property machinery whose key/descriptor half
//! lives in [`crate::exprctx`].
//!
//! An `EvalContext` carries at most one provider per optional property. This
//! package is the storage for those providers ([`OptionalEvalPropProviders`],
//! from `optional.go`) plus, per property, a *provider* type that supplies the
//! value and a *reader* type that expression code uses to fetch it — one file
//! per property, exactly as upstream splits them.
//!
//! Every production symbol of the Go package is here: `RequireOptionalEvalProps`,
//! `OptionalEvalPropProviders` with `Contains`/`Get`/`Add`/`PropKeySet`,
//! `getPropProvider`, and the nine provider/reader pairs of `current_user.go`,
//! `sessionvars.go`, `infoschema.go`, `kvstore.go`, `sqlexec.go`,
//! `sequence.go`, `advisory_lock.go`, `ddlowner.go` and `priv.go`. The Go
//! package's own test, `optional_test.go`'s `TestOptionalEvalPropProviders`,
//! is ported in [`mod tests`](self#tests).
//!
//! Imports the Go package makes into subsystems this crate cannot reach are
//! narrowed to local boundary shapes. Reused where the dependency direction
//! allows: `pkg/parser/auth` (`tidb_parser::auth`), `pkg/parser/mysql`
//! (`tidb_mysql::privilege`), and `pkg/infoschema/context.MetaOnlyInfoSchema`
//! (the existing [`crate::metabuild::MetaOnlyInfoSchema`] boundary trait).
//! Narrowed:
//!
//! - `// boundary:` Go `pkg/expression/exprctx.EvalContext` — modeled as
//!   [`EvalPropContext`], carrying only `GetOptionalPropProvider`, the single
//!   method every reader in this package calls. The crate-local `exprctx`
//!   module is still a seed without the umbrella interfaces.
//! - `// boundary:` Go `pkg/kv.Storage` — modeled as the opaque [`KvStorage`]
//!   marker trait. The ported storage client (`tidb-txnkv`) is an async,
//!   tonic/tokio-backed stack, and depending on it from this expression crate
//!   would invert the workspace layering; expropt only stores the value and
//!   hands it back.
//! - `// boundary:` Go `pkg/sessionctx/variable.SessionVarsProvider` and
//!   `variable.SessionVars` — modeled as [`SessionVarsProvider`] and the
//!   opaque [`SessionVars`] marker trait. `SessionVars` itself is ported in
//!   `tidb-session`, which sits *above* this crate, so a dependency on it
//!   would be a cycle.
//! - `// boundary:` Go `pkg/util/sqlexec.RestrictedSQLExecutor` — modeled as
//!   the opaque [`SqlExecutor`] marker trait. Its `ExecRestrictedSQL`
//!   signature names `sqlexec.OptionFuncAlias` and
//!   `planner/core/resolve.ResultField`, neither of which is ported, and
//!   returns `[]chunk.Row`, whose Rust counterpart is lifetime-bound to its
//!   chunk. expropt itself only passes the executor through.
//!
//! Two upstream behaviors are deliberately not reproduced, both `intest`-only
//! (Go's assertion build tag) and neither observable in release builds:
//! `getPropProvider`'s `stub.Desc().Key() == key` check, which needs a zero
//! value of a type parameter that Rust cannot conjure, and
//! `SessionVarsPropReader.GetSessionVars`'s
//! `exprctx.AssertLocationWithSessionVars`, which lives in the not-yet-ported
//! half of `exprctx`. `OptionalEvalPropProviders::add`'s per-key type
//! assertion *is* reproduced, as a `debug_assert`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::exprctx::{
    OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider, OPT_PROPS_CNT,
};

mod advisory_lock;
mod current_user;
mod ddlowner;
mod infoschema;
mod kvstore;
mod privilege;
mod sequence;
mod sessionvars;
mod sqlexec;

pub use advisory_lock::{AdvisoryLockContext, AdvisoryLockPropProvider, AdvisoryLockPropReader};
pub use current_user::{CurrentUserPropProvider, CurrentUserPropReader};
pub use ddlowner::{DdlOwnerInfoProvider, DdlOwnerPropReader};
pub use infoschema::{InfoSchemaPropProvider, InfoSchemaPropReader};
pub use kvstore::{KvStorage, KvStorePropProvider, KvStorePropReader};
pub use privilege::{PrivilegeChecker, PrivilegeCheckerPropReader, PrivilegeCheckerProvider};
pub use sequence::{SequenceOperator, SequenceOperatorPropReader, SequenceOperatorProvider};
pub use sessionvars::{
    new_session_vars_provider, SessionVars, SessionVarsPropProvider, SessionVarsPropReader,
    SessionVarsProvider,
};
pub use sqlexec::{SqlExecutor, SqlExecutorPropProvider, SqlExecutorPropReader};

/// The error Go raises with `errors.Errorf` from `getPropProvider`, and the
/// error type the fallible providers of this package hand back.
///
/// Go uses a bare `errors.Errorf` string here, with no error code attached, so
/// the message is the whole contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprOptError(String);

impl ExprOptError {
    /// Builds an error from a message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        ExprOptError(message.into())
    }

    /// The error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExprOptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ExprOptError {}

/// Go `RequireOptionalEvalProps`: implemented by things (readers, and the
/// builtin functions built on them) that need optional evaluation properties.
pub trait RequireOptionalEvalProps {
    /// Go `RequiredOptionalEvalProps`. An empty set means nothing is required.
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet;
}

/// The stored, downcastable form of a Go `exprctx.OptionalEvalPropProvider`
/// interface value.
///
/// Go stores the bare interface and recovers the concrete provider with a type
/// assertion; Rust needs an explicit route to [`Any`] for the same move, so
/// every provider gets one through this blanket-implemented extension trait.
pub trait DynOptionalEvalPropProvider: OptionalEvalPropProvider + Any + Send + Sync {
    /// Erases the provider to [`Any`] so [`get_prop_provider`] can recover the
    /// concrete type, which is Go's `val.(T)`. `Send + Sync` is required
    /// because that is what `Arc::downcast` needs, and because an
    /// `EvalContext` and its providers are shared across threads.
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;

    /// Borrowing form of [`DynOptionalEvalPropProvider::into_any`].
    fn as_any(&self) -> &dyn Any;
}

impl<T: OptionalEvalPropProvider + Any + Send + Sync> DynOptionalEvalPropProvider for T {
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The slice of Go `exprctx.EvalContext` that every reader in this package
/// consumes.
///
/// boundary: Go `pkg/expression/exprctx.EvalContext`
/// (`GetOptionalPropProvider`).
pub trait EvalPropContext {
    /// Go `EvalContext.GetOptionalPropProvider`.
    fn get_optional_prop_provider(
        &self,
        key: OptionalEvalPropKey,
    ) -> Option<Arc<dyn DynOptionalEvalPropProvider>>;
}

/// Go `OptionalEvalPropProviders`: the per-key provider slots an `EvalContext`
/// carries. A missing slot is Go's `nil` element.
#[derive(Clone, Default)]
pub struct OptionalEvalPropProviders([Option<Arc<dyn DynOptionalEvalPropProvider>>; OPT_PROPS_CNT]);

impl fmt::Debug for OptionalEvalPropProviders {
    /// Providers are closures and trait objects with no useful debug form, so
    /// only the key set is rendered.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OptionalEvalPropProviders")
            .field("keys", &self.prop_key_set().0)
            .finish()
    }
}

impl OptionalEvalPropProviders {
    /// An empty set of providers, Go's zero value.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `OptionalEvalPropProviders.Contains`.
    #[must_use]
    pub fn contains(&self, key: OptionalEvalPropKey) -> bool {
        self.0[key.index()].is_some()
    }

    /// Go `OptionalEvalPropProviders.Get`.
    ///
    /// Go additionally range-checks the key, because a Go
    /// `OptionalEvalPropKey` is an `int` that may hold anything; a Rust enum
    /// cannot, so the check is unreachable and dropped.
    #[must_use]
    pub fn get(&self, key: OptionalEvalPropKey) -> Option<Arc<dyn DynOptionalEvalPropProvider>> {
        let val = self.0[key.index()].as_ref()?;
        debug_assert_eq!(key, val.desc().key());
        Some(Arc::clone(val))
    }

    /// Go `OptionalEvalPropProviders.Add`.
    ///
    /// Go's `intest.AssertFunc` block cross-checks that the provider's
    /// concrete type is the one registered for its key; that check is kept as
    /// a `debug_assert`.
    pub fn add(&mut self, val: Arc<dyn DynOptionalEvalPropProvider>) {
        debug_assert!(
            provider_type_matches_key(val.as_ref()),
            "provider concrete type does not match its property key {}",
            val.desc().key()
        );
        let key = val.desc().key();
        self.0[key.index()] = Some(val);
    }

    /// Go `OptionalEvalPropProviders.PropKeySet`.
    #[must_use]
    pub fn prop_key_set(&self) -> OptionalEvalPropKeySet {
        let mut set = OptionalEvalPropKeySet::default();
        for provider in self.0.iter().flatten() {
            set = set.add(provider.desc().key());
        }
        set
    }
}

/// The body of Go's `intest.AssertFunc` inside `OptionalEvalPropProviders.Add`:
/// each key admits exactly one concrete provider type.
fn provider_type_matches_key(val: &dyn DynOptionalEvalPropProvider) -> bool {
    let any = val.as_any();
    match val.desc().key() {
        OptionalEvalPropKey::CurrentUser => any.is::<CurrentUserPropProvider>(),
        OptionalEvalPropKey::SessionVars => any.is::<SessionVarsPropProvider>(),
        OptionalEvalPropKey::InfoSchema => any.is::<InfoSchemaPropProvider>(),
        OptionalEvalPropKey::KvStore => any.is::<KvStorePropProvider>(),
        OptionalEvalPropKey::SqlExecutor => any.is::<SqlExecutorPropProvider>(),
        OptionalEvalPropKey::SequenceOperator => any.is::<SequenceOperatorProvider>(),
        OptionalEvalPropKey::AdvisoryLock => any.is::<AdvisoryLockPropProvider>(),
        OptionalEvalPropKey::DdlOwnerInfo => any.is::<DdlOwnerInfoProvider>(),
        OptionalEvalPropKey::PrivilegeChecker => any.is::<PrivilegeCheckerProvider>(),
    }
}

/// Go's generic `getPropProvider[T]`: fetches the provider registered under
/// `key` and recovers its concrete type.
///
/// Go's `intest` stub check (`stub.Desc().Key() == key`) has no Rust analogue —
/// it needs a zero value of the type parameter — so it is dropped; the failed
/// cast below still reports the same mismatch.
pub fn get_prop_provider<T: DynOptionalEvalPropProvider>(
    ctx: &dyn EvalPropContext,
    key: OptionalEvalPropKey,
) -> Result<Arc<T>, ExprOptError> {
    let Some(val) = ctx.get_optional_prop_provider(key) else {
        return Err(ExprOptError::new(format!(
            "optional property: '{key}' not exists in EvalContext"
        )));
    };

    val.into_any().downcast::<T>().map_err(|_| {
        debug_assert!(false, "provider for '{key}' has an unexpected type");
        ExprOptError::new(format!(
            "cannot cast OptionalEvalPropProvider to {} for key '{key}'",
            std::any::type_name::<T>()
        ))
    })
}

#[cfg(test)]
mod tests;
