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

//! Complete transcreation of Go `pkg/util/context` (`context.go`, `warn.go`,
//! `plancache.go`): the statement-context value-store shape, the SQL warning
//! (`SHOW WARNINGS`) domain, and the plan-cache/range-fallback trackers.
//!
//! Go's `SQLWarn.Err` is an open `error` that is either a `*terror.Error` or
//! any other error reduced to its message; the Rust equal is the closed
//! [`WarnErr`] enum, which is what the JSON form (`jsonSQLWarn`) already
//! distinguishes. Go's `errors.Cause` unwrapping of `errors.Trace` layers has
//! no Rust counterpart because wrapper layers don't exist here.

mod plancache;
mod warn;

pub use plancache::{
    PlanCacheTracker, PlanCacheType, RangeFallbackHandler, PLAN_CACHE_TRACKER_SAVED_FIELDS,
};
pub use warn::{
    FuncWarnAppender, IgnoreWarn, SqlWarn, StaticWarnHandler, WarnAppender, WarnErr, WarnHandler,
    WarnHandlerExt, MAX_WARNING_COUNT, WARN_LEVEL_ERROR, WARN_LEVEL_NOTE, WARN_LEVEL_WARNING,
};

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};

/// A context that can store values (Go `ValueStoreContext`).
///
/// Go accepts heterogeneous comparable `fmt.Stringer` keys. Rust models the
/// same application-level identity with an implementation-owned typed key
/// domain, normally an enum, instead of collapsing distinct key types onto
/// their rendered strings.
pub trait ValueStoreContext {
    /// The context's typed key domain.
    type Key: ?Sized;

    /// Saves a value associated with this context for `key`.
    fn set_value(&mut self, key: &Self::Key, value: Box<dyn Any>);
    /// Returns the value associated with this context for `key`.
    fn value(&self, key: &Self::Key) -> Option<&dyn Any>;
    /// Clears the value associated with this context for `key`.
    fn clear_value(&mut self, key: &Self::Key);
    /// Returns the domain bound to this context, or `None` for Go's nil value.
    fn get_domain(&self) -> Option<&dyn Any>;
}

static CONTEXT_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

/// Generates a unique context ID (Go `GenContextID`).
pub fn gen_context_id() -> u64 {
    CONTEXT_ID_GENERATOR
        .fetch_add(1, Ordering::SeqCst)
        .wrapping_add(1)
}
