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
    WarnHandlerExt, WARN_LEVEL_ERROR, WARN_LEVEL_NOTE, WARN_LEVEL_WARNING,
};

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};

/// A context that can store values (Go `ValueStoreContext`). Keys are Go
/// `fmt.Stringer`s, so the Rust boundary is their rendered string; values are
/// Go `any`.
pub trait ValueStoreContext {
    /// Saves a value associated with this context for `key`.
    fn set_value(&mut self, key: &str, value: Box<dyn Any>);
    /// Returns the value associated with this context for `key`.
    fn value(&self, key: &str) -> Option<&dyn Any>;
    /// Clears the value associated with this context for `key`.
    fn clear_value(&mut self, key: &str);
}

static CONTEXT_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

/// Generates a unique context ID (Go `GenContextID`).
pub fn gen_context_id() -> u64 {
    CONTEXT_ID_GENERATOR.fetch_add(1, Ordering::Relaxed) + 1
}
