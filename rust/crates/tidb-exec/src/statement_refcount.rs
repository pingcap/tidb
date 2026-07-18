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

//! Statement-context reference/freeze state from `stmtctx.go`.
//!
//! `ReferenceCount` is the small synchronization primitive used while a
//! session reuses a cached statement context. A frozen value (`-1`) rejects
//! new references; zero is the only state that can transition to frozen. This
//! leaf ports those atomic transitions only. It does not own the cached
//! contexts, reset locks, session variables, or statement execution.

use std::sync::atomic::{AtomicI32, Ordering};

/// Sentinel used while the statement context is being reset.
pub const REFERENCE_COUNT_IS_FROZEN: i32 = -1;
/// Value meaning that no other session currently references the context.
pub const REFERENCE_COUNT_NO_REFERENCE: i32 = 0;

/// Atomic reference count with the source frozen sentinel state.
pub struct ReferenceCount(AtomicI32);

impl ReferenceCount {
    /// Creates an unfrozen count with no references.
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicI32::new(REFERENCE_COUNT_NO_REFERENCE))
    }

    /// Creates a count from a source-compatible value.
    #[must_use]
    pub const fn from_value(value: i32) -> Self {
        Self(AtomicI32::new(value))
    }

    /// Returns the current count or frozen sentinel.
    pub fn load(&self) -> i32 {
        self.0.load(Ordering::SeqCst)
    }

    /// Attempts to add one reference unless the count is frozen.
    ///
    /// This is the source CAS loop from `TryIncrease`; the returned `false`
    /// means that the observed state was frozen. A concurrent freeze may
    /// still complete around a successful increment, matching the source
    /// method's documented race window.
    pub fn try_increase(&self) -> bool {
        let mut observed = self.load();
        loop {
            if observed == REFERENCE_COUNT_IS_FROZEN {
                return false;
            }
            match self.0.compare_exchange_weak(
                observed,
                observed.wrapping_add(1),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(next) => observed = next,
            }
        }
    }

    /// Decreases the count by one.
    ///
    /// Callers must hold a reference before decreasing, as in Go's source
    /// helper; this method intentionally does not add a new underflow policy.
    pub fn decrease(&self) {
        let mut observed = self.load();
        loop {
            match self.0.compare_exchange_weak(
                observed,
                observed.wrapping_sub(1),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(next) => observed = next,
            }
        }
    }

    /// Attempts to transition an unreferenced count to the frozen sentinel.
    pub fn try_freeze(&self) -> bool {
        if self.load() != REFERENCE_COUNT_NO_REFERENCE {
            return false;
        }
        self.0
            .compare_exchange(
                REFERENCE_COUNT_NO_REFERENCE,
                REFERENCE_COUNT_IS_FROZEN,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    /// Returns a frozen count to the unfrozen, unreferenced state.
    pub fn unfreeze(&self) {
        self.0.store(REFERENCE_COUNT_NO_REFERENCE, Ordering::SeqCst);
    }
}

impl Default for ReferenceCount {
    fn default() -> Self {
        Self::new()
    }
}
