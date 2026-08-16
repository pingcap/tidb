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

//! Stale-read TSO memoization from `stmtctx.go`.
//!
//! Source: `pkg/sessionctx/stmtctx/stmtctx.go:196-209` (the
//! `staleTSOProvider` holder and its `reset`) and `:1342-1369`
//! (`SetStaleTSOProviderIfNotExist`, `GetStaleTSO`).
//!
//! The provider seam is a boxed fallible closure, matching Go's stored
//! `func() (uint64, error)`; the error type is the caller's generic `E`
//! because this leaf does not own an error vocabulary. This leaf owns only
//! the once-set evaluator and its once-computed value; the PD client that
//! backs the closure, the stale-read planner that installs it, and the
//! statement reset cycle stay outside.

use std::sync::Mutex;

/// Go's stored `func() (uint64, error)` evaluator.
pub type StaleTsoEvaluator<E> = Box<dyn FnMut() -> Result<u64, E> + Send>;

struct StaleTsoState<E> {
    /// Go `staleTSOProvider.value`.
    value: Option<u64>,
    /// Go `staleTSOProvider.eval`.
    eval: Option<StaleTsoEvaluator<E>>,
}

/// Go `staleTSOProvider`: a mutex-guarded once-set evaluator whose result is
/// computed at most once per statement.
pub struct StaleTsoProvider<E> {
    state: Mutex<StaleTsoState<E>>,
}

impl<E> StaleTsoProvider<E> {
    /// Creates an empty provider (no evaluator, no cached value).
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(StaleTsoState {
                value: None,
                eval: None,
            }),
        }
    }

    /// Go `SetStaleTSOProviderIfNotExist`: installs `eval` unless an
    /// evaluator is already present; installing also clears any cached
    /// value, as in the source.
    pub fn set_if_not_exist(&self, eval: StaleTsoEvaluator<E>) {
        let mut state = self.state.lock().expect("stale TSO provider poisoned");
        if state.eval.is_some() {
            return;
        }
        state.value = None;
        state.eval = Some(eval);
    }

    /// Go `GetStaleTSO`: returns the cached TSO, or `0` when no evaluator is
    /// installed, or evaluates once and caches the result.
    ///
    /// As in the source, an evaluator error is returned as `Err` without
    /// caching, so a later call re-evaluates.
    pub fn get_stale_tso(&self) -> Result<u64, E> {
        let mut state = self.state.lock().expect("stale TSO provider poisoned");
        if let Some(value) = state.value {
            return Ok(value);
        }
        let Some(eval) = state.eval.as_mut() else {
            return Ok(0);
        };
        let tso = eval()?;
        state.value = Some(tso);
        Ok(tso)
    }

    /// Go `staleTSOProvider.reset`: drops the evaluator and the cached
    /// value.
    pub fn reset(&self) {
        let mut state = self.state.lock().expect("stale TSO provider poisoned");
        state.value = None;
        state.eval = None;
    }
}

impl<E> Default for StaleTsoProvider<E> {
    fn default() -> Self {
        Self::new()
    }
}
