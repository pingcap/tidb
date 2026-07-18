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

//! Plan-cache parameter storage from `session.go`.
//!
//! This leaf ports the dependency-closed `PlanCacheParamList` value object:
//! reset, append, indexed lookup, all-value borrowing, and the
//! non-prepared-cache privacy bit. Parameter coercion, string rendering,
//! prepared-plan evaluation, and live session/EvalContext attachment remain
//! external.

use tidb_datatype::Datum;

/// Statement parameter values retained for plan-cache/evaluation consumers.
#[derive(Clone, Debug, Default)]
pub struct PlanCacheParamList {
    param_values: Vec<Datum>,
    for_non_prep_cache: bool,
}

impl PlanCacheParamList {
    /// Creates an empty parameter list with the source's initial capacity.
    #[must_use]
    pub fn new() -> Self {
        Self {
            param_values: Vec::with_capacity(8),
            for_non_prep_cache: false,
        }
    }

    /// Clears all values and restores prepared-cache visibility.
    pub fn reset(&mut self) {
        self.param_values.clear();
        self.for_non_prep_cache = false;
    }

    /// Appends one or more typed parameter values in source order.
    pub fn append<I>(&mut self, values: I)
    where
        I: IntoIterator<Item = Datum>,
    {
        self.param_values.extend(values);
    }

    /// Appends a single typed parameter value.
    pub fn push(&mut self, value: Datum) {
        self.param_values.push(value);
    }

    /// Sets whether parameter text should be hidden for a non-prepared cache.
    pub fn set_for_non_prep_cache(&mut self, enabled: bool) {
        self.for_non_prep_cache = enabled;
    }

    /// Returns the non-prepared-cache privacy bit.
    pub const fn for_non_prep_cache(&self) -> bool {
        self.for_non_prep_cache
    }

    /// Returns the value at `index`, preserving source indexing semantics.
    ///
    /// Like Go's `paramValues[idx]`, an out-of-range index panics. Callers
    /// that need fallible access should check [`Self::all_param_values`]
    /// first.
    #[must_use]
    pub fn get_param_value(&self, index: usize) -> &Datum {
        &self.param_values[index]
    }

    /// Borrows all values in source order.
    #[must_use]
    pub fn all_param_values(&self) -> &[Datum] {
        &self.param_values
    }
}
