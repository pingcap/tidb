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

//! CTE close-error precedence from `pkg/executor/cte.go`.
//!
//! The source CTE executor records the first non-`nil` close error while still
//! logging every later error. This leaf ports only the dependency-closed
//! precedence rule; logging, CTE worker lifecycle, failpoints, cleanup order,
//! and error rendering remain executor responsibilities.

/// Retains the first error, adopting a new error only when no error exists.
///
/// `None` is the Rust representation of Go's `nil` error. Values are generic
/// because the source helper preserves the concrete error object unchanged.
#[must_use]
pub fn retain_first_error<T>(first: Option<T>, new: Option<T>) -> Option<T> {
    match first {
        Some(error) => Some(error),
        None => new,
    }
}
