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

//! Typed slow-log threshold helpers from `pkg/sessionctx/variable/slow_log.go`.
//!
//! Go's slow-log accessors use `any` values but require the threshold and
//! field value to have the same concrete type. This leaf makes that boundary
//! explicit, preserving exact equality, same-type numeric `>=`, zero handling,
//! and signed-to-unsigned rejection. Field parsing, accessor registration,
//! statement/session mutation, and slow-log output remain external.

/// A typed value accepted by the slow-log threshold helpers.
#[derive(Clone, Debug, PartialEq)]
pub enum SlowLogValue {
    /// Signed integer value.
    Signed(i64),
    /// Unsigned integer value.
    Unsigned(u64),
    /// Floating-point value.
    Float(f64),
    /// Boolean value.
    Boolean(bool),
    /// Text value.
    Text(String),
}

/// Returns true when threshold and value have the same source type and value.
#[must_use]
pub fn matches_equal(threshold: &SlowLogValue, value: &SlowLogValue) -> bool {
    threshold == value
}

/// Returns true when threshold and value have the same numeric source type and
/// `value >= threshold`.
#[must_use]
pub fn matches_greater_equal(threshold: &SlowLogValue, value: &SlowLogValue) -> bool {
    match (threshold, value) {
        (SlowLogValue::Signed(threshold), SlowLogValue::Signed(value)) => value >= threshold,
        (SlowLogValue::Unsigned(threshold), SlowLogValue::Unsigned(value)) => value >= threshold,
        (SlowLogValue::Float(threshold), SlowLogValue::Float(value)) => value >= threshold,
        _ => false,
    }
}

/// Converts a signed value to an unsigned value only when it is non-negative.
#[must_use]
pub const fn uint64_from_non_negative(value: i64) -> Option<u64> {
    if value < 0 {
        None
    } else {
        Some(value as u64)
    }
}

/// Returns true only for a numeric zero threshold.
#[must_use]
pub fn matches_zero(threshold: &SlowLogValue) -> bool {
    match threshold {
        SlowLogValue::Signed(value) => *value == 0,
        SlowLogValue::Unsigned(value) => *value == 0,
        SlowLogValue::Float(value) => *value == 0.0,
        SlowLogValue::Boolean(_) | SlowLogValue::Text(_) => false,
    }
}
