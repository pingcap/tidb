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

//! Assertion transitions translated from `pkg/kv/assertion.go`.

use crate::{AssertionState, KeyFlags};

/// An assertion operation on a key.
///
/// This is intentionally distinct from [`crate::FlagsOp`]. `AssertNone` is a
/// no-op in Go; it does not clear an assertion already present on the flags.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum AssertionOp {
    /// Assert that the key exists.
    AssertExist,
    /// Assert that the key does not exist.
    AssertNotExist,
    /// Mark the key as unable to carry an existence assertion.
    AssertUnknown,
    /// Leave the current assertion state unchanged.
    AssertNone,
}

impl KeyFlags {
    /// Applies one source assertion operation.
    ///
    /// This primitive performs the replacement defined by
    /// `pkg/kv/assertion.go`. Transaction-level first-assertion-wins behavior is
    /// owned by `pkg/table/tables/assertion.go` and requires a real transaction
    /// buffer; it is deliberately not pretended here.
    #[must_use]
    pub const fn apply_assertion_op(self, op: AssertionOp) -> Self {
        match op {
            AssertionOp::AssertExist => self.with_assertion_state(AssertionState::Exists),
            AssertionOp::AssertNotExist => self.with_assertion_state(AssertionState::NotExists),
            AssertionOp::AssertUnknown => self.with_assertion_state(AssertionState::Unknown),
            AssertionOp::AssertNone => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AssertionOp;
    use crate::{AssertionState, FlagsOp, KeyFlags};

    const ASSERTION_STATES: [AssertionState; 4] = [
        AssertionState::Unset,
        AssertionState::Exists,
        AssertionState::NotExists,
        AssertionState::Unknown,
    ];
    const ASSERTION_OPS: [AssertionOp; 4] = [
        AssertionOp::AssertExist,
        AssertionOp::AssertNotExist,
        AssertionOp::AssertUnknown,
        AssertionOp::AssertNone,
    ];

    fn all_semantic_states() -> Vec<KeyFlags> {
        let mut states = Vec::with_capacity(64);
        for presume_key_not_exists in [false, true] {
            for need_locked in [false, true] {
                for assertion in ASSERTION_STATES {
                    for need_constraint_check_in_prewrite in [false, true] {
                        for previous_presume_key_not_exists in [false, true] {
                            let mut ops = Vec::with_capacity(4);
                            if presume_key_not_exists {
                                ops.push(FlagsOp::SetPresumeKeyNotExists);
                            }
                            if need_locked {
                                ops.push(FlagsOp::SetNeedLocked);
                            }
                            if need_constraint_check_in_prewrite {
                                ops.push(FlagsOp::SetNeedConstraintCheckInPrewrite);
                            }
                            if previous_presume_key_not_exists {
                                ops.push(FlagsOp::SetPreviousPresumeKeyNotExists);
                            }
                            states.push(
                                KeyFlags::new()
                                    .apply_flags_ops(ops)
                                    .with_assertion_state(assertion),
                            );
                        }
                    }
                }
            }
        }
        states
    }

    #[test]
    fn every_assertion_operation_matches_all_64_source_states() {
        for origin in all_semantic_states() {
            for op in ASSERTION_OPS {
                let actual = origin.apply_assertion_op(op);
                let expected_state = match op {
                    AssertionOp::AssertExist => AssertionState::Exists,
                    AssertionOp::AssertNotExist => AssertionState::NotExists,
                    AssertionOp::AssertUnknown => AssertionState::Unknown,
                    AssertionOp::AssertNone => origin.assertion(),
                };
                let expected = origin.with_assertion_state(expected_state);

                assert_eq!(actual, expected, "origin={origin:?}, op={op:?}");
            }
        }
    }

    #[test]
    fn assert_none_is_noop_not_clear() {
        for assertion in ASSERTION_STATES {
            let origin = KeyFlags::new().with_assertion_state(assertion);
            assert_eq!(origin.apply_assertion_op(AssertionOp::AssertNone), origin);
        }
    }
}
