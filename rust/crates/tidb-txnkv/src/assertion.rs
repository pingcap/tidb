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
    /// `pkg/kv/assertion.go`. Table-level first-assertion-wins behavior is a
    /// separate transition in [`Self::apply_assertion_once`], so raw buffer
    /// code cannot accidentally acquire the stronger table rule.
    #[must_use]
    pub const fn apply_assertion_op(self, op: AssertionOp) -> Self {
        match op {
            AssertionOp::AssertExist => self.with_assertion_state(AssertionState::Exists),
            AssertionOp::AssertNotExist => self.with_assertion_state(AssertionState::NotExists),
            AssertionOp::AssertUnknown => self.with_assertion_state(AssertionState::Unknown),
            AssertionOp::AssertNone => self,
        }
    }

    /// Applies Go `pkg/table/tables.setAssertion`'s transaction-buffer rule.
    ///
    /// The first non-empty assertion attached to a key wins for the lifetime
    /// of that buffered entry. Later table operations may replace the value
    /// and add ordinary flags, but they cannot weaken or reverse the assertion
    /// made by the operation that first touched the key. `AssertNone` leaves
    /// an unasserted key available for a later real assertion.
    #[must_use]
    pub const fn apply_assertion_once(self, op: AssertionOp) -> Self {
        if self.has_assertion_flags() {
            self
        } else {
            self.apply_assertion_op(op)
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

    #[test]
    fn test_set_assertion() {
        // Direct port of pkg/table/tables/assertion_test.go::TestSetAssertion.
        // KeyFlags is the typed state stored by the Rust transaction buffer;
        // value writes are deliberately represented by leaving these flags
        // untouched, exactly as txn.Set does in the source test.
        fn set(flags: &mut KeyFlags, assertion: AssertionOp) {
            *flags = flags.apply_assertion_once(assertion);
        }

        fn assert_unchangeable(flags: &mut KeyFlags, expected: AssertionState) {
            for assertion in ASSERTION_OPS {
                set(flags, assertion);
                assert_eq!(flags.assertion(), expected);
            }
        }

        let mut k1 = KeyFlags::new();
        set(&mut k1, AssertionOp::AssertExist);
        assert_eq!(k1.assertion(), AssertionState::Exists);
        assert_unchangeable(&mut k1, AssertionState::Exists);

        let mut k2 = KeyFlags::new();
        set(&mut k2, AssertionOp::AssertNotExist);
        assert_eq!(k2.assertion(), AssertionState::NotExists);
        assert_unchangeable(&mut k2, AssertionState::NotExists);

        let mut k3 = KeyFlags::new();
        set(&mut k3, AssertionOp::AssertUnknown);
        assert_eq!(k3.assertion(), AssertionState::Unknown);
        assert_unchangeable(&mut k3, AssertionState::Unknown);

        let mut k4 = KeyFlags::new();
        set(&mut k4, AssertionOp::AssertNone);
        assert_eq!(k4.assertion(), AssertionState::Unset);
        set(&mut k4, AssertionOp::AssertExist);
        assert_eq!(k4.assertion(), AssertionState::Exists);
        assert_unchangeable(&mut k4, AssertionState::Exists);

        // A plain txn.Set writes a value without inventing an assertion.
        let mut k5 = KeyFlags::new();
        assert_eq!(k5.assertion(), AssertionState::Unset);
        set(&mut k5, AssertionOp::AssertNotExist);
        assert_unchangeable(&mut k5, AssertionState::NotExists);

        let mut k6 = KeyFlags::new();
        set(&mut k6, AssertionOp::AssertNotExist);
        k6 = k6.apply_flags_ops([FlagsOp::SetPresumeKeyNotExists]);
        assert_unchangeable(&mut k6, AssertionState::NotExists);
        assert!(k6.has_presume_key_not_exists());
        k6 = k6.apply_flags_ops([FlagsOp::SetNeedLocked]);
        assert_unchangeable(&mut k6, AssertionState::NotExists);
        assert!(k6.has_presume_key_not_exists());
        assert!(k6.has_need_locked());

        // LockKeys does not pass through table.setAssertion.
        let k7 = KeyFlags::new();
        assert_eq!(k7.assertion(), AssertionState::Unset);
        assert!(!k7.has_assertion_flags());
    }
}
