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

//! Logical SELECT-lock classification from
//! `pkg/planner/core/operator/logicalop/logical_lock.go`.
//!
//! TiDB's AST owns the complete SelectLockType parser enum. This dependency-
//! closed leaf keeps its source iota discriminants and ports the logical plan's
//! three predicates: supported locks are the three FOR UPDATE variants and the
//! two FOR SHARE variants; skip-locked variants and unknown values remain
//! unsupported. AST metadata, column pruning, child plans, session context,
//! and lock planning/execution remain external boundaries.

/// Raw source-shaped `ast.SelectLockType` value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SelectLockType(i32);

impl SelectLockType {
    /// `ast.SelectLockNone`.
    pub const NONE: Self = Self(0);
    /// `ast.SelectLockForUpdate`.
    pub const FOR_UPDATE: Self = Self(1);
    /// `ast.SelectLockForShare`.
    pub const FOR_SHARE: Self = Self(2);
    /// `ast.SelectLockForUpdateNoWait`.
    pub const FOR_UPDATE_NO_WAIT: Self = Self(3);
    /// `ast.SelectLockForUpdateWaitN`.
    pub const FOR_UPDATE_WAIT_N: Self = Self(4);
    /// `ast.SelectLockForShareNoWait`.
    pub const FOR_SHARE_NO_WAIT: Self = Self(5);
    /// `ast.SelectLockForUpdateSkipLocked`.
    pub const FOR_UPDATE_SKIP_LOCKED: Self = Self(6);
    /// `ast.SelectLockForShareSkipLocked`.
    pub const FOR_SHARE_SKIP_LOCKED: Self = Self(7);

    /// Creates a lock type from an AST-compatible raw discriminant.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        Self(raw)
    }

    /// Returns the source raw discriminant.
    #[must_use]
    pub const fn raw(self) -> i32 {
        self.0
    }
}

/// Returns whether the source lock type acquires a FOR UPDATE pessimistic lock.
#[must_use]
pub const fn is_for_update_lock_type(lock_type: SelectLockType) -> bool {
    let raw = lock_type.raw();
    raw == SelectLockType::FOR_UPDATE.raw()
        || raw == SelectLockType::FOR_UPDATE_NO_WAIT.raw()
        || raw == SelectLockType::FOR_UPDATE_WAIT_N.raw()
}

/// Returns whether the source lock type acquires a FOR SHARE pessimistic lock.
#[must_use]
pub const fn is_for_share_lock_type(lock_type: SelectLockType) -> bool {
    let raw = lock_type.raw();
    raw == SelectLockType::FOR_SHARE.raw() || raw == SelectLockType::FOR_SHARE_NO_WAIT.raw()
}

/// Returns whether TiDB supports acquiring a pessimistic lock for this type.
#[must_use]
pub const fn is_supported_select_lock_type(lock_type: SelectLockType) -> bool {
    is_for_update_lock_type(lock_type) || is_for_share_lock_type(lock_type)
}

#[cfg(test)]
mod tests {
    use super::{
        is_for_share_lock_type, is_for_update_lock_type, is_supported_select_lock_type,
        SelectLockType,
    };

    #[test]
    fn update_variants_match_source_predicate() {
        for lock_type in [
            SelectLockType::FOR_UPDATE,
            SelectLockType::FOR_UPDATE_NO_WAIT,
            SelectLockType::FOR_UPDATE_WAIT_N,
        ] {
            assert!(is_for_update_lock_type(lock_type));
            assert!(!is_for_share_lock_type(lock_type));
            assert!(is_supported_select_lock_type(lock_type));
        }
    }

    #[test]
    fn share_variants_match_source_predicate() {
        for lock_type in [SelectLockType::FOR_SHARE, SelectLockType::FOR_SHARE_NO_WAIT] {
            assert!(!is_for_update_lock_type(lock_type));
            assert!(is_for_share_lock_type(lock_type));
            assert!(is_supported_select_lock_type(lock_type));
        }
    }

    #[test]
    fn none_skip_locked_and_unknown_values_are_unsupported() {
        for raw in [
            SelectLockType::NONE.raw(),
            SelectLockType::FOR_UPDATE_SKIP_LOCKED.raw(),
            SelectLockType::FOR_SHARE_SKIP_LOCKED.raw(),
            -1,
            99,
        ] {
            let lock_type = SelectLockType::from_raw(raw);
            assert!(!is_for_update_lock_type(lock_type));
            assert!(!is_for_share_lock_type(lock_type));
            assert!(!is_supported_select_lock_type(lock_type));
        }
    }

    #[test]
    fn raw_discriminants_preserve_go_iota_order() {
        assert_eq!(SelectLockType::NONE.raw(), 0);
        assert_eq!(SelectLockType::FOR_UPDATE.raw(), 1);
        assert_eq!(SelectLockType::FOR_SHARE.raw(), 2);
        assert_eq!(SelectLockType::FOR_UPDATE_NO_WAIT.raw(), 3);
        assert_eq!(SelectLockType::FOR_UPDATE_WAIT_N.raw(), 4);
        assert_eq!(SelectLockType::FOR_SHARE_NO_WAIT.raw(), 5);
        assert_eq!(SelectLockType::FOR_UPDATE_SKIP_LOCKED.raw(), 6);
        assert_eq!(SelectLockType::FOR_SHARE_SKIP_LOCKED.raw(), 7);
    }
}
