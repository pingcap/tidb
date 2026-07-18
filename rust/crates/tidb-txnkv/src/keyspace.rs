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

//! Keyspace-mode predicates translated from `pkg/kv/utils.go`.
//!
//! The Go source asks the kernel mode and the storage's `GetKeyspace` value
//! independently. Keeping those inputs explicit avoids making a fake storage
//! client part of the transaction-KV foundation while preserving the exact
//! source distinction between classic mode and next-generation keyspaces.

/// The keyspace name reserved for system metadata in next-generation mode.
pub const SYSTEM_KEYSPACE: &str = "SYSTEM";

/// Kernel mode relevant to the source keyspace predicates.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum KernelType {
    /// The classic TiDB deployment has no keyspace-level routing.
    Classic,
    /// The next-generation deployment routes user and system keyspaces.
    NextGen,
}

/// Reports whether a store keyspace is a user keyspace.
///
/// This is the direct boolean expression from Go:
/// `kerneltype.IsNextGen() && store.GetKeyspace() != keyspace.System`.
#[must_use]
pub fn is_user_keyspace(kernel: KernelType, keyspace: &str) -> bool {
    matches!(kernel, KernelType::NextGen) && keyspace != SYSTEM_KEYSPACE
}

/// Reports whether a store is running on the reserved system keyspace.
///
/// This is the direct boolean expression from Go:
/// `kerneltype.IsNextGen() && store.GetKeyspace() == keyspace.System`.
#[must_use]
pub fn is_system_keyspace(kernel: KernelType, keyspace: &str) -> bool {
    matches!(kernel, KernelType::NextGen) && keyspace == SYSTEM_KEYSPACE
}

#[cfg(test)]
mod tests {
    use super::{is_system_keyspace, is_user_keyspace, KernelType, SYSTEM_KEYSPACE};

    #[test]
    fn classic_mode_never_classifies_a_keyspace() {
        for keyspace in ["", "user", SYSTEM_KEYSPACE, "other"] {
            assert!(!is_user_keyspace(KernelType::Classic, keyspace));
            assert!(!is_system_keyspace(KernelType::Classic, keyspace));
        }
    }

    #[test]
    fn next_gen_mode_distinguishes_only_the_system_name() {
        assert!(is_user_keyspace(KernelType::NextGen, "user"));
        assert!(!is_system_keyspace(KernelType::NextGen, "user"));
        assert!(!is_user_keyspace(KernelType::NextGen, SYSTEM_KEYSPACE));
        assert!(is_system_keyspace(KernelType::NextGen, SYSTEM_KEYSPACE));
        assert!(is_user_keyspace(KernelType::NextGen, ""));
    }
}
