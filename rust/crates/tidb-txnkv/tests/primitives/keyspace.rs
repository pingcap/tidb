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

//! Direct translations of `pkg/kv/utils_test.go` keyspace predicates.

use tidb_txnkv::{is_system_keyspace, is_user_keyspace, KernelType, SYSTEM_KEYSPACE};

/// Direct translation of `pkg/kv/utils_test.go:136 TestIsUserKS`.
#[test]
fn test_is_user_ks() {
    assert!(!is_user_keyspace(KernelType::Classic, ""));
    assert!(is_user_keyspace(KernelType::NextGen, "user"));
    assert!(!is_user_keyspace(KernelType::NextGen, SYSTEM_KEYSPACE));
}

/// Direct translation of `pkg/kv/utils_test.go:149 TestIsSystemKS`.
#[test]
fn test_is_system_ks() {
    assert!(!is_system_keyspace(KernelType::Classic, ""));
    assert!(!is_system_keyspace(KernelType::NextGen, "user"));
    assert!(is_system_keyspace(KernelType::NextGen, SYSTEM_KEYSPACE));
}
