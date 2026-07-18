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

//! Comma-separated privilege-set helpers from `pkg/executor/utils.go`.
//!
//! TiDB stores table/column privilege names as comma-separated strings. These
//! helpers preserve the source's empty-value nil boundary, exact string
//! matching, insertion order, duplicate suppression, and first-match removal.
//! SQL GRANT/REVOKE execution, privilege-table persistence, and collation
//! semantics remain external.

/// Splits a comma-separated set, returning `None` for the source empty/nil
/// representation.
#[must_use]
pub fn set_from_string(value: &str) -> Option<Vec<String>> {
    if value.is_empty() {
        None
    } else {
        Some(value.split(',').map(str::to_owned).collect())
    }
}

/// Joins set members with the source comma separator.
#[must_use]
pub fn set_to_string(set: &[String]) -> String {
    set.join(",")
}

/// Adds a value only when no exact value is already present.
#[must_use]
pub fn add_to_set(mut set: Vec<String>, value: &str) -> Vec<String> {
    if !set.iter().any(|entry| entry == value) {
        set.push(value.to_owned());
    }
    set
}

/// Removes the first exact matching value, preserving the order of others.
#[must_use]
pub fn delete_from_set(mut set: Vec<String>, value: &str) -> Vec<String> {
    if let Some(index) = set.iter().position(|entry| entry == value) {
        set.remove(index);
    }
    set
}
