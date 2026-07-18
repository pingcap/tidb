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

//! Locked-statistics table payload from
//! `pkg/statistics/handle/types/interfaces.go`.
//!
//! Lock operations carry a table's fully qualified name and, when applicable,
//! a mapping of partition IDs to names. `None` preserves Go's nil map while
//! `Some(empty)` preserves an explicitly allocated empty map. This leaf owns
//! only that caller-owned payload; lock SQL, session/storage state, skipped
//! messages, and lock/unlock lifecycle remain external.

use std::collections::BTreeMap;

/// Table and optional partition names supplied to statistics locking.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsLockTable {
    /// Partition ID to partition name; `None` represents a nil Go map.
    pub partition_info: Option<BTreeMap<i64, String>>,
    /// Fully qualified schema and table name.
    pub full_name: String,
}

impl StatsLockTable {
    /// Builds a lock payload while preserving nil versus empty partition maps.
    #[must_use]
    pub fn new(
        full_name: impl Into<String>,
        partition_info: Option<BTreeMap<i64, String>>,
    ) -> Self {
        Self {
            partition_info,
            full_name: full_name.into(),
        }
    }
}
