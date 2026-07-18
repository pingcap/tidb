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

//! Non-transactional DML metric labels from `pkg/session/metrics/metrics.go`.
//!
//! The session package binds three counters to the shared
//! `NonTransactionalDMLCount` family using the exact `delete`, `insert`, and
//! `update` labels. This leaf preserves that label identity/order without
//! importing Prometheus, registering metric families, or incrementing live
//! counters.

/// Non-transactional DML statement kind used as the metric label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NonTransactionalDmlKind {
    /// Non-transactional DELETE.
    Delete,
    /// Non-transactional INSERT.
    Insert,
    /// Non-transactional UPDATE.
    Update,
}

impl NonTransactionalDmlKind {
    /// Returns the source `type` label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Insert => "insert",
            Self::Update => "update",
        }
    }
}

/// Returns the source initialization order for the three counters.
#[must_use]
pub const fn nontransactional_dml_metric_kinds() -> [NonTransactionalDmlKind; 3] {
    [
        NonTransactionalDmlKind::Delete,
        NonTransactionalDmlKind::Insert,
        NonTransactionalDmlKind::Update,
    ]
}
