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

//! Predicate-column usage query boundaries from
//! `pkg/statistics/handle/usage/predicate_column.go`.
//!
//! The usage wrapper executes the all-column usage load without a transaction
//! wrapper, while predicate-column ID lookup explicitly passes
//! `FlagWrapTxn`. This leaf owns only that operation-to-transaction contract;
//! session pools, SQL execution, predicate-column storage, and analyze/session
//! lifecycle remain external.

/// Predicate-column usage operation exposed by the statistics handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredicateColumnOperation {
    /// Loads all recorded column usage with the caller's session context.
    LoadColumnStatsUsage,
    /// Reads predicate-column IDs inside a wrapped transaction.
    GetPredicateColumns,
}

impl PredicateColumnOperation {
    /// Returns whether the Go wrapper applies `FlagWrapTxn`.
    #[must_use]
    pub const fn wraps_transaction(self) -> bool {
        matches!(self, Self::GetPredicateColumns)
    }
}
