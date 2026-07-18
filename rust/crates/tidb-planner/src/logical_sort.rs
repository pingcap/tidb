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

//! LogicalSort identity from
//! `pkg/planner/core/operator/logicalop/logical_sort.go` and its generated
//! Hash64/Equals implementation.
//!
//! The source identity hashes the Sort plan tag and optional ordered ByItems.
//! This leaf preserves that framing over normalized column expressions and
//! directions; arbitrary expression Hash64 implementations, ExplainByItems
//! formatting, plan context, pruning, and runtime ordering remain explicit
//! external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized column expression identity used by a Sort ByItem.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SortColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl SortColumnIdentity {
    /// Creates a column identity without a caller-supplied type fingerprint.
    #[must_use]
    pub const fn new(id: i64, unique_id: i64, index: i64) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: None,
        }
    }

    /// Creates a column identity with a normalized type fingerprint.
    #[must_use]
    pub const fn with_type_fingerprint(
        id: i64,
        unique_id: i64,
        index: i64,
        type_fingerprint: u64,
    ) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: Some(type_fingerprint),
        }
    }
}

/// A source-shaped `util.ByItems` over a normalized column expression.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SortByItem {
    expr: SortColumnIdentity,
    desc: bool,
}

impl SortByItem {
    /// Creates a ByItem from a normalized expression and source direction.
    #[must_use]
    pub const fn new(expr: SortColumnIdentity, desc: bool) -> Self {
        Self { expr, desc }
    }
}

/// Minimal LogicalSort identity and ordered ByItems.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalSortIdentity {
    by_items: Option<Vec<SortByItem>>,
}

impl LogicalSortIdentity {
    /// Creates a Sort identity from an optional ordered ByItems list.
    #[must_use]
    pub fn new(by_items: Option<Vec<SortByItem>>) -> Self {
        Self { by_items }
    }

    /// Returns the optional ordered ByItems list.
    #[must_use]
    pub fn by_items(&self) -> Option<&[SortByItem]> {
        self.by_items.as_deref()
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("Sort");
        match &self.by_items {
            Some(items) => {
                hasher.hash_byte(NOT_NIL_FLAG);
                hasher.hash_int(items.len() as i64);
                for item in items {
                    hash_by_item(&mut hasher, item);
                }
            }
            None => hasher.hash_byte(NIL_FLAG),
        }
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_by_item(hasher: &mut impl Hasher, item: &SortByItem) {
    hash_column(hasher, &item.expr);
    hasher.hash_bool(item.desc);
}

fn hash_column(hasher: &mut impl Hasher, column: &SortColumnIdentity) {
    match column.type_fingerprint {
        Some(fingerprint) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_uint64(fingerprint);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.hash_int64(column.id);
    hasher.hash_int64(column.unique_id);
    hasher.hash_int(column.index);
}
