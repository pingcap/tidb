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

//! LogicalTopN identity from
//! `pkg/planner/core/operator/logicalop/logical_top_n.go` and its generated
//! Hash64/Equals implementation.
//!
//! The source identity hashes the TopN plan tag, output schema, ordered
//! ByItems and PartitionBy sort items, Offset, Count, and
//! PreferLimitToCop. This leaf preserves that field order over normalized
//! column adapters; arbitrary expression metadata, ExplainInfo formatting,
//! plan context/pruning, and runtime TopN behavior remain explicit external
//! boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized column identity used by TopN schemas and order expressions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TopNColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl TopNColumnIdentity {
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
pub struct TopNByItem {
    expr: TopNColumnIdentity,
    desc: bool,
}

impl TopNByItem {
    /// Creates an ordered ByItem from a normalized expression and direction.
    #[must_use]
    pub const fn new(expr: TopNColumnIdentity, desc: bool) -> Self {
        Self { expr, desc }
    }
}

/// A source-shaped `property.SortItem` over an optional column.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TopNSortItem {
    col: Option<TopNColumnIdentity>,
    desc: bool,
}

impl TopNSortItem {
    /// Creates a PartitionBy sort item from an optional column and direction.
    #[must_use]
    pub const fn new(col: Option<TopNColumnIdentity>, desc: bool) -> Self {
        Self { col, desc }
    }
}

/// Minimal LogicalTopN identity and ordering metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalTopNIdentity {
    schema: Option<Vec<TopNColumnIdentity>>,
    by_items: Option<Vec<TopNByItem>>,
    partition_by: Option<Vec<TopNSortItem>>,
    offset: u64,
    count: u64,
    prefer_limit_to_cop: bool,
}

impl LogicalTopNIdentity {
    /// Creates a TopN identity from source-owned metadata.
    #[must_use]
    pub fn new(
        schema: Option<Vec<TopNColumnIdentity>>,
        by_items: Option<Vec<TopNByItem>>,
        partition_by: Option<Vec<TopNSortItem>>,
        offset: u64,
        count: u64,
        prefer_limit_to_cop: bool,
    ) -> Self {
        Self {
            schema,
            by_items,
            partition_by,
            offset,
            count,
            prefer_limit_to_cop,
        }
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn schema(&self) -> Option<&[TopNColumnIdentity]> {
        self.schema.as_deref()
    }

    /// Returns the optional ordered ByItems list.
    #[must_use]
    pub fn by_items(&self) -> Option<&[TopNByItem]> {
        self.by_items.as_deref()
    }

    /// Returns the optional ordered PartitionBy list.
    #[must_use]
    pub fn partition_by(&self) -> Option<&[TopNSortItem]> {
        self.partition_by.as_deref()
    }

    /// Returns the source offset.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the source count.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Returns the source PreferLimitToCop flag.
    #[must_use]
    pub const fn prefer_limit_to_cop(&self) -> bool {
        self.prefer_limit_to_cop
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("TopN");
        hash_schema(&mut hasher, self.schema.as_deref());
        hash_by_items(&mut hasher, self.by_items.as_deref());
        hash_partition_by(&mut hasher, self.partition_by.as_deref());
        hasher.hash_uint64(self.offset);
        hasher.hash_uint64(self.count);
        hasher.hash_bool(self.prefer_limit_to_cop);
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_schema(hasher: &mut impl Hasher, schema: Option<&[TopNColumnIdentity]>) {
    match schema {
        Some(columns) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            for column in columns {
                hash_column(hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_by_items(hasher: &mut impl Hasher, by_items: Option<&[TopNByItem]>) {
    match by_items {
        Some(items) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(items.len() as i64);
            for item in items {
                hash_column(hasher, &item.expr);
                hasher.hash_bool(item.desc);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_partition_by(hasher: &mut impl Hasher, partition_by: Option<&[TopNSortItem]>) {
    match partition_by {
        Some(items) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(items.len() as i64);
            for item in items {
                match &item.col {
                    Some(column) => {
                        hasher.hash_byte(NOT_NIL_FLAG);
                        hash_column(hasher, column);
                    }
                    None => hasher.hash_byte(NIL_FLAG),
                }
                hasher.hash_bool(item.desc);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_column(hasher: &mut impl Hasher, column: &TopNColumnIdentity) {
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
