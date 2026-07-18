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

//! LogicalLimit identity from
//! `pkg/planner/core/operator/logicalop/logical_limit.go` and its generated
//! Hash64/Equals implementation.
//!
//! The source identity hashes the Limit plan tag, output schema, optional
//! PartitionBy sort items, Offset, and Count in that order. This leaf keeps
//! the same framing over normalized column identities; complete FieldType,
//! collation, VirtualExpr, property ExplainPartitionBy, plan context, and
//! runtime limit behavior remain explicit external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized expression-column identity used by a Limit schema or sort item.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LimitColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl LimitColumnIdentity {
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

/// A source-shaped `property.SortItem` with a normalized optional column.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LimitSortItem {
    col: Option<LimitColumnIdentity>,
    desc: bool,
}

impl LimitSortItem {
    /// Creates a sort item from an optional column and source direction.
    #[must_use]
    pub const fn new(col: Option<LimitColumnIdentity>, desc: bool) -> Self {
        Self { col, desc }
    }
}

/// Minimal LogicalLimit identity and explain metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalLimitIdentity {
    schema: Option<Vec<LimitColumnIdentity>>,
    partition_by: Option<Vec<LimitSortItem>>,
    offset: u64,
    count: u64,
}

impl LogicalLimitIdentity {
    /// Creates a Limit identity from source-owned metadata.
    #[must_use]
    pub fn new(
        schema: Option<Vec<LimitColumnIdentity>>,
        partition_by: Option<Vec<LimitSortItem>>,
        offset: u64,
        count: u64,
    ) -> Self {
        Self {
            schema,
            partition_by,
            offset,
            count,
        }
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn schema(&self) -> Option<&[LimitColumnIdentity]> {
        self.schema.as_deref()
    }

    /// Returns the optional source PartitionBy list.
    #[must_use]
    pub fn partition_by(&self) -> Option<&[LimitSortItem]> {
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

    /// Returns source-shaped ExplainInfo metadata.
    ///
    /// The no-PartitionBy branch is exact. The partition expression formatter
    /// is owned by `property.ExplainPartitionBy` in Go and is therefore kept
    /// as a structural item-count boundary in this normalized adapter.
    #[must_use]
    pub fn explain_info(&self) -> String {
        match &self.partition_by {
            Some(items) if !items.is_empty() => format!(
                "partition by {} items, offset:{}, count:{}",
                items.len(),
                self.offset,
                self.count
            ),
            _ => format!("offset:{}, count:{}", self.offset, self.count),
        }
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("Limit");
        hash_schema(&mut hasher, self.schema.as_deref());
        hash_partition_by(&mut hasher, self.partition_by.as_deref());
        hasher.hash_uint64(self.offset);
        hasher.hash_uint64(self.count);
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_schema(hasher: &mut impl Hasher, schema: Option<&[LimitColumnIdentity]>) {
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

fn hash_partition_by(hasher: &mut impl Hasher, partition_by: Option<&[LimitSortItem]>) {
    match partition_by {
        Some(items) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(items.len() as i64);
            for item in items {
                hash_sort_item(hasher, item);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_sort_item(hasher: &mut impl Hasher, item: &LimitSortItem) {
    match &item.col {
        Some(column) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hash_column(hasher, column);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.hash_bool(item.desc);
}

fn hash_column(hasher: &mut impl Hasher, column: &LimitColumnIdentity) {
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
