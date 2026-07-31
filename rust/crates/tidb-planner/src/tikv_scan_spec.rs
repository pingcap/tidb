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

//! Pre-resolved TiKV scan metadata at the planner-to-executor boundary.
//!
//! Go's physical scan `ToPB` methods consume table/schema metadata that the
//! bounded Rust planner does not yet own. This module carries the exact output
//! of that resolution instead of guessing it. Live DDL schema-state selection
//! and default-value evaluation remain upstream prerequisites.

use crate::cardinality::index_range_policy::{IndexRangeShape, RangeBoundKind};

/// A source-resolved `tipb.ColumnInfo` without a protobuf dependency in the
/// planner crate.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanColumnInfo {
    /// Stable table column identity.
    pub column_id: i64,
    /// MySQL type code, already converted for table or index scan use.
    pub tp: i32,
    /// Rewritten collation identifier.
    pub collation: i32,
    /// Declared column length.
    pub column_len: i32,
    /// Declared decimal scale.
    pub decimal: i32,
    /// MySQL column flags.
    pub flag: i32,
    /// Enum/set elements.
    pub elems: Vec<String>,
    /// Already encoded origin default value, when one must be sent.
    pub default_val: Option<Vec<u8>>,
    /// Whether this column is the row handle.
    pub pk_handle: bool,
    /// Whether this is an array column used by a multi-valued index.
    pub array: bool,
}

/// Source features whose dependencies are deliberately outside the bounded
/// TiKV row-store scan lowering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedScanFeature {
    /// Partition pruning or a physical partition table ID is required.
    Partition,
    /// A column default still needs expression/session evaluation.
    UnresolvedDefaultValue,
    /// TiFlash late-materialization filters are present.
    LateMaterialization,
    /// Runtime filters are present.
    RuntimeFilter,
    /// A TiFlash columnar index is present.
    ColumnarIndex,
    /// A multi-valued index is present.
    MultiValuedIndex,
    /// A full-text query payload is present.
    FullTextSearch,
    /// A vector-query payload is present.
    VectorSearch,
}

/// Pre-resolved ordinary TiKV table-scan payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiKvTableScanSpec {
    /// Logical table identity.
    pub table_id: i64,
    /// Columns in executor schema order.
    pub columns: Vec<ScanColumnInfo>,
    /// Reverse scan direction.
    pub desc: bool,
    /// Preserve storage key order.
    pub keep_order: bool,
    /// Common-handle primary column IDs.
    pub primary_column_ids: Vec<i64>,
    /// Common-handle prefix column IDs.
    pub primary_prefix_column_ids: Vec<i64>,
    /// Explicit unsupported feature, if schema/planner resolution found one.
    pub unsupported: Option<UnsupportedScanFeature>,
}

impl TiKvTableScanSpec {
    /// Creates an admitted non-partitioned TiKV table scan.
    #[must_use]
    pub fn new(table_id: i64, columns: Vec<ScanColumnInfo>) -> Self {
        Self {
            table_id,
            columns,
            desc: false,
            keep_order: false,
            primary_column_ids: Vec::new(),
            primary_prefix_column_ids: Vec::new(),
            unsupported: None,
        }
    }

    /// Marks one source feature whose owner is not available.
    #[must_use]
    pub const fn with_unsupported(mut self, feature: UnsupportedScanFeature) -> Self {
        self.unsupported = Some(feature);
        self
    }
}

/// Pre-resolved ordinary TiKV index-scan payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiKvIndexScanSpec {
    /// Logical table identity.
    pub table_id: i64,
    /// Index identity.
    pub index_id: i64,
    /// Columns in executor schema order, including pre-resolved extra handles.
    pub columns: Vec<ScanColumnInfo>,
    /// Reverse scan direction.
    pub desc: bool,
    /// Whether the schema index is declared UNIQUE.
    pub declared_unique: bool,
    /// Number of indexed columns used by `checkCoverIndex`.
    pub index_column_count: usize,
    /// Common-handle primary column IDs, when required.
    pub primary_column_ids: Vec<i64>,
    /// Explicit unsupported feature, if schema/planner resolution found one.
    pub unsupported: Option<UnsupportedScanFeature>,
}

/// Authoritative schema identity shared by the access path and its ToPB data.
///
/// Go obtains all three values from the same `p.Index`. Rust requires that
/// authority explicitly so a cost candidate for one index cannot emit another
/// index's protobuf payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedIndexDescriptor {
    /// Stable schema index ID.
    pub index_id: i64,
    /// Schema UNIQUE property.
    pub declared_unique: bool,
    /// Number of schema index columns.
    pub index_column_count: usize,
}

/// A rejected attempt to combine independent index identities or shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexPushdownMetadataError {
    /// The cost/range candidate and resolved schema descriptor name different
    /// indexes.
    CandidateIndexId {
        /// ID used by cardinality/cost selection.
        candidate: i64,
        /// ID resolved from the schema index.
        descriptor: i64,
    },
    /// The protobuf payload and resolved schema descriptor name different
    /// indexes.
    PushdownIndexId {
        /// Authoritative schema ID.
        descriptor: i64,
        /// ID proposed for protobuf emission.
        pushdown: i64,
    },
    /// The protobuf UNIQUE bit differs from the schema descriptor.
    DeclaredUnique {
        /// Authoritative schema value.
        descriptor: bool,
        /// Value proposed for protobuf emission.
        pushdown: bool,
    },
    /// The width used by `checkCoverIndex` differs from the schema index.
    IndexColumnCount {
        /// Authoritative schema width.
        descriptor: usize,
        /// Width proposed for protobuf emission.
        pushdown: usize,
    },
}

/// Pushdown data whose identity and schema shape were checked once at the
/// access-path/physical-plan boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedIndexPushdown {
    spec: TiKvIndexScanSpec,
}

impl ValidatedIndexPushdown {
    /// Validates one source candidate, authoritative descriptor, and emitted
    /// payload before they can enter a physical plan.
    pub fn new(
        candidate_index_id: i64,
        descriptor: ResolvedIndexDescriptor,
        spec: TiKvIndexScanSpec,
    ) -> Result<Self, IndexPushdownMetadataError> {
        if candidate_index_id != descriptor.index_id {
            return Err(IndexPushdownMetadataError::CandidateIndexId {
                candidate: candidate_index_id,
                descriptor: descriptor.index_id,
            });
        }
        if spec.index_id != descriptor.index_id {
            return Err(IndexPushdownMetadataError::PushdownIndexId {
                descriptor: descriptor.index_id,
                pushdown: spec.index_id,
            });
        }
        if spec.declared_unique != descriptor.declared_unique {
            return Err(IndexPushdownMetadataError::DeclaredUnique {
                descriptor: descriptor.declared_unique,
                pushdown: spec.declared_unique,
            });
        }
        if spec.index_column_count != descriptor.index_column_count {
            return Err(IndexPushdownMetadataError::IndexColumnCount {
                descriptor: descriptor.index_column_count,
                pushdown: spec.index_column_count,
            });
        }
        Ok(Self { spec })
    }

    /// Returns the sole validated protobuf payload.
    #[must_use]
    pub const fn spec(&self) -> &TiKvIndexScanSpec {
        &self.spec
    }
}

impl TiKvIndexScanSpec {
    /// Creates an admitted non-partitioned ordinary TiKV index scan.
    #[must_use]
    pub fn new(
        table_id: i64,
        index_id: i64,
        columns: Vec<ScanColumnInfo>,
        declared_unique: bool,
        index_column_count: usize,
    ) -> Self {
        Self {
            table_id,
            index_id,
            columns,
            desc: false,
            declared_unique,
            index_column_count,
            primary_column_ids: Vec::new(),
            unsupported: None,
        }
    }

    /// Marks one source feature whose owner is not available.
    #[must_use]
    pub const fn with_unsupported(mut self, feature: UnsupportedScanFeature) -> Self {
        self.unsupported = Some(feature);
        self
    }
}

/// Returns the `IndexScan.unique` value sent by Go's `checkCoverIndex`.
///
/// A declared unique index is covered only when every range has one low value
/// per index column and neither endpoint contains SQL NULL. The source does
/// not require a single point range and does not compare low/high values here.
#[must_use]
pub fn check_cover_index(
    declared_unique: bool,
    index_column_count: usize,
    ranges: &[IndexRangeShape],
) -> bool {
    if !declared_unique {
        return false;
    }
    ranges.iter().all(|range| {
        range.low().len() == index_column_count
            && !range.low().contains(&RangeBoundKind::Null)
            && !range.high().contains(&RangeBoundKind::Null)
    })
}
