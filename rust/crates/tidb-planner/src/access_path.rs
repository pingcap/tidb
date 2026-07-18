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

//! Access-path identities from `pkg/planner/util/path.go`.
//!
//! Go decides point-get eligibility and derives `AccessPath.CountAfterAccess`
//! before physical index conversion. Both are explicit at this boundary, so a
//! generic Rust candidate cannot silently become an index scan.

use crate::{
    cardinality::live_index_optimizer::{estimate_proven_point_rows, LiveIndexCandidate},
    scan_pushdown::{
        IndexPushdownMetadataError, ResolvedIndexDescriptor, TiKvIndexScanSpec,
        ValidatedIndexPushdown,
    },
};

/// Storage engine selected by an access path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessPathStore {
    /// TiKV row-store access.
    TiKv,
    /// TiFlash columnar-store access.
    TiFlash,
}

/// The source path's required read shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexReadShape {
    /// The index itself supplies all required columns.
    SingleRead,
    /// A table lookup would be required after the index scan.
    DoubleRead,
}

/// Result of Go's upstream point-get admission for an access path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PointGetAdmission {
    /// The upstream point-get decision was not supplied.
    Unproven,
    /// Upstream proved this path cannot become PointGet or BatchPointGet.
    NotEligible,
    /// Upstream admitted PointGet or BatchPointGet construction.
    Eligible,
}

/// Exact source cardinality for one limited `ExpectedCnt` property.
///
/// The Go adjustment depends on datasource statistics, ordering, filters, and
/// session state. The Rust slice accepts only an exact upstream result rather
/// than approximating it from a range estimate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExpectedCountRows {
    expected_cnt: f64,
    scan_rows: f64,
}

impl ExpectedCountRows {
    /// Records the exact Go scan cardinality for one requested row count.
    #[must_use]
    pub const fn new(expected_cnt: f64, scan_rows: f64) -> Self {
        Self {
            expected_cnt,
            scan_rows,
        }
    }

    /// Returns the source property value for which this was derived.
    #[must_use]
    pub const fn expected_cnt(self) -> f64 {
        self.expected_cnt
    }

    /// Returns the source-adjusted physical index-scan cardinality.
    #[must_use]
    pub const fn scan_rows(self) -> f64 {
        self.scan_rows
    }
}

/// Why the isolated equality adapter cannot construct an admitted path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PointEstimateAdmissionError {
    /// The normalized range is not an upstream-proven equality lookup.
    UnsupportedRange,
}

/// An ordinary index path that may be considered by the bounded task builder.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexAccessPath {
    candidate: LiveIndexCandidate,
    count_after_access: Option<f64>,
    point_get_admission: PointGetAdmission,
    expected_count_rows: Option<ExpectedCountRows>,
    store: AccessPathStore,
    read_shape: IndexReadShape,
    multi_valued: bool,
    pushdown: Option<Box<ValidatedIndexPushdown>>,
}

impl IndexAccessPath {
    /// Creates an unproven ordinary TiKV single-read path.
    ///
    /// It deliberately lacks both source admissions and therefore fails closed
    /// in task construction. Production translation must use one of the two
    /// admitted constructors below.
    #[must_use]
    pub const fn new(candidate: LiveIndexCandidate) -> Self {
        Self {
            candidate,
            count_after_access: None,
            point_get_admission: PointGetAdmission::Unproven,
            expected_count_rows: None,
            store: AccessPathStore::TiKv,
            read_shape: IndexReadShape::SingleRead,
            multi_valued: false,
            pushdown: None,
        }
    }

    /// Creates a source-admitted path with precomputed `CountAfterAccess`.
    #[must_use]
    pub const fn from_source_index_scan(
        candidate: LiveIndexCandidate,
        count_after_access: f64,
        point_get_admission: PointGetAdmission,
    ) -> Self {
        Self {
            candidate,
            count_after_access: Some(count_after_access),
            point_get_admission,
            expected_count_rows: None,
            store: AccessPathStore::TiKv,
            read_shape: IndexReadShape::SingleRead,
            multi_valued: false,
            pushdown: None,
        }
    }

    /// Creates a path through the isolated, proven equality-estimate adapter.
    ///
    /// This is the sole local use of `Index.QueryBytes`-style statistics. Full
    /// and arbitrary ranges must arrive through [`Self::from_source_index_scan`]
    /// with Go's already-derived `CountAfterAccess`.
    pub fn from_proven_point_estimate(
        candidate: LiveIndexCandidate,
        point_get_admission: PointGetAdmission,
    ) -> Result<Self, PointEstimateAdmissionError> {
        let Some(count_after_access) = estimate_proven_point_rows(&candidate) else {
            return Err(PointEstimateAdmissionError::UnsupportedRange);
        };
        Ok(Self::from_source_index_scan(
            candidate,
            count_after_access,
            point_get_admission,
        ))
    }

    /// Changes the selected storage engine.
    #[must_use]
    pub const fn with_store(mut self, store: AccessPathStore) -> Self {
        self.store = store;
        self
    }

    /// Changes whether this path requires a table lookup after the index scan.
    #[must_use]
    pub const fn with_read_shape(mut self, read_shape: IndexReadShape) -> Self {
        self.read_shape = read_shape;
        self
    }

    /// Marks this path as a multi-valued index path.
    #[must_use]
    pub const fn with_multi_valued(mut self, multi_valued: bool) -> Self {
        self.multi_valued = multi_valued;
        self
    }

    /// Supplies schema-resolved metadata for the later TiKV protobuf lowering.
    pub fn with_pushdown(
        mut self,
        descriptor: ResolvedIndexDescriptor,
        pushdown: TiKvIndexScanSpec,
    ) -> Result<Self, IndexPushdownMetadataError> {
        let validated = ValidatedIndexPushdown::new(self.candidate.index_id, descriptor, pushdown)?;
        self.pushdown = Some(Box::new(validated));
        Ok(self)
    }

    /// Supplies exact Go cardinality for one limited `ExpectedCnt`.
    #[must_use]
    pub const fn with_expected_count_rows(mut self, rows: ExpectedCountRows) -> Self {
        self.expected_count_rows = Some(rows);
        self
    }

    /// Returns the one authoritative cost candidate.
    #[must_use]
    pub const fn candidate(&self) -> &LiveIndexCandidate {
        &self.candidate
    }

    /// Returns the selected storage engine.
    #[must_use]
    pub const fn store(&self) -> AccessPathStore {
        self.store
    }

    /// Returns the source read shape.
    #[must_use]
    pub const fn read_shape(&self) -> IndexReadShape {
        self.read_shape
    }

    /// Reports whether the path is a multi-valued index.
    #[must_use]
    pub const fn is_multi_valued(&self) -> bool {
        self.multi_valued
    }

    /// Returns source `CountAfterAccess`, when upstream supplied it.
    #[must_use]
    pub const fn count_after_access(&self) -> Option<f64> {
        self.count_after_access
    }

    /// Returns the upstream point-get decision.
    #[must_use]
    pub const fn point_get_admission(&self) -> PointGetAdmission {
        self.point_get_admission
    }

    /// Returns exact limited-property cardinality, if supplied.
    #[must_use]
    pub const fn expected_count_rows(&self) -> Option<ExpectedCountRows> {
        self.expected_count_rows
    }

    /// Returns schema-resolved TiKV metadata, if the source adapter supplied it.
    #[must_use]
    pub fn pushdown(&self) -> Option<&ValidatedIndexPushdown> {
        match self.pushdown.as_ref() {
            Some(pushdown) => Some(pushdown.as_ref()),
            None => None,
        }
    }

    /// Returns whether ranger produced no ranges.
    #[must_use]
    pub fn has_empty_ranges(&self) -> bool {
        self.candidate.ranges.is_empty()
    }
}

/// A datasource access-path alternative considered by the bounded planner.
#[derive(Clone, Debug, PartialEq)]
pub enum DataSourceAccessPath {
    /// An ordinary index path.
    Index(IndexAccessPath),
    /// A table scan path.
    Table,
    /// An index-merge path.
    IndexMerge,
}
