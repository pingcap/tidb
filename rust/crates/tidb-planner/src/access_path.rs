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
        IndexPushdownMetadataError, ResolvedIndexDescriptor, TiKvIndexScanSpec, TiKvTableScanSpec,
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

/// Source-resolved table-scan kind returned by Go's
/// `PhysicalTableScan.IsFullScan` decision.
///
/// Cluster-table and index-lookup child identities are outside the bounded
/// ordinary TiKV table path, so only the full/range branches are admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedTableScanKind {
    /// Every source range is a full range for the table handle type.
    Full,
    /// At least one source range restricts the table handle domain.
    Range,
}

impl ResolvedTableScanKind {
    /// Returns the exact Go plan-codec identity for this bounded scan kind.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        match self {
            Self::Full => "TableFullScan",
            Self::Range => "TableRangeScan",
        }
    }
}

/// Source statement-context policy for `PhysicalTableScan.ExplainID`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableScanExplainIdSuffix {
    /// Normal Go explain output appends the physical plan ID.
    IncludePlanId,
    /// `StmtCtx.IgnoreExplainIDSuffix` suppresses the physical plan ID.
    Omit,
}

/// Authoritative source descriptor for one ordinary TiKV table scan.
///
/// Go derives table identity, common-handle state, range kind, and explain-ID
/// policy from the same table/range/session objects used to build the physical
/// scan. Keeping them together prevents caller-proposed protobuf metadata from
/// silently naming another table or dropping reader metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedTableDescriptor {
    table_id: i64,
    is_common_handle: bool,
    scan_kind: ResolvedTableScanKind,
    explain_id_suffix: TableScanExplainIdSuffix,
}

impl ResolvedTableDescriptor {
    /// Creates one source-resolved bounded table descriptor.
    #[must_use]
    pub const fn new(
        table_id: i64,
        is_common_handle: bool,
        scan_kind: ResolvedTableScanKind,
        explain_id_suffix: TableScanExplainIdSuffix,
    ) -> Self {
        Self {
            table_id,
            is_common_handle,
            scan_kind,
            explain_id_suffix,
        }
    }

    /// Returns the authoritative table identity.
    #[must_use]
    pub const fn table_id(self) -> i64 {
        self.table_id
    }

    /// Returns whether the source table uses a common handle.
    #[must_use]
    pub const fn is_common_handle(self) -> bool {
        self.is_common_handle
    }

    /// Returns the source full/range classification.
    #[must_use]
    pub const fn scan_kind(self) -> ResolvedTableScanKind {
        self.scan_kind
    }

    /// Returns the source explain-ID suffix policy.
    #[must_use]
    pub const fn explain_id_suffix(self) -> TableScanExplainIdSuffix {
        self.explain_id_suffix
    }
}

/// A rejected attempt to emit a table scan for a different source table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TablePushdownIdentityError {
    descriptor: i64,
    pushdown: i64,
}

impl TablePushdownIdentityError {
    /// Returns the source-authoritative table ID.
    #[must_use]
    pub const fn descriptor(self) -> i64 {
        self.descriptor
    }

    /// Returns the mismatched protobuf table ID.
    #[must_use]
    pub const fn pushdown(self) -> i64 {
        self.pushdown
    }
}

impl std::fmt::Display for TablePushdownIdentityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("table scan descriptor and pushdown table ID differ")
    }
}

impl std::error::Error for TablePushdownIdentityError {}

/// Table pushdown data validated against its source descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedTablePushdown {
    descriptor: ResolvedTableDescriptor,
    spec: TiKvTableScanSpec,
}

impl ValidatedTablePushdown {
    fn new(
        descriptor: ResolvedTableDescriptor,
        spec: TiKvTableScanSpec,
    ) -> Result<Self, TablePushdownIdentityError> {
        if descriptor.table_id() != spec.table_id {
            return Err(TablePushdownIdentityError {
                descriptor: descriptor.table_id(),
                pushdown: spec.table_id,
            });
        }
        Ok(Self { descriptor, spec })
    }

    /// Returns the sole source-authoritative table descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> ResolvedTableDescriptor {
        self.descriptor
    }

    /// Returns the validated protobuf payload.
    #[must_use]
    pub const fn spec(&self) -> &TiKvTableScanSpec {
        &self.spec
    }
}

/// An ordinary table path whose schema-dependent TiKV payload is already
/// resolved by the source adapter.
///
/// Go's `AccessPath` carries ranger output, storage choice, filters, and the
/// earlier PointGet decision into `convertToTableScan`. The bounded Rust
/// transition keeps those admissions explicit so a table path cannot skip a
/// planner branch that does not yet have an owner.
#[derive(Clone, Debug, PartialEq)]
pub struct TableAccessPath {
    pushdown: ValidatedTablePushdown,
    point_get_admission: PointGetAdmission,
    count_after_access: f64,
    store: AccessPathStore,
    empty_ranges: bool,
    partitioned: bool,
    sampled: bool,
    has_filters: bool,
}

impl TableAccessPath {
    /// Creates a non-empty TiKV table path with an explicit upstream
    /// PointGet admission.
    pub fn from_source_table_scan(
        descriptor: ResolvedTableDescriptor,
        pushdown: TiKvTableScanSpec,
        point_get_admission: PointGetAdmission,
        count_after_access: f64,
    ) -> Result<Self, TablePushdownIdentityError> {
        Ok(Self {
            pushdown: ValidatedTablePushdown::new(descriptor, pushdown)?,
            point_get_admission,
            count_after_access,
            store: AccessPathStore::TiKv,
            empty_ranges: false,
            partitioned: false,
            sampled: false,
            has_filters: false,
        })
    }

    /// Marks ranger's exact empty-range result.
    #[must_use]
    pub const fn with_empty_ranges(mut self) -> Self {
        self.empty_ranges = true;
        self
    }

    /// Changes the source storage engine.
    #[must_use]
    pub const fn with_store(mut self, store: AccessPathStore) -> Self {
        self.store = store;
        self
    }

    /// Records a physical partition path, which needs partition pruning and
    /// physical-table identity owners.
    #[must_use]
    pub const fn with_partitioned(mut self, partitioned: bool) -> Self {
        self.partitioned = partitioned;
        self
    }

    /// Records a `TABLESAMPLE` path, which uses a different physical plan.
    #[must_use]
    pub const fn with_table_sample(mut self, sampled: bool) -> Self {
        self.sampled = sampled;
        self
    }

    /// Records filters that require a Selection owner above the scan.
    #[must_use]
    pub const fn with_filters(mut self, has_filters: bool) -> Self {
        self.has_filters = has_filters;
        self
    }

    /// Returns the pre-resolved table-scan protobuf payload.
    #[must_use]
    pub const fn pushdown(&self) -> &TiKvTableScanSpec {
        self.pushdown.spec()
    }

    /// Returns source-authoritative table identity and scan metadata.
    #[must_use]
    pub const fn descriptor(&self) -> ResolvedTableDescriptor {
        self.pushdown.descriptor()
    }

    /// Returns the validated descriptor/payload pair for physical lowering.
    #[must_use]
    pub(crate) const fn validated_pushdown(&self) -> &ValidatedTablePushdown {
        &self.pushdown
    }

    /// Returns the upstream PointGet decision.
    #[must_use]
    pub const fn point_get_admission(&self) -> PointGetAdmission {
        self.point_get_admission
    }

    /// Returns source `CountAfterAccess` for the physical scan statistics.
    #[must_use]
    pub const fn count_after_access(&self) -> f64 {
        self.count_after_access
    }

    /// Returns the selected storage engine.
    #[must_use]
    pub const fn store(&self) -> AccessPathStore {
        self.store
    }

    /// Reports whether ranger produced no ranges.
    #[must_use]
    pub const fn has_empty_ranges(&self) -> bool {
        self.empty_ranges
    }

    /// Reports whether this path addresses a physical partition.
    #[must_use]
    pub const fn is_partitioned(&self) -> bool {
        self.partitioned
    }

    /// Reports whether this path came from `TABLESAMPLE`.
    #[must_use]
    pub const fn is_table_sample(&self) -> bool {
        self.sampled
    }

    /// Reports whether a Selection would be required around the scan.
    #[must_use]
    pub const fn has_filters(&self) -> bool {
        self.has_filters
    }
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
    /// An ordinary table scan path.
    Table(TableAccessPath),
    /// An index-merge path.
    IndexMerge,
}
