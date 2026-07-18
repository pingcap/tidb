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

//! Vector columnar-index metadata constructor from
//! `pkg/planner/core/columnar_index_utils.go`.
//!
//! The Go helper creates a `ColumnarIndexExtra` with a vector index type and
//! an ANN query payload, copying the index ID, query parameters, reference
//! vector bytes, and source column metadata. This leaf preserves that exact
//! field construction through opaque enum tokens and dependency-closed index
//!/column adapters; protobuf, model metadata, TiFlash scans, and vector
//! planning remain external boundaries.

/// Opaque ANN query-type token supplied by the protocol owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AnnQueryType(u32);

impl AnnQueryType {
    /// Creates an ANN query-type token.
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Returns the raw protocol value.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

/// Opaque vector-distance metric token supplied by the protocol owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DistanceMetric(u32);

impl DistanceMetric {
    /// Creates a distance-metric token.
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Returns the raw protocol value.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

/// Minimal source index metadata retained by ColumnarIndexExtra.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct IndexIdentity {
    id: i64,
    name: String,
}

impl IndexIdentity {
    /// Creates index metadata from source ID and name.
    #[must_use]
    pub fn new(id: i64, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }

    /// Returns the source index ID.
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Returns the source index name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Minimal source column metadata copied into the ANN query payload.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ColumnIdentity {
    id: i64,
}

impl ColumnIdentity {
    /// Creates column metadata from its source column ID.
    #[must_use]
    pub const fn new(id: i64) -> Self {
        Self { id }
    }

    /// Returns the source column ID.
    #[must_use]
    pub const fn id(self) -> i64 {
        self.id
    }
}

/// ANN query metadata nested under a vector columnar-index payload.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AnnQueryInfo {
    query_type: AnnQueryType,
    distance_metric: DistanceMetric,
    top_k: u32,
    column_name: String,
    index_id: i64,
    ref_vec_f32: Vec<u8>,
    column: ColumnIdentity,
}

impl AnnQueryInfo {
    /// Returns the source ANN query type.
    #[must_use]
    pub const fn query_type(&self) -> AnnQueryType {
        self.query_type
    }

    /// Returns the source vector distance metric.
    #[must_use]
    pub const fn distance_metric(&self) -> DistanceMetric {
        self.distance_metric
    }

    /// Returns the source top-k value.
    #[must_use]
    pub const fn top_k(&self) -> u32 {
        self.top_k
    }

    /// Returns the source vector column name.
    #[must_use]
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Returns the copied source index ID.
    #[must_use]
    pub const fn index_id(&self) -> i64 {
        self.index_id
    }

    /// Returns the copied reference-vector bytes.
    #[must_use]
    pub fn ref_vec_f32(&self) -> &[u8] {
        &self.ref_vec_f32
    }

    /// Returns the copied source column metadata.
    #[must_use]
    pub const fn column(&self) -> ColumnIdentity {
        self.column
    }
}

/// Columnar index metadata plus its vector ANN query payload.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ColumnarIndexExtra {
    index: IndexIdentity,
    index_type_vector: bool,
    query: AnnQueryInfo,
}

impl ColumnarIndexExtra {
    /// Returns retained index metadata.
    #[must_use]
    pub fn index(&self) -> &IndexIdentity {
        &self.index
    }

    /// Reports the fixed vector columnar-index type selected by the source.
    #[must_use]
    pub const fn is_vector_index(&self) -> bool {
        self.index_type_vector
    }

    /// Returns nested ANN query metadata.
    #[must_use]
    pub const fn query(&self) -> &AnnQueryInfo {
        &self.query
    }
}

/// Builds source-shaped vector columnar-index metadata.
#[must_use]
pub fn build_vector_index_extra(
    index: IndexIdentity,
    query_type: AnnQueryType,
    distance_metric: DistanceMetric,
    top_k: u32,
    column_name: impl Into<String>,
    ref_vec_f32: Vec<u8>,
    column: ColumnIdentity,
) -> ColumnarIndexExtra {
    let index_id = index.id();
    ColumnarIndexExtra {
        index,
        index_type_vector: true,
        query: AnnQueryInfo {
            query_type,
            distance_metric,
            top_k,
            column_name: column_name.into(),
            index_id,
            ref_vec_f32,
            column,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_index_extra_copies_source_fields() {
        let extra = build_vector_index_extra(
            IndexIdentity::new(100, "vector_index"),
            AnnQueryType::new(1),
            DistanceMetric::new(2),
            10,
            "vec",
            vec![1, 2, 3, 4],
            ColumnIdentity::new(3),
        );
        assert_eq!(extra.index().id(), 100);
        assert_eq!(extra.index().name(), "vector_index");
        assert!(extra.is_vector_index());
        assert_eq!(extra.query().query_type().raw(), 1);
        assert_eq!(extra.query().distance_metric().raw(), 2);
        assert_eq!(extra.query().top_k(), 10);
        assert_eq!(extra.query().column_name(), "vec");
        assert_eq!(extra.query().index_id(), 100);
        assert_eq!(extra.query().ref_vec_f32(), &[1, 2, 3, 4]);
        assert_eq!(extra.query().column().id(), 3);
    }

    #[test]
    fn test_index_id_is_derived_from_retained_index_metadata() {
        let index = IndexIdentity::new(-42, "negative-id");
        let extra = build_vector_index_extra(
            index.clone(),
            AnnQueryType::new(9),
            DistanceMetric::new(8),
            0,
            "",
            Vec::new(),
            ColumnIdentity::new(7),
        );
        assert_eq!(extra.index(), &index);
        assert_eq!(extra.query().index_id(), -42);
        assert_eq!(extra.query().top_k(), 0);
        assert!(extra.query().ref_vec_f32().is_empty());
    }

    #[test]
    fn test_reference_vector_is_owned_without_normalization() {
        let bytes = vec![0, 255, 17];
        let extra = build_vector_index_extra(
            IndexIdentity::new(1, "idx"),
            AnnQueryType::new(3),
            DistanceMetric::new(4),
            1,
            "caseSensitiveName",
            bytes.clone(),
            ColumnIdentity::new(11),
        );
        assert_eq!(extra.query().ref_vec_f32(), bytes.as_slice());
        assert_eq!(extra.query().column_name(), "caseSensitiveName");
    }
}
