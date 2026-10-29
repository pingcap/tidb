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

//! Dependency-closed index-column projection from `pkg/planner/util/column.go`.
//!
//! The Go helper resolves model metadata to expression columns and marks
//! prefix indexes. This leaf accepts normalized names, lengths, and caller
//! column positions, preserving prefix stopping and full-column nil slots
//! without importing the expression or catalog owners.

/// Go's `types.UnspecifiedLength` sentinel.
pub const UNSPECIFIED_LENGTH: i64 = -1;

/// Normalized table column metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnRef {
    name: String,
    full_length: i64,
}

impl ColumnRef {
    /// Creates column metadata from a normalized name and full field length.
    #[must_use]
    pub fn new(name: impl Into<String>, full_length: i64) -> Self {
        Self {
            name: name.into(),
            full_length,
        }
    }
}

/// One index column metadata entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexColumnRef {
    name: String,
    length: i64,
}

impl IndexColumnRef {
    /// Creates index metadata from a normalized name and prefix length.
    #[must_use]
    pub fn new(name: impl Into<String>, length: i64) -> Self {
        Self {
            name: name.into(),
            length,
        }
    }
}

/// A resolved column position and whether the index entry is a prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedColumn {
    /// Position in the caller's column metadata/column slice.
    pub source_index: usize,
    /// Whether the index covers only a strict prefix of the full column.
    pub is_prefix: bool,
}

/// Combined prefix/full projection produced by the source helper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexColumnProjection {
    /// Resolved leading prefix columns, stopping after the first missing one.
    pub prefix: Vec<ResolvedColumn>,
    /// Prefix lengths aligned with `prefix`.
    pub prefix_lengths: Vec<i64>,
    /// All index columns, retaining `None` for missing metadata.
    pub full: Vec<Option<ResolvedColumn>>,
    /// Full lengths aligned with `full`.
    pub full_lengths: Vec<i64>,
}

/// Projects index metadata to caller-owned column positions.
#[must_use]
pub fn project_index_columns(
    column_infos: &[ColumnRef],
    columns: &[ColumnRef],
    index_columns: &[IndexColumnRef],
) -> IndexColumnProjection {
    let mut prefix = Vec::with_capacity(index_columns.len());
    let mut prefix_lengths = Vec::with_capacity(index_columns.len());
    let mut full = Vec::with_capacity(index_columns.len());
    let mut full_lengths = Vec::with_capacity(index_columns.len());
    let mut prefix_complete = false;

    for index_column in index_columns {
        let resolved = column_infos
            .iter()
            .position(|info| info.name == index_column.name)
            .filter(|index| *index < columns.len())
            .map(|source_index| ResolvedColumn {
                source_index,
                is_prefix: index_column.length > 0
                    && column_infos[source_index].full_length > index_column.length,
            });

        let Some(resolved) = resolved else {
            prefix_complete = true;
            full.push(None);
            full_lengths.push(UNSPECIFIED_LENGTH);
            continue;
        };

        let full_length = column_infos[resolved.source_index].full_length;
        let length =
            if index_column.length != UNSPECIFIED_LENGTH && index_column.length == full_length {
                UNSPECIFIED_LENGTH
            } else {
                index_column.length
            };
        if !prefix_complete {
            prefix.push(resolved);
            prefix_lengths.push(length);
        }
        full.push(Some(resolved));
        full_lengths.push(length);
    }

    IndexColumnProjection {
        prefix,
        prefix_lengths,
        full,
        full_lengths,
    }
}
