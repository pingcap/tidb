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

//! Special global-index classification from `pkg/statistics/handle/util/util.go`.
//!
//! The Go helper receives schema-owned `IndexInfo` and `TableInfo` values. This
//! leaf takes the already-resolved per-column facts instead: whether an index
//! is global, whether each referenced column is virtual-generated, and whether
//! its index length is a prefix. Schema lookup and model decoding remain
//! explicit external boundaries.

/// Caller-owned facts for one column referenced by an index.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IndexColumnInfo {
    /// Whether the table column is virtual-generated.
    pub virtual_generated: bool,
    /// Whether the index uses a prefix length for this column.
    pub prefix: bool,
}

impl IndexColumnInfo {
    /// Creates facts for an ordinary full-length column.
    #[must_use]
    pub const fn regular() -> Self {
        Self {
            virtual_generated: false,
            prefix: false,
        }
    }

    /// Creates facts for a virtual-generated column.
    #[must_use]
    pub const fn virtual_generated() -> Self {
        Self {
            virtual_generated: true,
            prefix: false,
        }
    }

    /// Creates facts for a full column with a prefix length.
    #[must_use]
    pub const fn prefix() -> Self {
        Self {
            virtual_generated: false,
            prefix: true,
        }
    }
}

/// Returns whether a global index has a virtual-generated or prefix column.
#[must_use]
pub fn is_special_global_index(global: bool, columns: &[IndexColumnInfo]) -> bool {
    if !global {
        return false;
    }
    columns
        .iter()
        .any(|column| column.virtual_generated || column.prefix)
}
