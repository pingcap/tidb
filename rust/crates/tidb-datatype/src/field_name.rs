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

//! Source-shaped field-name authority from `pkg/types/field_name.go`.

/// The string returned by Go's `(*FieldName).String` for a hidden field.
pub const EMPTY_NAME: &str = "EMPTY_NAME";

/// A source-shaped case-insensitive identifier (`ast.CIStr`).
///
/// Go preserves both the spelling supplied by the caller (`O`) and the
/// normalized lookup spelling (`L`). Metadata adapters must not reconstruct
/// one spelling from the other because aliases truncate them independently.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdentifierMetadata {
    /// Original/display spelling (`CIStr.O`).
    pub original: String,
    /// Normalized lookup spelling (`CIStr.L`).
    pub lower: String,
}

impl IdentifierMetadata {
    /// Creates the ordinary `ast.NewCIStr` shape for ASCII SQL identifiers.
    ///
    /// Call [`Self::from_parts`] at an AST boundary that already has Go's
    /// one-rune-at-a-time Unicode `strings.ToLower` result. Rust applies full
    /// Unicode lowercasing here, whose expansion behavior is not identical for
    /// a small set of non-ASCII code points.
    pub fn new(original: impl Into<String>) -> Self {
        let original = original.into();
        let lower = original.to_lowercase();
        Self { original, lower }
    }

    /// Preserves an already-supplied `O`/`L` pair exactly.
    pub fn from_parts(original: impl Into<String>, lower: impl Into<String>) -> Self {
        Self {
            original: original.into(),
            lower: lower.into(),
        }
    }
}

/// The five name fields shared by TiDB's executor/protocol adapters.
///
/// Keeping this source-owned core in `tidb-datatype` removes the former
/// executor-local duplicate without making the datatype leaf depend on AST or
/// expression crates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldNameMetadata {
    /// Original table name (`OrigTblName`).
    pub original_table: IdentifierMetadata,
    /// Original column name (`OrigColName`).
    pub original_column: IdentifierMetadata,
    /// Database/schema name (`DBName`).
    pub database: IdentifierMetadata,
    /// Visible table name or alias (`TblName`).
    pub table: IdentifierMetadata,
    /// Visible column name or alias (`ColName`).
    pub column: IdentifierMetadata,
}

/// Complete resolution-relevant `types.FieldName` value.
///
/// Go stores these booleans beside the five names. The metadata core remains
/// separately reusable because executor result fields do not assign
/// resolution flags at that adapter boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldName {
    /// Source name spellings.
    pub names: FieldNameMetadata,
    /// Hidden fields stringify as [`EMPTY_NAME`].
    pub hidden: bool,
    /// A non-public column that must not be referenced explicitly.
    pub not_explicit_usable: bool,
    /// A duplicate field retained only for qualified resolution.
    pub redundant: bool,
}

impl FieldName {
    /// Constructs a visible, explicitly usable, non-redundant field.
    pub fn new(names: FieldNameMetadata) -> Self {
        Self {
            names,
            ..Self::default()
        }
    }

    /// Go's `(*FieldName).String`, using normalized (`L`) spellings.
    pub fn display_name(&self) -> String {
        if self.hidden {
            return EMPTY_NAME.to_owned();
        }
        let mut parts = Vec::with_capacity(3);
        if !self.names.database.lower.is_empty() {
            parts.push(self.names.database.lower.as_str());
        }
        if !self.names.table.lower.is_empty() {
            parts.push(self.names.table.lower.as_str());
        }
        parts.push(self.names.column.lower.as_str());
        parts.join(".")
    }

    /// Go's `NameSlice.FindAstColName` predicate for one field.
    pub fn matches_column(&self, column: &QualifiedColumnName) -> bool {
        (column.database.lower.is_empty() || column.database.lower == self.names.database.lower)
            && (column.table.lower.is_empty() || column.table.lower == self.names.table.lower)
            && column.column.lower == self.names.column.lower
    }
}

/// AST-column-name spellings accepted by the dependency-free datatype leaf.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QualifiedColumnName {
    /// Optional schema qualifier.
    pub database: IdentifierMetadata,
    /// Optional table qualifier.
    pub table: IdentifierMetadata,
    /// Required column name.
    pub column: IdentifierMetadata,
}

impl QualifiedColumnName {
    /// Builds the ordinary source shape from original spellings.
    pub fn new(
        database: impl Into<String>,
        table: impl Into<String>,
        column: impl Into<String>,
    ) -> Self {
        Self {
            database: IdentifierMetadata::new(database),
            table: IdentifierMetadata::new(table),
            column: IdentifierMetadata::new(column),
        }
    }

    /// Go's `ast.ColumnName.String` shape used by `FindFieldName` errors.
    pub fn display_name(&self) -> String {
        let mut parts = Vec::with_capacity(3);
        if !self.database.lower.is_empty() {
            parts.push(self.database.lower.as_str());
        }
        if !self.table.lower.is_empty() {
            parts.push(self.table.lower.as_str());
        }
        parts.push(self.column.lower.as_str());
        parts.join(".")
    }
}

/// Go's `NameSlice.FindAstColName` over the shared field-name authority.
pub fn contains_column(names: &[FieldName], column: &QualifiedColumnName) -> bool {
    names.iter().any(|name| name.matches_column(column))
}
