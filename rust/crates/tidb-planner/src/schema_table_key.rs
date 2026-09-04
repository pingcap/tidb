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

//! Case-insensitive schema/table identity keys from
//! `pkg/planner/core/schema_table_key.go`.
//!
//! Go's `ast.CIStr` carries both the original spelling and a lowercase `L`
//! field. The planner keys use only that normalized field, so this leaf takes
//! identifier text and materializes the same lowercase identity without
//! pulling in parser, catalog, or plan-builder state.

/// Normalized schema/table identity used for view and lock maps.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SchemaTableKey {
    schema: String,
    table: String,
}

impl SchemaTableKey {
    /// Creates a key from schema and table identifier text.
    #[must_use]
    pub fn new(schema: &str, table: &str) -> Self {
        Self {
            schema: tidb_mysql::to_lowercase(schema),
            table: tidb_mysql::to_lowercase(table),
        }
    }

    /// Returns the normalized schema identity.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the normalized table identity.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }
}

/// Normalized table-alias identity used for duplicate-alias checks.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TableAliasKey {
    schema: String,
    name: String,
    qualified: bool,
}

impl TableAliasKey {
    /// Creates an unqualified alias key.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            schema: String::new(),
            name: tidb_mysql::to_lowercase(name),
            qualified: false,
        }
    }

    /// Creates a schema-qualified alias key.
    #[must_use]
    pub fn qualified(schema: &str, name: &str) -> Self {
        Self {
            schema: tidb_mysql::to_lowercase(schema),
            name: tidb_mysql::to_lowercase(name),
            qualified: true,
        }
    }

    /// Returns the normalized schema identity, or an empty string when the
    /// alias is unqualified.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the normalized alias identity.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether the key includes an explicit schema qualifier.
    #[must_use]
    pub const fn is_qualified(&self) -> bool {
        self.qualified
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go keys read `ast.CIStr.L` (`strings.ToLower`, `ast/model.go:302`) —
    /// the SIMPLE Unicode case mapping. A trailing capital sigma lowercases
    /// to sigma (U+03C3), never to final sigma (U+03C2); Rust's full
    /// `str::to_lowercase` would produce the latter and split the identity
    /// of one and the same schema/table between writer and reader.
    #[test]
    fn keys_use_the_go_simple_case_mapping() {
        let key = SchemaTableKey::new("\u{39F}\u{394}\u{39F}\u{3A3}", "T");
        assert_eq!(key.schema(), "\u{3BF}\u{3B4}\u{3BF}\u{3C3}");
        assert_eq!(key.table(), "t");
        assert!(!key.schema().contains('\u{3C2}'));

        let alias = TableAliasKey::qualified("\u{39F}\u{394}\u{39F}\u{3A3}", "A");
        assert_eq!(alias.schema(), "\u{3BF}\u{3B4}\u{3BF}\u{3C3}");
        assert_eq!(alias.name(), "a");
    }
}
