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

//! `pkg/meta/model/db.go`: `DBInfo`, now unblocked by `TableInfo`.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use tidb_ast::CiString;

use crate::placement::PolicyRefInfo;
use crate::schema_state::SchemaState;
use crate::table_info::TableInfo;

/// Go `DBInfo`: metadata describing a database (schema).
///
/// Go's `Clone` deep-copies `Deprecated.Tables` while `Copy` shares the
/// `*TableInfo` pointers; with Rust's owned `Vec<TableInfo>` there is no
/// pointer sharing, so both converge on the derived deep `Clone`.
#[derive(Clone, Debug, Default)]
pub struct DBInfo {
    /// The database ID.
    pub id: i64,
    /// The database name.
    pub name: CiString,
    /// The database charset.
    pub charset: String,
    /// The database collation.
    pub collate: String,
    /// Go's `Deprecated.Tables` (not set in infoschema v2).
    pub deprecated_tables: Vec<TableInfo>,
    /// The online-DDL state.
    pub state: SchemaState,
    /// The placement-policy reference.
    pub placement_policy_ref: Option<PolicyRefInfo>,
    /// A table-name -> table-ID index (not serialized).
    pub table_name2id: BTreeMap<String, i64>,
}

/// Go `LessDBInfo`: orders two `DBInfo`s by their lower-cased name.
#[must_use]
pub fn less_db_info(a: &DBInfo, b: &DBInfo) -> Ordering {
    a.name.lowercase().cmp(b.name.lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_and_order() {
        let mut a = DBInfo {
            id: 1,
            name: CiString::new("Alpha"),
            deprecated_tables: vec![TableInfo {
                name: CiString::new("t1"),
                ..Default::default()
            }],
            ..Default::default()
        };
        // The clone deep-copies the tables.
        let c = a.clone();
        a.deprecated_tables[0].name = CiString::new("changed");
        assert_eq!(c.deprecated_tables[0].name.original(), "t1");

        let b = DBInfo {
            name: CiString::new("beta"),
            ..Default::default()
        };
        // Case-insensitive name ordering: "alpha" < "beta".
        assert_eq!(less_db_info(&a, &b), Ordering::Less);
        assert_eq!(less_db_info(&b, &a), Ordering::Greater);
        assert_eq!(less_db_info(&a, &a), Ordering::Equal);
    }
}
