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

use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use tidb_ast::CiString;

use crate::placement::PolicyRefInfo;
use crate::schema_state::SchemaState;
use crate::table_info::TableInfo;

/// Go `DBInfo`: metadata describing a database (schema).
///
/// Go's `Clone` deep-copies `Deprecated.Tables` while `Copy` shares the
/// `*TableInfo` pointers; with Rust's owned `Vec<TableInfo>` there is no
/// pointer sharing, so both converge on the derived deep `Clone`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DBInfo {
    /// The database ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The database name.
    #[serde(rename = "db_name", default)]
    pub name: CiString,
    /// The database charset.
    #[serde(
        rename = "charset",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub charset: String,
    /// The database collation.
    #[serde(
        rename = "collate",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub collate: String,
    /// Go's `Deprecated.Tables` (not set in infoschema v2). Go tags the inner
    /// field `json:"-"`, so only the empty wrapper object is ever stored.
    #[serde(skip)]
    pub deprecated_tables: Vec<TableInfo>,
    /// The online-DDL state.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
    /// The placement-policy reference.
    #[serde(rename = "policy_ref_info", default)]
    pub placement_policy_ref: Option<PolicyRefInfo>,
    /// A table-name -> table-ID index (Go `json:"-"`).
    #[serde(skip)]
    pub table_name2id: BTreeMap<String, i64>,
}

/// Go's anonymous `Deprecated struct { Tables []*TableInfo \`json:"-"\` }`
/// marshals as a constant empty object, which every stored `DBInfo` carries.
struct DeprecatedTables;

impl Serialize for DeprecatedTables {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_struct("Deprecated", 0)?.end()
    }
}

impl Serialize for DBInfo {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Field order is Go's declaration order, which `encoding/json` preserves.
        let mut value = serializer.serialize_struct("DBInfo", 7)?;
        value.serialize_field("id", &self.id)?;
        value.serialize_field("db_name", &self.name)?;
        value.serialize_field("charset", &self.charset)?;
        value.serialize_field("collate", &self.collate)?;
        value.serialize_field("Deprecated", &DeprecatedTables)?;
        value.serialize_field("state", &self.state)?;
        value.serialize_field("policy_ref_info", &self.placement_policy_ref)?;
        value.end()
    }
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
