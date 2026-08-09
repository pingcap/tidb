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
use serde::{Serialize, Serializer};
use tidb_ast::CiString;

use crate::go_runtime::{GoNullClonePolicy, GoShared, GoSharedPointerSlice};
use crate::placement::PolicyRefInfo;
use crate::schema_state::SchemaState;
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
    FatalSeed, NullNoopSeed, OptionSharedMergeSeed, ValueMergeSeed,
};
use crate::table_info::TableInfo;

/// Go `DBInfo`: metadata describing a database (schema).
///
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
    /// Go's `Deprecated.Tables` (not set in infoschema v2). Go tags the inner
    /// field `json:"-"`, so only the empty wrapper object is ever stored.
    pub deprecated_tables: GoSharedPointerSlice<TableInfo>,
    /// The online-DDL state.
    pub state: SchemaState,
    /// The placement-policy reference.
    pub placement_policy_ref: Option<GoShared<PolicyRefInfo>>,
    /// A table-name -> table-ID index (Go `json:"-"`).
    pub table_name2id: Option<GoShared<BTreeMap<String, i64>>>,
}

impl_go_json_merge_object!(DBInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "db_name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else if go_json_field_matches(&key, "charset") {
        map.next_value_seed(NullNoopSeed(&mut destination.charset))?;
    } else if go_json_field_matches(&key, "collate") {
        map.next_value_seed(NullNoopSeed(&mut destination.collate))?;
    } else if go_json_field_matches(&key, "Deprecated") {
        let mut deprecated = BTreeMap::<String, serde::de::IgnoredAny>::new();
        map.next_value_seed(NullNoopSeed(&mut deprecated))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "policy_ref_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.placement_policy_ref))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(DBInfo);

/// Go's anonymous `Deprecated struct { Tables []*TableInfo \`json:"-"\` }`
/// marshals as a constant empty object, which every stored `DBInfo` carries.
struct DeprecatedTables;

impl Serialize for DeprecatedTables {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_struct("Deprecated", 0)?.end()
    }
}

impl DBInfo {
    /// Go `DBInfo.Clone`: copy the struct, allocate a fresh outer table slice,
    /// and invoke `TableInfo.Clone` for every pointee. The placement-policy
    /// pointer and table-name map are not named by the source method and
    /// therefore retain their original allocation identity.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        Self {
            deprecated_tables: self
                .deprecated_tables
                .map_clone_with(GoNullClonePolicy::Panic, TableInfo::clone_like_go),
            ..self.clone()
        }
    }

    /// Go `DBInfo.Copy`: copy the struct and only copy the outer table-pointer
    /// slice. Table pointees, the placement-policy pointer, and the map remain
    /// shared. `make(..., len(nil))` turns a nil source slice into an allocated
    /// empty result, which [`GoSharedPointerSlice::copy_outer`] preserves.
    #[must_use]
    pub fn copy_like_go(&self) -> Self {
        Self {
            deprecated_tables: self.deprecated_tables.copy_outer(),
            ..self.clone()
        }
    }

    /// Pointer-shaped Go `(*DBInfo).Clone` boundary. The source dereferences a
    /// nil receiver at `newInfo := *db`, so nil is a panic rather than a nil
    /// result.
    #[must_use]
    pub fn clone_pointer(database: Option<&Self>) -> GoShared<Self> {
        GoShared::new(database.expect("nil *DBInfo").clone_like_go())
    }

    /// Pointer-shaped Go `(*DBInfo).Copy` boundary.
    #[must_use]
    pub fn copy_pointer(database: Option<&Self>) -> GoShared<Self> {
        GoShared::new(database.expect("nil *DBInfo").copy_like_go())
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
    fn clone_copy_and_order_preserve_source_ownership() {
        let table = GoShared::new(TableInfo {
            name: CiString::new("t1"),
            ..Default::default()
        });
        let policy = GoShared::new(PolicyRefInfo {
            id: 5,
            ..Default::default()
        });
        let names = GoShared::new(BTreeMap::from([("t1".to_owned(), 7)]));
        let a = DBInfo {
            id: 1,
            name: CiString::new("Alpha"),
            deprecated_tables: GoSharedPointerSlice::from_handles(vec![Some(table.clone())]),
            placement_policy_ref: Some(policy.clone()),
            table_name2id: Some(names.clone()),
            ..Default::default()
        };
        let structural = a.clone();
        assert!(structural
            .deprecated_tables
            .backing_ptr_eq(&a.deprecated_tables));
        assert!(structural.deprecated_tables.get(0).unwrap().ptr_eq(&table));
        assert!(structural
            .placement_policy_ref
            .as_ref()
            .unwrap()
            .ptr_eq(&policy));
        assert!(structural.table_name2id.as_ref().unwrap().ptr_eq(&names));

        let deep = a.clone_like_go();
        assert!(!deep.deprecated_tables.backing_ptr_eq(&a.deprecated_tables));
        assert!(!deep.deprecated_tables.get(0).unwrap().ptr_eq(&table));
        assert!(deep.placement_policy_ref.as_ref().unwrap().ptr_eq(&policy));
        assert!(deep.table_name2id.as_ref().unwrap().ptr_eq(&names));

        let shallow = a.copy_like_go();
        assert!(!shallow
            .deprecated_tables
            .backing_ptr_eq(&a.deprecated_tables));
        assert!(shallow.deprecated_tables.get(0).unwrap().ptr_eq(&table));
        shallow.deprecated_tables.get(0).unwrap().write().name = CiString::new("changed");
        assert_eq!(table.read().name.original(), "changed");

        let nil_tables = DBInfo::default();
        assert!(!nil_tables.deprecated_tables.is_allocated());
        assert!(nil_tables.clone_like_go().deprecated_tables.is_allocated());
        assert!(nil_tables.copy_like_go().deprecated_tables.is_allocated());

        let b = DBInfo {
            name: CiString::new("beta"),
            ..Default::default()
        };
        // Case-insensitive name ordering: "alpha" < "beta".
        assert_eq!(less_db_info(&a, &b), Ordering::Less);
        assert_eq!(less_db_info(&b, &a), Ordering::Greater);
        assert_eq!(less_db_info(&a, &a), Ordering::Equal);
    }

    #[test]
    fn db_json_uses_go_fold_null_duplicate_and_merge_rules() {
        use crate::serde_helpers::GoJsonMerge;

        let decoded: DBInfo = serde_json::from_str(
            r#"{"ID":1,"id":null,"CHARSET":"utf8mb4","charset":null,"deprecated":{"ignored":1}}"#,
        )
        .unwrap();
        assert_eq!(decoded.id, 1);
        assert_eq!(decoded.charset, "utf8mb4");

        let mut database = DBInfo {
            id: 9,
            placement_policy_ref: Some(GoShared::new(PolicyRefInfo {
                id: 11,
                name: CiString::new("before"),
            })),
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"id":"bad","policy_ref_info":{"name":{"O":"later","L":"later"}},"collate":"after-error"}"#,
        );
        assert!(database.go_json_merge(&mut decoder).is_err());
        assert_eq!(database.id, 9);
        assert_eq!(database.collate, "after-error");
        let policy = database.placement_policy_ref.unwrap();
        let policy = policy.read();
        assert_eq!(policy.id, 11);
        assert_eq!(policy.name.original(), "later");

        let mut named = DBInfo {
            name: CiString::new("Before"),
            collate: "kept".to_owned(),
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(r#"{"db_name":{"O":"Later"}}"#);
        named.go_json_merge(&mut decoder).unwrap();
        assert_eq!(named.name.original(), "Later");
        assert_eq!(named.name.lowercase(), "before");

        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"db_name":{"L":"later","O":1},"collate":"must-not-run"}"#,
        );
        assert!(named.go_json_merge(&mut decoder).is_err());
        assert_eq!(named.name.original(), "Later");
        assert_eq!(named.name.lowercase(), "later");
        assert_eq!(named.collate, "kept");
    }

    #[test]
    #[should_panic(expected = "nil *DBInfo")]
    fn clone_nil_receiver_matches_source_dereference() {
        let _ = DBInfo::clone_pointer(None);
    }
}
