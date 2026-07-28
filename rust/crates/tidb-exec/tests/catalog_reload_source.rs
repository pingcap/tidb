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

//! Source-backed tests for the incremental catalog reload.
//!
//! The stored bytes are the Go shapes a live cluster writes: `DBInfo` and
//! `TableInfo` JSON under `tidb-meta`'s key codec, and `SchemaDiff` JSON under
//! `Diff:<version>`. Go source of truth: `pkg/meta/meta.go`
//! `GetSchemaDiff`/`GetSchemaVersionWithNonEmptyDiff`,
//! `pkg/infoschema/issyncer/loader.go` `tryLoadSchemaDiffs`, and
//! `pkg/infoschema/builder.go` `ApplyDiff`.

use std::collections::BTreeMap;

use tidb_exec::catalog_reload::{
    reload_cluster_catalog, FullReloadReason, ReloadedCatalog,
    LOAD_SCHEMA_DIFF_VERSION_GAP_THRESHOLD,
};
use tidb_exec::cluster_catalog::{
    load_cluster_catalog, prefix_scan_end, ClusterCatalog, ClusterCatalogError, MetaPairs,
    MetaSnapshot,
};
use tidb_meta::{key, value};
use tidb_model::action_type::ActionType;

#[derive(Default)]
struct RecordedSnapshot {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl RecordedSnapshot {
    fn put(&mut self, raw_key: Vec<u8>, raw_value: impl Into<Vec<u8>>) {
        self.pairs.insert(raw_key, raw_value.into());
    }

    fn remove(&mut self, raw_key: &[u8]) {
        self.pairs.remove(raw_key);
    }

    /// Writes what one committed DDL leaves behind: the bumped version counter
    /// and the diff describing the change.
    fn commit_diff(&mut self, version: i64, diff_json: &str) {
        self.put(key::schema_version_kv_key(), value::encode_int_value(version));
        self.put(key::schema_diff_kv_key(version), diff_json);
    }
}

impl MetaSnapshot for RecordedSnapshot {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let end = prefix_scan_end(prefix).expect("finite scan end");
        Ok(self
            .pairs
            .range(prefix.to_vec()..end)
            .map(|(stored_key, stored_value)| (stored_key.clone(), stored_value.clone()))
            .collect())
    }
}

const GO_DBINFO: &str = r#"{"id":3,"db_name":{"O":"Campaign","L":"campaign"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#;

const GO_SECOND_DBINFO: &str = r#"{"id":4,"db_name":{"O":"Ledger","L":"ledger"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#;

fn go_table(id: i64, original: &str, lower: &str) -> String {
    format!(
        r#"{{"id":{id},"name":{{"O":"{original}","L":"{lower}"}},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{{"id":1,"name":{{"O":"id","L":"id"}},"offset":0,"type":{{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false}},"state":5,"version":2}},
{{"id":2,"name":{{"O":"balance","L":"balance"}},"offset":1,"type":{{"Tp":8,"Flag":1,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false}},"state":5,"version":2}}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}}"#
    )
}

fn diff_json(version: i64, action: ActionType, schema_id: i64, table_id: i64) -> String {
    format!(
        r#"{{"version":{version},"type":{},"schema_id":{schema_id},"table_id":{table_id},"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":null}}"#,
        action.0
    )
}

/// A cluster holding one database with one table, at schema version 100 whose
/// diff is stored, so the version is fully observable.
fn started_cluster() -> (RecordedSnapshot, ClusterCatalog) {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    snapshot.put(key::table_kv_key(3, 77), go_table(77, "Rows", "rows"));
    snapshot.commit_diff(
        100,
        &diff_json(100, ActionType::ACTION_CREATE_TABLE, 3, 77),
    );
    let catalog = load_cluster_catalog(&mut snapshot).expect("startup load");
    assert_eq!(catalog.schema_version, 100);
    (snapshot, catalog)
}

#[test]
fn an_unchanged_schema_version_reloads_nothing() {
    let (mut snapshot, catalog) = started_cluster();
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    assert!(matches!(reloaded, ReloadedCatalog::Unchanged { version: 100 }));
    assert!(reloaded.catalog().is_none());
}

#[test]
fn a_create_table_diff_adds_exactly_that_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_CREATE_TABLE, 3, 78));

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs { catalog: next, applied } = reloaded else {
        panic!("expected a diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 1);
    assert_eq!(next.schema_version, 101);
    assert_eq!(next.databases[0].tables.len(), 2);
    assert!(next.find_table("campaign", "notes").is_some());
    // The catalog the node was already serving is untouched.
    assert_eq!(catalog.databases[0].tables.len(), 1);
}

#[test]
fn a_drop_table_diff_removes_exactly_that_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_DROP_TABLE, 3, 77));

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let Some(next) = reloaded.catalog() else {
        panic!("expected a published catalog, got {reloaded:?}");
    };
    assert_eq!(next.schema_version, 101);
    assert!(next.databases[0].tables.is_empty());
    assert!(next.find_table("campaign", "rows").is_none());
}

#[test]
fn a_truncate_table_diff_swaps_the_old_table_id_for_the_new_one() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(3, 90), go_table(90, "Rows", "rows"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":90,"old_table_id":77,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":null}}"#,
            ActionType::ACTION_TRUNCATE_TABLE.0
        ),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let next = reloaded.catalog().expect("published catalog");
    assert_eq!(next.databases[0].tables.len(), 1);
    let (_, table) = next.find_table("campaign", "rows").expect("table survives");
    assert_eq!(table.id, 90);
}

#[test]
fn create_and_drop_schema_diffs_add_and_remove_a_database() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::database_kv_key(4), GO_SECOND_DBINFO);
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_CREATE_SCHEMA, 4, 0));

    let created = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let created = created.catalog().expect("published catalog").clone();
    assert_eq!(created.databases.len(), 2);
    assert!(created
        .databases
        .iter()
        .any(|db| db.info.name.lowercase() == "ledger"));

    snapshot.remove(&key::database_kv_key(4));
    snapshot.commit_diff(102, &diff_json(102, ActionType::ACTION_DROP_SCHEMA, 4, 0));
    let dropped = reload_cluster_catalog(&mut snapshot, &created).expect("reload runs");
    let dropped = dropped.catalog().expect("published catalog");
    assert_eq!(dropped.schema_version, 102);
    assert_eq!(dropped.databases.len(), 1);
}

#[test]
fn a_create_tables_diff_adds_every_affected_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.put(key::table_kv_key(3, 79), go_table(79, "Tags", "tags"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":0,"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":[{{"schema_id":3,"table_id":78,"old_table_id":0,"old_schema_id":0}},{{"schema_id":3,"table_id":79,"old_table_id":0,"old_schema_id":0}}]}}"#,
            ActionType::ACTION_CREATE_TABLES.0
        ),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let next = reloaded.catalog().expect("published catalog");
    assert_eq!(next.databases[0].tables.len(), 3);
    assert!(next.find_table("campaign", "notes").is_some());
    assert!(next.find_table("campaign", "tags").is_some());
}

#[test]
fn an_unsupported_diff_type_forces_a_full_reload_rather_than_a_partial_guess() {
    let (mut snapshot, catalog) = started_cluster();
    // A second table exists in the store but no diff this tier can apply says
    // so; only the full load can find it.
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_ADD_COLUMN, 3, 77));

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Full { catalog: next, reason } = reloaded else {
        panic!("expected a full reload");
    };
    assert_eq!(
        reason,
        FullReloadReason::UnsupportedAction {
            version: 101,
            action: ActionType::ACTION_ADD_COLUMN,
        }
    );
    assert_eq!(next.schema_version, 101);
    assert_eq!(next.databases[0].tables.len(), 2);
}

#[test]
fn a_regenerate_schema_map_diff_forces_a_full_reload() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":77,"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":true,"affected_options":null}}"#,
            ActionType::ACTION_CREATE_TABLE.0
        ),
    );
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    assert!(matches!(
        reloaded,
        ReloadedCatalog::Full {
            reason: FullReloadReason::RegenerateSchemaMap { version: 101 },
            ..
        }
    ));
}

#[test]
fn a_large_version_gap_takes_the_full_load_instead_of_replaying() {
    let (mut snapshot, catalog) = started_cluster();
    let far = 100 + LOAD_SCHEMA_DIFF_VERSION_GAP_THRESHOLD;
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(far, &diff_json(far, ActionType::ACTION_CREATE_TABLE, 3, 78));

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Full { catalog: next, reason } = reloaded else {
        panic!("expected a full reload");
    };
    assert_eq!(
        reason,
        FullReloadReason::TooManyDiffs {
            from: 100,
            to: far,
        }
    );
    assert_eq!(next.schema_version, far);
    assert_eq!(next.databases[0].tables.len(), 2);
}

#[test]
fn a_version_whose_diff_is_not_written_yet_is_not_adopted() {
    let (mut snapshot, catalog) = started_cluster();
    // Go `GetSchemaVersionWithNonEmptyDiff`: the counter moved but the DDL's
    // own transaction has not committed its diff, so version 101 is not yet
    // observable and the node stays at 100.
    snapshot.put(key::schema_version_kv_key(), value::encode_int_value(101));
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    assert!(matches!(reloaded, ReloadedCatalog::Unchanged { version: 100 }));
}

#[test]
fn an_empty_diff_in_the_middle_only_advances_the_version() {
    let (mut snapshot, catalog) = started_cluster();
    // Version 101's DDL was cancelled, leaving no diff; 102 creates a table.
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(102, &diff_json(102, ActionType::ACTION_CREATE_TABLE, 3, 78));

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs { catalog: next, applied } = reloaded else {
        panic!("expected a diff reload");
    };
    assert_eq!(applied, 1);
    assert_eq!(next.schema_version, 102);
    assert_eq!(next.databases[0].tables.len(), 2);
}

#[test]
fn a_backwards_version_takes_the_full_load() {
    let (mut snapshot, mut catalog) = started_cluster();
    catalog.schema_version = 150;
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Full { catalog: next, reason } = reloaded else {
        panic!("expected a full reload");
    };
    assert_eq!(
        reason,
        FullReloadReason::VersionWentBackwards { from: 150, to: 100 }
    );
    assert_eq!(next.schema_version, 100);
}

#[test]
fn a_diff_naming_an_unknown_database_takes_the_full_load() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_CREATE_TABLE, 9, 91));
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Full { reason, .. } = reloaded else {
        panic!("expected a full reload");
    };
    assert!(matches!(
        reason,
        FullReloadReason::MissingObject { version: 101, .. }
    ));
}
