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

// aggregate-test: standalone

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
        self.put(
            key::schema_version_kv_key(),
            value::encode_int_value(version),
        );
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

/// The same table shape as [`go_table`] plus one more `BIGINT` column, as if
/// an `ADD COLUMN`-shaped DDL had just committed and this is the fresh
/// `TableInfo` a reload reads back.
fn go_table_with_extra_column(
    id: i64,
    original: &str,
    lower: &str,
    col_id: i64,
    col_name: &str,
) -> String {
    let extra = format!(
        r#",{{"id":{col_id},"name":{{"O":"{col_name}","L":"{col_name}"}},"offset":2,"type":{{"Tp":8,"Flag":1,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false}},"state":5,"version":2}}"#
    );
    go_table(id, original, lower).replacen(
        "],\"index_info\"",
        &format!("{extra}],\"index_info\""),
        1,
    )
}

/// A Go `TableInfo` JSON for a materialized view log of base table 77, per
/// Go master `94a9cbedab`'s `pkg/meta/model` shape.
fn go_mlog_table(id: i64, original: &str, lower: &str) -> String {
    go_table(id, original, lower).replace(
        r#","index_info":null"#,
        r#","materialized_view_log":{"base_table_id":77,"columns":[{"O":"id","L":"id"}],"definition_sql_mode":0,"purge_schedule_time_zone":{"name":"UTC","offset":0}},"index_info":null"#,
    )
}

/// A Go `TableInfo` JSON for a materialized view over base table 77.
fn go_mview_table(id: i64, original: &str, lower: &str) -> String {
    go_table(id, original, lower).replace(
        r#","index_info":null"#,
        r#","materialized_view":{"base_table_ids":[77],"sql_content":"select id from rows","definition_sql_mode":0,"definition_div_precision_increment":4,"definition_time_zone":{"name":"UTC","offset":0},"refresh_schedule_time_zone":{"name":"UTC","offset":0}},"index_info":null"#,
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
    snapshot.commit_diff(100, &diff_json(100, ActionType::ACTION_CREATE_TABLE, 3, 77));
    let catalog = load_cluster_catalog(&mut snapshot).expect("startup load");
    assert_eq!(catalog.schema_version, 100);
    (snapshot, catalog)
}

#[test]
fn an_unchanged_schema_version_reloads_nothing() {
    let (mut snapshot, catalog) = started_cluster();
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    assert!(matches!(
        reloaded,
        ReloadedCatalog::Unchanged { version: 100 }
    ));
    assert!(reloaded.catalog().is_none());
}

#[test]
fn a_create_materialized_view_log_diff_adds_exactly_that_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(
        key::table_kv_key(3, 78),
        go_mlog_table(78, "Mlog77", "mlog77"),
    );
    snapshot.commit_diff(
        101,
        &diff_json(101, ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG, 3, 78),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
        panic!("expected a diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 1);
    assert_eq!(next.schema_version, 101);
    assert_eq!(next.databases[0].tables.len(), 2);
    let (_, table) = next
        .find_table("campaign", "mlog77")
        .expect("mlog table loads");
    let log = table
        .materialized_view_log
        .as_ref()
        .expect("the log metadata survives the reload");
    assert_eq!(log.read().base_table_id, 77);
    // The catalog the node was already serving is untouched.
    assert_eq!(catalog.databases[0].tables.len(), 1);
}

#[test]
fn a_create_materialized_view_diff_adds_exactly_that_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(
        key::table_kv_key(3, 79),
        go_mview_table(79, "Mview", "mview"),
    );
    snapshot.commit_diff(
        101,
        &diff_json(101, ActionType::ACTION_CREATE_MATERIALIZED_VIEW, 3, 79),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
        panic!("expected a diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 1);
    let (_, table) = next
        .find_table("campaign", "mview")
        .expect("mview table loads");
    let view = table
        .materialized_view
        .as_ref()
        .expect("the view metadata survives the reload");
    assert_eq!(view.read().sql_content, "select id from rows");
    assert_eq!(
        view.read()
            .base_table_ids
            .iter()
            .copied()
            .collect::<Vec<i64>>(),
        vec![77]
    );
}

#[test]
fn a_create_table_diff_adds_exactly_that_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_CREATE_TABLE, 3, 78));

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
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

/// Go `getTableIDs`'s `default:` case for `RENAME TABLE`: the table keeps its
/// ID, and `create_table`'s dedup-by-ID `retain` alone replaces the old name
/// in place (`old_schema_id == schema_id`, so `dropTableForUpdate`'s rename
/// special case never triggers).
#[test]
fn a_rename_table_diff_within_the_same_database_renames_in_place() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(3, 77), go_table(77, "Entries", "entries"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":77,"old_table_id":0,"old_schema_id":3,"regenerate_schema_map":false,"affected_options":null}}"#,
            ActionType::ACTION_RENAME_TABLE.0
        ),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
        panic!("expected an incremental diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 1);
    assert_eq!(next.databases[0].tables.len(), 1);
    assert!(next.find_table("campaign", "rows").is_none());
    let (_, table) = next
        .find_table("campaign", "entries")
        .expect("renamed table");
    assert_eq!(table.id, 77);
}

/// The cross-database case: `dropTableForUpdate` removes the OLD database's
/// copy (`old_schema_id`), and `applyCreateTable` (via `create_table`) adds
/// the freshly read `TableInfo` -- which the moving DDL persists under the
/// NEW database's meta prefix -- into `schema_id`.
#[test]
fn a_rename_table_diff_across_databases_moves_the_table() {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    snapshot.put(key::database_kv_key(4), GO_SECOND_DBINFO);
    snapshot.put(key::table_kv_key(3, 77), go_table(77, "Rows", "rows"));
    snapshot.commit_diff(100, &diff_json(100, ActionType::ACTION_CREATE_TABLE, 3, 77));
    let catalog = load_cluster_catalog(&mut snapshot).expect("startup load");

    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(4, 77), go_table(77, "Rows", "rows"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":4,"table_id":77,"old_table_id":0,"old_schema_id":3,"regenerate_schema_map":false,"affected_options":null}}"#,
            ActionType::ACTION_RENAME_TABLE.0
        ),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
        panic!("expected an incremental diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 1);
    let campaign = next
        .databases
        .iter()
        .find(|db| db.info.name.lowercase() == "campaign")
        .expect("campaign database");
    assert!(
        campaign.tables.is_empty(),
        "the old database keeps no stale copy"
    );
    let (db, table) = next.find_table("ledger", "rows").expect("moved table");
    assert_eq!(db.id, 4);
    assert_eq!(table.id, 77);
}

/// `RENAME TABLES`: the diff itself covers its first table exactly like a
/// single `RENAME TABLE` would, and every other renamed table is one
/// `AffectedOption` with its own `old_schema_id`
/// (`schema_version.go:96-115`) -- this one mixes a same-database rename
/// (the diff's own table) with a cross-database one (the affected table), so
/// both code paths run in one diff.
#[test]
fn a_rename_tables_diff_moves_every_affected_table() {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    snapshot.put(key::database_kv_key(4), GO_SECOND_DBINFO);
    snapshot.put(key::table_kv_key(3, 77), go_table(77, "Rows", "rows"));
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(100, &diff_json(100, ActionType::ACTION_CREATE_TABLE, 3, 77));
    let catalog = load_cluster_catalog(&mut snapshot).expect("startup load");
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));

    // Table 77 stays in database 3 under a new name; table 78 moves from
    // database 3 to database 4 keeping its name.
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(3, 77), go_table(77, "Entries", "entries"));
    snapshot.remove(&key::table_kv_key(3, 78));
    snapshot.put(key::table_kv_key(4, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":77,"old_table_id":0,"old_schema_id":3,"regenerate_schema_map":false,"affected_options":[{{"schema_id":4,"table_id":78,"old_table_id":0,"old_schema_id":3}}]}}"#,
            ActionType::ACTION_RENAME_TABLES.0
        ),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
        panic!("expected an incremental diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 1);
    let campaign = next
        .databases
        .iter()
        .find(|db| db.info.name.lowercase() == "campaign")
        .expect("campaign database");
    assert_eq!(
        campaign.tables.len(),
        1,
        "only the renamed-in-place table stays"
    );
    assert!(next.find_table("campaign", "rows").is_none());
    let (_, entries) = next
        .find_table("campaign", "entries")
        .expect("renamed table");
    assert_eq!(entries.id, 77);
    let (notes_db, notes) = next.find_table("ledger", "notes").expect("moved table");
    assert_eq!(notes_db.id, 4);
    assert_eq!(notes.id, 78);
}

#[test]
#[should_panic(expected = "nil affected option in schema diff")]
fn a_rename_tables_diff_panics_on_a_nil_affected_option() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":77,"old_table_id":0,"old_schema_id":3,"regenerate_schema_map":false,"affected_options":[null]}}"#,
            ActionType::ACTION_RENAME_TABLES.0
        ),
    );

    let _ = reload_cluster_catalog(&mut snapshot, &catalog);
}

/// A malformed rename diff (`old_schema_id: 0` while the current database is
/// `3`, never legitimately produced by
/// `SetSchemaDiffForRenameTable`/`SetSchemaDiffForRenameTables`) is not
/// silently treated as a same-database rename: Go's own
/// `dropTableForUpdate` would look database `0` up and fail, and this tier's
/// equivalent of that failure is falling back to a full load, the same as
/// any other diff naming an object that is not there.
#[test]
fn a_rename_table_diff_with_a_zero_old_schema_id_takes_the_full_load() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(3, 77), go_table(77, "Entries", "entries"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":77,"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":null}}"#,
            ActionType::ACTION_RENAME_TABLE.0
        ),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Full {
        catalog: next,
        reason,
    } = reloaded
    else {
        panic!("expected a full reload, got {reloaded:?}");
    };
    assert_eq!(
        reason,
        FullReloadReason::MissingObject {
            version: 101,
            detail: "unknown database 0".to_owned(),
        }
    );
    // The full load still reaches the correct end state from the snapshot.
    assert_eq!(next.schema_version, 101);
    let (_, table) = next
        .find_table("campaign", "entries")
        .expect("renamed table");
    assert_eq!(table.id, 77);
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
#[should_panic(expected = "nil affected option in create-tables schema diff")]
fn a_create_tables_diff_panics_on_a_nil_affected_option() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":0,"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":[null]}}"#,
            ActionType::ACTION_CREATE_TABLES.0
        ),
    );

    let _ = reload_cluster_catalog(&mut snapshot, &catalog);
}

/// The same table shape as [`go_table`] in another schema state, as a
/// multi-step DDL leaves it between its steps.
fn go_table_in_state(id: i64, original: &str, lower: &str, state: u8) -> String {
    go_table(id, original, lower).replacen(
        "\"state\":5,\"pk_is_handle\"",
        &format!("\"state\":{state},\"pk_is_handle\""),
        1,
    )
}

/// A Go `TableInfo` JSON as an older TiDB stored it: the given table-info
/// version and charset/collation, and the given column-info version and
/// charset/collation on both columns.
#[allow(clippy::too_many_arguments)]
fn go_old_table(
    id: i64,
    original: &str,
    lower: &str,
    table_version: u16,
    charset: &str,
    collate: &str,
    column_version: u64,
    column_charset: &str,
    column_collate: &str,
) -> String {
    go_table(id, original, lower)
        .replacen(
            r#""charset":"utf8mb4","collate":"utf8mb4_bin","cols""#,
            &format!(r#""charset":"{charset}","collate":"{collate}","cols""#),
            1,
        )
        .replace(
            r#""Charset":"binary","Collate":"binary""#,
            &format!(r#""Charset":"{column_charset}","Collate":"{column_collate}""#),
        )
        .replace(
            r#""state":5,"version":2}"#,
            &format!(r#""state":5,"version":{column_version}}}"#),
        )
        .replacen(
            r#""max_col_id":2,"version":5}"#,
            &format!(r#""max_col_id":2,"version":{table_version}}}"#),
            1,
        )
}

fn charsets(table: &tidb_model::table_info::TableInfo) -> (String, String, Vec<(String, String)>) {
    let columns = table
        .cols()
        .iter_handles()
        .map(|column| {
            let handle = column.expect("column");
            let column = handle.read();
            (
                column.get_charset().to_owned(),
                column.get_collate().to_owned(),
            )
        })
        .collect();
    (table.charset.clone(), table.collate.clone(), columns)
}

/// Go's builder normalizes every loaded `TableInfo`
/// (`ConvertCharsetCollateToLowerCaseIfNeed` below version 3,
/// `ConvertOldVersionUTF8ToUTF8MB4IfNeed` below version 2 with the default
/// `treat-old-version-utf8-as-utf8mb4`) -- in one order on the full load
/// (`loader.go:528-535`: `utf8` conversion, then lower-casing) and the other
/// on a diff (`builder.go:868-869`: lower-casing, then `utf8` conversion).
/// The order shows on a pre-version-2 table stored in upper case: `UTF8`
/// stays `utf8` on the full load and becomes `utf8mb4` on a diff. A
/// version-3 table is stored as is on both.
#[test]
fn old_table_infos_are_normalized_on_load_like_gos_builder() {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    snapshot.put(
        key::table_kv_key(3, 70),
        go_old_table(
            70, "V1l", "v1l", 1, "utf8", "utf8_bin", 1, "utf8", "utf8_bin",
        ),
    );
    snapshot.put(
        key::table_kv_key(3, 71),
        go_old_table(71, "V1", "v1", 1, "UTF8", "UTF8_BIN", 1, "UTF8", "UTF8_BIN"),
    );
    snapshot.put(
        key::table_kv_key(3, 72),
        go_old_table(72, "V2", "v2", 2, "UTF8", "UTF8_BIN", 2, "UTF8", "UTF8_BIN"),
    );
    snapshot.put(
        key::table_kv_key(3, 73),
        go_old_table(
            73,
            "V3",
            "v3",
            3,
            "UTF8MB4",
            "UTF8MB4_BIN",
            2,
            "UTF8",
            "UTF8_BIN",
        ),
    );
    snapshot.commit_diff(100, &diff_json(100, ActionType::ACTION_CREATE_TABLE, 3, 73));
    let catalog = load_cluster_catalog(&mut snapshot).expect("startup load");

    let expect = |charset: &str, collate: &str| {
        (
            charset.to_owned(),
            collate.to_owned(),
            vec![(charset.to_owned(), collate.to_owned()); 2],
        )
    };
    let (_, v1l) = catalog.find_table("campaign", "v1l").expect("v1l");
    assert_eq!(
        charsets(v1l),
        expect("utf8mb4", "utf8mb4_bin"),
        "version 1, lower case: utf8 becomes utf8mb4"
    );
    let (_, v1) = catalog.find_table("campaign", "v1").expect("v1");
    assert_eq!(
        charsets(v1),
        expect("utf8", "utf8_bin"),
        "version 1, upper case, full load: the utf8 conversion runs before the lower-casing and misses UTF8"
    );
    let (_, v2) = catalog.find_table("campaign", "v2").expect("v2");
    assert_eq!(
        charsets(v2),
        expect("utf8", "utf8_bin"),
        "version 2: lower-cased only"
    );
    let (_, v3) = catalog.find_table("campaign", "v3").expect("v3");
    assert_eq!(
        charsets(v3),
        (
            "UTF8MB4".to_owned(),
            "UTF8MB4_BIN".to_owned(),
            vec![("UTF8".to_owned(), "UTF8_BIN".to_owned()); 2]
        ),
        "version 3: stored as is"
    );

    // The diff path lower-cases first, so the same upper-case table converts.
    snapshot.put(
        key::table_kv_key(3, 74),
        go_old_table(
            74, "V1b", "v1b", 1, "UTF8", "UTF8_BIN", 1, "UTF8", "UTF8_BIN",
        ),
    );
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_CREATE_TABLE, 3, 74));
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "old table by diff",
    );
    let (_, v1b) = next.find_table("campaign", "v1b").expect("v1b");
    assert_eq!(
        charsets(v1b),
        expect("utf8mb4", "utf8mb4_bin"),
        "version 1, upper case, diff: lower-cased first, then utf8 becomes utf8mb4"
    );
}

/// Go `tryLoadSchemaDiffs` (`loader.go:378-401`) collects, per applied
/// diff, the physical table IDs `ApplyDiff` answered and the diff's action
/// for each -- the `RelatedSchemaChange` the schema validator records --
/// skipping only the TiFlash replica actions (`canSkipSchemaCheckerDDL`).
/// A partitioned table answers its partitions too (`appendAffectedIDs`); a
/// flashback answers `-1`, the every-table ID.
#[test]
fn diff_reloads_report_the_changed_physical_tables_and_actions() {
    let (mut snapshot, catalog) = started_cluster();
    // v101: CREATE TABLE 78 -> [78]
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_CREATE_TABLE, 3, 78));
    // v102: ADD COLUMN on 77 -> [77] (reloaded in place)
    snapshot.put(
        key::table_kv_key(3, 77),
        go_table_with_extra_column(77, "Rows", "rows", 3, "note"),
    );
    snapshot.commit_diff(102, &diff_json(102, ActionType::ACTION_ADD_COLUMN, 3, 77));
    // v103: SET TIFLASH REPLICA on 77 -> skipped entirely
    snapshot.commit_diff(
        103,
        &diff_json(103, ActionType::ACTION_SET_TI_FLASH_REPLICA, 3, 77),
    );
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog,
        applied,
        changes,
    } = reloaded
    else {
        panic!("expected an incremental diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 3);
    // An in-place ALTER answers its table twice: Go's `applyTableUpdate`
    // appends the dropped table's ID (`applyDropTable`) and then the
    // re-created one's (`applyCreateTable`), and both are 77.
    assert_eq!(changes.phy_tbl_ids, vec![78, 77, 77]);
    assert_eq!(
        changes.action_types,
        vec![
            u64::from(ActionType::ACTION_CREATE_TABLE.0),
            u64::from(ActionType::ACTION_ADD_COLUMN.0),
            u64::from(ActionType::ACTION_ADD_COLUMN.0),
        ]
    );

    // A second pass, so the drop below does not make v101's create fall to
    // the full load (its table would already be gone at the snapshot).
    // v104: DROP TABLE 78, key gone -> [78]
    snapshot.remove(&key::table_kv_key(3, 78));
    snapshot.commit_diff(104, &diff_json(104, ActionType::ACTION_DROP_TABLE, 3, 78));
    // v105: CREATE TABLE 79, partitioned -> [79, 791, 792]
    snapshot.put(
        key::table_kv_key(3, 79),
        go_table(79, "Parted", "parted").replace(
            r#","index_info":null"#,
            r#","partition":{"type":1,"expr":"`id`","columns":null,"enable":true,"definitions":[{"id":791,"name":{"O":"p0","L":"p0"},"less_than":["10"]},{"id":792,"name":{"O":"p1","L":"p1"},"less_than":["MAXVALUE"]}],"num":0},"index_info":null"#,
        ),
    );
    snapshot.commit_diff(105, &diff_json(105, ActionType::ACTION_CREATE_TABLE, 3, 79));
    // v106: FLASHBACK CLUSTER (not regenerating) -> [-1]
    snapshot.commit_diff(
        106,
        &diff_json(106, ActionType::ACTION_FLASHBACK_CLUSTER, 0, -1),
    );
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        applied, changes, ..
    } = reloaded
    else {
        panic!("expected an incremental diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 3);
    assert_eq!(changes.phy_tbl_ids, vec![78, 79, 791, 792, -1]);
    assert_eq!(
        changes.action_types,
        vec![
            u64::from(ActionType::ACTION_DROP_TABLE.0),
            u64::from(ActionType::ACTION_CREATE_TABLE.0),
            u64::from(ActionType::ACTION_CREATE_TABLE.0),
            u64::from(ActionType::ACTION_CREATE_TABLE.0),
            u64::from(ActionType::ACTION_FLASHBACK_CLUSTER.0),
        ]
    );
}

/// `TRUNCATE TABLE` answers the old and the new ID (`applyTableUpdate`:
/// the dropped table's, then the created one's); `DROP DATABASE` answers
/// every table it held; a schema-level or policy diff answers none.
#[test]
fn drop_schema_and_truncate_diffs_answer_gos_ids() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::table_kv_key(3, 90), go_table(90, "Rows", "rows"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":90,"old_table_id":77,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":null}}"#,
            ActionType::ACTION_TRUNCATE_TABLE.0
        ),
    );
    snapshot.commit_diff(
        102,
        &diff_json(102, ActionType::ACTION_CREATE_PLACEMENT_POLICY, 99, 0),
    );
    snapshot.commit_diff(103, &diff_json(103, ActionType::ACTION_DROP_SCHEMA, 3, 0));
    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs { changes, .. } = reloaded else {
        panic!("expected an incremental diff reload, got {reloaded:?}");
    };
    assert_eq!(changes.phy_tbl_ids, vec![77, 90, 90]);
    assert_eq!(
        changes.action_types,
        vec![
            u64::from(ActionType::ACTION_TRUNCATE_TABLE.0),
            u64::from(ActionType::ACTION_TRUNCATE_TABLE.0),
            u64::from(ActionType::ACTION_DROP_SCHEMA.0),
        ]
    );
}

fn diffs_reload(reloaded: ReloadedCatalog, what: &str) -> ClusterCatalog {
    let ReloadedCatalog::Diffs {
        catalog, applied, ..
    } = reloaded
    else {
        panic!("expected an incremental diff reload for {what}, got {reloaded:?}");
    };
    assert_eq!(applied, 1, "{what}");
    catalog
}

/// Go's `ApplyDiff` has no case for `MULTI_SCHEMA_CHANGE`: it is the
/// `default:` (`applyDefaultAction`), the table reloaded in place, and so is
/// it here now -- no action type forces a full load any more, only a missing
/// or conflicting object does.
#[test]
fn a_multi_schema_change_diff_reloads_the_table_in_place_like_gos_default() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(
        key::table_kv_key(3, 77),
        go_table_with_extra_column(77, "Rows", "rows", 3, "note"),
    );
    snapshot.commit_diff(
        101,
        &diff_json(101, ActionType::ACTION_MULTI_SCHEMA_CHANGE, 3, 77),
    );

    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "multi schema change",
    );
    let (_, table) = next.find_table("campaign", "rows").expect("table survives");
    assert_eq!(table.cols().len(), 3);
}

/// A schema-level action with no dedicated Go case (`MODIFY_SCHEMA_READ_ONLY`)
/// carries `TableID` 0: Go's default path finds no valid table ID to drop or
/// create and touches nothing.
#[test]
fn a_schema_level_diff_in_the_default_tier_touches_no_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.commit_diff(
        101,
        &diff_json(101, ActionType::ACTION_MODIFY_SCHEMA_READ_ONLY, 3, 0),
    );

    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "modify schema read only",
    );
    assert_eq!(next.schema_version, 101);
    assert_eq!(next.databases[0].tables.len(), 1);
}

/// Go `getTableIDs`'s drop case: a table whose stored copy is still in a
/// non-none state (a multi-step drop, kept for `ON DELETE CASCADE`) is
/// re-added after the drop; once its key is gone, so is the table.
#[test]
fn a_drop_table_diff_keeps_a_table_the_store_still_holds_in_a_non_none_state() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(
        key::table_kv_key(3, 77),
        go_table_in_state(77, "Rows", "rows", 2),
    );
    snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_DROP_TABLE, 3, 77));
    let kept = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "write-only drop step",
    );
    let (_, table) = kept.find_table("campaign", "rows").expect("table kept");
    assert_eq!(
        table.state,
        tidb_model::schema_state::SchemaState::WRITE_ONLY
    );

    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.commit_diff(102, &diff_json(102, ActionType::ACTION_DROP_TABLE, 3, 77));
    let gone = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &kept).expect("reload runs"),
        "final drop step",
    );
    assert!(gone.databases[0].tables.is_empty());
}

/// `DROP VIEW` and `DROP SEQUENCE` share `getTableIDs`'s drop case.
#[test]
fn drop_view_and_drop_sequence_diffs_drop_like_drop_table() {
    for action in [
        ActionType::ACTION_DROP_VIEW,
        ActionType::ACTION_DROP_SEQUENCE,
    ] {
        let (mut snapshot, catalog) = started_cluster();
        snapshot.remove(&key::table_kv_key(3, 77));
        snapshot.commit_diff(101, &diff_json(101, action, 3, 77));
        let next = diffs_reload(
            reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
            "drop view/sequence",
        );
        assert!(next.databases[0].tables.is_empty(), "{action}");
    }
}

/// `CREATE OR REPLACE VIEW` carries the replaced view's ID as `OldTableID`
/// (`schema_version.go:74-80`); `getTableIDs` drops it and adds the new one.
#[test]
fn a_create_or_replace_view_diff_drops_the_old_view_id() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(3, 90), go_table(90, "Rows", "rows"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":90,"old_table_id":77,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":null}}"#,
            ActionType::ACTION_CREATE_VIEW.0
        ),
    );
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "create or replace view",
    );
    assert_eq!(next.databases[0].tables.len(), 1);
    assert_eq!(next.databases[0].tables[0].id, 90);
}

/// `RECOVER TABLE` and `CREATE SEQUENCE` only add (`getTableIDs`'s first case).
#[test]
fn recover_table_and_create_sequence_diffs_add_the_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(
        101,
        &diff_json(101, ActionType::ACTION_RECOVER_TABLE, 3, 78),
    );
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "recover table",
    );
    assert_eq!(next.databases[0].tables.len(), 2);

    snapshot.put(key::table_kv_key(3, 79), go_table(79, "Seq", "seq"));
    snapshot.commit_diff(
        102,
        &diff_json(102, ActionType::ACTION_CREATE_SEQUENCE, 3, 79),
    );
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &next).expect("reload runs"),
        "create sequence",
    );
    assert_eq!(next.databases[0].tables.len(), 3);
}

/// Go `applyRecoverSchema`: the database comes back with the tables the
/// store lists under it (`ReadTableFromMeta`, which `RECOVER SCHEMA` always
/// sets), and recovering a database that is already loaded is Go's
/// `ErrDatabaseExists` -- here the conflicting-object full load.
#[test]
fn a_recover_schema_diff_reads_the_databases_tables_from_the_store() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::database_kv_key(4), GO_SECOND_DBINFO);
    snapshot.put(key::table_kv_key(4, 80), go_table(80, "A", "a"));
    snapshot.put(key::table_kv_key(4, 81), go_table(81, "B", "b"));
    let recover = |version: i64| {
        format!(
            r#"{{"version":{version},"type":{},"schema_id":4,"table_id":0,"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":false,"read_table_from_meta":true,"affected_options":null}}"#,
            ActionType::ACTION_RECOVER_SCHEMA.0
        )
    };
    snapshot.commit_diff(101, &recover(101));
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "recover schema",
    );
    assert_eq!(next.databases.len(), 2);
    assert!(next.find_table("ledger", "a").is_some());
    assert!(next.find_table("ledger", "b").is_some());

    snapshot.commit_diff(102, &recover(102));
    let reloaded = reload_cluster_catalog(&mut snapshot, &next).expect("reload runs");
    let ReloadedCatalog::Full { reason, .. } = reloaded else {
        panic!("expected a full reload, got {reloaded:?}");
    };
    assert_eq!(
        reason,
        FullReloadReason::ConflictingObject {
            version: 102,
            detail: "database 4 which is already loaded".to_owned(),
        }
    );
}

/// Go `applyModifySchemaCharsetAndCollate`: the fresh `DBInfo`'s charset and
/// collation are copied onto the loaded database; its tables stay.
#[test]
fn a_modify_schema_charset_diff_copies_the_fresh_fields_onto_the_loaded_database() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(
        key::database_kv_key(3),
        GO_DBINFO.replace(
            r#""charset":"utf8mb4","collate":"utf8mb4_bin""#,
            r#""charset":"latin1","collate":"latin1_bin""#,
        ),
    );
    snapshot.commit_diff(
        101,
        &diff_json(
            101,
            ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
            3,
            0,
        ),
    );
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "modify schema charset",
    );
    assert_eq!(next.databases[0].info.charset, "latin1");
    assert_eq!(next.databases[0].info.collate, "latin1_bin");
    assert_eq!(next.databases[0].tables.len(), 1);
}

/// Placement-policy, resource-group and masking-policy diffs touch maps this
/// catalog does not keep; they advance the version without a full load.
#[test]
fn policy_and_resource_group_diffs_change_nothing_and_stay_incremental() {
    for action in [
        ActionType::ACTION_CREATE_PLACEMENT_POLICY,
        ActionType::ACTION_DROP_RESOURCE_GROUP,
        ActionType::ACTION_ALTER_MASKING_POLICY,
    ] {
        let (mut snapshot, catalog) = started_cluster();
        snapshot.commit_diff(101, &diff_json(101, action, 99, 0));
        let next = diffs_reload(
            reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
            "policy diff",
        );
        assert_eq!(next.schema_version, 101, "{action}");
        assert_eq!(next.databases.len(), 1, "{action}");
        assert_eq!(next.databases[0].tables.len(), 1, "{action}");
    }
}

/// Go `applyExchangeTablePartition`'s public case: the normal table (its ID
/// carried as `OldTableID`) is replaced by the partition's ID, and the
/// partitioned table named by the first option is re-read whole.
#[test]
fn an_exchange_partition_diff_reloads_the_normal_and_the_partitioned_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(3, 90), go_table(90, "Rows", "rows"));
    snapshot.put(key::table_kv_key(3, 95), go_table(95, "Parted", "parted"));
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":90,"old_table_id":77,"old_schema_id":3,"regenerate_schema_map":false,"affected_options":[{{"schema_id":3,"table_id":95,"old_table_id":0,"old_schema_id":0}}]}}"#,
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION.0
        ),
    );
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "exchange partition",
    );
    let mut ids: Vec<i64> = next.databases[0].tables.iter().map(|t| t.id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![90, 95]);
}

/// Go `applyRefreshMeta`: the store decides. A table gone from the store is
/// dropped, one present is (re)loaded, a database present but not loaded is
/// added, and a table under an unloaded database is ignored.
#[test]
fn refresh_meta_diffs_follow_the_store() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.remove(&key::table_kv_key(3, 77));
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.put(key::database_kv_key(4), GO_SECOND_DBINFO);
    let refresh = |version: i64, schema_id: i64, table_id: i64| {
        diff_json(
            version,
            ActionType::ACTION_REFRESH_META,
            schema_id,
            table_id,
        )
    };
    snapshot.commit_diff(101, &refresh(101, 3, 77));
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "refresh meta drop",
    );
    assert!(next.databases[0].tables.is_empty());

    snapshot.commit_diff(102, &refresh(102, 3, 78));
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &next).expect("reload runs"),
        "refresh meta add table",
    );
    assert!(next.find_table("campaign", "notes").is_some());

    snapshot.commit_diff(103, &refresh(103, 5, 9));
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &next).expect("reload runs"),
        "refresh meta table under unloaded database",
    );
    assert_eq!(next.databases.len(), 1);

    snapshot.commit_diff(104, &refresh(104, 4, 0));
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &next).expect("reload runs"),
        "refresh meta add database",
    );
    assert_eq!(next.databases.len(), 2);
}

/// A flashback diff that does not demand a rebuilt map is a no-op in Go
/// (`ApplyDiff` returns `[]int64{-1}`).
#[test]
fn a_flashback_cluster_diff_without_regenerate_changes_nothing() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.commit_diff(
        101,
        &diff_json(101, ActionType::ACTION_FLASHBACK_CLUSTER, 0, -1),
    );
    let next = diffs_reload(
        reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs"),
        "flashback",
    );
    assert_eq!(next.databases[0].tables.len(), 1);
}

#[test]
fn common_alter_table_diffs_reload_only_the_changed_table_incrementally() {
    for action in [
        ActionType::ACTION_ADD_COLUMN,
        ActionType::ACTION_DROP_COLUMN,
        ActionType::ACTION_MODIFY_COLUMN,
        ActionType::ACTION_ADD_INDEX,
        ActionType::ACTION_DROP_INDEX,
        ActionType::ACTION_SET_DEFAULT_VALUE,
        ActionType::ACTION_MODIFY_TABLE_COMMENT,
        ActionType::ACTION_REBASE_AUTO_ID,
        ActionType::ACTION_ADD_FOREIGN_KEY,
        ActionType::ACTION_ALTER_TTLINFO,
    ] {
        let (mut snapshot, catalog) = started_cluster();
        // Seed a second table first (its own diff, its own reload), so the
        // action under test can be applied against a catalog that already
        // has something to leave untouched.
        snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
        snapshot.commit_diff(101, &diff_json(101, ActionType::ACTION_CREATE_TABLE, 3, 78));
        let catalog = reload_cluster_catalog(&mut snapshot, &catalog)
            .expect("seed reload runs")
            .catalog()
            .expect("seed catalog published")
            .clone();
        assert_eq!(catalog.databases[0].tables.len(), 2);

        // A second, untouched table exists purely to prove the reload did not
        // fall back to walking the whole catalog: an incremental reload never
        // reads it, so if the fallback fires, `applied`/table count below
        // would still look identical -- the real proof is `ReloadedCatalog`
        // being `Diffs`, not `Full`, which only the fixed tier produces for
        // these action types.
        snapshot.put(
            key::table_kv_key(3, 77),
            go_table_with_extra_column(77, "Rows", "rows", 3, "note"),
        );
        snapshot.commit_diff(102, &diff_json(102, action, 3, 77));

        let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
        let ReloadedCatalog::Diffs {
            catalog: next,
            applied,
            ..
        } = reloaded
        else {
            panic!("expected an incremental diff reload for {action}, got {reloaded:?}");
        };
        assert_eq!(applied, 1, "action {action}");
        assert_eq!(next.schema_version, 102, "action {action}");
        let (_, table) = next
            .find_table("campaign", "rows")
            .unwrap_or_else(|| panic!("table 'rows' survives {action}"));
        assert_eq!(
            table.cols().len(),
            3,
            "action {action} picks up the new column"
        );
        // Table 78 was never named by this diff and is untouched, and so is
        // the count -- the diff reloaded table 77 in place, it did not walk
        // the whole catalog; the catalog the node was already serving is
        // untouched too.
        assert_eq!(next.databases[0].tables.len(), 2, "action {action}");
        let (_, notes) = next
            .find_table("campaign", "notes")
            .expect("table 78 survives untouched");
        assert_eq!(
            notes.cols().len(),
            2,
            "action {action} does not touch table 78"
        );
        assert_eq!(catalog.databases[0].tables.len(), 2, "action {action}");
    }
}

#[test]
fn a_common_alter_table_diff_also_reloads_every_affected_table() {
    let (mut snapshot, catalog) = started_cluster();
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.put(
        key::table_kv_key(3, 77),
        go_table_with_extra_column(77, "Rows", "rows", 3, "note"),
    );
    snapshot.put(
        key::table_kv_key(3, 78),
        go_table_with_extra_column(78, "Notes", "notes", 3, "note"),
    );
    snapshot.commit_diff(
        101,
        &format!(
            r#"{{"version":101,"type":{},"schema_id":3,"table_id":77,"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":[{{"schema_id":3,"table_id":78,"old_table_id":0,"old_schema_id":0}}]}}"#,
            ActionType::ACTION_ADD_COLUMN.0
        ),
    );

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
        panic!("expected an incremental diff reload, got {reloaded:?}");
    };
    assert_eq!(applied, 1);
    let (_, rows) = next
        .find_table("campaign", "rows")
        .expect("primary table reloads");
    assert_eq!(rows.cols().len(), 3);
    let (_, notes) = next
        .find_table("campaign", "notes")
        .expect("affected table reloads too");
    assert_eq!(notes.cols().len(), 3);
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
    let ReloadedCatalog::Full {
        catalog: next,
        reason,
    } = reloaded
    else {
        panic!("expected a full reload");
    };
    assert_eq!(
        reason,
        FullReloadReason::TooManyDiffs { from: 100, to: far }
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
    assert!(matches!(
        reloaded,
        ReloadedCatalog::Unchanged { version: 100 }
    ));
}

#[test]
fn an_empty_diff_in_the_middle_only_advances_the_version() {
    let (mut snapshot, catalog) = started_cluster();
    // Version 101's DDL was cancelled, leaving no diff; 102 creates a table.
    snapshot.put(key::table_kv_key(3, 78), go_table(78, "Notes", "notes"));
    snapshot.commit_diff(102, &diff_json(102, ActionType::ACTION_CREATE_TABLE, 3, 78));

    let reloaded = reload_cluster_catalog(&mut snapshot, &catalog).expect("reload runs");
    let ReloadedCatalog::Diffs {
        catalog: next,
        applied,
        ..
    } = reloaded
    else {
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
    let ReloadedCatalog::Full {
        catalog: next,
        reason,
    } = reloaded
    else {
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
