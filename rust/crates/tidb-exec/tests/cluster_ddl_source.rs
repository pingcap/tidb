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

//! Source-backed tests for the catalog writer.
//!
//! The `GO_*` fixtures below are not hand-written: they are the exact
//! `TableInfo` JSON a real TiDB v8.5.6 stored for the same `CREATE TABLE`
//! text, read back from that server's own `/schema/<db>/<table>` status
//! endpoint (which re-marshals the stored struct). The decisive property is
//! that what this node writes carries the same values, because a Go server
//! must be able to load and serve it.

use std::collections::BTreeMap;

use tidb_exec::cluster_catalog::{prefix_scan_end, ClusterCatalogError, MetaPairs, MetaSnapshot};
use tidb_exec::cluster_ddl::{
    lower_ddl, lower_ddl_with_context, plan_ddl, plan_ddl_with_collation, DdlPlan, DdlPlanError,
    DdlStatement,
};
use tidb_meta::{key, value};
use tidb_model::GoAnyView;
use tidb_txnkv::transaction::OptimisticMutationKind;

/// A mutable snapshot of stored meta bytes: reads observe it, and a test may
/// apply a planned write set to it to model the transaction having committed.
#[derive(Default)]
pub(crate) struct MetaStore {
    pub(crate) pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MetaStore {
    fn put(&mut self, raw_key: Vec<u8>, raw_value: impl Into<Vec<u8>>) {
        self.pairs.insert(raw_key, raw_value.into());
    }
}

impl MetaSnapshot for MetaStore {
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

/// One database, `u6` with id 112, at schema version 60 and max used id 116 —
/// the shape the ground-truth cluster was actually in.
pub(crate) fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    store.put(key::next_global_id_kv_key(), b"116".to_vec());
    store.put(key::schema_version_kv_key(), b"60".to_vec());
    store.put(
        key::database_kv_key(112),
        br#"{"id":112,"db_name":{"O":"u6","L":"u6"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#.to_vec(),
    );
    store
}

pub(crate) fn statement(sql: &str) -> DdlStatement {
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    lower_ddl(&parsed, "u6")
        .unwrap_or_else(|error| panic!("the fixture SQL is admitted: {sql}: {error:?}"))
        .expect("the fixture SQL is a catalog change")
}

fn stored_default_bytes(sql: &str) -> Vec<u8> {
    let DdlStatement::CreateTable { build, .. } = statement(sql) else {
        panic!("the fixture is not CREATE TABLE: {sql}");
    };
    let column = build
        .template()
        .columns
        .get(0)
        .expect("the fixture declares one non-null column");
    let column = column.read();
    match column.default_value.view() {
        Some(GoAnyView::String(bytes)) => bytes.as_bytes().to_vec(),
        other => panic!("the fixture stored a non-string default: {other:?}"),
    }
}

fn refusal_with_code(sql: &str) -> (u16, String) {
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    let error =
        lower_ddl(&parsed, "u6").expect_err("this shape must be refused before any mutation");
    (error.code, error.reason)
}

fn refusal(sql: &str) -> String {
    refusal_with_code(sql).1
}

pub(crate) fn plan(store: &mut MetaStore, sql: &str, start_ts: u64) -> tidb_exec::cluster_ddl::DdlWrite {
    match plan_ddl(store, &statement(sql), start_ts).expect("the fixture plans") {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => {
            panic!("expected a write, got already-satisfied: {detail}")
        }
    }
}

/// Applies a planned write set, modelling its transaction having committed.
pub(crate) fn apply(store: &mut MetaStore, write: &tidb_exec::cluster_ddl::DdlWrite) {
    for mutation in &write.mutations {
        match mutation.kind() {
            OptimisticMutationKind::MetaPut => {
                store
                    .pairs
                    .insert(mutation.key().to_vec(), mutation.value().to_vec());
            }
            OptimisticMutationKind::MetaDelete => {
                store.pairs.remove(mutation.key());
            }
            other => panic!("a catalog change publishes only meta mutations, got {other:?}"),
        }
    }
}

pub(crate) fn stored_value<'write>(
    write: &'write tidb_exec::cluster_ddl::DdlWrite,
    raw_key: &[u8],
) -> &'write [u8] {
    write
        .mutations
        .iter()
        .find(|mutation| mutation.key() == raw_key)
        .unwrap_or_else(|| panic!("the write set carries this key"))
        .value()
}

/// Asserts that every field the Go server stored is present with the same value
/// in what this node writes.
///
/// Equality of the whole object is deliberately NOT asserted: this node's
/// `TableInfo` follows master and carries fields v8.5.6 has never heard of, and
/// Go's `encoding/json` ignores unknown fields on unmarshal. What must not drift
/// is any field Go DOES read.
fn assert_carries(go: &str, ours: &[u8], ignored: &[&str]) {
    let go: serde_json::Value = serde_json::from_str(go).expect("the Go fixture is JSON");
    let ours: serde_json::Value = serde_json::from_slice(ours).expect("what we wrote is JSON");
    let (go, ours) = (
        go.as_object().expect("a Go object"),
        ours.as_object().expect("our object"),
    );
    for (field, expected) in go {
        if ignored.contains(&field.as_str()) {
            continue;
        }
        assert_eq!(
            ours.get(field),
            Some(expected),
            "stored field `{field}` differs from what TiDB v8.5.6 wrote"
        );
    }
}

/// `CREATE TABLE u6.minimal (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)`
/// as TiDB v8.5.6 stored it.
const GO_MINIMAL: &str = r#"{"id":116,"name":{"O":"minimal","L":"minimal"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"id","L":"id"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4099,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"v","L":"v"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4097,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":2,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":467996279696261139,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":5,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"revision":0}"#;

/// The same for every column type this node admits.
const GO_SHAPES: &str = r#"{"id":114,"name":{"O":"shapes","L":"shapes"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"id","L":"id"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4099,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"amount","L":"amount"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4097,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":3,"name":{"O":"big","L":"big"},"offset":2,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":4129,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":4,"name":{"O":"ratio","L":"ratio"},"offset":3,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":5,"Flag":4097,"Flen":22,"Decimal":-1,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":5,"name":{"O":"tag","L":"tag"},"offset":4,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":4097,"Flen":8,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":6,"name":{"O":"name","L":"name"},"offset":5,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":4097,"Flen":32,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":7,"name":{"O":"price","L":"price"},"offset":6,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":246,"Flag":4097,"Flen":10,"Decimal":2,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":7,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":467996279683416098,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":5,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"revision":0}"#;

/// The Go server's own database object for `u6`.
const GO_DATABASE: &str = r#"{"id":112,"db_name":{"O":"u6","L":"u6"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#;

#[test]
fn a_created_table_is_stored_exactly_as_the_go_server_stores_it() {
    let mut store = bootstrapped();
    // The Go fixture's own table id was 116 and this store's max used id is
    // 115 short of that, so the allocation reproduces the same id: the fixture
    // can then be compared field for field, id included.
    store.put(key::next_global_id_kv_key(), b"115".to_vec());
    let write = plan(
        &mut store,
        "CREATE TABLE u6.minimal (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)",
        467_996_279_696_261_139,
    );
    assert_eq!(write.created_id, Some(116));
    assert_carries(
        GO_MINIMAL,
        stored_value(&write, &key::table_kv_key(112, 116)),
        &[],
    );
    assert_eq!(
        stored_table(&write, 116)["index_info"],
        serde_json::Value::Null,
        "a clustered integer primary key builds no IndexInfo, and Go persists the builder's nil slice"
    );
}

#[test]
fn every_admitted_column_type_is_stored_with_the_go_servers_field_type() {
    let mut store = bootstrapped();
    store.put(key::next_global_id_kv_key(), b"113".to_vec());
    let write = plan(
        &mut store,
        "CREATE TABLE u6.shapes (
           id BIGINT PRIMARY KEY CLUSTERED,
           amount BIGINT NOT NULL,
           big BIGINT UNSIGNED NOT NULL,
           ratio DOUBLE NOT NULL,
           tag CHAR(8) NOT NULL,
           name VARCHAR(32) NOT NULL,
           price DECIMAL(10,2) NOT NULL
         )",
        467_996_279_683_416_098,
    );
    assert_eq!(write.created_id, Some(114));
    assert_carries(
        GO_SHAPES,
        stored_value(&write, &key::table_kv_key(112, 114)),
        &[],
    );
}

#[test]
fn table_build_uses_the_loaded_database_defaults() {
    let mut store = bootstrapped();
    store.put(
        key::database_kv_key(112),
        br#"{"id":112,"db_name":{"O":"u6","L":"u6"},"charset":"utf8","collate":"utf8_general_ci","Deprecated":{},"state":5,"policy_ref_info":null}"#.to_vec(),
    );
    store.put(key::next_global_id_kv_key(), b"116".to_vec());

    let write = plan(
        &mut store,
        "CREATE TABLE u6.inherited (name VARCHAR(32) NOT NULL)",
        467_996_279_700_000_000,
    );
    let table = stored_table(&write, 117);
    assert_eq!(table["charset"], "utf8");
    assert_eq!(table["collate"], "utf8_general_ci");
    assert_eq!(table["cols"][0]["type"]["Charset"], "utf8");
    assert_eq!(table["cols"][0]["type"]["Collate"], "utf8_general_ci");
}

#[test]
fn a_table_constraint_primary_key_is_the_same_clustered_handle_as_an_inline_one() {
    let template = |sql: &str| {
        let DdlStatement::CreateTable { build, .. } = statement(sql) else {
            panic!("a CREATE TABLE");
        };
        build.template().clone()
    };
    let inline = template("CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)");
    let constraint =
        template("CREATE TABLE u6.t (id BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id))");
    assert_eq!(
        value::serialize_table_info(&inline).unwrap(),
        value::serialize_table_info(&constraint).unwrap()
    );
    assert!(inline.pk_is_handle);
    // The clustered handle IS the row key, so it is recorded in the flag and
    // in the column's own PriKeyFlag, and gets no IndexInfo of its own.
    assert!(inline.indices.is_empty());
    assert!(inline
        .columns
        .get(0)
        .expect("the fixture declares id")
        .read()
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY));
    assert!(!inline
        .columns
        .get(1)
        .expect("the fixture declares v")
        .read()
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY));
}

#[test]
fn a_created_database_is_stored_exactly_as_the_go_server_stores_it() {
    let mut store = MetaStore::default();
    store.put(key::next_global_id_kv_key(), b"111".to_vec());
    store.put(key::schema_version_kv_key(), b"60".to_vec());
    let write = plan(&mut store, "CREATE DATABASE u6", 1);
    assert_eq!(write.created_id, Some(112));
    assert_carries(
        GO_DATABASE,
        stored_value(&write, &key::database_kv_key(112)),
        &[],
    );
}

#[test]
fn a_created_database_persists_its_resolved_charset_and_collation() {
    let mut store = MetaStore::default();
    store.put(key::next_global_id_kv_key(), b"111".to_vec());
    store.put(key::schema_version_kv_key(), b"60".to_vec());
    let write = plan(
        &mut store,
        "CREATE DATABASE u8 CHARACTER SET utf8 COLLATE utf8_general_ci",
        1,
    );
    let database = value::parse_db_info(stored_value(&write, &key::database_kv_key(112)))
        .expect("the stored database metadata decodes");
    assert_eq!(database.charset, "utf8");
    assert_eq!(database.collate, "utf8_general_ci");

    for (sql, expected_charset, expected_collate) in [
        ("CREATE DATABASE c CHARACTER SET utf8", "utf8", "utf8_bin"),
        (
            "CREATE DATABASE c COLLATE utf8_general_ci",
            "utf8",
            "utf8_general_ci",
        ),
    ] {
        let DdlStatement::CreateDatabase {
            charset, collate, ..
        } = statement(sql)
        else {
            panic!("the fixture is CREATE DATABASE")
        };
        assert_eq!(charset, expected_charset, "{sql}");
        assert_eq!(collate, expected_collate, "{sql}");
    }

    assert!(refusal(
        "CREATE DATABASE c CHARACTER SET utf8 COLLATE utf8mb4_bin"
    )
    .contains("is not valid for CHARACTER SET"));
}

#[test]
fn every_catalog_change_writes_the_schema_version_and_its_diff() {
    // The version key in the write set is the whole concurrency story: it is
    // written from the value this snapshot read, so a competing DDL becomes a
    // TiKV write conflict instead of an interleaved half-change.
    for sql in [
        "CREATE DATABASE fresh",
        "DROP DATABASE u6",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    ] {
        let mut store = bootstrapped();
        let write = plan(&mut store, sql, 7);
        assert_eq!(write.schema_version, 61, "{sql}");
        assert_eq!(write.diff.version, 61, "{sql}");
        assert_eq!(
            stored_value(&write, &key::schema_version_kv_key()),
            b"61",
            "{sql}"
        );
        let stored_diff =
            value::parse_schema_diff(stored_value(&write, &key::schema_diff_kv_key(61)))
                .expect("the stored diff decodes")
                .expect("the stored diff is not empty");
        // The reloader (ours and a real TiDB's domain) reads exactly this back.
        assert_eq!(stored_diff, write.diff, "{sql}");
    }
}

#[test]
fn the_allocated_id_advances_the_global_counter_by_exactly_what_it_took() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        7,
    );
    // Go `GenGlobalIDs(1)`: the key holds the max USED id, so one table moves
    // it from 116 to 117 and the table's own id is 117.
    assert_eq!(write.created_id, Some(117));
    assert_eq!(stored_value(&write, &key::next_global_id_kv_key()), b"117");
}

#[test]
fn a_created_table_is_loadable_and_droppable_by_this_node() {
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.made (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        7,
    );
    apply(&mut store, &created);
    // The catalog reader finds what the writer wrote, at the version it wrote.
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the written catalog loads");
    assert_eq!(catalog.schema_version, 61);
    let (database, table) = catalog.find_table("u6", "made").expect("the created table");
    assert_eq!(database.id, 112);
    assert_eq!(table.id, 117);
    tidb_exec::cluster_catalog::configure_loaded_table(database.name.original(), table)
        .expect("a table this node created is one it can serve");

    // Its own auto-id allocator key was never written, so DROP removes exactly
    // the table key, and the next version is the one after the create's.
    let dropped = plan(&mut store, "DROP TABLE u6.made", 8);
    assert_eq!(dropped.schema_version, 62);
    let deleted: Vec<_> = dropped
        .mutations
        .iter()
        .filter(|mutation| mutation.kind() == OptimisticMutationKind::MetaDelete)
        .map(|mutation| mutation.key().to_vec())
        .collect();
    assert_eq!(deleted, vec![key::table_kv_key(112, 117)]);
    apply(&mut store, &dropped);
    assert!(tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the catalog loads")
        .find_table("u6", "made")
        .is_none());
}

#[test]
fn dropping_a_database_removes_every_field_of_its_hash() {
    let mut store = bootstrapped();
    let created = plan(
        &mut store,
        "CREATE TABLE u6.made (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        7,
    );
    apply(&mut store, &created);
    // A table that has allocated row IDs also owns an allocator field, which
    // Go's `HClear` removes with the rest of the hash.
    store.put(key::auto_table_id_kv_key(112, 117), b"30000".to_vec());

    let dropped = plan(&mut store, "DROP DATABASE u6", 9);
    let mut deleted: Vec<_> = dropped
        .mutations
        .iter()
        .filter(|mutation| mutation.kind() == OptimisticMutationKind::MetaDelete)
        .map(|mutation| mutation.key().to_vec())
        .collect();
    deleted.sort();
    let mut expected = vec![
        key::table_kv_key(112, 117),
        key::auto_table_id_kv_key(112, 117),
        key::database_kv_key(112),
    ];
    expected.sort();
    assert_eq!(deleted, expected);
    apply(&mut store, &dropped);
    assert!(tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the catalog loads")
        .databases
        .is_empty());
}

#[test]
fn an_if_exists_clause_turns_a_missing_object_into_a_no_op_that_spends_no_version() {
    let mut store = bootstrapped();
    for sql in [
        "DROP TABLE IF EXISTS u6.absent",
        "DROP TABLE IF EXISTS absent_db.absent",
        "DROP DATABASE IF EXISTS absent_db",
    ] {
        let plan = plan_ddl(&mut store, &statement(sql), 7).expect("the fixture plans");
        assert!(
            matches!(plan, DdlPlan::AlreadySatisfied { .. }),
            "{sql} must publish nothing"
        );
    }
    let plan = plan_ddl(
        &mut store,
        &statement("CREATE DATABASE IF NOT EXISTS u6"),
        7,
    )
    .expect("the fixture plans");
    assert!(matches!(plan, DdlPlan::AlreadySatisfied { .. }));
}

#[test]
fn a_missing_object_without_if_exists_is_named_precisely() {
    let mut store = bootstrapped();
    let unknown_table = plan_ddl(&mut store, &statement("DROP TABLE u6.absent"), 7)
        .expect_err("a missing table is an error");
    assert!(matches!(unknown_table, DdlPlanError::UnknownTable { .. }));
    assert_eq!(unknown_table.to_string(), "Unknown table 'u6.absent'");

    let unknown_database = plan_ddl(
        &mut store,
        &statement("CREATE TABLE nowhere.t (id BIGINT PRIMARY KEY)"),
        7,
    )
    .expect_err("a missing database is an error");
    assert_eq!(unknown_database.to_string(), "Unknown database 'nowhere'");

    let existing = plan_ddl(&mut store, &statement("CREATE DATABASE U6"), 7)
        .expect_err("a duplicate database is an error, case-insensitively");
    assert_eq!(
        existing.to_string(),
        "Can't create database 'U6'; database exists"
    );
}

#[test]
fn every_unservable_shape_is_refused_before_a_single_mutation_exists() {
    for (sql, expected) in [
        (
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT AS (id + 1))",
            "carries a generated expression, which this node does not support",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, KEY ((v + 1)))",
            "expression index parts are not supported",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BLOB NOT NULL DEFAULT 'x')",
            "can't have a default value",
        ),
        // A `PARTITION BY` clause itself is now BUILT and persisted; what is
        // still refused is a partitioned shape this node cannot serve. Go
        // 8264: a unique key that does not cover the partitioning columns
        // needs a GLOBAL index, which this node does not maintain.
        (
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, UNIQUE KEY uv (v)) \
             PARTITION BY HASH (id) PARTITIONS 2",
            "Global Index is needed for index 'uv'",
        ),
        (
            "CREATE TEMPORARY TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
            "TEMPORARY is not supported",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, ID BIGINT NOT NULL)",
            "declares column `ID` twice",
        ),
        (
            "DROP TABLE u6.a, u6.b",
            "DROP TABLE names exactly one table on this node",
        ),
    ] {
        let reason = refusal(sql);
        assert!(
            reason.contains(expected),
            "`{sql}` was refused with `{reason}`, which does not name `{expected}`"
        );
    }
}

#[test]
fn the_shapes_a_bootstrap_needs_are_admitted_rather_than_refused() {
    // Each of these was refused before the CREATE surface grew to cover the
    // `mysql.*` bootstrap DDL. They must build now, and they must build into
    // exactly the metadata Go builds, which is what
    // `mysql_bootstrap_tableinfo_source` proves table by table.
    //
    // Admitting them does NOT mean this node can serve them: whether a stored
    // table is readable is `configure_loaded_table`'s single decision, taken at
    // LOAD time, and it still refuses everything but a clustered signed-BIGINT
    // handle. Writing the catalog a real TiDB writes and serving it are two
    // separate questions, and only one of them belongs in DDL admission.
    for sql in [
        // A nullable column.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT)",
        // No primary key at all.
        "CREATE TABLE u6.t (id BIGINT NOT NULL, v BIGINT NOT NULL)",
        // A non-integer and an unsigned clustered handle.
        "CREATE TABLE u6.t (id VARCHAR(8) PRIMARY KEY, v BIGINT NOT NULL)",
        "CREATE TABLE u6.t (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT NOT NULL)",
        // A non-clustered and a composite primary key.
        "CREATE TABLE u6.t (id BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id) NONCLUSTERED)",
        "CREATE TABLE u6.t (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
        // Secondary and unique indexes.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, KEY (v))",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL UNIQUE)",
        // The column types the bootstrap corpus needs.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v TIMESTAMP NOT NULL)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v ENUM('N','Y') NOT NULL DEFAULT 'N')",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v SET('a','b'))",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v JSON)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v LONGTEXT)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v SMALLINT UNSIGNED)",
        // Defaults, literal and CURRENT_TIMESTAMP.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL DEFAULT 3)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        // Table options.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) ENGINE=InnoDB",
        // sysbench's own `sbtest1` shape, which this path used to refuse.
        "CREATE TABLE u6.t (id INTEGER NOT NULL AUTO_INCREMENT, k INTEGER NOT NULL, \
         PRIMARY KEY (id))",
        "CREATE TABLE u6.t (id BIGINT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=100",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        assert!(
            lower_ddl(&parsed, "u6").is_ok(),
            "`{sql}` was refused: {}",
            refusal(sql)
        );
    }
}

#[test]
fn cluster_table_options_keep_their_go_error_identity() {
    for (sql, code, reason) in [
        (
            "CREATE TABLE u6.t (id BIGINT) UNION=(u6.other)",
            8232,
            "CREATE/ALTER table with union option is not supported",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT) INSERT_METHOD=FIRST",
            8233,
            "CREATE/ALTER table with insert method option is not supported",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT) ENGINE=imaginary",
            1286,
            "Unknown storage engine 'imaginary'",
        ),
    ] {
        let (actual_code, actual_reason) = refusal_with_code(sql);
        assert_eq!(actual_code, code, "{sql}");
        assert!(actual_reason.contains(reason), "{sql}: {actual_reason}");
    }
}

#[test]
fn parsed_binary_enum_default_reaches_the_shared_normalizer_losslessly() {
    let sql = "CREATE TABLE u6.t (a ENUM(0xff,0x15) CHARACTER SET binary DEFAULT 0xff)";
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    let tidb_ast::Stmt::Ddl(ddl) = &parsed else {
        panic!("the fixture is DDL");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
        panic!("the fixture is CREATE TABLE");
    };
    let column = &create.columns[0];
    let field_type = tidb_executor::ddl::column_field_type::build_field_type(
        &column.name,
        &column.ty,
        "binary",
        "binary",
    )
    .expect("the parsed binary ENUM type is buildable");
    assert_eq!(field_type.elem(0).as_bytes(), [0xff]);
    assert_eq!(field_type.elem(1).as_bytes(), [0x15]);

    let default = column
        .options
        .iter()
        .find_map(|option| match option {
            tidb_ast::ColumnOption::Default(expr) => Some(expr),
            _ => None,
        })
        .expect("the parsed column retains its DEFAULT");
    let value = tidb_expr::eval(default).expect("the literal folds");
    assert!(matches!(value, tidb_datatype::Datum::BinaryLiteral(_)));
    assert_eq!(value.go_bytes(), [0xff]);

    let value = tidb_executor::ddl::normalize_column_default(
        value,
        &field_type,
        &column.name,
        &tidb_datatype::SessionTimeZone::utc(),
    )
    .expect("the parsed raw member passes final strict validation");
    assert_eq!(value.sql_bytes().unwrap(), [0xff]);
}

#[test]
fn enum_and_set_integer_defaults_keep_their_literal_kind() {
    for (sql, expected) in [
        (
            "CREATE TABLE u6.t (a ENUM('2','3','4') DEFAULT 2)",
            b"3".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('a','c','d') DEFAULT 2)",
            b"c".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('2','3','4') DEFAULT '2')",
            b"2".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('9223372036854775808') DEFAULT 9223372036854775808)",
            b"9223372036854775808".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM('first','second') DEFAULT TRUE)",
            b"first".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('2','x') DEFAULT 2)",
            b"x".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('2','x') DEFAULT '2')",
            b"2".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('9223372036854775808') DEFAULT 9223372036854775808)",
            b"9223372036854775808".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('1','4','10','21') DEFAULT 3)",
            b"1,4".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a SET('1','4','10','21') DEFAULT 15)",
            b"1,4,10,21".as_slice(),
        ),
        (
            "CREATE TABLE u6.t (a ENUM(0xff,0x15) CHARACTER SET binary DEFAULT 0xff)",
            &[0xff],
        ),
        (
            "CREATE TABLE u6.t (a SET(0xff,0x15) CHARACTER SET binary DEFAULT 0x15)",
            &[0x15],
        ),
        (
            "CREATE TABLE u6.t (a ENUM(b'11111111',b'00010101') CHARACTER SET binary DEFAULT b'00010101')",
            &[0x15],
        ),
        (
            "CREATE TABLE u6.t (a VARBINARY(2) DEFAULT b'000000001')",
            &[0x00, 0x01],
        ),
        (
            "CREATE TABLE u6.t (a BIGINT DEFAULT 0x10)",
            b"16".as_slice(),
        ),
    ] {
        assert_eq!(stored_default_bytes(sql), expected, "{sql}");
    }

    for sql in [
        "CREATE TABLE u6.t (a ENUM('1','4','10') DEFAULT 0)",
        "CREATE TABLE u6.t (a ENUM('1','4','10') DEFAULT FALSE)",
        "CREATE TABLE u6.t (a ENUM('1','4','10') DEFAULT 4)",
        "CREATE TABLE u6.t (a SET('1','4','10') DEFAULT 0)",
        "CREATE TABLE u6.t (a SET('1','4','10') DEFAULT 8)",
    ] {
        assert_eq!(refusal(sql), "Invalid default value for 'a'", "{sql}");
    }
}

#[test]
fn defaults_are_validated_and_persisted_against_the_final_column_type() {
    // A non-key NULL default is checked only after later nullability options.
    assert_eq!(
        refusal_with_code("CREATE TABLE u6.t (a BIGINT DEFAULT NULL NOT NULL)"),
        (1067, "Invalid default value for 'a'".to_owned())
    );

    // Go's first `checkPriKeyConstraint` arm can see only an INLINE key. Its
    // DEFAULT NULL is 1067 and wins even when an explicit NULL also exists.
    for sql in [
        "CREATE TABLE u6.t (a BIGINT PRIMARY KEY DEFAULT NULL)",
        "CREATE TABLE u6.t (a BIGINT PRIMARY KEY NULL DEFAULT NULL)",
    ] {
        assert_eq!(
            refusal_with_code(sql),
            (1067, "Invalid default value for 'a'".to_owned()),
            "{sql}"
        );
    }

    // The table-level key is installed only after that precheck. An explicit
    // NULL is then 1171, even ahead of a non-NULL spelling that final default
    // validation would reject. Without explicit NULL, a NULL default is also
    // 1171, including when a separate NOT NULL option is present.
    for sql in [
        "CREATE TABLE u6.t (a BIGINT DEFAULT NULL, PRIMARY KEY (a))",
        "CREATE TABLE u6.t (a BIGINT NULL DEFAULT NULL, PRIMARY KEY (a))",
        "CREATE TABLE u6.t (a BIGINT NULL DEFAULT 'bad', PRIMARY KEY (a))",
        "CREATE TABLE u6.t (a BIGINT NOT NULL DEFAULT NULL, PRIMARY KEY (a))",
    ] {
        assert_eq!(
            refusal_with_code(sql),
            (
                1171,
                "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead"
                    .to_owned()
            ),
            "{sql}"
        );
    }

    // The settled spelling includes Go's fixed-BINARY padding, while the
    // model setter retains a BIT default's raw-byte shadow.
    assert_eq!(
        stored_default_bytes("CREATE TABLE u6.t (a BINARY(4) DEFAULT 0x61)"),
        b"a\0\0\0".as_slice()
    );
    let DdlStatement::CreateTable { build, .. } =
        statement("CREATE TABLE u6.t (a BIT(9) DEFAULT b'1')")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    let column = build
        .template()
        .columns
        .get(0)
        .expect("the fixture declares a BIT column");
    let column = column.read();
    assert_eq!(column.default_value_bit.snapshot(), vec![1]);
    assert_eq!(
        column.default_value.builtin_string().map(|value| value.as_bytes()),
        Some(&[1][..])
    );

    // The non-expression clock marker stays on its computed-default path.
    assert_eq!(
        stored_default_bytes("CREATE TABLE u6.t (a TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"),
        b"CURRENT_TIMESTAMP".as_slice()
    );
}

#[test]
fn cluster_create_persists_a_literal_timestamp_in_utc_from_the_session_zone() {
    let context = tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
        .with_time_zone(tidb_datatype::SessionTimeZone::Fixed {
            name: "+08:00".to_owned(),
            offset_secs: 8 * 60 * 60,
        });
    let sql = "CREATE TABLE u6.t (a TIMESTAMP DEFAULT '2020-01-02 08:00:00')";
    let parsed = tidb_parser::parse_with_sql_mode(sql, context.sql_mode()).expect("parses");
    let DdlStatement::CreateTable { build, .. } =
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect("the timestamp default is admitted")
            .expect("the statement is cluster DDL")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    let column_handle = build.template().columns.get(0).expect("one column");
    let column = column_handle.read();
    assert_eq!(
        column.default_value.builtin_string().map(|value| value.as_bytes()),
        Some(b"2020-01-02 00:00:00".as_slice())
    );
}

#[test]
fn cluster_create_folds_a_timestamp_expression_in_the_session_zone() {
    fn stored(zone: tidb_datatype::SessionTimeZone) -> Vec<u8> {
        let context = tidb_executor::StmtContext::for_query()
            .with_strict(true)
            .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
            .with_time_zone(zone);
        let sql =
            "CREATE TABLE u6.t (v VARCHAR(64) DEFAULT (TIMESTAMP '2024-01-01 14:00:00+05:00'))";
        let parsed = tidb_parser::parse_with_sql_mode(sql, context.sql_mode()).expect("parses");
        let DdlStatement::CreateTable { build, .. } =
            lower_ddl_with_context(&parsed, "u6", &context)
                .expect("the expression default is admitted")
                .expect("the statement is cluster DDL")
        else {
            panic!("the fixture is CREATE TABLE");
        };
        let column_handle = build.template().columns.get(0).expect("one column");
        let column = column_handle.read();
        match column.default_value.view() {
            Some(GoAnyView::String(bytes)) => bytes.as_bytes().to_vec(),
            other => panic!("the fixture stored a non-string default: {other:?}"),
        }
    }

    assert_eq!(
        stored(tidb_datatype::SessionTimeZone::Fixed {
            name: "+02:00".to_owned(),
            offset_secs: 2 * 60 * 60,
        }),
        b"2024-01-01 11:00:00"
    );
    assert_eq!(
        stored(tidb_datatype::SessionTimeZone::utc()),
        b"2024-01-01 09:00:00"
    );
}

#[test]
fn cluster_create_default_admission_uses_the_captured_date_modes() {
    let sql = "CREATE TABLE u6.t (a DATE DEFAULT '0000-00-00')";
    let strict_default = tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
        .with_time_zone(tidb_datatype::SessionTimeZone::utc());
    let parsed = tidb_parser::parse_with_sql_mode(sql, strict_default.sql_mode()).expect("parses");
    let error = lower_ddl_with_context(&parsed, "u6", &strict_default)
        .expect_err("NO_ZERO_DATE rejects the default");
    assert_eq!(
        (error.code, error.sql_state(), error.reason.as_str()),
        (1067, *b"42000", "Invalid default value for 'a'")
    );

    let permissive = tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::default())
        .with_time_zone(tidb_datatype::SessionTimeZone::utc());
    assert!(lower_ddl_with_context(&parsed, "u6", &permissive)
        .expect("zero dates are admitted when the mode bits allow them")
        .is_some());
}

#[test]
fn cluster_create_preserves_coded_default_errors() {
    for (sql, code, state, message) in [
        (
            "CREATE TABLE u6.t (a INT DEFAULT (ABS(1)))",
            3770,
            *b"HY000",
            "Default value expression of column 'a' contains a disallowed function: `abs`.",
        ),
        (
            "CREATE TABLE u6.t (ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP)",
            1067,
            *b"42000",
            "Invalid default value for 'ts'",
        ),
    ] {
        let parsed = tidb_parser::parse(sql).expect("parses");
        let error = lower_ddl(&parsed, "u6").expect_err("the default is refused");
        assert_eq!((error.code, error.sql_state()), (code, state), "{sql}");
        assert_eq!(error.reason, message, "{sql}");
    }
}

#[test]
fn a_statement_this_module_does_not_own_is_left_to_its_own_path() {
    for sql in [
        "SELECT 1",
        "INSERT INTO u6.t VALUES (1, 2)",
        // Every column ALTER spelling now routes to the catalog writer;
        // partition management remains genuinely unowned.
        "ALTER TABLE u6.t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        assert!(
            lower_ddl(&parsed, "u6").expect("no refusal").is_none(),
            "`{sql}` is not a catalog change this module owns"
        );
    }
}

/// Go routes the single-action ALTER spelling through the same add/drop-index
/// job as standalone `CREATE INDEX`/`DROP INDEX`. The cluster catalog does the
/// same, so both spellings publish the metadata mutation and row backfill.
#[test]
fn alter_table_index_actions_share_the_catalog_backfill_path() {
    let mut store = bootstrapped();
    let table_id = table_with_two_columns(&mut store);

    let added = plan(
        &mut store,
        "ALTER TABLE u6.minimal ADD UNIQUE INDEX vi (v)",
        470_000_000,
    );
    assert_eq!(added.diff.action_type.0, 7, "ActionAddIndex");
    let backfill = added.backfill.as_ref().expect("the index owes entries");
    assert!(backfill.add);
    assert!(backfill.index.read().unique);
    assert_eq!(stored_table(&added, table_id)["index_info"][0]["idx_name"]["O"], "vi");
    apply(&mut store, &added);

    let dropped = plan(
        &mut store,
        "ALTER TABLE u6.minimal DROP INDEX vi",
        470_000_001,
    );
    assert_eq!(dropped.diff.action_type.0, 8, "ActionDropIndex");
    assert!(!dropped.backfill.as_ref().expect("entries are removed").add);

    // A bundle of TWO index actions now reaches the plan, where the single
    // backfill slot refuses it by name -- still before any partial change.
    let multiple = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.minimal ADD INDEX i1 (v), ADD INDEX i2 (v)"),
        470_000_002,
    )
    .expect_err("two index changes refuse")
    .to_string();
    assert!(
        multiple.contains("at most one index change"),
        "multi-index ALTER must fail before a partial catalog change: {multiple}"
    );
}

/// Go's single-table rename job keeps the table ID and its auto-ID authority,
/// while moving the metadata field to the destination schema.  `ALTER TABLE
/// ... RENAME TO` uses that same job rather than a distinct local-only path.
#[test]
fn alter_table_rename_moves_catalog_metadata_without_reissuing_ids() {
    let mut store = bootstrapped();
    let archive = plan(&mut store, "CREATE DATABASE archive", 470_000_100);
    apply(&mut store, &archive);
    let created = plan(
        &mut store,
        "CREATE TABLE u6.made (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
        470_000_101,
    );
    let table_id = created.created_id.expect("a table id");
    apply(&mut store, &created);

    let renamed = plan(
        &mut store,
        "ALTER TABLE u6.made RENAME TO archive.renamed",
        470_000_102,
    );
    assert_eq!(renamed.diff.action_type.0, 14, "ActionRenameTable");
    assert_eq!(renamed.diff.old_schema_id, 112);
    assert!(renamed
        .mutations
        .iter()
        .any(|mutation| mutation.kind() == OptimisticMutationKind::MetaDelete
            && mutation.key() == key::table_kv_key(112, table_id)));
    apply(&mut store, &renamed);
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the renamed catalog loads");
    assert!(catalog.find_table("u6", "made").is_none());
    let (database, table) = catalog
        .find_table("archive", "renamed")
        .expect("the renamed table");
    assert_eq!(table.id, table_id);
    assert_eq!(table.auto_id_schema_id, 112);
    assert_ne!(database.id, 112);

    let returned = plan(
        &mut store,
        "ALTER TABLE archive.renamed RENAME TO u6.made_again",
        470_000_103,
    );
    apply(&mut store, &returned);
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the returned catalog loads");
    let (_, table) = catalog
        .find_table("u6", "made_again")
        .expect("the returned table");
    assert_eq!(table.id, table_id);
    assert_eq!(table.auto_id_schema_id, 0);

    let identity = tidb_parser::parse("ALTER TABLE u6.made_again RENAME TO u6.made_again")
        .expect("the identity spelling parses");
    assert!(
        lower_ddl(&identity, "u6")
            .expect("an identity ALTER rename is accepted")
            .is_none(),
        "Go does not spend a DDL job on an identity ALTER rename"
    );

    let pairs = tidb_parser::parse("RENAME TABLE u6.made_again TO u6.a, u6.a TO u6.b")
        .expect("the multi-pair spelling parses");
    let lowered = lower_ddl(&pairs, "u6")
        .expect("the full multi-table rename is admitted")
        .expect("a catalog change");
    let DdlStatement::RenameTables { pairs } = lowered else {
        panic!("the multi-pair spelling retains every pair");
    };
    assert_eq!(pairs.len(), 2);
    let renamed_twice = match plan_ddl(&mut store, &DdlStatement::RenameTables { pairs }, 470_000_104)
        .expect("one atomic multi-table rename plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write, got {detail}"),
    };
    assert_eq!(renamed_twice.diff.action_type.0, 47, "ActionRenameTables");
    assert_eq!(renamed_twice.diff.affected_options.len(), 1);
    apply(&mut store, &renamed_twice);
    let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
        .expect("the multi-renamed catalog loads");
    assert!(catalog.find_table("u6", "made_again").is_none());
    assert!(catalog.find_table("u6", "a").is_none());
    let (_, table) = catalog
        .find_table("u6", "b")
        .expect("the second pair sees the first pair's namespace");
    assert_eq!(table.id, table_id);
}

#[test]
fn an_unqualified_name_resolves_against_the_sessions_default_schema() {
    let parsed = tidb_parser::parse("CREATE TABLE t (id BIGINT PRIMARY KEY)").expect("parses");
    let DdlStatement::CreateTable { schema, table, .. } = lower_ddl(&parsed, "campaign31")
        .expect("admitted")
        .expect("a catalog change")
    else {
        panic!("a CREATE TABLE");
    };
    assert_eq!((schema.as_str(), table.as_str()), ("campaign31", "t"));
}

/// The live bug this closes: `CREATE TABLE ... AUTO_INCREMENT` was accepted
/// and written, and the catalog loader then refused the very table the
/// statement had just created, so its creator answered `table not found in
/// catalog` to both `INSERT` and `SELECT` (`sysbench-readiness.md`, blocker 3,
/// from sysbench's own `sbtest1` shape). That was replaced by an honest
/// refusal, and the refusal is now gone in turn: the counter has the meta-key
/// home Go gives it (`tidb_exec::cluster_auto_id`), so the shape is admitted
/// and served.
#[test]
fn create_table_with_auto_increment_is_admitted_now_the_counter_has_a_home() {
    let parsed = tidb_parser::parse(
        "CREATE TABLE sbtest1 (id INTEGER NOT NULL AUTO_INCREMENT, k INTEGER NOT NULL, \
         PRIMARY KEY (id))",
    )
    .expect("the fixture SQL parses");
    let DdlStatement::CreateTable { build, .. } = lower_ddl(&parsed, "sbtest")
        .expect("admitted")
        .expect("a catalog change")
    else {
        panic!("a CREATE TABLE");
    };
    assert!(
        build
            .template()
            .columns
            .get(0)
            .expect("the fixture declares id")
            .read()
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT),
        "the admitted template keeps the AUTO_INCREMENT flag the loader reads"
    );
    // Go `SepAutoInc`: without `AUTO_ID_CACHE 1` the ids come from the row-id
    // key, the SAME one `_tidb_rowid` uses, which is what a Go `tidb-server`
    // on this cluster reads. Picking `IID:` because the name matches would
    // give the two nodes separate counters for one column, with nothing to
    // detect it.
    assert!(!build.template().sep_auto_inc());
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_id_key_for(7, build.template()),
        tidb_meta::key::auto_table_id_kv_key(7, build.template().id),
    );
}

#[test]
fn create_table_with_auto_random_persists_its_allocator_format() {
    let parsed = tidb_parser::parse(
        "CREATE TABLE ar (id BIGINT UNSIGNED AUTO_RANDOM(5, 32) PRIMARY KEY, v INT) \
         AUTO_RANDOM_BASE=100",
    )
    .expect("the fixture SQL parses");
    let DdlStatement::CreateTable { build, .. } = lower_ddl(&parsed, "test")
        .expect("admitted")
        .expect("a catalog change")
    else {
        panic!("a CREATE TABLE");
    };
    assert_eq!(build.template().auto_random_bits, 5);
    assert_eq!(build.template().auto_random_range_bits, 32);
    assert_eq!(build.template().auto_rand_id, 100);
    assert!(build.template().is_auto_random_bit_col_unsigned());
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_random_id_key_for(7, build.template()),
        tidb_meta::key::auto_random_table_id_kv_key(7, build.template().id),
    );

    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE ar (id BIGINT UNSIGNED AUTO_RANDOM(5, 32) PRIMARY KEY, v INT) \
         AUTO_RANDOM_BASE=100",
        123,
    );
    assert_eq!(
        stored_value(
            &write,
            &tidb_meta::key::auto_random_table_id_kv_key(112, write.created_id.unwrap())
        ),
        b"99"
    );
}

#[test]
fn alter_auto_random_base_updates_table_info_and_the_tarid_counter_together() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE ar_alter (id BIGINT AUTO_RANDOM(5) PRIMARY KEY, v INT)",
        123,
    );
    apply(&mut store, &create);
    store.put(key::schema_version_kv_key(), b"61".to_vec());

    let alter = plan(
        &mut store,
        "ALTER TABLE ar_alter AUTO_RANDOM_BASE=500",
        124,
    );
    let table_id = create.created_id.unwrap();
    let table: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &alter,
        &key::table_kv_key(112, table_id),
    ))
    .unwrap();
    assert_eq!(table.auto_rand_id, 500);
    assert_eq!(
        stored_value(
            &alter,
            &key::auto_random_table_id_kv_key(112, table_id)
        ),
        b"499"
    );
    assert_eq!(
        alter.diff.action_type,
        tidb_model::ActionType::ACTION_REBASE_AUTO_RANDOM_BASE
    );

    apply(&mut store, &alter);
    store.put(key::schema_version_kv_key(), b"62".to_vec());
    let lower = plan(
        &mut store,
        "ALTER TABLE ar_alter AUTO_RANDOM_BASE=10",
        125,
    );
    let lower_table: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &lower,
        &key::table_kv_key(112, table_id),
    ))
    .unwrap();
    assert_eq!(lower_table.auto_rand_id, 500);
    assert_eq!(
        stored_value(
            &lower,
            &key::auto_random_table_id_kv_key(112, table_id)
        ),
        b"499"
    );

    apply(&mut store, &lower);
    store.put(key::schema_version_kv_key(), b"63".to_vec());
    let forced = plan(
        &mut store,
        "ALTER TABLE ar_alter FORCE AUTO_RANDOM_BASE=2",
        126,
    );
    let forced_table: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &forced,
        &key::table_kv_key(112, table_id),
    ))
    .unwrap();
    assert_eq!(forced_table.auto_rand_id, 2);
    assert_eq!(
        stored_value(
            &forced,
            &key::auto_random_table_id_kv_key(112, table_id)
        ),
        b"1"
    );

    let forced_zero = statement("ALTER TABLE ar_alter FORCE AUTO_RANDOM_BASE=0");
    assert!(matches!(
        plan_ddl(&mut store, &forced_zero, 127).unwrap_err(),
        DdlPlanError::AutoIdReadFailed
    ));

    let plain_create = plan(
        &mut store,
        "CREATE TABLE not_random (id BIGINT PRIMARY KEY)",
        128,
    );
    apply(&mut store, &plain_create);
    store.put(key::schema_version_kv_key(), b"64".to_vec());
    let non_random = statement("ALTER TABLE not_random AUTO_RANDOM_BASE=10");
    assert!(matches!(
        plan_ddl(&mut store, &non_random, 129).unwrap_err(),
        DdlPlanError::InvalidAutoRandom(reason)
            if reason == "alter auto_random_base of a non auto_random table"
    ));
}

#[test]
fn alter_auto_id_cache_publishes_table_metadata_without_touching_the_counter() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE cached (id INT AUTO_INCREMENT PRIMARY KEY)",
        130,
    );
    apply(&mut store, &create);
    store.put(key::schema_version_kv_key(), b"61".to_vec());

    let alter = plan(&mut store, "ALTER TABLE cached AUTO_ID_CACHE=100", 131);
    let table_id = create.created_id.unwrap();
    let table: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &alter,
        &key::table_kv_key(112, table_id),
    ))
    .unwrap();
    assert_eq!(table.auto_id_cache, 100);
    assert_eq!(
        alter.diff.action_type,
        tidb_model::ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE
    );
    assert!(!alter
        .mutations
        .iter()
        .any(|mutation| mutation.key() == key::auto_table_id_kv_key(112, table_id)));

    apply(&mut store, &alter);
    store.put(key::schema_version_kv_key(), b"62".to_vec());
    assert!(matches!(
        plan_ddl(
            &mut store,
            &statement("ALTER TABLE cached AUTO_ID_CACHE=1"),
            132,
        )
        .unwrap_err(),
        DdlPlanError::Unsupported(reason)
            if reason == "Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different"
    ));
}

#[test]
fn modify_auto_random_bits_updates_table_info_and_the_tarid_counter_together() {
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE ar_bits_cluster (id BIGINT AUTO_RANDOM(5) PRIMARY KEY, v INT)",
        130,
    );
    apply(&mut store, &create);
    store.put(key::schema_version_kv_key(), b"61".to_vec());
    let table_id = create.created_id.unwrap();

    let alter = plan(
        &mut store,
        "ALTER TABLE ar_bits_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(8)",
        131,
    );
    let table: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &alter,
        &key::table_kv_key(112, table_id),
    ))
    .unwrap();
    assert_eq!(table.auto_random_bits, 8);
    assert_eq!(table.auto_random_range_bits, 64);
    assert_eq!(
        stored_value(
            &alter,
            &key::auto_random_table_id_kv_key(112, table_id)
        ),
        b"1"
    );
    assert_eq!(
        alter.diff.action_type,
        tidb_model::ActionType::ACTION_MODIFY_COLUMN
    );

    apply(&mut store, &alter);
    store.put(key::schema_version_kv_key(), b"62".to_vec());
    let decrease = statement(
        "ALTER TABLE ar_bits_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(7)",
    );
    assert!(matches!(
        plan_ddl(&mut store, &decrease, 132).unwrap_err(),
        DdlPlanError::InvalidAutoRandom(reason)
            if reason == "decreasing auto_random shard bits is not supported"
    ));
    let wrong_column = statement(
        "ALTER TABLE ar_bits_cluster MODIFY COLUMN v BIGINT AUTO_RANDOM(9)",
    );
    assert!(matches!(
        plan_ddl(&mut store, &wrong_column, 133).unwrap_err(),
        DdlPlanError::InvalidAutoRandom(reason)
            if reason == "auto_random can only be converted from auto_increment clustered primary key"
    ));

    let create_ai = plan(
        &mut store,
        "CREATE TABLE ai_bits_cluster (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)",
        134,
    );
    apply(&mut store, &create_ai);
    store.put(key::schema_version_kv_key(), b"63".to_vec());
    let ai_table_id = create_ai.created_id.unwrap();
    store.put(
        key::auto_table_id_kv_key(112, ai_table_id),
        b"100".to_vec(),
    );
    let converted = plan(
        &mut store,
        "ALTER TABLE ai_bits_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(5)",
        135,
    );
    let converted_table: tidb_model::TableInfo = serde_json::from_slice(stored_value(
        &converted,
        &key::table_kv_key(112, ai_table_id),
    ))
    .unwrap();
    assert_eq!(converted_table.auto_random_bits, 5);
    assert_eq!(
        converted_table
            .get_pk_col_info()
            .unwrap()
            .read()
            .get_flag()
            & u64::from(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT),
        0
    );
    assert_eq!(
        stored_value(
            &converted,
            &key::auto_random_table_id_kv_key(112, ai_table_id)
        ),
        b"101"
    );
    assert!(converted.mutations.iter().any(|mutation| {
        mutation.kind() == OptimisticMutationKind::MetaDelete
            && mutation.key() == key::auto_table_id_kv_key(112, ai_table_id)
    }));

    let create_separate = plan(
        &mut store,
        "CREATE TABLE ai_separate_cluster (id BIGINT AUTO_INCREMENT PRIMARY KEY)",
        136,
    );
    apply(&mut store, &create_separate);
    store.put(key::schema_version_kv_key(), b"64".to_vec());
    let separate_table_id = create_separate.created_id.unwrap();
    let separate_table_key = key::table_kv_key(112, separate_table_id);
    let mut separate_info: tidb_model::TableInfo = serde_json::from_slice(
        store
            .pairs
            .get(&separate_table_key)
            .expect("the committed table metadata exists"),
    )
    .unwrap();
    separate_info.auto_id_cache = 1;
    store.put(
        separate_table_key,
        value::serialize_table_info(&separate_info).unwrap(),
    );
    store.put(
        key::auto_increment_id_kv_key(112, separate_table_id),
        b"100".to_vec(),
    );
    store.put(
        key::auto_table_id_kv_key(112, separate_table_id),
        b"40".to_vec(),
    );
    let separate = plan(
        &mut store,
        "ALTER TABLE ai_separate_cluster MODIFY COLUMN id BIGINT AUTO_RANDOM(5)",
        137,
    );
    assert_eq!(
        stored_value(
            &separate,
            &key::auto_increment_id_kv_key(112, separate_table_id)
        ),
        b"101"
    );
    assert_eq!(
        stored_value(
            &separate,
            &key::auto_random_table_id_kv_key(112, separate_table_id)
        ),
        b"40"
    );
    assert!(separate.mutations.iter().any(|mutation| {
        mutation.kind() == OptimisticMutationKind::MetaDelete
            && mutation.key() == key::auto_table_id_kv_key(112, separate_table_id)
    }));
}

/// `AUTO_ID_CACHE 1` is Go's `SepAutoInc`, and only then does the counter move
/// to its own `IID:` key.
///
/// This node's own `CREATE TABLE` refuses the `AUTO_ID_CACHE` option (a
/// separate, pre-existing refusal), so the shape is built here the way it
/// really reaches this node: LOADED, from a table a Go `tidb-server` created.
/// The branch is not dead code — it is the case where reading `TID:` would
/// silently count in a key the owning Go node never touches.
#[test]
fn a_separate_allocator_table_counts_in_the_increment_key() {
    let mut template = tidb_model::table_info::TableInfo {
        id: 91,
        version: tidb_model::table_info::TABLE_INFO_VERSION5,
        ..tidb_model::table_info::TableInfo::default()
    };
    assert!(
        !template.sep_auto_inc(),
        "an ordinary table counts in the row-id key"
    );
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_id_key_for(7, &template),
        tidb_meta::key::auto_table_id_kv_key(7, 91),
    );

    template.auto_id_cache = 1;
    assert!(
        template.sep_auto_inc(),
        "AUTO_ID_CACHE 1 is Go's SepAutoInc"
    );
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_id_key_for(7, &template),
        tidb_meta::key::auto_increment_id_kv_key(7, 91),
    );
}

/// The stored table the index tests below are planned against.
fn table_with_two_columns(store: &mut MetaStore) -> i64 {
    let write = plan(
        store,
        "CREATE TABLE u6.minimal (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)",
        467_996_279_696_261_139,
    );
    apply(store, &write);
    write.created_id.expect("a table id")
}

/// Reads back the `TableInfo` a write set stored for `table_id`.
pub(crate) fn stored_table(write: &tidb_exec::cluster_ddl::DdlWrite, table_id: i64) -> serde_json::Value {
    serde_json::from_slice(stored_value(write, &key::table_kv_key(112, table_id)))
        .expect("a stored TableInfo")
}

/// The index lands in `index_info` with the id `max_idx_id` names, and its
/// column offset is resolved against the STORED table rather than trusted from
/// the statement -- Go's `IndexColumn.Offset` is a position in `TableInfo.Cols`.
#[test]
fn create_index_stores_the_index_and_owes_a_backfill() {
    let mut store = bootstrapped();
    let table_id = table_with_two_columns(&mut store);
    let write = plan(&mut store, "CREATE INDEX vi ON u6.minimal (v)", 470_000_000);
    let stored = stored_table(&write, table_id);

    assert_eq!(stored["max_idx_id"], 1, "the first index of this table");
    let index = &stored["index_info"][0];
    assert_eq!(index["id"], 1);
    assert_eq!(index["idx_name"]["O"], "vi");
    assert_eq!(index["is_unique"], false);
    assert_eq!(index["is_primary"], false);
    // `state` 5 is Go's `StatePublic`.
    assert_eq!(index["state"], 5);
    assert_eq!(index["idx_cols"][0]["name"]["O"], "v");
    assert_eq!(
        index["idx_cols"][0]["offset"], 1,
        "`v` is the second column"
    );
    assert_eq!(index["idx_cols"][0]["length"], -1, "not a prefix index");
    assert_eq!(
        stored["update_timestamp"], 470_000_000_u64,
        "Go stamps the job transaction's own start timestamp"
    );

    // `ActionAddIndex`, so a peer's schema reload knows what changed.
    assert_eq!(write.diff.action_type.0, 7);
    assert_eq!(write.diff.table_id, table_id);

    // The half that keeps the index from being EMPTY. Losing it is the silent
    // wrong answer this whole path exists to avoid, which is why the publisher
    // that cannot perform it refuses outright rather than writing the meta half.
    let backfill = write.backfill.as_ref().expect("entries are owed");
    assert!(backfill.add);
    {
        let index = backfill.index.read();
        assert_eq!(index.id, 1);
    }
    assert_eq!(
        backfill.table.indices.len(),
        0,
        "the walker gets the table as its stored ROWS have it: before the change"
    );
}

/// Go captures `collate.NewCollationEnabled()` in `DDLReorgMeta` when the job
/// is built. The backfill must carry that snapshot rather than re-read the
/// runtime switch after the catalog mutation has already been planned.
#[test]
fn index_backfill_carries_the_planned_collation_mode() {
    for use_new_collation in [false, true] {
        let mut store = bootstrapped();
        table_with_two_columns(&mut store);
        let write = match plan_ddl_with_collation(
            &mut store,
            &statement("CREATE INDEX vi ON u6.minimal (v)"),
            470_000_000,
            use_new_collation,
        )
        .expect("the fixture plans")
        {
            DdlPlan::Write(write) => write,
            DdlPlan::AlreadySatisfied { detail, .. } => {
                panic!("expected a write, got already-satisfied: {detail}")
            }
        };
        assert_eq!(
            write
                .backfill
                .as_ref()
                .expect("CREATE INDEX owes a backfill")
                .use_new_collation,
            use_new_collation
        );
    }
}

/// A second index of the same name is 1061, and `IF NOT EXISTS` makes it a
/// no-op that spends no schema version.
#[test]
fn a_duplicate_index_name_is_refused_and_if_not_exists_is_a_no_op() {
    let mut store = bootstrapped();
    table_with_two_columns(&mut store);
    let write = plan(&mut store, "CREATE INDEX vi ON u6.minimal (v)", 470_000_000);
    apply(&mut store, &write);

    let refused = plan_ddl(
        &mut store,
        &statement("CREATE INDEX vi ON u6.minimal (v)"),
        470_000_001,
    )
    .expect_err("a duplicate index name is an error");
    assert!(
        matches!(&refused, DdlPlanError::DuplicateKeyName(name) if name == "vi"),
        "{refused}"
    );
    assert_eq!(refused.to_string(), "Duplicate key name 'vi'");

    match plan_ddl(
        &mut store,
        &statement("CREATE INDEX IF NOT EXISTS vi ON u6.minimal (v)"),
        470_000_002,
    )
    .expect("IF NOT EXISTS plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("IF NOT EXISTS on an existing index must write nothing"),
    }
}

/// Every shape whose entries this node would not go on to maintain is refused
/// before a timestamp is spent: publishing one writes a `TableInfo` this node's
/// own catalog loader then drops, so the table would vanish from the very
/// connection that indexed it.
#[test]
fn index_shapes_this_node_cannot_maintain_are_refused_at_admission() {
    for (sql, expected) in [
        (
            "CREATE INDEX ci ON u6.minimal (c(4))",
            "a prefix-length index",
        ),
        (
            "CREATE INDEX ei ON u6.minimal ((v + 1))",
            "an expression index",
        ),
        (
            "CREATE FULLTEXT INDEX fi ON u6.minimal (c)",
            "CREATE FULLTEXT INDEX",
        ),
    ] {
        let reason = refusal(sql);
        assert!(
            reason.contains(expected),
            "`{sql}` must be refused for {expected}, got: {reason}"
        );
    }
}

/// The index leaves the stored table, and its entries are named for removal in
/// the same transaction -- a stale entry reads as a row that is not there.
#[test]
fn drop_index_removes_it_and_owes_the_entry_removal() {
    let mut store = bootstrapped();
    let table_id = table_with_two_columns(&mut store);
    let created = plan(&mut store, "CREATE INDEX vi ON u6.minimal (v)", 470_000_000);
    apply(&mut store, &created);

    let write = plan(&mut store, "DROP INDEX vi ON u6.minimal", 470_000_003);
    let stored = stored_table(&write, table_id);
    assert!(
        stored["index_info"]
            .as_array()
            .is_none_or(|indexes| indexes.is_empty()),
        "the index is gone from the stored table: {}",
        stored["index_info"]
    );
    assert_eq!(
        stored["max_idx_id"], 1,
        "Go never lowers MaxIndexID, so a later index cannot reuse the id"
    );
    // `ActionDropIndex`.
    assert_eq!(write.diff.action_type.0, 8);
    let backfill = write.backfill.as_ref().expect("entries are owed");
    assert!(!backfill.add);
    {
        let index = backfill.index.read();
        assert_eq!(index.name.original(), "vi");
    }
    assert_eq!(
        backfill.table.indices.len(),
        1,
        "the walker gets the table with the index still on it, so it can key its entries"
    );

    apply(&mut store, &write);
    let refused = plan_ddl(
        &mut store,
        &statement("DROP INDEX nosuch ON u6.minimal"),
        470_000_004,
    )
    .expect_err("a missing index is an error");
    // Go `ErrCantDropFieldOrKey` (1091), not a message of this port's own.
    assert_eq!(
        refused.to_string(),
        "Can't DROP 'nosuch'; check that column/key exists"
    );
    match plan_ddl(
        &mut store,
        &statement("DROP INDEX IF EXISTS nosuch ON u6.minimal"),
        470_000_005,
    )
    .expect("IF EXISTS plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("IF EXISTS on a missing index must write nothing"),
    }
}

/// Go `preprocessor.checkAutoIncrementOp`: the allocator hands out integers,
/// so a non-numeric AUTO_INCREMENT column is refused.
///
/// The cluster tier used to refuse EVERY `AUTO_INCREMENT` table for its own
/// reason, which hid this. Captured from a Go `tidb-server` on the same
/// cluster: `id VARCHAR(10) NOT NULL AUTO_INCREMENT` answers
/// `ERROR 1105 (HY000): Incorrect column specifier for column 'id'`, while
/// without the check this node created the table and then failed every INSERT
/// with a decode error -- an unusable table reported as a success.
#[test]
fn a_non_numeric_auto_increment_column_is_refused_the_way_go_refuses_it() {
    for sql in [
        "CREATE TABLE t (id VARCHAR(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id DATETIME NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id DECIMAL(10,2) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        let refused = lower_ddl(&parsed, "u6").expect_err("this shape must be refused");
        assert_eq!(
            refused.reason, "Incorrect column specifier for column 'id'",
            "`{sql}`"
        );
    }
    // Go's list is WIDER than "integer": FLOAT and DOUBLE are in it, and a Go
    // tidb-server really does accept `id DOUBLE NOT NULL AUTO_INCREMENT`.
    for sql in [
        "CREATE TABLE t (id TINYINT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id MEDIUMINT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id FLOAT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
        "CREATE TABLE t (id DOUBLE NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        assert!(lower_ddl(&parsed, "u6").is_ok(), "`{sql}` must be admitted");
    }
}

#[test]
fn add_column_appends_a_public_nullable_column_and_refuses_rewrites() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT NOT NULL)",
        100,
    );
    apply(&mut store, &write);

    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let write = plan(&mut store, "ALTER TABLE u6.t ADD COLUMN note VARCHAR(32)", 200);
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    assert_eq!(columns.len(), 3, "the new column is appended");
    let added = &columns[2];
    assert_eq!(added["name"]["O"], "note");
    // Go `AllocateColumnID`: past the existing max, never reused.
    assert_eq!(added["id"], 3);
    assert_eq!(added["offset"], 2);
    assert_eq!(added["state"], 5, "public immediately: no backfill was needed");
    assert_eq!(stored["max_col_id"], 3);

    // A duplicate is MySQL's own message; IF NOT EXISTS is a no-op. Both are
    // plan-time answers: only the stored table knows its columns.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN note VARCHAR(8)"),
        300,
    )
    .expect_err("a duplicate column is refused")
    .to_string();
    assert!(error.contains("Duplicate column name 'note'"), "{error}");
    match plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN IF NOT EXISTS note VARCHAR(8)"),
        300,
    )
    .expect("the no-op plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("IF NOT EXISTS over an existing column must be a no-op"),
    }

    // Go `generateOriginDefaultValue`: a declared default becomes the origin
    // default existing rows report; NOT NULL without one stamps the type's
    // zero value. Neither rewrites a row.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN flag BIGINT DEFAULT 7",
        400,
    );
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let flag = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"]["O"] == "flag")
        .expect("the defaulted column is stored");
    assert_eq!(flag["origin_default"], "7");
    assert_eq!(flag["default"], "7");

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN zeroed BIGINT NOT NULL",
        500,
    );
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let zeroed = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"]["O"] == "zeroed")
        .expect("the NOT NULL column is stored");
    assert_eq!(zeroed["origin_default"], "0", "the type's zero value");
    assert_eq!(zeroed["default"], serde_json::Value::Null);

    // Go's clock arm: the DECLARED default stays the word for every later
    // INSERT, while the origin default is stamped ONCE at DDL time.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN ts DATETIME DEFAULT CURRENT_TIMESTAMP",
        600,
    );
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let ts = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"]["O"] == "ts")
        .expect("the clock column is stored");
    assert_eq!(ts["default"], "CURRENT_TIMESTAMP");
    let stamped = ts["origin_default"].as_str().expect("a stamped instant");
    assert!(
        stamped.len() == 19 && stamped.contains('-') && stamped.contains(':'),
        "the origin default is one wall-clock instant, got {stamped}"
    );
}

/// Go `onTruncateTable`: the schema survives under a FRESH table id, the old
/// id's rows become unreachable, and the auto-id counters restart because the
/// allocator keys travel with the id.
#[test]
fn truncate_reallocates_the_table_id_and_restarts_the_allocators() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let old_id = write.created_id.expect("CREATE TABLE allocates an id");
    // A used allocator, which the truncate must delete.
    store.put(key::auto_table_id_kv_key(112, old_id), b"30".to_vec());

    let write = plan(&mut store, "TRUNCATE TABLE u6.t", 200);
    let new_id = write.diff.table_id;
    assert_ne!(new_id, old_id, "the id is fresh, never reused");
    assert_eq!(write.diff.old_table_id, old_id);
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_TRUNCATE_TABLE
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, new_id)))
            .expect("the truncated table is stored");
    assert_eq!(stored["id"], new_id);
    assert!(
        write
            .mutations
            .iter()
            .any(|mutation| mutation.key() == key::table_kv_key(112, old_id).as_slice()
                && matches!(mutation.kind(), OptimisticMutationKind::MetaDelete)),
        "the old table key is deleted"
    );
    assert!(
        write
            .mutations
            .iter()
            .any(|mutation| mutation.key() == key::auto_table_id_kv_key(112, old_id).as_slice()
                && matches!(mutation.kind(), OptimisticMutationKind::MetaDelete)),
        "the observed allocator is deleted with the old id"
    );
}

/// Go `isDroppableColumn` + `onDropColumn`: offsets close over the gap, a
/// single-column secondary index goes with its column, and the three
/// refusals answer Go's exact messages.
#[test]
fn drop_column_shifts_offsets_and_takes_its_single_column_index() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT, c BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("an id");
    let write = plan(&mut store, "CREATE INDEX idx_a ON u6.t (a)", 150);
    apply(&mut store, &write);

    let write = plan(&mut store, "ALTER TABLE u6.t DROP COLUMN b", 200);
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("the altered table is stored");
    let columns = stored["cols"].as_array().expect("columns");
    let names: Vec<_> = columns.iter().map(|c| c["name"]["O"].as_str().unwrap()).collect();
    assert_eq!(names, ["id", "a", "c"]);
    let offsets: Vec<_> = columns.iter().map(|c| c["offset"].as_i64().unwrap()).collect();
    assert_eq!(offsets, [0, 1, 2], "the gap closes");
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_DROP_COLUMN
    );

    // The single-column index on `a` goes with `a`.
    let write = plan(&mut store, "ALTER TABLE u6.t DROP COLUMN a", 300);
    apply(&mut store, &write);
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    // `[]`, not `null`: Go removes the entry from a non-nil slice, and an
    // emptied non-nil slice marshals as an empty array — unlike the builder's
    // untouched nil slice a fresh CREATE TABLE stores. Both states are pinned.
    assert_eq!(
        stored["index_info"],
        serde_json::json!([]),
        "listIndicesWithColumn drops idx_a with its column"
    );

    for (sql, message) in [
        (
            "ALTER TABLE u6.t DROP COLUMN id",
            "Unsupported drop integer primary key",
        ),
        (
            "ALTER TABLE u6.t DROP COLUMN missing",
            "Can't DROP 'missing'; check that column/key exists",
        ),
    ] {
        let error = plan_ddl(&mut store, &statement(sql), 400)
            .expect_err("refused")
            .to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
}

/// Go's one ActionMultiSchemaChange job: the sub-actions fold over ONE
/// evolving TableInfo inside one transaction, in SQL order, so a later action
/// sees the earlier one's change and the table lands whole or not at all.
#[test]
fn a_multi_action_alter_folds_over_one_evolving_table() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("an id");

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN c BIGINT DEFAULT 5, DROP COLUMN a, ADD COLUMN d BIGINT",
        200,
    );
    apply(&mut store, &write);
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    let names: Vec<_> = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"]["O"].as_str().unwrap())
        .collect();
    // `c` appended after `b`, then `a` dropped closing the gap, then `d`
    // appended after the shift — SQL order, one evolving table.
    assert_eq!(names, ["id", "b", "c", "d"]);
    let offsets: Vec<_> = stored["cols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["offset"].as_i64().unwrap())
        .collect();
    assert_eq!(offsets, [0, 1, 2, 3]);

    // One failing sub-action fails the whole bundle: nothing is staged.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN e BIGINT, DROP COLUMN missing"),
        300,
    )
    .expect_err("the failing drop fails the bundle")
    .to_string();
    assert!(error.contains("Can't DROP 'missing'"), "{error}");

    // Every sub-action a no-op is the statement already satisfied.
    match plan_ddl(
        &mut store,
        &statement(
            "ALTER TABLE u6.t ADD COLUMN IF NOT EXISTS c BIGINT, DROP COLUMN IF EXISTS missing",
        ),
        400,
    )
    .expect("the all-no-op bundle plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("an all-no-op bundle must publish nothing"),
    }
}

/// A bundle mixing a column change with ONE index change folds over the same
/// evolving table: the index resolves against the bundle-added column, the
/// backfill walks existing rows against the evolved columns, and a second
/// index action in one bundle is refused by name (one backfill per catalog
/// transaction).
#[test]
fn a_column_and_index_bundle_folds_and_backfills_together() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("an id");

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN c BIGINT DEFAULT 5, ADD INDEX idx_c (c)",
        200,
    );
    apply(&mut store, &write);
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE
    );
    let backfill = write.backfill.as_ref().expect("the index change backfills");
    assert!(backfill.add);
    assert_eq!(backfill.index.read().name.original(), "idx_c");
    assert_eq!(
        backfill.index.read().columns.iter_deref().next().unwrap().read().offset,
        2,
        "the index column resolves against the bundle-added column's offset"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    assert_eq!(stored["index_info"].as_array().unwrap().len(), 1);

    // The drop direction: the backfill's table still CARRIES the index the
    // walk removes, while the stored table no longer names it.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN d BIGINT, DROP INDEX idx_c",
        300,
    );
    apply(&mut store, &write);
    let backfill = write.backfill.as_ref().expect("the removal walks");
    assert!(!backfill.add);
    assert!(
        backfill
            .table
            .indices
            .iter_deref()
            .any(|index| index.read().name.original() == "idx_c"),
        "the walk's table still carries the dropped index"
    );
    let stored: serde_json::Value =
        serde_json::from_slice(stored_value(&write, &key::table_kv_key(112, table_id)))
            .expect("stored");
    assert_eq!(stored["index_info"].as_array().unwrap().len(), 0);

    // Two index actions cannot share one backfill slot.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD INDEX i1 (v), ADD INDEX i2 (c)"),
        400,
    )
    .expect_err("two index changes refuse")
    .to_string();
    assert!(error.contains("at most one index change"), "{error}");
}

/// Go `types.CheckModifyTypeCompatible` + `needReorgToChange`
/// (`pkg/types/field_type.go:1476,1535`). Upstream coverage of this decision
/// is testkit-bound — `pkg/ddl/tests/serial/serial_test.go:1261` drives it
/// through `alter table`, and its comment records the exact contract these
/// cases pin: `b int` -> `bigint` succeeds while `a bigint` -> `int` fails
/// with "length 11 is less than origin 20".
#[test]
fn a_modify_column_reorganizes_exactly_where_go_says_it_must() {
    fn refuse(store: &mut MetaStore, sql: &str, start_ts: u64) -> String {
        plan_ddl(store, &statement(sql), start_ts)
            .expect_err("a reorganizing modify must be refused")
            .to_string()
    }

    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE widen (id BIGINT PRIMARY KEY, small INT, big BIGINT, \
         name VARCHAR(10), money DECIMAL(10,2))",
        200,
    );
    apply(&mut store, &create);

    // Integer widening is metadata only: Go compares the types' DEFAULT
    // display widths, so INT(11) -> BIGINT(20) grows and costs nothing.
    let widened = plan(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN small BIGINT",
        201,
    );
    assert!(!widened.mutations.is_empty(), "the widening is planned");

    // The reverse narrows, and carries Go's own reason verbatim.
    let narrowed = refuse(&mut store, "ALTER TABLE widen MODIFY COLUMN big INT", 202);
    assert!(
        narrowed.contains("length 11 is less than origin 20"),
        "{narrowed}"
    );

    // A string widening is free; a shortening is not.
    let longer = plan(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN name VARCHAR(40)",
        203,
    );
    assert!(!longer.mutations.is_empty(), "the longer varchar is planned");
    let shorter = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN name VARCHAR(4)",
        204,
    );
    assert!(
        shorter.contains("length 4 is less than origin 10"),
        "{shorter}"
    );

    // Crossing families is never free.
    let crossed = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN small DATETIME",
        205,
    );
    assert!(crossed.contains("not match origin"), "{crossed}");

    // Go: char <-> varchar always reorganizes, in either direction.
    let recast = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN name CHAR(40)",
        206,
    );
    assert!(
        recast.contains("conversion between char and varchar string"),
        "{recast}"
    );

    // Sign is a rewrite of every stored row.
    let resigned = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN small INT UNSIGNED",
        207,
    );
    assert!(
        resigned.contains("can't change unsigned integer to signed or vice versa"),
        "{resigned}"
    );

    // A decimal must match exactly in flen, scale and sign.
    let rescaled = refuse(
        &mut store,
        "ALTER TABLE widen MODIFY COLUMN money DECIMAL(10,3)",
        208,
    );
    assert!(
        rescaled.contains("decimal change from decimal(10, 2) to decimal(10, 3)"),
        "{rescaled}"
    );
}

/// A prefix-length index must be refused wherever it is spelled. The
/// standalone `CREATE INDEX` and the `ALTER TABLE ... ADD INDEX` spellings
/// have always refused it, because this node's own catalog loader drops a
/// table carrying one; the INLINE spelling inside `CREATE TABLE` did not,
/// so the statement reported success and the table then did not exist.
#[test]
fn a_prefix_index_is_refused_in_every_spelling() {
    let expected = "a prefix-length index is not supported by this node";

    // All three spellings refuse at admission, before any mutation.
    for sql in [
        "CREATE TABLE pfx (id BIGINT PRIMARY KEY, c VARCHAR(20), INDEX px (c(5)))",
        "CREATE TABLE pfx (id BIGINT PRIMARY KEY, c VARCHAR(20), UNIQUE KEY ux (c(5)))",
        "CREATE INDEX px ON pfx2 (c(5))",
        "ALTER TABLE pfx2 ADD INDEX px (c(5))",
    ] {
        let (code, reason) = refusal_with_code(sql);
        assert_eq!(code, 8200, "{sql}");
        assert!(reason.contains(expected), "{sql}: {reason}");
    }

    // The same shapes without a prefix are admitted, so the guard refuses
    // the prefix rather than the index.
    let mut store = bootstrapped();
    let create = plan(
        &mut store,
        "CREATE TABLE pfx2 (id BIGINT PRIMARY KEY, c VARCHAR(20), INDEX cx (c))",
        300,
    );
    assert!(!create.mutations.is_empty(), "a plain inline index plans");
    apply(&mut store, &create);
    let plain = plan(&mut store, "CREATE INDEX cx2 ON pfx2 (c)", 301);
    assert!(!plain.mutations.is_empty(), "a plain index still plans");
}

/// A resolved view definition as the route would hand it over, for the
/// plan-arm pins below — the RESOLUTION itself is the session tier's
/// `resolve_view_definition`, tested beside `run_create_view_in`.
fn view_statement(schema: &str, name: &str, or_replace: bool) -> DdlStatement {
    let view = tidb_executor::ViewDef {
        name: name.to_owned(),
        columns: vec![(
            "a".to_owned(),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        )],
        select_sql: format!("SELECT `t`.`a` AS `a` FROM `{schema}`.`t`"),
        definer_user: "root".to_owned(),
        definer_host: "%".to_owned(),
        character_set_client: "utf8mb4".to_owned(),
        collation_connection: "utf8mb4_bin".to_owned(),
        algorithm: "UNDEFINED".to_owned(),
        security: "DEFINER".to_owned(),
        check_option: "CASCADED".to_owned(),
    };
    DdlStatement::CreateView {
        schema: schema.to_owned(),
        name: name.to_owned(),
        or_replace,
        info: Box::new(tidb_exec::cluster_ddl::build_view_table_info(name, &view)),
    }
}

#[test]
fn a_create_view_publishes_a_view_table_info() {
    // Go `onCreateView` (`ddl/create_table.go:371`): the finished TableInfo
    // goes to `createTableOrViewWithCheck` under ACTION_CREATE_VIEW; the
    // published metadata carries the view half, the resolved columns, and
    // the creator's client charset/collation in Charset/Collate — which is
    // where SHOW CREATE VIEW reads them back (`executor/show.go`).
    let mut store = bootstrapped();
    let create_t = plan(&mut store, "CREATE TABLE t (a BIGINT PRIMARY KEY)", 101);
    apply(&mut store, &create_t);

    let write = match plan_ddl(&mut store, &view_statement("u6", "v", false), 102)
        .expect("a fresh view plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write: {detail}"),
    };
    assert_eq!(
        write.diff.action_type,
        tidb_model::ActionType::ACTION_CREATE_VIEW
    );
    let put = write
        .mutations
        .iter()
        .find(|m| m.kind() == OptimisticMutationKind::MetaPut && m.value().starts_with(b"{"))
        .expect("the view's TableInfo is written");
    let info: tidb_model::TableInfo =
        serde_json::from_slice(put.value()).expect("the published value is a TableInfo");
    let view = info.view.as_ref().expect("the view half rides along").read();
    assert_eq!(view.select_stmt, "SELECT `t`.`a` AS `a` FROM `u6`.`t`");
    assert_eq!(info.charset, "utf8mb4");
    assert_eq!(info.collate, "utf8mb4_bin");
    assert_eq!(info.columns.len(), 1);
    apply(&mut store, &write);

    // The same name again without OR REPLACE is Go's ErrTableExists.
    let error = plan_ddl(&mut store, &view_statement("u6", "v", false), 103)
        .expect_err("a duplicate view name refuses");
    assert!(format!("{error:?}").contains("TableExists"), "{error:?}");

    // OR REPLACE drops the old id and creates a fresh one.
    let replace = match plan_ddl(&mut store, &view_statement("u6", "v", true), 104)
        .expect("OR REPLACE plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write: {detail}"),
    };
    assert!(
        replace
            .mutations
            .iter()
            .any(|m| m.kind() == OptimisticMutationKind::MetaDelete),
        "the old view's key is deleted"
    );
    apply(&mut store, &replace);

    // DROP VIEW deletes it under ACTION_DROP_VIEW; a base table under the
    // same statement is Go's ErrWrongObject; a missing name without
    // IF EXISTS is Go's Unknown table.
    let drop = match plan_ddl(
        &mut store,
        &DdlStatement::DropView {
            names: vec![("u6".to_owned(), "v".to_owned())],
            if_exists: false,
        },
        105,
    )
    .expect("the drop plans")
    {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail, .. } => panic!("expected a write: {detail}"),
    };
    assert_eq!(drop.diff.action_type, tidb_model::ActionType::ACTION_DROP_VIEW);
    apply(&mut store, &drop);

    let wrong = plan_ddl(
        &mut store,
        &DdlStatement::DropView {
            names: vec![("u6".to_owned(), "t".to_owned())],
            if_exists: true,
        },
        106,
    )
    .expect_err("a base table refuses DROP VIEW even under IF EXISTS");
    assert!(format!("{wrong:?}").contains("not a VIEW"), "{wrong:?}");

    let missing = plan_ddl(
        &mut store,
        &DdlStatement::DropView {
            names: vec![("u6".to_owned(), "gone".to_owned())],
            if_exists: false,
        },
        107,
    )
    .expect_err("a missing view without IF EXISTS refuses");
    assert!(format!("{missing:?}").contains("Unknown table"), "{missing:?}");
}

#[test]
fn a_check_constraint_is_ignored_with_gos_warning() {
    // Go's DEFAULT (`tidb_enable_check_constraint` off): both CHECK
    // spellings — the column option (`ddl/add_column.go:577`) and the table
    // constraint (`ddl/create_table.go:1470`) — warn
    // `tidb_enable_check_constraint is off` and are IGNORED; the table
    // creates and enforces nothing. Probe 24 caught this node refusing
    // where every default-configured Go server accepts.
    let context = tidb_executor::StmtContext::for_query();
    let parsed = tidb_parser::parse(
        "CREATE TABLE ck (v INT CHECK (v > 0), CONSTRAINT big CHECK (v < 100))",
    )
    .expect("parses");
    let statement = lower_ddl_with_context(&parsed, "u6", &context)
        .expect("admitted")
        .expect("a catalog change");
    let DdlStatement::CreateTable { build, .. } = statement else {
        panic!("a CreateTable");
    };
    assert!(
        build.template().view.is_none() && build.template().columns.len() == 1,
        "one plain column, no constraint metadata"
    );
    assert_eq!(
        context.warning_count(),
        2,
        "one warning per ignored CHECK spelling"
    );
}

/// Go `onAddColumn`'s write-reorganization step: the column is appended,
/// then `MoveColumnInfo` puts it where `FIRST`/`AFTER` asked -- renumbering
/// every offset it passed and re-pointing every INDEX column that addressed
/// one of them (`meta/model/table.go:434`).
///
/// The stored rows are untouched: a row's values are keyed by column id,
/// not by position, so what moves is only the descriptor readers resolve
/// names through. The column ID therefore stays at its allocation order
/// while the OFFSET follows the request.
#[test]
fn add_column_first_and_after_move_offsets_and_repoint_indexes() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT, KEY kb(b))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    // AFTER lands between `a` and `b`, so `b` shifts right by one and the
    // index on `b` must follow it.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN mid BIGINT AFTER a",
        200,
    );
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["id", "a", "mid", "b"]);
    for (position, column) in columns.iter().enumerate() {
        assert_eq!(column["offset"], position, "offsets are renumbered");
    }
    // The id keeps its allocation order even though the offset moved.
    assert_eq!(columns[2]["id"], 4, "mid was allocated after id/a/b");
    let index_column = &stored["index_info"][0]["idx_cols"][0];
    assert_eq!(index_column["name"]["O"], "b");
    assert_eq!(index_column["offset"], 3, "the index follows the column it names");

    // FIRST pushes everything right by one, index included.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t ADD COLUMN head BIGINT FIRST",
        300,
    );
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["head", "id", "a", "mid", "b"]);
    for (position, column) in columns.iter().enumerate() {
        assert_eq!(column["offset"], position);
    }
    assert_eq!(
        stored["index_info"][0]["idx_cols"][0]["offset"], 4,
        "the index follows again"
    );

    // Go `LocateOffsetToMove`'s AFTER arm answers ErrColumnNotExists (1054)
    // for a column that is not there.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ADD COLUMN late BIGINT AFTER nosuch"),
        400,
    )
    .expect_err("AFTER an unknown column is refused")
    .to_string();
    assert!(error.contains("Unknown column 'nosuch'"), "{error}");
}

/// Go `modify_column.go:704`: a MODIFY/CHANGE may also MOVE the column,
/// and the destination is located against the column's CURRENT offset --
/// unlike ADD COLUMN, which appends first and locates against that.
///
/// `MODIFY b AFTER b` names the column as its own anchor, which Go answers
/// as `ErrColumnNotExists` on that column rather than as a no-op
/// (`modify_column.go:700`).
#[test]
fn modify_column_moves_the_column_and_refuses_a_self_anchor() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT, KEY kb(b))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let write = plan(&mut store, "ALTER TABLE u6.t MODIFY b BIGINT FIRST", 200);
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let columns = stored["cols"].as_array().expect("columns array");
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["b", "id", "a"]);
    for (position, column) in columns.iter().enumerate() {
        assert_eq!(column["offset"], position);
    }
    assert_eq!(
        stored["index_info"][0]["idx_cols"][0]["offset"], 0,
        "the index follows the column it names"
    );

    // A CHANGE renames and moves in one statement.
    let write = plan(&mut store, "ALTER TABLE u6.t CHANGE a a2 BIGINT AFTER b", 300);
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    let names: Vec<&str> = stored["cols"]
        .as_array()
        .expect("columns array")
        .iter()
        .map(|column| column["name"]["O"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["b", "a2", "id"]);

    // Go's self-anchor rule, and its 1054 code.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t MODIFY b BIGINT AFTER b"),
        400,
    )
    .expect_err("a self-anchored MODIFY is refused");
    assert!(
        matches!(error, DdlPlanError::UnknownColumn { ref column, .. } if column == "b"),
        "{error:?}"
    );
    assert!(error.to_string().contains("Unknown column 'b'"), "{error}");
}

/// Go `onAlterIndexVisibility` (`ddl/index.go:720`): `ALTER TABLE ...
/// ALTER INDEX <i> VISIBLE|INVISIBLE` is metadata only -- the index is
/// still maintained by writes, it is only hidden from the optimizer.
///
/// Three of Go's rules ride with it: the index must exist AND be public,
/// else `ErrKeyNotExists` (1176, not DROP INDEX's 1091); a visibility that
/// already matches is an early return that spends no schema version; and
/// `setIndexVisibility` walks EVERY index of the matching name rather than
/// stopping at the first.
#[test]
fn alter_index_visibility_toggles_and_refuses_a_missing_index() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, KEY ia(a))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let invisible_of = |write: &tidb_exec::cluster_ddl::DdlWrite| {
        let stored = stored_table(write, table_id);
        stored["index_info"]
            .as_array()
            .expect("index array")
            .iter()
            .find(|index| index["idx_name"]["O"] == "ia")
            .expect("the index is there")["is_invisible"]
            .clone()
    };

    let write = plan(&mut store, "ALTER TABLE u6.t ALTER INDEX ia INVISIBLE", 200);
    apply(&mut store, &write);
    assert_eq!(invisible_of(&write), serde_json::json!(true));

    let write = plan(&mut store, "ALTER TABLE u6.t ALTER INDEX ia VISIBLE", 300);
    apply(&mut store, &write);
    assert_eq!(invisible_of(&write), serde_json::json!(false));

    // Go's early return: already visible, so the job finishes without
    // touching the table and no schema version is spent.
    match plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ALTER INDEX ia VISIBLE"),
        400,
    )
    .expect("an already-satisfied visibility plans")
    {
        DdlPlan::AlreadySatisfied { detail, .. } => {
            assert!(detail.contains("already visible"), "{detail}");
        }
        DdlPlan::Write(_) => panic!("a no-op visibility must publish nothing"),
    }

    // A missing index is Go's ErrKeyNotExists, not DROP INDEX's 1091.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ALTER INDEX nosuch INVISIBLE"),
        500,
    )
    .expect_err("a missing index is refused");
    assert!(
        matches!(error, DdlPlanError::KeyNotExists { ref index, .. } if index == "nosuch"),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("Key 'nosuch' doesn't exist in table 't'"),
        "{error}"
    );
}

/// `PARTITION BY` reaches the stored `TableInfo` rather than being refused,
/// and it carries Go's own stored shape: the restored expression, the
/// definitions in written order, and the `Enable` flag that makes
/// `GetPartitionInfo` return it at all.
#[test]
fn cluster_create_persists_a_partition_clause() {
    let DdlStatement::CreateTable { build, .. } = statement(
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 2",
    ) else {
        panic!("the fixture is CREATE TABLE");
    };
    let table = build.template();
    let partition = table
        .partition
        .as_ref()
        .expect("the clause reached the stored table")
        .read();
    assert_eq!(partition.partition_type, tidb_ast::PartitionType::HASH);
    assert!(
        partition.enable,
        "Go's GetPartitionInfo returns nil for metadata that is not enabled, \
         so a table stored with Enable false is not partitioned at all"
    );
    assert_eq!(partition.expr, "`id`");
    assert_eq!(partition.num, 2);
    let definitions = partition.definitions.snapshot();
    assert_eq!(
        definitions
            .iter()
            .map(|definition| definition.name.original().to_owned())
            .collect::<Vec<_>>(),
        vec!["p0".to_owned(), "p1".to_owned()]
    );
    // The builder leaves the physical ids for the writer: Go allocates them
    // at job submission, one per definition after the table's own.
    assert!(definitions.iter().all(|definition| definition.id == 0));
}

/// A RANGE clause stores its bounds as the TEXT Go stores, with `MAXVALUE`
/// kept as that literal word rather than folded into a number.
#[test]
fn cluster_create_persists_range_bounds_as_go_spells_them() {
    let DdlStatement::CreateTable { build, .. } = statement(
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY) PARTITION BY RANGE (id) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    ) else {
        panic!("the fixture is CREATE TABLE");
    };
    let table = build.template();
    let partition = table
        .partition
        .as_ref()
        .expect("the clause reached the stored table")
        .read();
    let bounds = partition
        .definitions
        .snapshot()
        .iter()
        .map(|definition| definition.less_than.snapshot())
        .collect::<Vec<_>>();
    assert_eq!(
        bounds,
        vec![vec!["10".to_owned()], vec!["MAXVALUE".to_owned()]]
    );
}
