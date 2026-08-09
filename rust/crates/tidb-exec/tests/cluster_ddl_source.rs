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
struct MetaStore {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
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
fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    store.put(key::next_global_id_kv_key(), b"116".to_vec());
    store.put(key::schema_version_kv_key(), b"60".to_vec());
    store.put(
        key::database_kv_key(112),
        br#"{"id":112,"db_name":{"O":"u6","L":"u6"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#.to_vec(),
    );
    store
}

fn statement(sql: &str) -> DdlStatement {
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    lower_ddl(&parsed, "u6")
        .unwrap_or_else(|error| panic!("the fixture SQL is admitted: {sql}: {error:?}"))
        .expect("the fixture SQL is a catalog change")
}

fn stored_default_bytes(sql: &str) -> Vec<u8> {
    let DdlStatement::CreateTable { template, .. } = statement(sql) else {
        panic!("the fixture is not CREATE TABLE: {sql}");
    };
    let column = template
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

fn plan(store: &mut MetaStore, sql: &str, start_ts: u64) -> tidb_exec::cluster_ddl::DdlWrite {
    match plan_ddl(store, &statement(sql), start_ts).expect("the fixture plans") {
        DdlPlan::Write(write) => *write,
        DdlPlan::AlreadySatisfied { detail } => {
            panic!("expected a write, got already-satisfied: {detail}")
        }
    }
}

/// Applies a planned write set, modelling its transaction having committed.
fn apply(store: &mut MetaStore, write: &tidb_exec::cluster_ddl::DdlWrite) {
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

fn stored_value<'write>(
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
fn a_table_constraint_primary_key_is_the_same_clustered_handle_as_an_inline_one() {
    let template = |sql: &str| {
        let DdlStatement::CreateTable { template, .. } = statement(sql) else {
            panic!("a CREATE TABLE");
        };
        template
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
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, CHECK (v > 0))",
            "CHECK and FOREIGN KEY constraints are not supported",
        ),
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
        (
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) PARTITION BY HASH (id) PARTITIONS 2",
            "PARTITION BY is not supported",
        ),
        (
            "CREATE TEMPORARY TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
            "TEMPORARY is not supported",
        ),
        (
            "CREATE TABLE u6.t LIKE u6.other",
            "... LIKE is not supported",
        ),
        (
            "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, ID BIGINT NOT NULL)",
            "declares column `ID` twice",
        ),
        (
            "DROP TABLE u6.a, u6.b",
            "DROP TABLE names exactly one table on this node",
        ),
        (
            "CREATE DATABASE d CHARACTER SET utf8mb4",
            "CREATE DATABASE options are not supported",
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
    let DdlStatement::CreateTable { template, .. } =
        statement("CREATE TABLE u6.t (a BIT(9) DEFAULT b'1')")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    let column = template
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
    let DdlStatement::CreateTable { template, .. } =
        lower_ddl_with_context(&parsed, "u6", &context)
            .expect("the timestamp default is admitted")
            .expect("the statement is cluster DDL")
    else {
        panic!("the fixture is CREATE TABLE");
    };
    let column_handle = template.columns.get(0).expect("one column");
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
        let DdlStatement::CreateTable { template, .. } =
            lower_ddl_with_context(&parsed, "u6", &context)
                .expect("the expression default is admitted")
                .expect("the statement is cluster DDL")
        else {
            panic!("the fixture is CREATE TABLE");
        };
        let column_handle = template.columns.get(0).expect("one column");
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
        // `ALTER TABLE ... ADD INDEX` reaches Go's `ActionAddIndex` too, but
        // this module admits only the `CREATE INDEX` spelling: an `ALTER` may
        // carry several actions in one statement, and half-applying them is
        // not something one meta transaction can take back.
        "ALTER TABLE u6.t ADD COLUMN c BIGINT NOT NULL",
        "ALTER TABLE u6.t ADD INDEX i (v)",
    ] {
        let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
        assert!(
            lower_ddl(&parsed, "u6").expect("no refusal").is_none(),
            "`{sql}` is not a catalog change this module owns"
        );
    }
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
    let DdlStatement::CreateTable { template, .. } = lower_ddl(&parsed, "sbtest")
        .expect("admitted")
        .expect("a catalog change")
    else {
        panic!("a CREATE TABLE");
    };
    assert!(
        template
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
    assert!(!template.sep_auto_inc());
    assert_eq!(
        tidb_exec::cluster_auto_id::auto_id_key_for(7, &template),
        tidb_meta::key::auto_table_id_kv_key(7, template.id),
    );
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
fn stored_table(write: &tidb_exec::cluster_ddl::DdlWrite, table_id: i64) -> serde_json::Value {
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
            DdlPlan::AlreadySatisfied { detail } => {
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
    assert_eq!(refused.to_string(), "index nosuch doesn't exist");
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
