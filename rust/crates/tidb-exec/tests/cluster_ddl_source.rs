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
use tidb_exec::cluster_ddl::{lower_ddl, plan_ddl, DdlPlan, DdlPlanError, DdlStatement};
use tidb_meta::{key, value};
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
        .expect("the fixture SQL is admitted")
        .expect("the fixture SQL is a catalog change")
}

fn refusal(sql: &str) -> String {
    let parsed = tidb_parser::parse(sql).expect("the fixture SQL parses");
    lower_ddl(&parsed, "u6")
        .expect_err("this shape must be refused before any mutation")
        .reason
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
    assert!(inline.columns[0]
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY));
    assert!(!inline.columns[1]
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
        let stored_diff = value::parse_schema_diff(stored_value(&write, &key::schema_diff_kv_key(61)))
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
    assert_eq!(
        stored_value(&write, &key::next_global_id_kv_key()),
        b"117"
    );
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
    assert!(
        tidb_exec::cluster_catalog::load_cluster_catalog(&mut store)
            .expect("the catalog loads")
            .databases
            .is_empty()
    );
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
        // AUTO_INCREMENT and table options.
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY AUTO_INCREMENT, v BIGINT NOT NULL)",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) ENGINE=InnoDB",
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
fn a_statement_this_module_does_not_own_is_left_to_its_own_path() {
    for sql in [
        "SELECT 1",
        "INSERT INTO u6.t VALUES (1, 2)",
        "ALTER TABLE u6.t ADD COLUMN c BIGINT NOT NULL",
        "CREATE INDEX i ON u6.t (v)",
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
