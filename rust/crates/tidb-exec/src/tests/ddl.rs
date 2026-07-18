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
// See the License for the specific language governing permissions and
// limitations under the License.

//! DDL coordination plus `CREATE TABLE`/`ALTER TABLE` tests against the live
//! catalog. `pkg/ddl/table.go` transitions have their own `table_ddl` owner.

use super::*;

#[test]
fn alter_instance_and_range_are_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table cluster_boundary (id int)"),
        "OK"
    );
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(
        step(&mut db, "insert into cluster_boundary values (1)"),
        "OK"
    );

    for (sql, expected) in [
        (
            "alter instance reload tls no rollback on error",
            "Unsupported(\"ALTER INSTANCE\")",
        ),
        (
            "alter range global placement policy default",
            "Unsupported(\"ALTER RANGE\")",
        ),
    ] {
        assert_eq!(step(&mut db, sql), expected, "source SQL: {sql}");
        assert!(db.transaction.is_active(), "source SQL: {sql}");
    }

    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(step(&mut db, "select id from cluster_boundary"), "RS:");
}

#[test]
fn alter_database_is_unsupported_before_ddl_side_effects() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "alter database db1 placement policy = pp1"),
        "Unsupported(\"ALTER DATABASE\")"
    );
}

#[test]
fn resource_group_ddl_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table rg_boundary (id int)"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(step(&mut db, "insert into rg_boundary values (1)"), "OK");

    for (sql, expected) in [
        (
            "create resource group rg1 ru_per_sec=100 query_limit=(ru=10 action=kill)",
            "Unsupported(\"CREATE RESOURCE GROUP\")",
        ),
        (
            "alter resource group rg1 background=(task_types='ddl')",
            "Unsupported(\"ALTER RESOURCE GROUP\")",
        ),
        (
            "drop resource group if exists rg1",
            "Unsupported(\"DROP RESOURCE GROUP\")",
        ),
    ] {
        assert_eq!(step(&mut db, sql), expected);
    }

    // None of the cluster-level commands may consume the active transaction
    // snapshot. Rollback must still remove the pending row.
    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(step(&mut db, "select id from rg_boundary"), "RS:");
}

#[test]
fn masking_policy_ddl_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table masking_boundary (id int)"),
        "OK"
    );
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(
        step(&mut db, "insert into masking_boundary values (1)"),
        "OK"
    );

    for (sql, expected) in [
        (
            "create masking policy p on masking_boundary(id) as id",
            "Unsupported(\"CREATE MASKING POLICY\")",
        ),
        (
            "alter table masking_boundary add masking policy p on (id) as id",
            "Unsupported(\"ALTER TABLE MASKING POLICY\")",
        ),
        (
            "alter table masking_boundary modify masking policy p set restrict on none",
            "Unsupported(\"ALTER TABLE MASKING POLICY\")",
        ),
        (
            "alter table masking_boundary enable masking policy p",
            "Unsupported(\"ALTER TABLE MASKING POLICY\")",
        ),
        (
            "alter table masking_boundary disable masking policy p",
            "Unsupported(\"ALTER TABLE MASKING POLICY\")",
        ),
        (
            "alter table masking_boundary drop masking policy p",
            "Unsupported(\"ALTER TABLE MASKING POLICY\")",
        ),
    ] {
        assert_eq!(step(&mut db, sql), expected, "source SQL: {sql}");
        assert!(db.transaction.is_active(), "source SQL: {sql}");
    }

    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(step(&mut db, "select id from masking_boundary"), "RS:");
}

#[test]
fn ddl_envelope_dispatches_catalog_mutations() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table ddl_envelope (a int)"), "OK");
    assert_eq!(step(&mut db, "alter table ddl_envelope add b int"), "OK");
    assert_eq!(
        step(&mut db, "rename table ddl_envelope to ddl_envelope_renamed"),
        "OK"
    );
    assert_eq!(step(&mut db, "truncate table ddl_envelope_renamed"), "OK");
    assert_eq!(step(&mut db, "drop table ddl_envelope_renamed"), "OK");
}

#[test]
fn auto_increment_schema_has_source_lifecycle_and_type_boundaries() {
    let mut db = Database::new();
    assert!(step(
        &mut db,
        "create table auto_varchar (id varchar(8) auto_increment primary key)",
    )
    .starts_with("Unsupported(\"AUTO_INCREMENT column type\")"));
    assert!(step(
        &mut db,
        "create table auto_two (a int auto_increment, b int auto_increment)",
    )
    .starts_with("Unsupported(\"multiple AUTO_INCREMENT columns\")"));

    step(
        &mut db,
        "create table auto_life (id int auto_increment primary key, v int) auto_increment=100",
    );
    step(&mut db, "insert into auto_life (v) values (1)");
    step(&mut db, "truncate table auto_life");
    step(&mut db, "insert into auto_life (v) values (2)");
    assert_eq!(step(&mut db, "select id, v from auto_life"), "RS:1|2");
    step(&mut db, "rename table auto_life to auto_life_renamed");
    step(&mut db, "insert into auto_life_renamed (v) values (3)");
    assert_eq!(
        step(&mut db, "select id, v from auto_life_renamed order by id"),
        "RS:1|2;2|3"
    );
    step(&mut db, "drop table auto_life_renamed");
    step(
        &mut db,
        "create table auto_life_renamed (id int auto_increment primary key, v int)",
    );
    step(&mut db, "insert into auto_life_renamed (v) values (4)");
    assert_eq!(
        step(&mut db, "select id, v from auto_life_renamed"),
        "RS:1|4"
    );

    // CREATE TABLE carries its AUTO_INCREMENT option through Go's legacy
    // signed AutoIncID field. A raw UInt64 spelling above MaxInt64 therefore
    // starts normally at 1, rather than becoming an unsigned seed.
    step(
        &mut db,
        "create table auto_large_option (id bigint unsigned auto_increment primary key)
         auto_increment=18446744073709551615",
    );
    step(&mut db, "insert into auto_large_option values ()");
    assert_eq!(step(&mut db, "select id from auto_large_option"), "RS:1");
}

#[test]
fn drop_index_is_unsupported_before_catalog_or_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table drop_index_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into drop_index_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("drop index idx_id on drop_index_boundary")
                .expect("parse DROP INDEX")
        ),
        Err(ExecError::Unsupported("DROP INDEX"))
    ));
    assert!(db.transaction.is_active());

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from drop_index_boundary"), "RS:");
}

#[test]
fn table_locks_are_unsupported_before_catalog_or_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table lock_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into lock_boundary values (1)");

    for (sql, operation) in [
        ("lock table lock_boundary write", "LOCK TABLES"),
        ("unlock tables", "UNLOCK TABLES"),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse table-lock statement")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
        assert!(
            db.transaction.is_active(),
            "{operation} must not commit the transaction"
        );
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from lock_boundary"), "RS:");
}

#[test]
fn split_region_is_unsupported_before_catalog_or_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table split_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into split_boundary values (1)");

    for sql in [
        "split table split_boundary by (10)",
        "split table split_boundary index idx by (10)",
        "alter table split_boundary split primary key between (0) and (10) regions 2",
        "alter table split_boundary split index idx between (0) and (10) regions 2",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse SPLIT statement")),
            Err(ExecError::Unsupported("SPLIT TABLE"))
                | Err(ExecError::Unsupported("ALTER TABLE SPLIT"))
        ));
        assert!(
            db.transaction.is_active(),
            "{sql} must not commit the transaction"
        );
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from split_boundary"), "RS:");
}

#[test]
fn json_columns_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table json_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into json_boundary values (1)");

    for sql in [
        "create table json_payload (doc json)",
        "alter table json_boundary add payload json",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse JSON column DDL")),
            Err(ExecError::Unsupported("JSON column type"))
        ));
    }

    // Neither rejected operation may implicitly commit or clear the active
    // snapshot. The pending write must still roll back.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from json_boundary"), "RS:");
}

#[test]
fn vector_columns_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table vector_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into vector_boundary values (1)");

    for sql in [
        "create table vector_payload (embedding vector(3))",
        "alter table vector_boundary add embedding vector(3)",
        "alter table vector_boundary modify id vector",
        "alter table vector_boundary change id embedding vector(16384)",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse VECTOR column DDL")),
            Err(ExecError::Unsupported("VECTOR column type"))
        ));
        assert!(db.transaction.is_active());
    }
    assert!(matches!(
        db.run(
            &tidb_parser::parse(
                "create table vector_index_payload (embedding vector(3), vector index ((vec_l2_distance(embedding))))",
            )
            .expect("parse VECTOR INDEX DDL"),
        ),
        Err(ExecError::Unsupported("VECTOR INDEX"))
    ));
    assert!(db.transaction.is_active());
    assert!(!db.tables.contains_key("vector_payload"));
    assert!(!db.tables.contains_key("vector_index_payload"));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from vector_boundary"), "RS:");
}

#[test]
fn prefix_primary_and_unique_keys_are_rejected_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table key_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into key_boundary values (1)");

    for sql in [
        "create table prefix_primary (name varchar(8), primary key(name(2)) clustered)",
        "create table prefix_unique (name varchar(8), unique key(name(2)))",
        "alter table key_boundary add unique key key_prefix(id desc)",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse prefix key DDL")),
            Err(ExecError::Unsupported(
                "PRIMARY/UNIQUE key prefix or direction"
            ))
        ));
        assert!(db.transaction.is_active());
    }
    assert!(!db.tables.contains_key("prefix_primary"));
    assert!(!db.tables.contains_key("prefix_unique"));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from key_boundary"), "RS:");
}

#[test]
fn column_level_reference_is_rejected_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table column_reference_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into column_reference_boundary values (1)");

    for sql in [
        "create table column_reference_payload (parent_id int references parent(id))",
        "alter table column_reference_boundary add parent_id int references parent(id)",
    ] {
        let statement = tidb_parser::parse(sql).expect("parse column-level REFERENCES DDL");
        assert!(matches!(
            db.run(&statement),
            Err(ExecError::Unsupported("column-level REFERENCES"))
        ));
        assert!(db.transaction.is_active());
    }
    assert!(!db.tables.contains_key("column_reference_payload"));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from column_reference_boundary"),
        "RS:"
    );
}

#[test]
fn qualified_create_table_columns_are_rejected_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table qualified_column_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into qualified_column_boundary values (1)");

    for sql in [
        "create table qualified_column_payload (db.t.a bigint)",
        "create table qualified_column_payload (t.a char)",
    ] {
        let statement = tidb_parser::parse(sql).expect("parse qualified CREATE TABLE column");
        assert!(matches!(
            db.run(&statement),
            Err(ExecError::Unsupported("qualified CREATE TABLE column name"))
        ));
        assert!(db.transaction.is_active(), "{sql} must not commit");
        assert!(!db.tables.contains_key("qualified_column_payload"));
    }

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from qualified_column_boundary"),
        "RS:"
    );
}

#[test]
fn generated_columns_are_rejected_before_ddl_transaction_mutation() {
    // Generated-column metadata and write/backfill evaluation are not yet
    // representable by this seed catalog. Every parsed create/column-alter
    // path must therefore reject before the implicit DDL commit, preserving
    // the transaction snapshot exactly like the other capability boundaries.
    let mut db = Database::new();
    step(&mut db, "create table generated_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into generated_boundary values (1)");

    for sql in [
        "create table generated_payload (a int, b int as (a + 1))",
        "alter table generated_boundary add b int as (id + 1)",
        "alter table generated_boundary modify id int generated always as (id + 1) stored",
        "alter table generated_boundary change id renamed int as (id + 1) virtual",
    ] {
        let statement = tidb_parser::parse(sql).expect("parse generated-column DDL");
        assert!(matches!(
            db.run(&statement),
            Err(ExecError::Unsupported("generated columns"))
        ));
        assert!(db.transaction.is_active(), "{sql} must not commit");
    }
    assert!(!db.tables.contains_key("generated_payload"));

    assert!(matches!(
        db.run(
            &tidb_parser::parse("create table on_update_payload (ts timestamp on update now())")
                .expect("parse Go ON UPDATE column")
        ),
        Err(ExecError::Unsupported("ON UPDATE columns"))
    ));
    assert!(db.transaction.is_active());
    assert!(!db.tables.contains_key("on_update_payload"));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from generated_boundary"), "RS:");
}

#[test]
fn typed_column_option_capability_gaps_reject_before_ddl_transaction_mutation() {
    // These Go parser payloads are structurally retained for restore/SHOW
    // fidelity but need catalog metadata or physical allocation semantics.
    // They must never be accepted and silently erased at the DDL commit edge.
    let mut db = Database::new();
    step(&mut db, "create table column_option_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into column_option_boundary values (1)");

    for (sql, feature) in [
        (
            "create table auto_random_payload (id bigint auto_random(3) primary key)",
            "AUTO_RANDOM column option",
        ),
        (
            "create table column_format_payload (id int column_format fixed)",
            "COLUMN_FORMAT column option",
        ),
        (
            "create table secondary_attribute_payload (id int secondary_engine_attribute='{}')",
            "SECONDARY_ENGINE_ATTRIBUTE column option",
        ),
        (
            "alter table column_option_boundary add auto_id bigint auto_random",
            "AUTO_RANDOM column option",
        ),
    ] {
        let statement = tidb_parser::parse(sql).expect("parse typed column option");
        assert_eq!(
            db.run(&statement),
            Err(ExecError::Unsupported(feature)),
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "{sql} must not commit");
    }
    assert!(!db.tables.contains_key("auto_random_payload"));
    assert!(!db.tables.contains_key("column_format_payload"));
    assert!(!db.tables.contains_key("secondary_attribute_payload"));
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from column_option_boundary"),
        "RS:"
    );

    // Go warns that STORAGE is parsed but ignored by every storage engine;
    // it is therefore an intentionally no-op catalog attribute, not an
    // unsupported physical feature.
    assert_eq!(
        step(
            &mut db,
            "create table storage_warning_contract (id int storage disk)"
        ),
        "OK"
    );
}

#[test]
fn binary_columns_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table binary_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into binary_boundary values (1)");

    for sql in [
        "create table binary_payload (value binary(16))",
        "alter table binary_boundary add value varbinary(16)",
        "alter table binary_boundary modify id binary(16)",
        "alter table binary_boundary change id renamed binary(16)",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse binary column DDL")),
            Err(ExecError::Unsupported("BINARY/VARBINARY column type"))
        ));
        assert!(db.transaction.is_active());
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from binary_boundary"), "RS:");
}

#[test]
fn blob_columns_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table blob_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into blob_boundary values (1)");

    for sql in [
        "create table blob_payload (value blob)",
        "alter table blob_boundary add value mediumblob",
        "alter table blob_boundary modify id tinyblob",
        "alter table blob_boundary change id renamed longblob",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse BLOB column DDL")),
            Err(ExecError::Unsupported("BLOB column type"))
        ));
        assert!(db.transaction.is_active());
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from blob_boundary"), "RS:");
}

#[test]
fn alter_table_add_check_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table check_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into check_boundary values (1)");

    for sql in [
        "alter table check_boundary add constraint positive_id check (id > 0)",
        // The parser's source-owned CURRENT_USER expression must still stop
        // at the same pre-mutation executor boundary as every other ADD CHECK
        // payload.
        "alter table check_boundary add check (CURRENT_USER != id)",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE ADD CHECK\")",
            "source SQL: {sql}"
        );
    }
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() == 0);
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from check_boundary"), "RS:");
}

#[test]
fn column_check_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table column_check_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into column_check_boundary values (1)");

    for sql in [
        "create table column_check_payload (id int check(id > 0))",
        "alter table column_check_boundary add value int constraint positive_value check(value > 0)",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse column CHECK DDL")),
            Err(ExecError::Unsupported("column-level CHECK"))
        ));
        assert!(db.transaction.is_active(), "source SQL: {sql}");
        assert_eq!(db.transaction.savepoint_count(), 0, "source SQL: {sql}");
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from column_check_boundary"), "RS:");
}

#[test]
fn alter_table_multi_action_is_unsupported_without_applying_a_prefix() {
    let mut db = Database::new();
    step(&mut db, "create table multi_action_boundary (a int)");
    step(&mut db, "begin");
    step(&mut db, "insert into multi_action_boundary values (1)");
    step(&mut db, "savepoint before_multi_action");

    // ADD COLUMN and DROP COLUMN are both executable as single actions. The
    // cardinality gate must reject their combination before implicit commit
    // or catalog mutation instead of applying a successful prefix.
    assert_eq!(
        step(
            &mut db,
            "alter table multi_action_boundary add column b int, drop column a",
        ),
        "Unsupported(\"ALTER TABLE multiple actions\")"
    );
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() != 0);
    assert_eq!(
        db.tables["multi_action_boundary"].cols,
        vec!["a".to_string()]
    );
    assert_eq!(
        step(&mut db, "rollback to savepoint before_multi_action"),
        "OK"
    );
    assert!(db.transaction.is_active());
    assert_eq!(
        db.tables["multi_action_boundary"].cols,
        vec!["a".to_string()]
    );

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select a from multi_action_boundary"), "RS:");
}

#[test]
fn grouped_add_column_list_is_unsupported_before_catalog_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table grouped_add_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into grouped_add_boundary values (1)");
    step(&mut db, "savepoint before_grouped_add");

    assert_eq!(
        step(
            &mut db,
            "alter table grouped_add_boundary add column(id2 tinyint default '11111111')",
        ),
        "Unsupported(\"ALTER TABLE ADD COLUMN list\")"
    );
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() != 0);
    assert_eq!(
        db.tables["grouped_add_boundary"].cols,
        vec!["id".to_string()]
    );

    assert_eq!(
        step(&mut db, "rollback to savepoint before_grouped_add"),
        "OK"
    );
    assert!(db.transaction.is_active());
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from grouped_add_boundary"), "RS:");
}

#[test]
fn grouped_add_column_constraints_are_unsupported_before_catalog_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table grouped_constraint_boundary (id int)");
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into grouped_constraint_boundary values (1)",
    );
    step(&mut db, "savepoint before_grouped_constraint_add");

    for sql in [
        "alter table grouped_constraint_boundary add column (index idx(id))",
        "alter table grouped_constraint_boundary add column (id2 int, primary key (id))",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE ADD COLUMN list\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "source SQL: {sql}");
        assert!(db.transaction.savepoint_count() != 0, "source SQL: {sql}");
        assert_eq!(
            db.tables["grouped_constraint_boundary"].cols,
            vec!["id".to_string()],
            "source SQL: {sql}"
        );
    }

    assert_eq!(
        step(
            &mut db,
            "rollback to savepoint before_grouped_constraint_add"
        ),
        "OK"
    );
    assert!(db.transaction.is_active());
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from grouped_constraint_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_add_partition_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table partition_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into partition_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "alter table partition_boundary add partition partitions 1",
        ),
        "Unsupported(\"ALTER TABLE ADD PARTITION\")"
    );
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() == 0);
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from partition_boundary"), "RS:");

    step(&mut db, "begin");
    step(&mut db, "insert into partition_boundary values (2)");
    assert_eq!(
        step(
            &mut db,
            "alter table partition_boundary add partition (partition p0 values less than (10))",
        ),
        "Unsupported(\"ALTER TABLE ADD PARTITION\")"
    );
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() == 0);
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from partition_boundary"), "RS:");
}

#[test]
fn alter_table_partition_placement_policy_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table partition_placement_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into partition_placement_boundary values (1)",
    );

    for sql in [
        "alter table partition_placement_boundary partition p0 placement policy = pp1",
        "alter table partition_placement_boundary partition p0 placement policy set default",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE PARTITION PLACEMENT POLICY\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "{sql} must not commit");
        assert_eq!(db.transaction.savepoint_count(), 0, "{sql}");
    }
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from partition_placement_boundary"),
        "RS:"
    );
}

#[test]
fn create_table_partition_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table create_partition_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into create_partition_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "create table partitioned_new (id int) partition by range (id) (partition p0 values less than (10))",
        ),
        "Unsupported(\"CREATE TABLE PARTITION BY\")"
    );
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() == 0);
    assert!(!db.tables.contains_key("partitioned_new"));
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from create_partition_boundary"),
        "RS:"
    );
}

#[test]
fn create_table_split_is_unsupported_before_ddl_transaction_or_catalog_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table create_split_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into create_split_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "create table split_new (id int, primary key (id)) split primary key between (0) and (10) regions 2 split index idx by (1)",
        ),
        "Unsupported(\"CREATE TABLE SPLIT\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 0);
    assert!(!db.tables.contains_key("split_new"));
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from create_split_boundary"), "RS:");
}

#[test]
fn ctas_and_bare_create_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table ctas_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into ctas_boundary values (1)");

    for (sql, expected) in [
        (
            "create table ctas_new as select id from ctas_boundary",
            "Unsupported(\"CREATE TABLE AS SELECT\")",
        ),
        (
            "create table ctas_values replace as values row(1)",
            "Unsupported(\"CREATE TABLE AS SELECT\")",
        ),
        (
            "create table source_valid_bare",
            "Unsupported(\"CREATE TABLE without columns\")",
        ),
    ] {
        assert_eq!(step(&mut db, sql), expected, "source SQL: {sql}");
        assert!(db.transaction.is_active(), "source SQL: {sql}");
        assert!(!db.tables.contains_key(
            sql.split_ascii_whitespace()
                .nth(2)
                .expect("CREATE TABLE target")
        ));
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from ctas_boundary"), "RS:");
}

#[test]
fn alter_table_partition_maintenance_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table partition_maintenance_boundary (id int)",
    );

    for sql in [
        "alter table partition_maintenance_boundary reorganize partition p0 into (partition p1 values less than (10))",
        "alter table partition_maintenance_boundary coalesce partition 1",
        "alter table partition_maintenance_boundary truncate partition p0",
        "alter table partition_maintenance_boundary remove partitioning",
        "alter table partition_maintenance_boundary repair partition p0",
    ] {
        step(&mut db, "begin");
        step(
            &mut db,
            "insert into partition_maintenance_boundary values (1)",
        );
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE partition maintenance\")",
            "{sql}"
        );
        assert!(db.transaction.is_active(), "{sql}");
        assert!(db.transaction.savepoint_count() == 0, "{sql}");
        step(&mut db, "rollback");
        assert_eq!(
            step(&mut db, "select id from partition_maintenance_boundary"),
            "RS:",
            "{sql}"
        );
    }
}

#[test]
fn tiflash_replica_and_compact_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table tiflash_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into tiflash_boundary values (1)");

    for (sql, expected) in [
        (
            "alter table tiflash_boundary set tiflash replica 1",
            "Unsupported(\"ALTER TABLE SET TIFLASH REPLICA\")",
        ),
        (
            "alter table tiflash_boundary compact tiflash replica",
            "Unsupported(\"ALTER TABLE COMPACT\")",
        ),
    ] {
        assert_eq!(step(&mut db, sql), expected);
        assert!(db.transaction.is_active());
        assert!(db.transaction.savepoint_count() == 0);
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from tiflash_boundary"), "RS:");
}

#[test]
fn alter_table_charset_options_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table charset_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into charset_boundary values (1)");

    for sql in [
        "alter table charset_boundary charset utf8mb4 collate utf8mb4_bin",
        "alter table charset_boundary convert to character set utf8mb4",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE table options\")"
        );
        assert!(db.transaction.is_active(), "{sql} must not commit");
        assert!(db.transaction.savepoint_count() == 0);
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from charset_boundary"), "RS:");
}

#[test]
fn alter_table_affinity_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table affinity_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into affinity_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "alter table affinity_boundary affinity = 'partition'",
        ),
        "Unsupported(\"ALTER TABLE AFFINITY\")"
    );
    assert!(db.transaction.is_active());
    assert!(db.transaction.savepoint_count() == 0);
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from affinity_boundary"), "RS:");
}

#[test]
fn alter_table_auto_increment_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table auto_increment_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into auto_increment_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "alter table auto_increment_boundary auto_increment = 30",
        ),
        "Unsupported(\"ALTER TABLE AUTO_INCREMENT\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 0);
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from auto_increment_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_auto_id_options_are_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table auto_id_option_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into auto_id_option_boundary values (1)");

    for sql in [
        "alter table auto_id_option_boundary auto_id_cache = 10",
        "alter table auto_id_option_boundary auto_random_base = 50",
        "alter table auto_id_option_boundary force auto_random_base = 50",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE AUTO_ID_CACHE/AUTO_RANDOM_BASE\")"
        );
        assert!(db.transaction.is_active(), "{sql} must not commit");
        assert_eq!(db.transaction.savepoint_count(), 0);
    }
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from auto_id_option_boundary"),
        "RS:",
    );
}

#[test]
fn alter_table_cache_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table cache_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into cache_boundary values (1)");

    for sql in [
        "alter table cache_boundary cache",
        "alter table cache_boundary nocache",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE CACHE/NOCACHE\")"
        );
        assert!(db.transaction.is_active(), "{sql} must not commit");
        assert_eq!(db.transaction.savepoint_count(), 0);
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from cache_boundary"), "RS:");
}

#[test]
fn alter_table_ttl_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table ttl_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into ttl_boundary values (1)");
    for sql in [
        "alter table ttl_boundary ttl_enable='on'",
        "alter table ttl_boundary remove ttl",
    ] {
        assert!(step(&mut db, sql).starts_with("Unsupported("));
        assert!(db.transaction.is_active(), "{sql} must not commit");
        assert_eq!(db.transaction.savepoint_count(), 0);
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from ttl_boundary"), "RS:");
}

#[test]
fn create_table_affinity_is_unsupported_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table affinity_create_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into affinity_create_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "create table affinity_create_target (id int) affinity = 'table'",
        ),
        "Unsupported(\"CREATE TABLE AFFINITY\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 0);
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from affinity_create_boundary"),
        "RS:"
    );
    assert_eq!(
        step(&mut db, "select id from affinity_create_target"),
        "UnknownTable(\"affinity_create_target\")"
    );
}

#[test]
fn create_table_compatibility_options_are_rejected_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table compat_boundary (id int)"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(
        step(&mut db, "insert into compat_boundary values (1)"),
        "OK"
    );

    for sql in [
        "create table autoextend_payload (id int) autoextend_size=4M",
        "create table checksum_payload (id int) page_checksum=1",
        "create table compressed_payload (id int) page_compressed=1",
        "create table level_payload (id int) page_compression_level=1",
        "create table transactional_payload (id int) transactional=0",
        "create table ietf_payload (id int) ietf_quotes=YES",
        "create table sequence_payload (id int) sequence=1",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse compatibility option")),
            Err(ExecError::Unsupported(
                "CREATE TABLE compatibility/MERGE options"
            ))
        ));
        assert!(db.transaction.is_active(), "{sql} must not commit");
    }

    assert!(!db.tables.contains_key("autoextend_payload"));
    assert!(!db.tables.contains_key("sequence_payload"));
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from compat_boundary"), "RS:");
}

#[test]
fn create_table_merge_union_is_rejected_before_ddl_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table merge_boundary (id int)"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(step(&mut db, "insert into merge_boundary values (1)"), "OK");

    let sql = "create table merge_payload (a int) engine=MERGE union=(x, y)";
    assert!(matches!(
        db.run(&tidb_parser::parse(sql).expect("parse MERGE UNION")),
        Err(ExecError::Unsupported(
            "CREATE TABLE compatibility/MERGE options"
        ))
    ));
    assert!(db.transaction.is_active(), "{sql} must not commit");
    assert!(!db.tables.contains_key("merge_payload"));
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from merge_boundary"), "RS:");
}

/// CREATE's static capability preflight runs before implicit commit, but
/// source-shaped catalog construction (such as resolving a key column) runs
/// afterward. Keep that observable error ordering while the physical code is
/// split across the DDL coordinator and create-table leaf.
#[test]
fn create_table_catalog_build_error_occurs_after_implicit_commit() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table committed_before_build_error (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into committed_before_build_error values (1)",
    );

    assert_eq!(
        step(
            &mut db,
            "create table invalid_key_owner (id int, primary key (missing))",
        ),
        "UnknownColumn(\"missing\")"
    );
    assert!(!db.transaction.is_active());
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from committed_before_build_error"),
        "RS:1"
    );
}

#[test]
fn composite_and_named_primary_key() {
    let mut db = Database::new();
    // A composite PRIMARY KEY conflict needs EVERY key column to match.
    step(
        &mut db,
        "create table cc (a int, b int, v int, primary key (a, b))",
    );
    step(&mut db, "insert into cc values (1, 1, 10)");
    step(&mut db, "insert into cc values (1, 2, 20)");
    step(
        &mut db,
        "insert into cc (a, b, v) values (1, 1, 99) on duplicate key update v = 99",
    );
    step(&mut db, "insert into cc (a, b, v) values (2, 1, 30)");
    assert_eq!(
        step(&mut db, "select a, b, v from cc order by a, b"),
        "RS:1|1|99;1|2|20;2|1|30"
    );

    // A named table-level PRIMARY KEY (CONSTRAINT name) behaves identically;
    // the name is purely cosmetic.
    step(
        &mut db,
        "create table named_pk (id int, v int, constraint pk_id primary key (id))",
    );
    step(&mut db, "insert into named_pk values (1, 10)");
    step(
        &mut db,
        "insert into named_pk values (1, 20) on duplicate key update v = 999",
    );
    assert_eq!(step(&mut db, "select * from named_pk"), "RS:1|999");
}

#[test]
fn table_level_secondary_index_executes_with_table_scan_semantics() {
    // A basic non-unique plain-column index changes only the access path.
    // This executor scans tables, so CREATE/INSERT/UPDATE/SELECT retain the
    // same observable results without a physical index catalog.
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table idx_t (a int, b int, key idx_a (a))"),
        "OK"
    );
    assert_eq!(
        step(
            &mut db,
            "insert into idx_t values (1, 10), (1, 20), (2, 30)"
        ),
        "OK"
    );
    assert_eq!(
        step(&mut db, "update idx_t set b = b + 1 where a = 1"),
        "OK"
    );
    assert_eq!(
        step(&mut db, "select a, b from idx_t where a = 1 order by b"),
        "RS:1|11;1|21"
    );
}

#[test]
fn create_temporary_table_is_explicitly_unsupported() {
    // The parser preserves local TEMPORARY, but execution cannot use the
    // ordinary catalog without falsely claiming session-local semantics.
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create temporary table temp_t (a int)"),
        "Unsupported(\"CREATE TEMPORARY TABLE\")"
    );
    assert!(step(&mut db, "select * from temp_t").starts_with("UnknownTable"));
    assert_eq!(step(&mut db, "create table persistent_t (a int)"), "OK");
    assert_eq!(
        step(
            &mut db,
            "create global temporary table global_temp_t (a int) on commit preserve rows",
        ),
        "Unsupported(\"CREATE TEMPORARY TABLE\")"
    );
    assert!(step(&mut db, "select * from global_temp_t").starts_with("UnknownTable"));
}

#[test]
fn create_view_is_explicitly_unsupported_before_ddl_transaction_mutation() {
    // A view needs stored query text, dependency invalidation, ownership, and
    // privilege-aware resolution. The seed catalog has none of those, so it
    // must reject the parsed DDL before an implicit commit can make a pending
    // transaction durable.
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table view_boundary (id int)"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(step(&mut db, "savepoint before_view"), "OK");
    assert_eq!(
        step(
            &mut db,
            "create definer = 'owner'@'localhost' sql security invoker view boundary_v as select id from view_boundary"
        ),
        "Unsupported(\"CREATE VIEW\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 1);
    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(
        step(&mut db, "select * from boundary_v"),
        "UnknownTable(\"boundary_v\")"
    );
}

#[test]
fn create_table_like_is_explicitly_unsupported_without_committing_the_transaction() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table source (a int)"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(step(&mut db, "savepoint before_clone"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 1);

    assert_eq!(
        step(&mut db, "create table clone like source"),
        "Unsupported(\"CREATE TABLE LIKE\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 1);
    assert_eq!(step(&mut db, "select * from source"), "RS:");
    assert!(step(&mut db, "select * from clone").starts_with("UnknownTable"));
}

#[test]
fn alter_table_drop_index_is_explicitly_unsupported() {
    // Basic secondary indexes can be created layout-neutrally for table
    // scans, but their names are not retained. Dropping cannot be a no-op:
    // real TiDB distinguishes missing indexes and index ownership.
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table idx_t (a int, key idx_a (a))"),
        "OK"
    );
    assert_eq!(
        step(&mut db, "alter table idx_t drop key if exists idx_a"),
        "Unsupported(\"ALTER TABLE DROP INDEX\")"
    );
    assert_eq!(step(&mut db, "insert into idx_t values (1)"), "OK");
    assert_eq!(step(&mut db, "select a from idx_t"), "RS:1");
}

#[test]
fn alter_table_drop_primary_key_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(
        step(
            &mut db,
            "create table primary_key_boundary (id int primary key)"
        ),
        "OK"
    );
    step(&mut db, "begin");
    step(&mut db, "savepoint before_drop_primary_key");
    assert_eq!(
        step(&mut db, "alter table primary_key_boundary drop primary key"),
        "Unsupported(\"ALTER TABLE DROP PRIMARY KEY\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 1);
    assert_eq!(
        step(&mut db, "insert into primary_key_boundary values (1)"),
        "OK"
    );
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select * from primary_key_boundary"), "RS:");
}

#[test]
fn unique_column_and_unsigned() {
    let mut db = Database::new();
    step(
        &mut db,
        // Go `HandParser.parseColumnOptions` accepts KEY as the inline
        // PRIMARY spelling and preserves GLOBAL for both key classes. The
        // compact executor's key registry must still see those structural
        // options after the AST representation is unified.
        "create table users (id int key global, email varchar(20) unique global, v int)",
    );
    step(&mut db, "insert into users values (1, 'a@x.com', 10)");
    // A conflict via the non-primary UNIQUE column dispatches the same way.
    step(
        &mut db,
        "insert into users values (2, 'a@x.com', 20) on duplicate key update v = 999",
    );
    assert_eq!(
        step(&mut db, "select id, email, v from users order by id"),
        "RS:1|a@x.com|999"
    );

    // The executor is type-agnostic, so UNSIGNED/ZEROFILL parse and
    // execute exactly like their bare counterparts.
    step(
        &mut db,
        "create table cnts (id int unsigned primary key, v bigint unsigned)",
    );
    step(&mut db, "insert into cnts values (1, 100)");
    step(&mut db, "insert into cnts values (2, 200)");
    assert_eq!(
        step(&mut db, "select id, v from cnts order by id"),
        "RS:1|100;2|200"
    );
    assert_eq!(step(&mut db, "select sum(v) from cnts"), "RS:300");
}

#[test]
fn alter_table_add_drop_modify_change_column() {
    let mut db = Database::new();
    step(&mut db, "create table alt_t (a int, b int)");
    step(&mut db, "insert into alt_t values (1, 10)");
    step(&mut db, "insert into alt_t values (2, 20)");
    step(&mut db, "alter table alt_t add column c int");
    assert_eq!(
        step(&mut db, "select * from alt_t"),
        "RS:1|10|<nil>;2|20|<nil>"
    );
    // ADD COLUMN backfills existing rows with DEFAULT, not just NULL.
    step(&mut db, "alter table alt_t add column d int default 99");
    assert_eq!(
        step(&mut db, "select * from alt_t"),
        "RS:1|10|<nil>|99;2|20|<nil>|99"
    );
    step(&mut db, "alter table alt_t drop column b");
    assert_eq!(
        step(&mut db, "select * from alt_t"),
        "RS:1|<nil>|99;2|<nil>|99"
    );
    // ADD COLUMN ... FIRST/AFTER positions it and shifts other columns.
    step(&mut db, "alter table alt_t add column e int first");
    assert_eq!(
        step(&mut db, "select * from alt_t"),
        "RS:<nil>|1|<nil>|99;<nil>|2|<nil>|99"
    );

    // MODIFY/CHANGE COLUMN reposition via FIRST/AFTER; CHANGE also renames.
    step(&mut db, "create table mc_t (a int, b int, c int)");
    step(&mut db, "insert into mc_t values (1, 10, 100)");
    step(&mut db, "insert into mc_t values (2, 20, 200)");
    step(&mut db, "alter table mc_t modify column c int first");
    assert_eq!(step(&mut db, "select * from mc_t"), "RS:100|1|10;200|2|20");
    step(&mut db, "alter table mc_t change column a z int after c");
    assert_eq!(step(&mut db, "select * from mc_t"), "RS:100|1|10;200|2|20");

    // A reposition on a PRIMARY KEY column keeps its tracked index correct.
    step(
        &mut db,
        "create table mc_pk (id int primary key, v int, w int)",
    );
    step(&mut db, "insert into mc_pk values (1, 10, 100)");
    step(&mut db, "alter table mc_pk change column v vv bigint first");
    assert_eq!(step(&mut db, "select * from mc_pk"), "RS:10|1|100");
    step(
        &mut db,
        "insert into mc_pk values (999, 1, 200) on duplicate key update w = 555",
    );
    assert_eq!(step(&mut db, "select * from mc_pk"), "RS:10|1|555");
}

#[test]
fn alter_enum_set_column_types_are_unsupported_before_mutation() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table enum_boundary (a varchar(8))"),
        "OK"
    );
    step(&mut db, "insert into enum_boundary values ('before')");
    step(&mut db, "begin");
    step(&mut db, "savepoint before_enum_type");
    assert_eq!(
        step(
            &mut db,
            "alter table enum_boundary modify column a enum('before', b'10101')"
        ),
        "Unsupported(\"ALTER TABLE ENUM/SET column type\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 1);
    assert_eq!(step(&mut db, "select a from enum_boundary"), "RS:before");
    step(&mut db, "rollback");
}

#[test]
fn alter_table_rename_and_add_unique() {
    let mut db = Database::new();
    step(&mut db, "create table rt (id int primary key, v int)");
    step(&mut db, "insert into rt values (1, 10)");
    step(&mut db, "insert into rt values (2, 20)");
    step(&mut db, "alter table rt rename to rt2");
    assert_eq!(step(&mut db, "select * from rt2"), "RS:1|10;2|20");
    step(
        &mut db,
        "insert into rt2 values (1, 30) on duplicate key update v = 999",
    );
    assert_eq!(step(&mut db, "select * from rt2"), "RS:1|999;2|20");
    // The old name no longer resolves.
    assert!(step(&mut db, "select * from rt").starts_with("UnknownTable"));

    // ADD UNIQUE extends conflict detection to an already-populated table.
    step(&mut db, "create table ai (a int, b int)");
    step(&mut db, "insert into ai values (1, 10)");
    step(&mut db, "insert into ai values (2, 20)");
    step(&mut db, "alter table ai add unique index (a)");
    step(
        &mut db,
        "insert into ai values (1, 999) on duplicate key update b = 999",
    );
    assert_eq!(step(&mut db, "select * from ai"), "RS:1|999;2|20");
    assert_eq!(step(&mut db, "alter table ai add index (b)"), "OK");
    assert_eq!(step(&mut db, "select * from ai"), "RS:1|999;2|20");
}

#[test]
fn alter_table_add_ordinary_index_registers_metadata_and_commits() {
    let mut db = Database::new();
    step(&mut db, "create table add_index_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into add_index_boundary values (1)");
    assert_eq!(
        step(&mut db, "alter table add_index_boundary add index (id)"),
        "OK"
    );
    assert!(!db.transaction.is_active());
    assert_eq!(db.tables["add_index_boundary"].indexes[0].name, "id");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from add_index_boundary"), "RS:1");

    step(&mut db, "begin");
    step(&mut db, "insert into add_index_boundary values (2)");
    assert_eq!(
        step(
            &mut db,
            "alter table add_index_boundary add index idx_id(missing)",
        ),
        "UnknownColumn(\"missing\")"
    );
    assert!(db.transaction.is_active());
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from add_index_boundary"), "RS:1");

    assert_eq!(
        step(
            &mut db,
            "alter table add_index_boundary add index idx_id(id)"
        ),
        "OK"
    );
    step(&mut db, "begin");
    step(&mut db, "insert into add_index_boundary values (3)");
    assert_eq!(
        step(
            &mut db,
            "alter table add_index_boundary add index idx_id(id)"
        ),
        "DuplicateIndex(\"idx_id\")"
    );
    assert!(db.transaction.is_active());
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from add_index_boundary"), "RS:1");
}

#[test]
fn alter_table_add_advanced_index_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table add_index_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into add_index_boundary values (1)");
    for sql in [
        "alter table add_index_boundary add index idx_expr((cast(id as signed array)))",
        "alter table add_index_boundary add index idx_global(id) global",
        "alter table add_index_boundary add index idx_invisible(id) invisible",
        "alter table add_index_boundary add index idx_partial(id) where id > 0",
    ] {
        assert!(step(&mut db, sql).starts_with("Unsupported(\""));
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from add_index_boundary"), "RS:");
}

#[test]
fn alter_table_alter_index_visibility_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table alter_index_visibility_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into alter_index_visibility_boundary values (1)",
    );
    for sql in [
        "alter table alter_index_visibility_boundary alter index idx_id invisible",
        "alter table alter_index_visibility_boundary alter index idx_id visible",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE ALTER INDEX VISIBILITY\")"
        );
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from alter_index_visibility_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_alter_check_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table alter_check_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into alter_check_boundary values (1)");
    for sql in [
        "alter table alter_check_boundary alter check id_positive enforced",
        "alter table alter_check_boundary alter constraint id_positive not enforced",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE ALTER CHECK\")"
        );
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from alter_check_boundary"), "RS:");
}

#[test]
fn alter_table_alter_column_default_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table alter_default_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into alter_default_boundary values (1)");
    for sql in [
        "alter table alter_default_boundary alter column id set default 1",
        "alter table alter_default_boundary alter id drop default",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE ALTER COLUMN DEFAULT\")"
        );
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from alter_default_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_rename_index_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table rename_index_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into rename_index_boundary values (1)");
    for sql in [
        "alter table rename_index_boundary rename index idx_id to idx_new",
        "alter table rename_index_boundary rename key idx_new to idx_id",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE RENAME INDEX\")"
        );
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from rename_index_boundary"), "RS:");
}

#[test]
fn alter_table_drop_check_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table drop_check_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into drop_check_boundary values (1)");
    for sql in [
        "alter table drop_check_boundary drop check id_positive",
        "alter table drop_check_boundary drop constraint id_positive",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE DROP CHECK\")"
        );
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from drop_check_boundary"), "RS:");
}

#[test]
fn alter_table_drop_foreign_key_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table drop_foreign_key_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into drop_foreign_key_boundary values (1)");
    assert_eq!(
        step(
            &mut db,
            "alter table drop_foreign_key_boundary drop foreign key fk_boundary",
        ),
        "Unsupported(\"ALTER TABLE DROP FOREIGN KEY\")"
    );
    assert!(db.transaction.is_active());
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from drop_foreign_key_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_shard_row_id_bits_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table shard_row_id_bits_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into shard_row_id_bits_boundary values (1)");
    assert_eq!(
        step(
            &mut db,
            "alter table shard_row_id_bits_boundary shard_row_id_bits = 4",
        ),
        "Unsupported(\"ALTER TABLE SHARD_ROW_ID_BITS\")"
    );
    assert!(db.transaction.is_active());
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from shard_row_id_bits_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_placement_policy_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table placement_policy_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into placement_policy_boundary values (1)");
    for sql in [
        "alter table placement_policy_boundary placement policy = pp1",
        "alter table placement_policy_boundary placement policy set default",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE PLACEMENT POLICY\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "{sql} must not commit");
        assert_eq!(db.transaction.savepoint_count(), 0, "{sql}");
    }
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from placement_policy_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_lock_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table lock_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into lock_boundary values (1)");
    for sql in [
        "alter table lock_boundary lock default",
        "alter table lock_boundary lock = exclusive",
    ] {
        assert_eq!(step(&mut db, sql), "Unsupported(\"ALTER TABLE LOCK\")");
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from lock_boundary"), "RS:");
}

#[test]
fn alter_table_comment_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table comment_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into comment_boundary values (1)");
    for sql in [
        "alter table comment_boundary comment 'comment'",
        "alter table comment_boundary comment = 'comment'",
    ] {
        assert_eq!(step(&mut db, sql), "Unsupported(\"ALTER TABLE COMMENT\")");
        assert!(db.transaction.is_active());
    }
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from comment_boundary"), "RS:");
}

#[test]
fn alter_table_engine_attribute_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table engine_attribute_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into engine_attribute_boundary values (1)");
    for sql in [
        "alter table engine_attribute_boundary engine_attribute = '{\"key\":\"value\"}'",
        "alter table engine_attribute_boundary engine_attribute = first engine_attribute = second",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE ENGINE_ATTRIBUTE\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "source SQL: {sql}");
    }
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from engine_attribute_boundary"),
        "RS:"
    );
}

#[test]
fn alter_table_exchange_partition_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table exchange_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into exchange_boundary values (1)");
    assert_eq!(
        step(
            &mut db,
            "alter table exchange_boundary exchange partition p0 with table archive without validation",
        ),
        "Unsupported(\"ALTER TABLE EXCHANGE PARTITION\")"
    );
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from exchange_boundary"), "RS:");
}

#[test]
fn alter_table_drop_partition_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table drop_partition_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into drop_partition_boundary values (1)");
    assert_eq!(
        step(
            &mut db,
            "alter table drop_partition_boundary drop partition p0, p1",
        ),
        "Unsupported(\"ALTER TABLE DROP PARTITION\")"
    );
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from drop_partition_boundary"),
        "RS:"
    );
}

#[test]
fn date_datetime_columns() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table dt (a date, b datetime, c varchar(9))",
    );
    step(
        &mut db,
        "insert into dt values ('2021-01-01', '2021-01-01 10:00:00', 'x')",
    );
    step(
        &mut db,
        "insert into dt values ('2021-06-15', '2021-06-15 12:30:00', 'y')",
    );
    assert_eq!(
        step(&mut db, "select * from dt"),
        "RS:2021-01-01|2021-01-01 10:00:00|x;2021-06-15|2021-06-15 12:30:00|y"
    );
    assert_eq!(
        step(&mut db, "select a, c from dt where a > '2021-01-01'"),
        "RS:2021-06-15|y"
    );
    assert_eq!(
        step(&mut db, "select a from dt order by a"),
        "RS:2021-01-01;2021-06-15"
    );
    // TO_DAYS over a real DATE column (see also tidb-expr's own test).
    assert_eq!(
        step(&mut db, "select to_days(a) from dt order by a"),
        "RS:738156;738321"
    );

    // FROM_DAYS over a real INT column (the reverse direction).
    step(&mut db, "create table fd (n int)");
    step(&mut db, "insert into fd values (719528)");
    step(&mut db, "insert into fd values (738156)");
    assert_eq!(
        step(&mut db, "select from_days(n) from fd order by n"),
        "RS:1970-01-01;2021-01-01"
    );

    // DATE_ADD/DATE_SUB with INTERVAL n DAY over real DATE/DATETIME
    // columns, including a time-of-day suffix preserved verbatim.
    step(&mut db, "create table da (d date, dt datetime)");
    step(
        &mut db,
        "insert into da values ('2021-01-31', '2021-01-31 10:30:00')",
    );
    assert_eq!(
        step(
            &mut db,
            "select date_add(d, interval 1 day), date_sub(dt, interval 5 day) from da"
        ),
        "RS:2021-02-01|2021-01-26 10:30:00"
    );

    // DATE_ADD with INTERVAL n MONTH/YEAR, including the leap-day
    // clamping case (2020-01-31 + 1 MONTH = 2020-02-29).
    step(&mut db, "create table dm (d date)");
    step(&mut db, "insert into dm values ('2021-01-31')");
    step(&mut db, "insert into dm values ('2020-01-31')");
    assert_eq!(
        step(
            &mut db,
            "select date_add(d, interval 1 month), date_add(d, interval 1 year) from dm order by d"
        ),
        "RS:2020-02-29|2021-01-31;2021-02-28|2022-01-31"
    );

    // DATE_ADD/DATE_SUB with INTERVAL n WEEK.
    step(&mut db, "create table dw (d date)");
    step(&mut db, "insert into dw values ('2021-01-01')");
    assert_eq!(
        step(
            &mut db,
            "select date_add(d, interval 1 week), date_sub(d, interval 2 week) from dw"
        ),
        "RS:2021-01-08|2020-12-18"
    );

    // DATE_ADD with INTERVAL n HOUR/MINUTE, including day-rollover
    // carry and a DATE-only input treated as midnight.
    step(&mut db, "create table dh (dt datetime, d date)");
    step(
        &mut db,
        "insert into dh values ('2021-01-01 22:00:00', '2021-01-01')",
    );
    assert_eq!(
        step(
            &mut db,
            "select date_add(dt, interval 5 hour), date_add(d, interval 30 minute) from dh"
        ),
        "RS:2021-01-02 03:00:00|2021-01-01 00:30:00"
    );
}

/// A `BIT`/`BIT(n)` column works end-to-end (`CREATE TABLE`/`INSERT`/
/// `SELECT`) purely because column types are stored/used generically
/// here — no bespoke per-type gating exists in this crate's execution
/// path at all, matching every other column type. Deliberately does
/// NOT assert that an inserted bit-literal's own WIDTH is validated
/// against the column's declared `BIT(n)` length (confirmed via
/// `gorun`: `CREATE TABLE t (a BIT); INSERT INTO t VALUES (b'101')` —
/// a 3-bit value into a 1-bit column — is a genuine `ERR` in real
/// TiDB) — this crate has NO column-width validation for ANY type on
/// `INSERT` (matching the already-documented, deliberately deferred
/// decimal-magnitude-clamping-at-evaluation-time boundary), not a
/// BIT-specific gap to fix here.
#[test]
fn bit_column_exec() {
    let mut db = Database::new();
    step(&mut db, "create table t (a bit(8))");
    step(&mut db, "insert into t values (b'101')");
    // `b'101'` (decimal 5) round-trips as its own raw byte (see
    // `tidb_expr::binary_literal`'s own doc, task #117) — a single
    // control-character byte, not a printable digit string.
    assert_eq!(step(&mut db, "select a from t"), "RS:\u{5}");
}

/// `VARCHAR(n)` length validation on `INSERT`/`UPDATE`, the first slice
/// of column-width enforcement (task #133). Every expected outcome copied
/// from a `gorun` probe: within length stores verbatim (trailing spaces
/// preserved); over length truncates iff the excess is all spaces, else
/// `DataTooLong`; length is counted in characters, not bytes.
#[test]
fn varchar_length_validation() {
    let mut db = Database::new();
    step(&mut db, "create table v (a varchar(3))");
    // Within length: stored verbatim, trailing space preserved.
    assert_eq!(step(&mut db, "insert into v values ('ab ')"), "OK");
    // Over length, excess is a space: truncated silently to 3 chars.
    assert_eq!(step(&mut db, "insert into v values ('abc ')"), "OK");
    // Over length, excess is non-space: a real error.
    assert_eq!(
        step(&mut db, "insert into v values ('abcd')"),
        "DataTooLong(\"a\")"
    );
    // Multibyte counts as characters, not bytes: 2 chars fits, 4 doesn't.
    assert_eq!(step(&mut db, "insert into v values ('中文')"), "OK");
    assert_eq!(
        step(&mut db, "insert into v values ('中文字八')"),
        "DataTooLong(\"a\")"
    );
    assert_eq!(
        step(&mut db, "select concat('[',a,']') from v order by a"),
        "RS:[ab ];[abc];[中文]"
    );

    // UPDATE enforces the same rule.
    let mut db2 = Database::new();
    step(&mut db2, "create table v (a varchar(3))");
    step(&mut db2, "insert into v values ('ab')");
    assert_eq!(
        step(&mut db2, "update v set a = 'abcd'"),
        "DataTooLong(\"a\")"
    );
    assert_eq!(step(&mut db2, "update v set a = 'abc '"), "OK");
    assert_eq!(
        step(&mut db2, "select concat('[',a,']') from v"),
        "RS:[abc]"
    );

    // The width follows the column across ALTER: a MODIFY narrowing the
    // type is enforced on the next INSERT; the col_types stay aligned
    // after an ADD/DROP COLUMN reshuffle.
    let mut db3 = Database::new();
    step(&mut db3, "create table v (a int, b varchar(5))");
    step(&mut db3, "alter table v drop column a");
    assert_eq!(step(&mut db3, "insert into v values ('abcde')"), "OK");
    assert_eq!(
        step(&mut db3, "insert into v values ('abcdef')"),
        "DataTooLong(\"b\")"
    );
    step(&mut db3, "alter table v modify column b varchar(2)");
    assert_eq!(
        step(&mut db3, "insert into v values ('abc')"),
        "DataTooLong(\"b\")"
    );
}

/// `CHAR(n)` length validation on `INSERT` (task #134): the SAME length
/// rule as `VARCHAR` (truncate iff excess is all spaces, else
/// `DataTooLong`), plus MySQL's storage-time right-trim of ALL trailing
/// spaces — so a within-length `'ab '` stores as `'ab'`, and an
/// over-length `'abc  '` first truncates its two excess spaces then trims
/// to `'abc'`. Every outcome copied from a `gorun` probe.
#[test]
fn char_length_validation() {
    let mut db = Database::new();
    step(&mut db, "create table c (a char(3))");
    assert_eq!(step(&mut db, "insert into c values ('ab')"), "OK");
    // Trailing spaces trimmed on storage, even within length.
    assert_eq!(step(&mut db, "insert into c values ('ab ')"), "OK");
    assert_eq!(step(&mut db, "insert into c values ('a  ')"), "OK");
    // Over length, excess all spaces: truncate then trim → 'abc'.
    assert_eq!(step(&mut db, "insert into c values ('abc  ')"), "OK");
    // Over length, excess has a non-space: a real error.
    assert_eq!(
        step(&mut db, "insert into c values ('abcd')"),
        "DataTooLong(\"a\")"
    );
    assert_eq!(
        step(&mut db, "insert into c values ('ab cd')"),
        "DataTooLong(\"a\")"
    );
    // Empty string stores as-is.
    assert_eq!(step(&mut db, "insert into c values ('')"), "OK");
    assert_eq!(
        step(&mut db, "select concat('[',a,']') from c order by a"),
        "RS:[];[a];[ab];[ab];[abc]"
    );
}

/// `BIT(n)` width validation on `INSERT` (task #135): the value's numeric
/// magnitude must fit in `n` bits, whether written as a bit literal or an
/// integer (`b'1000'` = 8 and the integer `8` both overflow `BIT(3)`,
/// whose max is 7). Every outcome copied from a `gorun` probe. `a + 0` /
/// raw-byte selection is avoided since a stored `BIT` value is raw bytes
/// with no string→number coercion in this crate; the row COUNT confirms
/// exactly which inserts survived.
#[test]
fn bit_width_validation() {
    let mut db = Database::new();
    step(&mut db, "create table b (a bit(3))");
    assert_eq!(step(&mut db, "insert into b values (b'101')"), "OK"); // 5
    assert_eq!(step(&mut db, "insert into b values (b'111')"), "OK"); // 7
    assert_eq!(
        step(&mut db, "insert into b values (b'1000')"), // 8 > 7
        "DataTooLong(\"a\")"
    );
    assert_eq!(
        step(&mut db, "insert into b values (b'1010')"), // 10 > 7
        "DataTooLong(\"a\")"
    );
    assert_eq!(step(&mut db, "insert into b values (5)"), "OK");
    assert_eq!(step(&mut db, "insert into b values (7)"), "OK");
    assert_eq!(
        step(&mut db, "insert into b values (8)"), // integer 8 > 7
        "DataTooLong(\"a\")"
    );
    assert_eq!(step(&mut db, "insert into b values (0)"), "OK");
    // Five inserts survived: b'101', b'111', 5, 7, 0.
    assert_eq!(step(&mut db, "select count(*) from b"), "RS:5");
}

/// `DECIMAL(p,s)` integer-digit overflow on `INSERT` (task #136), the
/// last column-width category. Real TiDB rounds to scale `s` FIRST, then
/// range-checks the integer part against `p - s` digits: `99.995` rounds
/// to `100.00` and overflows `DECIMAL(4,2)`, while `99.994` rounds to
/// `99.99` and fits; fractional excess rounds (never errors). Stored
/// values carry exactly `s` fractional digits. Every outcome copied from
/// a `gorun` probe.
#[test]
fn decimal_precision_validation() {
    let mut db = Database::new();
    step(&mut db, "create table d (a decimal(4,2))");
    assert_eq!(step(&mut db, "insert into d values (12.34)"), "OK");
    assert_eq!(step(&mut db, "insert into d values (99.99)"), "OK");
    // 3 integer digits > p - s = 2.
    assert_eq!(
        step(&mut db, "insert into d values (100)"),
        "OutOfRange(\"a\")"
    );
    assert_eq!(
        step(&mut db, "insert into d values (123.4)"),
        "OutOfRange(\"a\")"
    );
    // Fractional excess rounds, no error.
    assert_eq!(step(&mut db, "insert into d values (1.005)"), "OK"); // → 1.01
    assert_eq!(step(&mut db, "insert into d values (9.999)"), "OK"); // → 10.00
    assert_eq!(step(&mut db, "insert into d values (-99.99)"), "OK");
    assert_eq!(
        step(&mut db, "insert into d values (-100)"),
        "OutOfRange(\"a\")"
    );
    assert_eq!(step(&mut db, "insert into d values (5)"), "OK"); // → 5.00
    assert_eq!(step(&mut db, "insert into d values (99.994)"), "OK"); // → 99.99
                                                                      // Rounds to 100.00, which THEN overflows.
    assert_eq!(
        step(&mut db, "insert into d values (99.995)"),
        "OutOfRange(\"a\")"
    );
    assert_eq!(
        step(&mut db, "select a from d order by a"),
        "RS:-99.99;1.01;5.00;10.00;12.34;99.99;99.99"
    );
}
