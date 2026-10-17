// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Sessions over the REAL embedded store with the coprocessor wired.
//!
//! Every other module in this directory serves `cop_scans: None`, so a plan
//! that only misbehaves when base-table scans are answered by the pushdown
//! coprocessor -- `CopScanSource` over the in-process unistore transport --
//! never fails in-tree. This module builds the same stack `--store unistore`
//! boots and pins those plans.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::{Datum, MySqlDuration};
use tidb_ddl_notifier::{
    publish_schema_change_to_store, DdlNotifier, Handler, SchemaChangeEvent, SessionPool, Store,
    TEST_HANDLER_ID,
};
use tidb_model::go_runtime::GoSharedSlice;
use tidb_model::partition::{PartitionDefinition, PartitionInfo};

use super::super::ddl_notifier::{ClusterNotifierSessionPool, ClusterNotifierTableStore};
use super::super::{
    partition_id_map, ClusterHistoricalStatsHandle, ClusterPriorityQueueSource,
    ClusterServerSession, ClusterSessionFactory,
};
use super::node_fixture::{rows, session_context, ABC_HASH};
use crate::configured_user_store::ConfiguredUserStore;
use crate::sql_node::QuerySession;
use crate::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};
use crate::QuerySessionFactory;

fn cop_backed_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    cop_backed_stack_with_stats_lease(None)
}

fn cop_backed_stack_with_stats_lease(
    stats_lease: Option<crate::node_config::StatsLease>,
) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    let mut config = crate::node_config::NodeConfig::parse([
        "tidb-server",
        "--store",
        "unistore",
        "--port",
        "0",
        // Parse-time requirement only: the test passes its own user store
        // below, so the flag never has to name real rows.
        "--auth-file",
        "/dev/null",
    ])
    .expect("node config");
    if let Some(stats_lease) = stats_lease {
        config.stats_lease = stats_lease;
    }
    let users = Arc::new(
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store"),
    );
    let stack =
        unistore_cluster_session_stack(&config, &users, None, None).expect("unistore stack");
    (stack, users)
}

fn displayed(rows: Vec<Vec<Datum>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                // Every datum a served column can hold renders as the text
                // the wire would carry. A `{other:?}` fallback here reads as
                // a value mismatch when the value is in fact right, which
                // has cost this file three false failures.
                .map(|datum| match datum {
                    Datum::Int(v) => v.to_string(),
                    Datum::UInt(v) => v.to_string(),
                    Datum::Real(v) => v.to_string(),
                    Datum::Decimal(d) => d.to_string(),
                    Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    Datum::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Datum::Enum(value, _) => {
                        String::from_utf8_lossy(value.name().as_bytes()).into_owned()
                    }
                    Datum::Set(value, _) => {
                        String::from_utf8_lossy(value.name().as_bytes()).into_owned()
                    }
                    Datum::Time(time) => time.to_string(),
                    Datum::Duration(duration) => duration.to_string(),
                    Datum::Json(json) => json.to_string(),
                    Datum::Float32(v) => {
                        tidb_datatype::go_strconv::format_float(f64::from(v), b'f', -1, 32)
                    }
                    Datum::Null => "NULL".to_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

#[test]
fn session_activation_preserves_assertion_level_until_transaction_end() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(129)).unwrap();
    for (attempt, (mode, level, later, fails)) in [
        ("OPTIMISTIC", "STRICT", "OFF", true),
        ("PESSIMISTIC", "FAST", "OFF", true),
        ("OPTIMISTIC", "OFF", "STRICT", false),
    ]
    .into_iter()
    .enumerate()
    {
        rows(
            &mut session,
            &format!("SET tidb_txn_assertion_level = '{level}'"),
        );
        session
            .control_transaction(&format!("BEGIN {mode}"))
            .unwrap();
        // A later SET must not change this transaction's commit assertion level.
        rows(
            &mut session,
            &format!("SET tidb_txn_assertion_level = '{later}'"),
        );
        session
            .buffer
            .stage_owned_batch([(
                tidb_txnkv::Key::from_bytes(format!("activation_assertion_{attempt}").into_bytes()),
                Some(b"value".to_vec()),
                false,
                tidb_txnkv::AssertionOp::AssertExist,
            )])
            .unwrap();
        let result = session.control_transaction("COMMIT");
        if fails {
            assert_eq!(
                result
                    .expect_err("missing key must fail its Exist assertion")
                    .code,
                8141
            );
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn session_activation_controls_the_native_commit_protocol() {
    if crate::isolate_process_globals() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(130)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.activation_protocol (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut session,
        "INSERT INTO test.activation_protocol VALUES (1, 10)",
    );
    for (asynchronous, one_pc) in [(false, false), (true, false), (false, true), (true, true)] {
        for path in [
            "insert",
            "prefetched",
            "fallback",
            "OPTIMISTIC",
            "PESSIMISTIC",
        ] {
            rows(
                &mut session,
                &format!(
                    "SET tidb_enable_async_commit = {}, tidb_enable_1pc = {}",
                    u8::from(asynchronous),
                    u8::from(one_pc)
                ),
            );
            let before = tikv_client::metrics::get_txn_commit_counter();
            match path {
                "insert" => {
                    session
                        .execute_write("INSERT INTO test.activation_protocol VALUES (2, 20)")
                        .unwrap();
                }
                "prefetched" => {
                    rows(
                        &mut session,
                        "UPDATE test.activation_protocol SET v=v+1 WHERE id=1",
                    );
                }
                "fallback" => {
                    session
                        .execute_write("UPDATE test.activation_protocol SET v=v+1 WHERE id=1")
                        .unwrap();
                }
                mode => {
                    session
                        .control_transaction(&format!("BEGIN {mode}"))
                        .unwrap();
                    session
                        .execute_write("UPDATE test.activation_protocol SET v=v+1 WHERE id=1")
                        .unwrap();
                    // SET cannot change the options of the active transaction.
                    rows(
                        &mut session,
                        &format!(
                            "SET tidb_enable_async_commit = {}, tidb_enable_1pc = {}",
                            u8::from(!asynchronous),
                            u8::from(!one_pc)
                        ),
                    );
                    session.control_transaction("COMMIT").unwrap();
                }
            }
            let delta = tikv_client::metrics::get_txn_commit_counter().subtract(before);
            let expected = if one_pc {
                (0, 0, 1)
            } else if asynchronous {
                (0, 1, 0)
            } else {
                (1, 0, 0)
            };
            assert_eq!(
                (delta.two_pc, delta.async_commit, delta.one_pc),
                expected,
                "{path}: async={asynchronous}, 1pc={one_pc}"
            );
            if path == "insert" {
                session
                    .execute_write("DELETE FROM test.activation_protocol WHERE id=2")
                    .unwrap();
            }
        }
    }
}

/// Go `onModifyColumn` on the production node: each kind checks or rewrites
/// the rows in the DDL transaction, and the answers -- values, errors and the
/// final definitions -- are the Go oracle's for the same script. This node
/// refused every one of these changes ("Unsupported modify column: ...").
#[test]
fn a_modify_column_checks_or_rewrites_rows_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(136)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.mc (a BIGINT, b VARCHAR(20), c INT, d VARCHAR(10), \
         KEY ib (b), KEY ic (c))",
    );
    rows(
        &mut session,
        "INSERT INTO test.mc VALUES (1, 'x  ', 5, 'p'), (2, 'yy', NULL, 'q ')",
    );
    for sql in [
        // A checked narrowing.
        "ALTER TABLE test.mc MODIFY COLUMN a INT",
        // A sign change: a rewrite under a new column id.
        "ALTER TABLE test.mc MODIFY COLUMN a INT UNSIGNED",
        // VARCHAR -> CHAR over trailing spaces: the precheck fails, and the
        // statement is reorganized, which strips them.
        "ALTER TABLE test.mc MODIFY COLUMN b CHAR(10)",
        "ALTER TABLE test.mc MODIFY COLUMN d CHAR(5)",
    ] {
        session
            .execute_write(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    // Go `checkForNullValue`: a NULL row refuses NOT NULL.
    let error = session
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN c INT NOT NULL")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'c' at row 1")
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, CONCAT('[', b, ']'), c, CONCAT('[', d, ']') FROM test.mc ORDER BY a"
        )),
        [["1", "[x]", "5", "[p]"], ["2", "[yy]", "NULL", "[q]"]]
    );
    rows(&mut session, "UPDATE test.mc SET c = 0 WHERE c IS NULL");
    session
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN c INT NOT NULL DEFAULT 7 COMMENT 'cc'")
        .unwrap();
    // Go `checkModifyColumnData`: the first row the new length cannot hold.
    let error = session
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN b VARCHAR(1)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'b', value is 'yy'")
    );
    session
        .execute_write("ALTER TABLE test.mc CHANGE COLUMN a aa BIGINT FIRST")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.mc"));
    assert!(
        created[0][1].contains(
            "  `aa` bigint DEFAULT NULL,\n  `b` char(10) DEFAULT NULL,\n  \
             `c` int NOT NULL DEFAULT '7' COMMENT 'cc',\n  `d` char(5) DEFAULT NULL,\n"
        ),
        "{}",
        created[0][1]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM test.mc ORDER BY aa")),
        [["1", "x", "5", "p"], ["2", "yy", "0", "q"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.mc");
}

/// Go `CreateForeignKey` / `onCreateForeignKey` / `DropForeignKey` on the
/// production node; every answer is the Go oracle's for the same script. A
/// refused `checkForeignKeyConstrain` rolls the job back but keeps the id
/// `allocateFKIndexID` took, so the next anonymous key is `fk_3`, not `fk_2`.
/// This node refused both statements as unsupported.
#[test]
fn an_added_foreign_key_checks_rows_and_keeps_gos_ids_on_rollback() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(137)).unwrap();
    rows(&mut session, "SET @@foreign_key_checks = 1");
    rows(
        &mut session,
        "CREATE TABLE test.parent (id INT KEY, c INT, INDEX (c))",
    );
    rows(&mut session, "CREATE TABLE test.child (id INT KEY, cc INT)");
    rows(
        &mut session,
        "INSERT INTO test.parent VALUES (1, 1), (2, 2)",
    );
    rows(&mut session, "INSERT INTO test.child VALUES (1, 1), (2, 3)");
    let add = "ALTER TABLE test.child ADD FOREIGN KEY fk_2 (cc) REFERENCES test.parent (c)";
    let violation = "Cannot add or update a child row: a foreign key constraint fails \
                     (`test`.`child`, CONSTRAINT `fk_2` FOREIGN KEY (`cc`) REFERENCES \
                     `parent` (`c`))";
    let error = session.execute_write(add).unwrap_err();
    assert_eq!((error.code, error.message.as_str()), (1452, violation));
    rows(&mut session, "DELETE FROM test.child WHERE id = 2");
    session.execute_write(add).unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.child"));
    assert!(
        created[0][1].contains(
            "  KEY `fk_2` (`cc`),\n  \
             CONSTRAINT `fk_2` FOREIGN KEY (`cc`) REFERENCES `parent` (`c`)\n"
        ),
        "{}",
        created[0][1]
    );
    let error = session
        .execute_write("INSERT INTO test.child VALUES (3, 9)")
        .unwrap_err();
    assert_eq!((error.code, error.message.as_str()), (1452, violation));
    let error = session.execute_write(add).unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1826, "Duplicate foreign key constraint name 'fk_2'")
    );
    let drop = "ALTER TABLE test.child DROP FOREIGN KEY fk_2";
    session.execute_write(drop).unwrap();
    let error = session.execute_write(drop).unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1091, "Can't DROP 'fk_2'; check that column/key exists")
    );
    session
        .execute_write("ALTER TABLE test.child ADD FOREIGN KEY (cc) REFERENCES test.parent (c)")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.child"));
    assert!(
        created[0][1].contains(
            "  KEY `fk_2` (`cc`),\n  \
             CONSTRAINT `fk_3` FOREIGN KEY (`cc`) REFERENCES `parent` (`c`)\n"
        ),
        "{}",
        created[0][1]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.child");
}

/// Go `AlterTableCache` / `AlterTableNoCache` and the `ErrOptOnCacheTable`
/// refusals on the production node, answers from the Go oracle for the same
/// script. CACHE writes the `mysql.table_cache_meta` lock row and stages
/// `checkCacheTableSize`; every DDL Go's executor refuses on a cached table is
/// refused (8242) before a job. This node refused CACHE / NOCACHE and ran the
/// rest.
#[test]
fn a_cached_table_refuses_ddl_until_nocache_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(145)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.c1 (a INT PRIMARY KEY, b INT, KEY ib (b))",
    );
    rows(&mut session, "INSERT INTO test.c1 VALUES (1, 1), (2, 2)");
    session.execute_write("ALTER TABLE test.c1 CACHE").unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c1"));
    assert!(
        created[0][1].ends_with(" /* CACHED ON */"),
        "{}",
        created[0][1]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT lock_type, lease, oldReadLease FROM mysql.table_cache_meta WHERE tid = \
             (SELECT tidb_table_id FROM information_schema.tables \
              WHERE table_schema = 'test' AND table_name = 'c1')"
        )),
        [["NONE", "0", "0"]]
    );
    rows(&mut session, "INSERT INTO test.c1 VALUES (3, 3)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.c1 ORDER BY a")),
        [["1"], ["2"], ["3"]]
    );
    for (sql, operation) in [
        ("ALTER TABLE test.c1 ADD COLUMN c INT", "Alter Table"),
        ("ALTER TABLE test.c1 CACHE, ADD COLUMN d INT", "Alter Table"),
        ("CREATE INDEX i2 ON test.c1 (b)", "Create Index"),
        ("DROP INDEX ib ON test.c1", "Drop Index"),
        ("TRUNCATE TABLE test.c1", "Truncate Table"),
        ("RENAME TABLE test.c1 TO test.c2", "Rename Table"),
        ("DROP TABLE test.c1", "Drop Table"),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                8242,
                format!("'{operation}' is unsupported on cache tables.").as_str()
            ),
            "{sql}"
        );
    }
    session.execute_write("ALTER TABLE test.c1 CACHE").unwrap();
    session
        .execute_write("ALTER TABLE test.c1 NOCACHE")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c1"));
    assert!(!created[0][1].contains("CACHED ON"), "{}", created[0][1]);
    session
        .execute_write("ALTER TABLE test.c1 ADD COLUMN c INT")
        .unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.p (a INT) PARTITION BY HASH (a) PARTITIONS 2",
    );
    for (sql, code, message) in [
        (
            "ALTER TABLE test.p CACHE",
            8242,
            "'partition mode' is unsupported on cache tables.",
        ),
        (
            "ALTER TABLE mysql.user CACHE",
            8200,
            "ALTER table cache for tables in system database is currently unsupported",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
}

/// Go's expression defaults and clustered-index mode on the production node,
/// answers from the Go oracle for the same script:
/// - CREATE TABLE reads the session's `@@tidb_enable_clustered_index`
///   (`metabuild.WithClusteredIndexDefMode`); the node built every primary
///   key as if it were ON;
/// - ADD COLUMN with an expression default evaluates it once into the origin
///   default (`generateOriginDefaultValue`), after `CreateNewColumn` refuses
///   an unreplayable call (1674) and an ENUM that cannot hold the value
///   (1265, Go's argument-less `ErrTruncated`); the node refused every one;
/// - a metadata-only MODIFY keeps the old origin and evaluates nothing;
/// - `DEFAULT (CURRENT_DATE)`'s stored word is read through `GetTimeValue`;
///   the loader had refused the table, which vanished (1146);
/// - an AUTO_INCREMENT column takes no expression default (1067).
#[test]
fn expression_defaults_and_the_clustered_mode_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(144)).unwrap();
    rows(
        &mut session,
        "SET @@tidb_enable_clustered_index = 'int_only'",
    );
    rows(
        &mut session,
        "CREATE TABLE test.tc (a INT, b INT, PRIMARY KEY (a, b))",
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.tc"));
    assert!(
        created[0][1].contains("PRIMARY KEY (`a`,`b`) /*T![clustered_index] NONCLUSTERED */"),
        "{}",
        created[0][1]
    );
    rows(&mut session, "CREATE TABLE test.t (c INT)");
    rows(&mut session, "INSERT INTO test.t VALUES (1), (2)");
    for sql in [
        "ALTER TABLE test.t ADD COLUMN c3 DATETIME DEFAULT (STR_TO_DATE('1980-01-01','%Y-%m-%d'))",
        "ALTER TABLE test.t ADD COLUMN c6 ENUM('y','n')",
        "ALTER TABLE test.t MODIFY COLUMN c6 ENUM('y','n') DEFAULT (DATE_FORMAT(NOW(),'%Y-%m-%d'))",
        "ALTER TABLE test.t ADD COLUMN c7 DATE DEFAULT (CURRENT_DATE)",
    ] {
        session
            .execute_write(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    for (sql, code, message) in [
        (
            "ALTER TABLE test.t ADD COLUMN c4 VARCHAR(32) DEFAULT (UPPER(UUID()))",
            1674,
            "Statement is unsafe because it uses a system function that may return a different \
             value on the slave",
        ),
        (
            "ALTER TABLE test.t ADD COLUMN c5 ENUM('y','n') DEFAULT (DATE_FORMAT(NOW(),'%Y-%m-%d'))",
            1265,
            "Data truncated for column '%s' at row %d",
        ),
        (
            "CREATE TABLE test.ta (c INT, c1 INT AUTO_INCREMENT DEFAULT \
             (STR_TO_DATE('1980-01-01','%Y-%m-%d')))",
            1067,
            "Invalid default value for 'c1'",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT c, c3, c7 IS NOT NULL FROM test.t ORDER BY c"
        )),
        [
            ["1", "1980-01-01 00:00:00", "1"],
            ["2", "1980-01-01 00:00:00", "1"]
        ]
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].contains(
            "  `c3` datetime DEFAULT (str_to_date(_utf8mb4'1980-01-01', _utf8mb4'%Y-%m-%d')),\n  \
             `c6` enum('y','n') DEFAULT (date_format(now(), _utf8mb4'%Y-%m-%d')),\n  \
             `c7` date DEFAULT (CURRENT_DATE)\n"
        ),
        "{}",
        created[0][1]
    );
}

/// Go `generateOriginDefaultValue` for an ADD COLUMN ... NOT NULL with no
/// default, and `getColDefaultValue`'s cast of the stored text on read; the
/// answers are the Go oracle's. This node refused SET, ENUM and JSON, stamped
/// BIT as `"0"` (stored as the byte 0x30) and BINARY(n) as no bytes, and read
/// a JSON origin default back as raw text ("binary JSON document").
#[test]
fn an_added_not_null_column_reads_gos_zero_value_on_existing_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(143)).unwrap();
    rows(&mut session, "CREATE TABLE test.t (id INT PRIMARY KEY)");
    rows(&mut session, "INSERT INTO test.t VALUES (1), (2)");
    for column in [
        "s SET('a','b')",
        "e ENUM('x','y')",
        "b BIT(8)",
        "j JSON",
        "bn BINARY(3)",
        "d DECIMAL(6,2)",
        "f FLOAT",
        "y YEAR",
        "tm TIME(2)",
    ] {
        let sql = format!("ALTER TABLE test.t ADD COLUMN {column} NOT NULL");
        session
            .execute_write(&sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let zero = [
        "",
        "x",
        "0",
        "null",
        "000000",
        "0.00",
        "0",
        "0",
        "00:00:00.00",
    ];
    let read = displayed(rows(
        &mut session,
        "SELECT s, e, b + 0, j, HEX(bn), d, f, y + 0, tm FROM test.t ORDER BY id",
    ));
    assert_eq!(read, [zero, zero]);
    let error = session
        .execute_write("INSERT INTO test.t (id) VALUES (3)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1364, "Field 's' doesn't have a default value")
    );
    rows(&mut session, "ADMIN CHECK TABLE test.t");
}

/// Go's partial indexes on the production node. The builders dropped `WHERE`
/// (CREATE TABLE) or refused it (CREATE INDEX); now Go's
/// `CheckAndBuildIndexConditionString` / `buildAffectColumn` store the
/// condition, the loader and the backfill compile it, and Go's
/// `checkIndexCondition`, partitioned-table, primary-key and 8272 refusals
/// apply. Answers and messages are the Go oracle's for the same script.
#[test]
fn a_partial_index_holds_only_the_rows_its_condition_admits() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(142)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t (a INT, b INT, c VARCHAR(10), KEY ia (b) WHERE a > 2)",
    );
    rows(
        &mut session,
        "INSERT INTO test.t VALUES (1, 10, 'x'), (3, 30, 'y'), (5, 50, 'z')",
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].contains("  KEY `ia` (`b`) WHERE `a` > 2\n"),
        "{}",
        created[0][1]
    );
    let through_ia = "SELECT b FROM test.t USE INDEX (ia) WHERE a > 2 AND b > 0 ORDER BY b";
    assert_eq!(displayed(rows(&mut session, through_ia)), [["30"], ["50"]]);
    rows(&mut session, "UPDATE test.t SET a = 10 WHERE b = 10");
    assert_eq!(
        displayed(rows(&mut session, through_ia)),
        [["10"], ["30"], ["50"]]
    );
    // The backfill writes an entry for the rows the condition admits only.
    session
        .execute_write("CREATE INDEX ib ON test.t (b) WHERE a < 5")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM test.t USE INDEX (ib) WHERE a < 5 AND b > 0"
        )),
        [["30"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.t");
    let referenced =
        "Cannot drop, change or modify column 'a': it is referenced in partial index 'ia'";
    for (sql, code, message) in [
        (
            "ALTER TABLE test.t MODIFY COLUMN a BIGINT",
            8272,
            referenced,
        ),
        ("ALTER TABLE test.t DROP COLUMN a", 8272, referenced),
        (
            "ALTER TABLE test.t CHANGE COLUMN a aa INT",
            8272,
            referenced,
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad1 (b) WHERE a + 1 > 2",
            8200,
            "Unsupported add partial index: partial index condition must include a column name \
             in the binary operation",
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad3 (b) WHERE a = NULL",
            8200,
            "Unsupported add partial index: = NULL is not supported in partial index condition \
             because it is always false",
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad5 (b) WHERE d > 1",
            8200,
            "Unsupported add partial index: column name `d` referenced in partial index \
             condition is not found in table",
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad6 (b) WHERE a IS TRUE",
            8200,
            "Unsupported add partial index: the kind of partial index condition is not supported",
        ),
        (
            "CREATE TABLE test.tp (a INT, b INT, KEY i (b) WHERE a > 1, \
             PRIMARY KEY (a) WHERE a > 2)",
            8200,
            "Unsupported add partial index: create an primary key with partial index is not \
             supported",
        ),
        (
            "CREATE TABLE test.tq (a INT, b INT, KEY i (b) WHERE a > 5) \
             PARTITION BY RANGE (b) (PARTITION p0 VALUES LESS THAN (5))",
            8200,
            "Unsupported add partial index: partial index is not supported on partitioned table",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(
        &mut session,
        "CREATE TABLE test.tr (a INT PRIMARY KEY, b INT) PARTITION BY HASH (a) PARTITIONS 2",
    );
    let error = session
        .execute_write("CREATE INDEX ir ON test.tr (b) WHERE b > 1")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8200,
            "Unsupported add partial index: partial index on partitioned table is not supported"
        )
    );
    session
        .execute_write("ALTER TABLE test.t DROP INDEX ia")
        .unwrap();
    session
        .execute_write("ALTER TABLE test.t DROP INDEX ib")
        .unwrap();
    session
        .execute_write("ALTER TABLE test.t MODIFY COLUMN a BIGINT")
        .unwrap();
}

/// Go's table checks on the production node, answers from the Go oracle for
/// the same script: `checkTooLongSchema` / `checkTableInfoValidExtra` / ADD
/// COLUMN's name limit (1059), `buildColumnAndConstraint`'s reserved
/// `_tidb_rowid` (1166), a clustered or implicit primary key that would be
/// invisible (3522, from `buildTableInfo` and from
/// `updateVersionAndTableInfoWithCheck` after a change makes an invisible
/// unique index the primary key), `CreatePrimaryKey`'s clustered (8200) and
/// expression (3756) refusals, and a view's duplicate column (1060). This
/// node accepted every one of them.
#[test]
fn a_table_is_checked_as_go_checks_it_before_it_is_published() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(141)).unwrap();
    let long = "a".repeat(65);
    let too_long = format!("Identifier name '{long}' is too long");
    rows(&mut session, "CREATE TABLE test.t (a INT NOT NULL, b INT)");
    rows(
        &mut session,
        "ALTER TABLE test.t ADD UNIQUE ub (b) INVISIBLE",
    );
    let invisible = "A primary key index cannot be invisible";
    for (sql, code, message) in [
        (format!("CREATE DATABASE {long}"), 1059, too_long.as_str()),
        (
            format!("CREATE TABLE test.t_long (c1 INT, {long} INT)"),
            1059,
            too_long.as_str(),
        ),
        (
            format!("CREATE TABLE test.{long} (a INT)"),
            1059,
            too_long.as_str(),
        ),
        (
            format!("ALTER TABLE test.t ADD COLUMN {long} INT"),
            1059,
            too_long.as_str(),
        ),
        (
            "ALTER TABLE test.t ADD COLUMN `_tidb_rowid` INT".to_owned(),
            1166,
            "Incorrect column name '_tidb_rowid'",
        ),
        (
            "CREATE TABLE test.tpk (c1 INT NOT NULL, PRIMARY KEY (c1) INVISIBLE)".to_owned(),
            3522,
            invisible,
        ),
        (
            "ALTER TABLE test.t MODIFY COLUMN b INT NOT NULL".to_owned(),
            3522,
            invisible,
        ),
        (
            "ALTER TABLE test.t ADD PRIMARY KEY (a) CLUSTERED".to_owned(),
            8200,
            "Adding clustered primary key is not supported. Please consider adding \
             NONCLUSTERED primary key instead",
        ),
        (
            "ALTER TABLE test.t ADD PRIMARY KEY ((a + b)) NONCLUSTERED".to_owned(),
            3756,
            "The primary key cannot be an expression index",
        ),
        (
            "ALTER TABLE test.t ADD PRIMARY KEY (a) NONCLUSTERED INVISIBLE".to_owned(),
            3522,
            invisible,
        ),
        (
            "CREATE VIEW test.v AS SELECT 1 AS x, 1 AS x".to_owned(),
            1060,
            "Duplicate column name 'x'",
        ),
    ] {
        let error = session.execute_write(&sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].contains(
            "  `a` int NOT NULL,\n  `b` int DEFAULT NULL,\n  \
             UNIQUE KEY `ub` (`b`) /*!80000 INVISIBLE */\n"
        ),
        "{}",
        created[0][1]
    );
}

/// The session's cop client capability is shared, and a read that met
/// another user of it answered "unary client is already in use by another
/// response": the transport took it with `try_lock` and held it across the
/// whole RPC. Go's client is concurrent and never refuses a caller. This is
/// `executor/foreign_key`'s script, whose first `count(*)` failed every time.
#[test]
fn a_read_never_finds_the_shared_cop_client_in_use() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(140)).unwrap();
    rows(&mut session, "SET @@foreign_key_checks = 1");
    rows(
        &mut session,
        "CREATE TABLE test.t1 (id INT AUTO_INCREMENT KEY, b INT, INDEX (b))",
    );
    rows(
        &mut session,
        "CREATE TABLE test.t2 (id INT, b INT, \
         FOREIGN KEY fk (b) REFERENCES test.t1 (b) ON UPDATE SET NULL)",
    );
    rows(
        &mut session,
        "INSERT INTO test.t1 (b) VALUES (1), (2), (3), (4), (5), (6), (7), (8)",
    );
    for _ in 0..12 {
        rows(
            &mut session,
            "INSERT INTO test.t1 (b) SELECT id FROM test.t1",
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM test.t1")),
        [["32768"]]
    );
}

/// Go's foreign-key guards on the production node, answers from the Go
/// oracle for the same script: `AlterTable` names every anonymous key of one
/// ALTER before any spec runs (`fk_1`, `fk_2`); `checkDropColumnWithForeignKeyConstraint`
/// keeps a key's columns, on either side (1828, 1829); and
/// `checkIndexNeededInForeignKey` / `checkOperateDropIndexUseByForeignKey`
/// keep the index a key needs, however the ALTER is bundled (1553). This node
/// refused the bundled ALTER and accepted every drop.
#[test]
fn foreign_keys_guard_their_columns_and_indexes_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(138)).unwrap();
    rows(&mut session, "SET @@foreign_key_checks = 1");
    rows(
        &mut session,
        "CREATE TABLE test.p (id INT KEY, b INT, INDEX ib (b))",
    );
    rows(&mut session, "CREATE TABLE test.c (a INT, b INT)");
    session
        .execute_write(
            "ALTER TABLE test.c ADD FOREIGN KEY (a) REFERENCES test.p (id), \
             ADD FOREIGN KEY (b) REFERENCES test.p (b)",
        )
        .unwrap();
    let keys = "  KEY `fk_1` (`a`),\n  KEY `fk_2` (`b`),\n  \
                CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `p` (`id`),\n  \
                CONSTRAINT `fk_2` FOREIGN KEY (`b`) REFERENCES `p` (`b`)\n";
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c"));
    assert!(created[0][1].contains(keys), "{}", created[0][1]);
    for (sql, code, message) in [
        (
            "ALTER TABLE test.c DROP COLUMN a",
            1828,
            "Cannot drop column 'a': needed in a foreign key constraint 'fk_1'",
        ),
        (
            "ALTER TABLE test.p DROP COLUMN b",
            1829,
            "Cannot drop column 'b': needed in a foreign key constraint 'fk_2' of table 'c'",
        ),
        (
            "ALTER TABLE test.p DROP INDEX ib",
            1553,
            "Cannot drop index 'ib': needed in a foreign key constraint",
        ),
        (
            "ALTER TABLE test.c DROP INDEX fk_1",
            1553,
            "Cannot drop index 'fk_1': needed in a foreign key constraint",
        ),
        (
            "ALTER TABLE test.c ADD INDEX ia (a), DROP INDEX fk_1",
            1553,
            "Cannot drop index 'fk_1': needed in a foreign key constraint",
        ),
        (
            "ALTER TABLE test.c DROP FOREIGN KEY fk_1, DROP INDEX fk_1",
            1553,
            "Cannot drop index 'fk_1': needed in a foreign key constraint",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c"));
    assert!(created[0][1].contains(keys), "{}", created[0][1]);
    session
        .execute_write(
            "ALTER TABLE test.c DROP FOREIGN KEY fk_2, ADD FOREIGN KEY (b) REFERENCES test.p (b)",
        )
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c"));
    assert!(
        created[0][1].contains(
            "  KEY `fk_2` (`b`),\n  CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `p` (`id`),\n  \
             CONSTRAINT `fk_3` FOREIGN KEY (`b`) REFERENCES `p` (`b`)\n"
        ),
        "{}",
        created[0][1]
    );
}

/// Go `checkIsDroppableColumn`, `RenameColumn` and `AddColumn`'s generated
/// checks on the production node, answers from the Go oracle: a column a
/// generated column, an expression index or the partitioning reads stays
/// (3108, 3837), a hidden column can be neither read nor dropped (1054, 1091),
/// and an added generated column may read only generated columns before it
/// (3107). This node accepted every one of these. The table with a
/// multi-valued index is the loader's: it rebuilt the hidden
/// `CAST(... AS ... ARRAY)` column with the DDL checks and dropped the table.
#[test]
fn a_column_others_depend_on_stays_as_go_keeps_it() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(139)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.h (a INT, b INT AS (a + 1), c INT, KEY ((a + c))) \
         PARTITION BY HASH (c) PARTITIONS 2",
    );
    for (sql, code, message) in [
        (
            "ALTER TABLE test.h DROP COLUMN a",
            3108,
            "Column 'a' has a generated column dependency.",
        ),
        (
            "ALTER TABLE test.h DROP COLUMN c",
            3837,
            "Column 'c' has an expression index dependency and cannot be dropped or renamed",
        ),
        (
            "ALTER TABLE test.h RENAME COLUMN c TO cc",
            3837,
            "Column 'c' has an expression index dependency and cannot be dropped or renamed",
        ),
        (
            "ALTER TABLE test.h ADD COLUMN d INT AS (_V$_expression_index_0 + 1)",
            1054,
            "Unknown column '_v$_expression_index_0' in 'generated column function'",
        ),
        (
            "ALTER TABLE test.h ADD COLUMN e INT AS (b + 1) FIRST",
            3107,
            "Generated column can refer only to generated columns defined prior to it.",
        ),
        (
            "ALTER TABLE test.h DROP COLUMN _V$_expression_index_0",
            1091,
            "Can't DROP '_V$_expression_index_0'; check that column/key exists",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(
        &mut session,
        "CREATE TABLE test.m (id INT PRIMARY KEY, a JSON, \
         INDEX idx ((CAST(a->'$.d' AS SIGNED ARRAY))))",
    );
    rows(
        &mut session,
        "INSERT INTO test.m VALUES (1, '{\"d\": [1, 2]}')",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.m WHERE 1 MEMBER OF (a->'$.d')"
        )),
        [["1"]]
    );
}

/// Go `checkIndexNameAndColumns`: an ADD INDEX written without a name takes
/// `GetName4AnonymousIndex`'s name against the stored table, and an expression
/// part indexes a hidden generated column backfilled over the existing rows.
/// This node refused both ("needs an explicit index name", "an expression
/// index is not supported by this node"). The names are the Go oracle's.
#[test]
fn an_added_index_takes_gos_anonymous_name_and_indexes_expressions() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(135)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.ai (a INT, b VARCHAR(10), KEY (a))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ai VALUES (1, 'X'), (2, 'y')",
    );
    for sql in [
        "ALTER TABLE test.ai ADD INDEX (a)",
        "ALTER TABLE test.ai ADD INDEX ((a + 1))",
        "CREATE INDEX ei ON test.ai ((LOWER(b)))",
        "ALTER TABLE test.ai ADD INDEX (b), ADD INDEX ((a * 2))",
    ] {
        rows(&mut session, sql);
    }
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.ai"));
    assert!(
        created[0][1].contains(
            "  KEY `a` (`a`),\n  KEY `a_2` (`a`),\n  KEY `expression_index` ((`a` + 1)),\n  \
             KEY `ei` ((lower(`b`))),\n  KEY `b` (`b`),\n  KEY `expression_index_2` ((`a` * 2))\n"
        ),
        "{}",
        created[0][1]
    );
    for (sql, expected) in [
        (
            "SELECT a FROM test.ai USE INDEX(expression_index) WHERE a + 1 = 2",
            "1",
        ),
        (
            "SELECT a FROM test.ai USE INDEX(ei) WHERE LOWER(b) = 'x'",
            "1",
        ),
        (
            "SELECT a FROM test.ai USE INDEX(expression_index_2) WHERE a * 2 = 4",
            "2",
        ),
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), [[expected]], "{sql}");
    }
    rows(&mut session, "ADMIN CHECK TABLE test.ai");
}

#[test]
fn shared_session_serves_ranges_joins_and_locking_reads_without_table_selection() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(134)).unwrap();
    for name in ["shared_left", "shared_right", "shared_third"] {
        rows(
            &mut session,
            &format!("CREATE TABLE test.{name} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"),
        );
    }
    rows(
        &mut session,
        "INSERT INTO test.shared_left VALUES (-7, 10), (0, 20), (1, 30), (42, 40)",
    );
    rows(
        &mut session,
        "INSERT INTO test.shared_right VALUES (-7, 100), (1, 300), (42, 400)",
    );
    rows(
        &mut session,
        "INSERT INTO test.shared_third VALUES (1, 3000)",
    );
    for (sql, expected) in [
        ("SELECT id FROM test.shared_left WHERE id != 0 AND v >= 20 ORDER BY id", vec![vec!["1"], vec!["42"]]),
        ("SELECT id FROM test.shared_left WHERE id < -9223372036854775808", vec![]),
        ("SELECT l.id, r.v FROM test.shared_left l JOIN test.shared_right r ON l.id=r.id ORDER BY r.v DESC LIMIT 1, 1", vec![vec!["1", "300"]]),
        ("SELECT l.id, r.v, t.v FROM test.shared_left l JOIN test.shared_right r USING (id) JOIN test.shared_third t USING (id)", vec![vec!["1", "300", "3000"]]),
        ("SELECT id FROM test.shared_left WHERE id=1 FOR UPDATE", vec![vec!["1"]]),
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), expected, "{sql}");
    }
    session.control_transaction("BEGIN").unwrap();
    rows(&mut session, "UPDATE test.shared_left SET v=99 WHERE id=1");
    rows(&mut session, "INSERT INTO test.shared_left VALUES (2, 88)");
    rows(&mut session, "DELETE FROM test.shared_left WHERE id=0");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, v FROM test.shared_left WHERE id BETWEEN 0 AND 2 ORDER BY id"
        )),
        [["1", "99"], ["2", "88"]],
    );
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, v FROM test.shared_left WHERE id BETWEEN 0 AND 2 ORDER BY id"
        )),
        [["0", "20"], ["1", "30"]],
    );
}

#[test]
fn shared_session_activation_inherits_globals_and_obeys_set() {
    if crate::isolate_process_globals() {
        return;
    }
    let (stack, users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut ddl = factory.open_session(session_context(133)).unwrap();
    rows(
        &mut ddl,
        "CREATE TABLE test.activation_light (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    );
    let globals = users.global_vars();
    globals.load_from_cluster([
        ("tidb_enable_async_commit".to_owned(), "OFF".to_owned()),
        ("tidb_enable_1pc".to_owned(), "ON".to_owned()),
    ]);
    let mut first = factory.open_session(session_context(131)).unwrap();
    globals.load_from_cluster([("tidb_enable_1pc".to_owned(), "OFF".to_owned())]);
    let mut second = factory.open_session(session_context(132)).unwrap();
    for (session, id, one_pc) in [(&mut first, 1, true), (&mut second, 2, false)] {
        let before = tikv_client::metrics::get_txn_commit_counter();
        session
            .execute_write(&format!(
                "INSERT INTO test.activation_light (id, v) VALUES ({id}, 10)"
            ))
            .unwrap();
        let delta = tikv_client::metrics::get_txn_commit_counter().subtract(before);
        assert_eq!(
            (delta.two_pc, delta.async_commit, delta.one_pc),
            (i64::from(!one_pc), 0, i64::from(one_pc)),
            "{delta:?}"
        );
    }
    first.execute_write("SET tidb_enable_1pc = OFF").unwrap();
    let before = tikv_client::metrics::get_txn_commit_counter();
    first
        .execute_write("UPDATE test.activation_light SET v = v + 1 WHERE id=1")
        .unwrap();
    let delta = tikv_client::metrics::get_txn_commit_counter().subtract(before);
    assert_eq!((delta.two_pc, delta.async_commit, delta.one_pc), (1, 0, 0));
    first.execute_write("SET tidb_enable_1pc = ON, tidb_txn_assertion_level = 'STRICT', tidb_pessimistic_txn_fair_locking = OFF").unwrap();
    first.control_transaction("BEGIN OPTIMISTIC").unwrap();
    first
        .execute_write("UPDATE test.activation_light SET v = v + 1 WHERE id=1")
        .unwrap();
    first.execute_write("SET tidb_enable_1pc = OFF").unwrap();
    let before = tikv_client::metrics::get_txn_commit_counter();
    first.control_transaction("COMMIT").unwrap();
    let delta = tikv_client::metrics::get_txn_commit_counter().subtract(before);
    assert_eq!((delta.two_pc, delta.async_commit, delta.one_pc), (0, 0, 1));
    let error = first
        .execute_write("SET tidb_txn_assertion_level = 'invalid'")
        .unwrap_err();
    assert_eq!(error.code, 1231);
}

#[test]
fn cluster_account_expiry_survives_sql_writes_and_password_changes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(902)).unwrap();
    session
        .execute_write(
            "CREATE USER 'expiry_probe'@'%' IDENTIFIED BY 'pw' PASSWORD EXPIRE INTERVAL 7 DAY",
        )
        .unwrap();
    let policy = |session: &mut ClusterServerSession| {
        displayed(rows(session, "SELECT Password_lifetime, Password_last_changed FROM mysql.user WHERE User='expiry_probe'"))
    };
    assert_eq!(policy(&mut session)[0][0], "7");
    session.execute_write("UPDATE mysql.user SET Password_last_changed='2001-02-03 04:05:06' WHERE User='expiry_probe'").unwrap();
    session
        .execute_write("ALTER USER 'expiry_probe'@'%' ACCOUNT LOCK")
        .unwrap();
    assert_eq!(policy(&mut session), [["7", "2001-02-03 04:05:06"]]);
    session
        .execute_write("ALTER USER 'expiry_probe'@'%' PASSWORD EXPIRE NEVER")
        .unwrap();
    assert_eq!(policy(&mut session), [["0", "2001-02-03 04:05:06"]]);
    session
        .execute_write("ALTER USER 'expiry_probe'@'%' PASSWORD EXPIRE DEFAULT")
        .unwrap();
    assert_eq!(policy(&mut session), [["NULL", "2001-02-03 04:05:06"]]);
    session
        .execute_write("ALTER USER 'expiry_probe'@'%' IDENTIFIED BY 'changed'")
        .unwrap();
    assert_ne!(policy(&mut session)[0][1], "2001-02-03 04:05:06");
    session
        .execute_write("SET PASSWORD FOR 'expiry_probe'@'%' = 'changed-again'")
        .unwrap();
    assert_eq!(policy(&mut session)[0][0], "NULL");
}

#[test]
fn cluster_account_history_rejects_reuse_without_changing_credentials() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(903)).unwrap();
    session
        .execute_write("CREATE USER 'history_probe'@'%' IDENTIFIED BY 'first' PASSWORD HISTORY 3")
        .unwrap();
    session
        .execute_write("ALTER USER 'history_probe'@'%' IDENTIFIED BY 'second'")
        .unwrap();
    let before = displayed(rows(
        &mut session,
        "SELECT authentication_string FROM mysql.user WHERE User='history_probe'",
    ));
    let error = session
        .execute_write("ALTER USER 'history_probe'@'%' IDENTIFIED BY 'first'")
        .unwrap_err();
    assert_eq!(error.code, 3638);
    assert_eq!(error.message, "Cannot use these credentials for 'history_probe@%' because they contradict the password history policy.");
    session
        .execute_write("CREATE USER 'history_other'@'%' IDENTIFIED BY 'old' PASSWORD HISTORY 2")
        .unwrap();
    let other_before = displayed(rows(
        &mut session,
        "SELECT authentication_string FROM mysql.user WHERE User='history_other'",
    ));
    assert_eq!(session.execute_write("ALTER USER 'history_other'@'%' IDENTIFIED BY 'new', 'history_probe'@'%' IDENTIFIED BY 'first'").unwrap_err().code, 3638);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT authentication_string FROM mysql.user WHERE User='history_other'"
        )),
        other_before
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.password_history WHERE User='history_other'"
        )),
        [["1"]]
    );
    // Go ALL includes static role privileges, preserved by the same account image.
    session
        .execute_write("GRANT ALL ON *.* TO 'history_other'@'%'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT Create_role_priv, Drop_role_priv FROM mysql.user WHERE User='history_other'"
        )),
        [["Y", "Y"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW GRANTS FOR 'history_other'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'history_other'@'%'"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT authentication_string FROM mysql.user WHERE User='history_probe'"
        )),
        before
    );
    assert_eq!(displayed(rows(&mut session, "SELECT Password_reuse_history, Password_reuse_time FROM mysql.user WHERE User='history_probe'")), [["3", "NULL"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.password_history WHERE User='history_probe'"
        )),
        [["2"]]
    );

    session
        .execute_write("RENAME USER 'history_probe'@'%' TO 'history_renamed'@'%'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.password_history WHERE User='history_probe'"
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.password_history WHERE User='history_renamed'"
        )),
        [["2"]]
    );
    assert_eq!(
        session
            .execute_write("SET PASSWORD FOR 'history_renamed'@'%' = 'first'")
            .unwrap_err()
            .code,
        3638
    );
    session
        .execute_write(
            "ALTER USER 'history_renamed'@'%' IDENTIFIED WITH caching_sha2_password BY 'first'",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.password_history WHERE User='history_renamed'"
        )),
        [["1"]]
    );
    session
        .execute_write("DROP USER 'history_renamed'@'%'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.password_history WHERE User='history_renamed'"
        )),
        [["0"]]
    );
}

#[test]
fn cluster_account_locking_policy_survives_unrelated_account_writes() {
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(904)).unwrap();
    session.execute_write("CREATE USER 'locking_probe'@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 7 COMMENT 'keep'").unwrap();
    session
        .execute_write("ALTER USER 'locking_probe'@'%' PASSWORD EXPIRE NEVER")
        .unwrap();
    let attributes = displayed(rows(
        &mut session,
        "SELECT User_attributes FROM mysql.user WHERE User='locking_probe'",
    ));
    let value: serde_json::Value = serde_json::from_str(&attributes[0][0]).unwrap();
    assert_eq!(value["Password_locking"]["failed_login_attempts"], 3);
    assert_eq!(value["Password_locking"]["password_lock_time_days"], 7);
    assert_eq!(value["metadata"]["comment"], "keep");
    session.execute_write(r#"UPDATE mysql.user SET User_attributes='{"Password_locking":{"failed_login_attempts":3,"password_lock_time_days":7,"failed_login_count":2,"auto_account_locked":"Y","auto_locked_last_changed":"Sat Feb  3 04:05:06 UTC 2001"},"metadata":{"comment":"keep"},"additional_password":"preserve"}' WHERE User='locking_probe'"#).unwrap();
    session
        .execute_write("ALTER USER 'locking_probe'@'%' PASSWORD EXPIRE DEFAULT")
        .unwrap();
    let stored = users
        .accounts()
        .password_locking("locking_probe", "%")
        .unwrap();
    assert_eq!(stored.failed_login_count, 2);
    assert!(stored.auto_account_locked);
    assert_eq!(stored.auto_locked_last_changed, 981_173_106);
    session
        .execute_write("ALTER USER 'locking_probe'@'%' ACCOUNT UNLOCK")
        .unwrap();
    let reset = users
        .accounts()
        .password_locking("locking_probe", "%")
        .unwrap();
    assert_eq!(reset.failed_login_count, 0);
    assert!(!reset.auto_account_locked);
    assert!(reset.auto_locked_last_changed > stored.auto_locked_last_changed);
    session
        .execute_write(
            "ALTER USER 'locking_probe'@'%' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0",
        )
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(
        &displayed(rows(
            &mut session,
            "SELECT User_attributes FROM mysql.user WHERE User='locking_probe'",
        ))[0][0],
    )
    .unwrap();
    assert!(value.get("Password_locking").is_none());
    assert_eq!(value["metadata"]["comment"], "keep");
    assert_eq!(value["additional_password"], "preserve");
}

#[test]
fn cluster_account_drop_persists_through_its_own_transaction() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(901)).unwrap();
    session
        .execute_write("CREATE USER 'drop_probe'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    session
        .execute_write("GRANT SELECT ON test.* TO 'drop_probe'@'%'")
        .unwrap();
    session.execute_write("DROP USER 'drop_probe'@'%'").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.user WHERE User='drop_probe'"
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.db WHERE User='drop_probe'"
        )),
        [["0"]]
    );
    assert_eq!(
        session
            .execute_write("DROP USER 'drop_probe'@'%'")
            .unwrap_err()
            .code,
        1396
    );
    session
        .execute_write("CREATE USER 'drop_probe'@'%' IDENTIFIED BY 'newpw'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.user WHERE User='drop_probe'"
        )),
        [["1"]]
    );
}

#[test]
fn a_held_record_lock_does_not_suppress_a_new_duplicate_assertion() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(125)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.held_assertion (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut session,
        "INSERT INTO test.held_assertion VALUES (1,10),(2,20)",
    );
    for select in [
        "SELECT * FROM test.held_assertion WHERE id=1 FOR UPDATE",
        "SELECT * FROM test.held_assertion WHERE v=10 FOR UPDATE",
    ] {
        session.control_transaction("BEGIN PESSIMISTIC").unwrap();
        assert_eq!(displayed(rows(&mut session, select)), [["1", "10"]]);
        let error = session
            .execute_write("INSERT INTO test.held_assertion VALUES (1,99)")
            .err();
        session.control_transaction("ROLLBACK").unwrap();
        assert!(matches!(error, Some(ref error) if error.code == 1062),
            "an already-owned existing key must still reject the INSERT at statement end: {error:?}");
    }
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    assert!(rows(
        &mut session,
        "SELECT * FROM test.held_assertion WHERE id=3 FOR UPDATE"
    )
    .is_empty());
    rows(
        &mut session,
        "INSERT INTO test.held_assertion VALUES (3,30)",
    );
    rows(
        &mut session,
        "UPDATE test.held_assertion SET v=31 WHERE id=3",
    );
    session.control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.held_assertion ORDER BY id"
        )),
        [["1", "10"], ["2", "20"], ["3", "31"]]
    );
}

#[test]
fn system_table_hidden_ids_use_the_full_counter_key() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(119))
        .expect("session");
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('system counter allocation', 'SELECT 1', 'test', 'disabled', '2026-09-07 00:00:00', '2026-09-07 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'system_counter_probe')");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT original_sql FROM mysql.bind_info WHERE sql_digest='system_counter_probe'"
        )),
        [["system counter allocation"]]
    );
}

#[test]
fn global_binding_reload_decodes_timestamps_in_its_session_timezone() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(124))
        .expect("session");
    rows(&mut session, "SET GLOBAL time_zone = '+08:00'");
    rows(&mut session, "SET SESSION time_zone = '+00:00'");
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('select ?','SELECT 1','test','enabled','2026-09-07 00:00:00.123456','2026-09-07 01:00:00.654321','utf8mb4','utf8mb4_bin','manual','binding_timezone_probe')");
    let bindings = displayed(rows(&mut session, "SHOW GLOBAL BINDINGS"));
    let binding = bindings
        .iter()
        .find(|row| row[9] == "binding_timezone_probe")
        .expect("committed binding appears in the shared cache");
    assert_eq!(binding[4], "2026-09-07 08:00:00.123456");
    assert_eq!(binding[5], "2026-09-07 09:00:00.654321");
}

#[test]
fn global_bindings_do_not_depend_on_unrelated_staged_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(117))
        .expect("writer");
    rows(
        &mut writer,
        "CREATE TABLE test.binding_visibility (id BIGINT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.binding_visibility VALUES (1,20),(2,30),(3,40),(4,50),(5,60),(6,70)",
    );
    // A committed storage row, as if a peer had created the global binding.
    // The matcher must not infer its existence from the reader's write buffer.
    rows(&mut writer, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('select v from test.binding_visibility where v = ?', 'SELECT v FROM test.binding_visibility USE INDEX () WHERE v = 20', 'test', 'enabled', '2026-09-07 00:00:00', '2026-09-07 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'binding_visibility_probe')");
    let mut reader = stack
        .factory
        .open_session(session_context(118))
        .expect("reader");
    reader
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin reader");
    for dirty in [true, false] {
        if dirty {
            rows(
                &mut reader,
                "UPDATE test.binding_visibility SET v=v+1 WHERE id>1",
            );
        } else {
            reader
                .control_transaction("ROLLBACK")
                .expect("rollback reader");
            reader
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin reader");
        }
        assert_eq!(
            displayed(rows(
                &mut reader,
                "SELECT v FROM test.binding_visibility WHERE v=20"
            )),
            [["20"]]
        );
        assert_eq!(
            displayed(rows(&mut reader, "SELECT @@last_plan_from_binding")),
            [["1"]],
            "global binding must apply with dirty={dirty}"
        );
    }
    reader
        .control_transaction("ROLLBACK")
        .expect("rollback reader");

    let assert_binding = |reader: &mut crate::cluster_session_node::ClusterServerSession,
                          expected: &str,
                          phase: &str| {
        let (result, ops) = tidb_executor::storage::capture_storage_ops(|| {
            rows(reader, "SELECT v FROM test.binding_visibility WHERE v=20")
        });
        assert_eq!(displayed(result), [["20"]]);
        assert_eq!(
            ops.cop_scans, 1,
            "only the user table may be scanned: {ops:?}"
        );
        assert_eq!(
            displayed(rows(reader, "SELECT @@last_plan_from_binding")),
            [[expected]],
            "{phase}"
        );
    };
    // Uncommitted binding writes must not enter any session's planner image.
    writer
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    rows(
        &mut writer,
        "UPDATE mysql.bind_info SET status='disabled', update_time='2026-09-07 00:00:01' WHERE sql_digest='binding_visibility_probe'",
    );
    assert_binding(&mut reader, "1", "reader during uncommitted disable");
    assert_binding(&mut writer, "1", "writer during uncommitted disable");
    writer
        .control_transaction("ROLLBACK")
        .expect("rollback writer");
    assert_binding(&mut reader, "1", "after rollback");

    for (version, (status, expected)) in [("disabled", "0"), ("enabled", "1"), ("deleted", "0")]
        .into_iter()
        .enumerate()
    {
        writer
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        rows(&mut writer, &format!("UPDATE mysql.bind_info SET status='{status}', update_time='2026-09-07 00:00:0{}' WHERE sql_digest='binding_visibility_probe'", version + 1));
        writer.control_transaction("COMMIT").expect("commit writer");
        for _ in 0..3 {
            assert_binding(&mut reader, expected, status);
        }
    }
}

#[test]
fn unchanged_updates_lock_only_matched_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(122))
        .expect("writer");
    let mut reader = stack
        .factory
        .open_session(session_context(123))
        .expect("reader");
    rows(
        &mut writer,
        "CREATE TABLE test.unchanged_lock (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.unchanged_lock VALUES (1,20),(2,30)",
    );
    writer
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    let write = writer
        .execute_write("UPDATE test.unchanged_lock SET v=v WHERE v=20")
        .expect("unchanged update")
        .expect("write result");
    assert_eq!(write.affected_rows, 0);
    reader
        .execute_write("SET innodb_lock_wait_timeout=1")
        .expect("bounded lock wait");
    reader
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin reader");
    // Go TestSelectForUpdateNoWait and TestInnodbLockWaitTimeout: range
    // locking must preserve NOWAIT, WAIT n and the session timeout.
    for (clause, code) in [("NOWAIT", 3572), ("WAIT 1", 1205), ("", 1205)] {
        let sql = format!("SELECT v FROM test.unchanged_lock WHERE v=20 FOR UPDATE {clause}");
        let result = reader.execute(&sql);
        assert!(
            matches!(result, Err(ref error) if error.code == code),
            "locking range {clause} must report {code}; got {:?}",
            result.as_ref().err()
        );
    }
    let result = reader.execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=1");
    assert!(
        matches!(result, Err(ref error) if error.code == 1205),
        "the unchanged matched row must remain locked; error={:?}",
        result.as_ref().err()
    );
    let untouched = reader
        .execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=2")
        .expect("a rejected row is not locked")
        .expect("write result");
    assert_eq!(untouched.affected_rows, 1);
    writer
        .control_transaction("ROLLBACK")
        .expect("release writer");
    reader
        .control_transaction("ROLLBACK")
        .expect("release reader");
}

#[test]
fn global_binding_commands_commit_outside_the_user_transaction() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(120))
        .expect("writer");
    let mut reader = stack
        .factory
        .open_session(session_context(121))
        .expect("reader");
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT original_sql, source FROM mysql.bind_info WHERE status='builtin'"
        )),
        [["builtin_pseudo_sql_for_bind_lock", "builtin"]],
        "bootstrap supplies the shared binding-writer lock row"
    );
    rows(
        &mut writer,
        "CREATE TABLE test.binding_command_visibility (id BIGINT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.binding_command_visibility VALUES (1,20)",
    );
    writer
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    writer
        .execute_write("UPDATE test.binding_command_visibility SET v=21 WHERE id=1")
        .expect("staged write");
    rows(&mut writer, "CREATE GLOBAL BINDING FOR SELECT v FROM test.binding_command_visibility WHERE v=20 USING SELECT v FROM test.binding_command_visibility USE INDEX() WHERE v=20");
    assert_eq!(
        displayed(rows(
            &mut writer,
            "SELECT v FROM test.binding_command_visibility WHERE id=1"
        )),
        [["21"]]
    );
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.binding_command_visibility WHERE id=1"
        )),
        [["20"]]
    );
    let count = "SELECT COUNT(*) FROM mysql.bind_info WHERE status='enabled'";
    assert_eq!(
        displayed(rows(&mut reader, count)),
        [["1"]],
        "binding committed while caller transaction remains open"
    );
    writer
        .control_transaction("ROLLBACK")
        .expect("rollback user data");
    assert_eq!(
        displayed(rows(&mut reader, count)),
        [["1"]],
        "binding survives caller rollback"
    );
    assert_eq!(
        displayed(rows(
            &mut writer,
            "SELECT v FROM test.binding_command_visibility WHERE id=1"
        )),
        [["20"]]
    );
    for (command, expected) in [
        (
            "SET BINDING DISABLED FOR SELECT v FROM test.binding_command_visibility WHERE v=20",
            "disabled",
        ),
        (
            "SET BINDING ENABLED FOR SELECT v FROM test.binding_command_visibility WHERE v=20",
            "enabled",
        ),
        (
            "DROP GLOBAL BINDING FOR SELECT v FROM test.binding_command_visibility WHERE v=20",
            "deleted",
        ),
    ] {
        writer
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin user transaction");
        writer
            .execute_write("UPDATE test.binding_command_visibility SET v=21 WHERE id=1")
            .expect("staged user write");
        rows(&mut writer, command);
        let status = "SELECT status FROM mysql.bind_info WHERE status!='builtin'";
        assert_eq!(
            displayed(rows(&mut reader, status)),
            [[expected]],
            "binding command commits independently: {command}"
        );
        assert_eq!(
            displayed(rows(
                &mut writer,
                "SELECT v FROM test.binding_command_visibility WHERE id=1"
            )),
            [["21"]]
        );
        assert_eq!(
            displayed(rows(
                &mut reader,
                "SELECT v FROM test.binding_command_visibility WHERE id=1"
            )),
            [["20"]]
        );
        writer
            .control_transaction("ROLLBACK")
            .expect("rollback user transaction");
        assert_eq!(
            displayed(rows(&mut reader, status)),
            [[expected]],
            "binding command survives caller rollback: {command}"
        );
    }
}

fn handle_stats_schema_change(session: &mut ClusterServerSession, event: &SchemaChangeEvent) {
    session
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("stats subscriber transaction begins");
    if let Err(error) = session.stage_stats_notifier_event(event) {
        session
            .control_transaction("ROLLBACK")
            .expect("failed stats subscriber transaction rolls back");
        panic!("stats subscriber handles the source event: {error:?}");
    }
    session
        .control_transaction("COMMIT")
        .expect("stats subscriber transaction commits");
}

// Go statistics DDL tests explicitly HandleNextDDLEventWithTxn before reading
// statistics. This fixture does not campaign the background statistics owner.
fn drain_stats_ddl_events(
    factory: &Arc<ClusterSessionFactory>,
    session: &mut ClusterServerSession,
) {
    let notifier = super::super::ddl_notifier::build_notifier(factory, Duration::ZERO);
    drain_stats_ddl_events_with_notifier(session, notifier);
}

fn drain_stats_ddl_events_with_notifier(
    session: &mut ClusterServerSession,
    notifier: Arc<tidb_ddl_notifier::DdlNotifier>,
) {
    tidb_owner::Listener::on_become_owner(notifier.as_ref());
    // Go HandleNextDDLEventWithTxn waits for the statistics transaction,
    // not for an uninitialized auto-analyze queue or another subscriber.
    // This bit is committed in the same transaction as the stats changes.
    let stats_bit = 1_u64 << tidb_ddl_notifier::STATS_META_HANDLER_ID.0;
    let pending_stats_sql = format!(
        "SELECT count(*) FROM mysql.tidb_ddl_notifier WHERE processed_by_flag & {stats_bit} = 0"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let pending = displayed(rows(session, &pending_stats_sql));
        if pending == [["0"]] {
            notifier.stop();
            return;
        }
        if std::time::Instant::now() >= deadline {
            notifier.stop();
            panic!("DDL notifier did not commit pending statistics events: {pending:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn stats_ddl_wait_does_not_require_an_unready_other_subscriber() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(125))
        .expect("session");
    rows(&mut session, "CREATE TABLE test.stats_wait (a INT)");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_wait")
        .unwrap()
        .1
        .id;
    let notifier = super::super::ddl_notifier::build_notifier(&stack.factory, Duration::ZERO);
    // Deterministically model the uninitialized priority queue's retry result
    // without modifying process-global auto-analyze settings in parallel tests.
    notifier.register_handler(
        tidb_ddl_notifier::TEST_HANDLER_ID,
        Arc::new(|_, _| Err(tidb_ddl_notifier::NotifierError::NotReadyRetryLater)),
    );
    drain_stats_ddl_events_with_notifier(&mut session, notifier);
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {table_id}"
            )
        )),
        [["0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier"
        )),
        [["1"]],
        "the unfinished subscriber's event must remain durable"
    );
}

fn partition_payload(ids: &[i64]) -> PartitionInfo {
    PartitionInfo {
        definitions: GoSharedSlice::from_vec(
            ids.iter()
                .copied()
                .map(|id| PartitionDefinition {
                    id,
                    ..PartitionDefinition::default()
                })
                .collect(),
        ),
        num: ids.len() as u64,
        ..PartitionInfo::default()
    }
}

/// Pinned Go `checkExchangePartitionRecordValidation` and
/// `onExchangeTablePartition`: the default `WITH VALIDATION` reads the
/// standalone rows before publication, rejects 1737 atomically when one does
/// not route to the named partition, and otherwise makes the two old physical
/// record sets visible under their exchanged metadata IDs.
#[test]
fn exchange_partition_validates_and_swaps_real_rows_atomically() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(152))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE exchange_pt (id BIGINT PRIMARY KEY CLUSTERED) \
         PARTITION BY RANGE (id) (\
           PARTITION p0 VALUES LESS THAN (10),\
           PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    );
    rows(
        &mut session,
        "CREATE TABLE exchange_nt (id BIGINT PRIMARY KEY CLUSTERED)",
    );
    rows(&mut session, "INSERT INTO exchange_pt VALUES (1)");
    rows(&mut session, "INSERT INTO exchange_nt VALUES (5)");
    rows(
        &mut session,
        "ALTER TABLE exchange_pt EXCHANGE PARTITION p0 WITH TABLE exchange_nt",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM exchange_pt ORDER BY id")),
        [["5"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM exchange_nt ORDER BY id")),
        [["1"]]
    );

    rows(
        &mut session,
        "CREATE TABLE exchange_bad_pt (id BIGINT PRIMARY KEY CLUSTERED) \
         PARTITION BY RANGE (id) (\
           PARTITION p0 VALUES LESS THAN (10),\
           PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    );
    rows(
        &mut session,
        "CREATE TABLE exchange_bad_nt (id BIGINT PRIMARY KEY CLUSTERED)",
    );
    rows(&mut session, "INSERT INTO exchange_bad_pt VALUES (2)");
    rows(&mut session, "INSERT INTO exchange_bad_nt VALUES (15)");
    let error = match session
        .execute("ALTER TABLE exchange_bad_pt EXCHANGE PARTITION p0 WITH TABLE exchange_bad_nt")
    {
        Err(error) => error,
        Ok(_) => panic!("a row for p1 cannot be exchanged into p0"),
    };
    assert_eq!(error.code, 1737);
    assert_eq!(error.state, *b"HY000");
    assert_eq!(
        error.message,
        "Found a row that does not match the partition"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM exchange_bad_pt ORDER BY id"
        )),
        [["2"]],
        "the failed DDL leaves partition metadata and physical rows unchanged"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM exchange_bad_nt ORDER BY id"
        )),
        [["15"]]
    );
}

/// Pinned `TestExchangeAPartition`: the subscriber reads the exchanged
/// partition and standalone-table metadata and applies Go's count and modify
/// deltas to the logical/global row.
#[test]
fn exchange_partition_event_updates_global_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(83))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_exchange_pt (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), \
         PARTITION p3 VALUES LESS THAN (21))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_exchange_pt VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_exchange_pt");
    rows(
        &mut session,
        "CREATE TABLE stats_exchange_nt (a INT PRIMARY KEY, b INT, INDEX idx(b))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_exchange_nt VALUES (1,2),(2,2),(3,2),(4,2),(5,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_exchange_nt");

    let (partitioned, partition_id, standalone) = {
        let catalog = stack.factory.catalog.load();
        let partitioned = catalog
            .find_table("test", "stats_exchange_pt")
            .expect("partitioned table is published")
            .1
            .clone_like_go();
        let partition_id = partitioned
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot()
            .into_iter()
            .find(|definition| definition.name.lowercase() == "p0")
            .expect("p0 exists")
            .id;
        let standalone = catalog
            .find_table("test", "stats_exchange_nt")
            .expect("standalone table is published")
            .1
            .clone_like_go();
        (partitioned, partition_id, standalone)
    };
    let logical_id = partitioned.id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["5", "0"]]
    );
    handle_stats_schema_change(
        &mut session,
        &SchemaChangeEvent::exchange_partition(
            partitioned,
            partition_payload(&[partition_id]),
            standalone,
        ),
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["8", "7"]]
    );
}

/// Pinned Go `pkg/ddl/constraint_test.go`: CHECK DDL is submitted to the
/// durable queue, completed by the DDL owner, removed from the active table,
/// and enforced by subsequent writes.
#[test]
fn check_constraint_runs_through_the_owner_job_queue() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(153))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET @@global.tidb_enable_check_constraint = ON",
    );
    assert!(
        session
            .session
            .ddl_statement_context()
            .enable_check_constraint(),
        "the query-shaped DDL context must read the enabled global variable"
    );
    rows(&mut session, "CREATE TABLE queued_check (a INT)");
    rows(&mut session, "INSERT INTO queued_check VALUES (1)");
    rows(
        &mut session,
        "ALTER TABLE queued_check ADD CONSTRAINT positive CHECK (a > 0)",
    );

    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.tidb_ddl_job",
        )),
        [["0"]],
        "a completed owner job is no longer active"
    );
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE queued_check"));
    assert!(
        shown[0][1].contains("CONSTRAINT `positive` CHECK ((`a` > 0))"),
        "the terminal owner phase must publish the CHECK: {shown:?}"
    );
    let error = match session.execute("INSERT INTO queued_check VALUES (-1)") {
        Err(error) => error,
        Ok(_) => panic!("the owner-published CHECK must be enforced"),
    };
    assert_eq!(error.code, 3819);
    assert_eq!(error.message, "Check constraint 'positive' is violated.");
}

/// Pinned `pkg/ddl/notifier.TestPublishToTableStore`, `TestBasicPubSub`, and
/// `TestDeliverOrderAndCleanup` over the real bootstrapped notifier table.
#[test]
fn ddl_notifier_table_store_delivers_in_order_and_cleans_up() {
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(151))
        .expect("client session opens");
    assert_eq!(
        displayed(rows(
            &mut client,
            "SELECT variable_value FROM mysql.tidb \
             WHERE variable_name = 'ddl_table_version'",
        )),
        [["4"]]
    );
    rows(&mut client, "DELETE FROM mysql.tidb_ddl_notifier");

    let pool: Arc<dyn SessionPool> = Arc::new(ClusterNotifierSessionPool::new(
        factory.advanced_sys_session_pool(),
    ));
    let store = Arc::new(ClusterNotifierTableStore);
    let mut publisher = pool.get().expect("publisher session");
    for (job_id, table_name) in [(1, "t1"), (2, "t2#special-char?in'name"), (3, "t3")] {
        publish_schema_change_to_store(
            publisher.as_mut(),
            job_id,
            -1,
            SchemaChangeEvent::create_table(tidb_model::TableInfo {
                id: 999 + job_id,
                name: tidb_ast::CiString::new(table_name),
                ..tidb_model::TableInfo::default()
            }),
            store.as_ref(),
        )
        .expect("event publishes");
    }
    pool.put(publisher);

    let mut reader = pool.get().expect("list session");
    let mut list = store.list(reader.as_mut()).expect("list starts");
    let listed = list.read(reader.as_mut(), 2).expect("first page");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].event.create_table_info().name.original(), "t1");
    assert_eq!(
        listed[1].event.create_table_info().name.original(),
        "t2#special-char?in'name"
    );
    let listed = list.read(reader.as_mut(), 2).expect("second page");
    assert_eq!(
        listed
            .iter()
            .map(|change| (change.ddl_job_id, change.sub_job_id))
            .collect::<Vec<_>>(),
        [(3, -1)]
    );
    assert_eq!(listed[0].event.create_table_info().name.original(), "t3");
    list.close(reader.as_mut());
    pool.put(reader);

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler: Handler = {
        let seen = Arc::clone(&seen);
        Arc::new(move |_, event| {
            seen.lock().unwrap().push(event.create_table_info().id);
            Ok(())
        })
    };
    let notifier = DdlNotifier::new(
        Arc::clone(&pool),
        Arc::clone(&store) as Arc<dyn Store>,
        Duration::from_millis(10),
    );
    notifier.register_handler(TEST_HANDLER_ID, handler);
    tidb_owner::Listener::on_become_owner(&notifier);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = displayed(rows(
            &mut client,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier",
        ));
        if remaining == [["0"]] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "notifier did not process and clean up every event"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    tidb_owner::Listener::on_retire_owner(&notifier);
    assert_eq!(*seen.lock().unwrap(), [1000, 1001, 1002]);
}

/// Pinned notifier `processEventForHandler` plus statistics
/// `HandleDDLEvent`: the internal session opens a real pessimistic transaction
/// before the subscriber stages mutations, commits them with the processed
/// flag, and then removes the fully processed event.
#[test]
fn stats_notifier_uses_a_real_internal_transaction_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(85))
        .expect("client session opens");
    rows(&mut client, "USE test");
    rows(
        &mut client,
        "CREATE TABLE stats_notifier_transaction (a INT)",
    );
    rows(
        &mut client,
        "INSERT INTO stats_notifier_transaction VALUES (1),(2)",
    );
    rows(&mut client, "ANALYZE TABLE stats_notifier_transaction");
    let table_id = factory
        .catalog
        .load()
        .find_table("test", "stats_notifier_transaction")
        .expect("table is published")
        .1
        .id;
    let version_before = displayed(rows(
        &mut client,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is unsigned");
    rows(&mut client, "DELETE FROM mysql.tidb_ddl_notifier");
    // The event row is deleted only once every registered handler has
    // processed it. With no auto-analyze worker running (stats lease zero),
    // the priority queue is never initialized, and Go's
    // `AnalysisPriorityQueue.HandleDDLEvent` answers `ErrNotReadyRetryLater`
    // while `RunAutoAnalyze` is on, keeping the row. With auto-analyze off it
    // ignores the event, which leaves only the statistics subscriber under
    // test.
    rows(&mut client, "SET GLOBAL tidb_enable_auto_analyze = OFF");

    let pool: Arc<dyn SessionPool> = Arc::new(ClusterNotifierSessionPool::new(
        factory.advanced_sys_session_pool(),
    ));
    let mut publisher = pool.get().expect("publisher session");
    publish_schema_change_to_store(
        publisher.as_mut(),
        9_001,
        -1,
        SchemaChangeEvent::flashback_cluster(),
        &ClusterNotifierTableStore,
    )
    .expect("flashback event publishes");
    pool.put(publisher);

    let notifier = super::super::ddl_notifier::build_notifier(&factory, Duration::ZERO);
    tidb_owner::Listener::on_become_owner(notifier.as_ref());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let version_after = loop {
        let pending = displayed(rows(
            &mut client,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier",
        ));
        let version = displayed(rows(
            &mut client,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("stats version is unsigned");
        if pending == [["0"]] && version != version_before {
            break version;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "stats notifier did not commit the subscriber mutation and clean up the event: \
             pending {pending:?}, version {version} (before {version_before})"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    tidb_owner::Listener::on_retire_owner(notifier.as_ref());
    assert!(version_after > version_before);
}

/// Pinned Go `TestStatsCacheShouldNotCacheTemporaryTable`: GLOBAL temporary
/// metadata is published by an ordinary DDL job, its rows remain
/// connection-local and are deleted at commit, and only explicit `ANALYZE`
/// replaces pseudo statistics for that session.
#[test]
fn global_temporary_analyze_uses_session_rows_and_statistics() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(139))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE global_stats (a INT) ON COMMIT DELETE ROWS",
    );
    // Statistics reloads can concurrently add system-table entries. Resolve
    // this table from metadata rather than guessing from new cache keys.
    let temporary_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "global_stats")
        .expect("GLOBAL temporary DDL publishes its table metadata")
        .1
        .id;
    assert!(stack
        .factory
        .stats()
        .load()
        .get(&temporary_id)
        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
        .is_none());
    rows(
        &mut session,
        "INSERT INTO global_stats VALUES (1), (2), (3)",
    );
    assert!(
        rows(&mut session, "SELECT * FROM global_stats").is_empty(),
        "GLOBAL temporary rows are deleted when the autocommit transaction ends"
    );
    let before = displayed(rows(&mut session, "EXPLAIN SELECT * FROM global_stats"));
    assert!(
        before
            .iter()
            .flatten()
            .any(|value| value.contains("stats:pseudo")),
        "ordinary access must not publish temporary statistics: {before:?}"
    );

    rows(&mut session, "ANALYZE TABLE global_stats");
    let snapshot = stack.factory.stats().load();
    let analyzed = snapshot
        .get(&temporary_id)
        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
        .expect("explicit ANALYZE publishes a real GLOBAL temporary cache object");
    assert_eq!(analyzed.hist_coll.column_count(), 1);
    assert!(!analyzed.hist_coll.pseudo);

    let after = displayed(rows(&mut session, "EXPLAIN SELECT * FROM global_stats"));
    assert!(
        after
            .iter()
            .flatten()
            .any(|value| value.contains("stats:pseudo")),
        "an analyzed empty table retains Go's query-time pseudo policy: {after:?}"
    );
}

/// Pinned `lockstats.TestRemoveLockedTables`: the live cluster path wraps the
/// complete operation in Go's independent pessimistic transaction and merges
/// a partition lock delta into both physical and logical metadata.
#[test]
fn stats_lock_live_pessimistic_transaction_merges_partition_delta_like_go() {
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut session = factory
        .open_session(session_context(85))
        .expect("client session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE lock_live (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    rows(
        &mut session,
        "INSERT INTO lock_live VALUES (1),(2),(11),(12)",
    );
    rows(&mut session, "ANALYZE TABLE lock_live");
    let catalog = factory.catalog.load();
    let table = catalog
        .find_table("test", "lock_live")
        .expect("lock table is published")
        .1;
    let table_id = table.id;
    let partition = table.get_partition_info().expect("partition metadata");
    let partition = partition.read();
    let partition_ids = partition
        .definitions
        .map_visible(|definition| definition.id);
    let p0_id = partition_ids[0];
    let p1_id = partition_ids[1];
    drop(partition);
    drop(catalog);

    rows(&mut session, "LOCK STATS lock_live");
    rows(
        &mut session,
        &format!(
            "UPDATE mysql.stats_table_locked SET count = -1, modify_count = 1 \
             WHERE table_id = {p0_id}"
        ),
    );
    rows(&mut session, "UNLOCK STATS lock_live");

    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT table_id, count, modify_count FROM mysql.stats_meta \
                 WHERE table_id IN ({table_id},{p0_id},{p1_id}) ORDER BY table_id"
            ),
        )),
        [
            [table_id.to_string(), "3".to_owned(), "1".to_owned()],
            [p0_id.to_string(), "1".to_owned(), "1".to_owned()],
            [p1_id.to_string(), "2".to_owned(), "0".to_owned()],
        ]
    );
}

/// Pinned `autoanalyze/exec.TestExecAutoAnalyzes`: the live restricted
/// session consumes the complete auto-analyze option set, invokes the system
/// process tracker, releases the allocated process ID, and publishes the
/// ordinary version-2 statistics result.
struct ObservedSystemAnalyze {
    inner: Arc<dyn crate::cluster_analyze_seam::ClusterAnalyze>,
    processes: tidb_session::process::ProcessRegistry,
    kill: Option<bool>,
    process_id: u64,
    factory: std::sync::Weak<ClusterSessionFactory>,
    observed: Arc<AtomicBool>,
    interrupted: Arc<AtomicBool>,
}

impl crate::cluster_analyze_seam::ClusterAnalyze for ObservedSystemAnalyze {
    fn execute(
        &self,
        statement: &tidb_exec::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_exec::real_tikv_analyze::ApproximateTableCountProvider,
        killer: &tidb_util::sqlkiller::SqlKiller,
        historical_stats_enabled: &dyn Fn() -> bool,
        jobs: &dyn tidb_exec::real_tikv_analyze::AnalyzeJobLifecycle,
    ) -> Result<tidb_exec::real_tikv_analyze::ClusterAnalyzeReport, crate::sql_node::SqlQueryError>
    {
        let process = self
            .processes
            .snapshot()
            .into_iter()
            .find(|row| row.id == self.process_id)
            .expect("running system task is visible to the shared manager");
        assert!(process
            .info
            .as_deref()
            .unwrap()
            .to_ascii_lowercase()
            .starts_with("analyze table"));
        self.observed.store(true, Ordering::SeqCst);
        if let Some(query) = self.kill {
            let factory = self.factory.upgrade().unwrap();
            let mut admin = factory.open_session(session_context(87)).unwrap();
            let kind = if query { "QUERY" } else { "CONNECTION" };
            rows(&mut admin, &format!("KILL {kind} {}", self.process_id));
            self.interrupted
                .store(killer.handle_signal().is_some(), Ordering::SeqCst);
        }
        self.inner.execute(
            statement,
            resource_group,
            approximate_counts,
            killer,
            historical_stats_enabled,
            jobs,
        )
    }
}

/// Kills the running auto-analyze through SQL `KILL QUERY`, by the process ID
/// the shared process list shows for it, and records that ID.
struct KillingSystemAnalyze {
    inner: Arc<dyn crate::cluster_analyze_seam::ClusterAnalyze>,
    processes: tidb_session::process::ProcessRegistry,
    factory: std::sync::Weak<ClusterSessionFactory>,
    process_id: Arc<AtomicU64>,
}

impl crate::cluster_analyze_seam::ClusterAnalyze for KillingSystemAnalyze {
    fn execute(
        &self,
        statement: &tidb_exec::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_exec::real_tikv_analyze::ApproximateTableCountProvider,
        killer: &tidb_util::sqlkiller::SqlKiller,
        historical_stats_enabled: &dyn Fn() -> bool,
        jobs: &dyn tidb_exec::real_tikv_analyze::AnalyzeJobLifecycle,
    ) -> Result<tidb_exec::real_tikv_analyze::ClusterAnalyzeReport, crate::sql_node::SqlQueryError>
    {
        let process_id = self
            .processes
            .snapshot()
            .into_iter()
            .find(|row| {
                row.info
                    .as_deref()
                    .is_some_and(|info| info.to_ascii_lowercase().starts_with("analyze table"))
            })
            .expect("the running auto-analyze is in the shared process list")
            .id;
        self.process_id.store(process_id, Ordering::SeqCst);
        let factory = self.factory.upgrade().unwrap();
        let mut admin = factory.open_session(session_context(88)).unwrap();
        rows(&mut admin, &format!("KILL QUERY {process_id}"));
        self.inner.execute(
            statement,
            resource_group,
            approximate_counts,
            killer,
            historical_stats_enabled,
            jobs,
        )
    }
}

/// Go hands the stats handle `Domain.NextConnID` / `ReleaseConnID` as the
/// auto-analyze process-ID generator, so the process ID names this node's
/// server ID and `KILL` reaches it (`executeKillStmt` routes by
/// `gcid.ServerID`). The production auto-analyze must draw its ID from the
/// node's connection-ID allocator, not reuse an internal session's own
/// identifier, which `KILL` cannot route.
#[test]
fn production_auto_analyze_is_killable_by_its_process_id() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(86))
        .expect("client session opens");
    rows(&mut client, "USE test");
    rows(
        &mut client,
        "CREATE TABLE auto_kill_live (a INT, INDEX idx_a(a))",
    );
    rows(&mut client, "INSERT INTO auto_kill_live VALUES (1),(2),(3)");

    let process_id = Arc::new(AtomicU64::new(0));
    let pool = factory.advanced_sys_session_pool();
    tidb_stats_handle_util::call_with_sctx(
        pool.as_ref(),
        |context| {
            context.with_session(|session| {
                session.analyze = Arc::new(KillingSystemAnalyze {
                    inner: Arc::clone(&session.analyze),
                    processes: factory.processes(),
                    factory: Arc::downgrade(&factory),
                    process_id: Arc::clone(&process_id),
                });
                Ok(())
            })
        },
        &[],
    )
    .expect("the pooled system session takes the seam");

    let source = super::super::ClusterPriorityQueueSource {
        factory: Arc::downgrade(&factory),
        stats_lease: Duration::ZERO,
        session_pool: Arc::clone(&pool),
    };
    let analyzed = tidb_stats_handle_autoanalyze_priorityqueue::AnalysisJobContext::auto_analyze(
        &source,
        2,
        false,
        "analyze table %n.%n",
        &["test".to_owned(), "auto_kill_live".to_owned()],
    );

    let process_id = process_id.load(Ordering::SeqCst);
    let (gcid, truncated) = tidb_util::globalconn::parse_conn_id(process_id)
        .expect("an allocator-minted process ID parses");
    assert!(!truncated);
    assert_eq!(
        gcid.server_id,
        tidb_util::globalconn::SERVER_ID_FOR_STANDALONE,
        "process ID {process_id} names this node's server ID"
    );
    assert!(
        !analyzed,
        "KILL QUERY {process_id} must interrupt the auto-analyze"
    );
}

#[test]
fn internal_process_batch_auto_analyze_kill_query() {
    run_tracked_auto_analyze(Some(true));
}

#[test]
fn internal_process_batch_auto_analyze_kill_connection() {
    run_tracked_auto_analyze(Some(false));
}

#[test]
fn auto_analyze_exec_uses_live_tracking_and_current_session_like_go() {
    run_tracked_auto_analyze(None);
}

fn run_tracked_auto_analyze(kill: Option<bool>) {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(86))
        .expect("client session opens");
    rows(&mut client, "USE test");
    rows(
        &mut client,
        "CREATE TABLE auto_exec_live (a INT, b INT, INDEX idx_a(a))",
    );
    let table_id = factory
        .catalog
        .load()
        .find_table("test", "auto_exec_live")
        .expect("auto-analyze table is published")
        .1
        .id;
    rows(
        &mut client,
        "INSERT INTO auto_exec_live VALUES (1,1),(2,2),(3,3)",
    );
    rows(
        &mut client,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut client, "SET GLOBAL tidb_enable_analyze_snapshot = ON");

    // Go's auto-analyze process ID is `Domain.NextConnID()`: it names this
    // node's server ID, so `KILL` routes it to this node
    // (`executeKillStmt` compares `gcid.ServerID` with `sm.ServerID()`).
    let process_id = factory.connection_id_allocator().next_id();
    let observed = Arc::new(AtomicBool::new(false));
    let interrupted = Arc::new(AtomicBool::new(false));
    let tracked = Arc::new(AtomicBool::new(false));
    let untracked = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicU64::new(0));
    let pool = factory.advanced_sys_session_pool();
    let mut analyzed = false;
    tidb_stats_handle_util::call_with_sctx(
        pool.as_ref(),
        |context| {
            context
                .with_session(|session| {
                    session.analyze = Arc::new(ObservedSystemAnalyze {
                        inner: Arc::clone(&session.analyze),
                        processes: factory.processes(),
                        kill,
                        process_id,
                        factory: Arc::downgrade(&factory),
                        observed: Arc::clone(&observed),
                        interrupted: Arc::clone(&interrupted),
                    });
                    Ok(())
                })
                .unwrap();
            let (register, unregister) = super::super::system_process_trackers(factory.processes());
            let released_by_generator = Arc::clone(&released);
            let generator = tidb_stats_handle_util::Generator::new(
                move || process_id,
                move |id| released_by_generator.store(id, Ordering::SeqCst),
            );
            let tracked_by_callback = Arc::clone(&tracked);
            let track: tidb_sqlexec::TrackSysProc = Arc::new(move |id, process| {
                assert_eq!(id, process_id);
                assert!(
                    !(&*process as &dyn std::any::Any).is::<()>(),
                    "restricted ANALYZE must hand the tracker its live session"
                );
                tracked_by_callback.store(true, Ordering::SeqCst);
                register(id, process)
            });
            let untracked_by_callback = Arc::clone(&untracked);
            let untrack: tidb_sqlexec::UntrackSysProc = Arc::new(move |id| {
                assert_eq!(id, process_id);
                untracked_by_callback.store(true, Ordering::SeqCst);
                unregister(id);
            });
            analyzed = tidb_stats_handle_autoanalyze_exec::auto_analyze(
                context,
                &generator,
                track,
                untrack,
                2,
                false,
                "analyze table %n.%n",
                &[
                    tidb_util::sqlescape::SqlArg::from("test"),
                    tidb_util::sqlescape::SqlArg::from("auto_exec_live"),
                ],
            );
            Ok(())
        },
        &[],
    )
    .expect("auto analyze uses a system session");
    assert_eq!(analyzed, kill.is_none());
    assert!(observed.load(Ordering::SeqCst));
    assert_eq!(
        interrupted.load(Ordering::SeqCst),
        kill.is_some(),
        "SQL KILL must reach ANALYZE, not merely cause another failure"
    );
    assert!(!factory
        .processes()
        .snapshot()
        .iter()
        .any(|row| row.id == process_id));
    assert!(tracked.load(Ordering::SeqCst));
    assert!(untracked.load(Ordering::SeqCst));
    assert_eq!(released.load(Ordering::SeqCst), process_id);
    assert!(!tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(process_id));
    if kill.is_some() {
        return;
    }
    assert_eq!(
        displayed(rows(
            &mut client,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {table_id}"
            ),
        )),
        [["3", "0"]]
    );
}

struct AutoAnalyzeKillTarget {
    cancelled: Arc<AtomicBool>,
}

impl tidb_session::process::ProcessKillTarget for AutoAnalyzeKillTarget {
    fn cancel_query(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn kill_connection(&self) {
        self.cancel_query();
    }
}

/// Pinned `autoanalyze/exec.TestKillInWindows` and
/// `Domain.CheckAutoAnalyzeWindows`: an out-of-window domain check kills the
/// process ID installed by `RunAnalyzeStmt`; the inclusive configured minute
/// does not.
#[test]
fn auto_analyze_window_check_kills_only_outside_the_window_like_go() {
    const PROCESS_ID: u64 = 9_002;
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(87))
        .expect("client session opens");
    rows(
        &mut client,
        "SET GLOBAL tidb_auto_analyze_start_time = '00:00 +0000'",
    );
    rows(
        &mut client,
        "SET GLOBAL tidb_auto_analyze_end_time = '00:00 +0000'",
    );
    let cancelled = Arc::new(AtomicBool::new(false));
    let target: Arc<dyn tidb_session::process::ProcessKillTarget> =
        Arc::new(AutoAnalyzeKillTarget {
            cancelled: Arc::clone(&cancelled),
        });
    let _guard = factory.processes.register(
        PROCESS_ID,
        "root".to_owned(),
        "127.0.0.1:0".to_owned(),
        "test".to_owned(),
        Some(target),
    );
    tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.tracker(PROCESS_ID);

    let inside = chrono::DateTime::parse_from_rfc3339("2026-08-31T00:00:30Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    factory.check_auto_analyze_windows_at(inside);
    assert!(!cancelled.load(Ordering::SeqCst));
    let outside = chrono::DateTime::parse_from_rfc3339("2026-08-31T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    factory.check_auto_analyze_windows_at(outside);
    assert!(cancelled.load(Ordering::SeqCst));
    tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(PROCESS_ID);
}

#[test]
fn independent_unistore_stores_can_both_own_statistics() {
    let (first, _first_users) = cop_backed_stack();
    let (second, _second_users) = cop_backed_stack();
    let first_owner = first.factory.stats_owner.as_ref().unwrap();
    let second_owner = second.factory.stats_owner.as_ref().unwrap();
    first_owner.campaign_owner(&[]).unwrap();
    second_owner.campaign_owner(&[]).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !(first_owner.is_owner() && second_owner.is_owner())
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let ownership = (first_owner.is_owner(), second_owner.is_owner());
    first_owner.campaign_cancel();
    second_owner.campaign_cancel();
    assert_eq!(
        ownership,
        (true, true),
        "Go isolates mock owners by store.UUID()"
    );
}

#[test]
fn auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(crate::node_config::StatsLease::Zero)
        .expect("stats owner campaigns");
    let owner_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !factory
        .stats_owner
        .as_ref()
        .expect("stats owner")
        .is_owner()
    {
        assert!(
            std::time::Instant::now() < owner_deadline,
            "statistics ownership was not acquired"
        );
        std::thread::yield_now();
    }
    let mut session = factory
        .open_session(session_context(140))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE queue_analyze (a INT, b INT)");
    rows(&mut session, "CREATE TABLE queue_drop (a INT)");
    rows(&mut session, "CREATE TABLE queue_locked (a INT)");
    let values = (0..1_000)
        .map(|_| "(1)".to_owned())
        .collect::<Vec<_>>()
        .join(",");
    let pairs = (0..1_000)
        .map(|_| "(1,1)".to_owned())
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO queue_analyze VALUES {pairs}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO queue_drop VALUES {values}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO queue_locked VALUES {values}"),
    );
    rows(&mut session, "LOCK STATS queue_locked");
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < deadline,
            "statistics update did not run after FLUSH STATS_DELTA"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let catalog = factory.catalog.load();
    let analyze_id = catalog
        .find_table("test", "queue_analyze")
        .expect("analyze table exists")
        .1
        .id;
    let dropped_id = catalog
        .find_table("test", "queue_drop")
        .expect("drop table exists")
        .1
        .id;
    let locked_id = catalog
        .find_table("test", "queue_locked")
        .expect("locked table exists")
        .1
        .id;
    drop(catalog);
    rows(
        &mut session,
        &format!(
            "INSERT INTO mysql.column_stats_usage(table_id,column_id,last_used_at) \
             VALUES ({analyze_id},1,CURRENT_TIMESTAMP)"
        ),
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_analyze_column_options = 'PREDICATE'",
    );

    let refresher = factory.auto_analyze_refresher(Duration::ZERO);
    let queue = factory.auto_analyze_priority_queue(Duration::ZERO);
    queue.initialize().expect("priority queue initializes");
    assert_eq!(queue.len().unwrap(), 2);
    rows(&mut session, "DROP TABLE queue_drop");
    // Go priorityqueue.TestDropTable handles the DROP event before observing
    // the heap. Here the active notifier owns that transaction asynchronously.
    let queue_bit = 1_u64 << tidb_ddl_notifier::PRIORITY_QUEUE_HANDLER_ID.0;
    let pending_queue_sql = format!(
        "SELECT count(*) FROM mysql.tidb_ddl_notifier WHERE processed_by_flag & {queue_bit} = 0"
    );
    let queue_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while displayed(rows(&mut session, &pending_queue_sql)) != [["0"]] {
        assert!(
            std::time::Instant::now() < queue_deadline,
            "priority queue subscriber did not commit the DROP event"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let snapshot = queue.snapshot().unwrap();
    assert_eq!(snapshot.current_jobs.len(), 1);
    assert_eq!(snapshot.current_jobs[0].table_id, analyze_id);
    assert_ne!(snapshot.current_jobs[0].table_id, dropped_id);

    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    let refresher = refresher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    refresher.wait_auto_analyze_finished();
    assert!(refresher.running_jobs().is_empty());
    assert_eq!(refresher.len(), 0);
    drop(refresher);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info, state FROM mysql.analyze_jobs \
             WHERE table_name = 'queue_analyze' ORDER BY id",
        )),
        [[
            "auto analyze table column a with 256 buckets, 100 topn, 1 samplerate",
            "finished",
        ]]
    );

    factory.handle_auto_analyze_tick(false, Duration::ZERO);
    assert!(!queue.is_initialized());
    rows(&mut session, "UNLOCK STATS queue_locked");
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let reload_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < reload_deadline,
            "statistics update did not run after unlocking the table"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    rows(
        &mut session,
        "SET GLOBAL tidb_analyze_column_options = 'ALL'",
    );
    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    assert!(queue.is_initialized());
    let refresher = factory.auto_analyze_refresher(Duration::ZERO);
    refresher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .wait_auto_analyze_finished();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info, state FROM mysql.analyze_jobs \
             WHERE table_name = 'queue_locked' ORDER BY id",
        )),
        [[
            "auto analyze table all columns with 256 buckets, 100 topn, 1 samplerate",
            "finished",
        ]]
    );
    assert_ne!(locked_id, analyze_id);
}

/// Pinned root `TestAutoAnalyzeSkipColumnTypes`: auto analyze reads the live
/// GLOBAL skip list, retains index-mandatory columns, and records the same
/// selected-column job shape as ordinary ANALYZE planning.
#[test]
fn auto_analyze_skips_configured_column_types_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(crate::node_config::StatsLease::Zero)
        .expect("stats owner campaigns");
    let owner_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !factory
        .stats_owner
        .as_ref()
        .expect("stats owner")
        .is_owner()
    {
        assert!(std::time::Instant::now() < owner_deadline);
        std::thread::yield_now();
    }
    let mut session = factory
        .open_session(session_context(142))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE queue_skip_types (a INT, c JSON, d VARCHAR(32), INDEX idx_d(d))",
    );
    let values = (0..1_000)
        .map(|_| "(1,NULL,'value')".to_owned())
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO queue_skip_types VALUES {values}"),
    );
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let reload_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(std::time::Instant::now() < reload_deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    rows(
        &mut session,
        "SET GLOBAL tidb_analyze_skip_column_types = 'json'",
    );

    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    factory
        .auto_analyze_refresher(Duration::ZERO)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .wait_auto_analyze_finished();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info, state FROM mysql.analyze_jobs \
             WHERE table_name = 'queue_skip_types' ORDER BY id",
        )),
        [[
            "auto analyze table all indexes, columns a, d with 256 buckets, 100 topn, 1 samplerate",
            "finished",
        ]]
    );
}

/// Pinned planner `TestAutoAnalyzeForMissingPartition`: under dynamic pruning
/// and `tidb_skip_missing_partition_stats`, auto analyze fills the physical
/// partitions that ordinary partition ANALYZE intentionally left missing.
#[test]
fn auto_analyze_fills_missing_partition_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(crate::node_config::StatsLease::Zero)
        .expect("stats owner campaigns");
    let owner_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !factory
        .stats_owner
        .as_ref()
        .expect("stats owner")
        .is_owner()
    {
        assert!(
            std::time::Instant::now() < owner_deadline,
            "statistics ownership was not acquired"
        );
        std::thread::yield_now();
    }
    let mut session = factory
        .open_session(session_context(141))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_skip_missing_partition_stats = ON",
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "SET GLOBAL tidb_auto_analyze_ratio = 0.01");
    rows(
        &mut session,
        "CREATE TABLE missing_partition_stats (a INT, b INT, c INT, INDEX idx_b(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN (300))",
    );
    let values = [1, 101, 201]
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(format!("({value},{value},{value})"), 1_000))
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO missing_partition_stats VALUES {values}"),
    );
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let reload_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < reload_deadline,
            "statistics update did not run after FLUSH STATS_DELTA"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    rows(
        &mut session,
        "ANALYZE TABLE missing_partition_stats PARTITION p1",
    );

    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    let refresher = factory.auto_analyze_refresher(Duration::ZERO);
    refresher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .wait_auto_analyze_finished();

    let meta = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'missing_partition_stats'",
    ));
    for partition in ["p0", "p2"] {
        assert!(
            meta.iter()
                .any(|row| row[2] == partition && row[5] == "1000"),
            "auto analyze did not publish {partition}: {meta:?}"
        );
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.analyze_jobs WHERE \
             table_name = 'missing_partition_stats' AND state = 'finished' AND \
             job_info LIKE 'auto analyze table%'",
        )),
        [["2"]]
    );
}

/// Go `newLockCtx` asks for existence metadata when assertions are enabled,
/// including fair locks that do not return the record value.
#[test]
fn session_activation_checks_existence_for_pessimistic_locks() {
    use tidb_txnkv::transaction::BufferMutation;

    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(904)).unwrap();
    for (id, (level, fair)) in [
        ("FAST", "ON"),
        ("STRICT", "ON"),
        ("OFF", "ON"),
        ("FAST", "OFF"),
        ("STRICT", "OFF"),
        ("OFF", "OFF"),
    ]
    .into_iter()
    .enumerate()
    {
        rows(
            &mut session,
            &format!("SET tidb_txn_assertion_level = '{level}'"),
        );
        rows(
            &mut session,
            &format!("SET tidb_pessimistic_txn_fair_locking = {fair}"),
        );
        let key = format!("assert-existence/{id}").into_bytes();
        for existing in [false, true] {
            session.control_transaction("BEGIN PESSIMISTIC").unwrap();
            // Restricted SQL locks its planned keys without loading values,
            // then stages them. Both absent and existing records must survive
            // the native client's assertion check from the lock metadata.
            super::super::transactions::stage_pessimistic_statement(
                session.explicit.as_deref().unwrap(),
                &session.buffer,
                session.session.lock_wait_timeout(),
                |_, _| {
                    let mutation = if existing {
                        BufferMutation::put_existing(key.clone(), b"updated".to_vec())
                    } else {
                        BufferMutation::set_with_flags(
                            key.clone(),
                            b"inserted".to_vec(),
                            true,
                            tidb_txnkv::AssertionOp::AssertNotExist,
                        )
                    }
                    .unwrap();
                    Ok(((), vec![mutation]))
                },
            )
            .unwrap();
            session.control_transaction("COMMIT").unwrap();
        }
    }
}

/// Pinned DDL subscriber `ActionCreateTable`, `ActionTruncateTable`, and
/// `ActionDropTable`: create every physical table's zero-valued statistics
/// placeholders, then retire the old physical ID by advancing stats_meta.
#[test]
fn table_lifecycle_ddl_updates_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(74))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_lifecycle (a INT, b INT, INDEX idx_b(b))",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let old_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_lifecycle")
        .expect("created table is published")
        .1
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {old_id}"),
        )),
        [["0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {old_id}"),
        )),
        [["3"]]
    );

    rows(
        &mut session,
        "CREATE TABLE stats_lifecycle_like LIKE stats_lifecycle",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let like_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_lifecycle_like")
        .expect("CREATE TABLE LIKE result is published")
        .1
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {like_id}"),
        )),
        [["3"]]
    );

    rows(&mut session, "INSERT INTO stats_lifecycle VALUES (1, 2)");
    rows(&mut session, "ANALYZE TABLE stats_lifecycle");
    let old_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is an unsigned integer");
    rows(&mut session, "TRUNCATE TABLE stats_lifecycle");
    drain_stats_ddl_events(&stack.factory, &mut session);
    let new_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_lifecycle")
        .expect("truncated table is published")
        .1
        .id;
    assert_ne!(new_id, old_id, "truncate allocates a new physical ID");
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {new_id}"),
        )),
        [["0", "0"]]
    );
    let retired_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("retired stats version is an unsigned integer");
    assert!(retired_version > old_version);

    let new_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {new_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("new stats version is an unsigned integer");
    rows(&mut session, "DROP TABLE stats_lifecycle");
    drain_stats_ddl_events(&stack.factory, &mut session);
    let dropped_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {new_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("dropped stats version is an unsigned integer");
    assert!(dropped_version > new_version);
}

/// Pinned `pkg/statistics/handle/ddl.TestDDLAfterLoad`: ADD COLUMN remains
/// valid after the table has been analyzed, populated, analyzed again, and is
/// therefore backed by an initialized statistics-cache entry.
#[test]
fn ddl_after_loaded_statistics_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(75))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_ddl_after_load (c1 INT, c2 INT, INDEX idx(c1, c2))",
    );
    rows(&mut session, "ANALYZE TABLE stats_ddl_after_load");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_ddl_after_load")
        .expect("created table is published")
        .1
        .id;
    assert!(stack
        .factory
        .stats()
        .load()
        .get(&table_id)
        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
        .is_some_and(|stats| !stats.hist_coll.pseudo));

    let values = (0..1000)
        .map(|value| format!("({value},{})", value + 1))
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO stats_ddl_after_load VALUES {values}"),
    );
    rows(&mut session, "ANALYZE TABLE stats_ddl_after_load");
    assert!(stack
        .factory
        .stats()
        .load()
        .get(&table_id)
        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
        .is_some_and(|stats| !stats.hist_coll.pseudo));

    // Go keeps Domain.StatsHandle independently of an InfoSchema refresh.
    // Demand must still reload evicted payloads after a stats-only refresh
    // and after the DDL below replaces the connection's schema catalog.
    let check_reload = |session: &mut ClusterServerSession| {
        let peer = stack.factory.open_session(session_context(76)).unwrap();
        let peer_catalog = peer.session.shared_catalog();
        assert!(!peer_catalog
            .lock()
            .unwrap()
            .table_statistics(table_id)
            .unwrap()
            .column_is_load_needed(1, true));
        let mut snapshot = (*stack.factory.stats().load()).clone();
        let table = snapshot.get(&table_id).unwrap().loaded().unwrap();
        let table = table.copy_as(tidb_stats::CopyIntent::AllDataWritable);
        for column in table.hist_coll.stable_columns() {
            column.write().unwrap().drop_unnecessary_data();
        }
        for index in table.hist_coll.stable_indices() {
            index.write().unwrap().evict_all_stats();
        }
        snapshot.insert(
            table_id,
            tidb_exec::stats_watch::TableStatsState::Loaded(Arc::new(table)),
        );
        stack.factory.stats().store_after_analyze(snapshot);
        assert!(peer_catalog
            .lock()
            .unwrap()
            .table_statistics(table_id)
            .unwrap()
            .column_is_load_needed(1, true));
        let read_count = tidb_stats_handle_metrics::READ_STATS_HISTOGRAM.get_sample_count();
        rows(
            session,
            "EXPLAIN SELECT * FROM stats_ddl_after_load WHERE c1 = 42 AND c2 = 43",
        );
        assert!(
            tidb_stats_handle_metrics::READ_STATS_HISTOGRAM.get_sample_count() > read_count,
            "successful storage loads must record read latency"
        );
        let snapshot = stack.factory.stats().load();
        let table = snapshot.get(&table_id).unwrap().loaded().unwrap();
        assert!(
            table
                .hist_coll
                .get_column(1)
                .unwrap()
                .read()
                .unwrap()
                .stats_loaded_status
                .is_full_load(),
            "column payload must reload after catalog refresh"
        );
        assert!(
            table
                .hist_coll
                .get_index(1)
                .unwrap()
                .read()
                .unwrap()
                .stats_loaded_status
                .is_full_load(),
            "index payload must reload after catalog refresh"
        );
        // Go publishes into Domain.StatsHandle, not just the loading session.
        // This peer must see the payload without rebuilding its schema catalog.
        assert!(
            !peer_catalog
                .lock()
                .unwrap()
                .table_statistics(table_id)
                .unwrap()
                .column_is_load_needed(1, true),
            "the peer still sees an evicted column after the domain load completes"
        );
        assert!(
            Arc::ptr_eq(
                &peer_catalog
                    .lock()
                    .unwrap()
                    .table_statistics(table_id)
                    .unwrap(),
                &session
                    .session
                    .shared_catalog()
                    .lock()
                    .unwrap()
                    .table_statistics(table_id)
                    .unwrap(),
            ),
            "sessions must share the same converted statistics"
        );
    };
    check_reload(&mut session);

    rows(
        &mut session,
        "ALTER TABLE stats_ddl_after_load ADD COLUMN c10 INT",
    );
    assert!(stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_ddl_after_load")
        .is_some_and(|(_, table)| table.columns.iter_deref().any(|column| column
            .read()
            .name
            .lowercase()
            == "c10")));
    check_reload(&mut session);
}

/// Pinned `pkg/statistics/handle/ddl.TestTruncateAPartitionedTable`: whole
/// table truncation gives every partition a fresh physical ID, initializes
/// each replacement stats row, and advances every retired partition version.
#[test]
fn truncate_partitioned_table_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_truncate_partitioned (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11))",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_truncate_partitioned VALUES (1,2),(2,2),(6,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_truncate_partitioned");
    let old_ids = partition_id_map(
        &stack.factory.catalog.load(),
        "test",
        "stats_truncate_partitioned",
    )
    .expect("table is partitioned")
    .1
    .into_iter()
    .map(|(_, id)| id)
    .collect::<Vec<_>>();
    let old_versions = old_ids
        .iter()
        .map(|id| {
            displayed(rows(
                &mut session,
                &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {id}"),
            ))[0][0]
                .parse::<u64>()
                .expect("stats version is unsigned")
        })
        .collect::<Vec<_>>();

    rows(&mut session, "TRUNCATE TABLE stats_truncate_partitioned");
    drain_stats_ddl_events(&stack.factory, &mut session);
    let new_ids = partition_id_map(
        &stack.factory.catalog.load(),
        "test",
        "stats_truncate_partitioned",
    )
    .expect("truncated table remains partitioned")
    .1
    .into_iter()
    .map(|(_, id)| id)
    .collect::<Vec<_>>();
    assert_eq!(new_ids.len(), 2);
    assert!(new_ids.iter().all(|id| !old_ids.contains(id)));
    for new_id in new_ids {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {new_id}"),
            )),
            [["1"]]
        );
    }
    for (old_id, old_version) in old_ids.into_iter().zip(old_versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("stats version is unsigned");
        assert!(retired_version > old_version);
    }
}

/// Pinned `pkg/statistics/handle/ddl.TestTruncateAHashPartition`: truncating
/// p0 removes its one persisted row from global count, keeps that removal as
/// one modification, replaces p0's physical ID, and retires the old stats row.
#[test]
fn truncate_hash_partition_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(77))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_truncate_hash (a BIGINT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY HASH(a) PARTITIONS 4",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_truncate_hash VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_truncate_hash");
    let (logical_id, partitions) =
        partition_id_map(&stack.factory.catalog.load(), "test", "stats_truncate_hash")
            .expect("table is hash partitioned");
    let old_p0 = partitions
        .iter()
        .find(|(name, _)| name == "p0")
        .expect("p0 exists")
        .1;
    let old_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_p0}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is unsigned");

    rows(
        &mut session,
        "ALTER TABLE stats_truncate_hash TRUNCATE PARTITION p0",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["4", "1"]]
    );
    let new_p0 = partition_id_map(&stack.factory.catalog.load(), "test", "stats_truncate_hash")
        .expect("table remains hash partitioned")
        .1
        .into_iter()
        .find(|(name, _)| name == "p0")
        .expect("replacement p0 exists")
        .1;
    assert_ne!(new_p0, old_p0);
    let retired_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_p0}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is unsigned");
    assert!(retired_version > old_version);
}

/// Pinned `pkg/statistics/handle/ddl.TestDDLPartition`: ADD PARTITION creates
/// the new physical statistics row in both prune modes, while only dynamic
/// mode creates the logical/global row for the original table.
#[test]
fn add_partition_statistics_follow_global_prune_mode_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");

    for mode in ["static", "dynamic"] {
        rows(
            &mut session,
            &format!("SET GLOBAL tidb_partition_prune_mode = '{mode}'"),
        );
        rows(
            &mut session,
            &format!("SET SESSION tidb_partition_prune_mode = '{mode}'"),
        );
        let table_name = format!("stats_add_partition_{mode}");
        rows(
            &mut session,
            &format!(
                "CREATE TABLE {table_name} (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
                 PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
                 PARTITION p1 VALUES LESS THAN (11))"
            ),
        );
        drain_stats_ddl_events(&stack.factory, &mut session);
        let (logical_id, original) =
            partition_id_map(&stack.factory.catalog.load(), "test", &table_name)
                .expect("table is partitioned");
        for (_, physical_id) in original {
            assert_eq!(
                displayed(rows(
                    &mut session,
                    &format!(
                        "SELECT count(*) FROM mysql.stats_meta WHERE table_id = {physical_id}"
                    ),
                )),
                [["1"]]
            );
        }
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {logical_id}"),
            )),
            [[if mode == "dynamic" { "1" } else { "0" }]]
        );

        rows(
            &mut session,
            &format!(
                "ALTER TABLE {table_name} ADD PARTITION \
                 (PARTITION p2 VALUES LESS THAN (16))"
            ),
        );
        drain_stats_ddl_events(&stack.factory, &mut session);
        let new_id = partition_id_map(&stack.factory.catalog.load(), "test", &table_name)
            .expect("table remains partitioned")
            .1
            .into_iter()
            .find(|(name, _)| name == "p2")
            .expect("added partition exists")
            .1;
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {new_id}"),
            )),
            [["1"]]
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {new_id}"),
            )),
            [["3"]]
        );
    }
}

/// Pinned `pkg/statistics/handle/ddl.TestDropPartitions`: dropping p0 and p1
/// subtracts their three rows from global count, adds three modifications,
/// and advances both retired physical statistics versions.
#[test]
fn drop_partitions_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_drop_partitions (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), \
         PARTITION p3 VALUES LESS THAN (21))",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_drop_partitions VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_drop_partitions");
    let (logical_id, partitions) = partition_id_map(
        &stack.factory.catalog.load(),
        "test",
        "stats_drop_partitions",
    )
    .expect("table is partitioned");
    let retired_ids = partitions
        .into_iter()
        .filter_map(|(name, id)| matches!(name.as_str(), "p0" | "p1").then_some(id))
        .collect::<Vec<_>>();
    let versions = retired_ids
        .iter()
        .map(|id| {
            displayed(rows(
                &mut session,
                &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {id}"),
            ))[0][0]
                .parse::<u64>()
                .expect("stats version is unsigned")
        })
        .collect::<Vec<_>>();

    rows(
        &mut session,
        "ALTER TABLE stats_drop_partitions DROP PARTITION p0, p1",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["2", "3"]]
    );
    for (retired_id, version) in retired_ids.into_iter().zip(versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {retired_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("stats version is unsigned");
        assert!(retired_version > version);
    }
}

/// Pinned `TestReorgPartitions`, `TestIncreasePartitionCountOfHashPartitionTable`,
/// and `TestDecreasePartitionCountOfHashPartitionTable`: the subscriber does
/// not alter global count during a reorganization, initializes every added
/// physical ID, and advances every dropped physical ID for delayed GC.
#[test]
fn reorganize_partition_event_updates_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_reorganize_event (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_reorganize_event VALUES (1,2),(2,2),(6,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_reorganize_event");

    let table = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_reorganize_event")
        .expect("partitioned table is published")
        .1
        .clone_like_go();
    let logical_id = table.id;
    let dropped_id = table
        .partition
        .as_ref()
        .expect("partition metadata exists")
        .read()
        .definitions
        .snapshot()[0]
        .id;
    let dropped_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {dropped_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("partition version is unsigned");
    let logical_before = displayed(rows(
        &mut session,
        &format!("SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"),
    ));
    let added_id = 9_100_000_001_i64;
    let event = SchemaChangeEvent::reorganize_partitions(
        table,
        partition_payload(&[added_id]),
        partition_payload(&[dropped_id]),
    );
    handle_stats_schema_change(&mut session, &event);

    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {added_id}"
            ),
        )),
        [["0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {added_id}"),
        )),
        [["3"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        logical_before
    );
    let retired_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {dropped_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("retired partition version is unsigned");
    assert!(retired_version > dropped_version);
}

/// Pinned `TestAddPartitioning` and `TestRemovePartitioning`: newly added
/// partition placeholders are written before the six global-statistics table
/// IDs move, and removal moves those same tables before retiring partitions.
#[test]
fn add_and_remove_partitioning_events_move_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(81))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_partitioning_event \
         (a INT PRIMARY KEY, b INT, INDEX idx(b))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_partitioning_event VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_partitioning_event");
    let source = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_partitioning_event")
        .expect("table is published")
        .1
        .clone_like_go();
    let old_table_id = source.id;
    let global_table_id = 9_200_000_001_i64;
    let partition_ids = [9_200_000_002_i64, 9_200_000_003_i64];
    let mut partitioned = source.clone_like_go();
    partitioned.id = global_table_id;
    let added = partition_payload(&partition_ids);
    handle_stats_schema_change(
        &mut session,
        &SchemaChangeEvent::add_partitioning(old_table_id, partitioned, added.clone_like_go()),
    );

    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {global_table_id}"
            ),
        )),
        [["5", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {old_table_id}"),
        )),
        [["0"]]
    );
    let initial_partition_versions = partition_ids.map(|partition_id| {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {partition_id}"
                ),
            )),
            [["3"]]
        );
        displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {partition_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("partition version is unsigned")
    });

    let single_table_id = 9_200_000_004_i64;
    let mut single = source;
    single.id = single_table_id;
    handle_stats_schema_change(
        &mut session,
        &SchemaChangeEvent::remove_partitioning(global_table_id, single, added),
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {single_table_id}"
            ),
        )),
        [["5", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {global_table_id}"),
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {single_table_id}"
            ),
        )),
        [["3"]]
    );
    for (partition_id, old_version) in partition_ids.into_iter().zip(initial_partition_versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {partition_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("retired partition version is unsigned");
        assert!(retired_version > old_version);
    }
}

/// Pinned `ActionFlashbackCluster`: both table-wide version updates use one
/// transaction start TS, while `last_stats_histograms_version` is untouched.
#[test]
fn flashback_cluster_event_refreshes_statistics_versions_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(82))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_flashback_event (a INT, b INT, INDEX idx(b))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_flashback_event VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_flashback_event");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_flashback_event")
        .expect("table is published")
        .1
        .id;
    let before = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
             WHERE table_id = {table_id}"
        ),
    ));
    handle_stats_schema_change(&mut session, &SchemaChangeEvent::flashback_cluster());
    let after = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
             WHERE table_id = {table_id}"
        ),
    ));
    assert_ne!(after[0][0], before[0][0]);
    assert_eq!(after[0][1], before[0][1]);
    let histogram_versions = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_histograms WHERE table_id = {table_id}"),
    ));
    assert!(!histogram_versions.is_empty());
    assert!(histogram_versions.iter().all(|row| row[0] == after[0][0]));
}

/// Pinned `TestDumpStatsDeltaBeforeHandleDDLEvent` and
/// `TestDumpStatsDeltaBeforeHandleAddColumnEvent`: a delayed subscriber event
/// must not overwrite metadata or histogram rows already written by a delta
/// flush or a later analyze.
#[test]
fn delayed_create_and_add_column_events_preserve_newer_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(&mut session, "USE test");

    rows(&mut session, "CREATE TABLE stats_delayed_create (c1 INT)");
    rows(
        &mut session,
        "INSERT INTO stats_delayed_create VALUES (1),(2),(3)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let create_table = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_delayed_create")
        .expect("created table is published")
        .1
        .clone_like_go();
    let create_id = create_table.id;
    let create_before = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, modify_count, count, last_stats_histograms_version \
             FROM mysql.stats_meta WHERE table_id = {create_id}"
        ),
    ));
    handle_stats_schema_change(&mut session, &SchemaChangeEvent::create_table(create_table));
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT version, modify_count, count, last_stats_histograms_version \
                 FROM mysql.stats_meta WHERE table_id = {create_id}"
            ),
        )),
        create_before
    );

    rows(
        &mut session,
        "CREATE TABLE stats_delayed_column \
         (c1 INT, c2 INT, INDEX idx(c1, c2))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_delayed_column VALUES (1,2),(2,3),(3,4)",
    );
    rows(
        &mut session,
        "ALTER TABLE stats_delayed_column ADD COLUMN c3 INT",
    );
    rows(
        &mut session,
        "INSERT INTO stats_delayed_column VALUES (4,5,6)",
    );
    rows(
        &mut session,
        "ALTER TABLE stats_delayed_column ADD COLUMN c4 INT NOT NULL DEFAULT 0",
    );
    rows(
        &mut session,
        "INSERT INTO stats_delayed_column(c1,c2) VALUES (6,7)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE stats_delayed_column ALL COLUMNS",
    );
    let (column_table, delayed_columns) = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("test", "stats_delayed_column")
            .expect("altered table is published")
            .1
            .clone_like_go();
        let columns = table
            .columns
            .iter_deref()
            .filter_map(|column| {
                let column = column.read();
                matches!(column.name.lowercase(), "c3" | "c4").then(|| column.clone_like_go())
            })
            .collect::<Vec<_>>();
        (table, columns)
    };
    assert_eq!(delayed_columns.len(), 2);
    let column_table_id = column_table.id;
    let column_before = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
             WHERE table_id = {column_table_id}"
        ),
    ));
    for column in delayed_columns {
        handle_stats_schema_change(
            &mut session,
            &SchemaChangeEvent::add_columns(column_table.clone_like_go(), vec![column]),
        );
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
                 WHERE table_id = {column_table_id}"
            ),
        )),
        column_before
    );
}

/// Pinned DDL subscriber `ActionDropSchema` visits every table's partition
/// IDs in definition order and then its logical table ID, advancing each
/// extant stats row for delayed GC without failing the DROP on stats errors.
#[test]
fn drop_schema_ddl_retires_all_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(72))
        .expect("session opens");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "CREATE DATABASE stats_drop_schema");
    rows(&mut session, "USE stats_drop_schema");
    rows(&mut session, "CREATE TABLE ordinary (a INT)");
    rows(
        &mut session,
        "CREATE TABLE partitioned (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let retired_ids = {
        let catalog = stack.factory.catalog.load();
        let database = catalog
            .databases
            .iter()
            .find(|database| database.info.name.lowercase() == "stats_drop_schema")
            .expect("created database is published");
        let mut ids = Vec::new();
        for table in &database.tables {
            if let Some(partition) = &table.partition {
                ids.extend(
                    partition
                        .read()
                        .definitions
                        .snapshot()
                        .into_iter()
                        .map(|definition| definition.id),
                );
            }
            ids.push(table.id);
        }
        ids
    };
    assert_eq!(retired_ids.len(), 4);
    let old_versions = retired_ids
        .iter()
        .map(|physical_id| {
            displayed(rows(
                &mut session,
                &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {physical_id}"),
            ))[0][0]
                .parse::<u64>()
                .expect("stats version is an unsigned integer")
        })
        .collect::<Vec<_>>();

    rows(&mut session, "DROP DATABASE stats_drop_schema");
    drain_stats_ddl_events(&stack.factory, &mut session);
    for (physical_id, old_version) in retired_ids.iter().zip(old_versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {physical_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("retired stats version is an unsigned integer");
        assert!(retired_version > old_version);
    }
}

/// Pinned DDL subscriber `ActionAddColumn`: a defaultless nullable column is
/// NULL for every existing row, and `InsertColStats2KV` persists that fact
/// before advancing the physical table's statistics version.
#[test]
fn add_column_ddl_initializes_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(73))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE stats_add_column (a INT)");
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_add_column VALUES (1),(2),(3)",
    );
    rows(&mut session, "ANALYZE TABLE stats_add_column");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("created table is published")
        .1
        .id;
    let old_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is an unsigned integer");

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN b INT",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let column_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "b")
        .expect("added column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, stats_ver FROM mysql.stats_histograms \
                 WHERE table_id = {table_id} AND is_index = 0 AND hist_id = {column_id}"
            ),
        )),
        [["0", "3", "0"]]
    );
    let new_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is an unsigned integer");
    assert!(new_version > old_version);

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN c VARCHAR(15) DEFAULT '123'",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let defaulted_column_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "c")
        .expect("defaulted column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                 FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {defaulted_column_id}"
            ),
        )),
        [["1", "0", "9", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT repeats, count, lower_bound, upper_bound FROM mysql.stats_buckets \
                 WHERE table_id = {table_id} AND is_index = 0 \
                 AND hist_id = {defaulted_column_id}"
            ),
        )),
        [["3", "3", "123", "123"]]
    );

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN d BIGINT NOT NULL",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let zeroed_column_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "d")
        .expect("NOT NULL column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                 FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {zeroed_column_id}"
            ),
        )),
        [["1", "0", "0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT repeats, count, lower_bound, upper_bound FROM mysql.stats_buckets \
                 WHERE table_id = {table_id} AND is_index = 0 \
                 AND hist_id = {zeroed_column_id}"
            ),
        )),
        [["3", "3", "0", "0"]]
    );

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN e INT GENERATED ALWAYS AS (a + 1) VIRTUAL",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let virtual_column_id = {
        let catalog = stack.factory.catalog.load();
        let column = catalog
            .find_table("test", "stats_add_column")
            .expect("altered table is published")
            .1
            .columns
            .iter_deref()
            .find(|column| column.read().name.lowercase() == "e")
            .expect("virtual column is published")
            .read()
            .clone_like_go();
        assert!(column.is_virtual_generated());
        assert_eq!(column.generated_expr_string, "`a` + 1");
        assert!(column.dependences.contains("a"));
        column.id
    };
    session.rebuild_catalog_if_stale();
    {
        let catalog = session.session.shared_catalog();
        let catalog = catalog.lock().expect("session catalog is available");
        let tidb_executor::driver::TableEntry::Kv(table) = catalog
            .table_in("test", "stats_add_column")
            .expect("rebuilt executor table is available")
        else {
            panic!("rebuilt executor table is storage-backed");
        };
        assert!(table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case("e"))
            .expect("executor virtual column exists")
            .generated
            .is_some());
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, e FROM stats_add_column ORDER BY a",
        )),
        [["1", "2"], ["2", "3"], ["3", "4"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, stats_ver FROM mysql.stats_histograms \
                 WHERE table_id = {table_id} AND is_index = 0 AND hist_id = {virtual_column_id}"
            ),
        )),
        [["0", "0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM mysql.stats_buckets WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {virtual_column_id}"
            ),
        )),
        [["0"]]
    );

    // Pinned `TestDDLHistogram` performs both additions in one
    // ActionMultiSchemaChange. Go publishes one ActionAddColumn event for
    // each applied sub-job, in SQL order, and the statistics subscriber
    // initializes both histograms.
    rows(
        &mut session,
        "ALTER TABLE stats_add_column \
         ADD COLUMN f VARCHAR(15) DEFAULT '123', \
         ADD COLUMN g VARCHAR(15) DEFAULT '123'",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let (f_id, g_id) = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("test", "stats_add_column")
            .expect("altered table is published")
            .1;
        let column_id = |name: &str| {
            table
                .columns
                .iter_deref()
                .find(|column| column.read().name.lowercase() == name)
                .expect("added column is published")
                .read()
                .id
        };
        (column_id("f"), column_id("g"))
    };
    for column_id in [f_id, g_id] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                     FROM mysql.stats_histograms WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["1", "0", "9", "0"]]
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT repeats, count, lower_bound, upper_bound \
                     FROM mysql.stats_buckets WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["3", "3", "123", "123"]]
        );
    }

    rows(
        &mut session,
        &format!(
            "DELETE FROM mysql.stats_buckets WHERE table_id = {table_id} \
             AND is_index = 0 AND hist_id = {f_id}"
        ),
    );
    rows(
        &mut session,
        &format!(
            "DELETE FROM mysql.stats_histograms WHERE table_id = {table_id} \
             AND is_index = 0 AND hist_id = {f_id}"
        ),
    );
    rows(
        &mut session,
        "ALTER TABLE stats_add_column \
         ADD COLUMN IF NOT EXISTS f VARCHAR(15) DEFAULT '123', \
         ADD COLUMN h VARCHAR(15) DEFAULT '123'",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let h_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "h")
        .expect("applied sub-job column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {f_id}"
            ),
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                 FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {h_id}"
            ),
        )),
        [["1", "0", "9", "0"]]
    );
}

/// Pinned storage `TestDeleteAnalyzeJobs` begins with the ordinary ANALYZE
/// lifecycle: the job is inserted pending, run, and retained finished for
/// `SHOW ANALYZE STATUS` until timestamp-based cleanup removes it.
#[test]
fn analyze_job_lifecycle_is_persisted_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(77))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_analyze_job (a INT, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_analyze_job VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_analyze_job");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT table_schema, table_name, partition_name, processed_rows, state, \
                    process_id IS NULL \
             FROM mysql.analyze_jobs IGNORE INDEX \
             (PRIMARY, update_time, idx_schema_table_state, idx_schema_table_partition_state)",
        )),
        [["test", "stats_analyze_job", "", "2", "finished", "1"]]
    );
    let cutoff = tidb_exec::mysql_bootstrap::utc_now_timestamp()
        .add_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 0).unwrap())
        .unwrap();
    stack
        .factory
        .delete_analyze_jobs_before(cutoff)
        .expect("analyze-job cleanup commits");
    assert!(displayed(rows(
        &mut session,
        "SELECT id FROM mysql.analyze_jobs IGNORE INDEX \
         (PRIMARY, update_time, idx_schema_table_state, idx_schema_table_partition_state)",
    ))
    .is_empty());
}

/// Pinned executor `TestShowAnalyzeStatus`: the SHOW path reads the same
/// persisted job row as `mysql.analyze_jobs` and exposes Go's fourteen-column
/// shape rather than falling through as an unsupported inspection statement.
#[test]
fn show_analyze_status_reads_persisted_jobs_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_show_analyze_job (a INT, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_show_analyze_job VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_show_analyze_job");

    let shown = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_job'",
    ));
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0].len(), 14);
    assert_eq!(shown[0][0], "test");
    assert_eq!(shown[0][1], "stats_show_analyze_job");
    assert_eq!(shown[0][2], "");
    assert_eq!(shown[0][4], "2");
    assert_ne!(shown[0][5], "NULL");
    assert_ne!(shown[0][6], "NULL");
    assert_eq!(shown[0][7], "finished");
    assert_eq!(shown[0][8], "NULL");
    assert_eq!(shown[0][10], "NULL");
    assert_eq!(shown[0][11], "NULL");
    assert_eq!(shown[0][12], "NULL");
    assert_eq!(shown[0][13], "NULL");

    rows(&mut session, "DELETE FROM mysql.analyze_jobs");
    rows(
        &mut session,
        "CREATE TABLE stats_show_analyze_partition (a INT PRIMARY KEY, b INT) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_show_analyze_partition VALUES (1,1),(2,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_show_analyze_partition");
    let partition_jobs = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_partition'",
    ));
    assert_eq!(partition_jobs.len(), 2);
    let mut job_infos = partition_jobs
        .iter()
        .map(|row| row[3].clone())
        .collect::<Vec<_>>();
    job_infos.sort();
    assert_eq!(
        job_infos,
        [
            "analyze table all columns with 256 buckets, 100 topn, 1 samplerate",
            "merge global stats for test.stats_show_analyze_partition columns",
        ]
    );

    rows(&mut session, "DELETE FROM mysql.analyze_jobs");
    rows(
        &mut session,
        "ALTER TABLE stats_show_analyze_partition ADD INDEX idx(b)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE stats_show_analyze_partition INDEX idx",
    );
    let partition_index_jobs = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_partition'",
    ));
    assert_eq!(partition_index_jobs.len(), 3);
    let mut job_infos = partition_index_jobs
        .iter()
        .map(|row| row[3].clone())
        .collect::<Vec<_>>();
    job_infos.sort();
    assert_eq!(
        job_infos,
        [
            "analyze table all indexes, all columns with 256 buckets, 100 topn, 1 samplerate",
            "merge global stats for test.stats_show_analyze_partition columns",
            "merge global stats for test.stats_show_analyze_partition's index idx",
        ]
    );

    rows(&mut session, "DELETE FROM mysql.analyze_jobs");
    let started_at = tidb_exec::mysql_bootstrap::utc_now_timestamp()
        .add_duration(MySqlDuration::from_nanoseconds(-60_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    let mut job_id = 0;
    handle
        .commit_stats_plan(|snapshot, _| {
            let (created_job_id, plan) = tidb_exec::cluster_stats_write::plan_insert_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                "test",
                "stats_show_analyze_job",
                "",
                b"analyze table all columns with 256 buckets, 100 topn, 1 samplerate",
                "127.0.0.1:4000",
                78,
                started_at,
            )
            .map_err(|error| error.to_string())?;
            job_id = created_job_id;
            Ok(plan)
        })
        .expect("pending job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                job_id,
                started_at,
            )
            .map_err(|error| error.to_string())
        })
        .expect("running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_update_analyze_job_progress(
                snapshot,
                &stack.factory.catalog.load(),
                job_id,
                3,
                started_at,
            )
            .map_err(|error| error.to_string())
        })
        .expect("running progress commits");
    let running = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_job' AND state = 'running'",
    ));
    assert_eq!(running.len(), 1);
    assert_eq!(running[0][4], "3");
    assert_eq!(running[0][7], "running");
    // The embedded store has no PD HTTP authority. Go caches and uses the
    // helper's zero return after processed rows exceed loaded RealtimeCount.
    assert_eq!(running[0][11], "0s");
    assert_eq!(running[0][12], "100");
    assert_eq!(running[0][13], "0");
}

/// Pinned
/// `pkg/statistics/handle/autoanalyze/priorityqueue/intervaltimezone::TestLastFailedAnalysisDurationUseCorrectTimezone`:
/// a reused statistics session must replace its stale timezone with the live
/// global value before evaluating the failed-job interval.
#[test]
fn failed_analysis_duration_resets_the_pooled_session_timezone() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(80))
        .expect("client session opens");
    rows(&mut client, "SET GLOBAL time_zone = 'America/New_York'");
    let session_pool = factory.advanced_sys_session_pool();
    let source = ClusterPriorityQueueSource {
        factory: Arc::downgrade(&factory),
        stats_lease: Duration::ZERO,
        session_pool,
    };
    assert_eq!(
        source.scalar("SELECT @@time_zone").unwrap(),
        Some(Datum::String(tidb_datatype::StringDatum::new(
            b"America/New_York".to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )))
    );
    rows(&mut client, "SET GLOBAL time_zone = 'Europe/Berlin'");
    let now = tidb_exec::mysql_bootstrap::utc_now_timestamp();
    let started_at = now
        .add_duration(MySqlDuration::from_nanoseconds(-2_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&factory.transactions),
        catalog: Arc::clone(&factory.catalog),
        global_vars: factory.global_vars.clone(),
    };
    let mut job_id = 0;
    handle
        .commit_stats_plan(|snapshot, _| {
            let (created_job_id, plan) = tidb_exec::cluster_stats_write::plan_insert_analyze_job(
                snapshot,
                &factory.catalog.load(),
                "test",
                "t",
                "",
                b"analyze table `test`.`t`",
                "127.0.0.1:4000",
                1,
                started_at,
            )
            .map_err(|error| error.to_string())?;
            job_id = created_job_id;
            Ok(plan)
        })
        .expect("pending job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                &factory.catalog.load(),
                job_id,
                started_at,
            )
            .map_err(|error| error.to_string())
        })
        .expect("running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                &factory.catalog.load(),
                job_id,
                0,
                Some("simulated failure"),
                now,
            )
            .map_err(|error| error.to_string())
        })
        .expect("failed job commits");

    assert_eq!(
        source.scalar("SELECT @@time_zone").unwrap(),
        Some(Datum::String(tidb_datatype::StringDatum::new(
            b"Europe/Berlin".to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )))
    );
    let duration =
        tidb_stats_handle_autoanalyze_priorityqueue::AnalysisJobContext::last_failed_analysis_duration(
            &source,
            "test",
            "t",
            &[],
        )
        .expect("duration query");
    assert!(duration > 0, "duration must be positive: {duration}");
    assert!(
        duration < 60_000_000_000,
        "duration must be below one minute: {duration}"
    );
}

/// Pinned `TestCleanupCorruptedAnalyzeJobsOnCurrentInstance` and
/// `TestCleanupCorruptedAnalyzeJobsOnDeadInstances`: the two restricted
/// transactions select different corruptions and preserve the timestamps Go
/// does not update in `BatchUpdateAnalyzeJobSQL`.
#[test]
fn corrupted_analyze_job_cleanup_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    let local_instance = "127.0.0.1:0";
    let now = tidb_exec::mysql_bootstrap::utc_now_timestamp();
    let old = now
        .add_duration(MySqlDuration::from_nanoseconds(-660_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    for (process_id, instance, created_at) in [
        (1, local_instance, old),
        (2, local_instance, old),
        (3, local_instance, old),
        (4, local_instance, old),
        (5, local_instance, old),
        (6, local_instance, now),
        (7, "10.0.0.1:4000", old),
    ] {
        handle
            .commit_stats_plan(|snapshot, _| {
                let (_, plan) = tidb_exec::cluster_stats_write::plan_insert_analyze_job(
                    snapshot,
                    &stack.factory.catalog.load(),
                    "test",
                    "t",
                    "",
                    b"job",
                    instance,
                    process_id,
                    created_at,
                )
                .map_err(|error| error.to_string())?;
                Ok(plan)
            })
            .expect("pending job commits");
    }
    for id in [2, 3, 4] {
        handle
            .commit_stats_plan(|snapshot, _| {
                tidb_exec::cluster_stats_write::plan_start_analyze_job(
                    snapshot,
                    &stack.factory.catalog.load(),
                    id,
                    old,
                )
                .map_err(|error| error.to_string())
            })
            .expect("running job commits");
    }
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                2,
                0,
                None,
                old,
            )
            .map_err(|error| error.to_string())
        })
        .expect("temporary finish commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                2,
                old,
            )
            .map_err(|error| error.to_string())
        })
        .expect("NULL-process running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                5,
                0,
                None,
                old,
            )
            .map_err(|error| error.to_string())
        })
        .expect("finished job commits");

    let processes = stack.factory.processes();
    let _analyze = processes.register(
        3,
        "root".to_owned(),
        "local".to_owned(),
        "test".to_owned(),
        None,
    );
    processes.statement_started(3, "/*+ hint */ANALYZE TABLE test.t", "executing");
    let _ordinary = processes.register(
        4,
        "root".to_owned(),
        "local".to_owned(),
        "test".to_owned(),
        None,
    );
    processes.statement_started(4, "SELECT 1", "executing");

    let cutoff = now
        .add_duration(MySqlDuration::from_nanoseconds(-600_000_000_000, 0).unwrap())
        .unwrap();
    let update_times_before = displayed(rows(
        &mut session,
        "SELECT id, update_time FROM mysql.analyze_jobs ORDER BY id",
    ));
    stack
        .factory
        .cleanup_corrupted_analyze_jobs_on_current_instance(cutoff)
        .expect("current-instance cleanup commits");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, state, process_id IS NULL, fail_reason IS NULL \
             FROM mysql.analyze_jobs ORDER BY id",
        )),
        [
            ["1", "failed", "1", "0"],
            ["2", "running", "1", "1"],
            ["3", "running", "0", "1"],
            ["4", "failed", "1", "0"],
            ["5", "finished", "1", "1"],
            ["6", "pending", "0", "1"],
            ["7", "pending", "0", "1"],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, update_time FROM mysql.analyze_jobs ORDER BY id",
        )),
        update_times_before,
        "BatchUpdateAnalyzeJobSQL does not refresh update_time",
    );

    stack
        .factory
        .cleanup_corrupted_analyze_jobs_on_dead_instances(cutoff)
        .expect("dead-instance cleanup commits");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT state, process_id IS NULL, fail_reason FROM mysql.analyze_jobs WHERE id = 7",
        )),
        [[
            "failed",
            "1",
            tidb_exec::cluster_stats_write::CORRUPTED_ANALYZE_JOB_FAILURE,
        ]]
    );
}

#[test]
fn analyze_job_cleanup_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory.start_analyze_jobs_cleanup_worker(crate::node_config::StatsLease::Zero);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(crate::node_config::StatsLease::Disabled);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(crate::node_config::StatsLease::Positive(
        Duration::from_secs(60),
    ));
    assert!(factory.analyze_jobs_cleanup_worker.get().is_some());
    drop(factory);
}

#[test]
fn auto_analyze_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory.start_auto_analyze_worker(crate::node_config::StatsLease::Zero);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(crate::node_config::StatsLease::Disabled);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(crate::node_config::StatsLease::Positive(
        Duration::from_secs(60),
    ));
    assert!(factory.auto_analyze_worker.get().is_some());
    drop(factory);
}

#[test]
fn auto_analyze_worker_observes_the_live_process_switch() {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_vardef::RUN_AUTO_ANALYZE.store(self.0, Ordering::SeqCst);
        }
    }
    let _restore = Restore(tidb_vardef::RUN_AUTO_ANALYZE.swap(true, Ordering::SeqCst));
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = &stack.factory;
    let owner = factory.stats_owner.as_ref().unwrap();
    owner.campaign_owner(&[]).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !owner.is_owner() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let queue = factory.auto_analyze_priority_queue(Duration::ZERO);
    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    assert!(queue.is_initialized());
    factory.start_auto_analyze_worker(crate::node_config::StatsLease::Positive(
        Duration::from_millis(10),
    ));
    tidb_vardef::RUN_AUTO_ANALYZE.store(false, Ordering::SeqCst);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while queue.is_initialized() {
        assert!(
            std::time::Instant::now() < deadline,
            "worker must observe Go's current process switch"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    owner.campaign_cancel();
}

/// Go creates one pending job for every physical partition task before
/// dispatch, then creates independent global-merge jobs after all partition
/// results have been saved. Global jobs never accumulate processed rows.
#[test]
fn partition_analyze_jobs_match_go_task_shapes() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_analyze_partitioned (a INT, KEY idx(a)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), \
         PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_analyze_partitioned VALUES (1),(11)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE stats_analyze_partitioned WITH 0 TOPN, 1 BUCKETS",
    );

    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT partition_name, job_info, processed_rows, state, process_id IS NULL \
             FROM mysql.analyze_jobs IGNORE INDEX \
             (PRIMARY, update_time, idx_schema_table_state, idx_schema_table_partition_state) \
             WHERE table_schema = 'test' AND table_name = 'stats_analyze_partitioned' \
             ORDER BY id",
        )),
        [
            [
                "p0",
                "analyze table all indexes, all columns with 1 buckets, 0 topn, 1 samplerate",
                "1",
                "finished",
                "1",
            ],
            [
                "p1",
                "analyze table all indexes, all columns with 1 buckets, 0 topn, 1 samplerate",
                "1",
                "finished",
                "1",
            ],
            [
                "",
                "merge global stats for test.stats_analyze_partitioned columns",
                "0",
                "finished",
                "1",
            ],
            [
                "",
                "merge global stats for test.stats_analyze_partitioned's index idx",
                "0",
                "finished",
                "1",
            ],
        ]
    );
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::TestIssue39336`:
/// permissive zero-in DATETIME values must not make the dynamic partition
/// global-statistics merge job fail.
#[test]
fn partition_global_analyze_finishes_with_zero_in_datetime_values() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(146))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE issue39336 (a DATETIME(3) DEFAULT NULL, b INT) \
         PARTITION BY RANGE (b) (\
         PARTITION p0 VALUES LESS THAN (1000),\
         PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    rows(&mut session, "SET @@sql_mode = ''");
    rows(&mut session, "SET @@tidb_analyze_version = 2");
    rows(&mut session, "SET @@tidb_partition_prune_mode = 'dynamic'");
    rows(
        &mut session,
        "INSERT INTO issue39336 VALUES \
         ('1000-00-09 00:00:00.000',1),\
         ('1000-00-06 00:00:00.000',1),\
         ('1000-00-06 00:00:00.000',1),\
         ('2022-11-23 14:24:30.000',1),\
         ('2022-11-23 14:24:32.000',1),\
         ('2022-11-23 14:24:33.000',1),\
         ('2022-11-23 14:24:35.000',1),\
         ('2022-11-23 14:25:08.000',1001),\
         ('2022-11-23 14:25:09.000',1001)",
    );
    rows(&mut session, "ANALYZE TABLE issue39336 WITH 0 TOPN");

    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW ANALYZE STATUS WHERE job_info LIKE 'merge global stats%' \
             AND table_name = 'issue39336'",
        ))
        .into_iter()
        .map(|row| (row[3].clone(), row[7].clone()))
        .collect::<Vec<_>>(),
        [(
            "merge global stats for test.issue39336 columns".to_owned(),
            "finished".to_owned(),
        )]
    );
}

/// Pinned `TestSystemTableDDLHasNoEvent`: `asyncNotifyEvent` suppresses every
/// stats subscriber event for `metadef.IsMemOrSysDB`, so system-table DDL
/// changes schema metadata but never creates or refreshes stats rows.
#[test]
fn system_table_ddl_does_not_publish_statistics_events_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    let assert_no_stats = |session: &mut ClusterServerSession, ids: &[i64]| {
        let ids = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
        assert_eq!(
            displayed(rows(
                session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id IN ({ids})"),
            )),
            [["0"]]
        );
    };

    rows(&mut session, "CREATE TABLE mysql.stats_no_event (a INT)");
    let old_id = stack
        .factory
        .catalog
        .load()
        .find_table("mysql", "stats_no_event")
        .expect("system table is published")
        .1
        .id;
    assert_no_stats(&mut session, &[old_id]);
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event ADD COLUMN b INT",
    );
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event ADD COLUMN c INT, ADD COLUMN d INT",
    );
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event MODIFY COLUMN a BIGINT",
    );
    assert_no_stats(&mut session, &[old_id]);
    rows(&mut session, "TRUNCATE TABLE mysql.stats_no_event");
    let new_id = stack
        .factory
        .catalog
        .load()
        .find_table("mysql", "stats_no_event")
        .expect("truncated system table is published")
        .1
        .id;
    assert_ne!(new_id, old_id);
    assert_no_stats(&mut session, &[old_id, new_id]);
    rows(&mut session, "DROP TABLE mysql.stats_no_event");
    assert_no_stats(&mut session, &[old_id, new_id]);

    rows(
        &mut session,
        "CREATE TABLE mysql.stats_no_event_part (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10))",
    );
    let (logical_id, p0_id) = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
        let definitions = table
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot();
        (table.id, definitions[0].id)
    };
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event_part ADD PARTITION (\
         PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    let p1_id = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
        let definitions = table
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot();
        definitions
            .into_iter()
            .find(|definition| definition.name.lowercase() == "p1")
            .expect("p1 exists")
            .id
    };
    assert_no_stats(&mut session, &[logical_id, p0_id, p1_id]);
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event_part TRUNCATE PARTITION p1",
    );
    let replacement_p1_id = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
        let definitions = table
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot();
        definitions
            .into_iter()
            .find(|definition| definition.name.lowercase() == "p1")
            .expect("p1 exists")
            .id
    };
    assert_ne!(replacement_p1_id, p1_id);
    assert_no_stats(&mut session, &[logical_id, p0_id, p1_id, replacement_p1_id]);
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event_part DROP PARTITION p1",
    );
    rows(&mut session, "DROP TABLE mysql.stats_no_event_part");
    assert_no_stats(&mut session, &[logical_id, p0_id, p1_id, replacement_p1_id]);
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestStatsCacheShouldNotCacheSystemTable`. The SHOW scans may enumerate
/// every schema, but they must not publish system-table objects into the
/// canonical statistics cache.
#[test]
fn show_stats_does_not_cache_system_table_statistics() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(147))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE system_cache_guard (a INT)");
    rows(
        &mut session,
        "INSERT INTO system_cache_guard VALUES (1),(2),(3)",
    );
    rows(&mut session, "ANALYZE TABLE system_cache_guard");

    let before = stack.factory.stats().receipt();
    assert_eq!(before.loaded, 1);
    rows(&mut session, "SHOW STATS_META");
    rows(&mut session, "SHOW STATS_HEALTHY");
    assert_eq!(stack.factory.stats().receipt(), before);
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestUninitializedStatsStatus`. DDL placeholders and delta-only metadata
/// are not initialized histograms, and both settings of the outdated-stats
/// switch retain the planner's pseudo fallback.
#[test]
fn uninitialized_statistics_remain_hidden_and_pseudo() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(148))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE uninitialized_stats (a INT, b INT, c INT, INDEX idx_a(a))",
    );
    rows(
        &mut session,
        "INSERT INTO uninitialized_stats VALUES (1,2,2), (3,4,4), (5,6,6), (7,8,8), (9,10,10)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");

    assert!(rows(
        &mut session,
        "SHOW STATS_HISTOGRAMS WHERE db_name = 'test' AND table_name = 'uninitialized_stats'",
    )
    .is_empty());
    for enabled in ["ON", "OFF"] {
        rows(
            &mut session,
            &format!("SET @@tidb_enable_pseudo_for_outdated_stats = {enabled}"),
        );
        let explain = displayed(rows(
            &mut session,
            "EXPLAIN SELECT * FROM uninitialized_stats",
        ));
        assert!(
            explain
                .iter()
                .flatten()
                .any(|value| value.contains("stats:pseudo")),
            "uninitialized statistics must stay pseudo with the switch {enabled}: {explain:?}"
        );
    }
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestSkipMissingPartitionStats`. Dynamic global merge keeps the logical
/// row count, accounts for the unanalyzed partition as modifications, and
/// publishes every merged column/index when missing partition statistics are
/// explicitly skipped.
#[test]
fn global_statistics_skip_missing_partition_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(149))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "SET SESSION tidb_skip_missing_partition_stats = ON",
    );
    rows(
        &mut session,
        "CREATE TABLE skip_missing_stats (a INT, b INT, c INT, INDEX idx_b(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN (300))",
    );
    rows(
        &mut session,
        "INSERT INTO skip_missing_stats VALUES (1,1,1), (2,2,2), \
         (101,101,101), (102,102,102), (201,201,201), (202,202,202)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(
        &mut session,
        "ANALYZE TABLE skip_missing_stats PARTITION p0, p1",
    );

    let global = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'skip_missing_stats' AND partition_name = 'global'",
    ));
    assert_eq!(global.len(), 1, "missing global statistics row: {global:?}");
    assert_eq!((global[0][4].as_str(), global[0][5].as_str()), ("2", "6"));
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_HISTOGRAMS WHERE table_name = 'skip_missing_stats' AND partition_name = 'global'",
        ))
        .len(),
        4,
        "all three columns and idx_b must be initialized"
    );
}

/// Pinned DDL subscriber `ActionModifyColumn`: `InsertColStats2KV` is an
/// `INSERT IGNORE`, but if the histogram is absent it recreates it before
/// refreshing meta. An original CREATE TABLE column has no origin default,
/// even when it has a declared INSERT default, so Go records all old rows as
/// NULL and creates no bucket. Both MODIFY and RENAME COLUMN are this action.
#[test]
fn modify_column_ddl_recreates_missing_default_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(74))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_modify_column (a INT DEFAULT 7)",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_modify_column VALUES (DEFAULT),(DEFAULT),(DEFAULT)",
    );
    rows(&mut session, "ANALYZE TABLE stats_modify_column");
    let (table_id, column_id) = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("test", "stats_modify_column")
            .expect("created table is published")
            .1;
        (
            table.id,
            table.columns.get(0).expect("column exists").read().id,
        )
    };

    let remove_column_stats = |session: &mut ClusterServerSession| {
        rows(
            session,
            &format!(
                "DELETE FROM mysql.stats_buckets WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {column_id}"
            ),
        );
        rows(
            session,
            &format!(
                "DELETE FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {column_id}"
            ),
        );
    };
    let assert_recreated = |session: &mut ClusterServerSession| {
        assert_eq!(
            displayed(rows(
                session,
                &format!(
                    "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                     FROM mysql.stats_histograms WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["0", "3", "0", "0"]]
        );
        assert_eq!(
            displayed(rows(
                session,
                &format!(
                    "SELECT count(*) FROM mysql.stats_buckets WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["0"]]
        );
    };

    remove_column_stats(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column MODIFY COLUMN a BIGINT",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_recreated(&mut session);

    remove_column_stats(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column RENAME COLUMN a TO b",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_recreated(&mut session);
}

/// Pinned `TestRecordHistoryStatsAfterAnalyze`: the global switch suppresses
/// task creation while off; once enabled, a successful ANALYZE posts its
/// physical ID and the domain dump path writes canonical blocks to
/// `mysql.stats_history`.
#[test]
fn analyze_records_historical_stats_through_the_domain_worker() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(75))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE history_analyze (a INT, b VARCHAR(10), INDEX idx(a, b))",
    );

    rows(
        &mut session,
        "SET GLOBAL tidb_enable_historical_stats = OFF",
    );
    rows(&mut session, "ANALYZE TABLE history_analyze WITH 2 TOPN");
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(&mut session, "ANALYZE TABLE history_analyze WITH 2 TOPN");
    let table_id = session
        .historical_stats_worker
        .get_one_historical_stats_table();
    assert!(table_id > 0, "a successful ANALYZE posts its table ID");
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    session
        .historical_stats_worker
        .dump_historical_stats(
            table_id,
            &handle,
            &tidb_domain::historical_stats::NoopHistoricalStatsMetrics,
        )
        .expect("historical statistics dump succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_history WHERE table_id = {table_id}"),
        )),
        [["1"]]
    );
    let catalog = stack.factory.catalog.load();
    let (_, table) = catalog
        .find_table("test", "history_analyze")
        .expect("analyzed table remains published");
    let table = table.clone();
    drop(catalog);
    let (historical, fallbacks) = stack
        .factory
        .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
        .expect("historical statistics reload succeeds");
    assert!(fallbacks.is_empty(), "unexpected fallbacks: {fallbacks:?}");
    assert!(
        historical
            .expect("historical statistics exist")
            .is_historical_stats
    );
}

/// Pinned `TestGCOutdatedHistoryStats`: the global retention duration drives
/// explicit historical metadata and payload cleanup.
#[test]
fn clear_outdated_history_stats_uses_the_go_retention_duration() {
    const TIMEZONE_TEST: &str = "TIDB_HISTORY_GC_TIMEZONE_TEST";
    if std::env::var_os(TIMEZONE_TEST).is_none() {
        let thread = std::thread::current();
        let name = thread.name().expect("named test thread");
        // Each child initializes its local clock in this zone without changing
        // the environment beneath other concurrently running tests.
        for timezone in ["UTC", "Asia/Shanghai", "America/Los_Angeles"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env(TIMEZONE_TEST, "1")
                .env("TZ", timezone)
                .output()
                .expect("start history GC timezone test");
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && stdout.contains("1 passed; 0 failed"),
                "history GC in {timezone}: {stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    }
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(&mut session, "CREATE TABLE history_gc (a INT)");
    rows(&mut session, "ANALYZE TABLE history_gc");
    let table_id = session
        .historical_stats_worker
        .get_one_historical_stats_table();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    session
        .historical_stats_worker
        .dump_historical_stats(
            table_id,
            &handle,
            &tidb_domain::historical_stats::NoopHistoricalStatsMetrics,
        )
        .expect("historical statistics dump succeeds");
    for table in ["stats_meta_history", "stats_history"] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.{table} WHERE table_id = {table_id}"),
            )),
            [["1"]]
        );
    }

    rows(
        &mut session,
        "SET GLOBAL tidb_historical_stats_duration = '1h'",
    );
    stack
        .factory
        .clear_outdated_history_stats()
        .expect("fresh history survives GC");
    for table in ["stats_meta_history", "stats_history"] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.{table} WHERE table_id = {table_id}")
            )),
            [["1"]]
        );
    }
    rows(
        &mut session,
        "SET GLOBAL tidb_historical_stats_duration = '1s'",
    );
    std::thread::sleep(std::time::Duration::from_secs(2));
    stack
        .factory
        .clear_outdated_history_stats()
        .expect("historical statistics GC succeeds");
    for table in ["stats_meta_history", "stats_history"] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.{table} WHERE table_id = {table_id}"),
            )),
            [["0"]]
        );
    }
}

#[test]
fn stats_gc_runs_periodically_only_after_ownership_is_acquired() {
    let (stack, _users) = cop_backed_stack_with_stats_lease(Some(
        crate::node_config::StatsLease::Positive(Duration::from_millis(1)),
    ));
    let factory = &stack.factory;
    let owner = factory.stats_owner.as_ref().unwrap();
    let mut session = factory.open_session(session_context(79)).unwrap();
    let gc_marker = "SELECT count(*) FROM mysql.tidb WHERE variable_name = 'tidb_stats_gc_last_ts'";
    std::thread::sleep(Duration::from_millis(220));
    assert_eq!(displayed(rows(&mut session, gc_marker)), [["0"]]);
    rows(
        &mut session,
        "CREATE TABLE test.periodic_gc (a INT, KEY idx(a))",
    );
    rows(&mut session, "INSERT INTO test.periodic_gc VALUES (1),(2)");
    rows(&mut session, "ANALYZE TABLE test.periodic_gc WITH 0 TOPN");
    rows(&mut session, "ALTER TABLE test.periodic_gc DROP INDEX idx");
    // Make only the stats version old enough for Go's 10 * max(lease) window.
    // The production worker must retain the actual schema lease, not use zero.
    rows(&mut session, "UPDATE mysql.stats_meta SET version = 1");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms WHERE is_index = 1"
        )),
        [["1"]]
    );
    owner.campaign_owner(&[]).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if displayed(rows(&mut session, gc_marker)) == [["1"]] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "positive statistics lease must run GC through the elected owner without a manual call"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms WHERE is_index = 1"
        )),
        [["0"]]
    );
    owner.campaign_cancel();
    // A later ownership term must keep the same worker and continue after
    // storage errors. An invalid persisted watermark is a real GC read error.
    std::thread::sleep(Duration::from_millis(220));
    rows(&mut session, "UPDATE mysql.tidb SET variable_value = 'invalid' WHERE variable_name = 'tidb_stats_gc_last_ts'");
    owner.campaign_owner(&[]).unwrap();
    std::thread::sleep(Duration::from_millis(220));
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tidb_stats_gc_last_ts'"
        )),
        [["invalid"]]
    );
    rows(
        &mut session,
        "UPDATE mysql.tidb SET variable_value = '0' WHERE variable_name = 'tidb_stats_gc_last_ts'",
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if displayed(rows(
            &mut session,
            "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tidb_stats_gc_last_ts'",
        )) != [["0"]]
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a failed GC pass must not retire the owner worker"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    owner.campaign_cancel();
}

#[test]
fn stats_maintenance_is_absent_for_zero_and_negative_leases() {
    for lease in [
        crate::node_config::StatsLease::Zero,
        crate::node_config::StatsLease::Disabled,
    ] {
        let (stack, _users) = cop_backed_stack_with_stats_lease(Some(lease));
        assert!(stack._stats_maintenance.is_none());
    }
}

/// Pinned storage `TestGCStats`: stale index and column rows are removed one
/// item transaction at a time, while a dropped table needs one pass to clear
/// payload and a later pass to remove `stats_meta`.

#[test]
fn stats_gc_matches_go_item_and_dropped_table_phases() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE stats_gc_t (a INT, b INT, INDEX idx(a, b), INDEX idx_a(a))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_t VALUES (1,1),(2,2),(3,3)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_t WITH 0 TOPN");

    rows(&mut session, "ALTER TABLE stats_gc_t DROP INDEX idx");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("index statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["3".to_owned()]]
    );

    rows(&mut session, "ALTER TABLE stats_gc_t DROP INDEX idx_a");
    rows(&mut session, "ALTER TABLE stats_gc_t DROP COLUMN a");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("column statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["1".to_owned()]]
    );

    rows(&mut session, "DROP TABLE stats_gc_t");
    // Embedded DDL and GC can share one physical millisecond. Go's TSO has a
    // positive logical component while GC's wall-clock TSO has zero, so wait
    // until the strict `version < gcVer` window can include the drop event.
    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("first dropped-table GC phase succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["0".to_owned()]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["1".to_owned()]]
    );

    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("second dropped-table GC phase succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["0".to_owned()]]
    );
}

/// Pinned storage `TestGCPartition`, including its retained logical
/// meta-only row after both physical partitions finish two-phase GC.
#[test]
fn stats_gc_matches_go_partition_phases() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'static'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_gc_partition (a BIGINT, b BIGINT, INDEX idx(a, b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (3), \
         PARTITION p1 VALUES LESS THAN (6))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_partition VALUES (1,2),(2,3),(3,4),(4,5),(5,6)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_partition WITH 0 TOPN");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["6".to_owned()]]
    );

    rows(
        &mut session,
        "ALTER TABLE stats_gc_partition DROP INDEX idx",
    );
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("partition index statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["4".to_owned()]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_buckets",
        )),
        vec![vec!["10".to_owned()]]
    );

    rows(&mut session, "ALTER TABLE stats_gc_partition DROP COLUMN b");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("partition column statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["2".to_owned()]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_buckets",
        )),
        vec![vec!["5".to_owned()]]
    );

    rows(&mut session, "DROP TABLE stats_gc_partition");
    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("first partition-table GC phase succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["3".to_owned()]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["0".to_owned()]]
    );

    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("second partition-table GC phase succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["1".to_owned()]]
    );
}

/// Pinned storage `TestGCColumnStatsUsage`: a dropped column removes only its
/// usage row, then dropped-table GC removes every remaining usage row.
#[test]
fn stats_gc_matches_go_column_usage_cleanup() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(81))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_gc_usage (a INT, b INT, c INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_usage VALUES (1,1,1),(2,2,2),(3,3,3)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_usage");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_gc_usage")
        .expect("table is in the live catalog")
        .1
        .id;
    rows(
        &mut session,
        &format!(
            "INSERT INTO mysql.column_stats_usage(table_id,column_id) VALUES \
             ({table_id},1),({table_id},2),({table_id},3)"
        ),
    );

    rows(&mut session, "ALTER TABLE stats_gc_usage DROP COLUMN a");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("dropped-column usage GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.column_stats_usage",
        )),
        vec![vec!["2".to_owned()]]
    );

    rows(&mut session, "DROP TABLE stats_gc_usage");
    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("dropped-table usage GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.column_stats_usage",
        )),
        vec![vec!["0".to_owned()]]
    );
}

/// Pinned storage `TestExtremCaseOfGC`: an existing table with an empty
/// histogram set must keep its `stats_meta` row.
#[test]
fn stats_gc_keeps_meta_for_existing_table_without_histograms() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(82))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_gc_empty_hist (a INT, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_empty_hist VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_empty_hist");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_gc_empty_hist")
        .expect("table is in the live catalog")
        .1
        .id;
    rows(
        &mut session,
        &format!("DELETE FROM mysql.stats_histograms WHERE table_id = {table_id}"),
    );
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("empty-histogram GC succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["1".to_owned()]]
    );
}

/// Pinned `TestDumpHistoricalStatsFallback`: after ANALYZE ran with history
/// disabled, historical dump uses the latest statistics and names that table
/// in the fallback list once the feature is enabled.
#[test]
fn historical_stats_reader_falls_back_to_latest_stats_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(77))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE history_fallback (a INT, INDEX idx(a)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))",
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_enable_historical_stats = OFF",
    );
    rows(&mut session, "ANALYZE TABLE history_fallback");
    let catalog = stack.factory.catalog.load();
    let (_, table) = catalog
        .find_table("test", "history_fallback")
        .expect("analyzed table remains published");
    let table = table.clone();
    drop(catalog);
    assert_eq!(
        stack
            .factory
            .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
            .expect_err("the Go feature gate rejects historical reads"),
        "tidb_enable_historical_stats should be enabled"
    );
    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    let (latest, fallbacks) = stack
        .factory
        .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
        .expect("latest statistics fallback succeeds");
    assert_eq!(
        fallbacks,
        ["test.history_fallback p0", "test.history_fallback global"]
    );
    let latest = latest.expect("latest statistics exist");
    assert!(!latest.is_historical_stats);
    let partitions = latest.partitions.expect("partition statistics exist");
    assert!(
        !partitions["p0"]
            .as_ref()
            .expect("partition fallback exists")
            .is_historical_stats
    );
    assert!(
        !partitions[tidb_stats::TIDB_GLOBAL_STATS]
            .as_ref()
            .expect("global fallback exists")
            .is_historical_stats
    );
}

/// Pinned `TestDumpHistoricalStatsByTable`: static ANALYZE queues only the
/// physical partition, while dynamic ANALYZE queues that partition and the
/// logical table whose dump contains global statistics.
#[test]
fn partition_analyze_queues_the_same_historical_stats_ids_as_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(
        &mut session,
        "CREATE TABLE history_partition (a INT, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))",
    );
    let catalog = stack.factory.catalog.load();
    let (_, table) = catalog
        .find_table("test", "history_partition")
        .expect("partitioned table is published");
    let logical_id = table.id;
    let partition_id = table
        .get_partition_info()
        .expect("partition metadata")
        .read()
        .definitions
        .snapshot()[0]
        .id;
    let table = table.clone();
    drop(catalog);

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    rows(&mut session, "ANALYZE TABLE history_partition");
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        partition_id
    );
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "ANALYZE TABLE history_partition");
    let mut queued = vec![
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
    ];
    queued.sort_unstable();
    let mut expected = vec![partition_id, logical_id];
    expected.sort_unstable();
    assert_eq!(queued, expected);
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    for table_id in queued {
        session
            .historical_stats_worker
            .dump_historical_stats(
                table_id,
                &handle,
                &tidb_domain::historical_stats::NoopHistoricalStatsMetrics,
            )
            .expect("partition/global historical dump succeeds");
    }
    for table_id in [partition_id, logical_id] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_history WHERE table_id = {table_id}"),
            )),
            [["1"]]
        );
    }
    let (historical, _) = stack
        .factory
        .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
        .expect("partition historical statistics reload succeeds");
    let partitions = historical
        .expect("partition historical statistics exist")
        .partitions
        .expect("partition map exists");
    assert!(partitions["p0"].is_some());
    assert!(partitions[tidb_stats::TIDB_GLOBAL_STATS].is_some());
}

/// The probe-33 regression: a derived table whose inner SELECT plans as a
/// partial-aggregate push (root HashAgg over `TableReader(data:HashAgg)`)
/// must still answer the aggregate, not the bare scan rows. Go returns
/// `((1,30),(2,120))`; the broken handoff returned five raw rows.
#[test]
fn a_derived_aggregate_over_the_coprocessor_answers_its_output() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(7))
        .expect("session opens");
    rows(&mut session, "CREATE TABLE test.rep (g int, v int)");
    rows(
        &mut session,
        "INSERT INTO test.rep VALUES (1, 10), (1, 20), (2, 40), (2, 80), (1, 0)",
    );

    let inner = displayed(rows(
        &mut session,
        "SELECT g, sum(v) AS t FROM test.rep GROUP BY g ORDER BY g",
    ));
    assert_eq!(
        inner,
        [["1", "30"], ["2", "120"]],
        "the inner aggregate alone must already be right"
    );

    let derived = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, sum(v) AS t FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(
        derived,
        [["1", "30"], ["2", "120"]],
        "the derived consumer must see the aggregate, not the scan rows"
    );

    // The COUNT(*) shape panicked the worker thread on the live node
    // (chunk column index out of bounds); here a panic fails the test.
    let counted = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, count(*) AS c FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(counted, [["1", "3"], ["2", "2"]]);

    // The desc keep-order Limit rides the REVERSED region walk -- the
    // shape whose first live draft returned NOTHING because the reverse
    // scan's caller-swaps-the-bounds contract was missed.
    rows(
        &mut session,
        "CREATE TABLE test.walk (id bigint primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.walk VALUES (1, 10), (2, 20), (3, 30), (5, 50), (100, 1)",
    );
    let descending = displayed(rows(
        &mut session,
        "SELECT id FROM test.walk WHERE id > 1 ORDER BY id DESC LIMIT 2",
    ));
    assert_eq!(
        descending,
        [["100"], ["5"]],
        "the desc keep-order Limit must answer the LARGEST ids over the region walk"
    );

    // The covering-index COUNT rides an [IndexScan, Aggregation] DAG:
    // the region decodes the indexed values out of the KEY and counts
    // them, Go's PhysicalIndexReader carrying the partial stage.
    rows(&mut session, "CREATE INDEX walk_v ON test.walk (v)");
    let counted_over_index = displayed(rows(
        &mut session,
        "SELECT count(v) FROM test.walk WHERE v > 5",
    ));
    assert_eq!(counted_over_index, [["4"]]);

    // A full covering SUM uses the unordered Global aggregate contract. Its
    // input is indexed in the pruned scan schema, so lowering must translate
    // that offset back through the table schema before reading index keys.
    let summed_over_index = displayed(rows(&mut session, "SELECT sum(v) FROM test.walk"));
    assert_eq!(summed_over_index, [["111"]]);
}

/// Go `setDataForServersInfo` (`infoschema_reader.go:2730`) over
/// `GetAllServerInfo`: one row per server, in Go's eight-column order.
/// With no etcd client the syncer answers THIS node alone -- Go's
/// `etcdCli == nil` path -- which is what a single-node deployment shows.
#[test]
fn tidb_servers_info_reports_this_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(11))
        .expect("session opens");

    let rows = displayed(rows(
        &mut session,
        "SELECT DDL_ID, IP, PORT, STATUS_PORT, LEASE, VERSION, GIT_HASH, LABELS \
         FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(rows.len(), 1, "a single node reports itself alone");
    let row = &rows[0];

    // Go's DDL_ID is `uuid.New().String()`; the shape is what a peer's
    // stale-entry match and this reader both see.
    assert_eq!(row[0].len(), 36, "DDL_ID is a uuid: {}", row[0]);
    assert_eq!(row[0].matches('-').count(), 4, "{}", row[0]);
    // The port the node was configured with, as an integer column.
    assert_eq!(row[2], "0", "the fixture binds an ephemeral port");
    assert_eq!(row[3], "10080", "the default status port");
    // Go fills LEASE with the configured lease text verbatim
    // (`serverinfo.ServerInfo.Lease: cfg.Lease`); the fixture keeps Go's
    // default, `DefSchemaLease.String()`. The version pair is the build's.
    assert_eq!(row[4], "45s", "LEASE is the configured text");
    assert!(!row[5].is_empty(), "VERSION is reported");
    // No labels are configured, which renders as the empty string rather
    // than a stray separator (Go `BuildStringFromLabels`).
    assert_eq!(row[7], "");
}

/// Go `ALTER TABLE ... [FORCE] AUTO_INCREMENT = n` over the real node.
///
/// The DDL half -- the stored `AutoIncID` and the counter key -- is pinned in
/// `tidb-exec`. What only this stack can show is that the node's LIVE
/// allocator notices: it caches a reserved range that outlives schema
/// reloads by design, so a rebase that moved only the meta keys would leave
/// the next INSERT allocating from the range reserved before the change, and
/// the statement would look like it did nothing.
#[test]
fn a_rebased_auto_increment_reaches_the_next_insert() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(13))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.seq (id bigint primary key auto_increment, v int)",
    );
    rows(&mut session, "INSERT INTO test.seq (v) VALUES (1)");

    // FORCE sets the base exactly, even below the counter the first
    // reservation already wrote.
    rows(
        &mut session,
        "ALTER TABLE test.seq FORCE AUTO_INCREMENT = 500",
    );
    rows(&mut session, "INSERT INTO test.seq (v) VALUES (2)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.seq ORDER BY v")),
        [["1"], ["500"]],
        "the forced base is what the next INSERT allocates"
    );

    // Without FORCE the base is floored at the allocator's next id, and Go
    // says so rather than silently doing something else. The reservation
    // taken above ends at 500 + the default step, so the floor is well past
    // the 5 that was asked for.
    rows(&mut session, "ALTER TABLE test.seq AUTO_INCREMENT = 5");
    let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0][0], "Warning");
    assert_eq!(warnings[0][1], "1105");
    assert!(
        warnings[0][2].starts_with("Can't reset AUTO_INCREMENT to 5 without FORCE option, using "),
        "{}",
        warnings[0][2]
    );
}

/// Go `ShowDDLExec.Next` (`executor/show_ddl.go`): six columns describing the
/// DDL owner and this node.
///
/// `SCHEMA_VER` is the version this node currently follows, so it moves when a
/// catalog change lands. The owner columns name THIS node, which is what a
/// single-node deployment reports and what this node truthfully is: it runs no
/// election, and every catalog change it accepts, it performs itself. The two
/// job-list columns are structurally empty because a change is published in
/// one transaction rather than queued, so no later statement can observe one
/// in flight.
#[test]
fn admin_show_ddl_reports_this_node_and_the_followed_version() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(17))
        .expect("session opens");

    let before = displayed(rows(&mut session, "ADMIN SHOW DDL"));
    assert_eq!(before.len(), 1);
    let row = &before[0];
    let version: i64 = row[0].parse().expect("SCHEMA_VER is an integer");
    // Go's DDL_ID is a uuid, and the owner and self are the same node here.
    assert_eq!(row[1].len(), 36, "OWNER_ID is a uuid: {}", row[1]);
    assert_eq!(row[1], row[4], "this node is its own owner");
    assert!(
        row[2].contains(':'),
        "OWNER_ADDRESS is host:port: {}",
        row[2]
    );
    assert_eq!(row[3], "", "no job is ever observably in flight");
    assert_eq!(row[5], "", "and so no query is either");

    // The reported version follows the catalog, so a change moves it.
    rows(
        &mut session,
        "CREATE TABLE test.ddl_probe (id int primary key)",
    );
    let after = displayed(rows(&mut session, "ADMIN SHOW DDL"));
    let moved: i64 = after[0][0].parse().expect("SCHEMA_VER is an integer");
    assert!(
        moved > version,
        "a published change moves SCHEMA_VER: {version} -> {moved}"
    );
    assert_eq!(after[0][4], row[4], "the node identity is stable");

    // The identity is the one TIDB_SERVERS_INFO reports for this node.
    let servers = displayed(rows(
        &mut session,
        "SELECT DDL_ID FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(servers, [[row[4].clone()]]);
}

/// Go `dataForTiDBClusterInfo` (`infoschema_reader.go:1842`) over
/// `GetClusterServerInfo`: one row per node, describing where it is and how
/// long it has been up.
///
/// Only the TiDB source of Go's seven retrievers is implemented here. This
/// regression checks that no other nodes are fabricated; full cluster discovery
/// remains an open parity gap.
#[test]
fn cluster_info_reports_this_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(19))
        .expect("session opens");

    let reported = displayed(rows(
        &mut session,
        "SELECT TYPE, INSTANCE, STATUS_ADDRESS, VERSION, GIT_HASH, UPTIME, SERVER_ID \
         FROM information_schema.cluster_info",
    ));
    assert_eq!(reported.len(), 1, "a single node reports itself alone");
    let row = &reported[0];
    assert_eq!(row[0], "tidb");
    // Both addresses are host:port, and they are the node's own two ports.
    assert!(row[1].contains(':'), "INSTANCE is host:port: {}", row[1]);
    assert!(
        row[2].contains(':'),
        "STATUS_ADDRESS is host:port: {}",
        row[2]
    );
    assert_ne!(row[1], row[2], "the SQL and status ports differ");
    assert!(!row[3].is_empty(), "VERSION is reported");
    assert!(!row[4].is_empty(), "GIT_HASH is reported");
    // Go prints `time.Since(startTime).String()`, so the unit is spelled out.
    assert!(
        row[5].ends_with('s'),
        "UPTIME is a Go duration string: {}",
        row[5]
    );

    // The instance is the same node TIDB_SERVERS_INFO describes, which is the
    // point of the two tables agreeing.
    let servers = displayed(rows(
        &mut session,
        "SELECT IP, PORT FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(row[1], format!("{}:{}", servers[0][0], servers[0][1]));
}

/// A `DATETIME(n)`/`TIMESTAMP(n) DEFAULT CURRENT_TIMESTAMP(n)` column must
/// survive the round trip through this node's own catalog loader.
///
/// Go stores the marker WORD alone and re-derives the fsp from the column's
/// decimal wherever the default is printed. The loader used to rebuild the
/// written spelling as a bare word and then apply Go's admission-time
/// "written fsp must equal the column's" check to it, which no bare word can
/// satisfy for a column with an fsp. It therefore REFUSED a table its own
/// DDL had just published -- the worst shape a DDL can take, since CREATE
/// then reports 1050 while every read reports 1146.
#[test]
fn a_fractional_clock_default_survives_the_catalog_loader() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(23))
        .expect("session opens");

    rows(
        &mut session,
        "CREATE TABLE test.dt (o datetime(3) DEFAULT CURRENT_TIMESTAMP(3), v int)",
    );
    rows(
        &mut session,
        "CREATE TABLE test.ts (o timestamp(6) DEFAULT CURRENT_TIMESTAMP(6))",
    );

    // The table is READABLE, which is what the refusal used to break.
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE test.dt"));
    assert!(
        shown[0][1].contains("`o` datetime(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        "{}",
        shown[0][1]
    );
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE test.ts"));
    assert!(
        shown[0][1].contains("`o` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)"),
        "{}",
        shown[0][1]
    );

    // And the marker still evaluates per row rather than storing the word.
    rows(&mut session, "INSERT INTO test.dt (v) VALUES (1)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT v, o IS NOT NULL FROM test.dt")),
        [["1", "1"]]
    );
}

/// THE INVARIANT: every `TableInfo` this node's DDL publishes, its own
/// catalog loader must load.
///
/// Breaking it produces the worst shape a DDL can take -- the CREATE reports
/// success, a later CREATE of the same name reports 1050, and every read
/// reports 1146 -- and it broke for real on
/// `DATETIME(n) DEFAULT CURRENT_TIMESTAMP(n)`. Neither half's own tests could
/// catch that: the DDL wrote correct metadata and the loader correctly
/// refused what it was given. Only the two together show it.
///
/// The shapes below are the column and key forms this node admits. A new
/// admitted shape belongs here.
#[test]
fn every_shape_the_ddl_admits_the_loader_loads() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(29))
        .expect("session opens");

    const SHAPES: &[&str] = &[
        // Literal defaults, one per storage family.
        "a int DEFAULT 5",
        "a varchar(10) DEFAULT 'x'",
        "a decimal(10,2) DEFAULT 1.5",
        "a double DEFAULT 1.5",
        "a bit(8) DEFAULT b'101'",
        "a enum('x','y') DEFAULT 'y'",
        "a set('p','q') DEFAULT 'q'",
        "a date DEFAULT '2020-01-01'",
        "a time(3) DEFAULT '01:02:03.400'",
        "a year DEFAULT 2020",
        "a binary(4) DEFAULT 'ab'",
        "a char(3) CHARACTER SET latin1 DEFAULT 'q'",
        "a int UNSIGNED ZEROFILL DEFAULT 7",
        "a json",
        "a text",
        // The clock marker, at every fsp -- the shape that broke.
        "a timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "a datetime DEFAULT CURRENT_TIMESTAMP",
        "a datetime(3) DEFAULT CURRENT_TIMESTAMP(3)",
        "a timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)",
        // Keys and indexes.
        "id bigint PRIMARY KEY AUTO_INCREMENT, a int, KEY k(a)",
        "id varchar(20) PRIMARY KEY, a int",
        "id bigint, a int, PRIMARY KEY (id, a)",
        "id bigint PRIMARY KEY AUTO_RANDOM",
        "a int, b int, UNIQUE KEY u(a,b)",
        "a int, KEY k(a) COMMENT 'c'",
        "a int, KEY k(a) INVISIBLE",
        "id bigint PRIMARY KEY NONCLUSTERED, a int",
        "a int COMMENT 'col comment'",
    ];

    for (index, shape) in SHAPES.iter().enumerate() {
        let name = format!("test.shape{index}");
        rows(&mut session, &format!("CREATE TABLE {name} ({shape})"));
        // The read is the assertion: a table the loader dropped answers 1146
        // here while still colliding with a second CREATE.
        let loaded = displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = 'shape{index}'"
            ),
        ));
        assert_eq!(
            loaded,
            [["1"]],
            "the DDL published `{shape}` and the loader dropped it"
        );
        // And it is actually usable, not merely listed.
        rows(&mut session, &format!("SELECT * FROM {name}"));
    }
}

/// Go `fieldTypeFromPBColumn`: the coprocessor rebuilds each column's type
/// from the DAG request, and the type CODE alone is not the type.
///
/// Dropping the rest decoded the stored bytes under the wrong rules. An
/// `INT UNSIGNED` holding 4294967295 came back as -1 on the SCAN path while
/// the point-get path -- which builds its types from the catalog -- returned
/// the stored value: one table, two paths, two answers. Carrying the flag
/// then exposed the second half, since the region's filter understood only
/// `Datum::Int` and silently dropped every row of an unsigned column.
#[test]
fn unsigned_columns_survive_the_coprocessor_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(31))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.un (id int primary key, a tinyint unsigned, \
         b smallint unsigned, d int unsigned, e bigint unsigned)",
    );
    rows(
        &mut session,
        "INSERT INTO test.un VALUES (1, 255, 65535, 4294967295, 18446744073709551615), \
         (2, 1, 1, 1, 1)",
    );

    // The scan path returns the stored values, not their signed
    // reinterpretation.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, b, d, e FROM test.un WHERE id > 0"
        )),
        [
            ["255", "65535", "4294967295", "18446744073709551615"],
            ["1", "1", "1", "1"],
        ]
    );
    // And it agrees with the point-get path, which never lost the flag.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, d, e FROM test.un WHERE id = 1"
        )),
        [["255", "4294967295", "18446744073709551615"]]
    );

    // A pushed-down predicate over an unsigned column compares in the
    // unsigned domain, including past i64::MAX.
    for (predicate, expected) in [
        ("d > 2", "1"),
        ("a < 255", "1"),
        ("e > 9223372036854775807", "1"),
        ("e = 18446744073709551615", "1"),
        ("a IN (255, 1)", "2"),
    ] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.un WHERE {predicate}"),
            )),
            [[expected.to_owned()]],
            "`{predicate}` over the scan path"
        );
    }
}

/// The REST of Go `fieldTypeFromPBColumn`: flag is not the only field the
/// decode needs.
///
/// `elems` decides what an ENUM/SET ordinal means, and `decimal` the scale a
/// DECIMAL and the fsp a TIME/DATETIME read back with. The scan path rebuilt
/// none of them, so this pins each against the point-get path, which builds
/// its types from the catalog and therefore never lost them. A disagreement
/// here is the same class of bug as the unsigned one: one table, two paths,
/// two answers.
#[test]
fn the_scan_path_decodes_elems_and_scale_like_the_point_get_path() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(37))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.tt (id int primary key, en enum('alpha','beta','gamma'), \
         st set('p','q','r'), dc decimal(12,4), yr year, tm time(3), dt datetime(6))",
    );
    rows(
        &mut session,
        "INSERT INTO test.tt VALUES (1, 'gamma', 'q,r', 12345.6789, 2024, \
         '12:34:56.789', '2024-03-04 05:06:07.891011')",
    );

    let columns = "en, st, dc, yr, tm, dt";
    // `id = 1` is a point get; `id > 0 AND id < 2` is a scan of the same row.
    let point_get = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id = 1"),
    ));
    let scanned = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id > 0 AND id < 2"),
    ));
    assert_eq!(point_get, scanned, "the two paths must read one row alike");
    assert_eq!(
        scanned,
        [[
            "gamma".to_owned(),
            "q,r".to_owned(),
            "12345.6789".to_owned(),
            "2024".to_owned(),
            "12:34:56.789".to_owned(),
            "2024-03-04 05:06:07.891011".to_owned(),
        ]],
        "and both must read what was stored"
    );
}

/// An INDEX read and a TABLE scan of the same rows must agree, and both must
/// agree with what was stored.
///
/// The index path decodes from the index KEY rather than the row value, so it
/// is a second decode of the same data under the same signedness rules. The
/// table-scan path got those rules wrong once already (it rebuilt column
/// types from the DAG without the UNSIGNED flag); this pins the pair so a
/// change to either side cannot drift from the other.
#[test]
fn an_index_read_and_a_table_scan_agree_over_unsigned_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(41))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.ix (id int primary key, u bigint unsigned, KEY ku(u))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ix VALUES (1, 18446744073709551615), (2, 1), \
         (3, 9223372036854775808), (4, NULL), (5, 0)",
    );

    // Each predicate straddles the signed/unsigned boundary, where a signed
    // reading would answer differently.
    for predicate in [
        "u > 2",
        "u >= 9223372036854775808",
        "u = 18446744073709551615",
        "u < 9223372036854775808",
        "u IS NULL",
    ] {
        let indexed = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.ix WHERE {predicate}"),
        ));
        let scanned = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.ix IGNORE INDEX (ku) WHERE {predicate}"),
        ));
        assert_eq!(indexed, scanned, "`{predicate}`: index and scan disagree");
    }

    // The index also ORDERS in the unsigned domain, which is the reading a
    // signed key encoding would reverse at the top of the range.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT u FROM test.ix WHERE u IS NOT NULL ORDER BY u"
        )),
        [
            ["0"],
            ["1"],
            ["9223372036854775808"],
            ["18446744073709551615"],
        ]
    );
}

/// THE DIFFERENTIAL, as a test: a predicate pushed into the coprocessor and
/// the same predicate evaluated locally must select the same rows.
///
/// `WHERE p` is answered by the region's filter; `sum(CASE WHEN p ...)` is
/// answered by the local evaluator over the same rows. Any disagreement is a
/// silent wrong answer -- the region either invented a row or dropped one --
/// and no single-path test can see it, because each evaluator is correct
/// against its own inputs. This is how the UNSIGNED decode bug was found,
/// after every existing test passed straight through it.
///
/// The fixture is deliberately made of boundary values: agreement on
/// ordinary data proves nothing.
#[test]
fn a_pushed_down_predicate_selects_what_local_evaluation_selects() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(43))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.diff (id int primary key, i bigint, u bigint unsigned, \
         s varchar(20) COLLATE utf8mb4_general_ci, b varchar(20) COLLATE utf8mb4_bin, \
         dc decimal(12,3), d datetime)",
    );
    rows(
        &mut session,
        "INSERT INTO test.diff VALUES \
         (1, -9223372036854775808, 18446744073709551615, 'Hello', 'Hello', -999999.999, \
          '1000-01-01 00:00:00'), \
         (2, 0, 0, '', '', 0.000, '2024-06-15 12:30:45'), \
         (3, NULL, NULL, NULL, NULL, NULL, NULL), \
         (4, 9223372036854775807, 9223372036854775808, 'HELLO', 'HELLO', 999999.999, \
          '9999-12-31 23:59:59'), \
         (5, -1, 1, 'world', 'world', -0.001, '2000-02-29 00:00:00')",
    );

    const PREDICATES: &[&str] = &[
        // Signed and unsigned integers at their extremes.
        "i > 0",
        "i < 0",
        "i = -9223372036854775808",
        "u > 2",
        "u >= 9223372036854775808",
        "u = 18446744073709551615",
        // Three-valued logic.
        "i IS NULL",
        "i IS NOT NULL AND u > 0",
        "NOT (i > 0)",
        "i = 0 OR u = 0",
        "i IN (0, -1)",
        "i NOT IN (0)",
        // Collation-sensitive comparison, both sides of the pair.
        "s = 'hello'",
        "b = 'hello'",
        "s > 'HELLO'",
        // Other families, and cross-type coercion.
        "dc > 0",
        "dc = -0.001",
        "d > '2024-01-01'",
        "i = '0'",
        "i BETWEEN '-1' AND '1'",
    ];

    for predicate in PREDICATES {
        let pushed = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.diff WHERE {predicate}"),
        ));
        let local = displayed(rows(
            &mut session,
            &format!(
                "SELECT coalesce(sum(CASE WHEN ({predicate}) THEN 1 ELSE 0 END), 0) \
                 FROM test.diff"
            ),
        ));
        assert_eq!(
            pushed, local,
            "`{predicate}`: the region and the local evaluator disagree"
        );
    }
}

/// Statistics may change the PLAN; they must never change the ANSWER.
///
/// `ANALYZE` replaces pseudo estimates with real ones and the optimizer then
/// picks differently. What must hold either way is that every query returns
/// the same rows before and after: a cost decision that alters results is a
/// wrong answer no estimate can justify.
///
/// The plan TEXT changing shape across `ANALYZE` is not evidence of lost
/// pushdown -- see `crate::explain`'s first named divergence, where every row
/// prints task `root` whether or not the wire pushed anything, and
/// `analyze_does_not_stop_a_pushed_shape_reaching_the_region` for the receipt
/// that actually answers it.
#[test]
fn analyze_changes_the_plan_and_never_the_answer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(47))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.st (id int primary key, a int, u bigint unsigned, \
         s varchar(10), KEY ka(a), KEY ku(u))",
    );
    rows(
        &mut session,
        "INSERT INTO test.st VALUES (1,1,18446744073709551615,'x'), (2,1,0,'y'), \
         (3,2,9223372036854775808,'z'), (4,3,1,'w'), (5,3,NULL,NULL)",
    );

    const QUERIES: &[&str] = &[
        "SELECT id FROM test.st WHERE a = 1 ORDER BY id",
        "SELECT id FROM test.st WHERE a >= 2 ORDER BY id",
        "SELECT count(*) FROM test.st WHERE u > 2",
        "SELECT id FROM test.st WHERE u = 18446744073709551615",
        "SELECT a, count(*) FROM test.st GROUP BY a ORDER BY a",
        "SELECT id FROM test.st WHERE s IS NULL",
        "SELECT id FROM test.st ORDER BY u DESC LIMIT 2",
        "SELECT max(u), min(u) FROM test.st",
    ];

    let before: Vec<_> = QUERIES
        .iter()
        .map(|query| displayed(rows(&mut session, query)))
        .collect();

    rows(&mut session, "ANALYZE TABLE test.st");

    for (query, expected) in QUERIES.iter().zip(before) {
        assert_eq!(
            displayed(rows(&mut session, query)),
            expected,
            "`{query}` answered differently once statistics existed"
        );
    }
}

/// A grouped aggregate keeps the same answer once statistics exist.
#[test]
fn analyze_does_not_change_a_grouped_aggregate_answer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(53))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.pd (id int primary key, g int, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.pd VALUES (1,1,10),(2,1,20),(3,2,40),(4,2,80),(5,1,0)",
    );

    let query = "SELECT g, sum(v) FROM test.pd GROUP BY g ORDER BY g";
    let expected = displayed(rows(&mut session, query));

    rows(&mut session, "ANALYZE TABLE test.pd");

    assert_eq!(
        displayed(rows(&mut session, query)),
        expected,
        "the answer changed once statistics existed"
    );
}

/// Pinned `globalstats.TestShowGlobalStatsWithAsyncMergeGlobal` and
/// `TestShowGlobalStatsWithoutAsyncMergeGlobal`: static pruning publishes
/// only physical-partition statistics, while dynamic pruning also publishes
/// the logical table's global column and index statistics. Every SHOW surface
/// must traverse the same set.
#[test]
fn partition_analyze_show_surfaces_match_global_stats_visibility() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(57))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");

    let mut check = |table: &str,
                     prune_mode: &str,
                     meta_count: usize,
                     global_meta_count: usize,
                     bucket_count: usize,
                     global_bucket_count: usize,
                     histogram_count: usize,
                     global_histogram_count: usize,
                     healthy_count: usize,
                     global_healthy_count: usize| {
        rows(
            &mut session,
            &format!("SET SESSION tidb_partition_prune_mode = '{prune_mode}'"),
        );
        rows(
            &mut session,
            &format!("CREATE TABLE {table} (a int, KEY(a)) PARTITION BY HASH(a) PARTITIONS 2"),
        );
        rows(
            &mut session,
            &format!("INSERT INTO {table} VALUES (1), (2), (3), (4)"),
        );
        rows(
            &mut session,
            &format!("ANALYZE TABLE {table} WITH 0 TOPN, 1 BUCKETS"),
        );

        let show_count =
            |session: &mut _, statement: &str| displayed(rows(session, statement)).len();
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_META WHERE table_name = '{table}'"),
            ),
            meta_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_META WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_meta_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_BUCKETS WHERE table_name = '{table}'"),
            ),
            bucket_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_BUCKETS WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_bucket_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}'"),
            ),
            histogram_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_HISTOGRAMS WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_histogram_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_HEALTHY WHERE table_name = '{table}'"),
            ),
            healthy_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_HEALTHY WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_healthy_count
        );
    };

    check("gs_static", "static", 2, 0, 4, 0, 4, 0, 2, 0);
    check("gs_dynamic", "dynamic", 3, 1, 6, 2, 6, 2, 3, 1);
}

/// Pinned `globalstats.TestGlobalStatsHealthy`: global and physical metadata
/// receive the same flushed deltas, while health is calculated against each
/// object's own analyzed row count.
#[test]
fn partition_global_stats_health_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(102))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_health (a INT, KEY(a)) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20))",
    );

    let check_meta = |session: &mut _, expected: [(&str, &str); 3]| {
        let actual = displayed(rows(
            session,
            "SHOW STATS_META WHERE table_name = 'global_health'",
        ));
        assert_eq!(actual.len(), 3, "{actual:?}");
        assert_eq!(
            actual
                .iter()
                .map(|row| (row[4].as_str(), row[5].as_str()))
                .collect::<Vec<_>>(),
            expected
        );
    };
    let check_healthy = |session: &mut _, expected: [&str; 3]| {
        let actual = displayed(rows(
            session,
            "SHOW STATS_HEALTHY WHERE table_name = 'global_health'",
        ));
        assert_eq!(actual.len(), 3, "{actual:?}");
        assert_eq!(
            actual.iter().map(|row| row[3].as_str()).collect::<Vec<_>>(),
            expected
        );
    };
    let flush_and_update = |session: &mut _| {
        let reloads = stack._stats_reloader.stats().reloads;
        rows(session, "FLUSH STATS_DELTA *.*");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while stack._stats_reloader.stats().reloads == reloads {
            assert!(
                std::time::Instant::now() < deadline,
                "statistics update did not run after FLUSH STATS_DELTA"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    };

    rows(&mut session, "ANALYZE TABLE global_health");
    check_meta(&mut session, [("0", "0"), ("0", "0"), ("0", "0")]);
    check_healthy(&mut session, ["100", "100", "100"]);

    rows(&mut session, "INSERT INTO global_health VALUES (1),(2)");
    flush_and_update(&mut session);
    check_meta(&mut session, [("2", "2"), ("2", "2"), ("0", "0")]);
    check_healthy(&mut session, ["0", "0", "100"]);

    rows(
        &mut session,
        "INSERT INTO global_health VALUES (11),(12),(13),(14)",
    );
    flush_and_update(&mut session);
    check_meta(&mut session, [("6", "6"), ("2", "2"), ("4", "4")]);
    check_healthy(&mut session, ["0", "0", "0"]);

    rows(&mut session, "ANALYZE TABLE global_health");
    check_meta(&mut session, [("0", "6"), ("0", "2"), ("0", "4")]);
    check_healthy(&mut session, ["100", "100", "100"]);

    rows(
        &mut session,
        "INSERT INTO global_health VALUES (4),(5),(15),(16)",
    );
    flush_and_update(&mut session);
    check_meta(&mut session, [("4", "10"), ("2", "4"), ("2", "6")]);
    check_healthy(&mut session, ["33", "0", "50"]);
}

/// Pinned `globalstats.TestBuildGlobalLevelStats`: static ANALYZE publishes
/// physical rows only, dynamic ANALYZE adds the logical global row, and
/// predicate-column demand controls the complete histogram inventory.
#[test]
fn build_global_level_stats_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(103))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_level (a INT, b INT, c INT) PARTITION BY HASH(a) PARTITIONS 3",
    );
    rows(&mut session, "CREATE TABLE global_level_plain (a INT)");
    rows(
        &mut session,
        "INSERT INTO global_level VALUES (1,1,1),(3,12,3),(4,20,4),(2,7,2),(5,21,5)",
    );
    rows(
        &mut session,
        "INSERT INTO global_level_plain VALUES (1),(3),(4),(2),(5)",
    );
    rows(
        &mut session,
        "CREATE INDEX idx_global_level_ab ON global_level(a,b)",
    );
    rows(
        &mut session,
        "CREATE INDEX idx_global_level_b ON global_level(b)",
    );
    rows(&mut session, "SELECT * FROM global_level WHERE c = 0");
    rows(&mut session, "SELECT * FROM global_level_plain WHERE a = 0");
    stack
        .factory
        .dump_col_stats_usage_to_kv("default")
        .expect("predicate-column usage dump");

    let count_rows = |session: &mut _, table: &str| {
        let mut counts = displayed(rows(
            session,
            &format!("SHOW STATS_META WHERE table_name = '{table}'"),
        ))
        .into_iter()
        .map(|row| row[5].clone())
        .collect::<Vec<_>>();
        counts.sort();
        counts
    };
    let histogram_count = |session: &mut _, table: &str| {
        rows(
            session,
            &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}'"),
        )
        .len()
    };

    rows(
        &mut session,
        "ANALYZE TABLE global_level, global_level_plain",
    );
    assert_eq!(count_rows(&mut session, "global_level"), ["1", "2", "2"]);
    assert_eq!(histogram_count(&mut session, "global_level"), 15);
    assert_eq!(count_rows(&mut session, "global_level_plain"), ["5"]);
    assert_eq!(histogram_count(&mut session, "global_level_plain"), 1);

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "ANALYZE TABLE global_level, global_level_plain",
    );
    assert_eq!(
        count_rows(&mut session, "global_level"),
        ["1", "2", "2", "5"]
    );
    assert_eq!(histogram_count(&mut session, "global_level"), 20);
    assert_eq!(count_rows(&mut session, "global_level_plain"), ["5"]);
    assert_eq!(histogram_count(&mut session, "global_level_plain"), 1);

    rows(
        &mut session,
        "ANALYZE TABLE global_level INDEX idx_global_level_ab, idx_global_level_b",
    );
    assert_eq!(
        count_rows(&mut session, "global_level"),
        ["1", "2", "2", "5"]
    );
    assert_eq!(histogram_count(&mut session, "global_level"), 20);
}

#[test]
fn global_topn_merge_matches_issue_24349() {
    assert_global_topn_merge_matches_issue_24349(None, 104);
}

#[test]
fn concurrent_global_topn_merge_matches_issue_24349() {
    assert_global_topn_merge_matches_issue_24349(Some(2), 105);
}

/// Pinned `globalstats.TestIssues24349` and its concurrency-two twin: global
/// TopN candidates include values recovered from sibling partition histograms
/// before the remaining histogram buckets are merged.
fn assert_global_topn_merge_matches_issue_24349(
    merge_concurrency: Option<u64>,
    connection_id: u64,
) {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(connection_id))
        .expect("session opens");
    rows(&mut session, "USE test");
    if let Some(merge_concurrency) = merge_concurrency {
        rows(
            &mut session,
            &format!("SET GLOBAL tidb_merge_partition_stats_concurrency = {merge_concurrency}"),
        );
    }
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE global_topn_merge (a INT, b INT) PARTITION BY HASH(a) PARTITIONS 3",
    );
    rows(
        &mut session,
        "INSERT INTO global_topn_merge VALUES \
         (0,3),(0,3),(0,3),(0,2),(1,1),(1,2),(1,2),(1,2),(1,3),(1,4),(2,1),(2,1)",
    );
    rows(
        &mut session,
        "SELECT * FROM global_topn_merge WHERE a = 0 AND b = 3",
    );
    stack
        .factory
        .dump_col_stats_usage_to_kv("default")
        .expect("predicate-column usage dump");
    rows(
        &mut session,
        "ANALYZE TABLE global_topn_merge WITH 1 TOPN, 3 BUCKETS",
    );

    let mut global_topn = displayed(rows(
        &mut session,
        "SHOW STATS_TOPN WHERE table_name = 'global_topn_merge' AND partition_name = 'global'",
    ));
    global_topn.sort();
    assert_eq!(
        global_topn,
        [
            ["test", "global_topn_merge", "global", "a", "0", "1", "6"],
            ["test", "global_topn_merge", "global", "b", "0", "2", "4"],
        ]
    );

    rows(
        &mut session,
        "EXPLAIN SELECT * FROM global_topn_merge WHERE a > 0 AND b > 0",
    );
    let mut all_topn = displayed(rows(
        &mut session,
        "SHOW STATS_TOPN WHERE table_name = 'global_topn_merge'",
    ));
    all_topn.sort();
    assert_eq!(
        all_topn,
        [
            ["test", "global_topn_merge", "global", "a", "0", "1", "6"],
            ["test", "global_topn_merge", "global", "b", "0", "2", "4"],
            ["test", "global_topn_merge", "p0", "a", "0", "0", "4"],
            ["test", "global_topn_merge", "p0", "b", "0", "3", "3"],
            ["test", "global_topn_merge", "p1", "a", "0", "1", "6"],
            ["test", "global_topn_merge", "p1", "b", "0", "2", "3"],
            ["test", "global_topn_merge", "p2", "a", "0", "2", "2"],
            ["test", "global_topn_merge", "p2", "b", "0", "1", "2"],
        ]
    );

    let mut buckets = displayed(rows(
        &mut session,
        "SHOW STATS_BUCKETS WHERE table_name = 'global_topn_merge'",
    ));
    buckets.sort();
    assert_eq!(
        buckets,
        [
            [
                "test",
                "global_topn_merge",
                "global",
                "a",
                "0",
                "0",
                "4",
                "4",
                "0",
                "0",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "global",
                "a",
                "0",
                "1",
                "6",
                "2",
                "2",
                "2",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "global",
                "b",
                "0",
                "0",
                "8",
                "1",
                "1",
                "4",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "p0",
                "b",
                "0",
                "0",
                "1",
                "1",
                "2",
                "2",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "p1",
                "b",
                "0",
                "0",
                "2",
                "1",
                "1",
                "3",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "p1",
                "b",
                "0",
                "1",
                "3",
                "1",
                "4",
                "4",
                "0"
            ],
        ]
    );
}

/// Pinned `globalstats.TestMergeGlobalStatsForCMSketch`: a global equality
/// estimate uses the merged sketch while partition pruning retains p0.
#[test]
fn merged_global_cmsketch_drives_equality_estimate() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(106))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_cms (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    rows(
        &mut session,
        "INSERT INTO global_cms VALUES \
         (1),(2),(3),(4),(5),(6),(6),(NULL),(11),(12),(13),(14),(15),(16),(17),(18),(19),(19)",
    );
    rows(&mut session, "ANALYZE TABLE global_cms");

    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT * FROM global_cms WHERE a = 1",
        )),
        [
            [
                "TableReader",
                "1.00",
                "root",
                "partition:p0",
                "data:Selection"
            ],
            [
                "└─Selection",
                "1.00",
                "cop[tikv]",
                "",
                "eq(test.global_cms.a, 1)"
            ],
            [
                "  └─TableFullScan",
                "18.00",
                "cop[tikv]",
                "table:global_cms",
                "keep order:false"
            ],
        ]
    );
}

/// Pinned `globalstats.TestGlobalStatsData3`: composite-index global TopN,
/// buckets, and NDV preserve the source behavior for every covered key type.
#[test]
fn composite_index_global_stats_match_go_types() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(107))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );

    let cases = [
        (
            "tintint",
            "INT",
            "(1,1),(1,2),(2,1),(2,2),(2,3),(2,3),(3,1),(3,1),(3,1),\
             (11,1),(12,1),(12,2),(13,1),(13,1),(13,2),(13,2),(13,2)",
            ["1", "2", "3"],
        ),
        (
            "tintstr",
            "VARCHAR(32)",
            "(1,'1'),(1,'2'),(2,'1'),(2,'2'),(2,'3'),(2,'3'),(3,'1'),(3,'1'),(3,'1'),\
             (11,'1'),(12,'1'),(12,'2'),(13,'1'),(13,'1'),(13,'2'),(13,'2'),(13,'2')",
            ["1", "2", "3"],
        ),
        (
            "tintdouble",
            "DOUBLE",
            "(1,1),(1,2),(2,1),(2,2),(2,3),(2,3),(3,1),(3,1),(3,1),\
             (11,1),(12,1),(12,2),(13,1),(13,1),(13,2),(13,2),(13,2)",
            ["1", "2", "3"],
        ),
        (
            "tdoubledecimal",
            "DECIMAL(30,2)",
            "(1,1),(1,2),(2,1),(2,2),(2,3),(2,3),(3,1),(3,1),(3,1),\
             (11,1),(12,1),(12,2),(13,1),(13,1),(13,2),(13,2),(13,2)",
            ["1.00", "2.00", "3.00"],
        ),
        (
            "tstrdt",
            "DATETIME",
            "(1,'2000-01-01'),(1,'2000-01-02'),(2,'2000-01-01'),\
             (2,'2000-01-02'),(2,'2000-01-03'),(2,'2000-01-03'),\
             (3,'2000-01-01'),(3,'2000-01-01'),(3,'2000-01-01'),\
             (11,'2000-01-01'),(12,'2000-01-01'),(12,'2000-01-02'),\
             (13,'2000-01-01'),(13,'2000-01-01'),(13,'2000-01-02'),\
             (13,'2000-01-02'),(13,'2000-01-02')",
            [
                "2000-01-01 00:00:00",
                "2000-01-02 00:00:00",
                "2000-01-03 00:00:00",
            ],
        ),
    ];

    for (table, b_type, values, [one, two, three]) in cases {
        rows(
            &mut session,
            &format!(
                "CREATE TABLE {table} (a INT, b {b_type}, KEY(a,b)) \
                 PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), \
                 PARTITION p1 VALUES LESS THAN (20))"
            ),
        );
        rows(
            &mut session,
            &format!("INSERT INTO {table} VALUES {values}"),
        );
        rows(
            &mut session,
            &format!("ANALYZE TABLE {table} WITH 2 TOPN, 2 BUCKETS"),
        );

        let meta = displayed(rows(
            &mut session,
            &format!("SHOW STATS_META WHERE table_name = '{table}'"),
        ));
        assert_eq!(
            meta.iter().map(|row| row[5].as_str()).collect::<Vec<_>>(),
            ["17", "9", "8"],
            "{table} metadata"
        );

        let topn = displayed(rows(
            &mut session,
            &format!("SHOW STATS_TOPN WHERE table_name = '{table}' AND is_index = 1"),
        ))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>();
        assert_eq!(
            topn,
            [
                format!("test {table} global a 1 (3, {one}) 3"),
                format!("test {table} global a 1 (13, {two}) 3"),
                format!("test {table} p0 a 1 (2, {three}) 2"),
                format!("test {table} p0 a 1 (3, {one}) 3"),
                format!("test {table} p1 a 1 (13, {one}) 2"),
                format!("test {table} p1 a 1 (13, {two}) 3"),
            ],
            "{table} TopN"
        );

        let buckets = displayed(rows(
            &mut session,
            &format!("SHOW STATS_BUCKETS WHERE table_name = '{table}' AND is_index = 1"),
        ))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>();
        assert_eq!(
            buckets,
            [
                format!("test {table} global a 1 0 6 2 (1, {one}) (2, {three}) 0"),
                format!("test {table} global a 1 1 11 2 (11, {one}) (13, {one}) 0"),
                format!("test {table} p0 a 1 0 3 1 (1, {one}) (2, {one}) 0"),
                format!("test {table} p0 a 1 1 4 1 (2, {two}) (2, {two}) 0"),
                format!("test {table} p1 a 1 0 2 1 (11, {one}) (12, {one}) 0"),
                format!("test {table} p1 a 1 1 3 1 (12, {two}) (12, {two}) 0"),
            ],
            "{table} buckets"
        );

        let histograms = displayed(rows(
            &mut session,
            &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}' AND is_index = 1"),
        ));
        assert_eq!(
            histograms
                .iter()
                .map(|row| row[6].as_str())
                .collect::<Vec<_>>(),
            ["11", "6", "5"],
            "{table} NDV"
        );
    }
}

#[test]
fn scalar_column_and_index_global_stats_match_go() {
    assert_scalar_column_and_index_global_stats_match_go(None, 108);
}

#[test]
fn concurrent_scalar_column_and_index_global_stats_match_go() {
    assert_scalar_column_and_index_global_stats_match_go(Some(2), 109);
}

/// Pinned `globalstats.TestGlobalStatsData2` and its concurrency-two twin.
fn assert_scalar_column_and_index_global_stats_match_go(
    merge_concurrency: Option<u64>,
    connection_id: u64,
) {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(connection_id))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    if let Some(merge_concurrency) = merge_concurrency {
        rows(
            &mut session,
            &format!("SET GLOBAL tidb_merge_partition_stats_concurrency = {merge_concurrency}"),
        );
    }

    let cases = [
        (
            "tint",
            "CREATE TABLE tint (c INT, KEY(c)) PARTITION BY RANGE (c) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1),(2),(3),(4),(4),(5),(5),(5),(NULL),\
             (11),(12),(13),(14),(15),(16),(16),(16),(16),(17),(17)",
            ["1", "2", "3", "4", "5", "11", "13", "14", "15", "16", "17"],
        ),
        (
            "tdouble",
            "CREATE TABLE tdouble (a INT, c DOUBLE, KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,1),(2,2),(3,3),(4,4),(4,4),(5,5),(5,5),(5,5),(NULL,NULL),\
             (11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(16,16),(16,16),(16,16),(17,17),(17,17)",
            ["1", "2", "3", "4", "5", "11", "13", "14", "15", "16", "17"],
        ),
        (
            "tdecimal",
            "CREATE TABLE tdecimal (a INT, c DECIMAL(10,2), KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,1),(2,2),(3,3),(4,4),(4,4),(5,5),(5,5),(5,5),(NULL,NULL),\
             (11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(16,16),(16,16),(16,16),(17,17),(17,17)",
            ["1.00", "2.00", "3.00", "4.00", "5.00", "11.00", "13.00", "14.00", "15.00", "16.00", "17.00"],
        ),
        (
            "tdatetime",
            "CREATE TABLE tdatetime (a INT, c DATETIME, KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,'2000-01-01'),(2,'2000-01-02'),(3,'2000-01-03'),\
             (4,'2000-01-04'),(4,'2000-01-04'),(5,'2000-01-05'),\
             (5,'2000-01-05'),(5,'2000-01-05'),(NULL,NULL),\
             (11,'2000-01-11'),(12,'2000-01-12'),(13,'2000-01-13'),\
             (14,'2000-01-14'),(15,'2000-01-15'),(16,'2000-01-16'),\
             (16,'2000-01-16'),(16,'2000-01-16'),(16,'2000-01-16'),\
             (17,'2000-01-17'),(17,'2000-01-17')",
            [
                "2000-01-01 00:00:00", "2000-01-02 00:00:00", "2000-01-03 00:00:00",
                "2000-01-04 00:00:00", "2000-01-05 00:00:00", "2000-01-11 00:00:00",
                "2000-01-13 00:00:00", "2000-01-14 00:00:00", "2000-01-15 00:00:00",
                "2000-01-16 00:00:00", "2000-01-17 00:00:00",
            ],
        ),
        (
            "tstring",
            "CREATE TABLE tstring (a INT, c VARCHAR(32), KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,'a1'),(2,'a2'),(3,'a3'),(4,'a4'),(4,'a4'),(5,'a5'),(5,'a5'),(5,'a5'),(NULL,NULL),\
             (11,'b11'),(12,'b12'),(13,'b13'),(14,'b14'),(15,'b15'),\
             (16,'b16'),(16,'b16'),(16,'b16'),(16,'b16'),(17,'b17'),(17,'b17')",
            ["a1", "a2", "a3", "a4", "a5", "b11", "b13", "b14", "b15", "b16", "b17"],
        ),
    ];

    for (
        table,
        create,
        values,
        [one, two, three, four, five, eleven, thirteen, fourteen, fifteen, sixteen, seventeen],
    ) in cases
    {
        rows(&mut session, create);
        rows(
            &mut session,
            &format!("INSERT INTO {table} VALUES {values}"),
        );
        rows(
            &mut session,
            &format!("ANALYZE TABLE {table} WITH 2 TOPN, 2 BUCKETS"),
        );

        let meta = displayed(rows(
            &mut session,
            &format!("SHOW STATS_META WHERE table_name = '{table}'"),
        ));
        assert_eq!(
            meta.iter().map(|row| row[5].as_str()).collect::<Vec<_>>(),
            ["20", "9", "11"]
        );

        for is_index in [0, 1] {
            let topn = displayed(rows(
                &mut session,
                &format!("SHOW STATS_TOPN WHERE table_name = '{table}' AND column_name = 'c' AND is_index = {is_index}"),
            )).into_iter().map(|row| row.join(" ")).collect::<Vec<_>>();
            assert_eq!(
                topn,
                [
                    format!("test {table} global c {is_index} {five} 3"),
                    format!("test {table} global c {is_index} {sixteen} 4"),
                    format!("test {table} p0 c {is_index} {four} 2"),
                    format!("test {table} p0 c {is_index} {five} 3"),
                    format!("test {table} p1 c {is_index} {sixteen} 4"),
                    format!("test {table} p1 c {is_index} {seventeen} 2"),
                ]
            );

            let buckets = displayed(rows(
                &mut session,
                &format!("SHOW STATS_BUCKETS WHERE table_name = '{table}' AND column_name = 'c' AND is_index = {is_index}"),
            )).into_iter().map(|row| row.join(" ")).collect::<Vec<_>>();
            assert_eq!(
                buckets,
                [
                    format!("test {table} global c {is_index} 0 5 2 {one} {four} 0"),
                    format!("test {table} global c {is_index} 1 12 2 {eleven} {seventeen} 0"),
                    format!("test {table} p0 c {is_index} 0 2 1 {one} {two} 0"),
                    format!("test {table} p0 c {is_index} 1 3 1 {three} {three} 0"),
                    format!("test {table} p1 c {is_index} 0 3 1 {eleven} {thirteen} 0"),
                    format!("test {table} p1 c {is_index} 1 5 1 {fourteen} {fifteen} 0"),
                ]
            );

            let histograms = displayed(rows(
                &mut session,
                &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}' AND column_name = 'c' AND is_index = {is_index}"),
            ));
            assert_eq!(
                histograms
                    .iter()
                    .map(|row| row[6].as_str())
                    .collect::<Vec<_>>(),
                ["12", "5", "7"]
            );
            assert_eq!(
                histograms
                    .iter()
                    .map(|row| row[7].as_str())
                    .collect::<Vec<_>>(),
                ["1", "1", "0"]
            );
        }
    }
}

/// Pinned `globalstats.TestGlobalIndexStatistics`: all three ANALYZE forms
/// populate a partitioned table's global unique-index statistics and the
/// optimizer uses the global index across every partition.
#[test]
fn global_index_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(110))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");

    let create = |session: &mut _, clustered: bool| {
        rows(session, "DROP TABLE IF EXISTS global_index_stats");
        let key = if clustered {
            "PRIMARY KEY(b,a) CLUSTERED"
        } else {
            "KEY(a)"
        };
        rows(
            session,
            &format!(
                "CREATE TABLE global_index_stats (a INT, b INT, c INT DEFAULT 0, {key}) \
                 PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), \
                 PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30), \
                 PARTITION p3 VALUES LESS THAN (40))"
            ),
        );
        rows(
            session,
            "INSERT INTO global_index_stats(a,b) VALUES \
             (1,1),(2,2),(3,3),(15,15),(25,25),(35,35)",
        );
        rows(
            session,
            "ALTER TABLE global_index_stats ADD UNIQUE INDEX idx(b) GLOBAL",
        );
    };
    let expected = [
        [
            "IndexReader",
            "5.00",
            "root",
            "partition:all",
            "index:IndexRangeScan",
        ],
        [
            "└─IndexRangeScan",
            "5.00",
            "cop[tikv]",
            "table:global_index_stats, index:idx(b)",
            "range:[-inf,16), keep order:true",
        ],
    ];

    create(&mut session, false);
    rows(&mut session, "ANALYZE TABLE global_index_stats");
    // A global index's physical-table ID belongs to each entry, including
    // entries selected by an explicit partition list and a descending scan.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM global_index_stats PARTITION(p0,p1) USE INDEX(idx) ORDER BY b DESC"
        )),
        [["15"], ["3"], ["2"], ["1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b,c FROM global_index_stats USE INDEX(idx) WHERE b < 16 ORDER BY b"
        )),
        [["1", "0"], ["2", "0"], ["3", "0"], ["15", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM global_index_stats USE INDEX(idx) WHERE b < 16 ORDER BY b",
        )),
        [["1"], ["2"], ["3"], ["15"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT b FROM global_index_stats \
             USE INDEX(idx) WHERE b < 16 ORDER BY b",
        )),
        expected
    );

    create(&mut session, true);
    rows(&mut session, "ANALYZE TABLE global_index_stats INDEX idx");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM global_index_stats PARTITION(p0,p1) USE INDEX(idx) ORDER BY b DESC"
        )),
        [["15"], ["3"], ["2"], ["1"]]
    );
    rows(&mut session, "BEGIN");
    rows(
        &mut session,
        "INSERT INTO global_index_stats(a,b) VALUES (4,4),(16,16)",
    );
    let mut unordered = displayed(rows(
        &mut session,
        "SELECT b,c FROM global_index_stats USE INDEX(idx) WHERE b < 17",
    ));
    unordered.sort_unstable();
    assert_eq!(unordered,
        [["1", "0"], ["15", "0"], ["16", "0"], ["2", "0"], ["3", "0"], ["4", "0"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b,c FROM global_index_stats USE INDEX(idx) WHERE b < 17 ORDER BY b DESC"
        )),
        [
            ["16", "0"],
            ["15", "0"],
            ["4", "0"],
            ["3", "0"],
            ["2", "0"],
            ["1", "0"]
        ]
    );
    rows(&mut session, "ROLLBACK");
    let explain = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT b FROM global_index_stats \
         USE INDEX(idx) WHERE b < 16 ORDER BY b",
    ));
    assert_eq!(explain[0][1], "5.00");

    create(&mut session, true);
    rows(&mut session, "ANALYZE TABLE global_index_stats INDEX");
    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT b FROM global_index_stats \
             USE INDEX(idx) WHERE b < 16 ORDER BY b",
        )),
        expected
    );
}

#[test]
fn global_statistics_and_session_bindings_match_go() {
    assert_global_statistics_and_session_bindings_match_go(1, 111);
}

#[test]
fn concurrent_global_statistics_and_session_bindings_match_go() {
    assert_global_statistics_and_session_bindings_match_go(2, 112);
}

/// Pinned `globalstats.TestGlobalStatsAndSQLBinding` and its concurrency-two
/// twin: global statistics choose the index for hash, range, and list
/// partitions, while a matching session binding's `IGNORE INDEX` moves the
/// same statements to table scans until the binding is dropped.
fn assert_global_statistics_and_session_bindings_match_go(
    merge_concurrency: u64,
    connection_id: u64,
) {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(connection_id))
        .expect("session opens");
    rows(
        &mut session,
        &format!("SET GLOBAL tidb_merge_partition_stats_concurrency = {merge_concurrency}"),
    );
    rows(&mut session, "CREATE DATABASE test_global_stats");
    rows(&mut session, "USE test_global_stats");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "SET GLOBAL tidb_enable_auto_analyze = OFF");
    rows(
        &mut session,
        "SET SESSION tidb_enable_non_prepared_plan_cache = 0",
    );
    rows(
        &mut session,
        "CREATE TABLE thash(a INT, b INT, KEY(a)) PARTITION BY HASH(a) PARTITIONS 4",
    );
    rows(
        &mut session,
        "CREATE TABLE trange(a INT, b INT, KEY(a)) PARTITION BY RANGE(a) (\
         PARTITION p0 VALUES LESS THAN (200), PARTITION p1 VALUES LESS THAN (400), \
         PARTITION p2 VALUES LESS THAN (600), PARTITION p3 VALUES LESS THAN (800), \
         PARTITION p4 VALUES LESS THAN (1001))",
    );
    rows(
        &mut session,
        "CREATE TABLE tlist(a INT, b INT, KEY(a)) PARTITION BY LIST(a) (\
         PARTITION p0 VALUES IN (0,1,2,3,4,5,6,7,8,9), \
         PARTITION p1 VALUES IN (10,11,12,13,14,15,16,17,18,19), \
         PARTITION p2 VALUES IN (20,21,22,23,24,25,26,27,28,29), \
         PARTITION p3 VALUES IN (30,31,32,33,34,35,36,37,38,39), \
         PARTITION p4 VALUES IN (40,41,42,43,44,45,46,47,48,49,50))",
    );

    let mut range_values = Vec::with_capacity(1000);
    let mut list_values = Vec::with_capacity(1000);
    for i in 0..1000 {
        if i < 10 {
            range_values.push(format!("({i},{i})"));
            list_values.push("(0,0)".to_owned());
        } else {
            range_values.push(format!(
                "({},{})",
                100 + (i * 37) % 900,
                100 + (i * 53) % 900
            ));
            list_values.push(format!("({},{})", 1 + (i * 17) % 50, 1 + (i * 29) % 50));
        }
    }
    let range_values = range_values.join(",");
    rows(
        &mut session,
        &format!("INSERT INTO thash VALUES {range_values}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO trange VALUES {range_values}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO tlist VALUES {}", list_values.join(",")),
    );
    for table in ["thash", "trange", "tlist"] {
        rows(&mut session, &format!("ANALYZE TABLE {table}"));
    }
    rows(
        &mut session,
        "SET SESSION tidb_opt_table_full_scan_cost_factor = 100",
    );

    let queries = [
        ("thash", "a < 100"),
        ("trange", "a < 100"),
        ("tlist", "a < 1"),
    ];
    for (table, predicate) in queries {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN SELECT * FROM {table} WHERE {predicate}"),
        ));
        assert!(
            plan.iter().any(|row| row[0].contains("IndexRangeScan")),
            "{table} must use its index before binding: {plan:?}"
        );
    }

    for table in ["thash", "trange", "tlist"] {
        rows(
            &mut session,
            &format!(
                "CREATE SESSION BINDING FOR SELECT * FROM {table} WHERE a < 100 \
                 USING SELECT * FROM {table} IGNORE INDEX(a) WHERE a < 100"
            ),
        );
    }
    for (table, predicate) in queries {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN SELECT * FROM {table} WHERE {predicate}"),
        ));
        assert!(
            plan.iter().any(|row| row[0].contains("TableFullScan")),
            "{table} must honor the binding's ignored index: {plan:?}"
        );
    }

    for table in ["thash", "trange", "tlist"] {
        rows(
            &mut session,
            &format!("DROP SESSION BINDING FOR SELECT * FROM {table} WHERE a < 100"),
        );
    }
    for (table, predicate) in queries {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN SELECT * FROM {table} WHERE {predicate}"),
        ));
        assert!(
            plan.iter().any(|row| row[0].contains("IndexRangeScan")),
            "{table} must return to its index after dropping the binding: {plan:?}"
        );
    }
    rows(&mut session, "SET GLOBAL tidb_enable_auto_analyze = ON");
    rows(
        &mut session,
        "SET SESSION tidb_opt_table_full_scan_cost_factor = 1",
    );
}

/// Pinned `globalstats.TestGlobalStatsData`: partition and global histograms
/// retain Go's exact cumulative bucket counts, repeats, bounds, and zeroed
/// merged bucket NDV for both the column and its index.
#[test]
fn partition_global_stats_bucket_data_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(58))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_data (a int, KEY(a)) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    rows(
        &mut session,
        "INSERT INTO global_data VALUES \
         (1),(2),(3),(4),(5),(6),(6),(NULL),\
         (11),(12),(13),(14),(15),(16),(17),(18),(19),(19)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE global_data WITH 0 TOPN, 2 BUCKETS",
    );

    let bucket_rows = |session: &mut _, is_index| {
        displayed(rows(
            session,
            &format!(
                "SHOW STATS_BUCKETS WHERE table_name = 'global_data' AND is_index = {is_index}"
            ),
        ))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
    };
    assert_eq!(
        bucket_rows(&mut session, 0),
        [
            "test global_data global a 0 0 7 2 1 6 0",
            "test global_data global a 0 1 17 2 11 19 0",
            "test global_data p0 a 0 0 4 1 1 4 0",
            "test global_data p0 a 0 1 7 2 5 6 0",
            "test global_data p1 a 0 0 6 1 11 16 0",
            "test global_data p1 a 0 1 10 2 17 19 0",
        ]
    );
    assert_eq!(
        bucket_rows(&mut session, 1),
        [
            "test global_data global a 1 0 7 2 1 6 0",
            "test global_data global a 1 1 17 2 11 19 0",
            "test global_data p0 a 1 0 4 1 1 4 0",
            "test global_data p0 a 1 1 7 2 5 6 0",
            "test global_data p1 a 1 0 6 1 11 16 0",
            "test global_data p1 a 1 1 10 2 17 19 0",
        ]
    );
}

/// Pinned `globalstats.TestGlobalStatsNDV`: the global index NDV is the FM
/// sketch union across every physical partition, including empty partitions,
/// and repeated values do not increase it.
#[test]
fn partition_global_stats_ndv_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(60))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_ndv (a int, KEY(a)) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20),\
         PARTITION p2 VALUES LESS THAN (30),\
         PARTITION p3 VALUES LESS THAN (40))",
    );

    let check_ndv = |session: &mut _, expected: &[&str]| {
        rows(session, "ANALYZE TABLE global_ndv");
        let actual = displayed(rows(
            session,
            "SHOW STATS_HISTOGRAMS WHERE table_name = 'global_ndv'",
        ))
        .into_iter()
        .filter(|row| row[4] == "1")
        .collect::<Vec<_>>();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(
            actual.iter().map(|row| row[6].as_str()).collect::<Vec<_>>(),
            expected
        );
    };

    check_ndv(&mut session, &["0", "0", "0", "0", "0"]);
    rows(&mut session, "INSERT INTO global_ndv VALUES (1),(2),(3)");
    check_ndv(&mut session, &["3", "3", "0", "0", "0"]);
    rows(
        &mut session,
        "INSERT INTO global_ndv VALUES (11),(12),(13),(21),(22),(23)",
    );
    check_ndv(&mut session, &["9", "3", "3", "3", "0"]);
    rows(
        &mut session,
        "INSERT INTO global_ndv VALUES (31),(32),(33),(34)",
    );
    check_ndv(&mut session, &["13", "3", "3", "3", "4"]);
    rows(
        &mut session,
        "INSERT INTO global_ndv VALUES (31),(33),(34),(1),(2),(3)",
    );
    check_ndv(&mut session, &["13", "3", "3", "3", "4"]);
}

/// Pinned `globalstats.TestGlobalStatsIndexNDV`: FM-sketch union uses the
/// index-key encoding consistently for every datum family accepted by the
/// source test, so equal values in different partitions remain one NDV.
#[test]
fn partition_global_index_ndv_matches_go_for_all_source_types() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(61))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );

    let check_ndv = |session: &mut _, table: &str, expected: &[&str]| {
        rows(session, &format!("ANALYZE TABLE {table}"));
        let actual = displayed(rows(
            session,
            &format!("SHOW STATS_HISTOGRAMS WHERE is_index = 1 AND table_name = '{table}'"),
        ));
        assert_eq!(actual.len(), expected.len());
        assert_eq!(
            actual.iter().map(|row| row[6].as_str()).collect::<Vec<_>>(),
            expected
        );
    };

    let run_case = |session: &mut _, table: &str, sql_type: &str, values: [&str; 8]| {
        rows(
            session,
            &format!(
                "CREATE TABLE {table} (a INT, b {sql_type}, KEY(b)) \
                 PARTITION BY RANGE (a) (\
                 PARTITION p0 VALUES LESS THAN (10),\
                 PARTITION p1 VALUES LESS THAN (20))"
            ),
        );
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (1,{}),(1,{}),(1,{})",
                values[0], values[1], values[2]
            ),
        );
        check_ndv(session, table, &["3", "3", "0"]);
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (11,{}),(11,{}),(11,{})",
                values[0], values[1], values[2]
            ),
        );
        check_ndv(session, table, &["3", "3", "3"]);
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (11,{}),(11,{}),(11,{})",
                values[3], values[4], values[5]
            ),
        );
        check_ndv(session, table, &["6", "3", "6"]);
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (1,{}),(1,{}),(1,{}),(1,{}),(1,{})",
                values[3], values[4], values[5], values[6], values[7]
            ),
        );
        check_ndv(session, table, &["8", "8", "6"]);
    };

    run_case(
        &mut session,
        "global_index_ndv_int",
        "INT",
        ["1", "2", "3", "4", "5", "6", "7", "8"],
    );
    run_case(
        &mut session,
        "global_index_ndv_double",
        "DOUBLE",
        ["1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8"],
    );
    run_case(
        &mut session,
        "global_index_ndv_decimal",
        "DECIMAL(30,15)",
        ["1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8"],
    );
    run_case(
        &mut session,
        "global_index_ndv_string",
        "VARCHAR(30)",
        [
            "'111'", "'222'", "'333'", "'444'", "'555'", "'666'", "'777'", "'888'",
        ],
    );
    run_case(
        &mut session,
        "global_index_ndv_datetime",
        "DATETIME",
        [
            "'2001-01-01'",
            "'2002-01-01'",
            "'2003-01-01'",
            "'2004-01-01'",
            "'2005-01-01'",
            "'2006-01-01'",
            "'2007-01-01'",
            "'2008-01-01'",
        ],
    );
}

/// Pinned `globalstats.TestGlobalStatsVersion`: after global stats exist,
/// analyzing a newly added partition refreshes the global row while retaining
/// modifications in another partition until that partition is analyzed.
#[test]
fn partition_scoped_analyze_refreshes_global_count_and_modify_count() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(62))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_stats_version (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20))",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO global_stats_version VALUES (1),(5),(NULL),(11),(15)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(&mut session, "ANALYZE TABLE global_stats_version");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_META WHERE table_name = 'global_stats_version'",
        ))
        .len(),
        3
    );

    let global_meta = |session: &mut _| {
        displayed(rows(
            session,
            "SHOW STATS_META WHERE table_name = 'global_stats_version' \
             AND partition_name = 'global'",
        ))
        .into_iter()
        .next()
        .expect("global stats meta row")
    };

    rows(
        &mut session,
        "ALTER TABLE global_stats_version ADD PARTITION \
         (PARTITION p2 VALUES LESS THAN (30))",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO global_stats_version VALUES (13),(14),(22),(23)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(
        &mut session,
        "ANALYZE TABLE global_stats_version PARTITION p2",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let meta = global_meta(&mut session);
    assert_eq!((meta[4].as_str(), meta[5].as_str()), ("2", "9"));

    rows(
        &mut session,
        "ANALYZE TABLE global_stats_version PARTITION p1",
    );
    let meta = global_meta(&mut session);
    assert_eq!((meta[4].as_str(), meta[5].as_str()), ("0", "9"));

    rows(
        &mut session,
        "ALTER TABLE global_stats_version DROP PARTITION p2",
    );
    // Go DDL tests handle the subscriber event before inspecting stats;
    // SHOW reads the cache, so also perform the equivalent of h.Update.
    drain_stats_ddl_events(&stack.factory, &mut session);
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < deadline,
            "statistics cache did not refresh after dropping p2"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(global_meta(&mut session)[5], "7");
    rows(&mut session, "ANALYZE TABLE global_stats_version");
    let meta = global_meta(&mut session);
    assert_eq!(meta[5], "7");
}

#[test]
fn flush_stats_delta_missing_targets_match_go_warnings() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(63))
        .expect("session opens");
    rows(&mut session, "USE test");

    rows(
        &mut session,
        "FLUSH STATS_DELTA missing_database.*, test.missing_table",
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        vec![
            vec![
                "Warning".to_owned(),
                "1049".to_owned(),
                "Unknown database 'missing_database'".to_owned(),
            ],
            vec![
                "Warning".to_owned(),
                "1146".to_owned(),
                "Table 'test.missing_table' doesn't exist".to_owned(),
            ],
        ]
    );
}

/// Pinned `globalstats.TestDDLPartition4GlobalStats`: truncating partitions
/// replaces their physical IDs, subtracts their persisted counts from the
/// logical global row, and installs replacement partition metadata.
#[test]
fn truncate_partitions_refreshes_global_stats_meta_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(64))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_stats_truncate (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20),\
         PARTITION p2 VALUES LESS THAN (30),\
         PARTITION p3 VALUES LESS THAN (40),\
         PARTITION p4 VALUES LESS THAN (50),\
         PARTITION p5 VALUES LESS THAN (60))",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO global_stats_truncate VALUES \
         (1),(2),(3),(4),(5),(11),(21),(31),(41),(51),\
         (12),(22),(32),(42),(52)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(&mut session, "ANALYZE TABLE global_stats_truncate");
    let global_count = |session: &mut _| {
        displayed(rows(
            session,
            "SHOW STATS_META WHERE table_name = 'global_stats_truncate' \
             AND partition_name = 'global'",
        ))[0][5]
            .clone()
    };
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_META WHERE table_name = 'global_stats_truncate'",
        ))
        .len(),
        7
    );
    assert_eq!(global_count(&mut session), "15");

    rows(
        &mut session,
        "ALTER TABLE global_stats_truncate TRUNCATE PARTITION p2, p4",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    drain_stats_ddl_events(&stack.factory, &mut session);
    // Go calls h.Update after handling the DDL event before reading SHOW's
    // cached statistics. Trigger and await the corresponding cache refresh.
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < deadline,
            "statistics cache did not refresh"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(global_count(&mut session), "11");

    rows(&mut session, "ANALYZE TABLE global_stats_truncate");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_META WHERE table_name = 'global_stats_truncate'",
        ))
        .len(),
        7
    );
    assert_eq!(global_count(&mut session), "11");
}

/// Pinned `globalstats.TestGlobalStats`: dynamic partition pruning consumes
/// logical-table statistics, static pruning consumes each physical
/// partition's statistics under `PartitionUnion`, and switching to dynamic
/// before a global row exists uses pseudo statistics until the next analyze.
#[test]
fn global_stats_drive_partition_plans_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(65))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    rows(
        &mut session,
        "CREATE TABLE global_stats_plan (a INT, KEY(a)) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20),\
         PARTITION p2 VALUES LESS THAN (30))",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_plan VALUES (1),(5),(NULL),(11),(15),(21),(25)",
    );
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "ANALYZE TABLE global_stats_plan");

    let dynamic = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 5",
    );
    assert!(
        dynamic.contains("IndexRangeScan") && dynamic.contains("4.00"),
        "{dynamic}"
    );
    assert!(dynamic.contains("partition:all"), "{dynamic}");
    assert!(
        !dynamic.contains("PartitionUnion") && !dynamic.contains("stats:pseudo"),
        "{dynamic}"
    );

    let explicit = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM global_stats_plan PARTITION(p1) WHERE a > 15",
    );
    assert!(
        explicit.contains("IndexRangeScan") && explicit.contains("2.00"),
        "{explicit}"
    );
    assert!(explicit.contains("partition:p1"), "{explicit}");
    assert!(!explicit.contains("stats:pseudo"), "{explicit}");

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    let static_plan = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 5",
    );
    assert!(
        static_plan.contains("PartitionUnion") && static_plan.contains("5.00"),
        "{static_plan}"
    );
    for partition in ["p0", "p1", "p2"] {
        assert!(
            static_plan.contains(&format!("partition:{partition}")),
            "{static_plan}"
        );
    }

    rows(&mut session, "DROP TABLE global_stats_plan");
    rows(
        &mut session,
        "CREATE TABLE global_stats_plan (a INT, b INT, KEY(a)) \
         PARTITION BY HASH(a) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_plan VALUES (1,1),(3,3),(4,4),(2,2),(5,5)",
    );
    rows(&mut session, "ANALYZE TABLE global_stats_plan");
    let static_meta = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'global_stats_plan'",
    ));
    assert_eq!(static_meta.len(), 2);
    assert_eq!(
        static_meta
            .iter()
            .map(|row| row[5].as_str())
            .collect::<Vec<_>>(),
        ["2", "3"]
    );

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    let pseudo = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 3",
    );
    assert!(
        pseudo.contains("IndexRangeScan") && pseudo.contains("1.67"),
        "{pseudo}"
    );
    assert!(
        pseudo.contains("partition:all") && pseudo.contains("stats:pseudo"),
        "{pseudo}"
    );
    assert!(!pseudo.contains("PartitionUnion"), "{pseudo}");

    rows(&mut session, "ANALYZE TABLE global_stats_plan");
    let dynamic_meta = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'global_stats_plan'",
    ));
    assert_eq!(dynamic_meta.len(), 3);
    assert_eq!(
        dynamic_meta
            .iter()
            .map(|row| row[5].as_str())
            .collect::<Vec<_>>(),
        ["5", "2", "3"]
    );
    let analyzed = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 3",
    );
    assert!(
        analyzed.contains("IndexRangeScan") && analyzed.contains("2.00"),
        "{analyzed}"
    );
    assert!(analyzed.contains("partition:all"), "{analyzed}");
    assert!(
        !analyzed.contains("PartitionUnion") && !analyzed.contains("stats:pseudo"),
        "{analyzed}"
    );

    rows(&mut session, "DROP TABLE global_stats_plan");
    rows(
        &mut session,
        "CREATE TABLE global_stats_plan (a INT, b INT, c INT) \
         PARTITION BY HASH(a) PARTITIONS 2",
    );
    rows(
        &mut session,
        "CREATE INDEX idx_ab ON global_stats_plan(a, b)",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_plan VALUES \
         (1,1,1),(5,5,5),(11,11,11),(15,15,15),(21,21,21),(25,25,25)",
    );
    rows(&mut session, "ANALYZE TABLE global_stats_plan");

    let index_scan = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT b FROM global_stats_plan WHERE a > 5 AND b > 10",
    );
    assert!(
        index_scan.contains("IndexRangeScan") && index_scan.contains("2.67"),
        "{index_scan}"
    );
    assert!(
        index_scan.contains("partition:all") && !index_scan.contains("stats:pseudo"),
        "{index_scan}"
    );

    let index_lookup = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM global_stats_plan USE INDEX(idx_ab) WHERE a > 1",
    );
    assert!(
        index_lookup.contains("IndexLookUp") && index_lookup.contains("5.00"),
        "{index_lookup}"
    );
    assert!(
        index_lookup.contains("partition:all") && !index_lookup.contains("stats:pseudo"),
        "{index_lookup}"
    );

    let table_scan = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM global_stats_plan",
    );
    assert!(
        table_scan.contains("TableFullScan") && table_scan.contains("6.00"),
        "{table_scan}"
    );
    assert!(
        table_scan.contains("partition:all") && !table_scan.contains("stats:pseudo"),
        "{table_scan}"
    );
}

/// A write takes the same access paths a `SELECT` does, which is
/// `crate::explain`'s divergence 8 as it now stands.
///
/// That paragraph claimed the opposite for a while -- "none is offered to a
/// write" -- after `write_index_range_path` landed and nothing checked the
/// prose against the code. A doc that describes a gap which no longer exists
/// sends the next reader to build what is already there, so the claim is
/// pinned here rather than trusted.
#[test]
fn a_write_reaches_the_index_path_like_a_select() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(59))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.wi (id int primary key, a int, b int, KEY ka(a), UNIQUE KEY ub(b))",
    );
    rows(
        &mut session,
        "INSERT INTO test.wi VALUES (1,10,100),(2,10,200),(3,20,300)",
    );

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // A non-unique secondary index is chosen for a write, as it is for a read.
    for sql in [
        "EXPLAIN UPDATE test.wi SET b = b + 1 WHERE a = 10",
        "EXPLAIN DELETE FROM test.wi WHERE a = 10",
    ] {
        let plan = plan_of(&mut session, sql);
        assert!(
            plan.contains("IndexRangeScan") && plan.contains("index:ka(a)"),
            "`{sql}` did not reach the index path:\n{plan}"
        );
        // Go's exact integer equality is fully enforced by the index range.
        assert!(plan.contains("range:[10,10]"), "{plan}");
        assert!(!plan.contains("Selection"), "{plan}");
    }

    // A predicate on a non-access column must still filter the looked-up rows.
    let residual = plan_of(
        &mut session,
        "EXPLAIN UPDATE test.wi SET b = b + 1 WHERE a = 10 AND b > 100",
    );
    assert!(
        residual.contains("index:ka(a)")
            && residual.contains("Selection")
            && residual.contains("gt(test.wi.b, 100)"),
        "{residual}"
    );

    // A WHERE that pins a whole UNIQUE index still takes the point plan.
    let plan = plan_of(
        &mut session,
        "EXPLAIN UPDATE test.wi SET a = 1 WHERE b = 100",
    );
    assert!(plan.contains("Point_Get"), "{plan}");

    // And the rows a write touches are the rows the predicate names,
    // whichever path carried it there.
    rows(&mut session, "UPDATE test.wi SET b = b + 1 WHERE a = 10");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, b FROM test.wi ORDER BY id")),
        [["1", "101"], ["2", "201"], ["3", "300"]]
    );
    rows(&mut session, "DELETE FROM test.wi WHERE a = 10");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.wi ORDER BY id")),
        [["3"]]
    );
}

/// Go `handleUnsignedCol`: a NEGATIVE bound on an unsigned column is either
/// rewritten to `>= 0` or makes the range invalid, and an invalid range folds
/// to a `TableDual` (`crate::explain`'s divergence 9).
///
/// The distinction that matters is negative VALUE, not negative-looking
/// predicate: `a < 0` compares against zero, which Go treats as non-negative
/// and rewrites nothing, so it keeps a real `IndexRangeScan`. Mis-reading
/// that cost an incorrect doc edit once; it is pinned here so the next reader
/// gets the boundary from a test rather than from prose.
#[test]
fn a_negative_bound_on_an_unsigned_column_follows_gos_rewrite() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(61))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.ud (id int primary key, a int unsigned, KEY ka(a))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ud VALUES (1,0),(2,5),(3,4294967295)",
    );

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // A negative value with LT/LE/EQ makes the range invalid -> TableDual.
    for predicate in ["a < -1", "a <= -1", "a = -1"] {
        let plan = plan_of(
            &mut session,
            &format!("EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE {predicate}"),
        );
        assert!(
            plan.contains("TableDual"),
            "`{predicate}` should fold to a dual:\n{plan}"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.ud WHERE {predicate}")
            )),
            [["0"]],
        );
    }

    // A negative value with GT/GE/NE is rewritten to `>= 0`, so every row
    // qualifies rather than none.
    for predicate in ["a > -1", "a >= -5", "a <> -1"] {
        let plan = plan_of(
            &mut session,
            &format!("EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE {predicate}"),
        );
        assert!(
            plan.contains("range:[0"),
            "`{predicate}` should start at 0:\n{plan}"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.ud WHERE {predicate}")
            )),
            [["3"]],
        );
    }

    // `a < 0` is NOT a negative value: Go rewrites nothing, so the range
    // survives as a real scan that happens to find no rows.
    let plan = plan_of(
        &mut session,
        "EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE a < 0",
    );
    assert!(plan.contains("IndexRangeScan"), "{plan}");
    assert!(!plan.contains("TableDual"), "{plan}");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.ud WHERE a < 0"
        )),
        [["0"]],
    );
}

/// `CREATE VIEW` over the real embedded store creates the view, and it reads
/// back.
///
/// This is the path `--store unistore --cluster-session` actually serves, and
/// it had NO coverage: the mock-seam modules in this directory serve
/// `cop_scans: None`, and the pipeline session's own `CREATE VIEW` wire test
/// exercises a different session. That blind spot is how seven commits landed
/// on a working feature and broke it in silence.
///
/// Bisected, one build and one live server per point: `9b893f4abd` still
/// creates the view; `b1f979cc76` ("rust: complete unistore transaction batch
/// get") answers
///
/// ```text
/// ERROR 1105 (HY000): table bytes failed to decode
/// ```
///
/// and leaves no view behind. The real cause is
/// `Storage("Backend(\"query deadline exceeded\")")`, and the shape of the
/// failure is the clue: this test sits for ~20s before reporting, so the
/// coprocessor request never COMPLETES and the wait runs out. It is a request
/// that goes unanswered, not a deadline that was mis-set.
#[test]
fn a_view_over_the_coprocessor_is_created_and_reads_back() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(9))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.vsrc (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.vsrc VALUES (1, 10), (2, 20)",
    );
    rows(
        &mut session,
        "CREATE VIEW test.vview AS SELECT id FROM test.vsrc WHERE id > 1",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.vview")),
        [["2"]],
        "the view reads back its defining query"
    );
}

/// A HASH-partitioned table created HERE writes its rows to the right
/// physical tables and reads every one of them back.
///
/// This is the end-to-end claim the metadata round trip does NOT make. The
/// loader proves the stored bounds fold back; this proves a row written
/// under one partition's physical table id is found again by a read that has
/// to visit all of them. The four ids straddle both partitions under
/// `HASH(id) PARTITIONS 2`, so a read that reached only one physical table
/// would come back with half the rows rather than with an error.
#[test]
fn a_hash_partitioned_table_writes_and_reads_every_partition() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(63))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.hp (id int primary key, v int) PARTITION BY HASH (id) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.hp VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, v FROM test.hp ORDER BY id")),
        [["1", "10"], ["2", "20"], ["3", "30"], ["4", "40"]],
        "every partition's rows come back IN ORDER: the per-partition scans \
         are each ordered, and merging them is what makes the whole answer so"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.hp ORDER BY id DESC"
        )),
        [["4"], ["3"], ["2"], ["1"]],
        "the descending merge walks both partitions backwards together"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.hp")),
        [["4"]],
        "the aggregate reaches every physical table too"
    );
}

/// A RANGE-partitioned table prunes on read without losing rows.
///
/// The unpruned read is the control: if pruning dropped a partition it
/// should not have, only the narrowed query would be wrong, and only a
/// comparison against the full scan shows it.
///
/// This does NOT test cross-partition ordering, and cannot: a clustered
/// primary key must cover the partition columns, so a RANGE table keyed on
/// its own primary key stores the partitions in handle order and
/// concatenating them is already sorted. `HASH` is what separates the two,
/// because hashing scatters the handle across partitions.
#[test]
fn a_range_partitioned_table_prunes_without_losing_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(64))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.rp (id int primary key, v int) PARTITION BY RANGE (id) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    );
    rows(
        &mut session,
        "INSERT INTO test.rp VALUES (5, 50), (15, 150), (25, 250)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.rp ORDER BY id")),
        [["5"], ["15"], ["25"]],
        "the unpruned read sees both partitions"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.rp WHERE id < 10")),
        [["5"]],
        "the pruned read keeps the row that is actually below the bound"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.rp WHERE id >= 10 ORDER BY id"
        )),
        [["15"], ["25"]],
        "and the other side keeps the rows above it"
    );
}

/// A KEY-partitioned table answers an ordered read in order.
///
/// KEY hashes the partition columns exactly as HASH does, so the handles
/// scatter across partitions and the merge is what puts them back together.
/// This is the sibling of
/// [`a_hash_partitioned_table_writes_and_reads_every_partition`] over the
/// other method that stores rows out of handle order.
#[test]
fn a_key_partitioned_table_answers_an_ordered_read_in_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(65))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.kp (id int primary key, v int) PARTITION BY KEY (id) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.kp VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.kp ORDER BY id")),
        [["1"], ["2"], ["3"], ["4"], ["5"]],
        "the merge orders across every KEY partition"
    );
}

#[test]
fn completed_empty_transaction_does_not_lose_the_next_autocommit_insert() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(66)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.txn_lifetime (id int primary key, v int)",
    );
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (1, 10)");
    session.control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT v FROM test.txn_lifetime WHERE id = 1");
    session.control_transaction("COMMIT").unwrap();
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (2, 20)");

    rows(
        &mut session,
        "UPDATE test.txn_lifetime SET v = 0 WHERE id = 99",
    );
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (3, 30)");
    for (end, id) in [("COMMIT", 4), ("ROLLBACK", 5)] {
        session.control_transaction("BEGIN PESSIMISTIC").unwrap();
        rows(
            &mut session,
            "SELECT v FROM test.txn_lifetime WHERE id = 1 FOR UPDATE",
        );
        session.control_transaction(end).unwrap();
        rows(
            &mut session,
            &format!("INSERT INTO test.txn_lifetime VALUES ({id}, {})", id * 10),
        );
    }
    session.execute_write("SET autocommit = 0").unwrap();
    rows(&mut session, "SELECT v FROM test.txn_lifetime WHERE id = 1");
    session.execute_write("SET autocommit = 1").unwrap();
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (6, 60)");

    // Another connection proves the INSERT was persisted, not merely staged.
    let mut reader = stack.factory.open_session(session_context(67)).unwrap();
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id, v FROM test.txn_lifetime ORDER BY id"
        )),
        [
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
            ["4", "40"],
            ["5", "50"],
            ["6", "60"]
        ]
    );
}

#[test]
fn failed_autocommit_update_does_not_lose_the_next_insert() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(68)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.failed_txn_lifetime (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.failed_txn_lifetime VALUES (1, 10), (2, 20)",
    );
    let error = match session.execute("UPDATE test.failed_txn_lifetime SET id = 2 WHERE id = 1") {
        Err(error) => error,
        Ok(_) => panic!("the conflicting UPDATE must fail"),
    };
    assert_eq!(error.code, 1062);
    rows(
        &mut session,
        "INSERT INTO test.failed_txn_lifetime VALUES (3, 30)",
    );

    let mut reader = stack.factory.open_session(session_context(69)).unwrap();
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id, v FROM test.failed_txn_lifetime ORDER BY id"
        )),
        [["1", "10"], ["2", "20"], ["3", "30"]]
    );
}

#[test]
fn failed_explicit_statement_retains_native_transaction_and_savepoints() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(670)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.statement_lifetime (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.statement_lifetime VALUES (1, 10), (2, 20)",
    );
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    rows(
        &mut session,
        "UPDATE test.statement_lifetime SET v = 11 WHERE id = 1",
    );
    session.control_transaction("SAVEPOINT keep_first").unwrap();
    rows(
        &mut session,
        "INSERT INTO test.statement_lifetime VALUES (3, 30)",
    );
    let error = match session.execute("UPDATE test.statement_lifetime SET id = 2 WHERE id = 1") {
        Err(error) => error,
        Ok(_) => panic!("the conflicting UPDATE must fail"),
    };
    assert_eq!(error.code, 1062);
    assert!(session.buffer.native_owner().is_some());
    session
        .control_transaction("ROLLBACK TO keep_first")
        .unwrap();
    session.control_transaction("COMMIT").unwrap();
    rows(
        &mut session,
        "INSERT INTO test.statement_lifetime VALUES (4, 40)",
    );

    let mut reader = stack.factory.open_session(session_context(671)).unwrap();
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id, v FROM test.statement_lifetime ORDER BY id"
        )),
        [["1", "11"], ["2", "20"], ["4", "40"]]
    );
}

/// Two sessions racing their `CREATE TABLE`s both succeed -- sysbench's
/// parallel `prepare` (`--threads=4 --tables=2`) is exactly this shape, and
/// it was the workload that found the gap: `sbtest2` never existed and every
/// later statement on it failed.
///
/// Every catalog change writes `SchemaVersionKey`, so concurrent DDL is a
/// GUARANTEED optimistic write conflict. Go runs each DDL meta write under
/// `kv.RunInNewTxn(retryable=true)` (`pkg/ddl/ddl.go`), which rolls the
/// loser back and re-runs it from a fresh snapshot -- re-read, re-plan,
/// re-commit -- so no client ever sees the conflict. Before
/// `commit_cluster_ddl` carried that loop, the loser here surfaced error
/// 1105 ("... refuses to interleave: ... WriteConflict").
#[test]
fn concurrent_creates_both_succeed_like_gos_ddl_queue() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        for worker in 0u64..2 {
            let barrier = &barrier;
            scope.spawn(move || {
                let mut session = factory
                    .open_session(session_context(70 + worker))
                    .expect("session opens");
                barrier.wait();
                for table in 0..4 {
                    rows(
                        &mut session,
                        &format!(
                            "CREATE TABLE test.race_{worker}_{table} (id int primary key, v int)"
                        ),
                    );
                }
            });
        }
    });
    // Every one of the eight racing tables exists and serves reads and
    // writes: the retry made both sessions' schedules land, in some order.
    let mut session = factory
        .open_session(session_context(79))
        .expect("session opens");
    for worker in 0..2 {
        for table in 0..4 {
            rows(
                &mut session,
                &format!("INSERT INTO test.race_{worker}_{table} VALUES (1, 10)"),
            );
            assert_eq!(
                displayed(rows(
                    &mut session,
                    &format!("SELECT v FROM test.race_{worker}_{table} WHERE id = 1"),
                )),
                [["10"]],
                "table race_{worker}_{table} must exist and answer"
            );
        }
    }
}

/// Two explicit transactions racing an `UPDATE` of the same row both commit,
/// and the row carries BOTH increments -- Go's default `tidb_txn_mode =
/// 'pessimistic'` semantics, sysbench `oltp_read_write`'s exact shape.
///
/// The pessimistic wiring is what each half proves. The loser's `UPDATE`
/// blocks on the winner's row lock instead of proceeding on a stale
/// snapshot; when the lock releases, fair locking grants it WITH a conflict
/// and the statement is re-executed reading at the advanced `for_update_ts`
/// (Go `handlePessimisticDML` -> `UpdateForUpdateTS`), so its `v + 1`
/// computes from the winner's committed value. Before the wiring, `BEGIN`
/// held an optimistic transaction end to end: neither `UPDATE` blocked, both
/// computed from the same snapshot, and whichever `COMMIT` ran second failed
/// with 9007 -- this test then fails on the error, and would fail on
/// `v == 2` even if both were let through.
#[test]
fn racing_pessimistic_updates_both_commit_with_serial_effect() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.race_rw (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.race_rw VALUES (1, 0)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    rows(&mut first, "UPDATE test.race_rw SET v = v + 1 WHERE id = 1");

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(81))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's pessimistic row lock until
            // that transaction commits.
            rows(
                &mut second,
                "UPDATE test.race_rw SET v = v + 1 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        // Give the contender time to reach the lock wait, so the interesting
        // interleaving -- blocked UPDATE, then the winner's COMMIT -- is the
        // one exercised. The assertions hold under any interleaving.
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the lock out");

    assert_eq!(
        displayed(rows(&mut first, "SELECT v FROM test.race_rw WHERE id = 1")),
        [["2"]],
        "both increments landed: the loser re-read the winner's commit"
    );
}

/// A retried pessimistic statement re-reads through the COPROCESSOR at its
/// advanced `for_update_ts`, not at `start_ts`.
///
/// The retry exists so the statement recomputes from the winner's committed
/// row: Go re-runs it after `UpdateForUpdateTS`, and every read of the
/// re-executed statement -- point get and range scan alike -- uses the new
/// timestamp. This tier moved only the point path: `SessionSnapshot::start_ts`
/// answered the transaction's `start_ts` while ignoring the statement's
/// `read_ts`, and that value is what stamps `request.snapshot_ts` for a
/// pushdown (`tidb-executor/src/cluster_storage.rs`). A range-shaped DML
/// therefore recomputed from the SAME stale row it just lost the lock race
/// on, and its `v + 1` overwrote the winner: a silent lost update, and a
/// mixed-timestamp read inside one statement.
///
/// The predicate is on a NON-key column so the plan is a coprocessor scan
/// rather than a handle range; the sibling test above covers the point-get
/// shape, which already reads at the advanced timestamp.
#[test]
fn a_retried_range_dml_recomputes_at_the_advanced_for_update_ts() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(86))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.retry_range (k int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.retry_range VALUES (7, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    // Take the row lock so the contender below must wait and then retry.
    rows(
        &mut first,
        "UPDATE test.retry_range SET v = v + 1 WHERE v > 0",
    );

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(87))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's lock, then retries at an
            // advanced `for_update_ts`.
            rows(
                &mut second,
                "UPDATE test.retry_range SET v = v + 1 WHERE v > 0",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the lock out");

    assert_eq!(
        displayed(rows(
            &mut first,
            "SELECT v FROM test.retry_range WHERE k = 7"
        )),
        [["12"]],
        "both increments landed: the retry re-read the winner's commit \
         through the coprocessor, not through the stale start_ts snapshot"
    );
}

/// A NON-locking read must not be answered from the pessimistic lock cache.
///
/// Go gates that cache on `e.lock`: `PointGetExecutor.get`
/// (`pkg/executor/point_get.go:671-684`) consults
/// `TxnCtx.GetKeyInPessimisticLockCache` only inside `if e.lock`, so a plain
/// `SELECT` falls through to the snapshot and reads at the transaction's own
/// `start_ts`. The cached row is the one the LOCK saw, at a `for_update_ts`
/// at or after `start_ts`; answering a plain read from it publishes a newer
/// row into a repeatable read.
///
/// The window is exactly the one pessimistic locking is built to tolerate: a
/// writer that commits after `BEGIN`, which the lock survives by advancing
/// its own `for_update_ts` rather than failing.
#[test]
fn a_plain_read_is_not_answered_from_the_pessimistic_lock_cache() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut reader = factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(
        &mut reader,
        "CREATE TABLE test.lock_cache (id int primary key, v int)",
    );
    rows(&mut reader, "INSERT INTO test.lock_cache VALUES (1, 10)");

    assert_eq!(
        reader.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.lock_cache WHERE id = 1"
        )),
        [["10"]],
        "the transaction's snapshot is the row as of BEGIN"
    );

    let mut writer = factory
        .open_session(session_context(85))
        .expect("session opens");
    rows(
        &mut writer,
        "UPDATE test.lock_cache SET v = 99 WHERE id = 1",
    );

    // The LOCKING read may see the newer row -- it takes its own
    // `for_update_ts`, which is Go's behaviour too. This is what fills the
    // lock cache.
    let _ = reader.execute("SELECT v FROM test.lock_cache WHERE id = 1 FOR UPDATE");

    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.lock_cache WHERE id = 1"
        )),
        [["10"]],
        "the plain read that follows still reads at start_ts: the lock cache \
         belongs to locking reads only (`point_get.go:677`)"
    );
    reader.control_transaction("ROLLBACK").expect("rollback");
}

/// `BEGIN OPTIMISTIC` keeps Go's optimistic contract: neither `UPDATE`
/// blocks or locks, and the transaction that commits second fails at
/// `COMMIT` with the 9007 write conflict -- which is also the receipt that
/// [`racing_pessimistic_updates_both_commit_with_serial_effect`] tests the
/// WIRING and not some property both modes share: this test IS the
/// pre-wiring behavior, kept reachable by the keyword exactly as Go keeps
/// it.
#[test]
fn racing_optimistic_updates_still_conflict_at_commit() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.race_opt (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.race_opt VALUES (1, 0)");

    first
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut first,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );

    // The contender's whole transaction runs and commits while the first is
    // still open: optimistically nothing blocks it.
    let mut second = factory
        .open_session(session_context(85))
        .expect("session opens");
    second
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut second,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );
    second.control_transaction("COMMIT").expect("commit");

    // The first transaction's prewrite now finds the newer commit: 9007.
    let refused = first
        .control_transaction("COMMIT")
        .expect_err("an optimistic loser reports the conflict at COMMIT");
    assert_eq!(
        refused.code, 9007,
        "the loser's error keeps Go's write-conflict identity: {}",
        refused.message
    );
    assert_eq!(
        displayed(rows(
            &mut second,
            "SELECT v FROM test.race_opt WHERE id = 1"
        )),
        [["1"]],
        "only the winner's increment landed"
    );
}

/// `BEGIN` inside an open transaction implicitly COMMITS it -- Go's
/// documented `BEGIN` semantics -- and the staged writes are PUBLISHED, not
/// discarded. sysbench relies on this shape: an ignorable statement error
/// (1213) makes it abandon the transaction and simply issue the next
/// `BEGIN`. Before the fix the wrapper discarded the abandoned buffer, and
/// -- when a statistics republish had refreshed the catalog first -- the
/// implicit commit failed with a phantom 9007 "Write conflict" at `BEGIN`.
#[test]
fn begin_inside_a_transaction_implicitly_commits_it() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory
        .open_session(session_context(88))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.implicit_commit (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.implicit_commit VALUES (1, 0)",
    );

    session.control_transaction("BEGIN").expect("begin");
    rows(
        &mut session,
        "UPDATE test.implicit_commit SET v = 7 WHERE id = 1",
    );
    // No COMMIT: the next BEGIN carries it implicitly.
    session
        .control_transaction("BEGIN")
        .expect("BEGIN with an open transaction implicitly commits, never conflicts");
    session.control_transaction("COMMIT").expect("commit");

    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM test.implicit_commit WHERE id = 1"
        )),
        [["7"]],
        "the abandoned transaction's write was committed, not discarded"
    );
}

#[test]
fn prepared_select_replans_after_another_session_changes_columns() {
    use crate::resultset_source::ResultSetSource;
    use tidb_protocol::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let mut reader = stack
        .factory
        .open_session(session_context(90))
        .expect("reader");
    let mut ddl = stack
        .factory
        .open_session(session_context(91))
        .expect("DDL session");
    // Table count must not select a different SQL engine or stop schema refresh.
    for name in ["schema_anchor_a", "schema_anchor_b", "schema_anchor_c"] {
        rows(
            &mut ddl,
            &format!("CREATE TABLE test.{name} (id INT PRIMARY KEY)"),
        );
    }
    // Equality exercises the retained point reader; range exercises a physical SELECT.
    for (table, comparison) in [
        ("prepared_point_schema", "="),
        ("prepared_range_schema", ">="),
    ] {
        rows(
            &mut ddl,
            &format!("CREATE TABLE test.{table} (id INT PRIMARY KEY, v INT)"),
        );
        rows(
            &mut ddl,
            &format!("INSERT INTO test.{table} VALUES (1, 10)"),
        );
        let statement = reader
            .prepare_general(&format!(
                "SELECT * FROM test.{table} WHERE id {comparison} ?"
            ))
            .expect("prepare");
        let select = |session: &mut ClusterServerSession| {
            let crate::sql_node::GeneralExecuteOutcome::Rows(mut result) = session
                .execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
                .expect("execute")
            else {
                panic!("SELECT returns rows")
            };
            let source = result.source();
            let mut selected = Vec::new();
            loop {
                let batch = source.next_batch(8).expect("read rows");
                if batch.is_empty() {
                    break;
                }
                selected.extend(batch);
            }
            source.finish().expect("finish");
            source.close().expect("close");
            displayed(selected)
        };
        assert_eq!(select(&mut reader), [["1", "10"]]);
        assert_eq!(select(&mut reader), [["1", "10"]]);
        rows(
            &mut ddl,
            &format!("ALTER TABLE test.{table} ADD COLUMN extra INT DEFAULT 7"),
        );
        assert_eq!(select(&mut reader), [["1", "10", "7"]]);
        rows(
            &mut ddl,
            &format!("ALTER TABLE test.{table} DROP COLUMN extra"),
        );
        assert_eq!(select(&mut reader), [["1", "10"]]);
    }
}

#[test]
fn prepared_update_replans_after_another_session_adds_a_column() {
    use tidb_protocol::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(90))
        .expect("writer");
    let mut ddl = stack
        .factory
        .open_session(session_context(91))
        .expect("DDL session");
    rows(
        &mut writer,
        "CREATE TABLE test.prepared_schema (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.prepared_schema VALUES (1, 10)",
    );
    let statement = writer
        .prepare_general("UPDATE test.prepared_schema SET v = v + 1 WHERE id = ?")
        .expect("prepare");
    writer
        .execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("populate prepared cache");
    // Keep the prepared handle alive while a separate connection publishes DDL.
    rows(
        &mut ddl,
        "ALTER TABLE test.prepared_schema ADD COLUMN extra INT DEFAULT 7",
    );
    writer
        .execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute against the new schema");
    assert_eq!(
        displayed(rows(&mut writer, "SELECT * FROM test.prepared_schema")),
        [["1", "12", "7"]]
    );
    rows(
        &mut ddl,
        "ALTER TABLE test.prepared_schema DROP COLUMN extra",
    );
    writer
        .execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute after dropping the column");
    assert_eq!(
        displayed(rows(&mut writer, "SELECT * FROM test.prepared_schema")),
        [["1", "13"]]
    );
}

/// Go's pessimistic point write folds its row read INTO its lock:
/// `PointGetExecutor.getAndLock` (`pkg/executor/point_get.go:549`) locks with
/// `InitReturnValues(1)` (line 614) and reads the row from the answer, cached
/// in `TxnCtx.SetPessimisticLockCache`. This session's EXECUTE path does the
/// fold: the classified row is locked WITH its value BEFORE the snapshot is
/// bound, so the statement's one read is answered from the lock response. The
/// assertions here are behavioral, not counters: the pre-lock must REALLY
/// hold the row (a contender blocks mid-transaction), the second update of
/// the same row must compute from its own staged write, and COMMIT persists.
#[test]
fn a_prepared_point_update_locks_its_row_before_reading_it() {
    use tidb_protocol::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(90))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.fold (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.fold VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let statement = first
        .prepare_general("UPDATE test.fold SET v = v + 5 WHERE id = ?")
        .expect("prepare");
    let affected = match first
        .execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute")
    {
        crate::sql_node::GeneralExecuteOutcome::Write(outcome) => outcome.affected_rows,
        crate::sql_node::GeneralExecuteOutcome::Rows(_) => {
            panic!("an UPDATE answers OK, not with a result set")
        }
    };
    assert_eq!(affected, 1);

    // The lock was taken BEFORE the statement read anything, so a contender
    // on the same row must block until this transaction commits -- exactly
    // the interleaving
    // [`racing_pessimistic_updates_both_commit_with_serial_effect`] pins for
    // the post-run lock step, now exercised by the PRE-lock.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(91))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(&mut second, "UPDATE test.fold SET v = v + 100 WHERE id = 1");
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the prelock out");

    let mut after = factory
        .open_session(session_context(92))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.fold WHERE id = 1")),
        [["115"]],
        "+5 landed from the lock-carrying read, then +100 from the winner that re-read it"
    );
}

/// The same fold over the TEXT protocol: a client-side prepared driver
/// (Connector/J without `useServerPrepStmts`) sends the point `UPDATE` as
/// plain COM_QUERY, so the write-path classification must fire there too.
/// Behavioral assertion as above -- the row is locked BEFORE the statement
/// reads it, so a contender blocks mid-transaction.
#[test]
fn a_text_point_update_locks_its_row_before_reading_it() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(96))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldtxt (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldtxt VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let affected = {
        let out = rows(&mut first, "UPDATE test.foldtxt SET v = v + 5 WHERE id = 1");
        out.len()
    };
    let _ = affected;

    // A contender on the same row must block until this transaction commits:
    // the lock predates the statement's own read.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(97))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldtxt SET v = v + 100 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the prelock out");

    let mut after = factory
        .open_session(session_context(98))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldtxt WHERE id = 1")),
        [["115"]],
        "+5 landed from the lock-carrying read, then +100 from the winner that re-read it"
    );
}

/// Go's `SELECT ... FOR UPDATE` on one clustered handle-pinned row folds its
/// read INTO its lock (`TryFastPlan` -> `PointGetPlan(Lock=true)` ->
/// `getAndLock`). The text path classifies the same shape: the locking read's
/// row answers from the PessimisticLock response, and a contender on the row
/// blocks for the transaction's lifetime. The read-your-own-lock image is
/// also what a later statement in the same transaction must compute from.
#[test]
fn a_text_select_for_update_folds_its_read_into_its_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(99))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldsel (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldsel VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    assert_eq!(
        displayed(rows(
            &mut first,
            "SELECT v FROM test.foldsel WHERE id = 1 FOR UPDATE"
        )),
        [["10"]],
        "the locking read answers its row"
    );

    // The row is already locked by the statement itself: a contender's update
    // waits for this transaction to end.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(100))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldsel SET v = v + 100 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(&mut first, "UPDATE test.foldsel SET v = v + 5 WHERE id = 1");
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after the folding reader commits");

    let mut after = factory
        .open_session(session_context(101))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldsel WHERE id = 1")),
        [["115"]],
        "+5 from the folded reader, then +100 from the winner that re-read it"
    );
}

/// Go SelectLockExec locks the rows the range produces, including a LIMIT
/// winner. A contender must replay the range after that winner is deleted.
#[test]
fn a_locking_range_reselects_after_the_first_row_is_deleted() {
    locking_range_reselects("id", "ORDER BY id LIMIT 1", &["1"], &["2"]);
}

#[test]
fn a_locking_range_without_order_or_limit_still_locks() {
    locking_range_reselects("id", "", &["1", "2", "3"], &["2", "3"]);
}

#[test]
fn a_locking_range_projection_retains_its_composite_handle() {
    locking_range_reselects("id", "ORDER BY id", &["1", "2", "3"], &["2", "3"]);
}

#[test]
fn a_locking_aggregate_reselects_its_input_rows() {
    locking_range_reselects("SUM(id)", "", &["6"], &["5"]);
}

fn locking_range_reselects(projection: &str, window: &str, before: &[&str], after: &[&str]) {
    // Go TestSelectLockForPartitionTable exercises the concurrent LIMIT
    // interleaving; concurrency_tpcc_test uses the prepared composite range.
    for prepared in [false, true] {
        locking_range_reselects_with_protocol(projection, window, before, after, prepared);
    }
}

fn locking_range_reselects_with_protocol(
    projection: &str,
    window: &str,
    before: &[&str],
    after: &[&str],
    prepared: bool,
) {
    use crate::resultset_source::ResultSetSource;
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(110))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.lock_range (w int, d int, id int, PRIMARY KEY(w,d,id) CLUSTERED)",
    );
    rows(
        &mut first,
        "INSERT INTO test.lock_range VALUES (1,1,1),(1,1,2),(1,1,3)",
    );
    first
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    let query =
        format!("SELECT {projection} FROM test.lock_range WHERE w=1 AND d=1 {window} FOR UPDATE");
    let query_rows = |session: &mut ClusterServerSession| {
        if !prepared {
            return rows(session, &query);
        }
        let statement = session
            .prepare_general(&query.replace("w=1 AND d=1", "w=? AND d=?"))
            .expect("prepare locking range");
        let mut selected = Vec::new();
        // Execute the same prepared handle twice, covering retained-plan
        // reconstruction as well as fresh physical executor construction.
        for _ in 0..2 {
            let crate::sql_node::GeneralExecuteOutcome::Rows(mut result) = session
                .execute_general(
                    &statement,
                    &[1, 1].map(tidb_protocol::PreparedValue::SignedLongLong),
                )
                .expect("execute locking range")
            else {
                panic!("SELECT returns rows")
            };
            let source = result.source();
            selected.clear();
            loop {
                let batch = source.next_batch(8).expect("locking rows");
                if batch.is_empty() {
                    break;
                }
                selected.extend(batch);
            }
            source.finish().expect("finish locking rows");
            source.close().expect("close locking rows");
        }
        selected
    };
    let flatten = |rows: Vec<Vec<Datum>>| {
        let mut ids: Vec<String> = displayed(rows)
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(flatten(query_rows(&mut first)), before);

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let result = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(111))
                .expect("session opens");
            second
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin");
            started_tx.send(()).expect("signal begin");
            let result = flatten(query_rows(&mut second));
            second.control_transaction("ROLLBACK").expect("rollback");
            result
        });
        started_rx.recv().expect("contender began");
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(
            &mut first,
            "DELETE FROM test.lock_range WHERE w=1 AND d=1 AND id=1",
        );
        first.control_transaction("COMMIT").expect("commit");
        contender.join().expect("contender finishes")
    });
    assert_eq!(
        result, after,
        "a locking range must not return the deleted winner"
    );
}

#[test]
fn locking_queries_retain_unprojected_record_handles() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(112))
        .expect("session opens");
    for (name, definition) in [
        ("lock_heap", "id int, v int"),
        ("lock_integer", "id int primary key, v int"),
        ("lock_common", "id int, v int, primary key(id,v) clustered"),
    ] {
        rows(
            &mut session,
            &format!("CREATE TABLE test.{name} ({definition})"),
        );
        rows(
            &mut session,
            &format!("INSERT INTO test.{name} VALUES (1,10)"),
        );
        session
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT v FROM test.{name} WHERE v>0 LIMIT 1 FOR UPDATE")
            )),
            [["10"]],
            "record handles must be available without appearing in the result"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT * FROM test.{name} WHERE v>0 LIMIT 1 FOR UPDATE")
            )),
            [["1", "10"]],
            "hidden handles must not leak through wildcard projection"
        );
        session.control_transaction("ROLLBACK").expect("rollback");
    }
    // Go checkLockClauseTables / planner_issue_test's aliased OF targets:
    // the untargeted joined table must remain available to another locker.
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    for target in ["h", "test.lock_heap"] {
        assert_eq!(displayed(rows(&mut session, &format!(
            "SELECT h.v FROM test.lock_heap h JOIN test.lock_integer i ON h.id=i.id FOR UPDATE OF {target}"
        ))), [["10"]]);
    }
    let mut contender = stack.factory.open_session(session_context(115)).unwrap();
    contender.control_transaction("BEGIN PESSIMISTIC").unwrap();
    assert_eq!(
        displayed(rows(
            &mut contender,
            "SELECT v FROM test.lock_integer WHERE id=1 FOR UPDATE NOWAIT"
        )),
        [["10"]]
    );
    assert!(
        matches!(contender.execute("SELECT v FROM test.lock_heap WHERE v>0 FOR UPDATE NOWAIT"),
        Err(error) if error.code == 3572)
    );
    contender.control_transaction("ROLLBACK").unwrap();
    session.control_transaction("ROLLBACK").unwrap();
    assert!(
        matches!(session.execute("SELECT v FROM test.lock_heap FOR UPDATE OF missing"),
        Err(error) if error.code == 1109)
    );
}

/// A SELECT's FOR UPDATE locks its own rows, under the same keys a point
/// lock takes:
/// - Go `buildSubquery` pops the handle map a WHERE subquery leaves, so the
///   lock still sees the outer table's handles through an IN or `<> ANY`
///   semi join;
/// - a partitioned row's lock key names its partition, from the
///   `ExtraPhysTblID` storage fills in from the row's key (Go's TableScan
///   requests that column last);
/// - the subquery's own partitioned table carries that column too, and the
///   semi join drops it (`PhysicalLock.ResolveIndices` binds handles only).
#[test]
fn a_where_subquery_keeps_the_select_lock_on_its_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(993))
        .expect("session opens");
    for sql in [
        "CREATE TABLE test.sublock_t (id int primary key, v int)",
        "CREATE TABLE test.sublock_p (id int primary key, v int) PARTITION BY HASH(id) PARTITIONS 2",
        "CREATE TABLE test.sublock_u (id int primary key, v int) PARTITION BY HASH(id) PARTITIONS 2",
        "INSERT INTO test.sublock_t VALUES (1,10)",
        "INSERT INTO test.sublock_p VALUES (1,10)",
        "INSERT INTO test.sublock_u VALUES (1,10),(2,20)",
    ] {
        rows(&mut session, sql);
    }
    let mut contender = stack
        .factory
        .open_session(session_context(994))
        .expect("contender opens");
    for (locking, contended) in [
        (
            "SELECT id FROM test.sublock_t WHERE v IN (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_t WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_p WHERE v > 0 FOR UPDATE",
            "SELECT v FROM test.sublock_p WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_p WHERE v IN (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_p WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_t WHERE v <> ANY (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_t WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_p WHERE v <> ANY (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_p WHERE id=1 FOR UPDATE NOWAIT",
        ),
    ] {
        session
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        assert_eq!(displayed(rows(&mut session, locking)), [["1"]], "{locking}");
        contender
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("contender begins");
        assert!(
            matches!(contender.execute(contended), Err(error) if error.code == 3572),
            "{locking} left the row unlocked"
        );
        contender.control_transaction("ROLLBACK").unwrap();
        session.control_transaction("ROLLBACK").unwrap();
    }
}

#[test]
fn a_locking_range_sees_rows_committed_after_begin() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack
        .factory
        .open_session(session_context(113))
        .expect("reader");
    let mut writer = stack
        .factory
        .open_session(session_context(114))
        .expect("writer");
    rows(
        &mut reader,
        "CREATE TABLE test.fresh_lock (id int primary key, v int)",
    );
    reader
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    assert!(rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0").is_empty());
    rows(&mut writer, "INSERT INTO test.fresh_lock VALUES (1,10)");
    let locked = displayed(rows(
        &mut reader,
        "SELECT id FROM test.fresh_lock WHERE id>0 ORDER BY id LIMIT 1 FOR UPDATE",
    ));
    let plain = rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0");
    reader.control_transaction("ROLLBACK").expect("rollback");
    assert_eq!(
        locked,
        [["1"]],
        "locking reads use a fresh statement snapshot"
    );
    assert!(plain.is_empty(), "ordinary reads keep the BEGIN snapshot");
}

/// Go's repeatable-read BatchPointGet locks absent keys as well as rows.
/// Returning just the existing row must not permit an INSERT into another
/// key named by the same IN list while the locking transaction stays open.
#[test]
fn a_locking_batch_locks_missing_primary_keys() {
    use crate::resultset_source::ResultSetSource;
    use tidb_protocol::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    for (index, (isolation, change_default, implicit, prepared)) in [
        ("REPEATABLE-READ", false, false, false),
        ("READ-COMMITTED", false, false, false),
        ("REPEATABLE-READ", true, false, false),
        ("READ-COMMITTED", true, false, false),
        ("REPEATABLE-READ", false, true, false),
        ("REPEATABLE-READ", false, false, true),
    ]
    .into_iter()
    .enumerate()
    {
        let mut first = factory.open_session(session_context(115)).expect("reader");
        let table = format!("test.batch_missing_{index}");
        // The prepared case binds reordered, duplicated composite tuples.
        let definition = if prepared {
            "id INT, k INT, PRIMARY KEY(id,k) CLUSTERED"
        } else {
            "id INT PRIMARY KEY"
        };
        rows(&mut first, &format!("CREATE TABLE {table} ({definition})"));
        rows(
            &mut first,
            &format!(
                "INSERT INTO {table} VALUES ({})",
                if prepared { "1,10" } else { "1" }
            ),
        );
        rows(
            &mut first,
            &format!("SET SESSION transaction_isolation='{isolation}'"),
        );
        if implicit {
            rows(&mut first, "SET tidb_txn_mode='pessimistic'");
            rows(&mut first, "SET autocommit=0");
        } else {
            first
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin");
        }
        if change_default {
            let changed = if isolation == "REPEATABLE-READ" {
                "READ-COMMITTED"
            } else {
                "REPEATABLE-READ"
            };
            rows(
                &mut first,
                &format!("SET SESSION transaction_isolation='{changed}'"),
            );
        }
        let selected = if prepared {
            let statement = first
                .prepare_general(&format!(
                    "SELECT id FROM {table} WHERE (k,id) IN ((?,?),(?,?),(?,?)) FOR UPDATE"
                ))
                .expect("prepare batch");
            let params = [10, 1, 20, 2, 20, 2].map(PreparedValue::SignedLongLong);
            let crate::sql_node::GeneralExecuteOutcome::Rows(mut result) = first
                .execute_general(&statement, &params)
                .expect("execute batch")
            else {
                panic!("batch returns rows")
            };
            let source = result.source();
            let mut result = Vec::new();
            loop {
                let batch = source.next_batch(8).expect("batch");
                if batch.is_empty() {
                    break;
                }
                result.extend(batch);
            }
            source.finish().expect("finish");
            source.close().expect("close");
            result
        } else {
            rows(
                &mut first,
                &format!("SELECT id FROM {table} WHERE id IN (1,2) FOR UPDATE"),
            )
        };
        assert_eq!(displayed(selected), [["1"]], "case {index}");

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let blocked = std::thread::scope(|scope| {
            let contender = scope.spawn(|| {
                let mut second = factory.open_session(session_context(116)).expect("writer");
                started_tx.send(()).expect("writer started");
                rows(
                    &mut second,
                    &format!(
                        "INSERT INTO {table} VALUES ({})",
                        if prepared { "2,20" } else { "2" }
                    ),
                );
                finished_tx.send(()).expect("writer finished");
            });
            started_rx.recv().expect("writer starts");
            let blocked = matches!(
                finished_rx.recv_timeout(std::time::Duration::from_millis(200)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            );
            // Release before asserting so failure cannot strand the writer.
            first
                .control_transaction("ROLLBACK")
                .expect("release reader");
            contender.join().expect("writer finishes after release");
            blocked
        });
        assert_eq!(
            blocked,
            isolation == "REPEATABLE-READ",
            "case {index}: absent-key lock follows the transaction's isolation"
        );
        assert_eq!(
            displayed(rows(
                &mut first,
                &format!("SELECT id FROM {table} ORDER BY id")
            )),
            [["1"], ["2"]]
        );
        first
            .control_transaction("ROLLBACK")
            .expect("finish implicit read");
    }
}

/// The prelock joins the failed-statement release list: an EXECUTE that fails
/// AFTER its row was locked (a strict-mode cast error during the assignment)
/// must give the lock back AND drop its cached row image, so a contender can
/// take the row immediately (`OnPessimisticStmtEnd(isSuccessful=false)`).
#[test]
fn a_failed_prelocked_update_releases_its_row() {
    use tidb_protocol::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(93))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldfail (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldfail VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let statement = first
        .prepare_general("UPDATE test.foldfail SET v = 'not-a-number' WHERE id = ?")
        .expect("prepare");
    // The execute outcome borrows the session for its row source, so the
    // error check happens inside a scope that ends before the rollback.
    let failed = {
        let outcome = first.execute_general(&statement, &[PreparedValue::SignedLongLong(1)]);
        outcome.is_err()
    };
    if !failed {
        // A non-strict node coerces instead of failing; then there is no
        // post-lock failure to exercise and the release behavior above is
        // covered by the transaction-end path instead.
        first.control_transaction("ROLLBACK").expect("rollback");
        return;
    }
    // The statement failed after its prelock; the transaction stays open.
    assert_eq!(
        first.control_transaction("ROLLBACK").expect("rollback"),
        Some(false)
    );

    // A contender takes the released row immediately: had the prelock leaked,
    // this would block for the lock-wait timeout instead of completing.
    let mut second = factory
        .open_session(session_context(94))
        .expect("session opens");
    assert_eq!(
        second.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    rows(&mut second, "UPDATE test.foldfail SET v = 99 WHERE id = 1");
    second.control_transaction("COMMIT").expect("commit");

    let mut after = factory
        .open_session(session_context(95))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldfail WHERE id = 1")),
        [["99"]],
        "the failed statement left neither its lock nor its staged write behind"
    );
}

#[test]
fn analyze_sampling_memory_quota_rejects_then_session_recovers() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(991)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.sample_quota (id INT PRIMARY KEY, v VARCHAR(100))",
    );
    rows(
        &mut session,
        "INSERT INTO test.sample_quota VALUES (1,'one'),(2,'two'),(3,'three')",
    );
    rows(&mut session, "SET GLOBAL tidb_mem_quota_analyze = 1");
    let error = session
        .execute_write("ANALYZE TABLE test.sample_quota WITH 1 SAMPLERATE")
        .expect_err("sample exceeds quota");
    assert!(
        error.message.contains("memory") || error.message.contains("quota"),
        "{}",
        error.message
    );
    rows(&mut session, "SET GLOBAL tidb_mem_quota_analyze = -1");
    rows(
        &mut session,
        "ANALYZE TABLE test.sample_quota WITH 1 SAMPLERATE",
    );
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM test.sample_quota").len(),
        1
    );
}

/// Go `TestCreateTableWithForeignKeyMetaInfo`/`checkTableForeignKeysValid`:
/// a cluster CREATE TABLE resolves the parent through the catalog instead of
/// refusing every FOREIGN KEY with 1824.
#[test]
fn cluster_create_table_foreign_key_resolves_parent_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut s = stack.factory.open_session(session_context(901)).expect("session");
    let code = |s: &mut crate::cluster_session_node::ClusterServerSession, sql: &str| {
        s.execute_write(sql).err().map(|error| error.code)
    };
    rows(
        &mut s,
        "CREATE TABLE test.fk_p (id INT PRIMARY KEY, u INT, KEY iu(u))",
    );
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_p(id) ON DELETE CASCADE)"),
        None
    );
    let meta = rows(&mut s, "SHOW CREATE TABLE test.fk_c");
    let ddl = displayed(meta)[0][1].clone();
    assert!(ddl.contains("CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `fk_p` (`id`) ON DELETE CASCADE"), "{ddl}");
    assert!(ddl.contains("KEY `fk_1` (`pid`)"), "auto FK index: {ddl}");
    // Missing parent: 1824 (checks on); accepted with checks off.
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c2 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_none(id))"),
        Some(1824)
    );
    // Incompatible types: 3780; parent column without index: 1822;
    // missing parent column: 3734.
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c3 (id INT PRIMARY KEY, pid VARCHAR(10), FOREIGN KEY (pid) REFERENCES fk_p(id))"),
        Some(3780)
    );
    rows(&mut s, "CREATE TABLE test.fk_q (id INT PRIMARY KEY, v INT)");
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c4 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_q(v))"),
        Some(1822)
    );
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c5 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_q(nope))"),
        Some(3734)
    );
    s.execute_write("SET foreign_key_checks = 0").expect("fk checks off");
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c6 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_none(id))"),
        None
    );
}

/// Go `PBToExpr` decodes every signature the pushdown catalog admits
/// (`distsql_builtin.go` `getSignatureByPB`); CASE/IF/IFNULL pushed into a
/// coprocessor Selection were refused with 1105 before.
#[test]
fn pushed_conditional_signatures_evaluate_in_the_coprocessor_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut s = stack
        .factory
        .open_session(session_context(902))
        .expect("session");
    rows(
        &mut s,
        "CREATE TABLE test.pc (id INT PRIMARY KEY, a INT, b INT, d DATETIME, j JSON)",
    );
    s.execute_write(
        "INSERT INTO test.pc VALUES (1, 1, NULL, '2024-01-02 03:04:05', '\"x\"'), \
         (2, 5, 7, NULL, NULL), (3, NULL, 2, '2020-05-06 00:00:00', '\"yy\"')",
    )
    .expect("insert");
    let ids = |s: &mut crate::cluster_session_node::ClusterServerSession, sql: &str| {
        displayed(rows(s, sql)).into_iter().map(|row| row[0].clone()).collect::<Vec<_>>()
    };
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CASE WHEN a > 2 THEN a ELSE 0 END > 1 ORDER BY id"), ["2"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IF(a IS NULL, b, a) = 2 ORDER BY id"), ["3"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IFNULL(b, 0) < 3 ORDER BY id"), ["1", "3"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CAST(d AS SIGNED) > 20210000000000 ORDER BY id"), ["1"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IFNULL(CAST(j AS CHAR), 'n') = '\"yy\"' ORDER BY id"), ["3"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE IF(a > 2, 'yes', 'no') = 'yes' ORDER BY id"), ["2"]);
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CASE WHEN a > 2 THEN 'yes' ELSE 'no' END = 'yes' ORDER BY id"), ["2"]);
    // Go wraps non-integer conditions with typed IsTrueWithNull before
    // the control builtin reads EvalInt, so a nonzero fraction stays TRUE.
    assert_eq!(ids(&mut s, "SELECT id FROM test.pc WHERE CASE WHEN a / 10 THEN 1 ELSE 0 END = 1 ORDER BY id"), ["1", "2"]);
}

#[test]
fn pushed_conditionals_keep_the_request_timezone() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(903))
        .expect("session");
    rows(
        &mut session,
        "CREATE TABLE test.cop_zone (id INT PRIMARY KEY, d DATETIME)",
    );
    session
        .execute_write("INSERT INTO test.cop_zone VALUES (1, '2024-01-02 03:04:05')")
        .unwrap();
    for zone in ["+00:00", "+08:00", "America/New_York"] {
        rows(&mut session, &format!("SET time_zone='{zone}'"));
        // Compare a scalar with the identical builtin nested in a conditional
        // tree. Both belong to this DAG's timezone.
        let sql = "SELECT id FROM test.cop_zone WHERE \
            CASE WHEN id>0 THEN UNIX_TIMESTAMP(d) ELSE 0 END = UNIX_TIMESTAMP(d)";
        assert_eq!(displayed(rows(&mut session, sql)), [["1"]], "{zone}");
    }
}

#[test]
fn shared_update_record_uses_cluster_statement_buffer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(683)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.merged_update (id INT PRIMARY KEY, x INT, y INT, KEY ix(x))",
    );
    rows(
        &mut session,
        "INSERT INTO test.merged_update VALUES (1,10,20),(2,30,40)",
    );
    rows(&mut session, "UPDATE test.merged_update a JOIN test.merged_update b ON a.id=b.id SET a.x=a.x+1,b.y=b.y+1");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id,x,y FROM test.merged_update ORDER BY id"
        )),
        [["1", "11", "21"], ["2", "31", "41"]]
    );
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    rows(
        &mut session,
        "INSERT INTO test.merged_update VALUES (3,50,60)",
    );
    let before = session.buffer.snapshot();
    let error = match session.execute("UPDATE test.merged_update a JOIN test.merged_update b ON a.id=b.id SET a.id=IF(a.id=1,11,3) WHERE a.id<3") {
        Err(error) => error,
        Ok(_) => panic!("the later duplicate must fail the statement"),
    };
    assert_eq!(error.code, 1062);
    assert_eq!(session.buffer.snapshot(), before);
    session.control_transaction("COMMIT").unwrap();
    let mut reader = stack.factory.open_session(session_context(684)).unwrap();
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id,x,y FROM test.merged_update ORDER BY id"
        )),
        [["1", "11", "21"], ["2", "31", "41"], ["3", "50", "60"]]
    );
}

#[test]
fn generated_write_policy_reaches_cluster_storage_and_prepared_execution() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(990)).unwrap();
    rows(&mut session, "USE test");
    rows(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'");
    rows(&mut session, "CREATE TABLE generated_policy (id INT PRIMARY KEY, a INT, b TINYINT AS (a) STORED, KEY idx(b))");
    let error = session
        .execute_write("INSERT INTO generated_policy(id,a) VALUES (1,1000)")
        .unwrap_err();
    assert_eq!(error.code, 1264);
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM generated_policy")),
        [["0"]]
    );
    rows(&mut session, "SET sql_mode=''");
    rows(
        &mut session,
        "INSERT INTO generated_policy(id,a) VALUES (1,1000)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "1264",
            "Out of range value for column 'b' at row 1"
        ]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a,b FROM generated_policy FORCE INDEX(idx) WHERE b=127"
        )),
        [["1000", "127"]]
    );
    let prepared = session
        .prepare_general("UPDATE generated_policy SET a=? WHERE id=1")
        .unwrap();
    rows(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'");
    let error = session
        .execute_general(
            &prepared,
            &[tidb_protocol::PreparedValue::SignedLongLong(2000)],
        )
        .err()
        .expect("prepared UPDATE uses its execution-time statement policy");
    assert_eq!(error.code, 1264);
    assert_eq!(
        displayed(rows(&mut session, "SELECT a,b FROM generated_policy")),
        [["1000", "127"]]
    );
    rows(&mut session, "SET sql_mode=''");
    session
        .execute_general(
            &prepared,
            &[tidb_protocol::PreparedValue::SignedLongLong(2000)],
        )
        .unwrap();
    // Go answers a non-strict UPDATE's generated-column overflow with the
    // conversion's own warning, not INSERT's 1264: `castColumnValue` hands
    // `ErrOverflow` ("constant %v overflows %s", 1690) to
    // `TypeCtx.HandleTruncate`, which under TruncateAsWarning appends it and
    // returns nil, so `handleUpdateError`'s 1264 mapping never sees an
    // error. Captured from Go TiDB on this exact sequence:
    // `[[Warning 1690 constant 2000 overflows tinyint]]`.
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Warning", "1690", "constant 2000 overflows tinyint"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT a,b FROM generated_policy")),
        [["2000", "127"]]
    );
}

#[test]
fn generated_read_policy_reaches_cluster_point_and_scan_readers() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(991)).unwrap();
    rows(&mut session, "USE test");
    rows(&mut session, "SET sql_mode=''");
    rows(
        &mut session,
        "CREATE TABLE generated_read (id INT PRIMARY KEY, a VARCHAR(12), b INT AS (a) VIRTUAL)",
    );
    rows(
        &mut session,
        "INSERT INTO generated_read(id,a) VALUES (1,'12x')",
    );
    for sql in [
        "SELECT b FROM generated_read WHERE id=1",
        "SELECT b FROM generated_read",
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), [["12"]]);
        assert_eq!(
            displayed(rows(&mut session, "SHOW WARNINGS")),
            [["Warning", "1292", "Truncated incorrect DOUBLE value: '12x'"]]
        );
    }
}

#[test]
fn analyze_preserves_generated_execution_error() {
    use crate::resultset_source::ResultSetSource;

    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(992)).unwrap();
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE analyze_error (a DOUBLE)");
    rows(&mut session, "INSERT INTO analyze_error VALUES (0)");
    rows(
        &mut session,
        "ALTER TABLE analyze_error ADD COLUMN b DOUBLE AS (cot(a)) VIRTUAL",
    );
    let (original_code, original_state, original_message) =
        match session.execute("SELECT b FROM analyze_error") {
            Err(error) => (error.code, error.state, error.message),
            Ok(mut result) => {
                let error = result
                    .source()
                    .next_batch(8)
                    .expect_err("invalid expression");
                result.source().close().unwrap();
                (error.code, error.state, error.message)
            }
        };
    assert_eq!((original_code, original_state), (1690, *b"22003"));
    let error = session
        .execute_write("ANALYZE TABLE analyze_error")
        .expect_err("invalid virtual sample");
    assert_eq!((error.code, error.state), (1690, *b"22003"));
    assert_eq!(error.message, original_message);
    assert_eq!(displayed(rows(&mut session, "SELECT 1")), [["1"]]);
    assert!(rows(
        &mut session,
        "SHOW STATS_HISTOGRAMS WHERE table_name = 'analyze_error'"
    )
    .is_empty());
    assert_eq!(displayed(rows(&mut session,
        "SELECT state, process_id IS NULL, fail_reason FROM mysql.analyze_jobs \
         IGNORE INDEX (PRIMARY, update_time, idx_schema_table_state, idx_schema_table_partition_state) \
         WHERE table_name = 'analyze_error'")),
        vec![vec!["failed".to_owned(), "1".to_owned(), original_message]]);
    rows(&mut session, "ALTER TABLE analyze_error DROP COLUMN b");
    rows(&mut session, "ANALYZE TABLE analyze_error");
    assert!(!rows(
        &mut session,
        "SHOW STATS_HISTOGRAMS WHERE table_name = 'analyze_error'"
    )
    .is_empty());
}

#[test]
fn binding_maintenance_preserves_usage_and_collects_tombstones_without_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(128)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.binding_maintenance (id INT PRIMARY KEY, v INT, KEY iv(v))",
    );
    rows(
        &mut session,
        "INSERT INTO test.binding_maintenance VALUES (1,10)",
    );
    rows(&mut session, "CREATE GLOBAL BINDING FOR SELECT * FROM test.binding_maintenance WHERE v=10 USING SELECT * FROM test.binding_maintenance USE INDEX(iv) WHERE v=10");
    rows(
        &mut session,
        "SELECT * FROM test.binding_maintenance WHERE v=10",
    );
    let bindings = stack.factory.bindings.as_ref().unwrap();
    let owner = bindings.cache().load();
    let binding = owner
        .get_all_bindings()
        .into_iter()
        .find(|b| b.usage_snapshot().last_used_at.is_some())
        .expect("matching records shared usage");
    let digest = binding.usage_snapshot().sql_digest;
    bindings.reload().unwrap();
    assert!(Arc::ptr_eq(&owner, &bindings.cache().load()));
    assert!(Arc::ptr_eq(&binding, &owner.get_binding(&digest).unwrap()));
    rows(&mut session, "SET GLOBAL tidb_enable_binding_usage=OFF");
    bindings.write_usage().unwrap();
    assert!(binding.usage_snapshot().last_saved_at.is_none());
    rows(&mut session, "SET GLOBAL tidb_enable_binding_usage=ON");
    bindings.write_usage().expect("usage storage transaction");
    assert!(binding.usage_snapshot().last_saved_at.is_some());
    assert_eq!(displayed(rows(&mut session, &format!("SELECT last_used_date IS NOT NULL FROM mysql.bind_info WHERE sql_digest='{digest}'"))), [["1"]]);
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('aged binding', 'SELECT 1', 'test', 'deleted', '2001-01-01 00:00:00', '2001-01-01 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'aged_binding_maintenance')");
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('young binding', 'SELECT 1', 'test', 'deleted', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'utf8mb4', 'utf8mb4_bin', 'manual', 'young_binding_maintenance')");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.bind_info WHERE sql_digest='aged_binding_maintenance'"
        )),
        [["1"]]
    );
    bindings.gc().expect("owner GC independent of writes");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.bind_info WHERE sql_digest='aged_binding_maintenance'"
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.bind_info WHERE sql_digest='young_binding_maintenance'"
        )),
        [["1"]]
    );
}

#[test]
fn login_durability_batch_serializes_counters_and_preserves_attributes() {
    use crate::configured_user_store::AuthenticationFailure;
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(688)).unwrap();
    rows(&mut session, "CREATE USER 'durable_login'@'%' FAILED_LOGIN_ATTEMPTS 100 PASSWORD_LOCK_TIME 1 ATTRIBUTE '{\"keep\": \"metadata\"}'");
    let mut workers = Vec::new();
    for _ in 0..8 {
        let users = users.clone();
        workers.push(std::thread::spawn(move || {
            assert_eq!(
                users.authenticate_native(
                    "durable_login",
                    "127.0.0.1",
                    b"01234567890123456789",
                    b"wrong"
                ),
                Err(AuthenticationFailure::AccessDenied)
            );
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    let attributes = displayed(rows(
        &mut session,
        "SELECT user_attributes FROM mysql.user WHERE User='durable_login' AND Host='%'",
    ))
    .remove(0)
    .remove(0);
    let attributes: serde_json::Value = serde_json::from_str(&attributes).unwrap();
    assert_eq!(attributes["Password_locking"]["failed_login_count"], 8);
    assert_eq!(attributes["metadata"]["keep"], "metadata");
    assert!(attributes["Password_locking"]
        .get("auto_locked_last_changed")
        .is_none());
    users
        .authenticate_native("durable_login", "127.0.0.1", b"01234567890123456789", b"")
        .unwrap();
    let attributes = displayed(rows(
        &mut session,
        "SELECT user_attributes FROM mysql.user WHERE User='durable_login' AND Host='%'",
    ))
    .remove(0)
    .remove(0);
    let attributes: serde_json::Value = serde_json::from_str(&attributes).unwrap();
    assert_eq!(attributes["Password_locking"]["failed_login_count"], 0);
    assert_eq!(attributes["metadata"]["keep"], "metadata");
    assert!(attributes["Password_locking"]["auto_locked_last_changed"].is_string());
}

#[test]
fn login_durability_batch_locks_unlocks_and_checks_stale_cache() {
    use crate::configured_user_store::AuthenticationFailure;
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(689)).unwrap();
    rows(
        &mut session,
        "CREATE USER 'durable_lock'@'%' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1",
    );
    assert!(matches!(
        users.authenticate_native(
            "durable_lock",
            "127.0.0.1",
            b"01234567890123456789",
            b"wrong"
        ),
        Err(AuthenticationFailure::AutoLocked(_))
    ));
    let attributes = displayed(rows(
        &mut session,
        "SELECT user_attributes FROM mysql.user WHERE User='durable_lock' AND Host='%'",
    ))
    .remove(0)
    .remove(0);
    let attributes: serde_json::Value = serde_json::from_str(&attributes).unwrap();
    assert_eq!(attributes["Password_locking"]["auto_account_locked"], "Y");
    let locked = users
        .accounts()
        .password_locking("durable_lock", "%")
        .unwrap();
    let mut stale = locked;
    stale.auto_account_locked = false;
    users
        .accounts()
        .publish_password_locking("durable_lock", "%", stale);
    assert!(
        matches!(
            users.authenticate_native("durable_lock", "127.0.0.1", b"01234567890123456789", b""),
            Err(AuthenticationFailure::AutoLocked(_))
        ),
        "successful password must read the authoritative locked row"
    );
    rows(&mut session, "UPDATE mysql.user SET user_attributes=JSON_MERGE_PATCH(user_attributes, '{\"Password_locking\": {\"failed_login_attempts\": 0}}') WHERE User='durable_lock' AND Host='%'");
    assert!(
        matches!(
            users.authenticate_native("durable_lock", "127.0.0.1", b"01234567890123456789", b""),
            Err(AuthenticationFailure::AutoLocked(_))
        ),
        "authoritative lock wins even if tracking was concurrently disabled"
    );
    rows(&mut session, "UPDATE mysql.user SET user_attributes=JSON_MERGE_PATCH(user_attributes, '{\"Password_locking\": {\"failed_login_attempts\": 1}}') WHERE User='durable_lock' AND Host='%'");
    users
        .accounts()
        .publish_password_locking("durable_lock", "%", locked);
    users.accounts().clock().advance(24 * 60 * 60 + 1);
    users
        .authenticate_native("durable_lock", "127.0.0.1", b"01234567890123456789", b"")
        .unwrap();
    let attributes = displayed(rows(
        &mut session,
        "SELECT user_attributes FROM mysql.user WHERE User='durable_lock' AND Host='%'",
    ))
    .remove(0)
    .remove(0);
    let attributes: serde_json::Value = serde_json::from_str(&attributes).unwrap();
    assert_eq!(attributes["Password_locking"]["auto_account_locked"], "N");
    assert_eq!(attributes["Password_locking"]["failed_login_count"], 0);
}

#[test]
fn login_durability_batch_expiry_and_missing_row_do_not_publish_success() {
    use crate::configured_user_store::AuthenticationFailure;
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(690)).unwrap();
    rows(&mut session, "CREATE USER 'durable_expiry'@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 PASSWORD EXPIRE");
    assert_eq!(
        users.authenticate_native(
            "durable_expiry",
            "127.0.0.1",
            b"01234567890123456789",
            b"wrong"
        ),
        Err(AuthenticationFailure::AccessDenied)
    );
    assert_eq!(
        users.authenticate_native("durable_expiry", "127.0.0.1", b"01234567890123456789", b""),
        Err(AuthenticationFailure::PasswordExpired)
    );
    let attributes = displayed(rows(
        &mut session,
        "SELECT user_attributes FROM mysql.user WHERE User='durable_expiry' AND Host='%'",
    ))
    .remove(0)
    .remove(0);
    let attributes: serde_json::Value = serde_json::from_str(&attributes).unwrap();
    assert_eq!(attributes["Password_locking"]["failed_login_count"], 1);
    rows(
        &mut session,
        "DELETE FROM mysql.user WHERE User='durable_expiry' AND Host='%'",
    );
    let before = users.accounts().password_locking("durable_expiry", "%");
    assert!(matches!(
        users.authenticate_native(
            "durable_expiry",
            "127.0.0.1",
            b"01234567890123456789",
            b"wrong"
        ),
        Err(AuthenticationFailure::Storage(_))
    ));
    assert_eq!(
        users.accounts().password_locking("durable_expiry", "%"),
        before
    );
}

#[test]
fn historical_read_batch_schema_data_and_timestamp_lifetime() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(914)).unwrap();
    rows(&mut session, "CREATE DATABASE history_batch");
    rows(&mut session, "USE history_batch");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO t VALUES (1,10)");
    session.control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t");
    let ts: u64 = displayed(rows(&mut session, "SELECT @@tidb_current_ts"))[0][0]
        .parse()
        .unwrap();
    session.control_transaction("COMMIT").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    rows(&mut session, "UPDATE t SET v=20 WHERE id=1");
    rows(&mut session, "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7");
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT * FROM t AS OF TIMESTAMP {ts}")
        )),
        vec![vec!["1", "10"]]
    );
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts));
    session
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    assert!(tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts));
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(
        tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "transaction protects time between reads"
    );
    for sql in [
        format!("SET tidb_snapshot='{ts}'"),
        "SET tidb_snapshot=DEFAULT".into(),
    ] {
        assert_eq!(session.execute(&sql).err().unwrap().code, 1568);
    }
    assert_eq!(
        session
            .execute("SET tidb_snapshot=JSON_EXTRACT('invalid', '$')")
            .err()
            .unwrap()
            .code,
        3140,
        "SET evaluates before refusing transaction changes"
    );
    assert!(session.execute_write("UPDATE t SET v=30").is_err());
    assert!(session.execute("SELECT * FROM t FOR UPDATE").is_err());
    assert!(session
        .execute("EXPLAIN ANALYZE UPDATE t SET v=30")
        .is_err());
    session.control_transaction("ROLLBACK").unwrap();
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts));
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "20", "7"]]
    );
    let future = ts + (1_000_000 << 18);
    assert!(session
        .execute(&format!("SELECT * FROM t AS OF TIMESTAMP {future}"))
        .is_err());
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&future));
    assert!(session
        .execute(&format!("SET tidb_snapshot='{ts}'"))
        .err()
        .unwrap()
        .message
        .contains("can not get 'tikv_gc_safe_point'"));
    rows(
        &mut session,
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05 -0700', '')",
    );
    rows(&mut session, &format!("SET tidb_snapshot='{ts}'"));
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "idle snapshot SET retains schema, not an active native timestamp"
    );
    assert!(session
        .execute(&format!("SET tidb_snapshot='{future}'"))
        .is_err());
    assert_eq!(session.session.vars().snapshot_ts(), ts);
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(session.execute_write("UPDATE t SET v=30").is_err());
    assert!(session
        .execute("EXPLAIN ANALYZE UPDATE t SET v=30")
        .is_err());
    assert!(session
        .execute_write("ALTER TABLE t ADD COLUMN forbidden INT")
        .is_err());
    assert!(session
        .execute_write("CREATE DATABASE forbidden_history")
        .is_err());
    rows(&mut session, "SET autocommit=0");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(
        session.session.historical_read_ts().is_none(),
        "SnapshotTS is a statement pin"
    );
    rows(&mut session, "SET autocommit=1");
    rows(&mut session, "SET tidb_snapshot='' ");
    // A one-second staleness reads at @@timestamp - 1s, which Go's
    // `GoTimeToTS` floors to the millisecond, and @@timestamp reaches the
    // clock through a float64 (`math.Modf`): `<ts_ms>.925` arrives as
    // .924999952s. A nominal 1ms margin can therefore floor to `ts`'s own
    // millisecond, before the INSERT that committed in that millisecond.
    // Two milliseconds always clear it.
    rows(
        &mut session,
        &format!("SET timestamp={:.3}", (ts >> 18) as f64 / 1000.0 + 1.002),
    );
    rows(&mut session, "SET tidb_read_staleness=-1");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    rows(&mut session, "SET autocommit=0");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    let held = session.session.historical_read_ts().unwrap();
    assert!(tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&held));
    session.control_transaction("ROLLBACK").unwrap();
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&held));
    rows(&mut session, "UPDATE t SET v=20 WHERE id=1");
    assert!(
        session.session.historical_read_ts().is_none(),
        "a write starts an ordinary transaction"
    );
    session.control_transaction("ROLLBACK").unwrap();
    rows(&mut session, "SET autocommit=1");
    rows(&mut session, "SET timestamp=0, tidb_read_staleness=0");
    rows(
        &mut session,
        &format!("PREPARE history_query FROM 'SELECT * FROM t AS OF TIMESTAMP {ts}'"),
    );
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE history_query")),
        vec![vec!["1", "10"]]
    );
    rows(&mut session, "DEALLOCATE PREPARE history_query");
    session
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    session.control_transaction("ROLLBACK").unwrap();
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "rollback without a read retires the metadata snapshot"
    );
    session
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    drop(session);
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "disconnect retires the historical owner"
    );
}

#[test]
fn timestamp_entrypoints_batch_one_shot_and_external_share_history() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(925)).unwrap();
    rows(&mut session, "SET time_zone='+00:00'");
    rows(&mut session, "CREATE DATABASE ts_entry");
    rows(&mut session, "USE ts_entry");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO t VALUES (1,10)");
    std::thread::sleep(Duration::from_millis(5));
    session.control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t");
    let ts: u64 = displayed(rows(&mut session, "SELECT @@tidb_current_ts"))[0][0]
        .parse()
        .unwrap();
    session.control_transaction("COMMIT").unwrap();
    let stamp = displayed(rows(
        &mut session,
        &format!("SELECT FROM_UNIXTIME({:.3})", (ts >> 18) as f64 / 1000.0),
    ))[0][0]
        .clone();
    std::thread::sleep(Duration::from_millis(5));
    rows(&mut session, "UPDATE t SET v=20");
    rows(&mut session, "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7");
    let set = format!("SET tx_read_ts='{stamp}'");
    rows(&mut session, &set);
    assert!(session.execute_write("UPDATE t SET v=99").is_err());
    assert!(session.execute("SELECT * FROM t FOR UPDATE").is_err());

    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert_eq!(session.session.vars().txn_read_ts(), 0);
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "20", "7"]]
    );
    rows(&mut session, "PREPARE hist FROM 'SELECT * FROM t'");
    rows(&mut session, &set);
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE hist")),
        vec![vec!["1", "10"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE hist")),
        vec![vec!["1", "20", "7"]]
    );
    rows(&mut session, "DEALLOCATE PREPARE hist");
    rows(&mut session, &set);
    assert!(session
        .execute(&format!("SELECT * FROM t AS OF TIMESTAMP {ts}"))
        .is_err());
    assert_eq!(session.session.vars().txn_read_ts(), 0);
    rows(&mut session, &set);
    assert!(session
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .is_err());
    session.control_transaction("BEGIN").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(session.execute(&set).is_err());
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(session.session.vars().txn_read_ts(), 0);
    assert!(!tidb_txnkv::ACTIVE_START_TS
        .snapshot()
        .contains(&((ts >> 18) << 18)));
    rows(&mut session, &format!("SET GLOBAL tidb_external_ts={ts}"));
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@tidb_external_ts")),
        vec![vec![ts.to_string()]]
    );
    for sql in [
        format!("SET GLOBAL tidb_external_ts={}", ts - 1),
        "SET GLOBAL tidb_external_ts=DEFAULT".into(),
        format!("SET GLOBAL tidb_external_ts={}", ts + (1_000_000 << 18)),
    ] {
        assert!(session.execute(&sql).is_err(), "{sql}");
    }
    rows(&mut session, "SET tidb_enable_external_ts_read=ON");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );

    assert!(session.execute_write("UPDATE t SET v=99").is_err());
    session.control_transaction("BEGIN").unwrap();
    rows(&mut session, "SET tidb_enable_external_ts_read=OFF");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    session.control_transaction("ROLLBACK").unwrap();
    rows(
        &mut session,
        "PREPARE point_history FROM 'SELECT v FROM t WHERE id=?'",
    );
    rows(&mut session, "SET @id=1");
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE point_history USING @id")),
        vec![vec!["20"]]
    );
    rows(&mut session, &set);
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE point_history USING @id")),
        vec![vec!["10"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE point_history USING @id")),
        vec![vec!["20"]]
    );
    rows(&mut session, "SET tidb_enable_external_ts_read=ON");
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE point_history USING @id")),
        vec![vec!["10"]]
    );
    rows(&mut session, "DEALLOCATE PREPARE point_history");
    let mut peer = stack.factory.open_session(session_context(926)).unwrap();
    rows(&mut peer, "USE ts_entry");
    rows(&mut peer, "SET tidb_enable_external_ts_read=ON");
    assert_eq!(
        displayed(rows(&mut peer, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    peer.session.set_restricted_sql(true);
    assert_eq!(
        displayed(rows(&mut peer, "SELECT * FROM t")),
        vec![vec!["1", "20", "7"]]
    );
}

/// Go isolation snapshot switching: schema and storage change for the statement,
/// while the ordinary transaction and its buffered writes retain their identity.
#[test]
fn snapshot_provider_batch_real_storage_switches_without_exposing_buffered_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(937)).unwrap();
    rows(&mut session, "CREATE DATABASE snapshot_provider_batch");
    rows(&mut session, "USE snapshot_provider_batch");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO t VALUES (1,10)");
    rows(
        &mut session,
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05 -0700', '')",
    );
    session.control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t");
    let historical_ts = session.session.current_tso().value() as u64;
    session.control_transaction("COMMIT").unwrap();
    rows(&mut session, "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7");
    for mode in ["OPTIMISTIC", "PESSIMISTIC"] {
        session
            .control_transaction(&format!("BEGIN {mode}"))
            .unwrap();
        rows(&mut session, "UPDATE t SET v=30 WHERE id=1");
        rows(&mut session, "INSERT INTO t VALUES (2,40,8)");
        let ordinary_ts = session.session.current_tso().value();
        rows(&mut session, &format!("SET tidb_snapshot={historical_ts}"));
        assert_eq!(
            displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
            vec![vec!["1", "10"]]
        );
        assert_eq!(
            displayed(rows(&mut session, "SELECT v FROM t WHERE id=1")),
            vec![vec!["10"]]
        );
        rows(
            &mut session,
            "PREPARE snapshot_query FROM 'SELECT * FROM t ORDER BY id'",
        );
        assert_eq!(
            displayed(rows(&mut session, "EXECUTE snapshot_query")),
            vec![vec!["1", "10"]]
        );
        rows(&mut session, "DEALLOCATE PREPARE snapshot_query");
        assert!(session.execute("SELECT extra FROM t").is_err());
        assert!(session.execute_write("UPDATE t SET v=99").is_err());
        assert!(session.session.in_transaction());
        assert_eq!(session.session.current_tso().value(), ordinary_ts);
        assert!(
            !tidb_txnkv::ACTIVE_START_TS
                .snapshot()
                .contains(&historical_ts)
        );
        rows(&mut session, "SET tidb_snapshot=''");
        assert_eq!(
            displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
            vec![vec!["1", "30", "7"], vec!["2", "40", "8"]]
        );
        session.control_transaction("ROLLBACK").unwrap();
        assert_eq!(
            displayed(rows(&mut session, "SELECT * FROM t")),
            vec![vec!["1", "10", "7"]]
        );
    }
}

#[test]
fn deferred_uniqueness_batch_writes_skip_locks_and_check_at_commit() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(940)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_writes");
    rows(&mut session, "USE deferred_writes");
    rows(
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE)",
    );
    rows(&mut session, "INSERT INTO t VALUES (1,10),(2,20)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    rows(&mut session, "SET tidb_txn_assertion_level=OFF");
    for sql in [
        "INSERT INTO t VALUES (3,30)",
        "INSERT INTO t VALUES (1,30)",
        "INSERT INTO t VALUES (3,10)",
        "UPDATE t SET id=3,u=30 WHERE id=1",
        "INSERT INTO t VALUES (1,40) ON DUPLICATE KEY UPDATE id=3,u=30",
    ] {
        session.control_transaction("BEGIN PESSIMISTIC").unwrap();
        let stage = session.buffer.checkpoint();
        session.execute_write(sql).unwrap();
        let lock_keys = session.buffer.pessimistic_keys_since(stage);
        // New record/unique-index keys defer their absence check. UPDATE
        // retains the old record lock and deletion entries as Go does.
        let presumed = session.buffer.presume_not_exists_keys();
        assert!(!presumed.is_empty(), "{sql}");
        assert!(
            lock_keys.iter().all(|key| !presumed.contains(key)),
            "{sql}: deferred keys were locked"
        );
        session.buffer.release(stage);
        if sql == "INSERT INTO t VALUES (1,30)" || sql == "INSERT INTO t VALUES (3,10)" {
            let error = session.control_transaction("COMMIT").unwrap_err();
            assert_eq!(error.code, 1062, "{sql}: {error:?}");
        } else {
            session.control_transaction("ROLLBACK").unwrap();
        }
    }
    // Go unistore checks NotExist before Insert when assertions are enabled.
    rows(&mut session, "SET tidb_txn_assertion_level=STRICT");
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    rows(&mut session, "INSERT INTO t VALUES (1,30)");
    assert_eq!(
        session.control_transaction("COMMIT").unwrap_err().code,
        8141
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![vec!["1", "10"], vec!["2", "20"]]
    );
}

#[test]
fn deferred_uniqueness_batch_savepoint_follows_transaction_mode() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(941)).unwrap();
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    // Autocommit SAVEPOINT is still a no-op; optimistic transactions work.
    session.control_transaction("SAVEPOINT empty").unwrap();
    session.control_transaction("BEGIN OPTIMISTIC").unwrap();
    session.control_transaction("SAVEPOINT allowed").unwrap();
    session.control_transaction("ROLLBACK").unwrap();
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    let error = session
        .control_transaction("SAVEPOINT refused")
        .unwrap_err();
    assert_eq!(error.code, 1105);
    assert_eq!(error.message, "savepoint is not supported in pessimistic transactions when in-place constraint check is disabled");
    assert!(session.session.in_transaction());
    session.control_transaction("ROLLBACK").unwrap();
}

#[test]
fn deferred_uniqueness_batch_terminal_dml_error_aborts_prior_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(942)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_abort");
    rows(&mut session, "USE deferred_abort");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    rows(&mut session, "INSERT INTO t VALUES (1)");
    let error = session
        .execute_write("INSERT INTO t VALUES (1)")
        .unwrap_err();
    assert_eq!(error.code, 8147, "{error:?}");
    assert!(error.message.starts_with(
        "transaction aborted because lazy uniqueness check is enabled and an error occurred:"
    ));
    assert!(!session.session.in_transaction());
    assert!(session.explicit.is_none());
    assert!(session.buffer.is_empty());
    assert!(rows(&mut session, "SELECT * FROM t").is_empty());
    rows(&mut session, "INSERT INTO t VALUES (2)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["2"]]
    );
}

#[test]
fn deferred_uniqueness_batch_scope_tombstones_and_successful_commit() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(943)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_scope");
    rows(&mut session, "USE deferred_scope");
    rows(
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE)",
    );
    rows(&mut session, "INSERT INTO t VALUES (1,10),(2,20)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    session.control_transaction("BEGIN OPTIMISTIC").unwrap();
    session.control_transaction("ROLLBACK").unwrap();
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    session.session.set_restricted_sql(true);
    session.session.set_restricted_sql(false);
    session.session.set_connection_id(0);
    session.session.set_connection_id(943);
    rows(&mut session, "DELETE FROM t WHERE id=1");
    rows(&mut session, "INSERT INTO t VALUES (1,10)");
    assert!(
        session.buffer.presume_not_exists_keys().is_empty(),
        "local tombstones prove absence"
    );
    rows(&mut session, "INSERT INTO t VALUES (3,30),(4,NULL)");
    rows(&mut session, "UPDATE t SET u=31 WHERE id=3");
    rows(&mut session, "INSERT IGNORE INTO t VALUES (5,20)");
    rows(&mut session, "UPDATE IGNORE t SET u=20 WHERE id=3");
    assert!(session.session.in_transaction());
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=ON",
    );
    rows(&mut session, "INSERT INTO t VALUES (6,60)");
    session.control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![
            vec!["1", "10"],
            vec!["2", "20"],
            vec!["3", "31"],
            vec!["4", "NULL"],
            vec!["6", "60"]
        ]
    );
    rows(&mut session, "SET autocommit=0");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    assert!(session.control_transaction("SAVEPOINT implicit").is_err());
    session.control_transaction("ROLLBACK").unwrap();
    rows(&mut session, "SET autocommit=1");
}

#[test]
fn deferred_uniqueness_batch_binary_prepared_dml_shares_abort_policy() {
    use tidb_protocol::PreparedValue;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(944)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_binary");
    rows(&mut session, "USE deferred_binary");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    let insert = session.prepare_general("INSERT INTO t VALUES (?)").unwrap();
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    session
        .execute_general(&insert, &[PreparedValue::SignedLongLong(1)])
        .unwrap();
    assert!(session.execute("SELECT missing FROM t").is_err());
    assert!(
        session.session.in_transaction(),
        "ordinary query errors do not invoke DML abort policy"
    );
    let error = match session.execute_general(&insert, &[PreparedValue::SignedLongLong(1)]) {
        Err(error) => error,
        Ok(_) => panic!("a local duplicate must fail"),
    };
    assert_eq!(error.code, 8147, "{error:?}");
    assert!(!session.session.in_transaction());
    assert!(session.explicit.is_none());
    assert!(session.buffer.is_empty());
    assert!(rows(&mut session, "SELECT * FROM t").is_empty());
}

/// Go reorg.go derives expression error levels and location from ReorgMeta.
/// Exercise every SQL producer through the same real storage/backfiller.
#[test]
fn index_lifecycle_uses_reorg_sql_mode_and_preserves_failed_metadata() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(812)).unwrap();
    for (i, ddl) in [
        "CREATE INDEX ig ON test.reorg0(g)",
        "ALTER TABLE test.reorg1 ADD INDEX ig(g)",
        "ALTER TABLE test.reorg2 ADD INDEX ig(g), ADD COLUMN c INT",
    ]
    .into_iter()
    .enumerate()
    {
        rows(&mut session, "SET sql_mode = ''");
        rows(
            &mut session,
            &format!("CREATE TABLE test.reorg{i}(a INT,g INT AS (10 DIV a) VIRTUAL)"),
        );
        rows(
            &mut session,
            &format!("INSERT INTO test.reorg{i}(a) VALUES(0)"),
        );
        rows(
            &mut session,
            "SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
        );
        let error = session
            .execute_write(ddl)
            .expect_err("strict reorg rejects zero divisor");
        assert_eq!(error.code, 1365, "{ddl}: {error:?}");
        assert!(rows(&mut session, &format!("SHOW INDEX FROM test.reorg{i}")).is_empty());
        assert_eq!(
            rows(&mut session, &format!("SHOW COLUMNS FROM test.reorg{i}")).len(),
            2
        );
        rows(&mut session, "SET sql_mode = ''");
        rows(&mut session, ddl);
        assert!(!rows(&mut session, &format!("SHOW INDEX FROM test.reorg{i}")).is_empty());
        rows(
            &mut session,
            "SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
        );
        rows(&mut session, &format!("DROP INDEX ig ON test.reorg{i}"));
    }
}

#[test]
fn index_lifecycle_duplicate_backfill_preserves_sql_error_and_rolls_back() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(814)).unwrap();
    rows(&mut session, "CREATE TABLE test.reorg_dup(a INT)");
    rows(&mut session, "INSERT INTO test.reorg_dup VALUES(1),(1)");
    let error = session
        .execute_write("CREATE UNIQUE INDEX ia ON test.reorg_dup(a)")
        .unwrap_err();
    assert_eq!(error.code, 1062);
    assert_eq!(error.state, *b"23000");
    assert_eq!(error.message, "Duplicate entry '1' for key 'reorg_dup.ia'");
    assert!(rows(&mut session, "SHOW INDEX FROM test.reorg_dup").is_empty());
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.reorg_dup")),
        [["2"]]
    );
}

#[test]
fn index_lifecycle_backfill_uses_the_submitting_time_zone() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(813)).unwrap();
    rows(&mut session, "SET time_zone = '+08:00'");
    rows(
        &mut session,
        "CREATE TABLE test.reorg_tz(ts TIMESTAMP,g INT AS (HOUR(ts)) VIRTUAL)",
    );
    rows(
        &mut session,
        "INSERT INTO test.reorg_tz(ts) VALUES('2020-01-02 08:00:00')",
    );
    rows(&mut session, "CREATE INDEX ig ON test.reorg_tz(g)");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT g FROM test.reorg_tz FORCE INDEX(ig) WHERE g=8"
        )),
        [["8"]]
    );
}

/// Go keeps the BDR role in the meta key `BDRRole`, and refuses a DDL its
/// role denies: the submitter (`SubmitBatch`'s `IsDenied`) and the ADD /
/// MODIFY COLUMN builders. A cluster node reads and writes the key in TiKV
/// meta, and its planner admits each change it publishes the same way.
#[test]
fn cluster_bdr_role_lives_in_meta_and_refuses_denied_ddl() {
    fn run(
        session: &mut ClusterServerSession,
        sql: &str,
    ) -> Result<(), crate::sql_node::SqlQueryError> {
        if session.execute_write(sql)?.is_some() {
            return Ok(());
        }
        use crate::resultset_source::ResultSetSource;
        let mut result = session.execute(sql)?;
        let source = result.source();
        while !source.next_batch(8).expect("batch").is_empty() {}
        source.finish().expect("finish");
        source.close().expect("close");
        Ok(())
    }
    const DENIED: &str =
        "The operation is not allowed while the bdr role of this cluster is set to primary.";
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(931)).unwrap();
    run(&mut session, "create table test.bdr_t(a int)").unwrap();
    run(&mut session, "admin set bdr role primary").unwrap();
    // A second connection reads the role from meta, not from the first
    // connection's catalog.
    let mut other = stack.factory.open_session(session_context(932)).unwrap();
    assert_eq!(
        displayed(rows(&mut other, "admin show bdr role")),
        [["primary"]]
    );
    run(&mut session, "alter table test.bdr_t add column b int").unwrap();
    for sql in [
        "alter table test.bdr_t drop column b",
        "alter table test.bdr_t add column c int not null",
        "alter table test.bdr_t modify column a bigint",
        "create unique index u on test.bdr_t(a)",
        "truncate table test.bdr_t",
    ] {
        let error = run(&mut session, sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (8263, DENIED),
            "{sql}"
        );
    }
    run(&mut session, "create index i on test.bdr_t(a)").unwrap();
    run(
        &mut session,
        "alter table test.bdr_t modify column a int default 1",
    )
    .unwrap();
    // TiCDC's own DDL skips the submitter's admission.
    run(&mut session, "set @@tidb_cdc_write_source = 1").unwrap();
    run(&mut session, "alter table test.bdr_t drop column b").unwrap();
    run(&mut session, "set @@tidb_cdc_write_source = 0").unwrap();
    run(&mut session, "admin unset bdr role").unwrap();
    assert_eq!(displayed(rows(&mut other, "admin show bdr role")), [[""]]);
    run(&mut session, "truncate table test.bdr_t").unwrap();
    run(&mut session, "drop table test.bdr_t").unwrap();
}

/// Go `validateCommentLength` and `checkTooBigFieldLengthAndTryAutoConvert`,
/// from `ddl/db.test`: strict mode refuses an over-long table (1628), field
/// (1629) or index (1688) comment; otherwise the statement warns and keeps
/// the first bytes. The warnings a plan-time build raises (ADD COLUMN, ADD
/// INDEX, MODIFY) reach `SHOW WARNINGS` as Go's do. A GLOBAL temporary
/// table's allocator starts at its `AUTO_INCREMENT` (Go
/// `NewAllocatorFromTempTblInfo`). This node accepted every long comment
/// silently, answered ALTER TABLE COMMENT with 1105, dropped plan-time
/// warnings and started the temporary table at 1.
#[test]
fn comment_lengths_and_plan_time_warnings_follow_the_sql_mode_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(995)).unwrap();
    let field = "b".repeat(1025);
    let table = "b".repeat(2049);
    rows(&mut session, "CREATE TABLE test.t (c INT, d INT, e INT)");
    for (sql, code, message) in [
        (
            format!("CREATE TABLE test.t1 (c INT) COMMENT = '{table}'"),
            1628,
            "Comment for table 't1' is too long (max = 2048)",
        ),
        (
            format!("CREATE TABLE test.t1 (c INT COMMENT '{field}')"),
            1629,
            "Comment for field 'c' is too long (max = 1024)",
        ),
        (
            format!("CREATE TABLE test.t1 (c INT, KEY (c) COMMENT '{field}')"),
            1688,
            "Comment for index 'c' is too long (max = 1024)",
        ),
        (
            format!("ALTER TABLE test.t ADD COLUMN c1 INT COMMENT '{field}'"),
            1629,
            "Comment for field 'c1' is too long (max = 1024)",
        ),
        (
            format!("ALTER TABLE test.t ADD KEY (e) COMMENT '{field}'"),
            1688,
            "Comment for index 'e' is too long (max = 1024)",
        ),
        (
            format!("ALTER TABLE test.t COMMENT = '{table}'"),
            1628,
            "Comment for table 't' is too long (max = 2048)",
        ),
    ] {
        let error = session.execute_write(&sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message));
    }
    rows(&mut session, "SET @@sql_mode = ''");
    for (sql, warning) in [
        (
            format!("CREATE TABLE test.t1 (c INT) COMMENT = '{table}'"),
            [
                "Warning",
                "1628",
                "Comment for table 't1' is too long (max = 2048)",
            ],
        ),
        (
            format!("ALTER TABLE test.t ADD COLUMN c1 INT COMMENT '{field}'"),
            [
                "Warning",
                "1629",
                "Comment for field 'c1' is too long (max = 1024)",
            ],
        ),
        (
            format!("CREATE INDEX i1 ON test.t (d) COMMENT '{field}'"),
            [
                "Warning",
                "1688",
                "Comment for index 'i1' is too long (max = 1024)",
            ],
        ),
        (
            "CREATE TABLE test.v (a VARCHAR(200))".to_owned(),
            ["", "", ""],
        ),
        (
            "ALTER TABLE test.v MODIFY a VARCHAR(70000000)".to_owned(),
            [
                "Warning",
                "1246",
                "Converting column 'a' from VARCHAR to TEXT",
            ],
        ),
    ] {
        session.execute_write(&sql).unwrap();
        let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
        if warning[0].is_empty() {
            assert!(warnings.is_empty(), "{sql}: {warnings:?}");
        } else {
            assert_eq!(warnings, [warning], "{sql}");
        }
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT length(table_comment) FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 't1'"
        )),
        [["2048"]]
    );
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE test.g (id INT PRIMARY KEY AUTO_INCREMENT) \
         AUTO_INCREMENT = 100 ON COMMIT DELETE ROWS",
    );
    rows(&mut session, "BEGIN");
    rows(&mut session, "INSERT INTO test.g VALUES (NULL)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.g")),
        [["100"]]
    );
    rows(&mut session, "COMMIT");
}

/// Go's multi-schema change from `ddl/multi_schema_change.test`: the index,
/// default and table-option sub-jobs (`fillMultiSchemaInfo`), a MODIFY whose
/// rows change inside the bundle, a drop's `len(Columns)` check seeing the
/// columns other sub-jobs add, a later AUTO_INCREMENT sub-job adjusted
/// against the counter the earlier one left (a note), ADD PRIMARY KEY's NOT
/// NULL key columns, and an expression index's columns counted as relative.
/// This node refused every bundle with one of those sub-jobs.
#[test]
fn multi_schema_change_sub_jobs_run_as_go_runs_them() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(996)).unwrap();
    rows(&mut session, "CREATE TABLE test.t1 (a INT, b INT)");
    rows(&mut session, "INSERT INTO test.t1 VALUES (1, 2)");
    session
        .execute_write("ALTER TABLE test.t1 DROP COLUMN a, DROP COLUMN b, ADD COLUMN c INT DEFAULT 3, ADD COLUMN d INT DEFAULT 4")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM test.t1")),
        [["3", "4"]]
    );

    rows(
        &mut session,
        "CREATE TABLE test.t2 (a INT, b INT, c INT, PRIMARY KEY (a) NONCLUSTERED, KEY i3 (b), KEY i (c))",
    );
    session
        .execute_write("ALTER TABLE test.t2 DROP INDEX i3, DROP PRIMARY KEY, RENAME INDEX i TO i1, CHANGE COLUMN c d INT")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT key_name, column_name FROM information_schema.tidb_indexes \
             WHERE table_schema = 'test' AND table_name = 't2'"
        )),
        [["i1", "d"]]
    );

    rows(
        &mut session,
        "CREATE TABLE test.t3 (a INT, b INT, c INT, KEY ib (b))",
    );
    rows(
        &mut session,
        "INSERT INTO test.t3 VALUES (1, 2, 3), (11, 22, 33)",
    );
    session
        .execute_write("ALTER TABLE test.t3 MODIFY COLUMN a TINYINT AFTER c, MODIFY COLUMN b TINYINT, MODIFY COLUMN c TINYINT FIRST")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.t3 USE INDEX (ib) ORDER BY b"
        )),
        [["3", "2", "1"], ["33", "22", "11"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.t3");

    rows(
        &mut session,
        "CREATE TABLE test.t4 (a INT AUTO_INCREMENT PRIMARY KEY, b INT)",
    );
    session
        .execute_write("ALTER TABLE test.t4 AUTO_INCREMENT = 110, AUTO_INCREMENT = 90")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Note",
            "1105",
            "Can't reset AUTO_INCREMENT to 90 without FORCE option, using 110 instead"
        ]]
    );
    session
        .execute_write("ALTER TABLE test.t4 MODIFY COLUMN b TINYINT, AUTO_INCREMENT = 200, COMMENT = 'abc', CHARSET = utf8mb4")
        .unwrap();
    rows(&mut session, "INSERT INTO test.t4 (b) VALUES (1)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.t4")),
        [["200"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT table_comment FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 't4'"
        )),
        [["abc"]]
    );

    rows(&mut session, "CREATE TABLE test.t5 (a INT)");
    session
        .execute_write("ALTER TABLE test.t5 ADD INDEX i (a), ADD PRIMARY KEY (a)")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t5"));
    assert!(
        created[0][1].contains("`a` int NOT NULL"),
        "{}",
        created[0][1]
    );

    rows(&mut session, "CREATE TABLE test.t6 (a INT, b INT)");
    let error = session
        .execute_write("ALTER TABLE test.t6 MODIFY COLUMN b DOUBLE, ADD INDEX idx ((a + b))")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8200, "Unsupported operate same column 'b'")
    );
}

/// Go `handleTableOptions`' SHARD_ROW_ID_BITS / PRE_SPLIT_REGIONS, the
/// session's `tidb_shard_row_id_bits` a heap CREATE TABLE takes first, and
/// `ShardRowID` / `onShardRowID` for ALTER TABLE. This node refused the
/// options and the ALTER, and a multi-schema change never reported its
/// sub-job types (`SubActionTypes`).
#[test]
fn shard_row_id_bits_create_alter_and_session_default_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(997)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.s1 (a INT) SHARD_ROW_ID_BITS = 20 PRE_SPLIT_REGIONS = 30",
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.s1"));
    assert!(
        created[0][1].ends_with("/*T! SHARD_ROW_ID_BITS=15 PRE_SPLIT_REGIONS=15 */"),
        "{}",
        created[0][1]
    );
    let sharding = |session: &mut _, table: &str| {
        displayed(rows(
            session,
            &format!(
                "SELECT tidb_row_id_sharding_info FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = '{table}'"
            ),
        ))
    };
    session
        .execute_write("ALTER TABLE test.s1 SHARD_ROW_ID_BITS = 4")
        .unwrap();
    assert_eq!(sharding(&mut session, "s1"), [["SHARD_BITS=4"]]);

    rows(&mut session, "SET @@tidb_shard_row_id_bits = 3");
    rows(&mut session, "CREATE TABLE test.s2 (a INT)");
    rows(
        &mut session,
        "CREATE TABLE test.s3 (a INT PRIMARY KEY CLUSTERED)",
    );
    assert_eq!(sharding(&mut session, "s2"), [["SHARD_BITS=3"]]);
    assert_eq!(
        sharding(&mut session, "s3"),
        [["NOT_SHARDED(PK_IS_HANDLE)"]]
    );
    rows(&mut session, "SET @@tidb_shard_row_id_bits = 0");

    for (sql, code, message) in [
        (
            "CREATE TABLE test.s4 (a INT PRIMARY KEY CLUSTERED) SHARD_ROW_ID_BITS = 2",
            8200,
            "Unsupported shard_row_id_bits for table with primary key as row id",
        ),
        (
            "ALTER TABLE test.s3 SHARD_ROW_ID_BITS = 2",
            8200,
            "Unsupported shard_row_id_bits for table with primary key as row id",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE test.g (a INT) ON COMMIT DELETE ROWS",
    );
    let error = session
        .execute_write("ALTER TABLE test.g SHARD_ROW_ID_BITS = 4")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8006,
            "`shard_row_id_bits` is unsupported on temporary tables."
        )
    );
}

/// Go `CreateSequence` / `onCreateSequence`, `AlterSequence` /
/// `onAlterSequence` and `dropTableObject` for sequences, from
/// `ddl/sequence.test`: the counter is seeded one step outside START, an ALTER
/// discards the reserved batch and RESTART moves the counter, a DROP reports
/// a missing name (4139) once the list ends and a non-sequence (1347), and
/// TRUNCATE reads a sequence as missing (1146). This node refused every
/// sequence statement.
#[test]
fn sequence_ddl_runs_as_go_runs_it() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(998)).unwrap();
    session
        .execute_write("CREATE SEQUENCE test.s START 10 INCREMENT 2 CACHE 3 COMMENT 'c'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT nextval(test.s), nextval(test.s)"
        )),
        [["10", "12"]]
    );
    session
        .execute_write("CREATE SEQUENCE IF NOT EXISTS test.s")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "1050", "Table 'test.s' already exists"]]
    );
    session
        .execute_write("ALTER SEQUENCE test.s INCREMENT 5")
        .unwrap();
    // Seeded from the discarded batch's end, 14 (oracle).
    assert_eq!(
        displayed(rows(&mut session, "SELECT nextval(test.s)")),
        [["15"]]
    );
    session
        .execute_write("ALTER SEQUENCE test.s RESTART WITH 100")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT nextval(test.s)")),
        [["100"]]
    );
    let created = displayed(rows(&mut session, "SHOW CREATE SEQUENCE test.s"));
    assert_eq!(
        created[0][1],
        "CREATE SEQUENCE `s` start with 10 minvalue 1 maxvalue 9223372036854775806 \
         increment by 5 cache 3 nocycle ENGINE=InnoDB COMMENT='c'"
    );
    let error = session
        .execute_write("ALTER SEQUENCE test.s MINVALUE 50")
        .unwrap_err();
    assert_eq!(error.code, 4136, "{}", error.message);

    rows(&mut session, "CREATE TABLE test.t (a INT)");
    let error = session.execute_write("TRUNCATE TABLE test.s").unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1146, "Table 'test.s' doesn't exist")
    );
    let error = session.execute_write("DROP SEQUENCE test.t").unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1347, "'test.t' is not SEQUENCE")
    );
    let error = session
        .execute_write("DROP SEQUENCE test.s, test.nosuch")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (4139, "Unknown SEQUENCE: 'test.nosuch'")
    );
    session
        .execute_write("DROP SEQUENCE IF EXISTS test.s, test.t")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [
            ["Note", "1347", "'test.t' is not SEQUENCE"],
            ["Note", "4139", "Unknown SEQUENCE: 'test.s'"]
        ]
    );
}

/// Go `columnDefToCol` records a generated column's text and dependences,
/// and `checkGeneratedColumn` resolves every expression against the whole
/// table once it is built: a generated column reads a LATER ordinary column,
/// and the AUTO_INCREMENT column only when allowed (3109, also ADD COLUMN's
/// `checkAutoIncrementRef`). `BuildIndexInfo` refuses an index name over 64
/// characters (1059). This node resolved against the preceding columns only
/// (1054), and checked neither.
#[test]
fn generated_columns_resolve_against_the_whole_table_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(999)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.g (gc1 INT AS (r + 1), gc2 INT AS (r + 1) STORED, r INT)",
    );
    rows(&mut session, "INSERT INTO test.g (r) VALUES (1)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM test.g")),
        [["2", "2", "1"]]
    );
    for (sql, code, message) in [
        (
            "CREATE TABLE test.a1 (a BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT, b BIGINT AS (a + 1))"
                .to_owned(),
            3109,
            "Generated column 'b' cannot refer to auto-increment column.".to_owned(),
        ),
        (
            format!("CREATE TABLE test.a2 (c INT, INDEX {} (c))", "d".repeat(65)),
            1059,
            format!("Identifier name '{}' is too long", "d".repeat(65)),
        ),
    ] {
        let error = session.execute_write(&sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message.as_str()), "{sql}");
    }
    rows(
        &mut session,
        "CREATE TABLE test.a3 (a BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT, b INT)",
    );
    let error = session
        .execute_write("ALTER TABLE test.a3 ADD COLUMN d BIGINT GENERATED ALWAYS AS (a + 1)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3109,
            "Generated column 'd' cannot refer to auto-increment column."
        )
    );
}

/// Go's TTL table options: `handleTableOptions` and `checkTTLInfoValid` at
/// CREATE, `AlterTableTTLInfoOrEnable` / `onTTLInfoChange` keeping the stored
/// enable flag and interval a new `TTL=` leaves unset, `onTTLInfoRemove`, and
/// the multi-schema refusal (8200). This node refused every TTL option.
#[test]
fn ttl_table_options_run_as_go_runs_them() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1000)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t (created_at DATETIME, updated_at DATE, n INT) \
         TTL = `created_at` + INTERVAL 5 DAY TTL_ENABLE = 'OFF'",
    );
    session
        .execute_write("ALTER TABLE test.t TTL_JOB_INTERVAL = '2h'")
        .unwrap();
    session
        .execute_write("ALTER TABLE test.t TTL = `updated_at` + INTERVAL 1 YEAR")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].ends_with(
            "/*T![ttl] TTL=`updated_at` + INTERVAL 1 YEAR */ /*T![ttl] TTL_ENABLE='OFF' */ \
             /*T![ttl] TTL_JOB_INTERVAL='2h' */"
        ),
        "{}",
        created[0][1]
    );
    for (sql, code, message) in [
        (
            "ALTER TABLE test.t TTL = `n` + INTERVAL 1 YEAR",
            8148,
            "Field 'n' is of a not supported type for TTL config, expect DATETIME, DATE or TIMESTAMP",
        ),
        (
            "ALTER TABLE test.t TTL = `nosuch` + INTERVAL 1 YEAR",
            1054,
            "Unknown column 'nosuch' in 'TTL config'",
        ),
        (
            "ALTER TABLE test.t TTL_ENABLE = 'ON' REMOVE TTL",
            8200,
            "Unsupported multi schema change for alter table ttl",
        ),
        (
            "CREATE TABLE test.p (id1 INT, id2 DOUBLE, t TIMESTAMP, PRIMARY KEY (id1, id2) CLUSTERED) \
             TTL = `t` + INTERVAL 1 DAY",
            8153,
            "Unsupported clustered primary key type FLOAT/DOUBLE for TTL",
        ),
        (
            "CREATE TABLE test.q (id INT) TTL_JOB_INTERVAL = '1h'",
            8150,
            "Cannot set TTL_JOB_INTERVAL on a table without TTL config",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
    }
    session
        .execute_write("ALTER TABLE test.t REMOVE TTL")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(!created[0][1].contains("TTL"), "{}", created[0][1]);
    let error = session
        .execute_write("ALTER TABLE test.t TTL_ENABLE = 'ON'")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8150, "Cannot set TTL_ENABLE on a table without TTL config")
    );
}

/// Go `corColInFilter`: a reader whose pushed conditions read a correlated
/// column re-describes them on every `Open`, so each Apply iteration sends its
/// own binding. Constant propagation turns `c3 = 1 and c3 = t2.c3` into a
/// pushed `eq(t2.c3, 1)`; this node described it once, while the binding was
/// still NULL, and every probe counted nothing
/// (`planner/core/casetest/index/index`).
#[test]
fn a_pushed_correlated_condition_carries_each_iterations_binding() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1001)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t1 (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1), KEY (c2))",
    );
    rows(
        &mut session,
        "INSERT INTO test.t1 VALUES (1, 1, 1), (2, 2, 2)",
    );
    rows(
        &mut session,
        "CREATE TABLE test.t2 (c1 INT, c2 INT, c3 INT)",
    );
    rows(
        &mut session,
        "INSERT INTO test.t2 VALUES (1, 1, 1), (2, 2, 2)",
    );
    for sql in [
        "SELECT (SELECT count(*) FROM test.t1 WHERE c2 = 1 AND c2 = t2.c3) FROM test.t2 ORDER BY c1",
        "SELECT (SELECT count(*) FROM test.t1 WHERE c3 = 1 AND c3 = t2.c3) FROM test.t2 ORDER BY c1",
        "SELECT (SELECT count(*) FROM test.t1 USE INDEX (PRIMARY) WHERE c3 = 1 AND c3 = t2.c3) \
         FROM test.t2 ORDER BY c1",
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), [["1"], ["0"]], "{sql}");
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.t2 WHERE c1 < ALL(SELECT /*+ use_index_merge(t1) */ c1 FROM test.t1 \
             WHERE (c1 = 10 AND c1 = t2.c3 OR c2 = 1 AND c2 = t2.c3) AND reverse(c3)) ORDER BY c1"
        )),
        [["2", "2", "2"]]
    );
}

/// Go `onReorganizePartition` on the production node: REORGANIZE PARTITION,
/// `PARTITION BY` on an existing table and REMOVE PARTITIONING move every row
/// to the physical table the new definitions route it to, rebuild the
/// indexes, and give the table its `NewTableID` where Go does. This node
/// refused all three.
#[test]
fn partition_reorganizations_move_rows_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1002)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.r (a INT, b INT, KEY ib (b)) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    rows(
        &mut session,
        "INSERT INTO test.r VALUES (1, 1), (5, 5), (15, 15)",
    );
    session
        .execute_write(
            "ALTER TABLE test.r REORGANIZE PARTITION p0 INTO \
             (PARTITION pa VALUES LESS THAN (4), PARTITION pb VALUES LESS THAN (10))",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "1105",
            "The statistics of related partitions will be outdated after reorganizing \
             partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"
        ]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.r PARTITION (pb)")),
        [["5"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.r USE INDEX (ib) WHERE b > 0 ORDER BY a"
        )),
        [["1"], ["5"], ["15"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.r");

    rows(
        &mut session,
        "CREATE TABLE test.h (a INT PRIMARY KEY, b INT, KEY ib (b))",
    );
    rows(
        &mut session,
        "INSERT INTO test.h VALUES (1, 1), (2, 2), (3, 3), (4, 4)",
    );
    session
        .execute_write("ALTER TABLE test.h PARTITION BY HASH (a) PARTITIONS 2")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.h PARTITION (p1) ORDER BY a"
        )),
        [["1"], ["3"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");
    session
        .execute_write("ALTER TABLE test.h REMOVE PARTITIONING")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.h"));
    assert!(!created[0][1].contains("PARTITION"), "{}", created[0][1]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.h USE INDEX (ib) WHERE b > 0 ORDER BY a"
        )),
        [["1"], ["2"], ["3"], ["4"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");
}

/// Go `hashPartitionManagement`: on a HASH table ADD PARTITION and COALESCE
/// PARTITION reorganize every partition, so each row is re-hashed into the
/// new count; and INTERVAL maintenance (LAST / FIRST PARTITION LESS THAN) is
/// an ADD / DROP PARTITION job. This node appended a definition without
/// moving a row, and refused the interval statements.
#[test]
fn hash_partition_management_rehashes_rows_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1003)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.h (a INT, b INT, KEY ib (b)) PARTITION BY HASH (a) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.h VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)",
    );
    session
        .execute_write("ALTER TABLE test.h ADD PARTITION PARTITIONS 1")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.h PARTITION (p2) ORDER BY a"
        )),
        [["2"], ["5"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");
    session
        .execute_write("ALTER TABLE test.h COALESCE PARTITION 2")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.h PARTITION (p0)"
        )),
        [["6"]]
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.h"));
    assert!(
        created[0][1].ends_with("PARTITION BY HASH (`a`) PARTITIONS 1"),
        "{}",
        created[0][1]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");

    rows(
        &mut session,
        "CREATE TABLE test.iv (a INT) PARTITION BY RANGE (a) INTERVAL (10) \
         FIRST PARTITION LESS THAN (10) LAST PARTITION LESS THAN (30)",
    );
    session
        .execute_write("ALTER TABLE test.iv LAST PARTITION LESS THAN (50)")
        .unwrap();
    session
        .execute_write("ALTER TABLE test.iv FIRST PARTITION LESS THAN (20)")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT partition_name FROM information_schema.partitions \
             WHERE table_schema = 'test' AND table_name = 'iv' ORDER BY partition_ordinal_position"
        )),
        [["P_LT_20"], ["P_LT_30"], ["P_LT_40"], ["P_LT_50"]]
    );
}

/// Go `checkExchangePartitionRecordValidation` applies CHECK constraints only
/// while `tidb_enable_check_constraint` is on; and a heap row whose
/// `_tidb_rowid` collides when partitions collapse takes a fresh one from
/// the table's counter, which then moves to the new table id
/// (`onReorganizePartition`). This node validated the constraints with the
/// switch off (1737) and gave the colliding row a duplicate id.
#[test]
fn exchange_and_collapse_honour_go_switches_and_counters() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1004)).unwrap();
    rows(
        &mut session,
        "SET @@global.tidb_enable_check_constraint = 1",
    );
    rows(
        &mut session,
        "CREATE TABLE test.nt (a INT CHECK (a > 75) NOT ENFORCED, b INT CHECK (b > 50) ENFORCED)",
    );
    rows(&mut session, "INSERT INTO test.nt VALUES (80, 60)");
    rows(
        &mut session,
        "CREATE TABLE test.pt (a INT CHECK (a < 75) ENFORCED, b INT CHECK (b < 75 OR b > 100) ENFORCED) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (50), PARTITION p1 VALUES LESS THAN (100))",
    );
    let error = session
        .execute_write("ALTER TABLE test.pt EXCHANGE PARTITION p1 WITH TABLE test.nt")
        .unwrap_err();
    assert_eq!(error.code, 1737, "{}", error.message);
    rows(
        &mut session,
        "SET @@global.tidb_enable_check_constraint = 0",
    );
    session
        .execute_write("ALTER TABLE test.pt EXCHANGE PARTITION p1 WITH TABLE test.nt")
        .unwrap();

    rows(
        &mut session,
        "CREATE TABLE test.t (id INT NOT NULL, store_id INT NOT NULL) PARTITION BY RANGE (store_id) \
         (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), \
          PARTITION p2 VALUES LESS THAN (16), PARTITION p3 VALUES LESS THAN (21))",
    );
    rows(
        &mut session,
        "CREATE TABLE test.t1 (id INT NOT NULL, store_id INT NOT NULL)",
    );
    rows(&mut session, "INSERT INTO test.t VALUES (1, 1)");
    rows(&mut session, "INSERT INTO test.t VALUES (2, 17)");
    rows(&mut session, "INSERT INTO test.t1 VALUES (0, 18)");
    session
        .execute_write("ALTER TABLE test.t EXCHANGE PARTITION p3 WITH TABLE test.t1")
        .unwrap();
    session
        .execute_write("ALTER TABLE test.t REMOVE PARTITIONING")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT *, _tidb_rowid FROM test.t ORDER BY id"
        )),
        [["0", "18", "30001"], ["1", "1", "1"]]
    );
    rows(&mut session, "INSERT INTO test.t VALUES (5, 5)");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT _tidb_rowid FROM test.t WHERE id = 5"
        )),
        [["60001"]]
    );
}

/// Go `AlterTablePlacement` / `onAlterTablePlacement`: the table takes the
/// policy (or loses it under DEFAULT), an unknown one is 8239 and a
/// temporary table 8006. This node refused the statement.
#[test]
fn alter_table_placement_policy_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1005)).unwrap();
    rows(&mut session, "CREATE PLACEMENT POLICY p1 FOLLOWERS = 1");
    rows(&mut session, "CREATE TABLE test.t (a INT)");
    session
        .execute_write("ALTER TABLE test.t PLACEMENT POLICY p1")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].ends_with("/*T![placement] PLACEMENT POLICY=`p1` */"),
        "{}",
        created[0][1]
    );
    session
        .execute_write("ALTER TABLE test.t PLACEMENT POLICY DEFAULT")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(!created[0][1].contains("PLACEMENT"), "{}", created[0][1]);
    for (sql, code, message) in [
        (
            "ALTER TABLE test.t PLACEMENT POLICY nosuch",
            8239,
            "Unknown placement policy 'nosuch'",
        ),
        (
            "ALTER TABLE test.g PLACEMENT POLICY p1",
            8006,
            "`placement` is unsupported on temporary tables.",
        ),
    ] {
        if sql.contains("test.g") {
            rows(
                &mut session,
                "CREATE GLOBAL TEMPORARY TABLE test.g (a INT) ON COMMIT DELETE ROWS",
            );
        }
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
}

/// Go `isIntCol` admits only the five integer types, so a `YEAR` primary key
/// is never the INT row handle: under the default clustered mode it is a
/// common handle. This node made it the int handle, which the record-range
/// encoder does not accept, and every full scan read nothing
/// (TestIssue20768).
#[test]
fn year_primary_key_is_not_the_row_handle_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1006)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t1 (a YEAR, PRIMARY KEY (a))",
    );
    session
        .execute_write("INSERT IGNORE INTO test.t1 VALUES (NULL)")
        .unwrap();
    session
        .execute_write("INSERT INTO test.t1 VALUES (2001)")
        .unwrap();
    rows(&mut session, "CREATE TABLE test.t2 (a INT, KEY (a))");
    session
        .execute_write("INSERT INTO test.t2 VALUES (0)")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT HEX(a), a + 0 FROM test.t1 ORDER BY a"
        )),
        [["0", "0"], ["7D1", "2001"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM test.t1")),
        [["2"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ HASH_JOIN(t1) */ t1.a + 0, t2.a FROM test.t1 JOIN test.t2 ON t1.a = t2.a"
        )),
        [["0", "0"]]
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t1"));
    assert!(
        created[0][1].contains("PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */"),
        "{}",
        created[0][1]
    );
}

/// Go builds a fresh `TxnCtx` for every autocommit statement and at every
/// BEGIN / COMMIT / ROLLBACK, so the rows of an `ON COMMIT DELETE ROWS` table
/// live exactly as long as the transaction that wrote them. This node kept
/// them across autocommit statements.
#[test]
fn global_temporary_rows_end_with_their_transaction_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1007)).unwrap();
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE test.g (a INT) ON COMMIT DELETE ROWS",
    );
    session
        .execute_write("INSERT INTO test.g VALUES (1)")
        .unwrap();
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
    session.control_transaction("BEGIN").unwrap();
    session
        .execute_write("INSERT INTO test.g VALUES (2)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.g")),
        [["2"]]
    );
    session.control_transaction("COMMIT").unwrap();
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
    // A BEGIN the query path runs opens the session's own transaction, which
    // the next statement continues into just the same.
    rows(&mut session, "BEGIN");
    session
        .execute_write("INSERT INTO test.g VALUES (3)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.g")),
        [["3"]]
    );
    rows(&mut session, "COMMIT");
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
}

/// Go keeps hypothetical indexes and TiFlash replicas in `SessionVars`
/// (`addHypoIndexIntoCtx`, `dropHypoIndexFromCtx`, `setHypoTiFlashReplica`):
/// only EXPLAIN plans them and SHOW CREATE TABLE prints the indexes. This
/// node stored them as real indexes.
#[test]
fn hypothetical_indexes_are_session_state_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1008)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t (a INT, b INT, c INT, KEY (a))",
    );
    session
        .execute_write("CREATE INDEX hypo_bc TYPE HYPO ON test.t (b, c)")
        .unwrap();
    session
        .execute_write("CREATE UNIQUE INDEX hypo_c TYPE HYPO ON test.t (c)")
        .unwrap();
    let error = session
        .execute_write("CREATE INDEX hypo_bc TYPE HYPO ON test.t (b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1105, "conflict hypo index name hypo_bc")
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].contains(
            "  KEY `a` (`a`),\n  KEY `hypo_bc` (`b`,`c`) /* HYPO INDEX */,\n  \
             UNIQUE KEY `hypo_c` (`c`) /* HYPO INDEX */\n"
        ),
        "{}",
        created[0][1]
    );
    let explain = |session: &mut ClusterServerSession, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[3].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        explain(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT b FROM test.t WHERE b = 1 AND c = 2"
        )
        .last()
        .unwrap(),
        "table:t, index:hypo_bc(b, c)"
    );
    assert_eq!(
        explain(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT c FROM test.t WHERE c = 1"
        ),
        ["table:t, index:hypo_c(c)"]
    );
    // Only EXPLAIN sees them: the table has no such index to read.
    assert!(rows(&mut session, "SELECT c FROM test.t WHERE c = 1").is_empty());
    session
        .execute_write("DROP HYPO INDEX hypo_bc ON test.t")
        .unwrap();
    let error = session
        .execute_write("DROP HYPO INDEX hypo_bc ON test.t")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1091, "index hypo_bc doesn't exist")
    );
    session
        .execute_write("DROP HYPO INDEX IF EXISTS hypo_bc ON test.t")
        .unwrap();
    session
        .execute_write("ALTER TABLE test.t SET HYPO TIFLASH REPLICA 1")
        .unwrap();
    assert!(
        explain(&mut session, "EXPLAIN FORMAT='brief' SELECT b FROM test.t")
            .iter()
            .any(|object| object == "table:t")
    );
    assert!(displayed(rows(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT b FROM test.t"
    ))
    .iter()
    .any(|row| row[2] == "mpp[tiflash]"));
    session
        .execute_write("ALTER TABLE test.t SET HYPO TIFLASH REPLICA 0")
        .unwrap();
    assert!(!displayed(rows(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT b FROM test.t"
    ))
    .iter()
    .any(|row| row[2] == "mpp[tiflash]"));
    // Session state: another session sees none of it.
    let mut other = stack.factory.open_session(session_context(1009)).unwrap();
    let created = displayed(rows(&mut other, "SHOW CREATE TABLE test.t"));
    assert!(!created[0][1].contains("HYPO"), "{}", created[0][1]);
}

/// Two expression indexes on one expression own different hidden columns,
/// and Go's `EqualByExprAndID` treats them as one by `FieldType.Equal`,
/// which ignores key flags. Go stamps no key flag on a hidden column
/// (`setColumnFlagWithConstraint` skips expression parts), and pushes an
/// index filter on one through `PushDownExprs` alone. This node stamped
/// `MultipleKeyFlag`, compared flags, and kept the filter at root.
#[test]
fn expression_indexes_on_one_expression_cover_each_other_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1010)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t1 (a VARCHAR(10), b VARCHAR(10), KEY e1 ((lower(a))), \
         KEY e2 ((lower(a))), KEY idx ((lower(a)), a), KEY idx1 (a, (lower(a))), \
         KEY idx2 (a, (lower(a)), b))",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM test.t1 WHERE lower(a) LIKE \"aadwa\" AND a = \"a\""
        )),
        [
            ["IndexReader", "1.00", "root", "", "index:Projection"],
            ["└─Projection", "1.00", "cop[tikv]", "", "test.t1.a, test.t1.b"],
            [
                "  └─Selection",
                "1.00",
                "cop[tikv]",
                "",
                "like(lower(test.t1.a), \"aadwa\", 92)"
            ],
            [
                "    └─IndexRangeScan",
                "1.25",
                "cop[tikv]",
                "table:t1, index:idx2(a, lower(`a`), b)",
                "range:[\"a\" \"aadwa\",\"a\" \"aadwa\"], keep order:false, stats:pseudo"
            ],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT column_name, column_key FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 't1' ORDER BY ordinal_position"
        )),
        [["a", "MUL"], ["b", ""]]
    );
}

/// Go `handleTableOptions` records COMPRESSION and ignores every option
/// outside its switch, and the parser's "parsed but ignored" warning reaches
/// SHOW WARNINGS after the statement's own (`clientConn.handleStmt`). This
/// node refused the options, and its routed DDL reset the warning buffer a
/// second time, dropping the parser's warnings.
#[test]
fn ignored_table_options_warn_through_the_parser_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1011)).unwrap();
    // The connection's COM_QUERY order: parse at the statement boundary,
    // then the write.
    let sql = "CREATE TABLE test.a (id INT PRIMARY KEY) PAGE_CHECKSUM=1 COMPRESSION='zlib'";
    let parsed = session.parse_statement(sql).unwrap().unwrap();
    session.execute_write_parsed(sql, &parsed).unwrap();
    let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0][..2], ["Warning", "1064"]);
    assert!(
        warnings[0][2]
            .contains("The PAGE_CHECKSUM option is parsed but ignored by all storage engines."),
        "{warnings:?}"
    );
    let sql = "CREATE TABLE test.b (a INT)";
    let parsed = session.parse_statement(sql).unwrap().unwrap();
    session.execute_write_parsed(sql, &parsed).unwrap();
    assert!(rows(&mut session, "SHOW WARNINGS").is_empty());
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.a"));
    assert!(
        created[0][1].ends_with(" COMPRESSION='zlib'"),
        "{}",
        created[0][1]
    );
}

/// Go's `kv.CommitHook` records the cluster transaction's own start and
/// commit timestamps as `@@tidb_last_txn_info`, so a stale read AS OF that
/// commit_ts sees the commit. This node reported timestamps from a local
/// counter that no transaction committed at.
#[test]
fn last_txn_info_reports_the_cluster_commit_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1012)).unwrap();
    rows(&mut session, "CREATE TABLE test.t1 (a INT)");
    session
        .execute_write("INSERT INTO test.t1 VALUES (1)")
        .unwrap();
    session
        .execute_write("INSERT INTO test.t1 VALUES (2)")
        .unwrap();
    rows(
        &mut session,
        "SET @c = json_extract(@@tidb_last_txn_info, '$.commit_ts')",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.t1 AS OF TIMESTAMP @c"
        )),
        [["2"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.t1 AS OF TIMESTAMP cast(@c AS UNSIGNED) - 1"
        )),
        [["1"]]
    );
    session.control_transaction("BEGIN").unwrap();
    session
        .execute_write("INSERT INTO test.t1 VALUES (3)")
        .unwrap();
    session.control_transaction("COMMIT").unwrap();
    let info = displayed(rows(
        &mut session,
        "SELECT json_extract(@@tidb_last_txn_info, '$.start_ts') < \
         json_extract(@@tidb_last_txn_info, '$.commit_ts'), \
         json_extract(@@tidb_last_txn_info, '$.txn_commit_mode')",
    ));
    assert_eq!(info[0][0], "1", "{info:?}");
    rows(
        &mut session,
        "SET @c = json_extract(@@tidb_last_txn_info, '$.commit_ts')",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.t1 AS OF TIMESTAMP @c"
        )),
        [["3"]]
    );
}

/// Go `normalizeSplitPolicy` records CREATE TABLE's and ALTER TABLE's region
/// split policies on the table (`ActionAlterTableSetRegionSplitPolicy`), SHOW
/// CREATE TABLE prints them, and a new index on a table with policies warns
/// (`checkAndWarnMissingRegionSplitPolicy`). This node stored none of it.
#[test]
fn region_split_policies_persist_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1013)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t (id BIGINT NOT NULL, v BIGINT, KEY idx_v (v), \
         PRIMARY KEY (id) NONCLUSTERED) \
         SPLIT PRIMARY KEY BETWEEN (0) AND (10000) REGIONS 5",
    );
    session
        .execute_write("ALTER TABLE test.t SPLIT INDEX idx_v BETWEEN (0) AND (100) REGIONS 3")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].ends_with(
            "/*T![region_split] SPLIT INDEX `idx_v` BETWEEN (0) AND (100) REGIONS 3 */\n\
             /*T![region_split] SPLIT PRIMARY KEY `PRIMARY` BETWEEN (0) AND (10000) REGIONS 5 */"
        ),
        "{}",
        created[0][1]
    );
    session
        .execute_write("CREATE INDEX idx_w ON test.t (v, id)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "1105",
            "It is recommended to add a region split strategy to the new index 'idx_w' to avoid write hotspots"
        ]]
    );
    rows(
        &mut session,
        "CREATE TABLE test.c (id INT PRIMARY KEY CLUSTERED)",
    );
    let error = session
        .execute_write("ALTER TABLE test.c SPLIT PRIMARY KEY BETWEEN (0) AND (10) REGIONS 2")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8267,
            "SPLIT PRIMARY is only for non-clustered table is forbidden"
        )
    );
}

/// Go `handleTableOptions` / `AlterTableAffinity` record a table's AFFINITY,
/// the DDL job creates its PD affinity groups through `pkg/domain/affinity`
/// (the mock manager on unistore) and deletes them when the affinity goes,
/// partition management on such a table is refused, and a view is not a
/// BASE TABLE. This node refused the option outright.
#[test]
fn table_affinity_and_its_groups_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1014)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t1 (a INT) AFFINITY = 'TABLE'",
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t1"));
    assert!(
        created[0][1].ends_with("/*T![affinity] AFFINITY='table' */"),
        "{}",
        created[0][1]
    );
    let table_id = displayed(rows(
        &mut session,
        "SELECT tidb_table_id FROM information_schema.tables \
         WHERE table_schema = 'test' AND table_name = 't1'",
    ))[0][0]
        .clone();
    let group = format!("_tidb_t_{table_id}");
    let held = |group: &str| {
        tidb_domain::affinity::get_groups(&[group.to_owned()])
            .unwrap()
            .contains_key(group)
    };
    assert!(held(&group));
    session
        .execute_write("ALTER TABLE test.t1 AFFINITY = ''")
        .unwrap();
    assert!(!held(&group));
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t1"));
    assert!(!created[0][1].contains("AFFINITY"), "{}", created[0][1]);
    rows(
        &mut session,
        "CREATE TABLE test.t2 (a INT) AFFINITY = 'table'",
    );
    let t2_group = format!(
        "_tidb_t_{}",
        displayed(rows(
            &mut session,
            "SELECT tidb_table_id FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 't2'",
        ))[0][0]
    );
    assert!(held(&t2_group));
    rows(&mut session, "DROP TABLE test.t2");
    assert!(!held(&t2_group));

    rows(
        &mut session,
        "CREATE TABLE test.tp (a INT) AFFINITY = 'partition' PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    for (sql, message) in [
        (
            "ALTER TABLE test.tp ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))",
            "Unsupported ADD PARTITION of a table with AFFINITY option",
        ),
        (
            "ALTER TABLE test.tp DROP PARTITION p1",
            "Unsupported DROP PARTITION of a table with AFFINITY option",
        ),
    ] {
        let error = session.execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (8200, message),
            "{sql}"
        );
    }
    let error = session
        .execute_write("ALTER TABLE test.t1 AFFINITY = 'partition'")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8266,
            "Can not set AFFINITY='partition' on a non-partition table."
        )
    );
    rows(&mut session, "CREATE VIEW test.v1 AS SELECT 1");
    let error = session
        .execute_write("ALTER TABLE test.v1 AFFINITY = 'table'")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1347, "'test.v1' is not BASE TABLE")
    );
}

/// Go `fetchShowAffinity`: a row per affinity table or partition, schema then
/// table id descending, with NULL group state where PD holds no group (always
/// on unistore, whose PD HTTP client is nil); LIKE matches the table name.
/// This node answered no rows.
#[test]
fn show_affinity_lists_affinity_tables_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1015)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.a1 (a INT) AFFINITY = 'table'",
    );
    rows(
        &mut session,
        "CREATE TABLE test.a2 (a INT) AFFINITY = 'partition' PARTITION BY HASH (a) PARTITIONS 2",
    );
    rows(&mut session, "CREATE TABLE test.plain (a INT)");
    let null = "NULL".to_owned();
    let row = |table: &str, partition: &str| {
        let mut row = vec!["test".to_owned(), table.to_owned(), partition.to_owned()];
        row.extend(std::iter::repeat_n(null.clone(), 5));
        row
    };
    assert_eq!(
        displayed(rows(&mut session, "SHOW AFFINITY")),
        [row("a2", "p0"), row("a2", "p1"), row("a1", "NULL")]
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW AFFINITY LIKE 'A1'")),
        [row("a1", "NULL")]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW AFFINITY WHERE Partition_name = 'p1'"
        )),
        [row("a2", "p1")]
    );
}

/// Go's resource-group DDL (`onCreateResourceGroup` / `onAlterResourceGroup`
/// / `onDropResourceGroup`) keeps the group in meta and in the resource
/// manager (the mock on unistore), behind the executor's existence checks.
/// This node refused the statements outright.
#[test]
fn resource_group_ddl_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1016)).unwrap();
    session
        .execute_write("CREATE RESOURCE GROUP rg1 RU_PER_SEC = 100")
        .unwrap();
    session
        .execute_write("ALTER RESOURCE GROUP rg1 RU_PER_SEC = 200")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT name, ru_per_sec FROM information_schema.resource_groups WHERE name = 'rg1'"
        )),
        [["rg1", "200"]]
    );
    let error = session
        .execute_write("CREATE RESOURCE GROUP rg1 RU_PER_SEC = 100")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8248, "Resource group 'rg1' already exists")
    );
    session
        .execute_write("CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 100")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "8248", "Resource group 'rg1' already exists"]]
    );
    session.execute_write("SET RESOURCE GROUP rg1").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT CURRENT_RESOURCE_GROUP()")),
        [["rg1"]]
    );
    session.execute_write("SET RESOURCE GROUP default").unwrap();
    session.execute_write("DROP RESOURCE GROUP rg1").unwrap();
    assert!(rows(
        &mut session,
        "SELECT name FROM information_schema.resource_groups WHERE name = 'rg1'"
    )
    .is_empty());
    let error = session
        .execute_write("DROP RESOURCE GROUP rg1")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8249, "Unknown resource group 'rg1'")
    );
}

/// The error a query raises, whether at `execute` or while its rows stream.
fn query_error(session: &mut ClusterServerSession, sql: &str) -> (u16, String) {
    use crate::resultset_source::ResultSetSource;
    let mut result = match session.execute(sql) {
        Ok(result) => result,
        Err(error) => return (error.code, error.message),
    };
    let source = result.source();
    loop {
        match source.next_batch(8) {
            Ok(batch) if batch.is_empty() => panic!("{sql} streamed every row"),
            Ok(_) => {}
            Err(error) => return (error.code, error.message),
        }
    }
}

/// Go `copIterator` sums each response's `ScanDetailV2.ProcessedVersions`
/// into the statement-wide `MaxKeysReadCounter`: a pushed scan past
/// `tidb_max_keys_read` fails with 8274, an IndexLookUp's two iterators share
/// one budget, and every processed key reaches `tidb_keys_examined`.
#[test]
fn pushed_scans_share_the_max_keys_read_budget_and_feed_keys_examined() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1017)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t_mrs (id INT PRIMARY KEY AUTO_INCREMENT, val INT, extra INT)")
        .unwrap();
    let values = (1..=20)
        .map(|v| format!("({v}, 100)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute_write(&format!("INSERT INTO t_mrs (val, extra) VALUES {values}"))
        .unwrap();
    session.execute_write("FLUSH STATUS").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM t_mrs")),
        [["20"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW STATUS LIKE 'tidb_keys_examined'")),
        [["tidb_keys_examined", "20"]]
    );
    assert_eq!(
        query_error(
            &mut session,
            "SELECT /*+ SET_VAR(tidb_max_keys_read=2) */ * FROM t_mrs"
        ),
        (8274, "tidb_max_keys_read limit exceeded".to_owned())
    );
    session
        .execute_write("CREATE INDEX idx_val ON t_mrs (val)")
        .unwrap();
    assert_eq!(
        query_error(
            &mut session,
            "SELECT /*+ SET_VAR(tidb_max_keys_read=25), USE_INDEX(t_mrs, idx_val) */ extra \
             FROM t_mrs WHERE val >= 1"
        ),
        (8274, "tidb_max_keys_read limit exceeded".to_owned())
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ SET_VAR(tidb_max_keys_read=50), USE_INDEX(t_mrs, idx_val) */ SUM(extra) \
             FROM t_mrs WHERE val >= 1"
        )),
        [["2000"]]
    );
}

/// Go's label-rule DDL on the production node: `ALTER TABLE [PARTITION]
/// ATTRIBUTES` writes the node's label manager (Go `onAlterTableAttributes`),
/// RENAME TABLE re-keys the rules to the new names, `ATTRIBUTES=DEFAULT`
/// deletes one, DROP DATABASE deletes its tables' rules (`onDropSchema`), and
/// `information_schema.attributes` reads the same manager.
#[test]
fn label_rule_ddl_writes_the_nodes_label_manager() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1018)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE t1 (a INT) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .execute_write("ALTER TABLE t1 ATTRIBUTES='key=value'")
        .unwrap();
    session
        .execute_write("ALTER TABLE t1 PARTITION p0 ATTRIBUTES='k2=v2'")
        .unwrap();
    let attributes = |session: &mut ClusterServerSession| {
        displayed(rows(
            session,
            "SELECT ID, TYPE, ATTRIBUTES FROM information_schema.attributes ORDER BY ID",
        ))
    };
    assert_eq!(
        attributes(&mut session),
        [
            ["schema/test/t1", "key-range", "\"key=value\""],
            ["schema/test/t1/p0", "key-range", "\"k2=v2\""],
        ]
    );
    session.execute_write("RENAME TABLE t1 TO t2").unwrap();
    session.execute_write("TRUNCATE TABLE t2").unwrap();
    assert_eq!(
        attributes(&mut session),
        [
            ["schema/test/t2", "key-range", "\"key=value\""],
            ["schema/test/t2/p0", "key-range", "\"k2=v2\""],
        ]
    );
    session
        .execute_write("ALTER TABLE t2 ATTRIBUTES=DEFAULT")
        .unwrap();
    assert_eq!(
        attributes(&mut session),
        [["schema/test/t2/p0", "key-range", "\"k2=v2\""]]
    );
    let error = session
        .execute_write("ALTER TABLE t2 ATTRIBUTES='nokey'")
        .unwrap_err();
    assert_eq!(error.code, 8237);
    session.execute_write("CREATE TABLE plain (a INT)").unwrap();
    let error = session
        .execute_write("ALTER TABLE plain PARTITION p0 ATTRIBUTES='k=v'")
        .unwrap_err();
    assert_eq!(error.code, 1505);

    session.execute_write("CREATE DATABASE d2").unwrap();
    session.execute_write("CREATE TABLE d2.x (a INT)").unwrap();
    session
        .execute_write("ALTER TABLE d2.x ATTRIBUTES='a=b'")
        .unwrap();
    assert_eq!(attributes(&mut session).len(), 2);
    session.execute_write("DROP DATABASE d2").unwrap();
    assert_eq!(
        tidb_executor::label_manager::label_rule_manager()
            .get_label_rules(&["schema/d2/x".to_owned()])
            .unwrap()
            .len(),
        0
    );
}

/// Go `AlterTablePartitionPlacement` / `onAlterTablePartitionPlacement` and
/// the `AlterTableOption` loop's one charset job: on the production node a
/// partition takes (and clears) a placement policy, and several CHARSET /
/// COLLATE options settle through `GetCharsetAndCollateInTableOption`.
#[test]
fn partition_placement_and_multi_option_charset_on_the_production_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1019)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE PLACEMENT POLICY pp1 FOLLOWERS=1")
        .unwrap();
    session
        .execute_write(
            "CREATE TABLE tp (a INT) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let show_create = |session: &mut ClusterServerSession, table: &str| {
        displayed(rows(session, &format!("SHOW CREATE TABLE {table}")))[0][1].clone()
    };
    session
        .execute_write("ALTER TABLE tp PARTITION p0 PLACEMENT POLICY=pp1")
        .unwrap();
    assert!(show_create(&mut session, "tp").contains(
        "PARTITION `p0` VALUES LESS THAN (10) /*T![placement] PLACEMENT POLICY=`pp1` */"
    ));
    session
        .execute_write("ALTER TABLE tp PARTITION p0 PLACEMENT POLICY=DEFAULT")
        .unwrap();
    assert!(!show_create(&mut session, "tp").contains("PLACEMENT POLICY"));
    session
        .execute_write("CREATE TABLE c (a VARCHAR(10))")
        .unwrap();
    let error = session
        .execute_write("ALTER TABLE c PARTITION p0 PLACEMENT POLICY=pp1")
        .unwrap_err();
    assert_eq!(error.code, 1505);
    let error = session
        .execute_write("ALTER TABLE tp PARTITION nope PLACEMENT POLICY=pp1")
        .unwrap_err();
    assert_eq!(error.code, 1735);

    session
        .execute_write("ALTER TABLE c CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci")
        .unwrap();
    assert!(show_create(&mut session, "c")
        .contains("DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"));
    let error = session
        .execute_write("ALTER TABLE c CHARSET latin1 CHARSET utf8mb4")
        .unwrap_err();
    assert_eq!(error.code, 1302);
}

/// Go's generated-column rules on the production node: a table whose only
/// read column is VIRTUAL still scans its rows (Go's `TableScan.Columns`
/// carries the virtual column and `FillVirtualColumnValue` computes it);
/// `CheckPKOnGeneratedColumn` refuses a virtual primary key; and MODIFY of a
/// virtual column's expression runs through `checkModifyGeneratedColumn`
/// while a change of a column a generated column reads is refused.
#[test]
fn generated_columns_read_build_and_modify_as_go_on_the_production_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1020)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE only_virtual (c0 TEXT AS ('a\\\\b'))")
        .unwrap();
    session
        .execute_write("INSERT INTO only_virtual VALUES ()")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM only_virtual")),
        [["a\\b"]]
    );

    let error = session
        .execute_write("CREATE TABLE bad_pk (a INT, b INT, c INT AS (a+b), PRIMARY KEY (c))")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3106,
            "'Defining a virtual generated column as primary key' is not supported for \
             generated columns."
        )
    );

    session
        .execute_write("CREATE TABLE g (a INT, b INT AS (a+1))")
        .unwrap();
    session
        .execute_write("INSERT INTO g (a) VALUES (1)")
        .unwrap();
    session
        .execute_write("ALTER TABLE g MODIFY b INT AS (a+2)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b FROM g")),
        [["1", "3"]]
    );
    let error = session
        .execute_write("ALTER TABLE g MODIFY b INT AS (a+2) STORED")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3106,
            "'Changing the STORED status' is not supported for generated columns."
        )
    );
    let error = session
        .execute_write("ALTER TABLE g MODIFY a BIGINT")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3106,
            "'[ddl:3108]Column 'a' has a generated column dependency.' is not supported for \
             generated columns."
        )
    );
}

/// Go `finishDDLJob` files every finished job in the meta `DDLJobHistory`
/// hash, and `information_schema.DDL_JOBS` / `ADMIN SHOW DDL JOBS` read it
/// (running jobs first): on the production node each direct DDL leaves one
/// Go-shaped job -- its schema and table, type, multi-schema sub-jobs and
/// its statement text, with Go's INTERVAL query rewrite.
#[test]
fn production_ddl_jobs_land_in_the_meta_history_as_go_files_them() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1021)).unwrap();
    session.execute_write("CREATE DATABASE hist_db").unwrap();
    session
        .execute_write("CREATE TABLE hist_db.t (a INT)")
        .unwrap();
    // Go `buildIndexColumns`: an index may not name a column the same
    // statement adds.
    let error = session
        .execute_write("ALTER TABLE hist_db.t ADD COLUMN b INT, ADD INDEX ib (b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1072, "column does not exist: b")
    );
    session
        .execute_write("ALTER TABLE hist_db.t ADD COLUMN b INT, ADD INDEX ib (a)")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT db_name, table_name, job_type FROM information_schema.DDL_JOBS LIMIT 5"
        )),
        [
            ["hist_db", "t", "alter table multi-schema change"],
            ["hist_db", "t", "add column /* subjob */"],
            ["hist_db", "t", "add index /* subjob */"],
            ["hist_db", "t", "create table"],
            ["hist_db", "", "create schema"],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT query FROM information_schema.DDL_JOBS WHERE job_type = 'create table' LIMIT 1"
        )),
        [["CREATE TABLE hist_db.t (a INT)"]]
    );
    // ADMIN SHOW DDL JOBS 1: one history job, with its sub-job rows.
    assert_eq!(rows(&mut session, "ADMIN SHOW DDL JOBS 1").len(), 3);

    session
        .execute_write(
            "CREATE TABLE hist_db.tp (id INT) PARTITION BY RANGE (id) INTERVAL (100) \
             FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (300)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT query FROM information_schema.DDL_JOBS LIMIT 1"
        )),
        [[
            "CREATE TABLE hist_db.tp (id INT) PARTITION BY RANGE (id) (PARTITION `P_LT_100` \
             VALUES LESS THAN (100),\n PARTITION `P_LT_200` VALUES LESS THAN (200),\n PARTITION \
             `P_LT_300` VALUES LESS THAN (300))"
        ]]
    );
}

/// Three wrong answers of the production engine's pushed reads:
/// - Go `scalarFuncToPBExpr` sends every function node's own `RetType`, so
///   a pushed `SUM(bigint / smallint)` casts the BIGINT to DECIMAL(20,0);
///   a catalog-default DECIMAL(11,0) clamped it to 99999999999.
/// - Go `maxMin4Decimal.AppendFinalResult2Chunk` rounds the extremum to the
///   result scale, so a division's hidden digits never show.
/// - Go decodes a clustered key column from the record key with its type
///   (`DecodeOne(..., col.Ft)`): a DATETIME key part filtered in the
///   coprocessor matched nothing while it stayed a packed integer.
#[test]
fn pushed_reads_answer_large_decimals_and_clustered_time_keys_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1022)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE td (col_bigint BIGINT, col_smallint SMALLINT)")
        .unwrap();
    session
        .execute_write(
            "INSERT INTO td VALUES (NULL, 22876), (9220557287087669248, 32767), (28030, 32767), \
             (-3309864251140603904, 32767), (4, 0), (NULL, 0), (4, -23828), (54720, 32767), \
             (0, 29815), (10017, -32661)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT AVG(col_bigint / col_smallint) FROM td"
        )),
        [["25769363061037.62077260"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT MAX(col_bigint / 3) FROM td")),
        [["3073519095695889749.3333"]]
    );

    session
        .execute_write("SET @@tidb_enable_clustered_index = ON")
        .unwrap();
    session
        .execute_write(
            "CREATE TABLE ck (id INT, logtime DATETIME, money INT, PRIMARY KEY (id, logtime))",
        )
        .unwrap();
    session
        .execute_write(
            "INSERT INTO ck VALUES (1, '2022-06-09 00:00:00', 10), (2, '2022-06-10 00:00:00', 10)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT money FROM ck WHERE logtime >= '2022-06-10 00:00:00'"
        )),
        [["10"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT SUM(money) FROM ck WHERE logtime >= '2022-06-09 00:00:00'"
        )),
        [["20"]]
    );
}

/// Go `checkGlobalIndexes` on the production CREATE TABLE: a GLOBAL index
/// needs a partitioned table (8200), and one with a prefix part warns that
/// auto analyze cannot sample it (8265); Go's ADD INDEX check reads
/// `tblInfo.Columns[col.Offset]`, where an expression part's new hidden
/// column still has `Offset` 0, so a GLOBAL expression index does not warn.
#[test]
fn global_index_rules_on_the_production_create_table() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1023)).unwrap();
    session.execute_write("USE test").unwrap();
    let error = session
        .execute_write("CREATE TABLE plain (a INT, b INT, UNIQUE INDEX idx (a) GLOBAL)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8200, "Unsupported Global Index on non-partitioned table")
    );
    session
        .execute_write(
            "CREATE TABLE gp (a INT, d VARCHAR(10), UNIQUE INDEX d_s (d(3)) GLOBAL) \
             PARTITION BY HASH (a) PARTITIONS 3",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "8265",
            "Auto analyze is not effective for index 'd_s', need analyze manually"
        ]]
    );
    session
        .execute_write("ALTER TABLE gp ADD UNIQUE INDEX ie ((a + 2)) GLOBAL")
        .unwrap();
    assert!(rows(&mut session, "SHOW WARNINGS").is_empty());
}

/// Go `buildCheckIndexSchema`: `ADMIN CHECK INDEX t idx (lo, hi)` returns the
/// indexed columns under their own types and a BIGINT `extra_handle`; the
/// production text writer refused rows declared all-VARCHAR.
#[test]
fn admin_check_index_range_rows_carry_the_index_column_types() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1024)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE ci (a INT, b VARCHAR(10), INDEX a_b (a, b))")
        .unwrap();
    session
        .execute_write("INSERT INTO ci VALUES (3, 'ab'), (2, 'cd'), (1, 'ef'), (-1, 'hi')")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "ADMIN CHECK INDEX ci a_b (2, 4)")),
        [["1", "ef", "3"], ["2", "cd", "2"]]
    );
}

/// Go's `SET GLOBAL` writes `mysql.global_variables` through an internal
/// session (`SetGlobalSysVar`), so it neither commits the issuing session's
/// transaction nor clears `@@tidb_current_ts`: `set global
/// tidb_external_ts=@@tidb_current_ts` inside a transaction reads that
/// transaction's start timestamp. The production node committed first.
#[test]
fn set_global_inside_a_transaction_keeps_the_transaction_open() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1025)).unwrap();
    session.execute_write("USE test").unwrap();
    session.execute_write("CREATE TABLE sg (a INT)").unwrap();
    session
        .execute_write("SET GLOBAL tidb_external_ts = 19980613")
        .unwrap();
    session.control_transaction("BEGIN").unwrap();
    session.execute_write("INSERT INTO sg VALUES (1)").unwrap();
    let start = displayed(rows(&mut session, "SELECT @@tidb_current_ts"));
    session
        .execute_write("SET GLOBAL tidb_external_ts = @@tidb_current_ts")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@tidb_current_ts")),
        start
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@tidb_external_ts")),
        start
    );
    session.control_transaction("ROLLBACK").unwrap();
    assert!(rows(&mut session, "SELECT * FROM sg").is_empty());
}

/// Go's preprocess judges a table-qualified column name in CREATE TABLE
/// (1102 for another schema, 1103 for another table) and the builder then
/// reads the bare name; the production builder refused every qualifier.
#[test]
fn create_table_accepts_a_column_qualified_by_its_own_table() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1026)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE q2 (a CHAR, q2.b INT)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW COLUMNS FROM q2"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    let error = session
        .execute_write("CREATE TABLE q3 (q9.a CHAR)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1103, "Incorrect table name 'q9'")
    );
}

/// Go's CREATE TABLE and DROP INDEX on the production node:
/// - a written `PLACEMENT POLICY 'default'` is a reference Go's
///   `checkAndNormalizePlacementPolicy` clears afterwards, so the table does
///   not take the schema's default policy;
/// - `onDropIndex` removes an expression index's hidden columns
///   (`RemoveDependentHiddenColumns`), so their names are free again.
#[test]
fn create_table_default_policy_and_drop_index_hidden_columns_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1027)).unwrap();
    session
        .execute_write("CREATE PLACEMENT POLICY px FOLLOWERS=1")
        .unwrap();
    session
        .execute_write("CREATE DATABASE pdb PLACEMENT POLICY px")
        .unwrap();
    session
        .execute_write("CREATE TABLE pdb.t1 (a INT) PLACEMENT POLICY 'default'")
        .unwrap();
    session
        .execute_write("CREATE TABLE pdb.t2 (a INT)")
        .unwrap();
    let show = |session: &mut ClusterServerSession, table: &str| {
        displayed(rows(session, &format!("SHOW CREATE TABLE {table}")))[0][1].clone()
    };
    assert!(!show(&mut session, "pdb.t1").contains("PLACEMENT POLICY"));
    assert!(show(&mut session, "pdb.t2").contains("PLACEMENT POLICY=`px`"));
    session
        .execute_write("CREATE TEMPORARY TABLE pdb.tmp LIKE pdb.t1")
        .unwrap();

    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE h (a INT, b REAL)")
        .unwrap();
    session
        .execute_write("ALTER TABLE h ADD INDEX idx ((a + 1))")
        .unwrap();
    let error = session
        .execute_write("ALTER TABLE h ADD COLUMN (_V$_idx_0 INT)")
        .unwrap_err();
    assert_eq!(error.code, 1060);
    session
        .execute_write("ALTER TABLE h DROP INDEX idx")
        .unwrap();
    session
        .execute_write("ALTER TABLE h ADD COLUMN (_V$_idx_0 INT)")
        .unwrap();
}

/// Go `ddl.Start` -> `detectAndUpdateJobVersion` turns on the global index V1
/// key format (a single node at once), so a non-unique global index carries
/// the partition ID in its key and rows of different partitions sharing a
/// `_tidb_rowid` after EXCHANGE PARTITION keep separate entries (issue
/// 65289). The node never ran the detection and wrote the legacy key.
#[test]
fn global_index_keys_carry_the_partition_after_exchange() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1028)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED)")
        .unwrap();
    session
        .execute_write(
            "CREATE TABLE tp (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (11), \
             PARTITION p2 VALUES LESS THAN (20))",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO tp (a, b) VALUES (2, 2), (4, 4), (6, 6)")
        .unwrap();
    session
        .execute_write("INSERT INTO t (a, b) VALUES (12, 2), (14, 4), (16, 6)")
        .unwrap();
    session
        .execute_write("ALTER TABLE tp EXCHANGE PARTITION p2 WITH TABLE t")
        .unwrap();
    session
        .execute_write("CREATE INDEX idx_b ON tp (b) GLOBAL")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM tp USE INDEX (idx_b)"
        )),
        [["6"]]
    );
}

/// Go `table.GetColDefaultValue` casts `types.NewDatum(defaultVal)` -- a
/// UTF-8 `utf8mb4_bin` string -- into the column. The production loader built
/// a binary datum instead, so the cast into a GBK column decoded the stored
/// UTF-8 as GBK: `DEFAULT 0xC4E3BAC3` read back as mojibake and `DEFAULT '好'`
/// refused to insert at all.
#[test]
fn gbk_column_defaults_insert_as_the_text_they_store() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1029)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t (a CHAR(10) CHARSET gbk DEFAULT 0xC4E3BAC3)")
        .unwrap();
    session
        .execute_write("INSERT INTO t VALUES (DEFAULT)")
        .unwrap();
    session.execute_write("INSERT INTO t () VALUES ()").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, HEX(a) FROM t")),
        [["你好", "C4E3BAC3"], ["你好", "C4E3BAC3"]]
    );
    session
        .execute_write("CREATE TABLE t2 (a CHAR(10) CHARSET gbk DEFAULT '好')")
        .unwrap();
    session
        .execute_write("INSERT INTO t2 VALUES (DEFAULT)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, HEX(a) FROM t2")),
        [["好", "BAC3"]]
    );
}

/// Go `HandleNonTransactionalDML` runs every job with `se.ExecuteStmt`, so an
/// auto-committed job publishes before the next one reads: the second job
/// sees the row the first moved and moves both. The node ran the jobs inside
/// the BATCH statement's own buffer, where the second job's index lookup met
/// the first job's unpublished row and failed, and the failure discarded
/// every job's writes.
#[test]
fn non_transactional_jobs_each_commit_before_the_next_reads() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1030)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t (id INT, v INT, KEY (id))")
        .unwrap();
    session
        .execute_write("CREATE TABLE t2 (id INT, v INT, KEY (id))")
        .unwrap();
    session
        .execute_write("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    session
        .execute_write("INSERT INTO t2 VALUES (1, 1), (2, 2), (4, 4)")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "BATCH ON test.t.id LIMIT 1 UPDATE t JOIN t2 ON t.id = t2.id SET t2.id = t2.id + 1"
        )),
        [["3", "all succeeded"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t2 ORDER BY v")),
        [["4", "1"], ["4", "2"], ["4", "4"]]
    );
}

/// Go `getColDefaultValue` evaluates the clock words through `GetTimeValue`
/// for DATE/DATETIME/TIMESTAMP columns alone. The catalog loader refused any
/// column whose literal default spelled `CURRENT_TIMESTAMP`, so a VARCHAR
/// declaring that text vanished right after its CREATE, and a VARCHAR's
/// `'current_date'` would have been evaluated as the clock.
#[test]
fn string_columns_keep_clock_words_as_their_text_default() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1031)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE t (id INT, a VARCHAR(20) DEFAULT 'cUrrent_tImestamp', \
             b VARCHAR(20) DEFAULT 'current_date')",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO t (id) VALUES (1)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, a, b FROM t")),
        [["1", "cUrrent_tImestamp", "current_date"]]
    );
}

/// Go `NewWindowFuncDesc` applies its NOT NULL fixups by the name AS WRITTEN,
/// and a view's body is stored restored -- `ROW_NUMBER()` in upper case -- so
/// `DESC` of the view re-plans a window column that missed the `row_number`
/// arm and is nullable. The port lowered the name first and answered NO.
#[test]
fn view_window_columns_describe_as_nullable() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1032)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t1 (id INT NOT NULL, num INT)")
        .unwrap();
    session
        .execute_write(
            "CREATE VIEW v AS SELECT id, row_number() OVER (PARTITION BY num) AS r FROM t1",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "DESC v")),
        [
            ["id", "int", "NO", "", "NULL", ""],
            ["r", "bigint", "YES", "", "NULL", ""]
        ]
    );
}

/// The column flags a client reads are Go's `ConvertColumnInfo` over the
/// plan's result types: `DefaultTypeForValue` makes every non-NULL literal NOT
/// NULL, every non-string builtin result BINARY (`newReturnField...`), and
/// `adjustNullFlagForReturnType` settles NOT NULL by function name. Every
/// expectation below was measured on this branch's Go (low 16 bits of the
/// column flag): `'a'`=1, `1e0`=129, `NULL`=128, `a+1`=129 over a NOT NULL
/// `a`, `b+1`=128, `isnull(b)`=129, `abs(a)`/`length(a)`=128,
/// `a IN (1,2)`=128, `a IS NOT NULL`=129, `LEAST(a,1)`=4225.
#[test]
fn result_column_flags_follow_gos_null_and_binary_stamps() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1033)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t (a INT NOT NULL, b INT)")
        .unwrap();
    session
        .execute_write("INSERT INTO t VALUES (1, 1)")
        .unwrap();
    for (sql, flag) in [
        ("SELECT 'a' FROM t", 1),
        ("SELECT 1e0 FROM t", 129),
        ("SELECT NULL FROM t", 128),
        ("SELECT TRUE FROM t", 129),
        ("SELECT a FROM t", 4097),
        ("SELECT a + 1 FROM t", 129),
        ("SELECT b + 1 FROM t", 128),
        ("SELECT a + b FROM t", 128),
        ("SELECT a DIV 1 FROM t", 128),
        ("SELECT isnull(b) FROM t", 129),
        ("SELECT a <=> b FROM t", 129),
        ("SELECT abs(a) FROM t", 128),
        ("SELECT length(a) FROM t", 128),
        ("SELECT a IN (1, 2) FROM t", 128),
        ("SELECT a BETWEEN 1 AND 2 FROM t", 128),
        ("SELECT a IS NOT NULL FROM t", 129),
        ("SELECT LEAST(a, 1) FROM t", 4225),
        ("SELECT GREATEST(a, b) FROM t", 4224),
    ] {
        let mut result = session.execute(sql).unwrap();
        let columns = crate::resultset_source::ResultSetSource::columns(result.source()).unwrap();
        assert_eq!(columns[0].flag, flag, "{sql}");
    }
}

/// Go's `FieldType.Equal` ignores every flag but UNSIGNED when a CASE branch
/// is wrapped, so `mul(price, minus(1, discount))` over NOT NULL columns --
/// which now carries NOT NULL like Go's `adjustNullFlagForReturnType` gives
/// it -- is NOT cast to the CASE's own (nullable) `decimal(31,4)`. TPC-H Q8
/// and Q14 rendered a `cast(..., decimal(31,4) BINARY)` Go does not have.
#[test]
fn case_branch_differing_only_in_not_null_is_not_cast() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1034)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE li (k INT NOT NULL, price DECIMAL(15,2) NOT NULL, \
             disc DECIMAL(15,2) NOT NULL)",
        )
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT SUM(CASE WHEN k > 1 THEN price * (1 - disc) ELSE 0.0000 END) \
         FROM li",
    ));
    let text = plan
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !text.contains("cast("),
        "unexpected cast in the plan:\n{text}"
    );
}

/// Go `executeCreateUser` / `executeRenameUser` refuse a user name over 32
/// bytes and a host over 255 with `ErrWrongStringLength` (1470), whose
/// `%-.70s` prints at most the first 70 characters of the offender. The
/// production node accepted both, and the same message for a view's DEFINER
/// printed the whole value.
#[test]
fn account_names_over_their_limits_are_refused_with_1470() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1035)).unwrap();
    let long_user = "u".repeat(33);
    let error = session
        .execute_write(&format!("CREATE USER '{long_user}'@'%'"))
        .unwrap_err();
    assert_eq!(error.code, 1470);
    assert_eq!(
        error.message,
        format!("String '{long_user}' is too long for user name (should be no longer than 32)")
    );
    let long_host = "h".repeat(256);
    let error = session
        .execute_write(&format!("CREATE USER 'x'@'{long_host}'"))
        .unwrap_err();
    assert_eq!(error.code, 1470);
    assert_eq!(
        error.message,
        format!(
            "String '{}' is too long for host name (should be no longer than 255)",
            "h".repeat(70)
        )
    );
    session.execute_write("CREATE USER 'short'@'%'").unwrap();
    let error = session
        .execute_write(&format!("RENAME USER 'short'@'%' TO '{long_user}'@'%'"))
        .unwrap_err();
    assert_eq!(error.code, 1470);
}

/// Go `CreateIndex` turns `ErrDupKeyName` under `IF NOT EXISTS` into a Note
/// and succeeds; the production planner returned success with no Note.
#[test]
fn create_index_if_not_exists_notes_the_duplicate_key_name() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1036)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t (a INT, KEY idx (a))")
        .unwrap();
    session
        .execute_write("CREATE INDEX IF NOT EXISTS idx ON t (a)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "1061", "Duplicate key name 'idx'"]]
    );
}

/// Go `checkAlterTableCharset` runs `checkIndexLengthWithNewCharset` before a
/// CONVERT TO CHARACTER SET: `(varchar(500) utf8, varchar(500) utf8)` keys at
/// 3000 bytes and would key at 4000 as utf8mb4, past `MaxIndexLength`.
#[test]
fn convert_charset_refuses_an_index_that_would_grow_too_long() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1037)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, \
             title VARCHAR(500) CHARACTER SET utf8, subtitle VARCHAR(500) CHARACTER SET utf8, \
             UNIQUE KEY (title, subtitle))",
        )
        .unwrap();
    let error = session
        .execute_write("ALTER TABLE posts CONVERT TO CHARACTER SET utf8mb4")
        .unwrap_err();
    assert_eq!(error.code, 1071);
    assert_eq!(
        error.message,
        "Specified key was too long (4000 bytes); max key length is 3072 bytes"
    );
    session
        .execute_write("CREATE TABLE short (a VARCHAR(10) CHARACTER SET utf8, KEY (a))")
        .unwrap();
    session
        .execute_write("ALTER TABLE short CONVERT TO CHARACTER SET utf8mb4")
        .unwrap();
}

/// Go `SimpleExec.executeDropStats` soft-deletes every named physical id's
/// statistics (`DeleteTableStatsFromKV(ids, true)`): the buckets and TopN rows
/// go, the histogram rows are zeroed in place, and the meta row stays with a
/// fresh version. The production node answered "DROP STATS ... is not ported"
/// and left every row.
#[test]
fn drop_stats_soft_deletes_the_stored_statistics() {
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut session = factory
        .open_session(session_context(1038))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE ds (c1 VARCHAR(20), c2 VARCHAR(20))",
    );
    rows(
        &mut session,
        "INSERT INTO ds VALUES ('1','1'),('2','2'),('3','3'),('4','4'),('1','1'),('2','2'),('3','3'),('4','4')",
    );
    rows(&mut session, "ANALYZE TABLE ds ALL COLUMNS WITH 2 TOPN");
    let table_id = factory
        .catalog
        .load()
        .find_table("test", "ds")
        .expect("table is published")
        .1
        .id;
    let count = |session: &mut ClusterServerSession, table: &str| {
        displayed(rows(
            session,
            &format!("SELECT COUNT(*) FROM mysql.{table} WHERE table_id = {table_id}"),
        ))[0][0]
            .clone()
    };
    assert_ne!(count(&mut session, "stats_buckets"), "0");
    assert_ne!(count(&mut session, "stats_top_n"), "0");
    let version_before = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .unwrap();

    session.execute_write("DROP STATS ds").unwrap();

    assert_eq!(count(&mut session, "stats_buckets"), "0");
    assert_eq!(count(&mut session, "stats_top_n"), "0");
    // The histogram rows stay, zeroed; the meta row keeps its counts and is
    // re-versioned.
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT COUNT(*) FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND distinct_count <> 0"
            ),
        )),
        [["0"]]
    );
    let version_after = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .unwrap();
    assert!(version_after > version_before);
}

/// Go resolves DROP STATS' tables before it writes anything.
#[test]
fn drop_stats_refuses_an_unknown_table_and_a_partition_clause_on_a_plain_one() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1039)).unwrap();
    session.execute_write("USE test").unwrap();
    session.execute_write("CREATE TABLE dp (a INT)").unwrap();
    let error = session.execute_write("DROP STATS missing").unwrap_err();
    assert_eq!(error.code, 1146);
    let error = session
        .execute_write("DROP STATS dp PARTITION p0")
        .unwrap_err();
    assert_eq!(error.code, 1505);
}

/// Go `UnionScanExec.getOneRow` merges the transaction's added rows into the
/// snapshot rows by handle, whether or not the scan asked to keep order: a
/// replaced row that sorts before the next snapshot row comes out before it.
/// The node dropped the shadowed snapshot rows and then emitted every
/// remaining snapshot row before any added one, so an UPDATE moved its row to
/// the end of an unordered SELECT.
#[test]
fn union_scan_emits_added_rows_in_handle_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1040)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE us (c_str VARCHAR(40), c_int INT, c_datetime DATETIME, \
             PRIMARY KEY (c_str) CLUSTERED, UNIQUE KEY (c_datetime))",
        )
        .unwrap();
    session
        .execute_write(
            "INSERT INTO us VALUES ('laughing hertz', 1, '2020-04-27 20:29:30'), \
             ('sharp yalow', 2, '2020-04-01 05:53:36'), \
             ('pedantic hoover', 3, '2020-03-10 11:49:00')",
        )
        .unwrap();
    session.control_transaction("BEGIN").unwrap();
    session
        .execute_write("UPDATE us SET c_str = 'amazing herschel' WHERE c_int = 3")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT c_int, c_str FROM us WHERE c_datetime BETWEEN '2020-01-09 22:00:28' \
             AND '2020-04-08 15:12:37'"
        )),
        [["3", "amazing herschel"], ["2", "sharp yalow"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT c_int FROM us")),
        [["3"], ["1"], ["2"]]
    );
    session.control_transaction("ROLLBACK").unwrap();
}

/// Go assigns a `CREATE TABLE ... LIKE` copy its own physical table ids -- the
/// table's, then one per partition -- like any other new table. The node kept
/// the source's partition ids, so the two tables shared physical tables and a
/// row inserted into one was counted by the other.
#[test]
fn create_table_like_a_partitioned_table_gets_its_own_partition_ids() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1041)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE lk1 (c_int INT, PRIMARY KEY (c_int)) PARTITION BY HASH (c_int) PARTITIONS 2",
        )
        .unwrap();
    session.execute_write("CREATE TABLE lk2 LIKE lk1").unwrap();
    session.execute_write("INSERT INTO lk2 VALUES (1)").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM lk1")),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM lk2")),
        [["1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(DISTINCT tidb_partition_id) FROM information_schema.partitions \
             WHERE table_schema = 'test' AND table_name IN ('lk1', 'lk2')"
        )),
        [["4"]]
    );
}

/// Go reads a BIT column's default through `GetDefaultValue()`, which returns
/// the raw `DefaultValueBit` bytes; the JSON-facing `DefaultValue` string is
/// those bytes decoded lossily (every byte that is not valid UTF-8 becomes
/// U+FFFD). The node's catalog loader read the string, so a BIT default with a
/// byte at or above 0x80 came back as `EF BF BD` repeated.
#[test]
fn bit_defaults_keep_their_raw_bytes_across_the_stored_catalog() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1042)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE bd (k INT, v BIT(34) DEFAULT b'111010101111001001100111101111111', \
             z BIT(40) DEFAULT 0xFFEEDDCCBB, w BIT(8) DEFAULT b'101')",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO bd (k) VALUES (1)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT HEX(v), HEX(z), HEX(w) FROM bd")),
        [["1D5E4CF7F", "FFEEDDCCBB", "5"]]
    );
}

/// Go judges `HasCharset(colDef.Tp)` for a COLUMN `COLLATE` option on the
/// parsed type, which still carries `BinaryFlag` for a `BINARY` attribute: the
/// option is then not applied and `OverwriteCollationWithBinaryFlag`'s
/// `utf8_bin` stands. An explicit CHARACTER SET plus COLLATE ignores BINARY.
#[test]
fn binary_attribute_with_a_collate_option_keeps_the_charsets_bin_collation() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1043)).unwrap();
    session.execute_write("USE test").unwrap();
    for (create, expected) in [
        (
            "CREATE TABLE bc1 (a VARCHAR(10) BINARY COLLATE utf8_general_ci)",
            "CHARACTER SET utf8 COLLATE utf8_bin",
        ),
        (
            "CREATE TABLE bc2 (a VARCHAR(10) BINARY CHARSET utf8 COLLATE utf8_general_ci)",
            "CHARACTER SET utf8 COLLATE utf8_general_ci",
        ),
        (
            "CREATE TABLE bc3 (a VARCHAR(10) BINARY CHARSET utf8)",
            "CHARACTER SET utf8 COLLATE utf8_bin",
        ),
    ] {
        session.execute_write(create).unwrap();
        let table = create.split_whitespace().nth(2).unwrap();
        let shown = displayed(rows(&mut session, &format!("SHOW CREATE TABLE {table}")));
        assert!(
            shown[0][1].contains(&format!("`a` varchar(10) {expected} DEFAULT NULL")),
            "{create}: {}",
            shown[0][1]
        );
    }
}

/// Go's `CompactTableStmt` validates the table and partition names and, with
/// no TiFlash replica, answers a warning; it does not change the catalog. The
/// production node refused it as an unroutable catalog change.
#[test]
fn compact_table_without_a_replica_warns_instead_of_refusing() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1044)).unwrap();
    session.execute_write("USE test").unwrap();
    session.execute_write("CREATE TABLE ct (a INT)").unwrap();
    session
        .execute_write("ALTER TABLE ct COMPACT TIFLASH REPLICA")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "1105",
            "compact skipped: no tiflash replica in the table"
        ]]
    );
    let error = session
        .execute_write("ALTER TABLE ct COMPACT PARTITION p1 TIFLASH REPLICA")
        .unwrap_err();
    assert_eq!(error.code, 1105);
    let error = session
        .execute_write("ALTER TABLE missing COMPACT")
        .unwrap_err();
    assert_eq!(error.code, 1146);
}

/// Go `GetMergeJoin` returns nil for ENUM/SET join keys before it reaches
/// `getEnforcedMergeJoin` (issues 24473/25669: the index orders an ENUM by
/// its ordinal, the merge compares it by name). The port refused only the
/// naturally ordered form, so a MERGE_JOIN hint built a merge join over the
/// ordinal-ordered index and dropped every row whose name order disagreed.
#[test]
fn merge_join_hint_does_not_merge_enum_or_set_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1045)).unwrap();
    session.execute_write("USE test").unwrap();
    for column in [
        "ENUM('y','b','1','x','0','null')",
        "SET('y','b','1','x','0','null')",
    ] {
        session.execute_write("DROP TABLE IF EXISTS mjx").unwrap();
        session
            .execute_write(&format!(
                "CREATE TABLE mjx (a {column} DEFAULT NULL, KEY a (a))"
            ))
            .unwrap();
        session
            .execute_write("INSERT INTO mjx VALUES ('x'), ('x'), ('b'), ('y')")
            .unwrap();
        let mut answer = displayed(rows(
            &mut session,
            "SELECT /*+ merge_join (t2,t3) */ t2.a, t3.a FROM mjx t2 INNER JOIN mjx t3 ON t2.a = t3.a",
        ));
        answer.sort();
        assert_eq!(
            answer,
            [
                ["b", "b"],
                ["x", "x"],
                ["x", "x"],
                ["x", "x"],
                ["x", "x"],
                ["y", "y"]
            ],
            "{column}"
        );
    }
}

/// Go's pushed-down index lookup (`LocalIndexLookUp`) runs its build side --
/// `Limit | Selection | IndexScan` -- on index entries, sorts the cut handle
/// batch, and only then applies the root `IndexLookUp`'s embedded offset; a
/// root `Selection` stays above all of it. The port took the root Selection
/// into the source (filtering before the offset), lowered the build-side
/// Selection as a table-side residual (so the entry stream was never cut),
/// and let the direct chunk path run past the pushed count.
#[test]
fn index_lookup_pushdown_cuts_entries_before_the_root_filter() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1046)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE ilp1 (id INT PRIMARY KEY, a VARCHAR(32), b INT, c INT, INDEX i(a, b))",
        )
        .unwrap();
    session
        .execute_write(
            "INSERT INTO ilp1 VALUES (1, '9a', 10, 100), (2, '8b', 20, 200), (3, '7c', 30, 300), \
             (4, '6d', 40, 400), (5, '5e', 50, 500), (6, '4f', 60, 600), (7, '3g', 70, 700), \
             (8, '2h', 80, 800), (9, '1i', 90, 900), (10, '0j', 100, 1000)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM (SELECT /*+ index_lookup_pushdown(ilp1, i) */ * FROM ilp1 \
             WHERE a > '1i' LIMIT 1, 6) tx WHERE tx.c != 500 LIMIT 4",
        )),
        [["3"], ["4"], ["6"], ["7"]]
    );
    session
        .execute_write(
            "CREATE TABLE ilp2 (a VARCHAR(32), b INT, c INT, d INT, e INT, \
             PRIMARY KEY (a, b) CLUSTERED, INDEX i(c), UNIQUE INDEX u(e)) \
             CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
        )
        .unwrap();
    session
        .execute_write(
            "INSERT INTO ilp2 VALUES ('a', 1, 9010, 100, 90), ('A', 2, 8020, 200, 80), \
             ('a', 3, 7030, 300, 70), ('b', 1, 6040, 400, 60), ('B', 2, 5050, 500, 50), \
             ('b', 3, 4060, 600, 40), ('c', 1, 3070, 700, 30), ('C', 2, 2080, 800, 20), \
             ('c', 3, 1090, 900, 10)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ index_lookup_pushdown(ilp2, i) */ * FROM ilp2 \
             WHERE b < 3 AND a < 'c' LIMIT 3",
        )),
        [
            ["A", "2", "8020", "200", "80"],
            ["b", "1", "6040", "400", "60"],
            ["B", "2", "5050", "500", "50"]
        ]
    );
    // Pruned to the clustered key, the same read is a covering IndexReader:
    // its cop `Limit` counts the entries that pass the cop Selection below
    // it, in index order.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ index_lookup_pushdown(ilp2, i) */ a, b FROM ilp2 \
             WHERE b < 3 AND a < 'c' LIMIT 3",
        )),
        [["B", "2"], ["b", "1"], ["A", "2"]]
    );
}

/// A cache hit rebinds the index-merge reader's TABLE side too: its recheck
/// Selection carries the same parameter as the partial `a = ?`. Rebinding only
/// the partials left `a = 1` in the recheck, which dropped every `a = 3` row
/// the rebuilt partial found.
#[test]
fn cached_index_merge_rebinds_the_table_side_recheck() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1047)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE cim (id INT NOT NULL, a INT, b INT, c INT, \
             PRIMARY KEY (id) CLUSTERED, KEY ia (a, c), KEY ib (b, c))",
        )
        .unwrap();
    session
        .execute_write(
            "INSERT INTO cim VALUES (1,1,1,10), (2,1,2,20), (3,2,1,30), (4,2,2,40), \
             (5,1,3,50), (6,3,1,60), (7,3,2,70), (8,1,1,5), (9,2,3,15), (10,3,3,25)",
        )
        .unwrap();
    rows(
        &mut session,
        "PREPARE im FROM 'SELECT /*+ use_index_merge(cim) */ id FROM cim \
         WHERE a = ? OR b = 1 ORDER BY id'",
    );
    for (value, expected) in [
        (1, vec!["1", "2", "3", "5", "6", "8"]),
        (3, vec!["1", "3", "6", "7", "8", "10"]),
    ] {
        rows(&mut session, &format!("SET @a = {value}"));
        let got: Vec<String> = displayed(rows(&mut session, "EXECUTE im USING @a"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(got, expected, "a = {value}");
    }
}

/// Go `getNameValuePairs` declines the fast point plan when a column is
/// qualified by anything but the table's visible name; the ordinary planner
/// then reports the unknown column (1054). An alias hides the real name.
#[test]
fn point_get_fast_path_checks_the_column_qualifier() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1048)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE qpg (a CHAR(2) PRIMARY KEY, b CHAR(2))")
        .unwrap();
    session
        .execute_write("INSERT INTO qpg VALUES ('aa', 'bb')")
        .unwrap();
    for sql in [
        "SELECT a FROM qpg WHERE xxxxx.a = 'aa'",
        "SELECT a FROM qpg tmp WHERE qpg.a = 'aa'",
    ] {
        assert_eq!(query_error(&mut session, sql).0, 1054, "{sql}");
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM qpg tmp WHERE tmp.a = 'aa'"
        )),
        [["aa"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM qpg WHERE qpg.a = 'aa'")),
        [["aa"]]
    );
}

/// Go plans `Point_Get ... partition:p0` for a unique key on a partitioned
/// table and routes the key by the partition expression; UPDATE and DELETE
/// reach the same row, and a key no partition holds answers empty.
#[test]
fn partitioned_unique_key_point_get_routes_by_partition() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1049)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE ppg (a INT, b INT, UNIQUE KEY (a)) PARTITION BY LIST (a) \
             (PARTITION p0 VALUES IN (0,4), PARTITION p1 VALUES IN (1,3))",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO ppg VALUES (4, 40), (1, 10), (3, 30)")
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM ppg WHERE a = 4",
    ));
    assert_eq!(plan.len(), 1, "{plan:?}");
    assert!(
        plan[0][0].starts_with("Point_Get") && plan[0][3].contains("partition:p0"),
        "{plan:?}"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM ppg WHERE a = 4")),
        [["4", "40"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM ppg WHERE a = 7")),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM ppg PARTITION (p1) WHERE a = 4"
        )),
        Vec::<Vec<String>>::new()
    );
    session
        .execute_write("UPDATE ppg SET b = 41 WHERE a = 4")
        .unwrap();
    session
        .execute_write("DELETE FROM ppg WHERE a = 3")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM ppg ORDER BY a")),
        [["1", "10"], ["4", "41"]]
    );
}

/// Go's `ExecuteExec` runs a prepared DDL as the statement's body, so it must
/// reach the cluster catalog like any DDL: the prepared `CREATE TABLE` was
/// applied to the session's own copy and a later `DROP TABLE` found nothing.
/// Also pinned here: the table-build warnings (`LINEAR KEY` is ignored, 8200),
/// the schema-qualified sequence in a default, and the refusal of a stale-read
/// view body (1356).
#[test]
fn prepared_ddl_and_create_table_build_warnings_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1050)).unwrap();
    session.execute_write("USE test").unwrap();
    rows(
        &mut session,
        "PREPARE pc FROM 'CREATE TABLE pdl (id BIGINT PRIMARY KEY) PARTITION BY RANGE (id) \
         INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (500)'",
    );
    rows(&mut session, "EXECUTE pc");
    assert_eq!(
        displayed(rows(&mut session, "SHOW TABLES LIKE 'pdl'")),
        [["pdl"]]
    );
    session.execute_write("DROP TABLE pdl").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW TABLES LIKE 'pdl'")),
        Vec::<Vec<String>>::new()
    );

    session
        .execute_write(
            "CREATE TABLE lk (c1 INT, c2 CHAR(5), c3 DATE) PARTITION BY LINEAR KEY (c3) PARTITIONS 5",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "8200",
            "LINEAR KEY is not supported, using non-linear KEY instead"
        ]]
    );

    session.execute_write("CREATE SEQUENCE sq").unwrap();
    session
        .execute_write("CREATE TABLE dseq (a INT DEFAULT (NEXTVAL(sq)))")
        .unwrap();
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE dseq"));
    assert!(
        shown[0][1].contains("DEFAULT (nextval(`test`.`sq`))"),
        "{shown:?}"
    );

    session.execute_write("CREATE TABLE src (a INT)").unwrap();
    let error = session
        .execute_write(
            "CREATE VIEW v_stale AS SELECT * FROM src \
             AS OF TIMESTAMP DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)",
        )
        .unwrap_err();
    assert_eq!(error.code, 1356);
}

/// Go stamps `Grantor` (the session's `User.String()`) on the table-scope row
/// a GRANT writes, leaves it alone on REVOKE, and `SET DEFAULT ROLE` deletes
/// and re-inserts the account's `mysql.default_roles` rows, so a set rewritten
/// from `r_2` to `ALL` reads back `r_1, r_2` rather than the surviving row
/// first.
#[test]
fn grant_stamps_the_grantor_and_set_default_role_rewrites_its_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1051)).unwrap();
    session.execute_write("USE test").unwrap();
    for sql in [
        "CREATE USER gtu",
        "CREATE ROLE gr_1",
        "CREATE ROLE gr_2",
        "GRANT gr_1 TO gtu",
        "GRANT gr_2 TO gtu",
        "CREATE TABLE gtab (a INT)",
        "GRANT SELECT ON test.gtab TO gtu",
    ] {
        session
            .execute_write(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let grants = |session: &mut ClusterServerSession| {
        displayed(rows(
            session,
            "SELECT grantor, table_priv FROM mysql.tables_priv WHERE user = 'gtu'",
        ))
    };
    let granted_by = displayed(rows(&mut session, "SELECT CONCAT(user, '@', host) FROM (SELECT SUBSTRING_INDEX(CURRENT_USER(), '@', 1) AS user, SUBSTRING_INDEX(CURRENT_USER(), '@', -1) AS host) c"))
        [0][0]
        .clone();
    assert_eq!(grants(&mut session), [[granted_by.as_str(), "Select"]]);
    session
        .execute_write("GRANT INSERT ON test.gtab TO gtu")
        .unwrap();
    assert_eq!(
        grants(&mut session),
        [[granted_by.as_str(), "Select,Insert"]]
    );
    session
        .execute_write("REVOKE SELECT ON test.gtab FROM gtu")
        .unwrap();
    assert_eq!(grants(&mut session), [[granted_by.as_str(), "Insert"]]);

    let default_roles = |session: &mut ClusterServerSession| {
        displayed(rows(
            session,
            "SELECT default_role_user FROM mysql.default_roles WHERE user = 'gtu'",
        ))
    };
    session
        .execute_write("SET DEFAULT ROLE gr_2 TO gtu")
        .unwrap();
    assert_eq!(default_roles(&mut session), [["gr_2"]]);
    session
        .execute_write("SET DEFAULT ROLE ALL TO gtu")
        .unwrap();
    assert_eq!(default_roles(&mut session), [["gr_1"], ["gr_2"]]);
    session
        .execute_write("SET DEFAULT ROLE NONE TO gtu")
        .unwrap();
    assert_eq!(default_roles(&mut session), Vec::<Vec<String>>::new());
}

/// Go `checkTempTableSize` refuses a row once a temporary table holds more
/// than `@@tidb_tmp_table_max_size` -- what it committed plus this
/// transaction's own writes -- with 1114, before the row is written.
#[test]
fn temporary_table_refuses_rows_past_tmp_table_max_size() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1052)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE GLOBAL TEMPORARY TABLE tg (c1 INT, c2 MEDIUMTEXT) ON COMMIT DELETE ROWS",
        )
        .unwrap();
    session
        .execute_write("CREATE TEMPORARY TABLE tl (c1 INT, c2 MEDIUMTEXT)")
        .unwrap();
    session
        .execute_write("SET @@session.tidb_tmp_table_max_size = 1048576")
        .unwrap();
    let row = |table: &str| format!("INSERT INTO {table} VALUES (1, REPEAT('x', 512*1024))");
    for table in ["tg", "tl"] {
        session.control_transaction("BEGIN").unwrap();
        session.execute_write(&row(table)).unwrap();
        session.execute_write(&row(table)).unwrap();
        let error = session.execute_write(&row(table)).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (1114, format!("The table '{table}' is full").as_str())
        );
        session.control_transaction("ROLLBACK").unwrap();
    }
    // A LOCAL table's committed rows count against the next transaction.
    session.execute_write(&row("tl")).unwrap();
    session.control_transaction("BEGIN").unwrap();
    session.execute_write(&row("tl")).unwrap();
    let error = session.execute_write(&row("tl")).unwrap_err();
    assert_eq!(error.code, 1114);
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM tl")),
        [["1"]]
    );
}

/// Go's `tryPointGetPlan`/`tryBatchPointGetPlan` skip a unique index with a
/// condition (`idxInfo.HasCondition()`): a partial index stores only the rows
/// its predicate admits, so pinning `c = 1` through it misses a row that never
/// entered it. The fast plan chose `unique index idx3(c) where b > 10` and the
/// row with `b = 5` came back empty.
#[test]
fn point_get_never_pins_a_partial_unique_index() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1053)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE pidx (a INT, b INT, c INT, UNIQUE INDEX idx3 (c) WHERE b > 10)",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO pidx VALUES (1, 5, 1), (2, 20, 2)")
        .unwrap();
    for sql in [
        "SELECT a, b, c FROM pidx WHERE c = 1",
        "SELECT a, b, c FROM pidx WHERE c IN (1, 2) ORDER BY c",
    ] {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN FORMAT = 'brief' {sql}"),
        ));
        assert!(
            plan.iter().all(|row| !row[0].contains("Point_Get")),
            "{sql}: {plan:?}"
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b, c FROM pidx WHERE c = 1")),
        [["1", "5", "1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, b, c FROM pidx WHERE c IN (1, 2) ORDER BY c"
        )),
        [["1", "5", "1"], ["2", "20", "2"]]
    );
}

/// Go names a result column from the logical plan's `OutputNames` captured
/// BEFORE optimization, and an unaliased column reference keeps the column AS
/// WRITTEN (`buildProjectionFieldNameFromColumns`: `colNameField.Name.Name`),
/// not the declared spelling. A set operation's names are its first branch's,
/// also when the optimizer replaced its root (an always-false filter leaves a
/// TableDual that names nothing, and the columns read `Column#1`).
#[test]
fn result_columns_keep_the_names_the_query_wrote() {
    use crate::resultset_source::ResultSetSource;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1054)).unwrap();
    session.execute_write("USE test").unwrap();
    let names = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        let mut result = session.execute(sql).expect("the query runs");
        let source = result.source();
        let columns = source
            .columns()
            .expect("columns")
            .into_iter()
            .map(|column| column.name)
            .collect();
        let _ = source.next_batch(8);
        columns
    };
    for (sql, expected) in [
        (
            "(SELECT table_name FROM information_schema.tables WHERE 1 = 0) \
             UNION ALL (SELECT table_name FROM information_schema.tables WHERE 1 = 0)",
            vec!["table_name"],
        ),
        (
            "SELECT * FROM ((SELECT table_name FROM information_schema.tables WHERE 1 = 0) \
             UNION ALL (SELECT table_name FROM information_schema.tables WHERE 1 = 0)) u",
            vec!["table_name"],
        ),
        (
            "(SELECT digest, digest_text FROM information_schema.statements_summary WHERE 1 = 0 \
             UNION ALL SELECT digest, digest_text FROM information_schema.statements_summary_history \
             WHERE 1 = 0) ORDER BY digest",
            vec!["digest", "digest_text"],
        ),
    ] {
        assert_eq!(names(&mut session, sql), expected, "{sql}");
    }
}

/// Go `pruneAndBuildColPositionInfoForDelete`: a single-table DELETE reads
/// only the handle's and the indexes' columns, so `delete from t` scans the
/// narrowest covering index and `delete ... where a = 1` projects the rest
/// away. The rows it removes, and every index entry of them, are unchanged.
#[test]
fn single_table_delete_reads_only_the_columns_removal_needs() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1055)).unwrap();
    session.execute_write("USE test").unwrap();
    let plan = |session: &mut ClusterServerSession, sql: &str| {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| format!("{} {}", row[0], row[3]))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    // A heap table: the handle is the hidden _tidb_rowid.
    session
        .execute_write("CREATE TABLE dp1 (a INT, b INT, c VARCHAR(20), INDEX ib (b))")
        .unwrap();
    session
        .execute_write(
            "INSERT INTO dp1 VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 20, 'z'), (4, 40, 'w')",
        )
        .unwrap();
    let full = plan(&mut session, "DELETE FROM dp1");
    assert!(
        full.contains("IndexFullScan") && full.contains("index:ib(b)"),
        "{full}"
    );
    let filtered = plan(&mut session, "DELETE FROM dp1 WHERE a = 1");
    assert!(filtered.contains("Projection"), "{filtered}");
    session
        .execute_write("DELETE FROM dp1 WHERE a = 1")
        .unwrap();
    session
        .execute_write("DELETE FROM dp1 WHERE b = 20 AND c = 'y'")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b, c FROM dp1 ORDER BY a")),
        [["3", "20", "z"], ["4", "40", "w"]]
    );
    // The index lost exactly the removed rows' entries.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM dp1 USE INDEX (ib) ORDER BY b"
        )),
        [["20"], ["40"]]
    );
    session
        .execute_write("DELETE FROM dp1 ORDER BY a LIMIT 1")
        .unwrap();
    assert_eq!(displayed(rows(&mut session, "SELECT a FROM dp1")), [["4"]]);
    // A clustered integer key and a unique secondary index.
    session
        .execute_write(
            "CREATE TABLE dp2 (id INT PRIMARY KEY CLUSTERED, u INT, pad VARCHAR(20), UNIQUE KEY uu (u))",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO dp2 VALUES (1, 100, 'p'), (2, 200, 'q'), (3, 300, 'r')")
        .unwrap();
    session
        .execute_write("DELETE FROM dp2 WHERE pad = 'q'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, u FROM dp2 USE INDEX (uu) ORDER BY u"
        )),
        [["1", "100"], ["3", "300"]]
    );
    // The freed unique value can be inserted again.
    session
        .execute_write("INSERT INTO dp2 VALUES (9, 200, 's')")
        .unwrap();
    // A common handle.
    session
        .execute_write(
            "CREATE TABLE dp3 (k1 VARCHAR(10), k2 INT, v INT, w INT, PRIMARY KEY (k1, k2) CLUSTERED, KEY iv (v))",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO dp3 VALUES ('a', 1, 5, 0), ('b', 2, 6, 0), ('c', 3, 7, 0)")
        .unwrap();
    session
        .execute_write("DELETE FROM dp3 WHERE w = 0 AND v = 6")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT k1, k2 FROM dp3 ORDER BY k1")),
        [["a", "1"], ["c", "3"]]
    );
    // A referenced table keeps every column (its foreign keys read them).
    session
        .execute_write(
            "CREATE TABLE dpp (id INT PRIMARY KEY, name VARCHAR(10), INDEX iname (name))",
        )
        .unwrap();
    session
        .execute_write(
            "CREATE TABLE dpc (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_dp FOREIGN KEY (pid) REFERENCES dpp (id))",
        )
        .unwrap();
    let referenced = plan(&mut session, "DELETE FROM dpp");
    assert!(!referenced.contains("Projection"), "{referenced}");
}

/// Go prunes a multi-table write's read too: a multi-table UPDATE keeps every
/// column of an updated alias and what its SET expressions read from the
/// other sources (`buildUpdate`'s `used` set), so the source that only joins
/// is scanned through its covering index; a multi-table DELETE keeps the
/// handle's and the indexes' columns of each target. The rows written are
/// unchanged.
#[test]
fn multi_table_writes_read_only_the_columns_they_need() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1056)).unwrap();
    session.execute_write("USE test").unwrap();
    let plan = |session: &mut ClusterServerSession, sql: &str| {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| format!("{} {}", row[0], row[3]))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    session
        .execute_write("CREATE TABLE mu1 (a INT, b INT, pad INT)")
        .unwrap();
    session
        .execute_write("CREATE TABLE mu2 (a INT, b INT, other INT, INDEX ib (b))")
        .unwrap();
    session
        .execute_write("INSERT INTO mu1 VALUES (1, 1, 0), (2, 2, 0), (3, 9, 0)")
        .unwrap();
    session
        .execute_write("INSERT INTO mu2 VALUES (10, 1, 100), (20, 2, 200), (30, 7, 300)")
        .unwrap();
    // `mu2` is only joined: its read is the covering index.
    let update = plan(
        &mut session,
        "UPDATE mu1, mu2 SET mu1.a = 99 WHERE mu1.b = mu2.b",
    );
    assert!(
        update.contains("IndexFullScan") && update.contains("index:ib(b)"),
        "{update}"
    );
    session
        .execute_write("UPDATE mu1, mu2 SET mu1.a = 99 WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b FROM mu1 ORDER BY b")),
        [["99", "1"], ["99", "2"], ["3", "9"]]
    );
    // A column of `mu2` the SET expression reads is kept.
    session
        .execute_write("UPDATE mu1, mu2 SET mu1.pad = mu2.other WHERE mu1.b = mu2.b AND mu2.a = 20")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b, pad FROM mu1 ORDER BY b")),
        [["1", "0"], ["2", "200"], ["9", "0"]]
    );
    // Updating both sources keeps both whole.
    session
        .execute_write("UPDATE mu1, mu2 SET mu1.pad = 5, mu2.other = mu1.b WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b, other FROM mu2 ORDER BY b")),
        [["1", "1"], ["2", "2"], ["7", "300"]]
    );
    // A multi-table DELETE of one target keeps the other source whole and
    // prunes the target to the index and handle.
    session
        .execute_write("DELETE mu1 FROM mu1, mu2 WHERE mu1.b = mu2.b AND mu2.other = 2")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM mu1 ORDER BY b")),
        [["1"], ["9"]]
    );
    session
        .execute_write("DELETE mu1, mu2 FROM mu1, mu2 WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM mu2 ORDER BY b")),
        [["2"], ["7"]]
    );
    // A multi-table DELETE over one aliased source behaves as its
    // single-table form (this shape used to inherit the single-table prune).
    session
        .execute_write("DELETE m FROM mu2 AS m WHERE m.b = 2")
        .unwrap();
    assert_eq!(displayed(rows(&mut session, "SELECT b FROM mu2")), [["7"]]);
}

/// Go's coprocessor TopN admits rows ONE at a time (a tie never evicts, so the
/// earliest rows survive), lays survivors out by heap push/fix, and orders
/// them with its pdqsort. The cop-layer executor instead loaded the whole
/// child chunk, heapified and popped, which kept different tied rows and
/// printed them in a different order (`[1, 8, 3]` for Go's `[5, 1, 3]`); both
/// final sorts were stable where Go's pdqsort is not beyond 12 rows.
#[test]
fn cop_top_n_ties_follow_go_heap_layout_and_pdqsort() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1057)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tn (a INT, b CHAR)")
        .unwrap();
    session
        .execute_write(
            "INSERT INTO tn VALUES (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a'),(1,'x'),(1,'y'),(1,'z')",
        )
        .unwrap();
    let ids = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    // Expected: Go 1.27 on this branch (`TestZZ` probes).
    for (sql, expected) in [
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 3",
            vec!["5", "1", "3"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 2",
            vec!["3", "1"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 4",
            vec!["6", "5", "3", "1"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 5",
            vec!["7", "6", "3", "1", "5"],
        ),
        (
            "SELECT _tidb_rowid FROM tn ORDER BY a LIMIT 6",
            vec!["8", "7", "3", "1", "5", "6"],
        ),
        (
            "SELECT _tidb_rowid FROM tn WHERE a = 1 ORDER BY a LIMIT 3",
            vec!["1", "3", "5"],
        ),
    ] {
        assert_eq!(ids(&mut session, sql), expected, "{sql}");
    }
    // Beyond twelve survivors the order is pdqsort's, not a stable sort's.
    session
        .execute_write("CREATE TABLE tm (a INT, b CHAR)")
        .unwrap();
    session
        .execute_write("INSERT INTO tm VALUES (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a')")
        .unwrap();
    for _ in 0..3 {
        session
            .execute_write("INSERT INTO tm SELECT * FROM tm")
            .unwrap();
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b, COUNT(*) FROM (SELECT b FROM tm ORDER BY a LIMIT 20 OFFSET 2) s \
             GROUP BY b ORDER BY b",
        )),
        [["a", "6"], ["c", "7"], ["s", "7"]]
    );
}

/// Go's root `SortExec` sorts with `slices.SortFunc` (pdqsort): beyond twelve
/// rows tied keys come out in pdqsort's order, not scan order. The partition
/// sorted with Rust's stable sort and printed the ties in insertion order.
#[test]
fn sort_ties_beyond_twelve_rows_follow_go_pdqsort() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1099)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE st1 (id INT, k INT)")
        .unwrap();
    let values: Vec<String> = (1..=40).map(|id| format!("({id},{})", id % 3)).collect();
    session
        .execute_write(&format!("INSERT INTO st1 VALUES {}", values.join(",")))
        .unwrap();
    let ids = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    // Expected: Go 1.27 on this branch (`TestZZ` probe).
    assert_eq!(
        ids(&mut session, "SELECT id FROM st1 ORDER BY k"),
        [
            "15", "39", "3", "36", "33", "6", "30", "27", "9", "24", "21", "12", "18", "31", "4",
            "16", "40", "13", "19", "37", "28", "22", "34", "10", "25", "1", "7", "8", "29", "14",
            "26", "32", "5", "23", "35", "11", "20", "38", "2", "17"
        ]
    );
    assert_eq!(
        ids(&mut session, "SELECT id FROM st1 ORDER BY k DESC"),
        [
            "14", "2", "38", "35", "5", "32", "29", "8", "26", "23", "11", "20", "17", "31", "4",
            "16", "13", "40", "19", "37", "28", "22", "10", "34", "25", "1", "7", "27", "15", "30",
            "9", "6", "33", "24", "21", "36", "12", "3", "39", "18"
        ]
    );
}

/// Go's must-analyze set follows only VIRTUAL generated columns back to their
/// base columns (`VirtualExpr`); an indexed STORED generated column is decoded
/// from storage, so its JSON base columns stay under
/// `tidb_analyze_skip_column_types` and get no histogram. Rust followed every
/// generated column, collected `j1`/`j2`, and so priced the table's scans on a
/// wider row than Go does (`TableFullScan` 271.59 for Go's 410.74).
#[test]
fn analyze_skips_json_bases_of_stored_generated_index_columns() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1100)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE sgb (j1 JSON, j2 JSON, a INT AS (JSON_EXTRACT(j1, '$.a')) STORED, \
             b VARCHAR(20) AS (JSON_KEYS(j2)) STORED, KEY idx_a (a), KEY idx_b (b))",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO sgb(j1, j2) VALUES ('{\"a\": 1}', '{\"1\": \"1\"}')")
        .unwrap();
    rows(&mut session, "ANALYZE TABLE sgb");
    let analyzed: Vec<String> = displayed(rows(
        &mut session,
        "SHOW STATS_HISTOGRAMS WHERE table_name = 'sgb' AND is_index = 0",
    ))
    .into_iter()
    .map(|row| row[3].clone())
    .collect();
    // Expected: Go 1.27 on this branch (`TestZZ` probe, `show stats_histograms`).
    assert_eq!(analyzed, ["a", "b"]);
}

/// Go's DISTINCT variance family keeps the distinct values and computes the
/// variance in two passes (`calculateDistinctFloat64Variance`); the plain
/// aggregate folds incrementally. The two round differently, and Rust fed the
/// distinct values through the incremental fold (`0.25` for Go's
/// `0.24999999999999956`).
#[test]
fn distinct_variance_is_computed_in_two_passes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1101)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tv (a TINYINT, b SMALLINT, f FLOAT, g DOUBLE)")
        .unwrap();
    session
        .execute_write("INSERT INTO tv VALUES (1, 2, 6.1, 7.2), (1, 3, 7.1, 8.2), (1, 2, 6.1, 7.2)")
        .unwrap();
    // Expected: Go 1.27 on this branch (`TestZZ` probe). The plain
    // `var_pop` is left out: arm64 Go fuses its multiply-subtract (FMA) and
    // prints a different last digit than amd64 Go and this port.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT var_pop(DISTINCT b), var_pop(DISTINCT g), var_pop(DISTINCT f), \
             var_samp(DISTINCT g), stddev_pop(DISTINCT g) FROM tv GROUP BY a",
        )),
        [[
            "0.25",
            "0.24999999999999956",
            "0.25",
            "0.4999999999999991",
            "0.49999999999999956"
        ]]
    );
}

/// Go's `TableSampleExec` samples `GetSnapshot(TxnCtx.StartTS)`: the open
/// transaction's own pending writes are invisible to `TABLESAMPLE REGIONS`
/// (`TestTableSampleUnionScanIgnorePendingKV`).
#[test]
fn table_sample_ignores_the_transactions_pending_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1102)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("SET @@global.tidb_scatter_region = 'table'")
        .unwrap();
    session
        .execute_write("CREATE TABLE tsp (a INT PRIMARY KEY)")
        .unwrap();
    rows(
        &mut session,
        "SPLIT TABLE tsp BETWEEN (0) AND (40000) REGIONS 4",
    );
    session
        .execute_write("INSERT INTO tsp VALUES (1), (1000), (10002)")
        .unwrap();
    let sampled = |session: &mut ClusterServerSession| -> Vec<String> {
        displayed(rows(session, "SELECT * FROM tsp TABLESAMPLE REGIONS()"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session.control_transaction("BEGIN").unwrap();
    session
        .execute_write("INSERT INTO tsp VALUES (20006), (50000)")
        .unwrap();
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session
        .execute_write("DELETE FROM tsp WHERE a = 1")
        .unwrap();
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session.control_transaction("COMMIT").unwrap();
    assert_eq!(sampled(&mut session), ["1000", "10002", "20006", "50000"]);
    // The splits are store metadata: a DDL rebuilds the catalog from the
    // stored schema (`TestTableSampleNotSupportedPlanWarning`) and must not
    // forget them.
    session
        .execute_write("CREATE TABLE tsr (a INT PRIMARY KEY, b INT)")
        .unwrap();
    rows(
        &mut session,
        "SPLIT TABLE tsr BETWEEN (0) AND (10000) REGIONS 5",
    );
    session
        .execute_write("INSERT INTO tsr VALUES (1000, 1), (1001, 1), (2100, 2), (4500, 3)")
        .unwrap();
    session
        .execute_write("CREATE INDEX idx_0 ON tsr (b)")
        .unwrap();
    let rows_after_ddl: Vec<String> = displayed(rows(
        &mut session,
        "SELECT a FROM tsr TABLESAMPLE REGIONS() ORDER BY a",
    ))
    .into_iter()
    .map(|row| row[0].clone())
    .collect();
    assert_eq!(rows_after_ddl, ["1000", "2100", "4500"]);
}

/// A non-unique index scan with an equality prefix is ordered by the integer
/// handle the index appends, so `ORDER BY` the handle needs no Sort (Go's
/// `fillIndexPath` appends the handle to `IdxCols`).
#[test]
fn index_with_equality_prefix_provides_the_handle_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1105)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tho (a INT PRIMARY KEY, b INT, c INT, KEY idx(b))")
        .unwrap();
    let operators = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .to_owned()
            })
            .map(|id| id.split('_').next().unwrap_or_default().to_owned())
            .collect()
    };
    // Expected: Go 1.27 on this branch (`TestZZ` probe, `explain`).
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT a FROM tho WHERE b = 3 ORDER BY a"
        ),
        ["IndexReader", "Projection", "IndexRangeScan"]
    );
}

/// The table path of a derived table's data source counts its rows from the
/// pushed-down range: the statement's own WHERE (which the bridge replays)
/// is not where the predicate lives, and the row count must not stand in for
/// the range (`TableRangeScan` 10 rows for Go, 10000 here). The wrong count
/// then priced an ordered index scan below the Sort Go chose.
#[test]
fn derived_table_range_scan_is_estimated_from_its_range() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1106)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE tdr (id INT UNSIGNED NOT NULL AUTO_INCREMENT, k INT UNSIGNED NOT NULL DEFAULT 0, \
             PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */, KEY k (k))",
        )
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN SELECT a.k FROM (SELECT * FROM tdr WHERE id < 10) a ORDER BY a.k",
    ));
    let kinds: Vec<&str> = plan
        .iter()
        .map(|row| row[0].trim_start_matches(['├', '└', '─', '│', ' ']))
        .collect();
    assert!(kinds[0].starts_with("Sort"), "{kinds:?}");
    let scan = plan
        .iter()
        .find(|row| row[0].contains("TableRangeScan"))
        .expect("a range scan below the sort");
    assert_eq!(scan[1], "10.00", "{plan:?}");
}

/// Go `ResetContextOfStmt` unwraps an EXPLAIN before its kind switch, so an
/// EXPLAINed DML is still a DML to `checkStableResultMode`: with
/// `tidb_enable_ordered_result_mode` on, its read keeps no order (the
/// ResultReorder rule is not applied), as the DML itself would.
#[test]
fn explained_dml_is_exempt_from_ordered_result_mode() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1107)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE teo (a INT PRIMARY KEY, b INT, c INT, KEY(b))")
        .unwrap();
    session
        .execute_write("SET tidb_enable_ordered_result_mode = 1")
        .unwrap();
    for sql in [
        "EXPLAIN FORMAT='verbose' UPDATE teo SET a = a + 1",
        "EXPLAIN FORMAT='verbose' INSERT INTO teo SELECT * FROM teo",
    ] {
        let plan = displayed(rows(&mut session, sql));
        let scan = plan
            .iter()
            .find(|row| row[0].contains("TableFullScan"))
            .unwrap_or_else(|| panic!("{sql}: {plan:?}"));
        assert!(scan[5].contains("keep order:false"), "{sql}: {scan:?}");
    }
}

/// Go's hash pruner has no empty-condition shortcut: a full range over a
/// BIT(1) column still reaches the issue-22619 branch, so the third of three
/// hash partitions (which a one-bit value can never reach) is not scanned.
#[test]
fn full_scan_of_a_bit_hash_table_skips_the_unreachable_partition() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1108)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE tbh (a BIT(1) DEFAULT NULL, b INT) PARTITION BY HASH(a) PARTITIONS 3",
        )
        .unwrap();
    let plan = displayed(rows(&mut session, "EXPLAIN SELECT * FROM tbh"));
    let reader = plan
        .iter()
        .find(|row| row[0].contains("TableReader"))
        .expect("a table reader");
    assert_eq!(reader[3], "partition:p0,p1", "{plan:?}");
}

/// Go summarizes an EXECUTE under the digest of the text as PREPAREd
/// (`select ?`); the port ran a marker-aliased copy (`select ? as `?``) and
/// summarized that (`TestStmtSummaryPreparedStatements`).
#[test]
fn prepared_execute_is_summarized_under_the_prepared_text() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1109)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    session
        .execute_write("PREPARE stmt FROM 'select ?'")
        .unwrap();
    session.execute_write("SET @number = 1").unwrap();
    rows(&mut session, "EXECUTE stmt USING @number");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT exec_count FROM information_schema.statements_summary WHERE digest_text LIKE 'select ?'",
        )),
        [["1"]]
    );
    session
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go's statement warnings are one buffer in order of occurrence: the casts
/// inside an `INSERT ... SELECT` overflow while the SELECT evaluates, and the
/// store conversion of their result overflows after. The port appended the
/// conversion row first.
#[test]
fn insert_select_warnings_keep_the_order_they_were_raised_in() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1110)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tiw (a DECIMAL(10,2))")
        .unwrap();
    session.execute_write("SET @@sql_mode = ''").unwrap();
    session
        .execute_write(
            "INSERT INTO tiw SELECT ROUND(CAST('88888899999999999888888888888888888888888888888888888.11111111111111111111' AS DECIMAL(18,12)) \
             * CAST('88888899999999999888888888888888888888888888888888888.11111111111111111111' AS DECIMAL(42,18)))",
        )
        .unwrap();
    // Expected: Go 1.27 on this branch (`TestZZ` probe).
    let messages: Vec<String> = displayed(rows(&mut session, "SHOW WARNINGS"))
        .into_iter()
        .map(|row| row[2].clone())
        .collect();
    assert_eq!(
        messages,
        [
            "DECIMAL value is out of range in '(18, 12)'",
            "DECIMAL value is out of range in '(42, 18)'",
            "DECIMAL value is out of range in '(10, 2)'",
        ]
    );
}

/// Go keys a statement-summary row by the plan digest: the sha256 of the
/// normalized select plan (`NormalizeFlatPlan`). The expected digests below
/// are what Go 1.27 on this branch records for the same statements, so the
/// normalization (constants as `?`, no ranges or statistics, children by plan
/// type, casts as `cast(arg, type)`) has to match byte for byte.
#[test]
fn statement_summary_plan_digests_match_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1111)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    for ddl in [
        "CREATE TABLE a(id INT PRIMARY KEY, x INT, y INT)",
        "CREATE TABLE b(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
        "CREATE TABLE c(id INT PRIMARY KEY, z INT)",
    ] {
        session.execute_write(ddl).unwrap();
    }
    for sql in [
        "select * from a where id = 1",
        "select * from a where id in (1,2,3)",
        "select * from a where x > 5",
        "select x, count(*) from a group by x",
        "select * from a join b on a.id=b.id",
        "select * from a join b on a.x=b.y where a.y > 3",
        "select * from b where y = 3",
        "select y from b where y > 3",
        "select * from a order by x limit 5",
        "select * from a where x in (1,2,3) and y = 2",
        "select /*+ merge_join(a,b) */ * from a join b on a.id=b.id",
        "select /*+ inl_join(b) */ * from a join b on a.x = b.id",
        "select * from a left join b on a.x=b.id",
        "insert into a select * from b",
        "update a set x=1 where y=2",
        "delete from a where id=3",
        "select sum(x), max(y) from a where y < 10",
        "select * from a union all select * from a",
        "select * from (select * from a where x>1) t where y<2",
        "select a.x, sum(b.z) from a join b on a.x = b.id group by a.x",
        "select * from a where id = 1 or id = 2",
        "select x from a where y = 1 and x = 2 and id > 3",
        "select count(*) from a",
        "select * from b where y in (1,2,3,4,5)",
        "select * from b where y in (1,2)",
    ] {
        if sql.starts_with("select") {
            rows(&mut session, sql);
        } else {
            session.execute_write(sql).unwrap();
        }
    }
    let recorded: std::collections::BTreeMap<String, String> = displayed(rows(
        &mut session,
        "SELECT digest_text, plan_digest FROM information_schema.statements_summary \
         WHERE schema_name = 'test' AND digest_text NOT LIKE '%statements_summary%' \
         AND digest_text NOT LIKE 'set %' AND digest_text NOT LIKE 'drop%' \
         AND digest_text NOT LIKE 'create%'",
    ))
    .into_iter()
    .map(|row| (row[0].clone(), row[1].clone()))
    .collect();
    let expected: [(&str, &str); 23] = [
        (
            "delete from `a` where `id` = ?",
            "d29970167a003b98ca8a6f2e0daa0f807bf4f5ff5281dc96c28072e66a005520",
        ),
        (
            "insert into `a` select * from `b`",
            "b1aae3f2a4e07761a960a05c09c85e92d6bfa24d1152aee6e8f6d0b8791fe0bf",
        ),
        (
            "select * from ( select * from `a` where `x` > ? ) `t` where `y` < ?",
            "3273be776060fd8de6854902b84505c0a93b636e33d05b1e2654d6824b4120f5",
        ),
        (
            "select * from `a` join `b` on `a` . `id` = `b` . `id`",
            "0e1e7baf142e077d27a48d405e83fba1813fb6bf368841cfdeb65103737ecfa9",
        ),
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `id`",
            "dfdf2e6d50fccfde4254aba057430f6915f4b8d34e331004ad50c8d9c15ef7db",
        ),
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `y` where `a` . `y` > ?",
            "dd97e4de0b662db2162372f0771c79ae0790d5eaa208bd7582cd847f9427eb9a",
        ),
        (
            "select * from `a` left join `b` on `a` . `x` = `b` . `id`",
            "6cfb251e6cd76579b9580f81774b14f96840c31341976137c358bae9913edeb4",
        ),
        (
            "select * from `a` order by `x` limit ?",
            "1067bf004a3e0c7d495f241d6ba34dfb0536c1a0e2174991bb51731e2e8dab86",
        ),
        (
            "select * from `a` union all select * from `a`",
            "52b9478f1a9ef9515ee3d887f91906e4c1babb1bb492062dc4868ad48fb29308",
        ),
        (
            "select * from `a` where `id` = ?",
            "d29970167a003b98ca8a6f2e0daa0f807bf4f5ff5281dc96c28072e66a005520",
        ),
        (
            "select * from `a` where `id` = ? or `id` = ?",
            "014262c6f5bf53dd777929675ecf581b0aaed890b1816a2694e26e2649a4b623",
        ),
        (
            "select * from `a` where `id` in ( ... )",
            "014262c6f5bf53dd777929675ecf581b0aaed890b1816a2694e26e2649a4b623",
        ),
        (
            "select * from `a` where `x` > ?",
            "cf192d061522e508cd53b9d808c92b98bb0e8d2b93e70cfce94f00762c2ff934",
        ),
        (
            "select * from `a` where `x` in ( ... ) and `y` = ?",
            "83088a56b3604f0c1be1c19dd53d73b4b25ce2ffb40e7e565b530290e0c09ebd",
        ),
        (
            "select * from `b` where `y` = ?",
            "034a2651f373598c1085024d7e26a83d9f25cac34059d9cb9ae7cbd85c03bbc1",
        ),
        (
            "select * from `b` where `y` in ( ... )",
            "034a2651f373598c1085024d7e26a83d9f25cac34059d9cb9ae7cbd85c03bbc1",
        ),
        (
            "select `a` . `x` , sum ( `b` . `z` ) from `a` join `b` on `a` . `x` = `b` . `id` group by `a` . `x`",
            "cbf0060b5b52e0fb7ad74523a31c9739f0695e785722514a95f315e04637687b",
        ),
        (
            "select `x` , count ( ? ) from `a` group by `x`",
            "7e7b9248c5c70eaa7ecbc3a4677aafe4604c242e9d31d5e0952aeeb2426435c2",
        ),
        (
            "select `x` from `a` where `y` = ? and `x` = ? and `id` > ?",
            "84a2beb6b1d5300acdddcb8c4c56feae6c05689a8c5ef82d3ad05b4cb4fd98ee",
        ),
        (
            "select `y` from `b` where `y` > ?",
            "3da471c073d10289b7157ef67bb3c857bba94a88dd172aff63880d597718fa82",
        ),
        (
            "select count ( ? ) from `a`",
            "99c64d1aeb0cc1b601f09cc860790095f2dcb28f30b2242d3d2124bad53d8be9",
        ),
        (
            "select sum ( `x` ) , max ( `y` ) from `a` where `y` < ?",
            "7dfbcadf84bb9d1cd3251022e81c8ff61ba3c33265ec0132b0b85d0ad5e70a35",
        ),
        (
            "update `a` set `x` = ? where `y` = ?",
            "2cf881ae462c644791a5060e3d943ca47a14a58334581d0791f302411d979ef5",
        )
    ];
    for (text, digest) in expected {
        assert_eq!(
            recorded.get(text).map(String::as_str),
            Some(digest),
            "{text}"
        );
    }
    session
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go's IN-subquery rewrite turns `a IN (SELECT b FROM ...)` into an inner join
/// over a de-duplicating aggregation and sets the flags that let
/// `aggregationEliminator` remove it again. When the subquery's column is a
/// primary key the aggregation is redundant and Go's plan has none; without the
/// flags this port kept a `StreamAgg` below the join.
#[test]
fn in_subquery_over_a_primary_key_has_no_deduplicating_aggregation() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1112)).unwrap();
    session.execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE sia(id INT PRIMARY KEY, x INT, y INT)",
        "CREATE TABLE sib(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
    ] {
        session.execute_write(ddl).unwrap();
    }
    let operators = |session: &mut ClusterServerSession, sql: &str| -> String {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    let unique = operators(
        &mut session,
        "EXPLAIN SELECT * FROM sia WHERE id IN (SELECT id FROM sib WHERE z > 1)",
    );
    assert!(!unique.contains("Agg"), "{unique}");
    // A non-unique column still needs the de-duplication.
    let not_unique = operators(
        &mut session,
        "EXPLAIN SELECT * FROM sia WHERE x IN (SELECT y FROM sib)",
    );
    assert!(not_unique.contains("StreamAgg"), "{not_unique}");
}

/// Go's fast point-get plan accepts a locking read and sets `Lock` only when
/// `PessimisticLockEligible` (autocommit off, or a transaction open): an
/// autocommit `SELECT ... FOR UPDATE` on a primary key is a plain `Point_Get`
/// with no `SelectLock` above it, as is the IN-list form's `Batch_Point_Get`.
#[test]
fn autocommit_for_update_point_gets_have_no_select_lock() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1113)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tfu (id INT PRIMARY KEY, x INT)")
        .unwrap();
    let operators = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .split('_')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    };
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT * FROM tfu WHERE id = 1 FOR UPDATE"
        ),
        ["Point"]
    );
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT * FROM tfu WHERE id IN (1, 2) FOR UPDATE"
        ),
        ["Batch"]
    );
    // A non-point read keeps its SelectLock (a no-op at execution).
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT * FROM tfu WHERE x = 1 FOR UPDATE"
        )[0],
        "SelectLock"
    );
}

/// More operator shapes than `statement_summary_plan_digests_match_go`: window
/// and Shuffle, TopN, aggregates, semi-join rewrites, apply, CTE, unique-index
/// point gets and in-list collapsing. Expected digests are the Go oracle's,
/// recorded under the replay's session setup (`tidb_hash_join_concurrency=1`).
#[test]
fn statement_summary_plan_digests_match_go_for_more_shapes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1114)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("SET @@tidb_hash_join_concurrency = 1")
        .unwrap();
    session
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    for ddl in [
        "CREATE TABLE a(id INT PRIMARY KEY, x INT, y INT, KEY(x), KEY(y))",
        "CREATE TABLE b(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
        "CREATE TABLE c(id INT PRIMARY KEY, z INT)",
        "CREATE TABLE u(id INT, k INT, v VARCHAR(20), UNIQUE KEY uk(k), KEY(v))",
    ] {
        session.execute_write(ddl).unwrap();
    }
    for sql in [
        "select * from u where k = 1",
        "select * from u where k in (1,2,3)",
        "select * from u where v = 'a'",
        "select * from u where v like 'a%'",
        "select id, row_number() over (partition by x order by y) from a",
        "select x, y from a order by x desc limit 3",
        "select * from a where x = 1 or y = 2",
        "select /*+ use_index_merge(a) */ * from a where x = 1 or y = 2",
        "select x, count(distinct y) from a group by x",
        "select /*+ stream_agg() */ x, sum(y) from a group by x",
        "select * from a where id in (select id from b)",
        "select * from a where id not in (select id from b)",
        "select * from a where exists (select 1 from b where b.id = a.id)",
        "select * from a where x > (select avg(z) from b)",
        "with t as (select x, y from a where y > 1) select * from t join b on t.x = b.id",
        "select distinct x from a",
        "select x + y, abs(x) from a where y between 1 and 10",
        "select * from a where x in (1,2,3,4,5,6,7,8)",
        "select * from a where x in (1,2)",
        "select * from a where id = 1 and x = 2",
        "select * from a where x is null",
        "select * from a where x = y",
        "select a.*, b.z from a join b on a.id = b.id join c on b.id = c.id",
        "select * from a join b on a.x = b.y join c on b.z = c.id where a.y > 1",
        "select /*+ hash_join(a, b) */ * from a join b on a.x = b.y",
        "select * from a right join b on a.id = b.id",
        "select * from a where id > 5 order by id limit 2",
        "select * from a order by x limit 5 offset 2",
        "select max(x), min(y) from a",
        "select * from a union select * from a",
        "select count(*) from a where x > 1 group by y having count(*) > 1",
        "select x from a where x = 1 and y in (select y from b where z = 3)",
    ] {
        rows(&mut session, sql);
    }
    let recorded: std::collections::BTreeMap<String, String> = displayed(rows(
        &mut session,
        "SELECT digest_text, plan_digest FROM information_schema.statements_summary \
         WHERE schema_name = 'test' AND digest_text NOT LIKE '%statements_summary%' \
         AND digest_text NOT LIKE 'set %' AND digest_text NOT LIKE 'drop%' \
         AND digest_text NOT LIKE 'create%'",
    ))
    .into_iter()
    .map(|row| (row[0].clone(), row[1].clone()))
    .collect();
    let expected: [(&str, &str); 30] = [
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `y`",
            "9422e139f8cb8589192103534e1e6b7513fdb528bd83699e698657939ff6cd02",
        ),
        (
            "select * from `a` join `b` on `a` . `x` = `b` . `y` join `c` on `b` . `z` = `c` . `id` where `a` . `y` > ?",
            "c3c1da9ec3073353f92673b3ab1ff31baab954c50fc75814d3ef8e49288d6bf7",
        ),
        (
            "select * from `a` order by `x` limit ? offset ?",
            "aa7c930b8637b9589808d46b5c9ce524a4c0e9dc7e74f37b10e9f01736c04277",
        ),
        (
            "select * from `a` right join `b` on `a` . `id` = `b` . `id`",
            "76c89b9f77f013ca7de0de19a7751008715a384774c35d909e5a9f53d7ff1bc4",
        ),
        (
            "select * from `a` union select * from `a`",
            "9457d36f71476a29dac88288c79fbe278a9c265697092e04b7742f1437703b99",
        ),
        (
            "select * from `a` where `id` = ? and `x` = ?",
            "73937a629211beecade7391365368dbae3d44cc9dba7f53a398f0a5bb700f5e9",
        ),
        (
            "select * from `a` where `id` > ? order by `id` limit ?",
            "0c92331e892af0dfa50609875e3a5be6f7082541ce234cf295c484723078562b",
        ),
        (
            "select * from `a` where `id` in ( select `id` from `b` )",
            "0e1e7baf142e077d27a48d405e83fba1813fb6bf368841cfdeb65103737ecfa9",
        ),
        (
            "select * from `a` where `id` not in ( select `id` from `b` )",
            "9e0ede8bc4e81b6419df9f23b5fb65c4d1d20ed4eeddb99e44235f4c7c3aaeec",
        ),
        (
            "select * from `a` where `x` = ? or `y` = ?",
            "83d9e13aa02429530a794d7501b4aee4ffd0489c651eb2241d3df9e6b2d1d176",
        ),
        (
            "select * from `a` where `x` = `y`",
            "827fa0ddfdb6da9011b78e3562dfa97d75121112e5d9e020864424d3ba866607",
        ),
        (
            "select * from `a` where `x` > ( select avg ( `z` ) from `b` )",
            "04500b49f759e055d5625ad5c4f472f1a4b5c98d9d187400d2aae27d80b5bbd9",
        ),
        (
            "select * from `a` where `x` in ( ... )",
            "eff50ddd607cd7f575c486db304b1f63e65767db109024ea8561b00936a14317",
        ),
        (
            "select * from `a` where `x` is ?",
            "eff50ddd607cd7f575c486db304b1f63e65767db109024ea8561b00936a14317",
        ),
        (
            "select * from `a` where exists ( select ? from `b` where `b` . `id` = `a` . `id` )",
            "c1197fa02e2d5137b3eb5d9b60937668235224282121c7b810e5b7432342b199",
        ),
        (
            "select * from `u` where `k` = ?",
            "29ff78a0ff02e393c46a1134a52a665f9ce7231719e3b0bdf7a0d10945791661",
        ),
        (
            "select * from `u` where `k` in ( ... )",
            "23b7d175a9eb2de45a422305816f197a8d786b3e998d5644844887f7e4b81534",
        ),
        (
            "select * from `u` where `v` = ?",
            "75babbf1e7a60c27a17d0d5b91af7fe8ab5c7fd63eaf416ec81a81e7f52a8543",
        ),
        (
            "select * from `u` where `v` like ?",
            "f98956390255d7c8956504748cebf67e79a106eef191142abac58e4c235d2877",
        ),
        (
            "select `a` . * , `b` . `z` from `a` join `b` on `a` . `id` = `b` . `id` join `c` on `b` . `id` = `c` . `id`",
            "1297ae489c76eb4abc807eddb49a48161e773ea24ff48ace1191d467e0b6f4d7",
        ),
        (
            "select `id` , `row_number` ( ) `over` ( partition by `x` order by `y` ) from `a`",
            "c3ab2025bc7e7467f794ba33577ba173b8173268701bba1fcc971f573dbf4d58",
        ),
        (
            "select `x` + `y` , `abs` ( `x` ) from `a` where `y` between ? and ?",
            "b25f5fbd41ce96ba4c8f158836787f68fc6cac1abad3537d981f6e77c837e94d",
        ),
        (
            "select `x` , `y` from `a` order by `x` desc limit ?",
            "50529e8ff3c19402089a106189d1964052b89078992a617f528106b7ead33818",
        ),
        (
            "select `x` , count ( distinct `y` ) from `a` group by `x`",
            "e6362a0035c238cf0862091d697b6ba96457074ab7cd731904fab7437d2630cc",
        ),
        (
            "select `x` , sum ( `y` ) from `a` group by `x`",
            "235573aa0e74336de87fe3b3b98e601f3c89939d34c78da0f81bd6dc180e370b",
        ),
        (
            "select `x` from `a` where `x` = ? and `y` in ( select `y` from `b` where `z` = ? )",
            "6e118d7bc99503a32f679dd6027d7853057856fee1dbc3328d3575df43c02427",
        ),
        (
            "select count ( ? ) from `a` where `x` > ? group by `y` having count ( ? ) > ?",
            "7e6637b5bc2f3412427d21fda3a187866e7ec58eff40c39b65d0a950a74ebf98",
        ),
        (
            "select distinct `x` from `a`",
            "1cb38efe73f80d5bc368f7fb165b838c01361bcc3093827c0401d291a2c6f7db",
        ),
        (
            "select max ( `x` ) , min ( `y` ) from `a`",
            "bc3ae90d6c3a5cee3cf518d5bd81e7a0096506588c76a1ad2f6be97770983075",
        ),
        (
            "with `t` as ( select `x` , `y` from `a` where `y` > ? ) select * from `t` join `b` on `t` . `x` = `b` . `id`",
            "47c44b86fea9d9ae5e725900bd99ba2eb80ec49b4a758d7d85ee70bc54fabed4",
        )
    ];
    for (text, digest) in expected {
        assert_eq!(
            recorded.get(text).map(String::as_str),
            Some(digest),
            "{text}"
        );
    }
    session
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go's `canPushToIndexPlan` refuses a TopN over a column the index holds only
/// a prefix of (`IsPrefix`), so the TopN sits on the TABLE side of the lookup
/// and orders by the full value.
#[test]
fn top_n_over_a_prefix_index_column_stays_on_the_table_side() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1058)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write(
            "CREATE TABLE tpx (id INT PRIMARY KEY, name VARCHAR(50), INDEX ip (name(3)))",
        )
        .unwrap();
    session
        .execute_write("INSERT INTO tpx VALUES (1,'abcZ'),(2,'abcY'),(3,'abcA'),(4,'abcB')")
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT /*+ use_index(tpx, ip) */ id, name FROM tpx \
         ORDER BY name LIMIT 2",
    ));
    let text = plan
        .iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(text.contains("TopN(Probe)"), "{text}");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ use_index(tpx, ip) */ id, name FROM tpx ORDER BY name LIMIT 2",
        )),
        [["3", "abcA"], ["4", "abcB"]]
    );
}

/// Go runs `checkLockClauseTables` in `Preprocess`, before `tryPointGetPlan`:
/// an `OF` target naming an aliased table by its base name is accepted with
/// warning 1105, the alias itself is silent, and an unknown target is error
/// 1109 -- on the autocommit point-get fast path as much as in the planner.
/// Expected values: Go 1.27 on this branch (`TestZZ` probe).
#[test]
fn for_update_of_checks_its_targets_on_the_point_get_fast_path() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1114)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tfo (id INT PRIMARY KEY, a INT)")
        .unwrap();
    let warnings = |session: &mut ClusterServerSession| -> Vec<Vec<String>> {
        displayed(rows(session, "SHOW WARNINGS"))
    };
    rows(
        &mut session,
        "SELECT ue.id FROM tfo ue WHERE ue.id = 1 FOR UPDATE OF tfo",
    );
    assert_eq!(
        warnings(&mut session),
        [[
            "Warning",
            "1105",
            "FOR UPDATE OF references the base table name while the table is aliased. Use the alias 'ue' in OF to make the lock target explicit."
        ]]
    );
    rows(
        &mut session,
        "SELECT ue.id FROM tfo ue WHERE ue.id = 1 FOR UPDATE OF ue",
    );
    assert!(warnings(&mut session).is_empty());
    let error = session
        .execute("SELECT ue.id FROM tfo ue WHERE ue.id = 1 FOR UPDATE OF zz")
        .err()
        .map(|error| format!("{error:?}"))
        .unwrap_or_default();
    assert!(
        error.contains("Unknown table 'zz' in locking clause"),
        "an unresolvable OF target is error 1109, got {error}"
    );
}

/// Go's `LogicalAggregation.pushDownDNFPredicates`: a HAVING disjunction with
/// an aggregate-output branch cannot move below the aggregate whole, but the
/// group-by parts relax to a disjunction that can (`d_year = 2001 or d_year =
/// 2002` reaches the scan) while the original stays above. A DNF whose every
/// branch reads group-by columns moves entirely. Expected plans: Go 1.27 on
/// this branch (`TestZZ` probe).
#[test]
fn aggregation_pushes_the_relaxed_part_of_a_dnf_filter_below_itself() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1115)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tdd (d_sk INT PRIMARY KEY, d_year INT)")
        .unwrap();
    session
        .execute_write("CREATE TABLE tss (s_date INT, s_amt INT)")
        .unwrap();
    let plan = |session: &mut ClusterServerSession, filter: &str| -> String {
        let sql = format!(
            "EXPLAIN FORMAT = 'brief' SELECT * FROM (SELECT d_year, SUM(s_amt) total FROM tss, tdd \
             WHERE s_date = d_sk GROUP BY d_year) v WHERE {filter}"
        );
        displayed(rows(session, &sql))
            .into_iter()
            .map(|row| row.join(" | "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let relaxed = "or(eq(test.tdd.d_year, 2001), eq(test.tdd.d_year, 2002))";
    let pushed_to_scan = |plan: &str| {
        plan.lines()
            .any(|line| line.contains("cop[tikv]") && line.contains(relaxed))
    };

    // One branch reads the aggregate: the relaxed disjunction reaches the scan
    // and the original is kept above the aggregate.
    let mixed = plan(
        &mut session,
        "(d_year = 2001 AND total > 0) OR d_year = 2002",
    );
    assert!(
        pushed_to_scan(&mixed),
        "the relaxed filter should be pushed to the tdd scan:\n{mixed}"
    );
    assert!(
        mixed.contains("Selection") && mixed.contains("gt(Column#8, 0)"),
        "the original disjunction stays above the aggregate:\n{mixed}"
    );

    // Every branch is a group-by filter: it moves whole, none is left above.
    let pushable = plan(&mut session, "d_year = 2001 OR d_year = 2002");
    assert!(
        pushed_to_scan(&pushable),
        "the group-by disjunction should reach the scan:\n{pushable}"
    );
    assert!(
        !pushable
            .lines()
            .any(|line| line.contains("root") && line.contains("or(")),
        "no root Selection should keep the disjunction:\n{pushable}"
    );
}

/// Go charges a plan by the sum of every operator's `MemoryUsage()` and the
/// plan cache refuses one above `tidb_plan_cache_max_plan_size`
/// (`plan_cacheable_checker.go:588`). The sizes below were measured on Go 1.27
/// of this branch for each statement over `t(a int, b int, c varchar(20))` and
/// `ti(id int primary key, k int, k2 int, v int, key(k), key(k2))`; a plan
/// under 3% above Go's size must still be cached and one 3% below must be
/// refused, which fails if any operator's own terms go missing (the partition
/// info Go holds in the reader and the scan, the scan's filter conditions, the
/// properties, the names).
#[test]
fn plan_cache_admission_follows_go_plan_sizes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1116)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE t (a INT, b INT, c VARCHAR(20))")
        .unwrap();
    session
        .execute_write(
            "CREATE TABLE ti (id INT PRIMARY KEY, k INT, k2 INT, v INT, KEY(k), KEY(k2))",
        )
        .unwrap();
    session
        .execute_write("SET @@tidb_hash_join_concurrency = 1")
        .unwrap();
    let cases: [(&str, i64); 15] = [
        ("select * from t where a=1", 13_753),
        ("select a from t", 8_406),
        ("select * from t where a<10 order by b limit 5", 15_385),
        ("select a, count(*) from t group by a", 12_488),
        ("select * from t order by a", 9_245),
        ("select * from t limit 3", 9_144),
        ("select max(a) from t", 16_911),
        ("select k from ti where k=1", 8_056),
        ("select * from ti where k=1", 12_892),
        (
            "select /*+ use_index_merge(ti) */ * from ti where k=1 or k2=2",
            36_936,
        ),
        ("select a from t union all select b from t", 17_523),
        ("select * from ti where k>2 and v<5", 19_328),
        ("select distinct a from t", 10_385),
        ("select * from t where a in (select k from ti)", 32_745),
        (
            "select * from t t1 where exists (select 1 from ti where ti.k=t1.a)",
            25_883,
        ),
    ];
    let cached_at = |session: &mut ClusterServerSession, sql: &str, max: i64| -> bool {
        session
            .execute_write(&format!("SET @@tidb_plan_cache_max_plan_size = {max}"))
            .unwrap();
        session
            .execute_write(&format!("PREPARE sz FROM '{sql}'"))
            .unwrap();
        rows(session, "EXECUTE sz");
        rows(session, "EXECUTE sz");
        let cached = displayed(rows(session, "SELECT @@last_plan_from_cache"))[0][0] == "1";
        rows(session, "DEALLOCATE PREPARE sz");
        cached
    };
    for (sql, go_size) in cases {
        let above = go_size + go_size * 3 / 100;
        let below = go_size - go_size * 3 / 100;
        assert!(
            cached_at(&mut session, sql, above),
            "{sql}: a plan Go sizes at {go_size} must fit under {above}"
        );
        assert!(
            !cached_at(&mut session, sql, below),
            "{sql}: a plan Go sizes at {go_size} must not fit under {below}"
        );
    }
}

/// `TestPreparedPlanCacheLargePlan`: fifteen UNION ALL legs (1,289,867 bytes
/// to Go) fit the 2 MiB default, thirty (2,579,567) do not until the limit is
/// raised.
#[test]
fn plan_cache_refuses_the_thirty_leg_union_by_default() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1117)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tu (a INT, b INT, c VARCHAR(2048))")
        .unwrap();
    let leg = "select * from tu, tu t1 where t1.c=space(2048) and tu.c=space(2048) and tu.a=t1.b";
    let union = |legs: usize| vec![leg; legs].join(" union all ");
    let cached = |session: &mut ClusterServerSession, legs: usize| -> bool {
        session
            .execute_write(&format!("PREPARE big FROM '{}'", union(legs)))
            .unwrap();
        rows(session, "EXECUTE big");
        rows(session, "EXECUTE big");
        let cached = displayed(rows(session, "SELECT @@last_plan_from_cache"))[0][0] == "1";
        rows(session, "DEALLOCATE PREPARE big");
        cached
    };
    assert!(cached(&mut session, 15));
    assert!(!cached(&mut session, 30));
    session
        .execute_write("SET @@tidb_plan_cache_max_plan_size = 1073741824")
        .unwrap();
    assert!(cached(&mut session, 30));
}

/// Go reads a table through a `LogicalUnionScan` when the open transaction has
/// written it, and always for a local temporary table; the plan shows a
/// `UnionScan` above the reader, a `Point_Get` loses it again
/// (`eliminateUnionScanAndLock`), and a temporary table's scans cost nothing
/// (`tidb_temp_table_factor`), so an ordered read prefers its index to a sort.
/// Expected shapes: Go 1.27 on this branch (`TestZZ` probe, 26 statements).
#[test]
fn dirty_and_temporary_tables_are_read_through_a_union_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1118)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tus (a INT PRIMARY KEY, b INT, c INT, KEY(b))")
        .unwrap();
    session
        .execute_write("INSERT INTO tus VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    let operators = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .split('_')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    };
    // A clean table has no union scan.
    assert_eq!(
        operators(&mut session, "SELECT * FROM tus WHERE b = 1")[0],
        "IndexLookUp"
    );
    session.control_transaction("BEGIN").unwrap();
    session
        .execute_write("INSERT INTO tus VALUES (4,4,4)")
        .unwrap();
    let dirty = operators(&mut session, "SELECT * FROM tus WHERE b = 1");
    assert_eq!(&dirty[..2], ["UnionScan", "IndexLookUp"], "{dirty:?}");
    // A point get reads its own transaction's rows and carries no union scan.
    assert_eq!(
        operators(&mut session, "SELECT * FROM tus WHERE a = 1"),
        ["Point"]
    );
    let ordered = operators(&mut session, "SELECT * FROM tus ORDER BY b");
    assert_eq!(&ordered[..2], ["Sort", "UnionScan"], "{ordered:?}");
    let limited = operators(&mut session, "SELECT * FROM tus WHERE b > 1 LIMIT 1");
    assert_eq!(&limited[..2], ["Limit", "UnionScan"], "{limited:?}");
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        operators(&mut session, "SELECT * FROM tus WHERE b = 1")[0],
        "IndexLookUp"
    );

    // A local temporary table is always read through a union scan, and its
    // free scans make the index the cheaper way to deliver an order.
    session
        .execute_write("CREATE TEMPORARY TABLE ttus (a INT PRIMARY KEY, b INT, KEY(b))")
        .unwrap();
    let temp = operators(&mut session, "SELECT * FROM ttus WHERE b = 1");
    assert_eq!(&temp[..2], ["UnionScan", "IndexReader"], "{temp:?}");
    assert_eq!(
        operators(&mut session, "SELECT * FROM ttus WHERE a = 1"),
        ["Point"]
    );
    let temp_ordered = operators(&mut session, "SELECT * FROM ttus ORDER BY b");
    assert_eq!(
        &temp_ordered[..2],
        ["UnionScan", "IndexReader"],
        "{temp_ordered:?}"
    );
}

/// `executor/union_scan` `TestUpdateScanningHandles`: the inner side of an
/// index join over a table the transaction wrote is `UnionScan -> reader`, which
/// has to be rebuilt per probe like any other inner plan. Go updates every
/// row `a > 10` to `b = a - 10`.
#[test]
fn index_join_inner_side_under_a_union_scan_updates_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1119)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tij (a INT PRIMARY KEY, b INT)")
        .unwrap();
    let values = (2..100)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute_write(&format!("INSERT INTO tij VALUES {values}"))
        .unwrap();
    session.control_transaction("BEGIN").unwrap();
    session
        .execute_write("INSERT INTO tij VALUES (1, 1)")
        .unwrap();
    session
        .execute_write(
            "UPDATE /*+ INL_JOIN(t1) */ tij t1, (SELECT a, b FROM tij) t2 \
             SET t1.b = t2.b WHERE t1.a = t2.a + 10",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM tij WHERE a > 10 AND a - b != 10"
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM tij WHERE a > 10 AND a - b = 10"
        )),
        [["89"]]
    );
    session.control_transaction("ROLLBACK").unwrap();
}

/// An aggregate over an empty input keeps Go's `ndv >= 1` row estimate, a
/// `TableDual` is rebuilt (and takes a fresh plan id) for every parent
/// enumeration, and a constant scalar subquery draws its output column from
/// the same stream Go does: its `DoOptimize` runs without the rules only an
/// Apply turns on. Expected rows: Go 1.27 on this branch (`TestZZ` probe,
/// `select count(*) from t where 1=0` and `a > (select 1 from t where 1=0)`).
#[test]
fn empty_input_aggregation_and_scalar_subquery_ids_match_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1119)).unwrap();
    session.execute_write("USE test").unwrap();
    session
        .execute_write("CREATE TABLE tdz (a INT, b VARCHAR(10), c DECIMAL(10,2))")
        .unwrap();
    let plan = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .map(|row| format!("{} {} {} {}", row[0], row[1], row[2], row[5]))
            .collect()
    };
    assert_eq!(
        plan(&mut session, "SELECT COUNT(*) FROM tdz WHERE 1 = 0"),
        [
            "StreamAgg_11 1.00 49.90 funcs:count(1)->Column#6",
            "└─TableDual_16 0.00 0.00 rows:0",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "SELECT a, 1 + a FROM tdz WHERE a > (SELECT 1 FROM tdz WHERE 1 = 0)"
        ),
        [
            "Projection_14 0.00 10.08 test.tdz.a, plus(1, test.tdz.a)->Column#13",
            "└─TableDual_15 0.00 0.00 rows:0",
            "ScalarSubQuery_11 N/A N/A Output: ScalarQueryCol#12",
            "└─MaxOneRow_8 1.00 0.10 ",
            "  └─Projection_9 0.00 0.10 1->Column#11",
            "    └─TableDual_10 0.00 0.00 rows:0",
        ]
    );
}

/// Plan ids and a dirty partitioned table's shape follow Go: the skyline's
/// preferRange filter runs before any candidate draws a plan id, `Delete`
/// takes its id before the pruning projection, `INSERT ... SELECT` draws the
/// target's column ids first, and a table the transaction wrote in is read
/// through a `UnionScan` carrying the partition id. Expected rows: Go 1.27 on
/// this branch (`TestZZ` probes `US9`, `US16`, `US17`).
#[test]
fn plan_ids_and_dirty_partition_shapes_match_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1120)).unwrap();
    session.execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tpi (a INT PRIMARY KEY, b INT, c INT, KEY(c))",
        "CREATE TABLE tpd (a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY(b), KEY(c))",
        "CREATE TABLE tpp (a INT PRIMARY KEY, b INT) PARTITION BY HASH(a) PARTITIONS 3",
        "INSERT INTO tpd VALUES (1,1,1),(2,2,2),(3,3,3)",
        "INSERT INTO tpp VALUES (1,1),(2,2),(3,3)",
    ] {
        session.execute_write(ddl).unwrap();
    }
    let plan = |session: &mut ClusterServerSession, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .filter(|row| row.len() > 5)
            .map(|row| format!("{} {} {} {}", row[0], row[1], row[2], row[5]))
            .collect()
    };
    assert_eq!(
        plan(&mut session, "SELECT * FROM tpi WHERE c = 1")[0],
        "IndexLookUp_7 10.00 19521.54 "
    );
    assert_eq!(
        plan(&mut session, "DELETE FROM tpd WHERE c = 1")[..2],
        ["Delete_3 N/A N/A N/A", "└─IndexLookUp_8 10.00 19521.54 "]
    );
    assert_eq!(
        plan(
            &mut session,
            "INSERT INTO tpd SELECT a + 10, b + 10, c FROM tpd"
        )[1],
        "└─Projection_5 10000.00 630395.30 plus(test.tpd.a, 10)->Column#8, \
         plus(test.tpd.b, 10)->Column#9, test.tpd.c"
    );
    session.control_transaction("BEGIN").unwrap();
    session
        .execute_write("INSERT INTO tpp VALUES (4,4)")
        .unwrap();
    assert_eq!(
        plan(&mut session, "SELECT * FROM tpp WHERE a > 1")[..3],
        [
            "Projection_5 3333.33 93418.22 test.tpp.a, test.tpp.b",
            "└─UnionScan_7 3333.33 92752.88 gt(test.tpp.a, 1)",
            "  └─TableReader_9 3333.33 92752.88 data:TableRangeScan_8",
        ]
    );
    assert_eq!(
        plan(&mut session, "SELECT COUNT(*) FROM tpp")[..3],
        [
            "HashAgg_8 1.00 588672.28 funcs:count(1)->Column#5",
            "└─UnionScan_11 10000.00 387557.30 ",
            "  └─TableReader_13 10000.00 387557.30 data:TableFullScan_12",
        ]
    );
    session.control_transaction("ROLLBACK").unwrap();
}
