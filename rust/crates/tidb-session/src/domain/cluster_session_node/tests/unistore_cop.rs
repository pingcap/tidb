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
//! Every other module in this directory serves `kv_client: None`, so a plan
//! that only misbehaves when base-table scans are answered by the pushdown
//! coprocessor -- the executor's DAG sent through the in-process unistore
//! client -- never fails in-tree. This module builds the same stack `--store unistore`
//! boots and pins those plans.

use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use crate::domain::cluster_session_node::DomainSessions as _;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::{Datum, MySqlDuration};
use tidb_ddl_notifier::{
    publish_schema_change_to_store, DdlNotifier, Handler, SchemaChangeEvent, SessionPool, Store,
    TEST_HANDLER_ID,
};
use tidb_goruntime::go_runtime::GoSharedSlice;
use tidb_model::partition::{PartitionDefinition, PartitionInfo};

use tidb_domain_core::domain::ddl_notifier::{ClusterNotifierSessionPool, ClusterNotifierTableStore};
use super::super::{
    ClusterHistoricalStatsHandle, ClusterPriorityQueueSource, Domain, Session,
};
use super::node_fixture::{rows, session_context, ABC_HASH};
use crate::domain::configured_user_store::ConfiguredUserStore;
use crate::domain::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};

mod go_executor_protocol;

pub(super) fn cop_backed_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    cop_backed_stack_with_stats_lease(None)
}

fn cop_backed_stack_with_stats_lease(
    stats_lease: Option<tidb_domain_core::node_config::StatsLease>,
) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    let mut config = tidb_domain_core::node_config::NodeConfig::with_defaults(
        |config| {
            config.store = tidb_config::store::StoreType("unistore".to_owned());
            config.port = 0;
        },
        tidb_domain_core::node_config::NodeOptions {
            // A requirement of the account source only: the test passes its
            // own user store below, so the option never names real rows.
            auth_file: Some("/dev/null".to_owned()),
            ..tidb_domain_core::node_config::NodeOptions::default()
        },
    )
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

pub(super) fn displayed(rows: Vec<Vec<Datum>>) -> Vec<Vec<String>> {
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
                        tidb_goruntime::go_strconv::format_float(f64::from(v), b'f', -1, 32)
                    }
                    Datum::Null => "NULL".to_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

/// A node over a store bootstrapped in either kernel's layout serves the
/// tables it finds there by name: advisory locks, `mysql.*` reads and DDL
/// all work, and a user table takes the id Go's kernel would give it (117
/// after the classic bootstrap's 116 ids, 8 after the next-gen one's seven).
#[test]
fn a_node_serves_a_store_of_either_layout_by_name() {
    use crate::mysql_bootstrap::BootstrapLayout;
    if crate::domain::isolate_process_globals() {
        return;
    }
    for (layout, first_user_table) in [
        (BootstrapLayout::Classic, "117"),
        (BootstrapLayout::NextGen, "8"),
    ] {
        crate::bootstrap_publish::override_layout_on_this_thread(Some(layout));
        let (stack, _users) = cop_backed_stack();
        let mut session = stack.factory.open_session(session_context(130)).unwrap();
        assert_eq!(
            displayed(rows(&mut session, "select get_lock('layout_lock', 0)")),
            vec![vec!["1".to_owned()]],
            "{layout:?}"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                "select count(*) from information_schema.tables where table_schema = 'mysql' \
                 and table_name = 'advisory_locks'"
            )),
            vec![vec!["1".to_owned()]],
            "{layout:?}"
        );
        assert!(
            !displayed(rows(&mut session, "select user from mysql.user")).is_empty(),
            "{layout:?}"
        );
        rows(&mut session, "create table test.layout_t (a int)");
        assert_eq!(
            displayed(rows(
                &mut session,
                "select tidb_table_id from information_schema.tables where table_name = 'layout_t'"
            )),
            vec![vec![first_user_table.to_owned()]],
            "{layout:?}"
        );
        // The writers of the accounts, the global variables and the
        // bindings, over the tables this layout stored (common handles for
        // `mysql.user`, `mysql.db`, `mysql.tables_priv`, `mysql.global_variables`
        // ... under the next-gen layout).
        rows(&mut session, "create user layout_u identified by 'x'");
        rows(&mut session, "grant select on test.layout_t to layout_u");
        rows(&mut session, "grant create user on *.* to layout_u");
        let grants = displayed(rows(&mut session, "show grants for layout_u"));
        assert!(
            grants.iter().any(|row| row[0].contains("SELECT")),
            "{layout:?}: {grants:?}"
        );
        rows(&mut session, "alter user layout_u identified by 'y'");
        rows(&mut session, "set global max_connections = 777");
        assert_eq!(
            displayed(rows(&mut session, "select @@global.max_connections")),
            vec![vec!["777".to_owned()]],
            "{layout:?}"
        );
        rows(
            &mut session,
            "create global binding for select * from test.layout_t using select * from test.layout_t",
        );
        assert_eq!(
            displayed(rows(&mut session, "show global bindings")).len(),
            1,
            "{layout:?}"
        );
        rows(&mut session, "revoke select on test.layout_t from layout_u");
        rows(&mut session, "drop user layout_u");
        rows(&mut session, "set global max_connections = default");
    }
    crate::bootstrap_publish::override_layout_on_this_thread(None);
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
            .node_conn()
            .control_transaction(&format!("BEGIN {mode}"))
            .unwrap();
        // A later SET must not change this transaction's commit assertion level.
        rows(
            &mut session,
            &format!("SET tidb_txn_assertion_level = '{later}'"),
        );
        session
            .node()
            .buffer
            .stage_owned_batch([(
                tidb_txnkv::Key::from_bytes(format!("activation_assertion_{attempt}").into_bytes()),
                Some(b"value".to_vec()),
                false,
                tidb_txnkv::AssertionOp::AssertExist,
            )])
            .unwrap();
        let result = session.node_conn().control_transaction("COMMIT");
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
    if crate::domain::isolate_process_globals() {
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
                        .node_conn()
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
                        .node_conn()
                        .execute_write("UPDATE test.activation_protocol SET v=v+1 WHERE id=1")
                        .unwrap();
                }
                mode => {
                    session
                        .node_conn()
                        .control_transaction(&format!("BEGIN {mode}"))
                        .unwrap();
                    session
                        .node_conn()
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
                    session.node_conn().control_transaction("COMMIT").unwrap();
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
                    .node_conn()
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
            .node_conn()
            .execute_write(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    // Go `checkForNullValue`: a NULL row refuses NOT NULL.
    let error = session
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN c INT NOT NULL DEFAULT 7 COMMENT 'cc'")
        .unwrap();
    // Go `checkModifyColumnData`: the first row the new length cannot hold.
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN b VARCHAR(1)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'b', value is 'yy'")
    );
    session
        .node_conn()
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
    let error = session.node_conn().execute_write(add).unwrap_err();
    assert_eq!((error.code, error.message.as_str()), (1452, violation));
    rows(&mut session, "DELETE FROM test.child WHERE id = 2");
    session.node_conn().execute_write(add).unwrap();
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
        .node_conn()
        .execute_write("INSERT INTO test.child VALUES (3, 9)")
        .unwrap_err();
    assert_eq!((error.code, error.message.as_str()), (1452, violation));
    let error = session.node_conn().execute_write(add).unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1826, "Duplicate foreign key constraint name 'fk_2'")
    );
    let drop = "ALTER TABLE test.child DROP FOREIGN KEY fk_2";
    session.node_conn().execute_write(drop).unwrap();
    let error = session.node_conn().execute_write(drop).unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1091, "Can't DROP 'fk_2'; check that column/key exists")
    );
    session
        .node_conn()
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
    session
        .node_conn()
        .execute_write("ALTER TABLE test.c1 CACHE")
        .unwrap();
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                8242,
                format!("'{operation}' is unsupported on cache tables.").as_str()
            ),
            "{sql}"
        );
    }
    session
        .node_conn()
        .execute_write("ALTER TABLE test.c1 CACHE")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE test.c1 NOCACHE")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c1"));
    assert!(!created[0][1].contains("CACHED ON"), "{}", created[0][1]);
    session
        .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
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
            .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
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
            .node_conn()
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
        .node_conn()
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
    // Adding a partial index to an existing table needs Go's ingest
    // reorganization; without Lightning the owner picks txn-merge and
    // cancels the job (Go `checkAndBuildIndexInfo`'s refusal), as the Go
    // oracle answers on the embedded store.
    let error = session
        .node_conn()
        .execute_write("CREATE INDEX ib ON test.t (b) WHERE a < 5")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8200,
            "Unsupported add partial index: add partial index without fast reorg is not supported"
        )
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
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
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE test.t DROP INDEX ia")
        .unwrap();
    session
        .node_conn()
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
        let error = session.node_conn().execute_write(&sql).unwrap_err();
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

/// A store answers a split with `EpochNotMatch` carrying the new regions'
/// memcomparable-encoded bounds, and client-go's V1 codec decodes them in
/// place before they reach the region cache. A Cop read's error went in
/// encoded, and a raw key just past a split (`t<id2>_r`, whose ninth byte is
/// below the encoded bound's 0xFF group marker) then sorted inside the
/// encoded left region: the new table's reads went to a region that does not
/// hold them, so `COUNT(*)` answered 0 and snapshot reads failed with
/// `InvalidReqRange` until the node restarted.
#[test]
fn a_split_seen_by_a_cop_read_keeps_routing_new_tables_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(141)).unwrap();
    rows(&mut session, "CREATE TABLE test.t1 (a INT, b INT)");
    rows(&mut session, "INSERT INTO test.t1 VALUES (1, 1), (2, NULL), (3, 3)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM test.t1")),
        [["3"]]
    );
    for table in ["t2", "t3"] {
        // CREATE TABLE splits the region the cop read above cached, so the
        // next cop read of `t1` meets the split as an `EpochNotMatch`.
        rows(
            &mut session,
            &format!("CREATE TABLE test.{table} (a INT, b INT, KEY ia (a))"),
        );
        assert_eq!(
            displayed(rows(&mut session, "SELECT COUNT(*) FROM test.t1")),
            [["3"]]
        );
        rows(
            &mut session,
            &format!("INSERT INTO test.{table} VALUES (1, 1), (2, NULL)"),
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT COUNT(*), COUNT(b), COUNT(1) FROM test.{table} IGNORE INDEX (ia)"
                ),
            )),
            [["2", "1", "2"]]
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT COUNT(*) FROM test.{table} USE INDEX (ia)"),
            )),
            [["2"]]
        );
    }
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
        .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c"));
    assert!(created[0][1].contains(keys), "{}", created[0][1]);
    session
        .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
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
    session.node_conn().control_transaction("BEGIN").unwrap();
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
    session.node_conn().control_transaction("ROLLBACK").unwrap();
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
    if crate::domain::isolate_process_globals() {
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
            .node_conn()
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
    first
        .node_conn()
        .execute_write("SET tidb_enable_1pc = OFF")
        .unwrap();
    let before = tikv_client::metrics::get_txn_commit_counter();
    first
        .node_conn()
        .execute_write("UPDATE test.activation_light SET v = v + 1 WHERE id=1")
        .unwrap();
    let delta = tikv_client::metrics::get_txn_commit_counter().subtract(before);
    assert_eq!((delta.two_pc, delta.async_commit, delta.one_pc), (1, 0, 0));
    first.node_conn().execute_write("SET tidb_enable_1pc = ON, tidb_txn_assertion_level = 'STRICT', tidb_pessimistic_txn_fair_locking = OFF").unwrap();
    first
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .unwrap();
    first
        .node_conn()
        .execute_write("UPDATE test.activation_light SET v = v + 1 WHERE id=1")
        .unwrap();
    first
        .node_conn()
        .execute_write("SET tidb_enable_1pc = OFF")
        .unwrap();
    let before = tikv_client::metrics::get_txn_commit_counter();
    first.node_conn().control_transaction("COMMIT").unwrap();
    let delta = tikv_client::metrics::get_txn_commit_counter().subtract(before);
    assert_eq!((delta.two_pc, delta.async_commit, delta.one_pc), (0, 0, 1));
    let error = first
        .node_conn()
        .execute_write("SET tidb_txn_assertion_level = 'invalid'")
        .unwrap_err();
    assert_eq!(error.code, 1231);
}

#[test]
fn cluster_account_expiry_survives_sql_writes_and_password_changes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(902)).unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE USER 'expiry_probe'@'%' IDENTIFIED BY 'pw' PASSWORD EXPIRE INTERVAL 7 DAY",
        )
        .unwrap();
    let policy = |session: &mut Session| {
        displayed(rows(session, "SELECT Password_lifetime, Password_last_changed FROM mysql.user WHERE User='expiry_probe'"))
    };
    assert_eq!(policy(&mut session)[0][0], "7");
    session.node_conn().execute_write("UPDATE mysql.user SET Password_last_changed='2001-02-03 04:05:06' WHERE User='expiry_probe'").unwrap();
    session
        .node_conn()
        .execute_write("ALTER USER 'expiry_probe'@'%' ACCOUNT LOCK")
        .unwrap();
    assert_eq!(policy(&mut session), [["7", "2001-02-03 04:05:06"]]);
    session
        .node_conn()
        .execute_write("ALTER USER 'expiry_probe'@'%' PASSWORD EXPIRE NEVER")
        .unwrap();
    assert_eq!(policy(&mut session), [["0", "2001-02-03 04:05:06"]]);
    session
        .node_conn()
        .execute_write("ALTER USER 'expiry_probe'@'%' PASSWORD EXPIRE DEFAULT")
        .unwrap();
    assert_eq!(policy(&mut session), [["NULL", "2001-02-03 04:05:06"]]);
    session
        .node_conn()
        .execute_write("ALTER USER 'expiry_probe'@'%' IDENTIFIED BY 'changed'")
        .unwrap();
    assert_ne!(policy(&mut session)[0][1], "2001-02-03 04:05:06");
    session
        .node_conn()
        .execute_write("SET PASSWORD FOR 'expiry_probe'@'%' = 'changed-again'")
        .unwrap();
    assert_eq!(policy(&mut session)[0][0], "NULL");
}

#[test]
fn cluster_account_history_rejects_reuse_without_changing_credentials() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(903)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE USER 'history_probe'@'%' IDENTIFIED BY 'first' PASSWORD HISTORY 3")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER USER 'history_probe'@'%' IDENTIFIED BY 'second'")
        .unwrap();
    let before = displayed(rows(
        &mut session,
        "SELECT authentication_string FROM mysql.user WHERE User='history_probe'",
    ));
    let error = session
        .node_conn()
        .execute_write("ALTER USER 'history_probe'@'%' IDENTIFIED BY 'first'")
        .unwrap_err();
    assert_eq!(error.code, 3638);
    assert_eq!(error.message, "Cannot use these credentials for 'history_probe@%' because they contradict the password history policy.");
    session
        .node_conn()
        .execute_write("CREATE USER 'history_other'@'%' IDENTIFIED BY 'old' PASSWORD HISTORY 2")
        .unwrap();
    let other_before = displayed(rows(
        &mut session,
        "SELECT authentication_string FROM mysql.user WHERE User='history_other'",
    ));
    assert_eq!(session.node_conn().execute_write("ALTER USER 'history_other'@'%' IDENTIFIED BY 'new', 'history_probe'@'%' IDENTIFIED BY 'first'").unwrap_err().code, 3638);
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
        .node_conn()
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
        .node_conn()
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
            .node_conn()
            .execute_write("SET PASSWORD FOR 'history_renamed'@'%' = 'first'")
            .unwrap_err()
            .code,
        3638
    );
    session
        .node_conn()
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
        .node_conn()
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
    session.node_conn().execute_write("CREATE USER 'locking_probe'@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 7 COMMENT 'keep'").unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write(r#"UPDATE mysql.user SET User_attributes='{"Password_locking":{"failed_login_attempts":3,"password_lock_time_days":7,"failed_login_count":2,"auto_account_locked":"Y","auto_locked_last_changed":"Sat Feb  3 04:05:06 UTC 2001"},"metadata":{"comment":"keep"},"additional_password":"preserve"}' WHERE User='locking_probe'"#).unwrap();
    session
        .node_conn()
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
        .execute_write("CREATE USER 'drop_probe'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    session
        .node_conn()
        .execute_write("GRANT SELECT ON test.* TO 'drop_probe'@'%'")
        .unwrap();
    session
        .node_conn()
        .execute_write("DROP USER 'drop_probe'@'%'")
        .unwrap();
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
            .node_conn()
            .execute_write("DROP USER 'drop_probe'@'%'")
            .unwrap_err()
            .code,
        1396
    );
    session
        .node_conn()
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
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        assert_eq!(displayed(rows(&mut session, select)), [["1", "10"]]);
        let error = session
            .node_conn()
            .execute_write("INSERT INTO test.held_assertion VALUES (1,99)")
            .err();
        session.node_conn().control_transaction("ROLLBACK").unwrap();
        assert!(matches!(error, Some(ref error) if error.code == 1062),
            "an already-owned existing key must still reject the INSERT at statement end: {error:?}");
    }
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
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
    session.node_conn().control_transaction("COMMIT").unwrap();
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
        .node_conn()
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
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("rollback reader");
            reader
                .node_conn()
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
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback reader");

    let assert_binding = |reader: &mut crate::Session, expected: &str, phase: &str| {
        let (result, ops) = tidb_kv::storage::capture_storage_ops(|| {
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
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    rows(
        &mut writer,
        "UPDATE mysql.bind_info SET status='disabled', update_time='2026-09-07 00:00:01' WHERE sql_digest='binding_visibility_probe'",
    );
    assert_binding(&mut reader, "1", "reader during uncommitted disable");
    assert_binding(&mut writer, "1", "writer during uncommitted disable");
    writer
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback writer");
    assert_binding(&mut reader, "1", "after rollback");

    for (version, (status, expected)) in [("disabled", "0"), ("enabled", "1"), ("deleted", "0")]
        .into_iter()
        .enumerate()
    {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        rows(&mut writer, &format!("UPDATE mysql.bind_info SET status='{status}', update_time='2026-09-07 00:00:0{}' WHERE sql_digest='binding_visibility_probe'", version + 1));
        writer
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit writer");
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
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    let write = writer
        .node_conn()
        .execute_write("UPDATE test.unchanged_lock SET v=v WHERE v=20")
        .expect("unchanged update")
        .expect("write result");
    assert_eq!(write.affected_rows, 0);
    reader
        .node_conn()
        .execute_write("SET innodb_lock_wait_timeout=1")
        .expect("bounded lock wait");
    reader
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin reader");
    // Go TestSelectForUpdateNoWait and TestInnodbLockWaitTimeout: range
    // locking must preserve NOWAIT, WAIT n and the session timeout.
    for (clause, code) in [("NOWAIT", 3572), ("WAIT 1", 1205), ("", 1205)] {
        let sql = format!("SELECT v FROM test.unchanged_lock WHERE v=20 FOR UPDATE {clause}");
        let result = reader.node_conn().execute(&sql);
        assert!(
            matches!(result, Err(ref error) if error.code == code),
            "locking range {clause} must report {code}; got {:?}",
            result.as_ref().err()
        );
    }
    let result = reader
        .node_conn()
        .execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=1");
    assert!(
        matches!(result, Err(ref error) if error.code == 1205),
        "the unchanged matched row must remain locked; error={:?}",
        result.as_ref().err()
    );
    let untouched = reader
        .node_conn()
        .execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=2")
        .expect("a rejected row is not locked")
        .expect("write result");
    assert_eq!(untouched.affected_rows, 1);
    writer
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("release writer");
    reader
        .node_conn()
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
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    writer
        .node_conn()
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
        .node_conn()
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
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin user transaction");
        writer
            .node_conn()
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
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback user transaction");
        assert_eq!(
            displayed(rows(&mut reader, status)),
            [[expected]],
            "binding command survives caller rollback: {command}"
        );
    }
}

fn handle_stats_schema_change(session: &mut Session, event: &SchemaChangeEvent) {
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("stats subscriber transaction begins");
    let staged = session.node_conn().stage_stats_notifier_event(event);
    if let Err(error) = staged {
        session
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("failed stats subscriber transaction rolls back");
        panic!("stats subscriber handles the source event: {error:?}");
    }
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("stats subscriber transaction commits");
}

// Go statistics DDL tests explicitly HandleNextDDLEventWithTxn before reading
// statistics. This fixture does not campaign the background statistics owner.
fn drain_stats_ddl_events(factory: &Arc<Domain>, session: &mut Session) {
    let notifier = tidb_domain_core::domain::ddl_notifier::build_notifier(factory, Duration::ZERO);
    drain_stats_ddl_events_with_notifier(session, notifier);
}

fn drain_stats_ddl_events_with_notifier(
    session: &mut Session,
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    let notifier = tidb_domain_core::domain::ddl_notifier::build_notifier(&stack.factory, Duration::ZERO);
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
        .node_conn()
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        session.ddl_statement_context().enable_check_constraint(),
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
    let error = match session
        .node_conn()
        .execute("INSERT INTO queued_check VALUES (-1)")
    {
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

    let notifier = tidb_domain_core::domain::ddl_notifier::build_notifier(&factory, Duration::ZERO);
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
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
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
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
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
    inner: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
    processes: tidb_sessmgr::ProcessRegistry,
    kill: Option<bool>,
    process_id: u64,
    factory: std::sync::Weak<Domain>,
    observed: Arc<AtomicBool>,
    interrupted: Arc<AtomicBool>,
}

impl tidb_executor::cluster_analyze_seam::ClusterAnalyze for ObservedSystemAnalyze {
    fn execute(
        &self,
        statement: &tidb_executor::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        killer: &tidb_sqlkiller::SqlKiller,
        historical_stats_enabled: &dyn Fn() -> bool,
        jobs: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
    ) -> Result<tidb_executor::real_tikv_analyze::ClusterAnalyzeReport, tidb_store_driver_error::sql_query_error::SqlQueryError>
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

    fn update_stats_cache(
        &self,
        physical_ids: Vec<i64>,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        self.inner.update_stats_cache(physical_ids)
    }
}

/// Kills the running auto-analyze through SQL `KILL QUERY`, by the process ID
/// the shared process list shows for it, and records that ID.
struct KillingSystemAnalyze {
    inner: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
    processes: tidb_sessmgr::ProcessRegistry,
    factory: std::sync::Weak<Domain>,
    process_id: Arc<AtomicU64>,
}

impl tidb_executor::cluster_analyze_seam::ClusterAnalyze for KillingSystemAnalyze {
    fn execute(
        &self,
        statement: &tidb_executor::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        killer: &tidb_sqlkiller::SqlKiller,
        historical_stats_enabled: &dyn Fn() -> bool,
        jobs: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
    ) -> Result<tidb_executor::real_tikv_analyze::ClusterAnalyzeReport, tidb_store_driver_error::sql_query_error::SqlQueryError>
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

    fn update_stats_cache(
        &self,
        physical_ids: Vec<i64>,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        self.inner.update_stats_cache(physical_ids)
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
            super::super::cluster_stats_session(context).with_session(|session| {
                let analyze = Arc::new(KillingSystemAnalyze {
                    inner: crate::domain::cluster_session_node::node_services(&session.node.domain).analyze(),
                    processes: factory.processes(),
                    factory: Arc::downgrade(&factory),
                    process_id: Arc::clone(&process_id),
                });
                session.set_analyzer(&factory, analyze);
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
    let (gcid, truncated) = tidb_globalconn::parse_conn_id(process_id)
        .expect("an allocator-minted process ID parses");
    assert!(!truncated);
    assert_eq!(
        gcid.server_id,
        tidb_globalconn::SERVER_ID_FOR_STANDALONE,
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
            super::super::cluster_stats_session(context)
                .with_session(|session| {
                    let analyze = Arc::new(ObservedSystemAnalyze {
                        inner: crate::domain::cluster_session_node::node_services(&session.node.domain).analyze(),
                        processes: factory.processes(),
                        kill,
                        process_id,
                        factory: Arc::downgrade(&factory),
                        observed: Arc::clone(&observed),
                        interrupted: Arc::clone(&interrupted),
                    });
                    session.set_analyzer(&factory, analyze);
                    Ok(())
                })
                .unwrap();
            let (register, unregister) = tidb_domain_core::sys_session::system_process_trackers(factory.processes());
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
                    tidb_sqlescape::SqlArg::from("test"),
                    tidb_sqlescape::SqlArg::from("auto_exec_live"),
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

impl tidb_sessmgr::ProcessKillTarget for AutoAnalyzeKillTarget {
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
    let target: Arc<dyn tidb_sessmgr::ProcessKillTarget> =
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(tidb_domain_core::node_config::StatsLease::Zero)
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(tidb_domain_core::node_config::StatsLease::Zero)
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(tidb_domain_core::node_config::StatsLease::Zero)
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
            session
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .unwrap();
            // Restricted SQL locks its planned keys without loading values,
            // then stages them. Both absent and existing records must survive
            // the native client's assertion check from the lock metadata.
            super::super::transactions::stage_pessimistic_statement(
                session.node().txn.explicit.as_deref().unwrap(),
                &session.node().buffer,
                session.lock_wait_timeout(),
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
            session.node_conn().control_transaction("COMMIT").unwrap();
        }
    }
}

/// Pinned DDL subscriber `ActionCreateTable`, `ActionTruncateTable`, and
/// `ActionDropTable`: create every physical table's zero-valued statistics
/// placeholders, then retire the old physical ID by advancing stats_meta.
#[test]
fn table_lifecycle_ddl_updates_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
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
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
        .is_some_and(|stats| !stats.hist_coll.pseudo));

    // Go keeps Domain.StatsHandle independently of an InfoSchema refresh.
    // Demand must still reload evicted payloads after a stats-only refresh
    // and after the DDL below replaces the connection's schema catalog.
    let check_reload = |session: &mut Session| {
        let peer = stack.factory.open_session(session_context(76)).unwrap();
        let peer_catalog = peer.shared_catalog();
        assert!(!peer_catalog
            .lock()
            .unwrap()
            .table_statistics_in(&peer_catalog.stats_handle(), table_id)
            .unwrap()
            .column_load_needed(1, true).1);
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
            tidb_stats_handle_syncload::stats_watch::TableStatsState::Loaded(Arc::new(table)),
        );
        stack.factory.stats().store_after_analyze(snapshot);
        assert!(peer_catalog
            .lock()
            .unwrap()
            .table_statistics_in(&peer_catalog.stats_handle(), table_id)
            .unwrap()
            .column_load_needed(1, true).1);
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
                .table_statistics_in(&peer_catalog.stats_handle(), table_id)
                .unwrap()
                .column_load_needed(1, true).1,
            "the peer still sees an evicted column after the domain load completes"
        );
        assert!(
            Arc::ptr_eq(
                &peer_catalog
                    .lock()
                    .unwrap()
                    .table_statistics_in(&peer_catalog.stats_handle(), table_id)
                    .unwrap(),
                &session
                    .shared_catalog()
                    .lock()
                    .unwrap()
                    .table_statistics_in(&session.stats_handle(), table_id)
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    session.node_conn().rebuild_catalog_if_stale();
    {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().expect("session catalog is available");
        let tidb_infoschema::TableEntry::Kv(table) = catalog
            .table_in("test", "stats_add_column")
            .expect("rebuilt executor table is available")
        else {
            panic!("rebuilt executor table is storage-backed");
        };
        assert!(table
            .columns()
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

/// Go `analyzeColumnsPushDown` samples `FullIntRange` over an integer primary
/// key (an unsigned one split across the int64 boundary into one list, the
/// unsigned half first), `FullNotNullRange` over a clustered key and the
/// signed `FullIntRange` without a handle column -- never the record prefix
/// and its `PrefixNext`, which TiKV's table scan answers as a point get of
/// that prefix: no rows, so `stats_meta.count` 0. Unistore scans any range,
/// so the request is checked at the store's RPC client, as Go's
/// `UnistoreRPCClientSendHook` does.
#[test]
fn analyze_samples_the_go_handle_ranges() {
    use tikv_client::proto::coprocessor;
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    for statement in [
        "CREATE TABLE range_int (a INT PRIMARY KEY, b INT, KEY(b))",
        "CREATE TABLE range_unsigned (a BIGINT UNSIGNED PRIMARY KEY, b INT)",
        "CREATE TABLE range_clustered (a VARCHAR(10), b INT, PRIMARY KEY(a) CLUSTERED)",
        "CREATE TABLE range_rowid (a INT, b INT)",
        "INSERT INTO range_int VALUES (1, 1), (2, 2), (3, 3)",
        "INSERT INTO range_unsigned VALUES \
         (1, 1), (9223372036854775808, 2), (18446744073709551615, 3)",
        "INSERT INTO range_clustered VALUES ('a', 1), ('b', 2), ('c', 3)",
        "INSERT INTO range_rowid VALUES (1, 1), (NULL, 2), (3, 3)",
    ] {
        rows(&mut session, statement);
    }
    let sent: Arc<std::sync::Mutex<Vec<(Vec<u8>, Vec<u8>)>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let Some(request) = request.as_any().downcast_ref::<coprocessor::Request>() else {
            return;
        };
        if request.tp == tidb_unistore_cophandler::REQ_TYPE_ANALYZE {
            recorded.lock().unwrap().extend(
                request
                    .ranges
                    .iter()
                    .map(|range| (range.start.clone(), range.end.clone())),
            );
        }
    })));
    let min_int = vec![0; 8];
    let max_int_next = [[0xff; 8].as_slice(), &[0]].concat();
    let unsigned_boundary = [0x80, 0, 0, 0, 0, 0, 0, 0].to_vec();
    for (table, expected) in [
        ("range_int", vec![(min_int.clone(), max_int_next.clone())]),
        (
            "range_unsigned",
            vec![
                (min_int.clone(), unsigned_boundary.clone()),
                (unsigned_boundary.clone(), max_int_next.clone()),
            ],
        ),
        ("range_clustered", vec![(vec![0x01], vec![0xfb])]),
        ("range_rowid", vec![(min_int.clone(), max_int_next.clone())]),
    ] {
        let table_id: i64 = displayed(rows(
            &mut session,
            &format!(
                "SELECT TIDB_TABLE_ID FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = '{table}'"
            ),
        ))[0][0]
            .parse()
            .expect("table id");
        sent.lock().unwrap().clear();
        rows(&mut session, &format!("ANALYZE TABLE {table}"));
        let prefix = tidb_tablecodec::table_key::gen_table_record_prefix(table_id);
        let expected = expected
            .into_iter()
            .map(|(low, high)| {
                (
                    [prefix.as_slice(), &low].concat(),
                    [prefix.as_slice(), &high].concat(),
                )
            })
            .collect::<Vec<_>>();
        let analyzed = sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(start, _)| start.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(analyzed, expected, "{table}");
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count FROM mysql.stats_meta WHERE table_id = {table_id}"),
            )),
            [["3"]],
            "{table}"
        );
    }
    stack.rpc.set_send_hook(None);
}

/// Go `getAdjustedSampleRate` takes `RealtimeCount` from
/// `GetPhysicalTableStats` -- the statistics cache, or `PseudoTable` on a
/// miss -- not from the `mysql.stats_meta` row itself. With `stats-lease = 0`
/// nothing reloads the cache between statements, so a changed stored count
/// is not what the next ANALYZE's sample-rate note reports.
#[test]
fn analyze_sample_rate_reads_the_statistics_cache() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE rate_cache (a INT PRIMARY KEY, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO rate_cache VALUES (1, 1), (2, 2), (3, 3)",
    );
    let table_id: i64 = displayed(rows(
        &mut session,
        "SELECT TIDB_TABLE_ID FROM information_schema.tables \
         WHERE table_schema = 'test' AND table_name = 'rate_cache'",
    ))[0][0]
        .parse()
        .expect("table id");
    let note = |count: &str| {
        [
            "Note".to_owned(),
            "1105".to_owned(),
            format!(
                "Analyze use auto adjusted sample rate 1.000000 for table test.rate_cache, \
                 reason to use this rate is \"use min(1, 110000/{count}) as the sample-rate=1\""
            ),
        ]
    };
    rows(&mut session, "ANALYZE TABLE rate_cache");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [note("10000")],
        "a table the cache has not loaded is PseudoTable"
    );
    rows(
        &mut session,
        &format!("UPDATE mysql.stats_meta SET count = 1000 WHERE table_id = {table_id}"),
    );
    rows(&mut session, "ANALYZE TABLE rate_cache");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [note("3")],
        "the cache holds what the first ANALYZE saved"
    );
}

/// Pinned storage `TestDeleteAnalyzeJobs` begins with the ordinary ANALYZE
/// lifecycle: the job is inserted pending, run, and retained finished for
/// `SHOW ANALYZE STATUS` until timestamp-based cleanup removes it.
#[test]
fn analyze_job_lifecycle_is_persisted_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    let cutoff = tidb_datatype::current_time::utc_now_timestamp()
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    let started_at = tidb_datatype::current_time::utc_now_timestamp()
        .add_duration(MySqlDuration::from_nanoseconds(-60_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
        auto_ids: Arc::clone(&stack.factory.auto_ids),
    };
    let mut job_id = 0;
    handle
        .commit_stats_plan(|snapshot, _| {
            let (created_job_id, plan) = tidb_stats_handle_autoanalyze::analyze_jobs::plan_insert_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                "test",
                "stats_show_analyze_job",
                "",
                b"analyze table all columns with 256 buckets, 100 topn, 1 samplerate",
                "127.0.0.1:4000",
                78,
                started_at,
                Some(stack.factory.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())?;
            job_id = created_job_id;
            Ok(plan)
        })
        .expect("pending job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_stats_handle_autoanalyze::analyze_jobs::plan_start_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                job_id,
                started_at,
                Some(stack.factory.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
        .expect("running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_stats_handle_autoanalyze::analyze_jobs::plan_update_analyze_job_progress(
                snapshot,
                &stack.factory.catalog.load(),
                job_id,
                3,
                started_at,
                Some(stack.factory.auto_ids.as_ref()),
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
            tidb_charset::Collation::Utf8Mb4Bin,
        )))
    );
    rows(&mut client, "SET GLOBAL time_zone = 'Europe/Berlin'");
    let now = tidb_datatype::current_time::utc_now_timestamp();
    let started_at = now
        .add_duration(MySqlDuration::from_nanoseconds(-2_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&factory.transactions),
        catalog: Arc::clone(&factory.catalog),
        global_vars: factory.global_vars.clone(),
        auto_ids: Arc::clone(&factory.auto_ids),
    };
    let mut job_id = 0;
    handle
        .commit_stats_plan(|snapshot, _| {
            let (created_job_id, plan) = tidb_stats_handle_autoanalyze::analyze_jobs::plan_insert_analyze_job(
                snapshot,
                &factory.catalog.load(),
                "test",
                "t",
                "",
                b"analyze table `test`.`t`",
                "127.0.0.1:4000",
                1,
                started_at,
                Some(factory.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())?;
            job_id = created_job_id;
            Ok(plan)
        })
        .expect("pending job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_stats_handle_autoanalyze::analyze_jobs::plan_start_analyze_job(
                snapshot,
                &factory.catalog.load(),
                job_id,
                started_at,
                Some(factory.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
        .expect("running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_stats_handle_autoanalyze::analyze_jobs::plan_finish_analyze_job(
                snapshot,
                &factory.catalog.load(),
                job_id,
                0,
                Some("simulated failure"),
                now,
                Some(factory.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
        .expect("failed job commits");

    assert_eq!(
        source.scalar("SELECT @@time_zone").unwrap(),
        Some(Datum::String(tidb_datatype::StringDatum::new(
            b"Europe/Berlin".to_vec(),
            tidb_charset::Collation::Utf8Mb4Bin,
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    // Go's test names this instance from `infosync.GetServerInfo()`, as the
    // cleanup does.
    let local = stack
        .factory
        .server_info
        .as_ref()
        .expect("server info")
        .local_server_info();
    let local_instance = tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
        &local.static_info.ip,
        local.static_info.port,
    );
    let local_instance = local_instance.as_str();
    let now = tidb_datatype::current_time::utc_now_timestamp();
    let old = now
        .add_duration(MySqlDuration::from_nanoseconds(-660_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
        auto_ids: Arc::clone(&stack.factory.auto_ids),
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
                let (_, plan) = tidb_stats_handle_autoanalyze::analyze_jobs::plan_insert_analyze_job(
                    snapshot,
                    &stack.factory.catalog.load(),
                    "test",
                    "t",
                    "",
                    b"job",
                    instance,
                    process_id,
                    created_at,
                    Some(stack.factory.auto_ids.as_ref()),
                )
                .map_err(|error| error.to_string())?;
                Ok(plan)
            })
            .expect("pending job commits");
    }
    for id in [2, 3, 4] {
        handle
            .commit_stats_plan(|snapshot, _| {
                tidb_stats_handle_autoanalyze::analyze_jobs::plan_start_analyze_job(
                    snapshot,
                    &stack.factory.catalog.load(),
                    id,
                    old,
                    Some(stack.factory.auto_ids.as_ref()),
                )
                .map_err(|error| error.to_string())
            })
            .expect("running job commits");
    }
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_stats_handle_autoanalyze::analyze_jobs::plan_finish_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                2,
                0,
                None,
                old,
                Some(stack.factory.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
        .expect("temporary finish commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_stats_handle_autoanalyze::analyze_jobs::plan_start_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                2,
                old,
                Some(stack.factory.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
        .expect("NULL-process running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_stats_handle_autoanalyze::analyze_jobs::plan_finish_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                5,
                0,
                None,
                old,
                Some(stack.factory.auto_ids.as_ref()),
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
            tidb_stats_handle_autoanalyze::analyze_jobs::CORRUPTED_ANALYZE_JOB_FAILURE,
        ]]
    );
}

#[test]
fn analyze_job_cleanup_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory.start_analyze_jobs_cleanup_worker(tidb_domain_core::node_config::StatsLease::Zero);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(tidb_domain_core::node_config::StatsLease::Disabled);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(tidb_domain_core::node_config::StatsLease::Positive(
        Duration::from_secs(60),
    ));
    assert!(factory.analyze_jobs_cleanup_worker.get().is_some());
    drop(factory);
}

#[test]
fn auto_analyze_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Zero);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Disabled);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Positive(
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Positive(
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    let assert_no_stats = |session: &mut Session, ids: &[i64]| {
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Go writes a modify-column event only when the rows are rewritten
/// (`doModifyColumnTypeWithData`); a change of metadata alone and a RENAME
/// COLUMN write none, so the statistics subscriber recreates nothing for
/// them (oracle: `mysql.tidb_ddl_notifier` gains a row for `INT -> VARCHAR`
/// only).
#[test]
fn modify_column_ddl_events_follow_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

    let remove_column_stats = |session: &mut Session| {
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
    let notifier_rows = |session: &mut Session| {
        displayed(rows(
            session,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier",
        ))[0][0]
            .clone()
    };
    let assert_absent = |session: &mut Session| {
        assert_eq!(
            displayed(rows(
                session,
                &format!(
                    "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["0"]]
        );
    };

    remove_column_stats(&mut session);
    let before = notifier_rows(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column MODIFY COLUMN a BIGINT",
    );
    assert_eq!(notifier_rows(&mut session), before);
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_absent(&mut session);

    let before = notifier_rows(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column RENAME COLUMN a TO b",
    );
    assert_eq!(notifier_rows(&mut session), before);
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_absent(&mut session);

    // A rewrite does write the event.
    let before = notifier_rows(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column MODIFY COLUMN b VARCHAR(10)",
    );
    let after: i64 = notifier_rows(&mut session).parse().unwrap();
    assert_eq!(after, before.parse::<i64>().unwrap() + 1);
}

/// Pinned `TestRecordHistoryStatsAfterAnalyze`: the global switch suppresses
/// task creation while off; once enabled, a successful ANALYZE posts its
/// physical ID and the domain dump path writes canonical blocks to
/// `mysql.stats_history`.
#[test]
fn analyze_records_historical_stats_through_the_domain_worker() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
            .node()
            .domain
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(&mut session, "ANALYZE TABLE history_analyze WITH 2 TOPN");
    let table_id = session
        .node()
        .domain
        .historical_stats_worker
        .get_one_historical_stats_table();
    assert!(table_id > 0, "a successful ANALYZE posts its table ID");
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
        auto_ids: Arc::clone(&stack.factory.auto_ids),
    };
    session
        .node()
        .domain
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(&mut session, "CREATE TABLE history_gc (a INT)");
    rows(&mut session, "ANALYZE TABLE history_gc");
    let table_id = session
        .node()
        .domain
        .historical_stats_worker
        .get_one_historical_stats_table();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
        auto_ids: Arc::clone(&stack.factory.auto_ids),
    };
    session
        .node()
        .domain
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
        tidb_domain_core::node_config::StatsLease::Positive(Duration::from_millis(1)),
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
        tidb_domain_core::node_config::StatsLease::Zero,
        tidb_domain_core::node_config::StatsLease::Disabled,
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        !partitions[tidb_stats_util::TIDB_GLOBAL_STATS]
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
            .node()
            .domain
            .historical_stats_worker
            .get_one_historical_stats_table(),
        partition_id
    );
    assert_eq!(
        session
            .node()
            .domain
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
            .node()
            .domain
            .historical_stats_worker
            .get_one_historical_stats_table(),
        session
            .node()
            .domain
            .historical_stats_worker
            .get_one_historical_stats_table(),
    ];
    queued.sort_unstable();
    let mut expected = vec![partition_id, logical_id];
    expected.sort_unstable();
    assert_eq!(queued, expected);
    assert_eq!(
        session
            .node()
            .domain
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
        auto_ids: Arc::clone(&stack.factory.auto_ids),
    };
    for table_id in queued {
        session
            .node()
            .domain
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
    assert!(partitions[tidb_stats_util::TIDB_GLOBAL_STATS].is_some());
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
/// `kv_client: None`, and the pipeline session's own `CREATE VIEW` wire test
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
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT v FROM test.txn_lifetime WHERE id = 1");
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (2, 20)");

    rows(
        &mut session,
        "UPDATE test.txn_lifetime SET v = 0 WHERE id = 99",
    );
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (3, 30)");
    for (end, id) in [("COMMIT", 4), ("ROLLBACK", 5)] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        rows(
            &mut session,
            "SELECT v FROM test.txn_lifetime WHERE id = 1 FOR UPDATE",
        );
        session.node_conn().control_transaction(end).unwrap();
        rows(
            &mut session,
            &format!("INSERT INTO test.txn_lifetime VALUES ({id}, {})", id * 10),
        );
    }
    session
        .node_conn()
        .execute_write("SET autocommit = 0")
        .unwrap();
    rows(&mut session, "SELECT v FROM test.txn_lifetime WHERE id = 1");
    session
        .node_conn()
        .execute_write("SET autocommit = 1")
        .unwrap();
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
    let error = match session
        .node_conn()
        .execute("UPDATE test.failed_txn_lifetime SET id = 2 WHERE id = 1")
    {
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
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(
        &mut session,
        "UPDATE test.statement_lifetime SET v = 11 WHERE id = 1",
    );
    session
        .node_conn()
        .control_transaction("SAVEPOINT keep_first")
        .unwrap();
    rows(
        &mut session,
        "INSERT INTO test.statement_lifetime VALUES (3, 30)",
    );
    let error = match session
        .node_conn()
        .execute("UPDATE test.statement_lifetime SET id = 2 WHERE id = 1")
    {
        Err(error) => error,
        Ok(_) => panic!("the conflicting UPDATE must fail"),
    };
    assert_eq!(error.code, 1062);
    assert!(session.node().buffer.native_owner().is_some());
    session
        .node_conn()
        .control_transaction("ROLLBACK TO keep_first")
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
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
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    rows(&mut first, "UPDATE test.race_rw SET v = v + 1 WHERE id = 1");

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(81))
                .expect("session opens");
            assert_eq!(
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's pessimistic row lock until
            // that transaction commits.
            rows(
                &mut second,
                "UPDATE test.race_rw SET v = v + 1 WHERE id = 1",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        // Give the contender time to reach the lock wait, so the interesting
        // interleaving -- blocked UPDATE, then the winner's COMMIT -- is the
        // one exercised. The assertions hold under any interleaving.
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
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
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
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
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's lock, then retries at an
            // advanced `for_update_ts`.
            rows(
                &mut second,
                "UPDATE test.retry_range SET v = v + 1 WHERE v > 0",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
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
        reader
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
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
    let _ = reader
        .node_conn()
        .execute("SELECT v FROM test.lock_cache WHERE id = 1 FOR UPDATE");

    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.lock_cache WHERE id = 1"
        )),
        [["10"]],
        "the plain read that follows still reads at start_ts: the lock cache \
         belongs to locking reads only (`point_get.go:677`)"
    );
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
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
        .node_conn()
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
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut second,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );
    second
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

    // The first transaction's prewrite now finds the newer commit: 9007.
    let refused = first
        .node_conn()
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

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    rows(
        &mut session,
        "UPDATE test.implicit_commit SET v = 7 WHERE id = 1",
    );
    // No COMMIT: the next BEGIN carries it implicitly.
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("BEGIN with an open transaction implicitly commits, never conflicts");
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

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
    use crate::domain::resultset_source::ResultSetSource;
    use tidb_param::PreparedValue;

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
            .node_conn().prepare_general(&format!(
                "SELECT * FROM test.{table} WHERE id {comparison} ?"
            ))
            .expect("prepare");
        let select = |session: &mut Session| {
            let crate::domain::session_api::GeneralExecuteOutcome::Rows(mut result) = session
                .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
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
    use tidb_param::PreparedValue;

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
        .node_conn().prepare_general("UPDATE test.prepared_schema SET v = v + 1 WHERE id = ?")
        .expect("prepare");
    writer
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("populate prepared cache");
    // Keep the prepared handle alive while a separate connection publishes DDL.
    rows(
        &mut ddl,
        "ALTER TABLE test.prepared_schema ADD COLUMN extra INT DEFAULT 7",
    );
    writer
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
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
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
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
/// Go `decideTxnMode` with `pessimistic-auto-commit = true` makes an
/// autocommit UPDATE's implicit transaction pessimistic, so
/// `tryUpdatePointPlan` plans its point source with `Lock` and
/// `PointGetExecutor.getAndLock` locks the row with the session's
/// `LockWaitTimeout` (`getLockWaitTime`): a row another transaction holds
/// makes the statement wait the lock out. Under the default (optimistic)
/// mode the same statement takes no lock and reads the committed row.
#[test]
fn a_pessimistic_autocommit_point_update_locks_from_its_plan() {
    use tidb_param::PreparedValue;

    struct ConfigGuard;
    impl Drop for ConfigGuard {
        fn drop(&mut self) {
            tidb_config::config_tree::config::update_global(|config| {
                config.pessimistic_txn.pessimistic_auto_commit =
                    tidb_config::config_tree::AtomicBool::new(false);
            });
        }
    }

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(103))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.acpess (id int primary key, v int)",
    );
    rows(&mut holder, "INSERT INTO test.acpess VALUES (1, 10)");
    let mut writer = factory
        .open_session(session_context(104))
        .expect("session opens");
    writer
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    let statement = writer
        .node_conn().prepare_general("UPDATE test.acpess SET v = v + 1 WHERE id = ?")
        .expect("prepare");

    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.acpess WHERE id = 1 FOR UPDATE",
    );
    let _guard = ConfigGuard;
    tidb_config::config_tree::config::update_global(|config| {
        config.pessimistic_txn.pessimistic_auto_commit =
            tidb_config::config_tree::AtomicBool::new(true);
    });
    let error = match writer.node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)]) {
        Err(error) => error,
        Ok(_) => panic!("the held row makes the pessimistic autocommit UPDATE wait"),
    };
    assert_eq!(error.code, 1205, "{}", error.message);
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback holder");
    assert_eq!(
        displayed(rows(&mut holder, "SELECT v FROM test.acpess WHERE id = 1")),
        [["10"]]
    );
}

/// Go `LazyTxn.LockKeys` marks the transaction `LockWaiting` while its lock
/// request waits (`updateState(TxnLockAcquiring)`), including the lock a
/// point read takes inside `PointGetExecutor.Next`: `TIDB_TRX` shows it.
#[test]
fn a_point_lock_inside_execution_shows_the_transaction_lock_waiting() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(105))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.lockstate (id int primary key, v int)",
    );
    rows(&mut holder, "INSERT INTO test.lockstate VALUES (1, 1)");
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.lockstate WHERE id = 1 FOR UPDATE",
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let state = std::thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let mut writer = factory
                .open_session(session_context(106))
                .expect("session opens");
            writer
                .node_conn()
                .execute_write("SET SESSION innodb_lock_wait_timeout = 3")
                .expect("set lock wait");
            writer
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin writer");
            started_tx.send(()).expect("writer started");
            let result = writer
                .node_conn()
                .execute_write("UPDATE test.lockstate SET v = 2 WHERE id = 1");
            let _ = writer.node_conn().control_transaction("ROLLBACK");
            result.map(|_| ())
        });
        started_rx.recv().expect("writer starts");
        let mut observer = factory
            .open_session(session_context(107))
            .expect("session opens");
        let mut state = Vec::new();
        for _ in 0..40 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            state = displayed(rows(
                &mut observer,
                "SELECT STATE FROM information_schema.TIDB_TRX WHERE SESSION_ID = 106",
            ));
            if state == [["LockWaiting"]] {
                break;
            }
        }
        holder
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("release holder");
        waiter
            .join()
            .expect("writer thread")
            .expect("the writer's lock is granted once the holder leaves");
        state
    });
    assert_eq!(state, [["LockWaiting"]]);
}

/// Go `tidb_last_query_info.for_update_ts` is `TxnCtx.GetForUpdateTS` as the
/// statement left it: a non-point pessimistic UPDATE fetches a fresh
/// for-update timestamp (`GetStmtForUpdateTS`) above the transaction's start.
#[test]
fn a_pessimistic_range_update_reports_its_advanced_for_update_ts() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(108))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.futs (id int primary key, v int)",
    );
    rows(&mut session, "INSERT INTO test.futs VALUES (1, 1), (2, 2)");
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    session
        .node_conn()
        .execute_write("UPDATE test.futs SET v = v + 1 WHERE v > 0")
        .expect("update");
    let info = rows(&mut session, "SELECT @@tidb_last_query_info");
    let tidb_datatype::Datum::String(info) = &info[0][0] else {
        panic!("tidb_last_query_info is a string: {info:?}");
    };
    let info: serde_json::Value =
        serde_json::from_slice(info.bytes()).expect("tidb_last_query_info is JSON");
    let start_ts = info["start_ts"].as_u64().expect("start_ts");
    let for_update_ts = info["for_update_ts"].as_u64().expect("for_update_ts");
    assert!(
        for_update_ts > start_ts,
        "for_update_ts {for_update_ts} must be the statement's fresh timestamp above {start_ts}"
    );
    session
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
}

/// Go's locking point reads lock inside `Next` exactly the keys their
/// isolation names (`PointGetExecutor.Next` / `getAndLock`,
/// `BatchPointGetExec.initialize`): repeatable read locks a unique index key
/// and the row it names, and every named key even when it is absent; read
/// committed locks only what exists. Each outcome was captured live against
/// the Go server on TiKV with a contender probing the keys.
#[test]
fn locking_point_reads_hold_the_keys_their_isolation_names() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut setup = factory
        .open_session(session_context(109))
        .expect("session opens");
    for sql in [
        "CREATE TABLE test.lku (id int primary key, k int, v int, unique key uk(k))",
        "CREATE TABLE test.lkt (id int primary key, v int)",
        "INSERT INTO test.lku VALUES (1, 10, 1), (2, 20, 2)",
        "INSERT INTO test.lkt VALUES (1, 1)",
    ] {
        rows(&mut setup, sql);
    }
    let cases: [(&str, &str, &str, bool); 5] = [
        (
            "REPEATABLE-READ",
            "UPDATE test.lku SET v = v + 1 WHERE k = 10",
            "SELECT * FROM test.lku WHERE k = 10 FOR UPDATE NOWAIT",
            true,
        ),
        (
            "REPEATABLE-READ",
            "UPDATE test.lku SET v = v + 1 WHERE k = 90",
            "INSERT INTO test.lku VALUES (9, 90, 9)",
            true,
        ),
        (
            "REPEATABLE-READ",
            "UPDATE test.lku SET v = v + 1 WHERE k IN (10, 90)",
            "INSERT INTO test.lku VALUES (9, 90, 9)",
            true,
        ),
        (
            "READ-COMMITTED",
            "UPDATE test.lku SET v = v + 1 WHERE k IN (10, 90)",
            "INSERT INTO test.lku VALUES (9, 90, 9)",
            false,
        ),
        (
            "READ-COMMITTED",
            "UPDATE test.lkt SET v = v + 1 WHERE id = 9",
            "INSERT INTO test.lkt VALUES (9, 9)",
            false,
        ),
    ];
    let mut writer = factory
        .open_session(session_context(110))
        .expect("session opens");
    let mut contender = factory
        .open_session(session_context(111))
        .expect("session opens");
    contender
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (isolation, statement, probe, held) in cases {
        writer
            .node_conn()
            .execute_write(&format!(
                "SET SESSION transaction_isolation = '{isolation}'"
            ))
            .expect("set isolation");
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        writer
            .node_conn()
            .execute_write(statement)
            .expect("statement runs");
        contender
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin contender");
        let result = if probe.starts_with("SELECT") {
            contender.node_conn().execute(probe).map(|_| ())
        } else {
            contender.node_conn().execute_write(probe).map(|_| ())
        };
        let probed = match result {
            Ok(()) => false,
            Err(error) => {
                assert!(
                    matches!(error.code, 1205 | 3572),
                    "{probe}: {}",
                    error.message
                );
                true
            }
        };
        contender
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(probed, held, "{isolation}: {statement} / {probe}");
    }
}

/// Go `updateRecord` inside a transaction (no `SkipWriteUntouchedIndices`)
/// rewrites every untouched index entry into the memory buffer with the
/// untouched flag (`index.create(untouched = true)`), and `TiDBKVFilter`
/// keeps those entries out of the commit. Captured live against the Go
/// server on TiKV: `TIDB_TRX.MEM_BUFFER_KEYS` is 3 after a one-row UPDATE of
/// a non-indexed column of a table with two secondary indexes, 6 after the
/// second row, and the committed table still checks clean.
#[test]
fn an_update_in_a_transaction_stages_untouched_index_entries_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(112))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.untouched (id int primary key, k int, c int, v int, \
         unique key uk(k), key kc(c))",
    );
    rows(
        &mut session,
        "INSERT INTO test.untouched VALUES (1, 10, 100, 1), (2, 20, 200, 2)",
    );
    let keys = |session: &mut crate::Session| {
        displayed(rows(
            session,
            "SELECT MEM_BUFFER_KEYS FROM information_schema.TIDB_TRX \
             WHERE SESSION_ID = 112",
        ))
    };
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    session
        .node_conn()
        .execute_write("UPDATE test.untouched SET v = v + 1 WHERE id = 1")
        .expect("update");
    assert_eq!(keys(&mut session), [["3"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM test.untouched USE INDEX(uk) WHERE k = 10",
        )),
        [["2"]]
    );
    session
        .node_conn()
        .execute_write("UPDATE test.untouched SET v = v + 1 WHERE v > 1")
        .expect("update");
    assert_eq!(keys(&mut session), [["6"]]);
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    session
        .node_conn()
        .execute_write("ADMIN CHECK TABLE test.untouched")
        .expect("the untouched entries were not committed");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, v FROM test.untouched USE INDEX(kc) WHERE c > 0 ORDER BY id",
        )),
        [["1", "3"], ["2", "3"]]
    );
}

#[test]
fn a_prepared_point_update_locks_its_row_before_reading_it() {
    use tidb_param::PreparedValue;

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
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    let statement = first
        .node_conn().prepare_general("UPDATE test.fold SET v = v + 5 WHERE id = ?")
        .expect("prepare");
    let affected = match first
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute")
    {
        crate::domain::session_api::GeneralExecuteOutcome::Write(outcome) => outcome.affected_rows,
        crate::domain::session_api::GeneralExecuteOutcome::Rows(_) => {
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
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            rows(&mut second, "UPDATE test.fold SET v = v + 100 WHERE id = 1");
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
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
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
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
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldtxt SET v = v + 100 WHERE id = 1",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
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

/// A strict pessimistic `UPDATE`/`DELETE` whose key constant fails Go's
/// fast-plan conversion (`getNameValuePairs` under the statement's type
/// context) has no point plan, so it locks nothing before its scan reports
/// the conversion error. Captured live against the Go server on TiKV: with
/// another transaction holding key 0, `UPDATE t SET v = 9 WHERE id = 'abc'`
/// fails at once with 1292 instead of waiting out the lock (1205).
///
/// The truncation is raised by the store evaluating the pushed Selection,
/// which answers it in `SelectResponse.error` under its own code (Go
/// unistore's `toPBError`, TiKV alike). The text is the evaluating engine's:
/// TiKV's cast names INTEGER (captured), while Go's unistore -- like this
/// store -- evaluates TiDB's `eq` over two doubles, whose string cast names
/// DOUBLE.
#[test]
fn a_strict_point_write_whose_key_does_not_convert_takes_no_prelock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(99))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.noprelock (id int primary key, v int)",
    );
    rows(
        &mut holder,
        "INSERT INTO test.noprelock VALUES (0, 0), (1, 1)",
    );
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.noprelock WHERE id = 0 FOR UPDATE",
    );

    let mut writer = factory
        .open_session(session_context(100))
        .expect("session opens");
    writer
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (sql, value) in [
        ("UPDATE test.noprelock SET v = 9 WHERE id = 'abc'", "abc"),
        ("UPDATE test.noprelock SET v = 9 WHERE id IN ('a', 'b')", "a"),
        ("DELETE FROM test.noprelock WHERE id = 'abc'", "abc"),
        ("DELETE FROM test.noprelock WHERE id IN ('a', 'b')", "a"),
    ] {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        let error = writer
            .node_conn()
            .execute_write(sql)
            .expect_err("the conversion fails the statement");
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                1292,
                format!("Truncated incorrect DOUBLE value: '{value}'").as_str()
            ),
            "{sql} (1205 would be a wait on the held key 0)"
        );
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback holder");
}

/// Go takes a pessimistic point lock inside execution (compile -> build the
/// executor -> `PointGetExecutor.Next` -> `getAndLock`), so a point write
/// that fails to compile never reaches a lock. Captured live against the Go
/// server on TiKV with another transaction holding key 1: each statement
/// fails at once with its compile error instead of waiting out the lock.
#[test]
fn a_point_write_that_fails_to_compile_takes_no_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(101))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.compilelock (id int primary key, v int)",
    );
    rows(&mut holder, "INSERT INTO test.compilelock VALUES (1, 1)");
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.compilelock WHERE id = 1 FOR UPDATE",
    );

    let mut writer = factory
        .open_session(session_context(102))
        .expect("session opens");
    writer
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (sql, message) in [
        (
            "UPDATE test.compilelock SET nosuch = 1 WHERE id = 1",
            "Unknown column 'nosuch' in 'field list'",
        ),
        (
            "DELETE FROM test.compilelock WHERE id = 1 AND nosuch = 1",
            "Unknown column 'nosuch' in 'where clause'",
        ),
    ] {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        let error = writer
            .node_conn()
            .execute_write(sql)
            .expect_err("the statement does not compile");
        assert_eq!(
            (error.code, error.message.as_str()),
            (1054, message),
            "{sql}"
        );
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback holder");
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
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
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
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldsel SET v = v + 100 WHERE id = 1",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(&mut first, "UPDATE test.foldsel SET v = v + 5 WHERE id = 1");
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
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

/// Go's pessimistic `UPDATE`/`DELETE ... WHERE pk IN (...)` is a locking
/// `Batch_Point_Get` (`tryUpdatePointPlan`/`tryDeletePointPlan` set
/// `BatchPointGetPlan.Lock`), and `BatchPointGetExec` under repeatable read
/// locks every named key WITH its value before reading
/// (`batch_point_get.go:434-444`: `LockKeys` -> `InitReturnValues`), then
/// reads through the pessimistic-lock cache. One PessimisticLock RPC and no
/// BatchGet: counted at the store's RPC client, Go's
/// `UnistoreRPCClientSendHook`.
#[test]
fn a_pessimistic_batch_point_write_folds_its_read_into_one_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory
        .open_session(session_context(102))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.foldbatch (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.foldbatch VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    );
    let table_id: i64 = displayed(rows(
        &mut session,
        "SELECT TIDB_TABLE_ID FROM information_schema.tables \
         WHERE table_schema = 'test' AND table_name = 'foldbatch'",
    ))[0][0]
        .parse()
        .expect("table id");
    // Only this table's row reads and locks count: the session's own system
    // reads (statistics, catalog) share the store.
    fn is_row_of(table_id: i64, key: &[u8]) -> bool {
        tidb_tablecodec::table_key::decode_record_key(key).is_ok_and(|(table, _)| table == table_id)
    }
    let sent: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    for (statement, after) in [
        (
            "UPDATE test.foldbatch SET v = v + 1 WHERE id IN (1, 2, 3)",
            [["1", "11"], ["2", "21"], ["3", "31"], ["4", "40"]].as_slice(),
        ),
        (
            "DELETE FROM test.foldbatch WHERE id IN (2, 3, 9)",
            [["1", "11"], ["4", "40"]].as_slice(),
        ),
    ] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        let recorded = Arc::clone(&recorded);
        stack.rpc.set_send_hook(Some(Arc::new(move |request| {
            use tikv_client::proto::kvrpcpb;
            let any = request.as_any();
            let touched = if let Some(lock) = any.downcast_ref::<kvrpcpb::PessimisticLockRequest>()
            {
                lock.mutations.iter().any(|m| is_row_of(table_id, &m.key))
            } else if let Some(get) = any.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                get.keys.iter().any(|key| is_row_of(table_id, key))
            } else if let Some(get) = any.downcast_ref::<kvrpcpb::GetRequest>() {
                is_row_of(table_id, &get.key)
            } else {
                false
            };
            if touched {
                recorded.lock().unwrap().push(request.label());
            }
        })));
        rows(&mut session, statement);
        stack.rpc.set_send_hook(None);
        let labels = std::mem::take(&mut *sent.lock().unwrap());
        let count = |label: &str| labels.iter().filter(|sent| **sent == label).count();
        assert_eq!(
            (
                count("kv_pessimistic_lock"),
                count("kv_batch_get"),
                count("kv_get")
            ),
            (1, 0, 0),
            "{statement}: {labels:?}"
        );
        session
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
        assert_eq!(
            displayed(rows(
                &mut session,
                "SELECT id, v FROM test.foldbatch ORDER BY id"
            )),
            after,
            "{statement}"
        );
    }
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
    use crate::domain::resultset_source::ResultSetSource;
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
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    let query =
        format!("SELECT {projection} FROM test.lock_range WHERE w=1 AND d=1 {window} FOR UPDATE");
    let query_rows = |session: &mut Session| {
        if !prepared {
            return rows(session, &query);
        }
        let statement = session
            .node_conn().prepare_general(&query.replace("w=1 AND d=1", "w=? AND d=?"))
            .expect("prepare locking range");
        let mut selected = Vec::new();
        // Execute the same prepared handle twice, covering retained-plan
        // reconstruction as well as fresh physical executor construction.
        for _ in 0..2 {
            let crate::domain::session_api::GeneralExecuteOutcome::Rows(mut result) = session
                .node_conn().execute_general(
                    &statement,
                    &[1, 1].map(tidb_param::PreparedValue::SignedLongLong),
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
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin");
            started_tx.send(()).expect("signal begin");
            let result = flatten(query_rows(&mut second));
            second
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("rollback");
            result
        });
        started_rx.recv().expect("contender began");
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(
            &mut first,
            "DELETE FROM test.lock_range WHERE w=1 AND d=1 AND id=1",
        );
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
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
            .node_conn()
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
        session
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
    // Go checkLockClauseTables / planner_issue_test's aliased OF targets:
    // the untargeted joined table must remain available to another locker.
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    for target in ["h", "test.lock_heap"] {
        assert_eq!(displayed(rows(&mut session, &format!(
            "SELECT h.v FROM test.lock_heap h JOIN test.lock_integer i ON h.id=i.id FOR UPDATE OF {target}"
        ))), [["10"]]);
    }
    let mut contender = stack.factory.open_session(session_context(115)).unwrap();
    contender
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut contender,
            "SELECT v FROM test.lock_integer WHERE id=1 FOR UPDATE NOWAIT"
        )),
        [["10"]]
    );
    assert!(
        matches!(contender.node_conn().execute("SELECT v FROM test.lock_heap WHERE v>0 FOR UPDATE NOWAIT"),
        Err(error) if error.code == 3572)
    );
    contender
        .node_conn()
        .control_transaction("ROLLBACK")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(
        matches!(session.node_conn().execute("SELECT v FROM test.lock_heap FOR UPDATE OF missing"),
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
            .node_conn().control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        assert_eq!(displayed(rows(&mut session, locking)), [["1"]], "{locking}");
        contender
            .node_conn().control_transaction("BEGIN PESSIMISTIC")
            .expect("contender begins");
        assert!(
            matches!(contender.node_conn().execute(contended), Err(error) if error.code == 3572),
            "{locking} left the row unlocked"
        );
        contender.node_conn().control_transaction("ROLLBACK").unwrap();
        session.node_conn().control_transaction("ROLLBACK").unwrap();
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
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    assert!(rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0").is_empty());
    rows(&mut writer, "INSERT INTO test.fresh_lock VALUES (1,10)");
    let locked = displayed(rows(
        &mut reader,
        "SELECT id FROM test.fresh_lock WHERE id>0 ORDER BY id LIMIT 1 FOR UPDATE",
    ));
    let plain = rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0");
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
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
    use crate::domain::resultset_source::ResultSetSource;
    use tidb_param::PreparedValue;

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
                .node_conn()
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
                .node_conn().prepare_general(&format!(
                    "SELECT id FROM {table} WHERE (k,id) IN ((?,?),(?,?),(?,?)) FOR UPDATE"
                ))
                .expect("prepare batch");
            let params = [10, 1, 20, 2, 20, 2].map(PreparedValue::SignedLongLong);
            let crate::domain::session_api::GeneralExecuteOutcome::Rows(mut result) = first
                .node_conn().execute_general(&statement, &params)
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
                .node_conn()
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
            .node_conn()
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
    use tidb_param::PreparedValue;

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
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    let statement = first
        .node_conn().prepare_general("UPDATE test.foldfail SET v = 'not-a-number' WHERE id = ?")
        .expect("prepare");
    // The execute outcome borrows the session for its row source, so the
    // error check happens inside a scope that ends before the rollback.
    let failed = {
        let outcome = first.node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)]);
        outcome.is_err()
    };
    if !failed {
        // A non-strict node coerces instead of failing; then there is no
        // post-lock failure to exercise and the release behavior above is
        // covered by the transaction-end path instead.
        first
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        return;
    }
    // The statement failed after its prelock; the transaction stays open.
    assert_eq!(
        first
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback"),
        Some(false)
    );

    // A contender takes the released row immediately: had the prelock leaked,
    // this would block for the lock-wait timeout instead of completing.
    let mut second = factory
        .open_session(session_context(94))
        .expect("session opens");
    assert_eq!(
        second
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    rows(&mut second, "UPDATE test.foldfail SET v = 99 WHERE id = 1");
    second
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

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
        .node_conn()
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
    let code = |s: &mut crate::Session, sql: &str| {
        s.node_conn()
            .execute_write(sql)
            .err()
            .map(|error| error.code)
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
    s.node_conn()
        .execute_write("SET foreign_key_checks = 0")
        .expect("fk checks off");
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
    s.node_conn()
        .execute_write(
            "INSERT INTO test.pc VALUES (1, 1, NULL, '2024-01-02 03:04:05', '\"x\"'), \
         (2, 5, 7, NULL, NULL), (3, NULL, 2, '2020-05-06 00:00:00', '\"yy\"')",
    )
    .expect("insert");
    let ids = |s: &mut crate::Session, sql: &str| {
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
        .node_conn()
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
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(
        &mut session,
        "INSERT INTO test.merged_update VALUES (3,50,60)",
    );
    let before = session.node().buffer.snapshot();
    let error = match session.node_conn().execute("UPDATE test.merged_update a JOIN test.merged_update b ON a.id=b.id SET a.id=IF(a.id=1,11,3) WHERE a.id<3") {
        Err(error) => error,
        Ok(_) => panic!("the later duplicate must fail the statement"),
    };
    assert_eq!(error.code, 1062);
    assert_eq!(session.node().buffer.snapshot(), before);
    session.node_conn().control_transaction("COMMIT").unwrap();
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
        .node_conn()
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
        .node_conn().prepare_general("UPDATE generated_policy SET a=? WHERE id=1")
        .unwrap();
    rows(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'");
    let error = session
        .node_conn().execute_general(
            &prepared,
            &[tidb_param::PreparedValue::SignedLongLong(2000)],
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
        .node_conn().execute_general(
            &prepared,
            &[tidb_param::PreparedValue::SignedLongLong(2000)],
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
    assert_analyze_preserves_generated_execution_error(None);
}

/// The same statements while the stats worker passes every 5ms, so a pass
/// always caches the table between its first `stats_meta` row (the failing
/// ANALYZE's delta flush) and the second ANALYZE's own refresh -- the order
/// a 3s worker reached only under load. Go's `SELECT b` cached a
/// `PseudoTable` holding `a` (`Handle.getStatsByPhysicalID`), which that pass
/// copies; without it the pass cached a table holding no column, and
/// ANALYZE's histograms never became visible.
#[test]
fn analyze_preserves_generated_execution_error_under_stats_worker_passes() {
    assert_analyze_preserves_generated_execution_error(Some(
        tidb_domain_core::node_config::StatsLease::Positive(std::time::Duration::from_millis(5)),
    ));
}

fn assert_analyze_preserves_generated_execution_error(
    stats_lease: Option<tidb_domain_core::node_config::StatsLease>,
) {
    use crate::domain::resultset_source::ResultSetSource;

    let (stack, _users) = cop_backed_stack_with_stats_lease(stats_lease);
    let mut session = stack.factory.open_session(session_context(992)).unwrap();
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE analyze_error (a DOUBLE)");
    rows(&mut session, "INSERT INTO analyze_error VALUES (0)");
    rows(
        &mut session,
        "ALTER TABLE analyze_error ADD COLUMN b DOUBLE AS (cot(a)) VIRTUAL",
    );
    let (original_code, original_state, original_message) =
        match session.node_conn().execute("SELECT b FROM analyze_error") {
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
        .node_conn()
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
    use crate::domain::configured_user_store::AuthenticationFailure;
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
    use crate::domain::configured_user_store::AuthenticationFailure;
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
    use crate::domain::configured_user_store::AuthenticationFailure;
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
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t");
    let ts: u64 = displayed(rows(&mut session, "SELECT @@tidb_current_ts"))[0][0]
        .parse()
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
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
        .node_conn()
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
        assert_eq!(session.node_conn().execute(&sql).err().unwrap().code, 1568);
    }
    assert_eq!(
        session
            .node_conn()
            .execute("SET tidb_snapshot=JSON_EXTRACT('invalid', '$')")
            .err()
            .unwrap()
            .code,
        3140,
        "SET evaluates before refusing transaction changes"
    );
    assert!(session
        .node_conn()
        .execute_write("UPDATE t SET v=30")
        .is_err());
    assert!(session
        .node_conn()
        .execute("SELECT * FROM t FOR UPDATE")
        .is_err());
    assert!(session
        .node_conn()
        .execute("EXPLAIN ANALYZE UPDATE t SET v=30")
        .is_err());
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts));
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "20", "7"]]
    );
    let future = ts + (1_000_000 << 18);
    assert!(session
        .node_conn()
        .execute(&format!("SELECT * FROM t AS OF TIMESTAMP {future}"))
        .is_err());
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&future));
    assert!(session
        .node_conn()
        .execute(&format!("SET tidb_snapshot='{ts}'"))
        .err()
        .unwrap()
        .message
        .contains("can not get 'tikv_gc_safe_point'"));
    rows(
        &mut session,
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05.000 -0700', '')",
    );
    rows(&mut session, &format!("SET tidb_snapshot='{ts}'"));
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "idle snapshot SET retains schema, not an active native timestamp"
    );
    assert!(session
        .node_conn()
        .execute(&format!("SET tidb_snapshot='{future}'"))
        .is_err());
    assert_eq!(session.vars().snapshot_ts(), ts);
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(session
        .node_conn()
        .execute_write("UPDATE t SET v=30")
        .is_err());
    assert!(session
        .node_conn()
        .execute("EXPLAIN ANALYZE UPDATE t SET v=30")
        .is_err());
    assert!(session
        .node_conn()
        .execute_write("ALTER TABLE t ADD COLUMN forbidden INT")
        .is_err());
    assert!(session
        .node_conn()
        .execute_write("CREATE DATABASE forbidden_history")
        .is_err());
    rows(&mut session, "SET autocommit=0");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(
        session.historical_read_ts().is_none(),
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
    let held = session.historical_read_ts().unwrap();
    assert!(tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&held));
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&held));
    rows(&mut session, "UPDATE t SET v=20 WHERE id=1");
    assert!(
        session.historical_read_ts().is_none(),
        "a write starts an ordinary transaction"
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
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
        .node_conn()
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "rollback without a read retires the metadata snapshot"
    );
    session
        .node_conn()
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
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t");
    let ts: u64 = displayed(rows(&mut session, "SELECT @@tidb_current_ts"))[0][0]
        .parse()
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
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
    assert!(session
        .node_conn()
        .execute_write("UPDATE t SET v=99")
        .is_err());
    assert!(session
        .node_conn()
        .execute("SELECT * FROM t FOR UPDATE")
        .is_err());

    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert_eq!(session.vars().txn_read_ts(), 0);
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
        .node_conn()
        .execute(&format!("SELECT * FROM t AS OF TIMESTAMP {ts}"))
        .is_err());
    assert_eq!(session.vars().txn_read_ts(), 0);
    rows(&mut session, &set);
    assert!(session
        .node_conn()
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .is_err());
    session.node_conn().control_transaction("BEGIN").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(session.node_conn().execute(&set).is_err());
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(session.vars().txn_read_ts(), 0);
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
        assert!(session.node_conn().execute(&sql).is_err(), "{sql}");
    }
    rows(&mut session, "SET tidb_enable_external_ts_read=ON");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );

    assert!(session
        .node_conn()
        .execute_write("UPDATE t SET v=99")
        .is_err());
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SET tidb_enable_external_ts_read=OFF");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
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
    peer.set_restricted_sql(true);
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
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05.000 -0700', '')",
    );
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t");
    let historical_ts = session.current_tso().value() as u64;
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(&mut session, "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7");
    for mode in ["OPTIMISTIC", "PESSIMISTIC"] {
        session
            .node_conn()
            .control_transaction(&format!("BEGIN {mode}"))
            .unwrap();
        rows(&mut session, "UPDATE t SET v=30 WHERE id=1");
        rows(&mut session, "INSERT INTO t VALUES (2,40,8)");
        let ordinary_ts = session.current_tso().value();
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
        assert!(session.node_conn().execute("SELECT extra FROM t").is_err());
        assert!(session
            .node_conn()
            .execute_write("UPDATE t SET v=99")
            .is_err());
        assert!(session.in_transaction());
        assert_eq!(session.current_tso().value(), ordinary_ts);
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
        session.node_conn().control_transaction("ROLLBACK").unwrap();
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
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        let stage = session.node().buffer.checkpoint();
        session.node_conn().execute_write(sql).unwrap();
        let lock_keys = session.node().buffer.pessimistic_keys_since(stage);
        // New record/unique-index keys defer their absence check. UPDATE
        // retains the old record lock and deletion entries as Go does.
        let presumed = session.node().buffer.presume_not_exists_keys();
        assert!(!presumed.is_empty(), "{sql}");
        assert!(
            lock_keys.iter().all(|key| !presumed.contains(key)),
            "{sql}: deferred keys were locked"
        );
        session.node().buffer.release(stage);
        if sql == "INSERT INTO t VALUES (1,30)" || sql == "INSERT INTO t VALUES (3,10)" {
            let error = session
                .node_conn()
                .control_transaction("COMMIT")
                .unwrap_err();
            assert_eq!(error.code, 1062, "{sql}: {error:?}");
        } else {
            session.node_conn().control_transaction("ROLLBACK").unwrap();
        }
    }
    // Go unistore checks NotExist before Insert when assertions are enabled.
    rows(&mut session, "SET tidb_txn_assertion_level=STRICT");
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(&mut session, "INSERT INTO t VALUES (1,30)");
    assert_eq!(
        session
            .node_conn()
            .control_transaction("COMMIT")
            .unwrap_err()
            .code,
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
    session
        .node_conn()
        .control_transaction("SAVEPOINT empty")
        .unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .unwrap();
    session
        .node_conn()
        .control_transaction("SAVEPOINT allowed")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    let error = session
        .node_conn()
        .control_transaction("SAVEPOINT refused")
        .unwrap_err();
    assert_eq!(error.code, 1105);
    assert_eq!(error.message, "savepoint is not supported in pessimistic transactions when in-place constraint check is disabled");
    assert!(session.in_transaction());
    session.node_conn().control_transaction("ROLLBACK").unwrap();
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
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(&mut session, "INSERT INTO t VALUES (1)");
    let error = session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1)")
        .unwrap_err();
    assert_eq!(error.code, 8147, "{error:?}");
    // Captured on a Go tidb-server: the wrapper embeds the inner `Error()`
    // text, class prefix included.
    assert_eq!(
        error.message,
        "transaction aborted because lazy uniqueness check is enabled and an error occurred: \
         [kv:1062]Duplicate entry '1' for key 't.PRIMARY'"
    );
    assert_eq!(
        error.identity,
        Some(tidb_exeerrors::ERR_LAZY_UNIQUENESS_CHECK_FAILURE.identity())
    );
    assert!(!session.in_transaction());
    assert!(session.node().txn.explicit.is_none());
    assert!(session.node().buffer.is_empty());
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
    session
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    session.set_restricted_sql(true);
    session.set_restricted_sql(false);
    session.set_connection_id(0);
    session.set_connection_id(943);
    rows(&mut session, "DELETE FROM t WHERE id=1");
    rows(&mut session, "INSERT INTO t VALUES (1,10)");
    assert!(
        session.node().buffer.presume_not_exists_keys().is_empty(),
        "local tombstones prove absence"
    );
    rows(&mut session, "INSERT INTO t VALUES (3,30),(4,NULL)");
    rows(&mut session, "UPDATE t SET u=31 WHERE id=3");
    rows(&mut session, "INSERT IGNORE INTO t VALUES (5,20)");
    rows(&mut session, "UPDATE IGNORE t SET u=20 WHERE id=3");
    assert!(session.in_transaction());
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=ON",
    );
    rows(&mut session, "INSERT INTO t VALUES (6,60)");
    session.node_conn().control_transaction("COMMIT").unwrap();
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
    assert!(session
        .node_conn()
        .control_transaction("SAVEPOINT implicit")
        .is_err());
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    rows(&mut session, "SET autocommit=1");
}

#[test]
fn deferred_uniqueness_batch_binary_prepared_dml_shares_abort_policy() {
    use tidb_param::PreparedValue;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(944)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_binary");
    rows(&mut session, "USE deferred_binary");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    let insert = session.node_conn().prepare_general("INSERT INTO t VALUES (?)").unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    session
        .node_conn().execute_general(&insert, &[PreparedValue::SignedLongLong(1)])
        .unwrap();
    assert!(session
        .node_conn()
        .execute("SELECT missing FROM t")
        .is_err());
    assert!(
        session.in_transaction(),
        "ordinary query errors do not invoke DML abort policy"
    );
    let error = match session.node_conn().execute_general(&insert, &[PreparedValue::SignedLongLong(1)]) {
        Err(error) => error,
        Ok(_) => panic!("a local duplicate must fail"),
    };
    assert_eq!(error.code, 8147, "{error:?}");
    assert!(!session.in_transaction());
    assert!(session.node().txn.explicit.is_none());
    assert!(session.node().buffer.is_empty());
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
            .node_conn()
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
        .node_conn()
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
        session: &mut Session,
        sql: &str,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        if session.node_conn().execute_write(sql)?.is_some() {
            return Ok(());
        }
        use crate::domain::resultset_source::ResultSetSource;
        let mut result = session.node_conn().execute(sql)?;
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
        let error = session.node_conn().execute_write(&sql).unwrap_err();
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
        session.node_conn().execute_write(&sql).unwrap();
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
        .node_conn().execute_write("ALTER TABLE test.t1 DROP COLUMN a, DROP COLUMN b, ADD COLUMN c INT DEFAULT 3, ADD COLUMN d INT DEFAULT 4")
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
        .node_conn().execute_write("ALTER TABLE test.t2 DROP INDEX i3, DROP PRIMARY KEY, RENAME INDEX i TO i1, CHANGE COLUMN c d INT")
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
        .node_conn().execute_write("ALTER TABLE test.t3 MODIFY COLUMN a TINYINT AFTER c, MODIFY COLUMN b TINYINT, MODIFY COLUMN c TINYINT FIRST")
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
        .node_conn()
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
        .node_conn().execute_write("ALTER TABLE test.t4 MODIFY COLUMN b TINYINT, AUTO_INCREMENT = 200, COMMENT = 'abc', CHARSET = utf8mb4")
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
        .execute_write("CREATE SEQUENCE IF NOT EXISTS test.s")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "1050", "Table 'test.s' already exists"]]
    );
    session
        .node_conn()
        .execute_write("ALTER SEQUENCE test.s INCREMENT 5")
        .unwrap();
    // Seeded from the discarded batch's end, 14 (oracle).
    assert_eq!(
        displayed(rows(&mut session, "SELECT nextval(test.s)")),
        [["15"]]
    );
    session
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER SEQUENCE test.s MINVALUE 50")
        .unwrap_err();
    assert_eq!(error.code, 4136, "{}", error.message);

    rows(&mut session, "CREATE TABLE test.t (a INT)");
    let error = session
        .node_conn()
        .execute_write("TRUNCATE TABLE test.s")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1146, "Table 'test.s' doesn't exist")
    );
    let error = session
        .node_conn()
        .execute_write("DROP SEQUENCE test.t")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1347, "'test.t' is not SEQUENCE")
    );
    let error = session
        .node_conn()
        .execute_write("DROP SEQUENCE test.s, test.nosuch")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (4139, "Unknown SEQUENCE: 'test.nosuch'")
    );
    session
        .node_conn()
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
        let error = session.node_conn().execute_write(&sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message.as_str()), "{sql}");
    }
    rows(
        &mut session,
        "CREATE TABLE test.a3 (a BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT, b INT)",
    );
    let error = session
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE test.t TTL_JOB_INTERVAL = '2h'")
        .unwrap();
    session
        .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
    }
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t REMOVE TTL")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(!created[0][1].contains("TTL"), "{}", created[0][1]);
    let error = session
        .node_conn()
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE test.iv LAST PARTITION LESS THAN (50)")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE test.pt EXCHANGE PARTITION p1 WITH TABLE test.nt")
        .unwrap_err();
    assert_eq!(error.code, 1737, "{}", error.message);
    rows(
        &mut session,
        "SET @@global.tidb_enable_check_constraint = 0",
    );
    session
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE test.t EXCHANGE PARTITION p3 WITH TABLE test.t1")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE test.t PLACEMENT POLICY p1")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].ends_with("/*T![placement] PLACEMENT POLICY=`p1` */"),
        "{}",
        created[0][1]
    );
    session
        .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
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
        .node_conn()
        .execute_write("INSERT IGNORE INTO test.t1 VALUES (NULL)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.t1 VALUES (2001)")
        .unwrap();
    rows(&mut session, "CREATE TABLE test.t2 (a INT, KEY (a))");
    session
        .node_conn()
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
        .node_conn()
        .execute_write("INSERT INTO test.g VALUES (1)")
        .unwrap();
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.g VALUES (2)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.g")),
        [["2"]]
    );
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
    // A BEGIN the query path runs opens the session's own transaction, which
    // the next statement continues into just the same.
    rows(&mut session, "BEGIN");
    session
        .node_conn()
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
        .node_conn()
        .execute_write("CREATE INDEX hypo_bc TYPE HYPO ON test.t (b, c)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE UNIQUE INDEX hypo_c TYPE HYPO ON test.t (c)")
        .unwrap();
    let error = session
        .node_conn()
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
    let explain = |session: &mut Session, sql: &str| {
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
        // The unique hypothetical index answers `c = 2` with a point get,
        // which beats the two-column one (the oracle's plan, now that a
        // hypothetical index gets a filled path like any other).
        "table:t, index:hypo_c(c)"
    );
    assert_eq!(
        explain(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT b FROM test.t WHERE b = 1 AND c > 2"
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
        .node_conn()
        .execute_write("DROP HYPO INDEX hypo_bc ON test.t")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("DROP HYPO INDEX hypo_bc ON test.t")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1091, "index hypo_bc doesn't exist")
    );
    session
        .node_conn()
        .execute_write("DROP HYPO INDEX IF EXISTS hypo_bc ON test.t")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
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
    let parsed = session.parse_at_statement_boundary(sql).unwrap();
    assert!(session.node_conn().execute_stmt(sql, &parsed).unwrap().is_none());
    let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0][..2], ["Warning", "1064"]);
    assert!(
        warnings[0][2]
            .contains("The PAGE_CHECKSUM option is parsed but ignored by all storage engines."),
        "{warnings:?}"
    );
    let sql = "CREATE TABLE test.b (a INT)";
    let parsed = session.parse_at_statement_boundary(sql).unwrap();
    assert!(session.node_conn().execute_stmt(sql, &parsed).unwrap().is_none());
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
        .node_conn()
        .execute_write("INSERT INTO test.t1 VALUES (1)")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.t1 VALUES (3)")
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
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

/// Go `setLastTxnInfoBeforeTxnEnd` runs at the start of `CommitTxn` and
/// `RollbackTxn` and records the start-only struct only for a transaction
/// that was activated (`TxnCtx.StartTS != 0`); the commit hook then
/// overwrites it when a commit really ran. Captured on the Go oracle:
/// `USE`, `SET @x` and `PREPARE` leave `@@tidb_last_txn_info` alone (empty on
/// a fresh connection), an autocommit UPDATE matching no row leaves the
/// start-only record (`commit_ts` 0, no commit mode), and `BEGIN; ROLLBACK`
/// leaves the start-only record too. This node recorded a two-phase commit
/// at counter timestamps for every statement that produced no rows.
#[test]
fn last_txn_info_is_recorded_only_by_the_transaction_that_ended() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1014)).unwrap();
    let info = |session: &mut Session| {
        displayed(rows(session, "SELECT @@tidb_last_txn_info"))[0][0].clone()
    };
    rows(&mut session, "USE test");
    rows(&mut session, "SET @x = 1");
    rows(&mut session, "PREPARE p FROM 'SELECT 1'");
    assert_eq!(info(&mut session), "", "statements that ran no transaction");
    // A DDL does run one (`NewTxnInStmt`): a start-only record.
    rows(&mut session, "CREATE TABLE test.t_info (a INT PRIMARY KEY)");

    session
        .node_conn()
        .execute_write("INSERT INTO test.t_info VALUES (1)")
        .unwrap();
    let committed = info(&mut session);
    assert!(committed.contains("\"txn_commit_mode\":\"2pc\""), "{committed}");
    rows(&mut session, "USE test");
    rows(&mut session, "SET @x = 2");
    rows(&mut session, "PREPARE p2 FROM 'SELECT 1'");
    assert_eq!(info(&mut session), committed, "no transaction, no record");

    session
        .node_conn()
        .execute_write("UPDATE test.t_info SET a = a WHERE a = 99")
        .unwrap();
    let empty_update = info(&mut session);
    assert_ne!(empty_update, committed);
    assert!(
        empty_update.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
        "{empty_update}"
    );

    session.node_conn().control_transaction("BEGIN").unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    let rolled_back = info(&mut session);
    assert_ne!(rolled_back, empty_update);
    assert!(
        rolled_back.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
        "{rolled_back}"
    );

    // `SELECT 1` reads no table and never activates a transaction; a table
    // read does, and its end leaves the start-only record.
    rows(&mut session, "SELECT 1");
    assert_eq!(info(&mut session), rolled_back);
    rows(&mut session, "SELECT * FROM test.t_info");
    let read = info(&mut session);
    assert_ne!(read, rolled_back);
    assert!(
        read.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
        "{read}"
    );
}

/// Go's DDL, account and ANALYZE statements run under a session transaction
/// opened by `NewTxnInStmt` at a fresh TSO, and the autocommit `CommitTxn`
/// that ends them records its start-only `@@tidb_last_txn_info`. Captured on
/// the Go oracle: CREATE TABLE, ALTER TABLE, CREATE USER, GRANT, REVOKE,
/// DROP USER and ANALYZE each leave a newer start with `commit_ts` 0; a
/// local `CREATE TEMPORARY TABLE` returns before `NewTxnInStmt` and leaves
/// the record alone.
#[test]
fn new_txn_in_stmt_statements_record_a_fresh_start_only_txn_info() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1017)).unwrap();
    let start = |session: &mut Session| -> u64 {
        displayed(rows(
            session,
            "SELECT IF(@@tidb_last_txn_info = '', 0, cast(json_unquote(json_extract(\
             @@tidb_last_txn_info, '$.start_ts')) AS UNSIGNED))",
        ))[0][0]
            .parse()
            .unwrap_or(0)
    };
    let mut last = start(&mut session);
    assert_eq!(last, 0, "a fresh session has no record");
    for statement in [
        "CREATE TABLE test.t_new_txn (a INT PRIMARY KEY)",
        "ALTER TABLE test.t_new_txn ADD COLUMN b INT",
        "CREATE USER 'new_txn_u'@'%' IDENTIFIED BY ''",
    ] {
        session.node_conn().execute_write(statement).unwrap();
        let now = start(&mut session);
        assert!(now > last, "{statement} left start {now} after {last}");
        let info = displayed(rows(&mut session, "SELECT @@tidb_last_txn_info"))[0][0].clone();
        assert!(
            info.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
            "{statement}: {info}"
        );
        last = now;
    }
    session
        .node_conn()
        .execute_write("CREATE TEMPORARY TABLE test.tt_new_txn (a INT)")
        .unwrap();
    assert_eq!(start(&mut session), last, "a local temporary table is not one");
}

/// Go `PrepareTxnCtx` enters the `autocommit = 0` transaction with
/// `EnterNewTxnBeforeStmt` (`activeNow = false`): it stays pending, spending
/// no timestamp, until a statement activates it. Captured on the Go oracle:
/// with `autocommit = 0`, `SELECT 1`, `SET @a`, `SHOW TABLES` and `EXPLAIN
/// SELECT 1` leave `@@tidb_current_ts` at 0, a row a peer commits after
/// `SELECT 1` is visible to the next table read, and a row a peer commits
/// after a table read is not. This node opened the store transaction at the
/// entry of every statement.
#[test]
fn autocommit_off_transaction_is_pending_until_a_statement_activates_it() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1015)).unwrap();
    let mut peer = stack.factory.open_session(session_context(1016)).unwrap();
    rows(&mut session, "CREATE TABLE test.t_pending (a INT PRIMARY KEY)");
    let active = |session: &mut Session| {
        displayed(rows(session, "SELECT @@tidb_current_ts > 0"))[0][0].clone()
    };
    rows(&mut session, "SET autocommit = 0");
    for statement in [
        "SELECT 1",
        "SET @a = 1",
        // Go `buildShow` refuses SHOW TABLES with no current database.
        "SHOW TABLES FROM test",
        "EXPLAIN SELECT 1",
        "SELECT now()",
    ] {
        rows(&mut session, statement);
        assert_eq!(active(&mut session), "0", "after {statement}");
    }
    peer.node_conn()
        .execute_write("INSERT INTO test.t_pending VALUES (21)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.t_pending")),
        [["1"]],
        "the transaction is still pending, so it reads what the peer committed"
    );
    assert_eq!(active(&mut session), "1");
    // Go's `GetSession` hook for `tidb_current_ts` returns `FormatUint`, a
    // string like every system variable's value.
    let current_ts = rows(&mut session, "SELECT @@tidb_current_ts");
    assert_eq!(current_ts[0][0].kind(), tidb_datatype::DatumKind::String);
    peer.node_conn()
        .execute_write("INSERT INTO test.t_pending VALUES (22)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.t_pending")),
        [["1"]],
        "the activated transaction keeps its snapshot"
    );
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(active(&mut session), "0");
    // A pending transaction that never activates commits and rolls back
    // without taking a timestamp.
    rows(&mut session, "SELECT 1");
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(&mut session, "SELECT 1");
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    // DML activates at build, SAVEPOINT activates.
    session
        .node_conn()
        .execute_write("INSERT INTO test.t_pending VALUES (23)")
        .unwrap();
    assert_eq!(active(&mut session), "1");
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(&mut session, "SELECT 1");
    session
        .node_conn()
        .control_transaction("SAVEPOINT s1")
        .unwrap();
    assert_eq!(active(&mut session), "1");
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.t_pending")),
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
        .node_conn()
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
        .node_conn()
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
        .node_conn()
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
        tidb_domain_affinity::get_groups(&[group.to_owned()])
            .unwrap()
            .contains_key(group)
    };
    assert!(held(&group));
    session
        .node_conn()
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
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (8200, message),
            "{sql}"
        );
    }
    let error = session
        .node_conn()
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
        .node_conn()
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
        .node_conn()
        .execute_write("CREATE RESOURCE GROUP rg1 RU_PER_SEC = 100")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("CREATE RESOURCE GROUP rg1 RU_PER_SEC = 100")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8248, "Resource group 'rg1' already exists")
    );
    session
        .node_conn()
        .execute_write("CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 100")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "8248", "Resource group 'rg1' already exists"]]
    );
    session
        .node_conn()
        .execute_write("SET RESOURCE GROUP rg1")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT CURRENT_RESOURCE_GROUP()")),
        [["rg1"]]
    );
    session
        .node_conn()
        .execute_write("SET RESOURCE GROUP default")
        .unwrap();
    session
        .node_conn()
        .execute_write("DROP RESOURCE GROUP rg1")
        .unwrap();
    assert!(rows(
        &mut session,
        "SELECT name FROM information_schema.resource_groups WHERE name = 'rg1'"
    )
    .is_empty());
    let error = session
        .node_conn()
        .execute_write("DROP RESOURCE GROUP rg1")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8249, "Unknown resource group 'rg1'")
    );
}

/// The error a query raises, whether at `execute` or while its rows stream.
fn query_error(session: &mut Session, sql: &str) -> (u16, String) {
    use crate::domain::resultset_source::ResultSetSource;
    let mut result = match session.node_conn().execute(sql) {
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

/// The domain's `SHOW STATUS` providers (Go `ddl/stat.go`, registered by
/// `ddl.Start`, and the binding handle's): the DDL id is this server's id,
/// the schema version is the one `ADMIN SHOW DDL` reports, and the binding
/// handle reports the zero time until a binding is loaded.
#[test]
fn the_domain_reports_its_ddl_and_binding_statuses() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1016)).unwrap();
    let server_id = displayed(rows(
        &mut session,
        "SELECT DDL_ID FROM information_schema.TIDB_SERVERS_INFO",
    ));
    let schema_version = displayed(rows(&mut session, "ADMIN SHOW DDL"))[0][0].clone();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW GLOBAL STATUS WHERE Variable_name IN ('server_id', 'ddl_schema_version')"
        )),
        [
            ["ddl_schema_version".to_owned(), schema_version],
            ["server_id".to_owned(), server_id[0][0].clone()],
        ]
    );
    // Session scope only: GLOBAL drops it.
    assert_eq!(
        displayed(rows(&mut session, "SHOW STATUS LIKE 'last_plan_binding_update_time'")),
        [["last_plan_binding_update_time", "0000-00-00 00:00:00"]]
    );
    assert!(
        displayed(rows(&mut session, "SHOW GLOBAL STATUS LIKE 'last_plan_binding%'")).is_empty()
    );
}

/// Go `copIterator` sums each response's `ScanDetailV2.ProcessedVersions`
/// into the statement-wide `MaxKeysReadCounter`: a pushed scan past
/// `tidb_max_keys_read` fails with 8274, an IndexLookUp's two iterators share
/// one budget, and every processed key reaches `tidb_keys_examined`.
#[test]
fn pushed_scans_share_the_max_keys_read_budget_and_feed_keys_examined() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1017)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t_mrs (id INT PRIMARY KEY AUTO_INCREMENT, val INT, extra INT)")
        .unwrap();
    let values = (1..=20)
        .map(|v| format!("({v}, 100)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .node_conn()
        .execute_write(&format!("INSERT INTO t_mrs (val, extra) VALUES {values}"))
        .unwrap();
    session.node_conn().execute_write("FLUSH STATUS").unwrap();
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
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE t1 (a INT) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE t1 ATTRIBUTES='key=value'")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE t1 PARTITION p0 ATTRIBUTES='k2=v2'")
        .unwrap();
    let attributes = |session: &mut Session| {
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
    session
        .node_conn()
        .execute_write("RENAME TABLE t1 TO t2")
        .unwrap();
    session
        .node_conn()
        .execute_write("TRUNCATE TABLE t2")
        .unwrap();
    assert_eq!(
        attributes(&mut session),
        [
            ["schema/test/t2", "key-range", "\"key=value\""],
            ["schema/test/t2/p0", "key-range", "\"k2=v2\""],
        ]
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE t2 ATTRIBUTES=DEFAULT")
        .unwrap();
    assert_eq!(
        attributes(&mut session),
        [["schema/test/t2/p0", "key-range", "\"k2=v2\""]]
    );
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE t2 ATTRIBUTES='nokey'")
        .unwrap_err();
    assert_eq!(error.code, 8237);
    session
        .node_conn()
        .execute_write("CREATE TABLE plain (a INT)")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE plain PARTITION p0 ATTRIBUTES='k=v'")
        .unwrap_err();
    assert_eq!(error.code, 1505);

    session
        .node_conn()
        .execute_write("CREATE DATABASE d2")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE d2.x (a INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE d2.x ATTRIBUTES='a=b'")
        .unwrap();
    assert_eq!(attributes(&mut session).len(), 2);
    session
        .node_conn()
        .execute_write("DROP DATABASE d2")
        .unwrap();
    assert_eq!(
        tidb_domain_infosync::label_manager::label_rule_manager()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE PLACEMENT POLICY pp1 FOLLOWERS=1")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tp (a INT) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let show_create = |session: &mut Session, table: &str| {
        displayed(rows(session, &format!("SHOW CREATE TABLE {table}")))[0][1].clone()
    };
    session
        .node_conn()
        .execute_write("ALTER TABLE tp PARTITION p0 PLACEMENT POLICY=pp1")
        .unwrap();
    assert!(show_create(&mut session, "tp").contains(
        "PARTITION `p0` VALUES LESS THAN (10) /*T![placement] PLACEMENT POLICY=`pp1` */"
    ));
    session
        .node_conn()
        .execute_write("ALTER TABLE tp PARTITION p0 PLACEMENT POLICY=DEFAULT")
        .unwrap();
    assert!(!show_create(&mut session, "tp").contains("PLACEMENT POLICY"));
    session
        .node_conn()
        .execute_write("CREATE TABLE c (a VARCHAR(10))")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE c PARTITION p0 PLACEMENT POLICY=pp1")
        .unwrap_err();
    assert_eq!(error.code, 1505);
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE tp PARTITION nope PLACEMENT POLICY=pp1")
        .unwrap_err();
    assert_eq!(error.code, 1735);

    session
        .node_conn()
        .execute_write("ALTER TABLE c CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci")
        .unwrap();
    assert!(show_create(&mut session, "c")
        .contains("DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"));
    let error = session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE only_virtual (c0 TEXT AS ('a\\\\b'))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO only_virtual VALUES ()")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM only_virtual")),
        [["a\\b"]]
    );

    let error = session
        .node_conn()
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
        .node_conn()
        .execute_write("CREATE TABLE g (a INT, b INT AS (a+1))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO g (a) VALUES (1)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE g MODIFY b INT AS (a+2)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b FROM g")),
        [["1", "3"]]
    );
    let error = session
        .node_conn()
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
        .node_conn()
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
    session
        .node_conn()
        .execute_write("CREATE DATABASE hist_db")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE hist_db.t (a INT)")
        .unwrap();
    // Go `buildIndexColumns`: an index may not name a column the same
    // statement adds.
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE hist_db.t ADD COLUMN b INT, ADD INDEX ib (b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1072, "column does not exist: b")
    );
    session
        .node_conn()
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
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE td (col_bigint BIGINT, col_smallint SMALLINT)")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("SET @@tidb_enable_clustered_index = ON")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ck (id INT, logtime DATETIME, money INT, PRIMARY KEY (id, logtime))",
        )
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    let error = session
        .node_conn()
        .execute_write("CREATE TABLE plain (a INT, b INT, UNIQUE INDEX idx (a) GLOBAL)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8200, "Unsupported Global Index on non-partitioned table")
    );
    session
        .node_conn()
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
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE ci (a INT, b VARCHAR(10), INDEX a_b (a, b))")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE sg (a INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_external_ts = 19980613")
        .unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO sg VALUES (1)")
        .unwrap();
    let start = displayed(rows(&mut session, "SELECT @@tidb_current_ts"));
    session
        .node_conn()
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
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(rows(&mut session, "SELECT * FROM sg").is_empty());
}

/// Go's preprocess judges a table-qualified column name in CREATE TABLE
/// (1102 for another schema, 1103 for another table) and the builder then
/// reads the bare name; the production builder refused every qualifier.
#[test]
fn create_table_accepts_a_column_qualified_by_its_own_table() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1026)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
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
        .node_conn()
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
        .node_conn()
        .execute_write("CREATE PLACEMENT POLICY px FOLLOWERS=1")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE pdb PLACEMENT POLICY px")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE pdb.t1 (a INT) PLACEMENT POLICY 'default'")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE pdb.t2 (a INT)")
        .unwrap();
    let show = |session: &mut Session, table: &str| {
        displayed(rows(session, &format!("SHOW CREATE TABLE {table}")))[0][1].clone()
    };
    assert!(!show(&mut session, "pdb.t1").contains("PLACEMENT POLICY"));
    assert!(show(&mut session, "pdb.t2").contains("PLACEMENT POLICY=`px`"));
    session
        .node_conn()
        .execute_write("CREATE TEMPORARY TABLE pdb.tmp LIKE pdb.t1")
        .unwrap();

    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE h (a INT, b REAL)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE h ADD INDEX idx ((a + 1))")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE h ADD COLUMN (_V$_idx_0 INT)")
        .unwrap_err();
    assert_eq!(error.code, 1060);
    session
        .node_conn()
        .execute_write("ALTER TABLE h DROP INDEX idx")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED)")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tp (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (11), \
             PARTITION p2 VALUES LESS THAN (20))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tp (a, b) VALUES (2, 2), (4, 4), (6, 6)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t (a, b) VALUES (12, 2), (14, 4), (16, 6)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE tp EXCHANGE PARTITION p2 WITH TABLE t")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a CHAR(10) CHARSET gbk DEFAULT 0xC4E3BAC3)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (DEFAULT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t () VALUES ()")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, HEX(a) FROM t")),
        [["你好", "C4E3BAC3"], ["你好", "C4E3BAC3"]]
    );
    session
        .node_conn()
        .execute_write("CREATE TABLE t2 (a CHAR(10) CHARSET gbk DEFAULT '好')")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (id INT, v INT, KEY (id))")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t2 (id INT, v INT, KEY (id))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE t (id INT, a VARCHAR(20) DEFAULT 'cUrrent_tImestamp', \
             b VARCHAR(20) DEFAULT 'current_date')",
        )
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t1 (id INT NOT NULL, num INT)")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT NOT NULL, b INT)")
        .unwrap();
    session
        .node_conn()
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
        let mut result = session.node_conn().execute(sql).unwrap();
        let fields = crate::domain::resultset_source::ResultSetSource::fields(result.source()).unwrap();
        assert_eq!(fields[0].field_type.flags, flag, "{sql}");
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write(&format!("CREATE USER '{long_user}'@'%'"))
        .unwrap_err();
    assert_eq!(error.code, 1470);
    assert_eq!(
        error.message,
        format!("String '{long_user}' is too long for user name (should be no longer than 32)")
    );
    let long_host = "h".repeat(256);
    let error = session
        .node_conn()
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
    session
        .node_conn()
        .execute_write("CREATE USER 'short'@'%'")
        .unwrap();
    let error = session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT, KEY idx (a))")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, \
             title VARCHAR(500) CHARACTER SET utf8, subtitle VARCHAR(500) CHARACTER SET utf8, \
             UNIQUE KEY (title, subtitle))",
        )
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE posts CONVERT TO CHARACTER SET utf8mb4")
        .unwrap_err();
    assert_eq!(error.code, 1071);
    assert_eq!(
        error.message,
        "Specified key was too long (4000 bytes); max key length is 3072 bytes"
    );
    session
        .node_conn()
        .execute_write("CREATE TABLE short (a VARCHAR(10) CHARACTER SET utf8, KEY (a))")
        .unwrap();
    session
        .node_conn()
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
    let count = |session: &mut Session, table: &str| {
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

    session.node_conn().execute_write("DROP STATS ds").unwrap();

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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dp (a INT)")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("DROP STATS missing")
        .unwrap_err();
    assert_eq!(error.code, 1146);
    let error = session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE us (c_str VARCHAR(40), c_int INT, c_datetime DATETIME, \
             PRIMARY KEY (c_str) CLUSTERED, UNIQUE KEY (c_datetime))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "INSERT INTO us VALUES ('laughing hertz', 1, '2020-04-27 20:29:30'), \
             ('sharp yalow', 2, '2020-04-01 05:53:36'), \
             ('pedantic hoover', 3, '2020-03-10 11:49:00')",
        )
        .unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
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
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// Go assigns a `CREATE TABLE ... LIKE` copy its own physical table ids -- the
/// table's, then one per partition -- like any other new table. The node kept
/// the source's partition ids, so the two tables shared physical tables and a
/// row inserted into one was counted by the other.
#[test]
fn create_table_like_a_partitioned_table_gets_its_own_partition_ids() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1041)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn().execute_write(
            "CREATE TABLE lk1 (c_int INT, PRIMARY KEY (c_int)) PARTITION BY HASH (c_int) PARTITIONS 2",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE lk2 LIKE lk1")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO lk2 VALUES (1)")
        .unwrap();
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE bd (k INT, v BIT(34) DEFAULT b'111010101111001001100111101111111', \
             z BIT(40) DEFAULT 0xFFEEDDCCBB, w BIT(8) DEFAULT b'101')",
        )
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
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
        session.node_conn().execute_write(create).unwrap();
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE ct (a INT)")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("ALTER TABLE ct COMPACT PARTITION p1 TIFLASH REPLICA")
        .unwrap_err();
    assert_eq!(error.code, 1105);
    let error = session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    for column in [
        "ENUM('y','b','1','x','0','null')",
        "SET('y','b','1','x','0','null')",
    ] {
        session
            .node_conn()
            .execute_write("DROP TABLE IF EXISTS mjx")
            .unwrap();
        session
            .node_conn()
            .execute_write(&format!(
                "CREATE TABLE mjx (a {column} DEFAULT NULL, KEY a (a))"
            ))
            .unwrap();
        session
            .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ilp1 (id INT PRIMARY KEY, a VARCHAR(32), b INT, c INT, INDEX i(a, b))",
        )
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write(
            "CREATE TABLE ilp2 (a VARCHAR(32), b INT, c INT, d INT, e INT, \
             PRIMARY KEY (a, b) CLUSTERED, INDEX i(c), UNIQUE INDEX u(e)) \
             CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
        )
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE cim (id INT NOT NULL, a INT, b INT, c INT, \
             PRIMARY KEY (id) CLUSTERED, KEY ia (a, c), KEY ib (b, c))",
        )
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE qpg (a CHAR(2) PRIMARY KEY, b CHAR(2))")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ppg (a INT, b INT, UNIQUE KEY (a)) PARTITION BY LIST (a) \
             (PARTITION p0 VALUES IN (0,4), PARTITION p1 VALUES IN (1,3))",
        )
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("UPDATE ppg SET b = 41 WHERE a = 4")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
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
    session.node_conn().execute_write("DROP TABLE pdl").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW TABLES LIKE 'pdl'")),
        Vec::<Vec<String>>::new()
    );

    session
        .node_conn().execute_write(
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

    session
        .node_conn()
        .execute_write("CREATE SEQUENCE sq")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dseq (a INT DEFAULT (NEXTVAL(sq)))")
        .unwrap();
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE dseq"));
    assert!(
        shown[0][1].contains("DEFAULT (nextval(`test`.`sq`))"),
        "{shown:?}"
    );

    session
        .node_conn()
        .execute_write("CREATE TABLE src (a INT)")
        .unwrap();
    let error = session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
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
            .node_conn()
            .execute_write(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let grants = |session: &mut Session| {
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
        .node_conn()
        .execute_write("GRANT INSERT ON test.gtab TO gtu")
        .unwrap();
    assert_eq!(
        grants(&mut session),
        [[granted_by.as_str(), "Select,Insert"]]
    );
    session
        .node_conn()
        .execute_write("REVOKE SELECT ON test.gtab FROM gtu")
        .unwrap();
    assert_eq!(grants(&mut session), [[granted_by.as_str(), "Insert"]]);

    let default_roles = |session: &mut Session| {
        displayed(rows(
            session,
            "SELECT default_role_user FROM mysql.default_roles WHERE user = 'gtu'",
        ))
    };
    session
        .node_conn()
        .execute_write("SET DEFAULT ROLE gr_2 TO gtu")
        .unwrap();
    assert_eq!(default_roles(&mut session), [["gr_2"]]);
    session
        .node_conn()
        .execute_write("SET DEFAULT ROLE ALL TO gtu")
        .unwrap();
    assert_eq!(default_roles(&mut session), [["gr_1"], ["gr_2"]]);
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE GLOBAL TEMPORARY TABLE tg (c1 INT, c2 MEDIUMTEXT) ON COMMIT DELETE ROWS",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TEMPORARY TABLE tl (c1 INT, c2 MEDIUMTEXT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET @@session.tidb_tmp_table_max_size = 1048576")
        .unwrap();
    let row = |table: &str| format!("INSERT INTO {table} VALUES (1, REPEAT('x', 512*1024))");
    for table in ["tg", "tl"] {
        session.node_conn().control_transaction("BEGIN").unwrap();
        session.node_conn().execute_write(&row(table)).unwrap();
        session.node_conn().execute_write(&row(table)).unwrap();
        let error = session.node_conn().execute_write(&row(table)).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (1114, format!("The table '{table}' is full").as_str())
        );
        session.node_conn().control_transaction("ROLLBACK").unwrap();
    }
    // A LOCAL table's committed rows count against the next transaction.
    session.node_conn().execute_write(&row("tl")).unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session.node_conn().execute_write(&row("tl")).unwrap();
    let error = session.node_conn().execute_write(&row("tl")).unwrap_err();
    assert_eq!(error.code, 1114);
    session.node_conn().control_transaction("ROLLBACK").unwrap();
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE pidx (a INT, b INT, c INT, UNIQUE INDEX idx3 (c) WHERE b > 10)",
        )
        .unwrap();
    session
        .node_conn()
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
    use crate::domain::resultset_source::ResultSetSource;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1054)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    let names = |session: &mut Session, sql: &str| -> Vec<String> {
        let mut result = session.node_conn().execute(sql).expect("the query runs");
        let source = result.source();
        let columns = source
            .fields()
            .expect("fields")
            .into_iter()
            .map(|field| field.name)
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
    session.node_conn().execute_write("USE test").unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| format!("{} {}", row[0], row[3]))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    // A heap table: the handle is the hidden _tidb_rowid.
    session
        .node_conn()
        .execute_write("CREATE TABLE dp1 (a INT, b INT, c VARCHAR(20), INDEX ib (b))")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("DELETE FROM dp1 WHERE a = 1")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("DELETE FROM dp1 ORDER BY a LIMIT 1")
        .unwrap();
    assert_eq!(displayed(rows(&mut session, "SELECT a FROM dp1")), [["4"]]);
    // A clustered integer key and a unique secondary index.
    session
        .node_conn().execute_write(
            "CREATE TABLE dp2 (id INT PRIMARY KEY CLUSTERED, u INT, pad VARCHAR(20), UNIQUE KEY uu (u))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO dp2 VALUES (1, 100, 'p'), (2, 200, 'q'), (3, 300, 'r')")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("INSERT INTO dp2 VALUES (9, 200, 's')")
        .unwrap();
    // A common handle.
    session
        .node_conn().execute_write(
            "CREATE TABLE dp3 (k1 VARCHAR(10), k2 INT, v INT, w INT, PRIMARY KEY (k1, k2) CLUSTERED, KEY iv (v))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO dp3 VALUES ('a', 1, 5, 0), ('b', 2, 6, 0), ('c', 3, 7, 0)")
        .unwrap();
    session
        .node_conn()
        .execute_write("DELETE FROM dp3 WHERE w = 0 AND v = 6")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT k1, k2 FROM dp3 ORDER BY k1")),
        [["a", "1"], ["c", "3"]]
    );
    // A referenced table keeps every column (its foreign keys read them).
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE dpp (id INT PRIMARY KEY, name VARCHAR(10), INDEX iname (name))",
        )
        .unwrap();
    session
        .node_conn().execute_write(
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
    session.node_conn().execute_write("USE test").unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| format!("{} {}", row[0], row[3]))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    session
        .node_conn()
        .execute_write("CREATE TABLE mu1 (a INT, b INT, pad INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE mu2 (a INT, b INT, other INT, INDEX ib (b))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO mu1 VALUES (1, 1, 0), (2, 2, 0), (3, 9, 0)")
        .unwrap();
    session
        .node_conn()
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
        .node_conn()
        .execute_write("UPDATE mu1, mu2 SET mu1.a = 99 WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b FROM mu1 ORDER BY b")),
        [["99", "1"], ["99", "2"], ["3", "9"]]
    );
    // A column of `mu2` the SET expression reads is kept.
    session
        .node_conn()
        .execute_write("UPDATE mu1, mu2 SET mu1.pad = mu2.other WHERE mu1.b = mu2.b AND mu2.a = 20")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b, pad FROM mu1 ORDER BY b")),
        [["1", "0"], ["2", "200"], ["9", "0"]]
    );
    // Updating both sources keeps both whole.
    session
        .node_conn()
        .execute_write("UPDATE mu1, mu2 SET mu1.pad = 5, mu2.other = mu1.b WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b, other FROM mu2 ORDER BY b")),
        [["1", "1"], ["2", "2"], ["7", "300"]]
    );
    // A multi-table DELETE of one target keeps the other source whole and
    // prunes the target to the index and handle.
    session
        .node_conn()
        .execute_write("DELETE mu1 FROM mu1, mu2 WHERE mu1.b = mu2.b AND mu2.other = 2")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM mu1 ORDER BY b")),
        [["1"], ["9"]]
    );
    session
        .node_conn()
        .execute_write("DELETE mu1, mu2 FROM mu1, mu2 WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM mu2 ORDER BY b")),
        [["2"], ["7"]]
    );
    // A multi-table DELETE over one aliased source behaves as its
    // single-table form (this shape used to inherit the single-table prune).
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tn (a INT, b CHAR)")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "INSERT INTO tn VALUES (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a'),(1,'x'),(1,'y'),(1,'z')",
        )
        .unwrap();
    let ids = |session: &mut Session, sql: &str| -> Vec<String> {
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
        .node_conn()
        .execute_write("CREATE TABLE tm (a INT, b CHAR)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tm VALUES (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a')")
        .unwrap();
    for _ in 0..3 {
        session
            .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE st1 (id INT, k INT)")
        .unwrap();
    let values: Vec<String> = (1..=40).map(|id| format!("({id},{})", id % 3)).collect();
    session
        .node_conn()
        .execute_write(&format!("INSERT INTO st1 VALUES {}", values.join(",")))
        .unwrap();
    let ids = |session: &mut Session, sql: &str| -> Vec<String> {
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE sgb (j1 JSON, j2 JSON, a INT AS (JSON_EXTRACT(j1, '$.a')) STORED, \
             b VARCHAR(20) AS (JSON_KEYS(j2)) STORED, KEY idx_a (a), KEY idx_b (b))",
        )
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tv (a TINYINT, b SMALLINT, f FLOAT, g DOUBLE)")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET @@global.tidb_scatter_region = 'table'")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tsp (a INT PRIMARY KEY)")
        .unwrap();
    rows(
        &mut session,
        "SPLIT TABLE tsp BETWEEN (0) AND (40000) REGIONS 4",
    );
    session
        .node_conn()
        .execute_write("INSERT INTO tsp VALUES (1), (1000), (10002)")
        .unwrap();
    let sampled = |session: &mut Session| -> Vec<String> {
        displayed(rows(session, "SELECT * FROM tsp TABLESAMPLE REGIONS()"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tsp VALUES (20006), (50000)")
        .unwrap();
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session
        .node_conn()
        .execute_write("DELETE FROM tsp WHERE a = 1")
        .unwrap();
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(sampled(&mut session), ["1000", "10002", "20006", "50000"]);
    // The splits are store metadata: a DDL rebuilds the catalog from the
    // stored schema (`TestTableSampleNotSupportedPlanWarning`) and must not
    // forget them.
    session
        .node_conn()
        .execute_write("CREATE TABLE tsr (a INT PRIMARY KEY, b INT)")
        .unwrap();
    rows(
        &mut session,
        "SPLIT TABLE tsr BETWEEN (0) AND (10000) REGIONS 5",
    );
    session
        .node_conn()
        .execute_write("INSERT INTO tsr VALUES (1000, 1), (1001, 1), (2100, 2), (4500, 3)")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tho (a INT PRIMARY KEY, b INT, c INT, KEY idx(b))")
        .unwrap();
    let operators = |session: &mut Session, sql: &str| -> Vec<String> {
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn().execute_write(
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE teo (a INT PRIMARY KEY, b INT, c INT, KEY(b))")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    session
        .node_conn()
        .execute_write("PREPARE stmt FROM 'select ?'")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET @number = 1")
        .unwrap();
    rows(&mut session, "EXECUTE stmt USING @number");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT exec_count FROM information_schema.statements_summary WHERE digest_text LIKE 'select ?'",
        )),
        [["1"]]
    );
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go `handlePessimisticSelectForUpdate` drains the rows and takes the locks
/// inside `ExecStmt.Exec`, so a lock that fails after the drain is an `Exec`
/// error: `runStmt` finishes the statement with it (`FinishExecuteStmt`), and
/// the summary counts one execution with one error. The port dropped the
/// drained record set without closing it, so the statement never finished.
#[test]
fn failed_select_for_update_lock_finishes_the_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1131)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    rows(&mut session, "CREATE TABLE lock_summary (id int, v int)");
    rows(&mut session, "INSERT INTO lock_summary VALUES (1, 10)");
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM lock_summary WHERE v > 0 FOR UPDATE"
        )),
        [["10"]]
    );
    let mut contender = stack.factory.open_session(session_context(1132)).unwrap();
    contender.node_conn().execute_write("USE test").unwrap();
    contender
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert!(matches!(
        contender.node_conn().execute("SELECT v FROM lock_summary WHERE v > 0 FOR UPDATE NOWAIT"),
        Err(error) if error.code == 3572
    ));
    contender
        .node_conn()
        .control_transaction("ROLLBACK")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT exec_count, sum_errors FROM information_schema.statements_summary \
             WHERE digest_text LIKE '%lock_summary%for update nowait'",
        )),
        [["1", "1"]]
    );
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Delegates the analysis and fails the statement's closing cache update.
struct FailingStatsCacheUpdate {
    inner: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
    updated: Arc<std::sync::Mutex<Vec<i64>>>,
}

impl tidb_executor::cluster_analyze_seam::ClusterAnalyze for FailingStatsCacheUpdate {
    fn execute(
        &self,
        statement: &tidb_executor::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        killer: &tidb_sqlkiller::SqlKiller,
        historical_stats_enabled: &dyn Fn() -> bool,
        jobs: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
    ) -> Result<tidb_executor::real_tikv_analyze::ClusterAnalyzeReport, tidb_store_driver_error::sql_query_error::SqlQueryError>
    {
        self.inner.execute(
            statement,
            resource_group,
            approximate_counts,
            killer,
            historical_stats_enabled,
            jobs,
        )
    }

    fn update_stats_cache(
        &self,
        physical_ids: Vec<i64>,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        self.updated.lock().unwrap().extend(physical_ids);
        Err(tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(
            "statistics cache update failed",
        ))
    }
}

/// Go `AnalyzeExec.Next` ends in `return statsHandle.Update(ctx, infoSchema,
/// tableAndPartitionIDs...)`: one update after every table is saved, whose
/// failure is the statement's error although the statistics are stored. The
/// port refreshed per table and only logged the failure.
#[test]
fn failed_stats_cache_update_fails_the_analyze_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1133)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    rows(&mut session, "CREATE TABLE analyze_update_a (a INT)");
    rows(&mut session, "CREATE TABLE analyze_update_b (a INT)");
    rows(&mut session, "INSERT INTO analyze_update_a VALUES (1), (2)");
    rows(&mut session, "INSERT INTO analyze_update_b VALUES (3)");
    let updated = Arc::new(std::sync::Mutex::new(Vec::new()));
    let analyze = Arc::new(FailingStatsCacheUpdate {
        inner: crate::domain::cluster_session_node::node_services(&session.node().domain).analyze(),
        updated: Arc::clone(&updated),
    });
    session.node_conn().set_analyzer(&stack.factory, analyze);
    let error = session
        .node_conn()
        .execute("ANALYZE TABLE analyze_update_a, analyze_update_b")
        .err()
        .expect("the cache update failure is the statement's error");
    assert_eq!(error.message, "statistics cache update failed");
    let ids = displayed(rows(
        &mut session,
        "SELECT tidb_table_id FROM information_schema.tables \
         WHERE table_name IN ('analyze_update_a', 'analyze_update_b') ORDER BY table_name",
    ));
    assert_eq!(
        *updated.lock().unwrap(),
        ids.iter()
            .map(|row| row[0].parse::<i64>().unwrap())
            .collect::<Vec<_>>(),
        "one update over both tables, after both are analyzed"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(DISTINCT table_id) FROM mysql.stats_histograms \
                 WHERE table_id IN ({}, {})",
                ids[0][0], ids[1][0]
            ),
        )),
        [["2"]],
        "both tables' statistics are stored"
    );
}

/// Go's statement warnings are one buffer in order of occurrence: the casts
/// inside an `INSERT ... SELECT` overflow while the SELECT evaluates, and the
/// store conversion of their result overflows after. The port appended the
/// conversion row first.
#[test]
fn insert_select_warnings_keep_the_order_they_were_raised_in() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1110)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tiw (a DECIMAL(10,2))")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET @@sql_mode = ''")
        .unwrap();
    session
        .node_conn().execute_write(
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    for ddl in [
        "CREATE TABLE a(id INT PRIMARY KEY, x INT, y INT)",
        "CREATE TABLE b(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
        "CREATE TABLE c(id INT PRIMARY KEY, z INT)",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
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
            session.node_conn().execute_write(sql).unwrap();
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
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE sia(id INT PRIMARY KEY, x INT, y INT)",
        "CREATE TABLE sib(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let operators = |session: &mut Session, sql: &str| -> String {
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tfu (id INT PRIMARY KEY, x INT)")
        .unwrap();
    let operators = |session: &mut Session, sql: &str| -> Vec<String> {
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET @@tidb_hash_join_concurrency = 1")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    for ddl in [
        "CREATE TABLE a(id INT PRIMARY KEY, x INT, y INT, KEY(x), KEY(y))",
        "CREATE TABLE b(id INT PRIMARY KEY, y INT, z INT, KEY(y))",
        "CREATE TABLE c(id INT PRIMARY KEY, z INT)",
        "CREATE TABLE u(id INT, k INT, v VARCHAR(20), UNIQUE KEY uk(k), KEY(v))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
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
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tpx (id INT PRIMARY KEY, name VARCHAR(50), INDEX ip (name(3)))",
        )
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tfo (id INT PRIMARY KEY, a INT)")
        .unwrap();
    let warnings =
        |session: &mut Session| -> Vec<Vec<String>> { displayed(rows(session, "SHOW WARNINGS")) };
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
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tdd (d_sk INT PRIMARY KEY, d_year INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tss (s_date INT, s_amt INT)")
        .unwrap();
    let plan = |session: &mut Session, filter: &str| -> String {
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT, b INT, c VARCHAR(20))")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ti (id INT PRIMARY KEY, k INT, k2 INT, v INT, KEY(k), KEY(k2))",
        )
        .unwrap();
    session
        .node_conn()
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
    let cached_at = |session: &mut Session, sql: &str, max: i64| -> bool {
        session
            .node_conn()
            .execute_write(&format!("SET @@tidb_plan_cache_max_plan_size = {max}"))
            .unwrap();
        session
            .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tu (a INT, b INT, c VARCHAR(2048))")
        .unwrap();
    let leg = "select * from tu, tu t1 where t1.c=space(2048) and tu.c=space(2048) and tu.a=t1.b";
    let union = |legs: usize| vec![leg; legs].join(" union all ");
    let cached = |session: &mut Session, legs: usize| -> bool {
        session
            .node_conn()
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
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tus (a INT PRIMARY KEY, b INT, c INT, KEY(b))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tus VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    let operators = |session: &mut Session, sql: &str| -> Vec<String> {
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
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
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
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        operators(&mut session, "SELECT * FROM tus WHERE b = 1")[0],
        "IndexLookUp"
    );

    // A local temporary table is always read through a union scan, and its
    // free scans make the index the cheaper way to deliver an order.
    session
        .node_conn()
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tij (a INT PRIMARY KEY, b INT)")
        .unwrap();
    let values = (2..100)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .node_conn()
        .execute_write(&format!("INSERT INTO tij VALUES {values}"))
        .unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tij VALUES (1, 1)")
        .unwrap();
    session
        .node_conn()
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
    session.node_conn().control_transaction("ROLLBACK").unwrap();
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
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tdz (a INT, b VARCHAR(10), c DECIMAL(10,2))")
        .unwrap();
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
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
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tpi (a INT PRIMARY KEY, b INT, c INT, KEY(c))",
        "CREATE TABLE tpd (a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY(b), KEY(c))",
        "CREATE TABLE tpp (a INT PRIMARY KEY, b INT) PARTITION BY HASH(a) PARTITIONS 3",
        "INSERT INTO tpd VALUES (1,1,1),(2,2,2),(3,3,3)",
        "INSERT INTO tpp VALUES (1,1),(2,2),(3,3)",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
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
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
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
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// Inside a transaction a locking SELECT over a point key plans as the point
/// plan itself carrying `lock` (Go `PointGetPlan.Lock` /
/// `BatchPointGetPlan.Lock`), not as a `SelectLock` above it; a partitioned
/// table's integer-handle pin takes the same fast plan, outside a transaction
/// too. Expected rows: Go 1.27 on this branch (`TestZZ` probes `US9`, `US19`).
#[test]
fn locking_point_reads_in_a_transaction_plan_as_point_get_with_lock() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1121)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tlk (a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY(b), KEY(c))",
        "CREATE TABLE tlp (a INT PRIMARY KEY, b INT) PARTITION BY HASH(a) PARTITIONS 3",
        "INSERT INTO tlk VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4)",
        "INSERT INTO tlp VALUES (1,1),(2,2),(3,3)",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .map(|row| format!("{} {} {} {} {}", row[0], row[1], row[2], row[4], row[5]))
            .collect()
    };
    // Autocommit: no lock, and the partitioned pin is already a fast plan.
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlp WHERE a = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlp, partition:p1 handle:1"]
    );
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlk WHERE a = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlk handle:1, lock"]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlk WHERE b = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlk, index:b(b) lock"]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlk WHERE a IN (1, 2) FOR UPDATE"),
        ["Batch_Point_Get_1 2.00 0.00 table:tlk handle:[1 2], keep order:false, desc:false, lock"]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlp WHERE a = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlp, partition:p1 handle:1, lock"]
    );
    // The lock is taken: a second session cannot lock the row without waiting.
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM tlk WHERE a = 1 FOR UPDATE")),
        [["1"]]
    );
    let mut contender = stack.factory.open_session(session_context(1122)).unwrap();
    contender.node_conn().execute_write("USE test").unwrap();
    contender
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert!(
        matches!(
            contender.node_conn().execute("SELECT b FROM tlk WHERE a = 1 FOR UPDATE NOWAIT"),
            Err(error) if error.code == 3572
        ),
        "the point lock must be held"
    );
    // A prepared statement caches the fast plan: its second EXECUTE, with a
    // different key, locks that key too.
    session
        .node_conn()
        .execute_write("PREPARE lockp FROM 'SELECT b FROM tlk WHERE a = ? FOR UPDATE'")
        .unwrap();
    session.node_conn().execute_write("SET @lk = 3").unwrap();
    for key in [3, 2] {
        session
            .node_conn()
            .execute_write(&format!("SET @lk = {key}"))
            .unwrap();
        assert_eq!(
            displayed(rows(&mut session, "EXECUTE lockp USING @lk")),
            [[key.to_string()]]
        );
        assert!(
            matches!(
                contender.node_conn().execute(&format!(
                    "SELECT b FROM tlk WHERE a = {key} FOR UPDATE NOWAIT"
                )),
                Err(error) if error.code == 3572
            ),
            "EXECUTE must lock key {key}"
        );
    }
    // The same holds for a prepared locking read that is no point read.
    session
        .node_conn()
        .execute_write("PREPARE lockq FROM 'SELECT b FROM tlk WHERE c = ? FOR UPDATE'")
        .unwrap();
    session.node_conn().execute_write("SET @lc = 4").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE lockq USING @lc")),
        [["4"]]
    );
    assert!(
        matches!(
            contender.node_conn().execute("SELECT b FROM tlk WHERE a = 4 FOR UPDATE NOWAIT"),
            Err(error) if error.code == 3572
        ),
        "a prepared locking index read must lock its row"
    );
    contender
        .node_conn()
        .control_transaction("ROLLBACK")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// A correlated scalar subquery's aggregation, decorrelated into a join,
/// enumerates its index-join inner side the way Go does: the single/multi-read
/// kind a parent asks for does not refuse an index-join inner task, and the
/// decorrelation reuses the inner projection's node (no new plan id). The
/// plan ids and the partial aggregate's column ids below are Go 1.27's on
/// this branch (`TestZZ` probe `DZ5`).
#[test]
fn correlated_subquery_enumeration_draws_the_same_ids_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1123)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tcs (a INT, b VARCHAR(10), c DECIMAL(10,2), KEY(a))",
        "CREATE TABLE tcu (a INT, b INT, c DECIMAL(10,2), KEY(a))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let text = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'verbose' SELECT a FROM tcs WHERE c < \
         (SELECT AVG(c) FROM tcu WHERE tcu.a = tcs.a)",
    ))
    .into_iter()
    .map(|row| format!("{} {}", row[0], row[5]))
    .collect::<Vec<_>>()
    .join("\n");
    for expected in [
        "Selection_87(Build) not(isnull(Column#11))",
        "HashAgg_95 group by:test.tcu.a, funcs:avg(Column#31, Column#32)->Column#11, \
         funcs:firstrow(test.tcu.a)->test.tcu.a",
        "TableReader_96 data:HashAgg_88",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in\n{text}");
    }
}

/// The skyline's preferRange filter keeps an index path that serves a required
/// order next to a table range scan that does not: a stream aggregation over
/// `id < 10 GROUP BY k` reads the index `k` (the handle column answers the
/// filter) rather than sorting a table range. Expected rows: Go 1.27 on this
/// branch (`TestZZ` probe `SB3`).
#[test]
fn ordered_request_keeps_the_index_path_next_to_a_table_range() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1124)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tsk (id INT NOT NULL, k INT NOT NULL DEFAULT 0, c CHAR(120) NOT NULL \
             DEFAULT '', PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */, KEY k (k))",
        )
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'verbose' SELECT /*+ STREAM_AGG() */ k, COUNT(*) FROM tsk \
         WHERE id < 10 GROUP BY k",
    ))
    .into_iter()
    .take(3)
    .map(|row| format!("{} {} {}", row[0], row[1], row[2]))
    .collect::<Vec<_>>();
    assert_eq!(
        plan,
        [
            "Projection_5 8.00 169804.71",
            "└─StreamAgg_16 8.00 169803.12",
            "  └─IndexReader_17 8.00 169000.72",
        ]
    );
}

/// Static partition pruning spends the plan ids Go does: one for the
/// partition expression of a HASH or plain RANGE table, one for every
/// partition copy (`NewBaseLogicalPlan` before `SetID`), and the union's MPP
/// candidate. Expected rows: Go 1.27 on this branch (`TestZZ` probe `PP1`).
#[test]
fn static_partition_pruning_spends_the_plan_ids_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1125)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "SET @@tidb_partition_prune_mode = 'static'",
        "CREATE TABLE tsp (a BIT(1) DEFAULT NULL, b INT DEFAULT NULL) PARTITION BY HASH(a) PARTITIONS 3",
        "CREATE TABLE tsr (a INT, b INT, KEY(b)) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), \
         PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (MAXVALUE))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .filter(|row| row.len() > 5 && row[3] == "root")
            .map(|row| format!("{} {} {} {}", row[0], row[1], row[2], row[5]))
            .collect()
    };
    assert_eq!(
        plan(&mut session, "SELECT * FROM tsp"),
        [
            "PartitionUnion_8 20000.00 142694.60 ",
            "├─TableReader_11 10000.00 356736.49 data:TableFullScan_10",
            "└─TableReader_13 10000.00 356736.49 data:TableFullScan_12",
        ]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tsr WHERE a = 5"),
        ["TableReader_9 10.00 336428.45 data:Selection_8"]
    );
}

/// Join candidates spend the plan ids Go's constructors do even when a hint or
/// a refused child property drops them afterwards: the hash joins are built
/// before a `MERGE_JOIN` hint returns the merge joins, a skipped hash join and
/// a vetoed natural merge join are built then discarded, and an index join's
/// four variants are all built. Expected rows: Go 1.27 on this branch
/// (`TestZZ` probe `HJ1`).
#[test]
fn join_hints_spend_the_plan_ids_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1126)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tja (id INT PRIMARY KEY, x INT, y INT, KEY(x), KEY(y))",
        "CREATE TABLE tjb (id INT PRIMARY KEY, y INT, z INT, KEY(y))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .filter(|row| row.len() > 5 && row[3] == "root")
            .map(|row| format!("{} {} {}", row[0], row[1], row[2]))
            .collect()
    };
    assert_eq!(
        plan(
            &mut session,
            "SELECT /*+ MERGE_JOIN(tja, tjb) */ * FROM tja JOIN tjb ON tja.x = tjb.id"
        )[..4],
        [
            "MergeJoin_12 12487.50 7621831.92",
            "├─TableReader_29(Build) 10000.00 429797.30",
            "└─Sort_27(Probe) 9990.00 7182059.61",
            "  └─TableReader_23 9990.00 462937.25",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "SELECT /*+ NO_HASH_JOIN(tja, tjb) */ * FROM tja JOIN tjb ON tja.x = tjb.id"
        )[..3],
        [
            "IndexJoin_13 12487.50 5564987.43",
            "├─TableReader_47(Build) 9990.00 462937.25",
            "└─TableReader_52(Probe) 9990.00 18.78",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "SELECT /*+ NO_MERGE_JOIN(tja, tjb) */ * FROM tja JOIN tjb ON tja.x = tjb.id"
        )[1..3],
        [
            "├─TableReader_26(Build) 9990.00 462937.25",
            "└─TableReader_37(Probe) 10000.00 429797.30",
        ]
    );
}

/// Go keeps the index join candidates of the side a force hint does not name:
/// the hint only prefers a built candidate afterwards
/// (`preferIndexJoinFamily`), so the other side's inner task still spends its
/// ids. Expected ids: Go 1.27 on this branch (`TestZZ` probe `IJ1`).
#[test]
fn force_index_join_hint_still_builds_the_other_sides_inner_task() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1127)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE ihs1 (j1 JSON, a INT, b INT, KEY idx_a (a), KEY idx_b (b), KEY idx_a_b (a, b))",
        "CREATE TABLE ihs2 (j1 JSON, a INT, b INT, KEY idx_a (a), KEY idx_b (b), KEY idx_a_b (a, b))",
        "INSERT INTO ihs1 VALUES ('{\"a\": 1}',1,1),('{\"a\": 1}',1,1),('{\"a\": 1}',1,1),('{\"a\": 1}',1,1),('{\"a\": 1}',1,1)",
        "INSERT INTO ihs2 VALUES ('{\"a\": 1}',1,1)",
        "ANALYZE TABLE ihs1, ihs2",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let ids = |session: &mut Session, hint: &str| -> Vec<String> {
        displayed(rows(
            session,
            &format!(
                "EXPLAIN FORMAT = 'verbose' SELECT /*+ {hint}(ihs2) */ * FROM ihs1 JOIN ihs2 ON ihs1.a = ihs2.a"
            ),
        ))
        .into_iter()
        .filter(|row| row.len() > 5 && row[3] == "root")
        .skip(1)
        .map(|row| row[0].clone())
        .collect()
    };
    assert_eq!(
        ids(&mut session, "INL_JOIN"),
        ["└─IndexJoin_22", "  ├─TableReader_62(Build)", "  └─IndexLookUp_59(Probe)"]
    );
    assert_eq!(
        ids(&mut session, "INL_HASH_JOIN"),
        ["└─IndexHashJoin_26", "  ├─TableReader_62(Build)", "  └─IndexLookUp_59(Probe)"]
    );
}

/// A filter over `_tidb_rowid` has no column statistics in Go (`PseudoTable`
/// walks `TableInfo.Columns`), so the data source keeps the default selection
/// factor and `adjustCountAfterAccess` lifts the range's count to the table's.
/// Expected rows: Go 1.27 on this branch (`TestZZ` probe `RW2`).
#[test]
fn rowid_range_estimates_use_the_default_selectivity_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1128)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE rowid_range (a INT, b INT, KEY b (b))")
        .unwrap();
    let counts = |session: &mut Session, sql: &str| -> Vec<(String, String)> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| (row[0].trim_start_matches([' ', '└', '─']).to_string(), row[1].clone()))
            .collect()
    };
    let pair = |id: &str, rows: &str| (id.to_string(), rows.to_string());
    assert_eq!(
        counts(&mut session, "SELECT a FROM rowid_range WHERE _tidb_rowid BETWEEN 1 AND 100"),
        [
            pair("TableReader", "8000.00"),
            pair("Projection", "8000.00"),
            pair("TableRangeScan", "10000.00"),
        ]
    );
    assert_eq!(
        counts(&mut session, "DELETE FROM rowid_range WHERE _tidb_rowid BETWEEN 1 AND 100"),
        [
            pair("Delete", "N/A"),
            pair("TableReader", "10000.00"),
            pair("TableRangeScan", "10000.00"),
        ]
    );
}

/// `EXPLAIN FORMAT='verbose'` records Go's path-pruning and heuristic-selection
/// notes (`getPruningInfo`, `derivePathStatsAndTryHeuristics`) for `SHOW
/// WARNINGS`, and an index point get draws no scan id before its own. Expected
/// notes and ids: Go 1.27 on this branch (`TestZZ` probes `NP1` and `NP3`).
#[test]
fn verbose_explain_records_path_pruning_and_heuristic_notes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1130)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE note_t (a INT PRIMARY KEY, b INT, c INT, d INT, UNIQUE KEY ub (b), KEY ic (c))",
        "CREATE TABLE note_u (a INT, b INT, c INT, KEY ia (a), KEY ib (b), KEY iab (a, b))",
        "INSERT INTO note_t VALUES (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5)",
        "INSERT INTO note_u VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)",
        "ANALYZE TABLE note_t, note_u",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let notes = |session: &mut Session, sql: &str| -> Vec<String> {
        rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}"));
        displayed(rows(session, "SHOW WARNINGS"))
            .into_iter()
            .filter(|row| row[0] == "Note")
            .map(|row| row[2].clone())
            .collect()
    };
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_u WHERE a = 1 AND b = 2"),
        ["[note_u,iab] remain after pruning paths for note_u given Prop{SortItems: [], TaskTp: rootTask}"]
    );
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_u WHERE b > 1 ORDER BY b"),
        [
            "[note_u,ib] remain after pruning paths for note_u given Prop{SortItems: [], TaskTp: rootTask}",
            "[note_u,ib] remain after pruning paths for note_u given Prop{SortItems: [{test.note_u.b asc}], TaskTp: rootTask}",
        ]
    );
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_t WHERE b = 2 AND c > 0"),
        ["unique index ub of note_t is selected since the path only has point ranges with double scan"]
    );
    assert_eq!(
        notes(&mut session, "SELECT * FROM note_t WHERE a = 1 AND c > 0"),
        ["handle of note_t is selected since the path only has point ranges"]
    );
    // A join with no filter still estimates each index's full range, so an
    // ordered request prunes the indexes against the table path.
    assert_eq!(
        notes(
            &mut session,
            "SELECT * FROM note_t JOIN note_u ON note_t.a = note_u.a"
        )[..1],
        ["[note_t] remain after pruning paths for note_t given Prop{SortItems: [{test.note_t.a asc}], TaskTp: rootTask}"]
    );
    let point_get_ids = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'verbose' SELECT * FROM note_t WHERE b = 2 AND c > 0",
    ))
    .into_iter()
    .map(|row| row[0].trim_start_matches([' ', '└', '─']).to_string())
    .collect::<Vec<_>>();
    assert_eq!(point_get_ids, ["Selection_6", "Point_Get_5"]);
}

/// `EXPLAIN FORMAT='hint'` generates the hints that reproduce the plan
/// (`GenHintsFromFlatPlan`) followed by the statement's own. Expected rows: Go
/// 1.27 on this branch (`TestZZ` probes `HT1` and `HT2`).
#[test]
fn explain_format_hint_reproduces_the_plan_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1131)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE hint_t1 (a INT PRIMARY KEY, b INT, c INT, d INT, KEY ib (b), KEY ic (c), KEY ibc (b, c))",
        "CREATE TABLE hint_t2 (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "INSERT INTO hint_t1 VALUES (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5)",
        "INSERT INTO hint_t2 VALUES (1,1,1),(2,2,2),(3,3,3)",
        "ANALYZE TABLE hint_t1, hint_t2",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let hint = |session: &mut Session, sql: &str| -> String {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'hint' {sql}")))[0][0].clone()
    };
    assert_eq!(
        hint(&mut session, "SELECT * FROM hint_t1 WHERE a > 2 ORDER BY a"),
        "use_index(@`sel_1` `test`.`hint_t1`), order_index(@`sel_1` `test`.`hint_t1` `primary`)"
    );
    assert_eq!(
        hint(&mut session, "SELECT b FROM hint_t1 WHERE b > 1"),
        "use_index(@`sel_1` `test`.`hint_t1` `ib`), no_order_index(@`sel_1` `test`.`hint_t1` `ib`)"
    );
    assert_eq!(
        hint(&mut session, "SELECT * FROM hint_t1 WHERE b = 1 OR c = 2"),
        "use_index_merge(@`sel_1` `hint_t1` `ib`, `ic`)"
    );
    assert_eq!(
        hint(
            &mut session,
            "SELECT /*+ max_execution_time(1000) use_index(hint_t1, ib) */ * FROM hint_t1 WHERE b > 1"
        ),
        "use_index(@`sel_1` `test`.`hint_t1` `ib`), no_order_index(@`sel_1` `test`.`hint_t1` `ib`), \
         max_execution_time(1000), use_index(`hint_t1` `ib`)"
    );
    assert_eq!(
        hint(&mut session, "UPDATE hint_t1 SET c = 1 WHERE b = 2"),
        "use_index(@`upd_1` `test`.`hint_t1`), no_order_index(@`upd_1` `test`.`hint_t1` `primary`)"
    );
    // A point get is a complete plan that needs no hint.
    assert_eq!(hint(&mut session, "SELECT * FROM hint_t1 WHERE a = 1"), "");
}

/// The statement summary records Go's `PLAN_HINT` (the hints reproducing the
/// plan plus the statement's own of the kinds `getEncodedPlan` keeps) and the
/// statement's binding text and digest, with the default database filled in.
/// Expected values: Go 1.27 on this branch (`TestZZ` probe `SS4`).
#[test]
fn statement_summary_records_plan_hint_and_binding_text_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1132)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ss_hint")
        .unwrap();
    session.node_conn().execute_write("USE ss_hint").unwrap();
    for ddl in [
        "CREATE TABLE st (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "CREATE TABLE su (a INT, b INT, KEY ia (a))",
        "INSERT INTO st VALUES (1,1,1),(2,2,2),(3,3,3)",
        "ANALYZE TABLE st",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    rows(&mut session, "SELECT /*+ use_index(st, ib) max_execution_time(1000) */ * FROM st WHERE b > 1");
    rows(&mut session, "INSERT INTO su SELECT a, b FROM st");
    let summary = displayed(rows(
        &mut session,
        "SELECT binding_digest_text, plan_hint FROM information_schema.statements_summary \
         WHERE schema_name = 'ss_hint' AND (digest_text LIKE 'select%' OR digest_text LIKE 'insert into `su` select%') \
         ORDER BY digest_text",
    ));
    assert_eq!(
        summary,
        [
            [
                "insert into `ss_hint` . `su` select `a` , `b` from `ss_hint` . `st`",
                "use_index(@`sel_1` `ss_hint`.`st` `ib`), no_order_index(@`sel_1` `ss_hint`.`st` `ib`)"
            ],
            [
                "select * from `ss_hint` . `st` where `b` > ?",
                "use_index(@`sel_1` `ss_hint`.`st` `ib`), no_order_index(@`sel_1` `ss_hint`.`st` `ib`), \
                 max_execution_time(1000)"
            ],
        ]
    );
}

/// Go `selectResult.fetchResp` merges every coprocessor response's details
/// into `StmtCtx.ExecDetails`, which the summary reads as `SUM_COP_TASK_NUM`
/// and the key counts. Expected values: Go 1.27 on this branch (`TestZZ` probe
/// `CS1`); point gets never reach the coprocessor.
#[test]
fn statement_summary_records_cop_task_details_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1133)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ss_cop")
        .unwrap();
    session.node_conn().execute_write("USE ss_cop").unwrap();
    for ddl in [
        "CREATE TABLE ct (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "CREATE TABLE cu (a INT, b INT, KEY ia (a))",
        "INSERT INTO ct VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)",
        "INSERT INTO cu VALUES (1,1),(2,2)",
        "ANALYZE TABLE ct, cu",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    for query in [
        "SELECT a FROM ct WHERE b > 1",
        "SELECT * FROM ct JOIN cu ON ct.a = cu.a WHERE ct.b > 0",
        "SELECT COUNT(*) FROM ct GROUP BY b",
        "SELECT * FROM ct WHERE a = 1",
        "SELECT * FROM ct",
    ] {
        rows(&mut session, query);
    }
    let summary = displayed(rows(
        &mut session,
        "SELECT digest_text, sum_cop_task_num, avg_total_keys, max_total_keys, \
         avg_processed_keys, max_processed_keys FROM information_schema.statements_summary \
         WHERE schema_name = 'ss_cop' AND digest_text LIKE 'select%' ORDER BY digest_text",
    ));
    assert_eq!(
        summary,
        [
            ["select * from `ct`", "1", "0", "0", "5", "5"],
            [
                "select * from `ct` join `cu` on `ct` . `a` = `cu` . `a` where `ct` . `b` > ?",
                "2", "0", "0", "7", "7"
            ],
            ["select * from `ct` where `a` = ?", "0", "0", "0", "0", "0"],
            ["select `a` from `ct` where `b` > ?", "1", "0", "0", "4", "4"],
            ["select count ( ? ) from `ct` group by `b`", "1", "0", "0", "5", "5"],
        ]
    );
}

/// Go plans an `UPDATE`/`DELETE` read like a SELECT and reads it through the
/// same coprocessor `TableReader`, so its responses reach `StmtCtx.ExecDetails`
/// exactly as a query's do; a point write never reaches the coprocessor.
/// Expected values: the Go server on TiKV (same statements, `nn_dml.ct`),
/// whose cop task counts and processed keys this in-process store reports
/// alike (TiKV's MVCC `total_keys` are TiKV's own and are not compared).
#[test]
fn a_dml_read_reaches_the_coprocessor_like_a_select() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1135)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ss_dml")
        .unwrap();
    session.node_conn().execute_write("USE ss_dml").unwrap();
    for statement in [
        "CREATE TABLE ct (a INT PRIMARY KEY, b INT, c INT, KEY ib (b))",
        "INSERT INTO ct VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)",
        "ANALYZE TABLE ct",
        "UPDATE ct SET c = c + 1 WHERE c > 1",
        "DELETE FROM ct WHERE c = 3",
        "UPDATE ct SET c = 0 WHERE a = 1",
    ] {
        session.node_conn().execute_write(statement).unwrap();
    }
    let summary = displayed(rows(
        &mut session,
        "SELECT digest_text, sum_cop_task_num, avg_processed_keys, max_processed_keys, \
         avg_affected_rows FROM information_schema.statements_summary \
         WHERE schema_name = 'ss_dml' AND (digest_text LIKE 'update%' \
         OR digest_text LIKE 'delete%') ORDER BY digest_text",
    ));
    assert_eq!(
        summary,
        [
            ["delete from `ct` where `c` = ?", "1", "5", "5", "1"],
            ["update `ct` set `c` = ? where `a` = ?", "0", "0", "0", "1"],
            [
                "update `ct` set `c` = `c` + ? where `c` > ?",
                "1",
                "5",
                "5",
                "4"
            ],
        ]
    );
}

/// A multi-table `UPDATE` whose index-join inner side is a covering
/// `IndexReader` over the target table reads that side through the
/// coprocessor like a SELECT, and the write still needs each inner row's
/// handle, which the covering index response carries. Expected rows:
/// `tests/integrationtest/r/executor/write.result` (Go).
#[test]
fn a_covering_index_join_update_reads_its_target_handle_from_the_coprocessor() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1136)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ij_dml")
        .unwrap();
    session.node_conn().execute_write("USE ij_dml").unwrap();
    for statement in [
        "CREATE TABLE t (a VARCHAR(1), INDEX idx(a))",
        "CREATE TABLE t1 (a VARCHAR(5), INDEX idx(a))",
        "INSERT INTO t VALUES ('a'), ('b')",
        "INSERT INTO t1 VALUES ('a'), ('bbbbb')",
        "UPDATE /*+ INL_JOIN(t) */ t, t1 SET t.a = 'a' WHERE t.a = t1.a",
        "UPDATE /*+ INL_HASH_JOIN(t) */ t, t1 SET t.a = 'a' WHERE t.a = t1.a",
    ] {
        session.node_conn().execute_write(statement).unwrap();
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY a")),
        [["a"], ["b"]]
    );
}

/// `CREATE BINDING FROM HISTORY USING PLAN DIGEST` builds each binding from the
/// statement the summary recorded under that plan digest and the plan hint it
/// captured (Go `constructSQLBindOPFromPlanDigest`). Expected rows: Go 1.27 on
/// this branch (`TestZZ` probe `BH2`).
#[test]
fn create_binding_from_history_uses_the_recorded_plan_hint_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1134)).unwrap();
    session
        .node_conn()
        .execute_write("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS bh1")
        .unwrap();
    session.node_conn().execute_write("USE bh1").unwrap();
    for ddl in [
        "CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, KEY ib (b), KEY ic (c))",
        "CREATE TABLE u (a INT, b INT, KEY ia (a))",
        "INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8)",
        "INSERT INTO u VALUES (1,1),(2,2)",
        "ANALYZE TABLE t, u",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    for query in [
        "SELECT * FROM t WHERE b = 2",
        "SELECT * FROM t WHERE c > 5",
        "SELECT t.a FROM t JOIN u ON t.a = u.a WHERE t.b > 0",
    ] {
        rows(&mut session, query);
    }
    for (variable, pattern) in [
        ("@pd1", "select * from `t` where `b`%"),
        ("@pd2", "select * from `t` where `c`%"),
        ("@pd3", "select `t` . `a` from `t` join%"),
    ] {
        session
            .node_conn()
            .execute_write(&format!(
                "SET {variable} = (SELECT plan_digest FROM information_schema.statements_summary \
                 WHERE schema_name = 'bh1' AND digest_text LIKE '{pattern}' LIMIT 1)"
            ))
            .unwrap();
    }
    session
        .node_conn()
        .execute_write("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST @pd1")
        .unwrap();
    // The second spelling covers the same statement, so Go skips it with a note.
    session
        .node_conn()
        .execute_write("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST @pd2, @pd2")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Warning", "1105", "37da0e4188d56394b943d77850b13c70ea2c8b962a72de5ce6700ff3a5d34d9f is ignored because it corresponds to the same SQL digest as another Plan Digest"]]
    );
    session
        .node_conn()
        .execute_write("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST @pd3")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT original_sql, bind_sql, default_db, status, charset, collation, source, \
             sql_digest, plan_digest FROM mysql.bind_info WHERE source = 'history' \
             ORDER BY original_sql",
        )),
        [
            [
                "select * from `bh1` . `t` where `b` = ?",
                "SELECT /*+ use_index(@`sel_1` `bh1`.`t`) no_order_index(@`sel_1` `bh1`.`t` `primary`)*/ * FROM `bh1`.`t` WHERE `b` = 2",
                "bh1", "enabled", "utf8mb4", "utf8mb4_general_ci", "history",
                "859898f263e060dc0f7e3f98d6b1b676127b047c0e4bc332524fa75bdf1bb7aa",
                "ddbd56d504e06fd3d48488e68ac58e7d084ddfd86eac943d609b4b70a9c6e575",
            ],
            [
                "select * from `bh1` . `t` where `c` > ?",
                "SELECT /*+ use_index(@`sel_1` `bh1`.`t`) no_order_index(@`sel_1` `bh1`.`t` `primary`)*/ * FROM `bh1`.`t` WHERE `c` > 5",
                "bh1", "enabled", "utf8mb4", "utf8mb4_general_ci", "history",
                "b6d21e1ea8c2ce350cdee0dce9d442878d9437ea491a5a3179894d1b39cfb5c6",
                "37da0e4188d56394b943d77850b13c70ea2c8b962a72de5ce6700ff3a5d34d9f",
            ],
            [
                "select `t` . `a` from `bh1` . `t` join `bh1` . `u` on `t` . `a` = `u` . `a` where `t` . `b` > ?",
                "SELECT /*+ merge_join(`bh1`.`u`) use_index(@`sel_1` `bh1`.`u` `ia`) order_index(@`sel_1` `bh1`.`u` `ia`) use_index(@`sel_1` `bh1`.`t`) order_index(@`sel_1` `bh1`.`t` `primary`)*/ `t`.`a` FROM `bh1`.`t` JOIN `bh1`.`u` ON `t`.`a` = `u`.`a` WHERE `t`.`b` > 0",
                "bh1", "enabled", "utf8mb4", "utf8mb4_general_ci", "history",
                "28597854f0874fca45c632da580407490e8ba7ed63f267ab6449e5e2f06c5346",
                "900f2884f174fbe7eef63c4e3e582e61db570e297feffbf7ec39994b8e6cc639",
            ],
        ]
    );
    assert_eq!(
        session
            .node_conn()
            .execute_write("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST 'nosuchdigest'")
            .err()
            .unwrap()
            .message,
        "can't find any plans for 'nosuchdigest'"
    );
    assert_eq!(
        session
            .node_conn()
            .execute_write("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST ' , '")
            .err()
            .unwrap()
            .message,
        "plan digest is empty"
    );
    rows(&mut session, "SELECT * FROM t WHERE b = 2");
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@last_plan_from_binding")),
        [["1"]]
    );
}

/// `exhaustPhysicalPlans4LogicalApply` builds the embedded hash join before
/// the Apply itself, so the Apply's id is one past the hash join's. Expected
/// ids: Go 1.27 on this branch (`TestZZ` probe `AP1`).
#[test]
fn apply_enumeration_spends_the_embedded_hash_join_id() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1129)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "SET @@session.tidb_opt_insubq_to_join_and_agg = 0",
        "CREATE TABLE apply_ids (a INT PRIMARY KEY, b INT, c INT, KEY idx (b))",
        "INSERT INTO apply_ids VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 3, 4), (5, 3, 5)",
        "ANALYZE TABLE apply_ids",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'verbose' SELECT t.c IN (SELECT COUNT(*) FROM apply_ids s, apply_ids t1 \
         WHERE s.b = t.a AND s.b = 3 AND s.a = t1.a) FROM apply_ids t",
    ));
    let ids: Vec<&str> = plan
        .iter()
        .filter(|row| row.len() > 5 && row[3] == "root")
        .map(|row| row[0].trim_start_matches([' ', '├', '└', '─', '│']))
        .collect();
    assert_eq!(&ids[..2], ["Projection_15", "Apply_18"]);
}

/// Statements both engines reject (or, for the one MODIFY, accept) used to be
/// skipped by the corpus replay as "both rejected", which hid wrong codes. Each
/// expectation below is what Go on this branch answers (probe sets `PL3`-`PL5`).
#[test]
fn rejected_statements_answer_with_go_codes_and_messages() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1130)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE ge (a INT, b INT, c INT AS (a + b))",
        "CREATE TABLE tv0 (a INT PRIMARY KEY, b INT)",
        "CREATE VIEW vv AS SELECT * FROM tv0",
        "CREATE TABLE pt (a INT, b VARCHAR(10)) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), \
         PARTITION p2 VALUES LESS THAN (30))",
        "CREATE TABLE pt3 (a INT, b VARCHAR(10))",
        "INSERT INTO pt VALUES (1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')",
        "INSERT INTO pt3 VALUES (1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')",
        "CREATE TABLE tc1 (c VARCHAR(10) COLLATE utf8_general_ci, d INT)",
        "CREATE TABLE tc2 (c VARCHAR(10) COLLATE utf8_unicode_ci, d INT)",
        "CREATE TABLE dz (a INT) PARTITION BY RANGE (10 DIV a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        "ALTER TABLE tv0 MODIFY tv0.b BIGINT",
        "CREATE TABLE jd8 (id INT PRIMARY KEY, j2 JSON DEFAULT (JSON_ARRAY(id, id * 2)))",
        "CREATE VIEW v_a AS SELECT * FROM tv0",
        "CREATE VIEW v_b AS SELECT * FROM v_a",
        "CREATE TABLE sd (c INT, c1 VARCHAR(32) DEFAULT (STR_TO_DATE('01-01', '%Y-%m-%d')))",
    ] {
        session
            .node_conn()
            .execute_write(ddl)
            .unwrap_or_else(|error| panic!("{ddl}: {error:?}"));
    }
    let write_error = |session: &mut Session, sql: &str| {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        (error.code, error.message)
    };
    for (sql, code, message) in [
        (
            "INSERT INTO ge (c) SELECT a, b FROM ge",
            1136,
            "Column count doesn't match value count at row 1",
        ),
        (
            "INSERT INTO ge SELECT * FROM ge",
            3105,
            "The value specified for generated column 'c' in table 'ge' is not allowed.",
        ),
        (
            "INSERT INTO tv0 VALUES (11, (SELECT NOT b))",
            1054,
            "Unknown column 'b' in 'field list'",
        ),
        (
            "ALTER TABLE vv ADD COLUMN x INT",
            1347,
            "'test.vv' is not BASE TABLE",
        ),
        ("ALTER TABLE vv CACHE", 1347, "'test.vv' is not BASE TABLE"),
        (
            "CREATE TABLE tq (a INT) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (a))",
            1054,
            "Unknown column 'a' in 'expression'",
        ),
        (
            "CREATE USER pu2 ATTRIBUTE '{\"name\": \"Tom\", age: 19}'",
            3140,
            "Invalid JSON text: The document root must not be followed by other values.",
        ),
        (
            "CREATE TABLE ie (c1 INT, KEY (c_not_exist))",
            1072,
            "column does not exist: c_not_exist",
        ),
        (
            "ALTER TABLE tv0 MODIFY testx.tv0.b BIGINT",
            1102,
            "Incorrect database name 'testx'",
        ),
        (
            "ALTER TABLE tv0 MODIFY t.b BIGINT",
            1103,
            "Incorrect table name 't'",
        ),
        (
            "ALTER TABLE pt IMPORT PARTITION p1 TABLESPACE",
            8200,
            "Unsupported Unsupported/unknown ALTER TABLE specification",
        ),
        (
            "ALTER TABLE ge ADD COLUMN cc INT, AUTO_ID_CACHE = 10",
            8200,
            "Unsupported multi schema change for modify auto id cache",
        ),
        (
            "UPDATE pt PARTITION (p0, p_non), pt3 SET pt.a = 40 WHERE pt3.a = 2",
            1735,
            "Unknown partition 'p_non' in table 'pt'",
        ),
        (
            "UPDATE pt PARTITION (p0), pt3 SET pt.a = 15 WHERE pt3.a = 1",
            1748,
            "Found a row not matching the given partition set",
        ),
        (
            "ALTER DATABASE test SET TIFLASH REPLICA 1 SET TIFLASH REPLICA 2",
            8200,
            "Unsupported multi schema change for set tiflash replica",
        ),
        (
            "CREATE TABLE mvpk (f1 JSON, PRIMARY KEY mvi ((CAST(CAST(f1 AS UNSIGNED ARRAY) AS UNSIGNED ARRAY))))",
            1235,
            "This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'",
        ),
        (
            "CREATE TABLE mvfk (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, doc JSON, \
             FOREIGN KEY fk_user_id ((CAST(doc->'$[*]' AS SIGNED ARRAY))) REFERENCES tv0 (a))",
            1072,
            "Key column '_V$_fk_user_id_0' doesn't exist in table",
        ),
        (
            "CREATE TABLE ivn (id INT UNSIGNED) PARTITION BY RANGE (id) INTERVAL (1000000) \
             FIRST PARTITION LESS THAN (NULL) LAST PARTITION LESS THAN (10000000)",
            8200,
            "Unsupported INTERVAL partitioning: Error when generating partition values",
        ),
        (
            "CREATE TABLE ivn (id INT UNSIGNED) PARTITION BY RANGE (id) INTERVAL (1000000) \
             FIRST PARTITION LESS THAN (0) LAST PARTITION LESS THAN (NULL)",
            8200,
            "Unsupported INTERVAL: expr () not matching FIRST + n INTERVALs (0 + n * 1000000)",
        ),
        (
            "CREATE OR REPLACE VIEW v_a AS SELECT * FROM v_b",
            1146,
            "Table 'test.v_a' doesn't exist",
        ),
        (
            "INSERT INTO sd (c) VALUES (1)",
            1292,
            "Incorrect datetime value: '2001-01-00 00:00:00'",
        ),
        (
            format!(
                "ALTER TABLE pt ADD PARTITION (PARTITION p5 VALUES LESS THAN (100) COMMENT '{}')",
                "b".repeat(1100)
            )
            .as_str(),
            1793,
            "Comment for table partition 'p5' is too long (max = 1024)",
        ),
        (
            "INSERT INTO jd8 (id) VALUES (1)",
            1054,
            "Unknown column 'id' in 'expression'",
        ),
        (
            "CREATE OR REPLACE VIEW tv0 AS SELECT * FROM tv0",
            1347,
            "'test.tv0' is not VIEW",
        ),
        (
            "ALTER TABLE tv0 ENGINE_ATTRIBUTE = '{', ENGINE_ATTRIBUTE = '{\"storage_class\": \"STANDARD\"}'",
            8270,
            "Invalid engine attribute format: 'unexpected end of JSON input'",
        ),
        (
            "ALTER TABLE tv0 ENGINE_ATTRIBUTE = '{\"storage_class\": \"STANDARD\"}', STORAGE_CLASS = 'IA'",
            8271,
            "Invalid storage class: can not specify 'ENGINE_ATTRIBUTE' and 'STORAGE_CLASS' together",
        ),
        (
            "ALTER TABLE tv0 ADD INDEX i2 (b), MODIFY COLUMN b INT NULL DEFAULT 1 AFTER b",
            8200,
            "Unsupported operate same column 'b'",
        ),
        (
            "ALTER TABLE tv0 MODIFY COLUMN b INT AUTO_INCREMENT DEFAULT (STR_TO_DATE('1980-01-01', '%Y-%m-%d'))",
            1067,
            "Invalid default value for 'b'",
        ),
        (
            "SET @@sql_mode = 'STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
            0,
            "",
        ),
    ] {
        if code == 0 {
            session.node_conn().execute_write(sql).unwrap();
            continue;
        }
        assert_eq!(write_error(&mut session, sql), (code, message.to_owned()), "{sql}");
    }
    assert_eq!(
        write_error(&mut session, "INSERT INTO dz VALUES (NULL), (0), (1)"),
        (1365, "Division by 0".to_owned())
    );
    for (sql, code, message) in [
        (
            "SELECT t.upper(1) FROM nonexist",
            1305,
            "FUNCTION t.upper does not exist",
        ),
        (
            "SELECT * FROM tc1 WHERE c < ANY (SELECT c FROM tc2)",
            1267,
            "Illegal mix of collations (utf8_general_ci,IMPLICIT) and (utf8_unicode_ci,IMPLICIT) for operation '<'",
        ),
        (
            "SELECT * FROM tc1 WHERE (c, d) = ANY (SELECT c FROM tc2)",
            1241,
            "Operand should contain 2 column(s)",
        ),
        (
            "SELECT a, SUM(b) OVER (PARTITION BY zz) FROM tv0",
            1054,
            "Unknown column 'zz' in 'window partition by'",
        ),
        (
            "SELECT a, SUM(b) OVER (ORDER BY zz) FROM tv0",
            1054,
            "Unknown column 'zz' in 'window order by'",
        ),
        (
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT SUM(n) + 1 FROM c WHERE n < 3) SELECT * FROM c",
            3575,
            "Recursive Common Table Expression 'c' can contain neither aggregation nor window functions in recursive query block",
        ),
        (
            "SELECT CONCAT(b, GROUP_CONCAT(b)) FROM tv0 WHERE xxx = xxx",
            1054,
            "Unknown column 'xxx' in 'where clause'",
        ),
        (
            "SELECT 'a' COLLATE utf8mb4_bin UNION SELECT 'b' COLLATE utf8mb4_general_ci",
            1271,
            "Illegal mix of collations for operation 'UNION'",
        ),
    ] {
        assert_eq!(query_error(&mut session, sql), (code, message.to_owned()), "{sql}");
    }
}

/// Go's JSON builtin classes wrap their arguments in implicit casts
/// (`newBaseBuiltinFuncWithTp`), and `isnull` tests a JSON argument as its
/// string form. A cast whose result is JSON is never folded, so the constant
/// operands stay `cast(1, json BINARY)` in the plan. Each operator info below
/// is the Go oracle's text (database `pl17`/`pl18` renamed to `test`).
#[test]
fn json_function_arguments_carry_go_implicit_casts() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1131)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE jt (a INT, j JSON, k JSON, s VARCHAR(10))")
        .unwrap();
    for (sql, expected) in [
        (
            "select * from jt where 1 member of (j)",
            "json_memberof(cast(1, json BINARY), test.jt.j)",
        ),
        (
            "select * from jt where json_overlaps(j, '[1,2]')",
            "json_overlaps(test.jt.j, cast(\"[1,2]\", json BINARY))",
        ),
        (
            "select * from jt where json_contains(j, '1')",
            "json_contains(test.jt.j, cast(\"1\", json BINARY))",
        ),
        (
            "select * from jt where json_set(j, '$.a', 1) = k",
            "eq(json_set(test.jt.j, \"$.a\", cast(1, json BINARY)), test.jt.k)",
        ),
        (
            "select * from jt where json_array_append(j, '$', 1) is not null",
            "not(isnull(cast(json_array_append(test.jt.j, \"$\", cast(1, json BINARY)), var_string(16777216))))",
        ),
        (
            "select * from jt where j is null",
            "isnull(cast(test.jt.j, var_string(4294967295)))",
        ),
        (
            "select * from jt where json_unquote(j) = 'a'",
            "eq(json_unquote(cast(test.jt.j, var_string(4294967295))), \"a\")",
        ),
    ] {
        let text = displayed(rows(&mut session, &format!("explain format='brief' {sql}")))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains(expected), "{sql}\n{text}");
    }
}

/// Go prunes `PARTITION BY RANGE` by the statement's predicates
/// (`PartitionRangeForCNFExpr`): `!=` and `NOT IN` are not predicates it
/// knows, so they read every partition, and a non-clustered table's
/// `_tidb_rowid` extends a non-unique index so `ORDER BY a, _tidb_rowid`
/// needs no sort. Each expectation was confirmed on the Go oracle.
#[test]
fn range_pruning_and_handle_ordering_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1132)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET @@tidb_partition_prune_mode = 'dynamic'")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE pr (a INT, b INT) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), \
             PARTITION p2 VALUES LESS THAN (30), PARTITION p3 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE ho (a INT, KEY ia (a))")
        .unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("explain format='brief' {sql}")))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    for (sql, partitions) in [
        ("select * from pr where a != 5", "partition:all"),
        ("select * from pr where a not in (1,2,3)", "partition:all"),
        ("select * from pr where a in (1, 15, null)", "partition:p0,p1"),
        ("select * from pr where a < 5 or a > 25", "partition:p0,p2,p3"),
        ("select * from pr where a between 5 and 15", "partition:p0,p1"),
        ("select * from pr where a > 9 and a <= 10", "partition:p1"),
    ] {
        let text = plan(&mut session, sql);
        assert!(text.contains(partitions), "{sql}\n{text}");
    }
    let ordered = plan(
        &mut session,
        "select * from ho use index (ia) order by a, _tidb_rowid",
    );
    assert!(!ordered.contains("Sort"), "{ordered}");
    assert!(ordered.contains("keep order:true"), "{ordered}");
}

/// A pre-evaluated scalar subquery keeps its `ScalarQueryCol` reference even
/// when its value is NULL, and `tidb_opt_derive_topn` derives a TopN or Limit
/// below a window from a filter on `row_number()`. Go oracle texts.
#[test]
fn scalar_subquery_null_reference_and_derived_topn_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1133)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dq1 (a INT, b INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dq2 (a INT, b INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dw (a INT, b INT, c INT, PRIMARY KEY (b, a))")
        .unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("explain format='brief' {sql}")))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let text = plan(&mut session, "select dq1.a, (select min(dq2.a) from dq2) from dq1");
    assert!(text.contains("(<nil>)->Column"), "{text}");
    assert!(text.contains("ScalarQueryCol#"), "{text}");

    let window = "select * from (select row_number() over (order by a) as rn from dw) dt where rn <= 3";
    let off = plan(&mut session, window);
    assert!(!off.contains("TopN"), "{off}");
    session
        .node_conn()
        .execute_write("SET tidb_opt_derive_topn = 1")
        .unwrap();
    let on = plan(&mut session, window);
    assert!(on.contains("TopN") && on.contains("count:3"), "{on}");
    let no_order = plan(
        &mut session,
        "select * from (select row_number() over () as rn from dw) dt where rn <= 3",
    );
    assert!(no_order.contains("Limit") && no_order.contains("count:3"), "{no_order}");
}

/// Go's builtin classes wrap each argument in the cast their `argTps` ask for
/// (`newBaseBuiltinFuncWithTp`), so the plan shows the implicit casts. Each
/// expected text is the Go oracle's, under the real server's integer display
/// width (`bigint`, not `bigint(20)`).
#[test]
fn builtin_arguments_carry_go_signature_casts() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1134)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE sa (i INT, s VARCHAR(20), d DECIMAL(10,2), dt DATETIME)")
        .unwrap();
    for (expression, expected) in [
        ("length(i)", "length(cast(test.sa.i, var_string(20)))"),
        ("abs(s)", "abs(cast(test.sa.s, double BINARY))"),
        ("year(s)", "year(cast(test.sa.s, datetime(6) BINARY))"),
        (
            "concat(i, s)",
            "concat(cast(test.sa.i, var_string(20)), test.sa.s)",
        ),
        ("i & s", "bitand(test.sa.i, cast(test.sa.s, bigint BINARY))"),
        ("not s", "not(cast(test.sa.s, double BINARY))"),
        ("hex(dt)", "hex(cast(test.sa.dt, var_string(19)))"),
        ("format(s, 2)", "format(cast(test.sa.s, double BINARY), 2)"),
        (
            "greatest(i, s)",
            "greatest(cast(test.sa.i, var_string(20)), test.sa.s)",
        ),
        ("nullif(i, 1)", "if(eq(test.sa.i, 1), <nil>, test.sa.i)"),
        ("trim(i)", "trim(cast(test.sa.i, var_string(20)))"),
        (
            "trim(leading 'x' from s)",
            "trim(test.sa.s, x, 2)",
        ),
        (
            "if(s, 1, 2)",
            "if(istrue_with_null(cast(test.sa.s, double BINARY)), 1, 2)",
        ),
        (
            "case when s then 1 end",
            "case(istrue_with_null(cast(test.sa.s, double BINARY)), 1)",
        ),
    ] {
        let text = displayed(rows(
            &mut session,
            &format!("explain format='brief' select {expression} from sa"),
        ))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(text.contains(expected), "{expression}\n{text}");
    }
}

/// Go's prefer-range-scan pruning reads the candidate's own eq/in count, which
/// a DNF derives from its minimum access conditions, so `a = 1 OR a = 2` over
/// a pseudo-statistics table reads the index (the same range as
/// `a >= 1 AND a <= 2` loses on cost). An OR over an unindexed column keeps
/// the OR tree as Go's `ComposeDNFCondition` shaped it: balanced, not a chain.
#[test]
fn dnf_over_an_index_prefers_the_range_scan_and_keeps_go_or_shape() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1135)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dn (a INT, b INT, KEY (a))")
        .unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("explain format='brief' {sql}")))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let index = plan(&mut session, "select * from dn where a = 1 or a = 2");
    assert!(
        index.contains("IndexLookUp") && index.contains("range:[1,2]"),
        "{index}"
    );
    let range = plan(&mut session, "select * from dn where a >= 1 and a <= 2");
    assert!(range.contains("TableFullScan"), "{range}");
    let chain = plan(
        &mut session,
        "select * from dn where b = 1 or b = 2 or b = 3 or b = 4",
    );
    assert!(
        chain.contains(
            "or(or(eq(test.dn.b, 1), eq(test.dn.b, 2)), or(eq(test.dn.b, 3), eq(test.dn.b, 4)))"
        ),
        "{chain}"
    );
}

/// Go `matchProperty` Case 3: ranges that each pin an index column to one
/// constant (`a = 3 AND b IN (1, 3, 5) ORDER BY c`) satisfy the order by
/// reading every range group in order and merge-sorting the groups, so no
/// Sort sits above the reader. The merge covers a clustered table scan, a
/// covering index and an index lookup, ascending and descending, and the
/// 2048-group cap falls back to the Sort.
#[test]
fn grouped_range_reads_merge_sort_instead_of_sorting() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1137)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE gc (a INT, b INT, c INT, d INT, PRIMARY KEY (a, b, c) CLUSTERED)",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE gi (a INT, b INT, c INT, d INT, KEY iabc (a, b, c))")
        .unwrap();
    for table in ["gc", "gi"] {
        let values = (1..=3)
            .flat_map(|b| (1..=4).map(move |c| format!("(1, {b}, {}, {})", 10 - c * b, b * 100 + c)))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .node_conn()
            .execute_write(&format!("INSERT INTO {table} VALUES {values}"))
            .unwrap();
    }
    let text = |session: &mut Session, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let plan =
        |session: &mut Session, sql: &str| text(session, &format!("explain format='brief' {sql}"));
    for (sql, reader) in [
        (
            "select c from gc where a = 1 and b in (1, 3, 2) order by c",
            "TableRangeScan",
        ),
        (
            "select c from gi where a = 1 and b in (1, 3, 2) order by c",
            "IndexRangeScan",
        ),
        (
            "select /*+ order_index(gi, iabc) */ c, d from gi where a = 1 and b in (3, 1, 2) order by c desc",
            "IndexRangeScan",
        ),
    ] {
        let explained = plan(&mut session, sql);
        assert!(
            !explained.contains("Sort") && explained.contains(reader),
            "{sql}\n{explained}"
        );
        let answer = rows(&mut session, sql);
        let keys = displayed(answer)
            .into_iter()
            .map(|row| row[0].parse::<i64>().unwrap())
            .collect::<Vec<_>>();
        let mut expected = keys.clone();
        expected.sort_unstable();
        if sql.ends_with("desc") {
            expected.reverse();
        }
        assert_eq!(keys.len(), 12, "{sql}");
        assert_eq!(keys, expected, "{sql}");
    }
    // Both columns of the order are pinned per range or ordered by the
    // index: the merge key is the second sort item.
    let two = text(
        &mut session,
        "select b, c from gi where a = 1 and b in (3, 1) order by b, c",
    );
    let pairs = two.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut sorted = pairs.clone();
    sorted.sort_by_key(|line| {
        let mut fields = line.split(' ').map(|field| field.parse::<i64>().unwrap());
        (fields.next(), fields.next())
    });
    assert_eq!(pairs, sorted, "{two}");
    // A keep-order lookup on a dynamically pruned partitioned table also reads
    // each entry's partition id (`AddExtraPhysTblIDColumn`), which prices the
    // ordered scan above the plain one: Go sorts the unordered lookup, and
    // the hinted ordered plan keeps the order under a trimming Projection.
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE gp (a INT, b INT, c INT, d INT, KEY iabc (a, b, c)) \
             PARTITION BY RANGE (d) (PARTITION p0 VALUES LESS THAN (100), \
             PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN (300))",
        )
        .unwrap();
    let priced = plan(
        &mut session,
        "select a, c from gp where a = 2 and b = 3 and d < 200 order by a, c",
    );
    assert!(
        priced.contains("Sort") && priced.contains("keep order:false"),
        "{priced}"
    );
    let hinted = plan(
        &mut session,
        "select /*+ order_index(gp, iabc) */ a, c from gp where a = 2 and b in (1, 3, 5) \
         and d < 200 order by a, c",
    );
    assert!(
        !hinted.contains("Sort") && hinted.contains("keep order:true"),
        "{hinted}"
    );
    // More than 2048 groups keep the Sort.
    let many_a = (1..=50).map(|v| v.to_string()).collect::<Vec<_>>().join(", ");
    let many_b = (1..=50).map(|v| (v * 2).to_string()).collect::<Vec<_>>().join(", ");
    let capped = plan(
        &mut session,
        &format!("select c from gi where a in ({many_a}) and b in ({many_b}) order by c"),
    );
    assert!(capped.contains("Sort"), "{capped}");
}

/// Shapes captured from the oracle: a correlated scalar COUNT/BIT_* over a
/// non-unique key decorrelates into an outer join under `ifnull(agg, default)`
/// (`aggDefaultValueMap`), a HAVING on it becomes `if(cond, v, NULL)` rather
/// than a join condition, a LIMIT folds into a TableDual through a
/// Projection, a RANGE frame prints its offset operand and a rollup's
/// level-projection prints `expr->Column`.
#[test]
fn decorrelated_defaults_dual_limits_and_frames_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1138)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dd1 (c1 INT PRIMARY KEY, c2 INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dd2 (c1 INT, c2 INT, KEY (c1))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO dd1 VALUES (1,1),(2,2),(3,3),(4,4)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO dd2 VALUES (1,1),(1,2),(3,3),(3,5),(3,6)")
        .unwrap();
    let text = |session: &mut Session, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let counted = text(
        &mut session,
        "explain format='brief' select c1, (select count(*) from dd2 where dd2.c1 = dd1.c1) from dd1",
    );
    assert!(
        counted.contains("left outer join") && counted.contains("ifnull(Column#"),
        "{counted}"
    );
    assert!(!counted.contains("Apply"), "{counted}");
    let having = text(
        &mut session,
        "explain format='brief' select c1, (select count(*) from dd2 where dd2.c1 = dd1.c1 \
         having count(*) > 1) from dd1",
    );
    assert!(having.contains("if(gt(ifnull("), "{having}");
    let answer = text(
        &mut session,
        "select c1, (select count(*) from dd2 where dd2.c1 = dd1.c1 having count(*) > 1) \
         from dd1 order by c1",
    );
    assert_eq!(answer, "1 2\n2 NULL\n3 3\n4 NULL", "{answer}");
    let dual = text(&mut session, "explain format='brief' select 1 as a from dual order by a limit 1");
    assert!(!dual.contains("Limit") && dual.contains("TableDual"), "{dual}");
    session
        .node_conn()
        .execute_write("CREATE TABLE ff (a INT, b INT)")
        .unwrap();
    let frame = text(
        &mut session,
        "explain format='brief' select sum(a) over (partition by a order by b range between \
         1 preceding and 1 following) from ff",
    );
    assert!(frame.contains("range between 1 preceding and 1 following"), "{frame}");
}

/// Metadata answers captured from the oracle: a table reports the creating
/// job's time as `CREATE_TIME` (`TableInfo.UpdateTS`), an expression index
/// names its key part by the string `NULL`, a FULLTEXT key is refused with
/// 8200, and `ALTER INDEX` validates against the table as submitted.
#[test]
fn table_metadata_answers_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1139)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE md (a INT, b INT, KEY ((a + b)))")
        .unwrap();
    let created = displayed(rows(
        &mut session,
        "select create_time is null, create_time > now() - interval 1 hour \
         from information_schema.tables where table_schema = 'test' and table_name = 'md'",
    ));
    assert_eq!(created, [["0", "1"]], "{created:?}");
    let status = displayed(rows(&mut session, "show table status like 'md'"));
    assert_ne!(status[0][11], "NULL", "{status:?}");
    let index = displayed(rows(&mut session, "show index from md"));
    assert_eq!(index[0][4], "NULL", "{index:?}");
    let fulltext = session
        .node_conn()
        .execute_write("CREATE TABLE ft (a TEXT, FULLTEXT KEY k (a))")
        .unwrap_err();
    let fulltext = format!("{fulltext:?}");
    assert!(
        fulltext.contains("FULLTEXT index is only supported in starter deployment mode"),
        "{fulltext}"
    );
    let invisible = session
        .node_conn()
        .execute_write("ALTER TABLE md ADD INDEX kb (b), ALTER INDEX kb INVISIBLE")
        .unwrap_err();
    let invisible = format!("{invisible:?}");
    assert!(invisible.contains("Key 'kb' doesn't exist in table 'md'"), "{invisible}");
}

/// Bootstrap creates Go's `sys` schema with `schema_unused_indexes` and the
/// `mysql.tidb_mdl_view` by executing their `CREATE VIEW`s, a view column
/// takes its key flag from the base column, and a NOT NULL column without a
/// default reports a NULL `COLUMN_DEFAULT`. Each was captured from the oracle.
#[test]
fn bootstrap_views_and_column_defaults_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1140)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for view in ["sys.schema_unused_indexes", "mysql.tidb_mdl_view"] {
        let result = rows(&mut session, &format!("select * from {view} limit 0"));
        assert!(result.is_empty(), "{view}: {result:?}");
    }
    let described = displayed(rows(&mut session, "desc mysql.tidb_mdl_view"));
    assert!(!described.is_empty(), "{described:?}");
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE nd (a INT NOT NULL, b TEXT NOT NULL, c INT NOT NULL DEFAULT 5)",
        )
        .unwrap();
    let defaults = displayed(rows(
        &mut session,
        "select column_name, column_default from information_schema.columns \
         where table_schema = 'test' and table_name = 'nd' order by ordinal_position",
    ));
    assert_eq!(defaults, [["a", "NULL"], ["b", "NULL"], ["c", "5"]], "{defaults:?}");
}

/// Account statements follow Go's stored rows, captured from the oracle:
/// `WITH MAX_USER_CONNECTIONS n` is stored and printed by `SHOW CREATE USER`,
/// `CREATE ROLE` writes the role's `{}` row into `mysql.global_priv`, and
/// `RENAME USER` leaves `mysql.columns_priv` (and so the column grants) under
/// the old name.
#[test]
fn account_rows_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1141)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE USER cu1 WITH MAX_USER_CONNECTIONS 3")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE ROLE cr1")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE ct (a INT, b INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("GRANT SELECT (a) ON test.ct TO cu1")
        .unwrap();
    let limit = displayed(rows(
        &mut session,
        "select max_user_connections from mysql.user where user = 'cu1'",
    ));
    assert_eq!(limit, [["3"]], "{limit:?}");
    let created = displayed(rows(&mut session, "show create user cu1"));
    assert!(
        created[0][0].contains("REQUIRE NONE WITH MAX_USER_CONNECTIONS 3 PASSWORD EXPIRE"),
        "{created:?}"
    );
    let roles = displayed(rows(
        &mut session,
        "select user, priv from mysql.global_priv where user = 'cr1'",
    ));
    assert_eq!(roles, [["cr1", "{}"]], "{roles:?}");
    session
        .node_conn()
        .execute_write("RENAME USER cu1 TO cu2")
        .unwrap();
    let grants = displayed(rows(&mut session, "show grants for cu2"));
    assert!(
        grants.iter().all(|row| !row[0].contains("SELECT(a)")),
        "{grants:?}"
    );
    let columns = displayed(rows(
        &mut session,
        "select user from mysql.columns_priv where table_name = 'ct'",
    ));
    assert_eq!(columns, [["cu1"]], "{columns:?}");
}

/// `EXPLAIN FOR CONNECTION` renders the plan the connection's previous
/// statement published, under the analyze columns, as Go does: a statement
/// that was itself an EXPLAIN leaves an `Explain` plan that renders nothing,
/// and only brief/row/verbose are admitted. Captured from the oracle.
#[test]
fn explain_for_connection_renders_the_previous_plan() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1142)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE efc (a INT PRIMARY KEY, b INT)")
        .unwrap();
    let id = displayed(rows(&mut session, "select connection_id()"))[0][0].clone();
    rows(&mut session, "select * from efc where a = 1");
    let plan = displayed(rows(&mut session, &format!("explain for connection {id}")));
    assert_eq!(plan.len(), 1, "{plan:?}");
    assert!(plan[0][0].starts_with("Point_Get_"), "{plan:?}");
    assert_eq!(plan[0][3], "root");
    assert_eq!(plan[0][4], "table:efc");
    // The EXPLAIN FOR statement is itself an EXPLAIN: its own plan is empty.
    let after = displayed(rows(
        &mut session,
        &format!("explain format='brief' for connection {id}"),
    ));
    assert!(after.is_empty(), "{after:?}");
    rows(&mut session, "select * from efc where a = 1");
    let (_, message) = query_error(&mut session, &format!("explain format='json' for connection {id}"));
    assert!(
        message.contains("explain format 'json' for connection is not supported now"),
        "{message}"
    );
    let (code, message) = query_error(&mut session, "explain for connection 99999");
    assert_eq!((code, message.as_str()), (1094, "Unknown thread id: 99999"));
}

/// Placement policies live in meta, not in the node: the catalog a node
/// builds from the cluster must carry them, or `SHOW PLACEMENT`,
/// `information_schema.placement_policies` and `SHOW TABLE ... REGIONS`
/// (which prints the policy of a table or partition) see none. A partition's
/// own policy is resolved by id when the table is created, as
/// `handleTablePlacement` does. Captured from the oracle.
#[test]
fn placement_policies_reach_the_cluster_catalog() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1143)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE PLACEMENT POLICY ppa FOLLOWERS=4")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE PLACEMENT POLICY ppb PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\"")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ppt (a INT, b INT) PLACEMENT POLICY ppa PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10) PLACEMENT POLICY ppb, \
              PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "select policy_name, followers from information_schema.placement_policies order by 1"
        )),
        vec![vec!["ppa", "4"], vec!["ppb", "2"]]
    );
    let show = displayed(rows(&mut session, "show placement"));
    let targets: Vec<&str> = show.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POLICY ppa",
            "POLICY ppb",
            "TABLE test.ppt",
            "TABLE test.ppt PARTITION p0",
            "TABLE test.ppt PARTITION p1",
            "TABLE test.ppt PARTITION p2"
        ]
    );
    assert_eq!(show[3][1], "PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\"");
    assert_eq!(show[4][1], "FOLLOWERS=4");
    assert_eq!(show[2][2], "PENDING");
    // With split-table on (Go `main.go` sets `EnableSplitTableRegion`),
    // CREATE TABLE splits every partition into its own region, and each
    // region shows its partition's placement.
    let regions = displayed(rows(&mut session, "show table ppt regions"));
    let placement: Vec<(&str, &str)> = regions
        .iter()
        .map(|row| (row[11].as_str(), row[12].as_str()))
        .collect();
    assert_eq!(
        placement,
        vec![
            ("PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\"", "PENDING"),
            ("FOLLOWERS=4", "PENDING"),
            ("FOLLOWERS=4", "PENDING"),
        ],
        "{regions:?}"
    );
}

/// A CTE definition keeps its storage id in every format
/// (`CTEDefinition.ExplainID`), and the implicit casts a builtin's arguments
/// carry no longer decide push-down: `round(dt)` and `conv(int, ...)` read a
/// cast the catalog now lowers natively, while `period_add` has no pb code in
/// Go and stays above the reader. Each outcome was captured from the oracle.
#[test]
fn cte_ids_and_cast_wrapped_pushdown_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1136)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE pc (i INT NOT NULL, s VARCHAR(20) NOT NULL, dt DATETIME NOT NULL)",
        )
        .unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let cte = plan(
        &mut session,
        "explain format='plan_tree' with c as (select i from pc where i < 10 order by i limit 1) \
         select * from c where c.i < 18 union select * from c where c.i > 0",
    );
    assert!(
        cte.lines()
            .any(|line| line.starts_with("CTE_0 root") && line.contains("Non-Recursive CTE")),
        "{cte}"
    );
    for (sql, store) in [
        ("round(dt)", "cop[tikv]"),
        ("conv(i, i, i)", "cop[tikv]"),
        ("period_add(i, i)", "root"),
    ] {
        let text = plan(
            &mut session,
            &format!("explain format='brief' select i from pc where {sql}"),
        );
        let selection = text
            .lines()
            .find(|line| line.contains("Selection ") && line.contains(store))
            .is_some();
        assert!(selection, "{sql}\n{text}");
    }
}

/// The binding maintenance loop commits its GC through a session on its own
/// thread, waiting out a conflicting lock. On a default 2 MB thread stack that
/// commit overflowed at the first GC tick (300 s) of the `imdbload` corpus
/// topic, whose region-retry nesting this test does not reproduce; the topic
/// is the reproducer, and this pins the GC path end to end on the worker.
#[test]
fn binding_gc_commits_on_the_maintenance_thread() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(131)).unwrap();
    rows(
        &mut session,
        "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, \
         update_time, charset, collation, source) VALUES ('select 1', 'select 1', 'test', \
         'deleted', '2000-01-01 00:00:00', '2000-01-01 00:00:00', 'utf8mb4', 'utf8mb4_bin', \
         'manual')",
    );
    // A second transaction holds the row's lock, so the GC's prewrite meets
    // it and runs the client's lock-resolution path, where the overflow was.
    let mut holder = stack.factory.open_session(session_context(133)).unwrap();
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(
        &mut holder,
        "SELECT * FROM mysql.bind_info WHERE status = 'deleted' FOR UPDATE",
    );
    let bindings = Arc::clone(stack.factory.bindings.as_ref().expect("binding cache"));
    let long = Duration::from_secs(3600);
    let worker = tidb_bindinfo::cluster_binding_seam::BindingReloader::spawn(
        bindings,
        [long, Duration::from_millis(1), long],
        move || long,
    )
    .expect("binding worker");
    std::thread::sleep(Duration::from_millis(300));
    holder.node_conn().control_transaction("COMMIT").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let left = rows(
            &mut session,
            "SELECT count(*) FROM mysql.bind_info WHERE status = 'deleted'",
        );
        if left == vec![vec![Datum::Int(0)]] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the maintenance thread never collected the deleted binding: {left:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(worker);
}

/// `planner/core/indexmerge_path`: Go analyzes a multi-valued index as an
/// independent index task per physical table (its keys, not the row
/// samples), dumps each partition's FM sketch for the global merge, and the
/// column task's job lists only its own indexes. Values from the oracle.
#[test]
fn analyze_runs_multi_valued_indexes_as_independent_tasks() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(134)).unwrap();
    for sql in [
        "CREATE TABLE test.mvt (a INT, b INT, j JSON, INDEX ia(a), INDEX mv((CAST(j AS SIGNED ARRAY))))",
        "INSERT INTO test.mvt VALUES (1, 2, '[1,2,3]'), (2, 3, '[2,3]'), (3, 4, '[]')",
        "ANALYZE TABLE test.mvt",
        "CREATE TABLE test.mvp (a INT, j JSON, INDEX mv((CAST(j AS SIGNED ARRAY)))) \
         PARTITION BY HASH(a) PARTITIONS 2",
        "INSERT INTO test.mvp VALUES (1, '[1,2]'), (2, '[3]'), (3, '[4,5,6]')",
        "ANALYZE TABLE test.mvp",
    ] {
        rows(&mut session, sql);
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info FROM information_schema.analyze_status \
             WHERE table_name = 'mvt' ORDER BY job_info"
        )),
        [
            ["analyze index mv"],
            ["analyze table index ia, columns a, b with 256 buckets, 100 topn, 1 samplerate"],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT h.distinct_count FROM mysql.stats_histograms h \
             JOIN information_schema.tables t ON h.table_id = t.tidb_table_id \
             WHERE t.table_name = 'mvp' AND h.is_index = 1"
        )),
        [["3"]]
    );
}

/// Go rolls back a transaction whose COMMIT fails: the client sees the
/// error and nothing it staged is published, not even by the next
/// statement (captured from Go on this branch). The failure owner is the
/// store's prewrite (a presumed-absent key); a republish of the connection's
/// catalog while the transaction is open does not fail its commit, as Go's
/// schema and statistics reloads do not.
#[test]
fn a_failed_commit_publishes_nothing_later() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1901)).unwrap();
    rows(&mut session, "CREATE DATABASE failed_commit");
    rows(&mut session, "USE failed_commit");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO t VALUES (1, 10)");
    rows(&mut session, "SET tidb_txn_mode = 'optimistic'");
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1, 99)")
        .unwrap();
    let error = session
        .node_conn()
        .control_transaction("COMMIT")
        .unwrap_err();
    assert_eq!(error.code, 1062, "{error:?}");
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (2, 20)")
        .unwrap();

    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t WHERE id = 1 FOR UPDATE");
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (3, 30)")
        .unwrap();
    {
        // A republish of the connection's catalog lands while the
        // transaction is open, as a schema or statistics reload does.
        let shared = session.shared_catalog();
        let mut catalog = shared.lock().unwrap();
        assert!(catalog.set_database_placement_policy("failed_commit", None));
    }
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![vec!["1", "10"], vec!["2", "20"], vec!["3", "30"]]
    );
}

/// Go's executor ignores the completion type, and refuses `START
/// TRANSACTION READ ONLY` before its implicit commit, so the open
/// transaction survives the refusal (captured from Go on this branch).
#[test]
fn completion_types_and_a_refused_read_only_begin_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1902)).unwrap();
    rows(&mut session, "CREATE DATABASE txn_shapes");
    rows(&mut session, "USE txn_shapes");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (3, 30)")
        .unwrap();
    session
        .node_conn()
        .control_transaction("COMMIT AND CHAIN")
        .unwrap();
    session
        .node_conn()
        .control_transaction("ROLLBACK RELEASE")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![vec!["3", "30"]]
    );
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (4, 40)")
        .unwrap();
    let error = session
        .node_conn()
        .control_transaction("START TRANSACTION READ ONLY")
        .unwrap_err();
    assert_eq!(error.code, 1235, "{error:?}");
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![vec!["3", "30"]]
    );
}

/// With table locks disabled Go's preprocessor still resolves every named
/// table under its own schema, then answers OK with warning 1235
/// (captured from Go on this branch).
#[test]
fn disabled_lock_tables_resolves_every_table_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1903)).unwrap();
    rows(&mut session, "CREATE DATABASE lock_disabled");
    rows(&mut session, "USE lock_disabled");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    session
        .node_conn()
        .execute_write("LOCK TABLES mysql.user READ, t WRITE")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        vec![vec![
            "Warning",
            "1235",
            "LOCK TABLES is not supported. To enable this experimental feature, set 'enable-table-lock' in the configuration file."
        ]]
    );
    let error = session
        .node_conn()
        .execute_write("LOCK TABLES t READ, nosuch READ")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1146, "Table 'lock_disabled.nosuch' doesn't exist")
    );
}

/// Go matches partition names by their Unicode lower-case form in LOCK
/// STATS and ANALYZE, and refuses a PARTITION clause on a table that has
/// none with 1505 (captured from Go on this branch).
#[test]
fn stats_partition_names_and_the_nonpartitioned_refusal_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1904)).unwrap();
    rows(&mut session, "CREATE DATABASE stats_parts");
    rows(&mut session, "USE stats_parts");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(
        &mut session,
        "CREATE TABLE pt (a INT) PARTITION BY RANGE (a) \
         (PARTITION pÄ VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    session
        .node_conn()
        .execute_write("LOCK STATS pt PARTITION PÄ")
        .unwrap();
    session
        .node_conn()
        .execute_write("UNLOCK STATS pt PARTITION pä")
        .unwrap();
    session
        .node_conn()
        .execute_write("ANALYZE TABLE pt PARTITION PÄ")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ANALYZE TABLE t PARTITION p0")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1505,
            "Partition management on a not partitioned table is not possible"
        )
    );
}

/// Go refuses an unsupported ALTER specification only after the table
/// lookup (1146) and the base-table check (1347) (captured from Go on this
/// branch).
#[test]
fn an_unsupported_alter_spec_is_refused_after_the_table_checks() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1905)).unwrap();
    rows(&mut session, "CREATE DATABASE alter_order");
    rows(&mut session, "USE alter_order");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "CREATE VIEW vv AS SELECT 1");
    for (sql, code, message) in [
        (
            "ALTER TABLE nosuch REBUILD PARTITION p0",
            1146,
            "Table 'alter_order.nosuch' doesn't exist",
        ),
        (
            "ALTER TABLE vv REBUILD PARTITION p0",
            1347,
            "'alter_order.vv' is not BASE TABLE",
        ),
        (
            "ALTER TABLE t REBUILD PARTITION p0",
            8200,
            "Unsupported rebuild partition",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
}

/// Go's pessimistic READ-COMMITTED provider reads every statement at its
/// own fresh timestamp (`getStmtTS`), so a peer's commit after BEGIN is
/// visible to the next statement; repeatable read keeps the start
/// timestamp.
#[test]
fn read_committed_statements_see_commits_made_after_begin() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack.factory.open_session(session_context(1906)).unwrap();
    rows(&mut writer, "CREATE DATABASE rc_reads");
    rows(&mut writer, "USE rc_reads");
    rows(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(&mut writer, "INSERT INTO t VALUES (1)");
    for (isolation, visible) in [("REPEATABLE-READ", "1"), ("READ-COMMITTED", "2")] {
        let mut reader = stack.factory.open_session(session_context(1907)).unwrap();
        rows(&mut reader, "USE rc_reads");
        rows(
            &mut reader,
            &format!("SET transaction_isolation = '{isolation}'"),
        );
        reader
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        assert_eq!(
            displayed(rows(&mut reader, "SELECT COUNT(*) FROM t")),
            vec![vec!["1"]]
        );
        writer
            .node_conn()
            .execute_write("INSERT INTO t VALUES (2)")
            .unwrap();
        assert_eq!(
            displayed(rows(&mut reader, "SELECT COUNT(*) FROM t")),
            vec![vec![visible]],
            "{isolation}"
        );
        reader.node_conn().control_transaction("ROLLBACK").unwrap();
        writer
            .node_conn()
            .execute_write("DELETE FROM t WHERE id = 2")
            .unwrap();
    }
}

/// Go's DDL checks in Go's order, with Go's notes (captured from Go on this
/// branch): the preprocessor resolves a table before anything else (1146,
/// also for a missing database), CREATE TABLE checks its database before
/// building (1049), a LIKE source must be a base table (1347), IF [NOT]
/// EXISTS forms add their notes, `dropIndex` names the missing index, and
/// placement policies keep Go's IF EXISTS rules and reserved name.
#[test]
fn ddl_checks_and_notes_follow_go_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1908)).unwrap();
    rows(&mut session, "CREATE DATABASE ddl_order");
    rows(&mut session, "USE ddl_order");
    rows(&mut session, "CREATE TABLE t (a INT, b INT, INDEX ib (b))");
    rows(&mut session, "CREATE VIEW v AS SELECT 1 a");
    for (sql, code, message) in [
        (
            "ALTER TABLE nodb.t ADD COLUMN c INT",
            1146,
            "Table 'nodb.t' doesn't exist",
        ),
        (
            "TRUNCATE TABLE nodb.t",
            1146,
            "Table 'nodb.t' doesn't exist",
        ),
        (
            "DROP INDEX i ON nodb.t",
            1146,
            "Table 'nodb.t' doesn't exist",
        ),
        (
            "CREATE TABLE nodb.t (a INT, a INT)",
            1049,
            "Unknown database 'nodb'",
        ),
        (
            "CREATE TABLE t2 LIKE v",
            1347,
            "'ddl_order.v' is not BASE TABLE",
        ),
        (
            "ALTER TABLE v MODIFY COLUMN a INT DEFAULT 'abc'",
            1347,
            "'ddl_order.v' is not BASE TABLE",
        ),
        (
            "ALTER TABLE missing MODIFY COLUMN a INT DEFAULT 'abc'",
            1146,
            "Table 'ddl_order.missing' doesn't exist",
        ),
        ("DROP INDEX nosuch ON t", 1091, "index nosuch doesn't exist"),
        (
            "ALTER TABLE t DROP INDEX nosuch",
            1091,
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP INDEX nosuch, ADD COLUMN z INT",
            1091,
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP PRIMARY KEY",
            1091,
            "Can't DROP 'PRIMARY'; check that column/key exists",
        ),
        (
            "ALTER PLACEMENT POLICY IF EXISTS nosuch PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
            8239,
            "Unknown placement policy 'nosuch'",
        ),
        (
            "CREATE PLACEMENT POLICY `default` PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
            1382,
            "The 'default' syntax is reserved for purposes internal to the MySQL server",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(
        &mut session,
        "CREATE PLACEMENT POLICY p PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
    );
    for (sql, code, message) in [
        (
            "CREATE TABLE IF NOT EXISTS t (a INT)",
            "1050",
            "Table 'ddl_order.t' already exists",
        ),
        (
            "CREATE TABLE IF NOT EXISTS t LIKE t",
            "1050",
            "Table 'ddl_order.t' already exists",
        ),
        (
            "DROP INDEX IF EXISTS nosuch ON t",
            "1091",
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP INDEX IF EXISTS nosuch",
            "1091",
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP INDEX IF EXISTS nosuch, ADD COLUMN z INT",
            "1091",
            "index nosuch doesn't exist",
        ),
        (
            "CREATE PLACEMENT POLICY IF NOT EXISTS p PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
            "8238",
            "Placement policy 'p' already exists",
        ),
        (
            "DROP PLACEMENT POLICY IF EXISTS nosuch",
            "8239",
            "Unknown placement policy 'nosuch'",
        ),
    ] {
        session.node_conn().execute_write(sql).unwrap();
        assert_eq!(
            displayed(rows(&mut session, "SHOW WARNINGS")),
            vec![vec!["Note", code, message]],
            "{sql}"
        );
    }
}

/// Go's SetExecutor persists each assignment as it runs, so a later failing
/// assignment leaves the earlier ones stored (captured from Go on this
/// branch).
#[test]
fn a_failing_set_global_keeps_the_assignments_before_it() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1909)).unwrap();
    let error = session
        .node_conn()
        .execute_write("SET GLOBAL tidb_mem_quota_query = 2147483648, GLOBAL autocommit = 'bogus'")
        .unwrap_err();
    assert_eq!(error.code, 1231, "{error:?}");
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@global.tidb_mem_quota_query")),
        vec![vec!["2147483648"]]
    );
}

/// Go's PREPARE of a statement it does not plan-cache adds only that note:
/// a FLUSH STATS_DELTA's missing-table warning belongs to EXECUTE (captured
/// from Go on this branch).
#[test]
fn prepare_shows_only_the_plan_cache_note() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1910)).unwrap();
    session
        .node_conn().prepare_general("FLUSH STATS_DELTA test.nosuch")
        .unwrap();
    assert_eq!(
        session
            .warnings()
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1105]
    );
}

/// Go's pessimistic DML reads at `GetStmtForUpdateTS`, a fresh timestamp
/// at statement start: an UPDATE or DELETE after BEGIN acts on rows a peer
/// committed meanwhile (captured from Go on this branch).
#[test]
fn pessimistic_dml_acts_on_rows_committed_after_begin() {
    let (stack, _users) = cop_backed_stack();
    let mut a = stack.factory.open_session(session_context(1911)).unwrap();
    let mut b = stack.factory.open_session(session_context(1912)).unwrap();
    rows(&mut a, "CREATE DATABASE dml_current");
    rows(&mut a, "USE dml_current");
    rows(&mut b, "USE dml_current");
    rows(&mut a, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut a, "INSERT INTO t VALUES (1, 0)");
    a.node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut a, "SELECT * FROM t")),
        vec![vec!["1", "0"]]
    );
    b.node_conn()
        .execute_write("INSERT INTO t VALUES (2, 0)")
        .unwrap();
    let updated = a
        .node_conn()
        .execute_write("UPDATE t SET v = 9 WHERE id > 0")
        .unwrap()
        .unwrap();
    assert_eq!(updated.affected_rows, 2);
    assert_eq!(
        displayed(rows(&mut a, "SELECT * FROM t")),
        vec![vec!["1", "9"], vec!["2", "9"]]
    );
    a.node_conn().control_transaction("COMMIT").unwrap();

    a.node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(&mut a, "SELECT * FROM t");
    b.node_conn()
        .execute_write("INSERT INTO t VALUES (3, 0)")
        .unwrap();
    let deleted = a
        .node_conn()
        .execute_write("DELETE FROM t WHERE v = 0")
        .unwrap()
        .unwrap();
    assert_eq!(deleted.affected_rows, 1);
    a.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(&mut a, "SELECT * FROM t")),
        vec![vec!["1", "9"], vec!["2", "9"]]
    );
}

/// Go `LOAD DATA LOCAL`, through the production session's client-file doors
/// (path, acceptance, transfer), case for case against Go on this branch:
/// rows, the `Records/Deleted/Skipped/Warnings` message, warnings and errors
/// (oracle probe over `executor.LoadDataReaderBuilder`).
#[test]
fn load_data_local_answers_like_go() {
    run_load_data_cases(LOAD_DATA_CASES, 1931);
}

/// Go `LOAD DATA` casts (`getRow` / `completeLoadErr`), restrictive REPLACE
/// errors, REPLACE over unique keys, and the executor builder's refusals,
/// against Go on this branch.
#[test]
fn load_data_casts_and_refusals_answer_like_go() {
    run_load_data_cases(LOAD_DATA_CAST_CASES, 1933);
}

/// Replays `(kind, sql, data, expected, expected warnings)` steps: `ex` runs
/// a statement, `q` compares a query's rows, `ld` drives a load through the
/// connection's client-file doors.
fn run_load_data_cases(cases: &[(&str, &str, &[u8], &str, &str)], connection_id: u64) {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(connection_id))
        .unwrap();
    let render = |rows: Vec<Vec<Datum>>| {
        let rows: Vec<String> = rows
            .into_iter()
            .map(|row| {
                displayed(vec![row.clone()])
                    .remove(0)
                    .into_iter()
                    .zip(row)
                    .map(|(text, datum)| match datum {
                        Datum::Null => "<nil>".to_owned(),
                        // Go's testkit prints a BIT value's raw bytes.
                        Datum::Bit(bits) => String::from_utf8_lossy(bits.as_bytes()).into_owned(),
                        _ => text,
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        if rows.is_empty() {
            "[]".to_owned()
        } else {
            format!("[[{}]]", rows.join("] ["))
        }
    };
    let error_text =
        |error: tidb_store_driver_error::sql_query_error::SqlQueryError| format!("ERR [{}]{}", error.code, error.message);
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let mut failures = Vec::new();
    for (kind, sql, data, expected, expected_warnings) in cases {
        match *kind {
            "ex" => {
                if session.node_conn().execute_write(sql).expect(sql).is_none() {
                    rows(&mut session, sql);
                }
            }
            "q" => {
                let actual = render(rows(&mut session, sql));
                if actual != *expected {
                    failures.push(format!("{sql}\n  go:   {expected}\n  rust: {actual}"));
                }
            }
            _ => {
                // The connection's order: one `ExecuteStmt`, then the client's
                // file for a statement that parked a request for it.
                let stmt = session.parse_at_statement_boundary(sql).unwrap();
                let outcome = (|| {
                    assert!(session.node_conn().execute_stmt(sql, &stmt)?.is_none(), "{sql} answers no rows");
                    if session.local_infile_request().is_some() {
                        session.node_conn().finish_local_infile(data)?;
                    }
                    Ok(tidb_executor::ddl_exec::WriteOutcome {
                        affected_rows: session.affected_rows(),
                        last_insert_id: session.statement_last_insert_id(),
                    })
                })();
                let actual = match outcome {
                    Ok(outcome) => format!(
                        "ok affected={} msg={:?}",
                        outcome.affected_rows,
                        session.statement_message()
                    ),
                    Err(error) => error_text(error),
                };
                if actual != *expected {
                    failures.push(format!("{sql}\n  go:   {expected}\n  rust: {actual}"));
                }
                let warnings = rows(&mut session, "SHOW WARNINGS");
                let actual = if warnings.is_empty() {
                    String::new()
                } else {
                    render(warnings)
                };
                if actual != *expected_warnings {
                    failures.push(format!(
                        "{sql} (warnings)\n  go:   {expected_warnings}\n  rust: {actual}"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const LOAD_DATA_CASES: &[(&str, &str, &[u8], &str, &str)] = &[
    ("ex", "create table t (a int, b varchar(10))", b"", "", ""),
    ("ld", "load data local infile '/x' into table t", b"1\x09x\x0a2\x09y\x0a", "ok affected=2 msg=\"Records: 2  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from t", b"", "[[1 x] [2 y]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t", b"1\x0a2\x09b\x09c\x0a3\x09z", "ok affected=3 msg=\"Records: 3  Deleted: 0  Skipped: 0  Warnings: 2\"", "[[Warning 1261 Row 1 doesn't contain data for all columns] [Warning 1262 Row 2 was truncated; it contained more data than there were input columns]]"),
    ("q", "select * from t", b"", "[[1 <nil>] [2 b] [3 z]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ex", "set sql_mode=''", b"", "", ""),
    ("ld", "load data local infile '/x' into table t", b"1\x0a2\x09b\x09c\x0aabc\x09z\x0a", "ok affected=3 msg=\"Records: 3  Deleted: 0  Skipped: 0  Warnings: 3\"", "[[Warning 1261 Row 1 doesn't contain data for all columns] [Warning 1262 Row 2 was truncated; it contained more data than there were input columns] [Warning 1292 Truncated incorrect DOUBLE value: 'abc']]"),
    ("q", "select * from t", b"", "[[1 <nil>] [2 b] [0 z]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ex", "set sql_mode=default", b"", "", ""),
    ("ld", "load data local infile '/x' into table t", b"abc\x09z\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1292 Truncated incorrect DOUBLE value: 'abc']]"),
    ("ld", "load data local infile '/x' ignore into table t", b"abc\x09z\x0a1\x0a", "ok affected=2 msg=\"Records: 2  Deleted: 0  Skipped: 0  Warnings: 2\"", "[[Warning 1292 Truncated incorrect DOUBLE value: 'abc'] [Warning 1261 Row 2 doesn't contain data for all columns]]"),
    ("q", "select * from t", b"", "[[0 z] [0 z] [1 <nil>]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t fields terminated by ',' enclosed by '\"'", b"1,\x22a,b\x22\x0a2,\x22q\x22\x22r\x22\x0a3,\x22NULL\x22\x0a4,NULL\x0a5,\x5cN\x0a6,\x22x\x22y\x22\x0a7,\x22\x5cN\x22\x0a", "ok affected=7 msg=\"Records: 7  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select a, b, b is null from t", b"", "[[1 a,b 0] [2 q\"r 0] [3 NULL 0] [4 <nil> 1] [5 <nil> 1] [6 x\"y 0] [7 <nil> 1]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t fields terminated by ',' enclosed by '\"' defined null by 'NULL' optionally enclosed", b"1,\x22NULL\x22\x0a2,NULL\x0a3,\x5cN\x0a", "ok affected=3 msg=\"Records: 3  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select a, b, b is null from t", b"", "[[1 <nil> 1] [2 <nil> 1] [3 <nil> 1]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t fields terminated by ',' lines starting by 'xx' terminated by '|'", b"zzxx1,a|nothing|xx2,b|yyxx3,c", "ok affected=3 msg=\"Records: 3  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from t", b"", "[[1 a] [2 b] [3 c]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t fields terminated by ',' ignore 1 lines", b"h1,h2\x0a1,a\x0a2,b\x0a", "ok affected=2 msg=\"Records: 2  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from t", b"", "[[1 a] [2 b]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t ignore 5 lines", b"1\x09a\x0a", "ok affected=0 msg=\"Records: 0  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("ex", "create table t2 (a int, b varchar(10), c int)", b"", "", ""),
    ("ld", "load data local infile '/x' into table t2 fields terminated by ',' (a, @v) set b = concat('<', @v, '>'), c = @v * 2", b"1,3\x0a2,4\x0a3\x0a", "ok affected=3 msg=\"Records: 3  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1261 Row 3 doesn't contain data for all columns]]"),
    ("q", "select * from t2", b"", "[[1 <3> 6] [2 <4> 8] [3 <nil> <nil>]]", ""),
    ("q", "select @v", b"", "[[<nil>]]", ""),
    ("ld", "load data local infile '/x' into table t2 fields terminated by ',' (a, @V) set c = @v + 1", b"9,10\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from t2 where a = 9", b"", "[[9 <nil> 11]]", ""),
    ("ld", "load data local infile '/x' into table t2 (a, nosuch)", b"1\x0a", "ERR [1054]Unknown column 'nosuch' in 'field list'", "[[Error 1054 Unknown column 'nosuch' in 'field list']]"),
    ("ld", "load data local infile '/x' into table t2 set nosuch = 1", b"1\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1261 Row 1 doesn't contain data for all columns]]"),
    ("ex", "create table t3 (id int primary key, v varchar(10))", b"", "", ""),
    ("ex", "insert into t3 values (1, 'old')", b"", "", ""),
    ("ld", "load data local infile '/x' into table t3 fields terminated by ','", b"1,new\x0a2,b\x0a", "ok affected=1 msg=\"Records: 2  Deleted: 0  Skipped: 1  Warnings: 1\"", "[[Warning 1062 Duplicate entry '1' for key 't3.PRIMARY']]"),
    ("q", "select * from t3", b"", "[[1 old] [2 b]]", ""),
    ("ld", "load data local infile '/x' replace into table t3 fields terminated by ','", b"1,new\x0a2,b\x0a", "ok affected=1 msg=\"Records: 2  Deleted: 1  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from t3", b"", "[[1 new] [2 b]]", ""),
    ("ld", "load data local infile '/x' ignore into table t3 fields terminated by ','", b"1,x\x0a3,c\x0a3,d\x0a", "ok affected=1 msg=\"Records: 3  Deleted: 0  Skipped: 2  Warnings: 2\"", "[[Warning 1062 Duplicate entry '1' for key 't3.PRIMARY'] [Warning 1062 Duplicate entry '3' for key 't3.PRIMARY']]"),
    ("q", "select * from t3", b"", "[[1 new] [2 b] [3 c]]", ""),
    ("ex", "set sql_mode=''", b"", "", ""),
    ("ld", "load data local infile '/x' into table t3 fields terminated by ','", b"4,a\x0a4,b\x0a", "ok affected=1 msg=\"Records: 2  Deleted: 0  Skipped: 1  Warnings: 1\"", "[[Warning 1062 Duplicate entry '4' for key 't3.PRIMARY']]"),
    ("q", "select * from t3", b"", "[[1 new] [2 b] [3 c] [4 a]]", ""),
    ("ex", "set sql_mode=default", b"", "", ""),
    ("ex", "create table t4 (a int, ts datetime not null, g int as (a + 1))", b"", "", ""),
    ("ld", "load data local infile '/x' into table t4 (a)", b"5\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1364 Field 'ts' doesn't have a default value]]"),
    ("q", "select a, ts is not null, g from t4", b"", "[[5 1 6]]", ""),
    ("ld", "load data local infile '/x' into table t4", b"5\x092020-01-01\x0999\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select a, ts, g from t4", b"", "[[5 0000-00-00 00:00:00 6] [5 2020-01-01 00:00:00 6]]", ""),
    ("ex", "create table t5 (a int not null, b int not null default 7)", b"", "", ""),
    ("ld", "load data local infile '/x' into table t5", b"\x5cN\x09\x5cN\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 2\"", "[[Warning 1263 Column set to default value; NULL supplied to NOT NULL column 'a' at row 1] [Warning 1263 Column set to default value; NULL supplied to NOT NULL column 'b' at row 1]]"),
    ("ex", "set sql_mode=''", b"", "", ""),
    ("ld", "load data local infile '/x' into table t5", b"\x5cN\x09\x5cN\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 2\"", "[[Warning 1263 Column set to default value; NULL supplied to NOT NULL column 'a' at row 1] [Warning 1263 Column set to default value; NULL supplied to NOT NULL column 'b' at row 1]]"),
    ("ld", "load data local infile '/x' into table t5 (b)", b"3\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1364 Field 'a' doesn't have a default value]]"),
    ("q", "select * from t5", b"", "[[0 0] [0 0] [0 3]]", ""),
    ("ex", "set sql_mode=default", b"", "", ""),
    ("ld", "load data local infile '/x' into table t", b"1\x09a\x0a\x0a2\x09b\x0d\x0a3\x09c\x5ctd\x5cne\x5c0f\x5cZg\x0a", "ok affected=4 msg=\"Records: 4  Deleted: 0  Skipped: 0  Warnings: 2\"", "[[Warning 1261 Row 2 doesn't contain data for all columns] [Warning 1292 Truncated incorrect DOUBLE value: '']]"),
    ("q", "select a, hex(b) from t", b"", "[[1 61] [0 <nil>] [2 620D] [3 6309640A6500661A67]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data infile '' into table t", b"", "ERR [8156]The value of INFILE must not be empty when LOAD DATA from LOCAL", "[[Error 8156 The value of INFILE must not be empty when LOAD DATA from LOCAL]]"),
    ("ld", "load data local infile '' into table t", b"", "ERR [8156]The value of INFILE must not be empty when LOAD DATA from LOCAL", "[[Error 8156 The value of INFILE must not be empty when LOAD DATA from LOCAL]]"),
    ("ld", "load data local infile '/x' into table t fields defined null by 'a' optionally enclosed", b"", "ERR [8162]%!(EXTRA string=must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED)", "[[Error 8162 %!(EXTRA string=must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED)]]"),
    ("ld", "load data local infile '/x' into table t lines terminated by ''", b"", "ERR [8162]%!(EXTRA string=LINES TERMINATED BY is empty)", "[[Error 8162 %!(EXTRA string=LINES TERMINATED BY is empty)]]"),
    ("ld", "load data local infile '/x' into table t fields terminated by ''", b"", "ERR [8162]%!(EXTRA string=load data with empty field terminator)", "[[Error 8162 %!(EXTRA string=load data with empty field terminator)]]"),
    ("ld", "load data local infile '/x' into table t fields enclosed by 'a' terminated by 'a'", b"", "ERR [8162]%!(EXTRA string=FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other)", "[[Error 8162 %!(EXTRA string=FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other)]]"),
    ("ld", "load data local infile '/x' into table t fields enclosed by 'a' terminated by 'ab'", b"", "ERR [8162]%!(EXTRA string=FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other)", "[[Error 8162 %!(EXTRA string=FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other)]]"),
    ("ld", "load data local infile '/x' into table t lines starting by 'a|' terminated by '|'", b"", "ERR [8162]STARTING BY 'a|' cannot contain LINES TERMINATED BY '|'", "[[Error 8162 STARTING BY 'a|' cannot contain LINES TERMINATED BY '|']]"),
    ("ld", "load data local infile '/x' into table t character set gbk", b"1\x09\xc4\xe3\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select a, b, hex(b) from t", b"", "[[1 你 E4BDA0]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t character set latin1", b"1\x09\xe9\x80\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select a, b, hex(b) from t", b"", "[[1 é€ C3A9E282AC]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ex", "create view v as select * from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table v", b"1\x0a", "ERR [1288]The target table v of the LOAD is not updatable", "[[Error 1288 The target table v of the LOAD is not updatable]]"),
    ("ld", "load data local infile '/x' into table nosuch", b"1\x0a", "ERR [1146]Table 'test.nosuch' doesn't exist", "[[Error 1146 Table 'test.nosuch' doesn't exist]]"),
    ("ex", "create table tp (a int) partition by range (a) (partition p0 values less than (10))", b"", "", ""),
    ("ld", "load data local infile '/x' into table tp", b"1\x0a20\x0a", "ok affected=1 msg=\"Records: 2  Deleted: 0  Skipped: 1  Warnings: 1\"", "[[Warning 1526 Table has no partition for value 20]]"),
    ("q", "select * from tp", b"", "[[1]]", ""),
    ("ex", "create table ta (id int auto_increment primary key, v int)", b"", "", ""),
    ("ld", "load data local infile '/x' into table ta (v)", b"10\x0a20\x0a", "ok affected=2 msg=\"Records: 2  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select *, last_insert_id() from ta", b"", "[[1 10 1] [2 20 1]]", ""),
    ("ld", "load data local infile '/x' into table t fields terminated by ',' enclosed by '\"'", b"1,\x22abc\x0a", "ERR [8160]Failed to read source files. Reason: syntax error: unterminated quoted field. Only the following formats delimited text file (csv, tsv), parquet, sql are supported. Please provide the valid source file(s)", "[[Error 8160 Failed to read source files. Reason: syntax error: unterminated quoted field. Only the following formats delimited text file (csv, tsv), parquet, sql are supported. Please provide the valid source file(s)]]"),
    ("q", "select * from t", b"", "[]", ""),
    ("ld", "load data local infile '/x' into table t", b"\xef\xbb\xbf1\x09a\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from t", b"", "[[1 a]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t fields escaped by ''", b"1\x09a\x5ctb\x0a2\x09\x5cN\x0a", "ok affected=2 msg=\"Records: 2  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select a, b, b is null from t", b"", "[[1 a\\tb 0] [2 \\N 0]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' into table t columns terminated by ',' optionally enclosed by '\\''", b"1,'a'\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from t", b"", "[[1 a]]", ""),
    ("ex", "delete from t", b"", "", ""),
];

/// Go `getColDefaultValueFromNil`: a NOT NULL column without a default takes
/// its zero value uncast, with one 1364 per column (and once per statement
/// for a VALUES list) and no 1292 for the zero date (captured from Go).
#[test]
fn insert_ignore_no_default_zero_value_warns_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1932)).unwrap();
    rows(&mut session, "CREATE DATABASE nodefault_zero");
    rows(&mut session, "USE nodefault_zero");
    rows(
        &mut session,
        "CREATE TABLE t4 (a int, ts datetime not null, d date not null, s varchar(3) not null)",
    );
    let expected = vec![
        vec!["Warning", "1364", "Field 'ts' doesn't have a default value"],
        vec!["Warning", "1364", "Field 'd' doesn't have a default value"],
        vec!["Warning", "1364", "Field 's' doesn't have a default value"],
    ];
    for sql in [
        "INSERT IGNORE INTO t4 (a) VALUES (5)",
        "INSERT IGNORE INTO t4 (a) VALUES (5), (6)",
        "INSERT IGNORE INTO t4 (a) SELECT 7",
    ] {
        session.node_conn().execute_write(sql).unwrap();
        assert_eq!(
            displayed(rows(&mut session, "SHOW WARNINGS")),
            expected,
            "{sql}"
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t4 WHERE a = 7")),
        vec![vec!["7", "0000-00-00 00:00:00", "0000-00-00", ""]]
    );
}
const LOAD_DATA_CAST_CASES: &[(&str, &str, &[u8], &str, &str)] = &[
    ("ex", "create table t (a int, b varchar(3), c date, d tinyint unsigned, e decimal(4,1), f enum('x','y'), g bit(2), h json, i year)", b"", "", ""),
    ("ld", "load data local infile '/x'  into table t", b"1\x09abcdef\x092020-13-01\x09300\x0912345.6\x09z\x097\x09notjson\x093000\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 8\"", "[[Warning 1265 Data truncated for column 'b' at row 1] [Warning 1292 Incorrect date value: '2020-13-01'] [Warning 1690 constant 300 overflows tinyint] [Warning 1690 DECIMAL value is out of range in '(4, 1)'] [Warning 1265 Data truncated for column '%s' at row %d] [Warning 1265 Data truncated for column 'g' at row 1] [Warning 3140 Invalid JSON text: The document root must not be followed by other values.] [Warning 1264 Out of range value for column '%s' at row %d]]"),
    ("q", "select * from t", b"", "[[1 abc 0000-00-00 255 999.9  \u{0003} <nil> 2155]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x'  into table t", b"x1\x09ab\x090000-00-00\x09-1\x091.26\x09y\x091\x09[1]\x0999\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 5\"", "[[Warning 1292 Truncated incorrect DOUBLE value: 'x1'] [Warning 1292 Incorrect date value: '0000-00-00'] [Warning 1690 BIGINT UNSIGNED value is out of range in '-1'] [Warning 1292 Truncated incorrect DECIMAL value: '1.26'] [Warning 1265 Data truncated for column 'g' at row 1]]"),
    ("q", "select * from t", b"", "[[0 ab 0000-00-00 0 1.3 y \u{0003} [1] 1999]]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x'  into table t (a)", b"1\x0a2\x093\x0a", "ok affected=2 msg=\"Records: 2  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1262 Row 2 was truncated; it contained more data than there were input columns]]"),
    ("ld", "load data local infile '/x'  into table t", b"1\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1261 Row 1 doesn't contain data for all columns]]"),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' replace into table t", b"1\x09abcdef\x092020-13-01\x09300\x0912345.6\x09z\x097\x09notjson\x093000\x0a", "ERR [1265]Data truncated for column 'b' at row 1", "[[Error 1265 Data truncated for column 'b' at row 1]]"),
    ("q", "select * from t", b"", "[]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' replace into table t", b"x1\x09ab\x090000-00-00\x09-1\x091.26\x09y\x091\x09[1]\x0999\x0a", "ERR [1292]Truncated incorrect DOUBLE value: 'x1'", "[[Error 1292 Truncated incorrect DOUBLE value: 'x1']]"),
    ("q", "select * from t", b"", "[]", ""),
    ("ex", "delete from t", b"", "", ""),
    ("ld", "load data local infile '/x' replace into table t (a)", b"1\x0a2\x093\x0a", "ERR [1262]Row 2 was truncated; it contained more data than there were input columns", "[[Error 1262 Row 2 was truncated; it contained more data than there were input columns]]"),
    ("ld", "load data local infile '/x' replace into table t", b"1\x0a", "ERR [1261]Row 1 doesn't contain data for all columns", "[[Error 1261 Row 1 doesn't contain data for all columns]]"),
    ("ex", "delete from t", b"", "", ""),
    ("ex", "create table tn (a int not null, b int)", b"", "", ""),
    ("ld", "load data local infile '/x' replace into table tn", b"\x5cN\x091\x0a", "ERR [1263]Column set to default value; NULL supplied to NOT NULL column 'a' at row 1", "[[Error 1263 Column set to default value; NULL supplied to NOT NULL column 'a' at row 1]]"),
    ("ld", "load data local infile '/x' replace into table tn (b)", b"1\x0a", "ERR [1364]Field 'a' doesn't have a default value", "[[Error 1364 Field 'a' doesn't have a default value]]"),
    ("ex", "set sql_mode=''", b"", "", ""),
    ("ld", "load data local infile '/x' replace into table t", b"1\x09abcdef\x092020-13-01\x09300\x0912345.6\x09z\x097\x09notjson\x093000\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 8\"", "[[Warning 1265 Data truncated for column 'b' at row 1] [Warning 1292 Incorrect date value: '2020-13-01'] [Warning 1690 constant 300 overflows tinyint] [Warning 1690 DECIMAL value is out of range in '(4, 1)'] [Warning 1265 Data truncated for column '%s' at row %d] [Warning 1265 Data truncated for column 'g' at row 1] [Warning 3140 Invalid JSON text: The document root must not be followed by other values.] [Warning 1264 Out of range value for column '%s' at row %d]]"),
    ("q", "select * from t", b"", "[[1 abc 0000-00-00 255 999.9  \u{0003} <nil> 2155]]", ""),
    ("ex", "set sql_mode=default", b"", "", ""),
    ("ex", "create table tc (a int, check (a > 0))", b"", "", ""),
    ("ld", "load data local infile '/x' into table tc", b"1\x0a-1\x0a2\x0a", "ok affected=3 msg=\"Records: 3  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from tc", b"", "[[1] [-1] [2]]", ""),
    ("ld", "load data local infile '/x' replace into table tc", b"-5\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("ex", "create table tu (a int primary key, b int unique key)", b"", "", ""),
    ("ex", "insert into tu values (1, 1), (2, 2)", b"", "", ""),
    ("ld", "load data local infile '/x' replace into table tu", b"3\x091\x0a2\x092\x0a4\x095\x0a", "ok affected=3 msg=\"Records: 3  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from tu", b"", "[[3 1] [2 2] [4 5]]", ""),
    ("ld", "load data local infile '/x' into table t (a, _tidb_rowid)", b"1\x092\x0a", "ERR [1105]load data statement for _tidb_rowid are not supported", "[[Error 1105 load data statement for _tidb_rowid are not supported]]"),
    ("ld", "load data local infile '/x' into table t (a, a)", b"1\x092\x0a", "ERR [1110]Column 'a' specified twice", "[[Error 1110 Column 'a' specified twice]]"),
    ("ld", "load data local infile '/x' into table t (@a) set a = @a + nosuch", b"1\x0a", "ERR [1054]Unknown column 'nosuch' in 'expression'", "[[Error 1054 Unknown column 'nosuch' in 'expression']]"),
    ("ld", "load data local infile '/x' into table t (@a) set a = @a + 1, a = 3", b"1\x0a", "ERR [1110]Column 'a' specified twice", "[[Error 1110 Column 'a' specified twice]]"),
    ("ld", "load data local infile '/x' into table t (a) set b = 'x', c = '2020-01-01'", b"5\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select a, b, c from t where a = 5", b"", "[[5 x 2020-01-01]]", ""),
    ("ld", "load data local infile '/x' into table t (a) set b = 1/0", b"6\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 1\"", "[[Warning 1365 Division by 0]]"),
    ("ld", "load data local infile '/x' into table t.x", b"6\x0a", "ERR [1146]Table 't.x' doesn't exist", "[[Error 1146 Table 't.x' doesn't exist]]"),
    ("ld", "load data local infile '/x.gz' into table t (a)", b"6\x0a", "ERR [1105]unexpected EOF", "[[Error 1105 unexpected EOF]]"),
    ("ld", "load data local infile '/x.lz4' into table t (a)", b"6\x0a", "ERR [1105]compression 2 doesn't have related storage compressType", "[[Error 1105 compression 2 doesn't have related storage compressType]]"),
    ("ld", "load data local infile '/x' format 'sql file' into table t (a)", b"6\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("ex", "create table tg (a int, g int as (a*2) stored, b int)", b"", "", ""),
    ("ld", "load data local infile '/x' into table tg", b"1\x099\x093\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("ld", "load data local infile '/x' into table tg (a, b)", b"2\x094\x0a", "ok affected=1 msg=\"Records: 1  Deleted: 0  Skipped: 0  Warnings: 0\"", ""),
    ("q", "select * from tg", b"", "[[1 2 3] [2 4 4]]", ""),
];

/// Go `SPLIT TABLE`, `SHOW TABLE ... REGIONS` and `preSplitAndScatter` over
/// the embedded store's real regions, against Go on this branch (split-table
/// on, `tidb_scatter_region = 'table'` so Go splits synchronously). Region and
/// peer ids are left out: Go's bootstrap splits race its DDL goroutines.
#[test]
fn regions_split_and_show_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1934)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    session
        .node_conn()
        .execute_write("SET @@tidb_scatter_region = 'table'")
        .unwrap();
    rows(
        &mut session,
        "create table t (a int primary key, b int, key i(b))",
    );
    for (sql, expected) in [
        (
            "split table t index i between (0) and (100) regions 2",
            "2 1",
        ),
        ("split table t by (10), (20), (30)", "3 1"),
        ("split table t by (10), (20), (25)", "1 1"),
    ] {
        let answer = displayed(rows(&mut session, sql)).remove(0).join(" ");
        assert_eq!(answer, expected, "{sql}");
    }
    rows(
        &mut session,
        "create table tr (a bigint auto_random(3) primary key, b int) pre_split_regions = 2",
    );
    rows(
        &mut session,
        "create table tps (a int, b int, key k(b)) shard_row_id_bits = 1 pre_split_regions = 1 partition by hash(a) partitions 2",
    );
    rows(
        &mut session,
        "create table tpn (a int, b int, key k(b)) partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)",
    );
    let ids: std::collections::HashMap<String, String> = displayed(rows(
        &mut session,
        "select table_name, tidb_table_id from information_schema.tables where table_schema = 'test'",
    ))
    .into_iter()
    .map(|row| (row[0].clone(), row[1].clone()))
    .chain(
        displayed(rows(
            &mut session,
            "select concat(table_name, '.', partition_name), tidb_partition_id from information_schema.partitions where table_schema = 'test' and partition_name is not null",
        ))
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone())),
    )
    .collect();
    // The DDL tables are at reserved ids under both kernels; the other
    // `mysql.*` tables are too only under the next-gen kernel.
    let lowest_reserved = tidb_metadef::DDL_TABLE_VERSION_TABLES
        .iter()
        .flat_map(|group| group.tables.iter())
        .chain(
            tidb_metadef::BOOTSTRAP_TABLES
                .iter()
                .filter(|_| tidb_config_kerneltype::is_next_gen()),
        )
        .map(|table| table.id)
        .min()
        .unwrap()
        .to_string();
    let expand = |line: &str| {
        let mut line = line.replace("{lowest_reserved}", &lowest_reserved);
        for (name, id) in &ids {
            line = line.replace(&format!("{{{name}}}"), id);
        }
        line
    };
    // Captured from Go: (start key, end key, scattering) per table.
    let expected = [
        ("t", "t_{t}_i_2_ t_{t}_r_10 0"),
        ("t", "t_{t}_r_10 t_{t}_r_20 0"),
        ("t", "t_{t}_r_20 t_{t}_r_25 0"),
        ("t", "t_{t}_r_25 t_{t}_r_30 0"),
        ("t", "t_{t}_r_30 t_{tr}_ 0"),
        ("t", "t_{t}_ t_{t}_i_1_03800000000000003203000000000000 0"),
        (
            "t",
            "t_{t}_i_1_03800000000000003203000000000000 t_{t}_i_2_ 0",
        ),
        ("tr", "t_{tr}_ t_{tr}_r_2305843009213693952 0"),
        (
            "tr",
            "t_{tr}_r_2305843009213693952 t_{tr}_r_4611686018427387904 0",
        ),
        (
            "tr",
            "t_{tr}_r_4611686018427387904 t_{tr}_r_6917529027641081856 0",
        ),
        ("tr", "t_{tr}_r_6917529027641081856 t_{tps.p0}_ 0"),
        ("tps", "t_{tps.p0}_i_2_ t_{tps.p0}_r_4611686018427387904 0"),
        ("tps", "t_{tps.p0}_r_4611686018427387904 t_{tps.p1}_ 0"),
        ("tps", "t_{tps.p0}_ t_{tps.p0}_i_2_ 0"),
        ("tps", "t_{tps.p1}_i_2_ t_{tps.p1}_r_4611686018427387904 0"),
        ("tps", "t_{tps.p1}_r_4611686018427387904 t_{tpn.p0}_ 0"),
        ("tps", "t_{tps.p1}_ t_{tps.p1}_i_2_ 0"),
        ("tpn", "t_{tpn.p0}_ t_{tpn.p1}_ 0"),
        // Go's next boundary is its lowest reserved system table: a DDL
        // table (281474976710648) under the classic kernel this was captured
        // from, the lowest `mysql.*` table under the next-gen one.
        ("tpn", "t_{tpn.p1}_ t_{lowest_reserved}_ 0"),
    ];
    let mut actual = Vec::new();
    for table in ["t", "tr", "tps", "tpn"] {
        for row in displayed(rows(&mut session, &format!("show table {table} regions"))) {
            actual.push((table, format!("{} {} {}", row[1], row[2], row[6])));
        }
    }
    let expected: Vec<(&str, String)> = expected
        .iter()
        .map(|(table, line)| (*table, expand(line)))
        .collect();
    assert_eq!(actual, expected);
    // The index's regions alone, and every region's leader is its one peer.
    let index_regions = displayed(rows(&mut session, "show table t index i regions"));
    assert_eq!(index_regions.len(), 2);
    for row in &index_regions {
        assert_eq!(row[3], row[5], "leader is the only peer");
        assert_eq!(row[4], "1", "store 1");
    }
}

/// ADD INDEX runs as Go's persisted job: delete-only, write-only, the txn-merge
/// reorganization and public, with the job's comments; a duplicate rolls the
/// job back and a partial index is cancelled by the owner. Captured from the
/// Go oracle on this branch.
#[test]
fn add_index_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1935)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table t (a int, b int, c varchar(10))");
    rows(
        &mut session,
        "insert into t values (1,1,'x'),(2,2,'y'),(3,2,'z')",
    );
    // (type, schema_state, rows, state, comments) of the newest job.
    let mut last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [3, 4, 7, 11, 12]
            .map(|column| row[column].clone())
            .join("|")
    };
    for (sql, job) in [
        (
            "alter table t add index i(b)",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "alter table t add index j(c(2))",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "alter table t add index k((a+b))",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "alter table t add primary key (a)",
            "add primary key|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "create index ci on t (c)",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(last_job(&mut session), job, "{sql}");
    }
    let error = session
        .node_conn()
        .execute_write("alter table t add unique index u(b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1062, "Duplicate entry '2' for key 't.u'")
    );
    assert_eq!(
        last_job(&mut session),
        "add index|none|0|rollback done|txn-merge, max_node_count=3"
    );
    rows(&mut session, "create table e (a int)");
    for (table, sql) in [
        ("t", "alter table t add index p(a) where a > 1"),
        ("e", "alter table e add index p(a) where a > 1"),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                8200,
                "Unsupported add partial index: add partial index without fast reorg is not supported"
            ),
            "{table}"
        );
        assert_eq!(
            last_job(&mut session),
            "add index|none|0|cancelled|txn-merge, max_node_count=3",
            "{table}"
        );
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from t use index(i) where b = 2"
        )),
        vec![
            vec!["2".to_owned(), "2".to_owned(), "y".to_owned()],
            vec!["3".to_owned(), "2".to_owned(), "z".to_owned()],
        ]
    );
    rows(&mut session, "admin check table t");
    let created = displayed(rows(&mut session, "show create table t")).remove(0);
    assert!(!created[1].contains("`u`"), "{}", created[1]);
    let error = session
        .node_conn()
        .execute_write("alter table t add index i(b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1061, "Duplicate key name 'i'")
    );
    rows(&mut session, "alter table t add index if not exists i(b)");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        vec![vec![
            "Note".to_owned(),
            "1061".to_owned(),
            "Duplicate key name 'i'".to_owned()
        ]]
    );
    // More rows than one batch (`tidb_ddl_reorg_batch_size` 256), over
    // partitions: every batch commits and the handle moves on.
    rows(&mut session, "create table d (x int)");
    rows(
        &mut session,
        "insert into d values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)",
    );
    rows(
        &mut session,
        "create table big (a int, b int) partition by hash(a) partitions 3",
    );
    rows(
        &mut session,
        "insert into big select p.x*100+q.x*10+r.x, p.x from d p, d q, d r",
    );
    rows(&mut session, "alter table big add unique index ua(a)");
    assert_eq!(
        last_job(&mut session),
        "add index|public|1000|synced|txn-merge, max_node_count=3"
    );
    rows(&mut session, "admin check table big");
    // A duplicate found batches in rolls the job back.
    rows(&mut session, "create table big1 (a int, b int)");
    rows(
        &mut session,
        "insert into big1 select a, if(a = 999, 5, a) from big order by a",
    );
    let error = session
        .node_conn()
        .execute_write("alter table big1 add unique index ub(b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1062, "Duplicate entry '5' for key 'big1.ub'")
    );
    // Go counts the three batches committed before the failing one.
    assert_eq!(
        last_job(&mut session),
        "add index|none|768|rollback done|txn-merge, max_node_count=3"
    );
    rows(&mut session, "admin check table big1");
    // Go `cleanupDDLReorgHandles`: nothing is left behind.
    assert_eq!(
        displayed(rows(
            &mut session,
            "select count(*) from mysql.tidb_ddl_reorg"
        )),
        vec![vec!["0".to_owned()]]
    );
}

/// Go runs ADD INDEX through delete-only, write-only and the txn-merge
/// reorganization precisely so that DML running meanwhile keeps the new
/// index whole: writes during the backfill go to the temporary index and the
/// merge replays them. A second session inserts, updates and deletes rows
/// throughout the job; the table and every index agree afterwards.
#[test]
fn add_index_stays_consistent_with_concurrent_dml() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory.open_session(session_context(1936)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table d (x int)");
    rows(
        &mut session,
        "insert into d values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)",
    );
    for table in ["cd", "ce"] {
        rows(
            &mut session,
            &format!("create table {table} (a int primary key, b int, c int)"),
        );
        rows(
            &mut session,
            &format!(
                "insert into {table} select p.x*1000+q.x*100+r.x*10+s.x, p.x, q.x from d p, d q, d r, d s"
            ),
        );
    }
    let done = std::sync::atomic::AtomicBool::new(false);
    let rounds = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let (done, rounds) = (&done, &rounds);
        scope.spawn(move || {
            let mut session = factory.open_session(session_context(1937)).unwrap();
            rows(&mut session, "USE test");
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                let round = rounds.load(std::sync::atomic::Ordering::Acquire);
                let key = (round * 397) % 10000;
                for table in ["cd", "ce"] {
                    for sql in [
                        format!(
                            "insert into {table} values ({}, {round}, {round})",
                            10000 + round
                        ),
                        format!("update {table} set b = b + 1 where a = {key}"),
                        format!("delete from {table} where a = {}", (key + 5000) % 10000),
                    ] {
                        rows(&mut session, &sql);
                    }
                }
                rounds.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
        });
        for sql in [
            "alter table cd add index ib(b)",
            "alter table ce add index ibc((b + c))",
        ] {
            let before = rounds.load(std::sync::atomic::Ordering::Acquire);
            rows(&mut session, sql);
            // The writer must have run while the job did.
            assert!(
                rounds.load(std::sync::atomic::Ordering::Acquire) > before + 1,
                "{sql}"
            );
        }
        done.store(true, std::sync::atomic::Ordering::Release);
    });
    for table in ["cd", "ce"] {
        rows(&mut session, &format!("admin check table {table}"));
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "select count(*) from cd use index(ib) where b >= 0"
        )),
        displayed(rows(
            &mut session,
            "select count(*) from cd ignore index(ib)"
        )),
    );
}

/// Go `insertJobIntoDeleteRangeTable` and the delete-range emulator of a
/// store without native delete-range support: each finished job records the
/// keys it left behind in `mysql.gc_delete_range`, and the emulator deletes
/// them, advancing `start_key` past each batch, then moves the row to
/// `mysql.gc_delete_range_done`. Rows captured from the Go oracle (its ids
/// differ; keys are rebuilt from this node's ids).
#[test]
fn delete_ranges_are_recorded_and_worked_off_like_go() {
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    fn int_key(value: i64) -> Vec<u8> {
        let mut key = vec![0x03];
        key.extend_from_slice(&((value as u64) ^ (1 << 63)).to_be_bytes());
        key
    }
    let table = |id: i64| tidb_tablecodec::table_key::encode_table_prefix(id);
    let index = |id: i64, index: i64| tidb_tablecodec::table_key::encode_table_index_prefix(id, index);
    const TEMP: i64 = 0x7fff_0000_0000_0000;

    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1938)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let id_of = |session: &mut _, sql: &str| -> Vec<i64> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[0].parse().unwrap())
            .collect()
    };
    rows(&mut session, "create table t1 (a int, b int, key k(b))");
    rows(&mut session, "insert into t1 values (1,1)");
    let t1 = id_of(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name='t1'",
    )[0];
    rows(&mut session, "alter table t1 add index k2(a)");
    rows(&mut session, "alter table t1 drop index k");
    rows(&mut session, "truncate table t1");
    rows(
        &mut session,
        "create table tp (a int, b int, key k(b)) partition by hash(a) partitions 2",
    );
    let tp = id_of(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name='tp'",
    )[0];
    let tp_parts = id_of(
        &mut session,
        "select tidb_partition_id from information_schema.partitions where table_name='tp' order by 1",
    );
    rows(&mut session, "alter table tp drop index k");
    rows(&mut session, "drop table tp");
    rows(&mut session, "create database dd");
    rows(&mut session, "create table dd.x (a int)");
    let x = id_of(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_schema='dd'",
    )[0];
    rows(&mut session, "drop database dd");
    rows(
        &mut session,
        "create table tr (a int) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))",
    );
    let tr_parts = id_of(
        &mut session,
        "select tidb_partition_id from information_schema.partitions where table_name='tr' order by 1",
    );
    rows(&mut session, "alter table tr drop partition p0");
    rows(&mut session, "alter table tr truncate partition p1");

    // The emulator runs behind the DDL; wait for it to drain.
    let pending = "select count(*) from mysql.gc_delete_range";
    let started = std::time::Instant::now();
    while displayed(rows(&mut session, pending)) != [["0"]] {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "emulator stuck"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let done: Vec<String> = displayed(rows(
        &mut session,
        "select element_id, start_key, end_key from mysql.gc_delete_range_done order by job_id, element_id",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    let row = |element: i64, start: Vec<u8>, end: Vec<u8>| {
        format!("{element} {} {}", hex(&start), hex(&end))
    };
    let mut drop_k_start = index(t1, 1);
    drop_k_start.extend(int_key(1));
    drop_k_start.extend(int_key(1));
    drop_k_start.push(0);
    let mut truncate_start = tidb_tablecodec::table_key::encode_row_key_with_handle(
        t1,
        &tidb_tablecodec::table_key::RecordHandle::Int(1),
    );
    truncate_start.push(0);
    let expected = vec![
        // add index k2: its temporary index.
        row(1, index(t1, TEMP | 2), index(t1, TEMP | 3)),
        // drop index k: the emulator moved start_key past the one entry.
        row(1, drop_k_start, index(t1, 2)),
        // truncate table: the old table, start_key past its one row.
        row(1, truncate_start, table(t1 + 1)),
        // drop index on a partitioned table: per partition.
        row(1, index(tp_parts[0], 1), index(tp_parts[0], 2)),
        row(2, index(tp_parts[1], 1), index(tp_parts[1], 2)),
        // drop table: the partitions, then the logical table.
        row(1, table(tp_parts[0]), table(tp_parts[0] + 1)),
        row(2, table(tp_parts[1]), table(tp_parts[1] + 1)),
        row(3, table(tp), table(tp + 1)),
        // drop database: its tables.
        row(1, table(x), table(x + 1)),
        // drop and truncate partition: the replaced partitions.
        row(1, table(tr_parts[0]), table(tr_parts[0] + 1)),
        row(1, table(tr_parts[1]), table(tr_parts[1] + 1)),
    ];
    assert_eq!(done, expected);
}

/// DROP INDEX / DROP PRIMARY KEY run as Go's persisted job through
/// `onDropIndex`'s states; the refusals are the executor's. Captured from
/// the Go oracle on this branch.
#[test]
fn drop_index_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1939)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "create table t (a int, b int, c int, key ib(b), key ic(c), primary key (a) nonclustered)",
    );
    rows(&mut session, "insert into t values (1,1,1),(2,2,2)");
    let mut last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [3, 4, 7, 11, 12]
            .map(|column| row[column].clone())
            .join("|")
    };
    for (sql, job) in [
        ("alter table t drop index ib", "drop index|none|0|synced|"),
        ("drop index ic on t", "drop index|none|0|synced|"),
        (
            "alter table t drop primary key",
            "drop primary key|none|0|synced|",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(last_job(&mut session), job, "{sql}");
    }
    for sql in [
        "alter table t drop index if exists ib",
        "drop index if exists ib on t",
    ] {
        rows(&mut session, sql);
        assert_eq!(
            displayed(rows(&mut session, "SHOW WARNINGS")),
            vec![vec![
                "Note".to_owned(),
                "1091".to_owned(),
                "index ib doesn't exist".to_owned()
            ]],
            "{sql}"
        );
    }
    rows(&mut session, "create table tc (a int primary key, b int)");
    rows(
        &mut session,
        "create table p (id int primary key, k int, key kk(k))",
    );
    rows(
        &mut session,
        "create table c (id int primary key, pk int, key fk_idx(pk), foreign key fkc (pk) references p(id))",
    );
    for (sql, code, message) in [
        (
            "alter table t drop index ib",
            1091,
            "index ib doesn't exist",
        ),
        (
            "alter table t drop primary key",
            1091,
            "Can't DROP 'PRIMARY'; check that column/key exists",
        ),
        (
            "alter table tc drop primary key",
            8200,
            "Unsupported drop primary key when the table is using clustered index",
        ),
        (
            "alter table c drop index fk_idx",
            1553,
            "Cannot drop index 'fk_idx': needed in a foreign key constraint",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(&mut session, "admin check table t");
    let created = displayed(rows(&mut session, "show create table t")).remove(0);
    assert_eq!(
        created[1],
        "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL,\n  `c` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
}

/// ADD COLUMN and DROP COLUMN run as Go's persisted jobs (`onAddColumn`,
/// `onDropColumn`): the executor's notes and refusals, the jobs, the column
/// order, the delete ranges of the single-column indexes a drop takes along,
/// and the CHECK constraints `LoadCheckConstraint` prunes once a column they
/// read is gone. Captured from the Go oracle.
#[test]
fn add_and_drop_column_run_as_persisted_jobs_like_go() {
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    fn int_key(value: i64) -> Vec<u8> {
        let mut key = vec![0x03];
        key.extend_from_slice(&((value as u64) ^ (1 << 63)).to_be_bytes());
        key
    }
    let index = |id: i64, index: i64| tidb_tablecodec::table_key::encode_table_index_prefix(id, index);
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1941)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let id_of = |session: &mut _, sql: &str| -> Vec<i64> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[0].parse().unwrap())
            .collect()
    };
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 3, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    let warnings = |session: &mut _| -> Vec<String> {
        displayed(rows(session, "SHOW WARNINGS"))
            .into_iter()
            .map(|row| row.join("|"))
            .collect()
    };
    rows(
        &mut session,
        "create table t (a int, b int, c int not null, key kb(b), key kac(a,c))",
    );
    rows(&mut session, "insert into t values (1,1,1),(2,2,2)");
    let t = id_of(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name='t'",
    )[0];
    for sql in [
        "alter table t add column d int default 5",
        "alter table t add column e int first",
        "alter table t add column f varchar(10) after a",
    ] {
        rows(&mut session, sql);
        let job = last_job(&mut session);
        assert!(job.ends_with("|add column|public|0|synced"), "{sql}: {job}");
    }
    let before = last_job(&mut session);
    rows(&mut session, "alter table t add column if not exists d int");
    assert_eq!(
        warnings(&mut session),
        ["Note|1060|Duplicate column name 'd'"]
    );
    // IF NOT EXISTS over an existing column queues nothing.
    assert_eq!(last_job(&mut session), before);
    rows(&mut session, "alter table t drop column b");
    let job = last_job(&mut session);
    assert!(job.ends_with("|drop column|none|0|synced"), "{job}");
    let before = last_job(&mut session);
    rows(&mut session, "alter table t drop column if exists nosuch");
    assert_eq!(
        warnings(&mut session),
        ["Note|1091|Can't DROP 'nosuch'; check that column/key exists"]
    );
    assert_eq!(last_job(&mut session), before);
    rows(&mut session, "create table only1 (a int)");
    for (sql, code, message) in [
        (
            "alter table t add column d int",
            1060,
            "Duplicate column name 'd'",
        ),
        (
            "alter table t add column g int after nosuch",
            1054,
            "Unknown column 'nosuch' in 't'",
        ),
        (
            "alter table t drop column nosuch",
            1091,
            "Can't DROP 'nosuch'; check that column/key exists",
        ),
        (
            "alter table t drop column a",
            8200,
            "can't drop column a with composite index covered or Primary Key covered now",
        ),
        (
            "alter table only1 drop column a",
            1090,
            "can't drop only column a in table only1",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let columns: Vec<String> = displayed(rows(&mut session, "show columns from t"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(columns, ["e", "a", "f", "c", "d"]);
    assert_eq!(
        displayed(rows(&mut session, "select * from t order by a")),
        [
            ["NULL", "1", "NULL", "1", "5"],
            ["NULL", "2", "NULL", "2", "5"]
        ]
    );
    rows(&mut session, "admin check table t");

    // A NOT NULL column without a default drops cleanly.
    rows(&mut session, "create table tn (a int, b int not null)");
    rows(&mut session, "insert into tn values (1, 1)");
    rows(&mut session, "alter table tn drop column b");
    assert_eq!(displayed(rows(&mut session, "select * from tn")), [["1"]]);

    // The partitions' entries of a dropped single-column index.
    rows(
        &mut session,
        "create table tp (a int, b int, key kb(b)) partition by hash(a) partitions 2",
    );
    let tp_parts = id_of(
        &mut session,
        "select tidb_partition_id from information_schema.partitions where table_name='tp' order by 1",
    );
    rows(&mut session, "alter table tp drop column b");

    let pending = "select count(*) from mysql.gc_delete_range";
    let started = std::time::Instant::now();
    while displayed(rows(&mut session, pending)) != [["0"]] {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "emulator stuck"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let done: Vec<String> = displayed(rows(
        &mut session,
        "select element_id, start_key, end_key from mysql.gc_delete_range_done order by job_id, element_id",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    let mut drop_b_start = index(t, 1);
    drop_b_start.extend(int_key(2));
    drop_b_start.extend(int_key(2));
    drop_b_start.push(0);
    assert_eq!(
        done,
        [
            format!("1 {} {}", hex(&drop_b_start), hex(&index(t, 2))),
            format!(
                "1 {} {}",
                hex(&index(tp_parts[0], 1)),
                hex(&index(tp_parts[0], 2))
            ),
            format!(
                "2 {} {}",
                hex(&index(tp_parts[1], 1)),
                hex(&index(tp_parts[1], 2))
            ),
        ]
    );

    // A CHECK reading a dropped column stays in the stored metadata but is
    // gone from every reader.
    rows(&mut session, "set global tidb_enable_check_constraint=1");
    rows(
        &mut session,
        "create table tc (a int, b int check (b > 0), c int, constraint cc check (c > a), constraint cb2 check (b < 100))",
    );
    let error = session
        .node_conn()
        .execute_write("alter table tc drop column a")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3959,
            "Check constraint 'cc' uses column 'a', hence column cannot be dropped or renamed."
        )
    );
    rows(&mut session, "alter table tc drop column b");
    let created = displayed(rows(&mut session, "show create table tc")).remove(0);
    assert_eq!(
        created[1],
        "CREATE TABLE `tc` (\n  `a` int DEFAULT NULL,\n  `c` int DEFAULT NULL,\n  CONSTRAINT `cc` CHECK ((`c` > `a`))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    let error = session
        .node_conn()
        .execute_write("insert into tc values (1, 0)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (3819, "Check constraint 'cc' is violated.")
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from information_schema.check_constraints where constraint_schema = 'test'"
        )),
        [["def", "test", "cc", "(`c` > `a`)"]]
    );
}

/// CREATE DATABASE and the ALTER TABLE jobs Go finishes in one step run as
/// persisted jobs: a no-op queues nothing, CREATE DATABASE leaves no
/// notifier event, and each job's type and state are Go's. Captured from
/// the Go oracle.
#[test]
fn one_step_alters_and_create_database_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1942)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 3, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    let job_kind = |job: &str| job.split_once('|').unwrap().1.to_owned();
    let notifier_rows = |session: &mut _| {
        displayed(rows(
            session,
            "select count(*) from mysql.tidb_ddl_notifier",
        ))[0][0]
            .clone()
    };
    let notified = notifier_rows(&mut session);
    rows(&mut session, "create database d1");
    assert_eq!(
        job_kind(&last_job(&mut session)),
        "create schema|public|0|synced"
    );
    assert_eq!(notifier_rows(&mut session), notified);
    let before = last_job(&mut session);
    rows(&mut session, "create database if not exists d1");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Note",
            "1007",
            "Can't create database 'd1'; database exists"
        ]]
    );
    assert_eq!(last_job(&mut session), before);

    rows(&mut session, "create table p (id int primary key)");
    rows(
        &mut session,
        "create table t (a int, b int, c varchar(10), key ka(a), key kb(b), constraint fk foreign key (a) references p(id))",
    );
    for (sql, job) in [
        (
            "alter table t comment 'hello'",
            Some("modify table comment|public|0|synced"),
        ),
        (
            "alter table t alter index kb invisible",
            Some("alter index visibility|public|0|synced"),
        ),
        ("alter table t alter index kb invisible", None),
        (
            "alter table t rename index kb to kb2",
            Some("rename index|public|0|synced"),
        ),
        ("alter table t rename index kb2 to kb2", None),
        (
            "alter table t drop foreign key fk",
            Some("drop foreign key|none|0|synced"),
        ),
        (
            "alter table t alter column b set default 7",
            Some("set default value|public|0|synced"),
        ),
        (
            "alter table t alter column b drop default",
            Some("set default value|public|0|synced"),
        ),
        (
            "alter table t shard_row_id_bits = 4",
            Some("shard row ID|public|0|synced"),
        ),
        ("alter table t shard_row_id_bits = 4", None),
        (
            "alter table t auto_id_cache = 100",
            Some("modify auto id cache|public|0|synced"),
        ),
    ] {
        let before = last_job(&mut session);
        rows(&mut session, sql);
        let after = last_job(&mut session);
        match job {
            Some(job) => {
                assert_ne!(after, before, "{sql}");
                assert_eq!(job_kind(&after), job, "{sql}");
            }
            None => assert_eq!(after, before, "{sql}"),
        }
    }
    for (sql, code, message) in [
        (
            "alter table t alter index nosuch visible",
            1176,
            "Key 'nosuch' doesn't exist in table 't'",
        ),
        (
            "alter table t rename index nosuch to x",
            1176,
            "Key 'nosuch' doesn't exist in table 't'",
        ),
        (
            "alter table t rename index kb2 to ka",
            1061,
            "Duplicate key name 'ka'",
        ),
        (
            "alter table t drop foreign key fk",
            1091,
            "Can't DROP 'fk'; check that column/key exists",
        ),
        (
            "alter table t alter column nosuch set default 1",
            1054,
            "Unknown column 'nosuch' in 't'",
        ),
        (
            "alter table t convert to character set latin1",
            8200,
            "Unsupported modify charset from utf8mb4 to latin1",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let created = displayed(rows(&mut session, "show create table t")).remove(0);
    assert_eq!(
        created[1],
        "CREATE TABLE `t` (\n  `a` int DEFAULT NULL,\n  `b` int,\n  `c` varchar(10) DEFAULT NULL,\n  KEY `ka` (`a`),\n  KEY `kb2` (`b`) /*!80000 INVISIBLE */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=100 */ /*T! SHARD_ROW_ID_BITS=4 */ COMMENT='hello'"
    );
}

/// RENAME TABLE runs as Go's persisted `ActionRenameTable` /
/// `ActionRenameTables` jobs: the foreign keys naming a renamed parent
/// follow it, a chain inside one statement settles on its last name, and
/// the refusals are the executor's. Captured from the Go oracle.
#[test]
fn rename_tables_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1943)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 1, 2, 3, 4, 7, 11]
            .map(|column| row[column].clone())
            .join("|")
    };
    let after_id = |job: &str| job.split_once('|').unwrap().1.to_owned();
    rows(&mut session, "create database d2");
    rows(&mut session, "create table p (id int primary key)");
    rows(
        &mut session,
        "create table c (id int primary key, pid int, constraint fkc foreign key (pid) references p(id))",
    );
    rows(&mut session, "insert into p values (1)");
    rows(&mut session, "insert into c values (1, 1)");
    rows(&mut session, "rename table p to p2");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|p2|rename table|public|0|synced"
    );
    let fk_of_c = |session: &mut _| {
        let created = displayed(rows(session, "show create table c")).remove(0);
        created[1]
            .lines()
            .find(|line| line.contains("FOREIGN KEY"))
            .unwrap()
            .trim()
            .to_owned()
    };
    assert_eq!(
        fk_of_c(&mut session),
        "CONSTRAINT `fkc` FOREIGN KEY (`pid`) REFERENCES `p2` (`id`)"
    );
    let error = session
        .node_conn()
        .execute_write("insert into c values (2, 2)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1452,
            "Cannot add or update a child row: a foreign key constraint fails (`test`.`c`, CONSTRAINT `fkc` FOREIGN KEY (`pid`) REFERENCES `p2` (`id`))"
        )
    );
    rows(&mut session, "alter table p2 rename to p3");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|p3|rename table|public|0|synced"
    );
    let before = last_job(&mut session);
    rows(&mut session, "alter table p3 rename to p3");
    assert_eq!(last_job(&mut session), before);
    rows(&mut session, "rename table p3 to d2.p4");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "d2|p4|rename table|public|0|synced"
    );
    assert_eq!(
        fk_of_c(&mut session),
        "CONSTRAINT `fkc` FOREIGN KEY (`pid`) REFERENCES `d2`.`p4` (`id`)"
    );
    rows(&mut session, "create table a (x int)");
    rows(&mut session, "create table b (x int)");
    rows(&mut session, "rename table a to a1, b to b1, a1 to a2");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|a2,b1,a2|rename tables|public|0|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "show tables")),
        [["a2"], ["b1"], ["c"]]
    );
    rows(&mut session, "create view v as select 1");
    for (sql, code, message) in [
        (
            "rename table nosuch to z",
            1146,
            "Table 'test.nosuch' doesn't exist",
        ),
        (
            "rename table a2 to b1",
            1050,
            "Table 'test.b1' already exists",
        ),
        (
            "rename table a2 to nodb.z",
            1025,
            "Error on rename of 'test.a2' to 'nodb.z' (errno: 168 - Database `nodb` doesn't exist)",
        ),
        (
            "rename table v to d2.v",
            1450,
            "Changing schema from 'test' to 'd2' is not allowed.",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(&mut session, "rename table v to v2");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|v2|rename table|public|0|synced"
    );
    assert_eq!(displayed(rows(&mut session, "select * from v2")), [["1"]]);
    rows(&mut session, "insert into a2 values (1)");
    assert_eq!(displayed(rows(&mut session, "select * from a2")), [["1"]]);
}

/// ADD, DROP and TRUNCATE PARTITION run as Go's persisted
/// `ActionAddTablePartition` (none -> replica-only -> public),
/// `ActionDropTablePartition` and `ActionTruncateTablePartition` (public ->
/// write-only -> delete-only -> delete-reorganization -> none) jobs, a schema
/// version per step; IF [NOT] EXISTS queue nothing and leave Go's note, and
/// FIRST / LAST PARTITION rewrite the job's query (captured from Go on this
/// branch).
#[test]
fn partition_changes_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1952)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let version = |session: &mut _| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    // (job id, db|table|type|state|status, query) of the latest job.
    let job = |session: &mut _| -> (String, String, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        let query = displayed(rows(
            session,
            &format!("admin show ddl job queries {}", row[0]),
        ))
        .remove(0)
        .remove(0);
        (
            row[0].clone(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
            query,
        )
    };
    let step = |session: &mut Session, sql: &str| -> (i64, String, String) {
        let before = version(session);
        rows(session, sql);
        let (_, kind, query) = job(session);
        (version(session) - before, kind, query)
    };
    let refused = |session: &mut Session, sql: &str| -> (u16, String) {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        (error.code, error.message)
    };
    rows(
        &mut session,
        "create table r (a int, b int, unique key ua(a)) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20))",
    );
    rows(&mut session, "insert into r values (1,1),(11,11)");
    let sql = "alter table r add partition (partition p2 values less than (30), partition p3 values less than (40))";
    assert_eq!(
        step(&mut session, sql),
        (
            2,
            "test|r|add partition|public|synced".to_owned(),
            sql.to_owned()
        )
    );
    let (last, _, _) = job(&mut session);
    rows(
        &mut session,
        "alter table r add partition if not exists (partition p2 values less than (50))",
    );
    assert_eq!(
        displayed(rows(&mut session, "show warnings")),
        [["Note", "1517", "Duplicate partition name p2"]]
    );
    assert_eq!(job(&mut session).0, last, "IF NOT EXISTS queues nothing");
    assert_eq!(
        refused(
            &mut session,
            "alter table r add partition (partition p2 values less than (50))"
        ),
        (1517, "Duplicate partition name p2".to_owned())
    );
    assert_eq!(
        refused(
            &mut session,
            "alter table r add partition (partition p9 values less than (35))"
        ),
        (
            1493,
            "VALUES LESS THAN value must be strictly increasing for each partition".to_owned()
        )
    );
    let sql = "alter table r drop partition p3";
    assert_eq!(
        step(&mut session, sql),
        (
            4,
            "test|r|drop partition|none|synced".to_owned(),
            sql.to_owned()
        )
    );
    let (last, _, _) = job(&mut session);
    rows(&mut session, "alter table r drop partition if exists p3");
    assert_eq!(
        displayed(rows(&mut session, "show warnings")),
        [["Note", "1507", "Error in list of partitions to DROP"]]
    );
    assert_eq!(job(&mut session).0, last, "IF EXISTS queues nothing");
    for sql in [
        "alter table r truncate partition p0, p1",
        "alter table r truncate partition all",
        "alter table r truncate partition p0, p0",
    ] {
        assert_eq!(
            step(&mut session, sql),
            (
                4,
                "test|r|truncate partition|none|synced".to_owned(),
                sql.to_owned()
            )
        );
    }
    assert!(rows(&mut session, "select * from r").is_empty());
    assert_eq!(
        refused(&mut session, "alter table r truncate partition nosuch"),
        (1735, "Unknown partition 'nosuch' in table 'r'".to_owned())
    );
    rows(&mut session, "insert into r values (1,1),(11,11),(21,21)");
    assert_eq!(
        step(&mut session, "alter table r drop partition p0, p1").0,
        4
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from r")),
        [["21", "21"]]
    );
    assert_eq!(
        refused(&mut session, "alter table r drop partition p2"),
        (
            1508,
            "Cannot remove all partitions, use DROP TABLE instead".to_owned()
        )
    );

    // A HASH table's ADD is a reorganization; DROP is refused and TRUNCATE
    // is the same job as on RANGE.
    rows(
        &mut session,
        "create table h (a int) partition by hash (a) partitions 2",
    );
    rows(&mut session, "insert into h values (1),(2),(3)");
    rows(&mut session, "alter table h add partition (partition p9)");
    assert_eq!(
        displayed(rows(&mut session, "select a from h order by a")),
        [["1"], ["2"], ["3"]]
    );
    assert_eq!(
        refused(&mut session, "alter table h drop partition p0"),
        (
            1512,
            "DROP PARTITION can only be used on RANGE/LIST partitions".to_owned()
        )
    );
    let sql = "alter table h truncate partition p0";
    assert_eq!(
        step(&mut session, sql),
        (
            4,
            "test|h|truncate partition|none|synced".to_owned(),
            sql.to_owned()
        )
    );

    rows(
        &mut session,
        "create table l (a int) partition by list (a) (partition p0 values in (1,2), partition pd default)",
    );
    assert_eq!(
        refused(&mut session, "alter table l add partition (partition p1 values in (3))"),
        (
            8200,
            "Unsupported ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead".to_owned()
        )
    );
    rows(
        &mut session,
        "create table l2 (a int) partition by list (a) (partition p0 values in (1,2))",
    );
    let sql =
        "alter table l2 add partition (partition p1 values in (3), partition p2 values in (4))";
    assert_eq!(
        step(&mut session, sql),
        (
            2,
            "test|l2|add partition|public|synced".to_owned(),
            sql.to_owned()
        )
    );
    assert_eq!(
        refused(
            &mut session,
            "alter table l2 add partition (partition p3 values in (4))"
        ),
        (
            1495,
            "Multiple definition of same constant in list partitioning".to_owned()
        )
    );

    rows(
        &mut session,
        "create table i (a int) partition by range (a) interval (10) \
         first partition less than (10) last partition less than (40)",
    );
    assert_eq!(
        step(&mut session, "alter table i last partition less than (60)"),
        (
            2,
            "test|i|add partition|public|synced".to_owned(),
            "alter table i ADD PARTITION (PARTITION `P_LT_50` VALUES LESS THAN (50),\n PARTITION `P_LT_60` VALUES LESS THAN (60))".to_owned()
        )
    );
    assert_eq!(
        step(&mut session, "alter table i first partition less than (30)"),
        (
            4,
            "test|i|drop partition|none|synced".to_owned(),
            "alter table i DROP PARTITION `P_LT_10`, `P_LT_20`".to_owned()
        )
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select partition_name from information_schema.partitions \
             where table_schema = 'test' and table_name = 'i' order by partition_ordinal_position",
        )),
        [["P_LT_30"], ["P_LT_40"], ["P_LT_50"], ["P_LT_60"]]
    );

    rows(&mut session, "create table nt (a int)");
    for sql in [
        "alter table nt add partition (partition p0 values less than (10))",
        "alter table nt drop partition p0",
        "alter table nt truncate partition p0",
    ] {
        assert_eq!(
            refused(&mut session, sql),
            (
                1505,
                "Partition management on a not partitioned table is not possible".to_owned()
            ),
            "{sql}"
        );
    }

    // ALTER TABLE ... ENGINE_ATTRIBUTE / STORAGE_CLASS: Go's one-step
    // `ActionModifyEngineAttribute` job.
    rows(
        &mut session,
        "create table e (a int) partition by range (a) (partition p0 values less than (10))",
    );
    for sql in [
        "alter table e engine_attribute = '{\"storage_class\": \"STANDARD\"}'",
        "alter table e storage_class = 'IA'",
    ] {
        assert_eq!(
            step(&mut session, sql),
            (
                1,
                "test|e|modify engine attribute|public|synced".to_owned(),
                sql.to_owned()
            )
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "show create table e"))[0][1],
        "CREATE TABLE `e` (\n  `a` int DEFAULT NULL\n) ENGINE=InnoDB STORAGE_CLASS='IA' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE (`a`)\n(PARTITION `p0` VALUES LESS THAN (10))"
    );
}

/// MODIFY COLUMN with AUTO_RANDOM runs as Go's persisted
/// `ActionModifyColumn` job (`checkAutoRandom` at admission, the owner's
/// `checkAndApplyAutoRandomBits`): an AUTO_INCREMENT clustered key converts
/// and continues past the row IDs handed out, the shard bits may grow, and
/// every other change is Go's refusal (captured from Go on this branch).
#[test]
fn auto_random_changes_run_as_modify_column_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1956)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "set @@tidb_allow_remove_auto_inc = 1");
    let version = |session: &mut Session| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|")
    };
    let refused = |session: &mut Session, sql: &str| -> (u16, String) {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        (error.code, error.message)
    };
    rows(
        &mut session,
        "create table t (a bigint auto_increment primary key, b int)",
    );
    rows(&mut session, "insert into t (b) values (1),(2)");
    for sql in [
        "alter table t modify a bigint auto_random(5)",
        "alter table t modify a bigint auto_random(6)",
        "alter table t modify a bigint auto_random(6) comment 'x'",
    ] {
        let before = version(&mut session);
        rows(&mut session, sql);
        assert_eq!(version(&mut session) - before, 1, "{sql}");
        assert_eq!(
            job(&mut session),
            "test|t|modify column|public|synced",
            "{sql}"
        );
    }
    let shown = displayed(rows(&mut session, "show create table t"))
        .remove(0)
        .remove(1);
    assert!(
        shown.contains("`a` bigint NOT NULL /*T![auto_rand] AUTO_RANDOM(6) */ COMMENT 'x'"),
        "{shown}"
    );
    // The converted column continues past the AUTO_INCREMENT IDs.
    rows(&mut session, "insert into t (b) values (3)");
    let ids = displayed(rows(
        &mut session,
        "select a & 0x7FFFFFFFFFF from t order by b",
    ));
    let third: i64 = ids[2][0].parse().unwrap();
    assert_eq!((ids[0][0].as_str(), ids[1][0].as_str()), ("1", "2"));
    assert!(third > 2, "{ids:?}");
    let invalid = |reason: &str| (8216, format!("Invalid auto random: {reason}"));
    for (sql, expected) in [
        (
            "alter table t modify a bigint auto_random(4)",
            invalid("decreasing auto_random shard bits is not supported"),
        ),
        (
            "alter table t modify a bigint auto_random(16)",
            invalid("max allowed auto_random shard bits is 15, but got 16 on column `a`"),
        ),
        (
            "alter table t modify a bigint auto_random(6, 32)",
            invalid("alter the range bits of auto_random column is not supported"),
        ),
        (
            "alter table t modify a int auto_random(6)",
            (
                8200,
                "Unsupported modify column: this column has primary key flag".to_owned(),
            ),
        ),
        (
            "alter table t modify a bigint auto_random(6) default 3",
            invalid("auto_random is incompatible with default"),
        ),
        (
            "alter table t modify a bigint",
            invalid("adding/dropping/modifying auto_random is not supported"),
        ),
    ] {
        assert_eq!(refused(&mut session, sql), expected, "{sql}");
    }
    rows(&mut session, "create table u (a bigint primary key, b int)");
    rows(
        &mut session,
        "create table v (a bigint auto_increment primary key nonclustered, b int)",
    );
    for sql in [
        "alter table u modify a bigint auto_random(5)",
        "alter table v modify a bigint auto_random(5)",
    ] {
        assert_eq!(
            refused(&mut session, sql),
            invalid("auto_random can only be converted from auto_increment clustered primary key"),
            "{sql}"
        );
    }
}

/// REORGANIZE PARTITION, ADD PARTITION on a HASH table, COALESCE
/// PARTITION, ALTER TABLE ... PARTITION BY and REMOVE PARTITIONING run as
/// Go's persisted reorganization jobs: six schema versions (none ->
/// delete-only -> write-only -> write-reorganization -> delete-reorganization
/// -> public -> none), the rows copied and every index rebuilt, the job
/// counting one row per copy and per index entry (captured from Go on this
/// branch).
#[test]
fn partition_reorganizations_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1955)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let version = |session: &mut Session| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 7, 11]
            .map(|column| row[column].clone())
            .join("|")
    };
    // (versions, warnings) of one statement.
    let step = |session: &mut Session, sql: &str| -> (i64, Vec<Vec<String>>) {
        let before = version(session);
        rows(session, sql);
        let warnings = displayed(rows(session, "show warnings"));
        (version(session) - before, warnings)
    };
    let related = vec![vec![
        "Warning".to_owned(),
        "1105".to_owned(),
        "The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now".to_owned(),
    ]];
    let new = vec![vec![
        "Warning".to_owned(),
        "1105".to_owned(),
        "The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now".to_owned(),
    ]];
    rows(
        &mut session,
        "create table r (a int, b int, unique key ub(b) global, key ka(a)) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20), \
         partition p2 values less than (30))",
    );
    rows(
        &mut session,
        "insert into r values (1,1),(5,5),(11,11),(15,15),(25,25)",
    );
    assert_eq!(
        step(
            &mut session,
            "alter table r reorganize partition p0, p1 into \
             (partition p01 values less than (15), partition p11 values less than (20))",
        ),
        (6, related.clone())
    );
    assert_eq!(
        job(&mut session),
        "test|r|alter table reorganize partition|none|13|synced"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select a, b from r partition (p01) order by a"
        )),
        [["1", "1"], ["5", "5"], ["11", "11"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "select a, b from r partition (p11)")),
        [["15", "15"]]
    );
    rows(&mut session, "admin check table r");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from r use index(ub) where b = 15"
        )),
        [["15", "15"]]
    );

    rows(
        &mut session,
        "create table h (a int, b int) partition by hash (a) partitions 2",
    );
    rows(&mut session, "insert into h values (1,1),(2,2),(3,3),(4,4)");
    assert_eq!(
        step(&mut session, "alter table h add partition partitions 1"),
        (6, related.clone())
    );
    assert_eq!(
        job(&mut session),
        "test|h|alter table reorganize partition|none|4|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select a from h partition (p2)")),
        [["2"]]
    );
    assert_eq!(
        step(&mut session, "alter table h coalesce partition 2"),
        (6, related)
    );
    assert_eq!(
        displayed(rows(&mut session, "select count(*) from h")),
        [["4"]]
    );
    rows(&mut session, "admin check table h");

    rows(&mut session, "create table n (a int primary key, b int)");
    rows(&mut session, "insert into n values (1,1),(12,12)");
    assert_eq!(
        step(
            &mut session,
            "alter table n partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        ),
        (6, new.clone())
    );
    assert_eq!(
        job(&mut session),
        "test|n|alter table partition by|none|2|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select a from n partition (p1)")),
        [["12"]]
    );
    rows(&mut session, "admin check table n");
    assert_eq!(
        step(&mut session, "alter table n remove partitioning"),
        (6, Vec::<Vec<String>>::new())
    );
    assert_eq!(
        job(&mut session),
        "test|n|alter table remove partitioning|none|2|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select a from n order by a")),
        [["1"], ["12"]]
    );
    rows(&mut session, "admin check table n");

    assert_eq!(
        step(
            &mut session,
            "alter table r partition by hash(a) partitions 2"
        ),
        (6, new)
    );
    assert_eq!(
        job(&mut session),
        "test|r|alter table partition by|none|15|synced"
    );
    rows(&mut session, "admin check table r");
    assert_eq!(
        displayed(rows(&mut session, "select count(*) from r")),
        [["5"]]
    );
    let error = session
        .node_conn()
        .execute_write(
            "alter table r reorganize partition p9 into (partition p10 values less than (5))",
        )
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8200, "Unsupported reorganize partition")
    );
}

/// EXCHANGE PARTITION runs as Go's persisted `ActionExchangeTablePartition`
/// job: the standalone table is marked (`ExchangePartitionInfo`) under one
/// version, then the ids swap under the next; a row that fails the
/// validation rolls the job back after the marking version (captured from
/// Go on this branch).
#[test]
fn exchange_partition_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1954)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let version = |session: &mut Session| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|")
    };
    rows(
        &mut session,
        "create table pt (a int, b int) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20))",
    );
    rows(&mut session, "create table nt (a int, b int)");
    rows(&mut session, "insert into pt values (1,1),(11,11)");
    rows(&mut session, "insert into nt values (2,2),(3,3)");
    let before = version(&mut session);
    rows(
        &mut session,
        "alter table pt exchange partition p0 with table nt",
    );
    assert_eq!(
        displayed(rows(&mut session, "show warnings")),
        [[
            "Warning",
            "1105",
            "after the exchange, please analyze related table of the exchange to update statistics"
        ]]
    );
    assert_eq!(version(&mut session) - before, 2);
    assert_eq!(job(&mut session), "test|pt|exchange partition|none|synced");
    assert_eq!(
        displayed(rows(&mut session, "select * from pt order by a")),
        [["2", "2"], ["3", "3"], ["11", "11"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from nt")),
        [["1", "1"]]
    );

    rows(&mut session, "insert into nt values (15,15)");
    let before = version(&mut session);
    let error = session
        .node_conn()
        .execute_write("alter table pt exchange partition p0 with table nt")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1737, "Found a row that does not match the partition")
    );
    assert_eq!(version(&mut session) - before, 2);
    assert_eq!(
        job(&mut session),
        "test|nt|exchange partition|public|rollback done"
    );
    // The rollback lifted the restriction: nt takes any row again.
    rows(&mut session, "insert into nt values (16,16)");
    rows(&mut session, "delete from nt where a = 16");

    let before = version(&mut session);
    rows(
        &mut session,
        "alter table pt exchange partition p0 with table nt without validation",
    );
    assert_eq!(version(&mut session) - before, 2);
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from pt partition (p0) order by a"
        )),
        [["1", "1"], ["15", "15"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from nt order by a")),
        [["2", "2"], ["3", "3"]]
    );
    let error = session
        .node_conn()
        .execute_write("alter table pt exchange partition p9 with table nt")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1735, "Unknown partition 'p9' in table 'pt'")
    );
}

/// DROP and TRUNCATE PARTITION clean the old partitions' rows out of the
/// global indexes in their delete-reorganization step (Go
/// `cleanGlobalIndexEntriesFromDroppedPartitions`): their unique values are
/// free again, `ADMIN CHECK TABLE` passes, and the job counts one entry per
/// row and global index (captured from Go on this branch).
#[test]
fn dropped_partitions_leave_the_global_indexes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1953)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 7, 11]
            .map(|column| row[column].clone())
            .join("|")
    };
    rows(
        &mut session,
        "create table g (a int, b int, c int, unique key ub(b) global, key kc(c) global) \
         partition by range (a) (partition p0 values less than (10), \
         partition p1 values less than (20), partition p2 values less than (30))",
    );
    rows(
        &mut session,
        "insert into g values (1,1,1),(2,2,2),(11,11,11),(21,21,21)",
    );
    rows(&mut session, "alter table g drop partition p0");
    assert_eq!(job(&mut session), "test|g|drop partition|none|4|synced");
    rows(&mut session, "insert into g values (25,1,1)");
    rows(&mut session, "admin check table g");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from g use index(ub) where b = 1"
        )),
        [["25", "1", "1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select a from g use index(kc) where c in (1,2) order by a"
        )),
        [["25"]]
    );
    rows(&mut session, "alter table g truncate partition p1");
    assert_eq!(job(&mut session), "test|g|truncate partition|none|2|synced");
    rows(&mut session, "insert into g values (12,11,11)");
    rows(&mut session, "admin check table g");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select a from g use index(kc) order by a"
        )),
        [["12"], ["21"], ["25"]]
    );
}

/// LOCK TABLES, UNLOCK TABLES and ADMIN CLEANUP TABLE LOCK run as Go's
/// persisted `ActionLockTable` / `ActionUnlockTable` jobs: the session's
/// earlier locks are released one table per step, each new lock goes
/// none -> pre-lock -> public (one more READ holder skips the pre-lock), an
/// unlock that changes nothing still queues a job without a version, and an
/// unlock job records no query (captured from Go on this branch).
#[test]
fn table_locks_run_as_persisted_jobs_like_go() {
    let previous = tidb_config::config_tree::config::get_global_config();
    tidb_config::config_tree::config::update_global(|config| config.enable_table_lock = true);
    let (stack, _users) = cop_backed_stack();
    let mut first = stack.factory.open_session(session_context(1950)).unwrap();
    let mut second = stack.factory.open_session(session_context(1951)).unwrap();
    rows(&mut first, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut first, "USE test");
    rows(&mut second, "USE test");
    rows(&mut first, "create table t1 (a int)");
    rows(&mut first, "create table t2 (a int)");
    let version = |session: &mut _| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    // (job id, db|table|type|state|status, query) of the latest job.
    let job = |session: &mut _| -> (String, String, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        let query = displayed(rows(
            session,
            &format!("admin show ddl job queries {}", row[0]),
        ))
        .remove(0)
        .remove(0);
        (
            row[0].clone(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
            query,
        )
    };
    let step = |session: &mut _, sql: &str| -> (i64, String, String, String) {
        let before = version(session);
        rows(session, sql);
        let (id, kind, query) = job(session);
        (version(session) - before, id, kind, query)
    };
    for (on_second, sql, delta, expected, query) in [
        (
            false,
            "lock tables t1 write, t2 read",
            4,
            "test|t2|lock table|public|synced",
            "lock tables t1 write, t2 read",
        ),
        // t1 and t2 are released first, then t1 is locked.
        (
            false,
            "lock tables t1 read",
            4,
            "test|t1|lock table|public|synced",
            "lock tables t1 read",
        ),
        // One more READ holder: no pre-lock.
        (
            true,
            "lock tables t1 read",
            1,
            "test|t1|lock table|public|synced",
            "lock tables t1 read",
        ),
        (
            false,
            "unlock tables",
            1,
            "test|t1|unlock table|none|synced",
            "",
        ),
        (
            true,
            "unlock tables",
            1,
            "test|t1|unlock table|none|synced",
            "",
        ),
        (
            false,
            "lock tables t1 write",
            2,
            "test|t1|lock table|public|synced",
            "lock tables t1 write",
        ),
        (
            true,
            "admin cleanup table lock t1",
            1,
            "test|t1|unlock table|none|synced",
            "",
        ),
        // The cleanup already released t1: the job changes nothing.
        (
            false,
            "unlock tables",
            0,
            "test|t1|unlock table|none|synced",
            "",
        ),
        (
            false,
            "lock tables t2 write local",
            2,
            "test|t2|lock table|public|synced",
            "lock tables t2 write local",
        ),
        // t1 holds no lock; t2's goes.
        (
            true,
            "admin cleanup table lock t1, t2",
            1,
            "test|t2|unlock table|none|synced",
            "",
        ),
    ] {
        let session = if on_second { &mut second } else { &mut first };
        let (got_delta, _, kind, got_query) = step(session, sql);
        assert_eq!(
            (got_delta, kind.as_str(), got_query.as_str()),
            (delta, expected, query),
            "{sql}"
        );
    }
    // The cleanup did not touch this session's own list: its UNLOCK still
    // queues a job, and holding nothing, the next one queues none.
    let (delta, last, kind, _) = step(&mut first, "unlock tables");
    assert_eq!((delta, kind.as_str()), (0, "test|t2|unlock table|none|synced"));
    let (delta, id, _, _) = step(&mut first, "unlock tables");
    assert_eq!((delta, id), (0, last));
    // Another session's WRITE lock refuses before any job.
    step(&mut first, "lock tables t1 write");
    let (_, last, _, _) = step(&mut second, "select 1");
    let error = second
        .node_conn()
        .execute_write("lock tables t1 read")
        .unwrap_err();
    assert_eq!(error.code, 8020, "{}", error.message);
    assert_eq!(job(&mut second).0, last);
    rows(&mut first, "unlock tables");
    tidb_config::config_tree::config::store_global_config((*previous).clone());
}

/// The TiFlash replica manager's status update runs as Go's persisted
/// `ActionUpdateTiFlashReplicaStatus` job (`UpdateTableReplicaInfo` queues it
/// and the owner applies it): the new status is published, and a status
/// that already holds queues nothing.
#[test]
fn tiflash_replica_status_runs_as_a_persisted_job_like_go() {
    struct FailpointGuard;
    impl Drop for FailpointGuard {
        fn drop(&mut self) {
            fail::remove("mockTiFlashStoreCount");
        }
    }
    // Go `TestSetTableFlashReplica` enables `mockTiFlashStoreCount` so the
    // replica count passes `checkTiFlashReplicaCount` without TiFlash stores.
    fail::cfg("mockTiFlashStoreCount", "return(true)").expect("enable failpoint");
    let _guard = FailpointGuard;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1949)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table r (a int)");
    rows(&mut session, "alter table r set tiflash replica 1");
    let table_id: i64 = displayed(rows(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name = 'r'",
    ))[0][0]
        .parse()
        .unwrap();
    let job = |session: &mut _| -> (i64, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        (
            row[0].parse().unwrap(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
        )
    };
    let update = |available: bool| {
        stack.factory.ddl.execute(
            &tidb_ddl::cluster_ddl::DdlStatement::UpdateTiFlashReplicaStatus {
                table_id,
                available,
            },
            &tidb_sessionctx::StmtContext::for_query(),
        )
    };
    update(true).unwrap();
    let (updated, kind) = job(&mut session);
    assert_eq!(kind, "test|r|update tiflash replica status|public|synced");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select available from information_schema.tiflash_replica where table_name = 'r'"
        )),
        [["1"]]
    );
    update(true).unwrap();
    assert_eq!(
        job(&mut session).0,
        updated,
        "the status already holds: no job"
    );
    let missing = stack.factory.ddl.execute(
        &tidb_ddl::cluster_ddl::DdlStatement::UpdateTiFlashReplicaStatus {
            table_id: table_id + 1000,
            available: true,
        },
        &tidb_sessionctx::StmtContext::for_query(),
    );
    assert!(missing.is_err());
}

/// Table placement, attributes, the region split policy, TiFlash replicas
/// and ALTER TABLE CACHE run as Go's persisted jobs: CACHE and NOCACHE pass
/// through `TableCacheStatusSwitching`, a schema version each step (oracle:
/// ADMIN SHOW DDL's SCHEMA_VER moves by 2 for each, by 1 for ATTRIBUTES),
/// and a repeated CACHE queues nothing. Captured from the Go oracle.
#[test]
fn table_options_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1948)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let job = |session: &mut _| -> (i64, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        (
            row[0].parse().unwrap(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
        )
    };
    let version = |session: &mut _| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    rows(&mut session, "create table c1 (a int, b int, key ib(b))");
    rows(&mut session, "create placement policy p1 followers=2");
    for (sql, expected) in [
        (
            "alter table c1 placement policy p1",
            "test|c1|alter table placement|public|synced",
        ),
        (
            "alter table c1 placement policy default",
            "test|c1|alter table placement|public|synced",
        ),
        (
            "alter table c1 split index ib between (0) and (100) regions 2",
            "test|c1|alter table set region split policy|public|synced",
        ),
        (
            "alter table c1 set tiflash replica 0",
            "test|c1|set tiflash replica|public|synced",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(job(&mut session).1, expected, "{sql}");
    }
    let before = version(&mut session);
    rows(
        &mut session,
        "alter table c1 attributes 'merge_option=deny'",
    );
    assert_eq!(
        job(&mut session).1,
        "test|c1|alter table attributes|public|synced"
    );
    assert_eq!(version(&mut session), before + 1);

    let before = version(&mut session);
    rows(&mut session, "alter table c1 cache");
    let (cached, kind) = job(&mut session);
    assert_eq!(kind, "test|c1|alter table cache|public|synced");
    assert_eq!(version(&mut session), before + 2);
    rows(&mut session, "alter table c1 cache");
    assert_eq!(job(&mut session).0, cached, "already cached: no job");
    let before = version(&mut session);
    rows(&mut session, "alter table c1 nocache");
    assert_eq!(
        job(&mut session).1,
        "test|c1|alter table nocache|public|synced"
    );
    assert_eq!(version(&mut session), before + 2);

    rows(
        &mut session,
        "create table tp (a int) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20))",
    );
    rows(
        &mut session,
        "alter table tp partition p0 placement policy p1",
    );
    assert_eq!(
        job(&mut session).1,
        "test|tp|alter table partition placement|public|synced"
    );
    let error = session
        .node_conn()
        .execute_write("alter table tp cache")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8242, "'partition mode' is unsupported on cache tables.")
    );
}

/// Placement policies and resource groups run as Go's persisted jobs: a
/// policy's id comes from the policies' own sequence (Go
/// `genPlacementPolicyID`, so the first is 1 and OR REPLACE spends one
/// before the owner keeps the existing policy), a policy a database still
/// names cannot be dropped, and DROP RESOURCE GROUP's name-only group is
/// queued like Go's. Captured from the Go oracle.
#[test]
fn placement_policies_and_resource_groups_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1947)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let job = |session: &mut _| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 3, 4, 5, 11].map(|column| row[column].clone()).join("|")
    };
    rows(&mut session, "create placement policy p1 followers=2");
    assert_eq!(
        job(&mut session),
        "p1|create placement policy|public|1|synced"
    );
    rows(
        &mut session,
        "create or replace placement policy p1 followers=4",
    );
    assert_eq!(
        job(&mut session),
        "p1|create placement policy|public|1|synced"
    );
    rows(&mut session, "create placement policy p2 followers=2");
    assert_eq!(
        job(&mut session),
        "p2|create placement policy|public|3|synced"
    );
    rows(&mut session, "create database dp placement policy p2");
    let error = session
        .node_conn()
        .execute_write("drop placement policy p2")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8241, "Placement policy 'p2' is still in use")
    );
    rows(&mut session, "alter database dp placement policy default");
    rows(&mut session, "drop placement policy p2");
    assert_eq!(job(&mut session), "p2|drop placement policy|none|3|synced");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select policy_id, policy_name from information_schema.placement_policies"
        )),
        [["1", "p1"]]
    );

    rows(&mut session, "create resource group rg1 ru_per_sec=1000");
    let created = job(&mut session);
    assert!(
        created.starts_with("rg1|create resource group|public|"),
        "{created}"
    );
    rows(&mut session, "alter resource group rg1 ru_per_sec=2000");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select ru_per_sec from information_schema.resource_groups where name = 'rg1'"
        )),
        [["2000"]]
    );
    rows(&mut session, "drop resource group rg1");
    let dropped = job(&mut session);
    assert!(
        dropped.starts_with("rg1|drop resource group|none|"),
        "{dropped}"
    );
}

/// CREATE VIEW, CREATE / ALTER SEQUENCE, CREATE TABLE LIKE, the
/// AUTO_RANDOM rebase, TTL and ALTER DATABASE run as Go's persisted jobs:
/// Go queues a job even when the owner then finds nothing to change (a
/// repeated placement policy, an ALTER SEQUENCE to the same options), and
/// the AUTO_RANDOM rebase warns like the AUTO_INCREMENT one. Captured from
/// the Go oracle.
#[test]
fn objects_and_options_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1946)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let job = |session: &mut _| -> (i64, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        (
            row[0].parse().unwrap(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
        )
    };
    for (sql, expected) in [
        (
            "create view v1 as select 1",
            "test|v1|create view|public|synced",
        ),
        (
            "create or replace view v1 as select 2",
            "test|v1|create view|public|synced",
        ),
        (
            "create sequence s1 start 10",
            "test|s1|create sequence|public|synced",
        ),
        (
            "create table t1 (a bigint primary key auto_random, b int)",
            "test|t1|create table|public|synced",
        ),
        (
            "create table t2 like t1",
            "test|t2|create table|public|synced",
        ),
        (
            "create table t3 (a int, created timestamp)",
            "test|t3|create table|public|synced",
        ),
        (
            "alter table t3 ttl = created + interval 1 day",
            "test|t3|alter table ttl|public|synced",
        ),
        (
            "alter table t3 remove ttl",
            "test|t3|alter table no_ttl|public|synced",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(job(&mut session).1, expected, "{sql}");
    }
    assert_eq!(displayed(rows(&mut session, "select * from v1")), [["2"]]);
    assert_eq!(
        displayed(rows(&mut session, "select nextval(s1)")),
        [["10"]]
    );
    rows(&mut session, "alter sequence s1 restart with 100");
    assert_eq!(job(&mut session).1, "test|s1|alter sequence|public|synced");
    assert_eq!(
        displayed(rows(&mut session, "select nextval(s1)")),
        [["100"]]
    );
    // The owner finds nothing to change: done, no new schema state.
    rows(&mut session, "alter sequence s1 increment by 1");
    assert_eq!(job(&mut session).1, "test|s1|alter sequence|none|synced");

    rows(&mut session, "insert into t1 (b) values (1)");
    rows(&mut session, "alter table t1 auto_random_base = 1");
    let warnings = displayed(rows(&mut session, "show warnings"));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(
        (warnings[0][0].as_str(), warnings[0][1].as_str()),
        ("Warning", "1105")
    );
    assert!(
        warnings[0][2].starts_with("Can't reset AUTO_INCREMENT to 1 without FORCE option, using "),
        "{warnings:?}"
    );
    assert_eq!(
        job(&mut session).1,
        "test|t1|rebase auto_random ID|public|synced"
    );

    rows(&mut session, "create database dbx");
    rows(
        &mut session,
        "alter database dbx charset utf8mb4 collate utf8mb4_general_ci",
    );
    assert_eq!(
        job(&mut session).1,
        "dbx||modify schema charset and collate|public|synced"
    );
    rows(&mut session, "create placement policy p1 followers=2");
    rows(&mut session, "alter database dbx placement policy p1");
    let (first, kind) = job(&mut session);
    assert_eq!(kind, "dbx||modify schema default placement|public|synced");
    rows(&mut session, "alter database dbx placement policy p1");
    let (second, kind) = job(&mut session);
    assert_eq!(kind, "dbx||modify schema default placement|public|synced");
    assert!(second > first, "Go queues the repeated change too");
}

/// A multi-schema ALTER runs as Go's persisted `ActionMultiSchemaChange`
/// job: ADMIN SHOW DDL JOBS lists every sub-job under it (with the comments
/// column one row down, as Go writes it), a sub-job that fails rolls every
/// sub-job back, an ADD FOREIGN KEY with no index for its columns is a
/// multi-schema change of its own (the index first), a key the rows break
/// rolls back, and AUTO_INCREMENT is rebased by its own job. Captured from
/// the Go oracle.
#[test]
fn multi_schema_alter_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1945)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let jobs = |session: &mut _| -> Vec<String> {
        displayed(rows(session, "admin show ddl jobs 1"))
            .into_iter()
            .map(|row| {
                assert_ne!(row[9], "NULL", "every sub-job ran: {row:?}");
                [3, 4, 7, 11, 12]
                    .map(|column| row[column].clone())
                    .join("|")
            })
            .collect()
    };
    rows(
        &mut session,
        "create table t (a int, b int, c int, key kc(c))",
    );
    rows(&mut session, "insert into t values (1,1,1),(2,2,2)");
    rows(
        &mut session,
        "alter table t add column d int, add index kb(b), modify column a bigint, drop index kc",
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|",
            "add column /* subjob */|public|0|done|txn-merge, DXF",
            "add index /* subjob */|public|2|done|",
            "modify column /* subjob */|public|0|done|",
            "drop index /* subjob */|none|0|done|max_node_count=3",
        ]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from t")),
        [["1", "1", "1", "NULL"], ["2", "2", "2", "NULL"]]
    );
    rows(&mut session, "update t set c=1");
    let error = session
        .node_conn()
        .execute_write("alter table t add column f int, add unique index uc(c)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1062, "Duplicate entry '1' for key 't.uc'")
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|rollback done|",
            "add column /* subjob */|none|0|rollback done|txn-merge, DXF",
            "add index /* subjob */|none|0|rollback done|max_node_count=3",
        ]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from t")),
        [["1", "1", "1", "NULL"], ["2", "2", "1", "NULL"]]
    );
    // Go `fillMultiSchemaInfo` has no CHECK sub-job.
    rows(&mut session, "set global tidb_enable_check_constraint = 1");
    let error = session
        .node_conn()
        .execute_write("alter table t add column j int, add constraint chk check (a > 0)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8200,
            "Unsupported multi schema change for add check constraint"
        )
    );
    rows(&mut session, "set global tidb_enable_check_constraint = 0");
    rows(&mut session, "alter table t add column (h int, i int)");
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|",
            "add column /* subjob */|public|0|done|",
            "add column /* subjob */|public|0|done|",
        ]
    );

    rows(&mut session, "create table p (id int primary key)");
    rows(&mut session, "create table ch (id int, pid int)");
    rows(
        &mut session,
        "alter table ch add foreign key (pid) references p(id)",
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|txn-merge, DXF",
            "add index /* subjob */|public|0|done|",
            "add foreign key /* subjob */|public|0|done|max_node_count=3",
        ]
    );
    rows(
        &mut session,
        "create table ch2 (id int, pid int, key k(pid))",
    );
    rows(&mut session, "insert into ch2 values (1, 5)");
    let error = session
        .node_conn()
        .execute_write("alter table ch2 add constraint fk2 foreign key (pid) references p(id)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1452,
            "Cannot add or update a child row: a foreign key constraint fails (`test`.`ch2`, \
             CONSTRAINT `fk2` FOREIGN KEY (`pid`) REFERENCES `p` (`id`))"
        )
    );
    assert_eq!(
        jobs(&mut session),
        ["add foreign key|none|0|rollback done|"]
    );
    rows(&mut session, "insert into p values (5)");
    rows(
        &mut session,
        "alter table ch2 add constraint fk2 foreign key (pid) references p(id)",
    );
    assert_eq!(jobs(&mut session), ["add foreign key|public|0|synced|"]);

    rows(
        &mut session,
        "create table ai (id int primary key auto_increment)",
    );
    rows(&mut session, "insert into ai values (10)");
    rows(&mut session, "alter table ai auto_increment = 100");
    assert_eq!(
        jobs(&mut session),
        ["rebase auto_increment ID|public|0|synced|"]
    );
    rows(
        &mut session,
        "alter table ai auto_increment = 5000000, comment 'x'",
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|",
            "rebase auto_increment ID /* subjob */|public|0|done|",
            "modify table comment /* subjob */|public|0|done|",
        ]
    );
    rows(&mut session, "insert into ai values ()");
    assert_eq!(
        displayed(rows(&mut session, "select max(id) from ai")),
        [["5000000"]]
    );
    rows(&mut session, "alter table ai force auto_increment = 3");
    rows(&mut session, "insert into ai values ()");
    assert_eq!(
        displayed(rows(&mut session, "select id from ai order by id")),
        [["3"], ["10"], ["5000000"]]
    );
}

/// MODIFY / CHANGE / RENAME COLUMN run as Go's persisted `ActionModifyColumn`
/// job through each of `onModifyColumn`'s courses: metadata only, a checked
/// narrowing (rolled back on a row that does not fit), a VARCHAR -> CHAR
/// precheck that falls back to a rewrite, and rewrites with their indexes
/// rebuilt (new index ids, the old entries and the merge's temporary index
/// left to the delete range). A non-strict rewrite reports its warning.
/// Captured from the Go oracle.
#[test]
fn modify_column_runs_as_a_persisted_job_like_go() {
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    let index = |id: i64, index: i64| tidb_tablecodec::table_key::encode_table_index_prefix(id, index);
    const TEMP: i64 = 0x7fff_0000_0000_0000;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1944)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 3, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    let kind = |job: &str| job.split_once('|').unwrap().1.to_owned();
    rows(
        &mut session,
        "create table t (a int, b varchar(10), c int, d varchar(10), key kc(c), key kd(d))",
    );
    rows(
        &mut session,
        "insert into t values (1, 'x', 1, 'p'), (2, 'yy ', 300, 'q '), (null, null, null, null)",
    );
    let t: i64 = displayed(rows(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name='t'",
    ))[0][0]
        .parse()
        .unwrap();
    for (sql, job) in [
        (
            "alter table t modify b varchar(20)",
            "modify column|public|0|synced",
        ),
        (
            "alter table t modify c bigint",
            "modify column|public|0|synced",
        ),
        (
            "alter table t change c c2 bigint",
            "modify column|public|0|synced",
        ),
        (
            "alter table t rename column c2 to c",
            "modify column|public|0|synced",
        ),
        (
            "alter table t modify a int first",
            "modify column|public|0|synced",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(kind(&last_job(&mut session)), job, "{sql}");
    }
    let error = session
        .node_conn()
        .execute_write("alter table t modify c tinyint")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'c', value is '300'")
    );
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|none|0|rollback done"
    );
    rows(&mut session, "alter table t modify c smallint");
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|public|0|synced"
    );
    let error = session
        .node_conn()
        .execute_write("alter table t modify a int not null")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'a' at row 1")
    );
    rows(&mut session, "update t set a = 3 where a is null");
    for (sql, job) in [
        (
            "alter table t modify a int not null",
            "modify column|public|0|synced",
        ),
        // VARCHAR -> CHAR over a trailing space: the precheck fails and the
        // rows are rewritten.
        (
            "alter table t modify b char(10)",
            "modify column|public|3|synced",
        ),
        (
            "alter table t modify d char(10)",
            "modify column|public|6|synced",
        ),
        (
            "alter table t modify c varchar(10)",
            "modify column|public|6|synced",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(kind(&last_job(&mut session)), job, "{sql}");
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "select key_name, index_id from information_schema.tidb_indexes where table_name='t' order by key_name"
        )),
        [["kc", "4"], ["kd", "3"]]
    );
    let columns: Vec<String> = displayed(rows(&mut session, "show columns from t"))
        .into_iter()
        .map(|row| format!("{} {}", row[0], row[1]))
        .collect();
    assert_eq!(
        columns,
        ["a int", "b char(10)", "c varchar(10)", "d char(10)"]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from t order by a")),
        [
            ["1", "x", "1", "p"],
            ["2", "yy", "300", "q"],
            ["3", "NULL", "NULL", "NULL"]
        ]
    );
    rows(&mut session, "admin check table t");

    rows(&mut session, "create table s (a varchar(10))");
    rows(&mut session, "insert into s values ('abc'), ('12')");
    let error = session
        .node_conn()
        .execute_write("alter table s modify a int")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1292, "Truncated incorrect DOUBLE value: 'abc'")
    );
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|none|0|rollback done"
    );
    rows(&mut session, "set sql_mode=''");
    rows(&mut session, "alter table s modify a int");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Warning", "1292", "Truncated incorrect DOUBLE value: 'abc'"]]
    );
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|public|2|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from s order by a")),
        [["0"], ["12"]]
    );
    rows(&mut session, "set sql_mode=default");
    rows(&mut session, "create table r (a int, b int)");
    for (sql, code, message) in [
        (
            "alter table s modify nosuch int",
            1054,
            "Unknown column 'nosuch' in 's'",
        ),
        (
            "alter table s change a a int after nosuch",
            1054,
            "Unknown column 'nosuch' in 'generated column function'",
        ),
        (
            "alter table r modify a int after a",
            1054,
            "Unknown column 'a' in 'r'",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    // Go queues the self-anchored MODIFY and rolls it back at the owner.
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|none|0|rollback done"
    );

    // The rewrites' old index entries and the merge's temporary indexes.
    let pending = "select count(*) from mysql.gc_delete_range";
    let started = std::time::Instant::now();
    while displayed(rows(&mut session, pending)) != [["0"]] {
        assert!(started.elapsed() < std::time::Duration::from_secs(30));
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let done: Vec<String> = displayed(rows(
        &mut session,
        "select element_id, end_key from mysql.gc_delete_range_done order by job_id, element_id",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    assert_eq!(
        done,
        [
            format!("1 {}", hex(&index(t, 3))),
            format!("2 {}", hex(&index(t, TEMP | 3 + 1))),
            format!("1 {}", hex(&index(t, 2))),
            format!("2 {}", hex(&index(t, TEMP | 4 + 1))),
        ]
    );
    // Go `updateColumnWorker.getRowRecord`: a NULL bound for a TIMESTAMP NOT
    // NULL column takes the current time, read in the job's time zone (Go
    // issue 56051; oracle: both answer 0).
    rows(&mut session, "create table tz (a int, b int)");
    rows(&mut session, "insert into tz values (null, null)");
    rows(&mut session, "alter table tz modify a timestamp not null");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select floor((unix_timestamp() - unix_timestamp(a)) / 2) from tz",
        )),
        [["0"]]
    );
    rows(&mut session, "set @@time_zone = 'Asia/Tokyo'");
    rows(&mut session, "alter table tz modify b timestamp not null");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select floor((unix_timestamp() - unix_timestamp(b)) / 2) from tz",
        )),
        [["0"]]
    );
}

/// DROP TABLE / VIEW / SEQUENCE / DATABASE run as Go's persisted jobs, one
/// per object, each through `onDropTableOrView` / `onDropSchema`; the notes
/// and refusals are the executor's. Captured from the Go oracle.
#[test]
fn drop_objects_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1940)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let jobs = |session: &mut _, count: usize| -> Vec<String> {
        displayed(rows(session, &format!("admin show ddl jobs {count}")))
            .into_iter()
            .map(|row| [3, 2, 4, 11].map(|column| row[column].clone()).join("|"))
            .collect()
    };
    let notes = |session: &mut _| -> Vec<String> {
        displayed(rows(session, "SHOW WARNINGS"))
            .into_iter()
            .map(|row| row.join("|"))
            .collect()
    };
    rows(&mut session, "create table t1 (a int)");
    rows(&mut session, "create table t2 (a int)");
    rows(&mut session, "drop table t1, t2");
    assert_eq!(
        jobs(&mut session, 2),
        ["drop table|t2|none|synced", "drop table|t1|none|synced"]
    );
    rows(&mut session, "drop table if exists t1, t3");
    assert_eq!(
        notes(&mut session),
        [
            "Note|1051|Unknown table 'test.t1'",
            "Note|1051|Unknown table 'test.t3'"
        ]
    );
    rows(&mut session, "create view v as select 1");
    rows(&mut session, "drop view v");
    assert_eq!(jobs(&mut session, 1), ["drop view|v|none|synced"]);
    rows(&mut session, "drop view if exists v");
    assert_eq!(notes(&mut session), ["Note|1051|Unknown table 'test.v'"]);
    rows(&mut session, "create sequence s");
    rows(&mut session, "drop sequence s");
    assert_eq!(jobs(&mut session, 1), ["drop sequence|s|none|synced"]);
    rows(&mut session, "drop sequence if exists s");
    assert_eq!(
        notes(&mut session),
        ["Note|4139|Unknown SEQUENCE: 'test.s'"]
    );
    rows(&mut session, "create database d");
    rows(&mut session, "drop database d");
    assert_eq!(jobs(&mut session, 1), ["drop schema||none|synced"]);
    rows(&mut session, "drop database if exists d");
    assert!(notes(&mut session).is_empty());
    rows(&mut session, "create table tv (a int)");
    rows(&mut session, "create view vv as select 1");
    for (sql, code, message) in [
        ("drop table t1", 1051, "Unknown table 'test.t1'"),
        (
            "drop database d",
            1008,
            "Can't drop database 'd'; database doesn't exist",
        ),
        ("drop view tv", 1347, "'test.tv' is not VIEW"),
        ("drop table vv", 1051, "Unknown table 'test.vv'"),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    // Foreign keys: a parent outside the statement's targets stays.
    rows(&mut session, "create table p (id int primary key)");
    rows(
        &mut session,
        "create table c (id int primary key, pid int, constraint fkc foreign key (pid) references p(id))",
    );
    let error = session
        .node_conn()
        .execute_write("drop table p")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3730,
            "Cannot drop table 'p' referenced by a foreign key constraint 'fkc' on table 'c'."
        )
    );
    rows(&mut session, "drop table p, c");
    rows(&mut session, "create database dp");
    rows(&mut session, "create table dp.pp (id int primary key)");
    rows(
        &mut session,
        "create table c2 (id int primary key, pid int, constraint fk2 foreign key (pid) references dp.pp(id))",
    );
    let error = session
        .node_conn()
        .execute_write("drop database dp")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3730,
            "Cannot drop table 'pp' referenced by a foreign key constraint 'fk2' on table 'c2'."
        )
    );
    rows(&mut session, "set foreign_key_checks = 0");
    rows(&mut session, "drop database dp");
}

/// TRUNCATE TABLE runs as Go's persisted job (`executor.TruncateTable`, the
/// owner's `onTruncateTable`): new table and partition ids, the allocators
/// restarted, the refusals the executor's. Captured from the Go oracle.
#[test]
fn truncate_table_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1941)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [3, 2, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    rows(
        &mut session,
        "create table t (a int auto_increment primary key, b int)",
    );
    rows(&mut session, "insert into t (b) values (1),(2)");
    rows(&mut session, "truncate table t");
    assert_eq!(last_job(&mut session), "truncate table|t|public|0|synced");
    rows(&mut session, "insert into t (b) values (3)");
    assert_eq!(
        displayed(rows(&mut session, "select * from t")),
        [["1", "3"]]
    );
    rows(&mut session, "create table p (id int primary key)");
    rows(
        &mut session,
        "create table c (id int primary key, pid int, constraint fkc foreign key (pid) references p(id))",
    );
    rows(&mut session, "create view v as select 1");
    for (sql, code, message) in [
        (
            "truncate table p",
            1701,
            "Cannot truncate a table referenced in a foreign key constraint (`test`.`c` CONSTRAINT `fkc`)",
        ),
        ("truncate table v", 1146, "Table 'test.v' doesn't exist"),
        ("truncate table nosuch", 1146, "Table 'test.nosuch' doesn't exist"),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
    }
    rows(&mut session, "truncate table c");
    rows(
        &mut session,
        "create table tp (a int) partition by hash(a) partitions 2",
    );
    rows(&mut session, "insert into tp values (1),(2)");
    rows(&mut session, "truncate table tp");
    assert_eq!(last_job(&mut session), "truncate table|tp|public|0|synced");
    assert_eq!(
        displayed(rows(&mut session, "select count(*) from tp")),
        [["0"]]
    );
}

/// Go `onRenameIndex` renames an expression index's hidden columns with it
/// (`renameHiddenColumns`, Issue 51431), so a later index may take the old
/// name. Captured from the Go oracle.
#[test]
fn renaming_an_expression_index_frees_its_name_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1942)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table t(j json)");
    rows(
        &mut session,
        "alter table t add index idx1((cast(j as char(10) array)))",
    );
    rows(&mut session, "alter table t rename index idx1 to idx2");
    rows(
        &mut session,
        "alter table t add index idx1((cast(j as char(10) array)))",
    );
    assert_eq!(
        displayed(rows(&mut session, "show create table t")).remove(0)[1],
        "CREATE TABLE `t` (\n  `j` json DEFAULT NULL,\n  KEY `idx2` ((cast(`j` as char(10) array))),\n  KEY `idx1` ((cast(`j` as char(10) array)))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    rows(&mut session, "insert into t values ('[\"a\",\"b\"]')");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select count(*) from t where 'a' member of (j)"
        )),
        [["1"]]
    );
}


/// The `operation` column of `TRACE <sql>`: the span tree Go's `dfsTree`
/// prints, one line per span, region IDs (the embedded store's own) as `N`.
fn trace_tree(session: &mut Session, sql: &str) -> Vec<String> {
    displayed(rows(session, &format!("TRACE {sql}")))
        .into_iter()
        .map(|row| match row[0].split_once("region ID: ") {
            Some((head, tail)) => {
                let digits = tail.chars().take_while(char::is_ascii_digit).count();
                format!("{head}region ID: N{}", &tail[digits..])
            }
            None => row[0].clone(),
        })
        .collect()
}

/// `TRACE` records the spans the statement's run opens -- Go's regions in
/// the session, planner, executors (one `exec.Next` span per call) and
/// coprocessor client, and the client-go spans client-rust reports
/// (`tikvSnapshot.get`, `regionRequest.SendReqCtx`, `tikvTxn.Commit`,
/// `twoPhaseCommitter.*`, ...) -- so the trees are Go's for the same
/// statements. The embedded store is client-go's unistore `RPCClient`,
/// which sends without `rpcClient.SendRequest` and reports no execution
/// details, so neither that span nor the `tikv.*` nodes appear, as with Go
/// over unistore.
#[test]
fn trace_records_the_spans_each_statement_opens() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(154)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.tr (id INT PRIMARY KEY, k INT, v INT, KEY ik (k))",
    );
    rows(
        &mut session,
        "INSERT INTO test.tr VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (5, 5, 6)",
    );
    let compile = [
        "  ├─session.ExecuteStmt",
        "  │ ├─executor.Compile",
        "  │ │ ├─planner.Preprocess",
        "  │ │ └─planner.Optimize",
    ];
    // The coprocessor read: `distsql.Select` under `session.runStmt` (the
    // reader's Open), its send -- on a backoffer whose ctx is the select's
    // span -- outliving it, and the record set drained under the trace span
    // after `ExecuteStmt` returned: HashAgg reads its child to the end in
    // its first call.
    let mut expected = vec!["trace"];
    expected.extend(compile);
    expected.extend([
        "  │ └─session.runStmt",
        "  │   └─distsql.Select",
        "  │     ├─copr.buildCopTasks",
        "  │     └─regionRequest.SendReqCtx",
        "  ├─*aggregate.HashAggExec.Next",
        "  │ ├─*executor.IndexReaderExecutor.Next",
        "  │ └─*executor.IndexReaderExecutor.Next",
        "  └─*aggregate.HashAggExec.Next",
    ]);
    assert_eq!(
        trace_tree(&mut session, "SELECT COUNT(*) FROM test.tr"),
        expected
    );
    // client-go `KVSnapshot.Get` opens `tikvSnapshot.get` and discards its
    // ctx, so the send is its sibling; the BatchGet send is the snapshot's
    // own `SendReqCtx`.
    for (sql, executor, client) in [
        (
            "SELECT * FROM test.tr WHERE id = 5",
            "*executor.PointGetExecutor.Next",
            &["  │ ├─tikvSnapshot.get", "  │ └─regionRequest.SendReqCtx"][..],
        ),
        (
            "SELECT * FROM test.tr WHERE id IN (1, 2, 3)",
            "*executor.BatchPointGetExec.Next",
            &["  │ └─regionRequest.SendReqCtx"][..],
        ),
    ] {
        let data_call = format!("  ├─{executor}");
        let end_call = format!("  └─{executor}");
        let mut expected = vec!["trace"];
        expected.extend(compile);
        expected.extend(["  │ └─session.runStmt", &data_call]);
        expected.extend(client);
        expected.push(&end_call);
        assert_eq!(trace_tree(&mut session, sql), expected, "{sql}");
    }
    // An autocommit UPDATE: the executor runs under `handleNoDelayExecutor`
    // and the commit under `runStmt`; an unchanged row commits nothing.
    let update = [
        "trace",
        "  └─session.ExecuteStmt",
        "    ├─executor.Compile",
        "    │ ├─planner.Preprocess",
        "    │ └─planner.Optimize",
        "    └─session.runStmt",
        "      ├─executor.handleNoDelayExecutor",
        "      │ └─*executor.UpdateExec.Next",
        "      │   ├─*executor.PointGetExecutor.Next",
        "      │   │ ├─tikvSnapshot.get",
        "      │   │ └─regionRequest.SendReqCtx",
        "      │   ├─executor.updateRecord",
        "      │   └─*executor.PointGetExecutor.Next",
        "      └─session.CommitTxn",
        "        └─session.doCommitWithRetry",
    ];
    assert_eq!(
        trace_tree(&mut session, "UPDATE test.tr SET v = v WHERE id = 5"),
        update
    );
    // client-go's commit: `tikvTxn.Commit` over the prewrite, the commit-ts
    // fetch and the commit, each phase's send under its own span.
    let mut expected = update.to_vec();
    expected.extend([
        "          └─tikvTxn.Commit",
        "            ├─twoPhaseCommitter.prewriteMutations",
        "            │ └─regionRequest.SendReqCtx",
        "            ├─TiKVStore.getTimestampWithRetry",
        "            └─twoPhaseCommitter.commitMutations",
        "              └─regionRequest.SendReqCtx",
    ]);
    assert_eq!(
        trace_tree(&mut session, "UPDATE test.tr SET v = v + 1 WHERE id = 5"),
        expected
    );
    // The log form carries client-go's span events and tags: the sender's
    // `send <cmd> request to region` event and the commit's `commitTs` tag
    // on `tikvTxn.Commit`'s events.
    let logged = displayed(rows(
        &mut session,
        "TRACE FORMAT = 'log' UPDATE test.tr SET v = v + 1 WHERE id = 5",
    ));
    let event = |prefix: &str| {
        logged
            .iter()
            .find(|row| row[1].starts_with(prefix))
            .unwrap_or_else(|| panic!("no {prefix:?} event in {logged:#?}"))
            .clone()
    };
    let send = event("send Commit request to region ");
    assert_eq!(
        (send[2].as_str(), send[3].as_str()),
        ("", "regionRequest.SendReqCtx")
    );
    let commit_ts = event("start get commit ts");
    assert!(commit_ts[2].starts_with("map[commitTs:"), "{commit_ts:?}");
    assert_eq!(commit_ts[3], "tikvTxn.Commit");
}

/// The regions Go's executors and session open around the client spans: a
/// table reader's `TableReaderExecutor.Open` and the `table scan` event of
/// its every `Next`, an INSERT's event and its `table.AddRecord` /
/// `index.Create` per row (siblings: the index writes take the executor's
/// ctx), and COMMIT / ROLLBACK compiled and run as any statement --
/// `SimpleExec` under `handleNoDelayExecutor`, then `finishStmt`'s
/// `session.CommitTxn`, whose `doCommitWithRetry` runs only over a valid
/// transaction.
#[test]
fn trace_records_go_executor_and_transaction_regions() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(155)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.tr (id INT PRIMARY KEY, k INT, v INT, KEY ik (k))",
    );
    rows(
        &mut session,
        "INSERT INTO test.tr VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (5, 5, 6)",
    );
    let compile = [
        "trace",
        "  └─session.ExecuteStmt",
        "    ├─executor.Compile",
        "    │ ├─planner.Preprocess",
        "    │ └─planner.Optimize",
        "    └─session.runStmt",
    ];
    let tree = trace_tree(
        &mut session,
        "SELECT * FROM test.tr IGNORE INDEX (ik) WHERE v > 1",
    );
    assert_eq!(
        tree[5..9],
        [
            "  │ └─session.runStmt",
            "  │   └─TableReaderExecutor.Open",
            "  │     └─distsql.Select",
            "  │       ├─copr.buildCopTasks",
        ],
        "{tree:#?}"
    );
    let logged = displayed(rows(
        &mut session,
        "TRACE FORMAT = 'log' SELECT * FROM test.tr IGNORE INDEX (ik) WHERE v > 1",
    ));
    let scans = logged
        .iter()
        .filter(|row| row[1].starts_with("table scan "))
        .map(|row| (row[1].as_str(), row[3].as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        scans,
        [
            (
                "table scan table: tr, range: [[-inf,+inf]]",
                "*executor.TableReaderExecutor.Next"
            ),
            (
                "table scan table: tr, range: [[-inf,+inf]]",
                "*executor.TableReaderExecutor.Next"
            ),
        ]
    );

    let tree = trace_tree(
        &mut session,
        "INSERT INTO test.tr VALUES (7, 7, 7), (8, 8, 8)",
    );
    let mut expected = compile.to_vec();
    expected.extend([
        "      ├─executor.handleNoDelayExecutor",
        "      │ └─*executor.InsertExec.Next",
        "      │   ├─table.AddRecord",
        "      │   ├─index.Create",
        "      │   ├─table.AddRecord",
        "      │   └─index.Create",
        "      └─session.CommitTxn",
    ]);
    assert_eq!(tree[..expected.len()], expected[..], "{tree:#?}");
    let logged = displayed(rows(
        &mut session,
        "TRACE FORMAT = 'log' INSERT INTO test.tr VALUES (9, 9, 9)",
    ));
    assert!(
        logged
            .iter()
            .any(|row| row[1] == "insert 1 rows into table `tr`"
                && row[3] == "*executor.InsertExec.Next"),
        "{logged:#?}"
    );

    let simple = [
        "      ├─executor.handleNoDelayExecutor",
        "      │ └─*executor.SimpleExec.Next",
    ];
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    session
        .node_conn()
        .execute_write("UPDATE test.tr SET v = v + 1 WHERE id = 5")
        .unwrap();
    let mut expected = compile.to_vec();
    expected.extend(simple);
    expected.extend([
        "      └─session.CommitTxn",
        "        └─session.doCommitWithRetry",
        "          └─tikvTxn.Commit",
        "            ├─twoPhaseCommitter.prewriteMutations",
        "            │ └─regionRequest.SendReqCtx",
        "            ├─TiKVStore.getTimestampWithRetry",
        "            └─twoPhaseCommitter.commitMutations",
        "              └─regionRequest.SendReqCtx",
    ]);
    assert_eq!(trace_tree(&mut session, "COMMIT"), expected);
    assert!(!session.in_transaction());

    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM test.tr WHERE id = 1");
    let mut expected = compile.to_vec();
    expected.extend(simple);
    expected.push("      └─session.CommitTxn");
    assert_eq!(trace_tree(&mut session, "ROLLBACK"), expected);
    assert!(!session.in_transaction());
}

/// [`trace_tree`] without the client's spans (`regionRequest.SendReqCtx`,
/// region loads, snapshot reads, commits), whose presence depends on the
/// region cache and the store rather than on the TiDB-side regions.
fn tidb_trace_tree(session: &mut Session, sql: &str) -> Vec<String> {
    const CLIENT: [&str; 7] = [
        "regionRequest.SendReqCtx",
        "loadRegion",
        "tikvSnapshot.get",
        "tikvTxn.Commit",
        "twoPhaseCommitter.",
        "TiKVStore.getTimestampWithRetry",
        "copr.buildCopTasks",
    ];
    trace_tree(session, sql)
        .into_iter()
        .filter(|line| !CLIENT.iter().any(|client| line.contains(client)))
        .collect()
}

/// The rest of Go's statement-path regions and events: a subquery the
/// planner evaluates (`executor.EvalSubQuery` under `executor.Compile`),
/// a prepared point get in autocommit (`ExecStmt.PointGet` in place of
/// `session.runStmt`, the prepared statement planned inside the EXECUTE's
/// `planner.Optimize`), a failed autocommit statement's
/// `session.RollbackTxn`, the duplicate-check prefetches of INSERT IGNORE,
/// ON DUPLICATE KEY UPDATE and REPLACE (`tikvTxn.BatchGet`), the
/// auto-increment allocations (`alloc.alloc4Signed`, and
/// `table.AllocAutoIncrementValue` per selected row), one insert event per
/// source chunk of an INSERT ... SELECT, and a union scan's
/// `buildMemTableReader`.
#[test]
fn trace_records_go_statement_path_regions() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(156)).unwrap();
    for sql in [
        "CREATE TABLE test.tr (id INT PRIMARY KEY, k INT, v INT, KEY ik (k))",
        "INSERT INTO test.tr VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (5, 5, 6)",
        "CREATE TABLE test.u (id INT PRIMARY KEY, k INT, UNIQUE KEY uk (k))",
        "INSERT INTO test.u VALUES (1, 1), (2, 2)",
        "CREATE TABLE test.ai (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
        "CREATE TABLE test.big (id INT PRIMARY KEY, v INT)",
        "SET @@cte_max_recursion_depth = 5000",
        "INSERT INTO test.big SELECT seq, seq FROM (WITH RECURSIVE c(seq) AS \
         (SELECT 1 UNION ALL SELECT seq + 1 FROM c WHERE seq < 3000) SELECT seq FROM c) s",
        "CREATE TABLE test.dst (id INT PRIMARY KEY, v INT)",
        "PREPARE s FROM 'SELECT * FROM test.tr WHERE id = ?'",
        "SET @a = 5",
    ] {
        rows(&mut session, sql);
    }
    let compile = [
        "trace",
        "  └─session.ExecuteStmt",
        "    ├─executor.Compile",
        "    │ ├─planner.Preprocess",
        "    │ └─planner.Optimize",
        "    └─session.runStmt",
        "      ├─executor.handleNoDelayExecutor",
    ];
    let with = |tail: &[&'static str]| {
        let mut expected = compile.to_vec();
        expected.extend_from_slice(tail);
        expected
    };

    assert_eq!(
        tidb_trace_tree(
            &mut session,
            "SELECT * FROM test.tr WHERE v = (SELECT MAX(k) FROM test.u)"
        ),
        [
            "trace",
            "  ├─session.ExecuteStmt",
            "  │ ├─executor.Compile",
            "  │ │ ├─planner.Preprocess",
            "  │ │ ├─planner.Optimize",
            "  │ │ └─executor.EvalSubQuery",
            "  │ │   ├─distsql.Select",
            "  │ │   └─*executor.MaxOneRowExec.Next",
            "  │ │     ├─*aggregate.StreamAggExec.Next",
            "  │ │     │ ├─*executor.LimitExec.Next",
            "  │ │     │ │ └─*executor.IndexReaderExecutor.Next",
            "  │ │     │ └─*executor.LimitExec.Next",
            "  │ │     └─*aggregate.StreamAggExec.Next",
            "  │ └─session.runStmt",
            "  │   └─TableReaderExecutor.Open",
            "  │     └─distsql.Select",
            "  ├─*executor.TableReaderExecutor.Next",
            "  └─*executor.TableReaderExecutor.Next",
        ]
    );
    assert_eq!(
        tidb_trace_tree(&mut session, "EXECUTE s USING @a"),
        [
            "trace",
            "  ├─session.ExecuteStmt",
            "  │ ├─executor.Compile",
            "  │ │ ├─planner.Preprocess",
            "  │ │ └─planner.Optimize",
            "  │ └─ExecStmt.PointGet",
            "  ├─*executor.PointGetExecutor.Next",
            "  └─*executor.PointGetExecutor.Next",
        ]
    );
    assert_eq!(
        tidb_trace_tree(&mut session, "INSERT INTO test.tr VALUES (300, 'abc', 1)"),
        with(&[
            "      │ └─*executor.InsertExec.Next",
            "      └─session.RollbackTxn",
        ])
    );
    assert_eq!(
        tidb_trace_tree(
            &mut session,
            "INSERT IGNORE INTO test.u VALUES (1, 9), (3, 3)"
        ),
        with(&[
            "      │ └─*executor.InsertExec.Next",
            "      │   ├─InsertValues.batchCheckAndInsert",
            "      │   ├─prefetchUniqueIndices",
            "      │   │ └─tikvTxn.BatchGet",
            "      │   ├─table.AddRecord",
            "      │   └─index.Create",
            "      └─session.CommitTxn",
            "        └─session.doCommitWithRetry",
        ])
    );
    let prefetch_data_cache = [
        "      │   ├─prefetchDataCache",
        "      │   ├─prefetchUniqueIndices",
        "      │   │ └─tikvTxn.BatchGet",
        "      │   ├─prefetchConflictedOldRows",
        "      │   │ └─tikvTxn.BatchGet",
    ];
    let mut tail = vec!["      │ └─*executor.InsertExec.Next"];
    tail.extend(prefetch_data_cache);
    tail.extend([
        "      │   └─executor.updateRecord",
        "      └─session.CommitTxn",
        "        └─session.doCommitWithRetry",
    ]);
    assert_eq!(
        tidb_trace_tree(
            &mut session,
            "INSERT INTO test.u VALUES (1, 1) ON DUPLICATE KEY UPDATE k = 1"
        ),
        with(&tail)
    );
    assert_eq!(
        tidb_trace_tree(&mut session, "REPLACE INTO test.u VALUES (2, 2)"),
        with(&[
            "      │ └─*executor.ReplaceExec.Next",
            "      │   ├─prefetchDataCache",
            "      │   ├─prefetchUniqueIndices",
            "      │   │ └─tikvTxn.BatchGet",
            "      │   └─prefetchConflictedOldRows",
            "      │     └─tikvTxn.BatchGet",
            "      └─session.CommitTxn",
            "        └─session.doCommitWithRetry",
        ])
    );
    assert_eq!(
        tidb_trace_tree(&mut session, "INSERT INTO test.ai (v) VALUES (1), (2)"),
        with(&[
            "      │ └─*executor.InsertExec.Next",
            "      │   ├─alloc.alloc4Signed",
            "      │   ├─table.AddRecord",
            "      │   └─table.AddRecord",
            "      └─session.CommitTxn",
            "        └─session.doCommitWithRetry",
        ])
    );
    let tree = tidb_trace_tree(
        &mut session,
        "INSERT INTO test.ai (v) SELECT v FROM test.tr WHERE id < 4",
    );
    let allocations = tree
        .iter()
        .filter(|line| line.ends_with("table.AllocAutoIncrementValue"))
        .count();
    assert_eq!(allocations, 3, "{tree:#?}");

    let logged = displayed(rows(
        &mut session,
        "TRACE FORMAT = 'log' INSERT INTO test.dst SELECT * FROM test.big",
    ));
    let events = logged
        .iter()
        .filter(|row| row[1].starts_with("insert "))
        .map(|row| (row[1].as_str(), row[3].as_str()))
        .collect::<Vec<_>>();
    let event = |text| (text, "*executor.InsertExec.Next");
    assert_eq!(
        events,
        [
            event("insert 1024 rows into table `dst`"),
            event("insert 1024 rows into table `dst`"),
            event("insert 952 rows into table `dst`"),
        ]
    );

    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.tr VALUES (7, 7, 7)")
        .unwrap();
    let tree = tidb_trace_tree(
        &mut session,
        "SELECT * FROM test.tr IGNORE INDEX (ik) WHERE v > 0",
    );
    assert_eq!(
        tree[5..10],
        [
            "  │ └─session.runStmt",
            "  │   └─UnionScanExec.Open",
            "  │     ├─TableReaderExecutor.Open",
            "  │     │ └─distsql.Select",
            "  │     └─buildMemTableReader",
        ],
        "{tree:#?}"
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// The span names of a `TRACE` tree, each with its depth, the tree glyphs
/// stripped.
fn trace_spans(tree: &[String]) -> Vec<(usize, String)> {
    tree.iter()
        .map(|line| {
            let name = line.trim_start_matches([' ', '│', '├', '└', '─']);
            (line.chars().count() - name.chars().count(), name.to_owned())
        })
        .collect()
}

/// What a statement's `UpdateExec`/`DeleteExec`/locking read does with its
/// locks, as the markers of its trace in order: every `SelectLockExec.Next`,
/// every `updateRecord`, every point `Next`, and the lock request, the
/// `regionRequest.SendReqCtx` whose parent is a `SelectLockExec.Next` or
/// `PointGetExecutor.Next` span (Go's `doLockKeys` / `getAndLock`).
fn lock_markers(tree: &[String]) -> Vec<String> {
    let spans = trace_spans(tree);
    let mut markers = Vec::new();
    for (index, (depth, name)) in spans.iter().enumerate() {
        let parent = spans[..index]
            .iter()
            .rev()
            .find(|(parent_depth, _)| parent_depth < depth)
            .map(|(_, name)| name.as_str());
        if name == "*executor.SelectLockExec.Next"
            || name == "executor.updateRecord"
            || name == "*executor.PointGetExecutor.Next"
        {
            markers.push(name.clone());
        } else if name.starts_with("regionRequest.SendReqCtx")
            && matches!(
                parent,
                Some("*executor.SelectLockExec.Next" | "*executor.PointGetExecutor.Next")
            )
        {
            markers.push("lock request".to_owned());
        }
    }
    markers
}

/// Go plans a `SelectLock` over the source of a single-table pessimistic
/// UPDATE or DELETE (`buildUpdate` / `buildDelete`, `buildSelectLock`) and
/// over a `SELECT ... FOR UPDATE`, and `SelectLockExec.Next` locks the rows it
/// read when its child is drained: AFTER the chunk's `updateRecord`s, in the
/// `Next` call that returns no rows, not at the end of the statement. Each
/// tree was captured with `TRACE` from the Go server on TiKV in a
/// `BEGIN PESSIMISTIC`: the lock request sits under the second
/// `SelectLockExec.Next`, after every `updateRecord`; a point update locks in
/// its first `PointGetExecutor.Next` and reads EOF after its `updateRecord`.
#[test]
fn a_non_point_pessimistic_write_locks_inside_select_lock_next() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(2001))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.sllock (id int primary key, v int, w int, key iv(v))",
    );
    rows(
        &mut session,
        "INSERT INTO test.sllock VALUES (1, 2, 0), (2, 2, 0), (3, 5, 0), (4, 2, 0)",
    );
    let select = "*executor.SelectLockExec.Next";
    let update = "executor.updateRecord";
    let point = "*executor.PointGetExecutor.Next";
    let lock = "lock request";
    for (sql, expected) in [
        // Three rows changed.
        (
            "UPDATE test.sllock SET w = 1 WHERE v = 2",
            vec![select, update, update, update, select, lock],
        ),
        // Three rows matched and left as they were: the lock is still the
        // statement's `SelectLockExec` one, and nothing is left to lock.
        (
            "UPDATE test.sllock SET w = 0 WHERE v = 2",
            vec![select, update, update, update, select, lock],
        ),
        // A LIMIT sits above the lock only where Go cannot push it under.
        (
            "UPDATE test.sllock SET w = 1 WHERE v = 2 ORDER BY id LIMIT 2",
            vec![select, update, update, select, lock],
        ),
        ("UPDATE test.sllock SET w = 1 WHERE v = 99", vec![select]),
        ("DELETE FROM test.sllock WHERE v = 2", vec![select, select, lock]),
        (
            "SELECT * FROM test.sllock WHERE v = 2 FOR UPDATE",
            vec![select, select, lock],
        ),
        (
            "UPDATE test.sllock SET w = 1 WHERE id = 1",
            vec![point, lock, update, point],
        ),
    ] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        let tree = trace_tree(&mut session, sql);
        session
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(lock_markers(&tree), expected, "{sql}\n{tree:#?}");
    }
}

/// The rows a single-table pessimistic UPDATE, DELETE or locking read holds
/// when its transaction stays open are the rows `SelectLockExec` saw: below
/// the LIMIT where Go pushes it under the lock, every matched row of an
/// UPDATE that changed nothing, and none when nothing matched. Each case was
/// captured live from the Go server on TiKV, probing every id with
/// `FOR UPDATE NOWAIT` from a second transaction.
#[test]
fn non_point_pessimistic_statements_hold_the_rows_go_locks() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut setup = factory
        .open_session(session_context(2002))
        .expect("session opens");
    rows(
        &mut setup,
        "CREATE TABLE test.slheld (id int primary key, v int, w int, key iv(v))",
    );
    rows(
        &mut setup,
        "INSERT INTO test.slheld VALUES (1, 2, 0), (2, 2, 0), (3, 5, 0), (4, 2, 0)",
    );
    let mut writer = factory
        .open_session(session_context(2003))
        .expect("session opens");
    let mut contender = factory
        .open_session(session_context(2004))
        .expect("session opens");
    for (statement, held) in [
        (
            "UPDATE test.slheld SET w = 1 WHERE v = 2 ORDER BY id LIMIT 1",
            vec![1],
        ),
        ("UPDATE test.slheld SET w = 0 WHERE v = 2", vec![1, 2, 4]),
        (
            "DELETE FROM test.slheld WHERE v = 2 ORDER BY id DESC LIMIT 2",
            vec![2, 4],
        ),
        (
            "SELECT * FROM test.slheld WHERE v = 2 ORDER BY id LIMIT 1 FOR UPDATE",
            vec![1],
        ),
        ("UPDATE test.slheld SET w = 1 WHERE v = 99", vec![]),
    ] {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        if statement.starts_with("SELECT") {
            writer
                .node_conn()
                .execute(statement)
                .expect("statement runs");
        } else {
            writer
                .node_conn()
                .execute_write(statement)
                .expect("statement runs");
        }
        let mut probed = Vec::new();
        for id in 1..=4 {
            contender
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin contender");
            let refused = contender
                .node_conn()
                .execute(&format!(
                    "SELECT id FROM test.slheld WHERE id = {id} FOR UPDATE NOWAIT"
                ))
                .err();
            if let Some(error) = refused {
                assert_eq!(error.code, 3572, "{statement} id {id}: {}", error.message);
                probed.push(id);
            }
            contender
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("rollback");
        }
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(probed, held, "{statement}");
    }
}

/// Go's lock comes after the chunk's `updateRecord`s, so a row error preempts
/// a lock wait: with another transaction holding a matched row, the UPDATE
/// that violates a unique key or fails a cast reports that error at once,
/// and only the one that writes cleanly waits and times out with 1205.
/// Captured live from the Go server on TiKV.
#[test]
fn a_row_error_in_a_pessimistic_update_preempts_its_lock_wait() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(2005))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.slerr (id int primary key, uk int, w int, unique key uq(uk), key iw(w))",
    );
    rows(
        &mut holder,
        "INSERT INTO test.slerr VALUES (1, 10, 0), (2, 20, 0), (3, 30, 0)",
    );
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.slerr WHERE id = 2 FOR UPDATE",
    );
    let mut contender = factory
        .open_session(session_context(2006))
        .expect("session opens");
    contender
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (statement, code) in [
        ("UPDATE test.slerr SET uk = 10 WHERE id > 1", 1062),
        ("UPDATE test.slerr SET w = 'x' WHERE id > 1", 1292),
        ("UPDATE test.slerr SET w = 1 WHERE id > 1", 1205),
    ] {
        contender
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin contender");
        let error = match contender.node_conn().execute_write(statement) {
            Err(error) => error,
            Ok(_) => panic!("{statement} must fail"),
        };
        contender
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(error.code, code, "{statement}: {}", error.message);
    }
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
}

/// A non-point UPDATE that waits on a row and finds it changed when the lock
/// is granted re-runs at the new `for_update_ts` (Go
/// `handlePessimisticLockError` -> `OnStmtRetry`): `SET w = w + 1` computes
/// from the committed 100, not the 0 it first read. Captured live from the Go
/// server on TiKV: the rows end 1, 101, 1 and the statement affects 3.
#[test]
fn a_non_point_pessimistic_update_retries_after_a_conflict_found_by_its_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(2007))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.slretry (id int primary key, v int, w int, key iv(v))",
    );
    rows(
        &mut holder,
        "INSERT INTO test.slretry VALUES (1, 2, 0), (2, 2, 0), (3, 5, 0), (4, 2, 0)",
    );
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.slretry WHERE id = 2 FOR UPDATE",
    );
    let affected = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut contender = factory
                .open_session(session_context(2008))
                .expect("session opens");
            contender
                .node_conn()
                .execute_write("SET SESSION innodb_lock_wait_timeout = 10")
                .expect("set lock wait");
            contender
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin contender");
            let outcome = contender
                .node_conn()
                .execute_write("UPDATE test.slretry SET w = w + 1 WHERE v = 2")
                .expect("the update waits for the holder, then retries")
                .expect("write result");
            contender
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
            outcome.affected_rows
        });
        std::thread::sleep(std::time::Duration::from_millis(700));
        holder
            .node_conn()
            .execute_write("UPDATE test.slretry SET w = 100 WHERE id = 2")
            .expect("holder writes");
        holder
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit holder");
        contender.join().expect("contender thread")
    });
    assert_eq!(affected, 3);
    assert_eq!(
        displayed(rows(&mut holder, "SELECT id, w FROM test.slretry ORDER BY id")),
        [["1", "1"], ["2", "101"], ["3", "0"], ["4", "1"]]
    );
}

/// A conjunct the cop predicate description cannot express must never change
/// an answer. Go pushes down exactly the conditions `CanExprsPushDown` admits
/// and the coprocessor Selection applies all of them; a condition this port's
/// second description IR (`ScanPredicate`) cannot hold used to be dropped from
/// the request while the source believed the Selection carried it. Every shape
/// below is compared with the same predicate evaluated as a plain expression
/// over the same rows.
#[test]
fn a_conjunct_the_cop_description_cannot_express_never_changes_an_answer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(61))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.dropped (id int primary key, a int, b int, c int, \
         s varchar(10), KEY iab (a, b), KEY iabc (a, b, c))",
    );
    rows(
        &mut session,
        "INSERT INTO test.dropped VALUES \
         (1, 1, 0, 5, 'x'), (2, 1, 1, 6, 'y'), (3, 2, 0, 7, 'z'), (4, 2, 1, NULL, NULL), \
         (5, 3, NULL, 9, 'w'), (6, 3, 2, 1, ''), (7, NULL, 1, 2, 'v'), (8, 4, 4, 4, 'u'), \
         (9, 5, 0, 0, 'x'), (10, 5, 7, 3, 'q')",
    );
    const PREDICATES: &[&str] = &[
        // A bare column and its negation: no comparison to describe.
        "b",
        "NOT b",
        "a AND b",
        "a OR b",
        // Arithmetic and conditional shapes.
        "a + b > 3",
        "b * 2 = c",
        "CASE WHEN b > 0 THEN a > 1 ELSE FALSE END",
        "COALESCE(b, 0) > 0",
        "IF(b > 0, a, c) > 2",
        "a IN (b, 1)",
        "b IS TRUE",
        "(b IS NOT TRUE)",
        "b IS FALSE",
        "a - 1 = 0",
        "a DIV 2 = 1",
        "LENGTH(s) > 2",
        // A column against a column and a function of a column.
        "a = b",
        "a < c",
        "ABS(a - b) = 1",
        "s > 'u' AND b",
        // Mixed: one describable conjunct beside one that is not.
        "a >= 2 AND b",
        "a >= 2 AND a + b > 3",
        "a < 5 AND COALESCE(b, 0) > 0",
    ];
    for predicate in PREDICATES {
        // The reference keeps every row's verdict, evaluated by the root
        // projection over the very same rows.
        let verdicts = displayed(rows(
            &mut session,
            &format!(
                "SELECT id, a, b, c, ({predicate}) IS TRUE FROM test.dropped \
                 USE INDEX() ORDER BY id"
            ),
        ));
        let kept: Vec<&Vec<String>> = verdicts.iter().filter(|row| row[4] == "1").collect();

        // 1. A pushed partial aggregate.
        let pushed_count = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.dropped WHERE {predicate}"),
        ));
        assert_eq!(
            pushed_count,
            vec![vec![kept.len().to_string()]],
            "`{predicate}`: count(*)"
        );

        // 2. A covering index read, the filter riding the index Selection.
        let covering = displayed(rows(
            &mut session,
            &format!(
                "SELECT a, b, c FROM test.dropped USE INDEX (iabc) WHERE {predicate} \
                 ORDER BY id"
            ),
        ));
        let mut expected: Vec<Vec<String>> = kept
            .iter()
            .map(|row| vec![row[1].clone(), row[2].clone(), row[3].clone()])
            .collect();
        let mut got = covering;
        expected.sort();
        got.sort();
        assert_eq!(got, expected, "`{predicate}`: the covering index read");

        // 3. A pushed limit counts rows after the Selection.
        let limited = displayed(rows(
            &mut session,
            &format!("SELECT id FROM test.dropped WHERE {predicate} ORDER BY id LIMIT 2"),
        ));
        let expected_limit: Vec<Vec<String>> = kept
            .iter()
            .take(2)
            .map(|row| vec![row[0].clone()])
            .collect();
        assert_eq!(limited, expected_limit, "`{predicate}`: the pushed limit");

        // 4. A pushed TopN.
        let topn = displayed(rows(
            &mut session,
            &format!("SELECT id FROM test.dropped WHERE {predicate} ORDER BY c, id LIMIT 3"),
        ));
        let mut by_c: Vec<&&Vec<String>> = kept.iter().collect();
        by_c.sort_by_key(|row| {
            (
                row[3].parse::<i64>().map_or((0, 0), |c| (1, c)),
                row[0].parse::<i64>().unwrap(),
            )
        });
        let expected_topn: Vec<Vec<String>> = by_c
            .iter()
            .take(3)
            .map(|row| vec![row[0].clone()])
            .collect();
        assert_eq!(topn, expected_topn, "`{predicate}`: the pushed TopN");
    }
}

/// Go `kv.Storage.GetClient()`: a storage session's tables reach the node's
/// coprocessor client, which `ADMIN CHECKSUM TABLE` sends through. A session
/// opened without it answers unistore's in-process stub instead of
/// checksumming the regions.
#[test]
fn a_storage_sessions_tables_carry_the_nodes_coprocessor_client() {
    let (stack, _users) = cop_backed_stack();
    let mut opened = stack
        .factory
        .open_storage_session(141)
        .expect("storage session");
    let has_client = opened
        .with_committed_catalog_mut(|catalog| match catalog.table_in("mysql", "user") {
            Some(tidb_infoschema::TableEntry::Kv(table)) => Ok(table.store().client().is_some()),
            _ => Ok(false),
        })
        .expect("catalog");
    assert!(has_client);
}

fn partition_id_map(
    catalog: &tidb_infoschema::cluster_catalog::ClusterCatalog,
    schema: &str,
    table: &str,
) -> Option<(i64, Vec<(String, i64)>)> {
    let (_, table) = catalog.find_table(schema, table)?;
    let partition = table.partition.as_ref()?.read();
    Some((
        table.id,
        partition
            .definitions
            .snapshot()
            .into_iter()
            .map(|definition| (definition.name.lowercase().to_owned(), definition.id))
            .collect(),
    ))
}
