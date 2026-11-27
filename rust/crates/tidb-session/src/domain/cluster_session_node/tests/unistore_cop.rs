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
    pub_scheme_change_to_store, DdlNotifier, Handler, SchemaChangeEvent, SessionPool, Store,
    TEST_HANDLER_ID,
};
use tidb_goruntime::go_runtime::GoSharedSlice;
use tidb_model::partition::{PartitionDefinition, PartitionInfo};

use tidb_domain_core::domain::ddl_notifier::{ClusterNotifierSessionPool, ClusterNotifierTableStore};
use super::super::{ClusterPriorityQueueSource, Domain, Session};
use super::node_fixture::{rows, session_context, ABC_HASH};
use crate::domain::configured_user_store::ConfiguredUserStore;
use crate::domain::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};

mod admin_alter_ddl_job;
mod affinity_tests;
mod aggregates;
mod analyzetest;
mod chunk_allocator;
mod column_defaults;
mod column_type_flags;
mod cost_model_ver1;
mod create_table;
mod create_table_like;
mod create_table_options;
mod create_with_info;
mod cross_ks;
mod ddl_analyze;
mod ddl_delivery;
mod ddl_executor;
mod ddl_tests;
mod dml;
mod dxf_framework;
mod dxf_integrationtests;
mod dxf_scheduler;
mod dxf_taskexecutor;
mod executor_joins_inserts;
mod explain_explore;
mod fts_resolve_index;
mod go_executor_protocol;
mod import_into_job;
mod import_into_precheck;
mod import_into_show;
mod import_query;
mod index_prefix_lengths;
mod index_prefix_reads;
mod index_ranges;
mod index_tests;
mod indexes;
mod infoschema_v2;
mod joins;
mod lockstats;
mod masking_policy;
mod materialized_view;
mod mem_quota;
mod mpp_execution;
mod mpp_to_pb;
mod parallel_sort;
mod partition_tests;
mod physical_readers;
mod pipelineddml;
mod plan_tests;
mod point_get;
mod pointget;
mod predicate_pushdown;
mod prepared_params;
mod primary_keys;
mod repair_table;
mod select_clauses;
mod select_into;
mod session_schema_changes;
mod set_operations;
mod sql_kit;
mod statistics_tests;
mod subqueries;
mod table_mode;
mod table_round_trip;
mod table_tables;
mod tiflash_partition;
mod transaction_tests;
mod upgrade;

/// Go `rowcodec.RemoveKeyspacePrefix`: the native RPC hook observes physical keys.
fn logical_rpc_key(key: &[u8]) -> &[u8] {
    tidb_rowcodec::remove_keyspace_prefix(key, tidb_config_kerneltype::is_classic(), true, false)
}

/// Go `testkit.NewTestKit(t, store)` on the cop-backed stack.
pub(super) fn testkit(stack: &UnistoreClusterStack, connection_id: u64) -> crate::testkit::TestKit {
    crate::testkit::TestKit::from_session(
        stack.factory.open_session(session_context(connection_id)).expect("session opens"),
    )
}

pub(super) fn cop_backed_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    cop_backed_stack_with_stats_lease(None)
}

fn cop_backed_stack_with_stats_lease(
    stats_lease: Option<tidb_domain_core::node_config::StatsLease>,
) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    cop_backed_stack_with_config(|config| {
        // Go's testkit: `session.DisableStats4Test()` (`SetStatsLease(-1)`),
        // so no statistics loop reloads or flushes behind a test's back; a
        // test of that loop names its lease.
        config.stats_lease = tidb_domain_core::node_config::StatsLease::Disabled;
        if let Some(stats_lease) = stats_lease {
            config.stats_lease = stats_lease;
        }
    })
}

/// The cop-backed stack with the node config changed by `f` (the stack binds
/// no port: `port` is what the node's server info publishes).
pub(super) fn cop_backed_stack_with_config(
    f: impl FnOnce(&mut tidb_domain_core::node_config::NodeConfig),
) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    // Go `testenv.UpdateConfigForNextgen`, called by CreateMockStore.
    if tidb_config_kerneltype::is_next_gen() {
        tidb_config::config_tree::config::update_global(|config| {
            config.keyspace_name = tidb_keyspace::SYSTEM.to_owned();
            config.instance.tidb_service_scope = tidb_dxf_handle::NEXT_GEN_TARGET_SCOPE.to_owned();
        });
        tidb_vardef::set_service_scope(tidb_dxf_handle::NEXT_GEN_TARGET_SCOPE.to_owned());
    }
    let mut config = tidb_domain_core::node_config::NodeConfig::with_defaults(
        |config| {
            config.store = tidb_config::store::StoreType("unistore".to_owned());
            config.port = 0;
        },
        tidb_domain_core::node_config::NodeOptions::default(),
    )
    .expect("node config");
    f(&mut config);
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

#[test]
fn shared_session_activation_inherits_globals_and_obeys_set() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut ddl = factory.open_session(session_context(133)).unwrap();
    rows(
        &mut ddl,
        "CREATE TABLE test.activation_light (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    );
    // Go loadCommonGlobalVariablesIfNeeded reads the SQL-backed cache;
    // zzprobe/r01 pins first-session retention and new-session inheritance.
    rows(&mut ddl, "SET GLOBAL tidb_enable_async_commit=OFF");
    rows(&mut ddl, "SET GLOBAL tidb_enable_1pc=ON");
    let mut first = factory.open_session(session_context(131)).unwrap();
    // Go `loadCommonGlobalVariablesIfNeeded`: the session's copy is taken by
    // its first statement.
    rows(&mut first, "SELECT 1");
    rows(&mut ddl, "SET GLOBAL tidb_enable_1pc=OFF");
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
        [["GRANT ALL PRIVILEGES ON *.* TO `history_other`@`%`"]]
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

/// Go `PessimisticRCTxnContextProvider` under `tidb_rc_read_check_ts`: a
/// read-only statement of a pessimistic READ-COMMITTED transaction reads at
/// the transaction's latest fetched timestamp with `RCCheckTS`, so a row a
/// peer committed after it fails the read with a write conflict
/// (`handleAfterQueryError` then asks the connection to retry, which re-runs
/// it with `RetryInfo.Retrying` and a fresh timestamp).
#[test]
fn rc_read_check_ts_reads_at_the_latest_timestamp_and_conflicts_on_newer_data() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack
        .factory
        .open_session(session_context(127))
        .expect("reader");
    let mut writer = stack
        .factory
        .open_session(session_context(128))
        .expect("writer");
    for sql in [
        "CREATE TABLE test.rcts (id INT PRIMARY KEY, v INT)",
        "INSERT INTO test.rcts VALUES (1, 10)",
        "SET GLOBAL tidb_rc_read_check_ts = ON",
        "SET transaction_isolation = 'READ-COMMITTED'",
    ] {
        reader.node_conn().execute_write(sql).expect("setup");
    }
    // A coprocessor read: a point read at an unchanged timestamp answers
    // from the snapshot's cache, as client-go's `KVSnapshot` does.
    for read in ["SELECT v FROM test.rcts"] {
        reader
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["10"]], "{read}");
        writer
            .node_conn()
            .execute_write("UPDATE test.rcts SET v = v + 1 WHERE id = 1")
            .expect("peer commit");
        // The conflict surfaces at the first fetch, before any row reached
        // a client: Go's `writeChunks` reports it retryable there.
        let code = {
            let mut result = reader.node_conn().execute(read).expect("the read opens");
            let source = crate::domain::resultset_source::ResultSetSource::next_batch(
                result.source(),
                8,
            )
            .expect_err("the newer row conflicts with the latest timestamp");
            source.code
        };
        assert_eq!(code, 9007, "{read}");
        assert!(reader.retry_after_query_error(code), "{read}: StmtActionRetryReady");
        reader.set_retrying(true);
        let now = displayed(rows(&mut reader, read));
        reader.set_retrying(false);
        assert_ne!(now, vec![vec!["10"]], "{read}: the retry reads fresh");
        reader
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        writer
            .node_conn()
            .execute_write("UPDATE test.rcts SET v = 10 WHERE id = 1")
            .expect("reset");
    }
    // Off, every statement of the transaction reads fresh.
    reader
        .node_conn()
        .execute_write("SET GLOBAL tidb_rc_read_check_ts = OFF")
        .expect("off");
    reader
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    rows(&mut reader, "SELECT v FROM test.rcts");
    writer
        .node_conn()
        .execute_write("UPDATE test.rcts SET v = 11 WHERE id = 1")
        .expect("peer commit");
    assert_eq!(
        displayed(rows(&mut reader, "SELECT v FROM test.rcts")),
        vec![vec!["11"]]
    );
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
}

/// Go `HandleNextDDLEventWithTxn`'s statistics half: the handler on a
/// pooled system session, in one transaction.
fn handle_stats_schema_change(session: &mut Session, event: &SchemaChangeEvent) {
    let factory = session
        .node()
        .domain()
        .binding_session_factory
        .upgrade()
        .expect("the node's domain is live");
    let pool = factory.advanced_sys_session_pool();
    tidb_stats_handle_util::call_with_sctx(
        pool.as_ref(),
        |sctx| {
            sctx.handle_stats_ddl_event(event)
                .map_err(tidb_domain_core::sys_session::stats_session_error)
        },
        &[tidb_stats_handle_util::FLAG_WRAP_TXN],
    )
    .unwrap_or_else(|error| panic!("stats subscriber handles the source event: {error}"));
}

// Go statistics DDL tests explicitly HandleNextDDLEventWithTxn before reading
// statistics: the domain's own notifier does not deliver to the statistics
// handle in a test build.
fn drain_stats_ddl_events(factory: &Arc<Domain>, session: &mut Session) {
    // Go `statstestutil.HandleNextDDLEventWithTxn`, for every event the DDL
    // sent to the statistics handle's channel.
    while let Ok(event) = factory.ddl_event_ch().try_recv() {
        handle_stats_schema_change(session, &event);
    }
}

/// Stops the domain's own DDL notifier for good. The statistics owner's
/// campaign ends first, so its `OnBecomeOwner` cannot start the notifier
/// again after the stop.
fn stop_domain_ddl_notifier(factory: &Arc<Domain>) {
    if let Some(owner) = factory.stats_owner.as_ref() {
        owner.campaign_cancel();
    }
    if let Some(domain_notifier) = factory.ddl_notifier_for_test() {
        domain_notifier.stop();
    }
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
        plan: &tidb_planner::plan_builder::analyze::Analyze,
        vars: &tidb_executor::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        killer: &tidb_sqlkiller::SqlKiller,
        jobs: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
        stats_pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<tidb_executor::real_tikv_analyze::AnalyzeExecReport, tidb_store_driver_error::sql_query_error::SqlQueryError>
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
            plan,
            vars,
            resource_group,
            approximate_counts,
            killer,
            jobs,
            stats_pool,
        )
    }

    fn update_stats_cache(
        &self,
        physical_ids: Vec<i64>,
        local_temporary_tables: &[tidb_model::GoShared<tidb_model::TableInfo>],
        stats_pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        self.inner.update_stats_cache(physical_ids, local_temporary_tables, stats_pool)
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
        plan: &tidb_planner::plan_builder::analyze::Analyze,
        vars: &tidb_executor::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        killer: &tidb_sqlkiller::SqlKiller,
        jobs: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
        stats_pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<tidb_executor::real_tikv_analyze::AnalyzeExecReport, tidb_store_driver_error::sql_query_error::SqlQueryError>
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
            plan,
            vars,
            resource_group,
            approximate_counts,
            killer,
            jobs,
            stats_pool,
        )
    }

    fn update_stats_cache(
        &self,
        physical_ids: Vec<i64>,
        local_temporary_tables: &[tidb_model::GoShared<tidb_model::TableInfo>],
        stats_pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        self.inner.update_stats_cache(physical_ids, local_temporary_tables, stats_pool)
    }
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
        .get_latest()
        .table_info_by_name("test", "auto_exec_live")
        .expect("auto-analyze table is published")
        .1
        .read().id;
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
    let process_id = factory.next_conn_id().next_id();
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
                        inner: crate::domain::cluster_session_node::node_services(session.node.domain()).analyze(),
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
            let untrack: tidb_sqlexec::UnTrackSysProc = Arc::new(move |id| {
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
    // A job that failed two seconds after it started (Go `InsertAnalyzeJob`,
    // `StartAnalyzeJob`, `FinishAnalyzeJob` with an error).
    rows(&mut client, "SET time_zone = '+00:00'");
    rows(
        &mut client,
        "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, \
         start_time, end_time, state, fail_reason, instance, process_id) VALUES ('test', 't', '', \
         'analyze table `test`.`t`', UTC_TIMESTAMP() - INTERVAL 2 SECOND, UTC_TIMESTAMP(), \
         'failed', 'simulated failure', '127.0.0.1:4000', NULL)",
    );

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

/// Go `stmtExecInfo.MemArbitration = stmtCtx.MemTracker.MemArbitration()`:
/// a statement reserved in the memory arbitrator's root pool waited for
/// that grant, which the statement summary records.
#[test]
fn statement_summary_records_the_memory_arbitration_wait() {
    struct Recorder;
    impl tidb_memory::RecordMemState for Recorder {
        fn load(&self) -> Result<Option<tidb_memory::RuntimeMemStateV1>, String> {
            Ok(None)
        }
        fn store(&self, _: &tidb_memory::RuntimeMemStateV1) -> Result<(), String> {
            Ok(())
        }
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(82)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE arb (id BIGINT PRIMARY KEY)");
    let arbitrator = tidb_memory::MemArbitrator::new(1 << 30, 4, 3, 0, Box::new(Recorder));
    assert!(arbitrator.auto_run(
        tidb_memory::MemArbitratorActions::default(),
        tidb_memory::DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
        4,
        tidb_memory::DEF_TASK_TICK_DUR,
    ));
    arbitrator.set_work_mode(tidb_memory::ArbitratorWorkMode::Standard);
    session.set_mem_arbitrator(arbitrator.clone());
    rows(&mut session, "SET tidb_mem_arbitrator_query_reserved = 4096");
    rows(&mut session, "SELECT id FROM arb WHERE id > 7");
    rows(&mut session, "SET tidb_mem_arbitrator_query_reserved = 0");
    let waited = displayed(rows(
        &mut session,
        "SELECT MAX_MEM_ARBITRATION, AVG_MEM_ARBITRATION > 0 FROM information_schema.statements_summary WHERE QUERY_SAMPLE_TEXT = 'SELECT id FROM arb WHERE id > 7'",
    ));
    assert_eq!(waited.len(), 1, "{waited:?}");
    assert!(waited[0][0].parse::<f64>().unwrap() > 0.0, "{waited:?}");
    assert_eq!(waited[0][1], "1", "{waited:?}");
    arbitrator.set_work_mode(tidb_memory::ArbitratorWorkMode::Disable);
    assert!(arbitrator.stop());
}

/// Go `querywatch`: QUERY WATCH ADD validates and records a manual watch,
/// RUNAWAY_WATCHES syncs and lists them, QUERY WATCH REMOVE moves one to
/// `tidb_runaway_watch_done`; the values are the Go oracle's over unistore.
#[test]
fn query_watch_adds_lists_and_removes_gos_watches() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(79)).expect("session opens");
    rows(&mut session, "CREATE RESOURCE GROUP rg1 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=KILL)");
    assert_eq!(displayed(rows(&mut session, "QUERY WATCH ADD RESOURCE GROUP rg1 SQL TEXT EXACT TO 'select 1'")), [["1"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "QUERY WATCH ADD RESOURCE GROUP rg1 ACTION COOLDOWN SQL TEXT SIMILAR TO 'select * from t where a = 1'"
        )),
        [["2"]]
    );
    let refused = |session: &mut crate::Session, sql: &str| session.run(sql).unwrap_err().to_mysql_error().message;
    assert_eq!(refused(&mut session, "QUERY WATCH ADD RESOURCE GROUP rg1 SQL DIGEST 'xyz'"), "digest format error");
    rows(&mut session, "CREATE RESOURCE GROUP rg_plain RU_PER_SEC=100");
    assert_eq!(
        refused(&mut session, "QUERY WATCH ADD RESOURCE GROUP rg_plain SQL TEXT EXACT TO 'select 2'"),
        "must set runaway config for resource group `rg_plain`"
    );
    // No group is the default group.
    assert_eq!(
        refused(&mut session, "QUERY WATCH ADD SQL TEXT EXACT TO 'select 2'"),
        "must set runaway config for resource group `default`"
    );
    assert!(refused(&mut session, "QUERY WATCH ADD RESOURCE GROUP rgx SQL TEXT EXACT TO 'select 2'").contains("rgx"));
    assert_eq!(
        refused(&mut session, "QUERY WATCH ADD RESOURCE GROUP rg1 SQL TEXT EXACT TO 'select 1; select 2'"),
        "only support one SQL"
    );
    let watches = "SELECT id, resource_group_name, end_time, watch, watch_text, source, action, rule FROM information_schema.runaway_watches ORDER BY id";
    assert_eq!(
        displayed(rows(&mut session, watches)),
        [
            ["1", "rg1", "UNLIMITED", "Exact", "select 1", "manual", "Kill", "None"],
            [
                "2",
                "rg1",
                "UNLIMITED",
                "Similar",
                "077a87a576e42360c95530ccdac7a1771c4efba17619e26be50a4cfd967204a0",
                "manual",
                "CoolDown",
                "None"
            ],
        ]
    );
    rows(&mut session, "QUERY WATCH REMOVE 1");
    assert_eq!(displayed(rows(&mut session, "SELECT id FROM information_schema.runaway_watches")), [["2"]]);
    // Go `handleRunawayWatchDone` moves the row out of the watch table.
    assert_eq!(displayed(rows(&mut session, "SELECT id FROM mysql.tidb_runaway_watch")), [["2"]]);
    assert_eq!(
        displayed(rows(&mut session, "SELECT record_id, watch_text FROM mysql.tidb_runaway_watch_done")),
        [["1", "select 1"]]
    );
    assert_eq!(refused(&mut session, "QUERY WATCH REMOVE 99"), "no runaway watch with the specific ID");
    // Go explains the statement on a session of its own for its plan digest.
    rows(&mut session, "QUERY WATCH ADD RESOURCE GROUP rg1 SQL TEXT PLAN TO 'select 1'");
    assert_eq!(
        displayed(rows(&mut session, "SELECT watch_text FROM information_schema.runaway_watches WHERE watch = 'Plan'")),
        [["4df89435536060a4528770d1c135410a71890415043b7cd580da88c2e5ebcfbf"]]
    );
    rows(&mut session, "QUERY WATCH REMOVE RESOURCE GROUP rg1");
    assert!(rows(&mut session, "SELECT id FROM information_schema.runaway_watches").is_empty());
}

/// Go `getSessionFactoryInternal` marks a pooled internal session's globals
/// loaded without loading them: `@@sql_mode` reads its default while the
/// typed `SQLMode` stays `ModeNone`, so a zero date converts (the Go probe
/// over unistore printed the strict text and `0`). The runaway syncer's first
/// window starts at that zero date.
#[test]
fn domain_pool_sessions_keep_gos_zero_sql_mode() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let pool = stack.factory.advanced_sys_session_pool();
    let rows = pool
        .with_session(|session| {
            session.with_session_context(|context| {
                tidb_domain_core::sys_session::exec_sql(context, tidb_txnkv::INTERNAL_TXN_OTHERS, "select @@sql_mode, cast('0000-00-00' as datetime) is null", &[])
                    .map_err(tidb_syssession::SysSessionError::new)
            })
        })
        .unwrap();
    assert_eq!(
        displayed(rows),
        [[
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
            "0"
        ]]
    );
}

/// Go's identify path over unistore: a KILL group's PROCESSED_KEYS rule
/// interrupts the scan with its cause and quarantines the exact text, so the
/// same statement is refused before it runs; the oracle's watch row follows.
#[test]
fn runaway_processed_keys_rule_kills_then_quarantines() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(80)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE runaway_t1 (a INT, b INT)");
    rows(&mut session, "INSERT INTO runaway_t1 VALUES (1,1),(2,2),(3,3)");
    rows(
        &mut session,
        "CREATE RESOURCE GROUP rg2 RU_PER_SEC=1000 QUERY_LIMIT=(PROCESSED_KEYS=2 ACTION=KILL WATCH=EXACT DURATION='10m')",
    );
    rows(&mut session, "SET RESOURCE GROUP rg2");
    let error = session.run("SELECT * FROM runaway_t1").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 8253, "{}", error.message);
    assert_eq!(error.message, "Query execution was interrupted, identified as runaway query [ProcessedKeys = 3(2)]");
    let error = session.run("SELECT * FROM runaway_t1").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 8254, "{}", error.message);
    assert_eq!(error.message, "Quarantined and interrupted because of being in runaway watch list");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT resource_group_name, watch, watch_text, action, rule FROM information_schema.runaway_watches"
        )),
        [["rg2", "Exact", "SELECT * FROM runaway_t1", "Kill", "ProcessedKeys = 3(2)"]]
    );
    // An unrelated statement of the group still runs.
    assert_eq!(displayed(rows(&mut session, "SELECT 1")), [["1"]]);
    // Go's quarantine flusher writes the watch within its 1s interval.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let flushed = "SELECT resource_group_name, watch, watch_text, action, rule FROM mysql.tidb_runaway_watch";
    loop {
        let found = displayed(rows(&mut session, flushed));
        if !found.is_empty() {
            assert_eq!(found, [["rg2", "1", "SELECT * FROM runaway_t1", "3", "ProcessedKeys = 3(2)"]]);
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the quarantine record was never flushed");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Pinned storage `TestGCStats`: stale index and column rows are removed one
/// item transaction at a time, while a dropped table needs one pass to clear
/// payload and a later pass to remove `stats_meta`.

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

/// Go `dataForTiDBClusterInfo` (`infoschema_reader.go:1842`) over
/// `GetClusterServerInfo`: one row per node, describing where it is and how
/// long it has been up. Over an embedded store the TiDB row is this node and
/// `GetStoreServerInfo` reads the store's own PD client (a Go mock store
/// answers `tikv|store1|`); the PD and microservice retrievers find no etcd
/// members.
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
    assert_eq!(
        reported.iter().map(|row| row[0].as_str()).collect::<Vec<_>>(),
        ["tidb", "tikv"],
        "this node, then the embedded store"
    );
    assert_eq!(reported[1][1..3], ["store1", ""], "Go's mock store row");
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
        .dump_col_stats_usage_to_kv()
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
    assert!(session.node().txn.explicit().is_none());
    assert!(session.node().buffer.is_empty());
    assert!(rows(&mut session, "SELECT * FROM t").is_empty());
    rows(&mut session, "INSERT INTO t VALUES (2)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["2"]]
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

/// Delegates the analysis and fails the statement's closing cache update.
struct FailingStatsCacheUpdate {
    inner: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
    updated: Arc<std::sync::Mutex<Vec<i64>>>,
}

impl tidb_executor::cluster_analyze_seam::ClusterAnalyze for FailingStatsCacheUpdate {
    fn execute(
        &self,
        plan: &tidb_planner::plan_builder::analyze::Analyze,
        vars: &tidb_executor::cluster_analyze::AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        killer: &tidb_sqlkiller::SqlKiller,
        jobs: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
        stats_pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<tidb_executor::real_tikv_analyze::AnalyzeExecReport, tidb_store_driver_error::sql_query_error::SqlQueryError>
    {
        self.inner.execute(
            plan,
            vars,
            resource_group,
            approximate_counts,
            killer,
            jobs,
            stats_pool,
        )
    }

    fn update_stats_cache(
        &self,
        physical_ids: Vec<i64>,
        _local_temporary_tables: &[tidb_model::GoShared<tidb_model::TableInfo>],
        _stats_pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        self.updated.lock().unwrap().extend(physical_ids);
        Err(tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(
            "statistics cache update failed",
        ))
    }
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

/// Go `derivePathStatsAndTryHeuristics` selects a path while deriving the
/// DataSource's statistics, notes it once (`AppendNote` under `EXPLAIN
/// FORMAT='verbose'`, an extra note `SHOW WARNINGS` never shows otherwise)
/// and leaves only that path in `PossibleAccessPaths`, which index-join
/// planning then reads. Expected notes and warnings: Go on this branch, a
/// throwaway testkit probe running these statements (`EXPLAIN` then `SHOW
/// WARNINGS`) in the same order after the same DDL and ANALYZE.
#[test]
fn derived_statistics_select_heuristic_access_paths_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1132)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    // Go's run loaded every requested statistic within the default 100ms;
    // a loaded test machine must not time out into a sync-load warning.
    session
        .node_conn()
        .execute_write("SET tidb_stats_load_sync_wait = 60000")
        .unwrap();
    for ddl in [
        "CREATE TABLE note_t (a INT PRIMARY KEY, b INT, c INT, d INT, UNIQUE KEY ub (b), KEY ic (c))",
        "CREATE TABLE note_r (a INT, b INT, c INT, UNIQUE INDEX idx_b(b), INDEX idx_b_c(b, c))",
        "CREATE TABLE note_w (a INT, b INT, c INT, d INT, UNIQUE INDEX idx_a(a), UNIQUE INDEX idx_b_c(b, c), UNIQUE INDEX idx_b_c_a_d(b, c, a, d))",
        "CREATE TABLE note_s (a INT, b INT, c INT, KEY ic (c))",
        "INSERT INTO note_t VALUES (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5)",
        "INSERT INTO note_r VALUES (1,1,1),(2,2,2),(3,3,3)",
        "INSERT INTO note_w VALUES (1,1,1,1),(2,2,2,2),(3,3,3,3)",
        "INSERT INTO note_s VALUES (1,1,1),(2,2,2),(3,3,3)",
        "ANALYZE TABLE note_t, note_r, note_w, note_s",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let warnings = |session: &mut Session, format: &str, sql: &str| -> Vec<String> {
        rows(session, &format!("EXPLAIN {format}{sql}"));
        displayed(rows(session, "SHOW WARNINGS"))
            .into_iter()
            .map(|row| format!("{} {} {}", row[0], row[1], row[2]))
            .collect()
    };
    for (sql, verbose) in [
        (
            "SELECT * FROM note_t WHERE a = 1 AND c > 0",
            vec!["Note 1105 handle of note_t is selected since the path only has point ranges"],
        ),
        (
            "SELECT * FROM note_t WHERE b = 2 AND c > 0",
            vec!["Note 1105 unique index ub of note_t is selected since the path only has point ranges with double scan"],
        ),
        (
            "SELECT b, c FROM note_r WHERE b = 5 AND c > 10",
            vec!["Note 1105 index idx_b_c of note_r is selected since the path only fetches limited number of rows with single scan"],
        ),
        // The refining single scan needs five ranges, not fewer than twice
        // the unique path's one.
        (
            "SELECT a, b, c FROM note_w WHERE a = 1 AND b = 2 AND c IN (1, 2, 3, 4, 5)",
            vec!["Note 1105 unique index idx_a of note_w is selected since the path only has point ranges with double scan"],
        ),
        (
            "SELECT a, b, c FROM note_w WHERE a = 1 AND b = 2 AND c IN (1)",
            vec!["Note 1105 unique index idx_b_c_a_d of note_w is selected since the path only fetches limited number of rows with single scan"],
        ),
        // An empty range is selected outright, however its index is named.
        (
            "SELECT * FROM note_t WHERE b > 5 AND b < 3",
            vec!["Note 1105 unique index ub of note_t is selected since the path only has point ranges with double scan"],
        ),
        (
            "SELECT * FROM note_t t1 JOIN note_t t2 ON t1.c = t2.c WHERE t1.a = 1",
            vec!["Note 1105 handle of t1 is selected since the path only has point ranges"],
        ),
        (
            "SELECT * FROM note_t WHERE a = 1 ORDER BY c",
            vec!["Note 1105 handle of note_t is selected since the path only has point ranges"],
        ),
        (
            "SELECT * FROM note_t WHERE b IN (1, 2) AND (a = 1 OR c = 3)",
            vec!["Note 1105 unique index ub of note_t is selected since the path only has point ranges with double scan"],
        ),
        // Only the unique point path remains for the index join's probe,
        // and it cannot look up `c`.
        (
            "SELECT /*+ INL_JOIN(note_t) */ * FROM note_s JOIN note_t ON note_s.c = note_t.c WHERE note_t.b = 1",
            vec![
                "Note 1105 unique index ub of note_t is selected since the path only has point ranges with double scan",
                "Warning 1815 Optimizer Hint /*+ INL_JOIN(note_t) */ or /*+ TIDB_INLJ(note_t) */ is inapplicable",
            ],
        ),
    ] {
        let plain: Vec<_> = verbose
            .iter()
            .filter(|warning| !warning.starts_with("Note"))
            .copied()
            .collect();
        assert_eq!(warnings(&mut session, "", sql), plain, "EXPLAIN {sql}");
        assert_eq!(warnings(&mut session, "FORMAT = 'verbose' ", sql), verbose, "verbose {sql}");
    }
    // Go's plan for the inapplicable hint: a hash join over the unique point
    // get.
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN SELECT /*+ INL_JOIN(note_t) */ * FROM note_s JOIN note_t ON note_s.c = note_t.c WHERE note_t.b = 1",
    ))
    .into_iter()
    .map(|row| row[0].trim_start_matches([' ', '│', '└', '├', '─']).to_string())
    .collect::<Vec<_>>();
    assert_eq!(
        plan,
        [
            "Projection_11",
            "HashJoin_13",
            "Selection_28(Build)",
            "Point_Get_27",
            "TableReader_35(Probe)",
            "Selection_34",
            "TableFullScan_33",
        ]
    );
}

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

/// Go `TestLoadDataLocalPessimisticRetryDesync` (2964713e267): a retryable
/// single-statement deadlock does not re-run LOAD DATA LOCAL INFILE, whose
/// client file stream is one-shot; the deadlock (1213) stands, no row
/// lands, and the session stays usable.
#[test]
fn test_load_data_local_pessimistic_retry_desync() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1935)).unwrap();
    for sql in [
        "CREATE DATABASE IF NOT EXISTS test",
        "USE test",
        "CREATE TABLE t_pessimistic_retry (id INT PRIMARY KEY, v VARCHAR(16))",
        "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "BEGIN PESSIMISTIC",
    ] {
        rows(&mut session, sql);
    }
    // A retryable deadlock for the first lock request after the file was read.
    let _deadlock = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/pessimisticLockReturnDeadlock",
        "1*return(true)->return(false)",
    );
    let sql = "LOAD DATA LOCAL INFILE '/tmp/pessimistic_retry_desync.csv' REPLACE INTO TABLE t_pessimistic_retry FIELDS TERMINATED BY ','";
    let stmt = session.parse_at_statement_boundary(sql).unwrap();
    let outcome = (|| {
        assert!(session.node_conn().execute_stmt(sql, &stmt)?.is_none(), "{sql} answers no rows");
        assert!(session.local_infile_request().is_some(), "the load asks for the client's file");
        session.node_conn().finish_local_infile(b"1,one\n")
    })();
    let error = outcome.expect_err("the deadlock stands");
    assert_eq!(error.code, 1213, "{}", error.message);
    assert_eq!(displayed(rows(&mut session, "SELECT COUNT(*) FROM t_pessimistic_retry")), [["0"]]);
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
    without_conditional_spans(trace_tree(session, sql))
        .into_iter()
        .filter(|line| !CLIENT.iter().any(|client| line.contains(client)))
        .collect()
}

/// Go's `LimitExec.Close` starts its `limitExec.Close` span only when the
/// close took over a millisecond (`if elapsed > time.Millisecond`), so the
/// span is in a trace or not with the machine's speed. The tree is redrawn
/// as the server draws it, without that span.
fn without_conditional_spans(lines: Vec<String>) -> Vec<String> {
    let nodes: Vec<(usize, String)> = lines
        .iter()
        .map(|line| {
            let chars: Vec<char> = line.chars().collect();
            match chars.iter().position(|c| matches!(c, '├' | '└')) {
                Some(at) => ((at - 2) / 2 + 1, chars[at + 2..].iter().collect()),
                None => (0, line.clone()),
            }
        })
        .filter(|(_, label)| label != "limitExec.Close")
        .collect();
    // Whether each ancestor (by depth, from 1) has a later sibling.
    let mut path: Vec<bool> = Vec::new();
    nodes
        .iter()
        .enumerate()
        .map(|(index, (depth, label))| {
            if *depth == 0 {
                return label.clone();
            }
            let has_next_sibling = nodes[index + 1..]
                .iter()
                .find(|(later, _)| later <= depth)
                .is_some_and(|(later, _)| later == depth);
            path.truncate(depth - 1);
            let mut line = String::from("  ");
            for ancestor_has_next in &path {
                line.push_str(if *ancestor_has_next { "│ " } else { "  " });
            }
            line.push_str(if has_next_sibling { "├─" } else { "└─" });
            line.push_str(label);
            path.push(has_next_sibling);
            line
        })
        .collect()
}

#[test]
fn a_conditional_span_leaves_the_trace_tree_as_drawn_without_it() {
    let lines = |tree: &[&str]| tree.iter().map(|line| (*line).to_owned()).collect::<Vec<_>>();
    let without = lines(&[
        "trace",
        "  └─session.ExecuteStmt",
        "    ├─executor.Compile",
        "    │ └─executor.EvalSubQuery",
        "    │   ├─distsql.Select",
        "    │   └─*executor.MaxOneRowExec.Next",
        "    │     └─*aggregate.StreamAggExec.Next",
        "    └─session.runStmt",
    ]);
    let with = lines(&[
        "trace",
        "  └─session.ExecuteStmt",
        "    ├─executor.Compile",
        "    │ └─executor.EvalSubQuery",
        "    │   ├─distsql.Select",
        "    │   ├─*executor.MaxOneRowExec.Next",
        "    │   │ └─*aggregate.StreamAggExec.Next",
        "    │   └─limitExec.Close",
        "    └─session.runStmt",
    ]);
    assert_eq!(without_conditional_spans(without.clone()), without);
    assert_eq!(without_conditional_spans(with), without);
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

fn partition_id_map(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
) -> Option<(i64, Vec<(String, i64)>)> {
    let (_, table) = catalog.table_info_by_name(schema, table)?;
    let table = table.read();
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

/// Go `sysvar.go`'s GC variables live in `mysql.tidb` (what the GC worker
/// reads): SET GLOBAL writes the `tikv_gc_*` row (ON/OFF as true/false,
/// `tidb_gc_concurrency` with its auto switch), and `@@global` reads it back,
/// a direct edit of the row included. Pinned to the Go oracle's answers.
#[test]
fn gc_variables_live_in_mysql_tidb_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(181))
        .expect("session opens");
    let mut query = |sql: &str| displayed(rows(&mut session, sql));
    query("set global tidb_gc_enable = OFF");
    assert_eq!(
        query("select variable_value from mysql.tidb where variable_name = 'tikv_gc_enable'"),
        [["false"]]
    );
    assert_eq!(query("select @@global.tidb_gc_enable"), [["0"]]);
    query("update mysql.tidb set variable_value = 'true' where variable_name = 'tikv_gc_enable'");
    assert_eq!(query("select @@global.tidb_gc_enable"), [["1"]]);
    query("set global tidb_gc_concurrency = 5");
    assert_eq!(
        query(
            "select variable_name, variable_value from mysql.tidb where variable_name in \
             ('tikv_gc_auto_concurrency', 'tikv_gc_concurrency') order by variable_name"
        ),
        [["tikv_gc_auto_concurrency", "false"], ["tikv_gc_concurrency", "5"]]
    );
    assert_eq!(query("select @@global.tidb_gc_concurrency"), [["5"]]);
    query("set global tidb_gc_concurrency = -1");
    assert_eq!(query("select @@global.tidb_gc_concurrency"), [["-1"]]);
    query("set global tidb_gc_life_time = '20m'");
    assert_eq!(
        query("select variable_value from mysql.tidb where variable_name = 'tikv_gc_life_time'"),
        [["20m0s"]]
    );
    assert_eq!(query("select @@global.tidb_gc_life_time"), [["20m0s"]]);
}

/// Go `w.sess.Execute` inside the job step's transaction: a statement on the step session
/// reads at the step's start timestamp and stages its rows into the step's
/// buffer, publishing nothing itself; the rows appear when the step commits
/// that buffer and vanish when it does not. The INSERT's presume-not-exist
/// flag travels with the buffer, so a duplicate the optimistic statement did
/// not check in place is refused at the step's commit.

/// One statement on a DDL step session of its own (Go `w.sess.Execute` in
/// the step's transaction), the session ended after it.
fn exec_in_step_txn(
    pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    start_ts: u64,
    buffer: &tidb_store_driver_txn::cluster_storage::MutationBuffer,
    sql: &str,
    args: &[tidb_sqlescape::SqlArg<'_>],
) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
    let se = pool.open_step_session(start_ts, buffer)?;
    tidb_ddl_build::session_pool::execute(&se, sql, "test", args)
}

/// Go `SetGlobalSysVar` ends in `REPLACE INTO mysql.global_variables` for
/// the assigned variable, whatever its scope and even at its default
/// (gorun): an INSTANCE variable's row is stored, and `= DEFAULT` stores the
/// default's text.
#[test]
fn set_global_replaces_the_variables_row_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(210)).unwrap();
    let row = |session: &mut Session, name: &str| {
        displayed(rows(
            session,
            &format!("SELECT variable_value FROM mysql.global_variables WHERE variable_name = '{name}'"),
        ))
    };
    assert_eq!(row(&mut session, "tidb_general_log"), Vec::<Vec<String>>::new());
    rows(&mut session, "SET GLOBAL tidb_general_log = 1");
    assert_eq!(row(&mut session, "tidb_general_log"), [["ON"]]);
    rows(&mut session, "SET GLOBAL tidb_general_log = 0");
    assert_eq!(row(&mut session, "tidb_general_log"), [["OFF"]]);

    rows(&mut session, "SET GLOBAL tidb_mem_quota_query = 2147483648");
    assert_eq!(row(&mut session, "tidb_mem_quota_query"), [["2147483648"]]);
    rows(&mut session, "SET GLOBAL tidb_mem_quota_query = DEFAULT");
    assert_eq!(row(&mut session, "tidb_mem_quota_query"), [["1073741824"]]);
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@global.tidb_mem_quota_query")),
        [["1073741824"]]
    );

    // An alias's hook stores its row too (Go `SetGlobalSysVarOnly`).
    rows(&mut session, "SET GLOBAL transaction_isolation = 'READ-COMMITTED'");
    assert_eq!(row(&mut session, "transaction_isolation"), [["READ-COMMITTED"]]);
    assert_eq!(row(&mut session, "tx_isolation"), [["READ-COMMITTED"]]);
    rows(&mut session, "SET GLOBAL transaction_isolation = 'REPEATABLE-READ'");
}

fn table_id_of(session: &mut Session, table: &str) -> i64 {
    match rows(
        session,
        &format!(
            "SELECT TIDB_TABLE_ID FROM information_schema.tables \
             WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = '{table}'"
        ),
    )[0][0]
    {
        Datum::Int(id) => id,
        ref other => panic!("table id: {other:?}"),
    }
}

/// Go `Handle.InitStats` / `InitStatsLite` on the stack's domain sessions.
fn init_stats_on(
    stack: &UnistoreClusterStack,
    table_ids: &[i64],
    mode: tidb_stats_handle::bootstrap::InitialStatsLoad,
) -> tidb_stats_handle_syncload::stats_watch::StatsSnapshot {
    let catalog = stack.factory.catalog.get_latest();
    // Go `InfoSchema.SchemaTableInfos` over every schema (V2 keeps no
    // `DBInfo.Deprecated.Tables`).
    let targets: Vec<_> = catalog
        .stored_schemas()
        .iter()
        .flat_map(|database| {
            catalog
                .schema_table_infos(database.read().name.original())
                .unwrap()
        })
        .flat_map(|table| tidb_stats_handle_syncload::real_tikv_stats::StatsTarget::for_table(&table.read()))
        .collect();
    tidb_stats_handle::bootstrap::init_stats(
        Arc::new(tidb_domain_core::sys_session::OwnedStatsPool(
            stack.factory.advanced_sys_session_pool(),
        )),
        &targets,
        table_ids,
        mode,
    )
    .expect("init stats runs on the domain's sessions")
}

/// Go `TestBuiltinInEstWithoutStats`: a table whose only statistics are its
/// `stats_meta` count plans pseudo estimates after `InitStatsLite` and
/// `InitStats`, its existence map holding every column unanalyzed.
#[test]
fn builtin_in_estimate_survives_statistics_initialization_as_go() {
    use tidb_stats_handle::bootstrap::InitialStatsLoad;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(314)).unwrap();
    rows(&mut session, "CREATE TABLE test.t (a INT, b INT)");
    // Go `statstestutil.HandleNextDDLEventWithTxn`.
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO test.t VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)",
    );
    stack
        .factory
        .dump_stats_delta_to_kv(true, &[])
        .expect("the delta flush succeeds");
    let id = table_id_of(&mut session, "t");
    for mode in [InitialStatsLoad::Lite, InitialStatsLoad::IndexFull] {
        let snapshot = init_stats_on(&stack, &[], mode);
        let table = snapshot[&id].loaded().expect("stats_meta makes the table non-pseudo");
        assert_eq!(table.hist_coll.realtime_count, 10, "{mode:?}");
        let existence = table.existence_map.as_ref().unwrap().read().unwrap();
        assert!(!existence.is_empty(), "{mode:?}");
        for column in [1, 2] {
            assert!(existence.has(column, false), "{mode:?}");
            assert!(!existence.has_analyzed(column, false), "{mode:?}");
        }
        drop(existence);
        stack.factory.stats.store(snapshot);
        for column in ["a", "b"] {
            assert_eq!(
                displayed(rows(
                    &mut session,
                    &format!(
                        "EXPLAIN FORMAT='brief' SELECT * FROM test.t WHERE {column} IN (1,2,3,4,5,6,7,8)"
                    ),
                ))
                .into_iter()
                .map(|row| row[..2].join(" "))
                .collect::<Vec<_>>(),
                [
                    "TableReader 1.00".to_owned(),
                    "└─Selection 1.00".to_owned(),
                    "  └─TableFullScan 10.00".to_owned(),
                ],
                "{mode:?}: {column}"
            );
        }
    }
}

/// Go `statsReadWriter.LoadNeededHistograms`: the asynchronous demand is
/// drained on one statistics session in one wrapped transaction
/// (`loadNeededColumnHistograms` over `HistMetaFromStorageWithHighPriority`,
/// the histogram, CM sketch and TopN), and each item leaves the queue.
#[test]
fn the_async_load_drains_the_demand_on_the_domains_sessions_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(311)).unwrap();
    rows(&mut session, "CREATE TABLE test.drained (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO test.drained VALUES (1, 1), (2, 2), (3, 3)");
    rows(&mut session, "ANALYZE TABLE test.drained");
    let table_id = match rows(
        &mut session,
        "SELECT TIDB_TABLE_ID FROM information_schema.tables \
         WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'drained'",
    )[0][0]
    {
        Datum::Int(id) => id,
        ref other => panic!("table id: {other:?}"),
    };
    let stats = &stack.factory.stats;
    let evicted = Arc::new(
        stats
            .get(table_id)
            .expect("the analyzed table is cached")
            .copy_as(tidb_stats::CopyIntent::AllDataWritable),
    );
    evicted.hist_coll.drop_evicted();
    stats.publish_analyzed_table(table_id, evicted);
    let v_column = 2;
    let item = tidb_model::TableItemID {
        table_id,
        id: v_column,
        is_index: false,
        is_sync_load_failed: false,
    };
    let needed = &tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
    needed.insert(item, true);

    let catalog = stack.factory.catalog.get_latest();
    let table = catalog
        .table_info_by_id(table_id)
        .expect("the table is in the catalog");
    let targets = tidb_stats_handle_syncload::real_tikv_stats::StatsTarget::for_table(&table.read());
    let pool = stack.factory.advanced_sys_session_pool();
    tidb_stats_handle_syncload::real_tikv_stats::load_needed_histograms(
        &tidb_domain_core::sys_session::StatsPool {
            session_pool: Some(&pool),
        },
        &targets,
        stats,
    )
    .expect("the demand drains on the domain's sessions");
    assert!(needed.all_items().iter().all(|requested| requested.table_item_id != item));
    let table = stats.get(table_id).unwrap();
    let column = table.hist_coll.get_column(v_column).unwrap();
    let column = column.read().unwrap();
    assert!(column.stats_loaded_status.is_full_load());
    assert_eq!(column.top_n.as_ref().map(|top_n| top_n.num()), Some(3));
}

fn workload_learning_pool(stack: &UnistoreClusterStack) -> Arc<dyn tidb_workloadlearning::SessionPool> {
    Arc::new(tidb_domain_core::domain::workload_learning::SysSessionPool(
        stack.factory.advanced_sys_session_pool(),
    ))
}

fn workload_metric(table: &str, scan_time: i64, mem_usage: i64, read_frequency: i64, read_cost: f64) -> tidb_workloadlearning::TableReadCostMetrics {
    tidb_workloadlearning::TableReadCostMetrics {
        db_name: tidb_ast::CiString::new("test"),
        table_name: tidb_ast::CiString::new(table),
        table_scan_time: scan_time,
        table_mem_usage: mem_usage,
        read_frequency,
        table_read_cost: read_cost,
    }
}

fn now_naive() -> chrono::NaiveDateTime {
    chrono::Local::now().naive_local()
}

/// Go `workloadlearning.TestUpdateTableCostCache` and
/// `TestGetTableReadCacheMetricsWithNoData`.
#[test]
fn workload_learning_cache_loads_the_latest_version() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(315)).unwrap();
    let pool = workload_learning_pool(&stack);
    let empty = tidb_workloadlearning::WlCacheWorker::new(Arc::clone(&pool));
    assert!(empty.get_table_read_cost_metrics(1).is_none());

    rows(&mut session, "CREATE TABLE test.test (a int, b int, index idx(a))");
    let table_id = stack.factory.catalog.get_latest().table_info_by_name("test", "test").unwrap().1.read().id;
    let handle = tidb_workloadlearning::Handle::new(Arc::clone(&pool));
    let metrics = std::collections::HashMap::from([(table_id, workload_metric("test", 10, 10, 10, 10.0))]);
    handle.save_table_read_cost_metrics(&metrics, now_naive(), now_naive());
    let worker = tidb_workloadlearning::WlCacheWorker::new(pool);
    worker.update_table_read_cost_cache();
    let cached = worker.get_table_read_cost_metrics(table_id).expect("the saved metrics are cached");
    assert_eq!((cached.table_scan_time, cached.table_mem_usage, cached.read_frequency, cached.table_read_cost), (10, 10, 10, 10.0));
}

/// Go `TestSQLVariableAccelerateUserCreationUpdate`: with
/// `tidb_accelerate_user_creation_update` on, an account statement reloads
/// only the users already loaded, so a new user's grant is not in the cache
/// until that user logs in (`MatchIdentity`'s `ensureActiveUser`); turned
/// off, the next statement reloads every user.
#[test]
fn accelerate_user_creation_update_loads_users_as_they_log_in_like_go() {
    let (stack, users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(307)).unwrap();
    let select = |user: &str| {
        users.accounts().has_matching_global_or_schema_priv(
            user,
            "%",
            "test",
            tidb_privilege::PrivilegeTypeExt::mask(tidb_mysql::SelectPriv),
        )
    };
    // 1. check the default variable value
    assert_eq!(displayed(rows(&mut tk, "select @@global.tidb_accelerate_user_creation_update")), [["0"]]);
    rows(&mut tk, "create user aaa");
    assert!(users.accounts().full_data());
    assert!(!select("bbb"));

    // 2. change the variable and check
    rows(&mut tk, "set @@global.tidb_accelerate_user_creation_update = on");
    assert_eq!(displayed(rows(&mut tk, "select @@global.tidb_accelerate_user_creation_update")), [["1"]]);
    assert!(tidb_vardef::ACCELERATE_USER_CREATION_UPDATE.load(std::sync::atomic::Ordering::SeqCst));
    rows(&mut tk, "create user bbb");
    assert!(!users.accounts().full_data());
    // The reload runs, but bbb is not an active user: its grant is not
    // loaded ("counterintuitive, but it's still the expected behavior").
    rows(&mut tk, "grant select on test.* to bbb");
    assert!(!select("bbb"));
    // bbb logs in: everything works as expected.
    assert!(users.match_identity("bbb", "localhost").is_some());
    assert!(select("bbb"));

    // 3. change the variable and check again
    rows(&mut tk, "set @@global.tidb_accelerate_user_creation_update = off");
    assert_eq!(displayed(rows(&mut tk, "select @@global.tidb_accelerate_user_creation_update")), [["0"]]);
    rows(&mut tk, "drop user aaa");
    assert!(users.accounts().full_data());
    assert!(select("bbb"));
}

/// Go `LoadPrivilegeLoop` -> `Handle.UpdateAll`: a peer's account rows land
/// only in `mysql.*`, and this node's load reads them through the domain's
/// system sessions (`MySQLPrivilege.LoadAll`'s `SELECT HIGH_PRIORITY`s), a
/// TIMESTAMP read back as its instant (`GoTime(time.Local)`).
#[test]
fn the_privilege_load_reads_a_peers_rows_through_the_domains_sessions_as_go() {
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(306)).unwrap();
    // A peer's writes: the rows, with no CREATE USER or GRANT on this node.
    for sql in [
        "SET time_zone = '+08:00'",
        "INSERT INTO mysql.user (Host, User, authentication_string, Select_priv, password_last_changed) \
         VALUES ('%', 'peer', '', 'Y', '2026-01-02 11:04:05')",
        "INSERT INTO mysql.db (Host, DB, User, Insert_priv) VALUES ('%', 'app', 'peer', 'Y')",
        "INSERT INTO mysql.tables_priv (Host, DB, User, Table_name, Table_priv) \
         VALUES ('%', 'app', 'peer', 't', 'Update,Delete')",
        "INSERT INTO mysql.global_grants (USER, HOST, PRIV, WITH_GRANT_OPTION) \
         VALUES ('peer', '%', 'BACKUP_ADMIN', 'N')",
    ] {
        rows(&mut session, sql);
    }
    assert!(!users.accounts().user_exists("peer", "%"));
    users
        .accounts()
        .update_all()
        .expect("the load runs on the domain's sessions");
    let loaded = tidb_privilege::load::cluster_image_from_registry(&users.accounts());
    let peer = loaded
        .users
        .iter()
        .find(|user| user.user == "peer")
        .expect("the peer's row loads");
    assert_eq!(peer.privileges, ["SELECT"]);
    assert_eq!(
        peer.password_last_changed.map(|time| time.to_string()).as_deref(),
        Some("2026-01-02 03:04:05")
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW GRANTS FOR 'peer'@'%'")),
        [
            ["GRANT SELECT ON *.* TO `peer`@`%`"],
            ["GRANT INSERT ON `app`.* TO `peer`@`%`"],
            ["GRANT UPDATE,DELETE ON `app`.`t` TO `peer`@`%`"],
            ["GRANT BACKUP_ADMIN ON *.* TO `peer`@`%`"],
        ]
    );
}

/// Go `LoadSysVarCacheLoop`: the first `rebuildSysVarCache` runs on the
/// domain's sessions before the loop starts, so the cache is the cluster's
/// when the loop's start returns -- no tick, no nudge.
#[test]
fn the_sysvar_loop_builds_the_cache_before_it_starts_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(307)).unwrap();
    rows(
        &mut session,
        "REPLACE INTO mysql.global_variables VALUES ('tidb_mem_quota_query', '536870913')",
    );
    let live = crate::GlobalSysvars::from_cluster_rows([]);
    let _reloader = tidb_domain_core::cluster_sysvar_seam::SysvarReloader::spawn(
        live.clone(),
        stack.factory.advanced_sys_session_pool(),
        Duration::from_secs(3600),
        tidb_domain_core::cluster_sysvar_seam::SysvarPublicationFence::default(),
    )
    .expect("the first rebuild runs on the domain's sessions");
    assert_eq!(live.get("tidb_mem_quota_query").unwrap(), "536870913");
}

/// Go `Domain.LoadSysVarCacheLoop` and `Domain.rebuildSysVarCache`:
/// `require_secure_transport.SetGlobal` runs with the fetched value on the
/// first load and every rebuild, even when the stored row has not changed.
/// The row/hook pairs are captured by `zzprobe/r01_transport`'s
/// `TestRebuildSecureTransport`, following Go `TestUpgradeSysvars`.
#[test]
fn test_rebuild_secure_transport() {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_tls::REQUIRE_SECURE_TRANSPORT.store(self.0, Ordering::SeqCst);
        }
    }
    let _restore = Restore(tidb_tls::REQUIRE_SECURE_TRANSPORT.load(Ordering::SeqCst));
    let (mut stack, _users) = cop_backed_stack();
    stack._sysvar_reloader.shutdown().unwrap();
    let mut session = stack.factory.open_session(session_context(1794)).unwrap();
    rows(
        &mut session,
        "REPLACE INTO mysql.global_variables VALUES ('require_secure_transport', 'ON')",
    );
    let live = crate::GlobalSysvars::new();
    tidb_tls::REQUIRE_SECURE_TRANSPORT.store(false, Ordering::SeqCst);
    let mut reloader = tidb_domain_core::cluster_sysvar_seam::SysvarReloader::spawn(
        live.clone(),
        stack.factory.advanced_sys_session_pool(),
        Duration::from_secs(3600),
        tidb_domain_core::cluster_sysvar_seam::SysvarPublicationFence::default(),
    )
    .unwrap();
    assert!(
        tidb_tls::REQUIRE_SECURE_TRANSPORT.load(Ordering::SeqCst),
        "LoadSysVarCacheLoop must apply the stored ON before returning"
    );
    assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
    reloader.shutdown().unwrap();
    let mut capture = Vec::new();
    for (stored, expected) in [("ON", true), ("OFF", false), ("ON", true)] {
        rows(
            &mut session,
            &format!("REPLACE INTO mysql.global_variables VALUES ('require_secure_transport','{stored}')"),
        );
        for unchanged in [false, true] {
            tidb_tls::REQUIRE_SECURE_TRANSPORT.store(!expected, Ordering::SeqCst);
            live.rebuild_sys_var_cache().unwrap();
            let hook = tidb_tls::REQUIRE_SECURE_TRANSPORT.load(Ordering::SeqCst);
            let cached = live.get("require_secure_transport").unwrap();
            assert_eq!(hook, expected);
            assert_eq!(cached, stored);
            capture.push(if unchanged {
                format!("R01 transport unchanged={stored} hook={hook}")
            } else {
                format!("R01 transport stored={stored} cached={cached} hook={hook}")
            });
        }
    }
    assert_eq!(
        capture.join("\n"),
        include_str!("../../../../testdata/r01_secure_transport_go.txt").trim()
    );
}

/// Go `LoadSysVarCacheLoop`: a peer's `SET GLOBAL` lands only as its
/// `mysql.global_variables` row, and this node's reload reads that table
/// through the domain's system sessions (`rebuildSysVarCache` ->
/// `fetchTableValues`), then serves the value.
#[test]
fn the_sysvar_reload_reads_a_peers_row_through_the_domains_sessions_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(305)).unwrap();
    // A peer's write: the row, with no SET GLOBAL on this node.
    rows(
        &mut session,
        "REPLACE INTO mysql.global_variables VALUES ('tidb_mem_quota_query', '536870912')",
    );
    let reloads = stack._sysvar_reloader.stats().reloads;
    stack._sysvar_reloader.waker().nudge();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._sysvar_reloader.stats().reloads == reloads {
        assert!(std::time::Instant::now() < deadline, "the sysvar reload did not run");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@global.tidb_mem_quota_query")),
        [["536870912"]]
    );
}

/// Go `boostJobScheduleForTest`: every TTL loop interval down to 100ms.
fn boost_ttl_job_schedule_for_test() {
    use tidb_ttl_ttlworker::config::{set_failpoint, Failpoint};
    for failpoint in [
        Failpoint::CheckJobTriggeredInterval,
        Failpoint::SyncTimer,
        Failpoint::CheckJobInterval,
        Failpoint::ResizeWorkersInterval,
        Failpoint::UpdateStatusTableCacheInterval,
        Failpoint::UpdateInfoSchemaCacheInterval,
        Failpoint::TaskManagerLoopInterval,
        Failpoint::CheckTaskInterval,
        Failpoint::GcInterval,
    ] {
        set_failpoint(failpoint, std::time::Duration::from_millis(100));
    }
}

/// Go `waitTTLJobFinished`: the table's last job is recorded, no job is
/// current, and its timer has no triggered event.
fn wait_ttl_job_finished(session: &mut Session, table_id: i64) {
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let status = displayed(rows(
            session,
            &format!(
                "select last_job_id, current_job_id, parent_table_id from mysql.tidb_ttl_table_status where table_id={table_id}"
            ),
        ));
        let Some(row) = status.first() else {
            continue;
        };
        // Go testkit prints NULL as `<nil>`; this harness displays `NULL`.
        if row[0] == "NULL" || row[1] != "NULL" {
            continue;
        }
        let event_status = displayed(rows(
            session,
            &format!(
                "select EVENT_STATUS from mysql.tidb_timers where TIMER_KEY='/tidb/ttl/physical_table/{}/{table_id}'",
                row[2]
            ),
        ));
        if event_status.first().is_some_and(|status| status[0] == "TRIGGER") {
            continue;
        }
        return;
    }
    let state = [
        "select * from mysql.tidb_ttl_table_status",
        "select * from mysql.tidb_timers",
        "select * from mysql.tidb_ttl_task",
        "select * from mysql.tidb_ttl_job_history",
    ]
    .map(|sql| format!("{sql}: {:?}", displayed(rows(session, sql))));
    panic!("timeout: {state:#?}");
}

/// Go `session.setRequestSource` with `SetTxnOption` and the DistSQL
/// builder's `SetFromSessionVars`: a user statement's requests carry its
/// label as their source (`external_<label>`), its coprocessor reads and
/// its transaction's prewrites alike.
#[test]
fn a_statements_requests_carry_its_label_as_their_source() {
    use tikv_client::proto::{coprocessor, kvrpcpb};
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE request_source (a INT PRIMARY KEY, b INT)");
    let sent: Arc<std::sync::Mutex<Vec<(&'static str, String)>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let any = request.as_any();
        let source = if let Some(request) = any.downcast_ref::<coprocessor::Request>() {
            ("cop", request.context.as_ref().map(|context| context.request_source.clone()))
        } else if let Some(request) = any.downcast_ref::<kvrpcpb::PrewriteRequest>() {
            ("prewrite", request.context.as_ref().map(|context| context.request_source.clone()))
        } else if let Some(request) = any.downcast_ref::<kvrpcpb::GetRequest>() {
            ("get", request.context.as_ref().map(|context| context.request_source.clone()))
        } else if let Some(request) = any.downcast_ref::<kvrpcpb::BatchGetRequest>() {
            ("get", request.context.as_ref().map(|context| context.request_source.clone()))
        } else {
            return;
        };
        recorded.lock().unwrap().push((source.0, source.1.unwrap_or_default()));
    })));
    rows(&mut session, "INSERT INTO request_source VALUES (1, 1), (2, 2)");
    rows(&mut session, "SELECT b FROM request_source WHERE b > 0");
    rows(&mut session, "SELECT b FROM request_source WHERE a = 1");
    // Go labels an EXECUTE with its prepared statement (`resolvePreparedStmt`).
    rows(&mut session, "PREPARE request_source_update FROM 'UPDATE request_source SET b = b + 1 WHERE b > ?'");
    rows(&mut session, "SET @request_source_bound = 0");
    rows(&mut session, "EXECUTE request_source_update USING @request_source_bound");
    stack.rpc.set_send_hook(None);
    let sent = sent.lock().unwrap().clone();
    assert!(sent.contains(&("prewrite", "external_Insert".to_owned())), "{sent:?}");
    assert!(sent.contains(&("prewrite", "external_Update".to_owned())), "{sent:?}");
    assert!(!sent.iter().any(|(_, source)| source == "external_Execute"), "{sent:?}");
    assert!(sent.contains(&("cop", "external_Select".to_owned())), "{sent:?}");
    // Go `GetSnapshotWithTS`: the point read's snapshot carries it as well.
    assert!(sent.contains(&("get", "external_Select".to_owned())), "{sent:?}");

    // Go's DDL worker: an ADD INDEX job's steps commit as `ddl_add_index`
    // (`prepareTxn`'s `setDDLSourceForDiagnosis`), and its backfill writes
    // the index with `ExplicitTypeDDL` beside it.
    let sent: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let submitted: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let reorg_meta: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let (recorded, recorded_submission, recorded_reorg) =
        (Arc::clone(&sent), Arc::clone(&submitted), Arc::clone(&reorg_meta));
    let reorg_table =
        tidb_tablecodec::table_key::gen_table_prefix(tidb_metadef::system::TI_DBDDLREORG_TABLE_ID);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        if let Some(request) = request.as_any().downcast_ref::<tikv_client::proto::kvrpcpb::PrewriteRequest>() {
            let source = request.context.as_ref().map(|context| context.request_source.clone()).unwrap_or_default();
            // Go's `NextGlobalID` meta key, memcomparable-encoded.
            if request.mutations.iter().any(|mutation| logical_rpc_key(&mutation.key).starts_with(b"mNextGlob")) {
                recorded_submission.lock().unwrap().push(source.clone());
            }
            if request.mutations.iter().any(|mutation| logical_rpc_key(&mutation.key).starts_with(&reorg_table)) {
                recorded_reorg.lock().unwrap().push(source.clone());
            }
            recorded.lock().unwrap().push(source);
        }
    })));
    rows(&mut session, "ALTER TABLE request_source ADD INDEX request_source_b (b)");
    stack.rpc.set_send_hook(None);
    let sent = sent.lock().unwrap().clone();
    assert!(sent.contains(&"internal_ddl_add_index".to_owned()), "{sent:?}");
    assert!(sent.contains(&"internal_ddl_add_index_ddl".to_owned()), "{sent:?}");
    assert!(!sent.iter().any(|source| source == "unknown"), "{sent:?}");
    // The worker's pooled sessions write the job's rows as `ddl`, and the
    // submitter allocates the job's id as `ddl` (Go `ddl.Start`'s context).
    assert!(sent.contains(&"internal_ddl".to_owned()), "{sent:?}");
    // The reorganization's handle rows go through the worker's pooled
    // sessions (`UpdateReorgMeta` on `sessPool`).
    let reorg_meta = reorg_meta.lock().unwrap().clone();
    assert!(
        !reorg_meta.is_empty() && !reorg_meta.iter().any(|source| source == "unknown"),
        "{reorg_meta:?}"
    );
    let submitted = submitted.lock().unwrap().clone();
    assert!(!submitted.is_empty() && submitted.iter().all(|source| source == "internal_ddl"), "{submitted:?}");

    // Go `advisoryLock`: GET_LOCK's own session runs under
    // `kv.InternalTxnOthers`.
    let sent: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let any = request.as_any();
        let context = if let Some(request) = any.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
            request.context.as_ref()
        } else if let Some(request) = any.downcast_ref::<kvrpcpb::PrewriteRequest>() {
            request.context.as_ref()
        } else {
            return;
        };
        recorded
            .lock()
            .unwrap()
            .push(context.map(|context| context.request_source.clone()).unwrap_or_default());
    })));
    rows(&mut session, "SELECT GET_LOCK('request_source_lock', 1)");
    rows(&mut session, "SELECT RELEASE_LOCK('request_source_lock')");
    stack.rpc.set_send_hook(None);
    let sent = sent.lock().unwrap().clone();
    assert!(sent.contains(&"internal_others".to_owned()), "{sent:?}");

    // Go `LoadExprPushdownBlacklist`: the reload reads its table through
    // restricted SQL under `kv.InternalTxnSysVar`.
    let sent: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let any = request.as_any();
        let context = if let Some(request) = any.downcast_ref::<coprocessor::Request>() {
            request.context.as_ref()
        } else if let Some(request) = any.downcast_ref::<kvrpcpb::GetRequest>() {
            request.context.as_ref()
        } else if let Some(request) = any.downcast_ref::<kvrpcpb::BatchGetRequest>() {
            request.context.as_ref()
        } else {
            return;
        };
        recorded
            .lock()
            .unwrap()
            .push(context.map(|context| context.request_source.clone()).unwrap_or_default());
    })));
    rows(&mut session, "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST");
    stack.rpc.set_send_hook(None);
    let sent = sent.lock().unwrap().clone();
    assert!(sent.contains(&"internal_others".to_owned()), "{sent:?}");
    assert!(!sent.iter().any(|source| source == "unknown"), "{sent:?}");
    let sent: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let any = request.as_any();
        let (kind, key, context): (&str, Vec<u8>, _) = if let Some(request) = any.downcast_ref::<kvrpcpb::PrewriteRequest>() {
            ("prewrite", request.mutations.first().map(|m| m.key.clone()).unwrap_or_default(), request.context.as_ref())
        } else if let Some(request) = any.downcast_ref::<coprocessor::Request>() {
            ("cop", request.ranges.first().map(|r| r.start.to_vec()).unwrap_or_default(), request.context.as_ref())
        } else {
            return;
        };
        let source = context.map(|context| context.request_source.clone()).unwrap_or_default();
        recorded.lock().unwrap().push(format!("{kind} {} {source}", String::from_utf8_lossy(&key[..key.len().min(12)])));
    })));
    rows(&mut session, "ANALYZE TABLE request_source");
    stack.rpc.set_send_hook(None);
    // Go's analyze: the stats writes under `StatsCtx`, the scans under
    // `kv.InternalTxnStats`, and the auto IDs under `kv.InternalTxnMeta`.
    let sent = sent.lock().unwrap().clone();
    assert!(sent.iter().any(|request| request.ends_with(" internal_StatsForegroundPriority")), "{sent:?}");
    assert!(!sent.iter().any(|request| request.ends_with(" unknown")), "{sent:?}");

    // Go bindinfo: the binding's rows are written under
    // `kv.InternalTxnBindInfo`.
    let sent: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        if let Some(request) = request.as_any().downcast_ref::<tikv_client::proto::kvrpcpb::PrewriteRequest>() {
            recorded
                .lock()
                .unwrap()
                .push(request.context.as_ref().map(|context| context.request_source.clone()).unwrap_or_default());
        }
    })));
    rows(
        &mut session,
        "CREATE GLOBAL BINDING FOR SELECT b FROM request_source USING SELECT b FROM request_source",
    );
    stack.rpc.set_send_hook(None);
    let sent = sent.lock().unwrap().clone();
    assert!(sent.contains(&"internal_others".to_owned()), "{sent:?}");
    assert!(!sent.iter().any(|source| source == "unknown"), "{sent:?}");

    // Go `SQLBindExec.reloadBindings`: the reload reads `mysql.bind_info`
    // on the binding maintenance session, under `kv.InternalTxnBindInfo`.
    let sent: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let any = request.as_any();
        let context = if let Some(request) = any.downcast_ref::<coprocessor::Request>() {
            request.context.as_ref()
        } else if let Some(request) = any.downcast_ref::<kvrpcpb::ScanRequest>() {
            request.context.as_ref()
        } else {
            return;
        };
        recorded
            .lock()
            .unwrap()
            .push(context.map(|context| context.request_source.clone()).unwrap_or_default());
    })));
    rows(&mut session, "ADMIN RELOAD BINDINGS");
    stack.rpc.set_send_hook(None);
    let sent = sent.lock().unwrap().clone();
    assert!(sent.contains(&"internal_others".to_owned()), "{sent:?}");
    assert!(!sent.iter().any(|source| source == "unknown"), "{sent:?}");
}

/// `tidb_enable_resource_control` reaches client-rust's resource-control
/// switch however early the first global-variable load runs. The server's
/// `setGlobalVars` writes `port` before the domain exists. Go binds the
/// switch in `tikv_driver.go`'s `init`, so a point read is still priced by
/// the resource-group controller. Captured from a Go tidb-server over the
/// wire: `explain analyze select * from t where id=1` reports `RU:0.47`.
#[test]
fn resource_control_prices_requests_after_an_early_sysvar_load_like_go() {
    tidb_domain::global_sysvars::GlobalSysvars::new().set_sys_var("port", "4000".to_owned());
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1149)).unwrap();
    for sql in [
        "USE test",
        "create table t (id int primary key, uk int, b int, unique key(uk))",
        "insert into t values (1,10,0),(2,20,0),(3,30,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let plan = displayed(rows(&mut session, "explain analyze select * from t where id=1"));
    assert!(plan[0][5].contains(", RU:0.47, Get:{"), "{}", plan[0][5]);
}

/// The server's `setGlobalVars` writes a registry no SetGlobal hook reads
/// yet (Go runs them only from the domain's `LoadSysVarCacheLoop`); the
/// domain the stack builds arms them (`initDomainSysVars`), so
/// `tidb_enable_resource_control`'s ON reaches the client and a point read is
/// priced at Go's `RU:0.47`.
#[test]
fn the_stack_arms_the_set_global_hooks_like_gos_domain() {
    let config = tidb_domain_core::node_config::NodeConfig::with_defaults(
        |config| {
            config.store = tidb_config::store::StoreType("unistore".to_owned());
            config.port = 0;
        },
        tidb_domain_core::node_config::NodeOptions::default(),
    )
    .expect("node config");
    let globals = crate::GlobalSysvars::before_domain();
    globals.set_sys_var("port", "4000".to_owned());
    let users = Arc::new(
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store")
            .with_global_vars(globals),
    );
    let stack = unistore_cluster_session_stack(&config, &users, None, None).expect("unistore stack");
    let mut session = stack.factory.open_session(session_context(1154)).unwrap();
    for sql in [
        "USE test",
        "create table t (id int primary key, uk int, b int, unique key(uk))",
        "insert into t values (1,10,0),(2,20,0),(3,30,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let plan = displayed(rows(&mut session, "explain analyze select * from t where id=1"));
    assert!(plan[0][5].contains(", RU:0.47, Get:{"), "{}", plan[0][5]);
    // A later `SET GLOBAL` reaches its hook through the same registry.
    rows(&mut session, "set global tidb_enable_foreign_key = OFF");
    assert_eq!(displayed(rows(&mut session, "select @@global.tidb_enable_foreign_key")), [["0"]]);
    rows(&mut session, "set global tidb_enable_foreign_key = ON");
}

fn mask_durations(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find(|c: char| c.is_ascii_digit()) {
        out.push_str(&rest[..start]);
        let number = rest[start..]
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .map_or(rest.len(), |end| start + end);
        match ["ns", "µs", "ms", "s"].into_iter().find(|unit| rest[number..].starts_with(unit)) {
            Some(unit) => {
                out.push('D');
                rest = &rest[number + unit.len()..];
            }
            None => {
                out.push_str(&rest[start..number]);
                rest = &rest[number..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Go `SessionVars.InTxn` under autocommit=0: the pending transaction a
/// statement enters (`PrepareTxnCtx`) sets `ServerStatusInTrans` only once
/// it activates, so a memory-table read leaves it clear on the wire and in
/// the process row's `State`; and `ExecStmt.Exec` republishes the process
/// info after `buildExecutor`, so a statement whose build activates the
/// transaction shows `in transaction` while it runs (Go tidb-server
/// captures).
#[test]
fn server_status_in_trans_follows_gos_activation() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1193)).unwrap();
    for sql in [
        "CREATE TABLE test.ps2 (a INT)",
        "INSERT INTO test.ps2 VALUES (1)",
        "SET autocommit = 0",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let in_trans = |session: &crate::Session| {
        crate::domain::wire_status::session_status(session).bits()
            & tidb_variable::wire_status::SERVER_STATUS_IN_TRANS
            != 0
    };
    let own_state = "SELECT STATE FROM information_schema.PROCESSLIST WHERE ID = CONNECTION_ID()";
    assert_eq!(displayed(rows(&mut session, own_state)), [[""]]);
    rows(&mut session, "SELECT COUNT(*) FROM information_schema.ENGINES");
    assert!(!in_trans(&session));
    assert_eq!(displayed(rows(&mut session, own_state)), [[""]]);
    session.node_conn().execute_write("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT p.STATE FROM information_schema.PROCESSLIST p, test.ps2 WHERE p.ID = CONNECTION_ID()",
        )),
        [["in transaction"]]
    );
    assert!(in_trans(&session));
    session.node_conn().execute_write("ROLLBACK").unwrap();
    assert!(!in_trans(&session));
}

/// Go `SetFromSessionVars`: every statement's coprocessor requests take
/// `tidb_distsql_scan_concurrency`, a write's read among them. Go tidb-server
/// capture over four regions: an UPDATE's reader runs four tasks at once by
/// default and two under a concurrency of 2, as the same read does.
#[test]
fn a_writes_read_takes_the_distsql_scan_concurrency_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(163)).unwrap();
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE t (a INT PRIMARY KEY, b INT)");
    let values = (1..=100).map(|x| format!("({x}, {x})")).collect::<Vec<_>>().join(", ");
    rows(&mut session, &format!("INSERT INTO t VALUES {values}"));
    rows(&mut session, "SPLIT TABLE t BY (25), (50), (75)");
    let concurrency = |session: &mut Session, sql: &str| {
        let explained = displayed(rows(session, &format!("EXPLAIN ANALYZE {sql}")));
        let info = explained
            .iter()
            .find(|row| row[0].contains("TableReader"))
            .unwrap_or_else(|| panic!("{explained:?}"))[5]
            .clone();
        assert!(info.contains("num: 4"), "{info}");
        info.split("max_distsql_concurrency: ")
            .nth(1)
            .unwrap()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
    };
    let read = "SELECT * FROM t WHERE b > 0";
    let write = "UPDATE t SET b = b WHERE b > 0";
    assert_eq!((concurrency(&mut session, read), concurrency(&mut session, write)), ("1".to_owned(), "4".to_owned()));
    rows(&mut session, "SET tidb_distsql_scan_concurrency = 2");
    assert_eq!((concurrency(&mut session, read), concurrency(&mut session, write)), ("2".to_owned(), "2".to_owned()));
}

/// Go `TestUpgradeSysvars`' direct table write followed by cache rebuild,
/// plus zzprobe/r01's committed value capture. No reload-loop pre-read.
#[test]
fn test_upgrade_sysvars() {
    let (mut stack, _users) = cop_backed_stack();
    // Only the explicit rebuild below may discover the committed row.
    stack._sysvar_reloader.shutdown().unwrap();
    let mut session = stack.factory.open_session(session_context(1793)).unwrap();
    rows(
        &mut session,
        "REPLACE INTO mysql.global_variables VALUES ('tidb_executor_concurrency','12')",
    );
    stack.factory.global_vars.rebuild_sys_var_cache().unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT @@global.tidb_executor_concurrency"
        )),
        [["12"]]
    );
    rows(
        &mut session,
        "REPLACE INTO mysql.global_variables VALUES ('tidb_executor_concurrency','16')",
    );
    stack.factory.global_vars.rebuild_sys_var_cache().unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT @@global.tidb_executor_concurrency"
        )),
        [["16"]]
    );
    for (name, stored, expected) in [
        ("tidb_enable_noop_functions", "0", "OFF"),
        ("rpl_semi_sync_slave_enabled", "", "OFF"),
        ("tidb_executor_concurrency", "999", "256"),
        ("tidb_enable_noop_functions", "SOMEVAL", "OFF"),
    ] {
        rows(
            &mut session,
            &format!("REPLACE INTO mysql.global_variables VALUES ('{name}','{stored}')"),
        );
        stack.factory.global_vars.rebuild_sys_var_cache().unwrap();
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SHOW GLOBAL VARIABLES LIKE '{name}'")
            )),
            [[name, expected]]
        );
    }
}

/// Go `TestMaintenanceMode` (`pkg/executor/import_into_test.go`): in NextGen
/// maintenance mode a user statement submits no DDL job and no import, while
/// internal SQL, local temporary tables, DML and releasing table locks still
/// work; the classic kernel ignores the setting.
#[test]
fn test_maintenance_mode() {
    const DDL_UNAVAILABLE: &str = "[executor:8277]During the maintenance window, DDL is currently unavailable. It will be available after maintenance finishes";
    const IMPORT_UNAVAILABLE: &str = "[executor:8277]During the maintenance window, IMPORT INTO is currently unavailable. It will be available after maintenance finishes";
    let previous = tidb_config::config_tree::config::get_global_config();
    struct Restore(Arc<tidb_config::config_tree::config::Config>);
    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_config::config_tree::config::store_global_config((*self.0).clone());
        }
    }
    let _restore = Restore(previous);
    let set_maintenance = |enabled: bool| {
        tidb_config::config_tree::config::update_global(|config| config.maintenance_mode = enabled);
    };
    set_maintenance(true);
    // Startup's restricted DDL must work even when user job admission is disabled.
    let (stack, _users) = cop_backed_stack();
    let mut tk = testkit(&stack, 86);
    tk.must_exec("CREATE DATABASE IF NOT EXISTS test");
    tk.must_exec("use test");
    if !tidb_config_kerneltype::is_next_gen() {
        // The runtime guards are NextGen-only.
        tk.must_exec("create table maintenance_classic (a int)");
        tk.must_exec("alter table maintenance_classic add column b int");
        tk.must_exec("drop table maintenance_classic");
        return;
    }
    let mut internal = stack.factory.open_system_session().expect("system session opens");
    for sql in [
        "create table test.starter_target (a int)",
        "create table test.starter_source (a int)",
        "alter table test.starter_source add column upgrade_column int",
        "alter table test.starter_source drop column upgrade_column",
    ] {
        internal.node_conn().execute_internal(sql).unwrap();
    }

    // user DDL
    for sql in [
        "create table starter_rejected (a int)",
        "alter table starter_target add column b int",
        "alter table starter_target add column b int, add column c int",
        "drop table starter_target",
        "create database starter_rejected_db",
        "create global temporary table starter_global_temp (a int) on commit delete rows",
        // Go's `explain analyze alter table ...` case is left out: Rust
        // refuses EXPLAIN ANALYZE of DDL ("not supported yet").
    ] {
        tk.must_contain_err_msg(sql, DDL_UNAVAILABLE);
        tk.must_get_err_code(sql, 8277);
    }
    tk.must_query("show tables like 'starter_rejected'").check_empty();
    tk.must_query("select count(*) from information_schema.columns where table_schema = 'test' and table_name = 'starter_target'")
        .check(["1"]);
    tk.must_exec("prepare starter_ddl from 'create table starter_prepared (a int)'");
    tk.must_contain_err_msg("execute starter_ddl", DDL_UNAVAILABLE);
    tk.must_exec("deallocate prepare starter_ddl");
    tk.must_query("show tables like 'starter_prepared'").check_empty();

    // local temporary DDL and DML
    tk.must_exec("create temporary table starter_temp (a int)");
    tk.must_exec("insert into starter_temp values (1)");
    tk.must_query("select * from starter_temp").check(["1"]);
    tk.must_exec("drop temporary table starter_temp");
    tk.must_exec("insert into starter_source values (1)");
    tk.must_exec("update starter_source set a = 2");
    tk.must_query("select * from starter_source").check(["2"]);
    tk.must_exec("delete from starter_source");

    // release existing table locks
    tidb_config::config_tree::config::update_global(|config| {
        config.enable_table_lock = true;
        config.delay_clean_table_lock = 0;
    });
    for close_session in [false, true] {
        set_maintenance(false);
        let mut lock_tk = stack.factory.open_session(session_context(87)).expect("session opens");
        rows(&mut lock_tk, "use test");
        rows(&mut lock_tk, "lock tables starter_target write");
        assert_eq!(lock_tk.get_all_table_locks().len(), 1);
        tk.must_contain_err_msg("insert into starter_target values (1)", "Table 'starter_target' was locked");
        set_maintenance(true);
        assert!(DDL_UNAVAILABLE.ends_with(&query_error(&mut lock_tk, "lock tables starter_source write").1));
        // The held lock survives the refused LOCK TABLES.
        assert_eq!(lock_tk.get_all_table_locks().len(), 1);
        tk.must_contain_err_msg("insert into starter_target values (1)", "Table 'starter_target' was locked");
        if close_session {
            drop(lock_tk);
        } else {
            rows(&mut lock_tk, "unlock tables");
            assert!(lock_tk.get_all_table_locks().is_empty());
        }
        tk.must_exec("insert into starter_target values (1)");
        tk.must_exec("delete from starter_target");
    }

    // rejected table lock preserves session state
    let mut lock_tk = stack.factory.open_session(session_context(88)).expect("session opens");
    rows(&mut lock_tk, "use test");
    assert!(DDL_UNAVAILABLE.ends_with(&query_error(&mut lock_tk, "lock tables starter_target write").1));
    assert!(lock_tk.get_all_table_locks().is_empty());
    tk.must_exec("insert into starter_target values (1)");
    tk.must_exec("delete from starter_target");
    rows(&mut lock_tk, "create temporary table starter_lock_temp (a int)");
    rows(&mut lock_tk, "insert into starter_lock_temp values (1)");
    assert_eq!(displayed(rows(&mut lock_tk, "select * from starter_lock_temp")), [["1"]]);
    rows(&mut lock_tk, "drop temporary table starter_lock_temp");
    rows(&mut lock_tk, "unlock tables");

    // IMPORT INTO
    for sql in [
        "import into starter_target from 's3://bucket/disabled.csv'",
        "import into starter_target from 's3://bucket/disabled.csv' with detached",
    ] {
        tk.must_contain_err_msg(sql, IMPORT_UNAVAILABLE);
        tk.must_get_err_code(sql, 8277);
    }
    // No task was submitted (this stack starts no DXF task manager, which
    // Go's `show import jobs` check reads).
    tk.must_query("select count(*) from mysql.tidb_global_task").check(["0"]);
}

/// Go `TestStatementDurationMetrics` (`pkg/server/conn_test.go`), its
/// statement half: each user statement of a command is one sample of its own
/// type in `tidb_server_handle_query_duration_seconds` (multi-statement
/// timing, prepared executes, no sample for a parse or compile error), and a
/// restricted SQL operation is one `internal` sample in both duration
/// histograms.
#[test]
fn test_statement_duration_metrics() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(89)).expect("session opens");
    rows(&mut tk, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut tk, "use test");
    rows(&mut tk, "create table statement_duration (id int primary key, v int)");
    let group = tk.active_resource_group().to_owned();
    let statement = |sql_type: &str| {
        tidb_metrics::server::QUERY_DURATION_HISTOGRAM
            .with_label_values(&[sql_type, "", &group])
            .get_sample_count()
    };
    let command = |sql_type: &str| {
        tidb_metrics::server::COMMAND_DURATION_HISTOGRAM
            .with_label_values(&[sql_type, "", &group])
            .get_sample_count()
    };

    // multi statement timing: one sample per statement, by its own type.
    let (insert_before, update_before) = (statement("Insert"), statement("Update"));
    for sql in tk
        .split_statements(
            "insert into statement_duration values (1, 0); update statement_duration set v = v + 1 where id = 1",
            true,
        )
        .unwrap()
    {
        rows(&mut tk, &sql);
    }
    assert!(tk.vars.in_multi_stmts);
    // A multi-statement text the capability check refuses is still one
    // (Go labels the refused command `MultiStmt`); a single one is not.
    rows(&mut tk, "set tidb_multi_statement_mode = OFF");
    assert!(tk.split_statements("select 1; select 2", false).is_err());
    assert!(tk.vars.in_multi_stmts);
    tk.split_statements("select 1", false).unwrap();
    assert!(!tk.vars.in_multi_stmts);
    assert_eq!(statement("Insert"), insert_before + 1);
    assert_eq!(statement("Update"), update_before + 1);
    assert_eq!(
        tidb_metrics::server::QUERY_DURATION_HISTOGRAM
            .with_label_values(&["Insert", "", &group])
            .get_sample_count(),
        insert_before + 1
    );

    // prepared execute: a sample per execution, none for the prepare.
    let select_before = statement("Select");
    rows(&mut tk, "prepare duration_stmt from 'select v from statement_duration where id = 1'");
    rows(&mut tk, "execute duration_stmt");
    rows(&mut tk, "execute duration_stmt");
    rows(&mut tk, "deallocate prepare duration_stmt");
    assert_eq!(statement("Select"), select_before + 2);

    // parse and compile errors: no sample.
    let select_before = statement("Select");
    query_error(&mut tk, "select from");
    query_error(&mut tk, "select * from statement_duration_missing");
    assert_eq!(statement("Select"), select_before);

    // restricted SQL: an internal sample in both histograms, no user one.
    let context = tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_OTHERS,
    );
    let (internal_statement, internal_command) = (statement("internal"), command("internal"));
    let select_before = statement("Select");
    let cur: tidb_sqlexec::OptionFuncAlias = Arc::new(tidb_sqlexec::exec_option_use_cur_session);
    tk.execute_restricted_sql(&context, &[cur], "select v from test.statement_duration", &[])
        .unwrap();
    assert_eq!(statement("internal"), internal_statement + 1);
    assert_eq!(command("internal"), internal_command + 1);
    assert_eq!(statement("Select"), select_before);
}

/// The session variables executors read through `StmtContext::sysvar`
/// follow the session (Go reads `SessionVars` fields): before, these four
/// read as their defaults whatever the session set.
#[test]
fn statement_context_answers_executor_session_vars() {
    use tidb_exprctx::Columns as _;
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(90)).expect("session opens");
    let read = |tk: &Session, name: &str| tk.statement_context_of(false, false, true).sysvar(None, name);
    assert_eq!(read(&tk, "tiflash_fine_grained_shuffle_batch_size"), Some(Datum::UInt(8192)));
    assert_eq!(read(&tk, "tidb_enable_parallel_hashagg_spill"), Some(Datum::Int(1)));
    assert_eq!(read(&tk, "tidb_track_aggregate_memory_usage"), Some(Datum::Int(1)));
    rows(&mut tk, "set tiflash_fine_grained_shuffle_batch_size = 1024");
    rows(&mut tk, "set tidb_enable_parallel_hashagg_spill = 0");
    rows(&mut tk, "set tidb_track_aggregate_memory_usage = 0");
    rows(&mut tk, "set tidb_slow_query_file = 'other-slow.log'");
    assert_eq!(read(&tk, "tiflash_fine_grained_shuffle_batch_size"), Some(Datum::UInt(1024)));
    assert_eq!(read(&tk, "tidb_enable_parallel_hashagg_spill"), Some(Datum::Int(0)));
    assert_eq!(read(&tk, "tidb_track_aggregate_memory_usage"), Some(Datum::Int(0)));
    assert_eq!(read(&tk, "tidb_slow_query_file"), Some(Datum::Bytes(b"other-slow.log".to_vec())));
    assert_eq!(
        read(&tk, "tidb_slow_query_file"),
        tk.statement_context_of(true, false, false).sysvar(None, "tidb_slow_query_file")
    );
}


/// Go `TestLoadDataMissingDefaults` (9919555d3a5): a field missing from a
/// LOAD DATA row takes its column's default (a constant, an expression
/// evaluated per row, CURRENT_TIMESTAMP, or a NOT NULL column's zero value),
/// while an explicit NULL or empty field stays what it is.
#[test]
fn test_load_data_missing_defaults() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1937)).unwrap();
    let text = |session: &mut Session, sql: &str| displayed(rows(session, sql));
    let load = |session: &mut Session, data: &[u8], columns: &str| {
        let sql = format!("load data local infile '/tmp/missing_defaults.csv' into table missing_defaults {columns}");
        let stmt = session.parse_at_statement_boundary(&sql).unwrap();
        assert!(session.node_conn().execute_stmt(&sql, &stmt).unwrap().is_none(), "{sql}");
        assert!(session.local_infile_request().is_some(), "{sql} asks for the client's file");
        session.node_conn().finish_local_infile(data).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    };
    for sql in [
        "CREATE DATABASE IF NOT EXISTS test",
        "USE test",
        "set time_zone='+00:00'",
        "create table missing_defaults (id int primary key, a int not null default 100, \
         b varchar(20) not null default 'fallback', d date not null default '2000-01-01', \
         ts timestamp not null default '2000-01-01 01:02:03', n int, nullable_default int default 7, \
         expr_default varchar(36) default (uuid()), generated_value int as (a+1), \
         current_ts timestamp not null default current_timestamp)",
    ] {
        rows(&mut session, sql);
    }
    load(&mut session, b"1\n2\n", "");
    assert_eq!(
        text(&mut session, "select id,a,b,d,ts,n,nullable_default,length(expr_default),generated_value from missing_defaults order by id"),
        [
            ["1", "100", "fallback", "2000-01-01", "2000-01-01 01:02:03", "NULL", "7", "36", "101"],
            ["2", "100", "fallback", "2000-01-01", "2000-01-01 01:02:03", "NULL", "7", "36", "101"],
        ]
    );
    assert_eq!(text(&mut session, "select count(distinct expr_default) from missing_defaults"), [["2"]]);
    assert_eq!(
        text(&mut session, "select count(*) from missing_defaults where current_ts between current_timestamp - interval 1 minute and current_timestamp"),
        [["2"]]
    );
    // A mapped missing field gets its own default, independently of
    // physical column order.
    rows(&mut session, "set @missing=123");
    load(&mut session, b"3\n", "(id,@missing,b,a) set n=ifnull(@missing,9)");
    assert_eq!(
        text(&mut session, "select a,b,n,generated_value from missing_defaults where id=3"),
        [["100", "fallback", "9", "101"]]
    );
    // Explicit NULL and empty input remain different from an absent field.
    load(&mut session, b"4\t\\N\t\n", "(id,nullable_default,b)");
    assert_eq!(text(&mut session, "select nullable_default,b='' from missing_defaults where id=4"), [["NULL", "1"]]);
    rows(&mut session, "drop table missing_defaults");
    rows(&mut session, "create table missing_defaults (id int primary key, a int not null, b varchar(10) not null, d date not null, seq int auto_increment unique)");
    load(&mut session, b"1\n", "");
    assert_eq!(text(&mut session, "select id,a,b='',d,seq from missing_defaults"), [["1", "0", "1", "0000-00-00", "1"]]);
}
