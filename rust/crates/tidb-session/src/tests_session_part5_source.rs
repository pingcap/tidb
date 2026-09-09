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

//! Source-backed carriers for manifest batch `b148`, `pkg/session.part5`.
//!
//! This is the deterministic items 181--240 of the upstream `pkg/session`
//! test inventory. The pure parser/result-metadata checks and the focused
//! local-temporary-table DML checks run against the current Rust session. The
//! other declarations remain explicit ignored carriers where the Go test owns
//! a storage, Domain, failpoint, bootstrap, protocol, or internal transaction
//! seam that this crate does not expose.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::{Session, StmtOutput};

/// `pkg/session/test/session_test.go:512::TestResultField`.
#[test]
fn test_result_field() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (id INT)").unwrap();
    session.run("INSERT INTO t VALUES (1),(2)").unwrap();
    let StmtOutput::Rows { columns, rows } =
        session.run_with_columns("SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("COUNT must return a result set");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::LongLong);
    assert_eq!(columns[0].1.flen(), 21);
}

/// `pkg/session/test/session_test.go:533::TestResultType`.
#[test]
fn test_result_type() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT CAST(NULL AS CHAR(30))")
        .unwrap()
    else {
        panic!("CAST must return a result set");
    };
    assert!(rows[0][0].is_null());
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::VarString);
}

/// `pkg/session/test/session_test.go:547::TestFieldText`.
#[test]
fn test_field_text() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT)").unwrap();
    for (sql, expected) in [
        ("SELECT DISTINCT(a) FROM t", "a"),
        ("SELECT (1)", "1"),
        ("SELECT (1+1)", "(1+1)"),
        ("SELECT a FROM t", "a"),
        ("SELECT ((a+1)) FROM t", "((a+1))"),
    ] {
        let StmtOutput::Rows { columns, .. } = session.run_with_columns(sql).unwrap() else {
            panic!("{} must return a result set", sql);
        };
        assert_eq!(columns[0].0, expected, "field name for {sql}");
    }
}

/// `pkg/session/test/session_test.go:674::TestSQLModeOp`.
#[test]
fn test_sql_mode_op() {
    use tidb_mysql::{
        ModeAllowInvalidDates, ModeNoBackslashEscapes, ModeOnlyFullGroupBy, delete_sql_mode,
        set_sql_mode,
    };

    let mode = ModeNoBackslashEscapes | ModeOnlyFullGroupBy;
    assert_eq!(delete_sql_mode(mode, ModeAllowInvalidDates), mode);
    assert_eq!(
        delete_sql_mode(mode, ModeNoBackslashEscapes),
        ModeOnlyFullGroupBy
    );
    assert_eq!(set_sql_mode(mode, ModeOnlyFullGroupBy), mode);
    assert_eq!(
        set_sql_mode(mode, ModeAllowInvalidDates),
        ModeNoBackslashEscapes | ModeOnlyFullGroupBy | ModeAllowInvalidDates
    );
}

/// `pkg/session/test/session_test.go:253::TestPrepareZero`.
#[test]
fn test_prepare_zero() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (v TIMESTAMP)").unwrap();
    session
        .run("PREPARE s1 FROM 'INSERT INTO t (v) VALUES (?)'")
        .unwrap();
    session.run("SET @v1 = '0'").unwrap();
    assert!(session.run("EXECUTE s1 USING @v1").is_err());
    session.run("SET @v2 = '0000-00-00 00:00:00'").unwrap();
    session.run("SET @orig_sql_mode = @@sql_mode").unwrap();
    session.run("SET @@sql_mode = ''").unwrap();
    session.run("EXECUTE s1 USING @v2").unwrap();
    assert_eq!(
        row_text(session.run("SELECT v FROM t")),
        [["0000-00-00 00:00:00"]]
    );
    session.run("SET @@sql_mode = @orig_sql_mode").unwrap();
}

/// `pkg/session/test/session_test.go:270::TestPrimaryKeyAutoIncrement`.
#[test]
fn test_primary_key_auto_increment() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name VARCHAR(255) UNIQUE NOT NULL, status INT)")
        .unwrap();
    session
        .run_with_params(
            "INSERT INTO t (name) VALUES (?)",
            &[tidb_datatype::Datum::new_string("abc")],
        )
        .unwrap();
    let id = session.last_insert_id();
    assert_ne!(id, 0);
    assert_eq!(
        row_text(session.run("SELECT id, name, status FROM t")),
        [[id.to_string(), "abc".to_owned(), "NULL".to_owned()]]
    );
    session
        .run_with_params(
            "UPDATE t SET name = 'abc', status = ? WHERE id = ?",
            &[tidb_datatype::Datum::Int(1), tidb_datatype::Datum::UInt(id)],
        )
        .unwrap();
    assert_eq!(row_text(session.run("SELECT status FROM t")), [["1"]]);
}

/// `pkg/session/test/session_test.go:295::TestParseWithParams`.
#[test]
fn test_parse_with_params() {
    let mut session = Session::new();
    assert_eq!(session.parameter_count("SELECT 4").unwrap(), 0);
    assert_eq!(session.parameter_count("SELECT ?, ?").unwrap(), 2);
    let StmtOutput::Rows { rows, .. } = session
        .run_with_params("SELECT ? + 1", &[tidb_datatype::Datum::Int(3)])
        .unwrap()
    else {
        panic!("parameterized SELECT must return rows");
    };
    assert_eq!(
        rows.into_iter()
            .map(|row| row
                .iter()
                .map(crate::tests_support::cell_text)
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        [["4".to_owned()]]
    );
    assert!(session.run_with_params("SELECT ?", &[]).is_err());
    assert!(session.parameter_count("SELECT").is_err());
}

/// `pkg/session/test/temporarytabletest/temporary_table_test.go:35::TestLocalTemporaryTableUpdate`.
#[test]
fn test_local_temporary_table_update() {
    let mut session = Session::new();
    session
        .run("CREATE TEMPORARY TABLE tmp1 (id INT PRIMARY KEY, u INT UNIQUE, v INT)")
        .unwrap();
    session
        .run("INSERT INTO tmp1 VALUES (1,101,1001),(2,102,1002),(3,103,1003)")
        .unwrap();
    session
        .run("UPDATE tmp1 SET v = v + 1000 WHERE id IN (1,3)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT id,u,v FROM tmp1 ORDER BY id")),
        [
            ["1", "101", "2001"],
            ["2", "102", "1002"],
            ["3", "103", "2003"]
        ]
    );

    session.run("BEGIN").unwrap();
    session
        .run("UPDATE tmp1 SET id = id + 1 WHERE id IN (1,2)")
        .unwrap_err();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM tmp1 ORDER BY id")),
        [["1"], ["2"], ["3"]]
    );
}

/// `pkg/session/test/temporarytabletest/temporary_table_test.go:226::TestLocalTemporaryTableDelete`.
#[test]
fn test_local_temporary_table_delete() {
    let mut session = Session::new();
    session
        .run("CREATE TEMPORARY TABLE tmp1 (id INT PRIMARY KEY, u INT UNIQUE, v INT)")
        .unwrap();
    session
        .run("INSERT INTO tmp1 VALUES (1,101,1001),(2,102,1002),(3,103,1003)")
        .unwrap();
    session.run("BEGIN").unwrap();
    session
        .run("DELETE FROM tmp1 WHERE u IN (101,103)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM tmp1 ORDER BY id")),
        [["2"]]
    );
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM tmp1 ORDER BY id")),
        [["1"], ["2"], ["3"]]
    );
    session.run("DELETE FROM tmp1 WHERE id = 2").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM tmp1 ORDER BY id")),
        [["1"], ["3"]]
    );
}

/// `pkg/session/test/tidb_test.go:30::TestParseErrorWarn`.
#[test]
fn test_parse_error_warn() {
    let parsed = tidb_parser::parse_with_warnings("SELECT /*+ adf */ 1").unwrap();
    assert_eq!(parsed.warnings.len(), 1);
    assert!(tidb_parser::parse("SELECT").is_err());
}

/// `pkg/session/test/tidb_test.go:44::TestKeysNeedLock`.
#[test]
#[ignore = "go-parity-gap: TiDB tablecodec key classification and transaction lock flags are not transcreated"]
fn test_keys_need_lock() {}

/// `pkg/session/test/txn/txn_test.go:40::TestAutocommit`.
#[test]
fn test_autocommit() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    assert_eq!(session.status_text(), "autocommit");
    session.run("SET autocommit = 0").unwrap();
    assert_eq!(session.status_text(), "");
    session.run("INSERT INTO t VALUES (1)").unwrap();
    assert!(session.in_transaction());
    session.run("COMMIT").unwrap();
    assert!(!session.in_transaction());
    session.run("SET autocommit = 1").unwrap();
    assert_eq!(session.status_text(), "autocommit");
}

macro_rules! ignored_go_test {
    ($name:ident, $source:literal, $reason:literal) => {
        #[doc = $source]
        #[test]
        #[ignore = $reason]
        fn $name() {}
    };
}

ignored_go_test!(
    test_main_privileges,
    "Go `pkg/session/test/privileges/main_test.go:29::TestMain`.",
    "go-parity-gap: Go TestMain/common test harness is not a Rust behavior surface"
);
ignored_go_test!(
    test_skip_with_grant,
    "Go `pkg/session/test/privileges/privileges_test.go:26::TestSkipWithGrant`.",
    "go-parity-gap: package-global SkipWithGrant and authentication tables are not transcreated"
);
ignored_go_test!(
    test_session_auth,
    "Go `pkg/session/test/privileges/privileges_test.go:47::TestSessionAuth`.",
    "go-parity-gap: Go session Auth and privilege-manager password lookup are not transcreated"
);
ignored_go_test!(
    test_resource_group_hint_in_txn,
    "Go `pkg/session/test/resourcegrouptest/resource_group_test.go:25::TestResourceGroupHintInTxn`.",
    "go-parity-gap: failpoint-driven TiKV transaction resource-group callbacks are not transcreated"
);
ignored_go_test!(
    test_main_schematest,
    "Go `pkg/session/test/schematest/main_test.go:29::TestMain`.",
    "go-parity-gap: Go TestMain/common test harness is not a Rust behavior surface"
);
ignored_go_test!(
    test_prepare_stmt_commit_when_schema_changed,
    "Go `pkg/session/test/schematest/schema_test.go:57::TestPrepareStmtCommitWhenSchemaChanged`.",
    "go-parity-gap: multi-session schema lease/MDL commit validation is not transcreated"
);
ignored_go_test!(
    test_retry_schema_change_for_empty_change,
    "Go `pkg/session/test/schematest/schema_test.go:83::TestRetrySchemaChangeForEmptyChange`.",
    "go-parity-gap: mock-TiKV schema lease retry and transaction validation are not transcreated"
);
ignored_go_test!(
    test_table_reader_chunk,
    "Go `pkg/session/test/schematest/schema_test.go:105::TestTableReaderChunk`.",
    "go-parity-gap: TiKV region splitting, chunk readers, and DistSQL concurrency are not transcreated"
);
ignored_go_test!(
    test_insert_exec_chunk,
    "Go `pkg/session/test/schematest/schema_test.go:158::TestInsertExecChunk`.",
    "go-parity-gap: mock-TiKV chunk execution and streaming record sets are not transcreated"
);
ignored_go_test!(
    test_update_exec_chunk,
    "Go `pkg/session/test/schematest/schema_test.go:193::TestUpdateExecChunk`.",
    "go-parity-gap: mock-TiKV chunk execution and streaming record sets are not transcreated"
);
ignored_go_test!(
    test_delete_exec_chunk,
    "Go `pkg/session/test/schematest/schema_test.go:230::TestDeleteExecChunk`.",
    "go-parity-gap: mock-TiKV chunk execution and streaming record sets are not transcreated"
);
ignored_go_test!(
    test_delete_multi_table_exec_chunk,
    "Go `pkg/session/test/schematest/schema_test.go:260::TestDeleteMultiTableExecChunk`.",
    "go-parity-gap: multi-table DistSQL chunk execution is not transcreated"
);
ignored_go_test!(
    test_index_look_up_reader_chunk,
    "Go `pkg/session/test/schematest/schema_test.go:312::TestIndexLookUpReaderChunk`.",
    "go-parity-gap: mock-TiKV region splitting and index-lookup streaming are not transcreated"
);
ignored_go_test!(
    test_txn_size,
    "Go `pkg/session/test/schematest/schema_test.go:377::TestTxnSize`.",
    "go-parity-gap: Go kv.Transaction mutation-size accounting is not transcreated"
);
ignored_go_test!(
    test_validation_recursion,
    "Go `pkg/session/test/schematest/schema_test.go:393::TestValidationRecursion`.",
    "go-parity-gap: Go global-variable accessor and recursive validation registry are not transcreated"
);
ignored_go_test!(
    test_schema_checker_sql,
    "Go `pkg/session/test/session_test.go:61::TestSchemaCheckerSQL`.",
    "go-parity-gap: schema lease, MDL, and cross-session schema checker are not transcreated"
);
ignored_go_test!(
    test_load_schema_failed,
    "Go `pkg/session/test/session_test.go:133::TestLoadSchemaFailed`.",
    "go-parity-gap: failpoint-driven Domain schema reload failure is not transcreated"
);
ignored_go_test!(
    test_write_on_multiple_cached_table,
    "Go `pkg/session/test/session_test.go:188::TestWriteOnMultipleCachedTable`.",
    "go-parity-gap: cached-table leases and table-cache storage are not transcreated"
);
ignored_go_test!(
    test_fix_set_tidb_snapshot_ts,
    "Go `pkg/session/test/session_test.go:229::TestFixSetTiDBSnapshotTS`.",
    "go-parity-gap: TiKV GC safe-point metadata and historical snapshot reload are not transcreated"
);
ignored_go_test!(
    test_do_ddl_job_quit,
    "Go `pkg/session/test/session_test.go:334::TestDoDDLJobQuit`.",
    "go-parity-gap: cancellable Go DDL owner and store-close failpoint are not transcreated"
);
ignored_go_test!(
    test_process_info_issue22068,
    "Go `pkg/session/test/session_test.go:362::TestProcessInfoIssue22068`.",
    "go-parity-gap: concurrent sleep/process-info plan publication is not transcreated"
);
ignored_go_test!(
    test_per_stmt_task_id,
    "Go `pkg/session/test/session_test.go:379::TestPerStmtTaskID`.",
    "go-parity-gap: Go statement-context task IDs are not exposed by this Rust session"
);
ignored_go_test!(
    test_stmt_hints,
    "Go `pkg/session/test/session_test.go:396::TestStmtHints`.",
    "go-parity-gap: complete Go statement-hint session-variable matrix is not transcreated"
);
ignored_go_test!(
    test_rollback_on_compile_error,
    "Go `pkg/session/test/session_test.go:477::TestRollbackOnCompileError`.",
    "go-parity-gap: multi-session schema reload and compile-error retry are not transcreated"
);
ignored_go_test!(
    test_match_identity,
    "Go `pkg/session/test/session_test.go:578::TestMatchIdentity`.",
    "go-parity-gap: Go privilege host-pattern and reverse-DNS identity matching are not transcreated"
);
ignored_go_test!(
    test_handle_assertion_failure_for_partitioned_table,
    "Go `pkg/session/test/session_test.go:625::TestHandleAssertionFailureForPartitionedTable`.",
    "go-parity-gap: TiKV assertion-failure decoding and log hook are not transcreated"
);
ignored_go_test!(
    test_random_binary,
    "Go `pkg/session/test/session_test.go:641::TestRandomBinary`.",
    "go-parity-gap: mysql.stats_top_n storage writes and internal request-source context are not transcreated"
);
ignored_go_test!(
    test_request_source,
    "Go `pkg/session/test/session_test.go:690::TestRequestSource`.",
    "go-parity-gap: TiKV RPC interceptor request-origin/source propagation is not transcreated"
);
ignored_go_test!(
    test_empty_init_sql_file,
    "Go `pkg/session/test/session_test.go:749::TestEmptyInitSQLFile`.",
    "go-parity-gap: Go Domain bootstrap and initialize-sql-file configuration are not transcreated"
);
ignored_go_test!(
    test_init_system_variable,
    "Go `pkg/session/test/session_test.go:764::TestInitSystemVariable`.",
    "go-parity-gap: bootstrap initialize-sql-file execution and persisted system variables are not transcreated"
);
ignored_go_test!(
    test_init_users,
    "Go `pkg/session/test/session_test.go:818::TestInitUsers`.",
    "go-parity-gap: bootstrap initialize-sql-file user/privilege lifecycle is not transcreated"
);
ignored_go_test!(
    test_bootstrap_sql_with_extension,
    "Go `pkg/session/test/session_test.go:926::TestBootstrapSQLWithExtension`.",
    "go-parity-gap: extension registry, custom auth plugin, and bootstrap SQL are not transcreated"
);
ignored_go_test!(
    test_error_happen_while_init,
    "Go `pkg/session/test/session_test.go:998::TestErrorHappenWhileInit`.",
    "go-parity-gap: bootstrap SQL error recovery and persisted Domain initialization are not transcreated"
);
ignored_go_test!(
    test_issue60266,
    "Go `pkg/session/test/session_test.go:1073::TestIssue60266`.",
    "go-parity-gap: generated-column regexp behavior under TiDB NO_BACKSLASH_ESCAPES is not transcreated"
);
ignored_go_test!(
    test_process_info_for_stale_read_auto_commit,
    "Go `pkg/session/test/session_test.go:1123::TestProcessInfoForStaleReadAutoCommit`.",
    "go-parity-gap: stale-read timestamp process metadata, goroutine interruption, and session manager are not transcreated"
);
ignored_go_test!(
    test_get_db_names,
    "Go `pkg/session/test/session_test.go:1158::TestGetDBNames`.",
    "go-parity-gap: Go query metric DB-label tracking is not exposed by this Rust session"
);
ignored_go_test!(
    test_main_temporarytabletest,
    "Go `pkg/session/test/temporarytabletest/main_test.go:29::TestMain`.",
    "go-parity-gap: Go TestMain/common test harness is not a Rust behavior surface"
);
ignored_go_test!(
    test_schema_checker_temp_table,
    "Go `pkg/session/test/temporarytabletest/temporary_table_test.go:320::TestSchemaCheckerTempTable`.",
    "go-parity-gap: global-temporary-table MDL and cross-session schema lease are not transcreated"
);
ignored_go_test!(
    test_main_txn,
    "Go `pkg/session/test/txn/main_test.go:29::TestMain`.",
    "go-parity-gap: Go TestMain/common test harness is not a Rust behavior surface"
);
ignored_go_test!(
    test_txn_lazy_initialize,
    "Go `pkg/session/test/txn/txn_test.go:131::TestTxnLazyInitialize`.",
    "go-parity-gap: Go LazyTxn validity/start-TS API is not exposed by this Rust session"
);
ignored_go_test!(
    test_disable_txn_auto_retry,
    "Go `pkg/session/test/txn/txn_test.go:188::TestDisableTxnAutoRetry`.",
    "go-parity-gap: TiKV write conflicts, retry policy, local latches, and schema leases are not transcreated"
);
ignored_go_test!(
    test_auto_commit_respects_read_only,
    "Go `pkg/session/test/txn/txn_test.go:286::TestAutoCommitRespectsReadOnly`.",
    "go-parity-gap: concurrent restricted-read-only commit checks and privilege override are not transcreated"
);
ignored_go_test!(
    test_txn_retry_err_msg,
    "Go `pkg/session/test/txn/txn_test.go:324::TestTxnRetryErrMsg`.",
    "go-parity-gap: failpoint-driven TiKV retry error composition is not transcreated"
);
ignored_go_test!(
    test_error_rollback,
    "Go `pkg/session/test/txn/txn_test.go:346::TestErrorRollback`.",
    "go-parity-gap: concurrent TiKV duplicate-key rollback and retry cleanup are not transcreated"
);
ignored_go_test!(
    test_in_trans,
    "Go `pkg/session/test/txn/txn_test.go:378::TestInTrans`.",
    "go-parity-gap: Go LazyTxn.Valid lifecycle and protocol status flags are not exposed by this Rust session"
);
ignored_go_test!(
    test_commit_ts_order_check,
    "Go `pkg/session/test/txn/txn_test.go:424::TestCommitTSOrderCheck`.",
    "go-parity-gap: failpoint-controlled Oracle commit timestamp ordering is not transcreated"
);
ignored_go_test!(
    test_mem_buffer_snapshot_read,
    "Go `pkg/session/test/txn/txn_test.go:438::TestMemBufferSnapshotRead`.",
    "go-parity-gap: Go membuffer snapshot/UnionScan interaction over TiKV is not transcreated"
);
ignored_go_test!(
    test_mem_buffer_cleanup_memory_leak,
    "Go `pkg/session/test/txn/txn_test.go:483::TestMemBufferCleanupMemoryLeak`.",
    "go-parity-gap: Go membuffer cleanup accounting and query memory quota are not transcreated"
);
ignored_go_test!(
    test_panic_on_rollback_killed_txn,
    "Go `pkg/session/test/txn/txn_test.go:509::TestPanicOnRollbackKilledTxn`.",
    "go-parity-gap: killed pessimistic-transaction rollback cleanup is not transcreated"
);
