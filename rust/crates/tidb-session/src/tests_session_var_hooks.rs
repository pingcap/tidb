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

//! System variables whose value Go computes at READ time (`sysvar.go`'s
//! `GetSession` hooks) or refuses at WRITE time (its `Validation` hooks),
//! rather than storing in the variable table.

use crate::tests_support::row_text;
use crate::Session;

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .unwrap_or_default()
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestCollationServer`: the
/// validation canonicalizes case, unknown names return 1273, and the session
/// hook mirrors the collation's charset into `character_set_server`.
#[test]
fn collation_server_normalizes_rejects_and_updates_charset() {
    let mut session = Session::new();

    session.run("SET collation_server = 'LATIN1_bin'").unwrap();
    assert_eq!(one(&mut session, "SELECT @@collation_server"), "latin1_bin");
    assert_eq!(one(&mut session, "SELECT @@character_set_server"), "latin1");

    let error = session
        .run("SET collation_server = 'BOGUSCOLLation'")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1273);
    assert_eq!(error.message, "Unknown collation: 'BOGUSCOLLation'");
    assert_eq!(one(&mut session, "SELECT @@collation_server"), "latin1_bin");
    assert_eq!(one(&mut session, "SELECT @@character_set_server"), "latin1");

    session.run("SET collation_server = 'utf8mb4_bin'").unwrap();
    assert_eq!(
        one(&mut session, "SELECT @@collation_server"),
        "utf8mb4_bin"
    );
    assert_eq!(
        one(&mut session, "SELECT @@character_set_server"),
        "utf8mb4"
    );
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestDefaultCollationForUTF8MB4`:
/// accepted names normalize and warn with 1681, while a non-utf8mb4
/// collation is rejected with the registered 3721 error.
#[test]
fn default_collation_for_utf8mb4_normalizes_warns_and_rejects_other_charsets() {
    let mut session = Session::new();

    session
        .run("SET default_collation_for_utf8mb4 = 'utf8mb4_BIN'")
        .unwrap();
    let warnings = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(
        one(&mut session, "SELECT @@default_collation_for_utf8mb4"),
        "utf8mb4_bin"
    );
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0][1], "1681");
    assert_eq!(
        warnings[0][2],
        "Updating 'default_collation_for_utf8mb4' is deprecated. It will be made read-only in a future release."
    );

    session
        .run("SET default_collation_for_utf8mb4 = 'utf8mb4_GENeral_CI'")
        .unwrap();
    let warnings = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(
        one(&mut session, "SELECT @@default_collation_for_utf8mb4"),
        "utf8mb4_general_ci"
    );
    assert_eq!(warnings.len(), 1);

    let error = session
        .run("SET default_collation_for_utf8mb4 = 'LATIN1_bin'")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 3721);
    assert_eq!(
        error.message,
        "Invalid default collation latin1_bin: utf8mb4_0900_ai_ci or utf8mb4_general_ci or utf8mb4_bin expected"
    );
    assert_eq!(
        one(&mut session, "SELECT @@default_collation_for_utf8mb4"),
        "utf8mb4_general_ci"
    );
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
}

/// Go `TestSessionGetterFuncs`: session-only status variables read their live
/// fields rather than the registry defaults, including the zero-value JSON
/// shape of `LastQueryInfo`.
#[test]
fn session_getter_functions_read_live_defaults() {
    let mut session = Session::new();

    assert_eq!(one(&mut session, "SELECT @@tidb_current_ts"), "0");
    assert_eq!(one(&mut session, "SELECT @@tidb_last_txn_info"), "");
    assert_eq!(
        one(&mut session, "SELECT @@tidb_last_query_info"),
        "{\"txn_scope\":\"\",\"start_ts\":0,\"for_update_ts\":0,\"ru_consumption\":0}"
    );
    assert_eq!(one(&mut session, "SELECT @@last_plan_from_cache"), "0");
    assert_eq!(one(&mut session, "SELECT @@last_plan_from_binding"), "0");
}

/// Go `pkg/sessionctx/variable/sysvar_test.go::TestInstanceScopedVars`:
/// instance-only values are read from the process/config tier, not from a
/// session copy or a stale catalog default.
#[test]
fn instance_scoped_getters_read_live_config_defaults() {
    let mut session = Session::new();
    let expected = [
        ("tidb_general_log", "OFF"),
        ("tidb_pprof_sql_cpu", "0"),
        ("tidb_expensive_query_time_threshold", "60"),
        ("tidb_expensive_txn_time_threshold", "600"),
        ("tidb_memory_usage_alarm_ratio", "0.7"),
        ("tidb_memory_usage_alarm_keep_record_num", "5"),
        ("tidb_force_priority", "NO_PRIORITY"),
        ("ddl_slow_threshold", "300"),
        ("plugin_dir", "/data/deploy/plugin"),
        ("plugin_load", ""),
        ("tidb_slow_log_threshold", "300"),
        ("tidb_record_plan_in_slow_log", "1"),
        ("tidb_enable_slow_log", "ON"),
        ("tidb_check_mb4_value_in_utf8", "ON"),
        ("tidb_enable_collect_execution_info", "ON"),
        ("tidb_log_file_max_days", "0"),
        ("tidb_rc_read_check_ts", "OFF"),
    ];
    for (name, value) in expected {
        assert_eq!(session.vars.get_system(name).unwrap(), value, "{name}");
    }

    let config = session.vars.get_system("tidb_config").unwrap();
    assert!(config.starts_with('{'));
    assert!(config.contains("instance"));
}

/// Go `TestLcTimeNamesReadOnly`, `TestLcMessages`, and
/// `TestDefaultCharsetAndCollation`: locale and charset compatibility
/// variables expose their captured defaults, `lc_messages` remains mutable,
/// and the read-only `lc_time_names` write is rejected with 1238.
#[test]
fn locale_and_charset_compatibility_variables_match_go() {
    let mut session = Session::new();

    assert_eq!(
        one(&mut session, "SELECT @@character_set_connection"),
        "utf8mb4"
    );
    assert_eq!(
        one(&mut session, "SELECT @@collation_connection"),
        "utf8mb4_bin"
    );
    assert_eq!(one(&mut session, "SELECT @@lc_messages"), "en_US");
    assert_eq!(one(&mut session, "SELECT @@lc_time_names"), "en_US");

    session.run("SET lc_messages = 'zh_CN'").unwrap();
    assert_eq!(one(&mut session, "SELECT @@lc_messages"), "zh_CN");

    let error = session
        .run("SET GLOBAL lc_time_names = 'newvalue'")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1238);
    assert_eq!(one(&mut session, "SELECT @@lc_time_names"), "en_US");
}

/// Go's deprecated compatibility variables keep their source-specific
/// validation side effects: the TiFlash pipeline switch warns but stores the
/// requested boolean, MPP store-fail TTL is forced to `0s`, and column
/// tracking is forced to `ON`.
#[test]
fn deprecated_compatibility_variables_match_go() {
    let mut session = Session::new();

    session
        .run("SET GLOBAL tidb_enable_tiflash_pipeline_model = OFF")
        .unwrap();
    let warnings = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0][1], "1681");
    assert_eq!(
        warnings[0][2],
        "tidb_enable_tiflash_pipeline_model is deprecated and will be removed in a future release."
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT @@global.tidb_enable_tiflash_pipeline_model"
        ),
        "0"
    );

    session
        .run("SET tidb_mpp_store_fail_ttl = '10s'")
        .unwrap();
    let warnings = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0][0], "Warning");
    assert_eq!(warnings[0][1], "1105");
    assert_eq!(
        warnings[0][2],
        "tidb_mpp_store_fail_ttl is always 0s. This variable has been deprecated and will be removed in the future releases"
    );
    assert_eq!(one(&mut session, "SELECT @@tidb_mpp_store_fail_ttl"), "0s");

    session
        .run("SET GLOBAL tidb_enable_column_tracking = OFF")
        .unwrap();
    let warnings = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0][1], "1681");
    assert_eq!(
        warnings[0][2],
        "The 'tidb_enable_column_tracking' variable is deprecated and will be removed in future versions of TiDB. It is always set to 'ON' now."
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT @@global.tidb_enable_column_tracking"
        ),
        "1"
    );
}

/// Go's `tidb_scatter_region` Validation accepts only the empty, `table`, and
/// `global` modes, stores the mode lowercased, and leaves the previous value
/// untouched after a refusal.
#[test]
fn scatter_region_validation_matches_go() {
    let mut session = Session::new();

    session
        .run("SET tidb_scatter_region = 'TaBlE'")
        .unwrap();
    assert_eq!(one(&mut session, "SELECT @@tidb_scatter_region"), "table");

    session
        .run("SET GLOBAL tidb_scatter_region = 'GLOBAL'")
        .unwrap();
    assert_eq!(
        one(&mut session, "SELECT @@global.tidb_scatter_region"),
        "global"
    );

    let error = session
        .run("SET tidb_scatter_region = 'invalid'")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1105);
    assert_eq!(
        error.message,
        "invalid value for 'invalid', it should be either '', 'table' or 'global'"
    );
    assert_eq!(one(&mut session, "SELECT @@tidb_scatter_region"), "table");

    session.run("SET tidb_scatter_region = ''").unwrap();
    assert_eq!(one(&mut session, "SELECT @@tidb_scatter_region"), "");
}

/// `sql_auto_is_null` carries the same `Validation` as the five read-only
/// no-op variables: turning it ON needs `tidb_enable_noop_functions`, and the
/// refusal branch returns `Off` rather than the requested value.
///
/// The `SET_VAR` hint is where that returned value shows: Go applies a hint
/// through `SetSystemVarWithRelaxedValidation`, which keeps the value the
/// hook returned and discards its error, so the statement succeeds while the
/// variable reads `0`. Source rows: `tests/integrationtest/t/session/vars.test`.
#[test]
fn a_noop_gated_variable_refuses_to_off_and_a_hint_takes_that_value() {
    let mut session = Session::new();
    assert_eq!(
        one(&mut session, "SELECT @@tidb_enable_noop_functions"),
        "OFF"
    );

    // The plain SET is the branch that keeps the error.
    let error = session
        .run("SET sql_auto_is_null = 1")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1235);
    assert_eq!(one(&mut session, "SELECT @@sql_auto_is_null"), "0");

    // The hint is the branch that keeps only the value.
    assert_eq!(
        one(
            &mut session,
            "SELECT /*+ SET_VAR(sql_auto_is_null=1) */ @@sql_auto_is_null"
        ),
        "0"
    );

    // With the gate open, both branches take the requested value -- so this
    // is the gate speaking, not a blanket refusal of the variable.
    session.run("SET @@tidb_enable_noop_functions = 1").unwrap();
    assert_eq!(
        one(
            &mut session,
            "SELECT /*+ SET_VAR(sql_auto_is_null=1) */ @@sql_auto_is_null"
        ),
        "1"
    );
    session.run("SET sql_auto_is_null = 1").unwrap();
    assert_eq!(one(&mut session, "SELECT @@sql_auto_is_null"), "1");
}

/// `@@warning_count` is Go's `SysWarningCount`: the count of the PREVIOUS
/// statement's warnings, snapshotted by `ResetContextOfStmt` at every
/// statement start.
///
/// The warning BUFFER is inherited only by the three statements that report
/// it, so reading the buffer answers `0` for every other statement --
/// including one asked immediately after a statement that warned. The counts
/// are a separate channel for exactly that reason.
#[test]
fn warning_count_reports_the_previous_statements_warnings() {
    let mut session = Session::new();
    assert_eq!(one(&mut session, "SELECT @@warning_count"), "0");

    // A duplicated `SET_VAR` hint is warning 3126.
    session
        .run(
            "SELECT /*+ SET_VAR(group_concat_max_len = 1024) \
             SET_VAR(group_concat_max_len = 2048) */ 1",
        )
        .unwrap();
    assert_eq!(one(&mut session, "SELECT @@warning_count"), "1");
    // The reading statement itself warned about nothing, so the next read is
    // back to zero -- the recorded sequence in `session/vars`.
    assert_eq!(one(&mut session, "SELECT @@session.warning_count"), "0");
    assert_eq!(one(&mut session, "SELECT @@local.warning_count"), "0");
    // `SHOW WARNINGS` still reports the buffer it inherits, which is the
    // other channel and is unchanged.
    assert_eq!(one(&mut session, "SELECT @@error_count"), "0");
}
