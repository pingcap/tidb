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

//! Go `pkg/executor/set_test.go` (`TestSetClusterConfig`), on the mock store
//! through the session, as Go's testkit runs it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tidb_domain::cluster_topology::ClusterServer;

use crate::support::testkit::*;

fn server(server_type: &str, address: &str, status_address: &str) -> ClusterServer {
    ClusterServer {
        server_type: server_type.to_owned(),
        address: address.to_owned(),
        status_address: status_address.to_owned(),
        version: String::new(),
        git_hash: String::new(),
        start_timestamp: 0,
        server_id: 0,
    }
}

/// Go `TestSetClusterConfig`.
#[test]
fn test_set_cluster_config() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    let servers_info = vec![
        server("tidb", "127.0.0.1:1111", "127.0.0.1:1111"),
        server("tidb", "127.0.0.1:2222", "127.0.0.1:2222"),
        server("pd", "127.0.0.1:3333", "127.0.0.1:3333"),
        server("pd", "127.0.0.1:4444", "127.0.0.1:4444"),
        server("tikv", "127.0.0.1:5555", "127.0.0.1:5555"),
        server("tikv", "127.0.0.1:6666", "127.0.0.1:6666"),
        server("tiflash", "127.0.0.1:3933", "127.0.0.1:7777"),
        server("tso", "127.0.0.1:22379", "127.0.0.1:22379"),
        server("scheduling", "127.0.0.1:22479", "127.0.0.1:22479"),
    ];
    tk.session()
        .set_test_set_config_server_info(Arc::new(move || Ok(servers_info.clone())));

    let err = |tk: &mut TestKit, sql: &str| tk.exec_to_err(sql).expect("the statement fails").message;
    assert_eq!(err(&mut tk, "set config xxx log.level='info'"), "unknown type xxx");
    assert_eq!(err(&mut tk, "set config tidb log.level='info'"), "TiDB doesn't support to change configs online, please use SQL variables");
    assert_eq!(err(&mut tk, "set config tso log.level='info'"), "tso doesn't support to change configs online");
    assert_eq!(err(&mut tk, "set config scheduling log.level='info'"), "scheduling doesn't support to change configs online");
    assert_eq!(err(&mut tk, "set config '127.0.0.1:1111' log.level='info'"), "TiDB doesn't support to change configs online, please use SQL variables");
    assert_eq!(err(&mut tk, "set config '127.a.b.c:1234' log.level='info'"), "invalid instance 127.a.b.c:1234"); // name doesn't resolve.
    assert_eq!(err(&mut tk, "set config 'example.com:1111' log.level='info'"), "instance example.com:1111 is not found in this cluster"); // name resolves.
    assert_eq!(err(&mut tk, "set config tikv log.level=null"), "can't set config to null");
    assert_eq!(err(&mut tk, "set config '1.1.1.1:1111' log.level='info'"), "instance 1.1.1.1:1111 is not found in this cluster");
    assert_eq!(err(&mut tk, "set config tikv `raftstore.max-peer-down-duration`=DEFAULT"), "Unknown DEFAULT for SET CONFIG");
    assert!(err(&mut tk, "set config tiflash `server.snap-max-write-bytes-per-sec`='500MB'")
        .contains("This command can only change config items begin with 'raftstore-proxy'"));

    let http_cnt = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&http_cnt);
    tk.session().set_test_set_config_http_handler(Arc::new(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok((200, "200 OK".to_owned(), String::new()))
    }));
    tk.must_exec("set config tikv log.level='info'");
    assert_eq!(http_cnt.load(Ordering::SeqCst), 2);

    http_cnt.store(0, Ordering::SeqCst);
    tk.must_exec("set config '127.0.0.1:5555' log.level='info'");
    assert_eq!(http_cnt.load(Ordering::SeqCst), 1);

    http_cnt.store(0, Ordering::SeqCst);
    let counter = Arc::clone(&http_cnt);
    tk.session().set_test_set_config_http_handler(Arc::new(move |_, body| {
        counter.fetch_add(1, Ordering::SeqCst);
        // The `raftstore.` prefix is stripped.
        let got: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(got, serde_json::json!({"server.snap-max-write-bytes-per-sec": "500MB"}));
        Ok((200, "200 OK".to_owned(), String::new()))
    }));
    tk.must_exec("set config tiflash `raftstore-proxy.server.snap-max-write-bytes-per-sec`='500MB'");
    assert_eq!(http_cnt.load(Ordering::SeqCst), 1);

    http_cnt.store(0, Ordering::SeqCst);
    tk.session()
        .set_test_set_config_http_handler(Arc::new(|_, _| Err("something wrong".to_owned())));
    tk.must_exec("set config tikv log.level='info'");
    tk.must_query("show warnings").check(["Warning 1105 something wrong", "Warning 1105 something wrong"]);

    tk.session().set_test_set_config_http_handler(Arc::new(|_, _| {
        Ok((400, "400 Bad Request".to_owned(), "WRONG".to_owned()))
    }));
    tk.must_exec("set config tikv log.level='info'");
    tk.must_query("show warnings").check([
        "Warning 1105 bad request to http://127.0.0.1:5555/config: WRONG",
        "Warning 1105 bad request to http://127.0.0.1:6666/config: WRONG",
    ]);
}

/// Go `SessionVars.GetSessionOrGlobalSystemVar`: `SHOW VARIABLES` reads a
/// variable with a `GetSession` hook through the hook, as `@@x` does --
/// the previous statement's warning count, the statement's time -- not the
/// value stored for it. Expected rows: `./gorun` on this branch.
#[test]
fn show_variables_answers_get_session_hooks() {
    let mut tk = TestKit::new();
    tk.must_query("select 1/0").check(["<nil>"]);
    tk.must_query("show variables like 'warning_count'").check(["warning_count 1"]);
    tk.must_query("select @@warning_count").check(["0"]);
    tk.must_query("show variables like 'error_count'").check(["error_count 0"]);
    tk.must_query("show variables like 'last_insert_id'").check(["last_insert_id 0"]);
    tk.must_query("show variables like 'tidb_current_ts'").check(["tidb_current_ts 0"]);
    tk.must_query("show variables like 'last_plan_from_cache'").check(["last_plan_from_cache OFF"]);
    tk.must_query("show variables like 'rand_seed1'").check(["rand_seed1 0"]);
    let timestamp = tk.must_query("show variables like 'timestamp'");
    let value: f64 = timestamp.rows()[0][1].parse().unwrap();
    assert!(value > 1.0e9, "timestamp reads the statement's time: {value}");
    tk.must_exec("set rand_seed1 = 7");
    tk.must_query("select @@rand_seed1").check(["0"]);
}

/// Go `sysvar.go`'s `authentication_ldap_*` variables: `SetGlobal`
/// configures `ldap.LDAPSimpleAuthImpl` / `LDAPSASLAuthImpl` and `GetGlobal`
/// reads it back -- the defaults the boot rebuild set, a masked root
/// password, a CA path that failed to load, and the simple auth method's
/// refusal. Expected rows: `./gorun` on this branch.
#[test]
fn authentication_ldap_variables_configure_the_ldap_impls() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_query(
        "select @@global.authentication_ldap_simple_server_port, @@global.authentication_ldap_sasl_init_pool_size, \
         @@global.authentication_ldap_sasl_auth_method_name, @@global.authentication_ldap_simple_user_search_attr, \
         @@global.authentication_ldap_sasl_tls",
    )
    .check(["389 10 SCRAM-SHA-1 uid 0"]);
    tk.must_exec("set global authentication_ldap_simple_bind_root_pwd = 'pw'");
    tk.must_query("select @@global.authentication_ldap_simple_bind_root_pwd").check(["******"]);
    tk.must_query("show global variables like 'authentication_ldap_simple_bind_root_pwd'")
        .check(["authentication_ldap_simple_bind_root_pwd ******"]);
    // The table keeps the password; only `GetGlobal` masks it.
    tk.must_query(
        "select variable_value from mysql.global_variables where variable_name = 'authentication_ldap_simple_bind_root_pwd'",
    )
    .check(["pw"]);
    assert_eq!(tidb_privilege_ldap::LDAP_SIMPLE_AUTH_IMPL.get_bind_root_pw(), "pw");
    tk.must_exec("set global authentication_ldap_simple_server_port = 1636");
    assert_eq!(tidb_privilege_ldap::LDAP_SIMPLE_AUTH_IMPL.get_ldap_server_port(), 1636);
    tk.must_get_err_msg(
        "set global authentication_ldap_simple_auth_method_name = 'x'",
        "auth method X is not supported",
    );
    tk.must_exec_to_err("set global authentication_ldap_sasl_ca_path = '/nonexistent/ca'");
    tk.must_query("select @@global.authentication_ldap_sasl_ca_path").check(["/nonexistent/ca"]);
    tk.must_exec("set global authentication_ldap_sasl_ca_path = ''");
    tk.must_exec("set global authentication_ldap_simple_bind_root_pwd = ''");
    tk.must_exec("set global authentication_ldap_simple_server_port = 389");
}

/// Go `session.loadCommonGlobalVariablesIfNeeded`: a new session takes each
/// session-cache variable it does not hold through its `SetSession`
/// (`SetSystemVarWithRelaxedValidation`); `collation_server`'s pairs the
/// `skipInit` `character_set_server`, and `collation_connection`'s the
/// connection charset.
#[test]
fn a_new_session_loads_each_variable_through_its_set_session() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global collation_server = 'latin1_bin'");
    let mut tk2 = tk.new_tk();
    tk2.must_query("select @@character_set_server, @@collation_server")
        .check(["latin1 latin1_bin"]);
    tk2.must_exec("set collation_connection = 'gbk_bin'");
    tk2.must_query("select @@character_set_connection").check(["gbk"]);
    tk.must_exec("set global collation_server = default");
}

/// Go `sysvar.go`'s `SetGlobal` / `GetGlobal` over `vardef`'s process state:
/// `SET GLOBAL` stores the process-wide value, and `@@global` reads it back
/// from there -- so a change another part of the process makes (the TTL job
/// manager's switch, the user-creation shortcut) is what `@@global` reports.
#[test]
fn global_reads_of_process_state_variables_answer_from_the_process() {
    use std::sync::atomic::Ordering::SeqCst;
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_ttl_job_enable = OFF");
    assert!(!tidb_vardef::ENABLE_TTL_JOB.load(SeqCst));
    tk.must_query("select @@global.tidb_ttl_job_enable").check(["0"]);
    tidb_vardef::ENABLE_TTL_JOB.store(true, SeqCst);
    tk.must_query("select @@global.tidb_ttl_job_enable").check(["1"]);
    tk.must_exec("set global tidb_ttl_scan_worker_count = 7");
    assert_eq!(tidb_vardef::TTL_SCAN_WORKER_COUNT.load(SeqCst), 7);
    tk.must_query("show global variables like 'tidb_ttl_scan_worker_count'")
        .check(["tidb_ttl_scan_worker_count 7"]);
    tk.must_exec("set global tidb_ttl_scan_worker_count = default");
    tk.must_exec("set global tidb_ttl_job_enable = default");
}

/// Go `GetGlobal` formatting of process state: `%g` for the memory alarm
/// ratio (`1e-05`), the OOM action's registered spelling, `'f', -1` for the
/// reserved percentage, and `FullDayTimeFormat` for a TTL window bound.
/// Expected rows: `./gorun` on this branch.
#[test]
fn global_reads_of_process_state_use_gos_formatting() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_memory_usage_alarm_ratio = 0.00001");
    tk.must_query("select @@global.tidb_memory_usage_alarm_ratio").check(["1e-05"]);
    tk.must_exec("set global tidb_mem_oom_action = 'log'");
    tk.must_query("select @@global.tidb_mem_oom_action").check(["LOG"]);
    tk.must_exec("set global tidb_instance_plan_cache_reserved_percentage = 0.25");
    tk.must_query("select @@global.tidb_instance_plan_cache_reserved_percentage").check(["0.25"]);
    tk.must_exec("set global tidb_ttl_job_schedule_window_start_time = '01:02 +0800'");
    tk.must_query("select @@global.tidb_ttl_job_schedule_window_start_time").check(["01:02 +0800"]);
    for name in [
        "tidb_memory_usage_alarm_ratio",
        "tidb_mem_oom_action",
        "tidb_instance_plan_cache_reserved_percentage",
        "tidb_ttl_job_schedule_window_start_time",
    ] {
        tk.must_exec(&format!("set global {name} = default"));
    }
}

/// The `SetGlobal` / `GetGlobal` Go keeps on these variables themselves:
/// the analyze defaults, the auto-analyze priority queue, TS validation, the
/// committer concurrency, the metadata-lock switch, the schema cache size's
/// origin text and the process-wide log redaction.
#[test]
fn variables_carry_their_own_process_state_hooks() {
    use std::sync::atomic::Ordering::SeqCst;
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_analyze_default_num_buckets = 300");
    assert_eq!(tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(SeqCst), 300);
    tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.store(301, SeqCst);
    tk.must_query("select @@global.tidb_analyze_default_num_buckets").check(["301"]);
    tk.must_exec("set global tidb_analyze_default_num_topn = 50");
    assert_eq!(tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(SeqCst), 50);
    tk.must_exec("set global tidb_enable_ts_validation = OFF");
    assert!(!tikv_client::oracle::oracles::ENABLE_TS_VALIDATION.load(SeqCst));
    tk.must_exec("set global tidb_enable_ts_validation = ON");
    assert!(tikv_client::oracle::oracles::ENABLE_TS_VALIDATION.load(SeqCst));
    tk.must_exec("set global tidb_committer_concurrency = 64");
    assert_eq!(tidb_tikvutil::COMMITTER_CONCURRENCY.load(SeqCst), 64);
    tidb_tikvutil::COMMITTER_CONCURRENCY.store(65, SeqCst);
    tk.must_query("select @@global.tidb_committer_concurrency").check(["65"]);
    tk.must_exec("set global tidb_schema_cache_size = '600MB'");
    tk.must_query("select @@global.tidb_schema_cache_size").check(["600MB"]);
    tk.must_exec("set global tidb_redact_log = MARKER");
    assert!(matches!(tidb_mysql::errors::redaction_mode(), tidb_mysql::errors::RedactionMode::Marker));
    for name in [
        "tidb_analyze_default_num_buckets",
        "tidb_analyze_default_num_topn",
        "tidb_enable_ts_validation",
        "tidb_committer_concurrency",
        "tidb_schema_cache_size",
        "tidb_redact_log",
    ] {
        tk.must_exec(&format!("set global {name} = default"));
    }
}

/// Go `initDomainSysVars`' `SetPDClientDynamicOption`: the PD client
/// options' `SetGlobal` reach the domain, which stores `vardef`'s value that
/// their `GetGlobal` reads back; an unset slot is Go's noop.
#[test]
fn pd_client_options_store_and_read_their_process_state() {
    use std::sync::atomic::Ordering::SeqCst;
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_tso_follower_proxy = ON");
    tk.must_query("select @@global.tidb_enable_tso_follower_proxy").check(["1"]);
    tk.must_exec("set global tidb_tso_client_batch_max_wait_time = 1.5");
    assert_eq!(f64::from_bits(tidb_vardef::MAX_TSO_BATCH_WAIT_INTERVAL.load(SeqCst)), 1.5);
    tk.must_query("select @@global.tidb_tso_client_batch_max_wait_time").check(["1.5"]);
    tk.must_exec("set global tidb_low_resolution_tso_update_interval = 100");
    assert_eq!(tidb_vardef::LOW_RESOLUTION_TSO_UPDATE_INTERVAL.load(SeqCst), 100);
    for name in [
        "tidb_enable_tso_follower_proxy",
        "tidb_tso_client_batch_max_wait_time",
        "tidb_low_resolution_tso_update_interval",
    ] {
        tk.must_exec(&format!("set global {name} = default"));
    }
}

/// Go's statement-summary variables call `stmtsummaryv2`'s setters from their
/// own `SetGlobal`, and the memory-arbitration ones set the process-wide
/// limit.
#[test]
fn stmt_summary_and_memory_variables_set_their_process_state() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_stmt_summary = OFF");
    assert!(!tidb_stmtsummary::v2::stmtsummary::enabled());
    tk.must_exec("set global tidb_enable_stmt_summary = ON");
    assert!(tidb_stmtsummary::v2::stmtsummary::enabled());
    tk.must_exec("set global tidb_stmt_summary_internal_query = ON");
    assert!(tidb_stmtsummary::v2::stmtsummary::enabled_internal());
    tk.must_exec("set global tidb_server_memory_limit_sess_min_size = 1048576");
    assert_eq!(
        tidb_memory::SERVER_MEMORY_LIMIT_SESS_MIN_SIZE.load(std::sync::atomic::Ordering::SeqCst),
        1048576
    );
    for name in [
        "tidb_enable_stmt_summary",
        "tidb_stmt_summary_internal_query",
        "tidb_server_memory_limit_sess_min_size",
    ] {
        tk.must_exec(&format!("set global {name} = default"));
    }
}
