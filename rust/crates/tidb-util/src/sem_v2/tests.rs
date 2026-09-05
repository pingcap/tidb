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

//! The package's tests.
//!
//! Go's tests reach the process-wide `variable` registry, `mysql`'s release
//! version, and `globalSem`; Rust runs tests in parallel, so each test takes
//! [`GLOBAL_STATE`] and installs the [`FakeRegistry`] that stands in for
//! `pkg/sessionctx/variable`'s sysvar registry.
//!
//! Go's `TestSQLRules` parses eleven statements with the real parser and feeds
//! the AST to each rule. This crate cannot depend on the parser, so each case
//! carries the [`StmtView`] that statement parses to, with the SQL kept in a
//! comment.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use super::*;

fn lock_global_state() -> MutexGuard<'static, ()> {
    crate::SEM_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go `vardef.SuperReadOnly`.
const SUPER_READ_ONLY: &str = "super_read_only";
/// Go `vardef.TiDBMemQuotaQuery`.
const TIDB_MEM_QUOTA_QUERY: &str = "tidb_mem_quota_query";

/// The subset of `pkg/sessionctx/variable`'s registry these tests need.
#[derive(Debug)]
struct FakeRegistry {
    vars: Mutex<HashMap<String, SysVar>>,
}

impl FakeRegistry {
    fn install() -> Arc<Self> {
        let mut vars = HashMap::new();
        // `autocommit` is a tunable session/global variable, so SEM rejects a
        // configured value for it.
        vars.insert(
            "autocommit".to_owned(),
            SysVar {
                scope: SysVarScope::Other,
                value: ON.to_owned(),
            },
        );
        vars.insert(
            SUPER_READ_ONLY.to_owned(),
            SysVar {
                scope: SysVarScope::Global,
                value: OFF.to_owned(),
            },
        );
        vars.insert(
            TIDB_MEM_QUOTA_QUERY.to_owned(),
            SysVar {
                scope: SysVarScope::Session,
                value: "1073741824".to_owned(),
            },
        );
        // The one variable SEM writes: read-only, so a configured value is
        // accepted.
        vars.insert(
            TIDB_ENABLE_ENHANCED_SECURITY.to_owned(),
            SysVar {
                scope: SysVarScope::None,
                value: OFF.to_owned(),
            },
        );
        let registry = Arc::new(Self {
            vars: Mutex::new(vars),
        });
        set_sys_var_registry(Some(Arc::clone(&registry) as Arc<dyn SysVarRegistry>));
        registry
    }
}

impl SysVarRegistry for FakeRegistry {
    fn get_sys_var(&self, name: &str) -> Option<SysVar> {
        self.vars
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    fn set_sys_var(&self, name: &str, value: &str) {
        let mut vars = self
            .vars
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(var) = vars.get_mut(name) {
            var.value = value.to_owned();
        }
    }
}

/// Restores every piece of process-wide state a test touches.
struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        disable();
        set_sys_var_registry(None);
        set_tidb_release_version(None);
    }
}

/// Go `testConfig` from `sem_test.go`.
fn test_config() -> Config {
    Config {
        version: "1.0".to_owned(),
        tidb_version: "v9.0.0".to_owned(),
        restricted_privileges: vec!["SUPER".to_owned(), "process".to_owned()],
        restricted_databases: vec!["mysql".to_owned(), "test".to_owned()],
        restricted_tables: vec![
            TableRestriction {
                schema: "mysql".to_owned(),
                name: "user".to_owned(),
                hidden: true,
                columns: Vec::new(),
            },
            TableRestriction {
                schema: "test".to_owned(),
                name: "tbl2".to_owned(),
                hidden: false,
                columns: Vec::new(),
            },
        ],
        restricted_variables: vec![
            VariableRestriction {
                name: SUPER_READ_ONLY.to_owned(),
                hidden: true,
                readonly: false,
                value: String::new(),
            },
            VariableRestriction {
                name: TIDB_ENABLE_ENHANCED_SECURITY.to_owned(),
                hidden: false,
                readonly: true,
                value: ON.to_owned(),
            },
        ],
        ..Config::default()
    }
}

/// Go `parseSEMConfig`: writes the config to a temp file and parses it back.
fn parse_sem_config(config_str: &str) -> (tempfile::TempDir, Result<Config, String>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sem_config.json");
    std::fs::write(&path, config_str).unwrap();
    let parsed = config::parse_sem_config_from_file(path.to_str().unwrap());
    (dir, parsed)
}

#[test]
#[deny(unused_must_use)]
fn return_values_may_be_ignored_like_go() {
    get_sys_var("unknown");
    tidb_release_version();

    let sem = build_sem_from_config(&Config::default());
    sem.is_invisible_schema("mysql");
    sem.is_invisible_table("mysql", "user");
    sem.is_restricted_privilege("SELECT");
    sem.is_invisible_sys_var("autocommit");
    sem.is_invisible_status_var("Threads_connected");
    sem.is_read_only_variable("autocommit");

    let stmt = StmtView::new(StmtKind::Other);
    sem.is_restricted_sql(&stmt);
    is_invisible_schema("mysql");
    is_invisible_table("mysql", "user");
    is_restricted_privilege("SELECT");
    is_invisible_sys_var("autocommit");
    is_read_only_variable("autocommit");
    is_invisible_status_var("Threads_connected");
    is_restricted_sql(&stmt);
    is_enabled();
    time_to_live_sql_rule(&stmt);
    alter_table_attributes_rule(&stmt);
    import_with_external_id_rule(&stmt);
    select_into_file_rule(&stmt);
    import_from_local_rule(&stmt);
}

// Go `TestParseConfigWithDifferentFormat`.
#[test]
fn parse_config_with_different_format() {
    let cases = [
        (
            "valid config",
            r#"{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_databases": ["mysql", "test"],
				"restricted_tables": [
					{
						"schema": "test",
						"name": "t1",
						"hidden": false,
						"columns": [
							{"name": "c1", "hidden": true, "value": "default"}
						]
					}
				],
				"restricted_variables": [
					{"name": "autocommit", "hidden": false, "readonly": true, "value": "1"}
				],
				"restricted_privileges": ["SUPER"],
				"restricted_sql": {
					"sql": ["DROP DATABASE"],
					"rule": ["no_drop"]
				}
			}"#,
            false,
        ),
        (
            "invalid JSON",
            r#"{"version": "1.0", "tidb_version": "v6.0.0","#,
            true,
        ),
        ("empty JSON", "{}", false),
    ];

    for (name, config, want_err) in cases {
        let (_dir, parsed) = parse_sem_config(config);
        assert_eq!(parsed.is_err(), want_err, "{name}");
    }
}

// Go `TestValidateConfig`.
#[test]
fn validate_config() {
    let _guard = lock_global_state();
    let _registry = FakeRegistry::install();
    let _restore = Restore;
    set_tidb_release_version(Some("v9.0.0".to_owned()));

    let cases = [
        (
            "valid config",
            r#"{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_variables": [
					{"name": "autocommit", "hidden": false, "readonly": true, "value": ""}
				]
			}"#,
            "",
        ),
        (
            "invalid TiDB version",
            r#"{
				"version": "1.0",
				"tidb_version": "v99.0.0"
			}"#,
            "current TiDB version",
        ),
        (
            "unknown variable",
            r#"{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_variables": [
					{"name": "invalid_var", "hidden": false, "readonly": true, "value": "1"}
				]
			}"#,
            "restricted variable invalid_var is not a valid system variable",
        ),
        (
            "invalid value for variable",
            r#"{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_variables": [
					{"name": "autocommit", "hidden": false, "readonly": true, "value": "1"}
				]
			}"#,
            "restricted variable autocommit has a value set, but it is not a readonly variable",
        ),
        (
            "invalid restricted SQL rule",
            r#"{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_sql": {
					"sql": ["DROP DATABASE"],
					"rule": ["unknown_rule"]
				}
			}"#,
            "unknown SQL rule: unknown_rule",
        ),
    ];

    for (name, config, err_msg) in cases {
        let (_dir, parsed) = parse_sem_config(config);
        let sem_config = parsed.unwrap_or_else(|error| panic!("{name}: {error}"));
        match validate_sem_config(&sem_config) {
            Ok(()) => assert!(err_msg.is_empty(), "{name}: expected error {err_msg:?}"),
            Err(error) => {
                assert!(
                    !err_msg.is_empty(),
                    "{name}: expected no error, got {error}"
                );
                assert!(
                    error.contains(err_msg),
                    "{name}: expected error to contain {err_msg:?}, got {error}"
                );
            }
        }
    }
}

// Go `TestSEMMethods`.
#[test]
fn sem_methods() {
    let _guard = lock_global_state();
    let _registry = FakeRegistry::install();
    let _restore = Restore;

    let sem = build_sem_from_config(&test_config());

    // Test restricted privileges
    assert!(sem.is_restricted_privilege("SUPER"));
    assert!(sem.is_restricted_privilege("PROCESS"));
    assert!(!sem.is_restricted_privilege("RELOAD"));

    // Test restricted databases
    assert!(sem.is_invisible_schema("mysql"));
    assert!(sem.is_invisible_schema("test"));
    assert!(!sem.is_invisible_schema("information_schema"));

    // Test restricted tables
    assert!(sem.is_invisible_table("mysql", "user"));
    assert!(sem.is_invisible_table("mysql", "db"));
    assert!(!sem.is_invisible_table("test1", "tbl2"));

    // Test restricted variables
    assert!(sem.is_invisible_sys_var(SUPER_READ_ONLY));
    assert!(!sem.is_invisible_sys_var(TIDB_ENABLE_ENHANCED_SECURITY));

    // Test overrideRestrictedVariable
    assert_eq!(
        get_sys_var(TIDB_ENABLE_ENHANCED_SECURITY).unwrap().value,
        OFF
    );
    sem.override_restricted_variable();
    assert_eq!(
        get_sys_var(TIDB_ENABLE_ENHANCED_SECURITY).unwrap().value,
        ON
    );
}

// Go `TestEnableSEM`.
#[test]
fn enable_sem() {
    let _guard = lock_global_state();
    let _registry = FakeRegistry::install();
    let _restore = Restore;
    set_tidb_release_version(Some("v9.0.0".to_owned()));

    assert!(!is_enabled());
    assert_eq!(
        get_sys_var(TIDB_ENABLE_ENHANCED_SECURITY).unwrap().value,
        OFF
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sem_config.json");
    std::fs::write(&path, serde_json::to_vec(&test_config()).unwrap()).unwrap();

    enable(path.to_str().unwrap()).unwrap();

    // Test restricted privileges
    assert!(is_restricted_privilege("SUPER"));
    assert!(is_restricted_privilege("PROCESS"));
    assert!(!is_restricted_privilege("RELOAD"));

    // Test restricted databases
    assert!(is_invisible_schema("mysql"));
    assert!(is_invisible_schema("test"));
    assert!(!is_invisible_schema("information_schema"));

    // Test restricted tables
    assert!(is_invisible_table("mysql", "user"));
    assert!(load_global_sem().unwrap().is_invisible_table("mysql", "db"));
    assert!(!load_global_sem()
        .unwrap()
        .is_invisible_table("test1", "tbl2"));

    // Test restricted variables
    assert!(is_invisible_sys_var(SUPER_READ_ONLY));
    assert!(!is_invisible_sys_var(TIDB_ENABLE_ENHANCED_SECURITY));

    // Test overrideRestrictedVariable
    assert_eq!(
        get_sys_var(TIDB_ENABLE_ENHANCED_SECURITY).unwrap().value,
        CONFIG
    );
}

// Go `TestSQLRules`. Each case names the statement Go parses and the AST shape
// it parses to.
#[test]
fn sql_rules() {
    let ttl_create = StmtView::new(StmtKind::CreateTable {
        options: vec![TableOptionType::Ttl],
    });
    let ttl_alter = StmtView::new(StmtKind::AlterTable {
        specs: vec![AlterTableSpec {
            tp: AlterTableType::Option,
            options: vec![TableOptionType::Ttl],
        }],
    });
    let remove_ttl = StmtView::new(StmtKind::AlterTable {
        specs: vec![AlterTableSpec {
            tp: AlterTableType::RemoveTtl,
            options: Vec::new(),
        }],
    });
    let attributes = StmtView::new(StmtKind::AlterTable {
        specs: vec![AlterTableSpec {
            tp: AlterTableType::Attributes,
            options: Vec::new(),
        }],
    });
    let import_s3 = StmtView::new(StmtKind::ImportInto {
        from_select: false,
        path: "s3://xxx/xxx?external-id=xxx".to_owned(),
    });
    let select_outfile = StmtView::new(StmtKind::Select { select_into: true });
    let import_bare_path = StmtView::new(StmtKind::ImportInto {
        from_select: false,
        path: "/bucket/path/to/file.csv".to_owned(),
    });
    let import_file_url = StmtView::new(StmtKind::ImportInto {
        from_select: false,
        path: "file:///bucket/path/to/file.csv".to_owned(),
    });
    let load_bare_path = StmtView::new(StmtKind::LoadData {
        file_loc_client: false,
        path: "/bucket/path/to/file.csv".to_owned(),
    });
    let load_file_url = StmtView::new(StmtKind::LoadData {
        file_loc_client: false,
        path: "file:///bucket/path/to/file.csv".to_owned(),
    });
    let load_local = StmtView::new(StmtKind::LoadData {
        file_loc_client: true,
        path: "file:///bucket/path/to/file.csv".to_owned(),
    });

    let cases: [(SQLRule, &StmtView, bool, &str); 11] = [
        (
            time_to_live_sql_rule,
            &ttl_create,
            true,
            "CREATE TABLE t (a DATETIME) TTL = a + INTERVAL 1 DAY",
        ),
        (
            time_to_live_sql_rule,
            &ttl_alter,
            true,
            "ALTER TABLE t TTL = a + INTERVAL 1 DAY",
        ),
        (
            time_to_live_sql_rule,
            &remove_ttl,
            true,
            "ALTER TABLE t REMOVE TTL",
        ),
        (
            alter_table_attributes_rule,
            &attributes,
            true,
            "ALTER TABLE t ATTRIBUTES 'merge_option=deny'",
        ),
        (
            import_with_external_id_rule,
            &import_s3,
            false,
            "IMPORT INTO xxxx FROM 's3://xxx/xxx?external-id=xxx'",
        ),
        (
            select_into_file_rule,
            &select_outfile,
            true,
            "SELECT * FROM t1 INTO OUTFILE '/tmp/t1.txt' ",
        ),
        (
            import_from_local_rule,
            &import_bare_path,
            true,
            "IMPORT INTO t1 FROM '/bucket/path/to/file.csv'",
        ),
        (
            import_from_local_rule,
            &import_file_url,
            true,
            "IMPORT INTO t1 FROM 'file:///bucket/path/to/file.csv'",
        ),
        (
            import_from_local_rule,
            &load_bare_path,
            true,
            "LOAD DATA INFILE '/bucket/path/to/file.csv' INTO TABLE t1",
        ),
        (
            import_from_local_rule,
            &load_file_url,
            true,
            "LOAD DATA INFILE 'file:///bucket/path/to/file.csv' INTO TABLE t1",
        ),
        (
            import_from_local_rule,
            &load_local,
            false,
            "LOAD DATA LOCAL INFILE 'file:///bucket/path/to/file.csv' INTO TABLE t1",
        ),
    ];

    for (rule, stmt, expected, sql) in cases {
        assert_eq!(rule(stmt), expected, "SQL rule failed for statement: {sql}");
    }
}

// Go `TestRestrictedHint`.
#[test]
fn restricted_hint() {
    let sem = build_sem_from_config(&Config {
        restricted_variables: vec![VariableRestriction {
            name: TIDB_MEM_QUOTA_QUERY.to_owned(),
            hidden: true,
            readonly: false,
            value: String::new(),
        }],
        restricted_hints: vec![
            "resource_group".to_owned(),
            "memory_quota".to_owned(),
            "max_execution_time".to_owned(),
        ],
        ..Config::default()
    });

    // A hint with no backing variable is restricted unconditionally.
    assert!(sem.is_restricted_hint("resource_group").is_err());
    // A variable-overriding hint whose variable is hidden is restricted.
    assert!(sem.is_restricted_hint("memory_quota").is_err());
    // A variable-overriding hint whose variable is still tunable is allowed.
    assert!(sem.is_restricted_hint("max_execution_time").is_ok());
    // A hint not listed in restricted_hints is allowed.
    assert!(sem.is_restricted_hint("use_index").is_ok());
}
