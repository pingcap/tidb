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

static GLOBAL_STATE: Mutex<()> = Mutex::new(());

fn lock_global_state() -> MutexGuard<'static, ()> {
    GLOBAL_STATE
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
    assert!(global_sem().unwrap().is_invisible_table("mysql", "db"));
    assert!(!global_sem().unwrap().is_invisible_table("test1", "tbl2"));

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

    // An `IMPORT INTO ... FROM SELECT ...` carries no path and is allowed.
    assert!(!import_from_local_rule(&StmtView::new(
        StmtKind::ImportInto {
            from_select: true,
            path: String::new(),
        }
    )));
    // A remote object store is not local.
    assert!(!import_from_local_rule(&StmtView::new(
        StmtKind::ImportInto {
            from_select: false,
            path: "s3://bucket/path".to_owned(),
        }
    )));
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

    assert_eq!(
        sem.is_restricted_hint("resource_group").unwrap_err(),
        "the RESOURCE_GROUP() optimizer hint is restricted under the current security policy and is ignored"
    );
}

// The top-level accessors read the process-wide policy and return Go's
// disabled-SEM answers when none is installed.
#[test]
fn top_level_accessors_follow_the_global_policy() {
    let _guard = lock_global_state();
    let _registry = FakeRegistry::install();
    let _restore = Restore;
    set_tidb_release_version(Some("v9.0.0".to_owned()));

    assert!(!is_enabled());
    assert!(!is_invisible_schema("mysql"));
    assert!(!is_invisible_table("mysql", "user"));
    assert!(!is_restricted_privilege("SUPER"));
    assert!(!is_invisible_sys_var(SUPER_READ_ONLY));
    assert!(!is_read_only_variable(TIDB_ENABLE_ENHANCED_SECURITY));
    assert!(!is_invisible_status_var("tidb_gc_leader_desc"));
    assert!(!is_restricted_sql(&StmtView::new(StmtKind::Other)));
    assert!(is_restricted_hint("memory_quota").is_ok());

    let mut config = test_config();
    config.restricted_status_var = vec!["tidb_gc_leader_desc".to_owned()];
    config.restricted_sql = SQLRestriction {
        sql: vec![" drop database ".to_owned(), String::new()],
        rule: vec!["select_into_file".to_owned()],
    };
    enable_by(&config).unwrap();

    assert!(is_enabled());
    assert!(is_read_only_variable(TIDB_ENABLE_ENHANCED_SECURITY));
    assert!(is_invisible_status_var("tidb_gc_leader_desc"));
    // A configured SQL command matches on the trimmed, upper-cased spelling.
    assert!(is_restricted_sql(&StmtView {
        sem_command: "DROP DATABASE".to_owned(),
        kind: StmtKind::Other,
    }));
    // A configured rule matches on the statement shape.
    assert!(is_restricted_sql(&StmtView::new(StmtKind::Select {
        select_into: true
    })));
    assert!(!is_restricted_sql(&StmtView::new(StmtKind::Other)));

    // `testhelper.go`'s privilege mutators.
    assert!(!is_restricted_privilege("RELOAD"));
    add_restricted_privileges_for_test("reload");
    assert!(is_restricted_privilege("RELOAD"));
    remove_restricted_privileges_for_test("reload");
    assert!(!is_restricted_privilege("RELOAD"));
    // Every `RESTRICTED_*` privilege is restricted without being configured.
    assert!(is_restricted_privilege("RESTRICTED_TABLES_ADMIN"));

    disable();
    assert!(!is_enabled());
    assert_eq!(
        get_sys_var(TIDB_ENABLE_ENHANCED_SECURITY).unwrap().value,
        OFF
    );
}

// `enable_from_path_for_test` enables SEM and its cleanup restores both the
// switch and the variables the config overrode.
#[test]
fn enable_from_path_round_trips() {
    let _guard = lock_global_state();
    let _registry = FakeRegistry::install();
    let _restore = Restore;
    set_tidb_release_version(Some("v9.0.0".to_owned()));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sem_config.json");
    std::fs::write(&path, serde_json::to_vec(&test_config()).unwrap()).unwrap();

    let cleanup = enable_from_path_for_test(path.to_str().unwrap()).unwrap();
    assert!(is_enabled());
    assert_eq!(
        get_sys_var(TIDB_ENABLE_ENHANCED_SECURITY).unwrap().value,
        CONFIG
    );

    cleanup();
    assert!(!is_enabled());
    // The captured default, not the `OFF` that `Disable` writes first.
    assert_eq!(
        get_sys_var(TIDB_ENABLE_ENHANCED_SECURITY).unwrap().value,
        OFF
    );
}

// The hand-rolled semver replacement parses and orders the way
// `coreos/go-semver` does for the comparisons `validateSEMConfig` makes.
#[test]
fn sem_version_parses_and_orders() {
    assert_eq!(SemVersion::parse("9.0.0").unwrap().to_string(), "9.0.0");
    assert_eq!(
        SemVersion::parse("8.4.0-alpha+build.1")
            .unwrap()
            .to_string(),
        "8.4.0-alpha+build.1"
    );
    assert!(SemVersion::parse("9.0").is_err());
    assert!(SemVersion::parse("9.0.0.1").is_err());
    assert!(SemVersion::parse("").is_err());
    assert!(SemVersion::parse("v9.0.0").is_err());

    let version = |text: &str| SemVersion::parse(text).unwrap();
    assert!(version("6.0.0") < version("9.0.0"));
    assert!(version("9.0.0") < version("99.0.0"));
    assert!(version("9.0.0") > version("9.0.0-alpha"));
    assert!(version("9.0.0-alpha") < version("9.0.0-beta"));
    assert!(version("9.0.0-alpha.1") < version("9.0.0-alpha.2"));
    assert!(version("9.0.0-1") < version("9.0.0-alpha"));
    // Build metadata does not affect precedence.
    assert_eq!(version("9.0.0+a"), version("9.0.0+a"));
    assert_eq!(
        version("9.0.0").cmp(&version("9.0.0")),
        std::cmp::Ordering::Equal
    );
}

// The inlined `objstore.IsLocal` over the scheme half of `url.Parse`.
#[test]
fn local_urls_are_scheme_less_file_or_local() {
    assert!(is_local_url("/bucket/path/to/file.csv"));
    assert!(is_local_url("file:///bucket/path"));
    assert!(is_local_url("local:///bucket/path"));
    assert!(is_local_url("./relative/path"));
    assert!(is_local_url(""));
    assert!(!is_local_url("s3://bucket/path"));
    assert!(!is_local_url("gs://bucket/path"));

    assert_eq!(url_scheme("S3://bucket"), Some("s3".to_owned()));
    assert_eq!(url_scheme("a+b-c.d://x"), Some("a+b-c.d".to_owned()));
    assert_eq!(url_scheme("1abc://x"), None);
    assert_eq!(url_scheme("/no/scheme"), None);
}

// Every name in `sqlRuleNameMap` resolves, and nothing else does.
#[test]
fn sql_rule_names_resolve() {
    for name in SQL_RULE_NAMES {
        assert!(sql_rule_by_name(name).is_some(), "{name}");
    }
    assert!(sql_rule_by_name("no_drop").is_none());
    assert_eq!(SQL_RULE_NAMES.len(), 5);
}
