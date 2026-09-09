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

//! Go `pkg/util/sem/compat`: dispatch between SEM v1 and SEM v2.

fn assert_exclusive() {
    crate::intest::assert_with_message(
        !(crate::sem::is_enabled() && crate::sem_v2::is_enabled()),
        "SEM v1 and v2 cannot be enabled at the same time",
    );
}

/// Whether either SEM v1 or SEM v2 is enabled.
pub fn is_enabled() -> bool {
    assert_exclusive();
    crate::sem::is_enabled() || crate::sem_v2::is_enabled()
}

/// Go `compat.IsInvisibleSchema`.
pub fn is_invisible_schema(db_name: &str) -> bool {
    assert_exclusive();
    (crate::sem::is_enabled() && crate::sem::is_invisible_schema(db_name))
        || (crate::sem_v2::is_enabled() && crate::sem_v2::is_invisible_schema(db_name))
}

/// Go `compat.IsInvisibleTable`.
pub fn is_invisible_table(db_lower_name: &str, tbl_lower_name: &str) -> bool {
    assert_exclusive();
    (crate::sem::is_enabled() && crate::sem::is_invisible_table(db_lower_name, tbl_lower_name))
        || (crate::sem_v2::is_enabled()
            && crate::sem_v2::is_invisible_table(db_lower_name, tbl_lower_name))
}

/// Go `compat.IsInvisibleStatusVar`.
pub fn is_invisible_status_var(var_name: &str) -> bool {
    assert_exclusive();
    (crate::sem::is_enabled() && crate::sem::is_invisible_status_var(var_name))
        || (crate::sem_v2::is_enabled() && crate::sem_v2::is_invisible_status_var(var_name))
}

/// Go `compat.IsInvisibleSysVar`.
pub fn is_invisible_sys_var(var_name: &str) -> bool {
    assert_exclusive();
    (crate::sem::is_enabled() && crate::sem::is_invisible_sys_var(var_name))
        || (crate::sem_v2::is_enabled() && crate::sem_v2::is_invisible_sys_var(var_name))
}

/// Go `compat.IsRestrictedPrivilege`.
pub fn is_restricted_privilege(privilege: &str) -> bool {
    assert_exclusive();
    crate::intest::assert_with_message(
        privilege.to_uppercase() == privilege,
        "privilege name must be uppercase",
    );
    (crate::sem::is_enabled() && crate::sem::is_restricted_privilege(privilege))
        || (crate::sem_v2::is_enabled() && crate::sem_v2::is_restricted_privilege(privilege))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, MutexGuard};

    use super::*;
    use crate::sem_v2::{
        Config, SysVar, SysVarRegistry, SysVarScope, TableRestriction, VariableRestriction,
    };

    // Go permits callers to discard these predicate results; Rust must not add
    // a `must_use` diagnostic at the transcreation boundary.
    #[test]
    #[deny(unused_must_use)]
    fn return_values_may_be_ignored_like_go() {
        let _lock = crate::SEM_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        crate::sem::disable();
        crate::sem_v2::disable();

        is_enabled();
        is_invisible_schema("test");
        is_invisible_table("test", "t");
        is_invisible_status_var("status");
        is_invisible_sys_var("sys");
        is_restricted_privilege("SELECT");
    }

    const MYSQL_TABLES: &[&str] = &[
        "expr_pushdown_blacklist",
        "gc_delete_range",
        "gc_delete_range_done",
        "opt_rule_blacklist",
        "tidb",
        "global_variables",
    ];
    const INFORMATION_SCHEMA_TABLES: &[&str] = &[
        "cluster_config",
        "cluster_hardware",
        "cluster_load",
        "cluster_log",
        "cluster_systeminfo",
        "inspection_result",
        "inspection_rules",
        "inspection_summary",
        "metrics_summary",
        "metrics_summary_by_label",
        "metrics_tables",
        "tidb_hot_regions",
    ];
    const PERFORMANCE_SCHEMA_TABLES: &[&str] = &[
        "pd_profile_allocs",
        "pd_profile_block",
        "pd_profile_cpu",
        "pd_profile_goroutines",
        "pd_profile_memory",
        "pd_profile_mutex",
        "tidb_profile_allocs",
        "tidb_profile_block",
        "tidb_profile_cpu",
        "tidb_profile_goroutines",
        "tidb_profile_memory",
        "tidb_profile_mutex",
        "tikv_profile_cpu",
    ];
    const HIDDEN_VARIABLES: &[&str] = &[
        "ddl_slow_threshold",
        "tidb_check_mb4_value_in_utf8",
        "tidb_config",
        "tidb_enable_slow_log",
        "tidb_enable_telemetry",
        "tidb_expensive_query_time_threshold",
        "tidb_force_priority",
        "tidb_general_log",
        "tidb_metric_query_range_duration",
        "tidb_metric_query_step",
        "tidb_opt_write_row_id",
        "tidb_pprof_sql_cpu",
        "tidb_record_plan_in_slow_log",
        "tidb_row_format_version",
        "tidb_slow_query_file",
        "tidb_slow_log_threshold",
        "tidb_enable_collect_execution_info",
        "tidb_memory_usage_alarm_ratio",
        "tidb_redact_log",
        "tidb_restricted_read_only",
        "tidb_top_sql_max_time_series_count",
        "tidb_top_sql_max_meta_count",
    ];

    struct Registry(Mutex<HashMap<String, SysVar>>);

    impl SysVarRegistry for Registry {
        fn get_sys_var(&self, name: &str) -> Option<SysVar> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(name)
                .cloned()
        }

        fn set_sys_var(&self, name: &str, value: &str) {
            if let Some(variable) = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_mut(name)
            {
                variable.value = value.to_owned();
            }
        }
    }

    struct EnabledV2 {
        _lock: MutexGuard<'static, ()>,
    }

    impl Drop for EnabledV2 {
        fn drop(&mut self) {
            crate::sem_v2::disable();
            crate::sem_v2::set_sys_var_registry(None);
            crate::sem_v2::set_tidb_release_version(None);
        }
    }

    fn switch_to_v2() -> EnabledV2 {
        let lock = crate::SEM_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        crate::sem::disable();
        crate::sem_v2::disable();
        crate::sem_v2::set_tidb_release_version(Some("v9.0.0".to_owned()));

        let mut variables = HIDDEN_VARIABLES
            .iter()
            .chain(["hostname", "tidb_enable_enhanced_security"].iter())
            .map(|name| {
                (
                    (*name).to_owned(),
                    SysVar {
                        scope: SysVarScope::None,
                        value: String::new(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        variables.insert(
            "tidb_enable_enhanced_security".to_owned(),
            SysVar {
                scope: SysVarScope::None,
                value: crate::sem_v2::OFF.to_owned(),
            },
        );
        crate::sem_v2::set_sys_var_registry(Some(Arc::new(Registry(Mutex::new(variables)))));

        let tables = MYSQL_TABLES
            .iter()
            .map(|name| ("mysql", *name))
            .chain(
                INFORMATION_SCHEMA_TABLES
                    .iter()
                    .map(|name| ("information_schema", *name)),
            )
            .chain(
                PERFORMANCE_SCHEMA_TABLES
                    .iter()
                    .map(|name| ("performance_schema", *name)),
            )
            .map(|(schema, name)| TableRestriction {
                schema: schema.to_owned(),
                name: name.to_owned(),
                hidden: true,
                columns: Vec::new(),
            })
            .collect();
        let mut restricted_variables = HIDDEN_VARIABLES
            .iter()
            .map(|name| VariableRestriction {
                name: (*name).to_owned(),
                hidden: true,
                readonly: false,
                value: String::new(),
            })
            .collect::<Vec<_>>();
        restricted_variables.extend([
            VariableRestriction {
                name: "hostname".to_owned(),
                hidden: false,
                readonly: false,
                value: "localhost".to_owned(),
            },
            VariableRestriction {
                name: "tidb_enable_enhanced_security".to_owned(),
                hidden: false,
                readonly: false,
                value: "ON".to_owned(),
            },
        ]);
        crate::sem_v2::enable_by(&Config {
            version: "1.0".to_owned(),
            tidb_version: "v9.0.0".to_owned(),
            restricted_databases: vec!["metrics_schema".to_owned()],
            restricted_tables: tables,
            restricted_variables,
            restricted_status_var: vec!["tidb_gc_leader_desc".to_owned()],
            restricted_privileges: vec!["FILE".to_owned(), "BACKUP_ADMIN".to_owned()],
            ..Config::default()
        })
        .unwrap();
        EnabledV2 { _lock: lock }
    }

    // Go `compat_test.go::TestInvisibleSchema`.
    #[test]
    fn invisible_schema() {
        let _sem = switch_to_v2();
        assert!(is_invisible_schema("metrics_schema"));
        assert!(is_invisible_schema("METRICS_ScHEma"));
        assert!(!is_invisible_schema("mysql"));
        assert!(!is_invisible_schema("information_schema"));
        assert!(!is_invisible_schema("Bogusname"));
    }

    // Go `compat_test.go::TestIsInvisibleTable`.
    #[test]
    fn invisible_table() {
        let _sem = switch_to_v2();
        for table in MYSQL_TABLES {
            assert!(is_invisible_table("mysql", table));
        }
        for table in INFORMATION_SCHEMA_TABLES {
            assert!(is_invisible_table("information_schema", table));
        }
        for table in PERFORMANCE_SCHEMA_TABLES {
            assert!(is_invisible_table("performance_schema", table));
        }
        assert!(is_invisible_table("metrics_schema", "acdc"));
        assert!(is_invisible_table("metrics_schema", "fdsgfd"));
        assert!(!is_invisible_table("test", "t1"));
    }

    // Go `compat_test.go::TestIsRestrictedPrivilege`.
    #[test]
    fn restricted_privilege() {
        let _sem = switch_to_v2();
        assert!(is_restricted_privilege("RESTRICTED_TABLES_ADMIN"));
        assert!(is_restricted_privilege("RESTRICTED_STATUS_VARIABLES_ADMIN"));
        assert!(is_restricted_privilege("BACKUP_ADMIN"));
        assert!(!is_restricted_privilege("CONNECTION_ADMIN"));
        assert!(!is_restricted_privilege("AA"));
    }

    // Go `compat_test.go::TestIsInvisibleStatusVar`.
    #[test]
    fn invisible_status_var() {
        let _sem = switch_to_v2();
        assert!(is_invisible_status_var("tidb_gc_leader_desc"));
        assert!(!is_invisible_status_var("server_id"));
        assert!(!is_invisible_status_var("ddl_schema_version"));
        assert!(!is_invisible_status_var("Ssl_version"));
    }

    // Go `compat_test.go::TestIsInvisibleSysVar`.
    #[test]
    fn invisible_sys_var() {
        let _sem = switch_to_v2();
        assert!(!is_invisible_sys_var("hostname"));
        assert!(!is_invisible_sys_var("tidb_enable_enhanced_security"));
        assert!(!is_invisible_sys_var("tidb_allow_remove_auto_inc"));
        for variable in HIDDEN_VARIABLES {
            assert!(is_invisible_sys_var(variable), "{variable}");
        }
    }
}
