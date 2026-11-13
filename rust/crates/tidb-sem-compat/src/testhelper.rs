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


//! Go `pkg/util/sem/compat/testhelper.go`.

/// Go `V1`: SEM v1.
pub const V1: &str = "v1";
/// Go `V2`: SEM v2.
pub const V2: &str = "v2";

/// Go `SwitchToSEMForTest`: switches to SEM v1 or v2 and returns the
/// cleanup.
///
/// # Panics
///
/// On an unknown version, or a v2 configuration that cannot be written or
/// enabled (Go's `t.Fatalf` / `require.NoError`).
pub fn switch_to_sem_for_test(version: &str) -> Box<dyn FnOnce()> {
    match version {
        V1 => {
            tidb_sem::enable();
            Box::new(tidb_sem::disable)
        }
        V2 => {
            tidb_sem_v2::set_tidb_release_version(Some("v9.0.0".to_owned()));
            static NEXT_FILE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let file = std::env::temp_dir().join(format!(
                "semv2_config_{}_{}.json",
                std::process::id(),
                NEXT_FILE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::write(&file, COMPATIBLE_SEM_V2_CONFIG).expect("failed to create temp file");
            let cleanup = tidb_sem_v2::enable_from_path_for_test(
                file.to_str().expect("a temp path is UTF-8"),
            );
            std::fs::remove_file(&file).expect("remove the temp file");
            Box::new(cleanup.expect("enable SEM v2"))
        }
        _ => panic!("unknown SEM version: {version}"),
    }
}

/// Go `compatibleSEMV2Config`: the SEM v2 configuration equivalent to v1.
const COMPATIBLE_SEM_V2_CONFIG: &str = r#"{
	"version": "1.0",
	"tidb_version": "v9.0.0",
	"restricted_databases": ["metrics_schema"],
	"restricted_tables": [
		{"schema": "mysql", "name": "expr_pushdown_blacklist", "hidden": true},
		{"schema": "mysql", "name": "gc_delete_range", "hidden": true},
		{"schema": "mysql", "name": "gc_delete_range_done", "hidden": true},
		{"schema": "mysql", "name": "opt_rule_blacklist", "hidden": true},
		{"schema": "mysql", "name": "tidb", "hidden": true},
		{"schema": "mysql", "name": "global_variables", "hidden": true},
		{"schema": "information_schema", "name": "cluster_config", "hidden": true},
		{"schema": "information_schema", "name": "cluster_hardware", "hidden": true},
		{"schema": "information_schema", "name": "cluster_load", "hidden": true},
		{"schema": "information_schema", "name": "cluster_log", "hidden": true},
		{"schema": "information_schema", "name": "cluster_systeminfo", "hidden": true},
		{"schema": "information_schema", "name": "inspection_result", "hidden": true},
		{"schema": "information_schema", "name": "inspection_rules", "hidden": true},
		{"schema": "information_schema", "name": "inspection_summary", "hidden": true},
		{"schema": "information_schema", "name": "metrics_summary", "hidden": true},
		{"schema": "information_schema", "name": "metrics_summary_by_label", "hidden": true},
		{"schema": "information_schema", "name": "metrics_tables", "hidden": true},
		{"schema": "information_schema", "name": "tidb_hot_regions", "hidden": true},
		{"schema": "performance_schema", "name": "pd_profile_allocs", "hidden": true},
		{"schema": "performance_schema", "name": "pd_profile_block", "hidden": true},
		{"schema": "performance_schema", "name": "pd_profile_cpu", "hidden": true},
		{"schema": "performance_schema", "name": "pd_profile_goroutines", "hidden": true},
		{"schema": "performance_schema", "name": "pd_profile_memory", "hidden": true},
		{"schema": "performance_schema", "name": "pd_profile_mutex", "hidden": true},
		{"schema": "performance_schema", "name": "tidb_profile_allocs", "hidden": true},
		{"schema": "performance_schema", "name": "tidb_profile_block", "hidden": true},
		{"schema": "performance_schema", "name": "tidb_profile_cpu", "hidden": true},
		{"schema": "performance_schema", "name": "tidb_profile_goroutines", "hidden": true},
		{"schema": "performance_schema", "name": "tidb_profile_memory", "hidden": true},
		{"schema": "performance_schema", "name": "tidb_profile_mutex", "hidden": true},
		{"schema": "performance_schema", "name": "tikv_profile_cpu", "hidden": true}
	],
	"restricted_status_variables": [
		"tidb_gc_leader_desc"
	],
	"restricted_variables": [
		{"name": "hostname", "hidden": false, "value": "localhost"},
		{"name": "tidb_enable_enhanced_security", "hidden": false, "value": "ON"},
		{"name": "ddl_slow_threshold", "hidden": true},
		{"name": "tidb_check_mb4_value_in_utf8", "hidden": true},
		{"name": "tidb_config", "hidden": true},
		{"name": "tidb_enable_slow_log", "hidden": true},
		{"name": "tidb_enable_telemetry", "hidden": true},
		{"name": "tidb_expensive_query_time_threshold", "hidden": true},
		{"name": "tidb_force_priority", "hidden": true},
		{"name": "tidb_general_log", "hidden": true},
		{"name": "tidb_metric_query_range_duration", "hidden": true},
		{"name": "tidb_metric_query_step", "hidden": true},
		{"name": "tidb_opt_write_row_id", "hidden": true},
		{"name": "tidb_pprof_sql_cpu", "hidden": true},
		{"name": "tidb_record_plan_in_slow_log", "hidden": true},
		{"name": "tidb_row_format_version", "hidden": true},
		{"name": "tidb_slow_query_file", "hidden": true},
		{"name": "tidb_slow_log_threshold", "hidden": true},
		{"name": "tidb_enable_collect_execution_info", "hidden": true},
		{"name": "tidb_memory_usage_alarm_ratio", "hidden": true},
		{"name": "tidb_redact_log", "hidden": true},
		{"name": "tidb_restricted_read_only", "hidden": true},
		{"name": "tidb_top_sql_max_time_series_count", "hidden": true},
		{"name": "tidb_top_sql_max_meta_count", "hidden": true}
	],
	"restricted_privileges": [
		"FILE",
		"BACKUP_ADMIN"
	],
	"restricted_sql": {
		"rule": [
			"time_to_live",
			"alter_table_attributes",
			"import_with_external_id"
		],
		"sql": [
			"BACKUP",
			"RESTORE",
			"ALTER RESOURCE GROUP"
		]
	}
}"#;
