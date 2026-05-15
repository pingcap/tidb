// Copyright 2025 PingCAP, Inc.
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

package compat

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	semv1 "github.com/pingcap/tidb/pkg/util/sem"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
	"github.com/stretchr/testify/require"
)

const (
	// V1 represents SEM v1
	V1 = "v1"
	// V2 represents SEM v2
	V2 = "v2"
)

// SwitchToSEMForTest is a function that switches to SEM v1 or v2 and return a cleanup function in test.
func SwitchToSEMForTest(t *testing.T, version string) func() {
	switch version {
	case V1:
		semv1.Enable()
		return func() {
			semv1.Disable()
		}
	case V2:
		mysql.TiDBReleaseVersion = "v9.0.0"

		file, err := os.CreateTemp(t.TempDir(), "semv2_config_*.json")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		defer func() {
			require.NoError(t, file.Close())
			require.NoError(t, os.Remove(file.Name()))
		}()

		_, err = file.WriteString(compatibleSEMV2Config)
		require.NoError(t, err)
		cleanup, err := semv2.EnableFromPathForTest(file.Name())
		require.NoError(t, err)
		return cleanup
	}

	t.Fatalf("unknown SEM version: %s", version)
	return nil
}

var compatibleSEMV2Config = `{
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
}`
